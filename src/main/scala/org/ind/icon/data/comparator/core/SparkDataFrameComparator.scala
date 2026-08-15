package org.ind.icon.data.comparator.core

import org.ind.icon.data.comparator.model._
import org.ind.icon.data.comparator.transform.JsonDiffFlattener
import org.ind.icon.data.comparator.utils.ComparatorHelpers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * The core orchestrator for massive-scale DataFrame Comparison.
 * Executes the "Two-Tiered Evaluation" pattern. Modularized to keep core logic readable.
 */
class SparkDataFrameComparator extends DataFrameComparator with Serializable {

  override def compare(source: DataFrame, target: DataFrame, config: ComparatorConfig): ComparatorResult = {
    ComparatorHelpers.validateSchemasAndKeys(source, target, config)

    val spark = source.sparkSession

    // Standardize Arrays
    val sPrepared = ComparatorHelpers.standardizeArrays(source, config)
    val tPrepared = ComparatorHelpers.standardizeArrays(target, config)

    // Tier 1: Anti-Joins for Missing/Extra
    val missingRecords = sPrepared.join(tPrepared, config.primaryKeys, "left_anti")
    val extraRecords = tPrepared.join(sPrepared, config.primaryKeys, "left_anti")
    val joined = sPrepared.as("s").join(tPrepared.as("t"), config.primaryKeys, "inner")
    
    val commonCols = sPrepared.columns.intersect(tPrepared.columns)
    val payloadCols = commonCols.filterNot(c => config.primaryKeys.contains(c) || config.ignoreColumns.contains(c))
    
    if (payloadCols.isEmpty) {
      return ComparatorHelpers.buildEmptyResult(joined, missingRecords, extraRecords, spark)
    }

    // Tier 1: Wide-Schema Join & Mismatch Identification
    val (matchedRecords, mismatchedRecords) = performTier1Comparison(joined, payloadCols, sPrepared.schema, config)

    // Tier 2: Deep JSON Diffing (Triggered ONLY for complex columns on mismatched rows)
    val complexMismatchesDf = performTier2ComplexDiffing(mismatchedRecords, payloadCols, config, spark)

    ComparatorResult(matchedRecords, missingRecords, extraRecords, mismatchedRecords, complexMismatchesDf)
  }

  /**
   * Evaluates all payload columns simultaneously using Spark Catalyst expressions.
   * Isolates rows that have at least one column mismatch.
   */
  private def performTier1Comparison(joined: DataFrame, payloadCols: Array[String], schema: StructType, config: ComparatorConfig): (DataFrame, DataFrame) = {
    val mismatchChecks = payloadCols.map(c => ComparatorHelpers.buildMismatchCheckExpr(c, schema(c).dataType, config))
    
    val comparedDf = joined
      .withColumn("mismatched_columns_raw", array(mismatchChecks: _*))
      .withColumn("mismatched_columns", expr("filter(mismatched_columns_raw, x -> x is not null)")) 
      .withColumn("source_data", struct(payloadCols.map(c => col(s"s.$c")): _*))
      .withColumn("target_data", struct(payloadCols.map(c => col(s"t.$c")): _*))
      .drop("mismatched_columns_raw")
      .select(config.primaryKeys.map(col) ++ Seq(col("mismatched_columns"), col("source_data"), col("target_data")): _*)

    val matched = comparedDf.filter(size(col("mismatched_columns")) === 0).drop("mismatched_columns", "target_data").select("source_data.*")
    val mismatched = comparedDf.filter(size(col("mismatched_columns")) > 0)
    
    (matched, mismatched)
  }

  /**
   * Converts complex columns into JSON strings and delegates to the Jackson UDF.
   */
  private def performTier2ComplexDiffing(mismatchedRecords: DataFrame, payloadCols: Array[String], config: ComparatorConfig, spark: SparkSession): DataFrame = {
    val complexCols = payloadCols.filter(c => {
      val dt = mismatchedRecords.schema("source_data").dataType.asInstanceOf[StructType](c).dataType
      dt.isInstanceOf[ArrayType] || dt.isInstanceOf[StructType] || dt.isInstanceOf[MapType]
    })

    if (complexCols.isEmpty) return ComparatorHelpers.buildEmptyComplexDf(spark)

    val complexRows = mismatchedRecords.filter(arrays_overlap(col("mismatched_columns"), typedLit(complexCols)))
    
    if (complexRows.isEmpty) return ComparatorHelpers.buildEmptyComplexDf(spark)

    val diffUdf = udf((sJson: String, tJson: String) => 
      JsonDiffFlattener.diffComplexTypes(sJson, tJson, config.complexTypeKeys, config.enableNumericTolerance, config.numericTolerance)
    )
    
    // CRITICAL: ignoreNullFields -> false ensures explicit nulls are serialized into JSON and tracked
    val jsonOpts = Map("ignoreNullFields" -> "false")
    val sJsonExpr = to_json(struct(complexCols.map(c => col(s"source_data.$c")): _*), jsonOpts)
    val tJsonExpr = to_json(struct(complexCols.map(c => col(s"target_data.$c")): _*), jsonOpts)

    complexRows
      .withColumn("pk_json", to_json(struct(config.primaryKeys.map(col): _*)))
      .withColumn("complex_diffs", diffUdf(sJsonExpr, tJsonExpr))
      .select(col("pk_json"), explode(col("complex_diffs")).as("diff"))
      .select(
        col("pk_json"), 
        col("diff.column_path").as("column_path"), 
        col("diff.source_val").as("source_val"), 
        col("diff.target_val").as("target_val")
      )
  }

  override def compareAndWrite(source: DataFrame, target: DataFrame, config: ComparatorConfig, sink: ComparatorSinkConfig): ComparatorSinkResult = {
    val result = compare(source, target, config)
 // CRITICAL FIX: The DataFrames returned here are physically detached from the heavy execution DAG. 
    // They are simple Parquet scans of the data that was just written.
    val (missingCount, missingDf) = ComparatorHelpers.writeDeltaAndRead(result.missingRecords, "missing", sink)
    val (extraCount, extraDf) = ComparatorHelpers.writeDeltaAndRead(result.extraRecords, "extra", sink)
    val (mismatchCount, mismatchedDf) = ComparatorHelpers.writeDeltaAndRead(result.mismatchedRecords, "mismatched", sink)
    val (complexCount, complexDf) = ComparatorHelpers.writeDeltaAndRead(result.complexMismatches, "complex_mismatches", sink)

    // Matched rows are usually massive. We only materialize them to Delta if explicitly configured.
    val matchedDf = if (sink.writeMatchedRecords) {
      ComparatorHelpers.writeDeltaAndRead(result.matchedRecords, "matched", sink)._2
    } else {
      result.matchedRecords // Retained as an unresolved lazy DAG
    }

    val materializedResult = ComparatorResult(
      matchedRecords = matchedDf,
      missingRecords = missingDf,
      extraRecords = extraDf,
      mismatchedRecords = mismatchedDf,
      complexMismatches = complexDf
    )

    ComparatorSinkResult(
      runId = sink.runId,
      missingCount = missingCount,
      extraCount = extraCount,
      mismatchCount = mismatchCount,
      complexMismatchCount = complexCount,
      comparatorResult = materializedResult
    )
  }
}