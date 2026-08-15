package org.ind.icon.data.comparator.core

import org.ind.icon.data.comparator.model._
import org.ind.icon.data.comparator.transform.{FieldDiff, JsonDiffFlattener}
import org.ind.icon.data.comparator.utils.ComparatorHelpers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class SparkDataFrameComparator extends DataFrameComparator with Serializable {

  def compare(source: DataFrame, target: DataFrame, config: ComparatorConfig): ComparatorResult = {
    require(config.primaryKeys.nonEmpty, "ComparatorConfig must specify at least one primaryKey column.")
    val spark = source.sparkSession

    val sPrepared = ComparatorHelpers.standardizeArrays(source, config)
    val tPrepared = ComparatorHelpers.standardizeArrays(target, config)

    val missingRecords = sPrepared.join(tPrepared, config.primaryKeys, "left_anti")
    val extraRecords = tPrepared.join(sPrepared, config.primaryKeys, "left_anti")
    val joined = sPrepared.as("s").join(tPrepared.as("t"), config.primaryKeys, "inner")
    
    val payloadCols = sPrepared.columns.filterNot(c => config.primaryKeys.contains(c) || config.ignoreColumns.contains(c))
    
    if (payloadCols.isEmpty) {
      val emptyMismatches = spark.emptyDataFrame.withColumn("mismatched_columns", typedLit(Array.empty[String]))
        .withColumn("source_data", lit(null)).withColumn("target_data", lit(null))
      val emptyComplex = spark.emptyDataFrame.withColumn("pk_json", lit("")).withColumn("column_path", lit("")).withColumn("source_val", lit("")).withColumn("target_val", lit(""))
      return ComparatorResult(joined, missingRecords, extraRecords, emptyMismatches, emptyComplex)
    }

    val complexCols = payloadCols.filter(c => {
      val dt = sPrepared.schema(c).dataType
      dt.isInstanceOf[ArrayType] || dt.isInstanceOf[StructType] || dt.isInstanceOf[MapType]
    })

    val mismatchChecks = payloadCols.map(c => ComparatorHelpers.buildMismatchCheckExpr(c, sPrepared.schema(c).dataType, config))
    
    val comparedDf = joined
      .withColumn("mismatched_columns_raw", array(mismatchChecks: _*))
      .withColumn("mismatched_columns", expr("filter(mismatched_columns_raw, x -> x is not null)")) 
      .withColumn("source_data", struct(payloadCols.map(c => col(s"s.$c")): _*))
      .withColumn("target_data", struct(payloadCols.map(c => col(s"t.$c")): _*))
      .drop("mismatched_columns_raw")
      .select(config.primaryKeys.map(col) ++ Seq(col("mismatched_columns"), col("source_data"), col("target_data")): _*)

    val matchedRecords = comparedDf.filter(size(col("mismatched_columns")) === 0).drop("mismatched_columns", "target_data").select("source_data.*")
    val mismatchedRecords = comparedDf.filter(size(col("mismatched_columns")) > 0)

    val emptyComplex = spark.emptyDataFrame
      .withColumn("pk_json", lit("")).withColumn("column_path", lit("")).withColumn("source_val", lit("")).withColumn("target_val", lit(""))
    
    val complexMismatchesDf = if (complexCols.nonEmpty) {
      val complexRows = mismatchedRecords.filter(arrays_overlap(col("mismatched_columns"), typedLit(complexCols.toArray)))
      
      if (complexRows.isEmpty) emptyComplex else {
        val diffUdf = udf((sJson: String, tJson: String) => JsonDiffFlattener.diffComplexTypes(sJson, tJson, config.complexTypeKeys, config.numericTolerance))
        
        val sJsonExpr = to_json(struct(complexCols.map(c => col(s"source_data.$c")): _*))
        val tJsonExpr = to_json(struct(complexCols.map(c => col(s"target_data.$c")): _*))

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
    } else emptyComplex

    ComparatorResult(matchedRecords, missingRecords, extraRecords, mismatchedRecords, complexMismatchesDf)
  }

  def compareAndWrite(source: DataFrame, target: DataFrame, config: ComparatorConfig, sink: ComparatorSinkConfig): ComparatorSummary = {
    val result = compare(source, target, config)
    val spark = source.sparkSession

    def writeDelta(df: DataFrame, suffix: String): Long = {
      val path = s"${sink.basePath}/${sink.tablePrefix}_$suffix"
      if (df.isEmpty) return 0L
      df.withColumn("run_id", lit(sink.runId)).withColumn("run_date", lit(sink.runDate))
        .write.format("delta").mode(sink.saveMode).partitionBy(sink.partitionCols: _*).save(path)
      spark.read.format("delta").load(path).where(col("run_id") === sink.runId).count()
    }

    ComparatorSummary(
      runId = sink.runId,
      missingCount = writeDelta(result.missingRecords, "missing"),
      extraCount = writeDelta(result.extraRecords, "extra"),
      mismatchCount = writeDelta(result.mismatchedRecords, "mismatched"),
      complexMismatchCount = writeDelta(result.complexMismatches, "complex_mismatches")
    )
  }
}