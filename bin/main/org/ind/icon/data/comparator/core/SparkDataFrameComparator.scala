package org.ind.icon.data.comparator.core

import org.ind.icon.data.comparator.model._
import org.ind.icon.data.comparator.transform.JsonDiffFlattener
import org.ind.icon.data.comparator.utils.ComparatorHelpers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

/**
 * The core orchestrator for massive-scale DataFrame Comparison.
 * Executes the "Two-Tiered Evaluation" pattern to prevent Spark memory exhaustion on 100M+ row tables.
 */
class SparkDataFrameComparator extends DataFrameComparator with Serializable {

  def compare(source: DataFrame, target: DataFrame, config: ComparatorConfig): ComparatorResult = {
    require(config.primaryKeys.nonEmpty, "ComparatorConfig must specify at least one primaryKey column.")
    
    // ==========================================
    // STEP 1: Schema & Uniqueness Validation
    // ==========================================
    val missingSourcePKs = config.primaryKeys.filterNot(source.columns.contains)
    require(missingSourcePKs.isEmpty, s"Source is missing PKs: ${missingSourcePKs.mkString(",")}")
    
    val missingTargetPKs = config.primaryKeys.filterNot(target.columns.contains)
    require(missingTargetPKs.isEmpty, s"Target is missing PKs: ${missingTargetPKs.mkString(",")}")

    if (config.validateUniqueness) {
      // Prevents exponential Cartesian explosions by ensuring PKs strictly identify one row.
      require(source.groupBy(config.primaryKeys.map(col): _*).count().filter(col("count") > 1).isEmpty, "Source has duplicate PKs.")
      require(target.groupBy(config.primaryKeys.map(col): _*).count().filter(col("count") > 1).isEmpty, "Target has duplicate PKs.")
    }

    val spark = source.sparkSession

    // ==========================================
    // STEP 2: Array Standardization
    // ==========================================
    val sPrepared = ComparatorHelpers.standardizeArrays(source, config)
    val tPrepared = ComparatorHelpers.standardizeArrays(target, config)

    // ==========================================
    // STEP 3: Identification of Missing/Extra (Anti-Joins)
    // ==========================================
    val missingRecords = sPrepared.join(tPrepared, config.primaryKeys, "left_anti")
    val extraRecords = tPrepared.join(sPrepared, config.primaryKeys, "left_anti")
    
    // Proceed to compare only the rows that exist in both datasets
    val joined = sPrepared.as("s").join(tPrepared.as("t"), config.primaryKeys, "inner")
    
    // Dynamically intersect schemas to prevent resolution crashes when schemas drift
    val commonCols = sPrepared.columns.intersect(tPrepared.columns)
    val payloadCols = commonCols.filterNot(c => config.primaryKeys.contains(c) || config.ignoreColumns.contains(c))
    
    // Edge Case: Bridging tables that only contain primary keys
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

    // ==========================================
    // STEP 4: Tier-1 Mismatch Detection (Wide Schema)
    // Evaluates every column using Spark SQL. Builds an array of column names that failed the check.
    // ==========================================
    val mismatchChecks = payloadCols.map(c => ComparatorHelpers.buildMismatchCheckExpr(c, sPrepared.schema(c).dataType, config))
    
    val comparedDf = joined
      .withColumn("mismatched_columns_raw", array(mismatchChecks: _*))
      // Use SQL filter lambda to safely strip nulls. DO NOT USE array_remove(..., null) as it corrupts the array.
      .withColumn("mismatched_columns", expr("filter(mismatched_columns_raw, x -> x is not null)")) 
      .withColumn("source_data", struct(payloadCols.map(c => col(s"s.$c")): _*))
      .withColumn("target_data", struct(payloadCols.map(c => col(s"t.$c")): _*))
      .drop("mismatched_columns_raw")
      .select(config.primaryKeys.map(col) ++ Seq(col("mismatched_columns"), col("source_data"), col("target_data")): _*)

    val matchedRecords = comparedDf.filter(size(col("mismatched_columns")) === 0).drop("mismatched_columns", "target_data").select("source_data.*")
    val mismatchedRecords = comparedDf.filter(size(col("mismatched_columns")) > 0)

    val emptyComplex = spark.emptyDataFrame
      .withColumn("pk_json", lit("")).withColumn("column_path", lit("")).withColumn("source_val", lit("")).withColumn("target_val", lit(""))
    
    // ==========================================
    // STEP 5: Tier-2 Complex Diffing (UDF Fallback)
    // ONLY executed if complex columns exist AND actually failed the outer-level match.
    // ==========================================
    val complexMismatchesDf = if (complexCols.nonEmpty) {
      val complexRows = mismatchedRecords.filter(arrays_overlap(col("mismatched_columns"), typedLit(complexCols.toArray)))
      
      if (complexRows.isEmpty) emptyComplex else {
        val diffUdf = udf((sJson: String, tJson: String) => 
          JsonDiffFlattener.diffComplexTypes(sJson, tJson, config.complexTypeKeys, config.enableNumericTolerance, config.numericTolerance)
        )
        
        // Serialize ONLY the complex columns into JSON to pass to the UDF
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

  def compareAndWrite(source: DataFrame, target: DataFrame, config: ComparatorConfig, sink: ComparatorSinkConfig): ComparatorSinkResult = {
    val result = compare(source, target, config)
    val spark = source.sparkSession

    def writeDelta(df: DataFrame, suffix: String): Long = {
      // Unity Catalog table name resolution (e.g. catalog.schema.table_prefix_missing)
      val tableName = s"${sink.catalogAndSchema}.${sink.tablePrefix}_$suffix"
      
      // Blindly write the dataframe. Delta natively materializes empty directories/schemas 
      // safely without triggering OutOfMemory exceptions on empty DAGs.
      df.withColumn("run_id", lit(sink.runId)).withColumn("run_date", lit(sink.runDate))
        .write.format("delta").mode(sink.saveMode).partitionBy(sink.partitionCols: _*).saveAsTable(tableName)
      
      // PERFORMANCE OPTIMIZATION: O(1) Instant metric extraction straight from Delta's transaction log.
      // This retrieves the exact number of output rows written by THIS runId.
      val history = DeltaTable.forName(spark, tableName).history(1)
      val metrics = history.select(expr("element_at(operationMetrics, 'numOutputRows')")).first()
      val rowsStr = metrics.getAs[String](0)
      if (rowsStr != null) rowsStr.toLong else 0L
    }

    ComparatorSinkResult(
      runId = sink.runId,
      missingCount = writeDelta(result.missingRecords, "missing"),
      extraCount = writeDelta(result.extraRecords, "extra"),
      mismatchCount = writeDelta(result.mismatchedRecords, "mismatched"),
      complexMismatchCount = writeDelta(result.complexMismatches, "complex_mismatches"),
      comparatorResult = result
    )
  }
}