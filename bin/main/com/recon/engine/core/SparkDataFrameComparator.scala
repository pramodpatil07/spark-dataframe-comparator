package com.recon.engine.core

import com.recon.engine.model._
import com.recon.engine.transform.JsonDiffFlattener
import com.recon.engine.utils.ReconHelpers
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * The core orchestrator for massive-scale DataFrame Reconciliation.
 * Executes the "Two-Tiered Evaluation" pattern to prevent OOMs on 100M+ row tables.
 */
class SparkDataFrameComparator extends DataFrameComparator with Serializable {

  /** Compares two DataFrames using their business keys.
    *
    * The method is lazy. Trigger an action on one of the returned DataFrames to execute it.
    */
  override def compare(source: DataFrame, target: DataFrame, config: ReconConfig): ReconResult = {
    validateInputs(source, target, config)
    val spark = source.sparkSession

    // 1. Array Standardization (Prevent False Positives)
    val sPrepared = ReconHelpers.standardizeArrays(source, config)
    val tPrepared = ReconHelpers.standardizeArrays(target, config)

    // 2. Missing/Extra Record Identification (Anti-Joins)
    val missingRecords = sPrepared.join(tPrepared, config.primaryKeys, "left_anti")
    val extraRecords = tPrepared.join(sPrepared, config.primaryKeys, "left_anti")

    // 3. Matched/Mismatched Record Identification (Inner-Join)
    val joined = sPrepared.as("s").join(tPrepared.as("t"), config.primaryKeys, "inner")
    
    val payloadCols = sPrepared.columns.filterNot(c => config.primaryKeys.contains(c) || config.ignoreColumns.contains(c))
    
    // Edge Case: If no payload columns exist (e.g. bridging tables with only PKs)
    if (payloadCols.isEmpty) {
      val emptyMismatches = spark.emptyDataFrame.withColumn("mismatched_columns", typedLit(Array.empty[String]))
        .withColumn("source_data", lit(null)).withColumn("target_data", lit(null))
      val emptyComplex = spark.emptyDataFrame.withColumn("pk_json", lit("")).withColumn("column_path", lit("")).withColumn("source_val", lit("")).withColumn("target_val", lit(""))
      return ReconResult(joined, missingRecords, extraRecords, emptyMismatches, emptyComplex)
    }

    val complexCols = payloadCols.filter(c => {
      val dt = sPrepared.schema(c).dataType
      dt.isInstanceOf[ArrayType] || dt.isInstanceOf[StructType] || dt.isInstanceOf[MapType]
    })

    // 4. Outer-Level Diffing Array Generation
    // Evaluates every column. Returns an array containing ONLY the names of columns that mismatched.
    val mismatchChecks = payloadCols.map(c => ReconHelpers.buildMismatchCheckExpr(c, sPrepared.schema(c).dataType, config))
    
    val comparedDf = joined
      .withColumn("mismatched_columns_raw", array(mismatchChecks: _*))
      // Use SQL lambda to remove nulls safely
      .withColumn("mismatched_columns", expr("filter(mismatched_columns_raw, x -> x is not null)")) 
      .withColumn("source_data", struct(payloadCols.map(c => col(s"s.$c")): _*))
      .withColumn("target_data", struct(payloadCols.map(c => col(s"t.$c")): _*))
      .drop("mismatched_columns_raw")
      .select(config.primaryKeys.map(col) ++ Seq(col("mismatched_columns"), col("source_data"), col("target_data")): _*)

    val matchedRecords = comparedDf.filter(size(col("mismatched_columns")) === 0).drop("mismatched_columns", "target_data").select("source_data.*")
    val mismatchedRecords = comparedDf.filter(size(col("mismatched_columns")) > 0)

    val emptyComplex = spark.emptyDataFrame
      .withColumn("pk_json", lit("")).withColumn("column_path", lit("")).withColumn("source_val", lit("")).withColumn("target_val", lit(""))
    
    // 5. Tier-2 Complex Diffing Delegation
    // ONLY executed if complex columns exist AND actually failed the outer-level match
    val complexMismatchesDf = if (complexCols.nonEmpty) {
      // Find rows where the failed columns intersect with known complex columns
      val complexRows = mismatchedRecords.filter(arrays_overlap(col("mismatched_columns"), typedLit(complexCols.toArray)))
      
      {
        val tolerance = if (config.enableNumericTolerance) config.numericTolerance else 0.0
        val diffUdf = udf((sJson: String, tJson: String) => JsonDiffFlattener.diffComplexTypes(sJson, tJson, config.complexTypeKeys, tolerance))
        
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

    ReconResult(matchedRecords, missingRecords, extraRecords, mismatchedRecords, complexMismatchesDf)
  }

  /** Compares two DataFrames and appends each non-empty output to its Delta destination. */
  override def compareAndWrite(source: DataFrame, target: DataFrame, config: ReconConfig, sink: ReconSinkConfig): ReconSummary = {
    validateSink(sink)
    val result = compare(source, target, config)
    val spark = source.sparkSession

    // Private helper to isolate Delta I/O Logic
    def writeDelta(df: DataFrame, suffix: String): Long = {
      val path = s"${sink.basePath}/${sink.tablePrefix}_$suffix"
      val output = df.withColumn("run_id", lit(sink.runId)).withColumn("run_date", lit(sink.runDate)).cache()
      try {
        val count = output.count()
        if (count > 0L) {
          output.write.format("delta").mode(sink.saveMode).partitionBy(sink.partitionCols: _*).save(path)
        }
        count
      } finally {
        output.unpersist(blocking = false)
      }
    }

    ReconSummary(
      runId = sink.runId,
      missingCount = writeDelta(result.missingRecords, "missing"),
      extraCount = writeDelta(result.extraRecords, "extra"),
      mismatchCount = writeDelta(result.mismatchedRecords, "mismatched"),
      complexMismatchCount = writeDelta(result.complexMismatches, "complex_mismatches")
    )
  }

  private def validateInputs(source: DataFrame, target: DataFrame, config: ReconConfig): Unit = {
    require(config.primaryKeys.nonEmpty, "ReconConfig must specify at least one primary key column.")
    require(config.primaryKeys.distinct.size == config.primaryKeys.size, "ReconConfig primary key columns must be unique.")
    require(config.primaryKeys.forall(_.trim.nonEmpty), "ReconConfig primary key columns must not be blank.")
    require(config.ignoreColumns.distinct.size == config.ignoreColumns.size, "ReconConfig ignoreColumns must not contain duplicates.")
    require(config.numericTolerance >= 0.0 && !config.numericTolerance.isNaN,
      "ReconConfig numericTolerance must be a non-negative number.")

    val sourceColumns = source.schema.fieldNames.toSet
    val targetColumns = target.schema.fieldNames.toSet
    require(config.primaryKeys.forall(sourceColumns.contains), "Every primary key must exist in the source DataFrame.")
    require(config.primaryKeys.forall(targetColumns.contains), "Every primary key must exist in the target DataFrame.")
    require(config.ignoreColumns.forall(sourceColumns.contains) && config.ignoreColumns.forall(targetColumns.contains),
      "Every ignored column must exist in both DataFrames.")
    require(sourceColumns == targetColumns,
      s"Source and target must have the same column names. Source-only: ${(sourceColumns -- targetColumns).toSeq.sorted.mkString(", ")}; target-only: ${(targetColumns -- sourceColumns).toSeq.sorted.mkString(", ")}")

    source.schema.fields.foreach { sourceField =>
      val targetField = target.schema(sourceField.name)
      require(sourceField.dataType == targetField.dataType,
        s"Column '${sourceField.name}' has incompatible types: source=${sourceField.dataType.catalogString}, target=${targetField.dataType.catalogString}.")
    }

    config.complexTypeKeys.foreach { case (columnName, keys) =>
      require(keys.nonEmpty, s"complexTypeKeys.$columnName must contain at least one field name.")
      val field = source.schema.find(_.name == columnName).getOrElse(
        throw new IllegalArgumentException(s"complexTypeKeys.$columnName does not exist in the input DataFrames.")
      )
      field.dataType match {
        case ArrayType(struct: StructType, _) =>
          require(struct.fieldNames.contains(keys.head),
            s"complexTypeKeys.$columnName references missing struct field '${keys.head}'.")
        case _ =>
          throw new IllegalArgumentException(s"complexTypeKeys.$columnName requires an array of structs.")
      }
    }
  }

  private def validateSink(sink: ReconSinkConfig): Unit = {
    require(sink.basePath.trim.nonEmpty, "ReconSinkConfig basePath must not be blank.")
    require(sink.tablePrefix.matches("[A-Za-z0-9][A-Za-z0-9_-]*"),
      "ReconSinkConfig tablePrefix must contain only letters, digits, underscores, or hyphens.")
    require(sink.runId.trim.nonEmpty && sink.runDate.trim.nonEmpty, "ReconSinkConfig runId and runDate must not be blank.")
    require(sink.partitionCols.nonEmpty && sink.partitionCols.forall(col => col == "run_id" || col == "run_date"),
      "ReconSinkConfig partitionCols may contain only run_id and run_date.")
    require(Set("append", "overwrite", "error", "errorifexists", "ignore").contains(sink.saveMode.toLowerCase),
      "ReconSinkConfig saveMode must be append, overwrite, error, errorifexists, or ignore.")
  }
}
