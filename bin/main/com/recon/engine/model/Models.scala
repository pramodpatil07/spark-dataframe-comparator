package com.recon.engine.model

import org.apache.spark.sql.DataFrame

/** Configuration for a DataFrame comparison.
  *
  * `primaryKeys` must identify exactly one row in each input DataFrame. Inputs must have
  * the same column names and matching data types for compared columns.
  */
case class ReconConfig(
  primaryKeys: Seq[String],
  ignoreColumns: Seq[String] = Seq.empty,
  complexTypeKeys: Map[String, Seq[String]] = Map.empty,
  enableNumericTolerance: Boolean = false,
  numericTolerance: Double = 0.0,
  standardizeArrays: Boolean = true
)

/** Configuration for persisting non-empty reconciliation outputs as Delta tables. */
case class ReconSinkConfig(
  basePath: String,                   // Target ADLS or local path (e.g. "abfss://container@storage/recon/")
  tablePrefix: String,                // e.g., "api_recon" -> creates api_recon_missing, api_recon_mismatched, etc.
  runId: String,                      // Unique ID for the batch (UUID)
  runDate: String,                    // The logical date of the recon (for partitioning)
  partitionCols: Seq[String] = Seq("run_date"), 
  saveMode: String = "append"
)

/** Lazy DataFrame outputs from a reconciliation. No Spark action is run by `compare`. */
case class ReconResult(
  matchedRecords: DataFrame,
  missingRecords: DataFrame,
  extraRecords: DataFrame,
  mismatchedRecords: DataFrame,
  complexMismatches: DataFrame
)

/** Counts for rows written by one `compareAndWrite` invocation. */
case class ReconSummary(
  runId: String,
  missingCount: Long,
  extraCount: Long,
  mismatchCount: Long,
  complexMismatchCount: Long
) {
  def isPerfectMatch: Boolean = missingCount == 0 && extraCount == 0 && mismatchCount == 0
}
