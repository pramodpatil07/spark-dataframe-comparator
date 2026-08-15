package org.ind.icon.data.comparator.model

import org.apache.spark.sql.DataFrame

/**
 * Configuration for the DataFrame comparison engine.
 *
 * @param primaryKeys            The unique identifiers for a row. Used for joining Source and Target.
 * @param ignoreColumns          Columns to exclude from comparison (e.g., ETL timestamps, audit flags).
 * @param complexTypeKeys        A mapping of Array column names to their internal logical keys (e.g., Map("history" -> Seq("version_id"))).
 *                               This allows the engine to align and compare array elements regardless of their physical order.
 * @param enableNumericTolerance If true, numeric drift is evaluated using `numericTolerance`.
 * @param numericTolerance       The absolute threshold allowed for floating-point/decimal drift.
 * @param standardizeArrays      If true, natively sorts arrays using Spark Catalyst before comparison to prevent false positives.
 * @param validateUniqueness     If true, asserts that primary keys are strictly unique to prevent massive Cartesian explosions.
 */
case class ComparatorConfig(
  primaryKeys: Seq[String],
  ignoreColumns: Seq[String] = Seq.empty,
  complexTypeKeys: Map[String, Seq[String]] = Map.empty,
  enableNumericTolerance: Boolean = false,
  numericTolerance: Double = 0.0,
  standardizeArrays: Boolean = true,
  validateUniqueness: Boolean = false
)

/**
 * Configuration for sinking execution results into Delta Lake.
 *
 * @param basePath      The root storage path (e.g., ADLS abfss://... or local file path).
 * @param tablePrefix   The prefix appended to table names (e.g., "api_recon" creates "api_recon_missing").
 * @param runId         A unique UUID for this execution batch.
 * @param runDate       The logical execution date, used for Delta table partitioning.
 * @param partitionCols Columns to partition the Delta table by (defaults to run_date).
 * @param saveMode      Spark write mode (e.g., "append" or "overwrite").
 */
case class ComparatorSinkConfig(
  basePath: String,                   
  tablePrefix: String,                
  runId: String,                      
  runDate: String,                    
  partitionCols: Seq[String] = Seq("run_date"), 
  saveMode: String = "append"
)

/**
 * The Lazy Spark execution graph artifact containing all comparison segments.
 * No actions (.count, .write) are triggered when this is returned.
 */
case class ComparatorResult(
  matchedRecords: DataFrame,
  missingRecords: DataFrame,
  extraRecords: DataFrame,
  mismatchedRecords: DataFrame,
  complexMismatches: DataFrame
)

/**
 * The Action artifact returned after sinking data to Delta Lake.
 * Contains instantaneous KPIs retrieved directly from Delta Transaction logs.
 */
case class ComparatorSummary(
  runId: String,
  missingCount: Long,
  extraCount: Long,
  mismatchCount: Long,
  complexMismatchCount: Long
) {
  /** Returns true if both DataFrames are perfectly identical based on the provided configuration. */
  def isPerfectMatch: Boolean = missingCount == 0 && extraCount == 0 && mismatchCount == 0
}