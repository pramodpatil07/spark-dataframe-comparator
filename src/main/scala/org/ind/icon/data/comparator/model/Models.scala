package org.ind.icon.data.comparator.model

import org.apache.spark.sql.DataFrame

/**
 * Configuration for the Spark DataFrame Comparison engine.
 * 
 * 
 * @param primaryKeys            The unique identifiers for a row. Used for joining Source and Target datasets.
 * @param ignoreColumns          Columns to exclude from comparison (e.g., ETL timestamps, auto-generated IDs).
 * @param complexTypeKeys        A mapping of Array column names to their internal logical keys (e.g., Map("history" -> Seq("version_id"))).
 *                               This allows the engine to align and compare array elements regardless of their physical order.
 * @param enableNumericTolerance If true, numeric drift is evaluated using the `numericTolerance` parameter.
 * @param numericTolerance       The absolute threshold allowed for floating-point/decimal drift.
 * @param standardizeArrays      If true, natively sorts arrays using Spark Catalyst before comparison to prevent false positives.
 * @param validateUniqueness     If true, asserts that primary keys are strictly unique to prevent massive Cartesian explosions.
 */
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
 * Configuration for sinking execution results into Unity Catalog / Delta Lake via table names.
 *
 * @param catalogAndSchema The three-part target namespace excluding the table name (e.g., "prod_catalog.staging_schema").
 * @param tablePrefix      The prefix appended to the generated tables (e.g., "api_data" creates "api_data_missing").
 * @param runId            A unique UUID for this execution batch, attached to every written row.
 * @param runDate          The logical execution date, used natively for Delta table partitioning.
 * @param partitionCols    Columns to partition the Delta table by (defaults to run_date).
 * @param saveMode         Spark write mode (e.g., "append" or "overwrite").
 * @param writeMatchedRecords If true, writes the perfectly matched rows to Delta. Default is false to save massive I/O.
 */
case class ComparatorSinkConfig(
  catalogAndSchema: String,                   
  tablePrefix: String,                
  runId: String,                      
  runDate: String,                    
  partitionCols: Seq[String] = Seq("run_date"), 
  saveMode: String = "append",
  writeMatchedRecords: Boolean = false
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
 * Contains instantaneous KPIs retrieved directly from Delta Transaction logs, 
 * as well as the original ComparatorResult DataFrames for immediate downstream chaining.
 */
case class ComparatorSinkResult(
  runId: String,
  missingCount: Long,
  extraCount: Long,
  mismatchCount: Long,
  complexMismatchCount: Long,
  comparatorResult: ComparatorResult // Fully materialized DataFrames read from Delta!
) {
  def isPerfectMatch: Boolean = missingCount == 0 && extraCount == 0 && mismatchCount == 0
}