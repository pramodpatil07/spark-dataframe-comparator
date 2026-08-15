package org.ind.icon.data.comparator.model

import org.apache.spark.sql.DataFrame

case class ComparatorConfig(
  primaryKeys: Seq[String],
  ignoreColumns: Seq[String] = Seq.empty,
  complexTypeKeys: Map[String, Seq[String]] = Map.empty,
  enableNumericTolerance: Boolean = false,
  numericTolerance: Double = 0.0,
  standardizeArrays: Boolean = true
)

case class ComparatorSinkConfig(
  basePath: String,                   
  tablePrefix: String,                
  runId: String,                      
  runDate: String,                    
  partitionCols: Seq[String] = Seq("run_date"), 
  saveMode: String = "append"
)

case class ComparatorResult(
  matchedRecords: DataFrame,
  missingRecords: DataFrame,
  extraRecords: DataFrame,
  mismatchedRecords: DataFrame,
  complexMismatches: DataFrame
)

case class ComparatorSummary(
  runId: String,
  missingCount: Long,
  extraCount: Long,
  mismatchCount: Long,
  complexMismatchCount: Long
) {
  def isPerfectMatch: Boolean = missingCount == 0 && extraCount == 0 && mismatchCount == 0
}