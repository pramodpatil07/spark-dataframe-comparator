package com.recon.engine.core

import com.recon.engine.model._
import org.apache.spark.sql.DataFrame

trait DataFrameComparator {
  def compare(source: DataFrame, target: DataFrame, config: ReconConfig): ReconResult
  
  def compareAndWrite(source: DataFrame, target: DataFrame, config: ReconConfig, sink: ReconSinkConfig): ReconSummary
}