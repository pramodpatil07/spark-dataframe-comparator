package org.ind.icon.data.comparator.core

import org.ind.icon.data.comparator.model._
import org.apache.spark.sql.DataFrame

/** Base trait for DataFrame Reconciliation. */
trait DataFrameComparator {
  /** Compares two DataFrames lazily. Returns unresolved Catalyst plans. */
  def compare(source: DataFrame, target: DataFrame, config: ComparatorConfig): ComparatorResult
  
  /** Compares two DataFrames, writes them to Delta Lake, and returns instantaneous metrics. */
  def compareAndWrite(source: DataFrame, target: DataFrame, config: ComparatorConfig, sink: ComparatorSinkConfig): ComparatorSinkResult
}