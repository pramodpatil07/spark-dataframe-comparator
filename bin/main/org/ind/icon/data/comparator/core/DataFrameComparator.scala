package org.ind.icon.data.comparator.core

import org.ind.icon.data.comparator.model._
import org.apache.spark.sql.DataFrame

trait DataFrameComparator {
  def compare(source: DataFrame, target: DataFrame, config: ComparatorConfig): ComparatorResult
  def compareAndWrite(source: DataFrame, target: DataFrame, config: ComparatorConfig, sink: ComparatorSinkConfig): ComparatorSinkResult
}