package org.ind.icon.data.comparator

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait SparkTestBase extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  @transient lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("SparkDataComparatorTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }
}