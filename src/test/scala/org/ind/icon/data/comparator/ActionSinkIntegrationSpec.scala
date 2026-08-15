package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.{ComparatorConfig, ComparatorSinkConfig}
import java.util.UUID

class ActionSinkIntegrationSpec extends SparkTestBase {
  describe("SparkDataFrameComparator - Delta Lake Sinking via Unity Catalog Naming") {
    val comparator = new SparkDataFrameComparator()

    it("should execute compareAndWrite, save results as Delta Tables, and return DataFrames alongside KPIs") {
      import spark.implicits._
      
      val dfSource = Seq((1, "A"), (2, "B"), (3, "C")).toDF("id", "name")
      val dfTarget = Seq((1, "A"), (2, "MUTATED"), (4, "D")).toDF("id", "name")

      val config = ComparatorConfig(Seq("id"))
      
      // Use local spark_catalog for tests, simulating Unity Catalog's 3-part naming
      val sink = ComparatorSinkConfig(
        catalogAndSchema = "default",
        tablePrefix = "batch_compare",
        runId = UUID.randomUUID().toString,
        runDate = "2026-08-15"
      )

      val sinkResult = comparator.compareAndWrite(dfSource, dfTarget, config, sink)

      // Verify returned KPIs
      sinkResult.runId shouldBe sink.runId
      sinkResult.missingCount shouldBe 1
      sinkResult.extraCount shouldBe 1
      sinkResult.mismatchCount shouldBe 1
      sinkResult.complexMismatchCount shouldBe 0
      sinkResult.isPerfectMatch shouldBe false

      // Verify the DataFrames are seamlessly attached and accessible
      sinkResult.comparatorResult.mismatchedRecords.columns should contain("mismatched_columns")

      // Verify physical Delta Lake writes via table name (Not path!)
      val tableName = s"${sink.catalogAndSchema}.${sink.tablePrefix}_missing"
      val missingDelta = spark.read.table(tableName)
      
      missingDelta.count() shouldBe 1
      missingDelta.select("run_id").as[String].collect().head shouldBe sink.runId
    }
  }
}