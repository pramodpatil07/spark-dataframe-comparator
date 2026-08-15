package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.{ComparatorConfig, ComparatorSinkConfig}
import java.util.UUID

class ActionSinkIntegrationSpec extends SparkTestBase {
  describe("SparkDataFrameComparator - Delta Lake Sinking") {
    val comparator = new SparkDataFrameComparator()

    it("should execute compareAndWrite, save results to Delta, and return accurate KPIs") {
      import spark.implicits._
      
      val dfSource = Seq((1, "A"), (2, "B"), (3, "C")).toDF("id", "name")
      val dfTarget = Seq((1, "A"), (2, "MUTATED"), (4, "D")).toDF("id", "name")

      val config = ComparatorConfig(Seq("id"))
      val sink = ComparatorSinkConfig(
        basePath = "build/test-results-delta",
        tablePrefix = "batch_compare",
        runId = UUID.randomUUID().toString,
        runDate = "2026-08-15"
      )

      val summary = comparator.compareAndWrite(dfSource, dfTarget, config, sink)

      summary.runId shouldBe sink.runId
      summary.missingCount shouldBe 1
      summary.extraCount shouldBe 1
      summary.mismatchCount shouldBe 1
      summary.complexMismatchCount shouldBe 0
      summary.isPerfectMatch shouldBe false

      val missingDelta = spark.read.format("delta").load(s"${sink.basePath}/${sink.tablePrefix}_missing")
      missingDelta.count() shouldBe 1
      missingDelta.select("run_id").as[String].collect().head shouldBe sink.runId
    }
  }
}