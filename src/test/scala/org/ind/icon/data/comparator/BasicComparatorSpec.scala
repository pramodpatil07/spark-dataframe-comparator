package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig

class BasicComparatorSpec extends SparkTestBase {
  describe("SparkDataFrameComparator - Outer Level Validation") {
    val comparator = new SparkDataFrameComparator()

    it("should accurately isolate matched, missing, extra, and mismatched records") {
      import spark.implicits._
      val dfSource = Seq((1, "A", 100), (2, "B", 200), (3, "C", 300)).toDF("id", "name", "val")
      val dfTarget = Seq((1, "A", 100), (2, "B", 999), (4, "D", 400)).toDF("id", "name", "val")

      val result = comparator.compare(dfSource, dfTarget, ComparatorConfig(Seq("id")))
      result.matchedRecords.count() shouldBe 1
      result.missingRecords.count() shouldBe 1 
      result.extraRecords.count() shouldBe 1   
      result.mismatchedRecords.count() shouldBe 1 
      
      val mismatchedRow = result.mismatchedRecords.collect().head
      mismatchedRow.getAs[Seq[String]]("mismatched_columns") should contain("val")
    }

    it("should eliminate false positives for out-of-order arrays when standardizeArrays = true") {
      import spark.implicits._
      val dfSource = Seq((1, Seq("A", "B", "C"))).toDF("id", "tags")
      val dfTarget = Seq((1, Seq("C", "A", "B"))).toDF("id", "tags")

      val result = comparator.compare(dfSource, dfTarget, ComparatorConfig(Seq("id"), standardizeArrays = true))
      result.mismatchedRecords.count() shouldBe 0
      result.matchedRecords.count() shouldBe 1
    }
  }
}