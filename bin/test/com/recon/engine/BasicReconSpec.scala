package com.recon.engine

import com.recon.engine.core.SparkDataFrameComparator
import com.recon.engine.model.ReconConfig

class BasicReconSpec extends SparkTestBase {
  describe("SparkDataFrameComparator - Outer Level Validation") {
    val comparator = new SparkDataFrameComparator()

    it("should accurately isolate matched, missing, extra, and mismatched records") {
      import spark.implicits._
      val dfSource = Seq((1, "A", 100), (2, "B", 200), (3, "C", 300)).toDF("id", "name", "val")
      // 1: Match, 2: Mismatch, 3: Missing, 4: Extra
      val dfTarget = Seq((1, "A", 100), (2, "B", 999), (4, "D", 400)).toDF("id", "name", "val")

      val result = comparator.compare(dfSource, dfTarget, ReconConfig(Seq("id")))
      
      result.matchedRecords.count() shouldBe 1
      result.missingRecords.count() shouldBe 1 // ID 3
      result.extraRecords.count() shouldBe 1   // ID 4
      result.mismatchedRecords.count() shouldBe 1 // ID 2
      
      // Verify wide schema
      val mismatchedRow = result.mismatchedRecords.collect().head
      mismatchedRow.getAs[Seq[String]]("mismatched_columns") should contain("val")
    }

    it("should eliminate false positives for out-of-order arrays when standardizeArrays = true") {
      import spark.implicits._
      val dfSource = Seq((1, Seq("A", "B", "C"))).toDF("id", "tags")
      val dfTarget = Seq((1, Seq("C", "A", "B"))).toDF("id", "tags") // Same elements, different order

      val result = comparator.compare(dfSource, dfTarget, ReconConfig(Seq("id"), standardizeArrays = true))
      result.mismatchedRecords.count() shouldBe 0
      result.matchedRecords.count() shouldBe 1
    }

    it("should reject incompatible schemas and invalid keyed-array configuration") {
      import spark.implicits._
      val source = Seq((1, "A")).toDF("id", "value")
      val target = Seq((1, 1)).toDF("id", "value")

      an[IllegalArgumentException] shouldBe thrownBy {
        comparator.compare(source, target, ReconConfig(Seq("id")))
      }

      an[IllegalArgumentException] shouldBe thrownBy {
        comparator.compare(source, source, ReconConfig(Seq("id"), complexTypeKeys = Map("value" -> Seq("key"))))
      }
    }

    it("should describe a complex field changed from a value to null") {
      import spark.implicits._
      val source = Seq((1, "present")).toDF("id", "value").selectExpr("id", "named_struct('field', value) as payload")
      val target = Seq((1, Option.empty[String])).toDF("id", "value").selectExpr("id", "named_struct('field', value) as payload")

      val result = comparator.compare(source, target, ReconConfig(Seq("id")))

      result.mismatchedRecords.count() shouldBe 1
      val detail = result.complexMismatches.collect().head
      detail.getAs[String]("column_path") shouldBe "payload.field"
      detail.getAs[String]("source_val") shouldBe "present"
      detail.isNullAt(detail.fieldIndex("target_val")) shouldBe true
    }
  }
}
