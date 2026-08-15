package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class EdgeCaseComparatorSpec extends SparkTestBase {

  describe("SparkDataFrameComparator - Exhaustive Edge Cases") {
    val comparator = new SparkDataFrameComparator()

    it("should safely bypass array_sort and native <=> comparisons on MapType arrays and correctly detect mismatches") {
      import spark.implicits._
      // Maps are strictly non-orderable and do not support Spark's <=> operator.
      val sourceDf = Seq((1, Seq(Map("k1" -> "v1")))).toDF("id", "map_array")
      val targetDf = Seq((1, Seq(Map("k1" -> "MUTATED")))).toDF("id", "map_array")

      val result = comparator.compare(sourceDf, targetDf, ComparatorConfig(Seq("id"), standardizeArrays = true))
      result.mismatchedRecords.count() shouldBe 1
    }

    it("should retain native DecimalType precision and successfully evaluate tolerances") {
      import spark.implicits._
      val schema = StructType(Seq(StructField("id", IntegerType), StructField("amount", DecimalType(38, 18))))
      val sRows = Seq(org.apache.spark.sql.Row(1, BigDecimal("123456789.123456789123456789")))
      val tRows = Seq(org.apache.spark.sql.Row(1, BigDecimal("123456789.123456789123456790")))
      
      val sDf = spark.createDataFrame(spark.sparkContext.parallelize(sRows), schema)
      val tDf = spark.createDataFrame(spark.sparkContext.parallelize(tRows), schema)

      val config = ComparatorConfig(Seq("id"), enableNumericTolerance = true, numericTolerance = 0.01)
      val result = comparator.compare(sDf, tDf, config)
      
      result.mismatchedRecords.count() shouldBe 0
    }

    it("should properly identify explicit null changes inside JSON hierarchies") {
      import spark.implicits._
      // Map("k1" -> null) simulates an explicitly declared null value. Map.empty simulates a completely missing key.
      val sDf = Seq((1, Map("k1" -> null.asInstanceOf[String]))).toDF("id", "map_col")
      val tDf = Seq((1, Map.empty[String, String])).toDF("id", "map_col")
      
      val result = comparator.compare(sDf, tDf, ComparatorConfig(Seq("id")))
      
      // Mismatch should be detected by the fallback UDF perfectly
      result.mismatchedRecords.count() shouldBe 1
      val exceptions = result.complexMismatches.collect()
      
      exceptions.length shouldBe 1
      exceptions.head.getAs[String]("column_path") shouldBe "map_col.k1"
      exceptions.head.getAs[String]("source_val") shouldBe null
      exceptions.head.getAs[String]("target_val") shouldBe null
    }

    it("should block Cartesian products when validateUniqueness is enabled") {
      import spark.implicits._
      val df = Seq((1, "A"), (1, "B")).toDF("id", "val")
      
      assertThrows[IllegalArgumentException] {
        comparator.compare(df, df, ComparatorConfig(Seq("id"), validateUniqueness = true))
      }
    }

    it("should gracefully handle key-only DataFrames (Bridging Tables)") {
      import spark.implicits._
      val dfSource = Seq((1, 2), (3, 4)).toDF("pk1", "pk2")
      val dfTarget = Seq((1, 2), (5, 6)).toDF("pk1", "pk2")
      val result = comparator.compare(dfSource, dfTarget, ComparatorConfig(Seq("pk1", "pk2")))
      
      result.matchedRecords.count() shouldBe 1
      result.missingRecords.count() shouldBe 1
      result.extraRecords.count() shouldBe 1
      result.mismatchedRecords.count() shouldBe 0
    }
  }
}