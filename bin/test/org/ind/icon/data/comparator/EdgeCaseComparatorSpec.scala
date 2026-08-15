package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class EdgeCaseComparatorSpec extends SparkTestBase {

  describe("SparkDataFrameComparator - Exhaustive Edge Cases") {
    val comparator = new SparkDataFrameComparator()

    it("should safely bypass array_sort on MapType arrays and correctly detect mismatches") {
      import spark.implicits._
      // Maps are strictly non-orderable. Attempting to sort them crashes Spark.
      val sourceDf = Seq((1, Seq(Map("k1" -> "v1")))).toDF("id", "map_array")
      val targetDf = Seq((1, Seq(Map("k1" -> "MUTATED")))).toDF("id", "map_array")

      val result = comparator.compare(sourceDf, targetDf, ComparatorConfig(Seq("id"), standardizeArrays = true))
      result.mismatchedRecords.count() shouldBe 1
    }

    it("should retain native DecimalType precision and successfully evaluate tolerances") {
      import spark.implicits._
      // Decimals converted to Double suffer floating-point drift. Math must stay native.
      val schema = StructType(Seq(StructField("id", IntegerType), StructField("amount", DecimalType(38, 18))))
      val sRows = Seq(org.apache.spark.sql.Row(1, BigDecimal("123456789.123456789123456789")))
      val tRows = Seq(org.apache.spark.sql.Row(1, BigDecimal("123456789.123456789123456790"))) // Diff is 0.000000000000000001
      
      val sDf = spark.createDataFrame(spark.sparkContext.parallelize(sRows), schema)
      val tDf = spark.createDataFrame(spark.sparkContext.parallelize(tRows), schema)

      val config = ComparatorConfig(Seq("id"), enableNumericTolerance = true, numericTolerance = 0.01)
      val result = comparator.compare(sDf, tDf, config)
      
      // Because tolerance is 0.01 and diff is minuscule, it should be a perfect match
      result.mismatchedRecords.count() shouldBe 0
    }

    it("should properly identify explicit null changes inside JSON hierarchies") {
      import spark.implicits._
      // Source explicitly sends {"val": null}, Target completely omits the key {}
      val sDf = Seq((1, """{"val": null}""")).toDF("id", "payload")
      val tDf = Seq((1, """{}""")).toDF("id", "payload")
      
      val schema = StructType(Seq(StructField("val", StringType)))
      val sParsed = sDf.withColumn("struct", from_json($"payload", schema)).drop("payload")
      val tParsed = tDf.withColumn("struct", from_json($"payload", schema)).drop("payload")

      val result = comparator.compare(sParsed, tParsed, ComparatorConfig(Seq("id")))
      
      result.mismatchedRecords.count() shouldBe 1
      val exceptions = result.complexMismatches.collect()
      
      exceptions.length shouldBe 1
      exceptions.head.getAs[String]("column_path") shouldBe "struct.val"
      exceptions.head.getAs[String]("source_val") shouldBe null
      exceptions.head.getAs[String]("target_val") shouldBe null // Map getOrElse falls back to null, proving they diffed via Option
    }

    it("should block Cartesian products when validateUniqueness is enabled") {
      import spark.implicits._
      val df = Seq((1, "A"), (1, "B")).toDF("id", "val")
      
      assertThrows[IllegalArgumentException] {
        comparator.compare(df, df, ComparatorConfig(Seq("id"), validateUniqueness = true))
      }
    }
  }
}