package org.ind.icon.data.comparator

import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.functions._

class DeepNestedStressSpec extends SparkTestBase {

  describe("SparkDataFrameComparator - Exhaustive Deep Nested Edge Cases") {
    val comparator = new SparkDataFrameComparator()

    it("should effortlessly navigate 4-level deep structs, arrays of objects, and arrays of arrays") {
      import spark.implicits._

      val exprs = Seq(
        "id as pk_id",
        "named_struct('flat_val', id) as struct_1_flat",
        "named_struct('l1', named_struct('l2_val', concat('S2_', id))) as struct_2_deep",
        "named_struct('l1', named_struct('l2', named_struct('l3_val', id))) as struct_3_deep",
        "named_struct('l1', named_struct('l2', named_struct('l3', named_struct('l4_val', id * 100)))) as struct_4_deep",
        "array(id, id+1, id+2) as arr_1_ints",
        "array(concat('A', id), concat('B', id)) as arr_2_strs",
        "array(cast(id*0.1 as double), cast(id*0.2 as double)) as arr_3_doubles",
        "array(array(id), array(id+1)) as arr_4_nested_arrs", 
        "array(named_struct('key', 1, 'v', 'A'), named_struct('key', 2, 'v', 'B')) as obj_arr_1_flat",
        "named_struct('wrapper', array(named_struct('key', 10, 'v', 'X'))) as obj_arr_2_in_struct",
        "array(named_struct('id', 1, 'nested', named_struct('prop', 'Y')), named_struct('id', 2, 'nested', named_struct('prop', 'Z'))) as obj_arr_3_deep",
        "array(named_struct('master_key', 100, 'list', array(named_struct('sub_key', 1, 'data', concat('D_', id))))) as obj_arr_4_insane"
      )

      val sourceDf = spark.range(1, 1001).selectExpr(exprs: _*)

      val targetDf = sourceDf
        .withColumn("struct_4_deep", when($"pk_id" === 100L, expr("named_struct('l1', named_struct('l2', named_struct('l3', named_struct('l4_val', 999999))))")).otherwise($"struct_4_deep"))
        .withColumn("arr_1_ints", when($"pk_id" === 200L, expr("array(pk_id, pk_id+1, 999)")).otherwise($"arr_1_ints"))
        .withColumn("obj_arr_3_deep", when($"pk_id" === 300L, expr("array(named_struct('id', 1, 'nested', named_struct('prop', 'Y')), named_struct('id', 2, 'nested', named_struct('prop', 'MUTATED_Z'))) ")).otherwise($"obj_arr_3_deep"))
        .withColumn("obj_arr_1_flat", when($"pk_id" === 400L, expr("array(named_struct('key', 2, 'v', 'B'), named_struct('key', 1, 'v', 'A'))")).otherwise($"obj_arr_1_flat"))

      val config = ComparatorConfig(
        primaryKeys = Seq("pk_id"),
        complexTypeKeys = Map(
          "obj_arr_1_flat" -> Seq("key"),
          "obj_arr_3_deep" -> Seq("id"),
          "obj_arr_4_insane" -> Seq("master_key")
        ),
        standardizeArrays = true
      )

      val result = comparator.compare(sourceDf, targetDf, config)

      result.matchedRecords.count() shouldBe 997 
      result.mismatchedRecords.count() shouldBe 3
      
      val exceptions = result.complexMismatches.collect()
      exceptions.length shouldBe 3

      val ex1 = exceptions.find(_.getAs[String]("pk_json").contains("\"pk_id\":100")).get
      ex1.getAs[String]("column_path") shouldBe "struct_4_deep.l1.l2.l3.l4_val"
      ex1.getAs[String]("target_val") shouldBe "999999"

      val ex2 = exceptions.find(_.getAs[String]("pk_json").contains("\"pk_id\":200")).get
      ex2.getAs[String]("column_path") shouldBe "arr_1_ints[2]"
      ex2.getAs[String]("target_val") shouldBe "999"

      val ex3 = exceptions.find(_.getAs[String]("pk_json").contains("\"pk_id\":300")).get
      ex3.getAs[String]("column_path") shouldBe "obj_arr_3_deep[id=2].nested.prop"
      ex3.getAs[String]("target_val") shouldBe "MUTATED_Z"
    }
  }
}