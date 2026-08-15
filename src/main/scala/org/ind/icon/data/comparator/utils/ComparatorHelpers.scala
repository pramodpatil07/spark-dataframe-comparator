package org.ind.icon.data.comparator.utils

import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ComparatorHelpers extends Serializable {

  def standardizeArrays(df: DataFrame, config: ComparatorConfig): DataFrame = {
    if (!config.standardizeArrays) return df

    var res = df
    df.schema.fields.foreach { field => 
      field.dataType match {
        case ArrayType(elementType, _) =>
          if (config.complexTypeKeys.contains(field.name) && elementType.isInstanceOf[StructType]) {
            val key = config.complexTypeKeys(field.name).head
            val sortExpr = s"array_sort(`${field.name}`, (l, r) -> if(l.`$key` < r.`$key`, -1, if(l.`$key` > r.`$key`, 1, 0)))"
            res = res.withColumn(field.name, expr(sortExpr))
          } else {
            res = res.withColumn(field.name, array_sort(col(field.name)))
          }
        case _ => 
      }
    }
    res
  }

  def buildMismatchCheckExpr(colName: String, dataType: DataType, config: ComparatorConfig): org.apache.spark.sql.Column = {
    val sCol = col(s"s.$colName")
    val tCol = col(s"t.$colName")

    val isMismatch = if (config.enableNumericTolerance && dataType.isInstanceOf[NumericType]) {
      val sNum = expr(s"TRY_CAST(s.$colName AS DOUBLE)")
      val tNum = expr(s"TRY_CAST(t.$colName AS DOUBLE)")
      
      val bothNull = sCol.isNull && tCol.isNull
      val oneNull = sCol.isNull =!= tCol.isNull
      
      !bothNull && (oneNull || when(sNum.isNotNull && tNum.isNotNull, abs(sNum - tNum) > lit(config.numericTolerance)).otherwise(sCol =!= tCol))
    } else {
      not(sCol <=> tCol)
    }
    when(isMismatch, lit(colName)).otherwise(lit(null.asInstanceOf[String]))
  }
}