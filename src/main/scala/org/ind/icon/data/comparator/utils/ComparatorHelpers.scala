package org.ind.icon.data.comparator.utils

import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ComparatorHelpers extends Serializable {

  /**
   * Recursively checks if a Spark DataType can be deterministically ordered.
   * Maps are fundamentally un-orderable. Blindly sorting them crashes Catalyst.
   */
  private def isOrderable(dt: DataType): Boolean = dt match {
    case _: MapType => false
    case StructType(fields) => fields.forall(f => isOrderable(f.dataType))
    case ArrayType(et, _) => isOrderable(et)
    case _ => true
  }

  def standardizeArrays(df: DataFrame, config: ComparatorConfig): DataFrame = {
    if (!config.standardizeArrays) return df

    var res = df
    df.schema.fields.foreach { field => 
      field.dataType match {
        case ArrayType(elementType, _) =>
          // Safely handles missing configurations or empty key sequences
          val keyOpt = config.complexTypeKeys.get(field.name).flatMap(_.headOption)
          
          if (keyOpt.isDefined && elementType.isInstanceOf[StructType]) {
            val key = keyOpt.get
            val sortExpr = s"array_sort(`${field.name}`, (l, r) -> if(l.`$key` < r.`$key`, -1, if(l.`$key` > r.`$key`, 1, 0)))"
            res = res.withColumn(field.name, expr(sortExpr))
          } else if (isOrderable(elementType)) {
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
      val bothNull = sCol.isNull && tCol.isNull
      val oneNull = sCol.isNull =!= tCol.isNull
      
      // Native ABS handles Double, Float, and Decimal(p,s) perfectly.
      // Casting the Double tolerance to the native DataType prevents Decimal precision loss.
      val toleranceExpr = lit(config.numericTolerance).cast(dataType)
      val diff = abs(sCol - tCol)
      
      !bothNull && (oneNull || when(sCol.isNotNull && tCol.isNotNull, diff > toleranceExpr).otherwise(sCol =!= tCol))
    } else {
      not(sCol <=> tCol)
    }
    when(isMismatch, lit(colName)).otherwise(lit(null.asInstanceOf[String]))
  }
}