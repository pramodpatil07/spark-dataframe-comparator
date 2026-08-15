package org.ind.icon.data.comparator.utils

import org.ind.icon.data.comparator.model.ComparatorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * A utility object that isolates complex Spark SQL Catalyst expression generation.
 * By keeping SQL generation here, the main Orchestrator remains clean and readable.
 */
object ComparatorHelpers extends Serializable {

  /**
   * Recursively reflects on a Spark DataType to determine if it can be deterministically sorted.
   * Maps are fundamentally unordered in Spark; attempting to sort them crashes the Catalyst optimizer.
   */
  private def isOrderable(dt: DataType): Boolean = dt match {
    case _: MapType => false
    case StructType(fields) => fields.forall(f => isOrderable(f.dataType))
    case ArrayType(et, _) => isOrderable(et)
    case _ => true
  }

  /**
   * Standardizes arrays to eliminate false positive mismatches caused by elements being physically
   * out of order in the underlying storage (e.g., ADLS JSON dumps vs Delta Lake).
   */
  def standardizeArrays(df: DataFrame, config: ComparatorConfig): DataFrame = {
    if (!config.standardizeArrays) return df

    var res = df
    df.schema.fields.foreach { field => 
      field.dataType match {
        case ArrayType(elementType, _) =>
          val keyOpt = config.complexTypeKeys.get(field.name).flatMap(_.headOption)
          
          if (keyOpt.isDefined && elementType.isInstanceOf[StructType]) {
            // Keyed Arrays of Objects: We use Spark's higher-order SQL function `array_sort` 
            // with a custom lambda comparator to sort the objects deterministically by their logical key.
            val key = keyOpt.get
            val sortExpr = s"array_sort(`${field.name}`, (l, r) -> if(l.`$key` < r.`$key`, -1, if(l.`$key` > r.`$key`, 1, 0)))"
            res = res.withColumn(field.name, expr(sortExpr))
          } else if (isOrderable(elementType)) {
            // Primitive or Orderable Struct Arrays: We rely on native array_sort.
            res = res.withColumn(field.name, array_sort(col(field.name)))
          }
        case _ => // Skip non-array columns
      }
    }
    res
  }

  /**
   * Constructs the boolean SQL expression required to compare a single column between Source (s) and Target (t).
   * 
   * @return A Column expression that evaluates to the Column Name if a mismatch occurs, or explicitly NULL if matched.
   */
  def buildMismatchCheckExpr(colName: String, dataType: DataType, config: ComparatorConfig): org.apache.spark.sql.Column = {
    val sCol = col(s"s.$colName")
    val tCol = col(s"t.$colName")

    val isMismatch = if (config.enableNumericTolerance && dataType.isInstanceOf[NumericType]) {
      val bothNull = sCol.isNull && tCol.isNull
      val oneNull = sCol.isNull =!= tCol.isNull
      
      // Native ABS handles Double, Float, and Decimal(p,s) perfectly.
      // We explicitly avoid casting to DOUBLE to prevent massive floating-point truncation on Decimal types.
      val diff = abs(sCol - tCol)
      !bothNull && (oneNull || when(sCol.isNotNull && tCol.isNotNull, diff > lit(config.numericTolerance)).otherwise(sCol =!= tCol))
    } else {
      // Standard exact match: <=>' is Spark's null-safe equality operator.
      not(sCol <=> tCol)
    }
    
    // Evaluate the condition: return column name string if failed, null if passed. Nulls are filtered out later.
    when(isMismatch, lit(colName)).otherwise(lit(null.asInstanceOf[String]))
  }
}