package com.recon.engine.utils

import com.recon.engine.model.ReconConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Utility module isolating complex Spark SQL expression generation.
 * This keeps the main orchestrator clean and readable.
 */
object ReconHelpers extends Serializable {

  private def isOrderable(dataType: DataType): Boolean = dataType match {
    case _: MapType => false
    case ArrayType(elementType, _) => isOrderable(elementType)
    case StructType(fields) => fields.forall(field => isOrderable(field.dataType))
    case _ => true
  }

  /**
   * Sorts arrays within the DataFrame natively using Spark Catalyst.
   * Why? If arrays are un-ordered, `Array(A, B)` and `Array(B, A)` trigger false positive mismatches.
   * 
   * @param df     The DataFrame to standardize
   * @param config The Recon configuration containing the primary keys for arrays of objects
   */
  def standardizeArrays(df: DataFrame, config: ReconConfig): DataFrame = {
    if (!config.standardizeArrays) return df

    var res = df
    df.schema.fields.foreach { field => 
      field.dataType match {
        case ArrayType(elementType, _) =>
          if (config.complexTypeKeys.contains(field.name) && elementType.isInstanceOf[StructType]) {
            // It's a Keyed Array of Objects. Use higher-order SQL to sort deterministically by its key.
            val key = config.complexTypeKeys(field.name).headOption.getOrElse(
              throw new IllegalArgumentException(s"complexTypeKeys.${field.name} must contain at least one field name.")
            ).replace("`", "``")
            val sortExpr = s"array_sort(`${field.name.replace("`", "``")}`, (l, r) -> if(l.`$key` < r.`$key`, -1, if(l.`$key` > r.`$key`, 1, 0)))"
            res = res.withColumn(field.name, expr(sortExpr))
          } else if (isOrderable(elementType)) {
            // It's a Primitive Array. Use standard array_sort.
            res = res.withColumn(field.name, array_sort(col(field.name)))
          }
        case _ => // Non-array types remain untouched
      }
    }
    res
  }

  /**
   * Builds the boolean mismatch expression for a single column.
   * Handles null-safety, strict equality, and optional numeric tolerance.
   * 
   * @return A Column expression that evaluates to the Column Name if mismatched, or NULL if matched.
   */
  def buildMismatchCheckExpr(colName: String, dataType: DataType, config: ReconConfig): org.apache.spark.sql.Column = {
    val sCol = col(s"s.$colName")
    val tCol = col(s"t.$colName")

    val isMismatch = if (config.enableNumericTolerance && config.numericTolerance > 0.0 && dataType.isInstanceOf[NumericType]) {
      // numeric tolerance active: gracefully attempt to cast to double and compute absolute difference
      val sNum = expr(s"TRY_CAST(s.$colName AS DOUBLE)")
      val tNum = expr(s"TRY_CAST(t.$colName AS DOUBLE)")
      
      val bothNull = sCol.isNull && tCol.isNull
      val oneNull = sCol.isNull =!= tCol.isNull
      
      !bothNull && (oneNull || when(sNum.isNotNull && tNum.isNotNull, abs(sNum - tNum) > lit(config.numericTolerance)).otherwise(sCol =!= tCol))
    } else {
      // standard exact match: <=>' is null-safe equality operator.
      not(sCol <=> tCol)
    }

    // Return the column name string if failed, or null if passed. We filter nulls out later.
    when(isMismatch, lit(colName)).otherwise(lit(null.asInstanceOf[String]))
  }
}
