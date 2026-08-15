package org.ind.icon.data.comparator.utils

import org.ind.icon.data.comparator.model.{ComparatorConfig, ComparatorResult, ComparatorSinkConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

/**
 * A highly modularized utility object that isolates validation, complex Spark SQL Catalyst 
 * expression generation, and Delta Lake I/O operations.
 */
object ComparatorHelpers extends Serializable {

  /**
   * Validates that both DataFrames contain the required primary keys and checks for uniqueness 
   * to prevent catastrophic Cartesian products during the join phase.
   */
  def validateSchemasAndKeys(source: DataFrame, target: DataFrame, config: ComparatorConfig): Unit = {
    val missingSourcePKs = config.primaryKeys.filterNot(source.columns.contains)
    require(missingSourcePKs.isEmpty, s"Source is missing Primary Keys: ${missingSourcePKs.mkString(",")}")
    
    val missingTargetPKs = config.primaryKeys.filterNot(target.columns.contains)
    require(missingTargetPKs.isEmpty, s"Target is missing Primary Keys: ${missingTargetPKs.mkString(",")}")

    if (config.validateUniqueness) {
      require(source.groupBy(config.primaryKeys.map(col): _*).count().filter(col("count") > 1).isEmpty, "Source has duplicate PKs.")
      require(target.groupBy(config.primaryKeys.map(col): _*).count().filter(col("count") > 1).isEmpty, "Target has duplicate PKs.")
    }
  }

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
            // Keyed Arrays of Objects: Uses Spark's higher-order SQL function `array_sort` 
            val key = keyOpt.get
            val sortExpr = s"array_sort(`${field.name}`, (l, r) -> if(l.`$key` < r.`$key`, -1, if(l.`$key` > r.`$key`, 1, 0)))"
            res = res.withColumn(field.name, expr(sortExpr))
          } else if (isOrderable(elementType)) {
            // Primitive or Orderable Struct Arrays
            res = res.withColumn(field.name, array_sort(col(field.name)))
          }
        case _ => 
      }
    }
    res
  }

  /**
   * Constructs the boolean SQL expression required to compare a single column between Source (s) and Target (t).
   * @return A Column expression evaluating to the Column Name if mismatched, or explicitly NULL if matched.
   */
  def buildMismatchCheckExpr(colName: String, dataType: DataType, config: ComparatorConfig): org.apache.spark.sql.Column = {
    val sCol = col(s"s.$colName")
    val tCol = col(s"t.$colName")

    val isMismatch = if (config.enableNumericTolerance && dataType.isInstanceOf[NumericType]) {
      val bothNull = sCol.isNull && tCol.isNull
      val oneNull = sCol.isNull =!= tCol.isNull
      
      // Native ABS handles Double, Float, and Decimal(p,s) perfectly.
      val toleranceExpr = lit(config.numericTolerance).cast(dataType)
      val diff = abs(sCol - tCol)
      
      !bothNull && (oneNull || when(sCol.isNotNull && tCol.isNotNull, diff > toleranceExpr).otherwise(sCol =!= tCol))
    } else {
      not(sCol <=> tCol) // Null-safe exact equality
    }
    when(isMismatch, lit(colName)).otherwise(lit(null.asInstanceOf[String]))
  }

  /** Constructs an empty DataFrame representing the Tier-2 complex mismatches. */
  def buildEmptyComplexDf(spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
      .withColumn("pk_json", lit(""))
      .withColumn("column_path", lit(""))
      .withColumn("source_val", lit(""))
      .withColumn("target_val", lit(""))
  }

  /** Constructs an entirely empty ComparatorResult (used when comparing key-only tables). */
  def buildEmptyResult(joined: DataFrame, missing: DataFrame, extra: DataFrame, spark: SparkSession): ComparatorResult = {
    val emptyMismatches = spark.emptyDataFrame
      .withColumn("mismatched_columns", typedLit(Array.empty[String]))
      .withColumn("source_data", lit(null))
      .withColumn("target_data", lit(null))
    ComparatorResult(joined, missing, extra, emptyMismatches, buildEmptyComplexDf(spark))
  }

  /**
   * Writes the result DataFrame to Unity Catalog / Delta Lake and extracts the exact 
   * row count written in O(1) time by reading the transaction log's operation metrics.
   */
  def writeDeltaAndGetCount(df: DataFrame, suffix: String, sink: ComparatorSinkConfig): Long = {
    val tableName = s"${sink.catalogAndSchema}.${sink.tablePrefix}_$suffix"
    
    // Blindly write. Delta natively materializes empty directories/schemas safely.
    df.withColumn("run_id", lit(sink.runId)).withColumn("run_date", lit(sink.runDate))
      .write.format("delta").mode(sink.saveMode).partitionBy(sink.partitionCols: _*).saveAsTable(tableName)
    
    val history = DeltaTable.forName(df.sparkSession, tableName).history(1)
    val metrics = history.select(expr("element_at(operationMetrics, 'numOutputRows')")).first()
    val rowsStr = metrics.getAs[String](0)
    if (rowsStr != null) rowsStr.toLong else 0L
  }
}