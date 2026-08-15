# Spark Data Comparator

A highly optimized, distributed Spark framework for deep DataFrame comparison. Designed specifically for Enterprise Data Engineers migrating massive NoSQL-style JSON hierarchies (such as Azure Cosmos DB payloads) to platforms like Databricks Unity Catalog and ADLS.

## Architecture: Two-Tiered Evaluation
Comparing 100+ Million rows across hundreds of columns using naive `explode` mechanisms will instantly trigger Spark `OutOfMemory` (OOM) errors and catastrophic Shuffle Spills.

To solve this, this engine executes a **Two-Tiered Evaluation Pattern**:
1. **Tier 1 (Outer-Level Wide Join)**: Uses highly optimized Spark Catalyst Anti-Joins and Inner-Joins to dynamically compare all 500+ primitive columns simultaneously. It groups mismatched columns into an array, completely dropping perfectly matched rows.
2. **Tier 2 (UDF Fallback)**: **ONLY** if a row failed the Tier 1 evaluation, and **ONLY** if the failure occurred within a complex `Struct`, `Array`, or `Map`, the engine deploys a highly tuned Jackson JSON UDF. This UDF traverses infinite hierarchy depths to extract the exact `(column_path, value)` anomaly without crashing Catalyst.

## Enterprise Features
* **O(1) Delta Lake KPI Extraction**: Writing to partitioned Delta Lakes automatically fetches the `operationMetrics.numOutputRows` from the Delta Transaction Log, returning instantaneous KPIs (Matches, Missing, Extra) without ever executing a second DAG evaluation or scanning physical Parquet files.
* **Dynamic Array Standardization**: Automatically detects arrays and utilizes higher-order SQL (`array_sort`) to deterministically order arrays of primitives and arrays of objects by logical keys *before* comparison, entirely preventing false positive mismatches.
* **Decimal Preservation**: Evaluates numeric tolerances (`enableNumericTolerance`) using native `abs()` SQL functions, preventing floating-point truncation issues caused by casting `Decimal(38,18)` types.
* **Explicit Null Tracking**: Distinguishes between `"key": null` and a completely missing key using explicit `Option[String]` tracking during deep JSON diffing.
* **Cartesian Protection**: Configurable `.validateUniqueness` blocks catastrophic many-to-many joins caused by duplicate primary keys.

## Integration & Getting Started

**Group:** `org.ind.icon.data`  
**Artifact:** `spark-data-comparator`

### Basic Execution (Lazy Evaluation)
Used primarily in exploratory notebooks to visualize data without triggering expensive `.count()` operations.

```scala
import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig

val config = ComparatorConfig(
  primaryKeys = Seq("txn_id"),
  ignoreColumns = Seq("etl_timestamp"),
  complexTypeKeys = Map("history_array" -> Seq("version_id")),
  standardizeArrays = true,
  validateUniqueness = true
)

val comparator = new SparkDataFrameComparator()
val result = comparator.compare(sourceDf, targetDf, config)

display(result.mismatchedRecords)