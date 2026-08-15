# Spark Data Comparator

A highly optimized, distributed Spark framework for deep DataFrame comparison. Designed specifically for Enterprise Data Engineers migrating massive NoSQL-style JSON hierarchies (e.g., Azure Cosmos DB) into platforms like Databricks Unity Catalog and ADLS.

## Architecture: Two-Tiered Evaluation
Comparing hundreds of millions of rows across complex schemas using naive `explode()` mechanics will trigger Spark `OutOfMemory` (OOM) errors and catastrophic Shuffle Spills.

This engine solves this via a **Two-Tiered Pattern**:
1. **Tier 1 (Outer-Level Wide Join)**: Evaluates all primitive and complex columns natively via Catalyst. Drops identical rows instantly.
2. **Tier 2 (UDF Fallback)**: **ONLY** executes a specialized Jackson JSON parser on the rows that failed Tier 1, traversing infinite depths to extract the exact anomaly (e.g., `history[id=5].status`).

## Core Features
* **O(1) Delta Metric Extraction**: Writing to partitioned Delta Lakes fetches the `operationMetrics.numOutputRows` from the Delta Transaction Log, returning KPIs in milliseconds without evaluating a second DAG.
* **Array Standardization**: Utilizes higher-order SQL (`array_sort`) to deterministically order arrays of primitives and arrays of objects by logical keys *before* comparison.
* **Cartesian Protection**: Configurable `.validateUniqueness` blocks catastrophic exponential joins.
* **Decimal Preservation**: Evaluates numeric tolerances using native `abs()` SQL functions, preventing floating-point truncation issues caused by casting `Decimal(38,18)`.
* **Explicit Null Tracking**: Distinguishes between `"key": null` and a missing key entirely via explicit `Option[String]` matching.

---

## 🛠️ Use Cases

### Use Case 1: Simple Batch Comparison (Lazy Evaluation)
Used in exploratory notebooks to visualize data drifts without triggering expensive `.count()` operations.

```scala
import org.ind.icon.data.comparator.core.SparkDataFrameComparator
import org.ind.icon.data.comparator.model.ComparatorConfig

val config = ComparatorConfig(
  primaryKeys = Seq("txn_id"),
  ignoreColumns = Seq("etl_timestamp"),
  validateUniqueness = true
)

val result = new SparkDataFrameComparator().compare(sourceDf, targetDf, config)
display(result.mismatchedRecords)

### Use Case 2: Deep Nested NoSQL Comparison
Perfect for comparing Cosmos DB un-normalized JSON documents dumped to Parquet against a curated Delta Lake table.

val config = ComparatorConfig(
  primaryKeys = Seq("doc_id"),
  complexTypeKeys = Map("history_array" -> Seq("version_id")), // Aligns objects inside arrays!
  standardizeArrays = true,
  enableNumericTolerance = true,
  numericTolerance = 0.01 // Ignores minor float drift
)

val result = comparator.compare(cosmosDf, deltaDf, config)
// The output automatically maps the anomaly: "history_array[version_id=2].metadata.flag"

### Use Case 3: Databricks Unity Catalog Pipeline (Action Sink)
Built for scheduled Data Engineering jobs. Sinks the results into Unity Catalog and extracts execution metrics instantly.
import org.ind.icon.data.comparator.model.ComparatorSinkConfig
import java.util.UUID

val sink = ComparatorSinkConfig(
  catalogAndSchema = "prod_catalog.recon_schema",
  tablePrefix = "daily_recon",
  runId = UUID.randomUUID().toString,
  runDate = "2026-08-15"
)

val sinkResult = comparator.compareAndWrite(sourceDf, targetDf, config, sink)

println(s"Mismatch Count: ${sinkResult.mismatchCount}")

// Underlying DataFrames remain accessible without re-reading from disk
if (sinkResult.mismatchCount > 0) {
    val sampleAlertDf = sinkResult.comparatorResult.mismatchedRecords.limit(5)
    SlackNotifier.send(sampleAlertDf)
}


### Use Case 4: Bridging Tables / Dimension Keys
If you compare bridging tables that only contain Primary Keys and no payload data, the framework detects it and securely runs an anti-join validation.
val bridgeConfig = ComparatorConfig(primaryKeys = Seq("user_id", "group_id"))
val result = comparator.compare(sourceBridge, targetBridge, bridgeConfig)

// missingRecords and extraRecords are fully populated. mismatchedRecords remains empty.
