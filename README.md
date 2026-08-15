# Spark Data Comparator

A highly optimized, distributed Spark framework for deep DataFrame comparison. Designed to handle massive NoSQL-style JSON hierarchies extracted from databases like Azure Cosmos DB, comparing them effortlessly at scale.

## Features
- **Two-Tiered Evaluation**: Defers JSON flattening to prevent Catalyst explosion on 100M+ row tables.
- **Delta Lake Integration**: Sinks results natively to Delta Lake partitioned tables, extracting KPIs instantly from the transaction log.
- **Dynamic Array Standardization**: Prevents false positive mismatches by utilizing higher-order SQL to internally sort `Array[Struct]` objects by user-defined primary keys.
- **Floating Point Drift**: Handles slight mathematical anomalies via configurable `.enableNumericTolerance`.

## Group & Artifact
**Group:** `org.ind.icon.data`
**Artifact:** `spark-data-comparator`