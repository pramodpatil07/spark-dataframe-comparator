# Spark Recon Engine

Spark Recon Engine is a Scala 2.13 library for reconciling two Apache Spark
DataFrames by business key. It returns lazy DataFrames for matched, missing,
extra, mismatched, and field-level complex-type differences. It can also append
non-empty outputs to Delta Lake.

## Compatibility

| Component | Supported version |
| --- | --- |
| Scala | 2.13 |
| Apache Spark | 4.0.x |
| Java | 17+ |
| Delta Lake (only `compareAndWrite`) | 4.0.x |

Spark, Scala, and Jackson are `compileOnly` dependencies: applications provide
them through their Spark runtime. Delta Lake must be configured on the Spark
session before calling `compareAndWrite`.

## Install

Publish locally during development:

```shell
./gradlew publishToMavenLocal
```

Then depend on the Scala-suffixed artifact:

```kotlin
implementation("com.recon.engine:spark-recon-engine_2.13:1.0.0")
```

## Basic usage

```scala
import com.recon.engine.core.SparkDataFrameComparator
import com.recon.engine.model.ReconConfig

val result = new SparkDataFrameComparator().compare(
  source,
  target,
  ReconConfig(primaryKeys = Seq("id"))
)

result.matchedRecords.show()
result.missingRecords.show()
result.extraRecords.show()
result.mismatchedRecords.show(false)
result.complexMismatches.show(false)
```

The two inputs must contain the same column names and types, and every primary
key must be present in both schemas. Primary keys are expected to be unique;
the library does not attempt multiset reconciliation for duplicate keys.

## Configuration

- `ignoreColumns` removes existing columns from comparison output and checks.
- `enableNumericTolerance` applies `numericTolerance` to numeric columns and
  complex field details. The tolerance must be non-negative.
- `standardizeArrays` sorts orderable arrays before comparison. For arrays of
  structs, configure `complexTypeKeys`, for example
  `Map("line_items" -> Seq("line_id"))`, to sort by a stable business key.
  Arrays containing maps are not reordered because Spark does not define their
  ordering.

`complexMismatches` has this schema: `pk_json`, `column_path`, `source_val`,
and `target_val`.

## Delta output

```scala
import com.recon.engine.model.ReconSinkConfig

val summary = new SparkDataFrameComparator().compareAndWrite(
  source,
  target,
  ReconConfig(Seq("id")),
  ReconSinkConfig(
    basePath = "abfss://container@account.dfs.core.windows.net/reconciliation",
    tablePrefix = "orders",
    runId = "2026-08-15T00:00:00Z",
    runDate = "2026-08-15"
  )
)
```

Each non-empty category is appended to `<basePath>/<tablePrefix>_<category>`.
The returned counts describe only the current invocation, even when a run ID is
reused. `partitionCols` may contain `run_date` and/or `run_id`.

## Build and verify

```shell
./gradlew check
./gradlew publishToMavenLocal
```

Before publishing to a remote repository, add the real project URL, SCM
coordinates, developer details, repository credentials, and signing settings
required by that repository.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
