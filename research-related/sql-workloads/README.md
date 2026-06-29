# sql-workloads — GC-screening job jar (spark-submit, cluster mode)

The jar that `scripts/spark_submit/run-screening.sh` submits. It revives the
cluster-mode workflow on the current image (Spark 4.1.2 / Java 21 / Scala 2.13),
replacing the lost `sql-workloads-1.0.jar` that targeted Spark 3.x.

Self-contained Maven project — **not** part of Spark's multi-module build. It
depends on released `spark-sql_2.13:4.1.2` as `provided` and produces a thin jar;
the Spark runtime and S3A come from the container image (`gihong96/spark-screening:v1`).

## What it does

`ScreeningJob.run(benchmark, [query, scale, dataLocation])`:
1. `getOrCreate()` a SparkSession — all sizing/AQE/threshold/GC/S3A conf is set by
   `run-screening.sh` via `--conf`, so the job sets none of it.
2. Registers the benchmark's Parquet tables as temp views from `dataLocation`
   (ports `tpc_pyspark.register_tables`).
3. Loads the named query from a bundled resource (`q9` → `/<benchmark>/q9.sql`),
   runs it, prints the analyzed + post-AQE plans, times `count()`, and emits
   `QUERY_RESULT:<benchmark>:<query>:<rows>:<ms>:<exit>:<appId>`.

Main classes (match `run-screening.sh`): `com.research.gcaware.TpcdsQueryRunner`,
`com.research.gcaware.TpchQueryRunner`.

## Build & deploy

```bash
./build.sh           # -> target/sql-workloads-1.0.jar  (refreshes bundled .sql first)
./build.sh upload    # also uploads to s3a://spark-obj-storage/jars/  (needs awscli + creds)
```

The driver pulls the jar from `s3a://spark-obj-storage/jars/sql-workloads-1.0.jar`
(`WORKLOAD_JAR_URI` in the script), so upload it there after building.

## Run

```bash
# ./run-screening.sh <QUERY> <CONFIG> [BENCHMARK] [SCALE] [DATA_BASE]
../scripts/spark_submit/run-screening.sh q9 A tpch 200 file:///mnt/bench
```

The bundled query set is the TPC-DS/TPC-H `.sql` files from
`sql/core/src/test/resources/{tpcds,tpch}` (re-copied on each build).
