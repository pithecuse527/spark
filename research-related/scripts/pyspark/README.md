# PySpark workloads (client mode)

Pure-Python replacement for the `spark-submit` shell workflow. The driver runs
locally (notebook kernel / python process); only the executors run in the
Spark-on-K8s cluster. The SQL to run is written **directly as a string** in the
notebook (no `qN.sql` files are loaded anymore).

## Files

| File | Purpose |
|------|---------|
| `tpc_pyspark.py` | TPC-DS/TPC-H table lists + `register_tables()` (shared) |
| `screening.py` | GC-screening logic: config presets, session builder, SQL runner |
| `run_screening.ipynb` | Screening experiment entry point (all settings + SQL live here) |
| `pyspark_startup.py`, `start-pyspark-tpc.sh` | Interactive shell launcher (unchanged) |

## Screening experiment

Port of `scripts/spark_submit/run-screening.sh`. Open `run_screening.ipynb` from
this directory, fill in the `SQL` string and the settings cells, and run. Every
user-facing setting (SQL, config, benchmark/scale, image / namespace / endpoint,
event & GC log / PVC paths, and an arbitrary `EXTRA_CONF`) is tuned in the
notebook cells.

You can also drive it from plain Python:

```python
import screening
sql = "SELECT ca_state, count(*) FROM customer_address GROUP BY ca_state"
spark = screening.build_spark("A", "my_query", benchmark="tpcds", scale=100)
screening.run_screening(spark, sql, "A", label="my_query", benchmark="tpcds", scale=100)
spark.stop()
```

- `register=True` (default) registers the TPC tables as temp views before running
  the SQL, so it can reference them directly (`FROM store_sales ...`). Set it to
  `False` if you register views yourself or the SQL needs none.
- Configs `A | B | BHJ | SMJ | BHJ2` reproduce the heap / core / broadcast-threshold
  combinations from the shell script. To set values yourself, build a
  `screening.ScreeningConfig(...)` and pass it instead of a name.
- The executor **GC collector** is a separate axis, selected with `gc=` (orthogonal
  to the heap presets): `G1` (default) `| ZGC | ZGCGEN | SHENANDOAH`. `ZGCGEN` is
  generational ZGC (JDK 21+). Each collector's executor GC log is tagged with its
  name on the PVC. Requires the JDK 21 image (`apache/spark:4.1.2-scala2.13-java21-python3`).
- Each config needs its own `SparkSession` (executor sizing is fixed at session start).
- A run prints the machine-parseable
  `RESULT:<benchmark>:<label>:<config>:<threshold>:<exit>:<ts>:<app_id>:<gc>` line
  (the shell-script format plus a trailing GC field).

## Client-mode vs. the original cluster-mode submit

The shell script used `--deploy-mode cluster` (driver inside K8s). This runs in
**client** mode, which changes two things:

- **Driver GC logs**: the driver JVM is the local process and is already running
  before the session is built, so `spark.driver.memory` and driver `-Xlog:gc*`
  options do **not** take effect. Set them via `PYSPARK_SUBMIT_ARGS` before the
  kernel starts if you need them. **Executor** GC logging is unaffected and still
  writes to the shared `spark-logs-pvc` PVC.
- **Object-storage credentials**: the local driver reads S3A with the local
  `AWS_*` environment (no driver-side `secretKeyRef`); executors still get creds
  from the `s3-creds` K8s secret.
