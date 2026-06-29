"""GC screening experiment in pure PySpark (client mode).

Python port of ``scripts/spark_submit/run-screening.sh``.  Instead of submitting
the ``sql-workloads`` JAR to Kubernetes in *cluster* mode, this module builds a
``SparkSession`` in *client* mode -- the driver runs in the local process (e.g. a
notebook kernel) and only the executors run in the cluster, exactly like
``start-pyspark-tpc.sh``.

The SQL to run is supplied by the caller as a **string** (typically written
directly in ``run_screening.ipynb``); this module no longer loads ``qN.sql``
files from a bundled query set.  ``run_screening`` optionally registers the
TPC-DS / TPC-H Parquet tables as temp views first so the SQL can ``FROM`` them.

Every tunable is a function argument so the notebook can drive the whole
experiment without editing this module.  The constants below are just defaults.

Typical notebook use::

    import screening
    sql = "SELECT ca_state, count(*) FROM customer_address GROUP BY ca_state"
    spark = screening.build_spark("A", "my_query", benchmark="tpcds", scale=100)
    result = screening.run_screening(spark, sql, "A", label="my_query",
                                     benchmark="tpcds", scale=100)
    spark.stop()

The five named configs (A, B, BHJ, SMJ, BHJ2) reproduce the heap / core /
broadcast-threshold combinations from the shell script; pass a custom
``ScreeningConfig`` instead of a name to override them.

Client-mode caveats vs. the original cluster-mode submit:
  * The driver JVM is the local process and is already running by the time a
    ``SparkSession`` is built, so ``spark.driver.memory`` and the driver
    ``-Xlog:gc*`` options cannot resize / reconfigure it.  Set those via
    ``PYSPARK_SUBMIT_ARGS`` before the JVM starts if you need them.  Executor
    settings (memory, cores, GC logging) take full effect because executors are
    launched after the session is configured.
  * Executor GC logs still land on the shared ``spark-logs-pvc`` PVC inside the
    cluster; driver GC logs (if enabled at launch) land on the local host.
"""

from __future__ import annotations

import datetime
import os
import subprocess
import time
from typing import Final, Optional, Union

from tpc_pyspark import DEFAULT_DATA_BASE, register_tables, table_names


# --- Defaults (every one is overridable from build_spark / the notebook) ------

DEFAULT_SPARK_LOGS_BASE_DIR: Final = "/var/spark-logs"
DEFAULT_EVENT_LOGS_DIR: Final = "s3a://spark-obj-storage/event-logs"
DEFAULT_NAMESPACE: Final = "spark"
# Executors run the same image as the driver so Spark/JVM versions match.
DEFAULT_CONTAINER_IMAGE: Final = "gihong96/spark-screening:v1"
DEFAULT_OBJ_STORAGE_ENDPOINT: Final = "https://hel1.your-objectstorage.com"
DEFAULT_SERVICE_ACCOUNT: Final = "spark"
DEFAULT_S3_SECRET_NAME: Final = "s3-creds"
DEFAULT_BENCH_HOST_PATH: Final = "/mnt/bench"

VALID_BENCHMARKS: Final = ("tpcds", "tpch")

# Executor GC collector flags, selectable per run (orthogonal to the A/B heap
# presets). Requires the JDK 21 base image; all four are verified present in
# apache/spark:4.1.2-scala2.13-java21-python3-ubuntu (Temurin 21).
GC_OPTS: Final = {
    "G1": "-XX:+UseG1GC",
    "ZGC": "-XX:+UseZGC",                          # non-generational (JDK 21 default mode)
    "ZGCGEN": "-XX:+UseZGC -XX:+ZGenerational",    # generational ZGC (JDK 21+)
    "SHENANDOAH": "-XX:+UseShenandoahGC",
}
DEFAULT_GC: Final = "G1"


# --- Screening configs (CONFIG A / B / BHJ / SMJ / BHJ2) ----------------------

class ScreeningConfig:
    """One screening preset: executor sizing + broadcast thresholds.

    Build a custom one in the notebook to override the named presets::

        cfg = screening.ScreeningConfig(executor_heap="3g", executor_cores=2,
                                        executor_instances=4, driver_memory="4g",
                                        memory_overhead="768m", threshold="96MB",
                                        static_threshold="-1", name="custom")
        spark = screening.build_spark(cfg, "q5")
    """

    def __init__(
        self,
        executor_heap: str,
        executor_cores: int,
        executor_instances: int,
        driver_memory: str,
        memory_overhead: str,
        threshold: str,
        static_threshold: str,
        name: str = "custom",
        note: str = "",
    ) -> None:
        self.executor_heap = executor_heap
        self.executor_cores = executor_cores
        self.executor_instances = executor_instances
        self.driver_memory = driver_memory
        self.memory_overhead = memory_overhead
        self.threshold = threshold
        self.static_threshold = static_threshold
        self.name = name
        self.note = note


CONFIGS: Final = {
    # Screening: 4GB heap, 128MB threshold
    "A": ScreeningConfig("4g", 2, 2, "4g", "768m", "128MB", "128MB",
                         note="Screening: 4GB heap, 128MB threshold"),
    # Original screening: 2GB heap, 192MB threshold
    "B": ScreeningConfig("2g", 2, 2, "4g", "512m", "192MB", "192MB",
                         note="Original screening: 2GB heap, 192MB threshold"),
    # BHJ: 1GB heap, 200MB adaptive threshold, 1 core
    "BHJ": ScreeningConfig("1g", 1, 2, "4g", "800m", "200MB", "-1",
                           note="BHJ: 1GB heap, 200MB threshold, 1 core"),
    # SMJ control: 1GB heap, broadcast disabled, 1 core
    "SMJ": ScreeningConfig("1g", 1, 2, "4g", "800m", "-1", "-1",
                           note="SMJ control: broadcast disabled, 1 core"),
    # BHJ moderate: 1.2GB heap, 128MB threshold, 2 cores
    "BHJ2": ScreeningConfig("1200m", 2, 2, "4g", "800m", "128MB", "-1",
                            note="BHJ moderate: 1.2GB heap, 128MB threshold, 2 cores"),
}
# Stamp each preset with its registry name (used in app/run ids and banners).
for _name, _cfg in CONFIGS.items():
    _cfg.name = _name


def _resolve_config(config: Union[str, ScreeningConfig]):
    """Accept a preset name or a ScreeningConfig and return ``(name, cfg)``."""
    if isinstance(config, ScreeningConfig):
        return config.name, config
    if config not in CONFIGS:
        raise ValueError(f"CONFIG must be one of {sorted(CONFIGS)}, got '{config}'")
    return config, CONFIGS[config]


# --- Helpers ------------------------------------------------------------------

def detect_master(explicit: Optional[str] = None) -> str:
    """Resolve the ``k8s://`` master URL.

    Precedence: explicit arg -> ``$SPARK_MASTER_URL`` -> current kubeconfig
    context server (``kubectl config view``).
    """
    if explicit:
        return explicit if explicit.startswith("k8s://") else f"k8s://{explicit}"
    env = os.environ.get("SPARK_MASTER_URL")
    if env:
        return env if env.startswith("k8s://") else f"k8s://{env}"
    # Running inside the cluster (driver pod): use the in-cluster API server.
    k8s_host = os.environ.get("KUBERNETES_SERVICE_HOST")
    if k8s_host:
        k8s_port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS",
                                  os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
        return f"k8s://https://{k8s_host}:{k8s_port}"
    server = subprocess.check_output(
        ["kubectl", "config", "view", "--minify", "-o",
         "jsonpath={.clusters[0].cluster.server}"],
        text=True,
    ).strip()
    if not server:
        raise RuntimeError(
            "Could not resolve a Spark master. Pass master=... or set "
            "SPARK_MASTER_URL, or ensure kubectl has a current context."
        )
    return f"k8s://{server}"


DEFAULT_QUERY_DIR: Final = os.environ.get("SCREENING_QUERY_DIR", "/work/queries")


def load_sql(path: str, query_dir: str = DEFAULT_QUERY_DIR) -> str:
    """Read a ``.sql`` file and return its text (use as ``SQL`` in the notebook).

    Accepts a real path, or a ``<benchmark>/<name>`` shorthand resolved under
    ``query_dir`` (the baked TPC-DS / TPC-H query sets). The ``.sql`` suffix is
    optional::

        load_sql("tpcds/q9")                 # -> /work/queries/tpcds/q9.sql
        load_sql("/work/queries/tpch/q1.sql")

    Spark runs one statement, so each file must hold a single query (the bundled
    TPC-DS/TPC-H files do; leading ``--`` comments are fine).
    """
    candidates = [path, path + ".sql",
                  os.path.join(query_dir, path),
                  os.path.join(query_dir, path + ".sql")]
    for cand in candidates:
        if os.path.isfile(cand):
            with open(cand, "r") as fh:
                return fh.read()
    raise FileNotFoundError(
        f"No .sql file for '{path}' (looked at {candidates}). "
        f"Set query_dir or pass a full path."
    )


def _slug(label: str) -> str:
    """Make *label* safe for an app/run id (alnum, dash, underscore)."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in label) or "query"


def run_id(benchmark: str, label: str, config_name: str) -> str:
    return f"screen_{benchmark}_{_slug(label)}_{config_name}"


# --- Session construction -----------------------------------------------------

def build_spark(
    config: Union[str, ScreeningConfig],
    label: str = "screen",
    *,
    benchmark: str = "tpcds",
    scale="100",
    # --- cluster / image ---
    master: Optional[str] = None,
    image: str = DEFAULT_CONTAINER_IMAGE,
    namespace: str = DEFAULT_NAMESPACE,
    service_account: str = DEFAULT_SERVICE_ACCOUNT,
    node_selector_spark_data: Optional[str] = "true",
    # --- client-mode driver networking (executors must reach this driver) ---
    driver_host: Optional[str] = None,
    driver_bind_address: Optional[str] = None,
    driver_port: Optional[int] = None,
    block_manager_port: Optional[int] = None,
    # --- object storage ---
    obj_storage_endpoint: Optional[str] = None,
    s3_secret_name: str = DEFAULT_S3_SECRET_NAME,
    # --- driver S3A credentials (client mode reads s3a:// locally) ---
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    # --- benchmark data volume (hostPath) ---
    bench_host_path: Optional[str] = DEFAULT_BENCH_HOST_PATH,
    # --- event log ---
    event_log_enabled: bool = True,
    event_logs_dir: str = DEFAULT_EVENT_LOGS_DIR,
    # --- GC collector + logging / PVC ---
    gc: str = DEFAULT_GC,
    gc_logging: bool = True,
    spark_logs_base_dir: str = DEFAULT_SPARK_LOGS_BASE_DIR,
    gc_logs_dir: Optional[str] = None,
    spark_logs_pvc_claim: str = "spark-logs-pvc",
    gc_filecount: int = 10,
    gc_filesize: str = "20m",
    # --- escape hatch: arbitrary extra confs (override anything above) ---
    extra_conf: Optional[dict] = None,
    timestamp: Optional[str] = None,
):
    """Build a client-mode ``SparkSession`` for one screening config.

    Only the executor-side knobs (memory, cores, instances, overhead, GC logging,
    broadcast thresholds, AQE) take effect in client mode; ``spark.driver.*`` is
    set for completeness but the already-running local driver JVM ignores it.

    ``config`` is a preset name (``"A"`` ...) or a custom ``ScreeningConfig``.
    ``extra_conf`` is a ``{key: value}`` dict applied last, so it overrides every
    other setting -- the catch-all for any Spark conf not exposed as an argument.

    If ``aws_access_key`` / ``aws_secret_key`` are given they are set as
    ``fs.s3a.access.key`` / ``fs.s3a.secret.key`` for the local driver; otherwise
    the S3A default chain (e.g. ``AWS_*`` env vars) is used.  Note these keys land
    in the Spark conf (visible in the UI / event log unless redacted).
    """
    from pyspark.sql import SparkSession

    config_name, cfg = _resolve_config(config)
    benchmark = benchmark.lower()
    if benchmark not in VALID_BENCHMARKS:
        raise ValueError(f"benchmark must be 'tpcds' or 'tpch', got '{benchmark}'")

    gc_key = gc.upper()
    if gc_key not in GC_OPTS:
        raise ValueError(f"gc must be one of {sorted(GC_OPTS)}, got '{gc}'")

    endpoint = obj_storage_endpoint or os.environ.get(
        "OBJ_STORAGE_ENDPOINT", DEFAULT_OBJ_STORAGE_ENDPOINT
    )
    gc_dir = gc_logs_dir or f"{spark_logs_base_dir}/gc-logs-raw"
    ts = timestamp or datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    rid = run_id(benchmark, label, config_name)

    conf: dict = {
        # --- Kubernetes / image ---
        "spark.kubernetes.namespace": namespace,
        "spark.kubernetes.authenticate.driver.serviceAccountName": service_account,
        "spark.kubernetes.container.image": image,
        # --- Executor sizing (per config) ---
        "spark.driver.memory": cfg.driver_memory,
        "spark.executor.instances": str(cfg.executor_instances),
        "spark.executor.memory": cfg.executor_heap,
        "spark.executor.cores": str(cfg.executor_cores),
        "spark.executor.memoryOverhead": cfg.memory_overhead,
        # --- AQE / broadcast thresholds (per config) ---
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": cfg.threshold,
        "spark.sql.autoBroadcastJoinThreshold": cfg.static_threshold,
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        "spark.shuffle.compress": "true",
        # --- S3A object storage ---
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
        # --- Executor object-storage credentials (from k8s secret) ---
        "spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID":
            f"{s3_secret_name}:AWS_ACCESS_KEY_ID",
        "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY":
            f"{s3_secret_name}:AWS_SECRET_ACCESS_KEY",
    }

    # Driver S3A credentials: use them if provided, else the S3A default chain.
    if aws_access_key and aws_secret_key:
        conf["spark.hadoop.fs.s3a.access.key"] = aws_access_key
        conf["spark.hadoop.fs.s3a.secret.key"] = aws_secret_key

    if driver_host:
        conf["spark.driver.host"] = driver_host
    if driver_bind_address:
        conf["spark.driver.bindAddress"] = driver_bind_address
    if driver_port:
        conf["spark.driver.port"] = str(driver_port)
    if block_manager_port:
        conf["spark.driver.blockManager.port"] = str(block_manager_port)

    if node_selector_spark_data is not None:
        conf["spark.kubernetes.node.selector.spark-data"] = node_selector_spark_data

    if bench_host_path:
        conf.update({
            "spark.kubernetes.executor.volumes.hostPath.bench.mount.path": bench_host_path,
            "spark.kubernetes.executor.volumes.hostPath.bench.options.path": bench_host_path,
            "spark.kubernetes.executor.volumes.hostPath.bench.options.type": "Directory",
        })

    if event_log_enabled:
        conf["spark.eventLog.enabled"] = "true"
        conf["spark.eventLog.dir"] = event_logs_dir

    # Selected collector always applies; GC logging (+ its PVC) is optional.
    conf["spark.screening.gc"] = gc_key
    exec_gc = f"{GC_OPTS[gc_key]} -XX:+PrintCommandLineFlags"
    if gc_logging:
        # Executor GC logs -> shared PVC (mounted at spark_logs_base_dir).
        exec_gc += (
            f" -Xlog:gc*:file={gc_dir}/{ts}-{rid}-{gc_key}-executor-{{{{EXECUTOR_ID}}}}.log"
            f":utctime,uptime,level,tags:filecount={gc_filecount},filesize={gc_filesize}"
        )
        conf.update({
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-logs-pvc.mount.path": spark_logs_base_dir,
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-logs-pvc.mount.readOnly": "false",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-logs-pvc.options.claimName": spark_logs_pvc_claim,
        })
    conf["spark.executor.extraJavaOptions"] = exec_gc

    if extra_conf:
        conf.update({k: str(v) for k, v in extra_conf.items()})

    builder = SparkSession.builder.master(detect_master(master)).appName(rid)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def print_config_banner(spark, label, config_name, cfg, benchmark, scale, root, timestamp):
    sc = spark.sparkContext
    line = "=" * 35
    print(line)
    print(f"GC Screening: {label} Config {config_name} ({benchmark})")
    print(line)
    print(f"  Label:                {label}")
    print(f"  Config:               {config_name} (threshold={cfg.threshold})")
    print(f"  GC Collector:         {spark.conf.get('spark.screening.gc', 'G1')}")
    print(f"  Executor Heap:        {cfg.executor_heap}")
    print(f"  Memory Overhead:      {cfg.memory_overhead}")
    print(f"  Executor Cores:       {cfg.executor_cores}")
    print(f"  Executor Instances:   {cfg.executor_instances}")
    print(f"  Driver Memory:        {cfg.driver_memory}")
    print(f"  AQE:                  {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"  Adaptive BHJ thresh:  {spark.conf.get('spark.sql.adaptive.autoBroadcastJoinThreshold')}")
    print(f"  Static BHJ thresh:    {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
    print(f"  Coalesce Partitions:  {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")
    print(f"  Skew Join:            {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
    print(f"  Benchmark:            {benchmark} SF{scale}")
    print(f"  Data Location:        {root if root else '(tables not auto-registered)'}")
    print(f"  Spark Version:        {sc.version}")
    print(f"  App ID:               {sc.applicationId}")
    print(f"  Timestamp:            {timestamp}")
    print(line)


def _aqe_log_offset(spark) -> tuple[Optional[str], int]:
    """Return (driver AQE log path, current size) or (None, 0) if not configured.

    The path comes from the -Daqe.log.file system property set on the driver JVM
    (see log4j2-aqe.properties). In client mode the driver is the local process so
    the file is a plain local path we can read.
    """
    src = spark._jvm.System.getProperty("aqe.log.file")
    if src and os.path.isfile(src):
        return src, os.path.getsize(src)
    return src, 0


def _publish_aqe_log(spark, *, ts, rid, gc_name, start_offset, out_dir) -> Optional[str]:
    """Copy THIS run's slice of the driver AQE log to *out_dir* (s3a:// or local).

    Client mode keeps one long-lived driver JVM, so log4j appends every run to a
    single aqe.log. We upload only the bytes appended during this run
    (start_offset..EOF) as a per-run object named like the GC logs, so AQE captures
    land next to the event logs in S3 and survive the pod. Uses the driver's
    already-configured Hadoop FS (reuses the s3a creds -- no new deps/credentials).

    Returns the destination URI, or None if AQE file logging wasn't enabled. Any
    failure is swallowed with a warning -- logging must never fail the experiment.
    """
    try:
        src = spark._jvm.System.getProperty("aqe.log.file")
        if not src or not os.path.isfile(src):
            return None
        size = os.path.getsize(src)
        lo = start_offset if 0 <= start_offset <= size else 0  # rolled/truncated -> whole file
        with open(src, "rb") as fh:
            fh.seek(lo)
            data = fh.read()
        if not data:
            return None
        name = f"{ts}-{rid}-{gc_name}-driver-aqe.log"
        stage = f"/tmp/{name}"
        with open(stage, "wb") as fh:
            fh.write(data)
        dst = f"{out_dir.rstrip('/')}/{name}"
        jvm = spark._jvm
        Path = jvm.org.apache.hadoop.fs.Path
        dst_p = Path(dst)
        fs = dst_p.getFileSystem(spark._jsc.hadoopConfiguration())
        fs.copyFromLocalFile(False, True, Path("file://" + stage), dst_p)  # delSrc=F, overwrite=T
        os.remove(stage)
        return dst
    except Exception as exc:  # noqa: BLE001 - auxiliary; never fail the run
        print(f"WARN: AQE log publish failed: {exc}")
        return None


# --- Run ----------------------------------------------------------------------

def run_screening(
    spark,
    sql: str,
    config: Union[str, ScreeningConfig],
    *,
    label: str = "query",
    benchmark: str = "tpcds",
    scale="100",
    data_base: str = DEFAULT_DATA_BASE,
    register: bool = True,
    show_plans: bool = True,
    show_rows: int = 0,
    timestamp: Optional[str] = None,
    aqe_log_dir: str = "s3a://spark-obj-storage/aqe-logs",
) -> dict:
    """Run a SQL string against the configured session, with timing + plans.

    The SQL is supplied by the caller (e.g. written inline in the notebook).
    If ``register`` is true the TPC-DS / TPC-H Parquet tables are registered as
    temp views first so the SQL can ``FROM`` them; set it false if you register
    views yourself or the SQL needs none.  ``show_rows > 0`` also displays that
    many result rows (otherwise only the row count is measured).

    Prints the same machine-parseable ``RESULT:`` line as ``run-screening.sh``.
    """
    benchmark = benchmark.lower()
    config_name, cfg = _resolve_config(config)
    gc_name = spark.conf.get("spark.screening.gc", "G1")
    ts = timestamp or datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    rid = run_id(benchmark, label, config_name)

    root = ""
    if register:
        root = register_tables(spark, benchmark, scale, data_base)

    print_config_banner(spark, label, config_name, cfg, benchmark, scale, root, ts)
    if register:
        print(f"Registered {len(list(table_names(benchmark)))} "
              f"{benchmark.upper()} tables from {root}")

    exit_code = 0
    rows = -1
    duration_ms = 0.0
    # Mark the driver AQE log position so we upload only this run's slice.
    _aqe_src, aqe_off = _aqe_log_offset(spark)
    try:
        print(f"\n===== Running {label} =====")
        df = spark.sql(sql)
        if show_plans:
            print(f"--- Logical Plan ({label}) ---")
            print(df._jdf.queryExecution().analyzed().toString())
        print(f"--- Executing {label} ---")
        start = time.perf_counter()
        if show_rows > 0:
            df.show(show_rows, truncate=False)
            rows = df.count()
        else:
            rows = df.count()
        duration_ms = (time.perf_counter() - start) * 1000.0
        print(f"Query {label} completed: rows={rows}, duration={duration_ms:.2f} ms")
        if show_plans:
            print(f"--- Physical Plan after AQE ({label}) ---")
            print(df._jdf.queryExecution().executedPlan().toString())
        print(f"===== End {label} =====")
    except Exception as exc:  # noqa: BLE001 - report and surface as exit code
        exit_code = 1
        print(f"ERROR running {label}: {exc}")

    app_id = spark.sparkContext.applicationId
    # Ship this run's AQE log slice to S3 (next to the event logs). No-op if the
    # driver wasn't launched with -Daqe.log.file / the log4j2-aqe config.
    aqe_log_path = _publish_aqe_log(
        spark, ts=ts, rid=rid, gc_name=gc_name, start_offset=aqe_off,
        out_dir=aqe_log_dir,
    )
    line = "=" * 35
    print()
    print(line)
    print(f"Screening Complete: {label} Config {config_name}")
    print(line)
    print(f"  Exit Code:    {exit_code}")
    print(f"  App ID:       {app_id}")
    print(f"  Run ID:       {rid}")
    print(f"  Rows:         {rows}")
    print(f"  Duration:     {duration_ms:.2f} ms")
    print(f"  Timestamp:    {ts}")
    print(f"  AQE Log:      {aqe_log_path or '(not captured -- driver JVM not configured)'}")
    print(line)
    # Machine-parseable result (run-screening.sh format + trailing GC field).
    print(f"RESULT:{benchmark}:{label}:{config_name}:{cfg.threshold}:{exit_code}:{ts}:{app_id}:{gc_name}")

    return {
        "benchmark": benchmark,
        "label": label,
        "config": config_name,
        "gc": gc_name,
        "threshold": cfg.threshold,
        "exit_code": exit_code,
        "rows": rows,
        "duration_ms": duration_ms,
        "timestamp": ts,
        "app_id": app_id,
        "run_id": rid,
        "aqe_log_path": aqe_log_path,
    }
