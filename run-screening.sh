#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# GC Screening Experiment Script
#
# Fixed config: 2 executors, 2 cores, 2GB heap, 512m overhead
# AQE enabled with coalescePartitions=true, skewJoin=false
# Executor failures: max 2 task failures, then job aborts (fail fast)
# Job timeout: 15 minutes (K8s activeDeadlineSeconds on driver pod)
#
# Isolation intent:
#   - spark.local.dir=/var/spark-logs/shuffle: redirect shuffle/spill to the NFS PVC
#     (spark-logs-pvc, already mounted at /var/spark-logs) so shuffle writes do NOT
#     count against the node's 16 GB ephemeral-storage quota; eliminates K8s disk-
#     eviction as a confounder for TPC-H SF200 (12-37 GB shuffle per query)
#   - memoryOverhead 1536m (BHJ/SMJ configs): keeps off-heap slack generous so that
#     only the JVM heap becomes the binding constraint; failures must come from GC,
#     not from container memory or disk limits
#
# Usage: ./run-screening.sh <QUERY> <CONFIG> [BENCHMARK] [SHUFFLE_MODE]
#   CONFIG: A (128MB threshold) or B (192MB threshold)
#   BENCHMARK: tpcds (default) or tpch
#   SHUFFLE_MODE: native (default) or celeborn
#   SCALE=100|200 selects /mnt/bench/<benchmark>-scale-<SCALE> (default: 200)
#   SQL_TEXT, SQL_FILE, or SQL_B64 enables generic PySpark SQL mode instead of the workload jar.
#   ENTROPY_AQE=1 enables entropy-aqe-3.5.8.jar
#   ENTROPY_AQE_MODE=restore|dynamic chooses AQE coalesce intervention mode
#   JVM_AQE=1 enables jvm-aqe-plugin-3.5.8.jar
#
# Example: ./run-screening.sh q5 A tpcds celeborn
# Example: ENTROPY_AQE=1 ENTROPY_AQE_MODE=restore ./run-screening.sh q14b BHJ
# Example: JVM_AQE=1 ./run-screening.sh q38 BHJ
# =============================================================================

QUERY="${1:?Usage: $0 <QUERY> <CONFIG> [BENCHMARK]}"
CONFIG="${2:?Usage: $0 <QUERY> <CONFIG> [BENCHMARK]}"
BENCHMARK="${3:-tpcds}"
SHUFFLE_MODE="${4:-${SHUFFLE_MODE:-native}}"
SCALE="${SCALE:-200}"
SQL_TEXT="${SQL_TEXT:-}"
SQL_FILE="${SQL_FILE:-}"
SQL_B64="${SQL_B64:-}"
SQL_RUNNER_URI="${SQL_RUNNER_URI:-s3a://spark-obj-storage/jars/generic_sql_runner.py}"
SQL_RESULT_LIMIT="${SQL_RESULT_LIMIT:-20}"
SQL_SUMMARY_BASE="${SQL_SUMMARY_BASE:-s3a://spark-obj-storage/results/sql-screening}"
SQL_MODE=0
if [ -n "$SQL_TEXT" ] || [ -n "$SQL_FILE" ] || [ -n "$SQL_B64" ]; then
  SQL_MODE=1
fi

# --- Config-specific parameters ---
case "$CONFIG" in
  A)
    # Original screening: 2GB heap, 128MB threshold
    EXECUTOR_HEAP="2g"
    EXECUTOR_CORES=2
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="512m"
    THRESHOLD="128MB"
    STATIC_THRESHOLD="128MB"
    ;;
  B)
    # Original screening: 2GB heap, 192MB threshold
    EXECUTOR_HEAP="2g"
    EXECUTOR_CORES=2
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="512m"
    THRESHOLD="192MB"
    STATIC_THRESHOLD="192MB"
    ;;
  BHJ)
    # GC-blindness BHJ: 1GB heap, 200MB threshold, 1 core
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    ;;
  BHJ2G)
    # Fixed-plugin comparison: 2g heap, 200MB threshold, 2 cores
    EXECUTOR_HEAP="2g"
    EXECUTOR_CORES=2
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    ;;
  SMJ)
    # GC-blindness SMJ control: 1GB heap, broadcast disabled, 1 core
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="-1"
    STATIC_THRESHOLD="-1"
    ;;
  BHJ2)
    # GC-blindness BHJ moderate: 1.2GB heap, 128MB threshold, 2 cores
    EXECUTOR_HEAP="1200m"
    EXECUTOR_CORES=2
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="128MB"
    STATIC_THRESHOLD="-1"
    ;;
  BHJ500)
    # Wider broadcast gate: 1GB heap, 500MB threshold (pulls in more BHJ candidates)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="500MB"
    STATIC_THRESHOLD="-1"
    ;;
  BHJ1200)
    # 1.2g heap retry for Q5: threshold kept at 200MB, single executor core (same base as BHJ)
    EXECUTOR_HEAP="1200m"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    ;;
  BHJ1G2C)
    # Tier 1+2 config C1: 1g heap, 200MB threshold, 2 cores (matches Q5 Round 4 history)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=2
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="800m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    ;;
  SEGMENTED4)
    # Segmented BHJ variant C, N=4 shards (same base as BHJ config)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    SEGMENTED=1
    NUM_SHARDS=4
    ;;
  SEGMENTED8)
    # Segmented BHJ variant C, N=8 shards (same base as BHJ config)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    SEGMENTED=1
    NUM_SHARDS=8
    ;;
  SEGMENTED16)
    # Segmented BHJ variant C, N=16 shards (same base as BHJ config)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    SEGMENTED=1
    NUM_SHARDS=16
    ;;
  SEGMENTED32)
    # Segmented BHJ variant C, N=32 shards (same base as BHJ config)
    EXECUTOR_HEAP="1g"
    EXECUTOR_CORES=1
    EXECUTOR_INSTANCES=2
    DRIVER_MEMORY="3g"
    MEMORY_OVERHEAD="1000m"
    THRESHOLD="200MB"
    STATIC_THRESHOLD="-1"
    SEGMENTED=1
    NUM_SHARDS=32
    ;;
  *)
    echo "ERROR: CONFIG must be A, B, BHJ, SMJ, BHJ2, BHJ500, BHJ1200, BHJ1G2C, BHJ2G, SEGMENTED4, SEGMENTED8, SEGMENTED16, or SEGMENTED32, got '$CONFIG'"
    exit 1
    ;;
esac

if [ -n "${MEMORY_OVERHEAD_OVERRIDE:-}" ]; then
  MEMORY_OVERHEAD="$MEMORY_OVERHEAD_OVERRIDE"
fi

# --- Benchmark-specific settings ---
case "$BENCHMARK" in
  tpcds)
    MAIN_CLASS="com.research.gcaware.TpcdsQueryRunner"
    DATA_BASE="${DATA_BASE_OVERRIDE:-file:/mnt/bench/tpcds-scale-$SCALE}"
    ;;
  tpch)
    MAIN_CLASS="com.research.gcaware.TpchQueryRunner"
    DATA_BASE="${DATA_BASE_OVERRIDE:-file:/mnt/bench/tpch-scale-$SCALE}"
    ;;
  *)
    echo "ERROR: Unknown benchmark '$BENCHMARK'. Use 'tpcds' or 'tpch'."
    exit 1
    ;;
esac

case "$SHUFFLE_MODE" in
  native)
    USE_CELEBORN=0
    ;;
  celeborn)
    USE_CELEBORN=1
    ;;
  *)
    echo "ERROR: Unknown SHUFFLE_MODE '$SHUFFLE_MODE'. Use 'native' or 'celeborn'."
    exit 1
    ;;
esac

CELEBORN_MASTER_ENDPOINTS="${CELEBORN_MASTER_ENDPOINTS:-}"
if [ "$USE_CELEBORN" -eq 1 ] && [ -z "$CELEBORN_MASTER_ENDPOINTS" ]; then
  echo "ERROR: CELEBORN_MASTER_ENDPOINTS must be set when SHUFFLE_MODE=celeborn"
  exit 1
fi

# Entropy AQE plugin: non-invasive AQE rule injected through Spark SQL extensions.
# Modes:
#   restore: under pressure, remove AQEShuffleReadExec and execute close to original shuffle partitions
#   dynamic: under pressure, recompute AQEShuffleReadExec partition specs with a smaller advisory target
ENTROPY_AQE="${ENTROPY_AQE:-0}"
ENTROPY_AQE_PLUGIN_JAR_URI="${ENTROPY_AQE_PLUGIN_JAR_URI:-s3a://spark-obj-storage/jars/entropy-aqe-3.5.8.jar}"
ENTROPY_AQE_MODE="${ENTROPY_AQE_MODE:-restore}"
ENTROPY_AQE_GC_PRESSURE_THRESHOLD="${ENTROPY_AQE_GC_PRESSURE_THRESHOLD:-0.10}"
ENTROPY_AQE_MIN_TARGET_FACTOR="${ENTROPY_AQE_MIN_TARGET_FACTOR:-0.25}"
ENTROPY_AQE_SENSITIVITY="${ENTROPY_AQE_SENSITIVITY:-1.0}"
ENTROPY_AQE_SPILL_PRESSURE_BYTES="${ENTROPY_AQE_SPILL_PRESSURE_BYTES:-512m}"
ENTROPY_AQE_EMA_ALPHA="${ENTROPY_AQE_EMA_ALPHA:-0.30}"
ENTROPY_AQE_LOG_EVERY_TASKS="${ENTROPY_AQE_LOG_EVERY_TASKS:-200}"

# JVM AQE plugin: JVM pressure-aware AQE rule injected through Spark SQL extensions.
JVM_AQE="${JVM_AQE:-0}"
JVM_AQE_PLUGIN_JAR_URI="${JVM_AQE_PLUGIN_JAR_URI:-s3a://spark-obj-storage/jars/jvm-aqe-plugin-3.5.8.jar}"
JVM_AQE_PRESSURE_THRESHOLD="${JVM_AQE_PRESSURE_THRESHOLD:-0.70}"
JVM_AQE_MODIFY_PLANS_ENABLED="${JVM_AQE_MODIFY_PLANS_ENABLED:-false}"
JVM_AQE_DECISION_LOG_ENABLED="${JVM_AQE_DECISION_LOG_ENABLED:-true}"
JVM_AQE_SNAPSHOT_STALE_THRESHOLD_MS="${JVM_AQE_SNAPSHOT_STALE_THRESHOLD_MS:-10000}"
JVM_AQE_COALESCE_ENABLED="${JVM_AQE_COALESCE_ENABLED:-true}"
JVM_AQE_COALESCE_MODIFY_PLANS_ENABLED="${JVM_AQE_COALESCE_MODIFY_PLANS_ENABLED:-false}"
JVM_AQE_COALESCE_MIN_TARGET_FACTOR="${JVM_AQE_COALESCE_MIN_TARGET_FACTOR:-0.25}"
JVM_AQE_COALESCE_SENSITIVITY="${JVM_AQE_COALESCE_SENSITIVITY:-1.0}"
JVM_AQE_JOIN_SELECTION_ENABLED="${JVM_AQE_JOIN_SELECTION_ENABLED:-true}"
JVM_AQE_JOIN_SELECTION_MODIFY_PLANS_ENABLED="${JVM_AQE_JOIN_SELECTION_MODIFY_PLANS_ENABLED:-false}"
JVM_AQE_BROADCAST_EXPANSION_FACTOR="${JVM_AQE_BROADCAST_EXPANSION_FACTOR:-7.0}"
JVM_AQE_BROADCAST_HEAP_SAFETY_FRACTION="${JVM_AQE_BROADCAST_HEAP_SAFETY_FRACTION:-0.60}"
JVM_AQE_EXECUTOR_SAMPLE_INTERVAL_MS="${JVM_AQE_EXECUTOR_SAMPLE_INTERVAL_MS:-1000}"
JVM_AQE_DRIVER_SAMPLE_INTERVAL_MS="${JVM_AQE_DRIVER_SAMPLE_INTERVAL_MS:-1000}"
JVM_AQE_RECENT_GC_WINDOW_MS="${JVM_AQE_RECENT_GC_WINDOW_MS:-30000}"
JVM_AQE_LOG_SNAPSHOTS="${JVM_AQE_LOG_SNAPSHOTS:-true}"

case "$ENTROPY_AQE_MODE" in
  restore|dynamic)
    ;;
  *)
    echo "ERROR: ENTROPY_AQE_MODE must be restore or dynamic, got '$ENTROPY_AQE_MODE'"
    exit 1
    ;;
esac

TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# --- Paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_LOGS_BASE_DIR="/var/spark-logs"
GC_LOGS_DIR="$SPARK_LOGS_BASE_DIR/gc-logs-raw"
EVENT_LOGS_DIR="file://$SPARK_LOGS_BASE_DIR/event-logs"
DRIVER_POD_TEMPLATE_FILE="${DRIVER_POD_TEMPLATE_FILE:-$SCRIPT_DIR/driver-pod-template.yaml}"
if [ ! -f "$DRIVER_POD_TEMPLATE_FILE" ] && \
    [ -f "$SCRIPT_DIR/../../research-claude/scripts/driver-pod-template.yaml" ]; then
  DRIVER_POD_TEMPLATE_FILE="$SCRIPT_DIR/../../research-claude/scripts/driver-pod-template.yaml"
fi
if [ ! -f "$DRIVER_POD_TEMPLATE_FILE" ]; then
  echo "ERROR: driver pod template not found: $DRIVER_POD_TEMPLATE_FILE"
  echo "       Set DRIVER_POD_TEMPLATE_FILE or place driver-pod-template.yaml next to this script."
  exit 1
fi

# --- Kubernetes ---
NAMESPACE="spark"
SPARK_MASTER_URL="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

WORKLOAD_JAR_URI="s3a://spark-obj-storage/jars/gc-aware-workloads-3.5.8.jar"
SEGMENTED_PLUGIN_JAR_URI="${SEGMENTED_PLUGIN_JAR_URI:-s3a://spark-obj-storage/jars/segmented-bhj-3.5.8.jar}"
AQE_FEEDBACK="${AQE_FEEDBACK:-0}"
AQE_FEEDBACK_PLUGIN_JAR_URI="${AQE_FEEDBACK_PLUGIN_JAR_URI:-s3a://spark-obj-storage/jars/aqe-feedback-plugin-3.5.8.jar}"
AQE_FEEDBACK_MODIFY_PLANS_ENABLED="${AQE_FEEDBACK_MODIFY_PLANS_ENABLED:-false}"
AQE_FEEDBACK_ACTION="${AQE_FEEDBACK_ACTION:-split}"
AQE_FEEDBACK_GC_TIME_RATIO_THRESHOLD="${AQE_FEEDBACK_GC_TIME_RATIO_THRESHOLD:-0.30}"
AQE_FEEDBACK_SPILL_BYTES_THRESHOLD="${AQE_FEEDBACK_SPILL_BYTES_THRESHOLD:-128m}"
AQE_FEEDBACK_PRESSURED_TASK_FRACTION_THRESHOLD="${AQE_FEEDBACK_PRESSURED_TASK_FRACTION_THRESHOLD:-0.20}"
AQE_FEEDBACK_TARGET_POST_PRESSURE_BYTES="${AQE_FEEDBACK_TARGET_POST_PRESSURE_BYTES:-16m}"

# --- App config ---
EXPERIMENT_KIND="${EXPERIMENT_KIND:-pureaqe}"
if [ -n "${SEGMENTED:-}" ]; then
  EXPERIMENT_KIND="segmented"
fi
if [ "$AQE_FEEDBACK" = "1" ]; then
  EXPERIMENT_KIND="aqefeedback-${AQE_FEEDBACK_ACTION}"
fi
if [ "$ENTROPY_AQE" = "1" ]; then
  EXPERIMENT_KIND="entropy-${ENTROPY_AQE_MODE}"
fi
if [ "$JVM_AQE" = "1" ]; then
  EXPERIMENT_KIND="jvmaqe"
fi
RUN_ID="${EXPERIMENT_KIND}_${BENCHMARK}_${QUERY}_${CONFIG}"
if [ "$SHUFFLE_MODE" != "native" ]; then
  RUN_ID="${RUN_ID}_${SHUFFLE_MODE}"
fi
if [ -n "${RUN_ID_SUFFIX:-}" ]; then
  RUN_ID="${RUN_ID}_${RUN_ID_SUFFIX}"
fi

if [ "$SQL_MODE" = "1" ]; then
  provided_sql_sources=0
  [ -n "$SQL_TEXT" ] && provided_sql_sources=$((provided_sql_sources + 1))
  [ -n "$SQL_FILE" ] && provided_sql_sources=$((provided_sql_sources + 1))
  [ -n "$SQL_B64" ] && provided_sql_sources=$((provided_sql_sources + 1))
  if [ "$provided_sql_sources" -ne 1 ]; then
    echo "ERROR: SQL mode requires exactly one of SQL_TEXT, SQL_FILE, or SQL_B64"
    exit 1
  fi
  if [ -n "$SQL_TEXT" ]; then
    SQL_B64="$(printf '%s' "$SQL_TEXT" | base64 | tr -d '\n')"
    SQL_TEXT=""
  elif [ -n "$SQL_FILE" ] && [[ "$SQL_FILE" != s3a://* ]] && [[ "$SQL_FILE" != file://* ]]; then
    if [ ! -f "$SQL_FILE" ]; then
      echo "ERROR: SQL_FILE does not exist: $SQL_FILE"
      exit 1
    fi
    SQL_B64="$(base64 < "$SQL_FILE" | tr -d '\n')"
    SQL_FILE=""
  fi
fi

needs_s3a_submit_classpath() {
  [[ "$WORKLOAD_JAR_URI" == s3a://* ]] || \
    { [[ "$SQL_MODE" == "1" ]] && [[ "$SQL_RUNNER_URI" == s3a://* ]]; } || \
    { [[ "$SQL_MODE" == "1" ]] && [[ "$SQL_FILE" == s3a://* ]]; } || \
    [[ "$SEGMENTED_PLUGIN_JAR_URI" == s3a://* ]] || \
    { [[ "$AQE_FEEDBACK" == "1" ]] && [[ "$AQE_FEEDBACK_PLUGIN_JAR_URI" == s3a://* ]]; } || \
    { [[ "$ENTROPY_AQE" == "1" ]] && [[ "$ENTROPY_AQE_PLUGIN_JAR_URI" == s3a://* ]]; } || \
    { [[ "$JVM_AQE" == "1" ]] && [[ "$JVM_AQE_PLUGIN_JAR_URI" == s3a://* ]]; }
}

append_csv() {
  local current="$1"
  local addition="$2"
  if [ -z "$current" ]; then
    printf '%s' "$addition"
  else
    printf '%s,%s' "$current" "$addition"
  fi
}

ensure_s3a_submit_classpath() {
  if ! needs_s3a_submit_classpath; then
    return
  fi

  if [ -d "${SPARK_HOME:-}/jars" ] && find "$SPARK_HOME/jars" -maxdepth 1 -name 'hadoop-aws-*.jar' | grep -q .; then
    return
  fi

  local hadoop_aws_jar="${HADOOP_AWS_JAR:-$HOME/.m2/repository/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar}"
  local aws_bundle_jar="${AWS_SDK_BUNDLE_JAR:-$HOME/.m2/repository/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar}"
  if [ ! -f "$hadoop_aws_jar" ] || [ ! -f "$aws_bundle_jar" ]; then
    echo "ERROR: local spark-submit needs S3A support for s3a:// jars, but hadoop-aws/aws bundle jars were not found."
    echo "       Set HADOOP_AWS_JAR and AWS_SDK_BUNDLE_JAR, or use a SPARK_HOME distribution that includes hadoop-aws."
    exit 1
  fi

  export SPARK_DIST_CLASSPATH="$hadoop_aws_jar:$aws_bundle_jar${SPARK_DIST_CLASSPATH:+:$SPARK_DIST_CLASSPATH}"
  echo "  Submit S3A Classpath: enabled (hadoop-aws=$(basename "$hadoop_aws_jar"))"
}

ensure_s3a_submit_credentials() {
  if ! needs_s3a_submit_classpath; then
    return
  fi

  if [ -n "${AWS_ACCESS_KEY_ID:-${AWS_ACCESS_KEY:-}}" ] && \
      [ -n "${AWS_SECRET_ACCESS_KEY:-${AWS_SECRET_KEY:-}}" ]; then
    echo "  Submit S3A Credentials: using existing AWS env"
    return
  fi

  local mc_alias="${S3A_SUBMIT_MC_ALIAS:-spark}"
  if ! command -v mc >/dev/null 2>&1; then
    return
  fi

  local alias_json access_key secret_key
  alias_json="$(mc alias export "$mc_alias" 2>/dev/null || true)"
  access_key="$(printf '%s' "$alias_json" | sed -n 's/.*"accessKey":"\([^"]*\)".*/\1/p')"
  secret_key="$(printf '%s' "$alias_json" | sed -n 's/.*"secretKey":"\([^"]*\)".*/\1/p')"

  if [ -n "$access_key" ] && [ -n "$secret_key" ]; then
    export AWS_ACCESS_KEY_ID="$access_key"
    export AWS_SECRET_ACCESS_KEY="$secret_key"
    echo "  Submit S3A Credentials: loaded from mc alias '$mc_alias'"
  fi
}

LOCAL_SHUFFLE_READER_EFFECTIVE="${LOCAL_SHUFFLE_READER_ENABLED:-true}"
if { [ "$ENTROPY_AQE" = "1" ] || [ "$JVM_AQE" = "1" ]; } && [ -z "${LOCAL_SHUFFLE_READER_ENABLED+x}" ]; then
  LOCAL_SHUFFLE_READER_EFFECTIVE="false"
fi

# --- Print experiment config ---
echo "=============================================="
echo "GC Screening: $QUERY Config $CONFIG ($BENCHMARK)"
echo "=============================================="
echo "  Query:                $QUERY"
echo "  Config:               $CONFIG (threshold=$THRESHOLD)"
echo "  Executor Heap:        $EXECUTOR_HEAP"
echo "  Memory Overhead:      $MEMORY_OVERHEAD"
echo "  Executor Cores:       $EXECUTOR_CORES"
echo "  Executor Instances:   $EXECUTOR_INSTANCES"
echo "  Driver Memory:        $DRIVER_MEMORY"
echo "  AQE:                  enabled"
echo "  Coalesce Partitions:  true"
echo "  Skew Join:            false"
echo "  Local Shuffle Reader: ${LOCAL_SHUFFLE_READER_EFFECTIVE:-${LOCAL_SHUFFLE_READER_ENABLED:-true}}"
echo "  Shuffle Mode:         $SHUFFLE_MODE"
echo "  Celeborn Masters:     ${CELEBORN_MASTER_ENDPOINTS:-n/a}"
echo "  Benchmark:            $BENCHMARK SF$SCALE"
if [ "$SQL_MODE" = "1" ]; then
  echo "  Workload Mode:        generic SQL"
  echo "  SQL Runner:           $SQL_RUNNER_URI"
  echo "  SQL Summary:          $SQL_SUMMARY_BASE/$RUN_ID"
else
  echo "  Workload Mode:        workload jar"
  echo "  Workload Jar:         $WORKLOAD_JAR_URI"
fi
echo "  Data Base:            $DATA_BASE"
echo "  Driver Pod Template:  $DRIVER_POD_TEMPLATE_FILE"
if [ -n "${SEGMENTED:-}" ]; then
  echo "  Segmented Plugin:     enabled (N=${NUM_SHARDS:-4})"
else
  echo "  Segmented Plugin:     disabled"
fi
if [ "$AQE_FEEDBACK" = "1" ]; then
  echo "  AQE Feedback Plugin:  enabled (modifyPlans=$AQE_FEEDBACK_MODIFY_PLANS_ENABLED, action=$AQE_FEEDBACK_ACTION)"
else
  echo "  AQE Feedback Plugin:  disabled"
fi
if [ "$ENTROPY_AQE" = "1" ]; then
  echo "  Entropy AQE Plugin:   enabled (mode=$ENTROPY_AQE_MODE, threshold=$ENTROPY_AQE_GC_PRESSURE_THRESHOLD)"
  echo "  Entropy Target:       minFactor=$ENTROPY_AQE_MIN_TARGET_FACTOR sensitivity=$ENTROPY_AQE_SENSITIVITY spill=$ENTROPY_AQE_SPILL_PRESSURE_BYTES"
else
  echo "  Entropy AQE Plugin:   disabled"
fi
if [ "$JVM_AQE" = "1" ]; then
  echo "  JVM AQE Plugin:       enabled (pressure=$JVM_AQE_PRESSURE_THRESHOLD, modify=$JVM_AQE_MODIFY_PLANS_ENABLED)"
  echo "  JVM AQE Join Guard:   enabled=$JVM_AQE_JOIN_SELECTION_ENABLED modify=$JVM_AQE_JOIN_SELECTION_MODIFY_PLANS_ENABLED expansion=$JVM_AQE_BROADCAST_EXPANSION_FACTOR safety=$JVM_AQE_BROADCAST_HEAP_SAFETY_FRACTION"
  echo "  JVM AQE Coalesce:     enabled=$JVM_AQE_COALESCE_ENABLED modify=$JVM_AQE_COALESCE_MODIFY_PLANS_ENABLED minFactor=$JVM_AQE_COALESCE_MIN_TARGET_FACTOR"
else
  echo "  JVM AQE Plugin:       disabled"
fi
echo "  Run ID:               $RUN_ID"
echo "  Timestamp:            $TIMESTAMP"
echo "=============================================="

# --- Spark Submit ---
export SPARK_SCALA_VERSION=2.12
SPARK_SUBMIT_ARGS=(
  --master "k8s://$SPARK_MASTER_URL" \
  --deploy-mode cluster \
  --name "$RUN_ID" \
  --conf spark.app.name="$RUN_ID" \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.driver.podTemplateFile="$DRIVER_POD_TEMPLATE_FILE" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=gihong96/spark:3.5.8-s3a \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.hadoop.fs.s3a.endpoint=https://hel1.your-objectstorage.com \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf "spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID" \
  --conf "spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY" \
  --conf "spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID" \
  --conf "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOGS_DIR \
  --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+DisableExplicitGC -XX:+PrintCommandLineFlags -Xlog:gc*,gc+heap=info,gc+humongous=info:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-driver.log:utctime,uptime,level,tags:filecount=5,filesize=10m" \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+DisableExplicitGC -XX:+PrintCommandLineFlags -Xlog:gc*,gc+heap=info,gc+humongous=info:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-executor-{{EXECUTOR_ID}}.log:utctime,uptime,level,tags:filecount=5,filesize=10m" \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=local-path \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=50Gi \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/var/shuffle \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false \
  --conf spark.kubernetes.driver.ownPersistentVolumeClaim=true \
  --conf spark.kubernetes.driver.reusePersistentVolumeClaim=true \
  --conf spark.driver.memory=$DRIVER_MEMORY \
  --conf spark.driver.cores=2 \
  --conf spark.executor.instances=$EXECUTOR_INSTANCES \
  --conf spark.executor.memory=$EXECUTOR_HEAP \
  --conf spark.executor.cores=$EXECUTOR_CORES \
  --conf spark.executor.memoryOverhead=$MEMORY_OVERHEAD \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.mount.path=/mnt/bench \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.options.path=/mnt/bench \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.options.type=Directory \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.mount.path=/mnt/bench \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.options.path=/mnt/bench \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.options.type=Directory \
  --conf spark.kubernetes.node.selector.spark-data=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold="$THRESHOLD" \
  --conf spark.sql.autoBroadcastJoinThreshold="${STATIC_THRESHOLD:-$THRESHOLD}" \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=false \
  --conf spark.sql.adaptive.localShuffleReader.enabled="$LOCAL_SHUFFLE_READER_EFFECTIVE" \
  --conf spark.shuffle.compress=true \
  --conf spark.local.dir=/var/shuffle \
  --conf spark.task.maxFailures=4 \
  --conf spark.stage.maxConsecutiveAttempts=4 \
  --conf spark.excludeOnFailure.enabled=true \
  --conf spark.excludeOnFailure.task.maxTaskAttemptsPerNode=2 \
)

if [ "$SQL_MODE" != "1" ]; then
  SPARK_SUBMIT_ARGS+=(
    --class "$MAIN_CLASS"
  )
fi

if [ "$USE_CELEBORN" -eq 1 ]; then
  SPARK_SUBMIT_ARGS+=(
    --conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
    --conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO
    --conf spark.celeborn.master.endpoints="$CELEBORN_MASTER_ENDPOINTS"
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.sql.adaptive.localShuffleReader.enabled=false
  )
fi

SQL_EXTENSIONS_VALUE=""
SPARK_PLUGINS_VALUE=""
EXTRA_JARS_VALUE=""
if [ -n "${SEGMENTED:-}" ]; then
  EXTRA_JARS_VALUE="$(append_csv "$EXTRA_JARS_VALUE" "$SEGMENTED_PLUGIN_JAR_URI")"
  SQL_EXTENSIONS_VALUE="$(append_csv "$SQL_EXTENSIONS_VALUE" "org.apache.spark.gcaware.SegmentedJoinExtension")"
  SPARK_SUBMIT_ARGS+=(
    --conf spark.sql.join.segmented.enabled=true
    --conf spark.sql.join.segmented.numShards="${NUM_SHARDS:-4}"
  )
fi

if [ "$AQE_FEEDBACK" = "1" ]; then
  EXTRA_JARS_VALUE="$(append_csv "$EXTRA_JARS_VALUE" "$AQE_FEEDBACK_PLUGIN_JAR_URI")"
  SPARK_PLUGINS_VALUE="$(append_csv "$SPARK_PLUGINS_VALUE" "org.apache.spark.aqefeedback.AqeFeedbackPlugin")"
  SQL_EXTENSIONS_VALUE="$(append_csv "$SQL_EXTENSIONS_VALUE" "org.apache.spark.aqefeedback.AqeFeedbackExtensions")"
  SPARK_SUBMIT_ARGS+=(
    --conf spark.aqeFeedback.enabled=true
    --conf spark.aqeFeedback.modifyPlans.enabled="$AQE_FEEDBACK_MODIFY_PLANS_ENABLED"
    --conf spark.aqeFeedback.action="$AQE_FEEDBACK_ACTION"
    --conf spark.aqeFeedback.pressure.gcTimeRatioThreshold="$AQE_FEEDBACK_GC_TIME_RATIO_THRESHOLD"
    --conf spark.aqeFeedback.pressure.spillBytesThreshold="$AQE_FEEDBACK_SPILL_BYTES_THRESHOLD"
    --conf spark.aqeFeedback.pressure.taskFractionThreshold="$AQE_FEEDBACK_PRESSURED_TASK_FRACTION_THRESHOLD"
    --conf spark.aqeFeedback.targetPostPressureBytes="$AQE_FEEDBACK_TARGET_POST_PRESSURE_BYTES"
  )
fi

if [ "$ENTROPY_AQE" = "1" ]; then
  EXTRA_JARS_VALUE="$(append_csv "$EXTRA_JARS_VALUE" "$ENTROPY_AQE_PLUGIN_JAR_URI")"
  SPARK_PLUGINS_VALUE="$(append_csv "$SPARK_PLUGINS_VALUE" "org.apache.spark.entropy.EntropyAqePlugin")"
  SQL_EXTENSIONS_VALUE="$(append_csv "$SQL_EXTENSIONS_VALUE" "org.apache.spark.entropy.EntropyAqeExtensions")"
  SPARK_SUBMIT_ARGS+=(
    --conf spark.sql.adaptive.entropy.enabled=true
    --conf spark.sql.adaptive.entropy.mode="$ENTROPY_AQE_MODE"
    --conf spark.sql.adaptive.entropy.gcPressureThreshold="$ENTROPY_AQE_GC_PRESSURE_THRESHOLD"
    --conf spark.sql.adaptive.entropy.minTargetFactor="$ENTROPY_AQE_MIN_TARGET_FACTOR"
    --conf spark.sql.adaptive.entropy.sensitivity="$ENTROPY_AQE_SENSITIVITY"
    --conf spark.sql.adaptive.entropy.spillPressureBytes="$ENTROPY_AQE_SPILL_PRESSURE_BYTES"
    --conf spark.sql.adaptive.entropy.emaAlpha="$ENTROPY_AQE_EMA_ALPHA"
    --conf spark.sql.adaptive.entropy.logEveryTasks="$ENTROPY_AQE_LOG_EVERY_TASKS"
  )
fi

if [ "$JVM_AQE" = "1" ]; then
  EXTRA_JARS_VALUE="$(append_csv "$EXTRA_JARS_VALUE" "$JVM_AQE_PLUGIN_JAR_URI")"
  SPARK_PLUGINS_VALUE="$(append_csv "$SPARK_PLUGINS_VALUE" "org.apache.spark.jvmaqe.JvmAqePlugin")"
  SQL_EXTENSIONS_VALUE="$(append_csv "$SQL_EXTENSIONS_VALUE" "org.apache.spark.jvmaqe.JvmAqeExtensions")"
  SPARK_SUBMIT_ARGS+=(
    --conf spark.jvmAqe.enabled=true
    --conf spark.jvmAqe.executor.sampleIntervalMs="$JVM_AQE_EXECUTOR_SAMPLE_INTERVAL_MS"
    --conf spark.jvmAqe.driver.sampleIntervalMs="$JVM_AQE_DRIVER_SAMPLE_INTERVAL_MS"
    --conf spark.jvmAqe.recentGcWindowMs="$JVM_AQE_RECENT_GC_WINDOW_MS"
    --conf spark.jvmAqe.logSnapshots="$JVM_AQE_LOG_SNAPSHOTS"
    --conf spark.jvmAqe.sql.enabled=true
    --conf spark.jvmAqe.enabled="$JVM_AQE_MODIFY_PLANS_ENABLED"
    --conf spark.jvmAqe.sql.pressureThreshold="$JVM_AQE_PRESSURE_THRESHOLD"
    --conf spark.jvmAqe.sql.decisionLog.enabled="$JVM_AQE_DECISION_LOG_ENABLED"
    --conf spark.jvmAqe.sql.snapshotStaleThresholdMs="$JVM_AQE_SNAPSHOT_STALE_THRESHOLD_MS"
    --conf spark.jvmAqe.sql.coalesce.enabled="$JVM_AQE_COALESCE_ENABLED"
    --conf spark.jvmAqe.sql.coalesce.modifyPlans.enabled="$JVM_AQE_COALESCE_MODIFY_PLANS_ENABLED"
    --conf spark.jvmAqe.sql.coalesce.minTargetFactor="$JVM_AQE_COALESCE_MIN_TARGET_FACTOR"
    --conf spark.jvmAqe.sql.coalesce.sensitivity="$JVM_AQE_COALESCE_SENSITIVITY"
    --conf spark.jvmAqe.sql.joinSelection.enabled="$JVM_AQE_JOIN_SELECTION_ENABLED"
    --conf spark.jvmAqe.sql.joinSelection.modifyPlans.enabled="$JVM_AQE_JOIN_SELECTION_MODIFY_PLANS_ENABLED"
    --conf spark.jvmAqe.sql.joinSelection.broadcastExpansionFactor="$JVM_AQE_BROADCAST_EXPANSION_FACTOR"
    --conf spark.jvmAqe.sql.joinSelection.broadcastHeapSafetyFraction="$JVM_AQE_BROADCAST_HEAP_SAFETY_FRACTION"
  )
fi

if [ -n "$EXTRA_JARS_VALUE" ]; then
  SPARK_SUBMIT_ARGS+=(
    --jars "$EXTRA_JARS_VALUE"
  )
fi

if [ -n "$SPARK_PLUGINS_VALUE" ]; then
  SPARK_SUBMIT_ARGS+=(
    --conf spark.plugins="$SPARK_PLUGINS_VALUE"
  )
fi

if [ -n "$SQL_EXTENSIONS_VALUE" ]; then
  SPARK_SUBMIT_ARGS+=(
    --conf spark.sql.extensions="$SQL_EXTENSIONS_VALUE"
  )
fi

if [ "$SQL_MODE" = "1" ]; then
  SPARK_SUBMIT_ARGS+=(
    "$SQL_RUNNER_URI"
    --query-name "$QUERY"
    --benchmark "$BENCHMARK"
    --scale "$SCALE"
    --data-base "$DATA_BASE"
    --result-limit "$SQL_RESULT_LIMIT"
    --summary-path "$SQL_SUMMARY_BASE/$RUN_ID"
  )
  if [ -n "$SQL_B64" ]; then
    SPARK_SUBMIT_ARGS+=(--sql-b64 "$SQL_B64")
  elif [ -n "$SQL_FILE" ]; then
    SPARK_SUBMIT_ARGS+=(--sql-file "$SQL_FILE")
  else
    echo "ERROR: SQL mode reached submit without SQL_B64 or SQL_FILE"
    exit 1
  fi
else
  SPARK_SUBMIT_ARGS+=(
    "$WORKLOAD_JAR_URI" \
    "$QUERY" \
    "$SCALE" \
    "$DATA_BASE"
  )
fi

set +e
ensure_s3a_submit_classpath
ensure_s3a_submit_credentials
"$SPARK_HOME"/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}"
EXIT_CODE=$?
set -e

# Check actual driver pod status (spark-submit may return 0 even on driver failure)
# Note: Spark normalizes app name to lowercase with hyphens for K8s labels
NORMALIZED_RUN_ID=$(echo "$RUN_ID" | tr '[:upper:]_' '[:lower:]-')
DRIVER_POD=$(kubectl get pods -n spark --no-headers -l "spark-app-name=$NORMALIZED_RUN_ID" 2>/dev/null | grep driver || true)
DRIVER_POD=$(echo "$DRIVER_POD" | awk '{print $1}' | head -1)
if [ -n "$DRIVER_POD" ]; then
  POD_STATUS=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
  if [ -n "$POD_STATUS" ] && [ "$POD_STATUS" != "0" ]; then
    EXIT_CODE="$POD_STATUS"
  fi
fi

# Extract application ID from pod labels
APP_ID=""
if [ -n "$DRIVER_POD" ]; then
  APP_ID=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.metadata.labels.spark-app-selector}' 2>/dev/null || true)
fi

echo ""
echo "=============================================="
echo "Screening Complete: $QUERY Config $CONFIG"
echo "=============================================="
echo "  Exit Code:    $EXIT_CODE"
echo "  App ID:       ${APP_ID:-unknown}"
echo "  Run ID:       $RUN_ID"
echo "  Timestamp:    $TIMESTAMP"
echo "=============================================="

# Machine-parseable output (now includes app_id)
echo "RESULT:$BENCHMARK:$QUERY:$CONFIG:$THRESHOLD:$EXIT_CODE:$TIMESTAMP:${APP_ID:-unknown}"

# --- Cleanup: delete driver pod so ownerReference cascades to OnDemand shuffle PVCs ---
# Event/GC logs live on NFS PVC (spark-logs-pvc) so they survive this deletion.
# App ID already captured above; no information loss.
if [ -n "$DRIVER_POD" ] && [ "${KEEP_DRIVER_POD:-0}" != "1" ]; then
  echo "Cleaning up driver pod $DRIVER_POD (cascades to shuffle PVCs)..."
  kubectl delete pod "$DRIVER_POD" -n spark --wait=false 2>/dev/null || true
fi
