#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# GC Screening Experiment Script — one Spark run on the k3s `spark` namespace.
#
# Named flags (order-independent; every knob is explicit — no magic presets):
#   --query <q>        REQUIRED. e.g. q64, q38, q9
#   --gc <gc>          default g1.  HotSpot: g1 | zgc(=generational) | shen
#                                    OpenJ9: gencon | balanced | optthruput | optavgpause
#   --bench <b>        default tpcds.  tpcds | tpch
#   --scale <n>        default 200
#   --aqe <bool>       default true.  true | false  (false for GC-mechanism isolation)
#   --heap <size>      default 2g.    executor heap (spark.executor.memory)
#   --cores <n>        default 2      spark.executor.cores
#   --instances <n>    default 2      spark.executor.instances
#   --driver-mem <sz>  default 4g
#   --overhead <sz>    default 512m   spark.executor.memoryOverhead
#   --broadcast <v>    default off.   "off" (=-1, force SMJ) | a size like 128MB
#   --region <sz>      default "".    G1 only: -XX:G1HeapRegionSize (e.g. 8m)
#   --data-base <uri>  default s3a://spark-obj-storage
#   DRY_RUN=1 (env)    print resolved image + JVM opts and exit without submitting
#
# Pod / run name = {bench}-{query}-{scale}-{gc}-{aqe}-{heap}-{cores}[-r<region>]  (lowercased).
#
# Examples:
#   ./run-screening.sh --query q38 --gc shen --heap 4g --aqe false
#   ./run-screening.sh --query q9 --bench tpch --gc zgc --heap 2g --broadcast off --aqe false
#   ./run-screening.sh --query q64 --gc g1 --heap 2g --region 8m --aqe false
# =============================================================================

# --- defaults ---
QUERY=""; GC="g1"; BENCHMARK="tpcds"; SCALE="200"; AQE_ENABLED="true"
HEAP="2g"; CORES="2"; INSTANCES="2"; DRIVER_MEMORY="4g"; OVERHEAD="512m"
BROADCAST="off"; REGION=""; DATA_BASE="s3a://spark-obj-storage"

while [ $# -gt 0 ]; do
  case "$1" in
    --query)        QUERY="${2:?}"; shift ;;
    --gc)           GC="${2:?}"; shift ;;
    --bench|--benchmark) BENCHMARK="${2:?}"; shift ;;
    --scale)        SCALE="${2:?}"; shift ;;
    --aqe)          AQE_ENABLED="${2:?}"; shift ;;
    --heap)         HEAP="${2:?}"; shift ;;
    --cores)        CORES="${2:?}"; shift ;;
    --instances)    INSTANCES="${2:?}"; shift ;;
    --driver-mem)   DRIVER_MEMORY="${2:?}"; shift ;;
    --overhead)     OVERHEAD="${2:?}"; shift ;;
    --broadcast)    BROADCAST="${2:?}"; shift ;;
    --region)       REGION="${2:?}"; shift ;;
    --data-base)    DATA_BASE="${2:?}"; shift ;;
    -h|--help)      sed -n '4,33p' "$0"; exit 0 ;;
    *) echo "ERROR: unknown arg '$1' (see --help)"; exit 2 ;;
  esac
  shift
done
[ -n "$QUERY" ] || { echo "ERROR: --query is required (see --help)"; exit 2; }
case "$AQE_ENABLED" in true|false) ;; *) echo "ERROR: --aqe must be true|false, got '$AQE_ENABLED'"; exit 2 ;; esac

# --- Benchmark -> main class + data location ---
case "$BENCHMARK" in
  tpcds) MAIN_CLASS="com.research.gcaware.TpcdsQueryRunner"; DATA_LOCATION="${DATA_BASE%/}/tpcds-scale-$SCALE" ;;
  tpch)  MAIN_CLASS="com.research.gcaware.TpchQueryRunner";  DATA_LOCATION="${DATA_BASE%/}/tpch-scale-$SCALE" ;;
  *) echo "ERROR: --bench must be tpcds|tpch, got '$BENCHMARK'"; exit 2 ;;
esac

# --- Broadcast threshold: "off" => -1 (force SMJ); else a size like 128MB ---
case "$BROADCAST" in off|-1) BC="-1" ;; *) BC="$BROADCAST" ;; esac

# --- GC -> JVM family, image, GC flags ---
#   HotSpot collectors (g1/zgc/shen) -> Temurin image, -Xlog GC logging.
#   OpenJ9 policies (gencon/balanced/optthruput/optavgpause) -> Semeru image, -Xverbosegclog.
IMAGE="gihong96/spark-screening:v1"
JVM_FAMILY="hotspot"
case "$GC" in
  g1)
    GC_OPTS="-XX:+UseG1GC"
    GC_LOG_SEL="gc*,gc+heap=debug,gc+age=trace,gc+humongous=debug"
    [ -n "$REGION" ] && GC_OPTS="$GC_OPTS -XX:G1HeapRegionSize=$REGION"
    ;;
  zgc)
    GC_OPTS="-XX:+UseZGC -XX:+ZGenerational"
    GC_LOG_SEL="gc*,gc+heap=debug"
    ;;
  shen|shenandoah)
    GC="shen"
    GC_OPTS="-XX:+UseShenandoahGC"
    GC_LOG_SEL="gc*,gc+ergo=debug"
    ;;
  gencon|balanced|optthruput|optavgpause)
    JVM_FAMILY="openj9"
    IMAGE="gihong96/spark-screening:semeru-v1"
    GC_OPTS="-Xgcpolicy:$GC"
    # NOTE: --region is HotSpot-only and ignored on OpenJ9 (balanced would use -Xgc:regionSize).
    ;;
  *)
    echo "ERROR: --gc must be: g1 zgc shen (HotSpot) | gencon balanced optthruput optavgpause (OpenJ9), got '$GC'"
    exit 2 ;;
esac

# --- Run / pod name: {bench}-{query}-{scale}-{gc}-{aqe}-{heap}-{cores}[-r<region>], RFC1123-safe ---
RUN_ID="${BENCHMARK}-${QUERY}-${SCALE}-${GC}-${AQE_ENABLED}-${HEAP}-${CORES}"
[ -n "$REGION" ] && RUN_ID="${RUN_ID}-r${REGION}"
RUN_ID="$(printf '%s' "$RUN_ID" | tr '[:upper:]_' '[:lower:]-')"

# Spark downloads the remote primary jar into the container CWD (READ-ONLY /work in the
# image) -> AccessDeniedException. Fix via pod template: point workingDir at writable /tmp.
# (Do NOT set runAsUser/fsGroup root — that breaks the projected SA token -> 401 from K8s API.)
POD_TEMPLATE="$(mktemp -t spark-podtemplate.XXXXXX)"
trap 'rm -f "$POD_TEMPLATE"' EXIT
cat > "$POD_TEMPLATE" <<'YAML'
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: spark-kubernetes-driver
      workingDir: /tmp
YAML

TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# --- Paths / k8s ---
SPARK_LOGS_BASE_DIR="/var/spark-logs"
GC_LOGS_DIR="$SPARK_LOGS_BASE_DIR/gc-logs-raw"
EVENT_LOGS_DIR="s3a://spark-obj-storage/event-logs"
NAMESPACE="spark"
SPARK_MASTER_URL="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
WORKLOAD_JAR_URI="s3a://spark-obj-storage/jars/sql-workloads-1.0.jar"
OBJ_STORAGE_ENDPOINT="${OBJ_STORAGE_ENDPOINT:-https://hel1.your-objectstorage.com}"

# --- Print experiment config ---
echo "=============================================="
echo "GC Screening: $RUN_ID"
echo "=============================================="
echo "  Query:              $QUERY ($BENCHMARK SF$SCALE)"
echo "  GC:                 $GC  [$JVM_FAMILY]  $GC_OPTS"
echo "  Image:              $IMAGE"
echo "  AQE:                $AQE_ENABLED"
echo "  Heap / overhead:    $HEAP / $OVERHEAD"
echo "  Cores x instances:  $CORES x $INSTANCES"
echo "  Driver memory:      $DRIVER_MEMORY"
echo "  Broadcast thr:      $BC  (off=-1)"
echo "  G1 region size:     ${REGION:-<ergonomic>}"
echo "  Data location:      $DATA_LOCATION"
echo "  Timestamp:          $TIMESTAMP"
echo "=============================================="

# --- Per-JVM GC logging flags (HotSpot -Xlog vs OpenJ9 -Xverbosegclog) ---
if [ "$JVM_FAMILY" = "openj9" ]; then
  DRIVER_JAVA_OPTS="$GC_OPTS -Xverbosegclog:$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-driver.log"
  EXECUTOR_JAVA_OPTS="$GC_OPTS -Xverbosegclog:$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-executor-{{EXECUTOR_ID}}.log"
else
  DRIVER_JAVA_OPTS="$GC_OPTS -XX:+PrintCommandLineFlags -Xlog:$GC_LOG_SEL:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-driver.log:utctime,uptime,level,tags:filecount=1,filesize=20m"
  EXECUTOR_JAVA_OPTS="$GC_OPTS -XX:+PrintCommandLineFlags -Xlog:$GC_LOG_SEL:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-executor-{{EXECUTOR_ID}}.log:utctime,uptime,level,tags:filecount=10,filesize=20m"
fi

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "DRY_RUN: image=$IMAGE  family=$JVM_FAMILY  run_id=$RUN_ID"
  echo "DRY_RUN: heap=$HEAP cores=$CORES instances=$INSTANCES driver_mem=$DRIVER_MEMORY overhead=$OVERHEAD broadcast=$BC aqe=$AQE_ENABLED"
  echo "DRY_RUN: driver_opts=$DRIVER_JAVA_OPTS"
  echo "DRY_RUN: exec_opts=$EXECUTOR_JAVA_OPTS"
  exit 0
fi

# --- Spark Submit ---
"$SPARK_HOME"/bin/spark-submit \
  --master "k8s://$SPARK_MASTER_URL" \
  --deploy-mode cluster \
  --name "$RUN_ID" \
  --class "$MAIN_CLASS" \
  --conf spark.app.name="$RUN_ID" \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=$IMAGE \
  --conf spark.kubernetes.driver.podTemplateFile="$POD_TEMPLATE" \
  --conf spark.kubernetes.executor.podTemplateFile="$POD_TEMPLATE" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOGS_DIR \
  --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTS" \
  --conf "spark.executor.extraJavaOptions=$EXECUTOR_JAVA_OPTS" \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
  --conf spark.driver.memory=$DRIVER_MEMORY \
  --conf spark.driver.cores=2 \
  --conf spark.executor.instances=$INSTANCES \
  --conf spark.executor.memory=$HEAP \
  --conf spark.executor.cores=$CORES \
  --conf spark.executor.memoryOverhead=$OVERHEAD \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.endpoint="$OBJ_STORAGE_ENDPOINT" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.mount.path=/mnt/bench \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.options.path=/mnt/bench \
  --conf spark.kubernetes.executor.volumes.hostPath.bench.options.type=Directory \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.mount.path=/mnt/bench \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.options.path=/mnt/bench \
  --conf spark.kubernetes.driver.volumes.hostPath.bench.options.type=Directory \
  --conf spark.kubernetes.node.selector.spark-data=true \
  --conf spark.sql.adaptive.enabled=$AQE_ENABLED \
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold="$BC" \
  --conf spark.sql.autoBroadcastJoinThreshold="$BC" \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=false \
  --conf spark.shuffle.compress=true \
  "$WORKLOAD_JAR_URI" \
  "$QUERY" \
  "$SCALE" \
  "$DATA_LOCATION"

EXIT_CODE=$?

# Driver pod status (spark-submit may return 0 even on driver failure). RUN_ID is already
# the normalized lowercase-hyphen form Spark uses for the spark-app-name label.
DRIVER_POD=$(kubectl get pods -n spark --no-headers -l "spark-app-name=$RUN_ID" 2>/dev/null | grep driver || true)
DRIVER_POD=$(echo "$DRIVER_POD" | awk '{print $1}' | head -1)
if [ -n "$DRIVER_POD" ]; then
  POD_STATUS=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
  if [ -n "$POD_STATUS" ] && [ "$POD_STATUS" != "0" ]; then EXIT_CODE="$POD_STATUS"; fi
fi
APP_ID=""
[ -n "$DRIVER_POD" ] && APP_ID=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.metadata.labels.spark-app-selector}' 2>/dev/null || true)

echo ""
echo "=============================================="
echo "Screening Complete: $RUN_ID"
echo "  Exit Code:    $EXIT_CODE"
echo "  App ID:       ${APP_ID:-unknown}"
echo "  Timestamp:    $TIMESTAMP"
echo "=============================================="

# Machine-parseable (key=value so the parser is robust to field changes).
echo "RESULT exit=$EXIT_CODE appid=${APP_ID:-unknown} runid=$RUN_ID gc=$GC bench=$BENCHMARK query=$QUERY ts=$TIMESTAMP"
