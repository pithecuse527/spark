#!/usr/bin/env bash
# Run a single query under all four planner modes and record per-run metrics.
# Repeats N times, drops max/min, leaves raw event logs for downstream analysis.
#
# Usage:
#   QUERY=q3 BENCHMARK=tpcds SF=1000 REPS=4 ./10_run_oracle_sweep.sh

set -euo pipefail

QUERY="${QUERY:?set QUERY (e.g. q3)}"
BENCHMARK="${BENCHMARK:-tpcds}"
SF="${SF:-1000}"
REPS="${REPS:-4}"
CLUSTER_EXECUTORS="${CLUSTER_EXECUTORS:-32}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_HOME="$(cd "${ROOT}/../.." && pwd)"
RESULTS="${ROOT}/results/oracle_$(date +%Y%m%d_%H%M%S)_${BENCHMARK}_${QUERY}_n${CLUSTER_EXECUTORS}"
mkdir -p "${RESULTS}/event-logs"

K8S_MASTER="${K8S_MASTER:?set K8S_MASTER}"
NS="${NS:-spark}"
IMAGE="${IMAGE:?set IMAGE=registry/spark:tag}"

MODES=(baseline force-bhj force-shj force-smj)

for MODE in "${MODES[@]}"; do
  for REP in $(seq 1 "${REPS}"); do
    NAME="${BENCHMARK}-${QUERY}-${MODE}-r${REP}"
    EVT="${RESULTS}/event-logs/${NAME}"
    mkdir -p "${EVT}"
    echo "[run] ${NAME}"

    "${SPARK_HOME}"/bin/spark-submit \
      --master "${K8S_MASTER}" \
      --deploy-mode cluster \
      --name "${NAME}" \
      --class research.esem.QueryRunner \
      --properties-file "${ROOT}/configs/spark-${MODE}.conf" \
      --conf "spark.executor.instances=${CLUSTER_EXECUTORS}" \
      --conf "spark.kubernetes.namespace=${NS}" \
      --conf "spark.kubernetes.container.image=${IMAGE}" \
      --conf "spark.kubernetes.executor.podTemplateFile=${ROOT}/k8s/executor-pod-template.yaml" \
      --conf "spark.eventLog.enabled=true" \
      --conf "spark.eventLog.dir=file:${EVT}" \
      "${ROOT}/benchmarks/${BENCHMARK}/runner.jar" \
      --benchmark="${BENCHMARK}" --scaleFactor="${SF}" --query="${QUERY}"

    # Drop OS caches before the next run (sudo on the host, via DaemonSet or ssh).
    # Adjust to the cluster's mechanism.
    # ./drop_caches.sh
  done
done

echo
echo "raw event logs: ${RESULTS}/event-logs"
echo "next: ./30_collect.sh ${RESULTS}"
