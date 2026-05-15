#!/usr/bin/env bash
# Attach perf on the host to a running Spark executor pod's JVM.
# Captures LLC-loads/LLC-load-misses/instructions/cycles for a duration.
#
# Usage:
#   ./20_perf_attach.sh <pod-name> [duration_sec=60] [out_dir=results/perf]
#
# Requires sudo on the host running the pod.

set -euo pipefail

POD="${1:?usage: $0 <pod> [duration] [out_dir]}"
DURATION="${2:-60}"
OUT_DIR="${3:-$(cd "$(dirname "$0")/../results" && pwd)/perf}"
TS="$(date +%Y%m%d_%H%M%S)"

mkdir -p "${OUT_DIR}"

NODE="$(kubectl get pod "${POD}" -o jsonpath='{.spec.nodeName}')"
CONTAINER_ID="$(kubectl get pod "${POD}" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's|.*://||')"

if [[ -z "${NODE}" || -z "${CONTAINER_ID}" ]]; then
  echo "could not resolve node/container for pod ${POD}" >&2
  exit 1
fi

echo "[perf] pod=${POD} node=${NODE} cid=${CONTAINER_ID:0:12} duration=${DURATION}s"

# Run perf on the host. Assumes ssh key into the node as a user with sudo.
# Adjust the SSH user / runtime (containerd vs crictl) as needed.
ssh -o StrictHostKeyChecking=no "${NODE}" "
  set -e
  PID=\$(sudo crictl inspect ${CONTAINER_ID} 2>/dev/null | grep -m1 '\"pid\":' | awk '{print \$2}' | tr -d ',') || true
  if [ -z \"\$PID\" ]; then
    PID=\$(pgrep -f 'spark-kubernetes-executor' | head -1)
  fi
  echo \"[perf] target PID=\$PID\"
  sudo perf stat -e LLC-loads,LLC-load-misses,L1-dcache-loads,L1-dcache-load-misses,instructions,cycles,branch-misses \
    --per-thread -p \$PID -- sleep ${DURATION} 2> /tmp/perf_${TS}.txt
  cat /tmp/perf_${TS}.txt
" | tee "${OUT_DIR}/perf_${POD}_${TS}.txt"

echo
echo "wrote ${OUT_DIR}/perf_${POD}_${TS}.txt"
