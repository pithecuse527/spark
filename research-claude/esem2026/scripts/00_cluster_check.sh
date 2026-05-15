#!/usr/bin/env bash
# Collect per-node CPU cache info and perf capability on a k8s cluster.
# Run from a workstation with kubectl context set to the experiment cluster.
#
# Output: results/cluster_check_<timestamp>.txt

set -euo pipefail

OUT_DIR="$(cd "$(dirname "$0")/../results" && pwd)"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="${OUT_DIR}/cluster_check_${TS}.txt"

mkdir -p "${OUT_DIR}"

NODES=$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}')
if [[ -z "${NODES}" ]]; then
  echo "no nodes found via kubectl" >&2
  exit 1
fi

{
  echo "=== cluster check ${TS} ==="
  echo
  for NODE in ${NODES}; do
    echo "--- node: ${NODE} ---"
    # Use a debug pod (kubectl debug node/...) for cache info + perf paranoid.
    kubectl debug "node/${NODE}" -it --image=busybox -- chroot /host sh -c '
      echo "[cpu model]";   grep -m1 "model name" /proc/cpuinfo;
      echo "[lscpu caches]"; lscpu | grep -E "cache|Socket|NUMA|Core" | sed "s/^/  /";
      echo "[L3 per socket bytes]";
      for f in /sys/devices/system/cpu/cpu0/cache/index*/size; do
        idx="$(dirname "$f")";
        lvl=$(cat "$idx/level"); typ=$(cat "$idx/type"); sz=$(cat "$f");
        echo "  L${lvl} ${typ}: ${sz}";
      done;
      echo "[perf_event_paranoid]"; cat /proc/sys/kernel/perf_event_paranoid;
      echo "[kptr_restrict]";       cat /proc/sys/kernel/kptr_restrict;
      echo "[transparent_hugepage]"; cat /sys/kernel/mm/transparent_hugepage/enabled;
    ' 2>/dev/null || echo "  kubectl debug failed for ${NODE}"
    echo
  done
} | tee "${OUT}"

echo
echo "wrote ${OUT}"
