#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Collect post-run artifacts for a run-parallel.sh batch.
#
# Inputs:
#   - research-related/logs/parallel-runs/<timestamp>/summary.tsv
#
# Outputs:
#   - <batch-dir>/validity.tsv
#   - research-related/logs/gc-logs-raw/<batch-name>/raw GC logs copied from PVC
#   - <local-gc-dir>/manifest.tsv
#
# This replaces the previous ad hoc "curl Spark History + kubectl cp GC logs"
# workflow so the experiment can be reproduced from another submit host.
# =============================================================================

HERE="$(cd "$(dirname "$0")" && pwd)"
RESEARCH_ROOT="$(cd "$HERE/../.." && pwd)"
DEFAULT_PARALLEL_DIR="$RESEARCH_ROOT/logs/parallel-runs"
DEFAULT_GC_LOCAL_BASE="$RESEARCH_ROOT/logs/gc-logs-raw"

NAMESPACE="spark"
HISTORY_SERVICE="svc/spark-hs-spark-history-server"
HISTORY_PORT="18080"
HISTORY_URL=""
GC_POD=""
GC_POD_PATTERN="gc-analyzer-jvm-gc-logs-analyzer"
REMOTE_GC_DIR="/gc-logs-raw"
LOCAL_GC_DIR=""
SKIP_VALIDITY=0
SKIP_GC_COPY=0
BATCH_ARG=""

usage() {
  cat <<'EOF'
Usage:
  ./collect-run-artifacts.sh <batch-dir-or-timestamp> [options]

Examples:
  ./collect-run-artifacts.sh 20260701-120724
  ./collect-run-artifacts.sh ../../logs/parallel-runs/20260701-120724
  ./collect-run-artifacts.sh 20260701-120724 --history-url http://127.0.0.1:18080

Options:
  --namespace <ns>          Kubernetes namespace. Default: spark
  --history-url <url>       Existing Spark History API base URL. If omitted,
                            this script starts kubectl port-forward.
  --history-service <svc>   Port-forward target. Default: svc/spark-hs-spark-history-server
  --history-port <port>     Local port for Spark History. Default: 18080
  --gc-pod <pod>            Pod that mounts the GC-log PVC at --remote-gc-dir.
                            If omitted, a Running pod whose name contains
                            --gc-pod-pattern is selected.
  --gc-pod-pattern <text>   Name substring used to find the analyzer pod.
                            Default: gc-analyzer-jvm-gc-logs-analyzer
  --remote-gc-dir <path>    GC log dir inside analyzer pod. Default: /gc-logs-raw
  --local-gc-dir <path>     Local output dir for copied GC logs. Default:
                            research-related/logs/gc-logs-raw/<batch-name>
  --skip-validity           Do not write validity.tsv
  --skip-gc-copy            Do not copy raw GC logs
  -h, --help                Show this help

Required commands:
  kubectl, curl, jq, awk, sed

Environment:
  KUBECONFIG must point at the cluster config unless your kubectl context is
  already configured.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --namespace) NAMESPACE="${2:?--namespace needs a value}"; shift ;;
    --history-url) HISTORY_URL="${2:?--history-url needs a value}"; shift ;;
    --history-service) HISTORY_SERVICE="${2:?--history-service needs a value}"; shift ;;
    --history-port) HISTORY_PORT="${2:?--history-port needs a value}"; shift ;;
    --gc-pod) GC_POD="${2:?--gc-pod needs a value}"; shift ;;
    --gc-pod-pattern) GC_POD_PATTERN="${2:?--gc-pod-pattern needs a value}"; shift ;;
    --remote-gc-dir) REMOTE_GC_DIR="${2:?--remote-gc-dir needs a value}"; shift ;;
    --local-gc-dir) LOCAL_GC_DIR="${2:?--local-gc-dir needs a value}"; shift ;;
    --skip-validity) SKIP_VALIDITY=1 ;;
    --skip-gc-copy) SKIP_GC_COPY=1 ;;
    -h|--help) usage; exit 0 ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      if [ -n "$BATCH_ARG" ]; then
        die "only one batch dir/timestamp is allowed"
      fi
      BATCH_ARG="$1"
      ;;
  esac
  shift
done

[ -n "$BATCH_ARG" ] || { usage >&2; exit 2; }

if [ -d "$BATCH_ARG" ]; then
  BATCH_DIR="$(cd "$BATCH_ARG" && pwd)"
elif [ -d "$DEFAULT_PARALLEL_DIR/$BATCH_ARG" ]; then
  BATCH_DIR="$(cd "$DEFAULT_PARALLEL_DIR/$BATCH_ARG" && pwd)"
else
  die "batch directory not found: $BATCH_ARG"
fi

SUMMARY="$BATCH_DIR/summary.tsv"
[ -f "$SUMMARY" ] || die "missing summary.tsv in $BATCH_DIR"
BATCH_NAME="$(basename "$BATCH_DIR")"
[ -n "$LOCAL_GC_DIR" ] || LOCAL_GC_DIR="$DEFAULT_GC_LOCAL_BASE/$BATCH_NAME"

need_cmd awk
need_cmd sed
need_cmd curl
need_cmd jq
need_cmd kubectl

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || die "cannot reach namespace '$NAMESPACE'"

PF_PID=""
PF_LOG=""

cleanup() {
  if [ -n "$PF_PID" ] && kill -0 "$PF_PID" >/dev/null 2>&1; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
  [ -n "$PF_LOG" ] && rm -f "$PF_LOG"
}
trap cleanup EXIT

start_history_forward() {
  if [ -n "$HISTORY_URL" ]; then
    return
  fi

  PF_LOG="$(mktemp -t spark-history-port-forward.XXXXXX)"
  kubectl port-forward -n "$NAMESPACE" "$HISTORY_SERVICE" "$HISTORY_PORT:18080" \
    >"$PF_LOG" 2>&1 &
  PF_PID="$!"
  HISTORY_URL="http://127.0.0.1:$HISTORY_PORT"

  local i
  for i in $(seq 1 40); do
    if curl -fsS "$HISTORY_URL/api/v1/applications?limit=1" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$PF_PID" >/dev/null 2>&1; then
      sed 's/^/  /' "$PF_LOG" >&2 || true
      die "Spark History port-forward exited early"
    fi
    sleep 0.5
  done

  sed 's/^/  /' "$PF_LOG" >&2 || true
  die "Spark History API did not become reachable at $HISTORY_URL"
}

spec_value() {
  local flag="$1"
  local spec="$2"
  sed -n "s/.*$flag \([^ ]*\).*/\1/p" <<< "$spec"
}

write_validity() {
  start_history_forward

  local out="$BATCH_DIR/validity.tsv"
  local tmp
  tmp="$(mktemp -t validity.XXXXXX)"

  printf "idx\tstate\tdriver_exit\tgc\theap\tregion\tdriver_mem\texpected_executors\tappid\thistory_available\tcompleted\tduration_ms\texecutor_count\tremoved\toomkilled\texecutor_failed\texecutor_completed\texcluded\tstage_count\tnon_complete_stages\tstage_failed\tstage_killed\tmem_spill\tdisk_spill\tclean\n" > "$tmp"

  tail -n +2 "$SUMMARY" | sort -t$'\t' -n -k1,1 | while IFS=$'\t' read -r idx state submit driver spec appid; do
    local gc heap region driver_mem instances
    gc="$(spec_value "--gc" "$spec")"
    heap="$(spec_value "--heap" "$spec")"
    region="$(spec_value "--region" "$spec")"
    driver_mem="$(spec_value "--driver-mem" "$spec")"
    instances="$(spec_value "--instances" "$spec")"
    [ -n "$region" ] || region="NA"
    [ -n "$driver_mem" ] || driver_mem="NA"
    [ -n "$instances" ] || instances="2"

    if app_json="$(curl -fsS "$HISTORY_URL/api/v1/applications/$appid" 2>/dev/null)"; then
      local completed duration exec_json stage_json
      local executor_count removed oomkilled executor_failed executor_completed excluded
      local stage_count non_complete stage_failed stage_killed mem_spill disk_spill clean

      completed="$(jq -r '.attempts[0].completed // "NA"' <<< "$app_json")"
      duration="$(jq -r '.attempts[0].duration // "NA"' <<< "$app_json")"
      exec_json="$(curl -fsS "$HISTORY_URL/api/v1/applications/$appid/allexecutors")"
      IFS=$'\t' read -r executor_count removed oomkilled executor_failed executor_completed excluded <<< "$(
        jq -r '[.[] | select(.id != "driver")] as $e |
          [($e|length),
           ($e|map(select(.isActive == false))|length),
           ($e|map(select((.removeReason // "")|test("OOMKilled|exit code: 137")))|length),
           (($e|map(.failedTasks // 0)|add) // 0),
           (($e|map(.completedTasks // 0)|add) // 0),
           ($e|map(select(.isExcluded == true))|length)] | @tsv' <<< "$exec_json"
      )"
      stage_json="$(curl -fsS "$HISTORY_URL/api/v1/applications/$appid/stages")"
      IFS=$'\t' read -r stage_count non_complete stage_failed stage_killed mem_spill disk_spill <<< "$(
        jq -r '[length,
          ([.[] | select(.status != "COMPLETE")] | length),
          ((map(.numFailedTasks // 0)|add) // 0),
          ((map(.numKilledTasks // 0)|add) // 0),
          ((map(.memoryBytesSpilled // 0)|add) // 0),
          ((map(.diskBytesSpilled // 0)|add) // 0)] | @tsv' <<< "$stage_json"
      )"

      clean=false
      if [ "$state" = "OK" ] &&
         [ "$completed" = "true" ] &&
         [ "$executor_count" = "$instances" ] &&
         [ "$removed" = "0" ] &&
         [ "$oomkilled" = "0" ] &&
         [ "$executor_failed" = "0" ] &&
         [ "$excluded" = "0" ] &&
         [ "$non_complete" = "0" ] &&
         [ "$stage_failed" = "0" ] &&
         [ "$stage_killed" = "0" ] &&
         [ "$mem_spill" = "0" ] &&
         [ "$disk_spill" = "0" ]; then
        clean=true
      fi

      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\ttrue\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$idx" "$state" "$driver" "$gc" "$heap" "$region" "$driver_mem" "$instances" "$appid" \
        "$completed" "$duration" "$executor_count" "$removed" "$oomkilled" \
        "$executor_failed" "$executor_completed" "$excluded" "$stage_count" \
        "$non_complete" "$stage_failed" "$stage_killed" "$mem_spill" "$disk_spill" "$clean" \
        >> "$tmp"
    else
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tfalse\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tfalse\n" \
        "$idx" "$state" "$driver" "$gc" "$heap" "$region" "$driver_mem" "$instances" "$appid" >> "$tmp"
    fi
  done

  mv "$tmp" "$out"
  echo "wrote $out"
}

find_gc_pod() {
  if [ -n "$GC_POD" ]; then
    return
  fi

  GC_POD="$(
    kubectl get pods -n "$NAMESPACE" --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' |
      awk -v pat="$GC_POD_PATTERN" 'index($0, pat) { print; exit }'
  )"
  [ -n "$GC_POD" ] || die "no Running pod found with name containing '$GC_POD_PATTERN'"
}

result_field() {
  local logf="$1"
  local key="$2"
  grep '^RESULT ' "$logf" | tail -1 | sed -n "s/.*[[:space:]]$key=\([^[:space:]]*\).*/\1/p"
}

copy_gc_logs() {
  find_gc_pod
  mkdir -p "$LOCAL_GC_DIR"

  local remote_manifest local_manifest manifest
  remote_manifest="$(mktemp -t remote-gc-manifest.XXXXXX)"
  local_manifest="$(mktemp -t local-gc-manifest.XXXXXX)"
  manifest="$LOCAL_GC_DIR/manifest.tsv"
  : > "$remote_manifest"

  rm -f "$LOCAL_GC_DIR"/*.log "$LOCAL_GC_DIR/manifest.tsv" 2>/dev/null || true

  tail -n +2 "$SUMMARY" | sort -t$'\t' -n -k1,1 | while IFS=$'\t' read -r idx state submit driver spec appid; do
    local logf ts runid
    logf="$(find "$BATCH_DIR" -maxdepth 1 -type f -name "job-$(printf '%02d' "$idx")-*.log" ! -name "job-$(printf '%02d' "$idx")-DRIVER.log" | sort | head -1)"
    if [ -z "$logf" ]; then
      echo "WARN: no job log for idx=$idx; skipping GC log copy for this row" >&2
      continue
    fi
    ts="$(result_field "$logf" ts)"
    runid="$(result_field "$logf" runid)"
    if [ -z "$ts" ] || [ -z "$runid" ]; then
      echo "WARN: missing RESULT ts/runid in $logf; skipping GC log copy for idx=$idx" >&2
      continue
    fi

    kubectl exec -n "$NAMESPACE" "$GC_POD" -- sh -lc \
      "find '$REMOTE_GC_DIR' -maxdepth 1 -type f -name '${ts}-${runid}-*.log' -printf '%f\t%s\n' | sort" \
      >> "$remote_manifest"
  done

  sort -u "$remote_manifest" -o "$remote_manifest"
  if [ ! -s "$remote_manifest" ]; then
    die "no GC logs found in $REMOTE_GC_DIR on pod $GC_POD for batch $BATCH_NAME"
  fi

  cut -f1 "$remote_manifest" | while IFS= read -r file; do
    kubectl cp -n "$NAMESPACE" "$GC_POD:$REMOTE_GC_DIR/$file" "$LOCAL_GC_DIR/$file" >/dev/null
    echo "copied $file"
  done

  find "$LOCAL_GC_DIR" -maxdepth 1 -type f -name '*.log' -print0 | while IFS= read -r -d '' file; do
    local base size
    base="${file##*/}"
    size="$(stat -f %z "$file" 2>/dev/null || stat -c %s "$file")"
    printf "%s\t%s\n" "$base" "$size"
  done | sort > "$local_manifest"

  if ! diff -u "$remote_manifest" "$local_manifest" >/dev/null; then
    echo "Remote/local GC log size mismatch:" >&2
    diff -u "$remote_manifest" "$local_manifest" >&2 || true
    exit 1
  fi

  {
    printf "file\tbytes\n"
    cat "$local_manifest"
  } > "$manifest"

  rm -f "$remote_manifest" "$local_manifest"
  echo "wrote $manifest"
  echo "copied GC logs to $LOCAL_GC_DIR"
}

echo "batch:        $BATCH_DIR"
echo "summary:      $SUMMARY"
echo "namespace:    $NAMESPACE"

if [ "$SKIP_VALIDITY" -eq 0 ]; then
  echo "history:      ${HISTORY_URL:-port-forward $HISTORY_SERVICE on localhost:$HISTORY_PORT}"
  write_validity
else
  echo "validity:     skipped"
fi

if [ "$SKIP_GC_COPY" -eq 0 ]; then
  echo "gc output:    $LOCAL_GC_DIR"
  copy_gc_logs
else
  echo "gc copy:      skipped"
fi

echo "done"
