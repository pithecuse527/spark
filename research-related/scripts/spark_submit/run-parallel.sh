#!/usr/bin/env bash
set -uo pipefail
# NOTE: intentionally NOT using `set -e` — one failed run must not abort the batch.

# =============================================================================
# Parallel wrapper around run-screening.sh — JSON-driven.
#
# Core input  : -j <N>  (the number of parallel jobs; this is the main knob)
# Experiments : a JSON file; each entry maps to one run-screening.sh invocation,
#               every run-screening.sh parameter is per-experiment configurable.
#
# Usage:
#   ./run-parallel.sh <experiments.json> [-j N] [-y] [--dry-run]
#     -j N         max concurrent jobs. Overrides "parallelism" in the JSON.
#                  If neither is set, defaults to 2.
#     -y           skip the confirmation prompt
#     --dry-run    print the resolved plan and exit, submit nothing
#
# JSON format (every run-screening.sh knob is an explicit field — no presets):
#   {
#     "parallelism": 2,                         # optional; -j overrides
#     "experiments": [
#       { "query":"q64", "gc":"g1", "heap":"2g", "aqe":false, "broadcast":"off",
#         "benchmark":"tpcds", "scale":200, "cores":2, "instances":2,
#         "driver_memory":"4g", "overhead":"512m", "region":"8m", "node":"worker1",
#         "data_base":"s3a://spark-obj-storage" },
#       { "query":"q9", "benchmark":"tpch", "gc":"shen", "heap":"4g", "aqe":false }
#     ]
#   }
#   Only "query" is REQUIRED. Every other key is optional and falls back to
#   run-screening.sh's defaults: gc=g1, benchmark=tpcds, scale=200, aqe=true, heap=2g,
#   cores=2, instances=2, driver_memory=4g, overhead=512m, broadcast=off, region=none,
#   node=none.
#
# Env: SPARK_HOME + a working kubectl context (ns `spark`) required (unless --dry-run).
# =============================================================================

HERE="$(cd "$(dirname "$0")" && pwd)"
SCREEN="${SCREEN_BIN:-$HERE/run-screening.sh}"   # SCREEN_BIN overrides for testing
MAX_CLI=""
ASSUME_YES=0
DRY_RUN=0
JSON=""

# --- arg parsing ---
while [ $# -gt 0 ]; do
  case "$1" in
    -j) MAX_CLI="${2:?-j needs a number}"; shift ;;
    -y|--yes) ASSUME_YES=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) sed -n '5,40p' "$0"; exit 0 ;;
    -*) echo "ERROR: unknown flag '$1'" >&2; exit 2 ;;
    *) [ -z "$JSON" ] && JSON="$1" || { echo "ERROR: extra arg '$1'" >&2; exit 2; } ;;
  esac
  shift
done
[ -n "$JSON" ] || { echo "ERROR: give an experiments JSON file. See --help." >&2; exit 2; }
[ -f "$JSON" ] || { echo "ERROR: JSON file not found: $JSON" >&2; exit 2; }
command -v python3 >/dev/null || { echo "ERROR: python3 required to parse JSON" >&2; exit 2; }

# --- JSON -> (parallelism line) + one TAB-separated run-screening.sh named-flag argv per line ---
PARSED="$(python3 - "$JSON" <<'PY'
import json, sys
# JSON key -> run-screening.sh flag. Only emitted when present; run-screening.sh
# applies its own defaults for anything omitted. "query" is required.
FLAG = {"query":"--query","gc":"--gc","benchmark":"--bench","bench":"--bench",
        "scale":"--scale","aqe":"--aqe","heap":"--heap","cores":"--cores",
        "instances":"--instances","driver_memory":"--driver-mem","overhead":"--overhead",
        "broadcast":"--broadcast","region":"--region","node":"--node","data_base":"--data-base",
        "extra_exec_opts":"--extra-exec-opts","tag":"--tag"}
ORDER = ["query","gc","benchmark","bench","scale","aqe","heap","cores","instances",
         "driver_memory","overhead","broadcast","region","node","data_base",
         "extra_exec_opts","tag"]
try:
    doc = json.load(open(sys.argv[1]))
except Exception as e:
    sys.stderr.write(f"JSON parse error: {e}\n"); sys.exit(3)
exps = doc.get("experiments", doc if isinstance(doc, list) else None)
if not isinstance(exps, list) or not exps:
    sys.stderr.write("JSON must have a non-empty 'experiments' array.\n"); sys.exit(3)
print("#PARALLELISM\t%s" % (doc.get("parallelism","") if isinstance(doc, dict) else ""))
for i, e in enumerate(exps):
    if not isinstance(e, dict):
        sys.stderr.write(f"experiment[{i}] is not an object\n"); sys.exit(3)
    if not e.get("query"):
        sys.stderr.write(f"experiment[{i}] missing required 'query'\n"); sys.exit(3)
    toks, seen = [], set()
    for key in ORDER:
        if key not in e or key in seen: continue
        seen.add(key)
        v = e[key]
        if v is None: continue
        v = ("true" if v else "false") if isinstance(v, bool) else str(v)
        if key in ("gc","aqe"): v = v.lower()
        if v == "": continue
        toks += [FLAG[key], v]
    print("\t".join(toks))
PY
)" || { echo "ERROR: failed to parse $JSON (see above)" >&2; exit 3; }

# split parsed output
JSON_PAR="$(printf '%s\n' "$PARSED" | sed -n 's/^#PARALLELISM\t//p')"
SPECS=()
while IFS= read -r line; do
  [ -n "$line" ] || continue
  case "$line" in '#PARALLELISM'*) continue ;; esac
  SPECS+=("$line")
done < <(printf '%s\n' "$PARSED")
[ "${#SPECS[@]}" -gt 0 ] || { echo "ERROR: no experiments parsed" >&2; exit 3; }

# parallelism: CLI > JSON > 2
MAX="${MAX_CLI:-${JSON_PAR:-2}}"
case "$MAX" in (*[!0-9]*|"") echo "ERROR: parallelism must be a positive integer, got '$MAX'" >&2; exit 2 ;; esac
[ "$MAX" -ge 1 ] || { echo "ERROR: parallelism must be >= 1" >&2; exit 2; }

# --- preflight ---
[ -x "$SCREEN" ] || { echo "ERROR: not executable: $SCREEN" >&2; exit 2; }
if [ "$DRY_RUN" -eq 0 ]; then
  : "${SPARK_HOME:?SPARK_HOME must be set}"
  [ -x "$SPARK_HOME/bin/spark-submit" ] || { echo "ERROR: no spark-submit at \$SPARK_HOME/bin" >&2; exit 2; }
  command -v kubectl >/dev/null || { echo "ERROR: kubectl not on PATH" >&2; exit 2; }
  kubectl get ns spark >/dev/null 2>&1 || { echo "ERROR: cannot reach 'spark' namespace via current kubectl context" >&2; exit 2; }
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
LOGDIR="$HERE/../../logs/parallel-runs/$STAMP"
SUMMARY="$LOGDIR/summary.tsv"

# --- show the plan ---
echo "=============================================="
echo " Parallel GC-study batch (JSON-driven)"
echo "=============================================="
echo "  experiments file: $JSON"
echo "  jobs:             ${#SPECS[@]}     max concurrent (-j): $MAX"
echo "  per-job logs:     $LOGDIR/"
echo "----------------------------------------------"
i=0; for s in "${SPECS[@]}"; do i=$((i+1)); printf "  %2d) run-screening.sh %s\n" "$i" "$(printf '%s' "$s" | tr '\t' ' ')"; done
echo "=============================================="

if [ "$DRY_RUN" -eq 1 ]; then echo "(dry-run: nothing submitted)"; exit 0; fi

if [ "$ASSUME_YES" -ne 1 ]; then
  printf "Submit these %d jobs (max %d in parallel) to the 'spark' namespace? [y/N] " "${#SPECS[@]}" "$MAX"
  read -r ans; case "$ans" in y|Y|yes|YES) ;; *) echo "Aborted."; exit 1 ;; esac
fi

mkdir -p "$LOGDIR"
printf 'idx\tstate\tsubmit\tdriver\tspec\tappid\n' > "$SUMMARY"

# run-screening.sh exits 0 even when the driver pod fails; the real driver exit
# code is the `exit=` field of its "RESULT exit=.. appid=.. runid=.." line.
run_one() {
  local idx="$1"; shift
  local tabspec="$1"
  local argv; IFS=$'\t' read -r -a argv <<< "$tabspec"
  local tag; tag="$(printf '%s' "$tabspec" | tr '\t ' '__' | tr -cd 'A-Za-z0-9_.-' | cut -c1-80)"
  local logf="$LOGDIR/job-$(printf '%02d' "$idx")-$tag.log"
  echo "[$(date +%H:%M:%S)] START #$idx: ${argv[*]}"
  local st=0
  bash "$SCREEN" "${argv[@]}" >"$logf" 2>&1 || st=$?
  local res drv appid state
  res="$(grep '^RESULT ' "$logf" | tail -1)"
  drv="$(printf '%s' "$res" | sed -n 's/.*[[:space:]]exit=\([^[:space:]]*\).*/\1/p')"; [ -n "$drv" ] || drv="?"
  appid="$(printf '%s' "$res" | sed -n 's/.*[[:space:]]appid=\([^[:space:]]*\).*/\1/p')"; [ -n "$appid" ] || appid="?"
  if [ "$st" = 0 ] && [ "$drv" = 0 ]; then state=OK; else state=FAIL; fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$idx" "$state" "$st" "$drv" "${argv[*]}" "$appid" >> "$SUMMARY"
  echo "[$(date +%H:%M:%S)] END   #$idx [$state] submit=$st driver=$drv app=$appid"
  if [ "$state" = FAIL ]; then
    if [ "$appid" != "?" ] && command -v kubectl >/dev/null; then
      local dpod; dpod="$(kubectl get pods -n spark -l "spark-app-selector=$appid" --no-headers 2>/dev/null | grep driver | awk '{print $1}' | head -1)"
      if [ -n "$dpod" ]; then
        kubectl logs "$dpod" -n spark --tail=300 > "$LOGDIR/job-$(printf '%02d' "$idx")-DRIVER.log" 2>&1 || true
        kubectl get pod "$dpod" -n spark -o jsonpath='{range .status.containerStatuses[*]}{.name}{" exit="}{.state.terminated.exitCode}{" reason="}{.state.terminated.reason}{"\n"}{end}' \
          >> "$LOGDIR/job-$(printf '%02d' "$idx")-DRIVER.log" 2>&1 || true
        echo "   ↳ captured driver log → job-$(printf '%02d' "$idx")-DRIVER.log"
      fi
    fi
    echo "   ↳ submit-log tail:"; tail -n 12 "$logf" | sed 's/^/     /'
  fi
}

# --- slot limiter (poll; works on bash 3.2). ponytail: swap to `wait -n` if bash>=4.3 ---
idx=0
for spec in "${SPECS[@]}"; do
  while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$MAX" ]; do sleep 3; done
  idx=$((idx+1))
  run_one "$idx" "$spec" &
  sleep 1   # stagger so two spark-submits don't share a timestamp
done
wait

echo
echo "=============================================="
echo " Batch complete — summary ($SUMMARY):"
echo "=============================================="
column -t -s "$(printf '\t')" "$SUMMARY"
fails="$(awk -F'\t' 'NR>1 && $2=="FAIL"' "$SUMMARY" | wc -l | tr -d ' ')"
echo "----------------------------------------------"
echo "  failures: $fails / ${#SPECS[@]}   (FAIL = submit!=0 OR driver-exit!=0)"
echo "  collect:  ./collect-run-artifacts.sh $STAMP"
[ "$fails" -eq 0 ]
