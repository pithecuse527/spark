#!/usr/bin/env bash
# Parse Spark event logs from an oracle sweep and emit a tidy CSV.
# Output columns: query, mode, rep, wallclock_ms, shuffle_read_bytes,
# shuffle_write_bytes, spill_bytes, num_joins, chosen_strategies
#
# Usage:
#   ./30_collect.sh <results-dir-from-10_run_oracle_sweep>

set -euo pipefail

RUN_DIR="${1:?usage: $0 <results-dir>}"
OUT="${RUN_DIR}/summary.csv"

PY="$(command -v python3 || command -v python)"
"${PY}" - "${RUN_DIR}" "${OUT}" <<'PY'
import json, os, sys, glob, csv, gzip, io

run_dir, out = sys.argv[1], sys.argv[2]
rows = []

for evt_dir in sorted(glob.glob(os.path.join(run_dir, "event-logs", "*"))):
    name = os.path.basename(evt_dir)
    parts = name.split("-")
    # name pattern: <benchmark>-<query>-<mode>-r<rep>
    if len(parts) < 4: continue
    bench = parts[0]
    query = parts[1]
    mode  = "-".join(parts[2:-1])
    rep   = parts[-1].lstrip("r")

    wall = 0
    sread = swrite = spill = 0
    joins = []

    for log in sorted(glob.glob(os.path.join(evt_dir, "*"))):
        opener = gzip.open if log.endswith(".gz") else open
        with opener(log, "rt") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                t = e.get("Event", "")
                if t == "SparkListenerApplicationStart":
                    t0 = e.get("Timestamp", 0)
                elif t == "SparkListenerApplicationEnd":
                    wall = e.get("Timestamp", 0) - t0
                elif t == "SparkListenerTaskEnd":
                    m = e.get("Task Metrics", {})
                    sread  += m.get("Shuffle Read Metrics",  {}).get("Remote Bytes Read", 0) \
                            + m.get("Shuffle Read Metrics",  {}).get("Local Bytes Read", 0)
                    swrite += m.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    spill  += m.get("Memory Bytes Spilled", 0)
                elif t == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    plan = e.get("physicalPlanDescription", "") or e.get("Physical Plan Description", "")
                    for kw in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
                               "BroadcastNestedLoopJoin", "CartesianProduct"):
                        joins += [kw] * plan.count(kw)

    rows.append([bench, query, mode, rep, wall, sread, swrite, spill,
                 len(joins), "|".join(joins)])

with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["benchmark","query","mode","rep","wallclock_ms",
                "shuffle_read_bytes","shuffle_write_bytes","spill_bytes",
                "num_joins","chosen_strategies"])
    w.writerows(rows)

print(f"wrote {out} with {len(rows)} rows")
PY
