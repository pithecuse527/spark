# Methodology

## Workloads

- TPC-DS SF=1000 (full schema, query subset).
- TPC-H SF=1000 (full schema, full 22 queries).
- Query selection for TPC-DS: queries whose plans contain at least one join
  that AQE can plausibly switch (i.e. equi-joins on partitioned tables).
  Initial candidates: q3, q7, q19, q34, q42, q53, q63, q68, q73, q89.
  Final list pinned before any result figure is drawn.

## Cluster

- Kubernetes, 32 worker nodes available.
- Two cluster sizes per condition: 16 executors and 32 executors.
- One executor per node: 8 cores, 24 GB on-heap, 8 GB off-heap, 4 GB overhead.
- Driver: 8 GB.
- Spark version: this repository (3.5.x line), vanilla build, no patches.
- Data layout: TPC-DS / TPC-H Parquet on local NVMe per node; node affinity
  via `mascots.tpcds/has-shard` label.

## Independent variables

| Variable | Levels |
|---|---|
| workload | TPC-DS, TPC-H |
| query | per benchmark, fixed subset |
| planner mode | AQE-default, force-BHJ, force-SHJ, force-SMJ |
| cluster size | 16, 32 |

## Forcing strategies (no code change)

- **force-BHJ.** `spark.sql.autoBroadcastJoinThreshold` raised to a value
  larger than all build sides in the workload (e.g., 2 GB) so every join
  qualifies. Joins that fail (build does not fit) are recorded as
  "BHJ-infeasible" and excluded from the BHJ oracle pool.
- **force-SHJ.** `preferSortMergeJoin = false`, `shuffledHashJoinFactor = 1`,
  `autoBroadcastJoinThreshold = -1`. Add a SQL `/*+ SHUFFLE_HASH(...) */` hint
  via query rewriting where stable picking is needed.
- **force-SMJ.** `autoBroadcastJoinThreshold = -1`, `preferSortMergeJoin = true`.
- **AQE-default.** All planner configs at upstream defaults.

A join is "viable under mode M" if the run finished without error.

## Repetition and statistics

- 4 runs per (query, mode, cluster size). Drop one max and one min, report
  median of remaining two.
- 95% CI via bootstrap (5000 resamples) on the slowdown ratio.
- Warm-up: discard the first run after each Spark application start.

## Measurements

Per-run, collected from Spark event logs and metrics:

- query wall time
- per-stage time
- per-join wall time (from `SQLExecutionMetrics`)
- shuffle read / write bytes
- spill bytes
- executor CPU time

Per-host, optional (where perf paranoid permits):

- LLC-loads, LLC-load-misses, instructions, cycles, branch-misses
- Captured by `scripts/20_perf_attach.sh` on a randomly selected
  executor pod during the join stage window.

## Oracle definition

For each join site identified in the query plan:

```
oracle_strategy = argmin over viable s in {BHJ, SHJ, SMJ}:
                    median_runtime(query, mode=force-s)
```

The oracle is *per query*, not per join site, because forcing applies to the
whole plan. Where a query has multiple joins, we report at query granularity
and discuss this aggregation limit in Threats to Validity.

## Pre-registered analysis

- RQ1: per-query accuracy, plus a binary "correct within 10%" flag.
- RQ2: features computed at query granularity (max estimation error across
  joins, total estimated/actual build, etc.). Logistic regression and a
  shallow decision tree.
- RQ3: pick top-2 failure clusters from RQ2, apply pre-listed config
  adjustments, measure recovery rate.

## What is NOT measured in this paper

- Cost of decision logic itself (compile time).
- Memory pressure leading to OOM under each mode (recorded as
  "infeasibility" only).
- Latency tail beyond the 4-repetition design.
- Multi-tenant scheduling effects.
