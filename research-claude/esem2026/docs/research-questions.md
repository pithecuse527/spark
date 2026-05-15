# Research Questions — operationalization

## RQ1. AQE join-decision accuracy

**Question.** On TPC-DS and TPC-H, what fraction of joins does AQE select to
a strategy whose end-to-end runtime is within tolerance of the fastest viable
alternative?

**Variables.**
- Population: each *join site* in each query of the workload that resolves
  to one of {BroadcastHashJoin, ShuffledHashJoin, SortMergeJoin}.
- Treatment: planner mode in {AQE-default, force-BHJ, force-SHJ, force-SMJ}.
- Per (query, planner mode): wall-clock query time, per-stage time,
  per-join time (from Spark event log).
- Per join site: chosen strategy, viable strategies (the set of forced runs
  that finished without OOM/error), oracle = `argmin` of viable runtimes.

**Operationalization.**
1. Run each query four times per mode, drop max and min, keep median runtime.
2. A join is *correctly decided* if AQE's choice runtime is within
   `tol = 10%` of the oracle runtime.
3. Report (a) percent of join sites correctly decided, (b) distribution of
   slowdown ratio = AQE / oracle.

**Threats considered later** (see threats-to-validity.md): warm caches,
disk pressure, executor scheduling variance.

## RQ2. Predictors of mismatch

**Question.** Among mismatches, which workload features predict that AQE
chose sub-optimally?

**Candidate features (per join site, all readily available from Spark).**
- estimated build size (from Catalyst stats)
- actual build size (from shuffle write metrics)
- estimation error = |estimated - actual| / actual
- key cardinality (from `approx_count_distinct` of the join key on build side)
- input row counts (left, right)
- selectivity of upstream filters
- number of shuffle partitions after AQE coalesce
- per-executor L3 cache size (constant per cluster)
- skew indicator (max / median partition size)

**Analysis.**
1. Logistic regression: P(mismatch) ~ features. Report coefficients with
   confidence intervals.
2. Decision tree of depth ≤ 3 for an interpretable summary.
3. Permutation feature importance.

We do not claim causality; only predictive association.

## RQ3. Configuration-level mitigation

**Question.** For the dominant failure modes identified in RQ2, can adjusting
existing Spark configurations recover the gap to the oracle?

**Candidate adjustments (no source code change).**
- `spark.sql.autoBroadcastJoinThreshold` (tighter / looser)
- `spark.sql.adaptive.autoBroadcastJoinThreshold`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes`
- `spark.sql.join.preferSortMergeJoin`
- `spark.sql.shuffledHashJoinFactor`
- `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`

**Operationalization.**
1. From RQ2, pick the top 1-2 failure clusters.
2. For each cluster, pick the adjustment whose mechanism plausibly addresses
   the failure (pre-registered in this document before running).
3. Re-run mismatching queries with the adjustment. Compute recovery rate =
   fraction of previously-mismatched joins that now match oracle within `tol`.
4. Report regressions: queries that worsened.

## Pre-registration

This document is fixed before any results in the paper. Any RQ change after
running is reported in Threats to Validity.
