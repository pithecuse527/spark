# ESEM 2026 — When Does AQE Pick the Wrong Join?

## Track and category

- Venue: ESEM 2026, Emerging Results, Vision, and Reflection Papers
- Category: **Emerging Results** (10p + 2p, LIPIcs)
- Review: double-anonymous
- Submission link: https://esem26-ervr.hotcrp.com

## Key dates

- Abstract (mandatory): **2026-05-22 AoE**
- Paper submission: **2026-05-29 AoE**
- Notification: 2026-07-10
- Camera-ready: 2026-08-05

## One-line claim

Adaptive Query Execution (AQE) in Apache Spark switches join strategies at
runtime using statistics gathered after shuffle stages materialize. We
empirically measure how often this decision matches an oracle and characterize
the workload features that predict mismatches.

## Research questions

- **RQ1.** On TPC-DS and TPC-H, what fraction of joins does AQE plan to a
  strategy that is also the fastest among the viable alternatives?
- **RQ2.** Among the mismatches, which workload features (estimated vs actual
  build size, key cardinality, partition count, hardware cache size) predict
  the decision error?
- **RQ3.** Can a configuration-level adjustment (no code change) recover the
  performance gap, and for which subset of failures?

## Contribution

1. An empirical taxonomy of AQE join-decision failures.
2. Evidence that a non-trivial fraction of failures are predictable from
   readily available workload features.
3. A configuration-level mitigation with measured recovery rates, and a
   sketch of the longer-term system-level work needed to close the rest.

This is explicitly preliminary, suited to the Emerging Results track.

## Method

Controlled experiment. Three independent variables:

- workload (TPC-DS SF=1000 + TPC-H SF=1000, query subset)
- planner mode (AQE-default vs forced BHJ vs forced SHJ vs forced SMJ)
- cluster size (16 and 32 executors)

Dependent variables: per-query runtime, per-join wall time, shuffle bytes,
spill bytes, LLC miss rate (perf, where attainable).

Oracle = `argmin` over forced strategies. AQE's choice is correct if it
matches the oracle within a small tolerance (e.g., 10%).

## Out of scope for this paper

- A new join algorithm (no V1 partition sizing, no V2 radix). These belong
  in a follow-up systems paper. Mentioning them here is acceptable only as
  Future Work.
- New runtime statistics. We use what Spark already collects.

## Directory layout

```
esem2026/
  README.md                              this file
  docs/
    research-questions.md                RQs with operationalization
    methodology.md                       experiment protocol
    threats-to-validity.md               internal / external / construct
    open-science.md                      replication package plan
  paper/
    main.tex                             LIPIcs stub (double-anonymous)
    references.bib
  scripts/
    00_cluster_check.sh                  per-node cache + perf paranoid
    01_build_spark.sh                    vanilla Spark dist
    02_build_image.sh                    docker push for k8s
    10_run_oracle_sweep.sh               force BHJ/SHJ/SMJ per query
    11_run_aqe_default.sh                AQE default decision
    20_perf_attach.sh                    optional perf instrumentation
    30_collect.sh                        aggregate per-run metrics
  configs/
    spark-baseline.conf                  AQE default, no forcing
    spark-force-bhj.conf                 force BroadcastHashJoin
    spark-force-shj.conf                 force ShuffledHashJoin
    spark-force-smj.conf                 force SortMergeJoin
  k8s/
    executor-pod-template.yaml           hostPID + perf cap
  benchmarks/
    microbench/                          synthetic build/probe sweep
    tpcds/                               selected query list
  results/                               gitignored
```

## Double-anonymous reminders

- No author names, affiliations, acknowledgments, or grant numbers in the PDF.
- Self-citations in third person ("Prior work by X et al. showed...").
- Replication artifact hosted anonymously
  (e.g., https://anonymous.4open.science/ or anonymized Zenodo deposit).
- Repo path strings in artifact (e.g., `research-claude/esem2026/`) should be
  scrubbed before release to avoid deanonymization.

## Pivot note

This work was originally scoped as a MASCOTS systems paper (cache-aware SHJ
patches V1 and V2). For ESEM ER, the contribution is reframed as an
empirical measurement study; the patches are left as Future Work.
