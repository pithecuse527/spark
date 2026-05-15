# Threats to Validity

ESEM expects this section to be honest and specific. List the threat, then
the mitigation we use, then the residual risk.

## Construct validity

**T1. "Optimal" is approximated by a 3-way forced sweep.**
We restrict the oracle to {BHJ, SHJ, SMJ}. CartesianProduct and
BroadcastNestedLoop are excluded. Mitigation: scope is explicit; we
target equi-join sites only. Residual: a fourth strategy or a hybrid
might beat all three.

**T2. Query granularity vs join granularity.**
Forcing applies per query, not per join site. A "wrong choice" may be
correct at the dominant join but wrong elsewhere. Mitigation: we report
both per-query and per-dominant-join granularity and discuss limits.

**T3. AQE re-planning may change strategy mid-execution.**
A single query may execute multiple strategies across stages. We record
the *final* chosen strategy from the event log, not intermediate proposals.

## Internal validity

**T4. Run-to-run variance.**
4 runs, trim max/min, report median. CI via bootstrap. Residual: not
enough to detect <5% differences with confidence.

**T5. Caching across runs.**
Page cache, shuffle files, and JVM warm code paths may differ between
runs. Mitigation: discard the first run; drop caches with
`sync && echo 3 > /proc/sys/vm/drop_caches` between modes.

**T6. K8s scheduling drift.**
Executor pods may land on different nodes across runs. Mitigation:
node affinity pins shards; we still record `node -> pod` mapping per run.

**T7. Mode-specific infeasibility biases the oracle.**
A query where BHJ OOMs is compared only against SHJ and SMJ. This may
inflate AQE's measured accuracy where AQE correctly avoids BHJ.
Mitigation: report infeasibility rates explicitly; separate analysis
for "all three feasible" subset.

## External validity

**T8. Two benchmarks, one Spark version, one cluster.**
TPC-DS and TPC-H are synthetic. Spark 3.5.x. 32-node k8s. Generalization
to production workloads, other Spark versions, or other schedulers is
unclear. Mitigation: scope claims to "on these benchmarks with this
configuration"; release artifact so others can replicate.

**T9. Hardware homogeneity.**
All nodes share the same CPU and cache hierarchy. Cache effects we
attribute to L3 may differ on heterogeneous fleets.

## Conclusion validity

**T10. Multiple comparisons.**
RQ2's feature analysis tests several predictors. Mitigation: report
Bonferroni-adjusted p-values for the logistic regression; the decision
tree is presented as descriptive, not confirmatory.

**T11. Selection of failure clusters for RQ3.**
RQ3 acts on RQ2's output, so RQ3 inherits selection bias from cluster
choice. Mitigation: pre-register the rule that picks clusters in
research-questions.md before any RQ3 run.

## Replication

- All scripts, configs, query lists, and analysis notebooks are in the
  replication package (see open-science.md).
- Cluster spec, Spark version commit hash, kernel version recorded per run.
- Raw event logs preserved.
