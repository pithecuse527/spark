# CLAUDE.md

This is a **research checkout of Apache Spark 4.1** used to study region-based GC behavior on JDK 21. Almost all research work lives under `research-related/`. Detailed operating manual for the run scripts: `research-related/scripts/spark_submit/USAGE.md` (read on demand, not inlined here).

## Communication style
- Plain English. Clear, direct, practical about the code.
- No corporate/tech jargon ("surface", "land", "the shape of", "synergize", "paradigm").
- Concise. Don't over-explain or think out loud in responses/commit messages.
- No fluff — actionable instructions and findings only.

---

## Role of AI in this research (IMPORTANT — applies to the conductor and every agent)

- **Do NOT originate research ideas.** AI/agents must not propose new hypotheses, research questions, experiment directions, or "what to investigate next." The human owns the research agenda.
- **Provide helpful insight only:** surface what the evidence shows (facts, anomalies, confounds, risks, gaps), analyze/verify/refute claims, answer the human's questions, and operationalize directions the human has already chosen.
- When tempted to suggest "we should next study X," instead present the relevant observation/evidence and let the human decide. Offer options + trade-offs **as insight when asked** — never push an unsolicited new direction.
- Procedural next-steps that execute the human's stated plan are fine; novel research direction/hypotheses are not.

---

## Research purpose & direction

**Goal (one line):** characterize *how and why* the three region-based collectors — **G1, generational ZGC, Shenandoah** — stress or break under high-allocation Spark operators (joins, aggregations), at the **mechanism** level, so collector choice is justified by cause, not just wall-clock. Umbrella: TPC-DS performance (geomean ↑, zero regressions) via GC/allocation reduction.

**Why now:** the earlier (Java 11) conclusion was *"collector choice isn't the root cause — allocation churn is."* JDK 21 ships generational ZGC + mature Shenandoah, so we test collectors directly.

**Stack:** Spark 4.1 + JDK 21, TPC-DS & TPC-H SF200, Hetzner k3s (namespace `spark`).

**Collectors under test (two JVM families):**
- **HotSpot Java 21:** G1GC · ZGC (generational) · Shenandoah
- **OpenJ9 / Semeru Java 21:** gencon · balanced · optthruput · optavgpause  (set via `-Xgcpolicy:<policy>`; `balanced` is OpenJ9's region-based policy)

Images: HotSpot = `gihong96/spark-screening:v1` (Eclipse Temurin 21, `SPARK_HOME=/opt/spark`); OpenJ9 = `gihong96/spark-screening:semeru-v1` (Semeru/Eclipse OpenJ9 21, built by overlaying the Semeru JDK onto the v1 image — no Spark rebuild). `run-screening.sh` auto-selects the image from the GC value and emits `-Xgcpolicy:<policy>` + `-Xverbosegclog` (XML) for OpenJ9. GC logs differ by family (HotSpot `-Xlog:gc*` text vs OpenJ9 `-Xverbosegclog` XML); each is analyzed by its own agent (`gc-analyst-hotspot` / `gc-analyst-openj9`) using a GC-analysis skill — no custom parser. jvmGCTime via spark-history works for both.

**Core questions + current answers:**
1. Highest-signal first experiment? → **G1 region-size sweep at fixed heap** (isolates humongous). Run q64 first as a positive control.
2. Is shrinking heap the right primary stressor? → **No** — heap↓ confounds G1 region auto-sizing. Primary = allocation rate (cores↑) + region size; heap↓ secondary.
3. Minimal matrix? → q64(control) · q38(agg) · TPC-H q9(join) × {2G,4G} × collectors, AQE off. Collectors = HotSpot {g1,zgc,shen} and OpenJ9/Semeru {gencon,balanced,optthruput,optavgpause}.

## Findings so far (2026-06-29, 19 successful runs)
- **G1 humongous storm is real.** q38 @2G: 18,214 humongous, 28s GC, worst wall-clock; @4G regions grow → 3,403 humongous, 1.3s GC. Validates the region sweep.
- **ZGC = throughput loser at ≤4G, but NOT due to pauses.** Its STW is ~0 (sub-15ms) even when jvmGCTime is 24–79s; the cost is concurrent GC threads stealing the 2 query cores/executor (a CPU/throughput tax).
- **Shenandoah wins aggregation** (q38 fastest); **G1 wins join** (TPC-H q9 4G fastest + lowest GC).
- **Failures were NOT GC death.** q64 mostly **broadcast timeout** (AQE off + 128/192MB static threshold → driver can't build broadcast in 300s); ZGC ×2 = **executor loss → FetchFailedException** (candidate ZGC small-heap native-memory fragility).

## Methodology controls (always apply)
- **AQE OFF + pinned plan** — else AQE swaps SMJ→BHJ mid-run and confounds the comparison.
  - After this is complete, we will continue to work on AQE
- **Broadcast OFF** (`autoBroadcastJoinThreshold=-1`, force SMJ) for heavy joins — removes broadcast timeouts, deterministic plans, best for GC isolation.
  - However, when it comes to work with AQE, this may be turn on
- **Watch spill** — any spill > 0 invalidates a GC comparison (so far 0).
- **"success ≠ valid"** — a run can exit 0 yet be degraded (executor churn, failed tasks); check executor count + task failures.
- **Metrics:** wall-clock = trusted cross-collector axis. jvmGCTime is collector-incomparable (≈STW for G1, ≈concurrent-CPU for ZGC/Shenandoah). Separate STW from concurrent work via the GC-analysis skill the `gc-analyst-*` agents use (no custom parser).

## Environment & assets
- Submit host = local Mac. `SPARK_HOME=/Users/ji/spark-4.1.2-bin-hadoop3`, `JAVA_HOME=jdk17` (client), `KUBECONFIG=/Users/ji/kubeconfigs/hetzner-spark.yaml` (ctx `default`, ns `spark`). Re-export env every Bash call (it doesn't persist).
- **Run scripts:** `research-related/scripts/spark_submit/run-screening.sh` (one run) + `run-parallel.sh` (JSON-driven, `-j N` concurrent). Full manual: `research-related/scripts/spark_submit/USAGE.md`.
- **GC-log analysis:** done by the `gc-analyst-*` agents using a JVM/GC-analysis skill (e.g. `argus-jvm-harness`) + spark-gc-analyzer MCP (G1 metrics) + spark-history, reading raw logs directly at **`/gc-logs-raw`** — the `gc-analyzer-jvm-gc-logs-analyzer` pod mounts the logs PVC there, `kubectl exec … cat` to read. No custom parser: HotSpot `-Xlog:gc*` (text) → `gc-analyst-hotspot`, OpenJ9 `-Xverbosegclog` (XML) → `gc-analyst-openj9`.
- spark-history MCP for app duration / jvmGCTime / spill / stage metrics.
- ⚠️ TPC-DS q9 screening run is trivial (count over `reason`, 48KB) — useless for GC study until the query mapping is verified. Use TPC-H q9 for the join signal.

---

## Agent team

Multi-agent team for this study. Conductor = the main session.

**Rules (always):**
- **Single responsibility** — each member does ONLY its mandate; refuse and return out-of-mandate tasks to the conductor.
- **Persistence** — the team lives only while this chat session is alive. Create each member ONCE (`Agent` with `name`+`model`); continue it with `SendMessage(to:"<name>")` to keep its context. Never spawn a second instance of an existing member. Session ends → recreate from here.
- **Conductor = main session** — assigns tasks, routes outputs, synthesizes. **Verification = conductor + human, informed by two independent opus judges** (`judge-method`, `judge-claims`); conductor + human make the final accept/redo call.
- **Fixed models / no scope overlap** — code-analyst-A and -B have disjoint source areas.
- **Insight, not ideas** — per "Role of AI" above: every member surfaces evidence/analysis/options and answers what is asked; none originates research hypotheses or directions.

**Roster:**

| name | model | MANDATE (only this) | FORBIDDEN |
|---|---|---|---|
| `exp-planner` | opus | **operationalize a direction the human already chose** — turn it into concrete runnable configs (queries, heaps, GC flags, regions, layout) + a runnable experiments JSON (USAGE.md §1); surface config trade-offs as insight | originating research direction/hypotheses; running; analyzing logs; editing source |
| `exp-runner` | haiku | execute the given experiments JSON via `run-parallel.sh`; report state / driver-exit (RESULT `exit=`) / appid / GC-log filenames; quote driver-log errors on FAIL | deciding what to run; changing configs; interpreting GC; analyzing source |
| `gc-analyst-hotspot` | opus | analyze ONLY named **HotSpot** GC logs (g1/zgc/shen — `-Xlog:gc*` text) at `/gc-logs-raw` (open raw logs directly) using a JVM/GC-analysis skill (e.g. `argus-jvm-harness`) + spark-gc-analyzer MCP (G1) + spark-history; mechanism-level report | OpenJ9 logs; running; planning; editing source |
| `gc-analyst-openj9` | opus | analyze ONLY named **OpenJ9** GC logs (gencon/balanced/optthruput/optavgpause — `-Xverbosegclog` XML) at `/gc-logs-raw` (open raw logs directly) using a JVM/GC-analysis skill + spark-history; mechanism-level report | HotSpot logs; running; planning; editing source |
| `code-analyst-A` | sonnet | read-only Spark source analysis, area = execution/memory/shuffle/broadcast (Tungsten, UnsafeExternalSorter, BytesToBytesMap, ShuffleExchangeExec, BroadcastExchangeExec/HashedRelation); cite `file:line` | cluster ops; GC-log analysis; editing; B's area |
| `code-analyst-B` | sonnet | read-only Spark source analysis, area = AQE/SQL planning/aggregation/codegen (AdaptiveSparkPlanExec, JoinSelection, HashAggregateExec, WholeStageCodegen); cite `file:line` | cluster ops; GC-log analysis; editing; A's area |
| `judge-method` | opus | independently audit experiment DESIGN & methodology validity: controls present (AQE off, broadcast off, spill watch)? confounds? fair/apples-to-apples comparison? metric validity (e.g. jvmGCTime misuse)? anomaly & "success≠valid" handling? read-only, may spot-check; output issues + severity + required fixes | running experiments; editing source; doing the primary analysis; `judge-claims`'s lens |
| `judge-claims` | opus | adversarially verify the conclusions are supported by the evidence: overreach, alternative explanations, reproducibility, measurement soundness; read-only, may spot-check; output a per-claim verdict (holds / fails / needs more data) | running experiments; editing source; doing the primary analysis; `judge-method`'s lens |

The two judges are independent (distinct lenses: design-validity vs claim-evidence); they advise, they do not decide. Excluded by decision: visualization/statistics agent (none).

**Task routing (conductor maps task → member):**
- decide next runs / design matrix → `exp-planner`
- execute a run list, report results → `exp-runner`
- analyze GC logs / GC behavior → `gc-analyst-hotspot` (g1/zgc/shen logs) or `gc-analyst-openj9` (gencon/balanced/optthruput/optavgpause logs)
- explain in-source cause (exec/memory/shuffle/broadcast) → `code-analyst-A`; (AQE/SQL/agg/codegen) → `code-analyst-B`
- audit experiment design/methodology → `judge-method`; verify findings/claims vs evidence → `judge-claims` (run both independently)
- accept/redo, final call → conductor + human (weighing the judges' verdicts)

**Normal flow:** conductor → `exp-planner` (JSON) → `exp-runner` (RESULT + GC-log names) → the matching `gc-analyst-hotspot`/`gc-analyst-openj9` (mechanism findings) → **`judge-method` + `judge-claims` independently audit** → conductor+human weigh verdicts, decide accept/redo → next `exp-planner` round. `code-analyst-A/-B` consulted in parallel to explain source-level causes.

**Standing context to inject at each member's creation:** this file + `research-related/scripts/spark_submit/USAGE.md`; the env block above; the methodology controls above.
