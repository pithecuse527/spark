# Council Log

## 2026-06-28 — Region-based GC study: experiment prioritization

**Question:** For a Spark 4.x / JVM 21 study of region-based collectors (G1, generational ZGC, Shenandoah) on TPC-DS & TPC-H SF200, which experiments surface region-based pathologies (humongous, premature promotion, allocation stalls, degenerated/full GC) with highest signal-to-noise? Is shrinking the heap the right primary stressor? What is the minimal high-value matrix + metrics?

**Positions:**
- **Claude (chair):** G1 region-size sweep at fixed heap is the highest-signal first move (isolates humongous from heap shortage). Heap-shrink is a confounded primary stressor — it changes both headroom AND G1 region size; cross region×heap, use cores↑ as a clean allocation-rate axis. Watch Spark spill as a confound. Queries: q38 (agg) + TPC-H q9 (join) + q64 (churn).
- **Codex:** Run **Q64 first as a positive control** (known smoking gun) to prove the measurement pipeline before widening. Do NOT make heap-shrink primary; better primary stressor = more concurrent allocating tasks (cores↑) + larger working sets (shuffle.partitions↓). Critical: **fix the physical plan, disable AQE, pin shuffle partitions** so you measure GC, not plan/spill changes. Detailed flag sets + metric sources (GC log + JFR + Spark SQL metrics).
- **Gemini:** UNAVAILABLE (CLI tier/auth error — IneligibleTierError).

**Empirical check (ran during deliberation):** Analyzed existing `q38-mono-g2560c2-r1m-pdef-g1` GC log → 1032 Full GCs, 1971 humongous-triggered GCs, 132,666 humongous allocations (avg 16MB), maxSurvivorAge=0. Confirms humongous + premature-promotion hypothesis and that forced tiny regions (1MB) are pathological.

**Decision (chair):**
1. **Q64 region-size sweep first** (G1, 2G heap, fixed cores, AQE OFF + pinned plan, region ∈ {1,2,4,8,16,32m}) — positive control + humongous-relief curve. Adopt Codex's positive-control framing over running q9/q38 first.
2. **Q64 3-way** (G1 best-region vs gen-ZGC vs Shenandoah) at 2G and 4G — same allocation burst → humongous vs allocation-stall vs degenerated/full GC.
3. **Generalize**: TPC-DS q38 (agg) + TPC-H q9 (join) + TPC-DS q9, 3-way.
4. **Operator isolation**: force SMJ (`autoBroadcastJoinThreshold=-1`) vs force BHJ (high threshold), AQE off.
- **Primary stressor = allocation rate (cores↑ at fixed heap) + G1 region size**, NOT heap shrink. Heap shrink is a secondary failure-probe (1G), excluded from the main comparison unless Spark does not spill/OOM.
- **Mandatory controls**: AQE off + pinned shuffle partitions + recorded executed plan + fixed memoryOverhead. Metrics: GC log (primary: humongous count/regions, full GC, promotion/maxSurvivorAge, young/mixed pause, ZGC alloc-stall, Shenandoah degenerated/pacing) + JFR (alloc by class/TLAB) + Spark (jvmGCTime, spill bytes, peakExecutionMemory).

**Dissent / watch-outs:**
- Forcing 1MB regions manufactures an artificial worst case — read the sweep as "how large must regions be to relieve humongous," not "smaller = the bug."
- ZGC needs headroom (colored pointers / multi-mapping); 2G may make ZGC look unfairly stall-heavy — that is itself a finding, but report heap-normalized.
- Biggest risk (both advisors): accidentally measuring Spark spill or AQE plan changes instead of GC. The AQE SMJ→BHJ swap in particular changes the allocation profile mid-run — disabling AQE is required for clean collector comparison.
