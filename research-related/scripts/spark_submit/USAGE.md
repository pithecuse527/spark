# USAGE — run-screening.sh & run-parallel.sh

**Audience: a Claude agent operating these scripts (e.g. the `exp-runner` teammate).** Follow exactly; do not improvise flags or configs. These submit real jobs to a shared k3s cluster — outward-facing, costs resources.

Scripts live in `research-related/scripts/spark_submit/`. Run from there (paths inside are relative to the script dir).

---

## 0. Required environment (export before EVERY run)

```bash
export SPARK_HOME=/Users/honggeunji/Personal/spark
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.0.9+10/Contents/Home
export KUBECONFIG=/Users/honggeunji/kubeconfigs/hetzner-spark.yaml      # ctx `default`, ns `spark`
```
Shell env does NOT persist between separate Bash tool calls — re-export in the same command that runs the script. Verify reachability first: `kubectl get ns spark` must return `Active`.

---

## 1. run-parallel.sh — the normal entry point (JSON-driven, N at a time)

```bash
./run-parallel.sh <experiments.json> [-j N] [-y] [--dry-run]
```

| Flag / arg | Meaning |
|---|---|
| `<experiments.json>` | **required** — file describing the experiments (schema below) |
| `-j N` | **core knob** = max concurrent jobs. Overrides JSON `parallelism`. Priority: `-j` > JSON `parallelism` > `2`. |
| `-y` | skip the interactive confirm prompt (use for non-interactive/agent runs) |
| `--dry-run` | print the resolved plan and exit; submits nothing; needs no cluster/env |

**Always `--dry-run` first** to confirm the resolved `run-screening.sh` arg lines, then run with `-y`.

### JSON schema
```json
{
  "parallelism": 2,
  "experiments": [
    { "query":"q64", "gc":"g1",   "heap":"2g", "aqe":false, "broadcast":"off", "region":"4m", "node":"worker1" },
    { "query":"q64", "gc":"zgc",  "heap":"2g", "aqe":false, "broadcast":"off", "node":"worker2" },
    { "query":"q9",  "gc":"shen", "heap":"4g", "aqe":false, "broadcast":"off", "benchmark":"tpch", "node":"worker4" }
  ]
}
```
Per experiment (every run-screening.sh knob is an explicit field — no presets):

| key | required | type | default | allowed / notes |
|---|---|---|---|---|
| `query` | ✅ | string | — | e.g. `q64`, `q38`, `q9` |
| `gc` | | string | `g1` | HotSpot: `g1` `zgc` `shen` · OpenJ9: `gencon` `balanced` `optthruput` `optavgpause` (OpenJ9 auto-switches to the Semeru image) |
| `benchmark` | | string | `tpcds` | `tpcds` `tpch` |
| `scale` | | int | `200` | scale factor |
| `aqe` | | bool | `true` | `true` `false` (use `false` for GC-mechanism isolation) |
| `heap` | | string | `2g` | executor heap (`spark.executor.memory`) |
| `cores` | | int | `2` | executor cores |
| `instances` | | int | `2` | executor count |
| `driver_memory` | | string | `4g` | driver heap |
| `overhead` | | string | `512m` | executor `memoryOverhead` |
| `broadcast` | | string | `off` | `off` (=-1, force SMJ) or a size like `128MB` |
| `region` | | string | none | G1 only, e.g. `1m` `4m` `8m` `16m` `32m` (ignored on OpenJ9) |
| `node` | | string | none | Kubernetes node hostname, e.g. `worker1`; pins both driver and executors via `kubernetes.io/hostname=<node>` |
| `data_base` | | string | `s3a://spark-obj-storage` | base URI of bench data |

Only `query` is required; omitted keys use run-screening.sh's defaults. `experiments` may also be a bare top-level array.

### Outputs (per batch)
Written to `research-related/logs/parallel-runs/<timestamp>/`:
- `job-NN-<spec>.log` — full stdout/stderr of each `run-screening.sh` invocation.
- `summary.tsv` — columns: `idx  state  submit  driver  spec  appid`. **`state` is the verdict you report.**
- `job-NN-DRIVER.log` — **only created on FAIL**; the actual driver-pod log tail + container termination reason. **This is where the real error is — read it when a job FAILs.**

`state = FAIL` iff `submit != 0` OR `driver != 0`. Final line prints `failures: X / N`.

---

## 2. collect-run-artifacts.sh — reproducible post-run collection

After `run-parallel.sh` finishes, run this once for the batch timestamp or batch
directory:

```bash
./collect-run-artifacts.sh <timestamp>
```

Example:

```bash
./collect-run-artifacts.sh 20260701-120724
```

What it does:
- reads `research-related/logs/parallel-runs/<timestamp>/summary.tsv`
- starts a temporary Spark History port-forward unless `--history-url` is given
- writes `research-related/logs/parallel-runs/<timestamp>/validity.tsv`
- finds each row's `RESULT ts=<ts> runid=<run-id>` from the job logs
- copies matching raw GC logs from the PVC via the analyzer pod
- verifies local GC-log byte sizes against the PVC
- writes `research-related/logs/gc-logs-raw/<timestamp>/manifest.tsv`

Default raw-GC output:

```text
research-related/logs/gc-logs-raw/<timestamp>/
```

Useful options:

| option | meaning |
|---|---|
| `--history-url http://127.0.0.1:18080` | use an already-forwarded Spark History API |
| `--history-port 18081` | use a different local port for the temporary port-forward |
| `--gc-pod <pod>` | explicitly choose the pod that mounts `/gc-logs-raw` |
| `--local-gc-dir <dir>` | override the local raw-GC output directory |
| `--skip-validity` | copy GC logs only |
| `--skip-gc-copy` | write `validity.tsv` only |

Required commands on the submit host: `kubectl`, `curl`, `jq`, `awk`, `sed`.

The `validity.tsv` `clean` column is true only when:
- the runner state is `OK`
- Spark History says the app completed
- observed executor count equals requested `--instances`
- no removed/OOMKilled executors
- no failed/killed tasks
- no excluded executors
- zero memory and disk spill

---

## 3. run-screening.sh — one run (run-parallel calls this; use directly only for a single ad-hoc run)

```bash
./run-screening.sh --query <q> [--gc g1] [--bench tpcds] [--scale 200] [--aqe true] \
                   [--heap 2g] [--cores 2] [--instances 2] [--driver-mem 4g] [--overhead 512m] \
                   [--broadcast off] [--region <size>] [--node <worker>] [--data-base <uri>]
```
Named flags, order-independent. Only `--query` is required; every other flag defaults as in the table above. **No presets** — set `--heap` / `--cores` / `--broadcast` explicitly per run.

`--gc`: HotSpot `g1` | `zgc` (→ generational, adds `+ZGenerational`) | `shen` (image `…:v1`, `-Xlog:gc*`); OR OpenJ9 `gencon` | `balanced` | `optthruput` | `optavgpause` (auto image `…:semeru-v1`, `-Xgcpolicy:<gc>`, `-Xverbosegclog` XML). `--broadcast off` = `-1` (force SMJ). `--region`: G1 only (ignored on OpenJ9). `--node worker1` pins both driver and executors with `spark.kubernetes.node.selector.kubernetes.io/hostname=worker1` while keeping `spark-data=true`. `DRY_RUN=1` env prints the resolved image + JVM opts and exits without submitting.

Examples:
```bash
./run-screening.sh --query q38 --gc shen   --heap 4g --aqe false --broadcast off
./run-screening.sh --query q9  --bench tpch --gc gencon --heap 2g --aqe false --broadcast off
./run-screening.sh --query q64 --gc g1     --heap 2g --region 8m --aqe false --broadcast off
./run-screening.sh --query q38 --gc g1     --heap 2g --region 1m --aqe false --broadcast off --node worker1
```

### RESULT line (machine-parseable, last line of stdout)
```
RESULT exit=<code> appid=<id> runid=<run-id> gc=<gc> bench=<b> query=<q> ts=<ts>
```
**`exit=` = the real driver exit code; `appid=` = the Spark app ID.** ⚠️ `spark-submit` (cluster mode) often returns 0 even when the driver FAILED; the script re-checks the driver pod and puts the true code in `exit=`. Trust `exit=`, not the shell `$?`.

### Artifacts written by a run
- Pod / run name = `RUN_ID = {bench}-{query}-{scale}-{gc}-{aqe}-{heap}-{cores}[-r<region>][-n<node>]` (lowercased), e.g. `tpcds-q38-200-shen-false-4g-2`. The Spark History Server app name now matches `RUN_ID` too — the workload jar was updated (2026-06-29) to respect the passed `spark.app.name` instead of hardcoding `screen_<bench>_<query>`.
- GC logs on the cluster PVC: `/var/spark-logs/gc-logs-raw/<timestamp>-<RUN_ID>-(driver|executor-N).log`. HotSpot runs write `-Xlog:gc*` text; OpenJ9 runs write `-Xverbosegclog` XML. The `gc-analyst-hotspot` / `gc-analyst-openj9` agents read these directly (via a GC-analysis skill) — there is no parser script to run.
- Spark event log: `s3a://spark-obj-storage/event-logs` (read via spark-history MCP) — note FAILED apps may not flush an event log.

---

## 4. Operational rules (for the runner agent)

1. **Run only what you were given.** Do not invent queries/flags/values or change the JSON. If a value seems wrong, report back — do not "fix" it.
2. **Sequence:** export env → `kubectl get ns spark` (Active?) → `--dry-run` (sanity) → run with `-y` → `collect-run-artifacts.sh <timestamp>`. Cluster-mode submit BLOCKS until each job finishes; a batch of heavy queries can take hours.
3. **Report back, do not interpret GC behavior:** for each job give `state` (OK/FAIL), driver exit (`exit=`), `appid`, and the GC-log filenames produced (`<ts>-<RUN_ID>-executor-*.log`). Hand GC interpretation to the matching `gc-analyst-hotspot` (g1/zgc/shen) or `gc-analyst-openj9` (gencon/balanced/optthruput/optavgpause).
4. **On FAIL:** read `job-NN-DRIVER.log` in the batch dir and quote the error line(s). Common causes seen:
   - `Could not execute broadcast in 300 secs` → broadcast timeout (AQE off + a large broadcast threshold). Fix knob: set `"broadcast":"off"` (force SMJ) or raise `spark.sql.broadcastTimeout`.
   - `FetchFailedException` / executor lost → executor died (memory/native pressure, often ZGC at small heap).
5. **Parallelism vs capacity:** `-j N` = N concurrent jobs; each job uses its `instances × cores` + driver. Use the per-experiment `node` key when each job should stay on one worker. If pods sit `Pending`, the pinned node is over-committed — lower `-j`, move the run to another node, or reduce per-job resources. Parallel contention can also push a borderline broadcast over its 300s timeout (a serial `-j 1` may pass where `-j 2` failed).
6. **Stopping a run:** killing the local process does NOT stop cluster-mode driver pods. To truly stop: `kubectl delete pod <driver-pod> -n spark` (cascades to its executors).
7. `SCREEN_BIN` env var (points run-parallel at a fake screen) is for TESTING only — never set it for real runs.
