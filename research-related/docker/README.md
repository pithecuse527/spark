# Container images for the GC study

Two images, both `linux/amd64` (the k3s cluster is amd64; build with `--platform linux/amd64` even from an Apple-Silicon Mac):

| tag | JVM | how it's built |
|---|---|---|
| `gihong96/spark-screening:v1` | Eclipse Temurin 21 (HotSpot) | `Dockerfile.hotspot` — `apache/spark:4.1.2` + S3A jars (+ optional Jupyter); provenance below |
| `gihong96/spark-screening:semeru-v1` | Eclipse OpenJ9 21 (IBM Semeru) | overlay: v1 + Semeru JDK only (no Spark rebuild) — `Dockerfile.semeru` here |

`run-screening.sh` auto-selects the image from the GC value (HotSpot `g1/zgc/shen` → v1; OpenJ9 `gencon/balanced/optthruput/optavgpause` → semeru-v1).

---

## How `:v1` (HotSpot) was made — reconstructed from `docker buildx imagetools inspect`

**No Spark source build.** It layers, bottom to top:
1. **Base `eclipse-temurin:21-jdk`** (Ubuntu 22.04, `JAVA_HOME=/opt/java/openjdk`, Temurin 21.0.11+10).
2. **Apache Spark 4.1.2 official k8s Dockerfile** (`spark/kubernetes/dockerfiles/spark/Dockerfile`): creates `spark` user uid 185, installs tini/krb5/nss/procps, downloads + **gpg-verifies the prebuilt `spark-4.1.2-bin-hadoop3.tgz`** into `/opt/spark`, `ENTRYPOINT /opt/entrypoint.sh`, `SPARK_HOME=/opt/spark`.
3. **Custom additions:**
   - `apt install python3 python3-pip` + `pip install jupyterlab jupyter-server-proxy`.
   - **S3A jars into `/opt/spark/jars`** (this is why `s3a://` works):
     ```
     hadoop-aws-3.4.2.jar          (repo1.maven.org/.../hadoop-aws/3.4.2)
     bundle-2.29.52.jar            (repo1.maven.org/software/amazon/awssdk/bundle/2.29.52)
     ```
   - `COPY screening.py tpc_pyspark.py run_screening.ipynb run_nb_cells.py log4j2-aqe.properties` → `/work`, `COPY queries` → `/work/queries`, `WORKDIR /work`, `CMD jupyter lab …` (the PySpark/notebook path; the Scala-jar runs load the workload from `s3a://…/jars/sql-workloads-1.0.jar` at submit time, so the jar is NOT baked in).

The base layers (1–2) are exactly the published `apache/spark:4.1.2` image (verified: amd64 config has `JAVA_HOME=/opt/java/openjdk`, `JAVA_VERSION=jdk-21.0.11+10`, `SPARK_HOME=/opt/spark`, entrypoint `/opt/entrypoint.sh`, user `spark`). So `:v1 = apache/spark:4.1.2 + S3A + Jupyter + /work files`.

### Reproducing `:v1` (Dockerfile.hotspot)
`Dockerfile.hotspot` rebuilds the parts the GC study needs — `apache/spark:4.1.2` + the S3A jars — and optionally the Jupyter layer. It does NOT bake the `/work` notebook files (only used by the Jupyter workflow, not by `run-screening.sh`).
```bash
cd research-related/docker
# push to a REPRO tag — do not clobber the working :v1
docker buildx build --platform linux/amd64 -f Dockerfile.hotspot \
  -t gihong96/spark-screening:v1-repro --push .
# add --build-arg WITH_JUPYTER=1 to also install python3 + jupyterlab (matches v1)
```
The working `:v1` already exists in the registry, so you normally don't rebuild it — this file just captures the recipe for reuse/modification.

To recover the `/work` notebook files baked into the published `:v1`:
```bash
docker create --name v1tmp gihong96/spark-screening:v1
docker cp v1tmp:/work ./v1-work && docker rm v1tmp
```

---

## Building the OpenJ9 / Semeru variant (`:semeru-v1`)

`Dockerfile.semeru` overlays the Semeru OpenJ9 JDK onto the known-good `:v1` and just re-points `JAVA_HOME` — keeps all of v1's Spark/S3A/entrypoint, swaps only the runtime, no rebuild.

```bash
cd research-related/docker

# build + push (amd64). Needs: docker + dockerhub login with push rights to gihong96.
docker buildx build --platform linux/amd64 \
  -f Dockerfile.semeru -t gihong96/spark-screening:semeru-v1 --push .
```

### Validate on the cluster (java is OpenJ9, GC policies accepted, Spark intact)
```bash
export KUBECONFIG=/Users/ji/kubeconfigs/hetzner-spark.yaml
kubectl run semcheck -i --rm --image=gihong96/spark-screening:semeru-v1 -n spark --restart=Never --command -- sh -c '
  java -version 2>&1
  for p in gencon balanced optthruput optavgpause; do java -Xgcpolicy:$p -version >/dev/null 2>&1 && echo "$p OK" || echo "$p FAIL"; done
  java -Xgcpolicy:balanced -Xverbosegclog:/tmp/gc.log -version >/dev/null 2>&1; head -1 /tmp/gc.log
  echo SPARK_HOME=$SPARK_HOME; ls $SPARK_HOME/bin/spark-submit'
```
Expected: `Eclipse OpenJ9 VM 21…`, all 4 policies `OK`, `<?xml …` from the verbose GC log, `spark-submit` present.

### Notes / gotchas
- **Always `--platform linux/amd64`** — the Mac is arm64, the cluster is amd64; a native build won't schedule.
- Semeru base puts its JDK at `/opt/java/openjdk`; we copy it to `/opt/java/semeru` and set `JAVA_HOME` there to avoid mixing with v1's Temurin files.
- OpenJ9 GC log is **XML via `-Xverbosegclog`** (not HotSpot `-Xlog:gc*`); analyzed by the `gc-analyst-openj9` agent via a GC-analysis skill (no parser script).
- To cut a new tag, bump `:semeru-v1` → `:semeru-v2` etc. and update the `IMAGE` in `run-screening.sh`.
