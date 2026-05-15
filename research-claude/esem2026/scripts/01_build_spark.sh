#!/usr/bin/env bash
# Build Spark distribution tarball from this repo for k8s deployment.
# Output: dist tarball at $SPARK_HOME/spark-*-bin-mascots.tgz

set -euo pipefail

SPARK_HOME="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "${SPARK_HOME}"

echo "[build] SPARK_HOME=${SPARK_HOME}"
echo "[build] branch: $(git rev-parse --abbrev-ref HEAD)"
echo "[build] commit: $(git rev-parse --short HEAD)"

# Adjust profiles as needed. Default targets: Hadoop 3, Kubernetes, no Hive.
./dev/make-distribution.sh \
  --name mascots \
  --tgz \
  --pip \
  -Pkubernetes \
  -Phadoop-3 \
  -Phive \
  -Phive-thriftserver \
  -DskipTests

ls -la "${SPARK_HOME}"/spark-*-bin-mascots.tgz
