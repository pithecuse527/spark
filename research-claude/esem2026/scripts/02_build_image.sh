#!/usr/bin/env bash
# Build and push the Spark docker image used by k8s executors.
# Requires: spark dist tarball produced by 01_build_spark.sh.
#
# Env:
#   REGISTRY=<your.registry/spark>   target image prefix
#   TAG=<tag>                        default: mascots-$(date +%Y%m%d-%H%M)

set -euo pipefail

REGISTRY="${REGISTRY:?set REGISTRY (e.g. ghcr.io/your-user)}"
TAG="${TAG:-mascots-$(date +%Y%m%d-%H%M)}"

SPARK_HOME="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "${SPARK_HOME}"

DIST_DIR="$(ls -d dist 2>/dev/null || true)"
if [[ -z "${DIST_DIR}" ]]; then
  echo "no dist/ folder. run 01_build_spark.sh first." >&2
  exit 1
fi

cd "${DIST_DIR}"
./bin/docker-image-tool.sh -r "${REGISTRY}" -t "${TAG}" build
./bin/docker-image-tool.sh -r "${REGISTRY}" -t "${TAG}" push

echo
echo "image: ${REGISTRY}/spark:${TAG}"
echo "set in driver/executor pod templates."
