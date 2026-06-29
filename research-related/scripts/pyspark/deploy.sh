#!/usr/bin/env bash
# Build the screening image (Mac arm64 -> cluster amd64), push, and roll the pod.
#   ./deploy.sh            # tag v1 (default)
#   ./deploy.sh v2         # any other tag
set -euo pipefail

cd "$(dirname "$0")"
TAG="${1:-v1}"
IMAGE="gihong96/spark-screening:${TAG}"

# Refresh baked query sets from the repo (TPC-DS / TPC-H .sql files).
RES=../../../sql/core/src/test/resources
rm -rf queries && mkdir -p queries
cp -R "$RES/tpcds" queries/tpcds
cp -R "$RES/tpch"  queries/tpch

docker buildx build --platform linux/amd64 -t "$IMAGE" --push .

# If a non-default tag is used, point the deployment at it; else just re-pull (Always).
if [ "$TAG" != "v1" ]; then
  kubectl -n spark set image deploy/screening-jupyter jupyter="$IMAGE"
else
  kubectl -n spark rollout restart deploy/screening-jupyter
fi

kubectl -n spark rollout status deploy/screening-jupyter --timeout=180s
kubectl -n spark get pods -l app=screening-jupyter \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,IMAGE:.spec.containers[0].image'
