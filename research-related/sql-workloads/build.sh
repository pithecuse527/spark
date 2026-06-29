#!/usr/bin/env bash
# Build the screening job jar and (optionally) upload it for run-screening.sh.
#   ./build.sh           # mvn package -> target/sql-workloads-1.0.jar
#   ./build.sh upload    # also copy to s3a://spark-obj-storage/jars/ (needs awscli + creds)
set -euo pipefail
cd "$(dirname "$0")"

# Refresh bundled query sets from the repo (same .sql files as the image).
RES=../../sql/core/src/test/resources
rm -rf src/main/resources/tpcds src/main/resources/tpch
cp -R "$RES/tpcds" src/main/resources/tpcds
cp -R "$RES/tpch"  src/main/resources/tpch

mvn -q -B package
JAR=target/sql-workloads-1.0.jar
echo "built: $JAR"

if [ "${1:-}" = "upload" ]; then
  : "${OBJ_STORAGE_ENDPOINT:=https://hel1.your-objectstorage.com}"
  # Use the cluster's S3 creds (k8s secret s3-creds) unless AWS creds are already in env.
  # (A laptop's default ~/.aws profile is usually NOT authorized for this Hetzner bucket.)
  if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    : "${KUBECONFIG:?set KUBECONFIG (or AWS_ACCESS_KEY_ID/SECRET) to upload}"
    AWS_ACCESS_KEY_ID="$(kubectl get secret s3-creds -n spark -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"
    AWS_SECRET_ACCESS_KEY="$(kubectl get secret s3-creds -n spark -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  fi
  # aws-cli >= 2.23 adds checksum trailers the Hetzner S3 endpoint rejects -> when_required avoids it.
  AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
  aws --endpoint-url "$OBJ_STORAGE_ENDPOINT" s3 cp "$JAR" \
    s3://spark-obj-storage/jars/sql-workloads-1.0.jar
  echo "uploaded -> s3a://spark-obj-storage/jars/sql-workloads-1.0.jar"
fi
