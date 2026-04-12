#!/usr/bin/env bash
set -euo pipefail

# Wait for MinIO to be ready, then create the test bucket.
# Called by CI before running S3 integration tests.
# Requires a running MinIO container named "minio".

ENDPOINT="${AWS_ENDPOINT:-http://localhost:9000}"
BUCKET="openassay-test"
MAX_RETRIES=30

echo "Waiting for MinIO at ${ENDPOINT}..."
for i in $(seq 1 "$MAX_RETRIES"); do
    if curl -sf "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo "MinIO is ready."
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "ERROR: MinIO did not become ready after ${MAX_RETRIES} attempts."
        exit 1
    fi
    sleep 1
done

# Use the mc binary inside the running MinIO container to create the bucket.
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb --ignore-existing "local/${BUCKET}"
echo "Bucket '${BUCKET}' is ready."
