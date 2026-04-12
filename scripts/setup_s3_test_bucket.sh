#!/usr/bin/env bash
set -euo pipefail

# Wait for MinIO to be ready, then create the test bucket.
# Called by CI before running S3 integration tests.

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

# Create bucket using the S3 API (no mc binary needed)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X PUT "${ENDPOINT}/${BUCKET}" \
    -u "minioadmin:minioadmin")

if [ "$STATUS" = "200" ] || [ "$STATUS" = "409" ]; then
    echo "Bucket '${BUCKET}' is ready (HTTP ${STATUS})."
else
    echo "ERROR: Failed to create bucket (HTTP ${STATUS})."
    exit 1
fi
