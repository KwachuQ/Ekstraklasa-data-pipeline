#!/usr/bin/env bash
set -e

# Create MinIO connection for Airflow
echo "[INFO] >>> Running create_minio_conn.sh <<<"

# Check if connection already exists
if airflow connections get minio_s3 >/dev/null 2>&1; then
    echo "[INFO] Connection minio_s3 already exists – skipping creation"
else
    echo "[INFO] Creating minio_s3 connection..."
    airflow connections add 'minio_s3' \
        --conn-type 'aws' \
        --conn-extra '{"aws_access_key_id": "minio", "aws_secret_access_key": "minio123", "endpoint_url": "http://minio:9000", "region_name": "eu-central-1"}' \
        --conn-login 'minio' \
        --conn-password 'minio123'
    echo "[SUCCESS] Connection minio_s3 created successfully"
fi