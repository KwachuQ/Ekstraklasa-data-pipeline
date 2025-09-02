#!/bin/bash
echo "[INFO] >>> Running create_minio_buckets.sh <<<"
# Create MinIO buckets using Airflow's S3Hook
set -e
echo "[INFO] Starting MinIO bucket creation via Airflow S3Hook..."
export AIRFLOW__CORE__LOAD_EXAMPLES=False
python <<EOF
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import sys
buckets = ["bronze", "silver", "gold"]
try:
    s3 = S3Hook(aws_conn_id='minio_s3')
    for bucket in buckets:
        if not s3.check_for_bucket(bucket_name=bucket):
            s3.create_bucket(bucket_name=bucket)
            print(f"[SUCCESS] Bucket {bucket} created")
        else:
            print(f"[INFO] Bucket {bucket} already exists – skipping")
except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit(1)
EOF
echo "[INFO] MinIO bucket creation script finished."
