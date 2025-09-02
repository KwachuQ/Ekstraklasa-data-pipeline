from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def create_buckets():
    # Buckets to create
    buckets = ["bronze", "silver", "gold"]

    # Get MinIO hook (connection id: 'minio_s3')
    s3 = S3Hook(aws_conn_id='minio_s3')

    for bucket in buckets:
        # Check if bucket exists
        if not s3.check_for_bucket(bucket_name=bucket):
            s3.create_bucket(bucket_name=bucket)
            print(f"Bucket {bucket} created")
        else:
            print(f"Bucket {bucket} already exists – skipping")

# DAG definition
with DAG(
    dag_id="init_minio_buckets",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manually triggered or on first deploy
    catchup=False,
    tags=["init", "minio"],
) as dag:

    create_buckets_task = PythonOperator(
        task_id="create_buckets_task",
        python_callable=create_buckets,
    )
