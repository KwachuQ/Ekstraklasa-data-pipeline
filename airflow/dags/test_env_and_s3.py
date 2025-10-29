from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, platform, psutil, json

def show_env():
    keys = ["RAPIDAPI_KEY", "RAPIDAPI_HOST"]
    data = {k: (os.getenv(k)[:6] + "***" if os.getenv(k) else None) for k in keys}
    mem = psutil.virtual_memory()
    print("ENV:", json.dumps(data))
    print(f"Memory used={mem.used/1024/1024:.1f}MB avail={mem.available/1024/1024:.1f}MB")
    print("Platform:", platform.platform())

with DAG(
    "test_env_only",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["diag"],
) as dag:
    PythonOperator(
        task_id="show_env",
        python_callable=show_env
    )