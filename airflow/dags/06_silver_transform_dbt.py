from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='06_silver_transform_dbt',
    default_args=default_args,
    description='Run all dbt models in silver layer',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'silver', 'transformation'],
) as dag:

    # Run dbt debug to test connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='docker exec dbt bash -c "cd /opt/dbt/project && dbt debug --profiles-dir /opt/dbt/profiles"',
    )

    # Run all silver models
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='docker exec dbt bash -c "cd /opt/dbt/project && dbt run --select silver.* --profiles-dir /opt/dbt/profiles"',
    )

    # Define task dependencies
    dbt_debug >> dbt_run_silver