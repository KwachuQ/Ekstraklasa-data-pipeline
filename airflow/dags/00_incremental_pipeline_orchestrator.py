"""
Master DAG that orchestrates the entire incremental pipeline
Triggers all incremental DAGs in correct sequence with proper dependencies
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'orchestrate_incremental_pipeline',
    default_args=default_args,
    description='Master DAG - orchestrates full incremental pipeline execution',
    schedule='0 9 * * 2',  # Every Tuesday at 9:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['orchestrator', 'incremental', 'pipeline', 'master']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Step 1: Extract new matches from API → MinIO
    extract_matches = TriggerDagRunOperator(
        task_id='extract_matches',
        trigger_dag_id='01_bronze_extract_incremental_matches',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 2: Load matches from MinIO → PostgreSQL
    load_matches = TriggerDagRunOperator(
        task_id='load_matches',
        trigger_dag_id='02_bronze_load_incremental_matches',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 3: Extract statistics for new matches
    extract_stats = TriggerDagRunOperator(
        task_id='extract_stats',
        trigger_dag_id='03_bronze_extract_incremental_stats',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 4: Load statistics from MinIO → PostgreSQL
    load_stats = TriggerDagRunOperator(
        task_id='load_stats',
        trigger_dag_id='04_bronze_load_incremental_stats',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 5: Stage data in Silver layer
    stage_silver = TriggerDagRunOperator(
        task_id='stage_silver',
        trigger_dag_id='05_silver_stage_incremental',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 6: dbt Silver transformations
    dbt_silver = TriggerDagRunOperator(
        task_id='dbt_silver_transform',
        trigger_dag_id='06_silver_transform_dbt',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    # Step 7: dbt Gold transformations
    dbt_gold = TriggerDagRunOperator(
        task_id='dbt_gold_transform',
        trigger_dag_id='07_gold_transform_dbt',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    end = EmptyOperator(task_id='end')
    
    # Pipeline dependencies
    start >> extract_matches >> load_matches >> extract_stats >> load_stats >> stage_silver >> dbt_silver >> dbt_gold >> end