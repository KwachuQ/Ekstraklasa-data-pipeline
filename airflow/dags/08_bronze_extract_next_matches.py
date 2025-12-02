from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import subprocess
import json
import os
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_next_matches(**context):
    """Fetch next matches for the tournament"""
    logging.info("Fetching next matches from tournament...")
    
    rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
    if not rapidapi_key:
        raise AirflowFailException("RAPIDAPI_KEY not set in environment")
    
    command = f"""docker exec etl_worker python -c "
import json
import sys
sys.path.insert(0, '/opt')

from etl.bronze.extractors.next_matches_extractor import NextMatchesFetcher
from etl.utils.config_loader import get_active_config

config = get_active_config()

print(f'Fetching next matches for tournament_id={{config[\\\"league_id\\\"]}}, season_id={{config[\\\"season_id\\\"]}}...')

fetcher = NextMatchesFetcher(
    rapidapi_key='{rapidapi_key}',
    tournament_id=config['league_id'],
    season_id=config['season_id']
)

result = fetcher.fetch_and_save_next_matches(
    max_pages=2,
    skip_existing=True
)

print('FETCH_RESULT:' + json.dumps({{
    'successful': result['successful'],
    'failed': result['failed'],
    'skipped': result['skipped'],
    'extraction_date': result['extraction_date']
}}))
" """
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        logging.info(f"Output:\n{result.stdout}")
        
        if result.returncode != 0:
            raise AirflowFailException(f"Fetch failed: {result.stderr}")
        
        for line in result.stdout.split('\n'):
            if line.startswith('FETCH_RESULT:'):
                fetch_result = json.loads(line.replace('FETCH_RESULT:', ''))
                logging.info(
                    f"✓ Successful: {fetch_result['successful']}, "
                    f"Failed: {fetch_result['failed']}, "
                    f"Skipped: {fetch_result['skipped']}, "
                    f"Date: {fetch_result['extraction_date']}"
                )
                return fetch_result
                
        raise AirflowFailException("No fetch result in output")
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException("Fetch timeout (5 min)")


with DAG(
    '08_bronze_extract_next_matches',
    default_args=default_args,
    description='Extract next matches for tournament',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    catchup=False,
    tags=['bronze', 'next_matches', 'extraction'],
) as dag:
    
    fetch_matches = PythonOperator(
        task_id='fetch_next_matches',
        python_callable=fetch_next_matches
    )