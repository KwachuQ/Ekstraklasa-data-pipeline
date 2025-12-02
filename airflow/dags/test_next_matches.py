from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}


def test_next_matches_fetcher(**context):
    """Test NextMatchesFetcher initialization and basic fetch"""
    command = """docker exec etl_worker python -c "
import os
import json
from etl.bronze.extractors.next_matches_extractor import NextMatchesFetcher
from etl.utils.config_loader import get_active_config

api_key = os.getenv('RAPIDAPI_KEY')
if not api_key:
    raise ValueError('RAPIDAPI_KEY not set')

config = get_active_config()
print(f'Testing with tournament_id={config[\\\"league_id\\\"]}, season_id={config[\\\"season_id\\\"]}')

fetcher = NextMatchesFetcher(
    rapidapi_key=api_key,
    tournament_id=config['league_id'],
    season_id=config['season_id']
)

# Test single fetch
result = fetcher.fetch_next_matches(page_index=0)
print(f'Fetch status: {result[\\\"status\\\"]}')
print(f'Tournament ID: {result[\\\"tournament_id\\\"]}')
print(f'Season ID: {result[\\\"season_id\\\"]}')

if result['status'] == 'success':
    events_count = len(result.get('data', {}).get('events', []))
    print(f'Found {events_count} upcoming matches')
    print('NEXT_MATCHES_TEST:SUCCESS')
else:
    print(f'NEXT_MATCHES_TEST:FAILED - {result.get(\\\"error\\\", \\\"Unknown error\\\")}')
" """
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        print(f"Output:\n{result.stdout}")
        
        if result.returncode != 0:
            raise Exception(f"Test failed: {result.stderr}")
            
        if 'NEXT_MATCHES_TEST:SUCCESS' not in result.stdout:
            raise Exception(f"Test did not succeed: {result.stdout}")
            
        print("✅ Next matches fetcher test successful")
        
    except subprocess.TimeoutExpired:
        raise Exception("Test timeout")


with DAG(
    'test_next_matches',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test', 'next_matches'],
) as dag:
    
    test_task = PythonOperator(
        task_id='test_next_matches_fetcher',
        python_callable=test_next_matches_fetcher
    )