from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import json

TOURNAMENT_ID = 202  # Ekstraklasa 

def get_last_match_info(**context) -> dict:
    """Download the last match date, season_id, and season_name from Postgres"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Download match with the latest date and its season_id
    query = """
        SELECT 
            start_timestamp::date as match_date,
            season_id,
            season_name,
            start_timestamp
        FROM bronze.full_matches_data
        WHERE unique_tournament_id = 202
        ORDER BY start_timestamp DESC
        LIMIT 1
    """
    
    result = hook.get_first(query)
    
    if result and result[0]:
        info = {
            'last_date': str(result[0]),
            'season_id': result[1],
            'season_name': result[2] if result[2] else '25/26',
            'last_timestamp': str(result[3]),  # Convert to string for JSON serialization
            'tournament_id': TOURNAMENT_ID
        }
        print(f"Znaleziono ostatni mecz:")
        print(f"Data: {info['last_date']}")
        print(f"Tournament ID: {info['tournament_id']}")
        print(f"Season ID: {info['season_id']}")
        print(f"Season Name: {info['season_name']}")
        print(f"Timestamp: {info['last_timestamp']}")
    else:
        # No data - stop the pipeline
        raise ValueError(
            "No data in bronze.full_matches_data for tournament_id=202. "
            "Run full data load first."
        )
    
    return info

def extract_new_matches(**context) -> dict:
    """Download and save only new matches to MinIO"""
    ti = context['ti']
    info = ti.xcom_pull(task_ids='get_last_info')
    
    last_date = info['last_date']
    season_id = info['season_id']
    
    print(f"Searching for new matches after date: {last_date}")
    print(f"Season ID: {season_id}, Tournament ID: {TOURNAMENT_ID}")
    
    command = f"""docker exec etl_worker python -c "
import asyncio
import json
import sys
from etl.bronze.extractors.incremental_extractor import SofascoreIncrementalETL

async def run():
    async with SofascoreIncrementalETL() as etl:
        result = await etl.extract_new_matches(
            tournament_id={TOURNAMENT_ID},
            season_id={season_id},
            last_match_date='{last_date}',
            max_pages=25
        )
    
    # Print errors to stdout for Airflow
    if result['errors']:
        print('ERRORS:', file=sys.stderr)
        for error in result['errors']:
            print(f'  - {{error}}', file=sys.stderr)
    
    output = {{
        'new': result['total_new_matches'],
        'saved': len(result['stored_batches']),
        'pages': result['pages_scanned'],
        'errors': len(result['errors'])
    }}
    
    print('RESULT:' + json.dumps(output))
    
    # Exit with error code if extraction failed
    if result['errors'] and result['total_new_matches'] == 0:
        sys.exit(1)

asyncio.run(run())
" """
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=600
        )
        
        # Print stderr (errors) if present
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")
        
        print(f"Output:\n{result.stdout}")
        
        if result.returncode != 0:
            print(f"⚠️ ETL process failed with exit code {result.returncode}")
            raise Exception(f"ETL extraction failed: check logs above for errors")
        
        # Parse result
        for line in result.stdout.split('\n'):
            if line.startswith('RESULT:'):
                matches_result = json.loads(line.replace('RESULT:', ''))
                print(f"✓ New matches: {matches_result['new']}, saved batches: {matches_result['saved']}")
                
                # Fail if there were errors
                if matches_result['errors'] > 0:
                    raise Exception(
                        f"Extraction completed with {matches_result['errors']} errors. "
                        f"Only {matches_result['new']} matches saved."
                    )
                
                return matches_result
        
        print("⚠️ No RESULT: found in output")
        raise Exception("ETL did not return results")
        
    except subprocess.TimeoutExpired:
        print(f"⚠️ Timeout - process took longer than 10 minutes")
        raise
    except Exception as e:
        print(f"⚠️ Error: {type(e).__name__}: {e}")
        raise

# DAG
with DAG(
    '01_bronze_extract_incremental_matches',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Daily incremental update for Ekstraklasa - saves matches and stats to MinIO bronze bucket',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ekstraklasa', 'incremental', 'bronze']
) as dag:
    
    get_last_info = PythonOperator(
        task_id='get_last_info',
        python_callable=get_last_match_info
    )
    
    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=extract_new_matches,
        execution_timeout=timedelta(minutes=15)
    )
    

    # Task dependencies
    get_last_info >> extract_matches