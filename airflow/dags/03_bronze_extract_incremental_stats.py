from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import json
import os

TOURNAMENT_ID = 202  # Ekstraklasa

def get_today_match_ids(**context):
    """Download match_id of matches loaded today into bronze.raw_matches"""
    print("Searching for matches loaded today...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Download matches from today's date (ingestion_timestamp)
    query = f"""
        SELECT 
            match_id,
            ingestion_timestamp
        FROM bronze.raw_matches
        WHERE tournament_id = {TOURNAMENT_ID}
        AND DATE(ingestion_timestamp) = CURRENT_DATE
        ORDER BY match_id
    """
    
    results = hook.get_records(query)
    
    if not results:
        print("No matches loaded today")
        return {
            'match_ids': [],
            'count': 0,
            'date': str(datetime.now().date())
        }
    
    match_ids = [row[0] for row in results]
    
    print(f"Found {len(match_ids)} matches loaded today:")
    for i, row in enumerate(results[:10], 1):
        print(f"  {i}. match_id={row[0]}, loaded at {row[1]}")
    
    if len(results) > 10:
        print(f"  ... and {len(results) - 10} more")
    
    return {
        'match_ids': match_ids,
        'count': len(match_ids),
        'date': str(datetime.now().date())
    }

def check_existing_statistics(**context):
    """Check which matches already have statistics in bronze.raw_stats"""
    ti = context['ti']
    matches_info = ti.xcom_pull(task_ids='get_today_matches')
    
    if matches_info['count'] == 0:
        print("No matches to check")
        return {
            'missing_match_ids': [],
            'missing_count': 0,
            'already_loaded': 0
        }
    
    match_ids = matches_info['match_ids']
    
    print(f"Checking which of the {len(match_ids)} matches already have statistics...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check which match_id already have statistics
    placeholders = ','.join(['%s'] * len(match_ids))
    query = f"""
        SELECT DISTINCT match_id
        FROM bronze.raw_stats
        WHERE match_id IN ({placeholders})
    """
    
    existing_results = hook.get_records(query, parameters=match_ids)
    existing_ids = {row[0] for row in existing_results}
    
    missing_ids = [mid for mid in match_ids if mid not in existing_ids]
    
    print(f"Summary:")
    print(f"  • Matches from today: {len(match_ids)}")
    print(f"  • Already have statistics: {len(existing_ids)}")
    print(f"  • To fetch: {len(missing_ids)}")
    
    if missing_ids:
        print(f"\n Match IDs to fetch (first 10):")
        for mid in missing_ids[:10]:
            print(f"  • {mid}")
    
    return {
        'missing_match_ids': missing_ids,
        'missing_count': len(missing_ids),
        'already_loaded': len(existing_ids)
    }

def fetch_statistics(**context):
    """Call the ETL script to fetch statistics"""
    ti = context['ti']
    check_info = ti.xcom_pull(task_ids='check_existing')
    
    if check_info['missing_count'] == 0:
        print("All matches already have statistics - nothing to fetch")
        return {
            'fetched': 0,
            'failed': 0,
            'skipped': 0,
            'message': 'no_new_matches'
        }
    
    missing_ids = check_info['missing_match_ids']
    
    print(f"Starting to fetch statistics for {len(missing_ids)} matches...")
    
    # Get RAPIDAPI credentials from environment
    rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
    
    if not rapidapi_key:
        raise AirflowFailException(
            "Missing RAPIDAPI_KEY in environment variables! "
            "Set RAPIDAPI_KEY in docker-compose.yml"
        )
    
    # Pass match_ids as JSON
    match_ids_json = json.dumps(missing_ids)
    
    # Build command that gets season_id from config inside the worker
    command = f"""docker exec etl_worker python -c "
import json
import sys
sys.path.insert(0, '/opt')

from etl.bronze.extractors.statistics_extractor import StatisticsFetcher
from etl.utils.config_loader import get_active_config

# Get season_id from config
config = get_active_config()
season_id = config['season_id']
tournament_id = config['league_id']

match_ids = {match_ids_json}

print(f'Fetching statistics for {{len(match_ids)}} matches...')
print(f'Tournament ID: {{tournament_id}}, Season ID: {{season_id}}')

fetcher = StatisticsFetcher(
    rapidapi_key='{rapidapi_key}',
    tournament_id=tournament_id,
    season_id=season_id
)

result = fetcher.fetch_and_save_statistics(match_ids)

print('STATS_RESULT:' + json.dumps({{
    'fetched': result['successful'],
    'failed': result['failed'],
    'skipped': result['skipped'],
    'total': len(match_ids)
}}))
" """
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=1800  # 30 minutes
        )
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            print(f"stdout: {result.stdout}")
            raise AirflowFailException("ETL script failed")
        
        print(f"Output:\n{result.stdout}")
        
        # Parse result
        for line in result.stdout.split('\n'):
            if line.startswith('STATS_RESULT:'):
                stats_result = json.loads(line.replace('STATS_RESULT:', ''))
                print(f"\nFetch results:")
                print(f"  • Fetched: {stats_result['fetched']}")
                print(f"  • Failed: {stats_result['failed']}")
                print(f"  • Skipped: {stats_result['skipped']}")
                print(f"  • Total: {stats_result['total']}")
                return stats_result
        
        print("No STATS_RESULT: found in output")
        raise AirflowFailException("No valid result from ETL script")
        
    except subprocess.TimeoutExpired:
        print(f"Timeout - process took longer than 30 minutes")
        raise AirflowFailException("ETL script timeout")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        raise

def load_to_raw_stats(**context):
    """Load fetched statistics from MinIO to bronze.raw_stats"""
    ti = context['ti']
    fetch_result = ti.xcom_pull(task_ids='fetch_stats')
    
    if fetch_result.get('fetched', 0) == 0:
        print("No new statistics to load")
        return {'loaded': 0, 'message': 'no_data'}
    
    print(f"Loading {fetch_result['fetched']} statistics from MinIO to PostgreSQL...")
    
    # Use existing DAG for loading
    # Alternatively: direct load from MinIO
    
    command = """docker exec etl_worker python -c "
import json
import psycopg2
from minio import Minio
import uuid
from datetime import datetime

try:
    minio_client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)
    pg_conn = psycopg2.connect(host='postgres', port=5432, database='dwh', user='airflow', password='airflow')
    pg_cur = pg_conn.cursor()
    
    batch_id = str(uuid.uuid4())[:8]
    today = datetime.now().strftime('%Y-%m-%d')
    
    objects = list(minio_client.list_objects('bronze', prefix=prefix, recursive=True))
    json_files = [obj for obj in objects if obj.object_name.endswith('.json')]
    
    print(f'Found {len(json_files)} statistics files from today')
    
    loaded = 0
    duplicates = 0
    
    for obj in json_files:
        try:
            # Extract match_id from filename
            filename = obj.object_name.split('/')[-1]
            match_id = int(filename.replace('match_', '').replace('.json', ''))
            
            # Check if already exists
            pg_cur.execute('SELECT 1 FROM bronze.raw_stats WHERE match_id = %s LIMIT 1', (match_id,))
            if pg_cur.fetchone():
                duplicates += 1
                continue
            
            # Fetch data
            response = minio_client.get_object('bronze', obj.object_name)
            content = response.read().decode('utf-8')
            stats_data = json.loads(content)
            
            # Insert
            pg_cur.execute('''
                INSERT INTO bronze.raw_stats 
                (data, match_id, tournament_id, file_path, batch_id)
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                json.dumps(stats_data),
                match_id,
                202,
                obj.object_name,
                batch_id
            ))
            
            loaded += 1
            
            if loaded % 10 == 0:
                pg_conn.commit()
                print(f'Progress: {loaded} loaded, {duplicates} duplicates')
                
        except Exception as e:
            print(f'Error processing {obj.object_name}: {e}')
            continue
    
    pg_conn.commit()
    pg_conn.close()
    
    print(f'LOAD_RESULT:{loaded}:{duplicates}')
    
except Exception as e:
    print(f'LOAD_ERROR:{str(e)}')
    raise
" """
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        raise AirflowFailException(f"Load failed: {result.stderr}")
    
    print(f"Load output:\n{result.stdout}")
    
    for line in result.stdout.split('\n'):
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            loaded = int(parts[0])
            duplicates = int(parts[1])
            
            print(f"\nLoad complete:")
            print(f"  • Loaded: {loaded}")
            print(f"  • Duplicates: {duplicates}")
            
            return {'loaded': loaded, 'duplicates': duplicates}
        elif line.startswith('LOAD_ERROR:'):
            raise AirflowFailException(f"Load error: {line}")
    
    raise AirflowFailException("No valid result from load script")

def verify_statistics(**context):
    """Verify loaded statistics"""
    print("Verifying loaded statistics...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check today's statistics
    query = f"""
        SELECT 
            COUNT(*) as total_stats,
            COUNT(DISTINCT match_id) as unique_matches
        FROM bronze.raw_stats
        WHERE tournament_id = {TOURNAMENT_ID}
        AND DATE(ingestion_timestamp) = CURRENT_DATE
    """
    
    result = hook.get_first(query)
    
    print(f"Today's statistics:")
    print(f"  • Records: {result[0]}")
    print(f"  • Matches: {result[1]}")
    
    # Check coverage
    coverage_query = f"""
        SELECT 
            COUNT(DISTINCT rm.match_id) as matches_today,
            COUNT(DISTINCT rs.match_id) as with_stats,
            COUNT(DISTINCT rm.match_id) - COUNT(DISTINCT rs.match_id) as missing
        FROM bronze.raw_matches rm
        LEFT JOIN bronze.raw_stats rs ON rm.match_id = rs.match_id
        WHERE rm.tournament_id = {TOURNAMENT_ID}
        AND DATE(rm.ingestion_timestamp) = CURRENT_DATE
    """
    
    coverage = hook.get_first(coverage_query)
    
    print(f"\nCoverage of today's matches:")
    print(f"  • Matches loaded today: {coverage[0]}")
    print(f"  • With statistics: {coverage[1]}")
    print(f"  • Missing statistics: {coverage[2]}")
    
    if coverage[2] > 0:
        print(f"\n WARNING: {coverage[2]} matches still missing statistics!")
    
    return {
        'total_stats': result[0],
        'unique_matches': result[1],
        'missing': coverage[2]
    }

# DAG Configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '03_bronze_extract_incremental_stats',
    default_args=default_args,
    description='Fetch match statistics for matches loaded today to bronze.raw_matches',
    schedule=None,  # Manual trigger - run after bronze_loader_recent_matches
    catchup=False,
    max_active_runs=1,
    tags=['ekstraklasa', 'statistics', 'daily', 'bronze', 'api']
)

# Tasks
start = EmptyOperator(task_id='start', dag=dag)

get_today_matches = PythonOperator(
    task_id='get_today_matches',
    python_callable=get_today_match_ids,
    dag=dag
)

check_existing = PythonOperator(
    task_id='check_existing',
    python_callable=check_existing_statistics,
    dag=dag
)

fetch_stats = PythonOperator(
    task_id='fetch_stats',
    python_callable=fetch_statistics,
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

load_stats = PythonOperator(
    task_id='load_stats',
    python_callable=load_to_raw_stats,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

verify = PythonOperator(
    task_id='verify_stats',
    python_callable=verify_statistics,
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start >> get_today_matches >> check_existing >> fetch_stats >> load_stats >> verify >> end