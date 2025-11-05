from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import json

TOURNAMENT_ID = 202  # Ekstraklasa

def get_last_loaded_match_ids(**context):
    """Pobierz match_id meczów, które już mają statystyki w raw_stats"""
    print("📅 Checking existing statistics in bronze.raw_stats...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get all match_ids that already have statistics
    query = f"""
        SELECT 
            match_id,
            COUNT(*) as stats_count
        FROM bronze.raw_stats
        WHERE tournament_id = {TOURNAMENT_ID}
        GROUP BY match_id
        ORDER BY match_id DESC
        LIMIT 10
    """
    
    result = hook.get_records(query)
    
    if result:
        print(f"✅ Found {len(result)} matches with statistics (showing last 10)")
        for row in result[:5]:
            print(f"  • match_id: {row[0]}, stats: {row[1]}")
    else:
        print("⚠️ No statistics found in database - will load all available")
    
    # Get total count
    count_query = f"SELECT COUNT(DISTINCT match_id) FROM bronze.raw_stats WHERE tournament_id = {TOURNAMENT_ID}"
    total = hook.get_first(count_query)[0]
    
    print(f"📊 Total Ekstraklasa matches with statistics: {total}")
    
    # Get all existing match_ids for filtering
    all_ids_query = f"SELECT DISTINCT match_id FROM bronze.raw_stats WHERE tournament_id = {TOURNAMENT_ID}"
    existing_ids = [row[0] for row in hook.get_records(all_ids_query)]
    
    return {
        'existing_match_ids': existing_ids,
        'total_with_stats': total
    }

def get_matches_needing_statistics(**context):
    """Pobierz match_id meczów z full_matches_data, które nie mają statystyk"""
    print("🔍 Finding matches that need statistics...")
    
    ti = context['ti']
    stats_info = ti.xcom_pull(task_ids='get_existing_stats')
    existing_ids = stats_info['existing_match_ids']
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get all Ekstraklasa matches from full_matches_data
    query = f"""
        SELECT 
            match_id,
            start_timestamp,
            season_id
        FROM bronze.full_matches_data
        WHERE unique_tournament_id = {TOURNAMENT_ID}
        AND status_type = 'finished'
        ORDER BY start_timestamp DESC
        LIMIT 1000
    """
    
    all_matches = hook.get_records(query)
    
    print(f"📊 Total finished Ekstraklasa matches: {len(all_matches)}")
    print(f"📊 Matches already with statistics: {len(existing_ids)}")
    
    # Filter out matches that already have statistics
    missing_matches = [
        {'match_id': row[0], 'date': str(row[1]), 'season_id': row[2]} 
        for row in all_matches 
        if row[0] not in existing_ids
    ]
    
    print(f"📊 Matches needing statistics: {len(missing_matches)}")
    
    if missing_matches:
        print("Sample matches needing stats:")
        for match in missing_matches[:5]:
            print(f"  • match_id: {match['match_id']}, date: {match['date'][:10]}, season: {match['season_id']}")
    
    return {
        'missing_matches': missing_matches,
        'missing_count': len(missing_matches)
    }

def check_statistics_in_minio(**context):
    """Sprawdź dostępne statystyki w MinIO dla brakujących meczów"""
    ti = context['ti']
    matches_info = ti.xcom_pull(task_ids='get_missing_matches')
    missing_matches = matches_info['missing_matches']
    
    if not missing_matches:
        print("✅ All matches already have statistics")
        return {
            'available_files': [],
            'available_count': 0,
            'season_folders': []
        }
    
    print(f"🔍 Checking MinIO for statistics of {len(missing_matches)} matches...")
    
    # Get match_ids to check
    match_ids_str = ','.join([str(m['match_id']) for m in missing_matches])
    
    cmd = f'''docker exec etl_worker python -c "
from minio import Minio
import json

client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)

# Match IDs we're looking for
needed_match_ids = {{{match_ids_str}}}

print(f'Looking for statistics for {{len(needed_match_ids)}} matches...')

# Get all statistics files from season folders
objects = list(client.list_objects('bronze', prefix='match_statistics/', recursive=True))
json_files = [obj for obj in objects if obj.object_name.endswith('.json') and 'match_' in obj.object_name]

print(f'Found {{len(json_files)}} total statistics files in MinIO')

# Extract match_ids from filenames and check availability
available_files = []
season_folders = set()

for obj in json_files:
    try:
        filename = obj.object_name.split('/')[-1]
        if filename.startswith('match_') and filename.endswith('.json'):
            match_id = int(filename.replace('match_', '').replace('.json', ''))
            
            if match_id in needed_match_ids:
                available_files.append({{
                    'path': obj.object_name,
                    'match_id': match_id
                }})
                
                # Extract season folder
                parts = obj.object_name.split('/')
                if len(parts) > 1:
                    season_folders.add(parts[1])
    except Exception as e:
        continue

print(f'Found {{len(available_files)}} statistics files for needed matches')
print(f'Season folders: {{sorted(season_folders)}}')

if available_files:
    print('Sample available files:')
    for f in available_files[:5]:
        print(f'  - {{f[\\"path\\"]}} (match_id={{f[\\"match_id\\"]}})')

result = {{
    'available_files': available_files,
    'available_count': len(available_files),
    'season_folders': sorted(season_folders),
    'total_needed': len(needed_match_ids)
}}

print('RESULT:' + json.dumps(result))
"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"MinIO check failed: {result.stderr}")
    
    print("\n📋 MinIO Output:")
    print(result.stdout)
    
    # Parse result
    for line in result.stdout.strip().split('\n'):
        if line.startswith('RESULT:'):
            minio_data = json.loads(line.replace('RESULT:', ''))
            
            print(f"\n📊 Summary:")
            print(f"  • Matches needing stats: {minio_data['total_needed']}")
            print(f"  • Statistics available in MinIO: {minio_data['available_count']}")
            print(f"  • Season folders: {', '.join(minio_data['season_folders'])}")
            
            if minio_data['available_count'] == 0:
                print("  • ℹ️ Brak nowych statystyk do załadowania")
            else:
                print(f"  • ✅ Ready to load {minio_data['available_count']} statistics files")
            
            return minio_data
    
    raise Exception("No valid result from MinIO check")

def load_incremental_statistics(**context):
    """Ładuje tylko nowe statystyki z MinIO do raw_stats"""
    ti = context['ti']
    minio_info = ti.xcom_pull(task_ids='check_statistics')
    
    if minio_info['available_count'] == 0:
        print("ℹ️ Brak nowych statystyk do załadowania")
        return {'processed': 0, 'message': 'no_new_data'}
    
    available_files = minio_info['available_files']
    available_count = minio_info['available_count']
    
    print(f"🚀 Loading {available_count} statistics files from MinIO...")
    
    # Convert files list to JSON for script
    files_json = json.dumps(available_files)
    
    # Use template string to avoid f-string escaping issues
    script_template = '''import json
import psycopg2
from minio import Minio
import uuid

try:
    minio_client = Minio("minio:9000", access_key="minio", secret_key="minio123", secure=False)
    pg_conn = psycopg2.connect(host="postgres", port=5432, database="dwh", user="airflow", password="airflow")
    pg_cur = pg_conn.cursor()

    batch_id = str(uuid.uuid4())[:8]
    
    files_to_load = FILES_JSON_PLACEHOLDER
    
    print(f"Processing {len(files_to_load)} statistics files...")

    processed_files = 0
    processed_records = 0
    skipped_duplicates = 0

    for file_info in files_to_load:
        file_path = None
        try:
            file_path = file_info["path"]
            match_id = file_info["match_id"]
            
            response = minio_client.get_object("bronze", file_path)
            content = response.read().decode("utf-8")
            stats_data = json.loads(content)
            
            # Check if this match already has statistics
            pg_cur.execute(
                "SELECT 1 FROM bronze.raw_stats WHERE match_id = %s LIMIT 1", 
                (match_id,)
            )
            
            if pg_cur.fetchone():
                skipped_duplicates += 1
                continue
            
            # Insert statistics
            pg_cur.execute("""
                INSERT INTO bronze.raw_stats 
                (data, match_id, tournament_id, file_path, batch_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                json.dumps(stats_data), 
                match_id,
                TOURNAMENT_ID_PLACEHOLDER,
                file_path,
                batch_id
            ))
            
            processed_files += 1
            processed_records += 1
            
            # Commit every 50 files
            if processed_files % 50 == 0:
                pg_conn.commit()
                print(f"Progress: {processed_files}/{len(files_to_load)} files, {processed_records} records, {skipped_duplicates} duplicates")
                
        except Exception as e:
            error_file = file_path if file_path else "unknown"
            print(f"Error processing {error_file}: {str(e)}")
            continue

    pg_conn.commit()
    print(f"LOAD_RESULT:{processed_files}:{processed_records}:{skipped_duplicates}")
    pg_conn.close()
    
except Exception as e:
    print(f"LOAD_ERROR:{str(e)}")
    raise
'''
    
    # Replace placeholders
    script_content = script_template.replace('FILES_JSON_PLACEHOLDER', files_json)
    script_content = script_content.replace('TOURNAMENT_ID_PLACEHOLDER', str(TOURNAMENT_ID))
    
    write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_stats_incremental.py << "SCRIPT_END"
{script_content}
SCRIPT_END' '''
    
    write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
    
    if write_result.returncode != 0:
        raise Exception(f"Failed to write script: {write_result.stderr}")
    
    exec_cmd = 'docker exec etl_worker python /tmp/load_stats_incremental.py'
    result = subprocess.run(exec_cmd, shell=True, capture_output=True, text=True, timeout=1800)
    
    if result.returncode != 0:
        raise Exception(f"Load failed: {result.stderr}\nStdout: {result.stdout}")
    
    for line in result.stdout.strip().split('\n'):
        print(line)
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            if len(parts) >= 3:
                files_processed = int(parts[0])
                records_loaded = int(parts[1])
                duplicates_skipped = int(parts[2])
                
                print(f"\n✅ Load Complete:")
                print(f"  • Files: {files_processed}")
                print(f"  • New statistics: {records_loaded}")
                print(f"  • Duplicates: {duplicates_skipped}")
                
                return {
                    'files_processed': files_processed,
                    'records_loaded': records_loaded,
                    'duplicates_skipped': duplicates_skipped
                }
        elif line.startswith('LOAD_ERROR:'):
            raise Exception(f"Load error: {line.replace('LOAD_ERROR:', '')}")
    
    raise Exception("No valid result from load script")

def verify_statistics_load(**context):
    """Weryfikuje załadowane statystyki"""
    print("🔍 Verifying statistics load...")
    
    cmd = f'''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT batch_id) as batches
        FROM bronze.raw_stats
        WHERE tournament_id = {TOURNAMENT_ID};"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Verification failed: {result.stderr}")
    
    print("✅ Statistics table state:")
    print(result.stdout)
    
    # Check coverage
    cmd2 = f'''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(DISTINCT fm.match_id) as total_finished_matches,
            COUNT(DISTINCT rs.match_id) as matches_with_stats,
            COUNT(DISTINCT fm.match_id) - COUNT(DISTINCT rs.match_id) as missing_stats
        FROM bronze.full_matches_data fm
        LEFT JOIN bronze.raw_stats rs ON fm.match_id = rs.match_id
        WHERE fm.unique_tournament_id = {TOURNAMENT_ID}
        AND fm.status_type = 'finished';"'''
    
    result2 = subprocess.run(cmd2, shell=True, capture_output=True, text=True)
    
    if result2.returncode == 0:
        print("\n📊 Coverage:")
        print(result2.stdout)
    
    return "verification_complete"

def trigger_dbt_stats_incremental(**context):
    """Uruchamia dbt incremental refresh dla statystyk"""
    print("🔄 Triggering dbt stats incremental refresh...")
    
    cmd = 'docker exec dbt bash -c "cd /opt/dbt/project && dbt run --models bronze.stats_full_data_incremental"'
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        print(f"❌ dbt stderr: {result.stderr}")
        raise Exception("dbt stats incremental refresh failed")
    
    print("✅ dbt stats incremental refresh completed")
    print(result.stdout)
    
    # Verify counts
    verify_cmd = '''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total_stats,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT period) as unique_periods
        FROM bronze.full_stats_data;"'''
    
    verify_result = subprocess.run(verify_cmd, shell=True, capture_output=True, text=True)
    
    if verify_result.returncode == 0:
        print("\n📊 Stats table after refresh:")
        print(verify_result.stdout)
    
    return "dbt_stats_incremental_complete"

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
    '04_bronze_load_incremental_stats',
    default_args=default_args,
    description='Incremental load of match statistics from MinIO to PostgreSQL raw_stats',
    schedule=None,  
    catchup=False,
    max_active_runs=1,
    tags=['ekstraklasa', 'statistics', 'incremental', 'bronze', 'postgresql', 'minio', 'manual']
)

# Tasks
start_task = EmptyOperator(task_id='start', dag=dag)

get_existing_stats_task = PythonOperator(
    task_id='get_existing_stats',
    python_callable=get_last_loaded_match_ids,
    dag=dag
)

get_missing_matches_task = PythonOperator(
    task_id='get_missing_matches',
    python_callable=get_matches_needing_statistics,
    dag=dag
)

check_statistics_task = PythonOperator(
    task_id='check_statistics',
    python_callable=check_statistics_in_minio,
    dag=dag
)

load_statistics_task = PythonOperator(
    task_id='load_statistics',
    python_callable=load_incremental_statistics,
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

dbt_refresh_stats_task = PythonOperator(
    task_id='trigger_dbt_incremental',
    python_callable=trigger_dbt_stats_incremental,
    dag=dag
)

verify_task = PythonOperator(
    task_id='verify_statistics',
    python_callable=verify_statistics_load,
    dag=dag
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start_task >> get_existing_stats_task >> get_missing_matches_task >> check_statistics_task >> load_statistics_task >> dbt_refresh_stats_task >> verify_task >> end_task