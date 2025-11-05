from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import json

TOURNAMENT_ID = 202  # Ekstraklasa

def get_last_match_info(**context):
    """Pobierz datę i season_id ostatniego meczu z bronze.full_matches_data"""
    print("📅 Checking last match info in bronze.full_matches_data...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get last match date and season_id
    query = f"""
        SELECT 
            LEFT(start_timestamp::text, 10) as last_date,
            season_id,
            COUNT(*) as total_matches
        FROM bronze.full_matches_data
        WHERE unique_tournament_id = {TOURNAMENT_ID}
        GROUP BY LEFT(start_timestamp::text, 10), season_id
        ORDER BY LEFT(start_timestamp::text, 10) DESC, season_id DESC
        LIMIT 1
    """
    
    result = hook.get_first(query)
    
    if not result or not result[0]:
        error_msg = (
            f"❌ BŁĄD: Brak danych dla unique_tournament_id={TOURNAMENT_ID} w bronze.full_matches_data.\n"
            f"   Akcja wymagana: Najpierw załaduj dane historyczne."
        )
        print(error_msg)
        raise AirflowFailException(error_msg)
    
    last_date = result[0]
    season_id = result[1]
    total_in_db = result[2]
    
    print(f"✅ Last match info:")
    print(f"  • Date: {last_date}")
    print(f"  • Season ID: {season_id}")
    print(f"  • Matches on that date: {total_in_db}")
    
    # Validate date format
    try:
        datetime.strptime(last_date, '%Y-%m-%d')
    except ValueError as e:
        error_msg = f"❌ BŁĄD: Nieprawidłowy format daty: '{last_date}'. Szczegóły: {e}"
        print(error_msg)
        raise AirflowFailException(error_msg)
    
    return {
        'last_date': last_date,
        'season_id': season_id,
        'total_in_db': total_in_db
    }

def check_new_data_in_minio(**context):
    """Sprawdź nowe dane w MinIO po ostatniej dacie dla danego season_id"""
    ti = context['ti']
    db_info = ti.xcom_pull(task_ids='get_last_info')
    last_date = db_info['last_date']
    season_id = db_info['season_id']
    
    print(f"🔍 Looking for matches after {last_date} in season {season_id}...")
    
    # MinIO uses tournament_id (not unique_tournament_id) in path
    prefix = f'matches/tournament_id={TOURNAMENT_ID}/season_id={season_id}/'
    print(f"📂 MinIO prefix: '{prefix}'")
    
    cmd = f'''docker exec etl_worker python -c "
from minio import Minio
import json
from datetime import datetime

client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)

# Get all match files with specific prefix
objects = list(client.list_objects('bronze', prefix='{prefix}', recursive=True))
ndjson_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]

print(f'Found {{len(ndjson_files)}} .ndjson files in prefix: {prefix}')

# Show first 5 files
if ndjson_files:
    print('Sample files:')
    for i, obj in enumerate(ndjson_files[:5]):
        print(f'  {{i+1}}. {{obj.object_name}}')

# Filter files by date partition
cutoff_date = datetime.strptime('{last_date}', '%Y-%m-%d').date()
print(f'Cutoff date: {{cutoff_date}}')

new_files = []
old_files = []

for obj in ndjson_files:
    if '/date=' in obj.object_name:
        try:
            date_part = obj.object_name.split('/date=')[1].split('/')[0]
            file_date = datetime.strptime(date_part, '%Y-%m-%d').date()
            
            if file_date > cutoff_date:
                new_files.append(obj.object_name)
            else:
                old_files.append(obj.object_name)
        except Exception as e:
            print(f'Error parsing date from {{obj.object_name}}: {{e}}')
            continue

print(f'Files AFTER {{cutoff_date}}: {{len(new_files)}}')
print(f'Files BEFORE/ON {{cutoff_date}}: {{len(old_files)}}')

if new_files:
    print('New files (first 5):')
    for f in new_files[:5]:
        date_part = f.split('/date=')[1].split('/')[0]
        filename = f.split('/')[-1]
        print(f'  - {{date_part}}: {{filename}}')

result = {{
    'total_files': len(ndjson_files),
    'new_files': len(new_files),
    'old_files': len(old_files),
    'new_file_paths': new_files[:10]
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
            print(f"  • Total files: {minio_data['total_files']}")
            print(f"  • New files after {last_date}: {minio_data['new_files']}")
            
            if minio_data['new_files'] == 0:
                print("  • ℹ️ Brak meczów do załadowania")
            else:
                estimated = minio_data['new_files'] * 30
                print(f"  • Estimated new records: ~{estimated}")
            
            return {
                **minio_data,
                'last_date': last_date,
                'season_id': season_id,
                'estimated_records': estimated if minio_data['new_files'] > 0 else 0
            }
    
    raise Exception("No valid result from MinIO check")

def load_incremental_data(**context):
    """Ładuje tylko nowe mecze z MinIO do raw_matches"""
    ti = context['ti']
    minio_info = ti.xcom_pull(task_ids='check_new_data')
    
    if minio_info['new_files'] == 0:
        print("ℹ️ Brak meczów do załadowania - skipping")
        return {'processed': 0, 'message': 'no_new_data'}
    
    last_date = minio_info['last_date']
    season_id = minio_info['season_id']
    new_files_count = minio_info['new_files']
    
    # MinIO uses tournament_id in path
    prefix = f'matches/tournament_id={TOURNAMENT_ID}/season_id={season_id}/'
    
    print(f"🚀 Loading {new_files_count} new files (after {last_date})...")
    print(f"📂 Prefix: '{prefix}'")
    
    script_content = f'''import json
import psycopg2
from minio import Minio
import uuid
from datetime import datetime

try:
    minio_client = Minio("minio:9000", access_key="minio", secret_key="minio123", secure=False)
    pg_conn = psycopg2.connect(host="postgres", port=5432, database="dwh", user="airflow", password="airflow")
    pg_cur = pg_conn.cursor()

    batch_id = str(uuid.uuid4())[:8]
    cutoff_date = datetime.strptime("{last_date}", "%Y-%m-%d").date()

    objects = list(minio_client.list_objects("bronze", prefix="{prefix}", recursive=True))
    ndjson_files = [obj for obj in objects if obj.object_name.endswith(".ndjson")]

    new_files = []
    for obj in ndjson_files:
        if "/date=" in obj.object_name:
            try:
                date_part = obj.object_name.split("/date=")[1].split("/")[0]
                file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                if file_date > cutoff_date:
                    new_files.append(obj)
            except:
                continue

    print(f"Processing {{len(new_files)}} files...")

    processed_matches = 0
    processed_files = 0
    skipped_duplicates = 0

    for obj in new_files:
        try:
            response = minio_client.get_object("bronze", obj.object_name)
            content = response.read().decode("utf-8")
            
            for line in content.strip().split("\\n"):
                if line.strip():
                    try:
                        match_data = json.loads(line)
                        match_id = match_data.get("id")
                        unique_tournament_id = match_data.get("tournament", {{}}).get("uniqueTournament", {{}}).get("id")
                        season_id = match_data.get("season", {{}}).get("id")
                        
                        if unique_tournament_id == {TOURNAMENT_ID} and match_id:
                            pg_cur.execute("SELECT 1 FROM bronze.raw_matches WHERE match_id = %s LIMIT 1", (match_id,))
                            if pg_cur.fetchone():
                                skipped_duplicates += 1
                                continue
                            
                            pg_cur.execute("""
                                INSERT INTO bronze.raw_matches 
                                (data, tournament_id, season_id, match_id, file_path, batch_id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                json.dumps(match_data), 
                                unique_tournament_id, 
                                season_id, 
                                match_id,
                                obj.object_name,
                                batch_id
                            ))
                            processed_matches += 1
                    except Exception as e:
                        continue
            
            processed_files += 1
            
            if processed_files % 10 == 0:
                pg_conn.commit()
                print(f"Progress: {{processed_files}}/{{len(new_files)}} files, {{processed_matches}} matches, {{skipped_duplicates}} duplicates")
                
        except Exception as e:
            print(f"Error: {{obj.object_name}}: {{str(e)}}")
            continue

    pg_conn.commit()
    print(f"LOAD_RESULT:{{processed_files}}:{{processed_matches}}:{{skipped_duplicates}}")
    pg_conn.close()
    
except Exception as e:
    print(f"LOAD_ERROR:{{str(e)}}")
    raise
'''
    
    write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_incremental.py << "SCRIPT_END"
{script_content}
SCRIPT_END' '''
    
    write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
    
    if write_result.returncode != 0:
        raise Exception(f"Failed to write script: {write_result.stderr}")
    
    exec_cmd = 'docker exec etl_worker python /tmp/load_incremental.py'
    result = subprocess.run(exec_cmd, shell=True, capture_output=True, text=True, timeout=1800)
    
    if result.returncode != 0:
        raise Exception(f"Load failed: {result.stderr}\nStdout: {result.stdout}")
    
    for line in result.stdout.strip().split('\n'):
        print(line)
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            if len(parts) >= 3:
                files_processed = int(parts[0])
                matches_loaded = int(parts[1])
                duplicates_skipped = int(parts[2])
                
                print(f"\n✅ Load Complete:")
                print(f"  • Files: {files_processed}")
                print(f"  • New matches: {matches_loaded}")
                print(f"  • Duplicates: {duplicates_skipped}")
                
                return {
                    'files_processed': files_processed,
                    'matches_loaded': matches_loaded,
                    'duplicates_skipped': duplicates_skipped
                }
        elif line.startswith('LOAD_ERROR:'):
            raise Exception(f"Load error: {line.replace('LOAD_ERROR:', '')}")
    
    raise Exception("No valid result from load script")

def trigger_dbt_incremental(**context):
    """Uruchamia dbt incremental refresh"""
    print("🔄 Triggering dbt incremental refresh...")
    
    cmd = 'docker exec dbt bash -c "cd /opt/dbt/project && dbt run --models bronze.matches_full_data_incremental"'
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        print(f"❌ dbt stderr: {result.stderr}")
        raise Exception("dbt incremental refresh failed")
    
    print("✅ dbt incremental refresh completed")
    print(result.stdout)
    
    # Verify counts
    verify_cmd = '''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total_matches,
            MAX(start_timestamp) as latest_match,
            MAX(updated_at) as last_update
        FROM bronze.full_matches_data;"'''
    
    verify_result = subprocess.run(verify_cmd, shell=True, capture_output=True, text=True)
    
    if verify_result.returncode == 0:
        print("\n📊 Table stats after refresh:")
        print(verify_result.stdout)
    
    return "dbt_incremental_complete"

def verify_incremental_load(**context):
    """Weryfikuje nowe dane w raw_matches"""
    print("🔍 Verifying load...")
    
    cmd = f'''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT match_id) as unique_matches,
            MAX(to_timestamp((data->>'startTimestamp')::bigint)::date) as latest
        FROM bronze.raw_matches
        WHERE tournament_id = {TOURNAMENT_ID};"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Verification failed: {result.stderr}")
    
    print("✅ Database state:")
    print(result.stdout)
    return "verification_complete"

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
    '02_bronze_load_incremental_matches',
    default_args=default_args,
    description='Incremental load of NEW Ekstraklasa matches from MinIO to PostgreSQL raw_matches',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['ekstraklasa', 'incremental', 'bronze', 'postgresql', 'minio']
)

# Tasks
start_task = EmptyOperator(task_id='start', dag=dag)

get_last_info_task = PythonOperator(
    task_id='get_last_info',
    python_callable=get_last_match_info,
    dag=dag
)

check_new_data_task = PythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data_in_minio,
    dag=dag
)

load_incremental_task = PythonOperator(
    task_id='load_incremental_data',
    python_callable=load_incremental_data,
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

verify_task = PythonOperator(
    task_id='verify_incremental_load',
    python_callable=verify_incremental_load,
    dag=dag
)

dbt_refresh_task = PythonOperator(
    task_id='trigger_dbt_incremental',
    python_callable=trigger_dbt_incremental,
    dag=dag
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start_task >> get_last_info_task >> check_new_data_task >> load_incremental_task >> verify_task >> dbt_refresh_task >> end_task