from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import subprocess
import json
import time

def clean_and_recreate_bronze_table(**context):
    """Czyści i odtwarza tabelę bronze.raw_matches"""
    print("🗑️ Czyszczenie i odtwarzanie tabeli bronze.raw_matches...")
    
    sql_commands = [
        "DROP TABLE IF EXISTS bronze.raw_matches CASCADE;",
        """CREATE TABLE bronze.raw_matches (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL,
            tournament_id INTEGER,
            season_id INTEGER,
            match_id INTEGER,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            batch_id TEXT
        );""",
        "CREATE INDEX idx_bronze_matches_tournament_season ON bronze.raw_matches (tournament_id, season_id);",
        "CREATE INDEX idx_bronze_matches_match_id ON bronze.raw_matches (match_id);",
        "CREATE INDEX idx_bronze_matches_data_gin ON bronze.raw_matches USING GIN (data);"
    ]
    
    for sql in sql_commands:
        cmd = f'docker exec postgres psql -U airflow -d dwh -c "{sql}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"SQL failed: {sql} - Error: {result.stderr}")
    
    print("✅ Tabela bronze.raw_matches została odtworzona")
    return "success"

def check_minio_data_availability(**context):
    """Sprawdza dostępność danych w MinIO - uproszczona wersja"""
    print("🔍 Sprawdzanie dostępności danych w MinIO...")
    
    # Stwórz prosty skrypt w jednej linii
    cmd = '''docker exec etl_worker python -c "
from minio import Minio
import json
client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)
objects = list(client.list_objects('bronze', prefix='matches/tournament_id=202/', recursive=True))
ndjson_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]
result = {'total_files': len(objects), 'ndjson_files': len(ndjson_files)}
print('RESULT:' + json.dumps(result))
"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"MinIO check failed: {result.stderr}")
    
    # Parse result
    for line in result.stdout.strip().split('\n'):
        if line.startswith('RESULT:'):
            minio_data = json.loads(line.replace('RESULT:', ''))
            
            print(f"📊 MinIO Data Summary:")
            print(f"  • Total files: {minio_data['total_files']}")
            print(f"  • NDJSON files: {minio_data['ndjson_files']}")
            
            if minio_data['ndjson_files'] == 0:
                raise Exception("No NDJSON files found in MinIO!")
            
            # Estymacja rekordów - załóżmy 30 na plik
            minio_data['estimated_records'] = minio_data['ndjson_files'] * 30
            print(f"  • Estimated records: ~{minio_data['estimated_records']:,}")
            
            return minio_data
    
    raise Exception("No valid result from MinIO check")

def load_historical_data_full(**context):
    """Ładuje WSZYSTKIE dane historyczne w batch-ach"""
    print("🚀 Rozpoczynam ładowanie WSZYSTKICH danych historycznych...")
    
    # Get MinIO data info from previous task
    minio_data = context['ti'].xcom_pull(task_ids='check_minio_data')
    total_files = minio_data['ndjson_files']
    
    print(f"📊 Plan ładowania: {total_files} plików (~{minio_data['estimated_records']:,} rekordów)")
    
    # Batch configuration
    batch_size = 100  # Process 100 files per batch
    total_batches = (total_files + batch_size - 1) // batch_size
    
    print(f"🔢 Podzielone na {total_batches} batch-y po {batch_size} plików")
    
    total_processed_matches = 0
    
    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, total_files)
        
        print(f"\n📦 Batch {batch_num + 1}/{total_batches}: pliki {batch_start}-{batch_end}")
        
        # Create batch-specific script
        script_content = f'''import json
import psycopg2
from minio import Minio
import uuid

try:
    # Connect to services
    minio_client = Minio("minio:9000", access_key="minio", secret_key="minio123", secure=False)
    pg_conn = psycopg2.connect(host="postgres", port=5432, database="dwh", user="airflow", password="airflow")
    pg_cur = pg_conn.cursor()

    batch_id = str(uuid.uuid4())[:8]

    # Get files for this specific batch
    objects = list(minio_client.list_objects("bronze", prefix="matches/tournament_id=202/", recursive=True))
    ndjson_files = [obj for obj in objects if obj.object_name.endswith(".ndjson")]
    batch_files = ndjson_files[{batch_start}:{batch_end}]

    processed_matches = 0
    processed_files = 0

    print(f"Processing batch files: {{len(batch_files)}}")

    for i, obj in enumerate(batch_files):
        try:
            response = minio_client.get_object("bronze", obj.object_name)
            content = response.read().decode("utf-8")
            
            file_matches = 0
            
            for line in content.strip().split("\\n"):
                if line.strip():
                    try:
                        match_data = json.loads(line)
                        match_id = match_data.get("id")
                        unique_tournament_id = match_data.get("tournament", {{}}).get("uniqueTournament", {{}}).get("id")
                        season_id = match_data.get("season", {{}}).get("id")
                        
                        if unique_tournament_id == 202 and match_id:
                            pg_cur.execute("""
                                INSERT INTO bronze.raw_matches 
                                (data, tournament_id, season_id, match_id, file_path, batch_id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                json.dumps(match_data), 
                                unique_tournament_id, 
                                season_id, 
                                match_id,
                                obj.object_name,
                                batch_id
                            ))
                            processed_matches += 1
                            file_matches += 1
                    except Exception as e:
                        continue
            
            processed_files += 1
            
            # Commit every 20 files within batch
            if processed_files % 20 == 0:
                pg_conn.commit()
                print(f"Batch progress: {{processed_files}}/{{len(batch_files)}} files, {{processed_matches}} matches")
                
        except Exception as e:
            print(f"Error processing file {{obj.object_name}}: {{str(e)}}")
            continue

    # Final commit for this batch
    pg_conn.commit()
    print(f"BATCH_RESULT:{{processed_files}}:{{processed_matches}}")
    pg_conn.close()
    
except Exception as e:
    print(f"BATCH_ERROR:{{str(e)}}")
    raise
'''
        
        # Write and execute batch script
        write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_batch_{batch_num}.py << "SCRIPT_END"
{script_content}
SCRIPT_END' '''
        
        write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
        
        if write_result.returncode != 0:
            print(f"❌ Failed to write batch {batch_num + 1} script: {write_result.stderr}")
            continue
        
        # Execute the batch
        exec_cmd = f'docker exec etl_worker python /tmp/load_batch_{batch_num}.py'
        result = subprocess.run(exec_cmd, shell=True, capture_output=True, text=True, timeout=1800)  # 30 min per batch
        
        if result.returncode != 0:
            print(f"❌ Batch {batch_num + 1} failed: {result.stderr}")
            print(f"Stdout: {result.stdout}")
            continue  # Continue with next batch instead of failing
        
        # Parse batch result
        batch_processed = 0
        for line in result.stdout.strip().split('\n'):
            print(f"Batch output: {line}")
            if line.startswith('BATCH_RESULT:'):
                parts = line.replace('BATCH_RESULT:', '').split(':')
                if len(parts) >= 2:
                    batch_files_processed = int(parts[0])
                    batch_matches = int(parts[1])
                    total_processed_matches += batch_matches
                    print(f"✅ Batch {batch_num + 1}: {batch_files_processed} files, {batch_matches} matches")
                    break
            elif line.startswith('BATCH_ERROR:'):
                error_msg = line.replace('BATCH_ERROR:', '')
                print(f"❌ Batch {batch_num + 1} error: {error_msg}")
                break
        
        # Progress summary
        progress_pct = ((batch_num + 1) / total_batches) * 100
        print(f"📈 Overall progress: {progress_pct:.1f}% ({total_processed_matches:,} matches loaded)")
        
        # Rate limiting between batches
        if batch_num < total_batches - 1:
            print("⏳ Waiting 5s before next batch...")
            time.sleep(5)
    
    print(f"\n🎉 Ładowanie zakończone! Total: {total_processed_matches:,} meczów z {total_batches} batch-y")
    return {'total_processed': total_processed_matches, 'total_batches': total_batches}

def verify_loaded_data(**context):
    """Weryfikuje załadowane dane"""
    print("🔍 Weryfikacja załadowanych danych...")
    
    cmd = '''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT season_id) as unique_seasons
        FROM bronze.raw_matches
        WHERE tournament_id = 202;"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Verification failed: {result.stderr}")
    
    print("✅ Verification completed:")
    print(result.stdout)
    return "verification_complete"

def trigger_dbt_refresh(**context):
    """Uruchamia dbt refresh"""
    print("🔄 Triggering dbt refresh...")
    
    cmd = 'docker exec dbt bash -c "cd /opt/dbt/project && dbt run --models silver --full-refresh"'
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        print(f"dbt stderr: {result.stderr}")
        # Nie fail-uj całego pipeline na dbt error - może być problem z modelem
        print("⚠️ dbt failed, but continuing pipeline...")
        return "dbt_failed_but_continue"
    
    print("✅ dbt refresh completed")
    print(result.stdout)
    return "dbt_refresh_complete"

# DAG Configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_load_historical_matches',
    default_args=default_args,
    description='Load all historical Ekstraklasa matches from MinIO to PostgreSQL',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['ekstraklasa', 'historical', 'bronze', 'postgresql']
)

# Tasks
start_task = EmptyOperator(task_id='start', dag=dag)

clean_table_task = PythonOperator(
    task_id='clean_and_recreate_table',
    python_callable=clean_and_recreate_bronze_table,
    dag=dag
)

check_minio_task = PythonOperator(
    task_id='check_minio_data',
    python_callable=check_minio_data_availability,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_historical_data',
    python_callable=load_historical_data_full,
    dag=dag,
    execution_timeout=timedelta(hours=2)
)

verify_task = PythonOperator(
    task_id='verify_loaded_data',
    python_callable=verify_loaded_data,
    dag=dag
)

dbt_refresh_task = PythonOperator(
    task_id='trigger_dbt_refresh',
    python_callable=trigger_dbt_refresh,
    dag=dag
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start_task >> clean_table_task >> check_minio_task >> load_data_task >> verify_task >> dbt_refresh_task >> end_task