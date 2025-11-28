from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
import subprocess
import json
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ensure_table_exists(**context):
    """
    Check if bronze.raw_matches table exists and create it if not
    """
    logging.info("Checking if bronze.raw_matches table exists...")
    
    check_and_create_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'bronze' 
            AND table_name = 'raw_matches'
        ) THEN
            CREATE TABLE bronze.raw_matches (
                id SERIAL NOT NULL,
                data jsonb NOT NULL,
                tournament_id integer,
                season_id integer,
                match_id integer,
                ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                file_path text,
                batch_id text,
                PRIMARY KEY(id)
            );
            
            CREATE INDEX idx_bronze_matches_tournament_season ON bronze.raw_matches USING btree (tournament_id, season_id);
            CREATE INDEX idx_bronze_matches_match_id ON bronze.raw_matches USING btree (match_id);
            CREATE INDEX idx_bronze_matches_data_gin ON bronze.raw_matches USING gin (data);
            
            RAISE NOTICE 'Table bronze.raw_matches created successfully';
        ELSE
            RAISE NOTICE 'Table bronze.raw_matches already exists';
        END IF;
    END
    $$;
    """
    
    cmd = f'''docker exec postgres psql -U airflow -d dwh -c "{check_and_create_sql}"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise AirflowFailException(f"Table check/create failed: {result.stderr}")
    
    logging.info(f"Table check result:\n{result.stdout}")
    
    return {'table_ready': True}


def load_historical_matches(**context):
    """
    Load historical matches from MinIO to PostgreSQL raw_matches table
    Filters by tournament_id and season_id from league_config.yaml
    """
    logging.info("Loading historical matches from MinIO to PostgreSQL...")
    
    script_content = '''import json
import psycopg2
from minio import Minio
import uuid
import os
import sys

try:
    sys.path.insert(0, "/opt")
    from etl.utils.config_loader import get_active_config
    
    config = get_active_config()
    
    tournament_id = config["league_id"]
    season_id = config["season_id"]
    league_name = config["league_name"]
    country = config["country"]
    season_name = config["season_name"]
    
    print(f"=== Load Configuration ===")
    print(f"League: {league_name} ({country})")
    print(f"Tournament ID: {tournament_id}")
    print(f"Season: {season_name} (ID: {season_id})")
    
    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        secure=False
    )
    
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "dwh"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )
    pg_cur = pg_conn.cursor()
    
    batch_id = str(uuid.uuid4())[:8]
    print(f"Batch ID: {batch_id}")
    
    prefix = f"matches/tournament_id={tournament_id}/season_id={season_id}/"
    print(f"\\nSearching MinIO with prefix: {prefix}")
    
    objects = list(minio_client.list_objects("bronze", prefix=prefix, recursive=True))
    ndjson_files = [obj for obj in objects if obj.object_name.endswith(".ndjson")]
    
    print(f"Found {len(ndjson_files)} NDJSON files")
    
    if len(ndjson_files) == 0:
        print("WARNING: No files found in MinIO for this configuration")
        print("Make sure extraction DAG has been run first")
        print("LOAD_RESULT:0:0:0:no_files")
        sys.exit(0)
    
    print(f"\\nSample files (first 3):")
    for obj in ndjson_files[:3]:
        print(f"  {obj.object_name}")
    
    print(f"\\n=== Processing Files ===")
    processed_files = 0
    processed_matches = 0
    skipped_duplicates = 0
    errors = 0
    
    for i, obj in enumerate(ndjson_files, 1):
        try:
            response = minio_client.get_object("bronze", obj.object_name)
            content = response.read().decode("utf-8")
            
            for line in content.strip().split("\\n"):
                if not line.strip():
                    continue
                
                try:
                    match_data = json.loads(line)
                    match_id = match_data.get("id")
                    match_tournament_id = match_data.get("tournament", {}).get("uniqueTournament", {}).get("id")
                    match_season_id = match_data.get("season", {}).get("id")
                    
                    if not match_id:
                        continue
                    
                    if match_tournament_id != tournament_id or match_season_id != season_id:
                        continue
                    
                    pg_cur.execute(
                        "SELECT 1 FROM bronze.raw_matches WHERE match_id = %s LIMIT 1",
                        (match_id,)
                    )
                    
                    if pg_cur.fetchone():
                        skipped_duplicates += 1
                        continue
                    
                    pg_cur.execute(
                        """INSERT INTO bronze.raw_matches 
                           (data, tournament_id, season_id, match_id, file_path, batch_id)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (
                            json.dumps(match_data),
                            match_tournament_id,
                            match_season_id,
                            match_id,
                            obj.object_name,
                            batch_id
                        )
                    )
                    
                    processed_matches += 1
                    
                except json.JSONDecodeError:
                    errors += 1
                    continue
                except Exception:
                    errors += 1
                    continue
            
            processed_files += 1
            
            if processed_files % 10 == 0:
                pg_conn.commit()
                print(f"Progress: {processed_files}/{len(ndjson_files)} files, {processed_matches} matches, {skipped_duplicates} duplicates, {errors} errors")
        
        except Exception as e:
            errors += 1
            print(f"Error processing file {obj.object_name}: {e}")
            continue
    
    pg_conn.commit()
    
    print(f"\\n=== Load Complete ===")
    print(f"Processed files: {processed_files}")
    print(f"Loaded matches: {processed_matches}")
    print(f"Skipped duplicates: {skipped_duplicates}")
    print(f"Errors: {errors}")
    
    pg_cur.close()
    pg_conn.close()
    
    print(f"LOAD_RESULT:{processed_files}:{processed_matches}:{skipped_duplicates}:{errors}")
    
    if processed_matches == 0 and len(ndjson_files) > 0 and skipped_duplicates == 0:
        print("ERROR: No matches loaded despite files being present")
        sys.exit(1)

except FileNotFoundError as e:
    print(f"ERROR: Configuration file not found: {str(e)}")
    print("Please create config/league_config.yaml before running")
    print("LOAD_ERROR:config_not_found")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Load failed: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()
    print(f"LOAD_ERROR:{str(e)}")
    sys.exit(1)
'''
    
    write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_historical_matches.py << "HEREDOC_END"
{script_content}
HEREDOC_END
' '''
    
    write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
    
    if write_result.returncode != 0:
        raise AirflowFailException(f"Failed to write script: {write_result.stderr}")
    
    logging.info("Script written to container")
    
    exec_cmd = 'docker exec etl_worker python /tmp/load_historical_matches.py'
    
    try:
        result = subprocess.run(
            exec_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=1800
        )
        
        if result.stderr:
            logging.warning(f"STDERR output:\n{result.stderr}")
        
        logging.info(f"Load output:\n{result.stdout}")
        
        load_result = None
        for line in result.stdout.strip().split('\n'):
            print(line)
            
            if line.startswith('LOAD_RESULT:'):
                parts = line.replace('LOAD_RESULT:', '').split(':')
                if len(parts) >= 4:
                    files_processed = int(parts[0])
                    matches_loaded = int(parts[1])
                    duplicates_skipped = int(parts[2])
                    errors = int(parts[3]) if parts[3] != 'no_files' else 0
                    
                    load_result = {
                        'processed_files': files_processed,
                        'processed_matches': matches_loaded,
                        'skipped_duplicates': duplicates_skipped,
                        'errors': errors,
                        'message': 'success' if parts[3] != 'no_files' else 'no_files_found'
                    }
                    
                    logging.info(f"Load Complete:")
                    logging.info(f"  Files: {files_processed}")
                    logging.info(f"  New matches: {matches_loaded}")
                    logging.info(f"  Duplicates: {duplicates_skipped}")
                    if errors > 0:
                        logging.warning(f"  Errors: {errors}")
                    
                    break
            
            elif line.startswith('LOAD_ERROR:'):
                error_msg = line.replace('LOAD_ERROR:', '')
                raise AirflowFailException(f"Load error: {error_msg}")
        
        if not load_result:
            raise AirflowFailException("No valid result from load script")
        
        if result.returncode != 0:
            raise AirflowFailException(f"Load failed with exit code {result.returncode}")
        
        if load_result.get('message') == 'no_files_found':
            logging.warning("No files found in MinIO. Run extraction DAG first.")
        
        return load_result
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException("Load timeout after 30 minutes")
    except Exception as e:
        logging.error(f"Load task failed: {str(e)}")
        raise


def verify_load(**context):
    """
    Verify that matches were loaded to PostgreSQL
    """
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load_matches')
    
    if not load_result:
        raise AirflowFailException("No load result found")
    
    if load_result.get('message') == 'no_files_found':
        logging.warning("Skipping verification - no files were found to load")
        return {'verification_skipped': True}
    
    logging.info("Verifying load...")
    logging.info(f"  Matches loaded: {load_result['processed_matches']}")
    logging.info(f"  Files processed: {load_result['processed_files']}")
    logging.info(f"  Duplicates skipped: {load_result['skipped_duplicates']}")
    
    if load_result['errors'] > 0:
        logging.warning(f"  Errors encountered: {load_result['errors']}")
    
    cmd = '''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT season_id) as seasons,
            MAX(to_timestamp((data->>'startTimestamp')::bigint)::date) as latest
        FROM bronze.raw_matches
        WHERE tournament_id IN (SELECT DISTINCT tournament_id FROM bronze.raw_matches ORDER BY ingestion_timestamp DESC LIMIT 1);"'''
    
    verification = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if verification.returncode == 0:
        logging.info(f"Database state:\n{verification.stdout}")
    else:
        logging.warning(f"PostgreSQL verification failed: {verification.stderr}")
    
    return {
        'load_verified': True,
        'matches_loaded': load_result['processed_matches']
    }

def trigger_dbt_full_load(**context):
    """Trigger dbt full load refresh for matches"""
    logging.info("Triggering dbt full load refresh for matches...")
    
    cmd = 'docker exec dbt bash -c "cd /opt/dbt/project && dbt run --models matches_full_data_load --full-refresh"'
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        logging.error(f"dbt stdout: {result.stdout}")
        logging.error(f"dbt stderr: {result.stderr}")
        raise AirflowFailException(f"dbt full load refresh failed with return code {result.returncode}")
    
    logging.info("dbt full load refresh completed")
    logging.info(result.stdout)
    
    # Verify counts
    verify_cmd = '''docker exec postgres psql -U airflow -d dwh -c "
        SELECT 
            COUNT(*) as total_matches,
            COUNT(DISTINCT match_id) as unique_matches,
            MIN(start_timestamp) as earliest_match,
            MAX(start_timestamp) as latest_match
        FROM bronze.full_matches_data;"'''
    
    verify_result = subprocess.run(verify_cmd, shell=True, capture_output=True, text=True)
    
    if verify_result.returncode == 0:
        logging.info("\nTable stats after full refresh:")
        logging.info(verify_result.stdout)
    
    return "dbt_full_load_complete"

# DAG Definition
with DAG(
    dag_id='11_bronze_load_historical_matches',
    default_args=default_args,
    description='Load historical matches from MinIO to PostgreSQL raw_matches (config-based)',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'historical', 'matches', 'load', 'postgres', 'config-based']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ensure_table = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_table_exists
    )
    
    load_matches = PythonOperator(
        task_id='load_matches',
        python_callable=load_historical_matches,
        execution_timeout=timedelta(minutes=35)
    )
    
    verify = PythonOperator(
        task_id='verify_load',
        python_callable=verify_load
    )

    dbt_refresh = PythonOperator(
        task_id='dbt_full_refresh',
        python_callable=trigger_dbt_full_load,
        execution_timeout=timedelta(minutes=15)
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> ensure_table >> load_matches >>  dbt_refresh >> verify >> end 