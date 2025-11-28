from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import logging

def ensure_table_exists(**context):
    """
    Check if bronze.raw_stats table exists and create it if not
    """
    logging.info("Checking if bronze.raw_stats table exists...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    check_and_create_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'bronze' 
            AND table_name = 'raw_stats'
        ) THEN
            CREATE TABLE bronze.raw_stats (
                id SERIAL NOT NULL,
                data jsonb NOT NULL,
                tournament_id integer,
                match_id integer,
                ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                file_path text,
                batch_id text,
                PRIMARY KEY(id)
            );
            
            CREATE INDEX idx_raw_stats_match_id ON bronze.raw_stats USING btree (match_id);
            CREATE INDEX idx_raw_stats_file_path ON bronze.raw_stats USING btree (file_path);
            CREATE INDEX idx_raw_stats_data_gin ON bronze.raw_stats USING gin (data);
            
            RAISE NOTICE 'Table bronze.raw_stats created successfully';
        ELSE
            RAISE NOTICE 'Table bronze.raw_stats already exists';
        END IF;
    END
    $$;
    """
    
    try:
        hook.run(check_and_create_sql)
        logging.info("Table check/create completed successfully")
    except Exception as e:
        raise AirflowFailException(f"Table check/create failed: {str(e)}")
    
    return {'table_ready': True}


def load_statistics(**context):
    """
    Load statistics from MinIO to bronze.raw_stats
    Uses the current StatisticsFetcher path structure: match_statistics/season_folder/date=*/
    """
    logging.info("Loading statistics from MinIO to PostgreSQL...")
    
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
    
    print(f"Loading statistics for {league_name} (tournament_id={tournament_id}, season_id={season_id})")
    
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
    
    prefix = f"match_statistics/tournament_id={tournament_id}/"  
      
    print(f"\\nSearching MinIO with prefix: {prefix}")
    
    objects = list(minio_client.list_objects("bronze", prefix=prefix, recursive=True))
    json_files = [obj for obj in objects if obj.object_name.endswith(".json")]
    
    print(f"Found {len(json_files)} statistics files")
    
    if len(json_files) == 0:
        print("WARNING: No statistics files found in MinIO")
        print("Make sure extraction DAG has been run first")
        print("LOAD_RESULT:0:0:0:no_files")
        sys.exit(0)
    
    # Show sample files
    print(f"\\nSample files (first 3):")
    for obj in json_files[:3]:
        print(f"  {obj.object_name}")
    
    loaded = 0
    duplicates = 0
    errors = 0
    
    for obj in json_files:
        try:
            filename = obj.object_name.split("/")[-1]
            match_id = int(filename.replace("match_", "").replace(".json", ""))
            
            pg_cur.execute("SELECT 1 FROM bronze.raw_stats WHERE match_id = %s LIMIT 1", (match_id,))
            if pg_cur.fetchone():
                duplicates += 1
                continue
            
            response = minio_client.get_object("bronze", obj.object_name)
            content = response.read().decode("utf-8")
            data = json.loads(content)
            
            # Extract statistics from the enriched structure
            stats_data = data.get("statistics", data)
            
            pg_cur.execute(
                """INSERT INTO bronze.raw_stats 
                   (data, match_id, tournament_id, file_path, batch_id)
                   VALUES (%s, %s, %s, %s, %s)""",
                (
                    json.dumps(stats_data),
                    match_id,
                    tournament_id,
                    obj.object_name,
                    batch_id
                )
            )
            
            loaded += 1
            
            if loaded % 10 == 0:
                pg_conn.commit()
                print(f"Progress: {loaded} loaded, {duplicates} duplicates, {errors} errors")
                
        except Exception as e:
            errors += 1
            print(f"Error processing {obj.object_name}: {e}")
            continue
    
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    
    print(f"\\nLoad complete:")
    print(f"  Loaded: {loaded}")
    print(f"  Duplicates: {duplicates}")
    print(f"  Errors: {errors}")
    
    print(f"LOAD_RESULT:{loaded}:{duplicates}:{errors}")

except FileNotFoundError as e:
    print(f"ERROR: Configuration file not found: {str(e)}")
    print("LOAD_ERROR:config_not_found")
    sys.exit(1)
except Exception as e:
    print(f"LOAD_ERROR:{str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''
    
    write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_historical_stats.py << "HEREDOC_END"
{script_content}
HEREDOC_END
' '''
    
    write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
    
    if write_result.returncode != 0:
        raise AirflowFailException(f"Failed to write script: {write_result.stderr}")
    
    exec_cmd = 'docker exec etl_worker python /tmp/load_historical_stats.py'
    
    result = subprocess.run(exec_cmd, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        raise AirflowFailException(f"Load failed: {result.stderr}\nStdout: {result.stdout}")
    
    logging.info(f"Load output:\n{result.stdout}")
    
    for line in result.stdout.split('\n'):
        print(line)
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            
            if parts[0] == '0' and len(parts) > 3 and parts[3] == 'no_files':
                logging.warning("No files found in MinIO. Run extraction DAG first.")
                return {'loaded': 0, 'duplicates': 0, 'errors': 0, 'message': 'no_files'}
            
            loaded = int(parts[0])
            duplicates = int(parts[1])
            errors = int(parts[2]) if len(parts) > 2 else 0
            
            logging.info(f"Load complete:")
            logging.info(f"  Loaded: {loaded}")
            logging.info(f"  Duplicates: {duplicates}")
            logging.info(f"  Errors: {errors}")
            
            return {'loaded': loaded, 'duplicates': duplicates, 'errors': errors}
        elif line.startswith('LOAD_ERROR:'):
            raise AirflowFailException(f"Load error: {line}")
    
    raise AirflowFailException("No valid result from load script")


def verify_statistics(**context):
    """Verify loaded statistics"""
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load_stats')
    
    if load_result.get('message') == 'no_files':
        logging.warning("Skipping verification - no files were found to load")
        return {'verification_skipped': True}
    
    logging.info("Verifying loaded statistics...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    query = """
        SELECT 
            COUNT(*) as total_stats,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT tournament_id) as tournaments
        FROM bronze.raw_stats
    """
    
    result = hook.get_first(query)
    
    logging.info(f"Database state:")
    logging.info(f"  Total stats: {result[0]}")
    logging.info(f"  Unique matches: {result[1]}")
    logging.info(f"  Tournaments: {result[2]}")
    
    return {
        'load_verified': True,
        'loaded': load_result['loaded'],
        'total_stats': result[0],
        'unique_matches': result[1]
    }

# DAG Configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def trigger_dbt_full_load(**context):
    """Trigger dbt full load refresh for statistics"""
    logging.info("Triggering dbt full load refresh for statistics...")
    
    cmd = 'docker exec dbt bash -c "cd /opt/dbt/project && dbt run --models stats_full_data_load --full-refresh"'
    
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
            COUNT(*) as total_stats,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT period) as unique_periods,
            COUNT(DISTINCT group_name) as unique_groups
        FROM bronze.full_stats_data;"'''
    
    verify_result = subprocess.run(verify_cmd, shell=True, capture_output=True, text=True)
    
    if verify_result.returncode == 0:
        logging.info("\nStats table after full refresh:")
        logging.info(verify_result.stdout)
    
    return "dbt_full_load_complete"

with DAG(
    dag_id='13_bronze_load_historical_stats',
    default_args=default_args,
    description='Load match statistics from MinIO to PostgreSQL raw_stats (config-based)',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'historical', 'statistics', 'load', 'postgres', 'config-based']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ensure_table = PythonOperator(
        task_id='ensure_table_exists',
        python_callable=ensure_table_exists
    )
    
    load_stats = PythonOperator(
        task_id='load_stats',
        python_callable=load_statistics,
        execution_timeout=timedelta(minutes=15)
    )
    
    verify = PythonOperator(
        task_id='verify_stats',
        python_callable=verify_statistics
    )
    
    dbt_refresh = PythonOperator(
        task_id='dbt_full_refresh',
        python_callable=trigger_dbt_full_load,
        execution_timeout=timedelta(minutes=15)
    )

    end = EmptyOperator(task_id='end')
    
    start >> ensure_table >> load_stats >> dbt_refresh >> verify >> end