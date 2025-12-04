from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
import subprocess
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ensure_bronze_table(**context):
    """Create bronze.raw_upcoming_fixtures table if it doesn't exist"""
    logging.info("Ensuring bronze.raw_upcoming_fixtures table exists...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # First ensure bronze schema exists
    schema_sql = """
    CREATE SCHEMA IF NOT EXISTS bronze;
    """
    
    try:
        hook.run(schema_sql)
        logging.info("✓ Bronze schema ensured")
    except Exception as e:
        logging.error(f"Failed to create bronze schema: {e}")
        raise
    
    # Create table
    table_sql = """
    CREATE TABLE IF NOT EXISTS bronze.raw_upcoming_fixtures (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        tournament_id INTEGER,
        extraction_date DATE,
        file_path TEXT,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes if they don't exist
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'bronze' 
            AND tablename = 'raw_upcoming_fixtures' 
            AND indexname = 'idx_raw_upcoming_tournament'
        ) THEN
            CREATE INDEX idx_raw_upcoming_tournament ON bronze.raw_upcoming_fixtures(tournament_id);
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'bronze' 
            AND tablename = 'raw_upcoming_fixtures' 
            AND indexname = 'idx_raw_upcoming_extraction_date'
        ) THEN
            CREATE INDEX idx_raw_upcoming_extraction_date ON bronze.raw_upcoming_fixtures(extraction_date);
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'bronze' 
            AND tablename = 'raw_upcoming_fixtures' 
            AND indexname = 'idx_raw_upcoming_data_gin'
        ) THEN
            CREATE INDEX idx_raw_upcoming_data_gin ON bronze.raw_upcoming_fixtures USING gin(data);
        END IF;
    END
    $$;
    """
    
    try:
        hook.run(table_sql)
        logging.info("✓ Bronze table bronze.raw_upcoming_fixtures ready")
        
        # Verify table exists
        verify_sql = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name = 'raw_upcoming_fixtures';
        """
        result = hook.get_first(verify_sql)
        if result[0] == 1:
            logging.info("✓ Table verified successfully")
        else:
            raise AirflowFailException("Table creation verification failed!")
            
    except Exception as e:
        logging.error(f"Failed to create bronze table: {e}")
        raise


def load_upcoming_from_minio(**context):
    """Load upcoming matches from MinIO to bronze.raw_upcoming_fixtures"""
    logging.info("Loading upcoming matches from MinIO...")
    
    script_content = '''import json
import psycopg2
from minio import Minio
import os
import sys
from datetime import datetime

try:
    sys.path.insert(0, "/opt")
    from etl.utils.config_loader import get_active_config
    
    config = get_active_config()
    tournament_id = config["league_id"]
    
    print(f"Loading upcoming matches for tournament_id={tournament_id}")
    
    # Connect to MinIO
    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        secure=False
    )
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "dwh"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )
    pg_cur = pg_conn.cursor()
    
    # Find upcoming matches files
    prefix = f"next_matches/tournament_id={tournament_id}/"
    print(f"Searching MinIO with prefix: {prefix}")
    
    objects = list(minio_client.list_objects("bronze", prefix=prefix, recursive=True))
    json_files = [obj for obj in objects if obj.object_name.endswith(".json")]
    
    print(f"Found {len(json_files)} upcoming matches files")
    
    if len(json_files) == 0:
        print("WARNING: No upcoming matches files found")
        print("LOAD_RESULT:0:0:no_files")
        sys.exit(0)
    
    # Show sample files
    print("\\nSample files (first 3):")
    for obj in json_files[:3]:
        print(f"  {obj.object_name}")
    
    loaded = 0
    errors = 0
    
    # Clear existing data for this tournament
    pg_cur.execute(
        "DELETE FROM bronze.raw_upcoming_fixtures WHERE tournament_id = %s",
        (tournament_id,)
    )
    deleted = pg_cur.rowcount
    print(f"\\nDeleted {deleted} existing records for tournament_id={tournament_id}")
    
    for obj in json_files:
        try:
            # Extract date from path: next_matches/tournament_id=202/date=2025-12-02/next_matches.json
            path_parts = obj.object_name.split("/")
            extraction_date = None
            for part in path_parts:
                if part.startswith("date="):
                    extraction_date = part.replace("date=", "")
                    break
            
            # Read file from MinIO
            response = minio_client.get_object("bronze", obj.object_name)
            content = response.read().decode("utf-8")
            data = json.loads(content)
            
            # Insert to PostgreSQL
            pg_cur.execute(
                """INSERT INTO bronze.raw_upcoming_fixtures 
                   (data, tournament_id, extraction_date, file_path)
                   VALUES (%s, %s, %s, %s)""",
                (
                    json.dumps(data),
                    tournament_id,
                    extraction_date,
                    obj.object_name
                )
            )
            
            loaded += 1
            
        except Exception as e:
            errors += 1
            print(f"Error processing {obj.object_name}: {e}")
            continue
    
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    
    print(f"\\nLoad complete:")
    print(f"  Loaded: {loaded}")
    print(f"  Errors: {errors}")
    print(f"LOAD_RESULT:{loaded}:{errors}")
    
except Exception as e:
    print(f"LOAD_ERROR:{str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''
    
    # Write and execute script
    write_cmd = f'''docker exec etl_worker bash -c 'cat > /tmp/load_upcoming.py << "SCRIPT_END"
{script_content}
SCRIPT_END' '''
    
    write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
    
    if write_result.returncode != 0:
        raise AirflowFailException(f"Failed to write script: {write_result.stderr}")
    
    # Execute the script
    exec_cmd = 'docker exec etl_worker python /tmp/load_upcoming.py'
    result = subprocess.run(exec_cmd, shell=True, capture_output=True, text=True, timeout=300)
    
    logging.info(result.stdout)
    
    if result.returncode != 0:
        logging.error(result.stderr)
        raise AirflowFailException(f"Load failed: {result.stderr}")
    
    # Parse result
    for line in result.stdout.split('\n'):
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            if len(parts) >= 2 and parts[0] != '0':
                loaded = int(parts[0])
                logging.info(f"✓ Loaded {loaded} upcoming matches files to bronze")
                return {'loaded': loaded}
            elif parts[-1] == 'no_files':
                logging.warning("No files found in MinIO")
                return {'loaded': 0, 'message': 'no_files'}
    
    raise AirflowFailException("Could not parse load result")


def verify_bronze_load(**context):
    """Verify data was loaded to bronze"""
    logging.info("Verifying bronze.raw_upcoming_fixtures...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    count = hook.get_first("SELECT COUNT(*) FROM bronze.raw_upcoming_fixtures")[0]
    
    if count == 0:
        raise AirflowFailException("bronze.raw_upcoming_fixtures is EMPTY after load!")
    
    # Get sample data structure
    sample = hook.get_first("""
        SELECT 
            jsonb_pretty(data) 
        FROM bronze.raw_upcoming_fixtures 
        LIMIT 1
    """)
    
    logging.info(f"✓ Bronze layer contains {count} record(s)")
    logging.info(f"\nSample data structure:\n{sample[0][:500]}...")
    
    context['ti'].xcom_push(key='bronze_count', value=count)
    return {'count': count}


def create_silver_table(**context):
    """Create silver.full_upcoming_fixtures table and unpack NDJSON"""
    logging.info("Creating silver.full_upcoming_fixtures...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # First, inspect the actual data structure
    sample_query = """
        SELECT 
            jsonb_pretty(data) as full_structure
        FROM bronze.raw_upcoming_fixtures 
        LIMIT 1
    """
    
    sample = hook.get_first(sample_query)
    logging.info(f"Full data structure:\n{sample[0]}")
    
    # Check if data->data is an array or object
    check_query = """
        SELECT 
            jsonb_typeof(data->'data') as data_type,
            data->'data' as data_content
        FROM bronze.raw_upcoming_fixtures 
        LIMIT 1
    """
    
    check = hook.get_first(check_query)
    logging.info(f"data->data type: {check[0]}")
    logging.info(f"data->data content: {check[1]}")
    
    # The structure appears to be: data->data->events (array)
    # Let's use the correct path
    sql = """
    -- Create silver schema if needed
    CREATE SCHEMA IF NOT EXISTS silver;
    
    -- Drop and recreate table
    DROP TABLE IF EXISTS silver.full_upcoming_fixtures CASCADE;
    
    CREATE TABLE silver.full_upcoming_fixtures AS
    WITH unpacked_matches AS (
        SELECT 
            (data->'metadata'->>'tournament_id')::INTEGER as tournament_id,
            (data->'metadata'->>'season_id')::INTEGER as season_id,
            data->'metadata'->>'extraction_date' as extraction_date,
            TO_TIMESTAMP((data->'metadata'->>'extracted_at')::TEXT, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as extracted_at,
            jsonb_array_elements(data->'data'->'events') as match_data,
            file_path,
            ingestion_timestamp
        FROM bronze.raw_upcoming_fixtures
    )
    SELECT 
        -- Core Match Info
        (match_data->>'id')::INTEGER as match_id,
        match_data->>'slug' as match_slug,
        match_data->>'customId' as custom_id,
        TO_TIMESTAMP((match_data->>'startTimestamp')::BIGINT) as start_timestamp,
        
        -- Match Status
        (match_data->'status'->>'code')::INTEGER as status_code,
        match_data->'status'->>'type' as status_type,
        match_data->'status'->>'description' as status_description,
        
        -- Home Team
        (match_data->'homeTeam'->>'id')::INTEGER as home_team_id,
        match_data->'homeTeam'->>'name' as home_team_name,
        match_data->'homeTeam'->>'slug' as home_team_slug,
        match_data->'homeTeam'->>'shortName' as home_team_short_name,
        
        -- Away Team
        (match_data->'awayTeam'->>'id')::INTEGER as away_team_id,
        match_data->'awayTeam'->>'name' as away_team_name,
        match_data->'awayTeam'->>'slug' as away_team_slug,
        match_data->'awayTeam'->>'shortName' as away_team_short_name,
        
        -- Tournament
        (match_data->'tournament'->>'id')::INTEGER as tournament_id_from_match,
        match_data->'tournament'->>'name' as tournament_name,
        match_data->'tournament'->>'slug' as tournament_slug,
        
        -- Season
        (match_data->'season'->>'id')::INTEGER as season_id_from_match,
        match_data->'season'->>'name' as season_name,
        match_data->'season'->>'year' as season_year,
        
        -- Round Info
        (match_data->'roundInfo'->>'round')::INTEGER as round_number,
        match_data->'roundInfo'->>'name' as round_name,
        
        -- Metadata from extraction
        tournament_id as tournament_id_metadata,
        season_id as season_id_metadata,
        extraction_date::DATE as extraction_date,
        extracted_at,
        
        -- Original data
        match_data as original_match_data,
        file_path,
        ingestion_timestamp,
        CURRENT_TIMESTAMP as created_at
        
    FROM unpacked_matches;
    
    -- Create indexes
    CREATE INDEX idx_silver_upcoming_match_id ON silver.full_upcoming_fixtures(match_id);
    CREATE INDEX idx_silver_upcoming_start_time ON silver.full_upcoming_fixtures(start_timestamp);
    CREATE INDEX idx_silver_upcoming_teams ON silver.full_upcoming_fixtures(home_team_id, away_team_id);
    CREATE INDEX idx_silver_upcoming_tournament ON silver.full_upcoming_fixtures(tournament_id_from_match);
    CREATE INDEX idx_silver_upcoming_extraction ON silver.full_upcoming_fixtures(extraction_date);
    """
    
    hook.run(sql)
    logging.info("✓ Created silver.full_upcoming_fixtures table")


def verify_silver_table(**context):
    """Verify and inspect silver table"""
    logging.info("Verifying silver.full_upcoming_fixtures...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get counts
    count_query = """
        SELECT 
            COUNT(*) as total_matches,
            COUNT(DISTINCT match_id) as unique_matches,
            MIN(start_timestamp) as earliest_match,
            MAX(start_timestamp) as latest_match,
            COUNT(DISTINCT tournament_id_from_match) as tournaments,
            COUNT(DISTINCT season_id_from_match) as seasons
        FROM silver.full_upcoming_fixtures
    """
    
    result = hook.get_first(count_query)
    
    logging.info(f"\n{'='*60}")
    logging.info(f"SILVER LAYER - UPCOMING FIXTURES")
    logging.info(f"{'='*60}")
    logging.info(f"Total matches: {result[0]:,}")
    logging.info(f"Unique matches: {result[1]:,}")
    logging.info(f"Earliest match: {result[2]}")
    logging.info(f"Latest match: {result[3]}")
    logging.info(f"Tournaments: {result[4]}")
    logging.info(f"Seasons: {result[5]}")
    
    # Get column info
    columns_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'silver'
        AND table_name = 'full_upcoming_fixtures'
        ORDER BY ordinal_position
    """
    
    columns = hook.get_records(columns_query)
    
    logging.info(f"\n{'='*60}")
    logging.info(f"AVAILABLE COLUMNS ({len(columns)} total)")
    logging.info(f"{'='*60}")
    for col in columns:
        logging.info(f"  • {col[0]:<30} {col[1]:<20} {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
    
    # Sample data
    sample_query = """
        SELECT 
            match_id,
            start_timestamp,
            home_team_name,
            away_team_name,
            round_number,
            status_type
        FROM silver.full_upcoming_fixtures
        ORDER BY start_timestamp
        LIMIT 5
    """
    
    samples = hook.get_records(sample_query)
    
    logging.info(f"\n{'='*60}")
    logging.info(f"SAMPLE UPCOMING MATCHES")
    logging.info(f"{'='*60}")
    for match in samples:
        logging.info(f"  {match[1]} | Round {match[4]} | {match[2]} vs {match[3]} (status: {match[5]})")
    
    logging.info(f"\n{'='*60}")
    logging.info(f"✓ Silver layer ready for inspection")
    logging.info(f"  Next step: Review columns and create gold.mart_upcoming_fixtures")
    logging.info(f"{'='*60}\n")
    
    context['ti'].xcom_push(key='silver_count', value=result[0])
    context['ti'].xcom_push(key='unique_matches', value=result[1])


def generate_report(**context):
    """Generate final load report"""
    ti = context['ti']
    
    bronze_count = ti.xcom_pull(task_ids='verify_bronze', key='bronze_count')
    silver_count = ti.xcom_pull(task_ids='verify_silver', key='silver_count')
    unique_matches = ti.xcom_pull(task_ids='verify_silver', key='unique_matches')
    
    logging.info(f"\n{'='*70}")
    logging.info(f"UPCOMING FIXTURES LOAD REPORT")
    logging.info(f"{'='*70}")
    logging.info(f"Bronze layer (raw_upcoming_fixtures):     {bronze_count:,} files loaded")
    logging.info(f"Silver layer (full_upcoming_fixtures):    {silver_count:,} matches unpacked")
    logging.info(f"Unique matches:                           {unique_matches:,}")
    logging.info(f"{'='*70}")
    logging.info(f"\n✓ Pipeline completed successfully!")
    logging.info(f"\nNext steps:")
    logging.info(f"  1. Query silver.full_upcoming_fixtures to inspect data")
    logging.info(f"  2. Decide which columns to keep in gold.mart_upcoming_fixtures")
    logging.info(f"  3. Create gold layer transformation in dbt")
    logging.info(f"{'='*70}\n")


with DAG(
    dag_id='09_load_upcoming_fixtures',
    default_args=default_args,
    description='Load upcoming matches from MinIO (bronze) → unpack to silver → prepare for gold',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'silver', 'upcoming', 'fixtures', 'next-matches']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ensure_bronze = PythonOperator(
        task_id='ensure_bronze_table',
        python_callable=ensure_bronze_table
    )
    
    load_bronze = PythonOperator(
        task_id='load_to_bronze',
        python_callable=load_upcoming_from_minio,
        execution_timeout=timedelta(minutes=10)
    )
    
    verify_bronze = PythonOperator(
        task_id='verify_bronze',
        python_callable=verify_bronze_load
    )
    
    create_silver = PythonOperator(
        task_id='create_silver_table',
        python_callable=create_silver_table,
        execution_timeout=timedelta(minutes=10)
    )
    
    verify_silver = PythonOperator(
        task_id='verify_silver',
        python_callable=verify_silver_table
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task dependencies
    start >> ensure_bronze >> load_bronze >> verify_bronze >> create_silver >> verify_silver >> report >> end