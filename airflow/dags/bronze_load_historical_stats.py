from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import re
import uuid

def load_all_stats(**context):
    """Load all valid statistics from MinIO bronze/match_statistics/ to PostgreSQL"""
    
    # Initialize hooks
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Drop and recreate table with consistent column naming
    logging.info("Dropping and recreating bronze.raw_stats table...")
    
    drop_and_create_sql = """
        DROP TABLE IF EXISTS bronze.raw_stats CASCADE;
        
        CREATE TABLE bronze.raw_stats (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL,
            tournament_id INTEGER,
            season_id INTEGER,
            match_id INTEGER,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            batch_id TEXT
        );
        
        CREATE INDEX idx_raw_stats_tournament_season ON bronze.raw_stats(tournament_id, season_id);
        CREATE INDEX idx_raw_stats_match_id ON bronze.raw_stats(match_id);
        CREATE INDEX idx_raw_stats_file_path ON bronze.raw_stats(file_path);
        CREATE INDEX idx_raw_stats_data_gin ON bronze.raw_stats USING gin(data);
    """
    
    postgres_hook.run(drop_and_create_sql)
    logging.info("Table and indexes created successfully")
    
    # List all objects in match_statistics directory
    logging.info("Listing objects in MinIO...")
    objects = s3_hook.list_keys(
        bucket_name='bronze',
        prefix='match_statistics/'
    )
    
    if not objects:
        logging.warning("No objects found in match_statistics/ directory")
        return
    
    logging.info(f"Found {len(objects)} objects in MinIO")
    
    total_files = 0
    successful_loads = 0
    error_files = 0
    invalid_files = 0
    
    # Generate batch_id for this run
    batch_id = str(uuid.uuid4())[:8]
    logging.info(f"Using batch_id: {batch_id}")
    
    for obj_key in objects:
        if obj_key.endswith('.json'):
            total_files += 1
            
            try:
                # Extract season from path - this will be used for season_id
                season_part = obj_key.split('season_')[1].split('/')[0] if 'season_' in obj_key else 'unknown'
                
                # Convert season format to approximate season_id (this is a placeholder - real season_id should come from match data)
                season_id = None
                if season_part != 'unknown' and '_' in season_part:
                    try:
                        # Convert "08_09" format to approximate season_id
                        year_parts = season_part.split('_')
                        if len(year_parts) == 2:
                            start_year = int(year_parts[0])
                            # This is an approximation - real season_id mapping would need a lookup table
                            season_id = 10000 + start_year  # Placeholder mapping
                    except:
                        season_id = None
                
                # Extract match_id from filename using regex
                match_id_match = re.search(r'match_(\d+)\.json', obj_key)
                match_id = int(match_id_match.group(1)) if match_id_match else None
                
                # Download and parse JSON
                json_content = s3_hook.read_key(key=obj_key, bucket_name='bronze')
                stats_data = json.loads(json_content)
                
                # Check if file contains error response
                if "error" in stats_data:
                    error_files += 1
                    if error_files <= 5:  # Log first 5 errors
                        logging.warning(f"⚠️ Skipping error file {obj_key}: {stats_data.get('error', {}).get('message', 'Unknown error')}")
                    elif error_files == 6:
                        logging.warning(f"⚠️ Found many error files, suppressing further error logs...")
                    continue
                
                # Check if file contains valid statistics data
                if not stats_data.get("statistics") or not isinstance(stats_data.get("statistics"), list):
                    invalid_files += 1
                    if invalid_files <= 5:  # Log first 5 invalid files
                        logging.warning(f"⚠️ Skipping invalid file {obj_key}: missing or invalid statistics data")
                    elif invalid_files == 6:
                        logging.warning(f"⚠️ Found many invalid files, suppressing further invalid logs...")
                    continue
                
                # Validate that statistics contains actual data
                statistics = stats_data.get("statistics", [])
                if len(statistics) == 0:
                    invalid_files += 1
                    continue
                
                # Check if statistics has the expected structure
                has_valid_stats = False
                for stat_period in statistics:
                    if isinstance(stat_period, dict) and "groups" in stat_period:
                        groups = stat_period.get("groups", [])
                        if len(groups) > 0:
                            has_valid_stats = True
                            break
                
                if not has_valid_stats:
                    invalid_files += 1
                    if invalid_files <= 5:
                        logging.warning(f"⚠️ Skipping file with empty statistics {obj_key}")
                    continue
                
                # Only process if we have a valid match_id from filename
                if not match_id:
                    invalid_files += 1
                    if invalid_files <= 5:
                        logging.warning(f"⚠️ Skipping file without match_id in filename {obj_key}")
                    continue
                
                # Insert into PostgreSQL with consistent column names
                insert_sql = """
                    INSERT INTO bronze.raw_stats (data, tournament_id, season_id, match_id, file_path, batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                postgres_hook.run(
                    insert_sql, 
                    parameters=(
                        json.dumps(stats_data),
                        202,  # Default tournament_id for Ekstraklasa
                        season_id,  # Mapped season_id
                        match_id,
                        obj_key,
                        batch_id
                    )
                )
                
                successful_loads += 1
                
                # Log progress every 100 successful loads
                if successful_loads % 100 == 0:
                    logging.info(f"✅ Progress: {successful_loads} valid files loaded, {error_files} errors, {invalid_files} invalid skipped")
                elif successful_loads <= 5:  # Log first 5 successful loads
                    logging.info(f"✅ Loaded {obj_key} (season_id: {season_id}, match_id: {match_id})")
                
            except json.JSONDecodeError as e:
                invalid_files += 1
                if invalid_files <= 5:
                    logging.error(f"❌ JSON decode error in {obj_key}: {str(e)}")
                continue
            except Exception as e:
                invalid_files += 1
                if invalid_files <= 5:
                    logging.error(f"❌ Failed to load {obj_key}: {str(e)}")
                continue
    
    # Final summary
    logging.info(f"🎉 Processing complete!")
    logging.info(f"  📊 Total files processed: {total_files}")
    logging.info(f"  ✅ Valid statistics loaded: {successful_loads}")
    logging.info(f"  ⚠️ Error files skipped: {error_files}")
    logging.info(f"  ❌ Invalid files skipped: {invalid_files}")
    logging.info(f"  🏷️ Batch ID: {batch_id}")
    
    # Verify loaded data
    if successful_loads > 0:
        verify_sql = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT season_id) as unique_seasons,
                COUNT(DISTINCT match_id) as unique_matches,
                MIN(ingestion_timestamp) as first_load,
                MAX(ingestion_timestamp) as last_load
            FROM bronze.raw_stats;
        """
        
        result = postgres_hook.get_first(verify_sql)
        logging.info(f"📊 Database verification: {result}")
        
        # Show sample of loaded data
        sample_sql = "SELECT season_id, match_id, file_path FROM bronze.raw_stats ORDER BY id LIMIT 3;"
        sample_results = postgres_hook.get_records(sample_sql)
        logging.info("📋 Sample loaded records:")
        for row in sample_results:
            logging.info(f"  Season ID: {row[0]}, Match ID: {row[1]}, File: {row[2]}")
    
    return {
        'total_files': total_files,
        'successful_loads': successful_loads,
        'error_files': error_files,
        'invalid_files': invalid_files,
        'batch_id': batch_id
    }

def verify_stats_data(**context):
    """Verify the loaded statistics data"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get load results from previous task
    load_results = context['ti'].xcom_pull(task_ids='load_all_statistics')
    
    logging.info("🔍 Verifying loaded statistics data:")
    logging.info(f"Load Summary: {load_results}")
    
    verification_queries = [
        ("Total records", "SELECT COUNT(*) FROM bronze.raw_stats;"),
        ("Unique seasons", "SELECT COUNT(DISTINCT season_id) FROM bronze.raw_stats WHERE season_id IS NOT NULL;"),
        ("Unique matches", "SELECT COUNT(DISTINCT match_id) FROM bronze.raw_stats WHERE match_id IS NOT NULL;"),
        ("Sample seasons", "SELECT DISTINCT season_id FROM bronze.raw_stats WHERE season_id IS NOT NULL ORDER BY season_id LIMIT 10;"),
        ("Records per season", """
            SELECT season_id, COUNT(*) as records 
            FROM bronze.raw_stats 
            WHERE season_id IS NOT NULL
            GROUP BY season_id 
            ORDER BY season_id;
        """),
        ("Sample match data", """
            SELECT season_id, match_id, tournament_id, 
                   LEFT(file_path, 50) as file_path_short
            FROM bronze.raw_stats 
            WHERE match_id IS NOT NULL
            ORDER BY ingestion_timestamp 
            LIMIT 5;
        """),
        ("Statistics structure check", """
            SELECT season_id, match_id,
                   jsonb_array_length(data->'statistics') as stats_periods,
                   jsonb_array_length(data->'statistics'->0->'groups') as groups_count
            FROM bronze.raw_stats 
            WHERE data->'statistics' IS NOT NULL
            LIMIT 5;
        """),
        ("Batch info", """
            SELECT batch_id, COUNT(*) as records,
                   MIN(ingestion_timestamp) as batch_start,
                   MAX(ingestion_timestamp) as batch_end
            FROM bronze.raw_stats
            GROUP BY batch_id
            ORDER BY MIN(ingestion_timestamp);
        """)
    ]
    
    for description, query in verification_queries:
        try:
            if "Records per season" in description or "Batch info" in description:
                results = postgres_hook.get_records(query)
                logging.info(f"{description}:")
                for row in results:
                    if "Batch info" in description:
                        logging.info(f"  Batch {row[0]}: {row[1]} records ({row[2]} - {row[3]})")
                    else:
                        logging.info(f"  Season {row[0]}: {row[1]} records")
            elif "Sample match data" in description or "Statistics structure" in description:
                results = postgres_hook.get_records(query)
                logging.info(f"{description}:")
                for row in results:
                    logging.info(f"  {row}")
            else:
                result = postgres_hook.get_first(query)
                logging.info(f"{description}: {result[0] if result else 'None'}")
        except Exception as e:
            logging.error(f"Failed to execute {description}: {str(e)}")
    
    # Check for data quality
    quality_check_sql = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN match_id IS NOT NULL THEN 1 END) as with_match_id,
            COUNT(CASE WHEN tournament_id IS NOT NULL THEN 1 END) as with_tournament_id,
            COUNT(CASE WHEN season_id IS NOT NULL THEN 1 END) as with_season_id,
            COUNT(CASE WHEN data->'statistics' IS NOT NULL THEN 1 END) as with_statistics
        FROM bronze.raw_stats;
    """
    
    quality_result = postgres_hook.get_first(quality_check_sql)
    if quality_result:
        logging.info(f"📊 Data Quality Check:")
        logging.info(f"  Total records: {quality_result[0]}")
        logging.info(f"  With match_id: {quality_result[1]} ({quality_result[1]/quality_result[0]*100:.1f}%)")
        logging.info(f"  With tournament_id: {quality_result[2]} ({quality_result[2]/quality_result[0]*100:.1f}%)")
        logging.info(f"  With season_id: {quality_result[3]} ({quality_result[3]/quality_result[0]*100:.1f}%)")
        logging.info(f"  With statistics: {quality_result[4]} ({quality_result[4]/quality_result[0]*100:.1f}%)")
    
    return "verification_complete"

# DAG definition
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_load_historical_stats",
    default_args=default_args,
    description="Load valid match statistics from MinIO to PostgreSQL bronze layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "minio", "postgres", "bronze", "statistics"],
) as dag:

    load_stats_task = PythonOperator(
        task_id="load_all_statistics",
        python_callable=load_all_stats,
        execution_timeout=timedelta(hours=2),
    )
    
    verify_stats_task = PythonOperator(
        task_id="verify_statistics",
        python_callable=verify_stats_data,
    )
    
    load_stats_task >> verify_stats_task