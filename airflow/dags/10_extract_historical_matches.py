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


def extract_historical_matches(**context):
    """
    Extract historical matches using base_extractor.py with league_config.yaml
    """
    logging.info("Starting historical matches extraction from league_config.yaml...")
    
    command = """docker exec etl_worker python -c "
import asyncio
import json
import sys
from etl.bronze.extractors.base_extractor import SofascoreETL
from etl.utils.config_loader import get_active_config

async def run():
    try:
        # Load configuration from league_config.yaml
        config = get_active_config()
        
        print(f'=== Extraction Configuration ===')
        print(f'League: {config[\\"league_name\\"]} ({config[\\"country\\"]})')
        print(f'League ID: {config[\\"league_id\\"]}')
        print(f'Season: {config[\\"season_name\\"]} (ID: {config[\\"season_id\\"]})')
        print(f'Max pages: {config[\\"max_pages\\"]}')
        
        # Get date filters if configured
        start_date = config.get(\\"start_date\\")
        end_date = config.get(\\"end_date\\")
        
        if start_date or end_date:
            print(f'Date range: [{start_date or \\"ANY\\"} to {end_date or \\"ANY\\"}]')
        
        # Extract matches
        async with SofascoreETL() as etl:
            result = await etl.extract_tournament_matches(
                tournament_id=config[\\"league_id\\"],
                season_id=config[\\"season_id\\"],
                max_pages=config[\\"max_pages\\"],
                replace_partition=True,  # Replace existing data for full refresh
                start_date=start_date,
                end_date=end_date
            )
        
        # Print results
        print(f'\\n=== Extraction Results ===')
        print(f'Total matches: {result[\\"total_matches\\"]}')
        print(f'Stored batches: {len(result[\\"stored_batches\\"])}')
        print(f'Errors: {len(result[\\"errors\\"])}')
        
        if result[\\"errors\\"]:
            print(f'\\nErrors encountered:')
            for error in result[\\"errors\\"][:5]:
                print(f'  • {error}')
            if len(result[\\"errors\\"]) > 5:
                print(f'  ... and {len(result[\\"errors\\"]) - 5} more errors')
        
        # Output result as JSON for XCom
        output = {
            'total_matches': result[\\"total_matches\\"],
            'stored_batches': len(result[\\"stored_batches\\"]),
            'errors': len(result[\\"errors\\"])
        }
        print('RESULT:' + json.dumps(output))
        
        # Exit with error if no matches were extracted
        if result[\\"total_matches\\"] == 0 and result[\\"errors\\"]:
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f'ERROR: Configuration file not found: {str(e)}')
        print('Please create config/league_config.yaml before running')
        sys.exit(1)
    except Exception as e:
        print(f'ERROR: Extraction failed: {type(e).__name__}: {str(e)}')
        import traceback
        traceback.print_exc()
        sys.exit(1)

asyncio.run(run())
" """
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        # Log stderr if present
        if result.stderr:
            logging.warning(f"STDERR output:\n{result.stderr}")
        
        # Log stdout
        logging.info(f"Extraction output:\n{result.stdout}")
        
        # Check for errors
        if result.returncode != 0:
            raise AirflowFailException(
                f"Extraction failed with exit code {result.returncode}. "
                f"Check logs for details."
            )
        
        # Parse result
        extraction_result = None
        for line in result.stdout.split('\n'):
            if line.startswith('RESULT:'):
                extraction_result = json.loads(line.replace('RESULT:', ''))
                logging.info(f"✓ Extraction completed:")
                logging.info(f"  • Matches: {extraction_result['total_matches']}")
                logging.info(f"  • Batches: {extraction_result['stored_batches']}")
                logging.info(f"  • Errors: {extraction_result['errors']}")
                break
        
        if not extraction_result:
            raise AirflowFailException("No valid result found in extraction output")
        
        # Fail if there were errors and no matches
        if extraction_result['errors'] > 0 and extraction_result['total_matches'] == 0:
            raise AirflowFailException(
                f"Extraction completed with {extraction_result['errors']} errors "
                f"and no matches extracted"
            )
        
        return extraction_result
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException("Extraction timeout after 30 minutes")
    except Exception as e:
        logging.error(f"Extraction task failed: {str(e)}")
        raise


def verify_extraction(**context):
    """
    Verify that matches were saved to MinIO
    """
    ti = context['ti']
    extraction_result = ti.xcom_pull(task_ids='extract_matches')
    
    if not extraction_result:
        raise AirflowFailException("No extraction result found")
    
    logging.info("Extraction verification:")
    logging.info(f"  ✓ Matches extracted: {extraction_result['total_matches']}")
    logging.info(f"  ✓ Batches stored: {extraction_result['stored_batches']}")
    
    if extraction_result['errors'] > 0:
        logging.warning(f"  ⚠ Errors encountered: {extraction_result['errors']}")
    
    # Check MinIO for saved files
    command = """docker exec etl_worker python -c "
from minio import Minio
import os

minio_client = Minio(
    endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
    secure=False
)

# Count files in bronze bucket
objects = list(minio_client.list_objects('bronze', prefix='matches/', recursive=True))
ndjson_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]

print(f'Files in MinIO bronze/matches/: {len(ndjson_files)}')
if ndjson_files:
    print(f'Sample files:')
    for obj in ndjson_files[:3]:
        print(f'  • {obj.object_name}')
" """
    
    verification = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if verification.returncode == 0:
        logging.info(f"MinIO verification:\n{verification.stdout}")
    else:
        logging.warning(f"MinIO verification failed: {verification.stderr}")
    
    return {
        'extraction_verified': True,
        'matches_count': extraction_result['total_matches']
    }


# DAG Definition
with DAG(
    dag_id='10_bronze_extract_historical_matches',
    default_args=default_args,
    description='Extract historical matches from league_config.yaml to MinIO bronze layer',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'historical', 'matches', 'extraction', 'config-based']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=extract_historical_matches,
        execution_timeout=timedelta(minutes=35)
    )
    
    verify = PythonOperator(
        task_id='verify_extraction',
        python_callable=verify_extraction
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task dependencies
    start >> extract_matches >> verify >> end