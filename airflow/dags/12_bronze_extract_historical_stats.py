from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import json
import os
import logging

def get_config_based_match_ids(**context):
    """
    Get match_ids from raw_matches based on tournament_id and season_id from league_config.yaml
    """
    logging.info("Getting match IDs from league configuration...")
    
    command = """docker exec etl_worker python -c "
import json
import psycopg2
import sys

try:
    sys.path.insert(0, '/opt')
    from etl.utils.config_loader import get_active_config
    
    config = get_active_config()
    
    tournament_id = config['league_id']
    season_id = config['season_id']
    league_name = config['league_name']
    
    print(f'Getting matches for {league_name} (tournament_id={tournament_id}, season_id={season_id})')
    
    pg_conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='dwh',
        user='airflow',
        password='airflow'
    )
    pg_cur = pg_conn.cursor()
    
    pg_cur.execute('''
        SELECT match_id
        FROM bronze.raw_matches
        WHERE tournament_id = %s
        AND season_id = %s
        ORDER BY match_id
    ''', (tournament_id, season_id))
    
    results = pg_cur.fetchall()
    match_ids = [row[0] for row in results]
    
    print(f'Found {len(match_ids)} matches')
    if match_ids:
        print(f'Sample match_ids (first 5): {match_ids[:5]}')
    
    pg_cur.close()
    pg_conn.close()
    
    result = {
        'match_ids': match_ids,
        'count': len(match_ids),
        'tournament_id': tournament_id,
        'season_id': season_id
    }
    
    print('MATCH_RESULT:' + json.dumps(result))

except FileNotFoundError as e:
    print(f'ERROR: Configuration file not found: {str(e)}')
    print('MATCH_ERROR:config_not_found')
    sys.exit(1)
except Exception as e:
    print(f'ERROR: Failed to get match IDs: {type(e).__name__}: {str(e)}')
    import traceback
    traceback.print_exc()
    print(f'MATCH_ERROR:{str(e)}')
    sys.exit(1)
" """
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.stderr:
            logging.warning(f"STDERR output:\n{result.stderr}")
        
        logging.info(f"Output:\n{result.stdout}")
        
        for line in result.stdout.strip().split('\n'):
            print(line)
            
            if line.startswith('MATCH_RESULT:'):
                match_result = json.loads(line.replace('MATCH_RESULT:', ''))
                logging.info(f"Found {match_result['count']} matches")
                logging.info(f"  Tournament ID: {match_result['tournament_id']}")
                logging.info(f"  Season ID: {match_result['season_id']}")
                return match_result
            
            elif line.startswith('MATCH_ERROR:'):
                error_msg = line.replace('MATCH_ERROR:', '')
                raise AirflowFailException(f"Match lookup error: {error_msg}")
        
        if result.returncode != 0:
            raise AirflowFailException(f"Failed with exit code {result.returncode}")
        
        raise AirflowFailException("No valid result from match lookup")
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException("Match lookup timeout")
    except Exception as e:
        logging.error(f"Match lookup failed: {str(e)}")
        raise


def check_existing_statistics(**context):
    """Check which matches already have statistics in bronze.raw_stats"""
    ti = context['ti']
    matches_info = ti.xcom_pull(task_ids='get_match_ids')
    
    if matches_info['count'] == 0:
        logging.info("No matches to check")
        return {
            'missing_match_ids': [],
            'missing_count': 0,
            'already_loaded': 0,
            'tournament_id': matches_info.get('tournament_id'),
            'season_id': matches_info.get('season_id')
        }
    
    match_ids = matches_info['match_ids']
    tournament_id = matches_info['tournament_id']
    season_id = matches_info['season_id']
    
    logging.info(f"Checking which of the {len(match_ids)} matches already have statistics...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # First check if the table exists
    table_check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'bronze' 
            AND table_name = 'raw_stats'
        );
    """
    
    table_exists = hook.get_first(table_check_query)[0]
    
    if not table_exists:
        logging.info("Table bronze.raw_stats does not exist yet - all matches need to be fetched")
        return {
            'missing_match_ids': match_ids,
            'missing_count': len(match_ids),
            'already_loaded': 0,
            'tournament_id': tournament_id,
            'season_id': season_id
        }
    
    # Table exists, check for existing statistics
    placeholders = ','.join(['%s'] * len(match_ids))
    query = f"""
        SELECT DISTINCT match_id
        FROM bronze.raw_stats
        WHERE match_id IN ({placeholders})
    """
    
    existing_results = hook.get_records(query, parameters=match_ids)
    existing_ids = {row[0] for row in existing_results}
    
    missing_ids = [mid for mid in match_ids if mid not in existing_ids]
    
    logging.info(f"Summary:")
    logging.info(f"  Matches total: {len(match_ids)}")
    logging.info(f"  Already have statistics: {len(existing_ids)}")
    logging.info(f"  To fetch: {len(missing_ids)}")
    
    if missing_ids:
        logging.info(f"Match IDs to fetch (first 10): {missing_ids[:10]}")
    
    return {
        'missing_match_ids': missing_ids,
        'missing_count': len(missing_ids),
        'already_loaded': len(existing_ids),
        'tournament_id': tournament_id,
        'season_id': season_id
    }

def fetch_statistics(**context):
    """Fetch statistics for missing matches"""
    ti = context['ti']
    check_info = ti.xcom_pull(task_ids='check_existing')
    
    if check_info['missing_count'] == 0:
        logging.info("All matches already have statistics - nothing to fetch")
        return {
            'fetched': 0,
            'failed': 0,
            'skipped': 0,
            'message': 'no_new_matches'
        }
    
    missing_ids = check_info['missing_match_ids']
    tournament_id = check_info['tournament_id']
    
    logging.info(f"Starting to fetch statistics for {len(missing_ids)} matches...")
    
    rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
    
    if not rapidapi_key:
        raise AirflowFailException(
            "Missing RAPIDAPI_KEY in environment variables! "
            "Set RAPIDAPI_KEY in docker-compose.yml"
        )
    
    match_ids_json = json.dumps(missing_ids)
    
    command = f"""docker exec etl_worker python -c "
import json
from etl.bronze.extractors.statistics_extractor import StatisticsFetcher

match_ids = {match_ids_json}

fetcher = StatisticsFetcher(
    rapidapi_key='{rapidapi_key}',
    tournament_id={tournament_id},
    season_id='{check_info['season_id']}'
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
            timeout=1800
        )
        
        if result.returncode != 0:
            logging.error(f"Error: {result.stderr}")
            logging.error(f"stdout: {result.stdout}")
            raise AirflowFailException("ETL script failed")
        
        logging.info(f"Output:\n{result.stdout}")
        
        for line in result.stdout.split('\n'):
            if line.startswith('STATS_RESULT:'):
                stats_result = json.loads(line.replace('STATS_RESULT:', ''))
                logging.info(f"Fetch results:")
                logging.info(f"  Fetched: {stats_result['fetched']}")
                logging.info(f"  Failed: {stats_result['failed']}")
                logging.info(f"  Skipped: {stats_result['skipped']}")
                logging.info(f"  Total: {stats_result['total']}")
                return stats_result
        
        raise AirflowFailException("No valid result from ETL script")
        
    except subprocess.TimeoutExpired:
        raise AirflowFailException("ETL script timeout after 30 minutes")
    except Exception as e:
        logging.error(f"Error: {type(e).__name__}: {e}")
        raise


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

with DAG(
    dag_id='12_bronze_extract_historical_stats',
    default_args=default_args,
    description='Extract match statistics for historical matches based on league_config.yaml',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'historical', 'statistics', 'extract', 'config-based']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    get_match_ids = PythonOperator(
        task_id='get_match_ids',
        python_callable=get_config_based_match_ids
    )
    
    check_existing = PythonOperator(
        task_id='check_existing',
        python_callable=check_existing_statistics
    )
    
    fetch_stats = PythonOperator(
        task_id='fetch_stats',
        python_callable=fetch_statistics,
        execution_timeout=timedelta(minutes=35)
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> get_match_ids >> check_existing >> fetch_stats >> end