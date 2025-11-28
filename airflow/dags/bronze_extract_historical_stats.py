from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import subprocess
import json
import os

TOURNAMENT_ID = 202  # Ekstraklasa

def fetch_match_statistics(**kwargs):
    """Fetch match statistics using ETL worker StatisticsFetcher"""
    # Get season from dag_run.conf (manually set when triggering the DAG)
    dag_run = kwargs['dag_run']
    
    # Debug: check what is in dag_run
    print(f"dag_run: {dag_run}")
    print(f"dag_run.conf: {dag_run.conf if dag_run else 'dag_run is None'}")
    
    # Check if conf exists and handle both season and seasons
    if dag_run and dag_run.conf:
        # Handle multiple seasons
        seasons = dag_run.conf.get('seasons', [])
        # Handle single season
        single_season = dag_run.conf.get('season')
        
        if single_season:
            seasons = [single_season]
        elif not seasons:
            raise ValueError("Please provide 'season' or 'seasons' in dag_run.conf, e.g., {'season': '24/25'} or {'seasons': ['24/25', '23/24']}")
        
        print(f"Seasons to process: {seasons}")
    else:
        raise ValueError("Please provide 'season' or 'seasons' in dag_run.conf")
    
    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get RAPIDAPI_KEY from environment (set in docker-compose.yml)
    rapidapi_key = os.getenv('RAPIDAPI_KEY')
    if not rapidapi_key:
        raise AirflowFailException("RAPIDAPI_KEY environment variable not set")
    
    # Process each season
    total_summary = {'total_fetched': 0, 'total_failed': 0, 'total_skipped': 0}
    
    for season in seasons:
        print(f"\n=== Processing season: {season} ===")

    # Query to get match_ids AND season_id from raw_matches table
        query = """
            SELECT match_id, season_id 
            FROM bronze.raw_matches 
            WHERE tournament_id = %s
            AND season_year = %s 
            ORDER BY match_id;
        """
        records = postgres_hook.get_records(query, parameters=[TOURNAMENT_ID, season])

        if not records:
            print(f"No match_ids found in raw_matches table for season_year {season}")
            continue

    # Get season_id from first record (all should be same for this season)
    season_id = records[0][1]
    match_ids = [record[0] for record in records]
    print(f"Found {len(match_ids)} matches for season {season} (season_id={season_id})")

    # Prepare match_ids as JSON
    match_ids_json = json.dumps(match_ids)

    # Call ETL worker to fetch statistics
    command = f"""docker exec etl_worker python -c "
import json
from etl.bronze.extractors.statistics_extractor import StatisticsFetcher

match_ids = {match_ids_json}

fetcher = StatisticsFetcher(
    rapidapi_key='{rapidapi_key}',
    tournament_id={TOURNAMENT_ID},
    season_id={season_id}
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
            timeout=3600  # 1 hour per season
        )

        if result.returncode != 0:
            print(f"Execution error for season {season}: {result.stderr}")
            print(f"stdout: {result.stdout}")
            raise AirflowFailException(f"Statistics extraction failed for season {season}")

        print(f"Output:\n{result.stdout}")

        # Parse result
        for line in result.stdout.split('\n'):
            if line.startswith('STATS_RESULT:'):
                stats_result = json.loads(line.replace('STATS_RESULT:', ''))
                print(f"\n✅ Season {season} results:")
                print(f"  • Fetched: {stats_result['fetched']}")
                print(f"  • Failed: {stats_result['failed']}")
                print(f"  • Skipped: {stats_result['skipped']}")
                print(f"  • Total: {stats_result['total']}")

                total_summary['total_fetched'] += stats_result['fetched']
                total_summary['total_failed'] += stats_result['failed']
                total_summary['total_skipped'] += stats_result['skipped']
                break
        else:
            raise AirflowFailException(f"No valid result from statistics fetch for season {season}")

    except subprocess.TimeoutExpired:
        print(f"Timeout for season {season} - process exceeded 1 hour")
        raise AirflowFailException(f"Statistics fetch timeout for season {season}")
    except Exception as e:
        print(f"Error processing season {season}: {type(e).__name__}: {e}")
        raise
    
    print(f"\nAll seasons processing completed!")
    print(f"Total summary:")
    print(f"  • Total fetched: {total_summary['total_fetched']}")
    print(f"  • Total failed: {total_summary['total_failed']}")
    print(f"  • Total skipped: {total_summary['total_skipped']}")
    
    return total_summary

# DAG definition
with DAG(
    dag_id="bronze_extract_historical_stats",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manually triggered
    catchup=False,
    tags=["data_pipeline", "sofascore", "statistics"],
    description="Fetch match statistics for a specific season",
) as dag:

    fetch_statistics_task = PythonOperator(
        task_id="fetch_statistics_task",
        python_callable=fetch_match_statistics,
    )