from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import time

def fetch_match_statistics(**kwargs):
    # Get season from dag_run.conf (manually set when triggering the DAG)
    dag_run = kwargs['dag_run']
    
    # Debug: sprawdź co jest w dag_run
    print(f"dag_run: {dag_run}")
    print(f"dag_run.conf: {dag_run.conf if dag_run else 'dag_run is None'}")
    
    # Sprawdź czy conf istnieje i obsłuż zarówno season jak i seasons
    if dag_run and dag_run.conf:
        # Obsługa wielu sezonów
        seasons = dag_run.conf.get('seasons', [])
        # Obsługa pojedynczego sezonu
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
    
    # Get MinIO connection
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    
    # Get RAPIDAPI_KEY from environment (set in docker-compose.yml)
    rapidapi_key = os.getenv('RAPIDAPI_KEY')
    if not rapidapi_key:
        raise ValueError("RAPIDAPI_KEY environment variable not set")
    
    # Process each season
    for season in seasons:
        print(f"\n=== Processing season: {season} ===")
        
        # Query to get match_ids from full_matches_data table filtered by season_year
        query = "SELECT match_id FROM bronze.full_matches_data WHERE season_year = %s;"
        records = postgres_hook.get_records(query, parameters=[season])
        
        if not records:
            print(f"No match_ids found in full_matches_data table for season_year {season}")
            continue
        
        print(f"Found {len(records)} matches for season {season}")
        
        # API endpoint and headers
        url = "https://sofascore.p.rapidapi.com/matches/get-statistics"
        headers = {
            "x-rapidapi-key": rapidapi_key,
            "x-rapidapi-host": "sofascore.p.rapidapi.com"
        }
        
        # Process each match_id with rate limiting
        successful_requests = 0
        failed_requests = 0
        
        for i, record in enumerate(records):
            match_id = record[0]
            print(f"Fetching statistics for match_id: {match_id} ({i+1}/{len(records)})")
            
            # Rate limiting - pause between requests
            if i > 0:
                time.sleep(1)  # 1 second delay between requests
            
            # Make API request
            querystring = {"matchId": str(match_id)}
            
            try:
                response = requests.get(url, headers=headers, params=querystring, timeout=30)
                
                if response.status_code != 200:
                    print(f"Failed to fetch data for match_id {match_id}: {response.status_code}")
                    failed_requests += 1
                    continue
                
                # Parse JSON response
                data = response.json()
                
                # Save to MinIO bronze bucket as JSON
                key = f"match_statistics/season_{season.replace('/', '_')}/match_{match_id}.json"
                s3_hook.load_string(
                    string_data=json.dumps(data),
                    key=key,
                    bucket_name='bronze',
                    replace=True
                )
                
                successful_requests += 1
                print(f"Saved statistics for match_id {match_id} to MinIO bronze bucket")
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed for match_id {match_id}: {str(e)}")
                failed_requests += 1
                continue
            except Exception as e:
                print(f"Unexpected error for match_id {match_id}: {str(e)}")
                failed_requests += 1
                continue
        
        print(f"Season {season} completed: {successful_requests} successful, {failed_requests} failed requests")
    
    print("All seasons processing completed!")

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