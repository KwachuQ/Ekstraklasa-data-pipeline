from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import json

TOURNAMENT_ID = 202  # Ekstraklasa - hardcoded

def get_last_match_info(**context) -> dict:
    """Pobierz ostatnią datę i season_id z PostgreSQL"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Pobierz mecz z najnowszą datą i jego season_id
    query = """
        SELECT 
            start_timestamp::date as match_date,
            season_id,
            season_name,
            start_timestamp
        FROM bronze.full_matches_data
        WHERE unique_tournament_id = 202
        ORDER BY start_timestamp DESC
        LIMIT 1
    """
    
    result = hook.get_first(query)
    
    if result and result[0]:
        info = {
            'last_date': str(result[0]),
            'season_id': result[1],
            'season_name': result[2] if result[2] else '25/26',
            'last_timestamp': str(result[3]),  # Convert to string for JSON serialization
            'tournament_id': TOURNAMENT_ID
        }
        print(f"✅ Znaleziono ostatni mecz:")
        print(f"   📅 Data: {info['last_date']}")
        print(f"   🏆 Tournament ID: {info['tournament_id']}")
        print(f"   ⚽ Season ID: {info['season_id']}")
        print(f"   📋 Season Name: {info['season_name']}")
        print(f"   ⏰ Timestamp: {info['last_timestamp']}")
    else:
        # Brak danych - zatrzymaj pipeline
        raise ValueError(
            "❌ Brak danych w bronze.full_matches_data dla tournament_id=202. "
            "Uruchom najpierw pełny load danych."
        )
    
    return info

def extract_new_matches(**context) -> dict:
    """Pobierz i zapisz tylko nowe mecze do MinIO"""
    ti = context['ti']
    info = ti.xcom_pull(task_ids='get_last_info')
    
    last_date = info['last_date']
    season_id = info['season_id']
    
    print(f"🔍 Szukam nowych meczów po dacie: {last_date}")
    print(f"🔍 Season ID: {season_id}, Tournament ID: {TOURNAMENT_ID}")
    
    command = f"""docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
from sofascore_incremental_etl import SofascoreIncrementalETL
import asyncio
import json

async def run():
    etl = SofascoreIncrementalETL()
    result = await etl.extract_new_matches(
        tournament_id={TOURNAMENT_ID},
        season_id={season_id},
        last_match_date='{last_date}',
        max_pages=25
    )
    print('RESULT:' + json.dumps({{
        'new': result['total_new_matches'],
        'saved': len(result['stored_batches'])
    }}))

asyncio.run(run())
" """
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
        
        if result.returncode != 0:
            print(f"⚠️ Błąd wykonania: {result.stderr}")
            print(f"📋 stdout: {result.stdout}")
            return {'new': 0, 'saved': 0}
        
        print(f"📋 Output:\n{result.stdout}")
        
        for line in result.stdout.split('\n'):
            if line.startswith('RESULT:'):
                matches_result = json.loads(line.replace('RESULT:', ''))
                print(f"✅ Nowe mecze: {matches_result['new']}, zapisane batche: {matches_result['saved']}")
                return matches_result
        
        print("⚠️ Brak wyniku RESULT: w output")
        return {'new': 0, 'saved': 0}
        
    except subprocess.TimeoutExpired:
        print(f"⚠️ Timeout - proces trwał dłużej niż 10 minut")
        return {'new': 0, 'saved': 0}
    except Exception as e:
        print(f"⚠️ Błąd: {type(e).__name__}: {e}")
        return {'new': 0, 'saved': 0}


# DAG
with DAG(
    '01_bronze_extract_incremental_matches',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Daily incremental update for Ekstraklasa - saves matches and stats to MinIO bronze bucket',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ekstraklasa', 'incremental', 'bronze']
) as dag:
    
    get_last_info = PythonOperator(
        task_id='get_last_info',
        python_callable=get_last_match_info
    )
    
    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=extract_new_matches,
        execution_timeout=timedelta(minutes=15)
    )
    

    # Task dependencies
    get_last_info >> extract_matches