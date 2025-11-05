from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import subprocess
import json
import os

TOURNAMENT_ID = 202  # Ekstraklasa

def get_today_match_ids(**context):
    """Pobierz match_id meczów załadowanych dzisiaj do bronze.raw_matches"""
    print("🔍 Szukam meczów załadowanych dzisiaj...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Pobierz mecze z dzisiejszej daty (ingestion_timestamp)
    query = f"""
        SELECT 
            match_id,
            ingestion_timestamp
        FROM bronze.raw_matches
        WHERE tournament_id = {TOURNAMENT_ID}
        AND DATE(ingestion_timestamp) = CURRENT_DATE
        ORDER BY match_id
    """
    
    results = hook.get_records(query)
    
    if not results:
        print("ℹ️ Brak meczów załadowanych dzisiaj")
        return {
            'match_ids': [],
            'count': 0,
            'date': str(datetime.now().date())
        }
    
    match_ids = [row[0] for row in results]
    
    print(f"✅ Znaleziono {len(match_ids)} meczów załadowanych dzisiaj:")
    for i, row in enumerate(results[:10], 1):
        print(f"  {i}. match_id={row[0]}, loaded at {row[1]}")
    
    if len(results) > 10:
        print(f"  ... i {len(results) - 10} więcej")
    
    return {
        'match_ids': match_ids,
        'count': len(match_ids),
        'date': str(datetime.now().date())
    }

def check_existing_statistics(**context):
    """Sprawdź które mecze już mają statystyki w bronze.raw_stats"""
    ti = context['ti']
    matches_info = ti.xcom_pull(task_ids='get_today_matches')
    
    if matches_info['count'] == 0:
        print("ℹ️ Brak meczów do sprawdzenia")
        return {
            'missing_match_ids': [],
            'missing_count': 0,
            'already_loaded': 0
        }
    
    match_ids = matches_info['match_ids']
    
    print(f"🔍 Sprawdzam które z {len(match_ids)} meczów mają już statystyki...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Sprawdź które match_id już mają statystyki
    placeholders = ','.join(['%s'] * len(match_ids))
    query = f"""
        SELECT DISTINCT match_id
        FROM bronze.raw_stats
        WHERE match_id IN ({placeholders})
    """
    
    existing_results = hook.get_records(query, parameters=match_ids)
    existing_ids = {row[0] for row in existing_results}
    
    missing_ids = [mid for mid in match_ids if mid not in existing_ids]
    
    print(f"📊 Podsumowanie:")
    print(f"  • Mecze z dzisiaj: {len(match_ids)}")
    print(f"  • Już mają statystyki: {len(existing_ids)}")
    print(f"  • Do pobrania: {len(missing_ids)}")
    
    if missing_ids:
        print(f"\n📋 Match IDs do pobrania (pierwsze 10):")
        for mid in missing_ids[:10]:
            print(f"  • {mid}")
    
    return {
        'missing_match_ids': missing_ids,
        'missing_count': len(missing_ids),
        'already_loaded': len(existing_ids)
    }

def fetch_statistics(**context):
    """Wywołaj skrypt ETL do pobrania statystyk"""
    ti = context['ti']
    check_info = ti.xcom_pull(task_ids='check_existing')
    
    if check_info['missing_count'] == 0:
        print("✅ Wszystkie mecze już mają statystyki - nic do pobrania")
        return {
            'fetched': 0,
            'failed': 0,
            'skipped': 0,
            'message': 'no_new_matches'
        }
    
    missing_ids = check_info['missing_match_ids']
    
    print(f"🚀 Rozpoczynam pobieranie statystyk dla {len(missing_ids)} meczów...")
    
    # Pobierz RAPIDAPI credentials z environment
    rapidapi_key = os.getenv('RAPIDAPI_KEY', '')
    
    if not rapidapi_key:
        raise AirflowFailException(
            "❌ Brak RAPIDAPI_KEY w zmiennych środowiskowych! "
            "Ustaw RAPIDAPI_KEY w docker-compose.yml"
        )
    
    # Przekaż match_ids jako JSON
    match_ids_json = json.dumps(missing_ids)
    
    command = f"""docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
from fetch_match_statistics import StatisticsFetcher
import json

match_ids = {match_ids_json}

fetcher = StatisticsFetcher(
    rapidapi_key='{rapidapi_key}',
    tournament_id={TOURNAMENT_ID}
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
            timeout=1800  # 30 minut
        )
        
        if result.returncode != 0:
            print(f"⚠️ Błąd wykonania: {result.stderr}")
            print(f"📋 stdout: {result.stdout}")
            raise AirflowFailException("ETL script failed")
        
        print(f"📋 Output:\n{result.stdout}")
        
        # Parsuj wynik
        for line in result.stdout.split('\n'):
            if line.startswith('STATS_RESULT:'):
                stats_result = json.loads(line.replace('STATS_RESULT:', ''))
                print(f"\n✅ Wyniki pobierania:")
                print(f"  • Pobrane: {stats_result['fetched']}")
                print(f"  • Błędy: {stats_result['failed']}")
                print(f"  • Pominięte: {stats_result['skipped']}")
                print(f"  • Razem: {stats_result['total']}")
                return stats_result
        
        print("⚠️ Brak wyniku STATS_RESULT: w output")
        raise AirflowFailException("No valid result from ETL script")
        
    except subprocess.TimeoutExpired:
        print(f"⚠️ Timeout - proces trwał dłużej niż 30 minut")
        raise AirflowFailException("ETL script timeout")
    except Exception as e:
        print(f"⚠️ Błąd: {type(e).__name__}: {e}")
        raise

def load_to_raw_stats(**context):
    """Załaduj pobrane statystyki z MinIO do bronze.raw_stats"""
    ti = context['ti']
    fetch_result = ti.xcom_pull(task_ids='fetch_stats')
    
    if fetch_result.get('fetched', 0) == 0:
        print("ℹ️ Brak nowych statystyk do załadowania")
        return {'loaded': 0, 'message': 'no_data'}
    
    print(f"📥 Ładowanie {fetch_result['fetched']} statystyk z MinIO do PostgreSQL...")
    
    # Użyj istniejącego DAG do załadowania
    # Alternatywnie: bezpośredni load z MinIO
    
    command = """docker exec etl_worker python -c "
import json
import psycopg2
from minio import Minio
import uuid
from datetime import datetime

try:
    minio_client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)
    pg_conn = psycopg2.connect(host='postgres', port=5432, database='dwh', user='airflow', password='airflow')
    pg_cur = pg_conn.cursor()
    
    batch_id = str(uuid.uuid4())[:8]
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Znajdź pliki z dzisiejszej daty
    prefix = f'match_statistics/season_2025_26/date={today}/'
    
    objects = list(minio_client.list_objects('bronze', prefix=prefix, recursive=True))
    json_files = [obj for obj in objects if obj.object_name.endswith('.json')]
    
    print(f'Znaleziono {len(json_files)} plików statystyk z dzisiaj')
    
    loaded = 0
    duplicates = 0
    
    for obj in json_files:
        try:
            # Wyciągnij match_id z nazwy pliku
            filename = obj.object_name.split('/')[-1]
            match_id = int(filename.replace('match_', '').replace('.json', ''))
            
            # Sprawdź czy już istnieje
            pg_cur.execute('SELECT 1 FROM bronze.raw_stats WHERE match_id = %s LIMIT 1', (match_id,))
            if pg_cur.fetchone():
                duplicates += 1
                continue
            
            # Pobierz dane
            response = minio_client.get_object('bronze', obj.object_name)
            content = response.read().decode('utf-8')
            stats_data = json.loads(content)
            
            # Insert
            pg_cur.execute('''
                INSERT INTO bronze.raw_stats 
                (data, match_id, tournament_id, file_path, batch_id)
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                json.dumps(stats_data),
                match_id,
                202,
                obj.object_name,
                batch_id
            ))
            
            loaded += 1
            
            if loaded % 10 == 0:
                pg_conn.commit()
                print(f'Progress: {loaded} loaded, {duplicates} duplicates')
                
        except Exception as e:
            print(f'Error processing {obj.object_name}: {e}')
            continue
    
    pg_conn.commit()
    pg_conn.close()
    
    print(f'LOAD_RESULT:{loaded}:{duplicates}')
    
except Exception as e:
    print(f'LOAD_ERROR:{str(e)}')
    raise
" """
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
    
    if result.returncode != 0:
        raise AirflowFailException(f"Load failed: {result.stderr}")
    
    print(f"📋 Load output:\n{result.stdout}")
    
    for line in result.stdout.split('\n'):
        if line.startswith('LOAD_RESULT:'):
            parts = line.replace('LOAD_RESULT:', '').split(':')
            loaded = int(parts[0])
            duplicates = int(parts[1])
            
            print(f"\n✅ Load complete:")
            print(f"  • Załadowane: {loaded}")
            print(f"  • Duplikaty: {duplicates}")
            
            return {'loaded': loaded, 'duplicates': duplicates}
        elif line.startswith('LOAD_ERROR:'):
            raise AirflowFailException(f"Load error: {line}")
    
    raise AirflowFailException("No valid result from load script")

def verify_statistics(**context):
    """Weryfikuj załadowane statystyki"""
    print("🔍 Weryfikacja załadowanych statystyk...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Sprawdź dzisiejsze statystyki
    query = f"""
        SELECT 
            COUNT(*) as total_stats,
            COUNT(DISTINCT match_id) as unique_matches
        FROM bronze.raw_stats
        WHERE tournament_id = {TOURNAMENT_ID}
        AND DATE(ingestion_timestamp) = CURRENT_DATE
    """
    
    result = hook.get_first(query)
    
    print(f"✅ Statystyki z dzisiaj:")
    print(f"  • Rekordów: {result[0]}")
    print(f"  • Meczów: {result[1]}")
    
    # Sprawdź coverage
    coverage_query = f"""
        SELECT 
            COUNT(DISTINCT rm.match_id) as matches_today,
            COUNT(DISTINCT rs.match_id) as with_stats,
            COUNT(DISTINCT rm.match_id) - COUNT(DISTINCT rs.match_id) as missing
        FROM bronze.raw_matches rm
        LEFT JOIN bronze.raw_stats rs ON rm.match_id = rs.match_id
        WHERE rm.tournament_id = {TOURNAMENT_ID}
        AND DATE(rm.ingestion_timestamp) = CURRENT_DATE
    """
    
    coverage = hook.get_first(coverage_query)
    
    print(f"\n📊 Coverage dzisiejszych meczów:")
    print(f"  • Mecze załadowane dzisiaj: {coverage[0]}")
    print(f"  • Z statystykami: {coverage[1]}")
    print(f"  • Brakujące statystyki: {coverage[2]}")
    
    if coverage[2] > 0:
        print(f"\n⚠️ UWAGA: {coverage[2]} meczów wciąż bez statystyk!")
    
    return {
        'total_stats': result[0],
        'unique_matches': result[1],
        'missing': coverage[2]
    }

# DAG Configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '03_bronze_extract_incremental_stats',
    default_args=default_args,
    description='Fetch match statistics for matches loaded today to bronze.raw_matches',
    schedule=None,  # Manual trigger - run after bronze_loader_recent_matches
    catchup=False,
    max_active_runs=1,
    tags=['ekstraklasa', 'statistics', 'daily', 'bronze', 'api']
)

# Tasks
start = EmptyOperator(task_id='start', dag=dag)

get_today_matches = PythonOperator(
    task_id='get_today_matches',
    python_callable=get_today_match_ids,
    dag=dag
)

check_existing = PythonOperator(
    task_id='check_existing',
    python_callable=check_existing_statistics,
    dag=dag
)

fetch_stats = PythonOperator(
    task_id='fetch_stats',
    python_callable=fetch_statistics,
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

load_stats = PythonOperator(
    task_id='load_stats',
    python_callable=load_to_raw_stats,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

verify = PythonOperator(
    task_id='verify_stats',
    python_callable=verify_statistics,
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start >> get_today_matches >> check_existing >> fetch_stats >> load_stats >> verify >> end