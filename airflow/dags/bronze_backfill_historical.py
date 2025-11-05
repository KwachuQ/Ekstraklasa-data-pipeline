from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import requests
import json
import os
import time
import subprocess
from typing import Dict, List, Any

def run_diagnostic_analysis(**context):
    """
    Uruchamia analizę diagnostyczną w kontenerze etl_worker
    Zwraca listę brakujących i niekompletnych sezonów
    """
    print("🔍 Uruchamianie analizy diagnostycznej sezonów...")
    
    # Komenda do uruchomienia diagnostyki w etl_worker
    command = """
    docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
import asyncio
import json
from diagnose_seasons import EkstraklasaSeasonDiagnostic

async def run_diagnostic():
    diagnostic = EkstraklasaSeasonDiagnostic()
    report = await diagnostic.generate_comprehensive_report()
    
    # Zwróć tylko potrzebne listy
    result = {
        'missing_seasons': report['missing_seasons'],
        'incomplete_seasons': report['incomplete_seasons'],
        'complete_seasons': report['complete_seasons'],
        'total_api_seasons': len(report['api_summary']['seasons']),
        'total_bronze_seasons': len(report['bronze_summary']['season_ids'])
    }
    
    print('DIAGNOSTIC_RESULT:' + json.dumps(result))
    return result

asyncio.run(run_diagnostic())
"
    """
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"STDERR: {result.stderr}")
            raise Exception(f"Diagnostyka nie powiodła się: {result.stderr}")
        
        # Wyciągnij JSON z output
        output_lines = result.stdout.strip().split('\n')
        diagnostic_line = None
        
        for line in output_lines:
            if line.startswith('DIAGNOSTIC_RESULT:'):
                diagnostic_line = line.replace('DIAGNOSTIC_RESULT:', '')
                break
        
        if not diagnostic_line:
            raise Exception("Nie znaleziono wyniku diagnostyki w output")
        
        diagnostic_result = json.loads(diagnostic_line)
        
        print(f"📊 Wyniki diagnostyki:")
        print(f"  • Brakujące sezony: {len(diagnostic_result['missing_seasons'])}")
        print(f"  • Niekompletne sezony: {len(diagnostic_result['incomplete_seasons'])}")
        print(f"  • Kompletne sezony: {len(diagnostic_result['complete_seasons'])}")
        print(f"  • Total API sezony: {diagnostic_result['total_api_seasons']}")
        print(f"  • Total Bronze sezony: {diagnostic_result['total_bronze_seasons']}")
        
        return diagnostic_result
        
    except Exception as e:
        print(f"❌ Błąd podczas diagnostyki: {e}")
        raise

def load_checkpoint(**context):
    """
    Ładuje checkpoint z MinIO lub tworzy nowy
    """
    print("📂 Ładowanie checkpoint...")
    
    command = """
    docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
import json
import os
from minio import Minio
from datetime import datetime

# Inicjalizuj MinIO client
minio_client = Minio(
    endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
    secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
)

checkpoint_key = 'checkpoints/ekstraklasa_backfill_progress.json'

try:
    # Spróbuj załadować istniejący checkpoint
    response = minio_client.get_object('bronze', checkpoint_key)
    checkpoint = json.loads(response.read().decode('utf-8'))
    print('CHECKPOINT_LOADED:' + json.dumps(checkpoint))
except Exception as e:
    # Utwórz nowy checkpoint
    checkpoint = {
        'created_at': datetime.utcnow().isoformat(),
        'last_updated': datetime.utcnow().isoformat(),
        'completed_seasons': [],
        'failed_seasons': [],
        'in_progress_season': None,
        'status': 'initialized'
    }
    print('CHECKPOINT_NEW:' + json.dumps(checkpoint))
"
    """
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Błąd ładowania checkpoint: {result.stderr}")
        
        # Wyciągnij checkpoint z output
        output_lines = result.stdout.strip().split('\n')
        checkpoint = None
        
        for line in output_lines:
            if line.startswith('CHECKPOINT_LOADED:'):
                checkpoint = json.loads(line.replace('CHECKPOINT_LOADED:', ''))
                print("✅ Załadowano istniejący checkpoint")
                break
            elif line.startswith('CHECKPOINT_NEW:'):
                checkpoint = json.loads(line.replace('CHECKPOINT_NEW:', ''))
                print("🆕 Utworzono nowy checkpoint")
                break
        
        if not checkpoint:
            raise Exception("Nie udało się załadować ani utworzyć checkpoint")
        
        return checkpoint
        
    except Exception as e:
        print(f"❌ Błąd checkpoint: {e}")
        raise

def save_checkpoint(checkpoint_data: Dict[str, Any]):
    """
    Zapisuje checkpoint do MinIO
    """
    # Serialize JSON properly
    checkpoint_json = json.dumps(checkpoint_data, indent=None)
    
    # Escape quotes for shell command
    checkpoint_json_escaped = checkpoint_json.replace('"', '\\"')
    
    command = f"""
    docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
import json
import os
from minio import Minio
from datetime import datetime
from io import BytesIO

# Inicjalizuj MinIO client
minio_client = Minio(
    endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
    secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
)

# Parse checkpoint data from JSON string
checkpoint_json = '{checkpoint_json_escaped}'
checkpoint_data = json.loads(checkpoint_json)
checkpoint_data['last_updated'] = datetime.utcnow().isoformat()

checkpoint_bytes = json.dumps(checkpoint_data).encode('utf-8')
checkpoint_key = 'checkpoints/ekstraklasa_backfill_progress.json'

try:
    minio_client.put_object(
        'bronze',
        checkpoint_key,
        BytesIO(checkpoint_bytes),
        len(checkpoint_bytes),
        content_type='application/json'
    )
    print('CHECKPOINT_SAVED:SUCCESS')
except Exception as e:
    print(f'CHECKPOINT_SAVED:ERROR:{{e}}')
    raise
"
    """
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0 or 'CHECKPOINT_SAVED:ERROR' in result.stdout:
        raise Exception(f"Błąd zapisywania checkpoint: {result.stderr}")
    
    print("💾 Checkpoint zapisany")

def calculate_seasons_to_process(**context):
    """
    Oblicza które sezony należy przetworzyć na podstawie diagnostyki i checkpoint
    """
    # Pobierz dane z poprzednich tasków
    diagnostic_result = context['ti'].xcom_pull(task_ids='run_diagnostic')
    checkpoint = context['ti'].xcom_pull(task_ids='load_checkpoint')
    
    # Połącz missing i incomplete seasons
    all_needed_seasons = diagnostic_result['missing_seasons'] + diagnostic_result['incomplete_seasons']
    
    # Usuń już ukończone sezony z checkpoint
    completed_seasons = set(checkpoint.get('completed_seasons', []))
    failed_seasons = set(checkpoint.get('failed_seasons', []))
    
    # Filtruj sezony do przetworzenia
    seasons_to_process = [
        season_id for season_id in all_needed_seasons 
        if season_id not in completed_seasons and season_id not in failed_seasons
    ]
    
    # Jeśli jest sezon w trakcie, rozpocznij od niego
    in_progress = checkpoint.get('in_progress_season')
    if in_progress and in_progress not in completed_seasons:
        if in_progress in seasons_to_process:
            seasons_to_process.remove(in_progress)
        seasons_to_process.insert(0, in_progress)
    
    print(f"🎯 Sezony do przetworzenia: {seasons_to_process}")
    print(f"📋 Już ukończone: {list(completed_seasons)}")
    print(f"❌ Nieudane: {list(failed_seasons)}")
    
    return {
        'seasons_to_process': seasons_to_process,
        'total_needed': len(all_needed_seasons),
        'completed_count': len(completed_seasons),
        'failed_count': len(failed_seasons)
    }

def extract_season_with_retry(season_id: int, tournament_id: int = 202, max_retries: int = 3) -> bool:
    """
    Ekstraktuje sezon z retry logic
    Zwraca True jeśli sukces, False jeśli niepowodzenie
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔄 Próba {attempt}/{max_retries} dla season_id: {season_id}")
            
            # Escape the Python code properly
            command = f"""
            docker exec etl_worker python -c "
import sys
sys.path.append('/opt/etl/scripts')
from sofascore_etl import SofascoreETL
import asyncio

async def run():
    try:
        etl = SofascoreETL()
        result = await etl.extract_tournament_matches(
            tournament_id={tournament_id}, 
            season_id={season_id}, 
            max_pages=25,
            replace_partition=False
        )
        
        # Extract total_matches from result
        total_matches = result.get('total_matches', 0)
        print(f'EXTRACTION_SUCCESS:{{total_matches}}')
        return True
        
    except Exception as e:
        print(f'EXTRACTION_ERROR:{{str(e)}}')
        raise

asyncio.run(run())
"
            """
            
            result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0 and 'EXTRACTION_SUCCESS:' in result.stdout:
                # Wyciągnij liczbę meczów
                for line in result.stdout.split('\n'):
                    if line.startswith('EXTRACTION_SUCCESS:'):
                        match_count = line.replace('EXTRACTION_SUCCESS:', '')
                        print(f"✅ Sukces dla season_id {season_id}: {match_count} meczów")
                        return True
                
                print(f"✅ Sukces dla season_id {season_id}")
                return True
            else:
                print(f"❌ Niepowodzenie próby {attempt}: {result.stderr}")
                if result.stdout:
                    print(f"STDOUT: {result.stdout}")
                
        except subprocess.TimeoutExpired:
            print(f"⏱️ Timeout próby {attempt} dla season_id {season_id}")
        except Exception as e:
            print(f"❌ Błąd próby {attempt}: {e}")
        
        # Opóźnienie przed kolejną próbą
        if attempt < max_retries:
            delay = attempt * 30  # 30s, 60s, 90s
            print(f"⏳ Oczekiwanie {delay}s przed kolejną próbą...")
            time.sleep(delay)
    
    print(f"❌ Wszystkie próby nieudane dla season_id {season_id}")
    return False

def process_seasons_smart(**context):
    """
    Inteligentne przetwarzanie sezonów z checkpoint i retry
    """
    # Pobierz dane z poprzednich tasków
    processing_plan = context['ti'].xcom_pull(task_ids='calculate_seasons')
    checkpoint = context['ti'].xcom_pull(task_ids='load_checkpoint')
    
    seasons_to_process = processing_plan['seasons_to_process']
    
    if not seasons_to_process:
        print("🎉 Wszystkie sezony już przetworzone!")
        return
    
    print(f"🚀 Rozpoczynam przetwarzanie {len(seasons_to_process)} sezonów")
    
    # Aktualizuj checkpoint
    checkpoint['status'] = 'processing'
    
    processed_count = 0
    
    for season_id in seasons_to_process:
        try:
            print(f"\n📅 Przetwarzanie season_id: {season_id} ({processed_count + 1}/{len(seasons_to_process)})")
            
            # Oznacz jako w trakcie
            checkpoint['in_progress_season'] = season_id
            save_checkpoint(checkpoint)
            
            # Ekstraktuj z retry
            success = extract_season_with_retry(season_id)
            
            if success:
                # Dodaj do ukończonych
                if 'completed_seasons' not in checkpoint:
                    checkpoint['completed_seasons'] = []
                checkpoint['completed_seasons'].append(season_id)
                
                # Usuń z nieudanych jeśli był tam
                if 'failed_seasons' in checkpoint and season_id in checkpoint['failed_seasons']:
                    checkpoint['failed_seasons'].remove(season_id)
                
                processed_count += 1
                print(f"✅ Season {season_id} ukończony ({processed_count}/{len(seasons_to_process)})")
                
            else:
                # Dodaj do nieudanych
                if 'failed_seasons' not in checkpoint:
                    checkpoint['failed_seasons'] = []
                if season_id not in checkpoint['failed_seasons']:
                    checkpoint['failed_seasons'].append(season_id)
                
                print(f"❌ Season {season_id} nieudany")
            
            # Wyczyść in_progress
            checkpoint['in_progress_season'] = None
            save_checkpoint(checkpoint)
            
            # Opóźnienie między sezonami (rate limiting)
            print("⏳ Opóźnienie 5s między sezonami...")
            time.sleep(5)
            
        except Exception as e:
            print(f"❌ Krytyczny błąd dla season_id {season_id}: {e}")
            
            # Dodaj do nieudanych
            if 'failed_seasons' not in checkpoint:
                checkpoint['failed_seasons'] = []
            if season_id not in checkpoint['failed_seasons']:
                checkpoint['failed_seasons'].append(season_id)
            
            checkpoint['in_progress_season'] = None
            save_checkpoint(checkpoint)
            
            # Kontynuuj z kolejnym sezonem
            continue
    
    # Finalizuj checkpoint
    checkpoint['status'] = 'completed'
    checkpoint['completed_at'] = datetime.utcnow().isoformat()
    save_checkpoint(checkpoint)
    
    print(f"\n🎉 Przetwarzanie zakończone!")
    print(f"✅ Przetworzone: {processed_count}/{len(seasons_to_process)}")
    print(f"❌ Nieudane: {len(checkpoint.get('failed_seasons', []))}")

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_backfill_orchestrator',
    default_args=default_args,
    description='Smart Ekstraklasa backfill with diagnostic and checkpoint',
    schedule=None,
    catchup=False,
    tags=['ekstraklasa', 'backfill', 'bronze', 'smart']
)

# Tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

diagnostic_task = PythonOperator(
    task_id='run_diagnostic',
    python_callable=run_diagnostic_analysis,
    dag=dag
)

checkpoint_task = PythonOperator(
    task_id='load_checkpoint',
    python_callable=load_checkpoint,
    dag=dag
)

calculate_task = PythonOperator(
    task_id='calculate_seasons',
    python_callable=calculate_seasons_to_process,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_seasons',
    python_callable=process_seasons_smart,
    dag=dag,
    execution_timeout=timedelta(hours=6)  # Zwiększony timeout dla długiego przetwarzania
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag
)

# Dependencies
start_task >> [diagnostic_task, checkpoint_task] >> calculate_task >> process_task >> end_task