"""
Prosty skrypt do pobierania statystyk meczów z API Sofascore
i zapisywania ich do MinIO bucket bronze
"""
import requests
import json
import os
import time
from datetime import datetime
from typing import List, Dict
from minio import Minio
from minio.error import S3Error
from io import BytesIO

class StatisticsFetcher:
    def __init__(self, rapidapi_key: str, tournament_id: int = 202):
        self.rapidapi_key = rapidapi_key
        self.tournament_id = tournament_id
        self.api_url = "https://sofascore.p.rapidapi.com/matches/get-statistics"
        self.api_host = "sofascore.p.rapidapi.com"
        
        # MinIO setup
        self.minio_client = Minio(
            "minio:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        
        self.bucket_name = "bronze"
        
        # Season dla 2025/26
        self.season_folder = "season_2025_26"
        
        print(f"✅ StatisticsFetcher initialized")
        print(f"  • Tournament ID: {tournament_id}")
        print(f"  • API Key: {'*' * 20}{rapidapi_key[-8:]}")
    
    def fetch_match_statistics(self, match_id: int) -> Dict:
        """Pobierz statystyki dla pojedynczego meczu"""
        
        headers = {
            "x-rapidapi-key": self.rapidapi_key,
            "x-rapidapi-host": self.api_host
        }
        
        params = {"matchId": str(match_id)}
        
        try:
            response = requests.get(
                self.api_url, 
                headers=headers, 
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"  ✅ match_id={match_id}: pobrano statystyki")
                return {
                    'match_id': match_id,
                    'status': 'success',
                    'data': data
                }
            elif response.status_code == 404:
                print(f"  ⚠️ match_id={match_id}: brak statystyk (404)")
                return {
                    'match_id': match_id,
                    'status': 'not_found',
                    'data': None
                }
            else:
                print(f"  ❌ match_id={match_id}: HTTP {response.status_code}")
                print(f"     Error: {response.text[:200]}")
                return {
                    'match_id': match_id,
                    'status': 'error',
                    'data': None,
                    'error': f"HTTP {response.status_code}"
                }
                
        except requests.exceptions.Timeout:
            print(f"  ⏱️ match_id={match_id}: timeout")
            return {
                'match_id': match_id,
                'status': 'timeout',
                'data': None
            }
        except Exception as e:
            print(f"  ❌ match_id={match_id}: {type(e).__name__}: {e}")
            return {
                'match_id': match_id,
                'status': 'error',
                'data': None,
                'error': str(e)
            }
    
    def save_to_minio(self, match_id: int, data: Dict) -> bool:
        """Zapisz statystyki do MinIO"""
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Ścieżka: match_statistics/season_2025_26/date=YYYY-MM-DD/match_{id}.json
        object_name = (
            f"match_statistics/{self.season_folder}/"
            f"date={today}/match_{match_id}.json"
        )
        
        try:
            # Konwertuj do JSON
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            json_bytes = json_data.encode('utf-8')
            
            # Utwórz stream
            data_stream = BytesIO(json_bytes)
            
            # Upload do MinIO
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                data_stream,
                length=len(json_bytes),
                content_type='application/json'
            )
            
            print(f"  💾 Zapisano: {object_name}")
            return True
            
        except S3Error as e:
            print(f"  ❌ MinIO error dla match_id={match_id}: {e}")
            return False
        except Exception as e:
            print(f"  ❌ Save error dla match_id={match_id}: {e}")
            return False
    
    def fetch_and_save_statistics(
        self, 
        match_ids: List[int],
        delay_between_requests: float = 1.5
    ) -> Dict:
        """
        Pobierz statystyki dla listy meczów i zapisz do MinIO
        
        Args:
            match_ids: Lista match_id do pobrania
            delay_between_requests: Opóźnienie między requestami (sekundy)
        
        Returns:
            Dict z wynikami: {'successful': int, 'failed': int, 'skipped': int}
        """
        
        print(f"\n🚀 Rozpoczynam pobieranie statystyk dla {len(match_ids)} meczów")
        print(f"  • Delay: {delay_between_requests}s między requestami")
        
        results = {
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        for i, match_id in enumerate(match_ids, 1):
            print(f"\n📊 [{i}/{len(match_ids)}] match_id={match_id}")
            
            # Pobierz statystyki
            result = self.fetch_match_statistics(match_id)
            
            status = result['status']
            
            if status == 'success' and result['data']:
                # Zapisz do MinIO
                if self.save_to_minio(match_id, result['data']):
                    results['successful'] += 1
                else:
                    results['failed'] += 1
            
            elif status == 'not_found':
                results['skipped'] += 1
            
            else:
                results['failed'] += 1
            
            # Opóźnienie przed następnym requestem (oprócz ostatniego)
            if i < len(match_ids):
                time.sleep(delay_between_requests)
        
        print(f"\n✅ Pobieranie zakończone:")
        print(f"  • Sukces: {results['successful']}")
        print(f"  • Błędy: {results['failed']}")
        print(f"  • Pominięte: {results['skipped']}")
        
        return results


# Przykład użycia (do testów)
if __name__ == "__main__":
    import sys
    
    # Test dla pojedynczego meczu
    test_match_ids = [12648635]
    
    api_key = os.getenv('RAPIDAPI_KEY', '')
    
    if not api_key:
        print("❌ Brak RAPIDAPI_KEY w zmiennych środowiskowych!")
        sys.exit(1)
    
    fetcher = StatisticsFetcher(rapidapi_key=api_key)
    result = fetcher.fetch_and_save_statistics(test_match_ids)
    print(f"\n📊 Test result: {result}")