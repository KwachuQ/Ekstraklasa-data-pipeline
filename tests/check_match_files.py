import psycopg2
from minio import Minio
import json
import sys

def check_match_files_coverage():
    # Konfiguracja połączeń
    postgres_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'dwh',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    minio_client = Minio(
        'localhost:9000',
        access_key='minio',
        secret_key='minio123',
        secure=False
    )
    
    # Połączenie z PostgreSQL
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()
    
    # Pobierz wszystkie sezony
    cursor.execute("SELECT DISTINCT season_year FROM bronze.full_matches_data ORDER BY season_year;")
    seasons = [row[0] for row in cursor.fetchall()]
    
    print(f"Found seasons: {seasons}")
    
    for season in seasons:
        print(f"\n=== Checking season {season} ===")
        
        # Pobierz match_ids z bazy dla sezonu
        cursor.execute(
            "SELECT match_id FROM bronze.full_matches_data WHERE season_year = %s;",
            (season,)
        )
        db_match_ids = set(str(row[0]) for row in cursor.fetchall())
        print(f"Database match_ids count: {len(db_match_ids)}")
        
        # Pobierz pliki z MinIO dla sezonu
        bucket_name = 'bronze'
        prefix = f"match_statistics/season_{season.replace('/', '_')}/"
        
        try:
            objects = minio_client.list_objects(bucket_name, prefix=prefix)
            minio_match_ids = set()
            
            for obj in objects:
                filename = obj.object_name.split('/')[-1]  # match_123.json
                if filename.startswith('match_') and filename.endswith('.json'):
                    match_id = filename.replace('match_', '').replace('.json', '')
                    minio_match_ids.add(match_id)
            
            print(f"MinIO files count: {len(minio_match_ids)}")
            
            # Porównanie
            missing_in_minio = db_match_ids - minio_match_ids
            extra_in_minio = minio_match_ids - db_match_ids
            
            print(f"Missing in MinIO: {len(missing_in_minio)}")
            print(f"Extra in MinIO: {len(extra_in_minio)}")
            
            if missing_in_minio:
                print(f"Missing match_ids: {sorted(list(missing_in_minio))[:10]}...")  # Pokaż pierwszych 10
            
            if extra_in_minio:
                print(f"Extra match_ids: {sorted(list(extra_in_minio))[:10]}...")  # Pokaż pierwszych 10
                
            coverage_percent = (len(minio_match_ids) / len(db_match_ids) * 100) if db_match_ids else 0
            print(f"Coverage: {coverage_percent:.1f}%")
            
        except Exception as e:
            print(f"Error accessing MinIO for season {season}: {e}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_match_files_coverage()