#!/usr/bin/env python3
"""
Simple script to explore bronze data content
Usage: python explore_bronze_content.py [date]
"""

# filepath: etl/scripts/explore_bronze_content.py
import json
import sys
from datetime import datetime
from minio import Minio
import os

def init_minio_client():
    """Initialize MinIO client"""
    return Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    )

def list_available_dates(client):
    """List all available dates in bronze layer"""
    objects = client.list_objects('bronze', prefix='matches/', recursive=True)
    ndjson_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]
    
    dates = set()
    for obj in ndjson_files:
        path_parts = obj.object_name.split('/')
        date_part = [part for part in path_parts if part.startswith('date=')]
        if date_part:
            dates.add(date_part[0].replace('date=', ''))
    
    return sorted(list(dates), reverse=True)

def read_matches_for_date(client, target_date):
    """Read all matches for a specific date"""
    prefix = f"matches/tournament_id=202/season_id=76477/date={target_date}/"
    objects = client.list_objects('bronze', prefix=prefix, recursive=True)
    ndjson_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]
    
    all_matches = []
    for obj in ndjson_files:
        try:
            response = client.get_object('bronze', obj.object_name)
            content = response.read().decode('utf-8')
            
            for line in content.strip().split('\n'):
                if line.strip():
                    match = json.loads(line)
                    all_matches.append(match)
        except Exception as e:
            print(f"Error reading {obj.object_name}: {e}")
    
    return all_matches

def analyze_match_structure(match):
    """Analyze the structure of a match record"""
    print("🔍 STRUKTURA REKORDU MECZU:")
    print("=" * 50)
    
    # Remove metadata for cleaner view
    clean_match = {k: v for k, v in match.items() if k != '_metadata'}
    
    def print_structure(obj, level=0, max_level=2):
        indent = "  " * level
        if level > max_level:
            return
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, dict):
                    print(f"{indent}📁 {key}: dict ({len(value)} keys)")
                    if level < max_level:
                        print_structure(value, level + 1, max_level)
                elif isinstance(value, list):
                    print(f"{indent}📋 {key}: list ({len(value)} items)")
                    if value and level < max_level:
                        print(f"{indent}  Example item:")
                        print_structure(value[0], level + 2, max_level)
                else:
                    value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    print(f"{indent}📄 {key}: {type(value).__name__} = {value_str}")
    
    print_structure(clean_match)

def display_matches_summary(matches):
    """Display a summary of matches"""
    print(f"\n⚽ PRZEGLĄD {len(matches)} MECZÓW:")
    print("=" * 80)
    
    for i, match in enumerate(matches, 1):
        # Basic match info
        home = match.get('homeTeam', {}).get('name', 'Unknown')
        away = match.get('awayTeam', {}).get('name', 'Unknown')
        status = match.get('status', {}).get('type', 'Unknown')
        
        # Score if available
        home_score = match.get('homeScore', {}).get('current') if match.get('homeScore') else None
        away_score = match.get('awayScore', {}).get('current') if match.get('awayScore') else None
        score_str = f" ({home_score}-{away_score})" if home_score is not None and away_score is not None else ""
        
        # Time
        timestamp = match.get('startTimestamp')
        time_str = datetime.fromtimestamp(timestamp).strftime('%H:%M') if timestamp else 'N/A'
        
        # Additional info
        round_info = match.get('roundInfo', {}).get('round', 'N/A')
        venue = match.get('venue', {}).get('stadium', {}).get('name', 'N/A') if match.get('venue') else 'N/A'
        
        print(f"{i:2d}. {time_str} | {home} vs {away}{score_str} [{status}]")
        print(f"     🏟️  {venue} | 🎯 Kolejka {round_info} | 🆔 ID: {match.get('id', 'N/A')}")

def display_full_match_json(match, index=0):
    """Display full JSON of a match"""
    print(f"\n📄 PEŁNY JSON MECZU #{index + 1}:")
    print("=" * 80)
    
    # Remove metadata for cleaner display
    clean_match = {k: v for k, v in match.items() if k != '_metadata'}
    print(json.dumps(clean_match, indent=2, ensure_ascii=False))
    
    # Show metadata separately
    if '_metadata' in match:
        print(f"\n📦 METADANE BATCH:")
        print("-" * 40)
        for key, value in match['_metadata'].items():
            print(f"{key}: {value}")

def main():
    """Main exploration function"""
    client = init_minio_client()
    
    print("🔍 BRONZE DATA EXPLORER - EKSTRAKLASA")
    print("=" * 60)
    
    # Get available dates
    available_dates = list_available_dates(client)
    
    if not available_dates:
        print("❌ Brak danych w bronze layer")
        return
    
    print(f"📅 Dostępne daty ({len(available_dates)}):")
    for i, date in enumerate(available_dates[:10], 1):
        print(f"  {i:2d}. {date}")
    
    if len(available_dates) > 10:
        print(f"      ... i {len(available_dates) - 10} więcej")
    
    # Determine target date
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = available_dates[0]  # Most recent
    
    if target_date not in available_dates:
        print(f"❌ Data {target_date} nie jest dostępna")
        print(f"💡 Użyj: python {sys.argv[0]} [data]")
        print(f"💡 Przykład: python {sys.argv[0]} {available_dates[0]}")
        return
    
    print(f"\n🎯 Analizuję dane z daty: {target_date}")
    
    # Read matches for the date
    matches = read_matches_for_date(client, target_date)
    
    if not matches:
        print(f"❌ Brak meczów dla daty {target_date}")
        return
    
    # Display summary
    display_matches_summary(matches)
    
    # Analyze structure of first match
    if matches:
        print(f"\n" + "="*80)
        analyze_match_structure(matches[0])
    
    # Show full JSON of first match
    if matches:
        print(f"\n" + "="*80)
        display_full_match_json(matches[0])
    
    print(f"\n💡 Aby zobaczyć inny mecz, uruchom:")
    print(f"   python {sys.argv[0]} {target_date}")
    print(f"\n💡 Dostępne daty: {', '.join(available_dates[:5])}...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Przerwano przez użytkownika")
    except Exception as e:
        print(f"❌ Błąd: {e}")
        sys.exit(1)