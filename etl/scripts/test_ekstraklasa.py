#!/usr/bin/env python3
"""
Test script specifically for Ekstraklasa 2025-2026 data extraction
"""

import asyncio
import json
import logging
import os
from datetime import datetime

from sofascore_client import SofascoreClient
from storage_manager import BronzeStorageManager
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ekstraklasa 2025-2026 test configuration
EKSTRAKLASA_2025_CONFIG = {
    "tournament_id": 202,
    "season_id": 76477, 
    "category_id": 47,
    "tournament_name": "PKO BP Ekstraklasa",
    "season_name": "2025/26"
}

async def test_ekstraklasa_api_2025():
    """Test Ekstraklasa 2025-2026 API endpoints directly"""
    logger.info("Testing Ekstraklasa 2025-2026 API endpoints...")
    
    tournament_id = EKSTRAKLASA_2025_CONFIG["tournament_id"]
    season_id = EKSTRAKLASA_2025_CONFIG["season_id"]
    
    async with SofascoreClient() as client:
        try:
            # Test 1: Get tournament seasons to verify 2025-26 season
            logger.info("Test 1: Checking available seasons for Ekstraklasa...")
            seasons_response = await client.get_tournament_seasons(tournament_id)
            
            seasons = seasons_response['data'].get('items', [])
            logger.info(f"✅ Found {len(seasons)} seasons for tournament {tournament_id}")
            
            # Look for 2025-26 season
            target_season = None
            season_2025_found = False
            
            for season in seasons:
                season_name = season.get('name', '').lower()
                season_year = season.get('year', '')
                
                if '2025' in season_year or '2025' in season_name:
                    season_2025_found = True
                    if season.get('id') == season_id:
                        target_season = season
                        break
            
            if target_season:
                logger.info(f"✅ Target 2025-26 season found: {target_season.get('name', 'Unknown')} (ID: {season_id})")
            elif season_2025_found:
                logger.warning(f"⚠️  2025 season found but not with ID {season_id}")
                logger.info("Available 2025 seasons:")
                for season in seasons:
                    if '2025' in season.get('year', '') or '2025' in season.get('name', ''):
                        logger.info(f"   - {season.get('name', 'Unknown')} (ID: {season.get('id')})")
            else:
                logger.warning(f"⚠️  No 2025-26 season found yet. Season may not have started.")
                logger.info("Latest available seasons:")
                for season in seasons[:5]:
                    logger.info(f"   - {season.get('name', 'Unknown')} (ID: {season.get('id')})")
            
            # Test 2: Try to get matches for 2025-26 season
            logger.info(f"\nTest 2: Attempting to fetch Ekstraklasa 2025-26 matches...")
            try:
                matches_response = await client.get_tournament_matches(tournament_id, season_id, page=0)
                
                matches = matches_response['data'].get('items', [])
                logger.info(f"✅ Retrieved {len(matches)} matches for 2025-26 season")
                
                if matches:
                    sample_match = matches[0]
                    logger.info(f"Sample 2025-26 match:")
                    logger.info(f"   - ID: {sample_match.get('id')}")
                    logger.info(f"   - Home: {sample_match.get('homeTeam', {}).get('name', 'Unknown')}")
                    logger.info(f"   - Away: {sample_match.get('awayTeam', {}).get('name', 'Unknown')}")
                    
                    timestamp = sample_match.get('startTimestamp')
                    if timestamp:
                        match_date = datetime.fromtimestamp(timestamp)
                        logger.info(f"   - Date: {match_date.strftime('%Y-%m-%d %H:%M')}")
                        
                        # Verify it's actually from 2025-26 season
                        if match_date.year in [2025, 2026]:
                            logger.info(f"✅ Match date confirms 2025-26 season")
                        else:
                            logger.warning(f"⚠️  Match date {match_date.year} doesn't match 2025-26 season")
                else:
                    logger.info("ℹ️  No matches available yet (season may not have started)")
                    
            except Exception as e:
                logger.warning(f"⚠️  Could not fetch 2025-26 matches: {e}")
                logger.info("This is expected if the 2025-26 season hasn't started yet")
            
            # Test 3: Check batch metadata generation
            logger.info(f"\nTest 3: Testing batch metadata generation...")
            test_params = {"tournament_id": tournament_id, "season_id": season_id, "page": 0}
            batch_id = client._generate_batch_id("test_endpoint", test_params)
            metadata = client._get_ingestion_metadata("test_endpoint", test_params)
            
            logger.info(f"✅ Batch ID: {batch_id}")
            logger.info(f"✅ Metadata keys: {list(metadata.keys())}")
            logger.info(f"✅ Source: {metadata['source']}")
            
            return True
            
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

async def test_ekstraklasa_storage_2025():
    """Test storage with Ekstraklasa 2025-26 data structure"""
    logger.info("Testing Ekstraklasa 2025-26 storage structure...")
    
    # Initialize MinIO client
    minio_client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    )
    
    storage = BronzeStorageManager(minio_client)
    
    # Test partition key generation for Ekstraklasa 2025-26
    tournament_id = EKSTRAKLASA_2025_CONFIG["tournament_id"]
    season_id = EKSTRAKLASA_2025_CONFIG["season_id"]
    test_date = "2025-08-15"  # Expected early season date
    test_match_id = 12345
    
    partition_key = storage.generate_partition_key(tournament_id, season_id, test_date, test_match_id)
    expected_key = f"tournament_id={tournament_id}/season_id={season_id}/date={test_date}/match_id={test_match_id}"
    
    logger.info(f"✅ 2025-26 partition key: {partition_key}")
    assert partition_key == expected_key, f"Expected {expected_key}, got {partition_key}"
    
    # Test object key generation
    object_key = storage.generate_object_key("matches", partition_key, "ekstraklasa_2025_batch_123")
    logger.info(f"✅ 2025-26 object key: {object_key}")
    
    # Test sample Ekstraklasa 2025-26 data storage
    sample_2025_data = [
        {
            "id": 12345,
            "homeTeam": {"name": "Legia Warszawa", "id": 1001},
            "awayTeam": {"name": "Lech Poznań", "id": 1002},
            "status": {"type": "notstarted"},  # Future match in 2025
            "startTimestamp": 1723680000,  # Aug 15, 2025
            "tournament": {"id": tournament_id, "name": "PKO BP Ekstraklasa"},
            "season_info": {"year": "2025-26", "id": season_id}
        }
    ]
    
    sample_metadata = {
        "batch_id": "test_ekstraklasa_2025_123",
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "source": "sofascore_api",
        "endpoint": "test_2025_season",
        "request_params": {
            "tournament_id": tournament_id, 
            "season_id": season_id,
            "season": "2025-26"
        },
        "season_year": "2025-26"
    }
    
    try:
        storage_result = await storage.store_batch(
            data_type="test_matches_2025",
            records=sample_2025_data,
            metadata=sample_metadata,
            tournament_id=tournament_id,
            season_id=season_id,
            match_date=test_date,
            match_id=test_match_id
        )
        
        if storage_result['success']:
            logger.info(f"✅ 2025-26 test data stored: {storage_result['object_key']}")
            
            # Verify manifest
            manifest = storage.get_batch_manifest(storage_result['object_key'])
            if manifest:
                logger.info(f"✅ 2025-26 manifest verified: {manifest['record_count']} records")
                logger.info(f"✅ Season metadata: {manifest['metadata'].get('season_year', 'not_set')}")
            else:
                logger.warning("⚠️  2025-26 manifest not found")
                
            # List partitions to see 2025-26 structure
            partitions = storage.list_partitions("test_matches_2025", tournament_id)
            logger.info(f"✅ Available 2025-26 partitions: {partitions}")
                
        else:
            logger.error(f"❌ 2025-26 storage failed: {storage_result['error']}")
            
        return storage_result['success']
        
    except Exception as e:
        logger.error(f"Storage test failed: {e}")
        return False

async def test_ekstraklasa_integration_2025():
    """Test full integration for Ekstraklasa 2025-26"""
    logger.info("Testing Ekstraklasa 2025-26 full integration...")
    
    try:
        from ekstraklasa_config import EkstraklasaETL2025
        
        etl = EkstraklasaETL2025()
        
        # Test configuration
        config = etl.config
        logger.info(f"✅ 2025-26 configuration loaded:")
        logger.info(f"   Tournament: {config['tournament_name']}")
        logger.info(f"   Season: {config['season_name']}")
        logger.info(f"   IDs: {config['tournament_id']}/{config['season_id']}")
        
        # Test season validation
        validation = await etl.validate_season_data()
        logger.info(f"✅ 2025-26 season validation:")
        logger.info(f"   Has data: {validation['data_availability']['has_data']}")
        logger.info(f"   Quality: {validation['quality_checks']['partition_structure']}")
        
        # Test summary generation
        summary = await etl.get_season_summary()
        logger.info(f"✅ 2025-26 season summary:")
        logger.info(f"   Season partitions: {summary['season_partitions']}")
        logger.info(f"   Date range: {summary['date_range']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        return False

async def main():
    """Run Ekstraklasa 2025-26 specific tests"""
    logger.info("=" * 60)
    logger.info("🇵🇱 EKSTRAKLASA 2025-2026 ETL TESTS")
    logger.info("=" * 60)
    
    test_results = {}
    
    # Test API connectivity for 2025-26
    test_results['api_2025'] = await test_ekstraklasa_api_2025()
    
    # Test storage functionality for 2025-26
    test_results['storage_2025'] = await test_ekstraklasa_storage_2025()
    
    # Test integration for 2025-26
    test_results['integration_2025'] = await test_ekstraklasa_integration_2025()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("EKSTRAKLASA 2025-2026 TEST RESULTS:")
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"  {test_name.upper()}: {status}")
    
    all_passed = all(test_results.values())
    if all_passed:
        logger.info("🎉 All Ekstraklasa 2025-26 tests passed!")
        logger.info("🚀 Ready to extract Ekstraklasa 2025-26 data when season starts!")
    else:
        logger.warning("⚠️  Some tests failed, but this may be expected")
        logger.info("💡 Check if 2025-26 season has started and adjust season_id if needed")
    
    logger.info("\n📅 NOTE: If season hasn't started yet:")
    logger.info("   - API tests may show no data (expected)")
    logger.info("   - Storage tests should pass (system ready)")
    logger.info("   - Update season_id when 2025-26 season officially begins")
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
