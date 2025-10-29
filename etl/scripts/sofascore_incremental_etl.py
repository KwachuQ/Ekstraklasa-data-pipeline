#!/usr/bin/env python3
"""
Sofascore Incremental ETL - Bronze Layer
Fetches only NEW matches since last extraction date
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from minio import Minio
from sofascore_client import SofascoreClient
from storage_manager import BronzeStorageManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SofascoreIncrementalETL:
    """
    Incremental ETL for fetching only new matches since last extraction
    """
    
    def __init__(self):
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        self.storage = BronzeStorageManager(self.minio_client)
    
    async def extract_new_matches(
        self,
        tournament_id: int,
        season_id: int,
        last_match_date: str,  # Format: 'YYYY-MM-DD'
        max_pages: int = 25
    ) -> Dict[str, Any]:
        """
        Extract ONLY matches newer than last_match_date
        
        Args:
            tournament_id: Tournament ID (e.g., 202 for Ekstraklasa)
            season_id: Season ID (e.g., 76477 for 2025/26)
            last_match_date: Last extracted date in 'YYYY-MM-DD' format
            max_pages: Maximum pages to scan
        
        Returns:
            Results with list of new matches and storage summary
        """
        results = {
            "tournament_id": tournament_id,
            "season_id": season_id,
            "last_match_date": last_match_date,
            "new_matches": [],
            "total_new_matches": 0,
            "stored_batches": [],
            "errors": []
        }
        
        # Convert last_match_date to datetime for comparison
        cutoff_date = datetime.strptime(last_match_date, '%Y-%m-%d').date()
        logger.info(f"Fetching matches after {cutoff_date}")
        
        all_new_matches = []
        
        async with SofascoreClient() as client:
            for page in range(max_pages):
                try:
                    logger.info(f"Scanning page {page}...")
                    
                    # Fetch matches from API
                    response = await client.get_tournament_matches(
                        tournament_id, 
                        season_id, 
                        page
                    )
                    
                    matches = response['data'].get('items', [])
                    if not matches:
                        logger.info(f"No more matches at page {page}")
                        break
                    
                    # Filter NEW matches (after cutoff_date)
                    page_new_matches = []
                    found_old_match = False
                    
                    for match in matches:
                        timestamp = match.get('startTimestamp', 0)
                        if timestamp:
                            match_date = datetime.fromtimestamp(timestamp).date()
                            
                            if match_date > cutoff_date:
                                page_new_matches.append(match)
                                logger.info(f"  ✓ New match: ID={match.get('id')} Date={match_date}")
                            else:
                                found_old_match = True
                                logger.debug(f"  ⊗ Old match: ID={match.get('id')} Date={match_date}")
                    
                    all_new_matches.extend(page_new_matches)
                    
                    # Stop scanning if we hit old matches
                    if found_old_match:
                        logger.info(f"Reached old matches at page {page}, stopping scan")
                        break
                    
                except Exception as e:
                    error_msg = f"Error fetching page {page}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    continue
        
        # Store new matches if any found
        if all_new_matches:
            logger.info(f"Found {len(all_new_matches)} new matches, storing to MinIO...")
            
            # Group by date for partitioning
            matches_by_date = self._group_matches_by_date(all_new_matches)
            
            for match_date, date_matches in matches_by_date.items():
                try:
                    storage_result = await self.storage.store_batch(
                        data_type="matches",
                        records=date_matches,
                        metadata={
                            "batch_id": f"incremental_{tournament_id}_{season_id}_{match_date}",
                            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                            "source": "sofascore_api",
                            "endpoint": "get_tournament_matches",
                            "extraction_type": "incremental",
                            "last_match_date": last_match_date
                        },
                        tournament_id=tournament_id,
                        season_id=season_id,
                        match_date=match_date,
                        replace_partition=False  # APPEND, don't replace
                    )
                    
                    if storage_result['success']:
                        results['stored_batches'].append(storage_result)
                        results['total_new_matches'] += storage_result['record_count']
                        logger.info(f"✓ Stored {storage_result['record_count']} matches for {match_date}")
                    else:
                        results['errors'].append(f"Storage failed for {match_date}: {storage_result['error']}")
                
                except Exception as e:
                    error_msg = f"Error storing {match_date}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
        else:
            logger.info("No new matches found")
        
        results['new_matches'] = all_new_matches
        return results
    
    def _group_matches_by_date(self, matches: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group matches by date for partitioning"""
        groups = {}
        
        for match in matches:
            try:
                timestamp = match.get('startTimestamp', 0)
                if timestamp:
                    match_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                else:
                    match_date = 'unknown'
                
                if match_date not in groups:
                    groups[match_date] = []
                groups[match_date].append(match)
            
            except Exception as e:
                logger.warning(f"Error parsing date for match {match.get('id')}: {e}")
                if 'unknown' not in groups:
                    groups['unknown'] = []
                groups['unknown'].append(match)
        
        return groups

async def main():
    """
    Test incremental extraction
    """
    etl = SofascoreIncrementalETL()
    
    # Ekstraklasa config
    tournament_id = 202
    season_id = 76477
    last_date = "2025-09-15"  # Last extracted date
    
    try:
        logger.info("=== Starting Incremental ETL ===")
        
        result = await etl.extract_new_matches(
            tournament_id=tournament_id,
            season_id=season_id,
            last_match_date=last_date,
            max_pages=25
        )
        
        logger.info(f"=== Results ===")
        logger.info(f"New matches: {result['total_new_matches']}")
        logger.info(f"Stored batches: {len(result['stored_batches'])}")
        logger.info(f"Errors: {len(result['errors'])}")
        
        if result['errors']:
            for error in result['errors']:
                logger.error(f"  - {error}")
        
    except Exception as e:
        logger.error(f"Incremental ETL failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())