#!/usr/bin/env python3
"""
Sofascore ETL Processor - Bronze Layer Implementation
Implements secure HTTP client with retry/backoff and bronze partitioning
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from minio import Minio
from sofascore_client import SofascoreClient
from storage_manager import BronzeStorageManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SofascoreETL:
    """
    Main ETL processor for Sofascore data with:
    - Secure API client with retry/backoff
    - Bronze layer partitioning
    - Deterministic batch tracking
    - Defensive error handling
    """
    
    def __init__(self):
        # Initialize MinIO client
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        
        # Initialize storage manager
        self.storage = BronzeStorageManager(self.minio_client)
        
    async def extract_tournament_matches(
        self, 
        tournament_id: int, 
        season_id: int,
        max_pages: int = 5,
        replace_partition: bool = False
    ) -> Dict[str, Any]:
        """
        Extract matches for tournament/season with partitioning
        
        Args:
            tournament_id: Tournament identifier
            season_id: Season identifier  
            max_pages: Maximum pages to fetch
            replace_partition: Whether to replace existing data
        
        Returns:
            Extraction results with storage summary
        """
        results = {
            "tournament_id": tournament_id,
            "season_id": season_id,
            "total_matches": 0,
            "stored_batches": [],
            "errors": []
        }
        
        async with SofascoreClient() as client:
            for page in range(max_pages):
                try:
                    logger.info(f"Fetching page {page} for tournament {tournament_id}, season {season_id}")
                    
                    # Get matches from API
                    response = await client.get_tournament_matches(tournament_id, season_id, page)
                    
                    # Check if we got valid data
                    matches = response['data'].get('items', [])
                    if not matches:
                        logger.info(f"No more matches found at page {page}")
                        break
                    
                    # Group matches by date for partitioning
                    matches_by_date = self._group_matches_by_date(matches)
                    
                    # Store each date partition separately
                    for match_date, date_matches in matches_by_date.items():
                        try:
                            storage_result = await self.storage.store_batch(
                                data_type="matches",
                                records=date_matches,
                                metadata=response['metadata'],
                                tournament_id=tournament_id,
                                season_id=season_id,
                                match_date=match_date,
                                replace_partition=replace_partition and page == 0  # Only replace on first page
                            )
                            
                            if storage_result['success']:
                                results['stored_batches'].append(storage_result)
                                results['total_matches'] += storage_result['record_count']
                                logger.info(f"Stored {storage_result['record_count']} matches for {match_date}")
                            else:
                                results['errors'].append(f"Storage failed for {match_date}: {storage_result['error']}")
                                
                        except Exception as e:
                            error_msg = f"Error storing matches for {match_date}: {e}"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                    
                except Exception as e:
                    error_msg = f"Error fetching page {page}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    continue
        
        return results
    
    def _group_matches_by_date(self, matches: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group matches by date for partitioning"""
        groups = {}
        
        for match in matches:
            try:
                # Extract date from timestamp
                timestamp = match.get('startTimestamp', 0)
                if timestamp:
                    match_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                else:
                    match_date = 'unknown'
                
                if match_date not in groups:
                    groups[match_date] = []
                groups[match_date].append(match)
                
            except Exception as e:
                logger.warning(f"Error parsing date for match {match.get('id', 'unknown')}: {e}")
                # Use unknown date as fallback
                if 'unknown' not in groups:
                    groups['unknown'] = []
                groups['unknown'].append(match)
        
        return groups
    
    async def extract_match_details(
        self, 
        match_ids: List[int],
        tournament_id: int,
        season_id: int,
        batch_size: int = 50
    ) -> Dict[str, Any]:
        """
        Extract detailed match information in batches
        
        Args:
            match_ids: List of match IDs to fetch
            tournament_id: Tournament identifier for partitioning
            season_id: Season identifier for partitioning
            batch_size: Number of matches to process in each batch
        
        Returns:
            Extraction results with storage summary
        """
        results = {
            "total_processed": 0,
            "stored_batches": [],
            "errors": []
        }
        
        async with SofascoreClient() as client:
            # Process matches in batches
            for i in range(0, len(match_ids), batch_size):
                batch_ids = match_ids[i:i + batch_size]
                batch_matches = []
                
                logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_ids)} matches")
                
                # Fetch each match in the batch
                for match_id in batch_ids:
                    try:
                        response = await client.get_match_details(match_id)
                        
                        if response['data'].get('validation_status') != 'failed':
                            batch_matches.append(response['data'])
                        else:
                            logger.warning(f"Skipping match {match_id} due to validation failure")
                            
                    except Exception as e:
                        error_msg = f"Error fetching match {match_id}: {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                        continue
                
                # Store batch if we have matches
                if batch_matches:
                    try:
                        # Group by date for partitioning
                        matches_by_date = self._group_matches_by_date(batch_matches)
                        
                        for match_date, date_matches in matches_by_date.items():
                            storage_result = await self.storage.store_batch(
                                data_type="match_details",
                                records=date_matches,
                                metadata={
                                    "batch_id": f"details_batch_{i//batch_size + 1}_{match_date}",
                                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                                    "source": "sofascore_api",
                                    "endpoint": "match_details",
                                    "request_params": {"match_ids": batch_ids, "date": match_date}
                                },
                                tournament_id=tournament_id,
                                season_id=season_id,
                                match_date=match_date
                            )
                            
                            if storage_result['success']:
                                results['stored_batches'].append(storage_result)
                                results['total_processed'] += storage_result['record_count']
                            else:
                                results['errors'].append(f"Storage failed for batch {i//batch_size + 1}: {storage_result['error']}")
                                
                    except Exception as e:
                        error_msg = f"Error storing batch {i//batch_size + 1}: {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
        
        return results

async def main():
    """
    Example usage: Extract Ekstraklasa 2025/26 season data
    """
    etl = SofascoreETL()
    
    # Ekstraklasa 2025-2026 configuration
    ekstraklasa_id = 202
    season_2025_id = 76477  
    
    try:
        logger.info("=== Starting Sofascore ETL Process ===")
        
        # Extract tournament matches
        logger.info(f"Extracting matches for Ekstraklasa (ID: {ekstraklasa_id})")
        matches_result = await etl.extract_tournament_matches(
            tournament_id=ekstraklasa_id,
            season_id=season_2025_id,
            max_pages=3,  # Limit for testing
            replace_partition=True  # Replace existing data
        )
        
        logger.info(f"Match extraction completed:")
        logger.info(f"  - Total matches: {matches_result['total_matches']}")
        logger.info(f"  - Stored batches: {len(matches_result['stored_batches'])}")
        logger.info(f"  - Errors: {len(matches_result['errors'])}")
        
        if matches_result['errors']:
            for error in matches_result['errors']:
                logger.error(f"  - {error}")
        
        # List available partitions
        partitions = etl.storage.list_partitions("matches", ekstraklasa_id)
        logger.info(f"Available partitions: {partitions}")
        
        logger.info("=== ETL Process Completed ===")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
