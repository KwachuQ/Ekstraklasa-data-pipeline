#!/usr/bin/env python3
"""
Sofascore ETL Processor - Bronze Layer Implementation
Implements secure HTTP client with retry/backoff and bronze partitioning
"""

import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from minio import Minio
from etl.bronze.client import SofascoreClient
from etl.bronze.storage import BronzeStorageManager

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Standardized extraction result"""
    tournament_id: int
    season_id: int
    total_matches: int = 0
    stored_batches: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility with DAGs"""
        return {
            'tournament_id': self.tournament_id,
            'season_id': self.season_id,
            'total_matches': self.total_matches,
            'stored_batches': self.stored_batches,
            'errors': self.errors
        }


class SofascoreETL:
    """
    Main ETL processor for Sofascore data with:
    - Secure API client with retry/backoff
    - Bronze layer partitioning
    - Deterministic batch tracking
    - Defensive error handling
    """
    
    def __init__(self):
        """Initialize ETL with MinIO client and storage manager"""
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        self.storage = BronzeStorageManager(self.minio_client, bucket_name=os.getenv('BRONZE_BUCKET', 'bronze-test'))
    
    def __enter__(self):
        """Support sync context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources"""
        return False
    
    async def __aenter__(self):
        """Support async context manager"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources"""
        pass
    
    def _validate_extraction_params(
        self,
        tournament_id: int,
        season_id: int,
        max_pages: int
    ) -> None:
        """Validate extraction parameters"""
        if tournament_id <= 0:
            raise ValueError(f"Invalid tournament_id: {tournament_id}")
        if season_id <= 0:
            raise ValueError(f"Invalid season_id: {season_id}")
        if max_pages <= 0:
            raise ValueError(f"Invalid max_pages: {max_pages}")
    
    def _handle_error(
        self,
        error: Exception,
        context: str,
        results: ExtractionResult
    ) -> None:
        """Centralized error handling"""
        error_msg = f"{context}: {type(error).__name__}: {str(error)}"
        logger.error(error_msg, exc_info=logger.isEnabledFor(logging.DEBUG))
        results.errors.append(error_msg)
    
    def _group_matches_by_date(
        self,
        matches: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group matches by date for partitioning
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            Dictionary mapping dates to match lists
        """
        groups = defaultdict(list)
        
        for match in matches:
            try:
                timestamp = match.get('startTimestamp', 0)
                match_date = (
                    datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    if timestamp else 'unknown'
                )
            except (OSError, ValueError) as e:
                logger.warning(
                    f"Invalid timestamp for match {match.get('id', 'unknown')}: {e}"
                )
                match_date = 'unknown'
            
            groups[match_date].append(match)
        
        return dict(groups)
    
    def _store_matches_by_date(
        self,
        matches_by_date: Dict[str, List[Dict[str, Any]]],
        metadata: Dict[str, Any],
        tournament_id: int,
        season_id: int,
        replace_partition: bool,
        results: ExtractionResult
    ) -> None:
        """
        Store matches grouped by date
        
        Args:
            matches_by_date: Matches grouped by date
            metadata: Batch metadata
            tournament_id: Tournament identifier
            season_id: Season identifier
            replace_partition: Whether to replace existing partition
            results: Results object to update
        """
        for match_date, date_matches in matches_by_date.items():
            try:
                storage_result = self.storage.store_batch(
                    data_type="matches",
                    records=date_matches,
                    metadata=metadata,
                    tournament_id=tournament_id,
                    season_id=season_id,
                    match_date=match_date,
                    replace_partition=replace_partition
                )
                
                if storage_result['success']:
                    results.stored_batches.append(storage_result)
                    results.total_matches += storage_result['record_count']
                    logger.info(
                        f"✓ Stored {storage_result['record_count']} matches for {match_date}"
                    )
                else:
                    error_msg = storage_result.get('error', 'Unknown storage error')
                    self._handle_error(
                        Exception(error_msg),
                        f"Storage failed for {match_date}",
                        results
                    )
                    
            except Exception as e:
                self._handle_error(e, f"Error storing matches for {match_date}", results)
    
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
            Dictionary with extraction results (for DAG compatibility)
        """
        # Validate inputs
        self._validate_extraction_params(tournament_id, season_id, max_pages)
        
        # Initialize results
        results = ExtractionResult(
            tournament_id=tournament_id,
            season_id=season_id
        )
        
        logger.info(
            f"Starting extraction: tournament={tournament_id}, "
            f"season={season_id}, max_pages={max_pages}"
        )
        
        async with SofascoreClient() as client:
            for page in range(max_pages):
                try:
                    logger.info(f"Fetching page {page + 1}/{max_pages}...")
                    
                    # Fetch matches from API
                    response = await client.get_tournament_matches(
                        tournament_id,
                        season_id,
                        page
                    )
                    
                    # Check for data
                    matches = response.validated_items
                    if not matches:
                        logger.info(f"No more matches found at page {page + 1}")
                        break
                    
                    # Group and store by date
                    matches_by_date = self._group_matches_by_date(matches)
                    self._store_matches_by_date(
                        matches_by_date=matches_by_date,
                        metadata=response.metadata,
                        tournament_id=tournament_id,
                        season_id=season_id,
                        replace_partition=replace_partition and page == 0,
                        results=results
                    )
                    
                except Exception as e:
                    self._handle_error(e, f"Error fetching page {page + 1}", results)
                    continue
        
        logger.info(
            f"Extraction complete: {results.total_matches} matches, "
            f"{len(results.errors)} errors"
        )
        
        return results.to_dict()  # Return dict for DAG compatibility
    
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
            Dictionary with extraction results (for DAG compatibility)
        """
        if not match_ids:
            logger.warning("No match IDs provided")
            return {
                'total_processed': 0,
                'stored_batches': [],
                'errors': []
            }
        
        results = {
            'total_processed': 0,
            'stored_batches': [],
            'errors': []
        }
        
        total_batches = (len(match_ids) + batch_size - 1) // batch_size
        logger.info(f"Processing {len(match_ids)} matches in {total_batches} batches")
        
        async with SofascoreClient() as client:
            for batch_start_idx in range(0, len(match_ids), batch_size):
                batch_ids = match_ids[batch_start_idx:batch_start_idx + batch_size]
                batch_number = (batch_start_idx // batch_size) + 1
                batch_matches = []
                
                logger.info(f"Processing batch {batch_number}/{total_batches}")
                
                # Fetch matches in batch
                for match_id in batch_ids:
                    try:
                        response = await client.get_match_details(match_id)
                        
                        if response.is_valid:
                            if response.validated_items:
                                batch_matches.extend(response.validated_items)
                        else:
                            logger.warning(
                                f"Skipping match {match_id} - validation failed"
                            )
                            
                    except Exception as e:
                        error_msg = (
                            f"Error fetching match {match_id}: "
                            f"{type(e).__name__}: {str(e)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                        continue
                
                # Store batch if we have valid matches
                if batch_matches:
                    self._store_match_details_batch(
                        batch_matches=batch_matches,
                        batch_number=batch_number,
                        tournament_id=tournament_id,
                        season_id=season_id,
                        results=results
                    )
        
        logger.info(
            f"Match details extraction complete: {results['total_processed']} processed"
        )
        return results
    
    async def _store_match_details_batch(
        self,
        batch_matches: List[Dict[str, Any]],
        batch_number: int,
        tournament_id: int,
        season_id: int,
        results: Dict[str, Any]
    ) -> None:
        """Store a batch of match details"""
        try:
            matches_by_date = self._group_matches_by_date(batch_matches)
            
            for match_date, date_matches in matches_by_date.items():
                storage_result = self.storage.store_batch(
                    data_type="match_details",
                    records=date_matches,
                    metadata={
                        "batch_id": f"details_batch_{batch_number}_{match_date}",
                        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "sofascore_api",
                        "endpoint": "match_details",
                        "batch_number": batch_number
                    },
                    tournament_id=tournament_id,
                    season_id=season_id,
                    match_date=match_date
                )
                
                if storage_result['success']:
                    results['stored_batches'].append(storage_result)
                    results['total_processed'] += storage_result['record_count']
                else:
                    error_msg = (
                        f"Storage failed for batch {batch_number}: "
                        f"{storage_result.get('error', 'Unknown error')}"
                    )
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
        except Exception as e:
            error_msg = f"Error storing batch {batch_number}: {type(e).__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results['errors'].append(error_msg)


async def main():
    """
    Extract league data from configuration file
    Uses config/league_config.yaml for all parameters
    """
    from etl.utils.config_loader import get_active_config
    
    try:
        # Load configuration
        config = get_active_config()
        
        logger.info("=== Starting Sofascore ETL Process ===")
        logger.info(f"League: {config['league_name']} ({config['country']})")
        logger.info(f"League ID: {config['league_id']}")
        logger.info(f"Season: {config['season_name']} (ID: {config['season_id']})")
        logger.info(f"Max pages: {config['max_pages']}")
        
        async with SofascoreETL() as etl:
            # Extract tournament matches
            logger.info("Extracting tournament matches...")
            matches_result = await etl.extract_tournament_matches(
                tournament_id=config['league_id'],
                season_id=config['season_id'],
                max_pages=config['max_pages'],
                replace_partition=True
            )
            
            logger.info("Match extraction completed:")
            logger.info(f"  • Total matches: {matches_result['total_matches']}")
            logger.info(f"  • Stored batches: {len(matches_result['stored_batches'])}")
            logger.info(f"  • Errors: {len(matches_result['errors'])}")
            
            if matches_result['errors']:
                logger.error("Errors encountered:")
                for error in matches_result['errors'][:5]:  # Show first 5
                    logger.error(f"  • {error}")
                if len(matches_result['errors']) > 5:
                    logger.error(f"  ... and {len(matches_result['errors']) - 5} more errors")
            
            # List available partitions
            partitions = etl.storage.list_partitions("matches", config['league_id'])
            logger.info(f"Available partitions: {len(partitions)}")
        
        logger.info("=== ETL Process Completed Successfully ===")
        
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.error("Please create config/league_config.yaml before running the extractor")
        raise
    except Exception as e:
        logger.error(f"ETL process failed: {type(e).__name__}: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())