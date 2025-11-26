#!/usr/bin/env python3
"""
Sofascore Incremental ETL - Bronze Layer
Fetches only NEW matches since last extraction date
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
class IncrementalExtractionResult:
    """Incremental extraction result"""
    tournament_id: int
    season_id: int
    last_match_date: str
    total_new_matches: int = 0
    stored_batches: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    pages_scanned: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for DAG compatibility"""
        return {
            'tournament_id': self.tournament_id,
            'season_id': self.season_id,
            'last_match_date': self.last_match_date,
            'total_new_matches': self.total_new_matches,
            'stored_batches': self.stored_batches,
            'errors': self.errors,
            'pages_scanned': self.pages_scanned
        }


class SofascoreIncrementalETL:
    """
    Incremental ETL for fetching only new matches since last extraction
    
    Key features:
    - Date-based filtering to avoid re-processing
    - Early termination when historical data reached
    - Incremental storage (no data replacement)
    - Memory-efficient streaming processing
    """
    
    def __init__(self):
        """Initialize incremental ETL with storage manager"""
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        self.storage = BronzeStorageManager(self.minio_client)
    
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
    
    def _validate_incremental_params(
        self,
        tournament_id: int,
        season_id: int,
        last_match_date: str,
        max_pages: int
    ) -> datetime.date:
        """Validate incremental extraction parameters"""
        # Validate IDs
        if tournament_id <= 0:
            raise ValueError(f"Invalid tournament_id: {tournament_id}")
        if season_id <= 0:
            raise ValueError(f"Invalid season_id: {season_id}")
        if max_pages <= 0:
            raise ValueError(f"Invalid max_pages: {max_pages}")
        
        # Validate and parse date
        try:
            cutoff_date = datetime.strptime(last_match_date, '%Y-%m-%d').date()
        except ValueError as e:
            raise ValueError(f"Invalid date format '{last_match_date}': Expected YYYY-MM-DD")
        
        # Check date is not in future
        if cutoff_date > datetime.now().date():
            raise ValueError(f"Cutoff date cannot be in the future: {cutoff_date}")
        
        return cutoff_date
    
    def _handle_error(
        self,
        error: Exception,
        context: str,
        results: IncrementalExtractionResult
    ) -> None:
        """Centralized error handling"""
        error_msg = f"{context}: {type(error).__name__}: {str(error)}"
        logger.error(error_msg, exc_info=logger.isEnabledFor(logging.DEBUG))
        results.errors.append(error_msg)
    
    def _filter_new_matches(
        self,
        matches: List[Dict[str, Any]],
        cutoff_date: datetime.date
    ) -> List[Dict[str, Any]]:
        """
        Filter matches to only include those after cutoff date
        
        Args:
            matches: List of match dictionaries
            cutoff_date: Only include matches after this date
            
        Returns:
            List of new matches
        """
        new_matches = []
        
        for match in matches:
            try:
                timestamp = match.get('startTimestamp', 0)
                if not timestamp:
                    logger.debug(f"Match {match.get('id')} has no timestamp, skipping")
                    continue
                
                match_date = datetime.fromtimestamp(timestamp).date()
                
                if match_date > cutoff_date:
                    new_matches.append(match)
                    logger.debug(
                        f"  ✓ New match: ID={match.get('id')} Date={match_date}"
                    )
                else:
                    logger.debug(
                        f"  ⊗ Old match: ID={match.get('id')} Date={match_date}"
                    )
                    
            except (ValueError, OSError) as e:
                logger.warning(
                    f"Invalid timestamp for match {match.get('id', 'unknown')}: {e}"
                )
                continue
        
        return new_matches
    
    def _should_continue_scan(
        self,
        matches: List[Dict[str, Any]],
        cutoff_date: datetime.date,
        threshold: float = 0.8
    ) -> bool:
        """
        Determine if we should continue scanning pages
        
        Stops when > threshold of matches are historical (before cutoff)
        
        Args:
            matches: Matches from current page
            cutoff_date: Date threshold
            threshold: Proportion of old matches to trigger stop (0-1)
            
        Returns:
            True if should continue, False if reached historical data
        """
        if not matches:
            return False
        
        old_match_count = 0
        for match in matches:
            try:
                timestamp = match.get('startTimestamp', 0)
                if timestamp:
                    match_date = datetime.fromtimestamp(timestamp).date()
                    if match_date <= cutoff_date:
                        old_match_count += 1
            except (ValueError, OSError):
                continue
        
        old_ratio = old_match_count / len(matches)
        
        if old_ratio > threshold:
            logger.info(
                f"Page contains {old_ratio:.1%} historical matches "
                f"(threshold: {threshold:.1%})"
            )
            return False
        
        return True
    
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
            except (ValueError, OSError) as e:
                logger.warning(
                    f"Invalid timestamp for match {match.get('id', 'unknown')}: {e}"
                )
                match_date = 'unknown'
            
            groups[match_date].append(match)
        
        return dict(groups)
    
    def _store_new_matches(
        self,
        new_matches: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        tournament_id: int,
        season_id: int,
        results: IncrementalExtractionResult
    ) -> None:
        """
        Store new matches grouped by date
        
        Args:
            new_matches: List of new matches to store
            metadata: Request metadata
            tournament_id: Tournament identifier
            season_id: Season identifier
            results: Results object to update
        """
        # Group by date for partitioning
        matches_by_date = self._group_matches_by_date(new_matches)
        
        for match_date, date_matches in matches_by_date.items():
            try:
                # Enhance metadata with incremental info
                enhanced_metadata = {
                    **metadata,
                    "extraction_type": "incremental",
                    "cutoff_date": results.last_match_date,
                    "incremental_batch_id": (
                        f"incremental_{tournament_id}_{season_id}_{match_date}"
                    )
                }
                
                storage_result = self.storage.store_batch(
                    data_type="matches",
                    records=date_matches,
                    metadata=enhanced_metadata,
                    tournament_id=tournament_id,
                    season_id=season_id,
                    match_date=match_date,
                    replace_partition=False  # APPEND mode for incremental
                )
                
                if storage_result.success:
                    results.stored_batches.append(storage_result.to_dict())
                    results.total_new_matches += storage_result.record_count
                    logger.info(
                        f"✓ Stored {storage_result.record_count} matches for {match_date}"
                    )
                else:
                    error_msg = storage_result.error or 'Unknown storage error'
                    self._handle_error(
                        Exception(error_msg),
                        f"Storage failed for {match_date}",
                        results
                    )
                    
            except Exception as e:
                self._handle_error(e, f"Error storing matches for {match_date}", results)
    
    async def extract_new_matches(
        self,
        tournament_id: int,
        season_id: int,
        last_match_date: str,
        max_pages: int = 25
    ) -> Dict[str, Any]:
        """
        Extract ONLY matches newer than last_match_date
        
        This method:
        1. Validates input parameters
        2. Scans API pages for new matches
        3. Filters matches by date
        4. Stores incrementally (no replacement)
        5. Stops early when historical data reached
        
        Args:
            tournament_id: Tournament ID (e.g., 202 for Ekstraklasa)
            season_id: Season ID (e.g., 76477 for 2025/26)
            last_match_date: Last extracted date in 'YYYY-MM-DD' format
            max_pages: Maximum pages to scan (default: 25)
        
        Returns:
            Dictionary with extraction results (for DAG compatibility)
        """
        # Validate inputs
        cutoff_date = self._validate_incremental_params(
            tournament_id, season_id, last_match_date, max_pages
        )
        
        # Initialize results
        results = IncrementalExtractionResult(
            tournament_id=tournament_id,
            season_id=season_id,
            last_match_date=last_match_date
        )
        
        logger.info(
            f"Starting incremental extraction: "
            f"tournament={tournament_id}, season={season_id}, "
            f"after={last_match_date}"
        )
        
        async with SofascoreClient() as client:
            for page in range(max_pages):
                results.pages_scanned += 1
                
                try:
                    logger.info(f"Scanning page {page + 1}/{max_pages}...")
                    
                    # Fetch matches from API
                    response = await client.get_tournament_matches(
                        tournament_id, season_id, page
                    )
                    
                    matches = response.validated_items
                    if not matches:
                        logger.info(f"No more matches at page {page + 1}")
                        break
                    
                    # Filter new matches
                    new_matches = self._filter_new_matches(matches, cutoff_date)
                    
                    if new_matches:
                        logger.info(
                            f"Found {len(new_matches)}/{len(matches)} new matches on page"
                        )
                        # Store immediately (memory efficient)
                        self._store_new_matches(
                            new_matches,
                            response.metadata,
                            tournament_id,
                            season_id,
                            results
                        )
                    else:
                        logger.info(f"No new matches on page {page + 1}")
                    
                    # Check if we should continue
                    if not self._should_continue_scan(matches, cutoff_date):
                        logger.info(f"Reached historical data at page {page + 1}")
                        break
                    
                except Exception as e:
                    self._handle_error(e, f"Error fetching page {page + 1}", results)
                    continue
        
        logger.info(
            f"Incremental extraction complete: {results.total_new_matches} new matches, "
            f"{results.pages_scanned} pages scanned, {len(results.errors)} errors"
        )
        
        return results.to_dict()


async def main():
    """
    Incremental extraction from configuration file
    Uses config/league_config.yaml for all parameters
    """
    from etl.utils.config_loader import get_active_config
    
    try:
        # Load configuration
        config = get_active_config()
        
        # Check if incremental mode is configured
        if not config['last_extraction_date']:
            logger.error("Incremental extraction requires last_extraction_date")
            logger.error("Please set etl.last_extraction_date in config/league_config.yaml")
            logger.error("Example: last_extraction_date: '2025-01-15'")
            return
        
        logger.info("=== Starting Incremental ETL Process ===")
        logger.info(f"League: {config['league_name']} ({config['country']})")
        logger.info(f"League ID: {config['league_id']}")
        logger.info(f"Season: {config['season_name']} (ID: {config['season_id']})")
        logger.info(f"Extracting matches after: {config['last_extraction_date']}")
        logger.info(f"Max pages to scan: {config['max_pages']}")
        
        async with SofascoreIncrementalETL() as etl:
            result = await etl.extract_new_matches(
                tournament_id=config['league_id'],
                season_id=config['season_id'],
                last_match_date=config['last_extraction_date'],
                max_pages=config['max_pages']
            )
            
            logger.info("Incremental extraction completed:")
            logger.info(f"  • New matches: {result['total_new_matches']}")
            logger.info(f"  • Pages scanned: {result['pages_scanned']}")
            logger.info(f"  • Stored batches: {len(result['stored_batches'])}")
            logger.info(f"  • Errors: {len(result['errors'])}")
            
            if result['errors']:
                logger.error("Errors encountered:")
                for error in result['errors'][:5]:  # Show first 5
                    logger.error(f"  • {error}")
                if len(result['errors']) > 5:
                    logger.error(f"  ... and {len(result['errors']) - 5} more errors")
        
        logger.info("=== Incremental ETL Process Completed ===")
        
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.error("Please create config/league_config.yaml before running the extractor")
        raise
    except Exception as e:
        logger.error(f"Incremental ETL failed: {type(e).__name__}: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())