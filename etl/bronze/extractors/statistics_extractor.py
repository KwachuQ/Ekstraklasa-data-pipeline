#!/usr/bin/env python3
"""
Match Statistics Fetcher - Synchronous API client for Airflow DAGs
Fetches match statistics from Sofascore API and saves to MinIO bronze layer
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict, List, Any, Optional, Tuple

import requests
from minio import Minio
from minio.error import S3Error
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

logger = logging.getLogger(__name__)


class StatisticsFetchError(Exception):
    """Base exception for statistics fetching errors"""
    pass


class StatisticsFetcher:
    """
    Synchronous statistics fetcher for Airflow DAGs
    
    Features:
    - Retry logic with exponential backoff
    - Rate limiting
    - Duplicate detection
    - Structured metadata tracking
    - Proper error handling
    """
    
    # API Configuration
    API_BASE_URL = "https://sofascore.p.rapidapi.com"
    API_ENDPOINT = "/matches/get-statistics"
    
    def __init__(
        self,
        rapidapi_key: str,
        tournament_id: int = 202,
        season_folder: str = "season_2025_26",
        rate_limit_delay: float = 1.5
    ):
        """
        Initialize statistics fetcher
        
        Args:
            rapidapi_key: RapidAPI key for authentication
            tournament_id: Tournament identifier (default: 202 = Ekstraklasa)
            season_folder: Season folder name for MinIO paths
            rate_limit_delay: Delay between requests in seconds
        """
        if not rapidapi_key:
            raise ValueError("rapidapi_key is required")
        
        self.rapidapi_key = rapidapi_key
        self.tournament_id = tournament_id
        self.season_folder = season_folder
        self.rate_limit_delay = rate_limit_delay
        
        # API configuration
        self.api_url = f"{self.API_BASE_URL}{self.API_ENDPOINT}"
        self.api_headers = {
            "x-rapidapi-key": self.rapidapi_key,
            "x-rapidapi-host": "sofascore.p.rapidapi.com"
        }
        
        # MinIO configuration
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        self.bucket_name = "bronze"
        
        logger.info(
            f"StatisticsFetcher initialized: tournament_id={tournament_id}, "
            f"season={season_folder}"
        )
    
    def _get_object_name(self, match_id: int, match_date: Optional[str] = None) -> str:
        """
        Generate MinIO object path
        
        Args:
            match_id: Match identifier
            match_date: Match date (YYYY-MM-DD), uses today if None
            
        Returns:
            Full object path in MinIO
        """
        if match_date is None:
            match_date = datetime.now().strftime('%Y-%m-%d')
        
        return (
            f"match_statistics/{self.season_folder}/"
            f"date={match_date}/match_{match_id}.json"
        )
    
    def _extract_match_date(self, statistics_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract match date from statistics data
        
        Args:
            statistics_data: Raw statistics response
            
        Returns:
            Match date in YYYY-MM-DD format or None
        """
        try:
            # Try to extract from statistics data structure
            # This depends on the actual API response format
            # Fallback to today's date if not available
            return datetime.now().strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Could not extract match date: {e}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def statistics_exist(self, match_id: int, match_date: Optional[str] = None) -> bool:
        """
        Check if statistics already exist in MinIO
        
        Args:
            match_id: Match identifier
            match_date: Match date (optional)
            
        Returns:
            True if statistics exist, False otherwise
        """
        object_name = self._get_object_name(match_id, match_date)
        
        try:
            self.minio_client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            # Re-raise other MinIO errors
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,
            requests.exceptions.Timeout
        ))
    )

    def fetch_match_statistics(self, match_id: int) -> Dict[str, Any]:
        """
        Fetch statistics for a single match with retry logic
        
        Args:
            match_id: Match identifier
            
        Returns:
            Dict with keys:
                - match_id: int
                - status: str ('success', 'not_found', 'error', 'timeout')
                - data: Optional[Dict] (statistics data if successful)
                - error: Optional[str] (error message if failed)
        """
        params = {"matchId": str(match_id)}
        
        try:
            response = requests.get(
                self.api_url,
                headers=self.api_headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✓ Fetched statistics for match_id={match_id}")
                return {
                    'match_id': match_id,
                    'status': 'success',
                    'data': data
                }
            
            elif response.status_code == 404:
                logger.warning(f"⚠ No statistics available for match_id={match_id}")
                return {
                    'match_id': match_id,
                    'status': 'not_found',
                    'data': None
                }
            
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(
                    f"✗ Failed to fetch match_id={match_id}: {error_msg}",
                    extra={
                        "match_id": match_id,
                        "status_code": response.status_code,
                        "tournament_id": self.tournament_id
                    }
                )
                return {
                    'match_id': match_id,
                    'status': 'error',
                    'data': None,
                    'error': error_msg
                }
        
        except requests.exceptions.Timeout:
            logger.warning(f"⏱ Timeout fetching match_id={match_id}")
            return {
                'match_id': match_id,
                'status': 'timeout',
                'data': None,
                'error': 'Request timeout'
            }
        
        except requests.exceptions.RequestException as e:
            logger.error(
                f"✗ Request error for match_id={match_id}: {type(e).__name__}: {e}",
                exc_info=True
            )
            return {
                'match_id': match_id,
                'status': 'error',
                'data': None,
                'error': str(e)
            }
    
    def save_to_minio(
        self,
        match_id: int,
        data: Dict[str, Any],
        match_date: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Save statistics to MinIO with metadata
        
        Args:
            match_id: Match identifier
            data: Statistics data
            match_date: Match date (optional, uses today if None)
            
        Returns:
            Tuple of (success: bool, object_path: str)
        """
        if match_date is None:
            match_date = self._extract_match_date(data)
        
        object_name = self._get_object_name(match_id, match_date)
        
        try:
            # Enrich data with metadata
            enriched_data = {
                "match_id": match_id,
                "match_date": match_date,
                "statistics": data,
                "metadata": {
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "sofascore_api",
                    "endpoint": self.API_ENDPOINT,
                    "tournament_id": self.tournament_id,
                    "season": self.season_folder,
                    "api_version": "v1"
                }
            }
            
            # Convert to JSON bytes
            json_data = json.dumps(enriched_data, ensure_ascii=False, indent=2)
            json_bytes = json_data.encode('utf-8')
            
            # Upload to MinIO
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=BytesIO(json_bytes),
                length=len(json_bytes),
                content_type='application/json',
                metadata={
                    "match-id": str(match_id),
                    "tournament-id": str(self.tournament_id),
                    "ingestion-date": datetime.now().strftime('%Y-%m-%d')
                }
            )
            
            logger.info(f"💾 Saved: {object_name}")
            return True, object_name
        
        except S3Error as e:
            logger.error(
                f"✗ MinIO error for match_id={match_id}: {e}",
                extra={
                    "match_id": match_id,
                    "object_name": object_name,
                    "error_code": e.code
                }
            )
            return False, ""
        
        except Exception as e:
            logger.error(
                f"✗ Save error for match_id={match_id}: {type(e).__name__}: {e}",
                exc_info=True
            )
            return False, ""
    
    def fetch_and_save_statistics(
        self,
        match_ids: List[int],
        delay_between_requests: Optional[float] = None,
        skip_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch and save statistics for multiple matches
        
        Args:
            match_ids: List of match IDs to fetch
            delay_between_requests: Override default rate limit delay
            skip_existing: Skip matches that already have statistics
            
        Returns:
            Dict with results:
                - successful: int (count of successfully saved)
                - failed: int (count of failures)
                - skipped: int (count of skipped matches)
                - total: int (total matches processed)
                - details: List[Dict] (per-match details)
        """
        if not match_ids:
            logger.warning("No match_ids provided")
            return {
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'total': 0,
                'details': []
            }
        
        delay = delay_between_requests or self.rate_limit_delay
        
        logger.info(
            f"Starting batch fetch: {len(match_ids)} matches, "
            f"delay={delay}s, skip_existing={skip_existing}"
        )
        
        results = {
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total': len(match_ids),
            'details': []
        }
        
        for idx, match_id in enumerate(match_ids, 1):
            logger.info(f"[{idx}/{len(match_ids)}] Processing match_id={match_id}")
            
            try:
                # Check if already exists
                if skip_existing and self.statistics_exist(match_id):
                    logger.info(f"Statistics already exist for match {match_id}, skipping")
                    results['skipped'] += 1
                    results['details'].append({
                        'match_id': match_id,
                        'status': 'skipped',
                        'reason': 'already_exists'
                    })
                    continue
                
                # Fetch statistics
                fetch_result = self.fetch_match_statistics(match_id)
                status = fetch_result['status']
                
                if status == 'success' and fetch_result['data']:
                    # Save to MinIO
                    success, object_path = self.save_to_minio(
                        match_id,
                        fetch_result['data']
                    )
                    
                    if success:
                        results['successful'] += 1
                        results['details'].append({
                            'match_id': match_id,
                            'status': 'success',
                            'path': object_path
                        })
                    else:
                        results['failed'] += 1
                        results['details'].append({
                            'match_id': match_id,
                            'status': 'save_failed'
                        })
                
                elif status == 'not_found':
                    results['skipped'] += 1
                    results['details'].append({
                        'match_id': match_id,
                        'status': 'not_found'
                    })
                
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'match_id': match_id,
                        'status': 'fetch_failed',
                        'error': fetch_result.get('error', 'Unknown error')
                    })
                
                # Rate limiting (except for last item)
                if idx < len(match_ids):
                    time.sleep(delay)
            
            except Exception as e:
                logger.error(
                    f"Unexpected error processing match_id={match_id}: {e}",
                    exc_info=True
                )
                results['failed'] += 1
                results['details'].append({
                    'match_id': match_id,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Summary
        logger.info(
            f"Batch complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['skipped']} skipped"
        )
        
        return results


# ============================================================================
# Standalone Execution
# ============================================================================

def main():
    """Example usage - uses configuration from YAML"""
    import sys
    from etl.utils.config_loader import get_active_config
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get API key from environment
    api_key = os.getenv('RAPIDAPI_KEY')
    if not api_key:
        logger.error("❌ RAPIDAPI_KEY environment variable not set!")
        sys.exit(1)
    
    try:
        # Load configuration
        config = get_active_config()
        
        logger.info("=== Statistics Fetcher Test ===")
        logger.info(f"League: {config['league_name']}")
        logger.info(f"Season: {config['season_name']}")
        
        # Test match IDs (replace with actual match IDs)
        test_match_ids = [12648635]
        
        fetcher = StatisticsFetcher(
            rapidapi_key=api_key,
            tournament_id=config['league_id']
        )
        
        result = fetcher.fetch_and_save_statistics(
            match_ids=test_match_ids,
            skip_existing=False  # Force fetch for testing
        )
        
        logger.info(f"Test result: {result}")
        
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.error("Please create config/league_config.yaml before running")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()