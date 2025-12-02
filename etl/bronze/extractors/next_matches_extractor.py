#!/usr/bin/env python3
"""
Next Matches Fetcher - Get upcoming matches for a tournament
Fetches next matches from Sofascore API and saves to MinIO bronze layer
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict, List, Any, Optional

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


class NextMatchesFetchError(Exception):
    """Base exception for next matches fetching errors"""
    pass


class NextMatchesFetcher:
    """
    Fetcher for upcoming tournament matches
    
    Features:
    - Retry logic with exponential backoff
    - Rate limiting
    - Duplicate detection
    - Structured metadata tracking
    """
    
    API_BASE_URL = "https://sofascore.p.rapidapi.com"
    API_ENDPOINT = "/tournaments/get-next-matches"
    
    def __init__(
        self,
        rapidapi_key: str,
        tournament_id: int,
        season_id: int,
        rate_limit_delay: float = 1.5
    ):
        """
        Initialize next matches fetcher
        
        Args:
            rapidapi_key: RapidAPI key for authentication
            tournament_id: Tournament identifier (e.g., 202 for Ekstraklasa)
            season_id: Season identifier (numeric ID from config)
            rate_limit_delay: Delay between requests in seconds
        """
        if not rapidapi_key:
            raise ValueError("rapidapi_key is required")
        if not tournament_id:
            raise ValueError("tournament_id is required")
        if not season_id:
            raise ValueError("season_id is required")
            
        self.rapidapi_key = rapidapi_key
        self.tournament_id = tournament_id
        self.season_id = season_id
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
        self.bucket_name = 'bronze'
        
        logger.info(
            f"NextMatchesFetcher initialized: tournament_id={tournament_id}, "
            f"season_id={season_id}"
        )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def fetch_next_matches(
        self,
        page_index: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch next matches for the tournament with retry logic
        
        Args:
            page_index: Page number for pagination
            
        Returns:
            Dict with keys:
                - tournament_id: int
                - season_id: int
                - page_index: int
                - status: str ('success', 'not_found', 'error')
                - data: Optional[Dict] (next matches data if successful)
                - error: Optional[str] (error message if failed)
        """
        params = {
            "tournamentId": str(self.tournament_id),
            "seasonId": str(self.season_id),
            "pageIndex": str(page_index)
        }
        
        try:
            logger.info(
                f"Fetching next matches: tournament_id={self.tournament_id}, "
                f"season_id={self.season_id}, page={page_index}"
            )
            
            response = requests.get(
                self.api_url,
                headers=self.api_headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(
                    f"✓ Fetched next matches page {page_index} "
                    f"(tournament_id={self.tournament_id})"
                )
                return {
                    'tournament_id': self.tournament_id,
                    'season_id': self.season_id,
                    'page_index': page_index,
                    'status': 'success',
                    'data': data
                }
            elif response.status_code == 404:
                logger.warning(
                    f"⚠ No next matches available for page {page_index}"
                )
                return {
                    'tournament_id': self.tournament_id,
                    'season_id': self.season_id,
                    'page_index': page_index,
                    'status': 'not_found',
                    'data': None,
                    'error': 'No data available'
                }
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(f"✗ Error fetching next matches: {error_msg}")
                return {
                    'tournament_id': self.tournament_id,
                    'season_id': self.season_id,
                    'page_index': page_index,
                    'status': 'error',
                    'data': None,
                    'error': error_msg
                }
                
        except requests.Timeout:
            logger.error(f"⏱ Timeout fetching next matches page {page_index}")
            return {
                'tournament_id': self.tournament_id,
                'season_id': self.season_id,
                'page_index': page_index,
                'status': 'timeout',
                'data': None,
                'error': 'Request timeout'
            }
        except Exception as e:
            logger.error(f"✗ Unexpected error: {str(e)}")
            return {
                'tournament_id': self.tournament_id,
                'season_id': self.season_id,
                'page_index': page_index,
                'status': 'error',
                'data': None,
                'error': str(e)
            }
    
    def _generate_object_path(self, extraction_date: str) -> str:
        """
        Generate MinIO object path for next matches data
        
        Args:
            extraction_date: Date string in YYYY-MM-DD format
            
        Returns:
            Object path: next_matches/tournament_id={id}/date={date}/next_matches.json
        """
        return (
            f"next_matches/"
            f"tournament_id={self.tournament_id}/"
            f"date={extraction_date}/"
            f"next_matches.json"
        )
    
    def _check_if_exists(self, extraction_date: str) -> bool:
        """Check if next matches data already exists in MinIO for this date"""
        object_path = self._generate_object_path(extraction_date)
        
        try:
            self.minio_client.stat_object(self.bucket_name, object_path)
            return True
        except S3Error:
            return False
    
    def _save_to_minio(
        self,
        data: Dict[str, Any],
        extraction_date: str,
        page_index: int = 0
    ) -> str:
        """
        Save next matches data to MinIO
        
        Args:
            data: Next matches data from API
            extraction_date: Date string in YYYY-MM-DD format
            page_index: Page number that was fetched
            
        Returns:
            Object path where data was saved
        """
        object_path = self._generate_object_path(extraction_date)
        
        # Add metadata
        enriched_data = {
            "data": data,
            "metadata": {
                "tournament_id": self.tournament_id,
                "season_id": self.season_id,
                "extraction_date": extraction_date,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "page_index": page_index,
                "source": "sofascore_api",
                "api_endpoint": self.API_ENDPOINT
            }
        }
        
        # Convert to bytes
        json_bytes = json.dumps(enriched_data, indent=2).encode('utf-8')
        json_buffer = BytesIO(json_bytes)
        
        # Upload to MinIO
        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_path,
            data=json_buffer,
            length=len(json_bytes),
            content_type='application/json'
        )
        
        logger.info(f"💾 Saved to MinIO: {object_path}")
        return object_path
    
    def fetch_and_save_next_matches(
        self,
        max_pages: int = 1,
        skip_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch and save next matches for the tournament
        
        Args:
            max_pages: Maximum pages to fetch
            skip_existing: Skip if data already exists for today
            
        Returns:
            Dict with results summary
        """
        extraction_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        results = {
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'errors': [],
            'saved_files': [],
            'extraction_date': extraction_date
        }
        
        logger.info(
            f"Starting fetch for tournament_id={self.tournament_id}, "
            f"season_id={self.season_id}, max_pages={max_pages}"
        )
        
        # Check if already exists for today
        if skip_existing and self._check_if_exists(extraction_date):
            logger.info(
                f"⏭ Skipping - data already exists for {extraction_date}"
            )
            results['skipped'] += 1
            return results
        
        # Fetch all pages (usually just 1 for next matches)
        all_matches = []
        
        for page_index in range(max_pages):
            result = self.fetch_next_matches(page_index)
            
            if result['status'] == 'success' and result['data']:
                all_matches.append(result['data'])
                
                # Usually only one page needed, break if no more data
                events = result['data'].get('events', [])
                if not events:
                    logger.info(f"No more matches on page {page_index}")
                    break
                    
            elif result['status'] == 'not_found':
                logger.info(f"No more data at page {page_index}")
                break
            else:
                results['failed'] += 1
                results['errors'].append({
                    'page_index': page_index,
                    'error': result.get('error', 'Unknown error')
                })
                break
            
            # Rate limiting between pages
            if page_index < max_pages - 1:
                time.sleep(self.rate_limit_delay)
        
        # Save combined results if any were successful
        if all_matches:
            try:
                # If multiple pages, combine them; otherwise use the single page
                combined_data = all_matches[0] if len(all_matches) == 1 else {
                    'events': [
                        event
                        for page_data in all_matches
                        for event in page_data.get('events', [])
                    ],
                    'hasNextPage': all_matches[-1].get('hasNextPage', False)
                }
                
                saved_path = self._save_to_minio(
                    combined_data,
                    extraction_date,
                    page_index=0
                )
                results['successful'] += 1
                results['saved_files'].append({
                    'path': saved_path,
                    'extraction_date': extraction_date,
                    'pages_fetched': len(all_matches)
                })
                
            except Exception as e:
                logger.error(f"Failed to save: {str(e)}")
                results['failed'] += 1
                results['errors'].append({
                    'error': f"Save failed: {str(e)}"
                })
        
        logger.info(
            f"Fetch complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['skipped']} skipped"
        )
        
        return results


def main():
    """Example usage"""
    import sys
    from etl.utils.config_loader import get_active_config
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    api_key = os.getenv('RAPIDAPI_KEY')
    if not api_key:
        logger.error("RAPIDAPI_KEY environment variable not set!")
        sys.exit(1)
    
    try:
        config = get_active_config()
        
        fetcher = NextMatchesFetcher(
            rapidapi_key=api_key,
            tournament_id=config['league_id'],
            season_id=config['season_id']
        )
        
        results = fetcher.fetch_and_save_next_matches(
            max_pages=2,
            skip_existing=False
        )
        
        print("\n" + "="*50)
        print("NEXT MATCHES FETCH RESULTS")
        print("="*50)
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()