#!/usr/bin/env python3
"""
Sofascore API Client with defensive validation and retry/backoff
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel, ValidationError as PydanticValidationError, Field
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SofascoreAPIError(Exception):
    """Custom exception for Sofascore API errors"""
    pass

class RateLimitError(SofascoreAPIError):
    """Raised when rate limit is exceeded"""
    pass

class SofascoreValidationError(SofascoreAPIError):
    """Raised when response validation fails"""
    pass

# Response validation schemas using Pydantic
class MatchBasic(BaseModel):
    """Minimal match data validation"""
    id: int
    status: Dict[str, Any]
    startTimestamp: Optional[int] = None
    homeTeam: Dict[str, Any]
    awayTeam: Dict[str, Any]
    tournament: Dict[str, Any]
    
    class Config:
        extra = "allow"  # Allow additional fields

class TournamentBasic(BaseModel):
    """Minimal tournament data validation"""
    id: int
    name: str
    slug: str
    category: Dict[str, Any]
    
    class Config:
        extra = "allow"

class SeasonBasic(BaseModel):
    """Minimal season data validation"""
    id: int
    name: str
    year: str
    
    class Config:
        extra = "allow"

class SofascoreClient:
    """
    Robust Sofascore API client with:
    - Retry/backoff strategy
    - Rate limiting compliance
    - Defensive response validation
    - Deterministic batch metadata
    """
    
    BASE_URL = "https://sofascore.p.rapidapi.com"
    
    def __init__(
        self,
        api_key: str = None,
        max_retries: int = 3,
        timeout: int = 30,
        rate_limit_delay: float = 1.0,
        user_agent: str = "DataPipeline/1.0"
    ):
        self.api_key = api_key or os.getenv('RAPIDAPI_KEY')
        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY must be provided via parameter or environment variable")
        self.max_retries = max_retries
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.user_agent = user_agent
        
        # HTTP client configuration for RapidAPI
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "x-rapidapi-key": self.api_key,
                "x-rapidapi-host": "sofascore.p.rapidapi.com"
            },
            limits=httpx.Limits(
                max_keepalive_connections=5,
                max_connections=10
            )
        )
        
        # Rate limiting
        self._last_request_time = 0
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    def _generate_batch_id(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate deterministic batch ID based on request parameters"""
        content = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _get_ingestion_metadata(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Generate batch metadata for tracking"""
        return {
            "batch_id": self._generate_batch_id(endpoint, params),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "sofascore_api",
            "endpoint": endpoint,
            "request_params": params,
            "api_version": "v1"
        }
    
    async def _respect_rate_limit(self):
        """Ensure minimum delay between requests"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            delay = self.rate_limit_delay - time_since_last
            logger.debug(f"Rate limiting: waiting {delay:.2f}s")
            await asyncio.sleep(delay)
        
        self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, SofascoreAPIError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request with retry logic"""
        await self._respect_rate_limit()
        
        url = urljoin(self.BASE_URL, endpoint)
        
        try:
            logger.info(f"Making request to: {url} with params: {params}")
            response = await self.client.get(url, params=params or {})
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                raise RateLimitError(f"Rate limited, retry after {retry_after}s")
            
            # Handle other HTTP errors
            if response.status_code >= 400:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                raise SofascoreAPIError(f"HTTP {response.status_code}: {response.text}")
            
            return response.json()
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise SofascoreAPIError(f"HTTP error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            raise SofascoreAPIError(f"Invalid JSON response: {e}")
    
    def _validate_response(self, data: Dict[str, Any], schema: BaseModel, context: str) -> Dict[str, Any]:
        """Validate response against schema with fallback"""
        try:
            if isinstance(data, list):
                # Validate list of items
                validated_items = []
                for item in data:
                    try:
                        validated_items.append(schema(**item).dict())
                    except PydanticValidationError as e:
                        logger.warning(f"Skipping invalid item in {context}: {e}")
                        continue
                return {"items": validated_items, "total_count": len(validated_items)}
            else:
                # Validate single item
                return schema(**data).dict()
                
        except PydanticValidationError as e:
            logger.warning(f"Skipping invalid item in {context}: {e}")
            # Return raw data with warning flag for defensive handling
            return {
                "raw_data": data,
                "validation_error": str(e),
                "validation_status": "failed"
            }
    
    async def get_tournament_matches(
        self, 
        tournament_id: int, 
        season_id: int,
        page: int = 0
    ) -> Dict[str, Any]:
        """Get matches for tournament/season with validation using RapidAPI"""
        # Use RapidAPI endpoint for last matches
        endpoint = "/tournaments/get-last-matches"
        params = {
            "tournamentId": str(tournament_id),
            "seasonId": str(season_id), 
            "pageIndex": str(page)
        }
        
        # Get data and metadata
        raw_data = await self._make_request(endpoint, params)
        metadata = self._get_ingestion_metadata(endpoint, params)
        
        # Validate response
        events = raw_data.get("events", [])
        validated_data = self._validate_response(events, MatchBasic, f"tournament_{tournament_id}_matches")
        
        return {
            "data": validated_data,
            "metadata": metadata,
            "raw_response": raw_data  # Keep for debugging
        }
    
    async def get_featured_odds(self, match_id: int) -> Dict[str, Any]:
        """Get featured odds for a match using RapidAPI"""
        endpoint = "/matches/get-featured-odds"
        params = {"matchId": str(match_id)}
        
        raw_data = await self._make_request(endpoint, params)
        metadata = self._get_ingestion_metadata(endpoint, params)
        
        # For odds data, we'll return raw structure
        return {
            "data": raw_data,
            "metadata": metadata,
            "raw_response": raw_data
        }
    
    async def get_tournament_seasons(self, tournament_id: int) -> Dict[str, Any]:
        """Get available seasons for tournament using RapidAPI"""
        endpoint = "/tournaments/get-seasons"
        params = {"tournamentId": str(tournament_id)}
        
        raw_data = await self._make_request(endpoint, params)
        metadata = self._get_ingestion_metadata(endpoint, params)
        
        seasons = raw_data.get("seasons", [])
        validated_data = self._validate_response(seasons, SeasonBasic, f"tournament_{tournament_id}_seasons")
        
        return {
            "data": validated_data,
            "metadata": metadata,
            "raw_response": raw_data
        }
    
    async def get_tournaments_list(self, category_id: int = 47) -> Dict[str, Any]:
        """Get tournaments list for category (Poland=47) using RapidAPI"""
        endpoint = "/tournaments/list"
        params = {"categoryId": str(category_id)}
        
        raw_data = await self._make_request(endpoint, params)
        metadata = self._get_ingestion_metadata(endpoint, params)
        
        tournaments = raw_data.get("tournaments", [])
        validated_data = self._validate_response(tournaments, TournamentBasic, f"category_{category_id}_tournaments")
        
        return {
            "data": validated_data,
            "metadata": metadata,
            "raw_response": raw_data
        }

# Utility functions
def get_partition_key(tournament_id: int, season_id: int, match_date: str, match_id: int) -> str:
    """Generate deterministic partition key for MinIO storage"""
    return f"tournament_id={tournament_id}/season_id={season_id}/date={match_date}/match_id={match_id}"

def get_bronze_path(data_type: str, partition_key: str, batch_id: str) -> str:
    """Generate bronze layer path with partition structure"""
    return f"bronze/{data_type}/{partition_key}/batch_{batch_id}.ndjson"

async def main():
    """Example usage with RapidAPI endpoints"""
    async with SofascoreClient() as client:
        # Example: Get Ekstraklasa 2025-2026 matches using RapidAPI
        ekstraklasa_id = 202  # Ekstraklasa tournament ID
        season_2025_id = 76477  # 2025/26 season ID
        
        try:
            # First, get available tournaments for Poland
            tournaments = await client.get_tournaments_list(category_id=47)
            print(f"Available tournaments: {len(tournaments['data'].get('items', []))}")
            
            # Get tournament seasons
            seasons = await client.get_tournament_seasons(ekstraklasa_id)
            print(f"Available seasons: {len(seasons['data'].get('items', []))}")
            
            # Get tournament matches
            result = await client.get_tournament_matches(ekstraklasa_id, season_2025_id)
            
            print(f"Batch ID: {result['metadata']['batch_id']}")
            print(f"Retrieved {len(result['data'].get('items', []))} matches")
            
            # Example partition key generation
            if result['data'].get('items'):
                first_match = result['data']['items'][0]
                match_date = datetime.fromtimestamp(first_match.get('startTimestamp', 0)).strftime('%Y-%m-%d')
                partition_key = get_partition_key(
                    ekstraklasa_id, 
                    season_2025_id, 
                    match_date, 
                    first_match['id']
                )
                bronze_path = get_bronze_path("matches", partition_key, result['metadata']['batch_id'])
                print(f"Bronze path: {bronze_path}")
                
        except SofascoreAPIError as e:
            logger.error(f"API error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
