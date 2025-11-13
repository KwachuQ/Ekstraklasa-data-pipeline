"""
SofaScore API Client
Handles RapidAPI integration with retry logic, rate limiting, and validation
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

# ============================================================================
# Configuration & Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Exceptions
# ============================================================================

class SofascoreAPIError(Exception):
    """Base exception for API errors"""
    pass

class RateLimitError(SofascoreAPIError):
    """Rate limit exceeded"""
    pass

class ValidationError(SofascoreAPIError):
    """Response validation failed"""
    pass

# ============================================================================
# Pydantic Schemas
# ============================================================================

class MatchBasic(BaseModel):
    """Match validation schema"""
    id: int
    status: Dict[str, Any]
    startTimestamp: Optional[int] = None
    homeTeam: Dict[str, Any]
    awayTeam: Dict[str, Any]
    tournament: Dict[str, Any]
    
    class Config:
        extra = "allow"

class TournamentBasic(BaseModel):
    """Tournament validation schema"""
    id: int
    name: str
    slug: str
    category: Dict[str, Any]
    
    class Config:
        extra = "allow"

class SeasonBasic(BaseModel):
    """Season validation schema"""
    id: int
    name: str
    year: str
    
    class Config:
        extra = "allow"

# ============================================================================
# Response Wrapper
# ============================================================================

class APIResponse:
    """Standardized API response wrapper"""
    
    def __init__(
        self,
        raw_data: Dict[str, Any],
        metadata: Dict[str, Any],
        validated_items: Optional[List[Dict]] = None,
        validation_errors: Optional[List[str]] = None
    ):
        self.raw_data = raw_data
        self.metadata = metadata
        self.validated_items = validated_items or []
        self.validation_errors = validation_errors or []
    
    @property
    def is_valid(self) -> bool:
        """Check if response has valid data"""
        return len(self.validated_items) > 0
    
    @property
    def total_count(self) -> int:
        """Count of validated items"""
        return len(self.validated_items)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format (backward compatible)"""
        return {
            "data": {
                "items": self.validated_items,
                "total_count": self.total_count,
                "validation_status": "success" if self.is_valid else "failed",
                "validation_errors": self.validation_errors,
                "skipped_count": len(self.validation_errors)
            },
            "metadata": self.metadata,
            "raw_response": self.raw_data
        }

# ============================================================================
# Main Client
# ============================================================================

class SofascoreClient:
    """
    SofaScore API client with:
    - Automatic retry on transient failures
    - Rate limiting compliance
    - Response validation with Pydantic
    - Batch operations support
    """
    
    BASE_URL = "https://sofascore.p.rapidapi.com"
    DEFAULT_TIMEOUT = 30
    DEFAULT_RATE_LIMIT = 1.0  # seconds between requests
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        rate_limit_delay: float = DEFAULT_RATE_LIMIT
    ):
        """
        Initialize client
        
        Args:
            api_key: RapidAPI key (or set RAPIDAPI_KEY env var)
            timeout: Request timeout in seconds
            rate_limit_delay: Minimum delay between requests
        """
        self.api_key = api_key or os.getenv('RAPIDAPI_KEY')
        if not self.api_key:
            raise ValueError(
                "RAPIDAPI_KEY required. Set via parameter or environment variable."
            )
        
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0
        
        # Initialize HTTP client
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={
                "x-rapidapi-key": self.api_key,
                "x-rapidapi-host": "sofascore.p.rapidapi.com",
                "Accept": "application/json"
            },
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
    
    # ------------------------------------------------------------------------
    # Core Request Handling
    # ------------------------------------------------------------------------
    
    async def _enforce_rate_limit(self):
        """Wait if necessary to respect rate limit"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, RateLimitError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            RateLimitError: When rate limited
            SofascoreAPIError: For other errors
        """
        await self._enforce_rate_limit()
        
        url = urljoin(self.BASE_URL, endpoint)
        logger.debug(f"Request: {endpoint} {params or {}}")
        
        try:
            response = await self.client.get(url, params=params or {})
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Retry after {retry_after}s")
                await asyncio.sleep(retry_after)
                raise RateLimitError(f"Rate limited, retry after {retry_after}s")
            
            # Handle errors
            if response.status_code >= 400:
                error_msg = response.text[:200]
                logger.error(f"HTTP {response.status_code}: {error_msg}")
                raise SofascoreAPIError(
                    f"HTTP {response.status_code} for {endpoint}: {error_msg}"
                )
            
            # Log rate limit info if available
            if 'x-ratelimit-requests-remaining' in response.headers:
                remaining = int(response.headers['x-ratelimit-requests-remaining'])
                limit = int(response.headers.get('x-ratelimit-requests-limit', 'unknown'))
                logger.debug(f"Rate limit: {remaining}/{limit} requests remaining")
    
    # Warn if getting low
                if remaining < 50:
                    logger.warning(f"⚠️ Rate limit low: {remaining}/{limit} requests remaining")
                elif remaining < 100:
                    logger.info(f"Rate limit usage: {remaining}/{limit} requests remaining")

            return response.json()
            
        except httpx.TimeoutException as e:
            logger.error(f"Timeout for {endpoint}")
            raise SofascoreAPIError(f"Request timeout: {e}")
        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {e}")
            raise SofascoreAPIError(f"HTTP error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {endpoint}")
            raise SofascoreAPIError(f"Invalid JSON response: {e}")
    
    # ------------------------------------------------------------------------
    # Validation & Metadata
    # ------------------------------------------------------------------------
    
    def _validate(
        self,
        data: Union[Dict, List[Dict]],
        schema: BaseModel
    ) -> tuple[List[Dict], List[str]]:
        """
        Validate data against Pydantic schema
        
        Args:
            data: Data to validate (dict or list of dicts)
            schema: Pydantic model class
            
        Returns:
            Tuple of (validated_items, error_messages)
        """
        validated = []
        errors = []
        
        items = [data] if isinstance(data, dict) else data
        
        for idx, item in enumerate(items):
            try:
                validated.append(schema(**item).dict())
            except PydanticValidationError as e:
                error_msg = f"Item {idx}: {str(e)[:100]}"
                errors.append(error_msg)
                logger.debug(f"Validation error: {error_msg}")
        
        if errors:
            logger.warning(f"Validation: {len(errors)}/{len(items)} items failed")
        
        return validated, errors
    
    def _generate_batch_id(self, endpoint: str, params: Dict) -> str:
        """Generate deterministic batch ID from request params"""
        content = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _create_metadata(self, endpoint: str, params: Dict) -> Dict[str, Any]:
        """Create metadata for request tracking"""
        return {
            "batch_id": self._generate_batch_id(endpoint, params),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "sofascore_api",
            "endpoint": endpoint,
            "request_params": params,
            "client_version": "2.0.0"
        }
    
    async def _fetch_and_validate(
        self,
        endpoint: str,
        params: Dict[str, Any],
        data_key: str,
        schema: BaseModel
    ) -> APIResponse:
        """
        Fetch data and validate against schema
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            data_key: Key in response containing items to validate
            schema: Pydantic validation schema
            
        Returns:
            APIResponse object
        """
        raw_data = await self._request(endpoint, params)
        metadata = self._create_metadata(endpoint, params)
        
        items = self._extract_items(raw_data, data_key)
        validated, errors = self._validate(items, schema)
        
        return APIResponse(
            raw_data=raw_data,
            metadata=metadata,
            validated_items=validated,
            validation_errors=errors
        )
    
    def _extract_items(self, data: Dict, key_path: str) -> List[Dict]:
        """
        Extract items from nested structure using path notation
        
        Examples:
            "events" -> data.get("events", [])
            "groups[*].uniqueTournaments" -> flatten all uniqueTournaments from groups array
            "statistics.home" -> data.get("statistics", {}).get("home", [])
        """
        # Handle array flattening: "groups[*].uniqueTournaments"
        if "[*]." in key_path:
            array_key, item_key = key_path.split("[*].", 1)
            items = []
            for element in data.get(array_key, []):
                extracted = element.get(item_key, [])
                if isinstance(extracted, list):
                    items.extend(extracted)
                else:
                    items.append(extracted)
            return items
        
        # Handle simple nested paths: "statistics.home"
        if "." in key_path:
            keys = key_path.split(".")
            current = data
            for key in keys:
                current = current.get(key, {} if key != keys[-1] else [])
            return current if isinstance(current, list) else [current]
        
        # Simple key lookup: "events"
        return data.get(key_path, [])

    # ------------------------------------------------------------------------
    # Tournament & Season Endpoints
    # ------------------------------------------------------------------------
    
    async def get_tournaments_list(self, category_id: int = 47) -> APIResponse:
        """
        Get tournaments for category
        
        Args:
            category_id: Category ID (default: 47 = Poland)
            
        Returns:
            APIResponse with tournament data
        """
        return await self._fetch_and_validate(
        endpoint="/tournaments/list",
        params={"categoryId": str(category_id)},
        data_key="groups[*].uniqueTournaments",
        schema=TournamentBasic
    )
    
    async def get_tournament_seasons(self, tournament_id: int) -> APIResponse:
        """Get available seasons for tournament"""
        return await self._fetch_and_validate(
            endpoint="/tournaments/get-seasons",
            params={"tournamentId": str(tournament_id)},
            data_key="seasons",
            schema=SeasonBasic
        )
    
    # ------------------------------------------------------------------------
    # Match Endpoints
    # ------------------------------------------------------------------------
    
    async def get_tournament_matches(
        self,
        tournament_id: int,
        season_id: int,
        page: int = 0
    ) -> APIResponse:
        """
        Get matches for tournament season
        
        Args:
            tournament_id: Tournament ID
            season_id: Season ID
            page: Page index (0-based)
            
        Returns:
            APIResponse with match data
        """
        return await self._fetch_and_validate(
            endpoint="/tournaments/get-last-matches",
            params={
                "tournamentId": str(tournament_id),
                "seasonId": str(season_id),
                "pageIndex": str(page)
            },
            data_key="events",
            schema=MatchBasic
        )
    
    async def get_match_statistics(self, match_id: int) -> APIResponse:
        """
        Get match statistics
        
        Args:
            match_id: Match ID
            
        Returns:
            APIResponse with statistics (raw data, no validation)
        """
        raw_data = await self._request(
            endpoint="/matches/get-statistics",
            params={"matchId": str(match_id)}
        )
        
        metadata = self._create_metadata(
            "/matches/get-statistics",
            {"matchId": str(match_id)}
        )
        
        # Statistics structure is complex, return raw without validation
        return APIResponse(
            raw_data=raw_data,
            metadata=metadata,
            validated_items=[{"match_id": match_id, "statistics": raw_data}]
        )
    
    async def get_match_lineups(self, match_id: int) -> APIResponse:
        """Get match lineups"""
        raw_data = await self._request(
            endpoint="/matches/get-lineups",
            params={"matchId": str(match_id)}
        )
        
        metadata = self._create_metadata(
            "/matches/get-lineups",
            {"matchId": str(match_id)}
        )
        
        return APIResponse(
            raw_data=raw_data,
            metadata=metadata,
            validated_items=[{"match_id": match_id, "lineups": raw_data}]
        )
    
    async def get_match_details(self, match_id: int) -> APIResponse:
        """Get comprehensive match details"""
        raw_data = await self._request(
            endpoint="/matches/get-details",
            params={"matchId": str(match_id)}
        )
        
        metadata = self._create_metadata(
            "/matches/get-details",
            {"matchId": str(match_id)}
        )
        
        # Validate event if present
        if 'event' in raw_data:
            validated, errors = self._validate(raw_data['event'], MatchBasic)
            return APIResponse(
                raw_data=raw_data,
                metadata=metadata,
                validated_items=validated,
                validation_errors=errors
            )
        
        return APIResponse(
            raw_data=raw_data,
            metadata=metadata,
            validated_items=[raw_data]
        )
    
    async def get_featured_odds(self, match_id: int) -> APIResponse:
        """Get featured betting odds"""
        raw_data = await self._request(
            endpoint="/matches/get-featured-odds",
            params={"matchId": str(match_id)}
        )
        
        metadata = self._create_metadata(
            "/matches/get-featured-odds",
            {"matchId": str(match_id)}
        )
        
        return APIResponse(
            raw_data=raw_data,
            metadata=metadata,
            validated_items=[{"match_id": match_id, "odds": raw_data}]
        )
    
    # ------------------------------------------------------------------------
    # Batch Operations
    # ------------------------------------------------------------------------
    
    async def fetch_multiple_statistics(
        self,
        match_ids: List[int],
        delay: float = 1.5
    ) -> Dict[str, Any]:
        """
        Fetch statistics for multiple matches
        
        Args:
            match_ids: List of match IDs
            delay: Delay between requests (seconds)
            
        Returns:
            Dict with successful, failed, and summary info
        """
        results = {"successful": [], "failed": [], "total": len(match_ids)}
        
        for idx, match_id in enumerate(match_ids, 1):
            try:
                logger.info(f"Fetching stats {idx}/{len(match_ids)}: match_id={match_id}")
                
                response = await self.get_match_statistics(match_id)
                results["successful"].append({
                    "match_id": match_id,
                    "response": response.to_dict()
                })
                
                # Delay before next request (except last)
                if idx < len(match_ids):
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Failed to fetch stats for match {match_id}: {e}")
                results["failed"].append({
                    "match_id": match_id,
                    "error": str(e)
                })
        
        logger.info(
            f"Batch complete: {len(results['successful'])}/{len(match_ids)} successful"
        )
        
        return results
    
    # ------------------------------------------------------------------------
    # Health Check
    # ------------------------------------------------------------------------
    
    async def health_check(self) -> bool:
        """
        Verify API connectivity
        
        Returns:
            True if API is accessible and responding
        """
        try:
            response = await self.get_tournaments_list(category_id=47)
            is_healthy = response.is_valid and response.total_count > 0
            
            if is_healthy:
                logger.info("✅ API health check passed")
            else:
                logger.warning("⚠️ API health check: no data returned")
            
            return is_healthy
            
        except Exception as e:
            logger.error(f"❌ API health check failed: {e}")
            return False

# ============================================================================
# Utility Functions
# ============================================================================

def get_partition_key(
    tournament_id: int,
    season_id: int,
    match_date: str,
    match_id: int
) -> str:
    """
    Generate storage partition key
    
    Args:
        tournament_id: Tournament ID
        season_id: Season ID
        match_date: Match date (YYYY-MM-DD)
        match_id: Match ID
        
    Returns:
        Partition key string
    """
    return (
        f"tournament_id={tournament_id}/"
        f"season_id={season_id}/"
        f"date={match_date}/"
        f"match_id={match_id}"
    )

def get_bronze_path(data_type: str, partition_key: str, batch_id: str) -> str:
    """
    Generate bronze layer storage path
    
    Args:
        data_type: Data type (e.g., 'matches', 'statistics')
        partition_key: Partition key from get_partition_key()
        batch_id: Unique batch identifier
        
    Returns:
        Full storage path
    """
    return f"bronze/{data_type}/{partition_key}/batch_{batch_id}.ndjson"