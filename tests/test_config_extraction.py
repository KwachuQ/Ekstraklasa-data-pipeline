#!/usr/bin/env python3
"""
Test League Extraction Tool
Downloads sample data using existing extractors to bronze-test bucket
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, Any


# Add project root to path so etl.* imports work
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl.bronze.extractors.base_extractor import SofascoreETL
from etl.utils.config_loader import load_league_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_league_extraction(
    use_config: bool = True,
    tournament_id: int = None,
    season_id: int = None,
    max_pages: int = 2
) -> Dict[str, Any]:
    """
    Test extraction using existing extractors
    
    Args:
        use_config: If True, load from league_config.yaml
        tournament_id: Manual tournament ID (if use_config=False)
        season_id: Manual season ID (if use_config=False)
        max_pages: Number of pages to extract
    
    Returns:
        Extraction results dictionary
    """
    
    start_date = None
    end_date = None
    
    # Load configuration
    if use_config:
        logger.info("Loading configuration from league_config.yaml...")
        
        # Build absolute path to config file
        config_path = project_root / "config" / "league_config.yaml"
        config = load_league_config(str(config_path))
        
        tournament_id = config['active_league']['league_id']
        season_id = config['active_season']['season_id']
        league_name = config['active_league']['league_name']
        season_name = config['active_season']['name']
        max_pages = config['etl'].get('max_pages', max_pages)
        start_date = config['etl'].get('start_date')
        end_date = config['etl'].get('end_date')
        
        logger.info("="*70)
        logger.info(f"TEST EXTRACTION FOR: {league_name} - {season_name}")
        logger.info(f"Tournament ID: {tournament_id}")
        logger.info(f"Season ID: {season_id}")
        logger.info(f"Max Pages: {max_pages}")
        if start_date or end_date:
            logger.info(f"Date Range: [{start_date or 'ANY'} to {end_date or 'ANY'}]")
        logger.info("="*70)
    else:
        if not tournament_id or not season_id:
            raise ValueError("tournament_id and season_id required when use_config=False")
        
        logger.info("="*70)
        logger.info(f"TEST EXTRACTION")
        logger.info(f"Tournament ID: {tournament_id}")
        logger.info(f"Season ID: {season_id}")
        logger.info(f"Max Pages: {max_pages}")
        logger.info("="*70)
    
    # Run extraction
    logger.info("\n🚀 Starting extraction to bronze-test bucket...")
    
    async with SofascoreETL() as extractor:
        results = await extractor.extract_tournament_matches(
            tournament_id=tournament_id,
            season_id=season_id,
            max_pages=max_pages,
            replace_partition=False,
            start_date=start_date,
            end_date=end_date
        )
    
    # Display results
    logger.info("\n" + "="*70)
    logger.info("EXTRACTION RESULTS")
    logger.info("="*70)
    logger.info(f"✅ Total matches extracted: {results['total_matches']}")
    logger.info(f"📦 Batches stored: {len(results['stored_batches'])}")
    
    if results['stored_batches']:
        logger.info("\n📂 Stored files:")
        for batch in results['stored_batches']:
            logger.info(f"  - {batch['object_key']} ({batch['record_count']} matches)")
    
    if results['errors']:
        logger.warning(f"\n⚠️  Errors encountered: {len(results['errors'])}")
        for error in results['errors']:
            logger.warning(f"  - {error}")
    else:
        logger.info("\n✅ No errors!")
    
    logger.info("="*70)
    
    return results
async def get_match_ids_from_bucket(
    tournament_id: int,
    season_id: int,
    limit: int = 10
) -> list:
    """
    Get match IDs from bronze-test bucket
    
    Args:
        tournament_id: Tournament ID
        season_id: Season ID  
        limit: Maximum number of match IDs to return
    
    Returns:
        List of match IDs
    """
    import json
    from minio import Minio
    import os
    
    logger.info(f"\n📂 Reading match IDs from bronze-test bucket...")
    
    # Initialize MinIO client
    minio_client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    )
    
    match_ids = []
    
    try:
        # List objects in matches partition
        prefix = f"matches/tournament_id={tournament_id}/season_id={season_id}/"
        objects = minio_client.list_objects(
            'bronze-test',
            prefix=prefix,
            recursive=True
        )
        
        # Read NDJSON files and extract match IDs
        for obj in objects:
            if obj.object_name.endswith('.ndjson'):
                logger.info(f"  Reading {obj.object_name}")
                
                response = minio_client.get_object('bronze-test', obj.object_name)
                content = response.read().decode('utf-8')
                
                # Parse NDJSON
                for line in content.strip().split('\n'):
                    if line:
                        match_data = json.loads(line)
                        match_id = match_data.get('id')
                        if match_id and match_id not in match_ids:
                            match_ids.append(match_id)
                            
                            if len(match_ids) >= limit:
                                break
                
                if len(match_ids) >= limit:
                    break
        
        logger.info(f"✅ Found {len(match_ids)} match IDs")
        return match_ids[:limit]
        
    except Exception as e:
        logger.error(f"❌ Error reading match IDs: {e}", exc_info=True)
        return []

async def test_match_statistics(
    match_ids: list = None,
    use_config: bool = True,
    tournament_id: int = None,
    season_id: int = None,
    batch_size: int = 10,
    auto_fetch_ids: bool = False,
    limit: int = 10
) -> Dict[str, Any]:
    """
    Test match statistics extraction
    
    Args:
        match_ids: List of match IDs to extract stats for
        use_config: If True, load tournament/season from config
        tournament_id: Manual tournament ID
        season_id: Manual season ID
        batch_size: Batch size for processing
        auto_fetch_ids: If True, automatically fetch match IDs from bucket
        limit: Max number of matches to fetch stats for (if auto_fetch_ids=True)
    
    Returns:
        Extraction results dictionary
    """
    
    # Load configuration
    if use_config:
        config_path = project_root / "config" / "league_config.yaml"
        config = load_league_config(str(config_path))
        
        tournament_id = config['active_league']['league_id']
        season_id = config['active_season']['season_id']
        league_name = config['active_league']['league_name']
        
        logger.info("="*70)
        logger.info(f"TEST STATISTICS EXTRACTION: {league_name}")
        logger.info(f"Tournament ID: {tournament_id}")
        logger.info(f"Season ID: {season_id}")
        logger.info("="*70)
    
    # Auto-fetch match IDs from bucket if requested
    if auto_fetch_ids:
        logger.info(f"Auto-fetching up to {limit} match IDs from bucket...")
        match_ids = await get_match_ids_from_bucket(tournament_id, season_id, limit)
    
    if not match_ids:
        logger.error("No match IDs provided!")
        return {'error': 'No match IDs provided'}
    
    logger.info(f"\n🚀 Extracting statistics for {len(match_ids)} matches...")
    logger.info(f"Match IDs: {match_ids[:5]}{'...' if len(match_ids) > 5 else ''}")
    
    async with SofascoreETL() as extractor:
        results = await extractor.extract_match_details(
            match_ids=match_ids,
            tournament_id=tournament_id,
            season_id=season_id,
            batch_size=batch_size
        )
    
    # Display results
    logger.info("\n" + "="*70)
    logger.info("STATISTICS EXTRACTION RESULTS")
    logger.info("="*70)
    logger.info(f"✅ Total matches processed: {results.get('total_processed', 0)}")
    logger.info(f"📦 Batches stored: {len(results.get('stored_batches', []))}")
    
    if results.get('stored_batches'):
        logger.info("\n📂 Stored statistics files:")
        for batch in results.get('stored_batches', [])[:10]:  # Show first 10
            logger.info(f"  - {batch['object_key']} ({batch['record_count']} matches)")
        
        if len(results.get('stored_batches', [])) > 10:
            logger.info(f"  ... and {len(results['stored_batches']) - 10} more files")
    
    if results.get('errors'):
        logger.warning(f"\n⚠️  Errors: {len(results['errors'])}")
        for error in results.get('errors', [])[:5]:
            logger.warning(f"  - {error}")
    
    logger.info("="*70)
    
    return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test league data extraction to bronze-test bucket')
    parser.add_argument(
        '--mode',
        choices=['matches', 'stats'],
        default='matches',
        help='Extraction mode: matches or stats'
    )
    parser.add_argument(
        '--no-config',
        action='store_true',
        help='Do not use league_config.yaml (requires --tournament-id and --season-id)'
    )
    parser.add_argument(
        '--tournament-id',
        type=int,
        help='Tournament ID (required if --no-config)'
    )
    parser.add_argument(
        '--season-id',
        type=int,
        help='Season ID (required if --no-config)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=2,
        help='Maximum pages to extract (default: 2)'
    )
    parser.add_argument(
        '--match-ids',
        type=str,
        help='Comma-separated match IDs for stats mode (e.g., "12345,12346,12347")'
    )
    parser.add_argument(
        '--auto-fetch',
        action='store_true',
        help='Auto-fetch match IDs from bucket (stats mode only)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Max number of matches to process in stats mode with --auto-fetch (default: 10)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Batch size for statistics extraction (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.no_config and (not args.tournament_id or not args.season_id):
        parser.error("--tournament-id and --season-id are required when --no-config is used")
    
    # Run appropriate mode
    try:
        if args.mode == 'matches':
            results = asyncio.run(test_league_extraction(
                use_config=not args.no_config,
                tournament_id=args.tournament_id,
                season_id=args.season_id,
                max_pages=args.max_pages
            ))
        else:  # stats mode
            match_ids = None
            
            if args.match_ids:
                match_ids = [int(x.strip()) for x in args.match_ids.split(',')]
            
            results = asyncio.run(test_match_statistics(
                match_ids=match_ids,
                use_config=not args.no_config,
                tournament_id=args.tournament_id,
                season_id=args.season_id,
                batch_size=args.batch_size,
                auto_fetch_ids=args.auto_fetch,
                limit=args.limit
            ))
        
        logger.info("\n✅ Test completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ Test failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())