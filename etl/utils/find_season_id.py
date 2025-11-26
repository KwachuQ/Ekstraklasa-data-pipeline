# etl/utils/find_season_id.py
"""
Helper to find season_id for any league
"""
import asyncio
import logging
import sys
from etl.bronze.client import SofascoreClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def find_seasons(league_id: int, league_name: str = "Unknown"):
    """
    Find all available seasons for a league
    
    Args:
        league_id: League/tournament ID
        league_name: League name for display
    """
    try:
        async with SofascoreClient() as client:
            logger.info(f"Searching seasons for {league_name} (ID: {league_id})...")
            
            response = await client.get_tournament_seasons(league_id)
            
            if not response.is_valid:
                logger.error(f"No seasons found for {league_name}")
                return
            
            seasons = response.validated_items
            logger.info(f"\nFound {len(seasons)} seasons for {league_name}:\n")
            
            for season in seasons:
                print(f"  Season ID: {season['id']:6d}  |  Name: {season['name']:30s}  |  Year: {season['year']}")
            
            # Show the most recent season
            if seasons:
                latest = seasons[0]
                print(f"\n✓ Most recent season:")
                print(f"  season_id: {latest['id']}")
                print(f"  name: {latest['name']}")
                print(f"  year: {latest['year']}")
                
    except Exception as e:
        logger.error(f"Error finding seasons: {e}", exc_info=True)


async def main():
    """Find seasons for popular leagues"""
    
    leagues = [
        (8, "LaLiga"),
        (35, "Bundesliga"),
        (17, "Premier League"),
        (34, "Ligue 1"),
        (23, "Serie A"),
        (202, "Ekstraklasa"),
        (238, "Liga Portugal Betclic"),
    ]
    
    if len(sys.argv) > 1:
        # Use command line argument
        league_id = int(sys.argv[1])
        league_name = sys.argv[2] if len(sys.argv) > 2 else "Custom League"
        await find_seasons(league_id, league_name)
    else:
        # Show all popular leagues
        for league_id, league_name in leagues:
            await find_seasons(league_id, league_name)
            print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())