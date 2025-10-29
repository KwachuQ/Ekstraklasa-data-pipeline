#!/usr/bin/env python3
"""
Sofascore ETL - Ekstraklasa 2025-2026 Season Configuration
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any

from sofascore_etl import SofascoreETL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ekstraklasa 2025-2026 season configuration
EKSTRAKLASA_2025_26_CONFIG = {
    "category_id": 47,        # Poland
    "tournament_id": 202,     # Ekstraklasa
    "season_id": 76477,       # 2025/26 season (may need update when actual season starts)
    "tournament_name": "PKO BP Ekstraklasa",
    "season_name": "2025/26",
    "country": "Poland",
    "timezone": "Europe/Warsaw",
    "season_start": "2025-07-01",  # Approximate start
    "season_end": "2026-05-31"     # Approximate end
}

class EkstraklasaETL2025:
    """Specialized ETL for Ekstraklasa 2025-2026 season data"""
    
    def __init__(self):
        self.etl = SofascoreETL()
        self.config = EKSTRAKLASA_2025_26_CONFIG
    
    async def extract_full_season(self, max_pages: int = 20) -> Dict[str, Any]:
        """Extract complete Ekstraklasa 2025-2026 season data"""
        logger.info(f"Starting Ekstraklasa 2025-2026 ETL for season {self.config['season_name']}")
        logger.info(f"Tournament ID: {self.config['tournament_id']}, Season ID: {self.config['season_id']}")
        
        try:
            # Extract all matches for the season
            results = await self.etl.extract_tournament_matches(
                tournament_id=self.config['tournament_id'],
                season_id=self.config['season_id'],
                max_pages=max_pages,
                replace_partition=True  # Fresh start for new season
            )
            
            # Add Ekstraklasa-specific metadata
            results.update({
                "league_info": self.config,
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                "season_year": "2025-2026"
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Ekstraklasa 2025-2026 ETL failed: {e}")
            raise
    
    async def extract_recent_matches(self, pages: int = 3) -> Dict[str, Any]:
        """Extract recent Ekstraklasa 2025-2026 matches"""
        logger.info(f"Extracting recent Ekstraklasa 2025-2026 matches ({pages} pages)")
        
        return await self.etl.extract_tournament_matches(
            tournament_id=self.config['tournament_id'],
            season_id=self.config['season_id'],
            max_pages=pages,
            replace_partition=False  # Append to existing data
        )
    
    async def get_season_summary(self) -> Dict[str, Any]:
        """Get summary of stored Ekstraklasa 2025-2026 data"""
        partitions = self.etl.storage.list_partitions("matches", self.config['tournament_id'])
        
        # Filter partitions for 2025-2026 season
        season_partitions = [
            p for p in partitions 
            if f"season_id={self.config['season_id']}" in p
        ]
        
        summary = {
            "tournament_info": self.config,
            "season_partitions": len(season_partitions),
            "total_partitions": len(partitions),
            "date_range": {
                "earliest": None,
                "latest": None
            },
            "season_specific_partitions": season_partitions[:10]  # Show sample
        }
        
        if season_partitions:
            dates = []
            for partition in season_partitions:
                if 'date=' in partition:
                    date_part = partition.split('date=')[-1].split('/')[0]
                    dates.append(date_part)
            
            if dates:
                summary["date_range"] = {
                    "earliest": min(dates),
                    "latest": max(dates)
                }
        
        return summary
    
    async def validate_season_data(self) -> Dict[str, Any]:
        """Validate Ekstraklasa 2025-2026 season data quality"""
        logger.info("Validating Ekstraklasa 2025-2026 season data...")
        
        summary = await self.get_season_summary()
        
        validation_report = {
            "season_info": self.config,
            "data_availability": {
                "has_data": summary["season_partitions"] > 0,
                "partition_count": summary["season_partitions"],
                "date_coverage": summary["date_range"]
            },
            "quality_checks": {
                "partition_structure": "valid" if summary["season_partitions"] > 0 else "no_data",
                "date_range_valid": self._validate_date_range(summary["date_range"]),
                "expected_matches": self._estimate_season_matches()
            }
        }
        
        return validation_report
    
    def _validate_date_range(self, date_range: Dict[str, Any]) -> str:
        """Validate if date range makes sense for 2025-2026 season"""
        if not date_range["earliest"] or not date_range["latest"]:
            return "no_dates"
        
        try:
            earliest = datetime.strptime(date_range["earliest"], "%Y-%m-%d")
            latest = datetime.strptime(date_range["latest"], "%Y-%m-%d")
            
            # Check if dates are within reasonable range for 2025-2026 season
            season_start = datetime.strptime("2025-07-01", "%Y-%m-%d")
            season_end = datetime.strptime("2026-05-31", "%Y-%m-%d")
            
            if earliest >= season_start and latest <= season_end:
                return "valid_range"
            elif earliest.year in [2025, 2026] and latest.year in [2025, 2026]:
                return "reasonable_range"
            else:
                return "invalid_range"
                
        except ValueError:
            return "invalid_format"
    
    def _estimate_season_matches(self) -> int:
        """Estimate expected number of matches for Ekstraklasa season"""
        # Ekstraklasa: 18 teams, each plays 34 matches (17 home + 17 away)
        # Total season matches: 18 * 34 / 2 = 306 matches
        # Plus potential playoff matches
        return 306

async def main():
    """Extract Ekstraklasa 2025-2026 data - main execution"""
    ekstraklasa_etl = EkstraklasaETL2025()
    
    logger.info("=" * 70)
    logger.info("🇵🇱 EKSTRAKLASA 2025-2026 SEASON DATA EXTRACTION")
    logger.info("=" * 70)
    
    try:
        # Step 1: Test with recent matches
        logger.info("Step 1: Testing with recent matches...")
        recent_results = await ekstraklasa_etl.extract_recent_matches(pages=2)
        
        logger.info(f"✅ Recent matches extracted:")
        logger.info(f"   Total matches: {recent_results['total_matches']}")
        logger.info(f"   Stored batches: {len(recent_results['stored_batches'])}")
        
        if recent_results['errors']:
            logger.warning(f"   Errors encountered: {len(recent_results['errors'])}")
            for error in recent_results['errors'][:3]:
                logger.warning(f"     - {error}")
        
        # Step 2: Show partition structure for 2025-2026
        logger.info("\nStep 2: Checking 2025-2026 season partition structure...")
        summary = await ekstraklasa_etl.get_season_summary()
        
        logger.info(f"✅ Season 2025-2026 summary:")
        logger.info(f"   Season partitions: {summary['season_partitions']}")
        logger.info(f"   Date range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
        
        if summary['season_specific_partitions']:
            logger.info("   Sample 2025-2026 partitions:")
            for partition in summary['season_specific_partitions'][:5]:
                logger.info(f"     - {partition}")
        
        # Step 3: Validate season data
        logger.info("\nStep 3: Validating 2025-2026 season data...")
        validation = await ekstraklasa_etl.validate_season_data()
        
        logger.info(f"✅ Data validation:")
        logger.info(f"   Has data: {validation['data_availability']['has_data']}")
        logger.info(f"   Partition structure: {validation['quality_checks']['partition_structure']}")
        logger.info(f"   Date range validity: {validation['quality_checks']['date_range_valid']}")
        logger.info(f"   Expected matches: {validation['quality_checks']['expected_matches']}")
        
        # Step 4: Ready for full extraction (when season starts)
        logger.info("\nStep 4: Full season extraction readiness...")
        if validation['data_availability']['has_data']:
            logger.info("✅ System ready for 2025-2026 season extraction")
            logger.info("💡 Uncomment full extraction when season officially starts")
        else:
            logger.info("⏳ Waiting for 2025-2026 season to begin")
            logger.info("🔧 System configured and ready for when data becomes available")
        
        # Uncomment for full season extraction when season starts:
        # full_results = await ekstraklasa_etl.extract_full_season(max_pages=25)
        # logger.info(f"✅ Full season extracted: {full_results['total_matches']} matches")
        
        logger.info("\n" + "=" * 70)
        logger.info("🎉 Ekstraklasa 2025-2026 ETL setup completed successfully!")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"💥 Ekstraklasa 2025-2026 ETL failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
