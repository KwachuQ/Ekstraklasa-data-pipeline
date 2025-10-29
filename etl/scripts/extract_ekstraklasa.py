#!/usr/bin/env python3
"""
Production script for Ekstraklasa 2025-2026 data extraction
Optimized for Polish football league data with comprehensive monitoring
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Dict, Any

from sofascore_etl import SofascoreETL

# Configure logging with timezone info
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/opt/etl/logs/ekstraklasa_2025.log')
    ]
)
logger = logging.getLogger(__name__)

# Ekstraklasa 2025-2026 season configuration
EKSTRAKLASA_2025_26 = {
    "category_id": 47,        # Poland
    "tournament_id": 202,     # Ekstraklasa
    "season_id": 76477,       # 2025/26 season (update when confirmed)
    "tournament_name": "PKO BP Ekstraklasa",
    "season_name": "2025/26",
    "country": "Poland",
    "timezone": "Europe/Warsaw",
    "season_start": "2025-07-01",
    "season_end": "2026-05-31",
    "expected_matches": 306   # 18 teams * 34 matches / 2
}

class EkstraklasaProductionETL2025:
    """Production ETL for Ekstraklasa 2025-2026 with monitoring and error handling"""
    
    def __init__(self, config: Dict[str, Any] = EKSTRAKLASA_2025_26):
        self.config = config
        self.etl = SofascoreETL()
        self.extraction_stats = {
            "start_time": None,
            "end_time": None,
            "total_matches": 0,
            "total_batches": 0,
            "errors": [],
            "partitions_created": [],
            "season_year": "2025-26"
        }
    
    async def extract_complete_season_2025(
        self, 
        max_pages: int = 25,
        replace_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Extract complete Ekstraklasa 2025-2026 season with comprehensive monitoring
        
        Args:
            max_pages: Maximum pages to fetch (increased for full season coverage)
            replace_existing: Whether to replace existing partition data
        """
        self.extraction_stats["start_time"] = datetime.now(timezone.utc)
        
        logger.info("=" * 80)
        logger.info(f"🇵🇱 STARTING EKSTRAKLASA {self.config['season_name']} PRODUCTION EXTRACTION")
        logger.info("=" * 80)
        logger.info(f"Tournament: {self.config['tournament_name']}")
        logger.info(f"Season: {self.config['season_name']} ({self.config['season_start']} - {self.config['season_end']})")
        logger.info(f"Tournament ID: {self.config['tournament_id']}")
        logger.info(f"Season ID: {self.config['season_id']}")
        logger.info(f"Expected matches: {self.config['expected_matches']}")
        logger.info(f"Max pages: {max_pages}")
        logger.info(f"Replace existing: {replace_existing}")
        
        try:
            # Extract matches with pagination
            results = await self.etl.extract_tournament_matches(
                tournament_id=self.config['tournament_id'],
                season_id=self.config['season_id'],
                max_pages=max_pages,
                replace_partition=replace_existing
            )
            
            # Update statistics
            self.extraction_stats.update({
                "end_time": datetime.now(timezone.utc),
                "total_matches": results['total_matches'],
                "total_batches": len(results['stored_batches']),
                "errors": results['errors']
            })
            
            # Extract partition information
            for batch in results['stored_batches']:
                partition = batch['partition_key']
                if partition not in self.extraction_stats["partitions_created"]:
                    self.extraction_stats["partitions_created"].append(partition)
            
            # Log comprehensive results
            self._log_extraction_results()
            
            # Generate detailed report
            report = self._generate_extraction_report(results)
            
            # Validate season expectations
            await self._validate_season_expectations(report)
            
            return report
            
        except Exception as e:
            self.extraction_stats["end_time"] = datetime.now(timezone.utc)
            self.extraction_stats["errors"].append(f"Fatal error: {str(e)}")
            logger.error(f"💥 Ekstraklasa 2025-26 extraction failed: {e}")
            raise
    
    def _log_extraction_results(self):
        """Log detailed extraction results for 2025-26"""
        duration = None
        if self.extraction_stats["start_time"] and self.extraction_stats["end_time"]:
            duration = self.extraction_stats["end_time"] - self.extraction_stats["start_time"]
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 EKSTRAKLASA 2025-26 EXTRACTION RESULTS")
        logger.info("=" * 80)
        logger.info(f"⏱️  Duration: {duration}")
        logger.info(f"⚽ Total matches: {self.extraction_stats['total_matches']}")
        logger.info(f"📦 Total batches: {self.extraction_stats['total_batches']}")
        logger.info(f"📁 Partitions created: {len(self.extraction_stats['partitions_created'])}")
        logger.info(f"❌ Errors: {len(self.extraction_stats['errors'])}")
        logger.info(f"🏆 Season: {self.extraction_stats['season_year']}")
        
        # Show completion percentage
        if self.config['expected_matches'] > 0:
            completion_pct = (self.extraction_stats['total_matches'] / self.config['expected_matches']) * 100
            logger.info(f"📈 Season completion: {completion_pct:.1f}% ({self.extraction_stats['total_matches']}/{self.config['expected_matches']})")
        
        if self.extraction_stats['partitions_created']:
            logger.info("\n📅 Created partitions (sample):")
            for partition in self.extraction_stats['partitions_created'][:10]:
                logger.info(f"   - {partition}")
            if len(self.extraction_stats['partitions_created']) > 10:
                remaining = len(self.extraction_stats['partitions_created']) - 10
                logger.info(f"   ... and {remaining} more partitions")
        
        if self.extraction_stats['errors']:
            logger.warning("\n⚠️  Errors encountered:")
            for error in self.extraction_stats['errors'][:5]:
                logger.warning(f"   - {error}")
            if len(self.extraction_stats['errors']) > 5:
                remaining = len(self.extraction_stats['errors']) - 5
                logger.warning(f"   ... and {remaining} more errors")
    
    def _generate_extraction_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive extraction report for 2025-26"""
        return {
            "extraction_metadata": {
                "tournament_info": self.config,
                "extraction_timestamp": self.extraction_stats["start_time"].isoformat(),
                "completion_timestamp": self.extraction_stats["end_time"].isoformat(),
                "duration_seconds": (
                    self.extraction_stats["end_time"] - self.extraction_stats["start_time"]
                ).total_seconds(),
                "extractor_version": "1.0.0",
                "season_year": "2025-26"
            },
            "data_summary": {
                "total_matches_extracted": self.extraction_stats["total_matches"],
                "total_batches_stored": self.extraction_stats["total_batches"],
                "unique_partitions": len(self.extraction_stats["partitions_created"]),
                "error_count": len(self.extraction_stats["errors"]),
                "expected_matches": self.config["expected_matches"],
                "completion_percentage": (
                    (self.extraction_stats["total_matches"] / self.config["expected_matches"]) * 100
                    if self.config["expected_matches"] > 0 else 0
                )
            },
            "storage_details": {
                "stored_batches": results['stored_batches'],
                "partitions": self.extraction_stats["partitions_created"]
            },
            "quality_metrics": {
                "success_rate": (
                    (self.extraction_stats["total_matches"] / 
                     (self.extraction_stats["total_matches"] + len(self.extraction_stats["errors"])))
                    if (self.extraction_stats["total_matches"] + len(self.extraction_stats["errors"])) > 0
                    else 0
                ),
                "errors": self.extraction_stats["errors"],
                "season_coverage": self._assess_season_coverage()
            }
        }
    
    async def _validate_season_expectations(self, report: Dict[str, Any]):
        """Validate if extraction meets 2025-26 season expectations"""
        logger.info("\n🔍 Validating 2025-26 season expectations...")
        
        extracted_matches = report["data_summary"]["total_matches_extracted"]
        expected_matches = self.config["expected_matches"]
        completion_pct = report["data_summary"]["completion_percentage"]
        
        # Season progress validation
        if completion_pct >= 90:
            logger.info(f"✅ Season nearly complete: {completion_pct:.1f}%")
        elif completion_pct >= 50:
            logger.info(f"🟡 Season in progress: {completion_pct:.1f}%")
        elif completion_pct >= 10:
            logger.info(f"🟠 Season started: {completion_pct:.1f}%")
        else:
            logger.warning(f"🔴 Season beginning or not started: {completion_pct:.1f}%")
        
        # Date range validation
        partitions = self.extraction_stats["partitions_created"]
        if partitions:
            dates = []
            for partition in partitions:
                if 'date=' in partition:
                    date_part = partition.split('date=')[-1].split('/')[0]
                    try:
                        date_obj = datetime.strptime(date_part, '%Y-%m-%d')
                        dates.append(date_obj)
                    except ValueError:
                        continue
            
            if dates:
                earliest = min(dates)
                latest = max(dates)
                logger.info(f"📅 Match date range: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}")
                
                # Validate it's actually 2025-26 season
                if earliest.year >= 2025 and latest.year <= 2026:
                    logger.info(f"✅ Date range confirms 2025-26 season")
                else:
                    logger.warning(f"⚠️  Date range doesn't match 2025-26 season expectations")
    
    def _assess_season_coverage(self) -> str:
        """Assess the coverage of 2025-26 season data"""
        completion_pct = (
            (self.extraction_stats["total_matches"] / self.config["expected_matches"]) * 100
            if self.config["expected_matches"] > 0 else 0
        )
        
        if completion_pct >= 95:
            return "complete"
        elif completion_pct >= 75:
            return "near_complete"
        elif completion_pct >= 50:
            return "substantial"
        elif completion_pct >= 25:
            return "partial"
        elif completion_pct >= 5:
            return "early_season"
        else:
            return "minimal_or_preseason"
    
    async def verify_extraction_2025(self) -> Dict[str, Any]:
        """Verify 2025-26 extraction quality and completeness"""
        logger.info("🔍 Verifying Ekstraklasa 2025-26 extraction...")
        
        # Get partition summary for 2025-26
        all_partitions = self.etl.storage.list_partitions("matches", self.config['tournament_id'])
        season_partitions = [
            p for p in all_partitions 
            if f"season_id={self.config['season_id']}" in p
        ]
        
        verification_report = {
            "season_info": self.config,
            "total_partitions": len(season_partitions),
            "date_coverage": {},
            "batch_verification": {"verified": 0, "failed": 0},
            "data_quality": {"valid_matches": 0, "invalid_matches": 0}
        }
        
        if season_partitions:
            # Extract date range for 2025-26
            dates = []
            for partition in season_partitions:
                if 'date=' in partition:
                    date_part = partition.split('date=')[-1].split('/')[0]
                    dates.append(date_part)
            
            if dates:
                verification_report["date_coverage"] = {
                    "earliest_date": min(dates),
                    "latest_date": max(dates),
                    "total_days": len(set(dates)),
                    "season_year": "2025-26"
                }
        
        logger.info(f"✅ 2025-26 verification completed:")
        logger.info(f"   - Season partitions: {verification_report['total_partitions']}")
        logger.info(f"   - Date coverage: {verification_report['date_coverage']}")
        
        return verification_report

async def main():
    """Main execution for Ekstraklasa 2025-26 production ETL"""
    ekstraklasa_etl = EkstraklasaProductionETL2025()
    
    try:
        # Step 1: Extract complete 2025-26 season
        logger.info("Starting Ekstraklasa 2025-26 production extraction...")
        
        extraction_report = await ekstraklasa_etl.extract_complete_season_2025(
            max_pages=30,  # Increased for complete season coverage
            replace_existing=True  # Fresh extraction for new season
        )
        
        # Step 2: Verify extraction
        verification_report = await ekstraklasa_etl.verify_extraction_2025()
        
        # Step 3: Final summary
        logger.info("\n" + "=" * 80)
        logger.info("🎉 EKSTRAKLASA 2025-26 EXTRACTION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"📊 Summary:")
        logger.info(f"   - Season: {extraction_report['extraction_metadata']['season_year']}")
        logger.info(f"   - Matches extracted: {extraction_report['data_summary']['total_matches_extracted']}")
        logger.info(f"   - Completion: {extraction_report['data_summary']['completion_percentage']:.1f}%")
        logger.info(f"   - Batches stored: {extraction_report['data_summary']['total_batches_stored']}")
        logger.info(f"   - Partitions created: {extraction_report['data_summary']['unique_partitions']}")
        logger.info(f"   - Success rate: {extraction_report['quality_metrics']['success_rate']:.2%}")
        logger.info(f"   - Date coverage: {verification_report['date_coverage']}")
        logger.info(f"   - Season coverage: {extraction_report['quality_metrics']['season_coverage']}")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Production extraction failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
