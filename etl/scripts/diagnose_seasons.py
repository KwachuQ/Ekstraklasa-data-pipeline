#!/usr/bin/env python3
"""
Diagnose Ekstraklasa seasons - check what's already downloaded vs what's available
"""

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Set

from minio import Minio
from sofascore_client import SofascoreClient
from storage_manager import BronzeStorageManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EkstraklasaSeasonDiagnostic:
    """Diagnostic tool for Ekstraklasa season data completeness"""
    
    def __init__(self):
        # Initialize MinIO client
        self.minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        
        self.storage = BronzeStorageManager(self.minio_client)
        self.tournament_id = 202  # Ekstraklasa
        
        # Expected matches per season (18 teams * 34 rounds / 2)
        self.expected_matches_per_season = 306
    
    async def get_api_seasons(self) -> List[Dict[str, Any]]:
        """Get all available seasons from Sofascore API"""
        logger.info("🔍 Fetching available seasons from API...")
        
        async with SofascoreClient() as client:
            try:
                response = await client.get_tournament_seasons(self.tournament_id)
                seasons = response['data'].get('items', [])
                
                logger.info(f"✅ Found {len(seasons)} seasons in API")
                return seasons
                
            except Exception as e:
                logger.error(f"❌ Failed to fetch seasons from API: {e}")
                return []
    
    def extract_season_ids_from_partitions(self) -> Set[int]:
        """Extract season IDs from existing bronze partitions"""
        logger.info("🔍 Scanning bronze partitions for season IDs...")
        
        try:
            # Get all match partitions for Ekstraklasa
            objects = self.minio_client.list_objects(
                'bronze', 
                prefix=f'matches/tournament_id={self.tournament_id}/', 
                recursive=True
            )
            
            season_ids = set()
            season_id_pattern = re.compile(r'season_id=(\d+)')
            
            for obj in objects:
                match = season_id_pattern.search(obj.object_name)
                if match:
                    season_ids.add(int(match.group(1)))
            
            logger.info(f"✅ Found {len(season_ids)} unique seasons in bronze layer")
            return season_ids
            
        except Exception as e:
            logger.error(f"❌ Failed to scan partitions: {e}")
            return set()
    
    def analyze_season_completeness(self, season_id: int) -> Dict[str, Any]:
        """Analyze completeness of a specific season"""
        logger.debug(f"🔍 Analyzing season {season_id}...")
        
        analysis = {
            "season_id": season_id,
            "total_matches": 0,
            "total_batches": 0,
            "unique_dates": set(),
            "partitions": [],
            "date_range": {"earliest": None, "latest": None},
            "quality_issues": []
        }
        
        try:
            # Find all objects for this season
            prefix = f'matches/tournament_id={self.tournament_id}/season_id={season_id}/'
            objects = self.minio_client.list_objects('bronze', prefix=prefix, recursive=True)
            
            manifest_objects = [obj for obj in objects if obj.object_name.endswith('_manifest.json')]
            
            for manifest_obj in manifest_objects:
                try:
                    # Read manifest
                    response = self.minio_client.get_object('bronze', manifest_obj.object_name)
                    manifest = json.loads(response.read().decode('utf-8'))
                    
                    # Extract partition info
                    object_key = manifest['object_key']
                    date_match = re.search(r'date=(\d{4}-\d{2}-\d{2})', object_key)
                    
                    if date_match:
                        match_date = date_match.group(1)
                        analysis["unique_dates"].add(match_date)
                        
                        # Track date range
                        if not analysis["date_range"]["earliest"] or match_date < analysis["date_range"]["earliest"]:
                            analysis["date_range"]["earliest"] = match_date
                        if not analysis["date_range"]["latest"] or match_date > analysis["date_range"]["latest"]:
                            analysis["date_range"]["latest"] = match_date
                    
                    # Accumulate stats
                    analysis["total_matches"] += manifest.get('record_count', 0)
                    analysis["total_batches"] += 1
                    analysis["partitions"].append(object_key)
                    
                    # Quality checks
                    if manifest.get('record_count', 0) == 0:
                        analysis["quality_issues"].append(f"Empty batch: {object_key}")
                    
                except Exception as e:
                    analysis["quality_issues"].append(f"Cannot read manifest {manifest_obj.object_name}: {e}")
            
            # Convert set to count
            analysis["unique_dates"] = len(analysis["unique_dates"])
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze season {season_id}: {e}")
            analysis["quality_issues"].append(f"Analysis failed: {e}")
            return analysis
    
    def calculate_completeness_status(self, match_count: int) -> str:
        """Calculate season completeness status"""
        if match_count == 0:
            return "MISSING"
        
        completion_pct = (match_count / self.expected_matches_per_season) * 100
        
        if completion_pct >= 95:
            return "COMPLETE"
        elif completion_pct >= 75:
            return "SUBSTANTIAL"
        elif completion_pct >= 25:
            return "PARTIAL"
        else:
            return "MINIMAL"
    
    async def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive diagnostic report"""
        logger.info("📊 Generating comprehensive diagnostic report...")
        
        # Get API seasons
        api_seasons = await self.get_api_seasons()
        api_season_lookup = {season['id']: season for season in api_seasons}
        
        # Get bronze seasons
        bronze_season_ids = self.extract_season_ids_from_partitions()
        
        # Analyze each season in bronze
        bronze_analysis = {}
        for season_id in bronze_season_ids:
            bronze_analysis[season_id] = self.analyze_season_completeness(season_id)
        
        # Create comprehensive report
        report = {
            "diagnostic_timestamp": datetime.utcnow().isoformat(),
            "tournament_id": self.tournament_id,
            "tournament_name": "PKO BP Ekstraklasa",
            "api_summary": {
                "total_seasons_available": len(api_seasons),
                "seasons": api_seasons
            },
            "bronze_summary": {
                "total_seasons_downloaded": len(bronze_season_ids),
                "season_ids": sorted(list(bronze_season_ids))
            },
            "season_analysis": {},
            "missing_seasons": [],
            "incomplete_seasons": [],
            "complete_seasons": [],
            "quality_summary": {
                "total_matches_stored": 0,
                "total_batches_stored": 0,
                "seasons_with_issues": 0
            }
        }
        
        # Analyze each season
        all_season_ids = set(api_season_lookup.keys()) | bronze_season_ids
        
        for season_id in sorted(all_season_ids):
            season_info = api_season_lookup.get(season_id, {"name": "Unknown", "year": "Unknown"})
            
            if season_id in bronze_analysis:
                analysis = bronze_analysis[season_id]
                status = self.calculate_completeness_status(analysis["total_matches"])
                
                season_report = {
                    "season_id": season_id,
                    "season_name": season_info.get("name", "Unknown"),
                    "season_year": season_info.get("year", "Unknown"),
                    "status": status,
                    "in_bronze": True,
                    "in_api": season_id in api_season_lookup,
                    "matches_found": analysis["total_matches"],
                    "expected_matches": self.expected_matches_per_season,
                    "completion_percentage": round((analysis["total_matches"] / self.expected_matches_per_season) * 100, 1),
                    "unique_dates": analysis["unique_dates"],
                    "total_batches": analysis["total_batches"],
                    "date_range": analysis["date_range"],
                    "quality_issues": analysis["quality_issues"]
                }
                
                # Update quality summary
                report["quality_summary"]["total_matches_stored"] += analysis["total_matches"]
                report["quality_summary"]["total_batches_stored"] += analysis["total_batches"]
                if analysis["quality_issues"]:
                    report["quality_summary"]["seasons_with_issues"] += 1
                
                # Categorize seasons
                if status == "COMPLETE":
                    report["complete_seasons"].append(season_id)
                elif status in ["SUBSTANTIAL", "PARTIAL", "MINIMAL"]:
                    report["incomplete_seasons"].append(season_id)
                
            else:
                # Season not in bronze
                season_report = {
                    "season_id": season_id,
                    "season_name": season_info.get("name", "Unknown"),
                    "season_year": season_info.get("year", "Unknown"),
                    "status": "MISSING",
                    "in_bronze": False,
                    "in_api": season_id in api_season_lookup,
                    "matches_found": 0,
                    "expected_matches": self.expected_matches_per_season,
                    "completion_percentage": 0.0,
                    "unique_dates": 0,
                    "total_batches": 0,
                    "date_range": {"earliest": None, "latest": None},
                    "quality_issues": []
                }
                
                report["missing_seasons"].append(season_id)
            
            report["season_analysis"][season_id] = season_report
        
        return report
    
    def print_summary_report(self, report: Dict[str, Any]):
        """Print formatted summary report"""
        print("\n" + "=" * 80)
        print("🇵🇱 EKSTRAKLASA SEASONS DIAGNOSTIC REPORT")
        print("=" * 80)
        print(f"📅 Generated: {report['diagnostic_timestamp']}")
        print(f"🏆 Tournament: {report['tournament_name']} (ID: {report['tournament_id']})")
        
        print(f"\n📊 OVERVIEW:")
        print(f"  • Total seasons in API: {report['api_summary']['total_seasons_available']}")
        print(f"  • Total seasons in Bronze: {report['bronze_summary']['total_seasons_downloaded']}")
        print(f"  • Total matches stored: {report['quality_summary']['total_matches_stored']:,}")
        print(f"  • Total batches stored: {report['quality_summary']['total_batches_stored']:,}")
        
        print(f"\n🎯 SEASON STATUS:")
        print(f"  • ✅ Complete seasons: {len(report['complete_seasons'])}")
        print(f"  • 🟡 Incomplete seasons: {len(report['incomplete_seasons'])}")
        print(f"  • ❌ Missing seasons: {len(report['missing_seasons'])}")
        print(f"  • ⚠️  Seasons with issues: {report['quality_summary']['seasons_with_issues']}")
        
        # Show detailed season breakdown
        print(f"\n📋 DETAILED SEASON BREAKDOWN:")
        print("-" * 80)
        
        for season_id in sorted(report['season_analysis'].keys(), reverse=True):
            season = report['season_analysis'][season_id]
            status_emoji = {
                "COMPLETE": "✅",
                "SUBSTANTIAL": "🟢", 
                "PARTIAL": "🟡",
                "MINIMAL": "🟠",
                "MISSING": "❌"
            }
            
            emoji = status_emoji.get(season['status'], '❓')
            print(f"{emoji} {season['season_year']} ({season['season_name']}) - ID: {season['season_id']}")
            print(f"    Status: {season['status']} ({season['completion_percentage']:.1f}%)")
            print(f"    Matches: {season['matches_found']:,}/{season['expected_matches']:,}")
            
            if season['in_bronze']:
                print(f"    Dates: {season['unique_dates']}, Batches: {season['total_batches']}")
                if season['date_range']['earliest']:
                    print(f"    Period: {season['date_range']['earliest']} → {season['date_range']['latest']}")
                
                if season['quality_issues']:
                    print(f"    ⚠️  Issues: {len(season['quality_issues'])}")
        
        # Priority recommendations
        print(f"\n🎯 RECOMMENDATIONS:")
        if report['missing_seasons']:
            missing_recent = [sid for sid in report['missing_seasons'] 
                            if report['season_analysis'][sid]['season_year'] >= '2020']
            if missing_recent:
                print(f"  🔴 HIGH PRIORITY - Missing recent seasons: {missing_recent}")
        
        if report['incomplete_seasons']:
            incomplete_recent = [sid for sid in report['incomplete_seasons']
                               if report['season_analysis'][sid]['season_year'] >= '2020']
            if incomplete_recent:
                print(f"  🟡 MEDIUM PRIORITY - Incomplete recent seasons: {incomplete_recent}")
        
        print(f"\n💡 NEXT STEPS:")
        if report['missing_seasons']:
            print(f"  1. Run backfill for missing seasons: {report['missing_seasons'][:5]}...")
        if report['incomplete_seasons']:
            print(f"  2. Complete partial seasons: {report['incomplete_seasons'][:3]}...")
        if report['quality_summary']['seasons_with_issues'] > 0:
            print(f"  3. Investigate {report['quality_summary']['seasons_with_issues']} seasons with quality issues")

async def main():
    """Main diagnostic execution"""
    diagnostic = EkstraklasaSeasonDiagnostic()
    
    try:
        # Generate comprehensive report
        report = await diagnostic.generate_comprehensive_report()
        
        # Print summary
        diagnostic.print_summary_report(report)
        
        # Save detailed report to file
        report_file = f"ekstraklasa_diagnostic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Detailed report saved to: {report_file}")
        
        # Return key lists for other scripts to use
        return {
            "missing_seasons": report['missing_seasons'],
            "incomplete_seasons": report['incomplete_seasons'],
            "complete_seasons": report['complete_seasons']
        }
        
    except Exception as e:
        logger.error(f"❌ Diagnostic failed: {e}")
        raise

if __name__ == "__main__":
    result = asyncio.run(main())
    print(f"\n🎯 SUMMARY LISTS:")
    print(f"Missing seasons: {result['missing_seasons']}")
    print(f"Incomplete seasons: {result['incomplete_seasons']}")
    print(f"Complete seasons: {result['complete_seasons']}")