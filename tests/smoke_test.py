#!/usr/bin/env python3
"""
ETL Worker Smoke Test
Tests connectivity to PostgreSQL, MinIO and Sofascore ETL components
"""

import os
import sys
import logging
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_postgres_connection():
    """Test PostgreSQL connection"""
    try:
        import psycopg2
        
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'dwh'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
        }
        
        logger.info("Testing PostgreSQL connection...")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL connected successfully: {version}")
        
        # Test schemas existence
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('bronze', 'silver', 'gold')
            ORDER BY schema_name;
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        logger.info(f"Available schemas: {schemas}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False

def test_minio_connection():
    """Test MinIO connection"""
    try:
        from minio import Minio
        
        client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        
        logger.info("Testing MinIO connection...")
        
        # List buckets
        buckets = list(client.list_buckets())
        bucket_names = [bucket.name for bucket in buckets]
        logger.info(f"MinIO connected successfully. Available buckets: {bucket_names}")
        
        # Test if bronze/silver/gold buckets exist
        expected_buckets = ['bronze', 'silver', 'gold']
        missing_buckets = [b for b in expected_buckets if b not in bucket_names]
        
        if missing_buckets:
            logger.warning(f"Missing buckets: {missing_buckets}")
        else:
            logger.info("All required buckets (bronze, silver, gold) are available")
        
        return True
        
    except Exception as e:
        logger.error(f"MinIO connection failed: {e}")
        return False

def test_etl_environment():
    """Test ETL environment variables"""
    logger.info("Testing ETL environment...")
    
    required_vars = [
        'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 
        'POSTGRES_USER', 'POSTGRES_PASSWORD',
        'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"Missing environment variables: {missing_vars}")
        return False
    
    logger.info("All required environment variables are set")
    return True

def test_sofascore_dependencies():
    """Test Sofascore ETL dependencies"""
    logger.info("Testing Sofascore ETL dependencies...")
    
    try:
        # Test required packages
        import httpx
        import tenacity
        import pydantic
        logger.info("✅ HTTP client dependencies available")
        
        # Test our custom modules (with path adjustment)
        sys.path.append('/opt/etl/scripts')
        from scripts.sofascore_client import SofascoreClient
        from scripts.storage_manager import BronzeStorageManager
        from scripts.sofascore_etl import SofascoreETL
        logger.info("✅ Sofascore ETL modules loaded successfully")
        
        return True
        
    except ImportError as e:
        logger.error(f"Missing Sofascore ETL dependency: {e}")
        return False
    except Exception as e:
        logger.error(f"Sofascore ETL dependency test failed: {e}")
        return False

async def test_sofascore_client():
    """Test Sofascore client basic functionality"""
    logger.info("Testing Sofascore client...")
    
    try:
        sys.path.append('/opt/etl/scripts')
        from scripts.sofascore_client import SofascoreClient
        
        async with SofascoreClient(timeout=10) as client:
            # Test client initialization
            logger.info("✅ Sofascore client initialized")
            
            # Test batch ID generation
            batch_id = client._generate_batch_id("/test", {"param": "value"})
            assert len(batch_id) == 16
            logger.info(f"✅ Batch ID generation works: {batch_id}")
            
            # Test metadata generation
            metadata = client._get_ingestion_metadata("/test", {"param": "value"})
            required_fields = ["batch_id", "ingestion_timestamp", "source", "endpoint"]
            for field in required_fields:
                assert field in metadata
            logger.info("✅ Metadata generation works")
            
        return True
        
    except Exception as e:
        logger.error(f"Sofascore client test failed: {e}")
        return False

async def test_ekstraklasa_2025_config():
    """Test Ekstraklasa 2025-26 configuration"""
    logger.info("Testing Ekstraklasa 2025-26 configuration...")
    
    try:
        sys.path.append('/opt/etl/scripts')
        from scripts.ekstraklasa_config import EKSTRAKLASA_2025_26_CONFIG, EkstraklasaETL2025
        
        # Test configuration
        config = EKSTRAKLASA_2025_26_CONFIG
        required_keys = ["tournament_id", "season_id", "tournament_name", "season_name"]
        for key in required_keys:
            assert key in config, f"Missing config key: {key}"
        
        logger.info(f"✅ Ekstraklasa 2025-26 config loaded: {config['tournament_name']} {config['season_name']}")
        
        # Test ETL class initialization
        etl = EkstraklasaETL2025()
        assert etl.config == config
        logger.info("✅ EkstraklasaETL2025 initialized successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Ekstraklasa 2025-26 config test failed: {e}")
        return False

def main():
    """Run all smoke tests"""
    logger.info("=" * 50)
    logger.info("ETL Worker Smoke Test Started")
    logger.info("=" * 50)
    
    test_results = {
        'environment': test_etl_environment(),
        'postgres': test_postgres_connection(),
        'minio': test_minio_connection(),
        'sofascore_deps': test_sofascore_dependencies()
    }
    
    # Run async tests
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        test_results['sofascore_client'] = loop.run_until_complete(test_sofascore_client())
        test_results['ekstraklasa_2025_config'] = loop.run_until_complete(test_ekstraklasa_2025_config())
    finally:
        loop.close()
    
    logger.info("=" * 50)
    logger.info("Smoke Test Results:")
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"  {test_name.upper()}: {status}")
    
    all_passed = all(test_results.values())
    if all_passed:
        logger.info("🎉 All smoke tests passed!")
        logger.info("🇵🇱 Ready for Ekstraklasa 2025-26 ETL operations!")
        sys.exit(0)
    else:
        logger.error("💥 Some smoke tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
