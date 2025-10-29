#!/usr/bin/env python3
"""
ETL Worker Health Check
Simple health check for container monitoring
"""

import os
import sys
import logging

def health_check():
    """Perform basic health check"""
    try:
        # Check if required modules are available
        import psycopg2
        import minio
        import pandas
        
        # Check if environment variables are set
        required_vars = ['POSTGRES_HOST', 'MINIO_ENDPOINT']
        for var in required_vars:
            if not os.getenv(var):
                return False
        
        return True
        
    except ImportError as e:
        logging.error(f"Missing required module: {e}")
        return False
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return False

if __name__ == "__main__":
    if health_check():
        print("ETL Worker is healthy")
        sys.exit(0)
    else:
        print("ETL Worker health check failed")
        sys.exit(1)
