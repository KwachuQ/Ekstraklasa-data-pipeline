#!/usr/bin/env python3
"""
MinIO Storage Manager for Bronze Layer with Hive-style partitioning
"""

import json
import logging
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

# ============================================================================
# Result Types
# ============================================================================

@dataclass
class StorageResult:
    """Standardized storage operation result"""
    success: bool
    object_key: str
    manifest_key: Optional[str] = None
    partition_key: Optional[str] = None
    record_count: int = 0
    file_size_bytes: int = 0
    etag: Optional[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility"""
        return {k: v for k, v in asdict(self).items() if v is not None}

# ============================================================================
# Storage Manager
# ============================================================================

class BronzeStorageManager:
    """
    Manages bronze layer storage with:
    - Hive-style partitioning (tournament_id=X/season_id=Y/date=Z)
    - NDJSON format for streaming ingestion
    - Manifest files for metadata tracking
    - Partition replacement capability
    """
    
    SCHEMA_VERSION = "1.0"
    
    def __init__(self, minio_client: Minio, bucket_name: str = "bronze"):
        """
        Initialize storage manager
        
        Args:
            minio_client: Configured MinIO client instance
            bucket_name: Target bucket name (default: bronze)
        """
        self.client = minio_client
        self.bucket_name = bucket_name
        self._ensure_bucket_exists()  # Check once at initialization
    
    def _ensure_bucket_exists(self) -> None:
        """Ensure bronze bucket exists (called once at init)"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"✅ Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"❌ Error creating bucket {self.bucket_name}: {e}")
            raise
    
    # ------------------------------------------------------------------------
    # Partition & Object Key Generation
    # ------------------------------------------------------------------------
    
    def generate_partition_key(
        self, 
        tournament_id: int, 
        season_id: int, 
        match_date: str
    ) -> str:
        """
        Generate hierarchical partition key for Hive-style partitioning
        
        Args:
            tournament_id: Tournament identifier (e.g., 202 for Ekstraklasa)
            season_id: Season identifier (e.g., 76477 for 2025/26)
            match_date: Match date in YYYY-MM-DD format
            
        Returns:
            Partition key: tournament_id=X/season_id=Y/date=YYYY-MM-DD
        """
        return f"tournament_id={tournament_id}/season_id={season_id}/date={match_date}"
    
    def generate_object_key(
        self, 
        data_type: str, 
        partition_key: str, 
        batch_id: str
    ) -> str:
        """
        Generate complete object key for bronze storage
        
        Args:
            data_type: Data type (e.g., 'matches', 'statistics')
            partition_key: Partition key from generate_partition_key()
            batch_id: Unique batch identifier from API client
            
        Returns:
            Object key: {data_type}/{partition_key}/batch_{batch_id}.ndjson
        """
        return f"{data_type}/{partition_key}/batch_{batch_id}.ndjson"
    
    # ------------------------------------------------------------------------
    # Data Preparation
    # ------------------------------------------------------------------------
    
    def prepare_ndjson_data(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Convert records to NDJSON format (newline-delimited JSON)
        
        Note: Metadata is stored separately in manifest file to avoid duplication
        
        Args:
            records: List of data records
            
        Returns:
            UTF-8 encoded NDJSON bytes
        """
        # Use compact JSON format to minimize storage
        ndjson_str = "\n".join(
            json.dumps(record, ensure_ascii=False, separators=(',', ':'))
            for record in records
        )
        return ndjson_str.encode('utf-8')
    
    def create_manifest(
        self, 
        object_key: str, 
        metadata: Dict[str, Any], 
        record_count: int,
        file_size: int
    ) -> Dict[str, Any]:
        """
        Create manifest file for batch tracking and lineage
        
        Args:
            object_key: NDJSON object key
            metadata: Batch metadata from API client
            record_count: Number of records in batch
            file_size: File size in bytes
            
        Returns:
            Manifest dictionary
        """
        return {
            "object_key": object_key,
            "metadata": metadata,
            "record_count": record_count,
            "file_size_bytes": file_size,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": self.SCHEMA_VERSION
        }
    
    # ------------------------------------------------------------------------
    # Core Storage Operations
    # ------------------------------------------------------------------------
    
    def store_batch(
        self,
        data_type: str,
        records: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        tournament_id: int,
        season_id: int,
        match_date: str,
        replace_partition: bool = False
    ) -> StorageResult:
        """
        Store batch of records in bronze layer with partitioning
        
        Args:
            data_type: Type of data (matches, statistics, etc.)
            records: List of data records
            metadata: Batch metadata from API client (includes batch_id)
            tournament_id: Tournament identifier
            season_id: Season identifier
            match_date: Match date (YYYY-MM-DD)
            replace_partition: Whether to delete existing partition data first
        
        Returns:
            StorageResult with operation details
        
        Example:
            >>> result = storage.store_batch(
            ...     data_type="matches",
            ...     records=[{...}, {...}],
            ...     metadata={"batch_id": "abc123", ...},
            ...     tournament_id=202,
            ...     season_id=76477,
            ...     match_date="2025-08-15"
            ... )
            >>> print(f"Stored {result.record_count} records")
        """
        # Generate storage paths
        partition_key = self.generate_partition_key(tournament_id, season_id, match_date)
        object_key = self.generate_object_key(data_type, partition_key, metadata["batch_id"])
        
        # Prepare data
        ndjson_data = self.prepare_ndjson_data(records)
        data_stream = BytesIO(ndjson_data)
        
        try:
            # Handle partition replacement if requested
            if replace_partition:
                self._delete_partition(data_type, partition_key)
            
            # Store main NDJSON file
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_key,
                data=data_stream,
                length=len(ndjson_data),
                content_type="application/x-ndjson",
                metadata={
                    "batch-id": metadata["batch_id"],
                    "ingestion-timestamp": metadata["ingestion_timestamp"],
                    "record-count": str(len(records))
                }
            )
            
            # Create and store manifest
            manifest = self.create_manifest(
                object_key, 
                metadata, 
                len(records), 
                len(ndjson_data)
            )
            manifest_key = object_key.replace(".ndjson", "_manifest.json")
            manifest_data = BytesIO(
                json.dumps(manifest, ensure_ascii=False).encode('utf-8')
            )
            
            try:
                self.client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=manifest_key,
                    data=manifest_data,
                    length=len(json.dumps(manifest).encode('utf-8')),
                    content_type="application/json"
                )
            except S3Error as manifest_error:
                logger.error(
                    f"⚠️ Manifest storage failed for {manifest_key}: {manifest_error}"
                )
                # Continue - data is stored, manifest can be recreated
            
            logger.info(
                f"✅ Stored batch {metadata['batch_id']} to {object_key} "
                f"({len(records)} records, {len(ndjson_data)} bytes)"
            )
            
            return StorageResult(
                success=True,
                object_key=object_key,
                manifest_key=manifest_key,
                partition_key=partition_key,
                record_count=len(records),
                file_size_bytes=len(ndjson_data),
                etag=result.etag
            )
            
        except S3Error as e:
            error_msg = f"Failed to store batch {metadata['batch_id']}: {e}"
            logger.error(f"❌ {error_msg}")
            
            return StorageResult(
                success=False,
                object_key=object_key,
                error=error_msg
            )
    
    def _delete_partition(self, data_type: str, partition_key: str) -> None:
        """
        Delete all objects in a partition (for replace_partition=True)
        
        Args:
            data_type: Data type (matches, statistics, etc.)
            partition_key: Partition key to delete
        """
        prefix = f"{data_type}/{partition_key}/"
        
        try:
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=prefix, 
                recursive=True
            )
            
            deleted_count = 0
            for obj in objects:
                self.client.remove_object(self.bucket_name, obj.object_name)
                deleted_count += 1
                logger.debug(f"Deleted {obj.object_name}")
            
            if deleted_count > 0:
                logger.info(f"🗑️ Deleted {deleted_count} objects from partition {prefix}")
                
        except S3Error as e:
            logger.error(f"Error deleting partition {prefix}: {e}")
    
    # ------------------------------------------------------------------------
    # Query & Metadata Operations
    # ------------------------------------------------------------------------
    
    def list_partitions(
        self, 
        data_type: str, 
        tournament_id: Optional[int] = None
    ) -> List[str]:
        """
        List available partitions for a data type
        
        Args:
            data_type: Data type to list (matches, statistics, etc.)
            tournament_id: Optional tournament filter
            
        Returns:
            List of partition keys (e.g., ["tournament_id=202/season_id=76477/date=2025-08-15"])
        """
        prefix = f"{data_type}/"
        if tournament_id:
            prefix += f"tournament_id={tournament_id}/"
        
        try:
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=prefix, 
                recursive=True
            )
            
            partitions = set()
            for obj in objects:
                # Extract partition from object path
                # Example: matches/tournament_id=202/season_id=76477/date=2025-08-15/batch_x.ndjson
                path = obj.object_name[len(f"{data_type}/"):]
                parts = path.split("/")
                
                # We need at least tournament_id, season_id, date
                if len(parts) >= 3:
                    partition = "/".join(parts[:3])
                    partitions.add(partition)
            
            return sorted(partitions)
            
        except S3Error as e:
            logger.error(f"Error listing partitions for {data_type}: {e}")
            return []
    
    def get_batch_manifest(self, object_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve batch manifest for given object key
        
        Args:
            object_key: NDJSON object key
            
        Returns:
            Manifest dictionary or None if not found
        """
        manifest_key = object_key.replace(".ndjson", "_manifest.json")
        
        try:
            response = self.client.get_object(self.bucket_name, manifest_key)
            return json.loads(response.read().decode('utf-8'))
            
        except S3Error as e:
            logger.error(f"Error retrieving manifest {manifest_key}: {e}")
            return None
    
    def get_partition_stats(self, data_type: str, partition_key: str) -> Dict[str, Any]:
        """
        Get statistics for a specific partition
        
        Args:
            data_type: Data type
            partition_key: Partition key
            
        Returns:
            Statistics dictionary with batch count, total records, etc.
        """
        prefix = f"{data_type}/{partition_key}/"
        
        try:
            objects = list(self.client.list_objects(
                self.bucket_name, 
                prefix=prefix, 
                recursive=True
            ))
            
            batch_files = [obj for obj in objects if obj.object_name.endswith('.ndjson')]
            manifest_files = [obj for obj in objects if obj.object_name.endswith('_manifest.json')]
            
            total_size = sum(obj.size for obj in batch_files)
            
            # Try to get record count from manifests
            total_records = 0
            for manifest_obj in manifest_files:
                manifest = self.get_batch_manifest(
                    manifest_obj.object_name.replace("_manifest.json", ".ndjson")
                )
                if manifest:
                    total_records += manifest.get("record_count", 0)
            
            return {
                "partition_key": partition_key,
                "batch_count": len(batch_files),
                "manifest_count": len(manifest_files),
                "total_records": total_records,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }
            
        except S3Error as e:
            logger.error(f"Error getting partition stats for {prefix}: {e}")
            return {
                "partition_key": partition_key,
                "error": str(e)
            }