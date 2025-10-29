#!/usr/bin/env python3
"""
MinIO Storage Manager for Bronze Layer with partitioning
"""

import json
import logging
from datetime import datetime
from io import BytesIO
from typing import Dict, Any, List, Optional
import hashlib

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class BronzeStorageManager:
    """
    Manages bronze layer storage with:
    - Deterministic partitioning scheme
    - NDJSON format for streaming
    - Metadata tracking
    - UPSERT/replace-partition capability
    """
    
    def __init__(self, minio_client: Minio, bucket_name: str = "bronze"):
        self.client = minio_client
        self.bucket_name = bucket_name
        
    def _ensure_bucket_exists(self):
        """Ensure bronze bucket exists"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket {self.bucket_name}: {e}")
            raise
    
    def generate_partition_key(
        self, 
        tournament_id: int, 
        season_id: int, 
        match_date: str, 
        match_id: Optional[int] = None
    ) -> str:
        """
        Generate hierarchical partition key for Hive-style partitioning
        Format: tournament_id=XX/season_id=YY/date=YYYY-MM-DD[/match_id=ZZ]
        """
        parts = [
            f"tournament_id={tournament_id}",
            f"season_id={season_id}",
            f"date={match_date}"
        ]
        
        if match_id is not None:
            parts.append(f"match_id={match_id}")
            
        return "/".join(parts)
    
    def generate_object_key(
        self, 
        data_type: str, 
        partition_key: str, 
        batch_id: str,
        file_format: str = "ndjson"
    ) -> str:
        """
        Generate complete object key for bronze storage
        Format: {data_type}/{partition_key}/batch_{batch_id}.{format}
        """
        return f"{data_type}/{partition_key}/batch_{batch_id}.{file_format}"
    
    def prepare_ndjson_data(self, records: List[Dict[str, Any]], metadata: Dict[str, Any]) -> bytes:
        """
        Convert records to NDJSON format with metadata injection
        Each line is a complete JSON object with embedded metadata
        """
        lines = []
        
        for record in records:
            # Inject metadata into each record
            enriched_record = {
                **record,
                "_metadata": metadata
            }
            lines.append(json.dumps(enriched_record, ensure_ascii=False))
        
        return "\n".join(lines).encode('utf-8')
    
    def create_manifest(
        self, 
        object_key: str, 
        metadata: Dict[str, Any], 
        record_count: int,
        file_size: int
    ) -> Dict[str, Any]:
        """Create manifest file for batch tracking"""
        return {
            "object_key": object_key,
            "metadata": metadata,
            "record_count": record_count,
            "file_size_bytes": file_size,
            "created_at": datetime.utcnow().isoformat(),
            "schema_version": "1.0"
        }
    
    async def store_batch(
        self,
        data_type: str,
        records: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        tournament_id: int,
        season_id: int,
        match_date: str,
        match_id: Optional[int] = None,
        replace_partition: bool = False
    ) -> Dict[str, Any]:
        """
        Store batch of records in bronze layer with partitioning
        
        Args:
            data_type: Type of data (matches, lineups, statistics, etc.)
            records: List of data records
            metadata: Batch metadata from API client
            tournament_id: Tournament identifier
            season_id: Season identifier
            match_date: Match date (YYYY-MM-DD)
            match_id: Optional match identifier for match-level partitioning
            replace_partition: Whether to replace existing partition data
        
        Returns:
            Storage result with object key and metadata
        """
        self._ensure_bucket_exists()
        
        # Generate partition and object keys
        partition_key = self.generate_partition_key(tournament_id, season_id, match_date, match_id)
        object_key = self.generate_object_key(data_type, partition_key, metadata["batch_id"])
        
        # Prepare NDJSON data
        ndjson_data = self.prepare_ndjson_data(records, metadata)
        data_stream = BytesIO(ndjson_data)
        
        try:
            # Handle partition replacement if requested
            if replace_partition:
                await self._delete_partition(data_type, partition_key)
            
            # Store main data file
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_key,
                data=data_stream,
                length=len(ndjson_data),
                content_type="application/x-ndjson",
                metadata={
                    "batch-id": metadata["batch_id"],
                    "ingestion-timestamp": metadata["ingestion_timestamp"],
                    "source": metadata["source"],
                    "record-count": str(len(records))
                }
            )
            
            # Create and store manifest
            manifest = self.create_manifest(object_key, metadata, len(records), len(ndjson_data))
            manifest_key = object_key.replace(".ndjson", "_manifest.json")
            manifest_data = BytesIO(json.dumps(manifest, ensure_ascii=False).encode('utf-8'))
            
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=manifest_key,
                data=manifest_data,
                length=len(json.dumps(manifest).encode('utf-8')),
                content_type="application/json"
            )
            
            logger.info(f"Stored batch {metadata['batch_id']} to {object_key} ({len(records)} records)")
            
            return {
                "success": True,
                "object_key": object_key,
                "manifest_key": manifest_key,
                "partition_key": partition_key,
                "record_count": len(records),
                "file_size_bytes": len(ndjson_data),
                "etag": result.etag
            }
            
        except S3Error as e:
            logger.error(f"Failed to store batch {metadata['batch_id']}: {e}")
            return {
                "success": False,
                "error": str(e),
                "object_key": object_key
            }
    
    async def _delete_partition(self, data_type: str, partition_key: str):
        """Delete all objects in a partition (for replacement)"""
        prefix = f"{data_type}/{partition_key}/"
        
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            for obj in objects:
                self.client.remove_object(self.bucket_name, obj.object_name)
                logger.debug(f"Deleted {obj.object_name}")
                
        except S3Error as e:
            logger.error(f"Error deleting partition {prefix}: {e}")
    
    def list_partitions(self, data_type: str, tournament_id: Optional[int] = None) -> List[str]:
        """List available partitions for a data type"""
        prefix = f"{data_type}/"
        if tournament_id:
            prefix += f"tournament_id={tournament_id}/"
        
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=False)
            partitions = set()
            
            for obj in objects:
                # Extract partition path from object name
                parts = obj.object_name.replace(prefix, "").split("/")
                if len(parts) >= 3:  # tournament_id/season_id/date
                    partition = "/".join(parts[:3])
                    partitions.add(partition)
            
            return sorted(list(partitions))
            
        except S3Error as e:
            logger.error(f"Error listing partitions for {data_type}: {e}")
            return []
    
    def get_batch_manifest(self, object_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve batch manifest for given object key"""
        manifest_key = object_key.replace(".ndjson", "_manifest.json")
        
        try:
            response = self.client.get_object(self.bucket_name, manifest_key)
            return json.loads(response.read().decode('utf-8'))
            
        except S3Error as e:
            logger.error(f"Error retrieving manifest {manifest_key}: {e}")
            return None
