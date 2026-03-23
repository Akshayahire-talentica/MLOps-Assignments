"""
S3 Storage Module for MLOps Data Pipeline
=========================================

Handles upload/download of raw and processed data to/from AWS S3.

Features:
- Upload raw data files to S3
- Download data from S3
- List and manage S3 objects
- Support for boto3 sessions with profiles
- Metadata tracking for S3 objects
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import hashlib

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logging.warning("boto3 not installed. S3 features disabled. Install with: pip install boto3")

logger = logging.getLogger(__name__)


class S3DataStorage:
    """
    Manages S3 storage for MLOps data pipeline.
    
    Usage:
        storage = S3DataStorage(bucket_name="mlops-movielens-poc", region="ap-south-1")
        storage.upload_file("data/raw/movies.dat", "raw/movies.dat")
        storage.download_file("raw/movies.dat", "data/raw/movies.dat")
    """
    
    def __init__(
        self,
        bucket_name: str,
        region: str = "ap-south-1",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize S3 storage client.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            profile: AWS profile name (optional, uses default if None)
            endpoint_url: Custom S3 endpoint (for LocalStack/MinIO testing)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for S3 storage. Install with: pip install boto3")
        
        self.bucket_name = bucket_name
        self.region = region
        self.profile = profile
        self.endpoint_url = endpoint_url
        
        # Sanitize AWS credentials from environment (remove newlines/whitespace)
        self._sanitize_aws_credentials()
        
        # Initialize boto3 session
        session_kwargs = {}
        if profile:
            session_kwargs['profile_name'] = profile
        if region:
            session_kwargs['region_name'] = region
            
        self.session = boto3.Session(**session_kwargs)
        
        # Create S3 client
        client_kwargs = {}
        if endpoint_url:
            client_kwargs['endpoint_url'] = endpoint_url
            
        self.s3_client = self.session.client('s3', **client_kwargs)
        self.s3_resource = self.session.resource('s3', **client_kwargs)
        
        logger.info(f"[S3] Initialized S3 storage: bucket={bucket_name}, region={region}")
    
    def _sanitize_aws_credentials(self):
        """
        Sanitize AWS credentials from environment variables.
        Removes newlines and whitespace that can cause invalid HTTP headers.
        """
        credentials_to_clean = [
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'AWS_SESSION_TOKEN'
        ]
        
        for key in credentials_to_clean:
            if key in os.environ and os.environ[key]:
                # Strip whitespace, newlines, and null bytes
                cleaned = os.environ[key].strip().replace('\n', '').replace('\r', '').replace('\0', '')
                if cleaned != os.environ[key]:
                    logger.warning(f"[S3] Cleaned whitespace from {key}")
                    os.environ[key] = cleaned
    
    def bucket_exists(self) -> bool:
        """Check if S3 bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            raise
    
    def create_bucket(self) -> bool:
        """
        Create S3 bucket if it doesn't exist.
        
        Returns:
            True if bucket created or already exists
        """
        try:
            if self.bucket_exists():
                logger.info(f"[S3] Bucket {self.bucket_name} already exists")
                return True
            
            if self.region == 'ap-south-1':
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            else:
                try:
                    # Try with LocationConstraint for all other regions
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                except:
                    # Fallback for regions that don't support LocationConstraint (e.g. N. Virginia)
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
            
            logger.info(f"[S3] Created bucket: {self.bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"[S3] Failed to create bucket: {e}")
            return False
    
    def upload_file(
        self,
        local_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
        storage_class: str = 'STANDARD'
    ) -> bool:
        """
        Upload file to S3.
        
        Args:
            local_path: Local file path
            s3_key: S3 object key (path in bucket)
            metadata: Optional metadata dict
            storage_class: S3 storage class (STANDARD, INTELLIGENT_TIERING, etc.)
        
        Returns:
            True if successful
        """
        try:
            local_path = Path(local_path)
            if not local_path.exists():
                logger.error(f"[S3] Local file not found: {local_path}")
                return False
            
            file_size = local_path.stat().st_size
            logger.info(f"[S3] Uploading {local_path} ({file_size:,} bytes) to s3://{self.bucket_name}/{s3_key}")
            
            extra_args = {'StorageClass': storage_class}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"[S3] ✓ Upload successful: {s3_key}")
            return True
            
        except NoCredentialsError:
            logger.error("[S3] AWS credentials not found. Run 'aws configure'")
            return False
        except ClientError as e:
            logger.error(f"[S3] Upload failed: {e}")
            return False
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download file from S3.
        
        Args:
            s3_key: S3 object key
            local_path: Local destination path
        
        Returns:
            True if successful
        """
        try:
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"[S3] Downloading s3://{self.bucket_name}/{s3_key} to {local_path}")
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_path)
            )
            
            logger.info(f"[S3] ✓ Download successful: {local_path}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"[S3] Object not found: {s3_key}")
            else:
                logger.error(f"[S3] Download failed: {e}")
            return False
    
    def list_objects(self, prefix: str = "") -> List[Dict]:
        """
        List objects in S3 bucket with given prefix.
        
        Args:
            prefix: S3 key prefix filter
        
        Returns:
            List of object metadata dicts
        """
        try:
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'].strip('"')
                        })
            
            logger.info(f"[S3] Found {len(objects)} objects with prefix '{prefix}'")
            return objects
            
        except ClientError as e:
            logger.error(f"[S3] List objects failed: {e}")
            return []
    
    def delete_object(self, s3_key: str) -> bool:
        """Delete object from S3."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"[S3] Deleted: {s3_key}")
            return True
        except ClientError as e:
            logger.error(f"[S3] Delete failed: {e}")
            return False
    
    def get_object_metadata(self, s3_key: str) -> Optional[Dict]:
        """Get metadata for S3 object."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag'].strip('"'),
                'content_type': response.get('ContentType', ''),
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            logger.error(f"[S3] Get metadata failed: {e}")
            return None
    
    def upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
        exclude_patterns: Optional[List[str]] = None
    ) -> Tuple[int, int]:
        """
        Upload entire directory to S3.
        
        Args:
            local_dir: Local directory path
            s3_prefix: S3 key prefix for uploaded files
            exclude_patterns: File patterns to exclude (e.g., ['*.pyc', '__pycache__'])
        
        Returns:
            (success_count, fail_count)
        """
        local_dir = Path(local_dir)
        if not local_dir.is_dir():
            logger.error(f"[S3] Directory not found: {local_dir}")
            return (0, 0)
        
        exclude_patterns = exclude_patterns or []
        success = 0
        failed = 0
        
        for file_path in local_dir.rglob('*'):
            if file_path.is_file():
                # Check exclude patterns
                if any(file_path.match(pattern) for pattern in exclude_patterns):
                    continue
                
                relative_path = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace('\\', '/')
                
                if self.upload_file(str(file_path), s3_key):
                    success += 1
                else:
                    failed += 1
        
        logger.info(f"[S3] Upload directory complete: {success} succeeded, {failed} failed")
        return (success, failed)
    
    def generate_presigned_url(
        self,
        s3_key: str,
        expiration: int = 3600,
        http_method: str = 'get_object'
    ) -> Optional[str]:
        """
        Generate presigned URL for temporary S3 access.
        
        Args:
            s3_key: S3 object key
            expiration: URL expiration in seconds (default 1 hour)
            http_method: 'get_object' or 'put_object'
        
        Returns:
            Presigned URL string or None
        """
        try:
            url = self.s3_client.generate_presigned_url(
                http_method,
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            logger.info(f"[S3] Generated presigned URL for {s3_key} (expires in {expiration}s)")
            return url
        except ClientError as e:
            logger.error(f"[S3] Presigned URL generation failed: {e}")
            return None


def compute_file_hash(file_path: str) -> str:
    """Compute MD5 hash of file (same as S3 ETag for simple uploads)."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
