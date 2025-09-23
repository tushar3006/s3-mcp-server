import logging
import os
from typing import List, Dict, Any, Optional
import aioboto3
import asyncio
from botocore.config import Config
import re

logger = logging.getLogger("s3_mcp_server")

AWS_PROFILE = os.environ.get('AWS_PROFILE')
AWS_REGION = os.environ.get('AWS_REGION')

logger.info(f"🔧 AWS Configuration - Profile: {AWS_PROFILE or 'None'}, Region: {AWS_REGION or 'None'}")

# Global session will be initialized when needed
s3_session = None


def initialize_s3_client():
    """Initialize S3 session when needed."""
    global s3_session
    
    if s3_session is not None:
        return  # Already initialized
    
    logger.info('🚀 Initializing S3 session...')
    
    # Try using AWS profile and fallback to region only
    if AWS_PROFILE and AWS_REGION:
        logger.info('✅ Using AWS credentials from environment variables')
        s3_session = aioboto3.Session(
            profile_name=AWS_PROFILE,
            region_name=AWS_REGION
        )
    elif AWS_REGION:
        logger.info('✅ Using AWS region from environment variables')
        s3_session = aioboto3.Session(region_name=AWS_REGION)
    else:
        logger.error('❌ No AWS credentials or region provided')
        raise ValueError('No AWS credentials or region provided')

    logger.info(f'✅ S3 session initialized with profile: {AWS_PROFILE or "default"}')


class S3Resource:
    """
    S3 Resource provider that handles interactions with AWS S3 buckets.
    Part of a collection of resource providers (S3, DynamoDB, etc.) for the MCP server.
    """

    def __init__(self, max_buckets: int = 5):
        """
        Initialize S3 resource provider
        Args:
            max_buckets: Maximum number of buckets to process (default: 5)
        """
        # Configure boto3 with retries and timeouts
        self.config = Config(
            retries=dict(
                max_attempts=3,
                mode='adaptive'
            ),
            connect_timeout=5,
            read_timeout=60,
            max_pool_connections=50
        )

        self.max_buckets = max_buckets
        self.configured_buckets = self._get_configured_buckets()

    def _get_configured_buckets(self) -> List[str]:
        """
        Get configured bucket names from environment variables.
        Format in .env file:
        S3_BUCKETS=bucket1,bucket2,bucket3
        or
        S3_BUCKET_1=bucket1
        S3_BUCKET_2=bucket2
        see env.example ############
        """
        # Try comma-separated list first
        bucket_list = os.getenv('S3_BUCKETS')
        if bucket_list:
            return [b.strip() for b in bucket_list.split(',')]

        buckets = []
        i = 1
        while True:
            bucket = os.getenv(f'S3_BUCKET_{i}')
            if not bucket:
                break
            buckets.append(bucket.strip())
            i += 1

        return buckets

    async def list_buckets(self, start_after: Optional[str] = None) -> List[dict]:
        """
        List S3 buckets using async client with pagination
        """
        async with s3_session.client('s3', region_name=AWS_REGION) as s3:
            if self.configured_buckets:
                # If buckets are configured, only return those
                response = await s3.list_buckets()
                all_buckets = response.get('Buckets', [])
                configured_bucket_list = [
                    bucket for bucket in all_buckets
                    if bucket['Name'] in self.configured_buckets
                ]

                #
                if start_after:
                    configured_bucket_list = [
                        b for b in configured_bucket_list
                        if b['Name'] > start_after
                    ]

                return configured_bucket_list[:self.max_buckets]
            else:
                # Default behavior if no buckets configured
                response = await s3.list_buckets()
                buckets = response.get('Buckets', [])

                if start_after:
                    buckets = [b for b in buckets if b['Name'] > start_after]

                return buckets[:self.max_buckets]

    async def list_objects(self, bucket_name: str, prefix: str = "", max_keys: int = 1000) -> List[dict]:
        """
        List objects in a specific bucket using async client with pagination
        Args:
            bucket_name: Name of the S3 bucket
            prefix: Object prefix for filtering
            max_keys: Maximum number of keys to return
        """
        #
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            logger.warning(f"⚠️  Bucket {bucket_name} not in configured bucket list")
            return []

        async with s3_session.client('s3', region_name=AWS_REGION) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            return response.get('Contents', [])

    async def get_object(self, bucket_name: str, key: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Get object from S3 using streaming to handle large files and PDFs reliably.
        The method reads the stream in chunks and concatenates them before returning.
        """
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            raise ValueError(f"Bucket {bucket_name} not in configured bucket list")

        attempt = 0
        last_exception = None
        chunk_size = 69 * 1024  # Using same chunk size as example for proven performance

        while attempt < max_retries:
            try:
                async with s3_session.client('s3',
                                               region_name=AWS_REGION,
                                               config=self.config) as s3:

                    # Get the object and its stream
                    response = await s3.get_object(Bucket=bucket_name, Key=key)
                    stream = response['Body']

                    # Read the entire stream in chunks
                    chunks = []
                    async for chunk in stream:
                        chunks.append(chunk)

                    # Replace the stream with the complete data
                    response['Body'] = b''.join(chunks)
                    return response

            except Exception as e:
                last_exception = e
                if 'NoSuchKey' in str(e):
                    raise

                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"🔄 Attempt {attempt} failed, retrying in {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                continue

        raise last_exception or Exception("Failed to get object after all retries")

    async def put_object(self, bucket_name: str, key: str, body: bytes, content_type: str = None, max_retries: int = 3) -> Dict[str, Any]:
        """
        Upload an object to S3 bucket - RESTRICTED to protex-intelligence-artifacts bucket only
        Args:
            bucket_name: Name of the S3 bucket (must be protex-intelligence-artifacts)
            key: Object key (file path) in the bucket
            body: File content as bytes
            content_type: MIME type of the content
            max_retries: Maximum number of retry attempts
        """
        # Strict validation: only allow uploads to protex-intelligence-artifacts bucket
        allowed_upload_bucket = "protex-intelligence-artifacts"
          # Validate bucket restriction
        if not re.search(r'protex[-_ ]?intelligence', key, re.IGNORECASE):
            raise ValueError(f"PutObject is restricted to keys containing 'protex-intelligence'. Provided key: {key}")
        
        # Strict validation: only allow specific file types
        allowed_extensions = {'.xls', '.xlsx', '.csv'}
        file_extension = None
        for ext in allowed_extensions:
            if key.lower().endswith(ext):
                file_extension = ext
                break
        
        if not file_extension:
            allowed_ext_str = ', '.join(allowed_extensions)
            raise ValueError(f"File type not allowed. Only {allowed_ext_str} files are permitted for upload. File: {key}")

        attempt = 0
        last_exception = None

        while attempt < max_retries:
            try:
                async with s3_session.client('s3',
                                               region_name=AWS_REGION,
                                               config=self.config) as s3:

                    put_args = {
                        'Bucket': bucket_name,
                        'Key': key,
                        'Body': body
                    }
                    
                    if content_type:
                        put_args['ContentType'] = content_type

                    response = await s3.put_object(**put_args)
                    logger.info(f"✅ Successfully uploaded {key} to {bucket_name}")
                    return response

            except Exception as e:
                last_exception = e
                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"🔄 Upload attempt {attempt} failed, retrying in {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                continue

        raise last_exception or Exception("Failed to upload object after all retries")

    def is_text_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension - now restricted to allowed upload types"""
        # Only CSV files are considered text files among our allowed types
        # .xls and .xlsx are binary Excel formats
        text_extensions = {'.csv'}
        return any(key.lower().endswith(ext) for ext in text_extensions)