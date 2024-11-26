import logging
import os
from typing import List, Dict, Any, Optional
import aioboto3
import asyncio
from botocore.config import Config

logger = logging.getLogger("s3_mcp_server")

class S3Resource:
    """
    S3 Resource provider that handles interactions with AWS S3 buckets.
    Part of a collection of resource providers (S3, DynamoDB, etc.) for the MCP server.
    """

    def __init__(self, region_name: str = None, profile_name: str = None, max_buckets: int = 5):
        """
              Initialize S3 resource provider
              Args:
                  region_name: AWS region name
                  profile_name: AWS profile name
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

        self.session = aioboto3.Session(profile_name="bedrock")
        self.region_name = region_name
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
        async with self.session.client('s3', region_name=self.region_name) as s3:
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
            logger.warning(f"Bucket {bucket_name} not in configured bucket list")
            return []

        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            return response.get('Contents', [])

    async def get_object(self, bucket_name: str, key: str, max_retries: int = 3) -> Dict[str, Any]:
        """
              Get object from S3 using async client with retry logic
              Args:
                  bucket_name: Name of the S3 bucket
                  key: Object key
                  max_retries: Maximum number of retry attempts
              """
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            raise ValueError(f"Bucket {bucket_name} not in configured bucket list")

        attempt = 0
        last_exception = None

        while attempt < max_retries:
            try:
                async with self.session.client('s3',
                                               region_name=self.region_name,
                                               config=self.config) as s3:
                    # check file  size
                    head_response = await s3.head_object(Bucket=bucket_name, Key=key)
                    size = head_response.get('ContentLength', 0)

                    if size > 10 * 1024 * 1024:  # If file is larger than 10MB
                        logger.debug(f"Large file detected ({size/1024/1024:.2f}MB), using chunked download")
                        # For large files, stream the content
                        response = await s3.get_object(Bucket=bucket_name, Key=key)
                        chunks = []
                        async with response['Body'] as stream:
                            while True:
                                chunk = await stream.read(8192)  # Read in 8KB chunks
                                if not chunk:
                                    break
                                chunks.append(chunk)
                        response['Body'] = b''.join(chunks)
                        return response
                    else:
                        # smaller files, download directly
                        return await s3.get_object(Bucket=bucket_name, Key=key)

            except Exception as e:
                last_exception = e
                if 'NoSuchKey' in str(e):
                    # Don't retry if the key doesn't exist
                    raise

                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Attempt {attempt} failed, retrying in {wait_time} seconds: {str(e)}")
                    await asyncio.sleep(wait_time)
                continue

        # If we've exhausted all retries, raise the last exception
        raise last_exception or Exception("Failed to get object after all retries")

    def is_text_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension"""
        text_extensions = {
            '.txt', '.log', '.json', '.xml', '.yml', '.yaml', '.md',
            '.csv', '.ini', '.conf', '.py', '.js', '.html', '.css',
            '.sh', '.bash', '.cfg', '.properties'
        }
        return any(key.lower().endswith(ext) for ext in text_extensions)