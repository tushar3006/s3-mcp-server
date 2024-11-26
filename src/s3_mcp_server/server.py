import asyncio
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server, McpError
from pydantic import AnyUrl
import mcp.server.stdio
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any, Optional
from mcp.types import Resource, LoggingLevel, EmptyResult
import aioboto3


class S3ProtocolServer:
    def __init__(self, region_name: str = None, profile_name: str = None, max_buckets: int = 5):
        """
        Initialize aioboto3 session
        Args:
            region_name: AWS region name
            profile_name: AWS profile name
            max_buckets: Maximum number of buckets to process (default: 5)
        """
        self.session = aioboto3.Session(profile_name="bedrock")
        self.region_name = region_name
        self.max_buckets = max_buckets

    async def list_buckets(self, start_after: Optional[str] = None) -> List[dict]:
        """
        List S3 buckets using async client with pagination
        Args:
            start_after: Start listing after this bucket name
        """
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_buckets()
            buckets = response.get('Buckets', [])

            # Filter buckets if start_after is provided
            if start_after:
                buckets = [b for b in buckets if b['Name'] > start_after]

            # Limit the number of buckets
            return buckets[:self.max_buckets]

    async def list_objects(self, bucket_name: str, prefix: str = "", max_keys: int = 1000) -> List[dict]:
        """
        List objects in a specific bucket using async client with pagination
        Args:
            bucket_name: Name of the S3 bucket
            prefix: Object prefix for filtering
            max_keys: Maximum number of keys to return
        """
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            return response.get('Contents', [])

    async def get_object(self, bucket_name: str, key: str) -> Dict[str, Any]:
        """Get object from S3 using async client"""
        async with self.session.client('s3', region_name=self.region_name) as s3:
            return await s3.get_object(Bucket=bucket_name, Key=key)

    def is_text_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension"""
        text_extensions = {
            '.txt', '.log', '.json', '.xml', '.yml', '.yaml', '.md',
            '.csv', '.ini', '.conf', '.py', '.js', '.html', '.css',
            '.sh', '.bash', '.cfg', '.properties'
        }
        return any(key.lower().endswith(ext) for ext in text_extensions)


# Initialize server
server = Server("s3_service")

# Load environment variables
load_dotenv()

# Initialize protocol server with configurable max_buckets
s3_server = S3ProtocolServer(region_name="us-east-1", max_buckets=5)

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("s3-mcp-server")

@server.set_logging_level()
async def set_logging_level(level: LoggingLevel) -> EmptyResult:
    logger.setLevel(level.lower())
    await server.request_context.session.send_log_message(
        level="info",
        data=f"Log level set to {level}",
        logger="s3-mcp-server"
    )
    return EmptyResult()

@server.list_resources()
async def list_resources(start_after: Optional[str] = None) -> List[Resource]:
    """
    List S3 buckets and their contents as resources with pagination
    Args:
        start_after: Start listing after this bucket name
    """
    resources = []
    logger.debug("Starting to list resources")

    try:
        # Get limited number of buckets
        buckets = await s3_server.list_buckets(start_after)
        logger.debug(f"Processing {len(buckets)} buckets (max: {s3_server.max_buckets})")

        # Process buckets concurrently with semaphore to limit concurrent operations
        async def process_bucket(bucket):
            bucket_name = bucket['Name']
            logger.debug(f"Processing bucket: {bucket_name}")

            try:
                # List objects in the bucket with a reasonable limit
                objects = await s3_server.list_objects(bucket_name, max_keys=1000)

                for obj in objects:
                    if 'Key' in obj and not obj['Key'].endswith('/'):
                        object_key = obj['Key']
                        mime_type = "text/plain" if s3_server.is_text_file(object_key) else "application/json"

                        resource = Resource(
                            uri=f"s3://{bucket_name}/{object_key}",
                            name=object_key,
                            mimeType=mime_type
                        )
                        resources.append(resource)
                        logger.debug(f"Added resource: {resource.uri}")

            except Exception as e:
                logger.error(f"Error listing objects in bucket {bucket_name}: {str(e)}")

        # Use semaphore to limit concurrent bucket processing
        semaphore = asyncio.Semaphore(3)  # Limit concurrent bucket processing
        async def process_bucket_with_semaphore(bucket):
            async with semaphore:
                await process_bucket(bucket)

        # Process buckets concurrently
        await asyncio.gather(*[process_bucket_with_semaphore(bucket) for bucket in buckets])

    except Exception as e:
        logger.error(f"Error listing buckets: {str(e)}")
        raise

    logger.info(f"Returning {len(resources)} resources")
    return resources


async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="s3-mcp-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )