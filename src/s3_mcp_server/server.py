import asyncio
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server, McpError
from pydantic import AnyUrl
import mcp.server.stdio
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any
from mcp.types import Resource
import aioboto3

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("s3-server")

class S3ProtocolServer:
    def __init__(self, region_name: str = None, profile_name: str = None):
        """Initialize aioboto3 session"""
        self.session = aioboto3.Session(profile_name="bedrock")
        self.region_name = region_name

    async def list_buckets(self) -> List[dict]:
        """List all S3 buckets using async client"""
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_buckets()
            return response.get('Buckets', [])

    async def list_objects(self, bucket_name: str, prefix: str = "") -> List[dict]:
        """List objects in a specific bucket using async client"""
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
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

# Initialize protocol server
s3_server = S3ProtocolServer(region_name="us-east-1")

@server.list_resources()
async def list_resources() -> List[Resource]:
    """List all S3 buckets and their contents as resources"""
    resources = []
    logger.debug("Starting to list resources")

    try:
        # Get all buckets
        buckets = await s3_server.list_buckets()
        logger.debug(f"Found {len(buckets)} buckets")

        for bucket in buckets:
            bucket_name = bucket['Name']
            logger.debug(f"Processing bucket: {bucket_name}")

            # List objects in the bucket
            try:
                objects = await s3_server.list_objects(bucket_name)

                for obj in objects:
                    if 'Key' in obj:
                        object_key = obj['Key']
                        # Skip if it's a "directory"
                        if object_key.endswith('/'):
                            continue

                        # Determine mime type based on content type
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
                McpError(

                )
                continue

    except Exception as e:
        logger.error(f"Error listing buckets: {str(e)}")
        raise

    logger.debug(f"Returning {len(resources)} resources")
    return resources

@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read content from an S3 resource"""
    uri_str = str(uri)
    logger.debug(f"Reading resource: {uri_str}")

    if not uri_str.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    # Parse the S3 URI
    path = uri_str[5:]  # Remove "s3://"
    parts = path.split("/", 1)

    if len(parts) < 2:
        raise ValueError("Invalid S3 URI format")

    bucket_name = parts[0]
    key = parts[1]

    try:
        response = await s3_server.get_object(bucket_name, key)

        # Get the streaming body
        async with response['Body'] as stream:
            data = await stream.read()

        if s3_server.is_text_file(key):
            return data.decode('utf-8')
        else:
            import base64
            return base64.b64encode(data).decode('utf-8')

    except Exception as e:
        logger.error(f"Error reading object {key} from bucket {bucket_name}: {str(e)}")
        raise ValueError(f"Error reading resource: {str(e)}")

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