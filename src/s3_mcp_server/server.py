import asyncio
from importlib.resources import contents

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server, McpError
import mcp.server.stdio
from dotenv import load_dotenv
import logging
import os
from typing import List, Optional, Dict
from mcp.types import Resource, LoggingLevel, EmptyResult, Tool, TextContent, ImageContent, EmbeddedResource, BlobResourceContents, ReadResourceResult

from .resources.s3_resource import S3Resource, initialize_s3_client
from pydantic import AnyUrl

import base64
import mimetypes
import re

# Initialize server
server = Server("s3_service")

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("mcp_s3_server")

# Get max buckets from environment or use default
max_buckets = int(os.getenv('S3_MAX_BUCKETS', '5'))

# Initialize S3 client and resource
initialize_s3_client()
s3_resource = S3Resource(
    max_buckets=max_buckets
)

@server.set_logging_level()
async def set_logging_level(level: LoggingLevel) -> EmptyResult:
    logger.setLevel(level.lower())
    await server.request_context.session.send_log_message(
        level="info",
        data=f"Log level set to {level}",
        logger="mcp_s3_server"
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
    logger.debug(f"Configured buckets: {s3_resource.configured_buckets}")

    try:
        # Get limited number of buckets
        buckets = await s3_resource.list_buckets(start_after)
        logger.debug(f"Processing {len(buckets)} buckets (max: {s3_resource.max_buckets})")

        # limit concurrent operations
        async def process_bucket(bucket):
            bucket_name = bucket['Name']
            logger.debug(f"Processing bucket: {bucket_name}")

            try:
                # List objects in the bucket with a reasonable limit
                objects = await s3_resource.list_objects(bucket_name, max_keys=1000)

                for obj in objects:
                    if 'Key' in obj and not obj['Key'].endswith('/'):
                        object_key = obj['Key']
                        mime_type = "text/plain" if s3_resource.is_text_file(object_key) else "text/markdown"

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



@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """
    Read content from an S3 resource and return structured response

    Returns:
        Dict containing 'contents' list with uri, mimeType, and text for each resource
    """
    uri_str = str(uri)
    logger.debug(f"Reading resource: {uri_str}")

    if not uri_str.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    # Parse the S3 URI
    from urllib.parse import unquote
    path = uri_str[5:]  # Remove "s3://"
    path = unquote(path)  # Decode URL-encoded characters
    parts = path.split("/", 1)

    if len(parts) < 2:
        raise ValueError("Invalid S3 URI format")

    bucket_name = parts[0]
    key = parts[1]

    logger.debug(f"Attempting to read - Bucket: {bucket_name}, Key: {key}")

    try:
        response = await s3_resource.get_object(bucket_name, key)
        content_type = response.get("ContentType", "")
        logger.debug(f"Read MIMETYPE response: {content_type}")

        # Content type mapping for specific file types
        content_type_mapping = {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "application/markdown",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "application/csv",
            "application/vnd.ms-excel": "application/csv"
        }

        # Check if content type needs to be modified
        export_mime_type = content_type_mapping.get(content_type, content_type)
        logger.debug(f"Export MIME type: {export_mime_type}")

        if 'Body' in response:
            if isinstance(response['Body'], bytes):
                data = response['Body']
            else:
                # Handle streaming response
                async with response['Body'] as stream:
                    data = await stream.read()

            # Process the data based on file type
            if s3_resource.is_text_file(key):
                # text_content = data.decode('utf-8')
                text_content = base64.b64encode(data).decode('utf-8')

                return text_content
            else:
                text_content = str(base64.b64encode(data))

                result = ReadResourceResult(
                    contents=[
                        BlobResourceContents(
                            blob=text_content,
                            uri=uri_str,
                            mimeType=export_mime_type
                        )
                    ]
                )

                logger.debug(result)

                return text_content



        else:
            raise ValueError("No data in response body")

    except Exception as e:
        logger.error(f"Error reading object {key} from bucket {bucket_name}: {str(e)}")
        if 'NoSuchKey' in str(e):
            try:
                # List similar objects to help debugging
                objects = await s3_resource.list_objects(bucket_name, prefix=key.split('/')[0])
                similar_objects = [obj['Key'] for obj in objects if 'Key' in obj]
                logger.debug(f"Similar objects found: {similar_objects}")
            except Exception as list_err:
                logger.error(f"Error listing similar objects: {str(list_err)}")
        raise ValueError(f"Error reading resource: {str(e)}")


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="ListBuckets", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
            description="Returns a list of all buckets owned by the authenticated sender of the request. To grant IAM permission to use this operation, you must add the s3:ListAllMyBuckets policy action.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ContinuationToken": {"type": "string", "description": "ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results. Length Constraints: Minimum length of 0. Maximum length of 1024."},
                    "MaxBuckets": {"type": "integer", "description": "Maximum number of buckets to be returned in response. When the number is more than the count of buckets that are owned by an AWS account, return all the buckets in response. Valid Range: Minimum value of 1. Maximum value of 10000."},
                },
                "required": [],
            },
        ),
        Tool(
            name="ListObjectsV2", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
            description="Returns some or all (up to 1,000) of the objects in a bucket with each request. You can use the request parameters as selection criteria to return a subset of the objects in a bucket. To get a list of your buckets, see ListBuckets.",
            inputSchema={
                "type": "object",
                "properties": {
                    "Bucket": {"type": "string", "description": "When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "ContinuationToken": {"type": "string", "description": "ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results."},
                    "EncodingType": {"type": "string", "description": "Encoding type used by Amazon S3 to encode the object keys in the response. Responses are encoded only in UTF-8. An object key can contain any Unicode character. However, the XML 1.0 parser can't parse certain characters, such as characters with an ASCII value from 0 to 10. For characters that aren't supported in XML 1.0, you can add this parameter to request that Amazon S3 encode the keys in the response."},
                    "FetchOwner": {"type": "boolean", "description": "The owner field is not present in ListObjectsV2 by default. If you want to return the owner field with each key in the result, then set the FetchOwner field to true."},
                    "MaxKeys": {"type": "integer", "description": "Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."},
                    "Prefix": {"type": "string", "description": "Limits the response to keys that begin with the specified prefix."},
                    "StartAfter": {"type": "string", "description": "StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this specified key. StartAfter can be any key in the bucket."}
                },
                "required": ["Bucket"],
            },
        ),
        Tool(
            name="GetObject", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
            description="Retrieves an object from Amazon S3. In the GetObject request, specify the full key name for the object. General purpose buckets - Both the virtual-hosted-style requests and the path-style requests are supported. For a virtual hosted-style request example, if you have the object photos/2006/February/sample.jpg, specify the object key name as /photos/2006/February/sample.jpg. For a path-style request example, if you have the object photos/2006/February/sample.jpg in the bucket named examplebucket, specify the object key name as /examplebucket/photos/2006/February/sample.jpg. Directory buckets - Only virtual-hosted-style requests are supported. For a virtual hosted-style request example, if you have the object photos/2006/February/sample.jpg in the bucket named examplebucket--use1-az5--x-s3, specify the object key name as /photos/2006/February/sample.jpg. Also, when you make requests to this API operation, your requests are sent to the Zonal endpoint. These endpoints support virtual-hosted-style requests in the format https://bucket_name.s3express-az_id.region.amazonaws.com/key-name . Path-style requests are not supported.",
            inputSchema={
                "type": "object",
                "properties": {
                    "Bucket": {"type": "string", "description": "Directory buckets - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "Key": {"type": "string", "description": "Key of the object to get. Length Constraints: Minimum length of 1."},
                    "Range": {"type": "string", "description": "Downloads the specified byte range of an object."},
                    "VersionId": {"type": "string", "description": "Version ID used to reference a specific version of the object. By default, the GetObject operation returns the current version of an object. To return a different version, use the versionId subresource."},
                    "PartNumber": {"type": "integer", "description": "Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object."},
                },
                "required": ["Bucket", "Key"]
            }
        ),
        Tool(
            name="PutObject", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
            description="Adds spreadsheet and Excel files to the protex-intelligence-artifacts bucket ONLY. This tool is restricted to upload ONLY .xls, .xlsx, and .csv files to the 'protex-intelligence-artifacts' bucket for security purposes. All other file types will be rejected. You must have WRITE permissions on the bucket to add an object to it. Supports both local file paths and direct base64 content. Provide either 'Body' (for base64 content) OR 'FilePath' (for local file upload).",
            inputSchema={
                "type": "object",
                "properties": {
                    "Bucket": {"type": "string", "description": "The bucket name to which the PUT action was initiated. MUST be 'protex-intelligence-artifacts' - uploads to other buckets will be rejected.", "enum": ["protex-intelligence-artifacts"]},
                    "Key": {"type": "string", "description": "Object key for which the PUT action was initiated. This will be the file path/name in the bucket. MUST end with .xls, .xlsx, or .csv extension - other file types will be rejected. Length Constraints: Minimum length of 1."},
                    "Body": {"type": "string", "description": "Object data as base64 encoded string. For CSV files, you can also provide the raw text content. For Excel files (.xls, .xlsx), content must be base64 encoded. Optional if FilePath is provided."},
                    "FilePath": {"type": "string", "description": "Local file path to upload to S3. If provided, this takes precedence over Body parameter. The file will be read from the local filesystem and uploaded. Must be a valid path to a .xls, .xlsx, or .csv file. Optional if Body is provided."},
                    "ContentType": {"type": "string", "description": "A standard MIME type describing the format of the contents. Common types: 'text/csv' for CSV, 'application/vnd.ms-excel' for .xls, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' for .xlsx. If not specified, the system will infer it from the file extension."},
                },
                "required": ["Bucket", "Key"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[TextContent | ImageContent | EmbeddedResource]:
    try:
        match name:
            case "ListBuckets":
                buckets = await s3_resource.list_buckets()
                return [
                    TextContent(
                        type="text",
                        text=str(buckets)
                    )
                ]
            case "ListObjectsV2":
                bucket = arguments.get("Bucket")
                prefix = arguments.get("Prefix", "")
                max_keys = arguments.get("MaxKeys", 1000)
                objects = await s3_resource.list_objects(bucket, prefix, max_keys)
                return [
                    TextContent(
                        type="text",
                        text=str(objects)
                    )
                ]
            case "GetObject":
                bucket = arguments.get("Bucket")
                key = arguments.get("Key")
                response = await s3_resource.get_object(bucket, key)
                if isinstance(response['Body'], bytes):
                    file_content = response['Body'].decode('utf-8')
                else:
                    file_content = str(response['Body'])
                return [
                    TextContent(
                        type="text",
                        text=file_content
                    )
                ]
            case "PutObject":
                bucket = arguments.get("Bucket")
                key = arguments.get("Key")
                body_content = arguments.get("Body")
                file_path = arguments.get("FilePath")
                content_type = arguments.get("ContentType")
                
                # Validate bucket restriction
                if bucket != "protex-intelligence-artifacts":
                    raise ValueError("PutObject is restricted to 'protex-intelligence-artifacts' bucket only")
                
                # Validate file extension
                allowed_extensions = ['.csv', '.xls', '.xlsx']
                if not any(key.lower().endswith(ext) for ext in allowed_extensions):
                    raise ValueError(f"Only {', '.join(allowed_extensions)} files are allowed. Provided file: {key}")
                
                # Determine if content should be treated as base64 or raw text
                is_excel_file = key.lower().endswith(('.xls', '.xlsx'))
                
                # Handle file path vs body content
                if file_path:
                    # Read from local file system
                    logger.debug(f"Reading file from local path: {file_path}")
                    
                    # Validate file path and extension
                    if not os.path.exists(file_path):
                        raise ValueError(f"File not found: {file_path}")
                    
                    if not any(file_path.lower().endswith(ext) for ext in allowed_extensions):
                        raise ValueError(f"Only {', '.join(allowed_extensions)} files are allowed. Provided file: {file_path}")
                    
                    try:
                        # Read file as binary
                        with open(file_path, 'rb') as file:
                            body_bytes = file.read()
                        
                        logger.debug(f"Successfully read file {file_path} ({len(body_bytes)} bytes)")
                        
                        # Infer content type from file extension if not provided
                        if not content_type:
                            if file_path.lower().endswith('.csv'):
                                content_type = "text/csv"
                            elif file_path.lower().endswith('.xls'):
                                content_type = "application/vnd.ms-excel"
                            elif file_path.lower().endswith('.xlsx'):
                                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        
                    except Exception as e:
                        raise ValueError(f"Failed to read file {file_path}: {str(e)}")
                
                elif body_content:
                    # Handle body content (base64 or raw text)
                    if is_excel_file:
                        # Excel files must be base64 encoded
                        try:
                            # Validate base64 content
                            if not body_content:
                                raise ValueError("Body content cannot be empty for Excel files")
                            
                            # Clean base64 string (remove whitespace, newlines)
                            clean_body = ''.join(body_content.split())
                            
                            # More flexible base64 validation - just check if it can be decoded
                            # The previous regex was too strict for some valid base64 content
                            try:
                                # Test decode to validate base64 format
                                body_bytes = base64.b64decode(clean_body, validate=True)
                                logger.debug(f"Successfully decoded base64 Excel content for {key} ({len(body_bytes)} bytes)")
                            except Exception as decode_error:
                                # If strict validation fails, try without validation (more permissive)
                                try:
                                    body_bytes = base64.b64decode(clean_body)
                                    logger.debug(f"Successfully decoded base64 Excel content for {key} with permissive mode ({len(body_bytes)} bytes)")
                                except Exception:
                                    raise ValueError(f"Invalid base64 format for Excel file - content cannot be decoded")
                            
                        except ValueError:
                            # Re-raise ValueError as-is (our custom messages)
                            raise
                        except Exception as e:
                            raise ValueError(f"Failed to process base64 content for Excel file {key}: {str(e)}")
                            
                    else:
                        # CSV files - try base64 first, then raw text
                        try:
                            # Try to decode as base64 first
                            clean_body = ''.join(body_content.split())
                            body_bytes = base64.b64decode(clean_body, validate=True)
                            logger.debug(f"Successfully decoded base64 CSV content for {key}")
                        except Exception:
                            # If base64 decode fails, treat as raw text for CSV
                            body_bytes = body_content.encode('utf-8')
                            logger.debug(f"Treating CSV content as raw text for {key}")
                    
                    # Infer content type if not provided
                    if not content_type:
                        if key.lower().endswith('.csv'):
                            content_type = "text/csv"
                        elif key.lower().endswith('.xls'):
                            content_type = "application/vnd.ms-excel"
                        elif key.lower().endswith('.xlsx'):
                            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                
                else:
                    raise ValueError("Either 'Body' or 'FilePath' parameter must be provided")
                
                logger.debug(f"Uploading {key} to {bucket} with content type: {content_type} ({len(body_bytes)} bytes)")
                
                response = await s3_resource.put_object(bucket, key, body_bytes, content_type)
                
                source_info = f"from file: {file_path}" if file_path else "from body content"
                return [
                    TextContent(
                        type="text",
                        text=f"Successfully uploaded {key} to bucket {bucket} {source_info}. ETag: {response.get('ETag', 'N/A')}, Size: {len(body_bytes)} bytes"
                    )
                ]
    except Exception as error:
        logger.error(f"🚨 Tool call error: {str(error)}")
        return [
            TextContent(
                type="text",
                text=f"Error: {str(error)}"
            )
        ]

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

if __name__ == "__main__":
    asyncio.run(main())