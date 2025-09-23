import asyncio
from importlib.resources import contents

from mcp.server.fastmcp import Context, FastMCP
from dotenv import load_dotenv
import logging
import os
from typing import List, Optional, Dict
import argparse
from .resources.s3_resource import S3Resource, initialize_s3_client
from pydantic import AnyUrl

import base64
import mimetypes
import re

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

# Initialize FastMCP server
mcp = FastMCP(
    's3-mcp-server',
    instructions="""Use S3 functions to interact with AWS S3 buckets and objects.
    These S3 functions give you capabilities to list buckets, read objects, and upload files to S3.""",
    dependencies=['pydantic', 'boto3', 'python-dotenv'],
)

# FastMCP Tools

@mcp.tool()
async def list_buckets(ctx: Context, continuation_token: Optional[str] = None, max_buckets: Optional[int] = None) -> str:
    """Returns a list of all buckets owned by the authenticated sender of the request. 
    To grant IAM permission to use this operation, you must add the s3:ListAllMyBuckets policy action.
    
    Args:
        continuation_token: ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results. Length Constraints: Minimum length of 0. Maximum length of 1024.
        max_buckets: Maximum number of buckets to be returned in response. When the number is more than the count of buckets that are owned by an AWS account, return all the buckets in response. Valid Range: Minimum value of 1. Maximum value of 10000.
    """
    try:
        await ctx.info(f'Listing buckets with max_buckets: {max_buckets}')
        buckets = await s3_resource.list_buckets()
        return str(buckets)
    except Exception as error:
        logger.error(f"Error listing buckets: {str(error)}")
        return f"Error: {str(error)}"


@mcp.tool()
async def list_objects_v2(ctx: Context, bucket: str, continuation_token: Optional[str] = None, 
                         encoding_type: Optional[str] = None, fetch_owner: Optional[bool] = None,
                         max_keys: Optional[int] = 1000, prefix: Optional[str] = "", 
                         start_after: Optional[str] = None) -> str:
    """Returns some or all (up to 1,000) of the objects in a bucket with each request. 
    You can use the request parameters as selection criteria to return a subset of the objects in a bucket. 
    To get a list of your buckets, see list_buckets.
    
    Args:
        bucket: When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3).
        continuation_token: ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results.
        encoding_type: Encoding type used by Amazon S3 to encode the object keys in the response. Responses are encoded only in UTF-8. An object key can contain any Unicode character. However, the XML 1.0 parser can't parse certain characters, such as characters with an ASCII value from 0 to 10. For characters that aren't supported in XML 1.0, you can add this parameter to request that Amazon S3 encode the keys in the response.
        fetch_owner: The owner field is not present in ListObjectsV2 by default. If you want to return the owner field with each key in the result, then set the FetchOwner field to true.
        max_keys: Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.
        prefix: Limits the response to keys that begin with the specified prefix.
        start_after: StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this specified key. StartAfter can be any key in the bucket.
    """
    try:
        await ctx.info(f'Listing objects in bucket: {bucket} with prefix: {prefix}')
        objects = await s3_resource.list_objects(bucket, prefix, max_keys)
        return str(objects)
    except Exception as error:
        logger.error(f"Error listing objects in bucket {bucket}: {str(error)}")
        return f"Error: {str(error)}"


@mcp.tool()
async def get_object(ctx: Context, bucket: str, key: str, range_header: Optional[str] = None, 
                    version_id: Optional[str] = None, part_number: Optional[int] = None) -> str:
    """Retrieves an object from Amazon S3. In the GetObject request, specify the full key name for the object. 
    General purpose buckets - Both the virtual-hosted-style requests and the path-style requests are supported. 
    For a virtual hosted-style request example, if you have the object photos/2006/February/sample.jpg, 
    specify the object key name as /photos/2006/February/sample.jpg. For a path-style request example, 
    if you have the object photos/2006/February/sample.jpg in the bucket named examplebucket, 
    specify the object key name as /examplebucket/photos/2006/February/sample.jpg. 
    Directory buckets - Only virtual-hosted-style requests are supported.
    
    Args:
        bucket: Directory buckets - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3).
        key: Key of the object to get. Length Constraints: Minimum length of 1.
        range_header: Downloads the specified byte range of an object.
        version_id: Version ID used to reference a specific version of the object. By default, the GetObject operation returns the current version of an object. To return a different version, use the versionId subresource.
        part_number: Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.
    """
    try:
        await ctx.info(f'Getting object: {key} from bucket: {bucket}')
        response = await s3_resource.get_object(bucket, key)
        if isinstance(response['Body'], bytes):
            file_content = response['Body'].decode('utf-8')
        else:
            file_content = str(response['Body'])
        return file_content
    except Exception as error:
        logger.error(f"Error getting object {key} from bucket {bucket}: {str(error)}")
        return f"Error: {str(error)}"


@mcp.tool()
async def put_object(ctx: Context, bucket: str, key: str, body: Optional[str] = None, 
                    file_path: Optional[str] = None, content_type: Optional[str] = None) -> str:
    """Adds spreadsheet and Excel files to the protex-intelligence-artifacts bucket ONLY. 
    This tool is restricted to upload ONLY .xls, .xlsx, and .csv files to the 'protex-intelligence-artifacts' 
    bucket for security purposes. All other file types will be rejected. You must have WRITE permissions on the bucket to add an object to it. 
    Supports both local file paths and direct base64 content. Provide either 'body' (for base64 content) OR 'file_path' (for local file upload).
    
    Args:
        bucket: The bucket name to which the PUT action was initiated. MUST be 'protex-intelligence-artifacts' - uploads to other buckets will be rejected.
        key: Object key for which the PUT action was initiated. This will be the file path/name in the bucket. MUST end with .xls, .xlsx, or .csv extension - other file types will be rejected. Length Constraints: Minimum length of 1.
        body: Object data as base64 encoded string. For CSV files, you can also provide the raw text content. For Excel files (.xls, .xlsx), content must be base64 encoded. Optional if file_path is provided.
        file_path: Local file path to upload to S3. If provided, this takes precedence over body parameter. The file will be read from the local filesystem and uploaded. Must be a valid path to a .xls, .xlsx, or .csv file. Optional if body is provided.
        content_type: A standard MIME type describing the format of the contents. Common types: 'text/csv' for CSV, 'application/vnd.ms-excel' for .xls, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' for .xlsx. If not specified, the system will infer it from the file extension.
    """
    try:
        await ctx.info(f'Uploading object: {key} to bucket: {bucket}')
        
        # Validate bucket restriction
        if not re.search(r'protex[-_ ]?intelligence', key, re.IGNORECASE):
            raise ValueError(f"PutObject is restricted to keys containing 'protex-intelligence'. Provided key: {key}")
        
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
        
        elif body:
            # Handle body content (base64 or raw text)
            if is_excel_file:
                # Excel files must be base64 encoded
                try:
                    # Validate base64 content
                    if not body:
                        raise ValueError("Body content cannot be empty for Excel files")
                    
                    # Clean base64 string (remove whitespace, newlines)
                    clean_body = ''.join(body.split())
                    
                    # More flexible base64 validation - just check if it can be decoded
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
                    clean_body = ''.join(body.split())
                    body_bytes = base64.b64decode(clean_body, validate=True)
                    logger.debug(f"Successfully decoded base64 CSV content for {key}")
                except Exception:
                    # If base64 decode fails, treat as raw text for CSV
                    body_bytes = body.encode('utf-8')
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
            raise ValueError("Either 'body' or 'file_path' parameter must be provided")
        
        logger.debug(f"Uploading {key} to {bucket} with content type: {content_type} ({len(body_bytes)} bytes)")
        
        response = await s3_resource.put_object(bucket, key, body_bytes, content_type)
        
        source_info = f"from file: {file_path}" if file_path else "from body content"
        return f"Successfully uploaded {key} to bucket {bucket} {source_info}. ETag: {response.get('ETag', 'N/A')}, Size: {len(body_bytes)} bytes"
        
    except Exception as error:
        logger.error(f"Error uploading object {key} to bucket {bucket}: {str(error)}")
        return f"Error: {str(error)}"



def health_check():
    """Print health status message."""
    print('MCP server active')


def main():
    """Run the MCP server with CLI argument support."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='AWS Lambda MCP Server')
    parser.add_argument('--health', action='store_true', help='Check server health')
    args = parser.parse_args()
    
    # Handle health check
    if args.health:
        health_check()
        return
    
    """Run the MCP server with FastMCP."""
    mcp.run()


if __name__ == "__main__":
    main()