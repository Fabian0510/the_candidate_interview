#!/usr/bin/env python
"""
Simplified Azure Storage Blob Upload Script

This script uploads a text file to Azure Blob Storage using an account key.
It's compatible with both regular Blob Storage and Data Lake Storage Gen2.
"""

import os
import argparse
import logging
from azure.storage.blob import BlobServiceClient

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def upload_file_to_azure(account_name, account_key, container_name, file_path, directory_path='', destination_filename=None):
    """
    Upload a file to Azure Blob Storage using account key authentication.
    Works with both Blob Storage and Data Lake Storage Gen2.
    
    Args:
        account_name (str): Azure storage account name
        account_key (str): Azure storage account key
        container_name (str): Name of the container/filesystem
        file_path (str): Local path to the file to upload
        directory_path (str, optional): Path to the directory in storage
        destination_filename (str, optional): Filename to use in storage (defaults to original filename)
        
    Returns:
        bool: True if upload was successful
    """
    # Validate input file exists
    if not os.path.exists(file_path):
        logger.error(f"Local file not found: {file_path}")
        return False
    
    # Use original filename if not specified
    if destination_filename is None:
        destination_filename = os.path.basename(file_path)
    
    # Normalize directory path
    if directory_path and not directory_path.endswith('/'):
        directory_path += '/'
    
    # Create full path for destination
    blob_path = f"{directory_path}{destination_filename}"
    
    try:
        # Create connection string and service client
        conn_str = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get blob client
        blob_client = container_client.get_blob_client(blob_path)
        
        # Upload the file
        with open(file_path, 'rb') as file_content:
            blob_client.upload_blob(file_content, overwrite=True)
        
        logger.info(f"Successfully uploaded {file_path} to {container_name}/{blob_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to upload file to Azure Storage: {str(e)}")
        return False

def main():
    """Parse command line arguments and upload the file."""
    parser = argparse.ArgumentParser(description="Upload a text file to Azure Storage")
    
    # Required arguments
    parser.add_argument('--account', required=True, help="Azure storage account name")
    parser.add_argument('--key', required=True, help="Azure storage account key")
    parser.add_argument('--container', required=True, help="Name of the container/filesystem")
    parser.add_argument('--file', required=True, help="Local path to the file to upload")
    
    # Optional arguments
    parser.add_argument('--directory', default='', help="Directory path in storage (default: root)")
    parser.add_argument('--destination', help="Destination filename (default: same as source)")
    
    args = parser.parse_args()
    
    # Call the upload function with provided arguments
    success = upload_file_to_azure(
        account_name=args.account,
        account_key=args.key,
        container_name=args.container,
        file_path=args.file,
        directory_path=args.directory,
        destination_filename=args.destination
    )
    
    # Return appropriate exit code
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())