#!/usr/bin/env python
"""
Download and Upload CVs Webhook

Responds to POST requests and triggers the download and upload process.
Downloads CVs from the API for each role, saves them to role-specific directories,
and then uploads them to Azure Blob Storage using credentials from .streamlit/secrets.toml.
"""

import os
import requests
import logging
import time
import json
import toml
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from typing import Dict, Any, List, Tuple
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cv_process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Token
TOKEN = 'EgEls5yPpzOqhGdtL1CDcZkNolXhQhIFfwd4DIe0'

# API configuration
headers = {
    'accept': 'application/json',
    'xc-token': TOKEN
}

# Base URLs
BASE_API_URL = "http://20.254.105.163:8080/api/v2"
BASE_URL = "http://20.254.105.163:8080"

# Table and field IDs from example URL
TABLE_ID = "m0ro5phcebcdbt7"  # From example URL

# API endpoints
jobs_url = f"{BASE_API_URL}/tables/mgwvuug18vkrhg0/records"

# Load Azure Storage settings from .streamlit/secrets.toml
secrets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.streamlit', 'secrets.toml')
try:
    secrets = toml.load(secrets_path)
    AZURE_STORAGE_ACCOUNT_NAME = secrets.get('azure', {}).get('storage_account_name')
    AZURE_STORAGE_ACCOUNT_KEY = secrets.get('azure', {}).get('storage_account_key')
    AZURE_CONTAINER_NAME = secrets.get('azure', {}).get('container_name', 'landing')
    logger.info(f"Loaded Azure credentials from .streamlit/secrets.toml")
except Exception as e:
    logger.error(f"Error loading Azure credentials from secrets file: {str(e)}")
    raise ValueError("Azure Storage credentials not available in .streamlit/secrets.toml")

def fetch_data(url: str, params: Dict = None) -> List[Dict]:
    """Fetch data from API with pagination support."""
    if params is None:
        params = {}
    
    default_params = {
        'limit': 100,
        'shuffle': 0,
        'offset': 0
    }
    
    # Merge default params with provided params
    for key, value in default_params.items():
        if key not in params:
            params[key] = value
    
    all_records = []
    offset = 0
    total_records = None
    
    while True:
        params['offset'] = offset
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch data. Status code: {response.status_code}")
            logger.error(f"Error: {response.text}")
            break
            
        data = response.json()
        records = data.get('list', [])
        all_records.extend(records)
        
        # Set total records count if not yet set
        if total_records is None:
            total_records = data.get('pageInfo', {}).get('totalRows', 0)
            
        # Break if we've fetched all records or no records returned
        if not records or len(all_records) >= total_records:
            break
            
        offset += len(records)
    
    logger.info(f"Fetched {len(all_records)} records from {url}")
    return all_records

def download_cv(cv_url: str, output_path: str) -> bool:
    """Download CV file from the given URL."""
    try:
        # Make a copy of the headers for the download request
        download_headers = headers.copy()
        # For file downloads, we need different accept header
        download_headers['accept'] = '*/*'
        
        logger.info(f"Download URL: {cv_url}")
        
        # Get the file with the authentication token
        response = requests.get(cv_url, headers=download_headers, stream=True)
        
        if response.status_code != 200:
            logger.error(f"Failed to download CV. Status code: {response.status_code}")
            logger.error(f"Response: {response.text[:200]}...")  # Log first 200 chars of response
            return False
            
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write the file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Successfully downloaded CV to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading CV: {str(e)}")
        return False

def sanitize_filename(filename: str) -> str:
    """Sanitize a string to be used as a filename."""
    # Replace unsafe characters with underscore
    unsafe_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for char in unsafe_chars:
        filename = filename.replace(char, '_')
    return filename

def process_roles_and_cvs() -> Tuple[int, List[str]]:
    """
    Process all roles and download associated CVs.
    
    Returns:
        Tuple[int, List[str]]: Number of downloaded CVs and list of downloaded file paths
    """
    # Create a base directory for CVs (just use current directory)
    base_dir = os.getcwd()
    
    # Fetch all jobs (roles)
    logger.info("Fetching roles from API...")
    roles = fetch_data(jobs_url)
    logger.info(f"Found {len(roles)} roles")
    
    total_cvs = 0
    downloaded_cvs = 0
    downloaded_paths = []
    
    # Process each role
    for role in roles:
        role_id = role.get("id")
        role_title = role.get("Job Title")
        if not role_title or not role_id:
            logger.warning("Role without ID or title found, skipping")
            continue
            
        # Sanitize role name for directory creation
        sanitized_role_title = sanitize_filename(role_title)
        role_dir_name = f"{role_id}_{sanitized_role_title}"
        
        # Create role directory structure with cvs subdirectory
        role_dir_path = os.path.join(base_dir, role_dir_name)
        cv_dir_path = os.path.join(role_dir_path, "cvs")
        
        # Create directory structure if it doesn't exist
        os.makedirs(cv_dir_path, exist_ok=True)
        
        # Log the role being processed
        client = role.get("Client", "No Client")
        logger.info(f"Processing role: {role_title} (ID: {role_id}) for client: {client}")
        
        # Get CV relations for this role
        cv_relations = role.get("nc_92rx___nc_m2m_JobDescription_CVs", [])
        logger.info(f"Found {len(cv_relations)} CV relations for this role")
        
        for relation in cv_relations:
            cv = relation.get("CV")
            if not cv:
                continue
                
            # Get candidate name
            first_name = cv.get("First Name", "")
            last_name = cv.get("Last Name", "")
            candidate_name = f"{first_name}_{last_name}".replace(" ", "_")
            
            # Process CV files
            cv_files = cv.get("CV", [])
            if not cv_files:
                logger.warning(f"No CV files found for candidate {candidate_name}")
                continue
                
            # Process each CV file
            for cv_file in cv_files:
                total_cvs += 1
                
                # Get CV file path
                file_path = cv_file.get("path")
                if not file_path:
                    logger.warning(f"No path found for CV file")
                    continue
                
                # Get the file title (filename)
                file_title = cv_file.get("title", "unknown.pdf")
                
                # Create full download URL
                download_url = f"{BASE_URL}/{file_path}"
                
                # Create output filename
                output_filename = f"{candidate_name}_{file_title}"
                output_path = os.path.join(cv_dir_path, output_filename)
                
                # Download the CV
                logger.info(f"Downloading CV for {candidate_name}: {file_title}")
                success = download_cv(download_url, output_path)
                if success:
                    downloaded_cvs += 1
                    downloaded_paths.append(output_path)
                    
                # Add a small delay to avoid overwhelming the server
                time.sleep(0.5)
    
    # Log summary
    logger.info("=== Download Summary ===")
    logger.info(f"Total roles processed: {len(roles)}")
    logger.info(f"Total CVs found: {total_cvs}")
    logger.info(f"Total CVs successfully downloaded: {downloaded_cvs}")
    
    return downloaded_cvs, downloaded_paths

def upload_file_to_blob_storage(file_path: str, role_id: str, role_name: str, cv_filename: str = None) -> str:
    """
    Upload a CV file to Azure Blob Storage.
    
    Args:
        file_path (str): Path to the CV file to upload
        role_id (str): ID of the role
        role_name (str): Name of the role for directory structure
        cv_filename (str, optional): Name of the CV file, extracted from file_path if None
        
    Returns:
        str: Blob path if successful, None otherwise
    """
    # Sanitize role name for blob path
    sanitized_role_name = sanitize_filename(role_name)
    
    # Extract CV filename if not provided
    if cv_filename is None:
        cv_filename = os.path.basename(file_path)
    
    # Create blob path with roleid_rolename/cvs/<cv_name> structure
    blob_filename = f"{role_id}_{sanitized_role_name}/cvs/{cv_filename}"
    
    try:    
        # Connect to blob storage
        account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)
        
        # Get container client
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        # Upload the file to blob storage
        blob_client = container_client.get_blob_client(blob_filename)
        
        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        
        logger.info(f"Successfully uploaded {file_path} to {AZURE_CONTAINER_NAME}/{blob_filename}")
        return blob_filename
    
    except Exception as e:
        logger.error(f"Error uploading to blob storage: {str(e)}")
        return None

def upload_downloaded_cvs(downloaded_paths: List[str]) -> int:
    """
    Upload all downloaded CV files to Azure Blob Storage.
    
    Args:
        downloaded_paths (List[str]): List of paths to downloaded CV files
        
    Returns:
        int: Number of successfully uploaded files
    """
    successful_uploads = 0
    
    for file_path in downloaded_paths:
        # Extract role information from directory structure 
        # Expected path structure: /path/to/roleid_rolename/cvs/candidate_file.pdf
        path_parts = file_path.split(os.sep)
        
        # Find the index of "cvs" directory in the path
        try:
            cvs_index = path_parts.index("cvs")
            if cvs_index <= 0:
                logger.warning(f"Invalid path structure - 'cvs' is at the beginning: {file_path}")
                continue
                
            # Role directory is one level above "cvs"
            role_dir_name = path_parts[cvs_index - 1]
            
            # Split role directory into ID and name (format: roleid_rolename)
            role_parts = role_dir_name.split('_', 1)
            if len(role_parts) < 2:
                logger.warning(f"Role directory name does not contain ID and name: {role_dir_name}")
                continue
                
            role_id = role_parts[0]
            role_name = role_parts[1]
            
            # Get the CV filename
            cv_filename = os.path.basename(file_path)
            
            # Upload the file
            blob_path = upload_file_to_blob_storage(file_path, role_id, role_name, cv_filename)
            
            if blob_path:
                successful_uploads += 1
                
        except ValueError:
            logger.warning(f"Path does not contain 'cvs' directory: {file_path}")
            continue
    
    return successful_uploads

def process_cvs():
    """Run the download and upload process."""
    logger.info("=== Starting CV Download and Upload Process ===")
    logger.info(f"Azure Storage Account: {AZURE_STORAGE_ACCOUNT_NAME}")
    logger.info(f"Azure Container: {AZURE_CONTAINER_NAME}")
    
    # Download CVs
    logger.info("Starting CV download process...")
    downloaded_cvs, downloaded_paths = process_roles_and_cvs()
    
    # Upload CVs
    logger.info("Starting CV upload process...")
    uploaded_cvs = upload_downloaded_cvs(downloaded_paths)
    
    # Log final summary
    logger.info("=== Process Summary ===")
    logger.info(f"Total CVs downloaded: {downloaded_cvs}")
    logger.info(f"Total CVs uploaded to Azure: {uploaded_cvs}")
    logger.info("=== Process Completed ===")
    
    return {
        "success": downloaded_cvs > 0 and uploaded_cvs > 0,
        "cvs_downloaded": downloaded_cvs,
        "cvs_uploaded": uploaded_cvs
    }

# Create Flask app
app = Flask(__name__)

@app.route('/cv_uploader', methods=['POST'])
def cv_uploader():
    """Webhook endpoint to trigger CV download and upload process."""
    logger.info("CV uploader webhook triggered")
    
    # Run the process
    result = process_cvs()
    
    # Return response
    return jsonify(result)

def main():
    """Run the download and upload process as a standalone script."""
    result = process_cvs()
    return 0 if result["success"] else 1

if __name__ == "__main__":
    # Check if running as script or as webhook
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--webhook':
        # Run as webhook
        port = int(os.environ.get('PORT', 8888))
        app.run(host='0.0.0.0', port=port)
    else:
        # Run as script
        exit(main())