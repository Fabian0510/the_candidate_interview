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
import traceback
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
    'xc-token': TOKEN,
    'Content-Type': 'application/json'  # Add Content-Type for PATCH requests
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
    
    # Replace spaces with underscores to be consistent with app.py
    filename = filename.replace(' ', '_')
    return filename

def save_am_comments_to_file(role_id: str, role_title: str, comments) -> str:
    """
    Save AM Comments to a text file in the jd directory.
    
    Args:
        role_id (str): ID of the role
        role_title (str): Title of the role
        comments: AM Comments to save (any type, will be converted to string)
        
    Returns:
        str: Path to the saved file
    """
    # Create a top-level "roles" directory
    base_dir = os.path.join(os.getcwd(), "roles")
    
    # Sanitize role name for directory creation
    sanitized_role_title = sanitize_filename(role_title)
    role_dir_name = f"{role_id}_{sanitized_role_title}"
    
    # Create role directory structure with jd subdirectory
    role_dir_path = os.path.join(base_dir, role_dir_name)
    jd_dir_path = os.path.join(role_dir_path, "jd")
    
    # Create directory structure if it doesn't exist
    os.makedirs(jd_dir_path, exist_ok=True)
    
    # Create output file path
    output_path = os.path.join(jd_dir_path, "am_insight.txt")
    
    # Convert comments to string and write to file
    try:
        # Handle various types that might come from the API
        if comments is None:
            comments_str = "No AM comments available"
        elif isinstance(comments, (dict, list)):
            comments_str = json.dumps(comments, indent=2)
        else:
            comments_str = str(comments)
            
        # Write to file
        with open(output_path, 'w') as f:
            f.write(comments_str)
        
        logger.info(f"Saved AM Comments to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving AM Comments: {str(e)}")
        logger.error(f"Comments type: {type(comments)}")
        # Create empty file to avoid future issues
        with open(output_path, 'w') as f:
            f.write("Error processing AM Comments")
        return output_path

def download_job_description(jd_file, role_id: str, role_title: str) -> str:
    """
    Download Job Description PDF file and save it to the jd directory.
    
    Args:
        jd_file: Job Description file information (dict or other type)
        role_id (str): ID of the role
        role_title (str): Title of the role
        
    Returns:
        str: Path to the downloaded file or None if failed
    """
    try:
        # Check if jd_file is a dictionary
        if not isinstance(jd_file, dict):
            logger.warning(f"Job Description file is not a dictionary: {type(jd_file)}")
            # If not a dict, create an empty JD file with error message
            base_dir = os.path.join(os.getcwd(), "roles")
            sanitized_role_title = sanitize_filename(role_title)
            role_dir_name = f"{role_id}_{sanitized_role_title}"
            role_dir_path = os.path.join(base_dir, role_dir_name)
            jd_dir_path = os.path.join(role_dir_path, "jd")
            os.makedirs(jd_dir_path, exist_ok=True)
            output_path = os.path.join(jd_dir_path, "jd_error.txt")
            
            with open(output_path, 'w') as f:
                f.write(f"Error: Job Description is not in expected format. Type: {type(jd_file)}\n")
                f.write(f"Value: {str(jd_file)}")
            
            logger.info(f"Created error file for Job Description: {output_path}")
            return output_path
            
        # Create a top-level "roles" directory
        base_dir = os.path.join(os.getcwd(), "roles")
        
        # Sanitize role name for directory creation
        sanitized_role_title = sanitize_filename(role_title)
        role_dir_name = f"{role_id}_{sanitized_role_title}"
        
        # Create role directory structure with jd subdirectory
        role_dir_path = os.path.join(base_dir, role_dir_name)
        jd_dir_path = os.path.join(role_dir_path, "jd")
        
        # Create directory structure if it doesn't exist
        os.makedirs(jd_dir_path, exist_ok=True)
        
        # Extract file info with thorough error checking
        file_path = jd_file.get("path")
        if not file_path:
            logger.warning(f"No path found for Job Description file: {jd_file}")
            return None
            
        # Get file title with a default and sanitize it
        file_title = jd_file.get("title", "job_description.pdf")
        file_title = sanitize_filename(file_title)
        
        # Create full download URL
        download_url = f"{BASE_URL}/{file_path}"
        
        # Create output file path
        output_path = os.path.join(jd_dir_path, file_title)
        
        # Download the file
        logger.info(f"Downloading Job Description: {file_title}")
        success = download_cv(download_url, output_path)
        
        if success:
            logger.info(f"Successfully downloaded Job Description to {output_path}")
            return output_path
        else:
            logger.warning(f"Failed to download Job Description: {file_title}")
            return None
            
    except Exception as e:
        logger.error(f"Error in download_job_description: {str(e)}")
        logger.error(f"Job Description file data: {str(jd_file)[:200]}...")
        return None

def process_roles_and_cvs() -> Tuple[int, List[str]]:
    """
    Process all roles and download associated CVs, AM Comments, and Job Descriptions.
    
    Returns:
        Tuple[int, List[str]]: Number of downloaded files and list of downloaded file paths
    """
    # Create a top-level "roles" directory for CVs
    base_dir = os.path.join(os.getcwd(), "roles")
    os.makedirs(base_dir, exist_ok=True)
    
    # Fetch all jobs (roles)
    logger.info("Fetching roles from API...")
    roles = fetch_data(jobs_url)
    logger.info(f"Found {len(roles)} roles")
    
    total_cvs = 0
    downloaded_cvs = 0
    downloaded_jds = 0
    downloaded_paths = []
    
    # Process each role
    for role in roles:
        role_id = role.get("Id")  # Note: "Id" is capitalized in the API response
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
        jd_dir_path = os.path.join(role_dir_path, "jd")
        
        # Create directory structure if it doesn't exist
        os.makedirs(cv_dir_path, exist_ok=True)
        os.makedirs(jd_dir_path, exist_ok=True)
        
        # Log the role being processed
        client = role.get("Client", "No Client")
        logger.info(f"Processing role: {role_title} (ID: {role_id}) for client: {client}")
        
        # Process AM Comments
        try:
            am_comments = role.get("AM Comments")
            # Process even if None or empty - our function will handle this
            logger.info(f"Processing AM Comments for role: {role_title}")
            am_comments_path = save_am_comments_to_file(role_id, role_title, am_comments)
            if am_comments_path:
                downloaded_paths.append(am_comments_path)
                logger.info(f"AM Comments file created at: {am_comments_path}")
        except Exception as e:
            logger.error(f"Error processing AM Comments for role {role_title}: {str(e)}")
                
        # Process Job Description PDF
        try:
            jd_files = role.get("JobDescription (PDF)")
            if jd_files:
                logger.info(f"Found Job Description files for role: {role_title}")
                
                if isinstance(jd_files, list):
                    # Process each JD file if it's a list
                    logger.info(f"Processing {len(jd_files)} Job Description files")
                    for jd_file in jd_files:
                        try:
                            jd_path = download_job_description(jd_file, role_id, role_title)
                            if jd_path:
                                downloaded_paths.append(jd_path)
                                downloaded_jds += 1
                                logger.info(f"Downloaded Job Description to: {jd_path}")
                        except Exception as jd_err:
                            logger.error(f"Error downloading a Job Description file: {str(jd_err)}")
                
                elif isinstance(jd_files, dict):
                    # Process a single JD file
                    logger.info("Processing single Job Description file")
                    jd_path = download_job_description(jd_files, role_id, role_title)
                    if jd_path:
                        downloaded_paths.append(jd_path)
                        downloaded_jds += 1
                        logger.info(f"Downloaded Job Description to: {jd_path}")
                        
                else:
                    # Handle unexpected types
                    logger.warning(f"Unexpected Job Description data type: {type(jd_files)}")
                    logger.warning(f"Job Description data: {jd_files}")
            else:
                logger.info(f"No Job Description files found for role: {role_title}")
        except Exception as e:
            logger.error(f"Error processing Job Description files for role {role_title}: {str(e)}")
        
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
            
            # Process CV files - handle both null and array cases
            cv_files = cv.get("CV")
            if cv_files is None:
                logger.warning(f"No CV files found for candidate {candidate_name}")
                continue
                
            # Ensure cv_files is a list even if there's only one item
            if not isinstance(cv_files, list):
                logger.warning(f"CV field is not an array for candidate {candidate_name}")
                continue
                
            if not cv_files:
                logger.warning(f"Empty CV files array for candidate {candidate_name}")
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
                
                # Use the original filename without modification
                output_path = os.path.join(cv_dir_path, file_title)
                
                # Download the CV
                logger.info(f"Downloading CV for {candidate_name}: {file_title}")
                success = download_cv(download_url, output_path)
                if success:
                    downloaded_cvs += 1
                    downloaded_paths.append(output_path)
                    
                # Add a small delay to avoid overwhelming the server
                time.sleep(0.5)
    
    # Log file paths for debugging
    if downloaded_paths:
        logger.info(f"Sample downloaded files:")
        for i, path in enumerate(downloaded_paths[:5]):
            logger.info(f"  - {path}")
        if len(downloaded_paths) > 5:
            logger.info(f"  - ...and {len(downloaded_paths) - 5} more")
    
    # Log summary
    logger.info("=== Download Summary ===")
    logger.info(f"Total roles processed: {len(roles)}")
    logger.info(f"Total CVs found: {total_cvs}")
    logger.info(f"Total CVs successfully downloaded: {downloaded_cvs}")
    logger.info(f"Total Job Descriptions downloaded: {downloaded_jds}")
    logger.info(f"Total AM Comments files created: {len([p for p in downloaded_paths if p.endswith('am_insight.txt')])}")
    
    total_files = downloaded_cvs + downloaded_jds + len([p for p in downloaded_paths if p.endswith('am_insight.txt')])
    logger.info(f"Total files processed: {total_files}")
    
    return total_files, downloaded_paths

def upload_file_to_blob_storage(file_path: str, role_id: str, role_name: str, filename: str = None) -> str:
    """
    Upload a file to Azure Blob Storage with the appropriate directory structure.
    
    Args:
        file_path (str): Path to the file to upload
        role_id (str): ID of the role
        role_name (str): Name of the role for directory structure
        filename (str, optional): Name of the file, extracted from file_path if None
        
    Returns:
        str: Blob path if successful, None otherwise
    """
    # Sanitize role name for blob path
    # This will use the sanitize_filename function which now replaces spaces with underscores
    sanitized_role_name = sanitize_filename(role_name)
    
    # Extract filename if not provided
    if filename is None:
        filename = os.path.basename(file_path)
    
    # Determine the subdirectory based on the file path and name
    if "/cvs/" in file_path or "\\cvs\\" in file_path:
        # CV file
        subdirectory = "cvs"
    elif "/jd/" in file_path or "\\jd\\" in file_path:
        if filename == "am_insight.txt":
            # AM Comments file
            subdirectory = "jd"
        else:
            # Job Description file
            subdirectory = "jd"
    else:
        # Default to cvs if we can't determine
        subdirectory = "cvs"
    
    # Create blob path with roles/roleid_rolename/subdirectory/<filename> structure
    blob_filename = f"roles/{role_id}_{sanitized_role_name}/{subdirectory}/{filename}"
    
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

def upload_downloaded_files(downloaded_paths: List[str]) -> int:
    """
    Upload all downloaded files to Azure Blob Storage.
    
    Args:
        downloaded_paths (List[str]): List of paths to downloaded files (CVs, JDs, AM Comments)
        
    Returns:
        int: Number of successfully uploaded files
    """
    successful_uploads = 0
    
    for file_path in downloaded_paths:
        try:
            # Extract role information from directory structure 
            # Expected path structures: 
            # - /path/to/roles/roleid_rolename/cvs/candidate_file.pdf
            # - /path/to/roles/roleid_rolename/jd/job_description.pdf
            # - /path/to/roles/roleid_rolename/jd/am_insight.txt
            path_parts = file_path.split(os.sep)
            
            # Check if "roles" is in the path
            if "roles" not in path_parts:
                logger.warning(f"Path does not contain 'roles' directory: {file_path}")
                continue
            
            # Find the index of "roles" directory
            roles_index = path_parts.index("roles")
            
            # Role directory should be right after "roles"
            if roles_index + 1 >= len(path_parts):
                logger.warning(f"Invalid path structure - no directory after 'roles': {file_path}")
                continue
                
            # Get the role directory name
            role_dir_name = path_parts[roles_index + 1]
            
            # Split role directory into ID and name (format: roleid_rolename)
            role_parts = role_dir_name.split('_', 1)
            if len(role_parts) < 2:
                logger.warning(f"Role directory name does not contain ID and name: {role_dir_name}")
                continue
                
            role_id = role_parts[0]
            role_name = role_parts[1]
            
            # Get the filename
            filename = os.path.basename(file_path)
            
            # Upload the file
            blob_path = upload_file_to_blob_storage(file_path, role_id, role_name, filename)
            
            if blob_path:
                successful_uploads += 1
                logger.info(f"Successfully uploaded {file_path} to Azure")
                
        except Exception as e:
            logger.error(f"Error processing file for upload: {file_path}, Error: {str(e)}")
            continue
    
    return successful_uploads

def process_cvs():
    """Run the download and upload process."""
    logger.info("=== Starting Download and Upload Process ===")
    logger.info(f"Azure Storage Account: {AZURE_STORAGE_ACCOUNT_NAME}")
    logger.info(f"Azure Container: {AZURE_CONTAINER_NAME}")
    
    # Download files (CVs, JDs, AM Comments)
    logger.info("Starting download process...")
    total_files, downloaded_paths = process_roles_and_cvs()
    
    # Upload all files
    logger.info("Starting upload process...")
    uploaded_files = upload_downloaded_files(downloaded_paths)
    
    # Log final summary
    logger.info("=== Process Summary ===")
    logger.info(f"Total files downloaded: {total_files}")
    logger.info(f"Total files uploaded to Azure: {uploaded_files}")
    logger.info("=== Process Completed ===")
    
    return {
        "success": total_files > 0 and uploaded_files > 0,
        "files_downloaded": total_files,
        "files_uploaded": uploaded_files
    }

# Create Flask app
app = Flask(__name__)

def find_job_by_uuid(uuid_string):
    """
    Find the correct job/role ID from a UUID string by querying all jobs.
    
    This function uses multiple strategies to match a UUID to a job:
    1. Exact UUID match in any field
    2. Partial UUID match in any field
    3. Match by XC record ID (if UUID contains record path)
    4. Direct numeric ID comparison
    
    Args:
        uuid_string: The UUID string that might be a job identifier
        
    Returns:
        dict: Job record if found, None otherwise
    """
    try:
        uuid_string = str(uuid_string).strip()
        logger.info(f"Searching for job with UUID reference: {uuid_string}")
        
        # Clean up the UUID string if it contains extra characters
        # Remove any surrounding quotes or whitespace
        clean_uuid = uuid_string.strip('"\'').strip()
        logger.info(f"Using cleaned UUID for search: {clean_uuid}")
        
        # Fetch all jobs with a larger limit to ensure we get all available jobs
        fetch_response = requests.get(jobs_url, headers=headers, params={'limit': 500})
        if fetch_response.status_code != 200:
            logger.error(f"Failed to fetch jobs. Status code: {fetch_response.status_code}")
            return None
            
        jobs_data = fetch_response.json()
        
        # Log response structure for debugging
        logger.info(f"API response structure - top level keys: {list(jobs_data.keys())}")
        
        jobs_list = jobs_data.get('list', [])
        logger.info(f"Retrieved {len(jobs_list)} jobs, searching for a match")
        
        # Log available fields in the first job for debugging
        if jobs_list:
            first_job = jobs_list[0]
            logger.info(f"Sample job fields: {list(first_job.keys())}")
            
            # Check job ID field specifically
            job_id = first_job.get('Id')
            logger.info(f"Sample job ID field ('Id'): {job_id} (type: {type(job_id).__name__})")
            
            # Check for status field
            status_field = first_job.get('Status')
            logger.info(f"Sample job Status field ('Status'): {status_field}")
            
        # Log the first few jobs for debugging
        for i, job in enumerate(jobs_list[:3]):
            logger.info(f"Sample job {i+1}: Id={job.get('Id')}, Title={job.get('Job Title')}")
        
        # ----- STRATEGY 1: Direct field matching -----
        logger.info("Strategy 1: Searching for exact UUID match in any field")
        for job in jobs_list:
            # Check for UUID in any string field
            for field, value in job.items():
                if isinstance(value, str) and clean_uuid in value:
                    logger.info(f"FOUND! UUID match in field '{field}': {value}")
                    logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                    return job
        
        # ----- STRATEGY 2: String representation matching -----
        logger.info("Strategy 2: Searching in JSON string representation")
        for job in jobs_list:
            # Convert entire job to JSON string and search in it
            job_str = json.dumps(job)
            if clean_uuid in job_str:
                logger.info(f"FOUND! UUID in job JSON string representation")
                logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                return job
        
        # ----- STRATEGY 3: Record ID extraction -----
        logger.info("Strategy 3: Attempting to extract record ID from UUID")
        # First check if this is a URL or path containing /record/
        if '/record/' in clean_uuid:
            parts = clean_uuid.split('/record/')
            if len(parts) > 1:
                record_id = parts[1].split('/')[0]
                logger.info(f"Extracted record ID from URL path: {record_id}")
                
                # Now search for this ID in jobs
                for job in jobs_list:
                    job_id = str(job.get('Id', ''))
                    if job_id == record_id:
                        logger.info(f"FOUND! Matched job by record ID: {record_id}")
                        logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                        return job
        
        # ----- STRATEGY 4: Special case for dashed UUIDs -----
        logger.info("Strategy 4: Special UUID processing")
        if '-' in clean_uuid:
            # If UUID contains dashes, try checking for matching string with and without dashes
            uuid_no_dashes = clean_uuid.replace('-', '')
            logger.info(f"Looking for UUID without dashes: {uuid_no_dashes}")
            
            for job in jobs_list:
                # Check if any field contains this UUID with or without dashes
                for field, value in job.items():
                    if isinstance(value, str):
                        # Check with dashes
                        if clean_uuid in value:
                            logger.info(f"FOUND! UUID with dashes match in field '{field}': {value}")
                            logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                            return job
                        
                        # Check without dashes
                        if uuid_no_dashes in value.replace('-', ''):
                            logger.info(f"FOUND! UUID without dashes match in field '{field}': {value}")
                            logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                            return job
                
                # Try matching job ID
                job_id = str(job.get('Id', ''))
                if clean_uuid == job_id or uuid_no_dashes == job_id:
                    logger.info(f"FOUND! UUID matches job ID directly")
                    logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                    return job
        
        # ----- STRATEGY 5: Last resort - look for UUID fragments -----
        logger.info("Strategy 5: Looking for UUID fragments in jobs")
        uuid_fragments = clean_uuid.split('-')
        if len(uuid_fragments) > 1:
            for fragment in uuid_fragments:
                if len(fragment) >= 4:  # Only use fragments of reasonable length
                    logger.info(f"Searching for UUID fragment: {fragment}")
                    for job in jobs_list:
                        job_str = json.dumps(job)
                        if fragment in job_str:
                            logger.info(f"FOUND! UUID fragment '{fragment}' found in job")
                            logger.info(f"Matched job: Id={job.get('Id')}, Title={job.get('Job Title')}")
                            return job
        
        logger.warning(f"No job found matching UUID: {clean_uuid} - tried multiple strategies")
        return None
        
    except Exception as e:
        logger.error(f"Error searching for job by UUID: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None

def update_role_status(role_id_or_uuid, new_status: str = "Generating Questions") -> bool:
    """
    Update a role's status using the API.
    
    This function handles different ID formats:
    1. UUID strings like "1a4129c2-535c-4d97-8cdb-513d9400ad72"
    2. Integer IDs like 12345
    3. String IDs like "12345"
    
    Args:
        role_id_or_uuid: The ID or UUID reference of the role to update
        new_status (str): The new status to set
        
    Returns:
        bool: True if the update was successful, False otherwise
    """
    try:
        # Convert to string and clean up the input
        role_id_or_uuid = str(role_id_or_uuid).strip()
        logger.info(f"Starting role status update process for: {role_id_or_uuid}")
        
        # Determine if this is a UUID or a direct ID
        is_uuid_format = '-' in role_id_or_uuid
        job = None
        job_id = None
        
        if is_uuid_format:
            # UUID processing path
            logger.info(f"Input appears to be a UUID format: {role_id_or_uuid}")
            job = find_job_by_uuid(role_id_or_uuid)
            
            if job:
                # Get the correct ID to use for the API
                job_id = job.get('Id')
                logger.info(f"Found matching job with ID: {job_id} (type: {type(job_id).__name__})")
                
                # Get current status for logging
                current_status = job.get('Status', 'Unknown')
                logger.info(f"Current status: {current_status}")
            else:
                # Last resort - try using the UUID directly
                logger.error(f"Could not find job matching UUID: {role_id_or_uuid}")
                logger.info(f"Will attempt to use UUID directly as a fallback")
                job_id = role_id_or_uuid
                current_status = "Unknown"
        else:
            # Direct ID processing path
            logger.info(f"Input appears to be a direct ID: {role_id_or_uuid}")
            
            # Try to convert to integer if it's a numeric string
            direct_id = role_id_or_uuid
            if role_id_or_uuid.isdigit():
                direct_id = int(role_id_or_uuid)
                logger.info(f"Converted string ID to integer: {direct_id}")
            
            # Verify the job exists using the direct ID
            fetch_url = f"{jobs_url}/{direct_id}"
            logger.info(f"Verifying job existence at: {fetch_url}")
            
            fetch_response = requests.get(fetch_url, headers=headers)
            
            if fetch_response.status_code == 200:
                # Job exists, use its data
                job = fetch_response.json()
                job_id = job.get('Id')  # Use the ID from the response
                current_status = job.get('Status', 'Unknown')
                logger.info(f"Verified job ID: {job_id}, current status: {current_status}")
            else:
                # Job doesn't exist by direct ID, try UUID lookup as fallback
                logger.warning(f"Failed to verify job ID {direct_id}. Status: {fetch_response.status_code}")
                logger.warning(f"Trying UUID lookup as fallback...")
                
                job = find_job_by_uuid(role_id_or_uuid)
                if job:
                    job_id = job.get('Id')
                    current_status = job.get('Status', 'Unknown')
                    logger.info(f"Found job via UUID lookup: {job_id}, status: {current_status}")
                else:
                    # Last resort - use the original ID
                    logger.error(f"Could not find job by any method, using original ID as fallback")
                    job_id = direct_id
                    current_status = "Unknown"
        
        if job_id is None:
            logger.error("Failed to determine job ID after multiple attempts")
            return False
            
        # Add additional logging for job ID type
        logger.info(f"Using job ID: {job_id} (type: {type(job_id).__name__})")
        
        # Try to convert the job_id to the format expected by the API
        # The API might expect a specific format (string/int)
        try:
            # Try to convert to integer if it's a numeric string
            if isinstance(job_id, str) and job_id.isdigit():
                numeric_job_id = int(job_id)
                logger.info(f"Using numeric ID for API call: {numeric_job_id}")
                api_job_id = numeric_job_id
            else:
                # Keep as is
                api_job_id = job_id
                logger.info(f"Using original ID format for API call: {api_job_id}")
        except Exception as conv_err:
            logger.warning(f"Error converting job ID: {conv_err}")
            api_job_id = job_id  # Use original as fallback
        
        # Prepare the update payload - use exactly "Id" as the key (API is case-sensitive)
        update_payload = {
            "Id": api_job_id,  # Must use this exact case "Id" for API
            "Status": new_status
        }
        
        # Log the update details
        logger.info(f"Updating job {api_job_id} status from '{current_status}' to '{new_status}'")
        logger.info(f"Update URL: {jobs_url}")
        logger.info(f"Update payload: {json.dumps(update_payload)}")
        
        # Send PATCH request to update the job
        update_headers = headers.copy()
        update_headers['Content-Type'] = 'application/json'
        
        # Try the update
        response = requests.patch(jobs_url, headers=update_headers, json=update_payload)
        
        # Log the response
        logger.info(f"Update response status code: {response.status_code}")
        response_text = response.text
        logger.info(f"Update response: {response_text[:200]}...")
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"✓ Successfully updated job {api_job_id} status to '{new_status}'")
            
            # Verify the update
            verify_url = f"{jobs_url}/{api_job_id}"
            verify_response = requests.get(verify_url, headers=headers)
            
            if verify_response.status_code == 200:
                verify_data = verify_response.json()
                new_status_value = verify_data.get("Status")  # Use exact field name from API
                
                # Log the entire response for debugging
                logger.info(f"Verification - Response data keys: {list(verify_data.keys())}")
                logger.info(f"Verification - New status: {new_status_value}")
                
                if new_status_value == new_status:
                    logger.info("✓ Verification successful - Status was updated correctly")
                else:
                    logger.warning(f"⚠ Verification failed - Expected '{new_status}' but found '{new_status_value}'")
                    
                    # Check if the status field is using a different case
                    all_fields = verify_data.keys()
                    status_fields = [f for f in all_fields if f.lower() == "status"]
                    if status_fields and status_fields[0] != "Status":
                        actual_status_field = status_fields[0]
                        logger.warning(f"Status field in API response uses different case: '{actual_status_field}'")
                        logger.warning(f"Value in '{actual_status_field}': {verify_data.get(actual_status_field)}")
            
            return True
        else:
            logger.error(f"✗ Failed to update job status. Status code: {response.status_code}")
            logger.error(f"Error response: {response_text}")
            
            # Try alternative format if first attempt failed
            if isinstance(api_job_id, int):
                # Try with string version
                logger.info("First attempt with integer ID failed, trying with string ID...")
                string_job_id = str(api_job_id)
                
                update_payload["Id"] = string_job_id
                logger.info(f"Retrying with payload: {json.dumps(update_payload)}")
                
                retry_response = requests.patch(jobs_url, headers=update_headers, json=update_payload)
                logger.info(f"Retry response status code: {retry_response.status_code}")
                
                if retry_response.status_code >= 200 and retry_response.status_code < 300:
                    logger.info(f"✓ Retry successful! Updated job {string_job_id} status to '{new_status}'")
                    return True
            elif isinstance(api_job_id, str) and api_job_id.isdigit():
                # Try with integer version
                logger.info("First attempt with string ID failed, trying with integer ID...")
                int_job_id = int(api_job_id)
                
                update_payload["Id"] = int_job_id
                logger.info(f"Retrying with payload: {json.dumps(update_payload)}")
                
                retry_response = requests.patch(jobs_url, headers=update_headers, json=update_payload)
                logger.info(f"Retry response status code: {retry_response.status_code}")
                
                if retry_response.status_code >= 200 and retry_response.status_code < 300:
                    logger.info(f"✓ Retry successful! Updated job {int_job_id} status to '{new_status}'")
                    return True
            
            return False
            
    except Exception as e:
        logger.error(f"Error updating role status: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False

@app.route('/cv_uploader', methods=['POST'])
def cv_uploader():
    """
    Webhook endpoint to trigger CV download and upload process.
    
    This endpoint:
    1. Receives webhook requests with role ID information
    2. Downloads CVs, job descriptions, and AM comments
    3. Uploads them to Azure blob storage
    4. Updates the role status to "Generating Questions"
    
    Returns:
        JSON response with operation result
    """
    logger.info("===== CV UPLOADER WEBHOOK TRIGGERED =====")
    logger.info(f"Request received at: {datetime.now().isoformat()}")
    
    # Get the request JSON data with full logging
    try:
        # Log raw request data for debugging
        content_type = request.content_type
        logger.info(f"Request content type: {content_type}")
        
        raw_data = request.data.decode('utf-8') if request.data else ""
        if len(raw_data) > 1000:
            logger.info(f"Request data (truncated): {raw_data[:1000]}...")
        else:
            logger.info(f"Request data: {raw_data}")
        
        # Validate JSON content type
        is_json_content = content_type and 'json' in content_type.lower()
        logger.info(f"Content appears to be JSON: {is_json_content}")
        
        # Parse JSON depending on how it's sent
        if request.is_json:
            request_data = request.json
            logger.info("Parsed JSON data from request.json")
        else:
            # Try to parse as JSON if it's not automatically parsed
            try:
                request_data = json.loads(raw_data) if raw_data.strip() else {}
                logger.info("Manually parsed JSON data")
            except json.JSONDecodeError as json_err:
                logger.warning(f"Could not parse request data as JSON: {str(json_err)}")
                request_data = {}
        
        # Log the parsed data structure
        if isinstance(request_data, dict):
            top_level_keys = list(request_data.keys())
            logger.info(f"Parsed request data structure - top level keys: {top_level_keys}")
            # Log nested 'data' field if present
            if 'data' in request_data and isinstance(request_data['data'], dict):
                data_keys = list(request_data['data'].keys())
                logger.info(f"Keys in 'data' field: {data_keys}")
        else:
            logger.info(f"Parsed request data type: {type(request_data).__name__}")
            
    except Exception as e:
        logger.error(f"Error processing request data: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        request_data = {}
    
    # Extract the Role ID from the request data with enhanced search
    role_id = None
    id_source = None
    
    def extract_id_from_dict(data_dict, parent_key=""):
        """Helper function to recursively search for ID in nested dictionaries"""
        nonlocal role_id, id_source
        
        if not isinstance(data_dict, dict):
            return
            
        # Check for the exact "Id" field first (API uses this exact case)
        if "Id" in data_dict:
            candidate_id = data_dict["Id"]
            logger.info(f"Found API-style 'Id' field with value '{candidate_id}' in '{parent_key}.Id' (type: {type(candidate_id).__name__})")
            role_id = candidate_id
            id_source = f"{parent_key}.Id"
            # Since we found the exact key used by the API, prioritize this and return early
            return
            
        # Check various other ID field names at this level (fallback)
        id_field_names = ['id', 'ID', 'role_id', 'roleId', 'uuid', 'UUID', 'job_id', 'jobId']
        for field in id_field_names:
            if field in data_dict:
                candidate_id = data_dict[field]
                logger.info(f"Found potential ID '{candidate_id}' in field '{parent_key}.{field}' (type: {type(candidate_id).__name__})")
                
                # Only update if we don't have an ID yet or the new one looks more promising
                if role_id is None:
                    role_id = candidate_id
                    id_source = f"{parent_key}.{field}"
                # Prefer UUIDs for more specific matching
                elif isinstance(candidate_id, str) and '-' in candidate_id:
                    role_id = candidate_id
                    id_source = f"{parent_key}.{field}"
                    
        # Recurse into nested dictionaries
        for key, value in data_dict.items():
            if isinstance(value, dict):
                extract_id_from_dict(value, f"{parent_key}.{key}" if parent_key else key)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        extract_id_from_dict(item, f"{parent_key}.{key}[{i}]" if parent_key else f"{key}[{i}]")
    
    # Start the recursive search
    if isinstance(request_data, dict):
        extract_id_from_dict(request_data)
    
    # Log the result of ID extraction
    if role_id:
        logger.info(f"Extracted Role ID '{role_id}' from {id_source}")
        
        # Special handling for UUID-like strings
        if isinstance(role_id, str) and '-' in role_id:
            logger.info(f"Detected UUID format ID: {role_id}")
    else:
        logger.warning("No ID field found in the request data after deep search")
    
    # Run the process to download and upload files
    logger.info("Starting CV download and upload process...")
    result = process_cvs()
    
    # Update role status if we have a role ID
    if role_id:
        logger.info(f"Attempting to update status for role ID: {role_id}")
        status_updated = update_role_status(role_id)
        result["role_id"] = role_id
        result["status_updated"] = status_updated
        result["id_source"] = id_source
    else:
        logger.warning("No role ID found, skipping status update")
        result["status_updated"] = False
        result["role_id"] = None
    
    # Add timestamp to result
    result["timestamp"] = datetime.now().isoformat()
    
    # Return response
    logger.info(f"Webhook processing complete. Result summary: success={result.get('success', False)}, status_updated={result.get('status_updated', False)}")
    logger.info(f"Returning result: {json.dumps(result)}")
    logger.info("===== WEBHOOK PROCESSING COMPLETE =====")
    return jsonify(result)

def main():
    """
    Run the download and upload process as a standalone script.
    
    Can be invoked with arguments:
    - No args: Run download and upload process
    - --webhook: Run as Flask webhook server
    - --status <role_id>: Update status for a specific role
    """
    import sys
    import argparse
    
    # Configure command-line argument parser
    parser = argparse.ArgumentParser(description='Download and Upload CVs from API to Azure Storage')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Add webhook command
    webhook_parser = subparsers.add_parser('webhook', help='Run as webhook server')
    webhook_parser.add_argument('--port', type=int, default=int(os.environ.get('PORT', 8888)),
                               help='Port to run webhook server on (default: 8888)')
    webhook_parser.add_argument('--debug', action='store_true',
                               help='Run Flask in debug mode')
    
    # Add status command
    status_parser = subparsers.add_parser('status', help='Update status for a specific role')
    status_parser.add_argument('role_id', type=str, help='Role ID to update')
    status_parser.add_argument('--status', type=str, default="Generating Questions",
                              help='New status value (default: "Generating Questions")')
    
    # Parse arguments
    if len(sys.argv) > 1:
        args = parser.parse_args()
        
        # Process based on command
        if args.command == 'webhook':
            logger.info(f"Starting webhook server on port {args.port}")
            app.run(host='0.0.0.0', port=args.port, debug=args.debug)
            return 0
        elif args.command == 'status':
            logger.info(f"Updating status for role ID: {args.role_id} to '{args.status}'")
            success = update_role_status(args.role_id, args.status)
            logger.info(f"Status update {'successful' if success else 'failed'}")
            return 0 if success else 1
    else:
        # Default behavior - run download and upload process
        logger.info("Running CV download and upload process")
        result = process_cvs()
        return 0 if result["success"] else 1

if __name__ == "__main__":
    # Check if running as script or as webhook (legacy support)
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--webhook':
        # Legacy webhook mode
        port = int(os.environ.get('PORT', 8888))
        logger.info(f"Starting webhook server on port {port} (legacy mode)")
        app.run(host='0.0.0.0', port=port)
    else:
        # Use new command-line interface
        exit(main())