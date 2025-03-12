import requests
import logging
from typing import Dict, Any, List, Optional, Tuple
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('star_candidate_linker.log'),
        logging.StreamHandler()
    ]
)

# API Token
TOKEN = 'EgEls5yPpzOqhGdtL1CDcZkNolXhQhIFfwd4DIe0'

# API configuration
headers = {
    'accept': 'application/json',
    'xc-token': TOKEN,
    'Content-Type': 'application/json'
}

# URLs
jobs_url = "http://20.254.105.163:8080/api/v2/tables/mgwvuug18vkrhg0/records"
interview_url = "http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/records"
candidates_url = "http://20.254.105.163:8080/api/v2/tables/m0ro5phcebcdbt7/records"

# Job link API endpoint for connecting jobs to recommended candidates
# mgwvuug18vkrhg0 = Job table ID
# c2m9fbmh42orqbf = Link field ID for connecting to recommended candidates
job_link_api_url_template = "http://20.254.105.163:8080/api/v2/tables/mgwvuug18vkrhg0/links/c2m9fbmh42orqbf/records/{job_id}"

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
            logging.error(f"Failed to fetch data. Status code: {response.status_code}")
            logging.error(f"Error: {response.text}")
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
    
    logging.info(f"Fetched {len(all_records)} records from {url}")
    return all_records

def find_job_by_title(jobs: List[Dict], job_title: str, client: Optional[str] = None) -> Optional[Dict]:
    """Find a job record by title and optionally client."""
    for job in jobs:
        if job.get("Job Title") == job_title:
            # If client is specified, match that too
            if client:
                if job.get("Client") == client:
                    return job
            else:
                # If no client specified, return the first match by title
                return job
    return None

def extract_job_info_from_interview_title(title: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Extract job title, client, first name, and last name from an interview title.
    Expected format: "[Client - ]Job Title: First Name Last Name"
    """
    try:
        # First, split by colon to separate job info and candidate name
        parts = title.split(':', 1)
        if len(parts) != 2:
            return None, None, None, None
            
        job_part = parts[0].strip()
        candidate_part = parts[1].strip()
        
        # Extract candidate first and last name
        name_parts = candidate_part.split()
        if len(name_parts) < 2:
            return None, None, None, None
            
        first_name = name_parts[0]
        last_name = ' '.join(name_parts[1:])  # Handle multi-word last names
        
        # Check if job part has client
        if ' - ' in job_part:
            client, job_title = job_part.split(' - ', 1)
        else:
            client = None
            job_title = job_part
            
        return job_title, client, first_name, last_name
        
    except Exception as e:
        logging.error(f"Error parsing interview title '{title}': {str(e)}")
        return None, None, None, None

def link_candidate_to_job(job_id: int, candidate_ids: List[int]) -> bool:
    """Link candidate IDs to a job as recommended candidates."""
    if not job_id or not candidate_ids:
        logging.warning("Cannot link candidates to job: missing IDs")
        return False
    
    # Format the URL with the job ID
    link_url = job_link_api_url_template.format(job_id=job_id)
    
    # Format the payload as list of dicts with Id
    payload = [{"Id": candidate_id} for candidate_id in candidate_ids]
    
    try:
        logging.info(f"Linking candidates {candidate_ids} to job {job_id}")
        logging.info(f"Using link URL: {link_url}")
        logging.info(f"Payload: {payload}")
        
        response = requests.post(link_url, headers=headers, json=payload)
        
        # Log the response
        logging.info(f"Response status code: {response.status_code}")
        try:
            response_text = response.text
            logging.info(f"Response body: {response_text}")
            
            # Accept both 200 and 201 as success codes
            if response.status_code in [200, 201]:
                logging.info(f"✓ Successfully linked candidates to job {job_id}")
                return True
            else:
                logging.error(f"✗ Failed to link candidates to job. Status code: {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"Error processing response: {str(e)}")
            return False
    except Exception as e:
        logging.error(f"Error linking candidates to job: {str(e)}")
        return False

def main():
    logging.info("=== Starting 4-5 Star Candidate Linker ===")
    
    # Fetch all jobs
    logging.info("Fetching all jobs...")
    jobs = fetch_data(jobs_url)
    
    # Fetch all interviews
    logging.info("Fetching all interviews...")
    interviews = fetch_data(interview_url)
    
    # Track processed job IDs to avoid duplicate processing
    processed_job_ids = set()
    
    # Track candidate IDs by job ID
    job_candidates_map = {}
    
    # Process interviews with 4-5 star ratings
    logging.info("Processing interviews with 4-5 star ratings...")
    high_rated_interviews = []
    
    for interview in interviews:
        # Check if interview has a 4 or 5 star rating
        interview_rank = interview.get("Interview Rank")
        if interview_rank is not None and interview_rank >= 4:
            high_rated_interviews.append(interview)
    
    logging.info(f"Found {len(high_rated_interviews)} interviews with 4-5 star ratings")
    
    # Extract candidate IDs and match to jobs
    for interview in high_rated_interviews:
        title = interview.get("Title")
        if not title:
            logging.warning(f"Interview ID {interview.get('Id')} missing title, skipping")
            continue
            
        # Extract job and candidate info from title
        job_title, client, first_name, last_name = extract_job_info_from_interview_title(title)
        
        if not job_title or not first_name or not last_name:
            logging.warning(f"Could not parse interview title: {title}")
            continue
            
        # Find job record
        job = find_job_by_title(jobs, job_title, client)
        if not job:
            logging.warning(f"Job not found for title: {job_title}, client: {client}")
            continue
            
        job_id = job.get("Id")
        if not job_id:
            logging.warning(f"Job found but missing ID for title: {job_title}")
            continue
            
        # Get candidate ID from the interview
        candidate_id = None
        for link in interview.get("nc_m2jd___nc_rel_Candidate", []):
            candidate = link.get("Candidate")
            if candidate and candidate.get("First Name") == first_name and candidate.get("Last Name") == last_name:
                candidate_id = candidate.get("Id")
                break
        
        # If candidate ID wasn't found in the link, try to search for it
        if not candidate_id:
            logging.info(f"Searching for candidate ID for {first_name} {last_name}...")
            search_params = {
                'where': f"(First Name,eq,{first_name})~and(Last Name,eq,{last_name})"
            }
            candidates = fetch_data(candidates_url, search_params)
            if candidates:
                candidate_id = candidates[0].get("Id")
        
        if not candidate_id:
            logging.warning(f"Could not find candidate ID for {first_name} {last_name}")
            continue
            
        # Add candidate to the job's candidate list
        if job_id not in job_candidates_map:
            job_candidates_map[job_id] = set()
        job_candidates_map[job_id].add(candidate_id)
        
        logging.info(f"Adding candidate {candidate_id} ({first_name} {last_name}) to job {job_id} ({job_title})")
    
    # Link candidates to jobs
    successful_links = 0
    total_jobs = len(job_candidates_map)
    
    for job_id, candidate_ids in job_candidates_map.items():
        if job_id in processed_job_ids:
            logging.info(f"Job {job_id} already processed, skipping")
            continue
            
        candidate_ids_list = list(candidate_ids)
        logging.info(f"Linking {len(candidate_ids_list)} candidates to job {job_id}")
        
        success = link_candidate_to_job(job_id, candidate_ids_list)
        if success:
            successful_links += 1
            processed_job_ids.add(job_id)
    
    # Summary
    logging.info("=== Summary ===")
    logging.info(f"Total interviews with 4-5 stars: {len(high_rated_interviews)}")
    logging.info(f"Total jobs with high-rated candidates: {total_jobs}")
    logging.info(f"Successfully linked candidates to {successful_links} jobs")
    logging.info("Process completed!")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(2)