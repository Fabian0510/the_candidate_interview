import requests
from datetime import datetime, timedelta
import json
import urllib.parse
import time
import logging
from typing import Dict, Any, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('interview_scheduler.log'),
        logging.StreamHandler()
    ]
)

# API Token
TOKEN = 'EgEls5yPpzOqhGdtL1CDcZkNolXhQhIFfwd4DIe0'

# API configuration
headers = {
    'accept': 'application/json',
    'xc-token': TOKEN
}

params = {
    'limit': 100,
    'shuffle': 0,
    'offset': 0
}

# URLs
jobs_url = "http://20.254.105.163:8080/api/v2/tables/mgwvuug18vkrhg0/records"
interview_url = "http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/records"
candidates_url = "http://20.254.105.163:8080/api/v2/tables/m0ro5phcebcdbt7/records"
portal_base_url = "http://20.254.105.163:8501/"

# Link API endpoint for connecting interviews to candidates
# mpims4p3zrwsarx = Interview table ID
# c4gkn9aehmsg1n5 = Link field ID for connecting to candidates
link_api_url = "http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/links/c4gkn9aehmsg1n5/records/{recordId}"

# Define interview questions
ALL_QUESTIONS = [
    "Can you describe your experience with content creation for social media, particularly on LinkedIn, and how you've focused on lead generation in the past?",
    "What strategies have you used to increase social media engagement and follower growth, and how do you set and measure KPIs for these efforts?",
]

def get_interview_title(client, job_title, first_name, last_name):
    client_prefix = f"{client} - " if client else ""
    title = f"{client_prefix}{job_title}: {first_name} {last_name}".strip()
    logging.debug(f"Generated interview title: {title}")
    return title

def get_portal_link(job_title, first_name, last_name, interview_id=None):
    # URL encode the parameters
    role = urllib.parse.quote(job_title)
    candidate = urllib.parse.quote(f"{first_name} {last_name}")
    
    # Construct the portal link
    portal_link = f"{portal_base_url}?role={role}&candidate={candidate}"
    
    # Add interview_id parameter if available
    if interview_id is not None:
        portal_link += f"&interview_id={interview_id}"
        
    logging.debug(f"Generated portal link: {portal_link}")
    return portal_link

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def find_candidate_id(first_name: str, last_name: str) -> Optional[int]:
    """Find the candidate ID in the candidates table based on first and last name.
    Returns the ID as an integer."""
    try:
        search_params = {
            'limit': 100,
            'shuffle': 0,
            'offset': 0,
            'where': f"(First Name,eq,{first_name})~and(Last Name,eq,{last_name})"
        }
        
        response = requests.get(candidates_url, headers=headers, params=search_params)
        
        if response.status_code != 200:
            logging.error(f"Failed to search for candidate. Status code: {response.status_code}")
            return None
            
        candidates_data = response.json()
        candidates = candidates_data.get('list', [])
        
        if not candidates:
            logging.warning(f"No candidate found with name: {first_name} {last_name}")
            return None
            
        # Return the ID of the first matching candidate as an integer
        candidate_id = candidates[0].get('Id')
        if candidate_id is not None:
            # Convert to integer
            candidate_id = int(candidate_id)
            logging.info(f"Found candidate ID {candidate_id} for {first_name} {last_name}")
            return candidate_id
        else:
            logging.warning(f"Candidate found but ID is missing for {first_name} {last_name}")
            return None
        
    except Exception as e:
        logging.error(f"Error finding candidate ID: {str(e)}")
        return None

def get_all_candidate_job_pairs(jobs_data):
    logging.info("Extracting candidate-job pairs...")
    pairs = []
    
    for job in jobs_data['list']:
        job_title = job.get("Job Title")
        client = job.get("Client")
        
        if not job_title:
            logging.warning(f"Skipping job record - no job title found")
            continue
            
        logging.info(f"Processing job: {job_title} for client: {client or 'No Client'}")
        for cv_relation in job.get("nc_92rx___nc_m2m_JobDescription_CVs", []):
            cv = cv_relation.get("CV")
            if cv:
                first_name = cv.get("First Name")
                last_name = cv.get("Last Name")
                
                # Look up the candidate ID
                candidate_id = find_candidate_id(first_name, last_name)
                
                logging.info(f"  Found candidate: {first_name} {last_name}" + 
                           (f" (ID: {candidate_id})" if candidate_id else " (ID not found)"))
                
                pairs.append({
                    "job_title": job_title,
                    "client": client,
                    "first_name": first_name,
                    "last_name": last_name,
                    "candidate_id": candidate_id
                })
    
    logging.info(f"Found {len(pairs)} total candidate-job pairs")
    return pairs

def link_interview_to_candidate(interview_id: int, candidate_id: int) -> bool:
    """Link an interview to a candidate using the link API."""
    if interview_id is None or candidate_id is None:
        logging.warning("Cannot link interview to candidate: missing ID")
        return False
    
    # Format the URL with the interview ID
    link_url = f"http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/links/c4gkn9aehmsg1n5/records/{interview_id}"
    
    # Format the payload exactly like the working CURL example
    payload = [{"Id": candidate_id}]
    
    try:
        logging.info(f"Linking interview {interview_id} to candidate {candidate_id}")
        logging.info(f"Using link URL: {link_url}")
        logging.info(f"Payload: {json.dumps(payload)}")
        
        # Make the request with the exact same headers as in the CURL example
        link_headers = {
            'accept': 'application/json',
            'xc-token': TOKEN,
            'Content-Type': 'application/json'
        }
        
        response = requests.post(link_url, headers=link_headers, json=payload)
        
        # Log the complete response
        logging.info(f"Response status code: {response.status_code}")
        logging.info(f"Response headers: {response.headers}")
        try:
            response_text = response.text
            logging.info(f"Response body: {response_text}")
            
            # Accept both 200 and 201 as success codes
            # 201 means "Created" which is a success for link creation
            if response.status_code == 200 or response.status_code == 201:
                logging.info(f"✓ Successfully linked interview {interview_id} to candidate {candidate_id}")
                return True
            else:
                logging.error(f"✗ Failed to link interview to candidate. Status code: {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"Error processing response: {str(e)}")
            return False
    except Exception as e:
        logging.error(f"Error linking interview to candidate: {str(e)}")
        return False

def create_interview(client, job_title, first_name, last_name, candidate_id=None):
    interview_date = (datetime.now() + timedelta(weeks=2)).strftime("%Y-%m-%d")
    current_datetime = get_current_datetime()
    
    logging.info(f"Creating interview for:")
    logging.info(f"  Client: {client or 'No Client'}")
    logging.info(f"  Job Title: {job_title}")
    logging.info(f"  Candidate: {first_name} {last_name}")
    logging.info(f"  Candidate ID: {candidate_id or 'Not found'}")
    logging.info(f"  Scheduled Date: {interview_date}")
    logging.info(f"  Date Added: {current_datetime}")
    
    # Initially create with a temporary portal link
    initial_portal_link = get_portal_link(job_title, first_name, last_name)
    
    payload = {
        "Title": get_interview_title(client, job_title, first_name, last_name),
        "Interview Portal Link": initial_portal_link,
        "Interview Due Date": interview_date,
        "Interview Status": "Ready for Interview",  # Updated to "Ready for Interview"
        "Interview Rank": 0,
        "Questions": INTERVIEW_QUESTIONS,  # Added the interview questions
        "Date Added": current_datetime
    }
    
    logging.debug("Sending API request with payload:")
    logging.debug(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(interview_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            response_data = response.json()
            # Check for Id with capital I (as shown in the logs)
            interview_id = response_data.get('Id')
            
            # Convert interview_id to integer if it's not None
            if interview_id is not None:
                try:
                    interview_id = int(interview_id)
                    logging.info(f"✓ Interview created successfully with ID: {interview_id}")
                    
                    # Create the updated portal link with the interview ID
                    updated_portal_link = get_portal_link(job_title, first_name, last_name, interview_id)
                    logging.info(f"Updating portal link to include interview ID: {updated_portal_link}")
                    
                    # Use the PATCH format that works based on your CURL example
                    update_payload = {
                        "Id": interview_id,
                        "Interview Portal Link": updated_portal_link
                    }
                    
                    logging.info(f"Using update URL: {interview_url}")
                    logging.info(f"Update payload: {json.dumps(update_payload)}")
                    
                    update_response = requests.patch(interview_url, headers=headers, json=update_payload)
                    if update_response.status_code >= 200 and update_response.status_code < 300:
                        logging.info("✓ Updated interview portal link successfully")
                    else:
                        logging.warning(f"✗ Failed to update portal link. Status code: {update_response.status_code}")
                        logging.warning(f"Error response: {update_response.text}")
                        
                except ValueError:
                    logging.warning(f"Interview ID could not be converted to integer: {interview_id}")
            else:
                logging.warning("Interview created but no ID returned")
                logging.info(f"Full response data: {json.dumps(response_data)}")
                return True
                
            # Link the interview to the candidate if we have a candidate ID
            if candidate_id is not None and interview_id is not None:
                # Add a small delay to ensure the interview record is fully created
                time.sleep(2)
                link_success = link_interview_to_candidate(interview_id, candidate_id)
                if link_success:
                    logging.info("✓ Interview linked to candidate successfully")
                else:
                    logging.warning("✗ Failed to link interview to candidate")
                    
            return True
        else:
            logging.error(f"✗ Failed to create interview. Status code: {response.status_code}")
            logging.error(f"Error response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Error creating interview: {str(e)}")
        return False

def update_existing_interviews():
    """Updates all existing interviews to set status to Ready for Interview and add questions."""
    logging.info("=== Updating Existing Interviews ===")
    
    try:
        # Fetch existing interviews
        interviews_response = requests.get(interview_url, headers=headers, params=params)
        
        if interviews_response.status_code != 200:
            logging.error(f"Failed to fetch interviews. Status code: {interviews_response.status_code}")
            return 0
            
        interviews_data = interviews_response.json()
        existing_interviews = interviews_data.get('list', [])
        logging.info(f"Found {len(existing_interviews)} existing interviews to update")
        
        updated_count = 0
        for interview in existing_interviews:
            interview_id = interview.get('Id')
            if interview_id is not None:
                update_payload = {
                    "Id": interview_id,
                    "Text": INTERVIEW_QUESTIONS,
                    "Interview Status": "Ready for Interview"
                }
                
                logging.info(f"Updating interview ID {interview_id}")
                logging.info(f"Update payload: {json.dumps(update_payload)}")
                
                update_response = requests.patch(interview_url, headers=headers, json=update_payload)
                if update_response.status_code >= 200 and update_response.status_code < 300:
                    logging.info(f"✓ Successfully updated interview {interview_id}")
                    updated_count += 1
                else:
                    logging.error(f"✗ Failed to update interview {interview_id}. Status code: {update_response.status_code}")
                    logging.error(f"Error response: {update_response.text}")
        
        logging.info(f"=== Updated {updated_count} existing interviews ===")
        return updated_count
    except Exception as e:
        logging.error(f"Error updating existing interviews: {str(e)}")
        return 0

def check_and_create_interviews():
    logging.info("=== Starting Interview Scheduling Check ===")
    logging.info(f"Time: {get_current_datetime()}")
    
    try:
        # Fetch jobs
        logging.info("=== Fetching Current Jobs and Candidates ===")
        jobs_response = requests.get(jobs_url, headers=headers, params=params)
        
        if jobs_response.status_code != 200:
            logging.error(f"Failed to fetch jobs. Status code: {jobs_response.status_code}")
            return
            
        jobs_data = jobs_response.json()
        logging.info(f"Successfully fetched {len(jobs_data.get('list', []))} jobs")
        
        # Fetch interviews
        logging.info("=== Fetching Current Interviews ===")
        interviews_response = requests.get(interview_url, headers=headers, params=params)
        
        if interviews_response.status_code != 200:
            logging.error(f"Failed to fetch interviews. Status code: {interviews_response.status_code}")
            return
            
        interviews_data = interviews_response.json()
        existing_interviews = interviews_data.get('list', [])
        logging.info(f"Successfully fetched {len(existing_interviews)} existing interviews")
        
        existing_titles = set(interview.get("Title") for interview in existing_interviews)
        logging.info(f"Found {len(existing_titles)} existing interview titles")
        
        # Get all candidate-job pairs
        pairs = get_all_candidate_job_pairs(jobs_data)
        
        # Create missing interviews
        logging.info("=== Creating Missing Interviews ===")
        created_count = 0
        skipped_count = 0
        
        for pair in pairs:
            title = get_interview_title(pair["client"], pair["job_title"], pair["first_name"], pair["last_name"])
            if title not in existing_titles:
                success = create_interview(
                    pair["client"], 
                    pair["job_title"], 
                    pair["first_name"], 
                    pair["last_name"],
                    pair.get("candidate_id")  # Pass the candidate ID for linking
                )
                if success:
                    created_count += 1
            else:
                logging.info(f"Skipping existing interview: {title}")
                skipped_count += 1
        
        # Summary
        logging.info("=== Summary ===")
        logging.info(f"Total candidate-job pairs processed: {len(pairs)}")
        logging.info(f"Interviews created: {created_count}")
        logging.info(f"Interviews skipped (already exist): {skipped_count}")
        logging.info("Check completed!")
        
        return created_count
    except Exception as e:
        logging.error(f"Error during check: {str(e)}")
        return 0

def main():
    logging.info("=== Starting Continuous Interview Scheduling Monitor ===")
    
    # Update existing interviews with the questions and status
    updated_count = update_existing_interviews()
    logging.info(f"Updated {updated_count} existing interviews with questions and status")
    
    # Configuration
    check_interval_seconds = 10  # Check every 10 seconds
    
    logging.info(f"Monitor will check for updates every {check_interval_seconds} seconds")
    
    try:
        while True:
            new_interviews = check_and_create_interviews()
            
            if new_interviews > 0:
                logging.info(f"Created {new_interviews} new interviews in this check")
            else:
                logging.info("No new interviews created in this check")
                
            next_check_time = datetime.now() + timedelta(seconds=check_interval_seconds)
            logging.info(f"Next check scheduled for: {next_check_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info("Waiting for next check cycle...")
            
            # Sleep until next check
            time.sleep(check_interval_seconds)
    except KeyboardInterrupt:
        logging.info("Monitor stopped by user")
    except Exception as e:
        logging.error(f"Monitor stopped due to error: {str(e)}")
        raise
    finally:
        logging.info("=== Continuous Interview Scheduling Monitor Stopped ===")

if __name__ == "__main__":
    # Call update_existing_interviews once when script is run
    update_existing_interviews()
    main()