import requests
from datetime import datetime, timedelta
import json
import urllib.parse
import time
import logging
import random
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

# List of special CV filenames that should get rank 5 when interview is complete
SPECIAL_CV_FILES = [
    "101619_original_cv.pdf",
    "102541_original_cv.pdf",
    "103272_original_cv.pdf",
    "111467_original_cv.pdf"
]

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

# Load interview questions from file
def load_questions_from_file(filename="interview_questions.txt"):
    """
    Load interview questions from a text file.
    Each question should be on a separate line in the file.
    
    Args:
        filename (str): Path to the file containing questions
        
    Returns:
        list: List of questions
    """
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            # Read lines and strip whitespace, filter out empty lines
            questions = [line.strip() for line in file if line.strip()]
            logging.info(f"Loaded {len(questions)} questions from {filename}")
            return questions
    except FileNotFoundError:
        logging.warning(f"Questions file {filename} not found. Using default questions.")
        # Return default questions if file not found
        return [
            "Can you describe your experience with content creation for social media, particularly on LinkedIn, and how you've focused on lead generation in the past?",
            "What strategies have you used to increase social media engagement and follower growth, and how do you set and measure KPIs for these efforts?",
        ]
    except Exception as e:
        logging.error(f"Error loading questions from file: {str(e)}")
        # Return default questions on error
        return [
            "Can you describe your experience with content creation for social media, particularly on LinkedIn, and how you've focused on lead generation in the past?",
            "What strategies have you used to increase social media engagement and follower growth, and how do you set and measure KPIs for these efforts?",
        ]

# Load questions when module is imported
ALL_QUESTIONS = load_questions_from_file()

def get_random_questions(num_questions=6):
    """
    Select a specified number of random questions from the ALL_QUESTIONS list
    and format them as a single string with each question on a new line.
    
    Args:
        num_questions (int): Number of questions to select (default: 6)
        
    Returns:
        str: A formatted string of randomly selected questions
    """
    # Ensure we don't try to select more questions than available
    num_to_select = min(num_questions, len(ALL_QUESTIONS))
    
    # Select random questions without replacement if possible
    selected_questions = random.sample(ALL_QUESTIONS, num_to_select) if num_to_select <= len(ALL_QUESTIONS) else ALL_QUESTIONS.copy()
    
    # Format the questions as a string with each question on a single line
    formatted_questions = "\n".join([f"{i+1}. {q}" for i, q in enumerate(selected_questions)])
    
    return formatted_questions

def get_interview_title(client, job_title, first_name, last_name):
    client_prefix = f"{client} - " if client else ""
    title = f"{client_prefix}{job_title}: {first_name} {last_name}".strip()
    logging.debug(f"Generated interview title: {title}")
    return title

def get_portal_link(job_title, first_name, last_name, interview_id=None, cv_filename=None):
    # URL encode the parameters
    role = urllib.parse.quote(job_title)
    candidate = urllib.parse.quote(f"{first_name} {last_name}")
    
    # Construct the portal link
    portal_link = f"{portal_base_url}?role={role}&candidate={candidate}"
    
    # Add interview_id parameter if available
    if interview_id is not None:
        portal_link += f"&interview_id={interview_id}"
    
    # Add cv_filename parameter if available
    if cv_filename is not None:
        portal_link += f"&cv={urllib.parse.quote(cv_filename)}"
        
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
                
                # Extract CV file name from CV array
                cv_filename = None
                cv_files = cv.get("CV", [])
                if cv_files and len(cv_files) > 0:
                    # Use the title from the first file in the array
                    cv_filename = cv_files[0].get("title")
                
                # Look up the candidate ID
                candidate_id = find_candidate_id(first_name, last_name)
                
                logging.info(f"  Found candidate: {first_name} {last_name}" + 
                           (f" (ID: {candidate_id})" if candidate_id else " (ID not found)") +
                           (f", CV File: {cv_filename}" if cv_filename else ", No CV File"))
                
                pairs.append({
                    "job_title": job_title,
                    "client": client,
                    "first_name": first_name,
                    "last_name": last_name,
                    "candidate_id": candidate_id,
                    "cv_filename": cv_filename
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

def create_interview(client, job_title, first_name, last_name, candidate_id=None, cv_filename=None):
    interview_date = (datetime.now() + timedelta(weeks=2)).strftime("%Y-%m-%d")
    current_datetime = get_current_datetime()
    
    logging.info(f"Creating interview for:")
    logging.info(f"  Client: {client or 'No Client'}")
    logging.info(f"  Job Title: {job_title}")
    logging.info(f"  Candidate: {first_name} {last_name}")
    logging.info(f"  Candidate ID: {candidate_id or 'Not found'}")
    logging.info(f"  CV Filename: {cv_filename or 'Not available'}")
    logging.info(f"  Scheduled Date: {interview_date}")
    logging.info(f"  Date Added: {current_datetime}")
    
    # Initially create with a temporary portal link, including CV filename if available
    initial_portal_link = get_portal_link(job_title, first_name, last_name, cv_filename=cv_filename)
    
    # Get random questions for this interview
    interview_questions = get_random_questions()
    
    payload = {
        "Title": get_interview_title(client, job_title, first_name, last_name),
        "Interview Portal Link": initial_portal_link,
        "Interview Due Date": interview_date,
        "Interview Status": "Ready for Interview",  # Updated to "Ready for Interview"
        "Interview Rank": 0,
        "Questions": interview_questions,  # Using the randomly selected questions
        "Date Added": current_datetime,
        "CV Name": cv_filename  # Added CV Name field
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
                    
                    # Create the updated portal link with the interview ID and CV filename
                    updated_portal_link = get_portal_link(job_title, first_name, last_name, interview_id, cv_filename)
                    logging.info(f"Updating portal link to include interview ID and CV filename: {updated_portal_link}")
                    
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

def update_interview_ranks():
    """Check interviews with 'Complete' status and special CV filenames and set their rank to 5."""
    logging.info("=== Checking for Completed Interviews with Special CV Files ===")
    
    try:
        # Fetch existing interviews
        interviews_response = requests.get(interview_url, headers=headers, params=params)
        
        if interviews_response.status_code != 200:
            logging.error(f"Failed to fetch interviews. Status code: {interviews_response.status_code}")
            return 0
            
        interviews_data = interviews_response.json()
        existing_interviews = interviews_data.get('list', [])
        logging.info(f"Found {len(existing_interviews)} existing interviews to check")
        
        updated_count = 0
        for interview in existing_interviews:
            interview_id = interview.get('Id')
            interview_status = interview.get('Interview Status')
            cv_name = interview.get('CV Name')
            current_rank = interview.get('Interview Rank')
            
            # Check if the interview status is "Complete" and the CV name is in our special list
            if (interview_id is not None and 
                interview_status == "Complete" and 
                cv_name in SPECIAL_CV_FILES and
                current_rank != 5):
                
                update_payload = {
                    "Id": interview_id,
                    "Interview Rank": 5
                }
                
                logging.info(f"Setting rank to 5 for interview ID {interview_id} with CV: {cv_name}")
                logging.info(f"Update payload: {json.dumps(update_payload)}")
                
                update_response = requests.patch(interview_url, headers=headers, json=update_payload)
                if update_response.status_code >= 200 and update_response.status_code < 300:
                    logging.info(f"✓ Successfully updated interview rank for {interview_id}")
                    updated_count += 1
                else:
                    logging.error(f"✗ Failed to update interview rank. Status code: {update_response.status_code}")
                    logging.error(f"Error response: {update_response.text}")
        
        logging.info(f"=== Updated rank for {updated_count} completed interviews with special CV files ===")
        return updated_count
    except Exception as e:
        logging.error(f"Error updating interview ranks: {str(e)}")
        return 0

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
                # Generate new random questions for each interview
                interview_questions = get_random_questions()
                
                update_payload = {
                    "Id": interview_id,
                    "Text": interview_questions,
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
                    pair.get("candidate_id"),  # Pass the candidate ID for linking
                    pair.get("cv_filename")    # Pass the CV filename
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
    
    # Check for completed interviews with special CV files and update ranks
    rank_updates = update_interview_ranks()
    logging.info(f"Updated rank for {rank_updates} interviews with special CV files")
    
    # Configuration
    check_interval_seconds = 10  # Check every 10 seconds
    reload_questions_interval = 300  # Reload questions every 5 minutes (300 seconds)
    rank_check_interval = 60  # Check ranks every minute
    
    logging.info(f"Monitor will check for updates every {check_interval_seconds} seconds")
    logging.info(f"Questions will be reloaded every {reload_questions_interval} seconds")
    logging.info(f"Interview ranks will be checked every {rank_check_interval} seconds")
    
    last_questions_reload = datetime.now()
    last_rank_check = datetime.now()
    
    try:
        while True:
            # Check if it's time to reload questions
            if (datetime.now() - last_questions_reload).total_seconds() >= reload_questions_interval:
                logging.info("Reloading interview questions from file...")
                global ALL_QUESTIONS
                ALL_QUESTIONS = load_questions_from_file()
                last_questions_reload = datetime.now()
            
            # Check if it's time to check interview ranks
            if (datetime.now() - last_rank_check).total_seconds() >= rank_check_interval:
                logging.info("Checking for interviews that need rank updates...")
                rank_updates = update_interview_ranks()
                if rank_updates > 0:
                    logging.info(f"Updated rank for {rank_updates} interviews with special CV files")
                else:
                    logging.info("No interview ranks needed updating")
                last_rank_check = datetime.now()
            
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