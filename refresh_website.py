import requests
TOKEN = 'EgEls5yPpzOqhGdtL1CDcZkNolXhQhIFfwd4DIe0'

jobs_url = "http://20.254.105.163:8080/api/v2/tables/mgwvuug18vkrhg0/records"
candidates_url = "http://20.254.105.163:8080/api/v2/tables/m0ro5phcebcdbt7/records"
interview_url = "http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/records"


import requests
from datetime import datetime, timedelta
import json
import urllib.parse

print("Starting interview scheduling process...")

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
portal_base_url = "http://20.254.105.163:8501/"

print(f"\nConfigured API endpoints:")
print(f"Jobs URL: {jobs_url}")
print(f"Interview URL: {interview_url}")
print(f"Portal Base URL: {portal_base_url}")

def get_interview_title(client, job_title, first_name, last_name):
    client_prefix = f"{client} - " if client else ""
    title = f"{client_prefix}{job_title}: {first_name} {last_name}".strip()
    print(f"Generated interview title: {title}")
    return title

def get_portal_link(job_title, first_name, last_name):
    # URL encode the parameters
    role = urllib.parse.quote(job_title)
    candidate = urllib.parse.quote(f"{first_name} {last_name}")
    
    # Construct the portal link
    portal_link = f"{portal_base_url}?role={role}&candidate={candidate}"
    print(f"Generated portal link: {portal_link}")
    return portal_link

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_all_candidate_job_pairs(jobs_data):
    print("\nExtracting candidate-job pairs...")
    pairs = []
    
    for job in jobs_data['list']:
        job_title = job.get("Job Title")
        client = job.get("Client")
        
        if not job_title:
            print(f"Skipping job record - no job title found")
            continue
            
        print(f"\nProcessing job: {job_title} for client: {client or 'No Client'}")
        for cv_relation in job.get("nc_92rx___nc_m2m_JobDescription_CVs", []):
            cv = cv_relation.get("CV")
            if cv:
                first_name = cv.get("First Name")
                last_name = cv.get("Last Name")
                print(f"  Found candidate: {first_name} {last_name}")
                pairs.append({
                    "job_title": job_title,
                    "client": client,
                    "first_name": first_name,
                    "last_name": last_name
                })
    
    print(f"\nFound {len(pairs)} total candidate-job pairs")
    return pairs

def create_interview(client, job_title, first_name, last_name):
    interview_date = (datetime.now() + timedelta(weeks=2)).strftime("%Y-%m-%d")
    current_datetime = get_current_datetime()
    
    print(f"\nCreating interview for:")
    print(f"  Client: {client or 'No Client'}")
    print(f"  Job Title: {job_title}")
    print(f"  Candidate: {first_name} {last_name}")
    print(f"  Scheduled Date: {interview_date}")
    print(f"  Date Added: {current_datetime}")
    
    # Generate the portal link
    portal_link = get_portal_link(job_title, first_name, last_name)
    
    payload = {
        "Title": get_interview_title(client, job_title, first_name, last_name),
        "Interview Portal Link": portal_link,
        "Interview Due Date": interview_date,
        "Interview Status": "Not Started",
        "Interview Rank": 0,
        "Text": "",
        "Date Added": current_datetime
    }
    
    print("\nSending API request with payload:")
    print(json.dumps(payload, indent=2))
    
    response = requests.post(interview_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print("✓ Interview created successfully")
        return True
    else:
        print(f"✗ Failed to create interview. Status code: {response.status_code}")
        print(f"Error response: {response.text}")
        return False

# Main execution
def main():
    print("\n=== Fetching Current Jobs and Candidates ===")
    jobs_response = requests.get(jobs_url, headers=headers, params=params)
    
    if jobs_response.status_code != 200:
        print(f"Failed to fetch jobs. Status code: {jobs_response.status_code}")
        return
        
    jobs_data = jobs_response.json()
    print(f"Successfully fetched {len(jobs_data.get('list', []))} jobs")
    
    print("\n=== Fetching Current Interviews ===")
    interviews_response = requests.get(interview_url, headers=headers, params=params)
    
    if interviews_response.status_code != 200:
        print(f"Failed to fetch interviews. Status code: {interviews_response.status_code}")
        return
        
    interviews_data = interviews_response.json()
    existing_interviews = interviews_data.get('list', [])
    print(f"Successfully fetched {len(existing_interviews)} existing interviews")
    
    existing_titles = set(interview.get("Title") for interview in existing_interviews)
    print(f"\nFound {len(existing_titles)} existing interview titles")
    
    pairs = get_all_candidate_job_pairs(jobs_data)
    
    print("\n=== Creating Missing Interviews ===")
    created_count = 0
    skipped_count = 0
    
    for pair in pairs:
        title = get_interview_title(pair["client"], pair["job_title"], pair["first_name"], pair["last_name"])
        if title not in existing_titles:
            success = create_interview(pair["client"], pair["job_title"], pair["first_name"], pair["last_name"])
            if success:
                created_count += 1
        else:
            print(f"\nSkipping existing interview: {title}")
            skipped_count += 1
    
    print("\n=== Summary ===")
    print(f"Total candidate-job pairs processed: {len(pairs)}")
    print(f"Interviews created: {created_count}")
    print(f"Interviews skipped (already exist): {skipped_count}")
    print("Process completed!")

if __name__ == "__main__":
    main()