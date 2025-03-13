import os
import json
from datetime import datetime

def save_interview_responses(interview_data):
    """
    Save the interview responses to a JSON file in a local directory.
    
    Args:
        interview_data (dict): Dictionary containing interview data including responses
    
    Returns:
        str: Path to the saved file
    """
    # Create a directory for storing interview responses if it doesn't exist
    save_dir = os.path.join(os.getcwd(), "interview_responses")
    os.makedirs(save_dir, exist_ok=True)
    
    # Generate a filename based on candidate name, role, and timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate_name = interview_data.get("candidate_name", "unknown").replace(" ", "_")
    role_name = interview_data.get("role_name", "unknown").replace(" ", "_")
    interview_id = interview_data.get("interview_id", "unknown")
    
    filename = f"{candidate_name}_{role_name}_{interview_id}_{timestamp}.json"
    file_path = os.path.join(save_dir, filename)
    
    # Save the interview data as JSON
    with open(file_path, "w") as f:
        json.dump(interview_data, f, indent=2)
    
    return file_path