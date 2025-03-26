import streamlit as st
import random
import time
import json
import requests
import os
from datetime import datetime
import re
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage Configuration
AZURE_STORAGE_ACCOUNT_NAME = "stcandidatedev"  # Replace with your storage account name
AZURE_STORAGE_ACCOUNT_KEY = "7j9fQrp2GBUlBJJjEFu1LrKNIaVKD73MTRin+hRsP1R8tdiasOkg7H3if5bJQfHNwdkw2NoBjdez+ASt9IbpRw=="    # Replace with your storage account key
AZURE_CONTAINER_NAME = "landing"
AZURE_STORAGE_CONNECTION_STRING = None  # Set to None since we're using account name and key

# Function to save interview responses to Azure Blob Storage
# Function to save interview responses to Azure Blob Storage
def save_to_blob_storage(interview_data):
    try:
        # Create a filename based on candidate name, role, and date
        sanitized_candidate_name = interview_data['candidate_name'].replace(' ', '_')
        sanitized_role_name = interview_data['role_name'].replace(' ', '_')
        
        # Create filename with no spaces
        blob_filename = f"answers/{sanitized_candidate_name}_{sanitized_role_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        # Generate the content to be saved
        content = ""
        # Write header information
        content += f"Interview with {interview_data['candidate_name']} for {interview_data['role_name']}\n"
        content += f"Date: {interview_data['interview_date']}\n"
        content += f"Interview ID: {interview_data['interview_id']}\n\n"
        
        # Write each question and answer
        for i, response in enumerate(interview_data['responses'], 1):
            content += f"Question {i}: {response['question']}\n\n"
            content += f"Answer: {response['answer']}\n\n"
            content += "-" * 80 + "\n\n"
        
        # Connect to blob storage using hard-coded credentials
        # This approach directly creates the connection using account name and key
        account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)
        
        # Get container client
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        # Upload the content to blob storage
        blob_client = container_client.get_blob_client(blob_filename)
        blob_client.upload_blob(content, overwrite=True)
        
        return blob_filename
    except Exception as e:
        st.error(f"Error saving to blob storage: {str(e)}")
        return None
    
# Function to save interview responses to a text file
def save_interview_responses(interview_data):
    try:
        # Create a directory for saving responses if it doesn't exist
        os.makedirs("interview_responses", exist_ok=True)
        
        # Create a filename based on candidate name, role, and date
        # Replace spaces with underscores in candidate name and role name
        sanitized_candidate_name = interview_data['candidate_name'].replace(' ', '_')
        sanitized_role_name = interview_data['role_name'].replace(' ', '_')
        
        # Create filename with no spaces
        filename = f"interview_responses/{sanitized_candidate_name}_{sanitized_role_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        # Write responses to the text file
        with open(filename, "w") as file:
            # Write header information
            file.write(f"Interview with {interview_data['candidate_name']} for {interview_data['role_name']}\n")
            file.write(f"Date: {interview_data['interview_date']}\n")
            file.write(f"Interview ID: {interview_data['interview_id']}\n\n")
            
            # Write each question and answer
            for i, response in enumerate(interview_data['responses'], 1):
                file.write(f"Question {i}: {response['question']}\n\n")
                file.write(f"Answer: {response['answer']}\n\n")
                file.write("-" * 80 + "\n\n")
        
        # Also save to Azure Blob Storage
        blob_path = save_to_blob_storage(interview_data)
        if blob_path:
            st.sidebar.success(f"✓ Responses saved to Azure Blob Storage: {blob_path}")
        
        return filename
    except Exception as e:
        st.error(f"Error saving responses: {str(e)}")
        return None


    
# Streamed response emulator
def response_generator(response_text):
    for word in response_text.split():
        yield word + " "
        time.sleep(0.05)

# API configuration
BASE_API_URL = "http://20.254.105.163:8080/api/v2"
INTERVIEW_TABLE = "mpims4p3zrwsarx"
INTERVIEW_URL = f"{BASE_API_URL}/tables/{INTERVIEW_TABLE}/records"

# Handle token configuration with fallback
try:
    # Try to get token from Streamlit secrets
    import os
    API_TOKEN = os.environ.get("API_TOKEN", "EgEls5yPpzOqhGdtL1CDcZkNolXhQhIFfwd4DIe0")  # Default token from example
except Exception as e:
    # Fallback to environment variable or hardcoded token for development
    print(f"Error accessing environment variables: {e}")
    

HEADERS = {
    'accept': 'application/json',
    'xc-token': API_TOKEN,
    'Content-Type': 'application/json'
}

# Function to fetch questions from the API
def fetch_interview_questions(interview_id):
    try:
        # Construct the API URL with query parameters
        questions_url = f"{INTERVIEW_URL}?fields=Questions&where=(Id,eq,{interview_id})&limit=25&shuffle=0&offset=0"
        
        st.sidebar.write(f"Fetching questions from API...")
        st.sidebar.code(questions_url)
        
        # Send the GET request
        response = requests.get(questions_url, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            st.sidebar.success("✓ Questions fetched successfully")
            
            # Check if we got any records back
            if data and 'list' in data and len(data['list']) > 0:
                # Extract the Questions field from the first record
                questions_text = data['list'][0].get('Questions', '')
                
                if not questions_text:
                    st.sidebar.warning("No questions found in response. Using default questions.")
                    return get_default_questions()
                
                # Parse the numbered list of questions
                # Split the text by newlines first, then extract question text
                questions_list = []
                for line in questions_text.split('\n'):
                    # Use regex to match numbered questions (e.g., "1. Question text")
                    match = re.match(r'^\s*\d+\.\s*(.*\S)\s*$', line)
                    if match:
                        # Add the question text to our list
                        questions_list.append(match.group(1))
                
                if not questions_list:
                    st.sidebar.warning("Failed to parse questions. Using default questions.")
                    return get_default_questions()
                
                st.sidebar.write(f"Found {len(questions_list)} questions")
                return questions_list
            else:
                st.sidebar.warning("No interview data found. Using default questions.")
                return get_default_questions()
        else:
            st.sidebar.error(f"Failed to fetch questions. Status code: {response.status_code}")
            st.sidebar.error(f"Error response: {response.text}")
            return get_default_questions()
    except Exception as e:
        st.sidebar.error(f"Error fetching questions: {str(e)}")
        return get_default_questions()

# Default questions as fallback
def get_default_questions():
    return [    
        "Can you describe your experience with content creation for social media, particularly on LinkedIn, and how you focus on lead generation?",
        "What design tools have you used to create digital-first content, and do you have experience with video content creation?",
        "How do you approach setting and measuring KPIs for social media growth, such as follower growth and engagement metrics?",
        "Can you provide examples of successful marketing strategies you've developed and executed that increased conversions and brand awareness?",
        "How do you balance reporting and analytics with creativity in your marketing campaigns, and what metrics do you typically report on?",
        "What excites you about joining a collaborative and innovative team in a growing law firm, and how do you see yourself contributing to our team culture?",
    ]

# Get URL parameters
role_name = st.query_params.get("role", "Unknown Role")
candidate_name = st.query_params.get("candidate", "Unknown User")
interview_id = st.query_params.get("interview_id", "1")  # Default to ID 1 if not provided

# Initialize interview data structure
if "interview_data" not in st.session_state:
    st.session_state.interview_data = {
        "candidate_name": candidate_name,
        "role_name": role_name,
        "interview_id": interview_id,
        "interview_date": datetime.now().strftime("%Y-%m-%d"),
        "responses": []
    }

st.title(f"{candidate_name} | {role_name} Interview")

# Initialize chat history and fetch interview questions
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": f"Good morning, {candidate_name}! Welcome to The Candidate's interview platform!"}
    ]
    # Fetch questions from API
    st.session_state.interview_questions = fetch_interview_questions(interview_id)
    st.session_state.question_index = 0
    st.session_state.interview_complete = False

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Function to update interview status and rank
def update_interview_status():
    try:
        # Log what we're doing
        st.sidebar.write("Updating interview status to Complete...")
        
        # Generate a random interview rank between 1 and 5
        interview_rank = random.randint(1, 5)
        
        # Compile all answers into a single text document
        all_answers = ""
        for i, response in enumerate(st.session_state.interview_data['responses'], 1):
            all_answers += f"Q{i}: {response['question']}\n\n"
            all_answers += f"A{i}: {response['answer']}\n\n"
            all_answers += "-" * 40 + "\n\n"
        
        # Prepare the update payload
        update_payload = {
            "Id": interview_id,  # Use the interview_id from URL parameters
            "Interview Status": "Complete",
            "Interview Rank": interview_rank,
            "Answers": all_answers  # Add all answers to the Answers field
        }
        
        st.sidebar.write("Sending PATCH request:")
        st.sidebar.code(json.dumps(update_payload, indent=2))
        
        # Send the PATCH request
        patch_response = requests.patch(INTERVIEW_URL, headers=HEADERS, json=update_payload)
        
        if patch_response.status_code == 200:
            st.sidebar.success(f"✓ Interview status updated to Complete with rank {interview_rank}")
            st.sidebar.success(f"✓ Interview answers submitted to API")
            
            # Save all responses to a text file when interview completes
            file_path = save_interview_responses(st.session_state.interview_data)
            if file_path:
                st.sidebar.success(f"✓ All responses saved to: {file_path}")
                st.session_state.interview_data["final_file_path"] = file_path
            
            return True
        else:
            st.sidebar.error(f"✗ Failed to update status. Status code: {patch_response.status_code}")
            st.sidebar.error(f"Error response: {patch_response.text}")
            return False
            
    except Exception as e:
        st.sidebar.error(f"Error updating interview status: {str(e)}")
        return False

# Add a button in the sidebar to test the blob storage functionality
if st.sidebar.button("Save Responses to Azure Blob"):
    blob_path = save_to_blob_storage(st.session_state.interview_data)
    if blob_path:
        st.sidebar.success(f"Responses manually saved to Azure Blob: {blob_path}")


# Function to ask the next question
def ask_next_question():
    if st.session_state.question_index < len(st.session_state.interview_questions):
        # Get the current question
        question = st.session_state.interview_questions[st.session_state.question_index]
        
        # Display the question
        with st.chat_message("assistant"):
            response = st.write_stream(response_generator(question))
        
        # Add question to chat history
        st.session_state.messages.append({"role": "assistant", "content": question})
        
        # Increment question index
        st.session_state.question_index += 1
        
    elif not st.session_state.interview_complete:
        # Display final message
        with st.chat_message("assistant"):
            final_message = "Thank you for your time! The interview is now complete."
            st.write_stream(response_generator(final_message))
            
        # Add final message to chat history
        st.session_state.messages.append({"role": "assistant", "content": final_message})
        
        # Update the interview status and save final responses
        st.session_state.interview_complete = True
        update_interview_status()

# Start conversation with a greeting
if "greeted" not in st.session_state:
    st.session_state.greeted = True
    with st.chat_message("assistant"):
        greeting = f"We'll be asking some questions to establish your suitability for the role of {role_name}. Let's get started."
        st.write_stream(response_generator(greeting))
    st.session_state.messages.append({"role": "assistant", "content": greeting})
    ask_next_question()

# Accept user input
if prompt := st.chat_input("Your response here..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Store the response in the interview data
    if st.session_state.question_index > 0 and st.session_state.question_index <= len(st.session_state.interview_questions):
        current_question = st.session_state.interview_questions[st.session_state.question_index - 1]
        
        # Add the question-answer pair to the responses list
        st.session_state.interview_data["responses"].append({
            "question": current_question,
            "answer": prompt,
            "question_number": st.session_state.question_index
        })

    # Ask the next question
    ask_next_question()

# Display debug information
st.sidebar.title("Dev Debug")
st.sidebar.write(f"Current role: {role_name}")
st.sidebar.write(f"Current candidate: {candidate_name}")
st.sidebar.write(f"Interview ID: {interview_id}")
st.sidebar.write(f"Question index: {st.session_state.question_index}/{len(st.session_state.interview_questions)}")
st.sidebar.write(f"API Token: {API_TOKEN[:5]}..." if API_TOKEN else "No API token found")

# Button to manually save responses
if st.sidebar.button("Save Responses to File"):
    file_path = save_interview_responses(st.session_state.interview_data)
    if file_path:
        st.sidebar.success(f"Responses manually saved to: {file_path}")

# Display current interview data
if st.sidebar.checkbox("Show interview data"):
    st.sidebar.write("Current interview data:")
    st.sidebar.code(json.dumps(st.session_state.interview_data, indent=2))

# Display current questions
if st.sidebar.checkbox("Show questions"):
    st.sidebar.write("Interview questions:")
    for i, q in enumerate(st.session_state.interview_questions):
        st.sidebar.write(f"{i+1}. {q}")