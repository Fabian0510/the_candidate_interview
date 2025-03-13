import streamlit as st
import random
import time
import json
import requests
import os
from datetime import datetime
import re

# Import the file saving function
from interview_file_save import save_interview_responses

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

# Function to save responses after each answer and when interview completes
def save_responses():
    if "interview_data" in st.session_state:
        try:
            file_path = save_interview_responses(st.session_state.interview_data)
            st.sidebar.success(f"✓ Responses saved to central file")
            return file_path
        except Exception as e:
            st.sidebar.error(f"Failed to save responses: {str(e)}")
            return None

# Function to get all saved interviews
def get_all_interviews():
    save_dir = os.path.join(os.getcwd(), "interview_responses")
    file_path = os.path.join(save_dir, "all_interviews.json")
    
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return []
    else:
        return []

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

# Create tabs for interview and review
tab1, tab2 = st.tabs(["Interview", "Review All Interviews"])

with tab1:
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
            
            # Prepare the update payload
            update_payload = {
                "Id": interview_id,  # Use the interview_id from URL parameters
                "Interview Status": "Complete",
                "Interview Rank": interview_rank
            }
            
            st.sidebar.write("Sending PATCH request:")
            st.sidebar.code(json.dumps(update_payload, indent=2))
            
            # Send the PATCH request
            patch_response = requests.patch(INTERVIEW_URL, headers=HEADERS, json=update_payload)
            
            if patch_response.status_code == 200:
                st.sidebar.success(f"✓ Interview status updated to Complete with rank {interview_rank}")
                
                # Mark interview as complete in data
                st.session_state.interview_data["complete"] = True
                st.session_state.interview_data["rank"] = interview_rank
                
                # Save responses to file when interview completes
                file_path = save_responses()
                
                return True
            else:
                st.sidebar.error(f"✗ Failed to update status. Status code: {patch_response.status_code}")
                st.sidebar.error(f"Error response: {patch_response.text}")
                return False
                
        except Exception as e:
            st.sidebar.error(f"Error updating interview status: {str(e)}")
            return False

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
            
            # Save responses after each answer
            save_responses()

        # Ask the next question
        ask_next_question()

# Tab for reviewing all interviews
with tab2:
    st.title("Review All Interviews")
    
    # Get all interviews
    all_interviews = get_all_interviews()
    
    if not all_interviews:
        st.info("No interviews have been saved yet.")
    else:
        st.success(f"Found {len(all_interviews)} interview(s) in the central file.")
        
        # Create an expander for each interview
        for idx, interview in enumerate(all_interviews):
            candidate = interview.get("candidate_name", "Unknown")
            role = interview.get("role_name", "Unknown")
            date = interview.get("interview_date", "Unknown Date")
            timestamp = interview.get("timestamp", "")
            
            with st.expander(f"Interview #{idx+1}: {candidate} for {role} ({date})"):
                # Display interview metadata
                st.write(f"**Candidate:** {candidate}")
                st.write(f"**Role:** {role}")
                st.write(f"**Date:** {date}")
                st.write(f"**Interview ID:** {interview.get('interview_id', 'Unknown')}")
                
                # Display completion status
                status = "Complete" if interview.get("complete", False) else "In Progress"
                st.write(f"**Status:** {status}")
                
                if interview.get("rank"):
                    st.write(f"**Rank:** {interview.get('rank')}/5")
                
                # Display responses
                st.subheader("Responses")
                for response in interview.get("responses", []):
                    st.markdown(f"**Q{response.get('question_number', '?')}:** {response.get('question', 'Unknown question')}")
                    st.markdown(f"**A:** {response.get('answer', 'No answer provided')}")
                    st.divider()

    # Button to open the file location
    if st.button("Open File Location"):
        save_dir = os.path.join(os.getcwd(), "interview_responses")
        try:
            # Try to open the file explorer to the directory
            if os.name == 'nt':  # Windows
                os.startfile(save_dir)
            elif os.name == 'posix':  # macOS or Linux
                import subprocess
                subprocess.call(('open' if os.uname().sysname == 'Darwin' else 'xdg-open', save_dir))
            
            st.success(f"Opened file location: {save_dir}")
        except Exception as e:
            st.error(f"Failed to open file location: {e}")
            st.code(save_dir)  # At least show the path

# Display debug information
st.sidebar.title("Dev Debug")
st.sidebar.write(f"Current role: {role_name}")
st.sidebar.write(f"Current candidate: {candidate_name}")
st.sidebar.write(f"Interview ID: {interview_id}")

if hasattr(st.session_state, 'question_index') and hasattr(st.session_state, 'interview_questions'):
    st.sidebar.write(f"Question index: {st.session_state.question_index}/{len(st.session_state.interview_questions)}")

st.sidebar.write(f"API Token: {API_TOKEN[:5]}..." if API_TOKEN else "No API token found")

# Button to manually save responses
if st.sidebar.button("Save Current Interview"):
    file_path = save_responses()
    if file_path:
        st.sidebar.success(f"Interview saved to central file")

# Display current interview data
if st.sidebar.checkbox("Show current interview data"):
    st.sidebar.write("Current interview data:")
    st.sidebar.code(json.dumps(st.session_state.interview_data, indent=2))

# Display current questions
if hasattr(st.session_state, 'interview_questions') and st.sidebar.checkbox("Show questions"):
    st.sidebar.write("Interview questions:")
    for i, q in enumerate(st.session_state.interview_questions):
        st.sidebar.write(f"{i+1}. {q}")