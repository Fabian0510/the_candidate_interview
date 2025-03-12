import streamlit as st
import random
import time
import json
import requests
from datetime import datetime

# Streamed response emulator
def response_generator(response_text):
    for word in response_text.split():
        yield word + " "
        time.sleep(0.05)

# API configuration
API_URL = "http://20.254.105.163:8080/api/v2/tables/mpims4p3zrwsarx/records"  # Using the interview URL from original code
HEADERS = {
    'accept': 'application/json',
    'xc-token': st.secrets.get("API_TOKEN", "YOUR_TOKEN_HERE")  # Use Streamlit secrets for token
}

# Get URL parameters
role_name = st.query_params.get("role", "Unknown Role")
candidate_name = st.query_params.get("candidate", "Unknown User")
interview_id = st.query_params.get("interview_id", "No ID")

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

# Pre-set list of interview questions
interview_questions = [    
    "Can you describe your experience with content creation for social media, particularly on LinkedIn, and how you focus on lead generation?",
    "What design tools have you used to create digital-first content, and do you have experience with video content creation?",
    "How do you approach setting and measuring KPIs for social media growth, such as follower growth and engagement metrics?",
    "Can you provide examples of successful marketing strategies you've developed and executed that increased conversions and brand awareness?",
    "How do you balance reporting and analytics with creativity in your marketing campaigns, and what metrics do you typically report on?",
    "What excites you about joining a collaborative and innovative team in a growing law firm, and how do you see yourself contributing to our team culture?",
]

# Initialize chat history and question index
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": f"Good morning, {candidate_name}! Welcome to The Candidate's interview platform!"}
    ]
    st.session_state.question_index = 0
    st.session_state.interview_complete = False

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Function to submit interview data to API
def submit_interview_data():
    try:
        # Format the final data for API submission
        interview_json = json.dumps(st.session_state.interview_data, indent=2)
        
        # Log what we're sending (for debugging)
        st.sidebar.write("Sending data to API:")
        st.sidebar.code(interview_json)
        
        # Generate a random interview rank between 1 and 5
        interview_rank = random.randint(1, 5)
        
        # Prepare the payload according to the API requirements
        payload = {
            "Title": f"Interview: {candidate_name} for {role_name}",
            "Interview Portal Link": "",
            "Interview Due Date": datetime.now().strftime("%Y-%m-%d"),
            "Interview Status": "In Progress",  # Start with In Progress
            "Interview Rank": interview_rank,
            "Text": interview_json,
            "Date Added": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Interview ID": interview_id
        }
        
        # Send the initial POST API request
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        
        if response.status_code == 200:
            st.sidebar.success("✓ Interview data successfully submitted to API")
            
            # Get the record ID from the response
            response_data = response.json()
            record_id = response_data.get('id')
            
            if record_id:
                # Now update the status to Complete with a PATCH request
                update_payload = {
                    "Id": record_id,
                    "Interview Status": "Complete",
                    "Interview Rank": interview_rank
                }
                
                st.sidebar.write("Sending PATCH request to update status:")
                st.sidebar.code(json.dumps(update_payload, indent=2))
                
                patch_response = requests.patch(API_URL, headers=HEADERS, json=update_payload)
                
                if patch_response.status_code == 200:
                    st.sidebar.success(f"✓ Interview status updated to Complete with rank {interview_rank}")
                else:
                    st.sidebar.error(f"✗ Failed to update status. Status code: {patch_response.status_code}")
                    st.sidebar.error(f"Error response: {patch_response.text}")
            else:
                st.sidebar.warning("Could not find record ID in response, status not updated")
                
            return True
        else:
            st.sidebar.error(f"✗ Failed to submit data. Status code: {response.status_code}")
            st.sidebar.error(f"Error response: {response.text}")
            return False
            
    except Exception as e:
        st.sidebar.error(f"Error submitting interview data: {str(e)}")
        return False

# Function to ask the next question
def ask_next_question():
    if st.session_state.question_index < len(interview_questions):
        # Get the current question
        question = interview_questions[st.session_state.question_index]
        
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
        
        # Submit the interview data to the API
        st.session_state.interview_complete = True
        submit_interview_data()

# Start conversation with a greeting
if "greeted" not in st.session_state:
    st.session_state.greeted = True
    with st.chat_message("assistant"):
        greeting = f"We'll be asking some questions to establish your suitability for the role of {role_name}, for X (Formerly known as Twitter). Let's get started."
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
    if st.session_state.question_index > 0 and st.session_state.question_index <= len(interview_questions):
        current_question = interview_questions[st.session_state.question_index - 1]
        
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
st.sidebar.write(f"Question index: {st.session_state.question_index}/{len(interview_questions)}")

# Display current interview data
if st.sidebar.checkbox("Show interview data"):
    st.sidebar.write("Current interview data:")
    st.sidebar.code(json.dumps(st.session_state.interview_data, indent=2))