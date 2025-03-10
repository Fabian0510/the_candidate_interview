import streamlit as st
import random
import time

# Streamed response emulator
def response_generator(response_text):
    for word in response_text.split():
        yield word + " "
        time.sleep(0.05)

# Get URL parameter
role_name = st.query_params.get("role", "Unkown Role")  # Default value if no parameter
candidate_name = st.query_params.get("candidate", "Unkown User")  # Default value if no parameter

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

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Function to ask the next question
def ask_next_question():
    if st.session_state.question_index < len(interview_questions):
        question = interview_questions[st.session_state.question_index]
        with st.chat_message("assistant"):
            response = st.write_stream(response_generator(question))
        st.session_state.messages.append({"role": "assistant", "content": question})
        st.session_state.question_index += 1
    else:
        with st.chat_message("assistant"):
            final_message = "Thank you for your time! The interview is now complete."
            st.write_stream(response_generator(final_message))
        st.session_state.messages.append({"role": "assistant", "content": final_message})

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

    # Ask the next question
    ask_next_question()

# Display current role (for debugging)
st.sidebar.title("Dev Debug")
st.sidebar.write(f"Current role: {role_name}")
st.sidebar.write(f"Current candidate: {candidate_name}")
