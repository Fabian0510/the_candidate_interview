# The Candidate Interview Platform

This repository contains tools and utilities for managing candidate CVs, interviews, and recruitment processes.

## Core Components

### CV Download and Upload Webhook (`download_and_upload_cvs.py`)

A dual-purpose script that functions as both a standalone tool and a webhook server for CV management.

#### Features:
- Downloads candidate CVs from the API for each role
- Organizes CVs into role-specific directories
- Uploads CVs to Azure Blob Storage
- Provides a webhook endpoint for automation
- Logs all operations for auditing and troubleshooting

#### Usage as a Webhook:
```bash
# Start the webhook server
python download_and_upload_cvs.py --webhook

# Custom port (default: 8888)
PORT=9000 python download_and_upload_cvs.py --webhook
```

#### Webhook API:
- **Endpoint**: `/cv_uploader`
- **Method**: POST
- **Response**: JSON with statistics:
  ```json
  {
    "success": true,
    "cvs_downloaded": 10,
    "cvs_uploaded": 10
  }
  ```

#### Usage as a Script:
```bash
python download_and_upload_cvs.py
```

#### Logging:
Logs are written to `cv_process.log` and include:
- Process start/completion timestamps
- Download statistics
- Upload confirmations
- Error details when operations fail

### Candidate Shortlisting (`add_shortlist.py`)

Processes candidate information to identify and tag shortlisted candidates.

```bash
python add_shortlist.py
```

### Interview Scheduling (`interview_scheduler.log`)

Logs of interview scheduling activities and operations.

### Azure Storage Utilities (`save_to_adls.py`)

Tool for uploading files to Azure Data Lake Storage.

```bash
python save_to_adls.py --account <account> --key <key> --container <container> --file <file>
```

### Web Application (`app.py`)

Streamlit-based web interface for the platform.

```bash
streamlit run app.py
```

### Website Refresh (`refresh_website.py`)

Updates the public-facing website with current information.

```bash
python refresh_website.py
```

## Development and Maintenance

### Build/Lint/Test Commands
- Run application: `streamlit run app.py`
- Lint Python files: `pylint *.py --disable=C0111,C0103`
- Type checking: `mypy --ignore-missing-imports *.py`

### Code Style Guidelines

#### Python
- Indentation: 4 spaces
- Line length: 100 characters max
- Naming: snake_case for variables/functions, UPPERCASE for constants
- Imports: 
  - Standard library first
  - Third-party packages next
  - Local modules last
  - Use explicit imports (from x import y) for clarity
- Type annotations: Use for function parameters and return values
- Documentation: Google-style docstrings with Args and Returns sections
- Error handling: Use specific exceptions with descriptive messages in try/except blocks
- Logging: Configure with both file and stream handlers, use appropriate log levels

#### Azure Storage
- Store credentials in Streamlit secrets.toml or environment variables, never in code
- Handle connection errors gracefully with informative error messages
- Use consistent container/directory naming conventions

#### API Interaction
- Store API tokens in environment variables or secrets
- Use descriptive variable names for API endpoints and parameters
- Handle pagination for API requests that may return large datasets
- Log API request/response details at appropriate levels

## Project Structure

```
/
├── app.py                       # Main Streamlit application
├── download_and_upload_cvs.py   # CV downloader and uploader (script and webhook)
├── add_shortlist.py             # Candidate shortlisting tool
├── save_to_adls.py              # Azure Data Lake Storage utility
├── refresh_website.py           # Website refresh utility
├── interview_file_save.py       # Interview file handler
├── interview_questions.txt      # Standard interview questions
├── CLAUDE.md                    # Development guidelines
├── *.log                        # Log files for various operations
└── role_cvs/                    # Downloaded CV files organized by role
    ├── Account Manager/
    ├── Brand Manager/
    ├── Digital Marketing Assistant/
    └── ...
```

## Requirements

- Python 3.7+
- Flask (for webhook functionality)
- Streamlit
- Azure Storage Blob client
- Requests library
- TOML parser