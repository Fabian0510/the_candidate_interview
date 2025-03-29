# The Candidate Interview - Development Guidelines

## Build/Lint/Test Commands
- Run application: `streamlit run app.py`
- Lint Python files: `pylint *.py --disable=C0111,C0103`
- Type checking: `mypy --ignore-missing-imports *.py`
- Run Azure file upload tool: `python save_to_adls.py --account <account> --key <key> --container <container> --file <file>`
- Run candidate shortlist process: `python add_shortlist.py`
- Run website refresh process: `python refresh_website.py`

## Code Style Guidelines

### Python
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

### Azure Storage
- Store credentials in Streamlit secrets.toml or environment variables, never in code
- Handle connection errors gracefully with informative error messages
- Use consistent container/directory naming conventions

### API Interaction
- Store API tokens in environment variables or secrets
- Use descriptive variable names for API endpoints and parameters
- Handle pagination for API requests that may return large datasets
- Log API request/response details at appropriate levels