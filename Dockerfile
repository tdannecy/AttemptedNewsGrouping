# Start from a minimal Python image
FROM python:3.9-slim

# Create a working directory
WORKDIR /app

# Copy in requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase into the container
COPY . /app

# Expose the default Streamlit port
EXPOSE 8501

# By default, run "main.py" which:
#  - Spawns scraper threads
#  - Runs date.py
#  - Launches the Streamlit app
CMD ["python", "main.py"]
