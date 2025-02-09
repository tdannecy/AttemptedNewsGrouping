# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your entire codebase into /app
COPY . /app

# Expose the port Streamlit runs on
EXPOSE 8501

# Default command to run your main script (which launches Streamlit)
CMD ["python", "main.py"]