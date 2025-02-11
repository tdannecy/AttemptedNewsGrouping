# Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase into the container
COPY . /app

# Expose Streamlit port
EXPOSE 8501

# Default command to run the 'main.py' script
# which eventually calls "streamlit run app.py"
CMD ["python", "main.py"]
