# Cybersecurity News Aggregator

A comprehensive cybersecurity news aggregation system that scrapes multiple authoritative sources, stores articles in a SQLite database, and provides an interactive Streamlit interface for viewing and analyzing the content.

## Features

- Multi-source news scraping from reputable cybersecurity websites:
  - BleepingComputer
  - Dark Reading
  - Krebs on Security
  - NIST Cybersecurity
  - The Register
  - Schneier on Security
  - The Hacker News
  - Securelist
  - Slashdot
  - Sophos
  - TechCrunch
  - TechRadar

- Automated article grouping and analysis using OpenAI's API
- Interactive Streamlit dashboard for viewing and managing content
- Docker support for easy deployment
- Automatic date standardization
- Duplicate detection and filtering
- Rate limiting and respectful scraping practices

## Prerequisites

- Docker and Docker Compose
- OpenAI API key (for article grouping features)

## Quick Start

1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Build and run using Docker Compose:
```bash
docker-compose up --build
```

The application will be available at `http://localhost:8501`

## Configuration

- OpenAI API key can be entered directly in the Streamlit interface

## Architecture

The system consists of several components:

1. **Scrapers**: Individual Python scripts for each news source
2. **Database**: SQLite database for storing articles and their metadata
3. **Date Standardization**: Ensures consistent datetime formats
4. **Article Grouping**: Two approaches:
   - Single-step grouping
   - Two-phase grouping
5. **Web Interface**: Streamlit dashboard for interaction and visualization

## System Flow

1. Scrapers collect articles from various sources
2. Articles are stored in the SQLite database
3. Date standardization is performed
4. Articles can be grouped using AI-powered analysis
5. Content is viewable and manageable through the Streamlit interface

## Project Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── main.py
├── date.py
├── streamlit_app.py
├── run_scrapers.py
└── scrapers/
    ├── bleepingcomputer.py
    ├── darkreading-scraper.py
    ├── krebsonsecurityscraper.py
    ├── nist.py
    ├── register-scraper.py
    ├── schneier-scraper.py
    ├── Scrapinghackernews.py
    ├── securelist-scraper.py
    ├── Slashdotit.py
    ├── sophos.py
    ├── techcrunch.py
    └── techradar.py
```

## Manual Setup (Without Docker)

If you prefer to run without Docker:

1. Create a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python main.py
```

