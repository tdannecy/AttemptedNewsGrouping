# Cybersecurity News Aggregator

A comprehensive cybersecurity news aggregation system that scrapes multiple authoritative sources, stores articles in a SQLite database, and provides an interactive Streamlit interface for viewing and analyzing the content. It also supports automated two-phase grouping of articles by category and subcategory, as well as extraction of CVE references and associated details from MITRE.

This repository includes:

- **Scrapers** for multiple cybersecurity news sources
- **SQLite database** schemas and setup
- **Automated pipelines** for:
  - Company name extraction
  - CVE mention extraction
  - CVE detail lookup from MITRE
  - Two-phase article grouping (high-level category, then subgroups)
- **Streamlit** application (`app.py`) for interactive browsing
- **Docker** configuration for easy deployment

---

## Features

1. **Multi-source News Scraping**  
   Scrapes articles from sources such as:
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

2. **Two-Phase Grouping**  
   - Phase 1: Assign articles to predefined top-level categories.
   - Phase 2: Group articles within each category into subgroups via AI analysis.

3. **CVE Extraction & Enrichment**  
   - Simple regex-based CVE detection from article text.
   - Automatic fetch of CVE details from MITRE’s CVE API.

4. **Company Name Extraction**  
   - Identify and store referenced company names using OpenAI’s LLM.

5. **Interactive Web Interface**  
   - A Streamlit app for browsing articles, filtering by date or company, and viewing CVE info and groupings.

6. **Docker Support**  
   - Deploy the entire system quickly in a Docker container.

---

## Prerequisites

- **Docker** and **Docker Compose** (if using the Docker-based approach).
- **Python 3.9+** (if running locally without Docker).
- **OpenAI API key** (required for article grouping and company extractions).  
  Make sure to set the environment variable `OPENAI_API_KEY`.
  Or you can add it to the docker-compose.yml (see example edits below).
---

## Quick Start (Docker)

1. **Clone or download** this repository.

2. **Set your OpenAI API key** in your environment. For example:
export OPENAI_API_KEY="your_openai_api_key_here"
Build & run the application via Docker Compose:
bash
Copy
docker-compose up --build
The Streamlit interface will be available at:
arduino
Copy
http://localhost:8501
