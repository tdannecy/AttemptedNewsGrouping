# main.py
import subprocess
import sys
import threading
import os

# Import the pipeline function from pipeline.py
from pipeline import run_full_pipeline_headless

import os
api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    print("Error: OPENAI_API_KEY environment variable not found")
    sys.exit(1)
logs = run_full_pipeline_headless(api_key=api_key, db_path="db/news.db")

def run_scraper(script_path: str):
    print(f"--- Running {script_path} ---")
    subprocess.run([sys.executable, script_path], check=True)

def run_all_scrapers_in_threads(scraper_scripts):
    threads = []

    def wrapper(script):
        try:
            run_scraper(script)
        except subprocess.CalledProcessError as e:
            print(f"Error running {script}: {e}")

    for script in scraper_scripts:
        t = threading.Thread(target=wrapper, args=(script,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

def main():
    # 1) Define the scraper scripts
    scraper_scripts = [
        "scrapers/bleepingcomputer.py",
        "scrapers/darkreading-scraper.py",
        "scrapers/krebsonsecurityscraper.py",
        "scrapers/nist.py",
        "scrapers/register-scraper.py",
        "scrapers/schneier-scraper.py",
        "scrapers/Scrapinghackernews.py",
        "scrapers/securelist-scraper.py",
        "scrapers/Slashdotit.py",
        "scrapers/sophos.py",
        "scrapers/techcrunch.py",
        "scrapers/techradar.py",
    ]

    # 2) Run all scrapers
    run_all_scrapers_in_threads(scraper_scripts)

    # 3) Run date.py to standardize publication dates
    print("\n--- Running date.py to standardize publication dates ---")
    try:
        run_scraper("date.py")
    except subprocess.CalledProcessError as e:
        print(f"Error running date.py: {e}")

    # 4) Automatically run the pipeline (assuming your pipeline doesn’t need an API key now)
    print("\n--- Running the full pipeline (headless) ---")
    logs = run_full_pipeline_headless(db_path="db/news.db")
    for line in logs:
        print(line)
    print("--- Finished pipeline ---")

    # 5) Launch Streamlit UI for browsing
    print("\n--- Starting Streamlit interface ---")
    try:
        subprocess.run(["streamlit", "run", "app.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error launching Streamlit app: {e}")

if __name__ == "__main__":
    main()
