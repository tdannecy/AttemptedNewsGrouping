# main.py
import subprocess
import sys
import os
import time
import threading

from pipeline import run_full_pipeline_headless

# Reuse your existing functions for running scrapers in threads:
# (e.g., from the old main.py code)
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

def run_full_cycle(api_key):
    """
    Runs the entire cycle:
      1) Scrape all sources
      2) Standardize dates
      3) Run the pipeline
    """
    # 1) Run all scrapers in parallel
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
    run_all_scrapers_in_threads(scraper_scripts)

    # 2) Standardize publication dates
    print("\n--- Running date.py to standardize publication dates ---")
    run_scraper("date.py")

    # 3) Run the pipeline (company extraction, CVE, grouping, etc.)
    print("\n--- Running the full pipeline (headless) ---")
    logs = run_full_pipeline_headless(api_key=api_key, db_path="db/news.db")
    for line in logs:
        print(line)
    print("--- Finished pipeline cycle ---")

def background_loop(api_key):
    """
    Background loop that repeats the entire pipeline every 15 minutes.
    """
    while True:
        run_full_cycle(api_key)
        # Sleep 15 minutes
        time.sleep(15 * 60)

def main():
    # 1) Read API key from environment
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable not found")
        sys.exit(1)

    # 2) Launch the background thread that continuously updates everything
    t = threading.Thread(target=background_loop, args=(api_key,), daemon=True)
    t.start()

    # 3) Immediately run Streamlit so the UI is up right away
    print("\n--- Starting Streamlit interface ---")
    try:
        subprocess.run(["streamlit", "run", "app.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error launching Streamlit app: {e}")

if __name__ == "__main__":
    main()
