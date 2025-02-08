# main.py
import subprocess
import threading
import time
import sys
import os

def run_scraper(script_path: str):
    """Helper to run a scraper script. Raises CalledProcessError on failure."""
    print(f"--- Running {script_path} ---")
    subprocess.run([sys.executable, script_path], check=True)

def run_all_scrapers_in_threads(scraper_scripts):
    """Spawn each scraper in its own thread, wait for them all to finish."""
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

    # Join all threads
    for t in threads:
        t.join()

def main():
    # 1. Define the scripts you want to run.
    #    These are the newly refactored files from above.
    #    Adjust paths as needed if they are in a subfolder.
    scraper_scripts = [
        "bleepingcomputer.py",
        "darkreading-scraper.py",
        "krebsonsecurityscraper.py",
        "nist.py",
        "register-scraper.py",
        "schneier-scraper.py",
        "Scrapinghackernews.py",
        "securelist-scraper.py",
        "Slashdotit.py",
        "sophos.py",
        "techcrunch.py",
        "techradar.py"
    ]

    # 2. Run all scrapers (in parallel or sequentially).
    run_all_scrapers_in_threads(scraper_scripts)

    # 3. Run date.py to standardize all published_date columns.
    print("\n--- Running date.py to standardize publication dates ---")
    try:
        run_scraper("date.py")
    except subprocess.CalledProcessError as e:
        print(f"Error running date.py: {e}")

    # 4. Launch the Streamlit app.
    #    You can do so by calling `streamlit run streamlit_app.py`.
    print("\n--- Starting Streamlit interface ---")
    # On some systems you might need 'python -m streamlit ...'
    # Or simply 'streamlit run ...' if it's in your PATH.
    try:
        subprocess.run(["streamlit", "run", "streamlit_app.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error launching Streamlit app: {e}")

if __name__ == "__main__":
    main()
