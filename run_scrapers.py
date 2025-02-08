import subprocess
import time

def run_scraper(script_path):
    """
    Runs the given scraper script using subprocess.
    If the script exits with an error, the error is printed.
    """
    try:
        # You can modify the command list if your scraper needs arguments.
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")

def main():
    # List the paths (or names if in the same directory) of your scraper scripts.
    scraper_scripts = [
        "register-scraper.py",
        "Scrapinghackernews.py",
        "darkreading-scraper.py",
        "krebsonsecurityscraper.py",
        "schneier-scraper.py",
        "techradar.py",
        "bleepingcomputer.py",
        "securelist-scraper.py",
        "sophos.py",
        "date.py",
        "summary.py"
    ]
    
    print("Starting scraper loop. Each scraper will run once every 60 seconds.\n")
    while True:
        print("=== Running all scrapers ===")
        for script in scraper_scripts:
            print(f"\n--- Running {script} ---")
            run_scraper(script)
        print("\nAll scrapers have run. Waiting 60 seconds before the next run...\n")
        time.sleep(60)

if __name__ == "__main__":
    main()
