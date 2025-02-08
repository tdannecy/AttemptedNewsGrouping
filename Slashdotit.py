import sqlite3
import requests
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from datetime import datetime
from typing import Optional, Dict, Any, List

class SlashdotITNewsScraper:
    def __init__(self,
                 db_name: str = 'news.db',
                 feed_url: str = "https://rss.slashdot.org/Slashdot/slashdotit"):
        self.db_name = db_name
        self.feed_url = feed_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/91.0.4472.124 Safari/537.36")
        })
        self.setup_database()

    def setup_database(self):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS articles (
                        link TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        published_date TIMESTAMP,
                        content TEXT NOT NULL,
                        source TEXT NOT NULL,
                        processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
        except sqlite3.Error as e:
            sys.exit(f"Database initialization error: {e}")

    def fetch_feed_entries(self) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(self.feed_url)
            entries = []
            for entry in feed.entries:
                published = entry.get('published', entry.get('dc_date', None))
                entries.append({
                    'link': entry.link,
                    'title': entry.title,
                    'published_date': published
                })
            return entries
        except Exception as e:
            print(f"Error fetching feed entries: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            content_div = None
            body_div = soup.find("div", class_="body")
            if body_div:
                content_div = body_div.find("div", class_="p")
            if not content_div:
                content_div = soup.find("div", class_="p")
            if not content_div:
                print(f"Could not locate article content at {url}")
                return None

            paragraphs = content_div.find_all("p")
            article_text = "\n\n".join(
                p.get_text().strip() for p in paragraphs if p.get_text().strip()
            )
            if not article_text:
                article_text = content_div.get_text().strip()
            return article_text if article_text else None

        except requests.RequestException as e:
            print(f"Request error while scraping {url}: {e}")
            return None
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return None

    def already_processed(self, link: str) -> bool:
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("SELECT link FROM articles WHERE link = ?", (link,))
                row = c.fetchone()
                return bool(row)
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

    def process_articles(self, limit: int = 10):
        feed_entries = self.fetch_feed_entries()
        if not feed_entries:
            print("No feed entries found.")
            return

        new_entries = []
        for entry in feed_entries:
            if not self.already_processed(entry['link']):
                new_entries.append(entry)
            if len(new_entries) >= limit:
                break

        if not new_entries:
            print("No new articles to process.")
            return

        for entry in new_entries:
            print(f"\nProcessing article: {entry['title']}")
            content = self.scrape_article(entry['link'])
            if not content:
                print(f"Failed to retrieve content for {entry['link']}\n")
                continue

            pub_date = entry['published_date'] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with sqlite3.connect(self.db_name) as conn:
                    c = conn.cursor()
                    c.execute("""
                        INSERT OR REPLACE INTO articles
                        (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        entry['link'],
                        entry['title'],
                        pub_date,
                        content,
                        "slashdot_it"
                    ))
                    conn.commit()

                print("Title:", entry['title'])
                print("Link:", entry['link'])
                print("Published:", pub_date)
                print("\nContent Preview:")
                print(content[:500] + "...\n")
                print("-" * 80 + "\n")

                time.sleep(2)
            except sqlite3.Error as db_error:
                print(f"Database error while storing article: {db_error}")
                continue

def main():
    scraper = SlashdotITNewsScraper()
    scraper.process_articles(limit=10)

if __name__ == "__main__":
    main()
