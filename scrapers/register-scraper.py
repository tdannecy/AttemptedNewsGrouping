import sqlite3
import requests
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher

class RegisterScraper:
    def __init__(self, 
                 db_name: str = 'db/news.db', 
                 feed_url: str = "https://www.theregister.com/headlines.atom"):
        self.db_name = db_name
        self.feed_url = feed_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')
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

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        return ' '.join(text.lower().split())

    def is_similar_content(self, t1: str, t2: str, threshold: float = 0.85) -> bool:
        return SequenceMatcher(None, self.clean_text(t1), self.clean_text(t2)).ratio() > threshold

    def is_duplicate(self, link: str, title: str, content: str) -> bool:
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("SELECT link FROM articles WHERE link = ?", (link,))
                if c.fetchone():
                    print(f"Duplicate found (exact link match): {link}")
                    return True

                c.execute("SELECT title, content FROM articles WHERE source = 'register'")
                existing = c.fetchall()

                ct = self.clean_text(title)
                cc = self.clean_text(content)
                for et, ec in existing:
                    if self.is_similar_content(ct, et, 0.9):
                        if self.is_similar_content(cc, ec, 0.85):
                            print(f"Duplicate found (similar content): {link}")
                            return True
                return False
        except sqlite3.Error as e:
            print(f"Database error while checking duplicates: {e}")
            return False

    def fetch_register_feed_entries(self) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(self.feed_url)
            entries = []
            for entry in feed.entries:
                link = getattr(entry, 'link', None)
                title = getattr(entry, 'title', "No Title")
                published = getattr(entry, 'published', getattr(entry, 'updated', None))
                if link:
                    entries.append({
                        'link': link,
                        'title': title,
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
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            article_div = soup.find('div', id='article')
            if not article_div:
                return None
            
            body_div = article_div.find('div', id='body')
            if not body_div:
                return None

            for div in body_div.find_all('div', class_=['adun', 'wptl', 'listinks']):
                div.decompose()

            paragraphs = body_div.find_all('p')
            article_text = '\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            return article_text if article_text else None

        except requests.RequestException as e:
            print(f"Request error while scraping {url}: {e}")
            return None
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return None

    def process_register_articles(self, limit: int = 10):
        feed_entries = self.fetch_register_feed_entries()
        if not feed_entries:
            print("No feed entries found.")
            return

        processed_count = 0
        for entry in feed_entries:
            if processed_count >= limit:
                break

            print(f"\nProcessing article: {entry['title']}")
            content = self.scrape_article(entry['link'])
            if not content:
                print(f"Failed to scrape content for {entry['link']}")
                continue

            if self.is_duplicate(entry['link'], entry['title'], content):
                print("Skipping duplicate article")
                continue

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
                        entry['published_date'],
                        content,
                        "register"
                    ))
                    conn.commit()
                
                print(f"Stored article: {entry['title']}")
                print(f"Link: {entry['link']}")
                print(f"Published: {entry['published_date']}")
                print("Content Preview:")
                print(content[:500] + "...\n")
                print("-" * 80)
                
                processed_count += 1
                time.sleep(2)
            except sqlite3.Error as db_error:
                print(f"Database error while storing article: {db_error}")
                continue

def main():
    scraper = RegisterScraper()
    scraper.process_register_articles(limit=100)

if __name__ == "__main__":
    main()
