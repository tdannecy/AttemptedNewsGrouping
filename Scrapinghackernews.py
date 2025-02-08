#!/usr/bin/env python3
import argparse
import sqlite3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import logging
import sys
import time
import xml.etree.ElementTree as ET
import re
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher

class THNScraper:
    def __init__(self, db_name: str, feed_url: str, batch_size: int, rate_limit: float, log_level: str):
        self.db_name = db_name
        self.feed_url = feed_url
        self.batch_size = batch_size
        self.rate_limit = rate_limit
        self.logger = self.setup_logging(log_level)
        self.setup_database()
        self.session = self.setup_http_session()

    def setup_logging(self, log_level: str):
        logger = logging.getLogger("THNScraper")
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('thn_scraper_no_desc.log')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger

    def setup_database(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    link TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    published_date TEXT,
                    content TEXT,
                    source TEXT NOT NULL,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
            self.logger.info("Database initialized successfully.")
        except sqlite3.Error:
            self.logger.exception("Database initialization error")
            sys.exit(1)

    def setup_http_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')
        })
        return session

    def parse_rss_feed(self) -> List[Dict[str, Any]]:
        try:
            response = self.session.get(self.feed_url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            entries = []
            for item in root.findall('.//item'):
                pub_date = None
                pub_date_elem = item.find('pubDate')
                if pub_date_elem is not None and pub_date_elem.text:
                    try:
                        dt = parsedate_to_datetime(pub_date_elem.text)
                        pub_date = dt.isoformat()
                    except Exception:
                        self.logger.warning(f"Error parsing date {pub_date_elem.text}", exc_info=True)
                entry = {
                    'title': item.find('title').text if item.find('title') is not None else None,
                    'link': item.find('link').text if item.find('link') is not None else None,
                    'published_date': pub_date,
                }
                entries.append(entry)
            return entries
        except requests.RequestException:
            self.logger.exception("Error fetching RSS feed")
            return []
        except ET.ParseError:
            self.logger.exception("Error parsing RSS XML")
            return []

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            article_div = soup.find('div', {'class': 'articlebody', 'id': 'articlebody'})
            if not article_div:
                self.logger.warning(f"Article content not found for URL: {url}")
                return None

            elements_to_remove = [
                ('div', {'class': ['dog_two', 'note-b', 'stophere']}),
                ('div', {'id': ['hiddenH1']}),
                ('center', {}),
                ('div', {'class': 'separator'}),
            ]
            for tag, attrs in elements_to_remove:
                for element in article_div.find_all(tag, attrs=attrs):
                    element.decompose()

            paragraphs = [
                p.get_text().strip() for p in article_div.find_all('p') if p.get_text().strip()
            ]
            return {'content': "\n\n".join(paragraphs)}
        except requests.RequestException:
            self.logger.exception(f"Error fetching article URL: {url}")
            return None
        except Exception:
            self.logger.exception(f"Error processing article URL: {url}")
            return None

    def remove_emojis(self, text: Optional[str]) -> str:
        if not text:
            return ""
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE,
        )
        return emoji_pattern.sub(r"", text)

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        return ' '.join(text.lower().split())

    def is_similar_content(self, t1: str, t2: str, threshold: float = 0.85) -> bool:
        return SequenceMatcher(None, self.clean_text(t1), self.clean_text(t2)).ratio() > threshold

    def is_duplicate(self, link: str, title: str, content: str) -> bool:
        try:
            self.cursor.execute("SELECT link FROM articles WHERE link = ?", (link,))
            if self.cursor.fetchone():
                self.logger.info(f"Duplicate found (exact link match): {link}")
                return True

            self.cursor.execute("SELECT title, content FROM articles WHERE source = 'TheHackerNews'")
            existing_articles = self.cursor.fetchall()

            ct = self.clean_text(title)
            cc = self.clean_text(content)
            for row in existing_articles:
                existing_title = row['title']
                existing_content = row['content']
                if self.is_similar_content(ct, existing_title, 0.9):
                    if self.is_similar_content(cc, existing_content, 0.85):
                        self.logger.info(f"Duplicate found (similar content): {link}")
                        return True
            return False
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking duplicates: {e}")
            return False

    def insert_article(self, link: str, title: str, published_date: Optional[str], content: str, source: str = "TheHackerNews"):
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                VALUES (?, ?, ?, ?, ?)
            """, (link, title, published_date, content, source))
            self.conn.commit()
            self.logger.info(f"Successfully inserted article: {title}")
        except sqlite3.Error:
            self.logger.exception(f"Database insertion error for article: {title}")

    def process_all_articles(self):
        entries = self.parse_rss_feed()
        if not entries:
            self.logger.info("No entries found in feed")
            return

        self.logger.info(f"Found {len(entries)} entries in feed")
        for i in range(0, len(entries), self.batch_size):
            batch = entries[i:i + self.batch_size]
            for entry in batch:
                if not entry.get('link'):
                    continue
                title = self.remove_emojis(entry.get('title'))
                self.logger.info(f"Processing article: {title}")

                article_data = self.scrape_article(entry['link'])
                if article_data:
                    content = self.remove_emojis(article_data.get('content', ''))

                    if self.is_duplicate(entry['link'], title, content):
                        self.logger.info("Skipping duplicate article")
                        continue

                    pub_date = entry.get('published_date')
                    self.insert_article(entry['link'], title, pub_date, content)
                else:
                    self.logger.warning(f"Failed to scrape article: {entry['link']}")

                time.sleep(self.rate_limit)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            self.logger.exception("Error closing database connection")
        self.session.close()

def main():
    parser = argparse.ArgumentParser(description="The Hacker News Scraper")
    parser.add_argument("--db", type=str, default="news.db", help="SQLite database file name")
    parser.add_argument("--feed_url", type=str, default="https://feeds.feedburner.com/TheHackersNews")
    parser.add_argument("--batch_size", type=int, default=5)
    parser.add_argument("--rate_limit", type=float, default=2.0)
    parser.add_argument("--log_level", type=str, default="INFO")
    args = parser.parse_args()

    scraper = THNScraper(
        db_name=args.db,
        feed_url=args.feed_url,
        batch_size=args.batch_size,
        rate_limit=args.rate_limit,
        log_level=args.log_level
    )
    try:
        scraper.process_all_articles()
    except KeyboardInterrupt:
        scraper.logger.info("Processing interrupted by user.")
    except Exception:
        scraper.logger.exception("An unexpected error occurred.")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
