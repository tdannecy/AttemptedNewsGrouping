import sqlite3
import requests
import feedparser
from bs4 import BeautifulSoup
import logging
from typing import Optional, Dict, Any, List
import time
import sys
import re
from difflib import SequenceMatcher

class SecurelistProcessor:
    def __init__(self, db_name: str = 'news.db'):
        self.db_name = db_name
        self.feed_url = "https://securelist.com/feed/"
        self.logger = self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.setup_database()

    def setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler('securelist_scraper.log')
        fh.setFormatter(fmt)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)

        if not logger.handlers:
            logger.addHandler(fh)
            logger.addHandler(ch)
        return logger

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
            self.logger.error(f"Database initialization error: {e}")
            raise

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
                    self.logger.info(f"Duplicate found (exact link match): {link}")
                    return True

                c.execute("SELECT title, content FROM articles WHERE source = 'securelist'")
                existing = c.fetchall()

                ct = self.clean_text(title)
                cc = self.clean_text(content)
                for et, ec in existing:
                    if self.is_similar_content(ct, et, 0.9):
                        if self.is_similar_content(cc, ec, 0.85):
                            self.logger.info(f"Duplicate found (similar content): {link}")
                            return True
                return False
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking duplicates: {e}")
            return False

    def fetch_feed(self) -> List[Dict]:
        try:
            self.logger.info(f"Fetching RSS feed from {self.feed_url}")
            response = self.session.get(self.feed_url, timeout=10)
            response.raise_for_status()
            feed = feedparser.parse(response.text)

            if feed.bozo != 0:
                self.logger.error(f"Feed parsing error: {feed.bozo_exception}")
                return []

            articles = []
            for entry in feed.entries:
                article = {
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'date': entry.get('published', '')
                }
                articles.append(article)
            self.logger.info(f"Found {len(articles)} articles in feed")
            return articles
        except requests.RequestException as e:
            self.logger.error(f"Error fetching feed: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            article_div = soup.find('div', class_='js-reading-content')
            if not article_div:
                self.logger.warning(f"Could not find article content for {url}")
                return None

            content_div = article_div.find('div', class_='c-wysiwyg')
            if not content_div:
                self.logger.warning(f"Could not find content div for {url}")
                return None

            for element in content_div.find_all('div', class_=['wp-caption', 'js-infogram-embed']):
                element.decompose()

            content_elements = content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            article_text = ""
            for element in content_elements:
                text = element.get_text().strip()
                if text:
                    if element.name.startswith('h'):
                        article_text += f"\n\n{text}\n"
                    else:
                        article_text += f"{text}\n"

            return article_text.strip()
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            return None

    def process_article(self, article: Dict):
        link = article['link']
        title = article['title']
        published_date = article['date']

        self.logger.info(f"Processing: {title}")
        content = self.scrape_article(link)
        if not content:
            return

        if self.is_duplicate(link, title, content):
            self.logger.info(f"Skipping duplicate article: {title}")
            return

        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT OR REPLACE INTO articles
                    (link, title, published_date, content, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    link,
                    title,
                    published_date,
                    content,
                    "securelist"
                ))
                conn.commit()
            self.logger.info(f"Stored article: {title}")
            print(f"Link: {link}")
            print(f"Published: {published_date}")
            print(f"**{title}**")
            print(content.strip())
            print("\n---\n")
        except sqlite3.Error as e:
            self.logger.error(f"Database error storing article {title}: {e}")

    def process_all_articles(self, limit: int = 100):
        try:
            articles = self.fetch_feed()
            if not articles:
                self.logger.info("No articles fetched from feed.")
                return

            processed_count = 0
            for article in articles:
                if processed_count >= limit:
                    self.logger.info(f"Reached processing limit of {limit} articles.")
                    break

                self.process_article(article)
                processed_count += 1
                time.sleep(2)

            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM articles WHERE source = 'securelist'")
                total_count = c.fetchone()[0]
                self.logger.info("\nFinal Statistics:")
                self.logger.info(f"Total articles in database from Securelist: {total_count}")

        except Exception as e:
            self.logger.error(f"Error processing articles: {e}")

def main():
    processor = SecurelistProcessor()
    processor.process_all_articles(limit=100)

if __name__ == "__main__":
    main()
