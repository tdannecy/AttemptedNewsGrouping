import sqlite3
import requests
from bs4 import BeautifulSoup
import logging
from typing import Optional, Dict, Any, List
import time
import sys
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher

class CybersecurityScraper:
    def __init__(self,
                 db_name: str = 'news.db',
                 site_config: Dict[str, Any] = None):
        self.db_name = db_name
        self.site_config = site_config or {}
        self.logger = self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')
        })
        self.setup_database()

    def setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('cybersec_scraper.log')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

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
                        content TEXT,
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

    def is_similar_content(self, text1: str, text2: str, threshold: float = 0.85) -> bool:
        return SequenceMatcher(None, self.clean_text(text1), self.clean_text(text2)).ratio() > threshold

    def is_duplicate(self, link: str, title: str, content: str, source_name: str) -> bool:
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("SELECT link FROM articles WHERE link = ?", (link,))
                if c.fetchone():
                    self.logger.info(f"Duplicate found (exact link match): {link}")
                    return True

                c.execute("SELECT title, content FROM articles WHERE source = ?", (source_name,))
                existing_articles = c.fetchall()

                ct = self.clean_text(title)
                cc = self.clean_text(content)

                for et, ec in existing_articles:
                    if self.is_similar_content(ct, et, 0.9):
                        if self.is_similar_content(cc, ec, 0.85):
                            self.logger.info(f"Duplicate found (similar content): {link}")
                            return True
                return False
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking duplicates: {e}")
            return False

    def parse_atom_feed(self, feed_content: str) -> List[Dict[str, Any]]:
        try:
            root = ET.fromstring(feed_content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = []

            for entry in root.findall('atom:entry', ns):
                link_elem = entry.find('atom:link[@rel="alternate"]', ns)
                link = link_elem.get('href') if link_elem is not None else None

                title_elem = entry.find('atom:title', ns)
                title = title_elem.text if title_elem is not None else None

                published_elem = entry.find('atom:published', ns)
                published = published_elem.text if published_elem is not None else None

                content = ''
                content_elem = entry.find('atom:content', ns)
                if content_elem is not None:
                    if content_elem.get('type') == 'html':
                        soup = BeautifulSoup(content_elem.text or '', 'html.parser')
                        content_parts = []
                        for tag in soup.find_all(['p', 'blockquote'], recursive=False):
                            if not any(cls in (tag.get('class') or []) for cls in ['entry-tags', 'posted']):
                                content_parts.append(tag.get_text().strip())
                        content = '\n'.join(filter(None, content_parts))
                    else:
                        content = content_elem.text or ''

                if link:
                    entries.append({
                        'link': link,
                        'title': title,
                        'published_date': published,
                        'content': content
                    })
            return entries
        except ET.ParseError as e:
            self.logger.error(f"Error parsing feed XML: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error parsing feed: {e}")
            return []

    def process_feed(self, feed_url: str, source_name: str):
        try:
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            articles = self.parse_atom_feed(response.text)

            seen_links = set()
            for article in articles:
                if article['link'] in seen_links:
                    continue
                seen_links.add(article['link'])

                if not all(k in article for k in ['link', 'title', 'published_date']):
                    continue

                if self.is_duplicate(article['link'], article['title'], article['content'], source_name):
                    self.logger.info(f"Skipping duplicate article: {article['link']}")
                    continue

                with sqlite3.connect(self.db_name) as conn:
                    c = conn.cursor()
                    c.execute("""
                        INSERT OR REPLACE INTO articles
                        (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        article['link'],
                        article['title'],
                        article['published_date'],
                        article['content'],
                        source_name
                    ))
                    conn.commit()
                self.logger.info(f"Stored article: {article['title']}")

                print(f"Link: {article['link']}")
                print(f"Published: {article['published_date']}")
                print(f"**{article['title']}**")
                print(article['content'].strip())
                print("\n---\n")

                time.sleep(1)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching feed {feed_url}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")

def main():
    site_configs = {
        "schneier": {
            "feed_url": "https://www.schneier.com/feed/atom/"
        }
    }
    scraper = CybersecurityScraper(site_config=site_configs)
    scraper.process_feed(site_configs["schneier"]["feed_url"], "schneier")

if __name__ == "__main__":
    main()
