import sqlite3
import requests
from bs4 import BeautifulSoup
import logging
import sys
import time
from datetime import datetime
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Optional, List, Dict, Any
from difflib import SequenceMatcher

class DarkReadingScraper:
    def __init__(self, db_name: str = 'news.db', site_config: Dict[str, Any] = None):
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

    def setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('darkreading_scraper.log')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger

    def setup_database(self):
        """Initialize the database with the articles table."""
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
            sys.exit(1)

    def clean_text(self, text: str) -> str:
        """Clean text for comparison by removing whitespace and lowercasing."""
        if not text:
            return ""
        text = self.remove_emojis(text)
        return ' '.join(text.lower().split())

    def is_similar_content(self, text1: str, text2: str, threshold: float = 0.85) -> bool:
        """Check if two texts are similar using SequenceMatcher."""
        return SequenceMatcher(None, self.clean_text(text1), self.clean_text(text2)).ratio() > threshold

    def is_duplicate(self, link: str, title: str, content: str) -> bool:
        """
        Check if an article is a duplicate based on:
        1. Exact link match
        2. Similar title and content match
        """
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                
                # Check for exact link match
                c.execute("SELECT link FROM articles WHERE link = ?", (link,))
                if c.fetchone():
                    self.logger.info(f"Duplicate found (exact link match): {link}")
                    return True

                # Check for similar title/content
                c.execute("SELECT title, content FROM articles WHERE source = 'darkreading'")
                existing_articles = c.fetchall()
                
                new_title_clean = self.clean_text(title)
                new_content_clean = self.clean_text(content)
                
                for existing_title, existing_content in existing_articles:
                    if self.is_similar_content(new_title_clean, existing_title, 0.9):
                        if self.is_similar_content(new_content_clean, existing_content, 0.85):
                            self.logger.info(f"Duplicate found (similar content): {link}")
                            return True
                return False
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking duplicates: {e}")
            return False

    def remove_emojis(self, text: str) -> str:
        """Remove emojis from a string."""
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE
        )
        return emoji_pattern.sub(r"", text)

    def parse_rss_feed(self, feed_content: str) -> List[Dict[str, Any]]:
        """Parse the RSS feed and return a list of articles."""
        try:
            root = ET.fromstring(feed_content)
            channel = root.find('channel')
            if channel is None:
                self.logger.error("No <channel> element found in RSS feed.")
                return []
            articles = []
            for item in channel.findall('item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                pub_date_elem = item.find('pubDate')
                description_elem = item.find('description')

                title = title_elem.text if title_elem is not None else None
                link = link_elem.text if link_elem is not None else None
                pub_date = pub_date_elem.text if pub_date_elem is not None else None
                description = description_elem.text if description_elem is not None else ''

                articles.append({
                    'link': link,
                    'title': title,
                    'published_date': pub_date,
                    'description': description
                })
            return articles
        except ET.ParseError as e:
            self.logger.error(f"Error parsing RSS feed XML: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error parsing RSS feed: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        """Scrape the full article content from a DarkReading article URL."""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            article_div = soup.find('div', class_='ArticleBase-BodyContent')
            if not article_div:
                self.logger.warning(f"Could not find article content for {url}")
                return None

            for tag in article_div.find_all(['div', 'p'], class_=['RelatedArticle', 'ContentImage-Link']):
                tag.decompose()

            paragraphs = article_div.find_all('p', class_='ContentParagraph')
            article_text = '\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())

            headers_tags = article_div.find_all(['h1', 'h2', 'h3'])
            for header in headers_tags:
                header_text = header.get_text().strip()
                if header_text:
                    article_text = header_text + "\n\n" + article_text

            return article_text

        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            return None

    def process_feed(self, feed_url: str, source_name: str):
        """Process articles from a DarkReading RSS feed."""
        try:
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            feed_content = response.text

            articles = self.parse_rss_feed(feed_content)
            seen_links = set()

            for article in articles:
                if not article.get('link'):
                    continue
                if article['link'] in seen_links:
                    continue
                seen_links.add(article['link'])

                if not all(k in article for k in ['link', 'title', 'published_date']):
                    continue

                cleaned_title = self.remove_emojis(article['title'])
                self.logger.info(f"Processing article: {cleaned_title}")

                content = self.scrape_article(article['link'])
                if not content:
                    self.logger.warning(f"Failed to scrape content for {article['link']}")
                    continue

                if self.is_duplicate(article['link'], cleaned_title, content):
                    self.logger.info("Skipping duplicate article")
                    continue

                pub_date = None
                if article['published_date']:
                    try:
                        pub_date = parsedate_to_datetime(article['published_date'])
                    except Exception as ex:
                        self.logger.warning(f"Could not parse date '{article['published_date']}': {ex}")
                        pub_date = article['published_date']

                try:
                    with sqlite3.connect(self.db_name) as conn:
                        c = conn.cursor()
                        c.execute("""
                            INSERT OR REPLACE INTO articles 
                            (link, title, published_date, content, source)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            article['link'],
                            cleaned_title,
                            pub_date,
                            content,
                            source_name
                        ))
                        conn.commit()
                    self.logger.info(f"Stored article: {cleaned_title}")

                    print(f"Link: {article['link']}")
                    print(f"Published: {article['published_date']}")
                    print(f"**{cleaned_title}**")
                    print(content.strip())
                    print("\n---\n")

                    time.sleep(1)  # Rate limit
                except sqlite3.Error as db_error:
                    self.logger.error(f"Database error while storing article: {db_error}")
                    continue

        except requests.RequestException as e:
            self.logger.error(f"Error fetching feed {feed_url}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")

def main():
    site_config = {
        "darkreading": {
            "feed_url": "https://www.darkreading.com/rss.xml"
        }
    }
    scraper = DarkReadingScraper(site_config=site_config)
    feed_url = site_config["darkreading"]["feed_url"]
    scraper.process_feed(feed_url, "darkreading")

if __name__ == "__main__":
    main()
