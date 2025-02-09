import sqlite3
import time
from datetime import datetime

def get_connection(db_path="db/news.db"):
    """
    Returns a new connection to the SQLite database.
    """
    return sqlite3.connect(db_path)

def setup_database(db_path="db/news.db"):
    """
    Create all necessary tables in the SQLite database.
    (Call this once at startup or whenever you need to ensure the schema exists.)
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Articles table (assumes you have this table in your schema)
    # Adjust as needed if you store articles differently
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        link TEXT PRIMARY KEY,
        title TEXT,
        content TEXT,
        published_date TIMESTAMP
    )
    """)

    # -------------------------------
    # Two-phase grouping tables
    # -------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS two_phase_article_groups (
        group_id INTEGER PRIMARY KEY AUTOINCREMENT,
        main_topic TEXT NOT NULL,
        sub_topic TEXT NOT NULL,
        group_label TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS two_phase_article_group_memberships (
        article_link TEXT NOT NULL,
        group_id INTEGER NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (group_id) REFERENCES two_phase_article_groups (group_id),
        PRIMARY KEY (article_link, group_id)
    )
    """)

    # -------------------------------
    # Subgroup tables
    # -------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS two_phase_subgroups (
        subgroup_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL,
        group_label TEXT NOT NULL,
        summary TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS two_phase_subgroup_memberships (
        article_link TEXT NOT NULL,
        subgroup_id INTEGER NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (subgroup_id) REFERENCES two_phase_subgroups (subgroup_id),
        PRIMARY KEY (article_link, subgroup_id)
    )
    """)

    # -------------------------------
    # Company references
    # -------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_companies (
        article_link TEXT NOT NULL,
        company_name TEXT NOT NULL,
        PRIMARY KEY(article_link, company_name)
    )
    """)

    # -------------------------------
    # CVE references + CVE info
    # -------------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_cves (
        article_link TEXT NOT NULL,
        cve_id TEXT NOT NULL,
        published_date TIMESTAMP,
        PRIMARY KEY (article_link, cve_id)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cve_info (
        cve_id TEXT PRIMARY KEY,
        base_score REAL,
        vendor TEXT,
        affected_products TEXT,
        cve_url TEXT,
        vendor_link TEXT,
        solution TEXT,
        times_mentioned INTEGER DEFAULT 0,
        raw_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

#
# Below you can add optional "getter" or "setter" functions that encapsulate queries
# for articles, groups, etc. For example:
#

def insert_article_company(article_link, company_name, db_path="db/news.db"):
    """
    Insert or ignore a (article_link, company_name) pair into article_companies.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO article_companies (article_link, company_name)
            VALUES (?, ?)
        """, (article_link, company_name))
        conn.commit()
    finally:
        conn.close()

def insert_article_cve(article_link, cve_id, published_date, db_path="db/news.db"):
    """
    Insert or ignore a (article_link, cve_id, published_date) record into article_cves.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO article_cves (article_link, cve_id, published_date)
            VALUES (?, ?, ?)
        """, (article_link, cve_id, published_date))
        conn.commit()
    finally:
        conn.close()

def insert_or_update_cve_info(cve_id,
                              base_score,
                              vendor,
                              affected_products,
                              cve_url,
                              vendor_link,
                              solution,
                              times_mentioned,
                              raw_json_str,
                              db_path="db/news.db"):
    """
    Insert or update the cve_info table with the given details.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO cve_info (
                cve_id,
                base_score,
                vendor,
                affected_products,
                cve_url,
                vendor_link,
                solution,
                times_mentioned,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cve_id) DO UPDATE SET
                base_score=excluded.base_score,
                vendor=excluded.vendor,
                affected_products=excluded.affected_products,
                cve_url=excluded.cve_url,
                vendor_link=excluded.vendor_link,
                solution=excluded.solution,
                times_mentioned=excluded.times_mentioned,
                raw_json=excluded.raw_json,
                updated_at=CURRENT_TIMESTAMP
        """, (
            cve_id,
            base_score,
            vendor,
            affected_products,
            cve_url,
            vendor_link,
            solution,
            times_mentioned,
            raw_json_str
        ))
        conn.commit()
    finally:
        conn.close()

# Add more DB helper functions here if needed...
