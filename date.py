#!/usr/bin/env python3
import sqlite3
from dateutil import parser
import datetime

def convert_dates(conn):
    cur = conn.cursor()
    cur.execute("SELECT link, published_date FROM articles")
    rows = cur.fetchall()

    for row in rows:
        link, published_date = row
        if not published_date:
            continue
        try:
            dt = parser.parse(published_date)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            dt_utc = dt.astimezone(datetime.timezone.utc)
            new_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            print(f"Error parsing date '{published_date}' for article with link '{link}': {e}")
            continue

        cur.execute("UPDATE articles SET published_date = ? WHERE link = ?", (new_date, link))
        print(f"Updated article (link: {link}): '{published_date}' -> '{new_date}'")

    conn.commit()

def main():
    conn = sqlite3.connect("news.db")
    try:
        convert_dates(conn)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
