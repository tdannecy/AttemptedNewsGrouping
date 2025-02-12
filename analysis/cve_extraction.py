import requests
import json
import re
from datetime import datetime, timedelta
import pytz
import sqlite3
import math
from db.database import (
    get_connection,
    insert_article_cve,
    insert_or_update_cve_info
)
from utils import extract_cves

#
# Regex for CVE detection
#
CVE_REGEX = r'\bCVE-\d{4}-\d{4,7}\b'

def process_cves_in_articles(db_path="db/news.db"):
    """
    - For each article in 'articles', extract CVE numbers with a simple regex.
    - Insert each CVE mention into 'article_cves' with (article_link, cve_id, published_date).
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Fetch all articles
    cursor.execute("SELECT link, published_date, content FROM articles")
    articles = cursor.fetchall()
    conn.close()

    total_found = 0
    for link, published_date, content in articles:
        found_cves = extract_cves(content or "")
        if not found_cves:
            continue
        for cve in found_cves:
            insert_article_cve(link, cve, published_date, db_path=db_path)
            total_found += 1

    print(f"Finished processing CVEs in articles. Inserted {total_found} new CVE references.")

def build_cve_table(date_hours=None, db_path="db/news.db"):
    """
    Return a list or DataFrame with columns from article_cves (times seen, date range)
    plus cve_info (base score, vendor, products, etc.).
    If you use pandas, you can return a DataFrame directly.
    """
    import pandas as pd

    conn = get_connection(db_path)
    c = conn.cursor()

    # Filter by date if date_hours is provided
    if date_hours is not None:
        cutoff_utc = datetime.now(pytz.UTC) - timedelta(hours=date_hours)
        cutoff_str = cutoff_utc.isoformat()
        query = """
            SELECT ac.cve_id, ac.article_link, a.published_date
            FROM article_cves ac
            JOIN articles a ON ac.article_link = a.link
            WHERE a.published_date >= ?
        """
        rows = c.execute(query, (cutoff_str,)).fetchall()
    else:
        query = """
            SELECT ac.cve_id, ac.article_link, a.published_date
            FROM article_cves ac
            JOIN articles a ON ac.article_link = a.link
        """
        rows = c.execute(query).fetchall()

    # cve_info
    cve_info_rows = c.execute("SELECT * FROM cve_info").fetchall()
    cve_info_cols = [desc[0] for desc in c.description]
    conn.close()

    if not rows:
        empty_cols = [
            "CVE ID", "Times Seen", "First Mention", "Last Mention", "Articles",
            "Base Score", "Vendor", "Affected Products", "CVE Page Link",
            "Vendor Link", "Solution"
        ]
        return pd.DataFrame(columns=empty_cols)

    df = pd.DataFrame(rows, columns=["cve_id", "article_link", "published_date"])
    df["published_date"] = pd.to_datetime(df["published_date"], utc=True, errors="coerce")

    # Convert cve_info_rows to a dict
    cve_info_dict = {}
    for row in cve_info_rows:
        rec = dict(zip(cve_info_cols, row))
        cve_info_dict[rec["cve_id"]] = rec

    # Group by cve_id
    cve_groups = df.groupby("cve_id")
    table_rows = []
    for cve_id, group in cve_groups:
        article_links = group["article_link"].unique().tolist()
        times_seen = len(article_links)
        first_mention = group["published_date"].min()
        last_mention = group["published_date"].max()

        link_list_str = ""
        for link in article_links:
            link_list_str += f"- {link}\n"

        info = cve_info_dict.get(cve_id, {})
        base_score = info.get("base_score")
        vendor = info.get("vendor", "")
        products = info.get("affected_products", "")
        cve_page_link = info.get("cve_url", "")
        vendor_link = info.get("vendor_link", "")
        solution = info.get("solution", "")

        table_rows.append({
            "CVE ID": cve_id,
            "Times Seen": times_seen,
            "First Mention": first_mention,
            "Last Mention": last_mention,
            "Articles": link_list_str.strip(),
            "Base Score": base_score if base_score is not None else math.nan,
            "Vendor": vendor,
            "Affected Products": products,
            "CVE Page Link": cve_page_link,
            "Vendor Link": vendor_link,
            "Solution": solution
        })

    result_df = pd.DataFrame(table_rows)
    result_df = result_df.sort_values(by=["Times Seen","CVE ID"], ascending=[False, True])
    return result_df


def update_cve_details_from_api(db_path="db/news.db"):
    """
    For each unique CVE in article_cves:
      1) Look up the CVE details via the CVE Mitre API (https://cveawg.mitre.org/api/cve/).
      2) Parse out details.
      3) Count mentions from article_cves (store as times_mentioned).
      4) Insert/update the details into the cve_info table.
    """
    conn = get_connection(db_path)
    c = conn.cursor()

    # 1) Gather all unique CVE IDs from article_cves
    c.execute("SELECT DISTINCT cve_id FROM article_cves")
    all_cves = [row[0] for row in c.fetchall()]

    # Pre-build times_mentioned from article_cves
    times_mentioned_map = {}
    rows = c.execute("""
        SELECT cve_id, COUNT(*) as cnt
        FROM article_cves
        GROUP BY cve_id
    """).fetchall()
    for r in rows:
        times_mentioned_map[r[0]] = r[1]

    updated_count = 0

    for cve_id in all_cves:
        url = f"https://cveawg.mitre.org/api/cve/{cve_id}"
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                continue
            if data.get("message") == "CVE not found":
                continue
        except Exception:
            continue

        is_new_format = (data.get("dataType") == "CVE_RECORD")
        base_score = None
        vendor_str = ""
        products_str = ""
        cve_page_link = f"https://cveawg.mitre.org/cve/{cve_id}"
        vendor_link = ""
        solution_str = ""

        if is_new_format:
            cna_data = data.get("containers", {}).get("cna", {})
            # (1) Attempt to find a base_score from the "metrics"
            metrics_list = []
            if isinstance(cna_data.get("metrics"), list):
                metrics_list.extend(cna_data["metrics"])
            adp_list = data.get("containers", {}).get("adp", [])
            if isinstance(adp_list, list):
                for adp_item in adp_list:
                    if isinstance(adp_item.get("metrics"), list):
                        metrics_list.extend(adp_item["metrics"])

            for m in metrics_list:
                for cvss_key in ["cvssV4_0", "cvssV3_1", "cvssV3_0", "cvssV2_0"]:
                    if cvss_key in m and isinstance(m[cvss_key], dict):
                        maybe_score = m[cvss_key].get("baseScore")
                        if maybe_score:
                            try:
                                base_score = float(maybe_score)
                                break
                            except:
                                pass
                if base_score is not None:
                    break

            # (2) Vendors / products
            affected_list = cna_data.get("affected", [])
            all_vendors = set()
            all_products = set()
            for aff in affected_list:
                v = aff.get("vendor", "")
                p = aff.get("product", "")
                if v:
                    all_vendors.add(v)
                if p:
                    all_products.add(p)
            vendor_str = ", ".join(sorted(all_vendors))
            products_str = ", ".join(sorted(all_products))

            # (3) References (try to find vendor link)
            references_list = cna_data.get("references", [])
            for ref in references_list:
                tags = ref.get("tags", [])
                url_ref = ref.get("url", "")
                if "vendor-advisory" in tags or "vendor" in url_ref.lower():
                    vendor_link = url_ref
                    break
            if not vendor_link and references_list:
                vendor_link = references_list[0].get("url", "")

            # (4) Solutions
            solutions_list = cna_data.get("solutions", [])
            if solutions_list:
                solution_texts = []
                for sol in solutions_list:
                    val = sol.get("value", "")
                    if val:
                        solution_texts.append(val)
                solution_str = "\n\n".join(solution_texts)
        else:
            # If not new format, skip or parse differently if needed
            continue

        mention_count = times_mentioned_map.get(cve_id, 0)
        raw_json_str = json.dumps(data)

        # Insert or update DB
        insert_or_update_cve_info(
            cve_id=cve_id,
            base_score=base_score,
            vendor=vendor_str,
            affected_products=products_str,
            cve_url=cve_page_link,
            vendor_link=vendor_link,
            solution=solution_str,
            times_mentioned=mention_count,
            raw_json_str=raw_json_str,
            db_path=db_path
        )
        updated_count += 1

    print(f"Updated/Inserted details for {updated_count} CVEs in cve_info table.")
    conn.close()
