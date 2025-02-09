import streamlit as st
import pandas as pd
import sqlite3
import time
import json
import re
import hashlib
from openai import OpenAI
from datetime import datetime, timedelta

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

MODEL = "o3-mini"          # Model used for grouping and extraction
MAX_RETRIES = 3
REQUEST_TIMEOUT = 240
DEFAULT_API_KEY = ""

# Up to ~70k tokens in a single chunk for LLM calls
MAX_TOKEN_CHUNK = 70000

# Date filter options in the sidebar
DATE_FILTER_OPTIONS = {
    "All time": None,
    "Last 24 hours": 24,
    "Last 7 days": 24*7,
    "Last 30 days": 24*30
}

# Predefined categories for two-phase approach
PREDEFINED_CATEGORIES = [
    "Science & Environment",
    "Business, Finance & Trade",
    "Artificial Intelligence & Machine Learning",
    "Software Development & Open Source",
    "Cybersecurity & Data Privacy",
    "Politics & Government",
    "Consumer Technology & Gadgets",
    "Automotive, Space & Transportation",
    "Enterprise Technology & Cloud Computing",
    "Other"
]

# --------------------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------------------

def generate_content_hash(text):
    """Generate a hash for content (if you need dedup checks)."""
    return hashlib.md5(text.encode()).hexdigest()

def call_gpt_api(messages, api_key, model=MODEL):
    """
    Call OpenAI API with retry logic and basic error handling.
    """
    if not api_key:
        st.error("Please provide an API key in the sidebar.")
        return None

    # Estimate tokens (very rough)
    total_token_estimate = int(sum(len(m['content'].split()) for m in messages) * 1.3)
    st.write("API Request Details:")
    st.write(f"- Model: {model}")
    st.write(f"- Timeout: {REQUEST_TIMEOUT}s")
    st.write(f"- Message count: {len(messages)}")
    st.write(f"- Approx token count: {total_token_estimate}")

    client = OpenAI(api_key=api_key)
    for attempt in range(MAX_RETRIES):
        try:
            st.info(f"Making API call (attempt {attempt+1}/{MAX_RETRIES}) to {model}...")
            start_time = time.time()
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                timeout=REQUEST_TIMEOUT
            )
            elapsed_time = time.time() - start_time
            st.success(f"API call successful in {elapsed_time:.2f}s with model='{model}'")
            return response.choices[0].message.content.strip()
        except Exception as e:
            elapsed_time = time.time() - start_time
            st.error(f"Error on attempt {attempt+1}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES - 1:
                st.warning("Retrying in 2 seconds...")
                time.sleep(2)
            else:
                return None

# --------------------------------------------------------------------------------
# Database Setup
# --------------------------------------------------------------------------------

def setup_database():
    """
    Create the necessary tables for the two-phase approach if they don't exist.
    Also create the article_companies table for storing extracted company tags.
    """
    conn = sqlite3.connect("news.db")
    cursor = conn.cursor()
    
    # Two-phase grouping tables
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

    # Subgroup tables (fine-grained grouping inside each category)
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

    # Table for storing company references extracted by the LLM
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_companies (
        article_link TEXT NOT NULL,
        company_name TEXT NOT NULL,
        PRIMARY KEY(article_link, company_name)
    )
    """)

    conn.commit()
    conn.close()

# --------------------------------------------------------------------------------
# Approximate Tokens & Chunk Summaries
# --------------------------------------------------------------------------------

def approximate_tokens(text):
    """Rough heuristic for token count."""
    return int(len(text.split()) * 1.3)

def chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK):
    """
    Splits articles into chunks without exceeding max_token_chunk (~70k).
    If a single article itself exceeds the chunk limit, we yield it alone.
    """
    current_chunk = {}
    current_tokens = 0

    for link, summary in summaries_dict.items():
        tokens_for_article = approximate_tokens(summary)

        # If an article alone exceeds the chunk limit, yield it separately
        if tokens_for_article > max_token_chunk:
            if current_chunk:
                yield current_chunk
                current_chunk = {}
                current_tokens = 0
            # yield this article alone
            yield {link: summary}
            continue

        # Otherwise see if we can add it to the current chunk
        if current_tokens + tokens_for_article > max_token_chunk:
            if current_chunk:
                yield current_chunk
            current_chunk = {link: summary}
            current_tokens = tokens_for_article
        else:
            current_chunk[link] = summary
            current_tokens += tokens_for_article

    if current_chunk:
        yield current_chunk

# --------------------------------------------------------------------------------
# Article + Company Queries
# --------------------------------------------------------------------------------

def get_articles_missing_company_extraction():
    """
    Returns articles that do NOT have any existing entry in article_companies.
    We can run LLM to extract company names for these.
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        a.link,
        a.title || ' - ' || a.content AS expanded_summary
    FROM articles a
    WHERE NOT EXISTS (
        SELECT 1 FROM article_companies ac
        WHERE ac.article_link = a.link
    )
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def extract_company_names_for_all_articles(api_key):
    """
    1. Identify articles with no company extractions yet.
    2. Chunk them up to 70k tokens.
    3. LLM: "Extract all company names from each article."
    4. Store results in article_companies table.
    """
    df = get_articles_missing_company_extraction()
    if df.empty:
        st.info("All articles already have company extractions.")
        return

    # Build dictionary: {link: summary_text}
    summaries_dict = {}
    for _, row in df.iterrows():
        link = row["link"]
        content = row["expanded_summary"].strip()
        summaries_dict[link] = content

    chunked_articles = list(chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK))
    total_extractions = 0

    for idx, chunk_dict in enumerate(chunked_articles, start=1):
        st.write(f"Extracting company names for chunk {idx}/{len(chunked_articles)} with {len(chunk_dict)} articles.")

        prompt = (
            "You are a named-entity recognition AI. For each article, extract all company names mentioned. "
            "Return only JSON with the format:\n"
            "{ \"extractions\": [ {\"article_id\": \"...\", \"companies\": [\"CompanyA\", \"CompanyB\"]}, ... ] }\n\n"
        )
        for art_id, text in chunk_dict.items():
            prompt += f"Article ID={art_id}:\n{text[:5000]}\n\n"

        messages = [
            {
                "role": "system",
                "content": "Extract company names from the provided article texts."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        resp = call_gpt_api(messages, api_key, model=MODEL)
        if not resp:
            st.warning("No response from GPT for this chunk.")
            continue

        # Try parse
        cleaned = resp.strip().strip("```")
        cleaned = re.sub(r'^json\s+', '', cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
            extractions = data.get("extractions", [])
        except json.JSONDecodeError as e:
            st.error(f"Error parsing extraction JSON:\n{cleaned}\n{e}")
            extractions = []

        conn = sqlite3.connect("news.db")
        c = conn.cursor()
        try:
            for item in extractions:
                article_id = item.get("article_id")
                companies = item.get("companies", [])
                if not article_id or not isinstance(companies, list):
                    continue
                for comp in companies:
                    comp_name = comp.strip()
                    if comp_name:
                        c.execute("""
                            INSERT OR IGNORE INTO article_companies (article_link, company_name)
                            VALUES (?, ?)
                        """, (article_id, comp_name))
                        total_extractions += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            st.error(f"DB error saving company extraction: {e}")
        finally:
            conn.close()

    st.success(f"Finished extracting company names. Inserted {total_extractions} new (article, company) pairs.")

def get_companies_in_article_list(article_links):
    """
    Given a list of article links, return distinct company names in article_companies
    that match those links.
    """
    if not article_links:
        return []
    conn = sqlite3.connect("news.db")
    placeholders = ",".join("?" for _ in article_links)
    query = f"""
        SELECT DISTINCT company_name 
        FROM article_companies
        WHERE article_link IN ({placeholders})
        ORDER BY company_name ASC
    """
    rows = conn.execute(query, article_links).fetchall()
    conn.close()
    return [r[0] for r in rows]

def filter_articles_by_company(df_articles, company_name):
    """
    Return only articles that mention 'company_name' in article_companies.
    If company_name is (All) or empty, return the original df_articles.
    """
    if not company_name or company_name == "(All)":
        return df_articles

    if df_articles.empty:
        return df_articles

    conn = sqlite3.connect("news.db")
    placeholders = ",".join("?" for _ in df_articles["link"])
    query = f"""
        SELECT article_link 
        FROM article_companies
        WHERE company_name = ?
          AND article_link IN ({placeholders})
    """
    params = [company_name] + list(df_articles["link"])
    matched = conn.execute(query, params).fetchall()
    conn.close()

    valid_links = {row[0] for row in matched}
    return df_articles[df_articles["link"].isin(valid_links)].copy()

# --------------------------------------------------------------------------------
# Date-Filtering Helpers
# --------------------------------------------------------------------------------

def get_articles_for_date_range(df_articles, hours):
    if hours is None:
        return df_articles

    # Make a naive cutoff
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    # Parse as UTC, then remove the timezone => naive
    temp = pd.to_datetime(df_articles["published_date"], utc=True, errors="coerce")
    df_articles["published_date"] = temp.dt.tz_convert(None)

    return df_articles.loc[df_articles["published_date"] >= cutoff].copy()


# --------------------------------------------------------------------------------
# Two-Phase Group Queries
# --------------------------------------------------------------------------------

def get_ungrouped_articles_two_phase():
    """
    Articles not assigned to any two-phase category.
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        a.link as article_link,
        a.title || ' - ' || a.content as expanded_summary,
        a.published_date as created_at
    FROM articles a
    WHERE NOT EXISTS (
        SELECT 1 FROM two_phase_article_group_memberships tgm
        WHERE tgm.article_link = a.link
    )
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_existing_groups_two_phase():
    """
    Fetch all first-level categories (two_phase_article_groups)
    along with a count of assigned articles (ignoring date).
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        tpg.group_id,
        tpg.main_topic,
        tpg.sub_topic,
        tpg.group_label,
        GROUP_CONCAT(tgm.article_link) as article_links,
        COUNT(tgm.article_link) as article_count,
        tpg.created_at,
        tpg.updated_at
    FROM two_phase_article_groups tpg
    LEFT JOIN two_phase_article_group_memberships tgm ON tpg.group_id = tgm.group_id
    GROUP BY tpg.group_id
    ORDER BY tpg.updated_at DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['article_links'] = df['article_links'].apply(lambda x: x.split(',') if x else [])
    return df

def get_articles_for_group_two_phase(group_id):
    """
    Return all articles for a top-level two_phase group_id.
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        a.link,
        a.title,
        a.content,
        a.published_date
    FROM articles a
    JOIN two_phase_article_group_memberships tgm ON a.link = tgm.article_link
    WHERE tgm.group_id = ?
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(group_id,))
    conn.close()
    return df

def get_articles_in_category_not_subgrouped(category: str):
    """
    Return articles assigned to 'category' but NOT in any subgroups for that category.
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT a.link, a.title || ' - ' || a.content AS expanded_summary, a.published_date
    FROM articles a
    JOIN two_phase_article_group_memberships tgm ON tgm.article_link = a.link
    JOIN two_phase_article_groups tg ON tg.group_id = tgm.group_id
    WHERE tg.main_topic = ?
      AND NOT EXISTS (
          SELECT 1 
          FROM two_phase_subgroup_memberships tsgm
          JOIN two_phase_subgroups tsg ON tsg.subgroup_id = tsgm.subgroup_id
          WHERE tsgm.article_link = a.link
            AND tsg.category = ?
      )
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(category, category))
    conn.close()
    return df

def get_subgroups_for_category(category: str):
    """
    Fetch subgroups in a given category from two_phase_subgroups,
    along with a count of assigned articles (ignoring date).
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        tsg.subgroup_id,
        tsg.category,
        tsg.group_label,
        tsg.summary,
        tsg.created_at,
        tsg.updated_at,
        COUNT(tsgm.article_link) as article_count
    FROM two_phase_subgroups tsg
    LEFT JOIN two_phase_subgroup_memberships tsgm ON tsg.subgroup_id = tsgm.subgroup_id
    WHERE tsg.category = ?
    GROUP BY tsg.subgroup_id
    ORDER BY tsg.updated_at DESC
    """
    df = pd.read_sql_query(query, conn, params=(category,))
    conn.close()
    return df

def get_articles_for_subgroup(subgroup_id: int):
    """
    Return articles for a given subgroup within a two-phase category.
    """
    conn = sqlite3.connect("news.db")
    query = """
    SELECT a.link, a.title, a.content, a.published_date
    FROM articles a
    JOIN two_phase_subgroup_memberships tsgm ON a.link = tsgm.article_link
    WHERE tsgm.subgroup_id = ?
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(subgroup_id,))
    conn.close()
    return df

# --------------------------------------------------------------------------------
# Two-Phase Grouping Logic (High-Level)
# --------------------------------------------------------------------------------

def two_phase_grouping_with_predefined_categories(summaries_dict, api_key):
    """
    Assign articles to one of the predefined categories or 'Other', up to 70k tokens of context.
    """
    if not summaries_dict:
        return {"groups": []}

    all_assignments = []
    for chunk_dict in chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK):
        categories_text = "\n".join(f"- {cat}" for cat in PREDEFINED_CATEGORIES)
        snippet_text = ""
        for link, summary in chunk_dict.items():
            snippet_text += f"Article ID={link}:\n{summary}\n\n"

        system_msg = {
            "role": "system",
            "content": (
                "You are an AI that assigns each article to exactly one category from the list. "
                "If no category fits, choose 'Other'. Return valid JSON only."
            )
        }
        user_msg = {
            "role": "user",
            "content": (
                f"Here is the list of valid categories:\n\n{categories_text}\n\n"
                "Below are article summaries. For each article, pick one category (or 'Other'). "
                "Return JSON only, in this format:\n"
                "{ \"assignments\": [ {\"article_id\": \"...\", \"category\": \"...\"}, ... ] }\n\n"
                f"{snippet_text}"
            )
        }

        response = call_gpt_api([system_msg, user_msg], api_key, model=MODEL)
        if not response:
            st.warning("No response from GPT for this chunk.")
            continue

        cleaned = response.strip().strip("```")
        cleaned = re.sub(r'^json\s+', '', cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
            chunk_assignments = data.get("assignments", [])
        except Exception as e:
            st.error(f"Could not parse JSON:\n{cleaned}\nError: {e}")
            chunk_assignments = []

        all_assignments.extend(chunk_assignments)

    grouped_data = {cat: [] for cat in PREDEFINED_CATEGORIES}
    for assn in all_assignments:
        art_id = assn.get("article_id")
        cat = assn.get("category", "Other")
        if cat not in grouped_data:
            cat = "Other"
        grouped_data[cat].append(art_id)

    result = {"groups": []}
    for cat in PREDEFINED_CATEGORIES:
        articles = [a for a in grouped_data[cat] if a]  # filter out None
        if articles:
            result["groups"].append({
                "main_topic": cat,
                "sub_topic": "",
                "group_label": cat,
                "articles": articles
            })
    return result

def save_two_phase_groups(grouped_results):
    """Save the two-phase groups (categories) to DB."""
    conn = sqlite3.connect("news.db")
    c = conn.cursor()
    try:
        for grp in grouped_results["groups"]:
            c.execute("""
                INSERT INTO two_phase_article_groups (main_topic, sub_topic, group_label)
                VALUES (?, ?, ?)
            """, (grp["main_topic"], grp["sub_topic"], grp["group_label"]))
            new_gid = c.lastrowid

            for art_id in grp["articles"]:
                if art_id:
                    c.execute("""
                        INSERT OR IGNORE INTO two_phase_article_group_memberships (article_link, group_id)
                        VALUES (?, ?)
                    """, (art_id, new_gid))

        conn.commit()
        st.success("Saved two-phase groups to DB.")
    except Exception as e:
        conn.rollback()
        st.error(f"Error saving two-phase groups: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------
# Second-Phase Subgrouping
# --------------------------------------------------------------------------------

def group_articles_within_category(category: str, api_key: str):
    """
    Gather articles that belong to this category but have NOT been subgrouped yet,
    then cluster them by sub-topic (also up to 70k tokens).
    """
    df = get_articles_in_category_not_subgrouped(category)
    if df.empty:
        st.info(f"No un-subgrouped articles found for category '{category}'.")
        return

    summaries_dict = {}
    for _, row in df.iterrows():
        link = row["link"]
        summary = row["expanded_summary"]
        if summary:
            summaries_dict[link] = summary.strip()

    if not summaries_dict:
        st.warning("No valid summaries for these articles.")
        return

    total_new_subgroups = 0
    chunked = list(chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK))

    # LLM prompt to create subgroups
    for i, chunk_dict in enumerate(chunked, start=1):
        st.write(f"Processing chunk {i}/{len(chunked)} for category: {category}")

        prompt_text = (
            "Below are articles assigned to this category. Group them by specific sub-topic.\n"
            "For each subgroup, return:\n"
            "  - group_label: a short descriptive title\n"
            "  - summary: a 2-3 sentence summary of these articles\n"
            "  - articles: an array of article IDs\n\n"
            "Return JSON only, with the structure:\n"
            "{ \"groups\": [ {\"group_label\": \"...\", \"summary\": \"...\", \"articles\": [ ... ]}, ... ] }\n\n"
        )
        for art_id, art_summary in chunk_dict.items():
            prompt_text += f"Article {art_id}: {art_summary}\n\n"

        messages = [
            {
                "role": "system",
                "content": f"You are grouping articles specifically for category '{category}'."
            },
            {
                "role": "user",
                "content": prompt_text
            }
        ]

        response = call_gpt_api(messages, api_key, model=MODEL)
        if not response:
            st.warning("No response from GPT for subgroup chunk.")
            continue

        cleaned = response.strip().strip("```")
        cleaned = re.sub(r'^json\s+', '', cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            st.error(f"Could not parse JSON for subgrouping:\n{cleaned}\nError: {e}")
            continue
        
        groups = data.get("groups", [])
        if not groups:
            st.info("No subgroups returned for this chunk.")
            continue

        conn = sqlite3.connect("news.db")
        c = conn.cursor()
        try:
            for grp in groups:
                label = grp.get("group_label", "Untitled Subgroup")
                summary = grp.get("summary", "")
                articles = grp.get("articles", [])

                # Insert new subgroup
                c.execute("""
                INSERT INTO two_phase_subgroups (category, group_label, summary)
                VALUES (?, ?, ?)
                """, (category, label, summary))
                new_subgroup_id = c.lastrowid

                # Insert memberships
                for art_link in articles:
                    c.execute("""
                    INSERT OR IGNORE INTO two_phase_subgroup_memberships (article_link, subgroup_id)
                    VALUES (?, ?)
                    """, (art_link, new_subgroup_id))

                total_new_subgroups += 1
            conn.commit()
            st.success(f"Saved {len(groups)} new subgroups for chunk {i} in category '{category}'.")
        except Exception as e:
            conn.rollback()
            st.error(f"Error saving subgroups: {e}")
        finally:
            conn.close()

    st.success(f"Done grouping articles for category '{category}'. Total new subgroups created: {total_new_subgroups}")

# --------------------------------------------------------------------------------
# Main Streamlit Interface
# --------------------------------------------------------------------------------

def main():
    st.title("Two-Phase Article Grouping with Date & Company Filters")

    # 1) DB Setup
    setup_database()

    # 2) Left Sidebar: Global Input
    with st.sidebar:
        api_key = st.text_input("Enter OpenAI API Key:", value=DEFAULT_API_KEY, type="password")
        st.session_state["api_key"] = api_key

        # Global date filter
        selected_date_range = st.selectbox("Date Filter", list(DATE_FILTER_OPTIONS.keys()))
        date_hours = DATE_FILTER_OPTIONS[selected_date_range]

        # Show overall stats
        conn = sqlite3.connect("news.db")
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM articles")
        total_articles = c.fetchone()[0]

        # Two-phase stats
        c.execute("""
        SELECT COUNT(*) FROM articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM two_phase_article_group_memberships m
            WHERE m.article_link=a.link
        )
        """)
        ungrouped_two = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM two_phase_article_group_memberships")
        grouped_two = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM two_phase_article_groups")
        total_groups_two = c.fetchone()[0]

        conn.close()

        st.markdown("### Overall Stats")
        st.write(f"Total Articles: {total_articles}")

        st.markdown("### Two-Phase Stats")
        st.write(f"Ungrouped (awaiting category): {ungrouped_two}")
        st.write(f"Grouped (in categories): {grouped_two}")
        st.write(f"Total Two-Phase Groups: {total_groups_two}")

        # Button to extract company names across all articles (LLM)
        st.write("---")
        if st.button("Extract Company Names (LLM)"):
            if not api_key:
                st.error("Please enter your API key above.")
            else:
                extract_company_names_for_all_articles(api_key)

        # Navigation
        pages = [
            "Two-Phase Grouping",
            "View Two-Phase Groups"
        ]
        for cat in PREDEFINED_CATEGORIES:
            pages.append(f"Category: {cat}")

        selected_page = st.radio("Navigation", pages)

    # 3) Two columns: main (wide) & right (narrow) for the Company Filter
    col_main, col_right = st.columns([3,1], gap="large")

    with col_main:
        # Render pages that don't require immediate company filtering
        if selected_page == "Two-Phase Grouping":
            st.header("Two-Phase Grouping with Predefined Categories")
            st.write(f"**Date Filter:** {selected_date_range}")
            st.write("Collect all ungrouped articles, then assign them to one of the fixed categories.")
            if st.button("Generate/Update Two-Phase Groups"):
                with st.spinner("Assigning categories..."):
                    df = get_ungrouped_articles_two_phase()
                    if df.empty:
                        st.info("No ungrouped articles found.")
                    else:
                        st.write(f"Found {len(df)} ungrouped articles.")
                        summaries_dict = {}
                        p_bar = st.progress(0)
                        for i, row in df.iterrows():
                            p_bar.progress((i+1)/len(df))
                            s = str(row['expanded_summary']).strip()
                            if s:
                                summaries_dict[row['article_link']] = s
                        if not summaries_dict:
                            st.warning("No valid summaries for two-phase grouping.")
                        else:
                            result = two_phase_grouping_with_predefined_categories(summaries_dict, api_key)
                            if result["groups"]:
                                save_two_phase_groups(result)
                            else:
                                st.warning("No groups created in the two-phase approach.")

        elif selected_page == "View Two-Phase Groups":
            st.header("View Two-Phase Groups")
            st.write(f"**Date Filter:** {selected_date_range}")

            df2 = get_existing_groups_two_phase()
            if df2.empty:
                st.info("No two-phase groups found.")
            else:
                # Filter groups by date: only keep if they have > 0 articles in the range
                valid_groups = []
                for idx, row in df2.iterrows():
                    group_id = row["group_id"]
                    articles_df = get_articles_for_group_two_phase(group_id)
                    articles_df = get_articles_for_date_range(articles_df, date_hours)
                    if not articles_df.empty:
                        valid_groups.append(row)

                if not valid_groups:
                    st.warning("No groups found under this date filter.")
                else:
                    filtered2 = pd.DataFrame(valid_groups)
                    # Recalc article counts for the date filter
                    new_counts = []
                    for _, r in filtered2.iterrows():
                        g_id = r["group_id"]
                        arts = get_articles_for_group_two_phase(g_id)
                        arts = get_articles_for_date_range(arts, date_hours)
                        new_counts.append(len(arts))
                    filtered2["article_count"] = new_counts

                    sort_by_size_2 = st.checkbox("Sort by largest group size?")
                    if sort_by_size_2:
                        filtered2 = filtered2.sort_values(by="article_count", ascending=False)

                    st.write(f"Showing {len(filtered2)} groups after date filter.")
                    st.dataframe(filtered2[["group_id","main_topic","sub_topic","group_label","article_count"]])
                    group_ids_2 = filtered2["group_id"].tolist()

                    if group_ids_2:
                        chosen_2 = st.selectbox("Select a group to view details", group_ids_2, key="chosen_2_val")
                        if chosen_2:
                            st.subheader(f"Group {chosen_2}")
                            articles_2p = get_articles_for_group_two_phase(chosen_2)
                            articles_2p = get_articles_for_date_range(articles_2p, date_hours)

                            if articles_2p.empty:
                                st.warning("No articles found in this date filter.")
                            else:
                                # Company filter is handled in col_right below
                                pass

        else:
            # If "Category: X"
            category_name = selected_page.replace("Category: ", "")
            st.header(f"Fine-Grained Subgroups in Category: {category_name}")
            st.write(f"**Date Filter:** {selected_date_range}")

            if st.button(f"Group Articles in {category_name}"):
                group_articles_within_category(category_name, st.session_state["api_key"])

            st.write("---")
            st.write("**View existing subgroups** for this category:")

            sub_df = get_subgroups_for_category(category_name)
            if sub_df.empty:
                st.info("No subgroups found. Try grouping some articles first.")
            else:
                # We'll finalize display after we read the company filter in col_right
                pass

    # 4) Right Column: dynamic "Company Filter" based on the page/context
    with col_right:
        selected_company = "(All)"

        if selected_page == "View Two-Phase Groups":
            # Re-check which group user selected
            chosen_2_val = st.session_state.get("chosen_2_val", None)
            if chosen_2_val:
                # gather articles in that group for the chosen date range
                arts_df = get_articles_for_group_two_phase(chosen_2_val)
                arts_df = get_articles_for_date_range(arts_df, date_hours)
                if arts_df.empty:
                    st.write("No articles in this group for this date range.")
                else:
                    # find distinct companies in those articles
                    comp_list = get_companies_in_article_list(arts_df["link"].tolist())
                    if comp_list:
                        selected_company = st.selectbox("Company Filter", ["(All)"] + comp_list)
                    else:
                        st.write("No companies found in this group/date range.")
                        selected_company = "(All)"
            else:
                st.write("Select a group on the left to filter by company.")

        elif selected_page.startswith("Category: "):
            category_name = selected_page.replace("Category: ", "")
            # gather all articles for that category (top-level groups) in the date range
            conn = sqlite3.connect("news.db")
            cat_groups = conn.execute(
                "SELECT group_id FROM two_phase_article_groups WHERE main_topic = ?",
                (category_name,)
            ).fetchall()
            conn.close()

            group_ids = [r[0] for r in cat_groups]
            all_links = set()
            for g_id in group_ids:
                arts_df = get_articles_for_group_two_phase(g_id)
                arts_df = get_articles_for_date_range(arts_df, date_hours)
                for link in arts_df["link"]:
                    all_links.add(link)

            if not all_links:
                st.write("No articles in this category for the chosen date filter.")
            else:
                comp_list = get_companies_in_article_list(list(all_links))
                if comp_list:
                    selected_company = st.selectbox("Company Filter", ["(All)"] + comp_list)
                else:
                    st.write("No companies found in this category/date range.")
                    selected_company = "(All)"

        else:
            # "Two-Phase Grouping" or default
            st.write("No company filter on this page.")
            selected_company = "(All)"

    # 5) Final pass: Show articles or subgroups with the selected date + company filter
    if selected_page == "View Two-Phase Groups":
        # Re-show the chosen group with company filtering
        df2 = get_existing_groups_two_phase()
        valid_groups = []
        for _, row in df2.iterrows():
            group_id = row["group_id"]
            arts_df = get_articles_for_group_two_phase(group_id)
            arts_df = get_articles_for_date_range(arts_df, date_hours)
            if not arts_df.empty:
                valid_groups.append(row)

        if valid_groups:
            filtered2 = pd.DataFrame(valid_groups)
            group_ids_2 = filtered2["group_id"].tolist()
            chosen_2_val = st.session_state.get("chosen_2_val", None)
            if chosen_2_val:
                st.subheader(f"Group {chosen_2_val}")
                articles_2p = get_articles_for_group_two_phase(chosen_2_val)
                articles_2p = get_articles_for_date_range(articles_2p, date_hours)
                articles_2p = filter_articles_by_company(articles_2p, selected_company)

                if articles_2p.empty:
                    st.warning("No articles found under this date + company filter.")
                else:
                    for _, row in articles_2p.iterrows():
                        with st.expander(f"{row['title']} ({row['published_date']})"):
                            st.write(row["content"])
                            st.write(f"Link: {row['link']}")

    elif selected_page.startswith("Category: "):
        category_name = selected_page.replace("Category: ", "")
        sub_df = get_subgroups_for_category(category_name)
        if not sub_df.empty:
            valid_subgroups = []
            for _, srow in sub_df.iterrows():
                sg_id = srow["subgroup_id"]
                arts_df = get_articles_for_subgroup(sg_id)
                arts_df = get_articles_for_date_range(arts_df, date_hours)
                arts_df = filter_articles_by_company(arts_df, selected_company)
                if not arts_df.empty:
                    new_row = dict(srow)
                    new_row["filtered_article_count"] = len(arts_df)
                    valid_subgroups.append(new_row)

            if not valid_subgroups:
                st.warning("No subgroups match this date + company filter.")
            else:
                sub_filtered = pd.DataFrame(valid_subgroups)
                # Sort subgroups by descending article count after filtering
                sub_filtered = sub_filtered.sort_values(by="filtered_article_count", ascending=False)

                for _, row in sub_filtered.iterrows():
                    subgroup_id = row["subgroup_id"]
                    group_label = row["group_label"]
                    summary = row["summary"] or "(No summary)"
                    article_count = row["filtered_article_count"]

                    with st.expander(f"{group_label} (Articles: {article_count})"):
                        st.write(summary)
                        arts_in_subgroup = get_articles_for_subgroup(subgroup_id)
                        arts_in_subgroup = get_articles_for_date_range(arts_in_subgroup, date_hours)
                        arts_in_subgroup = filter_articles_by_company(arts_in_subgroup, selected_company)

                        for _, arow in arts_in_subgroup.iterrows():
                            st.write(f"- **{arow['title']}** ({arow['published_date']})")
                            st.caption(f"[Link]({arow['link']})")

# --------------------------------------------------------------------------------
# Run App
# --------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
