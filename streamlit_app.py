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

MODEL = "o3-mini"          # Used for the initial grouping of articles
MERGE_MODEL_1 = "o3-mini"  # First model for merges/refinements
MERGE_MODEL_2 = "o1-mini"  # Second model for merges/refinements
MAX_RETRIES = 3
REQUEST_TIMEOUT = 240
DEFAULT_API_KEY = ""

# For time filtering in the "View" tabs
TIME_FILTER_OPTIONS = {
    "Last 36 hours": 36,
    "Last 7 days": 24*7,
    "Last 30 days": 24*30,
    "All time": None
}

# --------------------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------------------

def generate_content_hash(text):
    """Generate a hash for content to avoid duplicates."""
    return hashlib.md5(text.encode()).hexdigest()

def call_gpt_api(messages, api_key, model=MODEL):
    """
    Call OpenAI API with retry logic and error handling.

    1) If calling 'o1-mini', we transform system messages into user messages 
       (because 'o1-mini' might not support system role).
    2) Then we pass the final messages to the OpenAI client.
    3) We handle token estimates, retries, etc.
    """
    if not api_key:
        st.error("Please provide an API key in the sidebar.")
        return None

    final_messages = []
    if model == "o1-mini":
        # Transform system → user
        for m in messages:
            if m["role"] == "system":
                final_messages.append({
                    "role": "user",
                    "content": "System Instruction:\n" + m["content"]
                })
            else:
                final_messages.append(m)
    else:
        final_messages = messages

    try:
        total_token_estimate = int(sum(len(m['content'].split()) for m in final_messages) * 1.3)
        st.write("API Request Details:")
        st.write(f"- Model: {model}")
        st.write(f"- Timeout: {REQUEST_TIMEOUT}s")
        st.write(f"- Message count: {len(final_messages)}")
        st.write(f"- Approx token count: {total_token_estimate}")
        
        client = OpenAI(api_key=api_key)
        for attempt in range(MAX_RETRIES):
            try:
                st.info(f"Making API call (attempt {attempt+1}/{MAX_RETRIES}) to {model}...")
                start_time = time.time()
                response = client.chat.completions.create(
                    model=model,
                    messages=final_messages,
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
    except Exception as e:
        st.error(f"Client error: {type(e).__name__} -> {str(e)}")
        return None

# --------------------------------------------------------------------------------
# Database Setup
# --------------------------------------------------------------------------------

def setup_database():
    """
    Create the necessary tables for article grouping if they don't exist.
    Single-step approach uses article_groups / article_group_memberships.
    Two-phase approach uses two_phase_article_groups / two_phase_article_group_memberships.
    """
    conn = sqlite3.connect("news.db")
    cursor = conn.cursor()
    
    # Single-step grouping tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_groups (
        group_id INTEGER PRIMARY KEY AUTOINCREMENT,
        main_topic TEXT NOT NULL,
        sub_topic TEXT NOT NULL,
        group_label TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS article_group_memberships (
        article_link TEXT NOT NULL,
        group_id INTEGER NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (group_id) REFERENCES article_groups (group_id),
        PRIMARY KEY (article_link, group_id)
    )
    """)

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

    conn.commit()
    conn.close()

# --------------------------------------------------------------------------------
# Data Retrieval
# --------------------------------------------------------------------------------

def get_ungrouped_articles_single_step():
    """Articles not assigned to single-step grouping."""
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        a.link as article_link,
        a.title || ' - ' || a.content as expanded_summary,
        a.published_date as created_at
    FROM articles a
    WHERE NOT EXISTS (
        SELECT 1 FROM article_group_memberships agm
        WHERE agm.article_link = a.link
    )
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_ungrouped_articles_two_phase():
    """Articles not assigned to two-phase grouping."""
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

def get_existing_groups_single_step():
    """Fetch all single-step groups with their articles."""
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        ag.group_id,
        ag.main_topic,
        ag.sub_topic,
        ag.group_label,
        GROUP_CONCAT(agm.article_link) as article_links,
        COUNT(agm.article_link) as article_count,
        ag.created_at,
        ag.updated_at
    FROM article_groups ag
    LEFT JOIN article_group_memberships agm ON ag.group_id = agm.group_id
    GROUP BY ag.group_id
    ORDER BY ag.updated_at DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['article_links'] = df['article_links'].apply(lambda x: x.split(',') if x else [])
    return df

def get_existing_groups_two_phase():
    """Fetch all two-phase groups with their articles."""
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

def get_articles_for_group_single_step(group_id):
    """Return all articles for a single-step group_id."""
    conn = sqlite3.connect("news.db")
    query = """
    SELECT 
        a.link,
        a.title,
        a.content,
        a.published_date
    FROM articles a
    JOIN article_group_memberships agm ON a.link = agm.article_link
    WHERE agm.group_id = ?
    ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(group_id,))
    conn.close()
    return df

def get_articles_for_group_two_phase(group_id):
    """Return all articles for a two-phase group_id."""
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

# --------------------------------------------------------------------------------
# Representative Article Selection (Single-Step Only)
# --------------------------------------------------------------------------------

def select_representative_articles(group_id, max_articles=3):
    """
    Heuristic to pick 2-3 representative articles from a single-step group,
    by published_date DESC.
    """
    articles_df = get_articles_for_group_single_step(group_id)
    if articles_df.empty:
        return []
    return articles_df.head(max_articles).to_dict(orient="records")

# --------------------------------------------------------------------------------
# Approximate Tokens & Chunk Summaries
# --------------------------------------------------------------------------------

def approximate_tokens(text):
    return int(len(text.split()) * 1.3)

def chunk_summaries(summaries_dict, max_token_chunk=70000):
    current_chunk = {}
    current_tokens = 0
    for link, summary in summaries_dict.items():
        tokens_for_article = approximate_tokens(summary)
        if current_tokens + tokens_for_article > max_token_chunk and current_chunk:
            yield current_chunk
            current_chunk = {}
            current_tokens = 0
        current_chunk[link] = summary
        current_tokens += tokens_for_article
    if current_chunk:
        yield current_chunk

# --------------------------------------------------------------------------------
# Single-Step Grouping Logic
# --------------------------------------------------------------------------------

def generate_grouping(summaries_dict, api_key):
    """
    Single-step approach: Group articles by topic using one LLM prompt.
    """
    st.write("Starting single-step grouping process...")
    st.write(f"Number of summaries to group: {len(summaries_dict)}")
    
    if not summaries_dict:
        st.error("No summaries provided for grouping.")
        return {"groups": []}
    
    summaries_text = "\n".join(
        f"Article {key}: {str(summary).strip()}"
        for key, summary in summaries_dict.items()
    )
    
    messages = [
        {
            "role": "system",
            "content": (
                "You are an AI assistant that groups news articles based on their semantic content. "
                "You must return valid JSON with a 'groups' key. Do not add extra commentary."
            )
        },
        {
            "role": "user",
            "content": (
                "Below are news article summaries with their article IDs. Please group them. "
                "Use JSON format with 'main_topic', 'sub_topic', 'group_label', and 'articles'.\n\n"
                f"{summaries_text}\n"
                "Return only JSON, no extra keys or text."
            )
        }
    ]
    
    grouping_response = call_gpt_api(messages, api_key, model=MODEL)
    if not grouping_response:
        st.error("No response from the LLM.")
        return {"groups": []}
    
    # Attempt parse
    try:
        data = json.loads(grouping_response)
        if not isinstance(data, dict) or "groups" not in data:
            st.error("Did not find 'groups' in JSON.")
            return {"groups": []}
        return data
    except Exception as e:
        st.error(f"Error parsing grouping JSON: {e}")
        return {"groups": []}

def save_groups(grouped_results, summaries_dict):
    """
    Save the single-step groups to article_groups / article_group_memberships.
    """
    conn = sqlite3.connect("news.db")
    c = conn.cursor()
    try:
        for group in grouped_results["groups"]:
            c.execute("""
            INSERT INTO article_groups (main_topic, sub_topic, group_label)
            VALUES (?, ?, ?)
            """, (group['main_topic'], group['sub_topic'], group['group_label']))
            group_id = c.lastrowid
            for article_id in group['articles']:
                c.execute("""
                INSERT INTO article_group_memberships (article_link, group_id)
                VALUES (?, ?)
                """, (article_id, group_id))
        conn.commit()
        st.success("Saved single-step groups to DB.")
    except Exception as e:
        conn.rollback()
        st.error(f"Error saving single-step groups: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------
# Two-Phase Grouping
# --------------------------------------------------------------------------------

def generate_labels_phase(summaries_dict, api_key):
    snippet_text = ""
    for i, (link, summary) in enumerate(summaries_dict.items(), start=1):
        snippet_text += f"Article {i} (ID={link}): {summary[:300]}\n\n"

    system_msg = {
        "role": "system",
        "content": "You are an AI that proposes a short set of broad labels in JSON."
    }
    user_msg = {
        "role": "user",
        "content": (
            "Below are article snippets. Suggest 5-10 broad topic labels. "
            "Return JSON: {\"proposed_labels\":[\"Label1\",\"Label2\"]}.\n\n"
            f"{snippet_text}"
        )
    }

    resp = call_gpt_api([system_msg, user_msg], api_key, model=MODEL)
    if not resp:
        return []
    cleaned = resp.strip().strip("```")
    try:
        data = json.loads(cleaned)
        return data.get("proposed_labels", [])
    except Exception:
        return []

def assign_articles_phase(summaries_dict, labels, api_key):
    label_list = "\n".join(f"- {lbl}" for lbl in labels)
    snippet_text = ""
    for i, (link, summary) in enumerate(summaries_dict.items(), start=1):
        snippet_text += f"Article {i} (ID={link}): {summary[:300]}\n\n"

    system_msg = {
        "role": "system",
        "content": "You are an AI that assigns articles to existing labels or creates a new label if none fit."
    }
    user_msg = {
        "role": "user",
        "content": (
            f"Labels:\n{label_list}\n\n"
            "Return JSON: {\"assignments\":[{\"article_id\":\"...\",\"label\":\"...\"},...]}\n\n"
            f"{snippet_text}"
        )
    }

    resp = call_gpt_api([system_msg, user_msg], api_key, model=MODEL)
    if not resp:
        return []
    try:
        data = json.loads(resp)
        return data.get("assignments", [])
    except Exception:
        return []

def two_phase_grouping(summaries_dict, api_key):
    if not summaries_dict:
        return {"groups": []}

    labels = generate_labels_phase(summaries_dict, api_key)
    if not labels:
        st.warning("No labels from Phase 1.")
        return {"groups": []}

    assignments = assign_articles_phase(summaries_dict, labels, api_key)
    if not assignments:
        st.warning("No assignments from Phase 2.")
        return {"groups": []}

    group_map = {lbl: [] for lbl in labels}
    new_labels = set(labels)

    for assn in assignments:
        art_id = assn.get("article_id")
        lbl = assn.get("label", "Unlabeled")
        if lbl not in group_map:
            group_map[lbl] = []
            new_labels.add(lbl)
        group_map[lbl].append(art_id)

    result = {"groups": []}
    for lbl in new_labels:
        result["groups"].append({
            "main_topic": lbl,
            "sub_topic": "",
            "group_label": lbl,
            "articles": group_map[lbl]
        })
    return result

def save_two_phase_groups(grouped_results):
    """
    Save the two-phase groups to two_phase_article_groups / two_phase_article_group_memberships.
    """
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
                c.execute("""
                    INSERT INTO two_phase_article_group_memberships (article_link, group_id)
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
# Merging Logic for Single-Step Groups
# --------------------------------------------------------------------------------

def chunk_groups_for_merging(groups_data, max_token_chunk=70000):
    current_batch = []
    current_tokens = 0
    for group_info in groups_data:
        meta_text = (
            f"GroupID: {group_info['group_id']}, "
            f"Main Topic: {group_info['main_topic']}, "
            f"Sub Topic: {group_info['sub_topic']}, "
            f"Group Label: {group_info['group_label']}"
        )
        tokens_for_meta = approximate_tokens(meta_text)
        tokens_for_articles = 0
        for art in group_info['representative_articles']:
            tokens_for_articles += approximate_tokens(art['title'] + " " + art['content'])
        group_tokens = tokens_for_meta + tokens_for_articles
        if current_tokens + group_tokens > max_token_chunk and current_batch:
            yield current_batch
            current_batch = []
            current_tokens = 0
        current_batch.append(group_info)
        current_tokens += group_tokens
    if current_batch:
        yield current_batch

def merge_existing_groups(api_key):
    """
    Merge logic for single-step groups only.
    """
    groups_df = get_existing_groups_single_step()
    if groups_df.empty or len(groups_df) < 2:
        st.info("No groups or only one group found—nothing to merge.")
        return

    groups_data = []
    for _, row in groups_df.iterrows():
        group_id = row['group_id']
        rep_arts = select_representative_articles(group_id, max_articles=3)
        groups_data.append({
            "group_id": group_id,
            "main_topic": row['main_topic'],
            "sub_topic": row['sub_topic'],
            "group_label": row['group_label'],
            "representative_articles": rep_arts
        })

    all_batches = list(chunk_groups_for_merging(groups_data, max_token_chunk=70000))
    st.write(f"Total groups: {len(groups_data)}, batches needed: {len(all_batches)}")

    for batch_index, batch_groups in enumerate(all_batches, start=1):
        st.write(f"Processing merge batch {batch_index}/{len(all_batches)} with {len(batch_groups)} groups")

        user_content = (
            "We have several groups below. Each group has:\n"
            "- group_id\n"
            "- main_topic\n"
            "- sub_topic\n"
            "- group_label\n"
            "- representative_articles\n\n"
            "Decide if any groups should be merged. Return JSON:\n"
            "{ \"merge_instructions\": [ {\"group_ids\":[1,2],\"new_main_topic\":\"...\",\"new_sub_topic\":\"...\",\"new_group_label\":\"...\"}, ... ] }"
        )
        
        for g in batch_groups:
            rep_texts = []
            for art in g['representative_articles']:
                excerpt = art['content'][:300]
                rep_texts.append(f"Title: {art['title']}, Content Excerpt: {excerpt}")
            group_block = (
                f"Group ID: {g['group_id']}\n"
                f"Main Topic: {g['main_topic']}\n"
                f"Sub Topic: {g['sub_topic']}\n"
                f"Group Label: {g['group_label']}\n"
                f"Articles:\n" + "\n".join(rep_texts) + "\n\n"
            )
            user_content += group_block

        system_msg = {
            "role": "system",
            "content": (
                "You are an AI that merges single-step groups if necessary. Return only valid JSON."
            )
        }
        messages = [system_msg, {"role": "user", "content": user_content}]

        response1 = call_gpt_api(messages, api_key, model=MERGE_MODEL_1)
        merges1 = parse_merge_response(response1)

        response2 = call_gpt_api(messages, api_key, model=MERGE_MODEL_2)
        merges2 = parse_merge_response(response2)

        final_merges = combine_merge_instructions(merges1, merges2)
        if not final_merges:
            st.info("No concurrent merges found for this batch.")
            continue

        apply_merge_instructions(final_merges)

def parse_merge_response(response):
    if not response:
        return []
    cleaned = response.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        cleaned = cleaned.strip("```").strip()
    pattern = r'^json\s+'
    cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    try:
        merges_json = json.loads(cleaned)
        return merges_json.get("merge_instructions", [])
    except json.JSONDecodeError as e:
        st.error(f"Error parsing merge response:\n{e}\nResponse:\n{repr(cleaned)}")
        return []

def combine_merge_instructions(merges_model1, merges_model2):
    dict1 = {}
    for m in merges_model1:
        key = tuple(sorted(m.get("group_ids", [])))
        dict1[key] = m
    dict2 = {}
    for m in merges_model2:
        key = tuple(sorted(m.get("group_ids", [])))
        dict2[key] = m
    intersection = dict1.keys() & dict2.keys()
    final = []
    for key in intersection:
        final.append(dict1[key])
    return final

def apply_merge_instructions(merge_instructions):
    """
    Applies merge instructions for single-step groups. 
    Uses a safer approach to avoid UNIQUE constraint failures:
      1) Collect all article_links from the groups to be merged
      2) Create the new group
      3) INSERT OR IGNORE each article_link into the new group
      4) DELETE all old memberships for the merged groups
      5) DELETE the old groups themselves
      6) Update the new group’s updated_at
    """
    if not merge_instructions:
        return

    conn = sqlite3.connect("news.db")
    c = conn.cursor()
    try:
        for instruction in merge_instructions:
            group_ids = instruction.get("group_ids", [])
            if not group_ids or len(group_ids) < 2:
                continue

            main_topic = instruction.get("new_main_topic", "Merged Topic")
            sub_topic = instruction.get("new_sub_topic", "Merged SubTopic")
            group_label = instruction.get("new_group_label", "Merged Group")

            # 1) Gather all article_links from the old groups
            placeholders = ",".join(["?"] * len(group_ids))
            rows = c.execute(
                f"SELECT article_link FROM article_group_memberships WHERE group_id IN ({placeholders})",
                group_ids
            ).fetchall()
            article_links = {row[0] for row in rows}

            # 2) Create the new group
            c.execute("""
                INSERT INTO article_groups (main_topic, sub_topic, group_label)
                VALUES (?, ?, ?)
            """, (main_topic, sub_topic, group_label))
            new_gid = c.lastrowid

            # 3) INSERT OR IGNORE each article_link into the new group
            for link in article_links:
                c.execute("""
                    INSERT OR IGNORE INTO article_group_memberships (article_link, group_id)
                    VALUES (?, ?)
                """, (link, new_gid))

            # 4) DELETE all old memberships for the merged groups
            c.execute(
                f"DELETE FROM article_group_memberships WHERE group_id IN ({placeholders})",
                group_ids
            )

            # 5) DELETE the old groups themselves
            c.execute(
                f"DELETE FROM article_groups WHERE group_id IN ({placeholders})",
                group_ids
            )

            # 6) Update the new group’s updated_at
            c.execute("""
                UPDATE article_groups
                SET updated_at = CURRENT_TIMESTAMP
                WHERE group_id = ?
            """, (new_gid,))

            st.write(f"Merged group IDs {group_ids} into new group {new_gid}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.error(f"Error applying merges: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------
# Single-Step "Process New Articles"
# --------------------------------------------------------------------------------

def process_new_articles():
    api_key = st.session_state.get('api_key', '')
    if not api_key:
        st.error("Please provide an API key.")
        return
    
    df = get_ungrouped_articles_single_step()
    if df.empty:
        st.info("No new articles to process.")
        return

    st.write(f"Processing {len(df)} new articles (single-step).")
    existing_df = get_existing_groups_single_step()
    groups_data = []
    for _, row in existing_df.iterrows():
        group_id = row['group_id']
        rep_arts = select_representative_articles(group_id, max_articles=3)
        groups_data.append({
            "group_id": group_id,
            "main_topic": row['main_topic'],
            "sub_topic": row['sub_topic'],
            "group_label": row['group_label'],
            "representative_articles": rep_arts
        })

    conn = sqlite3.connect("news.db")
    c = conn.cursor()
    try:
        progress_bar = st.progress(0)
        total_articles = len(df)

        for idx, row in df.iterrows():
            progress_bar.progress((idx+1)/total_articles)
            article_link = row['article_link']
            summary = row['expanded_summary']
            assigned_group_id = None

            all_batches = list(chunk_groups_for_merging(groups_data))
            for batch in all_batches:
                system_msg = {
                    "role":"system",
                    "content":"You categorize a single new article vs existing groups."
                }
                user_content = (
                    f"New article:\n{summary[:7000]}\n\n"
                    "Existing groups in this batch:\n"
                )
                for g in batch:
                    rep_texts = []
                    for art in g['representative_articles']:
                        excerpt = art['content'][:300]
                        rep_texts.append(f"Title: {art['title']}, excerpt: {excerpt}")
                    user_content += (
                        f"Group ID: {g['group_id']}\n"
                        f"Main Topic: {g['main_topic']}\n"
                        f"Sub Topic: {g['sub_topic']}\n"
                        f"Group Label: {g['group_label']}\n"
                        "Representative Articles:\n" + "\n".join(rep_texts) + "\n\n"
                    )
                user_content += (
                    "Decide if the new article belongs in any group_id above. If yes, return JSON:\n"
                    "{\"should_add_to_existing\": true, \"group_id\": X, \"reason\": \"...\"}\n"
                    "Else:\n"
                    "{\"should_add_to_existing\": false, \"group_id\": null, \"reason\":\"...\"}\n"
                )
                messages = [system_msg, {"role":"user","content":user_content}]
                response = call_gpt_api(messages, api_key, model=MODEL)
                if not response:
                    continue
                try:
                    suggestion = json.loads(response.strip())
                except json.JSONDecodeError:
                    alt_suggestion = parse_merge_response(response)
                    if isinstance(alt_suggestion, list) or not alt_suggestion:
                        st.error(f"Could not parse JSON for {article_link}:\n{response}")
                        continue
                    else:
                        suggestion = alt_suggestion
                if isinstance(suggestion, list):
                    st.warning(f"Response isn't single-article. Skipping. Raw:\n{response}")
                    continue
                if suggestion.get("should_add_to_existing"):
                    assigned_group_id = suggestion.get("group_id")
                    if assigned_group_id is not None:
                        c.execute("""
                        INSERT INTO article_group_memberships (article_link, group_id)
                        VALUES (?, ?)
                        """, (article_link, assigned_group_id))
                        c.execute("""
                        UPDATE article_groups
                        SET updated_at=CURRENT_TIMESTAMP
                        WHERE group_id=?
                        """, (assigned_group_id,))
                        st.write(f"Article {article_link} assigned to group {assigned_group_id}")
                        break
            if assigned_group_id is None:
                # Create new group
                new_main_topic = "New Main Topic"
                new_sub_topic = "New Sub Topic"
                new_label = "New Group Label"
                c.execute("""
                INSERT INTO article_groups (main_topic, sub_topic, group_label)
                VALUES (?,?,?)
                """,(new_main_topic,new_sub_topic,new_label))
                new_id = c.lastrowid
                c.execute("""
                INSERT INTO article_group_memberships (article_link,group_id)
                VALUES (?,?)
                """,(article_link,new_id))
                st.write(f"Created new group {new_id} for article {article_link}")

        conn.commit()
        st.success("Finished processing new articles (single-step).")
    except Exception as e:
        conn.rollback()
        st.error(f"Error processing new articles: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------
# Filtering & Viewing
# --------------------------------------------------------------------------------

def filter_single_step_groups_by_date_and_sort(df, hours: int=None, sort_by_size=False):
    """
    Return only groups that have at least 1 article in the time window if hours != None.
    Optionally sort by article_count descending.
    """
    if hours is None:
        # All time
        df_filtered = df.copy()
    else:
        cut_off = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        conn = sqlite3.connect("news.db")
        group_ids_in_range = conn.execute("""
        SELECT DISTINCT gm.group_id
        FROM article_groups g
        JOIN article_group_memberships gm ON g.group_id=gm.group_id
        JOIN articles a ON a.link=gm.article_link
        WHERE datetime(a.published_date) >= datetime(?)
        """,(cut_off,)).fetchall()
        conn.close()
        valid = {r[0] for r in group_ids_in_range}
        df_filtered = df[df["group_id"].isin(valid)].copy()
    if sort_by_size:
        df_filtered = df_filtered.sort_values(by="article_count", ascending=False)
    return df_filtered

def filter_two_phase_groups_by_date_and_sort(df, hours: int=None, sort_by_size=False):
    """
    Similar filter for two_phase_article_groups.
    """
    if hours is None:
        df_filtered = df.copy()
    else:
        cut_off = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        conn = sqlite3.connect("news.db")
        group_ids_in_range = conn.execute("""
        SELECT DISTINCT gm.group_id
        FROM two_phase_article_groups g
        JOIN two_phase_article_group_memberships gm ON g.group_id=gm.group_id
        JOIN articles a ON a.link=gm.article_link
        WHERE datetime(a.published_date) >= datetime(?)
        """,(cut_off,)).fetchall()
        conn.close()
        valid = {r[0] for r in group_ids_in_range}
        df_filtered = df[df["group_id"].isin(valid)].copy()
    if sort_by_size:
        df_filtered = df_filtered.sort_values(by="article_count", ascending=False)
    return df_filtered

# --------------------------------------------------------------------------------
# Main Streamlit Interface
# --------------------------------------------------------------------------------

def main():
    st.title("Article Grouping & Merging Tool")

    # 1) DB Setup
    setup_database()

    # 2) Sidebar
    with st.sidebar:
        api_key = st.text_input("Enter OpenAI API Key:", value=DEFAULT_API_KEY, type="password")
        st.session_state["api_key"] = api_key

        # Show overall stats
        conn = sqlite3.connect("news.db")
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM articles")
        total_articles = c.fetchone()[0]

        # Single-step
        c.execute("""
        SELECT COUNT(*) FROM articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM article_group_memberships m
            WHERE m.article_link=a.link
        )
        """)
        ungrouped_single = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM article_group_memberships")
        grouped_single = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM article_groups")
        total_groups_single = c.fetchone()[0]

        # Two-phase
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

        st.markdown("### Single-Step Stats")
        st.write(f"Ungrouped: {ungrouped_single}")
        st.write(f"Grouped: {grouped_single}")
        st.write(f"Total Groups: {total_groups_single}")

        st.markdown("### Two-Phase Stats")
        st.write(f"Ungrouped: {ungrouped_two}")
        st.write(f"Grouped: {grouped_two}")
        st.write(f"Total Groups: {total_groups_two}")

        # -- Vertical Page Navigation --
        pages = [
            "Single-Step Grouping",
            "Two-Phase Grouping",
            "Process New Articles (Single)",
            "Merge Single Groups",
            "View Single Groups",
            "View Two-Phase Groups"
        ]
        selected_page = st.radio("Navigation", pages)

    # 3) Show the page content based on selected_page
    if selected_page == "Single-Step Grouping":
        st.header("Single-Step: Initial Grouping")
        st.write("Collect all ungrouped articles (single-step) and group them with one LLM pass per chunk.")
        if st.button("Generate Single-Step Groups"):
            with st.spinner("Grouping single-step..."):
                df = get_ungrouped_articles_single_step()
                if df.empty:
                    st.info("No ungrouped single-step articles found.")
                else:
                    st.write(f"Found {len(df)} ungrouped single-step articles.")
                    summaries_dict = {}
                    progress_bar = st.progress(0)
                    for i, row in df.iterrows():
                        progress_bar.progress((i+1)/len(df))
                        summ = str(row['expanded_summary']).strip()
                        if summ:
                            summaries_dict[row['article_link']] = summ
                    if not summaries_dict:
                        st.warning("No valid summaries.")
                    else:
                        chunked = list(chunk_summaries(summaries_dict))
                        total_groups_created = 0
                        chunk_idx = 1
                        for chunk_dict in chunked:
                            st.write(f"Chunk {chunk_idx}/{len(chunked)} with {len(chunk_dict)} articles.")
                            chunk_idx+=1
                            group_res = generate_grouping(chunk_dict, api_key)
                            if group_res["groups"]:
                                save_groups(group_res, chunk_dict)
                                total_groups_created += len(group_res["groups"])
                        st.success(f"Created {total_groups_created} new single-step groups.")

    elif selected_page == "Two-Phase Grouping":
        st.header("Two-Phase: Initial Grouping")
        st.write("Collect ungrouped (two-phase) articles, do a two-phase grouping.")
        if st.button("Generate Two-Phase Groups"):
            with st.spinner("Grouping two-phase..."):
                df = get_ungrouped_articles_two_phase()
                if df.empty:
                    st.info("No ungrouped two-phase articles found.")
                else:
                    st.write(f"Found {len(df)} ungrouped two-phase articles.")
                    summaries_dict = {}
                    p_bar = st.progress(0)
                    for i, row in df.iterrows():
                        p_bar.progress((i+1)/len(df))
                        s = str(row['expanded_summary']).strip()
                        if s:
                            summaries_dict[row['article_link']] = s
                    if not summaries_dict:
                        st.warning("No valid summaries for two-phase.")
                    else:
                        result = two_phase_grouping(summaries_dict, api_key)
                        if result["groups"]:
                            save_two_phase_groups(result)
                        else:
                            st.warning("No groups created in two-phase approach.")

    elif selected_page == "Process New Articles (Single)":
        st.header("Process New Articles (Single-Step Only)")
        if st.button("Process New Articles Now"):
            process_new_articles()

    elif selected_page == "Merge Single Groups":
        st.header("Merge Existing Groups (Single-Step, Dual-Model Concurrence)")
        st.write("Refine/merge single-step groups. Compares representative articles with both o3-mini and o1-mini.")
        if st.button("Run Merging (Single-Step)"):
            merge_existing_groups(api_key)

    elif selected_page == "View Single Groups":
        st.header("View Single-Step Groups")
        # Time filter
        time_filter = st.selectbox("Time Filter", list(TIME_FILTER_OPTIONS.keys()))
        sort_by_size = st.checkbox("Sort by largest group size?")
        df = get_existing_groups_single_step()
        if df.empty:
            st.info("No single-step groups in DB.")
        else:
            hours = TIME_FILTER_OPTIONS[time_filter]
            filtered = filter_single_step_groups_by_date_and_sort(df, hours, sort_by_size)
            if filtered.empty:
                st.warning("No groups found with that filter.")
            else:
                st.write(f"Showing {len(filtered)} single-step groups.")
                st.dataframe(filtered[["group_id","main_topic","sub_topic","group_label","article_count"]])
                group_ids = filtered["group_id"].tolist()
                if group_ids:
                    chosen = st.selectbox("Select a group to view details", group_ids)
                    if chosen:
                        # Show articles
                        st.subheader(f"Group {chosen}")
                        articles = get_articles_for_group_single_step(chosen)
                        # If hours is not None, filter articles themselves
                        if hours is not None:
                            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
                            articles["parsed_dt"] = pd.to_datetime(articles["published_date"], utc=True, errors="coerce")
                            articles_in_range = articles[articles["parsed_dt"] >= cutoff]
                        else:
                            articles_in_range = articles
                        for _, row in articles_in_range.iterrows():
                            with st.expander(f"{row['title']} ({row['published_date']})"):
                                st.write(row["content"])
                                st.write(f"Link: {row['link']}")

    elif selected_page == "View Two-Phase Groups":
        st.header("View Two-Phase Groups")
        time_filter_2 = st.selectbox("Time Filter (Two-Phase)", list(TIME_FILTER_OPTIONS.keys()))
        sort_by_size_2 = st.checkbox("Sort by largest group size? (Two-Phase)")

        df2 = get_existing_groups_two_phase()
        if df2.empty:
            st.info("No two-phase groups found.")
        else:
            hours_2 = TIME_FILTER_OPTIONS[time_filter_2]
            filtered2 = filter_two_phase_groups_by_date_and_sort(df2, hours_2, sort_by_size_2)
            if filtered2.empty:
                st.warning("No two-phase groups found with that filter.")
            else:
                st.write(f"Showing {len(filtered2)} two-phase groups.")
                st.dataframe(filtered2[["group_id","main_topic","sub_topic","group_label","article_count"]])
                group_ids_2 = filtered2["group_id"].tolist()
                if group_ids_2:
                    chosen_2 = st.selectbox("Select a two-phase group", group_ids_2)
                    if chosen_2:
                        st.subheader(f"Two-Phase Group {chosen_2}")
                        articles_2p = get_articles_for_group_two_phase(chosen_2)
                        if hours_2 is not None:
                            cutoff2 = datetime.utcnow() - timedelta(hours=hours_2)
                            articles_2p["parsed_dt"] = pd.to_datetime(
                                articles_2p["published_date"], utc=True, errors="coerce"
                            )
                            arts_in_range_2 = articles_2p[articles_2p["parsed_dt"] >= cutoff2]
                        else:
                            arts_in_range_2 = articles_2p
                        for _, row in arts_in_range_2.iterrows():
                            with st.expander(f"{row['title']} ({row['published_date']})"):
                                st.write(row["content"])
                                st.write(f"Link: {row['link']}")

if __name__ == "__main__":
    main()
