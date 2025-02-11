# analysis/two_phase_grouping.py

import sqlite3
import json
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz

from db.database import get_connection
from llm_calls import call_gpt_api
from utils import chunk_summaries, MAX_TOKEN_CHUNK

# Predefined categories, as in original code
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

def get_ungrouped_articles_two_phase(db_path="db/news.db"):
    """
    Articles not assigned to any two-phase category.
    """
    conn = get_connection(db_path)
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

def get_existing_groups_two_phase(db_path="db/news.db"):
    """
    Fetch all first-level categories (two_phase_article_groups),
    along with the articles that belong to each group.
    """
    conn = get_connection(db_path)
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
        LEFT JOIN two_phase_article_group_memberships tgm 
            ON tpg.group_id = tgm.group_id
        GROUP BY tpg.group_id
        ORDER BY tpg.updated_at DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert group-concatenated article_links into lists
    if not df.empty:
        df['article_links'] = df['article_links'].apply(
            lambda x: x.split(',') if x else []
        )
    return df

def get_articles_for_group_two_phase(group_id, db_path="db/news.db"):
    """
    Return all articles for a top-level two_phase group_id.
    """
    conn = get_connection(db_path)
    query = """
        SELECT 
            a.link,
            a.title,
            a.content,
            a.published_date
        FROM articles a
        JOIN two_phase_article_group_memberships tgm 
            ON a.link = tgm.article_link
        WHERE tgm.group_id = ?
        ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(group_id,))
    conn.close()
    return df

def get_articles_in_category_not_subgrouped(category: str, db_path="db/news.db"):
    """
    Return articles assigned to 'category' but NOT in any subgroups for that category.
    """
    conn = get_connection(db_path)
    query = """
        SELECT 
            a.link, 
            a.title || ' - ' || a.content AS expanded_summary, 
            a.published_date
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

def get_subgroups_for_category(category: str, db_path="db/news.db"):
    """
    Fetch subgroups in a given category from two_phase_subgroups,
    along with a count of assigned articles.
    """
    conn = get_connection(db_path)
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
        LEFT JOIN two_phase_subgroup_memberships tsgm 
            ON tsg.subgroup_id = tsgm.subgroup_id
        WHERE tsg.category = ?
        GROUP BY tsg.subgroup_id
        ORDER BY tsg.updated_at DESC
    """
    df = pd.read_sql_query(query, conn, params=(category,))
    conn.close()
    return df

def get_articles_for_subgroup(subgroup_id: int, db_path="db/news.db"):
    """
    Return articles for a given subgroup.
    """
    conn = get_connection(db_path)
    query = """
        SELECT 
            a.link, 
            a.title, 
            a.content, 
            a.published_date
        FROM articles a
        JOIN two_phase_subgroup_memberships tsgm ON a.link = tsgm.article_link
        WHERE tsgm.subgroup_id = ?
        ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn, params=(subgroup_id,))
    conn.close()
    return df

def two_phase_grouping_with_predefined_categories(summaries_dict, api_key, db_path="db/news.db"):
    """
    Assign articles to one of the PREDEFINED_CATEGORIES or 'Other'.
    Returns a dict like:
    {
      "groups": [
         {
           "main_topic": "...",
           "sub_topic": "...",
           "group_label": "...",
           "articles": [...]
         },
         ...
      ]
    }
    """
    if not summaries_dict:
        return {"groups": []}

    all_assignments = []

    # Split into large chunks so we don't exceed token limits
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

        from llm_calls import call_gpt_api  # local import to avoid circular references
        response = call_gpt_api([system_msg, user_msg], api_key)
        if not response:
            continue

        cleaned = response.strip().strip("```")
        cleaned = re.sub(r'^json\s+', '', cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
            chunk_assignments = data.get("assignments", [])
        except Exception:
            chunk_assignments = []

        all_assignments.extend(chunk_assignments)

    grouped_data = {cat: [] for cat in PREDEFINED_CATEGORIES}
    # fallback 'Other' category
    if "Other" not in grouped_data:
        grouped_data["Other"] = []

    for assn in all_assignments:
        art_id = assn.get("article_id")
        cat = assn.get("category", "Other")
        if cat not in grouped_data:
            cat = "Other"
        if art_id:
            grouped_data[cat].append(art_id)

    result = {"groups": []}
    for cat in PREDEFINED_CATEGORIES:
        articles = grouped_data[cat]
        if articles:
            result["groups"].append({
                "main_topic": cat,
                "sub_topic": "",
                "group_label": cat,
                "articles": articles
            })
    if grouped_data["Other"]:
        result["groups"].append({
            "main_topic": "Other",
            "sub_topic": "",
            "group_label": "Other",
            "articles": grouped_data["Other"]
        })
    return result

def save_two_phase_groups(grouped_results, db_path="db/news.db"):
    conn = get_connection(db_path)
    c = conn.cursor()
    try:
        for grp in grouped_results["groups"]:
            # Insert a new row in 'two_phase_article_groups' for this group
            c.execute("""
                INSERT INTO two_phase_article_groups (main_topic, sub_topic, group_label)
                VALUES (?, ?, ?)
            """, (grp["main_topic"], grp["sub_topic"], grp["group_label"]))
            new_gid = c.lastrowid

            # For each article in this group, delete any old membership first,
            # then insert the new membership
            for art_id in grp["articles"]:
                if art_id:
                    c.execute("""
                        DELETE FROM two_phase_article_group_memberships
                        WHERE article_link = ?
                    """, (art_id,))

                    c.execute("""
                        INSERT OR IGNORE INTO two_phase_article_group_memberships (article_link, group_id)
                        VALUES (?, ?)
                    """, (art_id, new_gid))

        conn.commit()
        print("Saved two-phase groups to DB with reassignment logic.")
    except Exception as e:
        conn.rollback()
        print(f"Error saving two-phase groups: {e}")
    finally:
        conn.close()


def group_articles_within_category(category: str, api_key: str, db_path="db/news.db"):
    """
    Gather articles that belong to this category but have NOT been subgrouped yet,
    then cluster them by sub-topic using GPT, and insert the subgroups into DB.
    """
    df = get_articles_in_category_not_subgrouped(category, db_path=db_path)
    if df.empty:
        print(f"No un-subgrouped articles found for category '{category}'.")
        return

    summaries_dict = {}
    for _, row in df.iterrows():
        link = row["link"]
        summary = row["expanded_summary"]
        if summary:
            summaries_dict[link] = summary.strip()

    if not summaries_dict:
        print("No valid summaries for these articles.")
        return

    total_new_subgroups = 0
    chunked = list(chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK))

    from llm_calls import call_gpt_api

    for i, chunk_dict in enumerate(chunked, start=1):
        print(f"Processing chunk {i}/{len(chunked)} for category: {category}")

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

        response = call_gpt_api(messages, api_key)
        if not response:
            print("No response from GPT for this chunk.")
            continue

        cleaned = response.strip().strip("```")
        cleaned = re.sub(r'^json\s+', '', cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            print(f"Could not parse JSON for subgrouping:\n{cleaned}\nError: {e}")
            continue

        groups = data.get("groups", [])
        if not groups:
            print("No subgroups returned for this chunk.")
            continue

        conn = get_connection(db_path)
        c = conn.cursor()
        try:
            for grp in groups:
                label = grp.get("group_label", "Untitled Subgroup")
                summary = grp.get("summary", "")
                articles = grp.get("articles", [])

                c.execute("""
                    INSERT INTO two_phase_subgroups (category, group_label, summary)
                    VALUES (?, ?, ?)
                """, (category, label, summary))
                new_subgroup_id = c.lastrowid

                for art_link in articles:
                    c.execute("""
                        INSERT OR IGNORE INTO two_phase_subgroup_memberships (article_link, subgroup_id)
                        VALUES (?, ?)
                    """, (art_link, new_subgroup_id))

                total_new_subgroups += 1
            conn.commit()
            print(f"Saved {len(groups)} new subgroups for chunk {i} in category '{category}'.")
        except Exception as e:
            conn.rollback()
            print(f"Error saving subgroups: {e}")
        finally:
            conn.close()

    print(f"Done grouping articles for category '{category}'. "
          f"Total new subgroups created: {total_new_subgroups}.")
