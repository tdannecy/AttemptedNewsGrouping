# app.py

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import pytz

# Import your existing modules for viewing data
from analysis.company_extraction import (
    get_companies_in_article_list,
    filter_articles_by_company
)
from analysis.cve_extraction import build_cve_table
from analysis.two_phase_grouping import (
    PREDEFINED_CATEGORIES,
    get_existing_groups_two_phase,
    get_articles_for_group_two_phase,
    get_subgroups_for_category,
    get_articles_for_subgroup
)
from db.database import setup_database

# Constants for date filtering
DATE_FILTER_OPTIONS = {
    "All time": None,
    "Last 24 hours": 24,
    "Last 7 days": 24*7,
    "Last 30 days": 24*30
}

def get_articles_for_date_range(df_articles, hours):
    if hours is None:
        return df_articles
    cutoff = datetime.now(tz=pytz.UTC) - timedelta(hours=hours)
    df_articles["published_date"] = pd.to_datetime(df_articles["published_date"], utc=True, errors="coerce")
    return df_articles.loc[df_articles["published_date"] >= cutoff].copy()

def main():
    st.title("Two-Phase Grouping Viewer: CVEs and Categories")

    # Ensure the database is set up
    setup_database()

    # -----------------------------
    # Sidebar
    # -----------------------------
    with st.sidebar:
        selected_date_range = st.selectbox("Date Filter", list(DATE_FILTER_OPTIONS.keys()))
        date_hours = DATE_FILTER_OPTIONS[selected_date_range]

        # Gather basic stats for display
        conn = sqlite3.connect("db/news.db")
        c = conn.cursor()
        
        # total articles
        c.execute("SELECT COUNT(*) FROM articles")
        total_articles = c.fetchone()[0]

        # ungrouped articles
        c.execute("""
            SELECT COUNT(*) FROM articles a
            WHERE NOT EXISTS (
                SELECT 1 FROM two_phase_article_group_memberships m
                WHERE m.article_link=a.link
            )
        """)
        ungrouped_two = c.fetchone()[0]

        # grouped
        c.execute("SELECT COUNT(*) FROM two_phase_article_group_memberships")
        grouped_two = c.fetchone()[0]

        # total groups
        c.execute("SELECT COUNT(*) FROM two_phase_article_groups")
        total_groups_two = c.fetchone()[0]

        conn.close()

        # Show overall stats in sidebar
        st.markdown("### Overall Stats")
        st.write(f"Total Articles: {total_articles}")

        st.markdown("### Two-Phase Stats")
        st.write(f"Ungrouped (awaiting category): {ungrouped_two}")
        st.write(f"Grouped (in categories): {grouped_two}")
        st.write(f"Total Two-Phase Groups: {total_groups_two}")

        # Page navigation
        st.write("---")
        pages = [
            "CVE Mentions",
            "View Two-Phase Groups"
        ]
        for cat in PREDEFINED_CATEGORIES:
            pages.append(f"Category: {cat}")
        selected_page = st.radio("Navigation", pages)

    # -----------------------------
    # Main Layout
    # -----------------------------
    col_main, col_right = st.columns([3,1], gap="large")

    # -----------------------------------
    # 1) CVE Mentions Page
    # -----------------------------------
    if selected_page == "CVE Mentions":
        with col_main:
            st.header("CVE Extraction & Mentions")

            # Build the CVE table
            cve_table = build_cve_table(date_hours, db_path="db/news.db")
            if cve_table.empty:
                st.info("No CVEs found for this date range.")
            else:
                st.write("**Current CVE Mentions**:")
                st.dataframe(cve_table, use_container_width=True)
                st.markdown("_Tip: Hover over the **Articles** cell to see links._")

        with col_right:
            st.write("Adjust date filter on the left sidebar as needed.")
        return

    # -----------------------------------
    # 2) View Two-Phase Groups Page
    # -----------------------------------
    elif selected_page == "View Two-Phase Groups":
        with col_main:
            st.header("View Two-Phase Groups")
            st.write(f"**Date Filter:** {selected_date_range}")

            df2 = get_existing_groups_two_phase(db_path="db/news.db")
            if df2.empty:
                st.info("No two-phase groups found.")
            else:
                valid_groups = []
                for _, row in df2.iterrows():
                    group_id = row["group_id"]
                    articles_df = get_articles_for_group_two_phase(group_id, db_path="db/news.db")
                    articles_df = get_articles_for_date_range(articles_df, date_hours)
                    if not articles_df.empty:
                        valid_groups.append(row)

                if not valid_groups:
                    st.warning("No groups found under this date filter.")
                else:
                    filtered2 = pd.DataFrame(valid_groups)
                    new_counts = []
                    for _, r in filtered2.iterrows():
                        g_id = r["group_id"]
                        arts = get_articles_for_group_two_phase(g_id, db_path="db/news.db")
                        arts = get_articles_for_date_range(arts, date_hours)
                        new_counts.append(len(arts))
                    filtered2["article_count"] = new_counts

                    sort_by_size_2 = st.checkbox("Sort by largest group size?")
                    if sort_by_size_2:
                        filtered2 = filtered2.sort_values(by="article_count", ascending=False)

                    st.write(f"Showing {len(filtered2)} groups after date filter.")
                    st.dataframe(filtered2[["group_id","main_topic","sub_topic","group_label","article_count"]])

                    # Let user choose a group to expand
                    group_ids_2 = filtered2["group_id"].tolist()
                    if group_ids_2:
                        chosen_2 = st.selectbox("Select a group to view details", group_ids_2, key="chosen_2_val")
                        if chosen_2:
                            st.subheader(f"Group {chosen_2}")
                            articles_2p = get_articles_for_group_two_phase(chosen_2, db_path="db/news.db")
                            articles_2p = get_articles_for_date_range(articles_2p, date_hours)
                            if articles_2p.empty:
                                st.warning("No articles found in this date filter.")
                            else:
                                for _, row in articles_2p.iterrows():
                                    with st.expander(f"{row['title']} ({row['published_date']})"):
                                        st.write(row["content"])
                                        st.write(f"Link: {row['link']}")
        return

    # -----------------------------------
    # 3) Category-Specific Pages
    # -----------------------------------
    else:
        # The user selected "Category: X"
        category_name = selected_page.replace("Category: ", "")
        with col_main:
            st.header(f"Fine-Grained Subgroups in Category: {category_name}")
            st.write(f"**Date Filter:** {selected_date_range}")

            sub_df = get_subgroups_for_category(category_name, db_path="db/news.db")
            if sub_df.empty:
                st.info("No subgroups found. Possibly run the pipeline on the backend.")
            else:
                valid_subgroups = []
                for _, srow in sub_df.iterrows():
                    sg_id = srow["subgroup_id"]
                    arts_df = get_articles_for_subgroup(sg_id, db_path="db/news.db")
                    arts_df = get_articles_for_date_range(arts_df, date_hours)
                    if not arts_df.empty:
                        new_row = dict(srow)
                        new_row["filtered_article_count"] = len(arts_df)
                        valid_subgroups.append(new_row)
                if not valid_subgroups:
                    st.warning("No subgroups match this date filter.")
                else:
                    sub_filtered = pd.DataFrame(valid_subgroups)
                    sub_filtered = sub_filtered.sort_values(by="filtered_article_count", ascending=False)
                    for _, row in sub_filtered.iterrows():
                        subgroup_id = row["subgroup_id"]
                        group_label = row["group_label"]
                        summary = row.get("summary") or "(No summary)"
                        article_count = row["filtered_article_count"]

                        with st.expander(f"{group_label} (Articles: {article_count})"):
                            st.write(summary)
                            arts_in_subgroup = get_articles_for_subgroup(subgroup_id, db_path="db/news.db")
                            arts_in_subgroup = get_articles_for_date_range(arts_in_subgroup, date_hours)
                            for _, arow in arts_in_subgroup.iterrows():
                                st.write(f"- **{arow['title']}** ({arow['published_date']})")
                                st.caption(f"[Link]({arow['link']})")

if __name__ == "__main__":
    main()
