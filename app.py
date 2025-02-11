import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import pytz

# Import your existing modules
from analysis.company_extraction import get_companies_in_article_list, filter_articles_by_company
from analysis.cve_extraction import build_cve_table
from analysis.two_phase_grouping import (
    PREDEFINED_CATEGORIES,
    get_existing_groups_two_phase,
    get_articles_for_group_two_phase,
    get_subgroups_for_category,
    get_articles_for_subgroup
)
from db.database import setup_database

# Constants
DATE_FILTER_OPTIONS = {
    "All time": None,
    "Last 24 hours": 24,
    "Last 7 days": 24*7,
    "Last 30 days": 24*30
}

# Custom CSS to improve the look and feel
st.set_page_config(
    page_title="Security News Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# Apply custom styling
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 4rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 28px;
    }
    .stat-card {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .article-card {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }
    .article-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .article-title {
        color: #1f2937;
        font-size: 1.25rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .article-meta {
        color: #6b7280;
        font-size: 0.875rem;
        margin-bottom: 0.5rem;
    }
    .article-content {
        color: #374151;
        font-size: 1rem;
        margin-bottom: 1rem;
    }
    .article-link {
        color: #2563eb;
        text-decoration: none;
        font-size: 0.875rem;
    }
    .article-link:hover {
        text-decoration: underline;
    }
    </style>
""", unsafe_allow_html=True)

def get_articles_for_date_range(df_articles, hours):
    if hours is None:
        return df_articles
    cutoff = datetime.now(tz=pytz.UTC) - timedelta(hours=hours)
    df_articles["published_date"] = pd.to_datetime(df_articles["published_date"], utc=True, errors="coerce")
    return df_articles.loc[df_articles["published_date"] >= cutoff].copy()

def display_article(article):
    st.markdown(f"""
        <div class="article-card">
            <div class="article-title">{article['title']}</div>
            <div class="article-meta">Published: {article['published_date']}</div>
            <div class="article-content">{article['content'][:300]}...</div>
            <a href="{article['link']}" target="_blank" class="article-link">Read more →</a>
        </div>
    """, unsafe_allow_html=True)

def main():
    st.title("🛡️ Security News Dashboard")

    # Ensure database is set up
    setup_database()

    # ------------------------------------------------------------------
    # 1) Gather "all-time" stats to display in top-level metrics
    # ------------------------------------------------------------------
    conn = sqlite3.connect("db/news.db")
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM articles")
    total_articles = c.fetchone()[0]

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

    # ------------------------------------------------------------------
    # 2) Show the top-level (all-time) metrics
    # ------------------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Articles (All Time)", f"{total_articles:,}")
    with col2:
        st.metric("Ungrouped Articles (All Time)", f"{ungrouped_two:,}")
    with col3:
        st.metric("Grouped Articles (All Time)", f"{grouped_two:,}")
    with col4:
        st.metric("Total Groups (All Time)", f"{total_groups_two:,}")

    # ------------------------------------------------------------------
    # 3) Date filter (time range) and time-based stats
    # ------------------------------------------------------------------
    st.divider()
    selected_date_range = st.select_slider(
        "Time Range",
        options=list(DATE_FILTER_OPTIONS.keys()),
        value="Last 24 hours"
    )
    date_hours = DATE_FILTER_OPTIONS[selected_date_range]

    # If user chose a time-limited range, compute new stats
    # Otherwise, if "All time", we'll just skip and show nothing
    if date_hours is not None:
        cutoff_utc = datetime.now(pytz.UTC) - timedelta(hours=date_hours)
        cutoff_str = cutoff_utc.isoformat()

        conn = sqlite3.connect("db/news.db")
        c = conn.cursor()

        # Total articles in the selected time window
        c.execute("""
            SELECT COUNT(*)
            FROM articles
            WHERE published_date >= ?
        """, (cutoff_str,))
        range_total_articles = c.fetchone()[0]

        # Ungrouped articles in the selected time window
        c.execute("""
            SELECT COUNT(*)
            FROM articles a
            WHERE a.published_date >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM two_phase_article_group_memberships m
                  WHERE m.article_link = a.link
              )
        """, (cutoff_str,))
        range_ungrouped = c.fetchone()[0]

        # Grouped articles in the selected time window
        # We count DISTINCT article_link so each article is only counted once
        c.execute("""
            SELECT COUNT(DISTINCT a.link)
            FROM articles a
            JOIN two_phase_article_group_memberships m ON a.link = m.article_link
            WHERE a.published_date >= ?
        """, (cutoff_str,))
        range_grouped = c.fetchone()[0]

        # Total groups that contain at least one article in this time window
        c.execute("""
            SELECT COUNT(DISTINCT g.group_id)
            FROM two_phase_article_groups g
            JOIN two_phase_article_group_memberships m ON g.group_id = m.group_id
            JOIN articles a ON a.link = m.article_link
            WHERE a.published_date >= ?
        """, (cutoff_str,))
        range_total_groups = c.fetchone()[0]

        conn.close()

        # Now display time-range-based stats (below the top-level metrics)
        st.subheader(f"Stats for '{selected_date_range}' Range")
        colA, colB, colC, colD = st.columns(4)
        with colA:
            st.metric("Total Articles (Time Range)", f"{range_total_articles:,}")
        with colB:
            st.metric("Ungrouped Articles (Time Range)", f"{range_ungrouped:,}")
        with colC:
            st.metric("Grouped Articles (Time Range)", f"{range_grouped:,}")
        with colD:
            st.metric("Total Groups (Time Range)", f"{range_total_groups:,}")

    # ------------------------------------------------------------------
    # 4) Create the main content tabs
    # ------------------------------------------------------------------
    tab_cve, tab_groups, tab_categories = st.tabs([
        "🎯 CVE Mentions",
        "📊 View Groups",
        "🗂️ Categories"
    ])

    # =========== CVE Mentions Tab ===========
    with tab_cve:
        st.header("CVE Mentions & Analysis")
        cve_table = build_cve_table(date_hours, db_path="db/news.db")
        if cve_table.empty:
            st.info("No CVEs found in the selected time range.")
        else:
            st.dataframe(
                cve_table,
                use_container_width=True
            )

    # =========== Groups Tab ===========
    with tab_groups:
        st.header("Article Groups")
        df2 = get_existing_groups_two_phase(db_path="db/news.db")

        if df2.empty:
            st.info("No groups found.")
        else:
            # Filter groups based on date range
            valid_groups = []
            for _, row in df2.iterrows():
                articles_df = get_articles_for_group_two_phase(row["group_id"], db_path="db/news.db")
                articles_df = get_articles_for_date_range(articles_df, date_hours)
                if not articles_df.empty:
                    new_row = dict(row)
                    new_row["article_count"] = len(articles_df)
                    valid_groups.append(new_row)

            if not valid_groups:
                st.warning(f"No groups found in the {selected_date_range} range.")
            else:
                filtered2 = pd.DataFrame(valid_groups)
                filtered2 = filtered2.sort_values(by="article_count", ascending=False)

                st.dataframe(
                    filtered2[["group_id", "main_topic", "sub_topic", "group_label", "article_count"]],
                    use_container_width=True
                )

                st.subheader("Group Details")
                chosen_group = st.selectbox(
                    "Select a group to view articles",
                    filtered2["group_id"].tolist(),
                    format_func=lambda x: f"Group {x}"
                )

                if chosen_group:
                    articles_df = get_articles_for_group_two_phase(chosen_group, db_path="db/news.db")
                    articles_df = get_articles_for_date_range(articles_df, date_hours)
                    
                    if articles_df.empty:
                        st.info("No articles found in the selected time range.")
                    else:
                        for _, article in articles_df.iterrows():
                            display_article(article)

    # =========== Categories Tab ===========
    with tab_categories:
        st.header("Category Analysis")
        category = st.selectbox("Select Category", PREDEFINED_CATEGORIES)
        
        if category:
            sub_df = get_subgroups_for_category(category, db_path="db/news.db")
            
            if sub_df.empty:
                st.info(f"No subgroups found for {category}.")
            else:
                valid_subgroups = []
                for _, row in sub_df.iterrows():
                    articles_df = get_articles_for_subgroup(row["subgroup_id"], db_path="db/news.db")
                    articles_df = get_articles_for_date_range(articles_df, date_hours)
                    if not articles_df.empty:
                        new_row = dict(row)
                        new_row["article_count"] = len(articles_df)
                        valid_subgroups.append(new_row)

                if not valid_subgroups:
                    st.warning(f"No subgroups found in the {selected_date_range} range.")
                else:
                    sub_filtered = pd.DataFrame(valid_subgroups)
                    sub_filtered = sub_filtered.sort_values(by="article_count", ascending=False)

                    for _, row in sub_filtered.iterrows():
                        with st.expander(f"{row['group_label']} ({row['article_count']} articles)"):
                            st.markdown(f"**Summary:** {row.get('summary', 'No summary available')}")
                            st.divider()

                            articles_df = get_articles_for_subgroup(row["subgroup_id"], db_path="db/news.db")
                            articles_df = get_articles_for_date_range(articles_df, date_hours)

                            for _, article in articles_df.iterrows():
                                display_article(article)


if __name__ == "__main__":
    main()