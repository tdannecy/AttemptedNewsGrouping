import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import pytz

# === Imports from your own modules ===
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


# === Constants & Configuration ===
DATE_FILTER_OPTIONS = {
    "All time": None,
    "Last hour": 1,
    "Last 8 hours": 8,
    "Last 24 hours": 24,
    "Last 7 days": 24*7,
    "Last 30 days": 24*30
}

st.set_page_config(
    page_title="Security News Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# Custom CSS to improve the look/feel
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


# === Helper Functions ===

def setup_connection(db_path="db/news.db"):
    """
    Returns a tuple: (conn, cursor) for the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()


def get_articles_for_date_range(df_articles, hours):
    """
    Filters df_articles to only those published within 'hours' back from now (UTC).
    If hours is None, returns df_articles unfiltered.
    """
    if hours is None:
        return df_articles
    cutoff = datetime.now(tz=pytz.UTC) - timedelta(hours=hours)
    df_articles["published_date"] = pd.to_datetime(
        df_articles["published_date"],
        format="%Y-%m-%dT%H:%M:%SZ",
        utc=True,
        errors="coerce"
    )
    return df_articles.loc[df_articles["published_date"] >= cutoff].copy()


def display_article(article):
    """
    Renders a single article card using HTML.
    """
    st.markdown(f"""
        <div class="article-card">
            <div class="article-title">{article['title']}</div>
            <div class="article-meta">Published: {article['published_date']}</div>
            <div class="article-content">{article['content'][:300]}...</div>
            <a href="{article['link']}" target="_blank" class="article-link">Read more →</a>
        </div>
    """, unsafe_allow_html=True)


# === Toggling Topics Logic ===
def toggle_topic(topic_key: str):
    """
    Callback to toggle the given `topic_key` in st.session_state.selected_topics.
    """
    if topic_key in st.session_state.selected_topics:
        st.session_state.selected_topics.remove(topic_key)
    else:
        st.session_state.selected_topics.add(topic_key)


def toggled_button(label, toggled=False, key=None, on_click=None, args=None):
    """
    A helper to display a standard st.button(), but wrap it in a DIV
    that applies custom styling (e.g. a red border) if `toggled=True`.
    """
    if toggled:
        st.markdown('<div class="toggled-button">', unsafe_allow_html=True)
        clicked = st.button(label, key=key, on_click=on_click, args=args)
        st.markdown('</div>', unsafe_allow_html=True)
        return clicked
    else:
        return st.button(label, key=key, on_click=on_click, args=args)


# === Main App ===
def main():
    # Ensure database is set up
    setup_database()

    st.title("🛡️ Security News Dashboard")

    # === 1) Gather all-time stats ===
    conn, c = setup_connection()
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

    # === 2) Show top-level (all-time) metrics ===
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Articles (All Time)", f"{total_articles:,}")
    with col2:
        st.metric("Ungrouped Articles (All Time)", f"{ungrouped_two:,}")
    with col3:
        st.metric("Grouped Articles (All Time)", f"{grouped_two:,}")
    with col4:
        st.metric("Total Groups (All Time)", f"{total_groups_two:,}")

    st.divider()

    # === 3) Date filter ===
    selected_date_range = st.select_slider(
        "Time Range",
        options=list(DATE_FILTER_OPTIONS.keys()),
        value="Last 24 hours"
    )
    date_hours = DATE_FILTER_OPTIONS[selected_date_range]

    # If a time range (hours) is set, compute time-range-based stats
    if date_hours is not None:
        cutoff_utc = datetime.now(tz=pytz.UTC) - timedelta(hours=date_hours)
        cutoff_str = cutoff_utc.isoformat()

        conn, c = setup_connection()

        # total articles
        c.execute("""
            SELECT COUNT(*)
            FROM articles
            WHERE published_date >= ?
        """, (cutoff_str,))
        range_total_articles = c.fetchone()[0]

        # ungrouped articles
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

        # grouped articles
        c.execute("""
            SELECT COUNT(DISTINCT a.link)
            FROM articles a
            JOIN two_phase_article_group_memberships m ON a.link = m.article_link
            WHERE a.published_date >= ?
        """, (cutoff_str,))
        range_grouped = c.fetchone()[0]

        # total groups with at least 1 article in range
        c.execute("""
            SELECT COUNT(DISTINCT g.group_id)
            FROM two_phase_article_groups g
            JOIN two_phase_article_group_memberships m ON g.group_id = m.group_id
            JOIN articles a ON a.link = m.article_link
            WHERE a.published_date >= ?
        """, (cutoff_str,))
        range_total_groups = c.fetchone()[0]

        conn.close()

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

    # === 4) Main content tabs ===
    tab_cve, tab_groups, tab_categories = st.tabs([
        "🎯 CVE Mentions",
        "📊 View Groups",
        "🗂️ Categories"
    ])

    # ----------------- TAB 1: CVE Mentions ------------------
    with tab_cve:
        st.header("CVE Mentions & Analysis")
        cve_table = build_cve_table(date_hours, db_path="db/news.db")
        if cve_table.empty:
            st.info("No CVEs found in the selected time range.")
        else:
            st.dataframe(cve_table, use_container_width=True)

    # ----------------- TAB 2: View Groups -------------------
    with tab_groups:
        st.header("Article Groups")

        # CSS for toggled button red outline
        st.markdown("""
            <style>
            .toggled-button button {
                border: 2px solid red !important;
                color: red !important;
                background-color: #fff !important;
            }
            </style>
        """, unsafe_allow_html=True)

        df2 = get_existing_groups_two_phase(db_path="db/news.db")
        if df2.empty:
            st.info("No groups found.")
        else:
            # Filter out groups with zero articles in the selected date range
            valid_groups = []
            for _, row in df2.iterrows():
                articles_df = get_articles_for_group_two_phase(row["group_id"], db_path="db/news.db")
                articles_df = get_articles_for_date_range(articles_df, date_hours)
                if not articles_df.empty:
                    new_row = dict(row)
                    # Each row will have group_id, main_topic, sub_topic, group_label, 
                    # possibly 'summary' if you store it in your table, etc.
                    # We'll store the article_count from how many matched
                    new_row["article_count"] = len(articles_df)
                    valid_groups.append(new_row)

            if not valid_groups:
                st.warning(f"No groups found in the {selected_date_range} range.")
            else:
                valid_groups_df = pd.DataFrame(valid_groups)

                # Maintain selected topics in session state
                if "selected_topics" not in st.session_state:
                    st.session_state.selected_topics = set()

                # Gather unique main topics
                main_topics = sorted(valid_groups_df["main_topic"].unique())

                st.subheader("Click a category to toggle it on/off")

                # Create columns for each main topic, so we can display horizontally
                cols = st.columns(len(main_topics))

                for idx, topic in enumerate(main_topics):
                    is_selected = (topic in st.session_state.selected_topics)
                    btn_label = topic + (" ✓" if is_selected else "")

                    with cols[idx]:
                        toggled_button(
                            label=btn_label,
                            toggled=is_selected,
                            key=f"toggle_btn_{topic}",
                            on_click=toggle_topic,
                            args=(topic,)
                        )

                # Filter groups based on selected topics
                selected = st.session_state.selected_topics
                if not selected:
                    # If nothing is selected, show all
                    filtered_groups_df = valid_groups_df
                else:
                    filtered_groups_df = valid_groups_df[
                        valid_groups_df["main_topic"].isin(selected)
                    ]

                if filtered_groups_df.empty:
                    st.warning("No groups match the currently selected topics.")
                else:
                    # Sort by article_count (descending), then display each group
                    filtered_groups_df = filtered_groups_df.sort_values(
                        by="article_count", ascending=False
                    )

                    for _, grp_row in filtered_groups_df.iterrows():
                        # For each group row, show the group_label and number of articles
                        group_label = grp_row["group_label"]
                        article_count = grp_row["article_count"]
                        group_id = grp_row["group_id"]

                        # If your table includes a 'summary' or 'subgroup_summary' column,
                        # you can display it. If not, it will fallback to "No summary available".
                        subgroup_summary = grp_row.get("summary", "No summary available")

                        with st.expander(f"{group_label} ({article_count} articles)", expanded=False):
                            # Show the summary at top
                            st.markdown(f"**Summary:** {subgroup_summary}")
                            st.divider()

                            # Retrieve articles for this group, re-filtered by date
                            articles_df = get_articles_for_group_two_phase(group_id, db_path="db/news.db")
                            articles_df = get_articles_for_date_range(articles_df, date_hours)

                            for _, article in articles_df.iterrows():
                                display_article(article)

    # ----------------- TAB 3: Categories --------------------
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
