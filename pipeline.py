# -------------------------------------------------------------
# In app.py (or pipeline.py if you prefer a separate file)
# -------------------------------------------------------------

from analysis.company_extraction import extract_company_names_for_all_articles
from analysis.cve_extraction import process_cves_in_articles, update_cve_details_from_api
from analysis.two_phase_grouping import (
    get_ungrouped_articles_two_phase,
    two_phase_grouping_with_predefined_categories,
    save_two_phase_groups,
    group_articles_within_category,
    PREDEFINED_CATEGORIES
)

def run_full_pipeline_headless(api_key=None, db_path="db/news.db"):
    """
    Run all steps in one go, but WITHOUT any Streamlit calls.
    If api_key is not provided, attempts to get it from environment variable.
    Returns a dict of messages or logs that you can print or ignore.
    """
    import os
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            return ["Error: No API key provided and OPENAI_API_KEY environment variable not found"]

    logs = []

    # 1) Extract company names
    logs.append("Extracting company names...")
    extract_company_names_for_all_articles(api_key, db_path=db_path)
    logs.append("Done extracting company names.")

    # 2) Extract CVE mentions
    logs.append("Extracting CVE mentions from articles...")
    process_cves_in_articles(db_path=db_path)
    logs.append("Done extracting CVE mentions.")

    # 3) Pull CVE details from MITRE
    logs.append("Pulling CVE details from MITRE API...")
    update_cve_details_from_api(db_path=db_path)
    logs.append("Done pulling CVE details.")

    # 4) Group ungrouped articles into top-level categories
    df = get_ungrouped_articles_two_phase(db_path=db_path)
    if df.empty:
        logs.append("No ungrouped articles found for top-level grouping.")
    else:
        logs.append(f"Found {len(df)} articles needing top-level grouping.")
        summaries_dict = {}
        for _, row in df.iterrows():
            s = str(row["expanded_summary"]).strip()
            if s:
                summaries_dict[row["article_link"]] = s
        if summaries_dict:
            result = two_phase_grouping_with_predefined_categories(summaries_dict, api_key, db_path=db_path)
            if result["groups"]:
                save_two_phase_groups(result, db_path=db_path)
                logs.append("Saved top-level groups.")
            else:
                logs.append("No groups created in two-phase approach.")
        else:
            logs.append("No valid summaries for top-level grouping.")

    # 5) Sub-group articles for each predefined category
    logs.append("Sub-grouping for each predefined category...")
    for cat in PREDEFINED_CATEGORIES:
        group_articles_within_category(cat, api_key, db_path=db_path)
        logs.append(f"Finished grouping articles for category: {cat}")

    logs.append("All steps in the pipeline are complete.")
    return logs
