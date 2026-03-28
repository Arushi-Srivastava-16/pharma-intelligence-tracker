"""
Pipeline orchestrator.

Entry point for both local execution and Cloud Function deployment.

RULE: Always read the return value of insert_new_rows (which wraps
insert_rows_json). It returns a list of errors silently — no exception
is raised on BigQuery write failure.

Local usage:
    python -m src.main

Cloud Function (Step 5) — add this HTTP trigger wrapper:
    def pipeline_entry(request):
        result = run_pipeline()
        return str(result), 200
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> dict[str, int]:
    """Run the full pipeline: fetch → score → deduplicate → insert.

    Returns a summary dict for testability and Cloud Function response:
        {"fetched": N, "scored": N, "inserted": N, "errors": N}
    """
    # Import here so Cloud Function cold-start only pays import cost when needed
    from src.bigquery_client import get_client, ensure_table_exists, insert_new_rows
    from src.fetchers import fetch_fda_approvals, fetch_clinical_trials, fetch_pharma_news
    from src.sentiment import score_batch

    # Step 1: Initialize BigQuery and ensure the table exists
    bq_client = get_client()
    ensure_table_exists(bq_client)

    # Step 2: Fetch from all three sources
    fda_records  = fetch_fda_approvals()
    ct_records   = fetch_clinical_trials()
    news_records = fetch_pharma_news()

    logger.info(
        "Fetched: FDA=%d, ClinicalTrials=%d, News=%d",
        len(fda_records), len(ct_records), len(news_records),
    )

    all_records = fda_records + ct_records + news_records
    total_fetched = len(all_records)

    if not all_records:
        logger.warning("No records fetched from any source — pipeline exiting early")
        return {"fetched": 0, "scored": 0, "inserted": 0, "errors": 0}

    # Step 3: Score sentiment on all records
    scored_records = score_batch(all_records)
    logger.info("Sentiment scoring complete: %d records scored", len(scored_records))

    # Step 4: Insert to BigQuery (dedup is inside insert_new_rows)
    errors = insert_new_rows(bq_client, scored_records)

    # CRITICAL: read the errors return value — insert_rows_json does not raise
    error_count = len(errors)
    inserted_count = total_fetched - error_count  # approximate

    if errors:
        logger.error(
            "Pipeline completed with %d insert error(s). First error: %s",
            error_count,
            errors[0],
        )
    else:
        logger.info(
            "Pipeline completed successfully. Fetched=%d, inserted (net new)=%d",
            total_fetched,
            inserted_count,
        )

    return {
        "fetched":  total_fetched,
        "scored":   len(scored_records),
        "inserted": inserted_count,
        "errors":   error_count,
    }


if __name__ == "__main__":
    result = run_pipeline()
    print(result)
