"""
Cloud Function entry point + pipeline orchestrator.

GCP calls pipeline_entry() on every trigger.
The request argument is required even if unused.

Local usage (from deploy/ directory):
    python main.py
"""

import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> dict[str, int]:
    """Run the full pipeline: fetch → score → deduplicate → insert."""
    # Flat imports — no src/ package in Cloud Function environment
    from bigquery_client import get_client, ensure_table_exists, insert_new_rows
    from fetchers import fetch_fda_approvals, fetch_clinical_trials, fetch_pharma_news
    from sentiment import score_batch

    bq_client = get_client()
    ensure_table_exists(bq_client)

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

    scored_records = score_batch(all_records)
    logger.info("Sentiment scoring complete: %d records scored", len(scored_records))

    errors = insert_new_rows(bq_client, scored_records)

    error_count = len(errors)
    inserted_count = total_fetched - error_count

    if errors:
        logger.error(
            "Pipeline completed with %d insert error(s). First error: %s",
            error_count, errors[0],
        )
    else:
        logger.info(
            "Pipeline completed successfully. Fetched=%d, inserted (net new)=%d",
            total_fetched, inserted_count,
        )

    return {
        "fetched":  total_fetched,
        "scored":   len(scored_records),
        "inserted": inserted_count,
        "errors":   error_count,
    }


def pipeline_entry(request):
    """HTTP entry point for Cloud Functions. GCP calls this on every trigger."""
    result = run_pipeline()
    return str(result), 200


if __name__ == "__main__":
    print(run_pipeline())
