#!/usr/bin/env python3
"""
Verify that data landed correctly in BigQuery.

Prints:
  1. Summary grouped by source — row count, date range, sentiment breakdown
  2. The 10 most recently ingested rows

Usage:
    python scripts/check_bq.py

Requires a .env file with GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, and
GOOGLE_APPLICATION_CREDENTIALS.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()


def main() -> None:
    from src.bigquery_client import get_client, get_table_ref

    client    = get_client()
    table_ref = get_table_ref(client)

    # ------------------------------------------------------------------
    # Table 1: summary by source
    # ------------------------------------------------------------------
    summary_query = f"""
        SELECT
            source,
            COUNT(*)                               AS row_count,
            MIN(published_date)                    AS earliest,
            MAX(published_date)                    AS latest,
            COUNTIF(sentiment_label = 'positive')  AS positive,
            COUNTIF(sentiment_label = 'negative')  AS negative,
            COUNTIF(sentiment_label = 'neutral')   AS neutral
        FROM `{table_ref}`
        GROUP BY source
        ORDER BY source
    """

    print(f"\n{'=' * 70}")
    print(f"BigQuery Summary: {table_ref}")
    print(f"{'=' * 70}\n")

    rows = list(client.query(summary_query).result())

    if not rows:
        print("  No rows found in table.")
        print("  Run `python -m src.main` first to populate it.\n")
        return

    total = 0
    for row in rows:
        print(
            f"  [{row.source:<16}] "
            f"{row.row_count:>4} rows | "
            f"{str(row.earliest)} → {str(row.latest)} | "
            f"+{row.positive} pos  -{row.negative} neg  ={row.neutral} neu"
        )
        total += row.row_count

    print(f"\n  Total rows across all sources: {total}")

    # ------------------------------------------------------------------
    # Table 2: most recent 10 rows
    # ------------------------------------------------------------------
    detail_query = f"""
        SELECT
            source,
            published_date,
            sentiment_label,
            sentiment_score,
            title,
            ingested_at
        FROM `{table_ref}`
        ORDER BY ingested_at DESC
        LIMIT 10
    """

    print(f"\n{'=' * 70}")
    print("Most Recent 10 Rows")
    print(f"{'=' * 70}\n")

    for row in client.query(detail_query).result():
        title_display = (row.title or "")[:65]
        if len(row.title or "") > 65:
            title_display += "…"
        print(
            f"  [{row.source:<16}] "
            f"{str(row.published_date)} | "
            f"{row.sentiment_label:8s} {row.sentiment_score:.3f} | "
            f"{title_display}"
        )

    print()


if __name__ == "__main__":
    main()
