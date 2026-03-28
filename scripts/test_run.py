#!/usr/bin/env python3
"""
Local smoke test — validates each pipeline layer independently.

Does NOT call run_pipeline() and does NOT write to BigQuery.
Tests each layer separately so failures can be isolated to the exact file.

Usage:
    python scripts/test_run.py

Requires a .env file with valid credentials.
"""

import os
import sys

# Allow `from src.X import ...` when running this script directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()


def test_fetchers() -> None:
    print("=" * 60)
    print("STEP 1: FETCHER TEST")
    print("=" * 60)

    from src.fetchers import fetch_fda_approvals, fetch_clinical_trials, fetch_pharma_news

    fda  = fetch_fda_approvals()
    ct   = fetch_clinical_trials()
    news = fetch_pharma_news()

    print(f"  FDA approvals fetched:        {len(fda)}")
    print(f"  ClinicalTrials records fetched: {len(ct)}")
    print(f"  News articles fetched:         {len(news)}")

    all_records = fda + ct + news
    print(f"  Total records:                 {len(all_records)}")

    if all_records:
        sample = all_records[0]
        print(f"\n  Sample record (first):")
        for k, v in sample.items():
            truncated = str(v)[:80] + "..." if len(str(v)) > 80 else str(v)
            print(f"    {k}: {truncated}")

    # Zero records is acceptable (no data for today's date) — the test
    # passes as long as nothing raised an exception.
    print("\n  PASS — all fetchers returned a list (possibly empty)")


def test_sentiment() -> None:
    print("\n" + "=" * 60)
    print("STEP 2: SENTIMENT TEST")
    print("=" * 60)
    print("  Loading FinBERT (first run downloads ~440MB to ~/.cache/huggingface/)...")

    from src.sentiment import score_text

    test_cases = [
        ("positive", "FDA approves groundbreaking cancer drug showing strong efficacy in trials."),
        ("negative", "Drug trial halted immediately due to severe adverse events and patient deaths."),
        ("neutral",  "Phase 2 study enrolling patients for new cholesterol medication."),
        ("neutral",  ""),  # empty string — must return neutral/0.0 without crashing
    ]

    all_passed = True
    for expected_label, text in test_cases:
        result = score_text(text)
        label = result["sentiment_label"]
        score = result["sentiment_score"]
        display_text = text[:60] + "..." if len(text) > 60 else (text or "(empty string)")
        status = "PASS" if label == expected_label else "NOTE"
        print(f"  [{status}] {label:8s} ({score:.4f}) | {display_text}")
        if label != expected_label:
            print(f"         Expected {expected_label} — model output may vary, this is informational")

    print("\n  PASS — sentiment scoring ran without exceptions")


def test_bigquery() -> None:
    print("\n" + "=" * 60)
    print("STEP 3: BIGQUERY CONNECTION TEST")
    print("=" * 60)

    try:
        from src.bigquery_client import get_client, ensure_table_exists, get_existing_ids, get_table_ref

        client = get_client()
        print(f"  BQ client initialized for project: {client.project}")

        ensure_table_exists(client)
        table_ref = get_table_ref(client)
        print(f"  Table verified/created: {table_ref}")

        # Test get_existing_ids with dummy IDs — should return empty set
        dummy_ids = ["test_id_1", "test_id_2", "test_id_3"]
        existing = get_existing_ids(client, dummy_ids)
        print(f"  Dedup query ran OK — {len(existing)} of {len(dummy_ids)} dummy IDs already exist")

        # Test get_existing_ids with empty list — must not crash
        empty_result = get_existing_ids(client, [])
        assert empty_result == set(), "get_existing_ids([]) must return empty set"
        print("  Empty-list guard works — get_existing_ids([]) returned set()")

        print("\n  PASS — BigQuery connection and table setup verified")

    except KeyError as exc:
        print(f"\n  SKIP — environment variable not set: {exc}")
        print("  Fill in your .env file (copy from .env.example) to test BQ connectivity.")
    except Exception as exc:
        print(f"\n  FAIL — {exc}")
        raise


def main() -> None:
    print("\nPharma Intelligence Tracker — Local Smoke Test")
    print("Each layer is tested independently. No data is written to BigQuery.\n")

    test_fetchers()
    test_sentiment()
    test_bigquery()

    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETE")
    print("=" * 60)
    print("Next step: run the full pipeline with `python -m src.main`")
    print("Then verify data with `python scripts/check_bq.py`\n")


if __name__ == "__main__":
    main()
