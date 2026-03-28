"""
BigQuery client: schema creation, deduplication, and row insertion.

RULE: get_existing_ids() MUST run BEFORE insert_rows_json — checking
after means duplicates are already in the table.

RULE: always read the return value of insert_rows_json. It does not
raise exceptions on row-level errors — it returns a list of error dicts
silently. Callers must check `if errors:` on the return value.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

load_dotenv()
logger = logging.getLogger(__name__)

# Path to schema file resolved relative to this file — not cwd.
# This ensures the schema is found whether we're running locally or in a
# Cloud Function where the working directory may differ.
_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "bq_schema.json"


# ---------------------------------------------------------------------------
# Client and table reference
# ---------------------------------------------------------------------------

def get_client() -> bigquery.Client:
    """Create a BigQuery client for the configured project.

    Raises KeyError if GCP_PROJECT_ID is not set — intentional.
    A pipeline with no project ID cannot proceed and should fail loudly.
    """
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])


def get_table_ref(client: bigquery.Client) -> str:
    """Return the fully-qualified BigQuery table ID: project.dataset.table."""
    project = client.project
    dataset = os.environ["BQ_DATASET"]
    table   = os.environ["BQ_TABLE"]
    return f"{project}.{dataset}.{table}"


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------

def ensure_table_exists(client: bigquery.Client) -> None:
    """Create the BigQuery table if it does not already exist.

    Idempotent — safe to call on every pipeline run.
    Loads the schema from schema/bq_schema.json relative to this file.
    """
    table_ref = get_table_ref(client)

    try:
        client.get_table(table_ref)
        logger.info("BQ table already exists: %s", table_ref)
        return
    except NotFound:
        pass

    with open(_SCHEMA_PATH) as f:
        schema_json = json.load(f)

    schema = [
        bigquery.SchemaField(
            name=col["name"],
            field_type=col["type"],
            mode=col["mode"],
            description=col.get("description", ""),
        )
        for col in schema_json
    ]

    # Ensure the dataset exists first
    dataset_id = f"{client.project}.{os.environ['BQ_DATASET']}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_id))
        logger.info("BQ dataset created: %s", dataset_id)

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    logger.info("BQ table created: %s", table_ref)


# ---------------------------------------------------------------------------
# Deduplication  (runs BEFORE insert)
# ---------------------------------------------------------------------------

def get_existing_ids(client: bigquery.Client, ids: list[str]) -> set[str]:
    """Return the subset of candidate IDs that already exist in BigQuery.

    Uses a parameterized UNNEST query — NOT string interpolation — to avoid
    SQL injection and to handle large ID lists correctly.

    Returns an empty set immediately if ids is empty (avoids sending a
    zero-element UNNEST to BigQuery, which would fail).
    """
    if not ids:
        return set()

    table_ref = get_table_ref(client)
    query = f"SELECT id FROM `{table_ref}` WHERE id IN UNNEST(@candidate_ids)"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("candidate_ids", "STRING", ids)
        ]
    )

    results = client.query(query, job_config=job_config).result()
    return {row.id for row in results}


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def insert_new_rows(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
) -> list:
    """Deduplicate then insert rows into BigQuery.

    Sequence (order matters):
      1. Stamp ingested_at on every row
      2. Query BQ for already-present IDs  ← dedup happens HERE, before insert
      3. Filter to genuinely new rows only
      4. Call insert_rows_json
      5. READ the return value — it's a list of error dicts, not an exception

    Returns [] on success, list of BigQuery error dicts on failure.
    Callers should check `if errors:`.
    """
    if not rows:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["ingested_at"] = now_iso

    existing_ids = get_existing_ids(client, [r["id"] for r in rows])
    new_rows = [r for r in rows if r["id"] not in existing_ids]

    if not new_rows:
        logger.info("BQ insert: all %d rows already exist, nothing to insert", len(rows))
        return []

    logger.info(
        "BQ insert: %d new rows (skipping %d duplicates)",
        len(new_rows),
        len(rows) - len(new_rows),
    )

    table_ref = get_table_ref(client)

    # insert_rows_json does NOT raise exceptions on row-level errors.
    # It returns a list of error dicts — always read it.
    errors = client.insert_rows_json(table_ref, new_rows)

    if errors:
        logger.error("BQ insert_rows_json returned errors: %s", errors)
    else:
        logger.info("BQ insert: %d rows written successfully", len(new_rows))

    return errors
