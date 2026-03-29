# Pharma Intelligence Tracker

## Business Case

Pharma analysts spend hours manually reading fragmented sources — FDA approval notices, clinical trial updates, and market news — to identify high-signal events. This pipeline automates that process: it ingests data from three sources daily, scores each item using a domain-specific NLP model (FinBERT), and surfaces high-sentiment events in a live dashboard. A 2-hour manual review becomes a 30-second check.

## Architecture

```
OpenFDA API  ─┐
ClinicalTrials.gov API  ─┤──► fetchers.py ──► sentiment.py (FinBERT) ──► BigQuery ──► Looker Studio
NewsAPI  ─────┘                                                              ▲
                                                               Cloud Scheduler → Cloud Run
```

**Data flow:**
1. Cloud Scheduler triggers the Cloud Run service daily at 08:00 UTC
2. The service calls three public APIs (OpenFDA, ClinicalTrials.gov, NewsAPI)
3. Each record is scored by FinBERT (`ProsusAI/finbert`) for sentiment (positive / negative / neutral)
4. Deduplicated results are inserted into BigQuery
5. Looker Studio reads from BigQuery for the live dashboard

## Live Dashboard

_Coming soon — pipeline has been running since 2026-03-29. Dashboard will be published once sufficient data has accumulated (~1 week)._

## Deployed Infrastructure

| Component | Detail |
|-----------|--------|
| Cloud Run service | `pharma-pipeline` — `us-central1` |
| Service URL | `https://pharma-pipeline-138103340271.us-central1.run.app` |
| Scheduler | `daily-pharma-pipeline` — every day at 08:00 UTC |
| BigQuery | `pharma-intelligence-491514.pharma_intelligence.drug_signals` |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| NLP Model | FinBERT (`ProsusAI/finbert`) via HuggingFace Transformers |
| Data warehouse | Google BigQuery |
| Compute | Google Cloud Run (Dockerfile-based, 2GB memory) |
| Scheduler | Google Cloud Scheduler |
| Dashboard | Google Looker Studio (in progress) |
| Sources | OpenFDA API, ClinicalTrials.gov v2 API, NewsAPI |

## Local Setup

```bash
# 1. Clone and install dependencies
git clone https://github.com/Arushi-Srivastava-16/pharma-intelligence-tracker.git
cd pharma-intelligence-tracker
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your GCP project ID, BigQuery dataset/table, and NewsAPI key

# 3. Run smoke test (no BQ writes)
python scripts/test_run.py

# 4. Run full pipeline locally
python -m src.main

# 5. Verify data in BigQuery
python scripts/check_bq.py
```

## Trigger the Pipeline Manually

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://pharma-pipeline-138103340271.us-central1.run.app
```

Returns a JSON summary: `{'fetched': N, 'scored': N, 'inserted': N, 'errors': 0}`

## Project Structure

```
pharma-intelligence-tracker/
├── Dockerfile                  # Cloud Run container definition (FinBERT baked in)
├── main.py                     # WSGI entry point for Cloud Run
├── schema/bq_schema.json       # BigQuery table schema (8 columns)
├── src/
│   ├── fetchers.py             # Data fetchers for 3 sources
│   ├── bigquery_client.py      # BQ schema, dedup-before-insert, row insertion
│   ├── sentiment.py            # FinBERT wrapper (module-level model load)
│   └── main.py                 # Pipeline orchestrator
├── deploy/                     # Flat-copy for Cloud Functions deployment (alternative)
└── scripts/
    ├── test_run.py             # Local smoke test (no BQ writes)
    └── check_bq.py             # Verify rows in BigQuery
```

## Key Design Decisions

- **Dedup before insert** — `get_existing_ids()` queries BigQuery for existing IDs before calling `insert_rows_json`, not after. Checking after means duplicates are already in the table.
- **FinBERT at module level** — the model loads once on container startup, not once per record. Reloading per record would add ~6 minutes of latency per run.
- **Always read `insert_rows_json` return value** — the BigQuery client does not raise exceptions on row-level write errors. It returns a list of error dicts silently.
- **Every fetcher returns `[]` on failure** — never `None`, never a raised exception. Downstream code concatenates all three lists; `None` would crash with `TypeError`.
- **FinBERT baked into Docker image** — model weights (~440MB) are downloaded at build time, not at runtime. This avoids HuggingFace rate-limiting Cloud Run's shared IP on cold starts.
