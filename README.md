# Pharma Intelligence Tracker

## Business Case

Pharma analysts spend hours manually reading fragmented sources — FDA approval notices, clinical trial updates, and market news — to identify high-signal events. This pipeline automates that process: it ingests data from three sources daily, scores each item using a domain-specific NLP model (FinBERT), and surfaces high-sentiment events in a live dashboard. A 2-hour manual review becomes a 30-second check.

## Architecture

```
OpenFDA API  ─┐
ClinicalTrials.gov API  ─┤──► fetchers.py ──► sentiment.py (FinBERT) ──► BigQuery ──► Looker Studio
NewsAPI  ─────┘                                                              ▲
                                                               Cloud Scheduler → Cloud Function
```

**Data flow:**
1. Cloud Scheduler triggers the Cloud Function daily
2. The function calls three public APIs (OpenFDA, ClinicalTrials.gov, NewsAPI)
3. Each record is scored by FinBERT (`ProsusAI/finbert`) for sentiment (positive / negative / neutral)
4. Deduplicated results are inserted into BigQuery
5. Looker Studio reads from BigQuery for the live dashboard

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| NLP Model | FinBERT (`ProsusAI/finbert`) via HuggingFace Transformers |
| Data warehouse | Google BigQuery |
| Compute | Google Cloud Functions (2nd gen) |
| Scheduler | Google Cloud Scheduler |
| Dashboard | Google Looker Studio |
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

# 4. Run full pipeline
python -m src.main

# 5. Verify data in BigQuery
python scripts/check_bq.py
```

## Project Structure

```
pharma-intelligence-tracker/
├── schema/bq_schema.json       # BigQuery table schema
├── src/
│   ├── fetchers.py             # Data fetchers for 3 sources
│   ├── bigquery_client.py      # BQ schema, dedup, insert
│   ├── sentiment.py            # FinBERT wrapper
│   └── main.py                 # Pipeline orchestrator + Cloud Function entry point
└── scripts/
    ├── test_run.py             # Local smoke test (no BQ writes)
    └── check_bq.py             # Verify rows in BigQuery
```

## Live Dashboard

_Link will be added after Step 6 deployment._
