<div align="center">

# Pharma Intelligence Tracker

**A production-deployed NLP pipeline that monitors FDA approvals, clinical trials, and pharma news daily — scoring every signal with FinBERT and surfacing high-impact events in a live dashboard.**

[![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)](https://www.python.org/)
[![FinBERT](https://img.shields.io/badge/NLP-FinBERT-orange?style=flat-square)](https://huggingface.co/ProsusAI/finbert)
[![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-4285F4?style=flat-square&logo=google-cloud)](https://cloud.google.com/run)
[![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-4285F4?style=flat-square&logo=google-cloud)](https://cloud.google.com/bigquery)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)

[**Live Dashboard →**](https://datastudio.google.com/s/h_f6JcGJl1w) · [**Architecture**](#%EF%B8%8F-architecture) · [**Local Setup**](#-local-setup)

</div>

---

## 💼 Business Case

Pharma analysts spend hours manually reading fragmented sources — FDA approval notices, clinical trial updates, and market news — to identify high-signal events. This pipeline automates that process entirely.

Every day at 08:00 UTC, three public APIs are queried, every record is scored by **FinBERT** for sentiment (positive / negative / neutral), deduplicated results are inserted into **BigQuery**, and a **Looker Studio** dashboard surfaces the highest-impact signals automatically.

> A 2-hour manual review becomes a 30-second check.

---

## 📊 Live Dashboard

**[→ Open Looker Studio Dashboard](https://datastudio.google.com/s/h_f6JcGJl1w)**

The dashboard shows:
- Daily FDA drug approvals scored by sentiment
- Clinical trial updates (phase changes, results) ranked by signal strength
- News sentiment trends across pharma companies and drug names
- FinBERT confidence distribution across data sources

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│               Google Cloud Scheduler (08:00 UTC)              │
└──────────────────────────────┬───────────────────────────────┘
                               │ HTTP trigger
┌──────────────────────────────▼───────────────────────────────┐
│                    Google Cloud Run                           │
│                  (pharma-pipeline, us-central1)               │
│                                                              │
│  ┌────────────┐  ┌────────────────────┐  ┌────────────────┐  │
│  │ OpenFDA    │  │ ClinicalTrials.gov │  │   NewsAPI      │  │
│  │ fetcher.py │  │ fetcher.py         │  │ fetcher.py     │  │
│  └─────┬──────┘  └─────────┬──────────┘  └───────┬────────┘  │
│        └──────────────────┬┘                     │           │
│                           ▼                      │           │
│              ┌────────────────────────┐           │           │
│              │  sentiment.py          │◄──────────┘           │
│              │  ProsusAI/finbert      │                       │
│              │  (module-level load)   │                       │
│              └────────────┬───────────┘                       │
│                           │                                   │
│              ┌────────────▼───────────┐                       │
│              │  bigquery_client.py    │                       │
│              │  dedup → insert_rows   │                       │
│              └────────────────────────┘                       │
└──────────────────────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Google BigQuery     │
                    │  pharma_intelligence │
                    │  .drug_signals       │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Looker Studio       │
                    │  Live Dashboard      │
                    └─────────────────────┘
```

---

## ✨ Features

| Feature | Detail |
|---|---|
| 🔬 **Multi-Source Ingestion** | OpenFDA, ClinicalTrials.gov v2 API, NewsAPI — 3 sources in one run |
| 🧠 **FinBERT Sentiment** | `ProsusAI/finbert` — domain-specific finance/pharma sentiment (positive / negative / neutral) |
| 🔁 **Daily Automation** | Cloud Scheduler triggers Cloud Run every day at 08:00 UTC |
| 🚫 **Deduplication** | Pre-insert ID check against BigQuery prevents duplicate rows |
| 📊 **Live Dashboard** | Looker Studio connected directly to BigQuery |
| 🐳 **Model Baked In** | FinBERT (~440MB) downloaded at build time — no HuggingFace cold starts |

---

## 🚀 Deployed Infrastructure

| Component | Detail |
|-----------|--------|
| **Cloud Run** | `pharma-pipeline` — `us-central1` |
| **Service URL** | `https://pharma-pipeline-138103340271.us-central1.run.app` |
| **Scheduler** | `daily-pharma-pipeline` — every day at 08:00 UTC |
| **BigQuery** | `pharma-intelligence-491514.pharma_intelligence.drug_signals` |
| **Dashboard** | [Looker Studio](https://datastudio.google.com/s/h_f6JcGJl1w) |

---

## 🔬 Tech Stack

| Layer | Technology |
|-------|------------|
| **Language** | Python 3.11 |
| **NLP Model** | FinBERT (`ProsusAI/finbert`) via HuggingFace Transformers |
| **Data Warehouse** | Google BigQuery |
| **Compute** | Google Cloud Run (Dockerfile-based, 2GB memory) |
| **Scheduler** | Google Cloud Scheduler |
| **Dashboard** | Google Looker Studio |
| **Data Sources** | OpenFDA API · ClinicalTrials.gov v2 API · NewsAPI |

---

## 💻 Local Setup

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

### Trigger Manually

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://pharma-pipeline-138103340271.us-central1.run.app
```

Returns: `{'fetched': N, 'scored': N, 'inserted': N, 'errors': 0}`

---

## 📂 Project Structure

```
pharma-intelligence-tracker/
├── Dockerfile                  # Cloud Run container (FinBERT baked in at build time)
├── main.py                     # WSGI entry point for Cloud Run
├── schema/
│   └── bq_schema.json          # BigQuery table schema (8 columns)
├── src/
│   ├── fetchers.py             # Data fetchers for 3 sources
│   ├── bigquery_client.py      # BQ schema, dedup-before-insert, row insertion
│   ├── sentiment.py            # FinBERT wrapper (module-level model load)
│   └── main.py                 # Pipeline orchestrator
└── scripts/
    ├── test_run.py             # Local smoke test (no BQ writes)
    └── check_bq.py             # Verify rows in BigQuery
```

---

## 🔑 Key Design Decisions

| Decision | Why |
|---|---|
| **Dedup before insert** | `get_existing_ids()` queries BigQuery for existing IDs *before* `insert_rows_json`. Checking after means duplicates are already in the table. |
| **FinBERT at module level** | Model loads once on container startup. Reloading per record would add ~6 minutes of latency per run. |
| **Read `insert_rows_json` return value** | BigQuery client does not raise exceptions on row-level write errors — it returns a list of error dicts silently. |
| **Fetchers return `[]` on failure** | Never `None`, never a raised exception. Downstream code concatenates all three lists; `None` crashes with `TypeError`. |
| **FinBERT baked into Docker image** | Model weights (~440MB) downloaded at build time. Avoids HuggingFace rate-limiting Cloud Run's shared IP on cold starts. |

---

## 📄 License

MIT

---

<div align="center">
  <sub>FinBERT · Google Cloud Run · BigQuery · Looker Studio · Deployed & Running</sub>
</div>
