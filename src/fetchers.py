"""
Data fetchers for three pharma intelligence sources.

RULE: Every public function returns [] on any failure — never None,
never a raised exception. The orchestrator concatenates all three
lists; None would crash with TypeError.
"""

import hashlib
import logging
import os
from datetime import date, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15  # seconds — never block the pipeline indefinitely


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_id(*parts: str) -> str:
    """Deterministic 32-char ID from arbitrary string parts.

    Uses "||" as separator so ("ab", "c") and ("a", "bc") don't collide.
    The same real-world item always produces the same ID across pipeline
    runs — this is what makes dedup reliable.
    """
    combined = "||".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:32]


def _yesterday_and_today() -> tuple[date, date]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    return yesterday, today


# ---------------------------------------------------------------------------
# FDA drug approvals  (OpenFDA API — no key required)
# ---------------------------------------------------------------------------

def fetch_fda_approvals() -> list[dict[str, Any]]:
    """Fetch new original drug approvals from the OpenFDA Drugs@FDA API.

    Endpoint: https://api.fda.gov/drug/drugsfda.json
    Date format: YYYYMMDD  (OpenFDA compact format, NOT ISO 8601)
    HTTP 404 = no results for the date range — not an error.
    """
    try:
        yesterday, today = _yesterday_and_today()
        # OpenFDA uses YYYYMMDD in the search filter
        date_from = yesterday.strftime("%Y%m%d")
        date_to   = today.strftime("%Y%m%d")

        # Build URL manually — requests encodes "+" as "%2B" which breaks
        # OpenFDA's Lucene query syntax. Simplify to date-range only and
        # filter for AP + ORIG in Python after fetching.
        search = f"submissions.submission_status_date:[{date_from}+TO+{date_to}]"
        url = f"https://api.fda.gov/drug/drugsfda.json?search={search}&limit=99"

        resp = requests.get(url, timeout=REQUEST_TIMEOUT)

        # 404 means no records matched — return empty list, not an error
        if resp.status_code == 404:
            logger.info("FDA: no approvals found for %s–%s", date_from, date_to)
            return []

        # 500 can also mean no results on some OpenFDA query patterns
        if resp.status_code == 500:
            logger.info("FDA: API returned 500 (likely no results) for %s–%s", date_from, date_to)
            return []

        resp.raise_for_status()
        data = resp.json()

        records = []
        for application in data.get("results", []):
            app_number = application.get("application_number", "")
            openfda    = application.get("openfda", {})
            products   = application.get("products", [])
            product    = products[0] if products else {}

            brand_name   = (openfda.get("brand_name")    or ["Unknown"])[0]
            generic_name = (openfda.get("generic_name")  or [""])[0]
            sponsor      = application.get("sponsor_name", "Unknown sponsor")
            dosage_form  = product.get("dosage_form", "")
            route        = product.get("route", "")

            # Pull the most recent AP submission in this application
            for submission in application.get("submissions", []):
                if (
                    submission.get("submission_type") == "ORIG"
                    and submission.get("submission_status") == "AP"
                ):
                    raw_date = submission.get("submission_status_date", "")
                    # Convert YYYYMMDD → YYYY-MM-DD for BigQuery DATE
                    if len(raw_date) == 8:
                        pub_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
                    else:
                        pub_date = today.isoformat()

                    records.append({
                        "id": _make_id("fda", app_number, raw_date),
                        "source": "fda",
                        "title": (
                            f"{brand_name} ({generic_name}) approved — {sponsor}"
                        ),
                        "summary": (
                            f"Application {app_number}. "
                            f"{dosage_form}, {route}. "
                            f"Sponsor: {sponsor}."
                        )[:1000],
                        "published_date": pub_date,
                    })
                    break  # one record per application

        logger.info("FDA: fetched %d approval records", len(records))
        return records

    except Exception as exc:
        logger.warning("FDA fetcher failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# ClinicalTrials.gov  (v2 API — no key required)
# ---------------------------------------------------------------------------

def fetch_clinical_trials() -> list[dict[str, Any]]:
    """Fetch recently updated pharma studies from ClinicalTrials.gov v2 API.

    Endpoint: https://clinicaltrials.gov/api/v2/studies
    Date filter uses ISO 8601 (YYYY-MM-DD), unlike OpenFDA.
    pageSize=25 is sufficient for a daily pipeline run.
    """
    try:
        yesterday, today = _yesterday_and_today()
        date_from = yesterday.isoformat()
        date_to   = today.isoformat()

        params = {
            "format": "json",
            "query.term": "pharmaceutical drug",
            "filter.advanced": f"AREA[LastUpdatePostDate]RANGE[{date_from},{date_to}]",
            "fields": "NCTId,BriefTitle,BriefSummary,LastUpdatePostDate,OverallStatus,Phase",
            "pageSize": 25,
            "sort": "LastUpdatePostDate:desc",
        }

        resp = requests.get(
            "https://clinicaltrials.gov/api/v2/studies",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        records = []
        for study in data.get("studies", []):
            protocol = study.get("protocolSection", {})

            id_module     = protocol.get("identificationModule", {})
            desc_module   = protocol.get("descriptionModule", {})
            status_module = protocol.get("statusModule", {})

            nct_id  = id_module.get("nctId", "")
            title   = id_module.get("briefTitle", "")
            summary = desc_module.get("briefSummary", "")

            # Date is nested: statusModule → lastUpdatePostDateStruct → date
            date_struct = status_module.get("lastUpdatePostDateStruct", {})
            pub_date    = date_struct.get("date", today.isoformat())

            if not nct_id:
                continue

            records.append({
                "id": _make_id("ct", nct_id),
                "source": "clinical_trials",
                "title": title,
                "summary": summary[:1000],
                "published_date": pub_date,  # v2 API returns YYYY-MM-DD already
            })

        # NOTE: v2 returns nextPageToken for pagination; 25 records is fine
        # for daily volume. Add pagination here if volume grows.

        logger.info("ClinicalTrials: fetched %d study records", len(records))
        return records

    except Exception as exc:
        logger.warning("ClinicalTrials fetcher failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Pharma news  (NewsAPI — free tier key required)
# ---------------------------------------------------------------------------

def fetch_pharma_news() -> list[dict[str, Any]]:
    """Fetch pharma news articles from NewsAPI.

    Endpoint: https://newsapi.org/v2/everything
    Requires NEWS_API_KEY environment variable.
    Returns [] immediately if the key is not set — not an error.
    """
    try:
        api_key = os.environ.get("NEWS_API_KEY", "")
        if not api_key:
            logger.warning("NEWS_API_KEY not set — skipping news fetch")
            return []

        yesterday, today = _yesterday_and_today()

        params = {
            "q": "pharmaceutical drug approval FDA",
            "from": yesterday.isoformat(),
            "to": today.isoformat(),
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 20,
            "apiKey": api_key,
        }

        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        records = []
        for article in data.get("articles", []):
            title = article.get("title", "")

            # Skip deleted/removed articles that NewsAPI returns as placeholders
            if title == "[Removed]":
                continue

            # description is frequently None on the free tier — fall through to content
            summary = (
                article.get("description")
                or article.get("content", "")
                or ""
            )

            url      = article.get("url", "")
            pub_date = (article.get("publishedAt") or today.isoformat())[:10]

            records.append({
                "id": _make_id("news", url),
                "source": "news",
                "title": title,
                "summary": summary[:500],
                "published_date": pub_date,
            })

        logger.info("News: fetched %d article records", len(records))
        return records

    except Exception as exc:
        logger.warning("News fetcher failed: %s", exc)
        return []
