"""
ingest.py — Raw ingestion layer for the Warsaw Data Engineer job-market pipeline.

What this does:
  1. Calls the Apify actor `cheap_scraper/linkedin-job-scraper` with a fixed
     search (keyword + location + your real skill list for match scoring).
  2. Pulls back every field for every result (no trimming) — this is the
     "raw" layer, so nothing gets cleaned or dropped here. That happens in
     transform.py (the next pipeline step).
  3. Writes one timestamped JSON file per run to data/raw/, so you build up
     a history of snapshots over time instead of overwriting.

Setup:
  pip install apify-client
  export APIFY_API_TOKEN="your-real-token-from-console.apify.com"

Run:
  python ingest.py
  python ingest.py --keyword "Analytics Engineer" --location "Krakow, Poland"

The actor output is stored without trimming so the raw snapshot can be
reprocessed when transformation logic changes.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from apify_client import ApifyClient
except ImportError:
    sys.exit("Missing dependency. Run: pip install apify-client")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ACTOR_ID = "cheap_scraper/linkedin-job-scraper"
RAW_DIR = Path(__file__).parent / "data" / "raw"

# Tied to your real, verifiable skills — used for keywordMatchScorePercentage
# in the output. Keep this in sync with what's actually on your LinkedIn
# profile / resume, since that's the whole point of the match score.
RESUME_KEYWORDS = [
    {"keyword": "Python"},
    {"keyword": "SQL", "aliases": ["PostgreSQL", "MySQL"]},
    {"keyword": "PySpark", "aliases": ["Spark", "Apache Spark"]},
    {"keyword": "Databricks", "aliases": ["Azure Databricks"]},
    {"keyword": "Git", "aliases": ["GitHub", "GitLab"]},
    {"keyword": "Machine Learning", "aliases": ["ML"]},
    {"keyword": "Azure", "aliases": ["Microsoft Azure"]},
    {"keyword": "C++"},
]


def run_ingest(keyword: str, location: str, max_items: int, published_window: str) -> Path:
    token = os.environ.get("APIFY_API_TOKEN")
    if not token:
        sys.exit(
            "Set APIFY_API_TOKEN first, e.g.:\n"
            "  export APIFY_API_TOKEN='apify_api_xxxxxxxx'\n"
            "Get a token from https://console.apify.com/account/integrations"
        )

    client = ApifyClient(token)

    run_input = {
        "keyword": [keyword],
        "locations": [location],
        "maxItems": max_items,
        "publishedAt": published_window,
        "saveOnlyUniqueItems": True,
        "resumeKeywords": RESUME_KEYWORDS,
    }

    logger.info("Starting actor run: keyword=%r location=%r maxItems=%d", keyword, location, max_items)
    try:
        run = client.actor(ACTOR_ID).call(run_input=run_input)
    except Exception as exc:
        # apify-client raises its own ApifyApiError plus assorted network
        # errors (requests.exceptions.*); surface a clear, actionable
        # message instead of a raw traceback.
        sys.exit(f"Apify actor run failed: {exc}")

    dataset_id = run["defaultDatasetId"]
    logger.info("Run finished (id=%s). Pulling all items from dataset %s ...", run["id"], dataset_id)

    items = list(client.dataset(dataset_id).iterate_items())
    logger.info("Pulled %d raw items (no fields trimmed).", len(items))

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = (
        RAW_DIR
        / f"raw_linkedin_jobs_{location.split(',')[0].strip().lower()}_{keyword.strip().lower().replace(' ', '_')}_{ts}.json"
    )

    payload = {
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": f"apify:{ACTOR_ID}",
        "run_id": run["id"],
        "dataset_id": dataset_id,
        "query": {
            "keyword": keyword,
            "location": location,
            "publishedAt_window": published_window,
        },
        "item_count": len(items),
        "items": items,  # full, untrimmed records — including jobDescription
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("Wrote raw snapshot: %s", out_path)
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keyword", default="Data Engineer")
    parser.add_argument("--location", default="Warsaw, Poland")
    parser.add_argument("--max-items", type=int, default=150)
    parser.add_argument(
        "--published-window",
        default="r2592000",
        choices=["r86400", "r604800", "r2592000"],
        help="r86400=past day, r604800=past week, r2592000=past month",
    )
    args = parser.parse_args()

    run_ingest(args.keyword, args.location, args.max_items, args.published_window)
