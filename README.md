# Warsaw Data Engineer Job Market Pipeline

A small end-to-end data pipeline that tracks live "Data Engineer" job postings
in Warsaw, Poland and scores each one against my real skill set. Built while
pivoting from EV/hybrid vehicle engineering into data engineering — I wanted
real evidence I could build a pipeline, not just another course certificate,
so I pointed it at the market I was actually applying into.

## Pipeline

```
ingest.py  -->  transform.py  -->  load.py  -->  (Power BI / any SQL client)
 (Apify)         (PySpark)       (SQLite)
```

1. **`ingest.py`** — Pulls live LinkedIn job postings via the Apify
   `cheap_scraper/linkedin-job-scraper` actor, searching "Data Engineer" in
   Warsaw, Poland. Each posting is scored against a fixed list of my real
   skills (Python, SQL, PySpark, Databricks, Git, Machine Learning, Azure,
   C++) via Apify's `resumeKeywords` matching. Writes one timestamped, raw,
   untouched JSON snapshot per run to `data/raw/`.

2. **`transform.py`** — Cleans the raw snapshot with PySpark: dedupes by
   job ID, strips decorative emoji from titles, derives a normalized
   `seniority` bucket (junior/mid/senior/unspecified) primarily from the
   job title text since LinkedIn's own `experienceLevel` field is blank on
   roughly a third of postings, and extracts a clean city name. Writes
   `data/processed/jobs.parquet` and `jobs.csv`.

3. **`load.py`** — Loads the cleaned data into a small SQLite warehouse
   (`data/warehouse/jobs.db`) using an upsert keyed on job ID, so re-running
   the pipeline on a new day's snapshot updates existing postings instead
   of duplicating them. Includes a `v_daily_summary` view for a quick
   sanity check straight from SQL.

From there the warehouse file is just a normal SQLite database — point
Power BI, DB Browser for SQLite, or any BI tool at it directly.

## Sample output

`sample_output/` has a real run committed so you can see the shape of the
data without running anything: `raw_snapshot_sample.json` (126 postings,
trimmed to metadata fields) and `jobs_sample.csv` (the cleaned/transformed
version).

One finding from the July 2026 snapshot: of 126 Warsaw "Data Engineer"
postings, only 9 were explicitly junior-level, and junior postings had the
lowest average skill-match score of any seniority bucket. Small market for
entry-level roles, and the few that exist ask for a narrower overlap with
a typical junior skill set.

## Running it yourself

```bash
pip install -r requirements.txt
export APIFY_API_TOKEN="your-token-from-console.apify.com"

python ingest.py       # -> data/raw/raw_linkedin_jobs_*.json
python transform.py    # -> data/processed/jobs.parquet + jobs.csv
python load.py          # -> data/warehouse/jobs.db
```

## Stack

Python, PySpark, SQLite, Apify (LinkedIn scraping actor). Local-only —
no cloud infra required to run it, though `data/warehouse/jobs.db` swaps
into Postgres with a small change to `load.py` if you want to run it that
way instead.
