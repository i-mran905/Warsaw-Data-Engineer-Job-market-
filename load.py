"""
load.py — Loads the cleaned parquet output into a small SQLite warehouse.

Reads:  data/processed/jobs.parquet   (written by transform.py)
Writes: data/warehouse/jobs.db        (SQLite database, table: jobs)

Why SQLite instead of Postgres: zero setup, the whole warehouse is one
portable file you can open with any DB tool (DB Browser for SQLite, VS
Code's SQLite extension, even `sqlite3` on the command line), and Power BI
connects to it directly via its built-in ODBC/SQLite connector. If you later
want a "real" server-based warehouse for the portfolio story, swapping the
engine here for psycopg2 + a connection string is a small change — the
schema and load logic stay the same.

What this does beyond a raw dump:
  - Creates the table with an explicit schema (not just "whatever pandas
    guesses"), including a NOT NULL constraint on jobId and a UNIQUE index
    on it, so re-running ingest+transform+load never produces duplicate
    postings in the warehouse even across multiple days of snapshots.
  - Uses UPSERT (INSERT ... ON CONFLICT) so re-running this after a fresh
    ingest updates existing postings (e.g. keywordMatchScorePercentage
    might change if you update your skill list) instead of erroring out
    or duplicating rows.
  - Adds a `loaded_at_utc` column so you can see when each row was last
    refreshed, and a small `v_daily_summary` view for quick sanity checks.

Run:
  python load.py
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
PARQUET_PATH = BASE_DIR / "data" / "processed" / "jobs.parquet"
WAREHOUSE_DIR = BASE_DIR / "data" / "warehouse"
DB_PATH = WAREHOUSE_DIR / "jobs.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    jobId                       TEXT PRIMARY KEY,
    jobTitleClean               TEXT,
    companyName                 TEXT,
    city                        TEXT,
    seniority                   TEXT,
    contractType                TEXT,
    workType                    TEXT,
    sector                      TEXT,
    publishedAt                 TEXT,
    keywordMatchScorePercentage INTEGER,
    matchedKeywords              TEXT,
    unmatchedKeywords            TEXT,
    jobUrl                      TEXT,
    loaded_at_utc                TEXT NOT NULL
);
"""

SUMMARY_VIEW = """
CREATE VIEW IF NOT EXISTS v_daily_summary AS
SELECT
    seniority,
    COUNT(*)                                   AS postings,
    ROUND(AVG(keywordMatchScorePercentage), 1) AS avg_match_score,
    MAX(loaded_at_utc)                         AS last_loaded
FROM jobs
GROUP BY seniority
ORDER BY postings DESC;
"""

UPSERT = """
INSERT INTO jobs (
    jobId, jobTitleClean, companyName, city, seniority, contractType,
    workType, sector, publishedAt, keywordMatchScorePercentage,
    matchedKeywords, unmatchedKeywords, jobUrl, loaded_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(jobId) DO UPDATE SET
    jobTitleClean=excluded.jobTitleClean,
    companyName=excluded.companyName,
    city=excluded.city,
    seniority=excluded.seniority,
    contractType=excluded.contractType,
    workType=excluded.workType,
    sector=excluded.sector,
    publishedAt=excluded.publishedAt,
    keywordMatchScorePercentage=excluded.keywordMatchScorePercentage,
    matchedKeywords=excluded.matchedKeywords,
    unmatchedKeywords=excluded.unmatchedKeywords,
    jobUrl=excluded.jobUrl,
    loaded_at_utc=excluded.loaded_at_utc;
"""


def main():
    if not PARQUET_PATH.exists():
        raise SystemExit(f"{PARQUET_PATH} not found. Run transform.py first.")

    df = pd.read_parquet(PARQUET_PATH)
    print(f"Read {len(df)} rows from {PARQUET_PATH}")

    # array columns come out of parquet as numpy arrays/lists; flatten to
    # pipe-delimited strings for SQLite storage (mirrors the CSV output).
    for col in ("matchedKeywords", "unmatchedKeywords"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: "|".join(v) if hasattr(v, "__iter__") and not isinstance(v, str) else (v or "")
            )

    loaded_at = datetime.now(timezone.utc).isoformat()

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(SCHEMA)
        conn.execute(SUMMARY_VIEW)

        rows = [
            (
                r.jobId, r.jobTitleClean, r.companyName, r.city, r.seniority,
                r.contractType, r.workType, r.sector, r.publishedAt,
                int(r.keywordMatchScorePercentage) if pd.notna(r.keywordMatchScorePercentage) else None,
                r.matchedKeywords, r.unmatchedKeywords, r.jobUrl, loaded_at,
            )
            for r in df.itertuples(index=False)
        ]
        conn.executemany(UPSERT, rows)
        conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        print(f"Upserted {len(rows)} rows. Warehouse table now has {total} total rows.")

        print("\nv_daily_summary:")
        for row in conn.execute("SELECT * FROM v_daily_summary"):
            print(" ", row)
    finally:
        conn.close()

    print(f"\nWarehouse file: {DB_PATH}")


if __name__ == "__main__":
    main()
