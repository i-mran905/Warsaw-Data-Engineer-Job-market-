# Database Design

The warehouse is a single SQLite database, `data/warehouse/jobs.db`, created
and populated by `load.py`.

## `jobs` table

One row per job posting, keyed on the LinkedIn job ID.

| Column | Type | Notes |
|--------|------|-------|
| `jobId` | TEXT | **Primary key.** LinkedIn's own ID — the natural key for a posting. |
| `jobTitleClean` | TEXT | Title with decorative emoji stripped. |
| `companyName` | TEXT | |
| `city` | TEXT | Extracted from the free-text location field. |
| `seniority` | TEXT | junior / mid / senior / unspecified. |
| `contractType` | TEXT | e.g. Full-time. |
| `workType` | TEXT | |
| `sector` | TEXT | |
| `publishedAt` | TEXT | ISO date from the source. |
| `keywordMatchScorePercentage` | INTEGER | Skill-match score at ingest time. |
| `matchedKeywords` | TEXT | Pipe-delimited (`Python\|SQL\|...`). |
| `unmatchedKeywords` | TEXT | Pipe-delimited. |
| `jobUrl` | TEXT | |
| `loaded_at_utc` | TEXT | **NOT NULL.** When this row was last upserted. |

### Key choices

- **`jobId` as primary key** gives idempotency for free. The load uses
  `INSERT ... ON CONFLICT(jobId) DO UPDATE`, so re-running never duplicates a
  posting and always reflects the latest snapshot.
- **`loaded_at_utc NOT NULL`** guarantees every row carries a load timestamp,
  which is what a future history/trend feature would build on.
- **Array fields flattened to pipe-delimited TEXT.** SQLite has no array
  type, so `matchedKeywords` / `unmatchedKeywords` are stored as
  `Python|SQL|...`. This mirrors the CSV output and stays queryable with
  `LIKE`.

## `v_daily_summary` view

A convenience view for a quick sanity check straight from SQL, without a BI
tool:

```sql
SELECT seniority,
       COUNT(*)                                   AS postings,
       ROUND(AVG(keywordMatchScorePercentage), 1) AS avg_match_score,
       MAX(loaded_at_utc)                         AS last_loaded
FROM jobs
GROUP BY seniority
ORDER BY postings DESC;
```

## Path to Postgres

If this needed concurrent writers or larger volumes, the migration is
deliberately small: the `CREATE TABLE`, the `ON CONFLICT` upsert, and the
view are all standard SQL that Postgres also supports. `load.py` would swap
`sqlite3.connect(...)` for a `psycopg2` connection; the schema and load logic
stay put. This is noted directly in `load.py`'s module docstring.
