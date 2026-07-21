# Future Improvements

Ordered roughly by value-for-effort. Nothing here is implemented — this is a
roadmap, not a description of what exists.

## Testing

- **Mock the Apify client** so `ingest.py`'s dataset-pull-and-parse logic can
  be unit-tested without a live token.
- **Run `transform.py` against a tiny local Spark session in CI** using a
  small fixture snapshot, so the actual PySpark job (dedupe, UDF wiring,
  output schema) is covered, not just the pure functions inside it.

## Data model

- **Track posting history over time.** `loaded_at_utc` is already stored, so
  a `jobs_history` table or a slowly-changing-dimension pattern could record
  when a posting first appeared and when it disappeared, enabling
  day-over-day trend queries.
- **Split company into its own dimension table** so company-level attributes
  (sector, size) aren't repeated on every posting row.

## Robustness

- **Retry/backoff around the Apify call** for transient network failures.
- **Schema validation on the raw JSON** (e.g. with `pydantic`) so a change in
  the actor's output shape fails loudly at ingest instead of surfacing as a
  confusing error deep in the transform.

## Scale / productionization

- **Swap SQLite for Postgres** if this ever needs concurrent access — the
  schema and load logic already anticipate it (see `docs/database-design.md`).
- **Orchestrate with Airflow or Dagster** to run the three stages on a
  schedule with dependency tracking and retries, instead of running them by
  hand. See `docs/deployment.md`.

## Analytics

- **Semantic skill matching** (embeddings) instead of keyword overlap, to
  fix the "no Python required still matches Python" problem noted in
  `docs/limitations.md`.
