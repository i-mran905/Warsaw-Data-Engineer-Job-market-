# Deployment & CI/CD

## Current state: local, on demand

This pipeline is **not deployed to any cloud environment.** It runs on a
single machine, invoked by hand, three scripts in sequence. That's an honest
statement of what exists — everything below the "CI" section is a
forward-looking plan, not something that's running.

Running it locally:

```bash
pip install -r requirements.txt
export APIFY_API_TOKEN="..."
python ingest.py
python transform.py
python load.py
```

Prerequisites: Python 3.9+ and a Java runtime (8/11/17) on `PATH` for
PySpark.

## CI (this part is real)

Continuous integration runs on GitHub Actions (`.github/workflows/ci.yml`)
on every push and PR to `main`:

- `ruff check .` — lint
- `ruff format --check .` — formatting
- `pytest` — unit tests

There is no continuous *deployment* — nothing is shipped anywhere on merge,
because there's no running service to ship to.

## How I would deploy this if productionized

Written as a plan, to show the path — not implemented.

1. **Orchestration.** Wrap the three stages in an Airflow or Dagster DAG:
   `ingest → transform → load`, with retries and a daily schedule, so it runs
   unattended instead of by hand.
2. **Warehouse.** Move from the local SQLite file to a managed Postgres (or a
   lakehouse table). The schema and upsert logic are already portable — see
   `docs/database-design.md`.
3. **Secrets.** The Apify token would come from the orchestrator's secret
   store / environment, never committed. `.gitignore` already excludes
   `.env`.
4. **Storage.** Raw snapshots would land in object storage (e.g. S3/ADLS)
   instead of a local `data/raw/` directory, keeping the immutable-raw
   property while making it durable and shareable.
5. **Compute.** For real data volumes, run the transform on an actual Spark
   cluster (e.g. Databricks) rather than `local[*]`.

Each of these is deliberately a small delta from the current design, which
was structured with this path in mind — but none of it is done, and the repo
doesn't claim otherwise.
