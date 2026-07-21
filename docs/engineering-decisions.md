# Engineering Decisions

A record of the non-obvious choices in this project and the reasoning behind
them. These are the decisions I'd expect to be asked about, so they're
written down rather than left implicit in the code.

## Seniority is read from the job title first, not LinkedIn's field

LinkedIn exposes an `experienceLevel` field, but it's blank ("Not
Applicable") on roughly a third of the postings in the sample. The job
title, on the other hand, almost always says "Senior", "Junior", "Lead",
etc. So `derive_seniority` checks the title against regex patterns first and
only falls back to `experienceLevel` when the title carries no signal.

Trade-off: the title patterns are a fixed keyword list, so an unusual title
wording can be missed. I accepted that because title text is far higher
signal than a field that's empty a third of the time, and the fallback
still catches the cases where the title is silent.

## Raw snapshots are timestamped and never overwritten

Every ingest writes `raw_linkedin_jobs_<...>_<timestamp>.json`. This costs
disk but buys a re-runnable history: I can re-transform any past day's
snapshot, and if I change the skill list I can see how match scores would
have differed historically. Overwriting would throw that away for no real
saving.

## SQLite, not Postgres

The warehouse is a single SQLite file. For a single-machine, single-user
analytics project this is the right amount of database: zero setup, one
portable file, and it opens directly in Power BI, DB Browser, or the
`sqlite3` CLI. `load.py`'s docstring notes exactly what would change to move
to Postgres (swap the connection, keep the schema and upsert logic). I
didn't do that swap because there's no multi-user or concurrency requirement
that would justify it — see `docs/limitations.md`.

## Load is an UPSERT, not an INSERT

`load.py` keys the `jobs` table on `jobId` and uses
`INSERT ... ON CONFLICT DO UPDATE`. This makes the load idempotent: running
the pipeline on a new day's snapshot updates postings that still exist and
adds new ones, without duplicating anything or erroring on a repeat. It also
means re-running load after changing the skill list refreshes the match
scores in place.

## Pure logic is separated from Spark

`normalize.py` holds the title/seniority/city functions as plain Python with
no Spark import. `transform.py` wraps them in UDFs. The point is
testability: the interesting logic can be unit-tested without a JVM, which
is what keeps CI fast and dependency-light (see `docs/testing.md`).

## Logging over print

All three scripts use the `logging` module rather than `print`, so output
carries timestamps and levels and can be filtered or redirected like any
real job's logs. `print` would have been shorter but throws that away.
