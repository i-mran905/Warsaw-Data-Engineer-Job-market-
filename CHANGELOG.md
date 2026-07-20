# Changelog

## [1.1.0] — 2026-07-19

Went back through this after the initial version and cleaned it up — mostly
testability and observability, no change to what the pipeline actually
outputs.

- Pulled the title/seniority/city cleanup logic out of `transform.py` into
  its own `normalize.py`. It was tangled up with the Spark code before,
  which made it annoying to test — now it's plain Python and I added
  `tests/test_normalize.py` (13 cases, including the annoying edge cases
  like a title and `experienceLevel` disagreeing with each other).
- Swapped `print()` for proper `logging` in all three scripts.
- Added actual error handling instead of letting things blow up with a
  traceback: `ingest.py` catches a failed Apify run and tells you why,
  `load.py` rolls back the SQLite write if it fails partway instead of
  leaving a half-written table, `transform.py` closes the Spark session
  even if something crashes mid-run.
- Set up GitHub Actions to run ruff + pytest on every push (`.github/workflows/ci.yml`).
- Added a LICENSE (MIT) — never got around to it before.
- Rewrote the README with an actual architecture diagram instead of just
  the ASCII arrows.

## [1.0.0] — 2026-07-12

First version. Apify ingest → PySpark transform → SQLite load, tracking
live "Data Engineer" postings in Warsaw.

## Not done yet

Being upfront about the gaps instead of pretending they don't exist:

- `ingest.py` and `transform.py` don't have automated tests — would need a
  mocked Apify client and a Spark session in CI, which is more setup than
  I've put in so far.
- `load.py` targets SQLite because it's zero-setup. If this ever needed to
  handle more than one person's worth of data, swapping in Postgres would
  be a small change (noted in the docstring) but it's not done.
