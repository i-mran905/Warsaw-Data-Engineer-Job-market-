# Contributing

This is a solo portfolio project, not a maintained open-source library, so
don't expect fast turnaround on PRs — but if you spot a bug or want to
extend it (different city, different role keyword, whatever), feel free to
open an issue or send a PR.

## Getting it running

```bash
git clone https://github.com/i-mran905/Warsaw-Data-Engineer-Job-market-.git
cd Warsaw-Data-Engineer-Job-market-
pip install -r requirements.txt -r requirements-dev.txt
```

You'll also need an Apify account/API token to run `ingest.py`, and a Java
runtime on your `PATH` for PySpark to run `transform.py`. Both are free.

## Before you send a PR

```bash
ruff check .
ruff format --check .
pytest
```

These run in CI too, so if they fail locally they'll fail on the PR.

Heads up: only `normalize.py` has real unit tests right now. `ingest.py`
hits a live API and `transform.py` needs a JVM, so I haven't gotten around
to properly mocking those — if you touch either one, just say in the PR
what you actually ran to check it still works.
