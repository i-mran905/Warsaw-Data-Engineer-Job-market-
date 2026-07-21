# Testing

## What's tested

`tests/test_normalize.py` covers the pure logic in `normalize.py` — the only
part of the pipeline where the interesting behaviour lives:

- `clean_title` — emoji stripping, whitespace collapsing, `None` handling.
- `derive_seniority` — title-based classification for each bucket, the
  fallback to `experienceLevel`, the default to "unspecified", and the case
  where the title and `experienceLevel` disagree (title wins).
- `extract_city` — full "City, Region, Country" strings, single-token
  locations, empty/`None` input.

Run them with:

```bash
pip install -r requirements-dev.txt
pytest
```

## Why the scope is what it is

The pipeline has three scripts, but only `normalize.py`'s functions are
worth (and cheap enough) to unit-test directly:

- **`ingest.py`** talks to a live Apify API. Testing it properly means
  mocking the client — worth doing, not done yet.
- **`transform.py`** needs a JVM and a Spark session to run at all. Its own
  logic is thin (it wires the `normalize.py` functions into UDFs and writes
  files); the substance it delegates *is* tested.

This is why the pure functions were pulled out of `transform.py` into
`normalize.py` in the first place: it lets the meaningful logic be tested in
milliseconds with no JVM, instead of being untestable inside a Spark job.

## Linting and formatting

`ruff` handles both:

```bash
ruff check .          # lint
ruff format --check . # formatting
```

Configuration is in `pyproject.toml` (rule set: pycodestyle/pyflakes errors,
import sorting, pyupgrade, and flake8-bugbear).

## CI

`.github/workflows/ci.yml` runs `ruff check`, `ruff format --check`, and
`pytest` on every push and pull request to `main`. See `docs/deployment.md`
for the CI/CD section in full.

## Known gaps

Tracked honestly in `docs/limitations.md` and `docs/future-improvements.md`:
no integration test for the Spark job, and no mocked test for the live
ingest.
