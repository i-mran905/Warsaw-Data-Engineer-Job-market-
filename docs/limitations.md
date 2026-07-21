# Limitations

Being explicit about what this project does not do, so nobody has to reverse-
engineer the boundaries from the code.

## Data source

- **Single source.** Postings come only from LinkedIn via one Apify actor.
  It is not a census of the Warsaw market — roles posted only on other job
  boards, or only on company career pages, are not captured.
- **Point-in-time snapshots.** Each run captures whatever was live at crawl
  time. A posting taken down between runs simply stops appearing; the
  pipeline does not track a posting's full lifecycle.
- **Skill match is keyword-based.** The `keywordMatchScorePercentage` comes
  from Apify matching my skill list against the posting text. It is a
  keyword overlap, not a semantic understanding of the role — a posting that
  says "no Python required" still matches on the word "Python".

## Scale

- **Single machine.** PySpark runs in `local[*]` mode. The design is
  Spark-shaped, but it has only ever been run against hundreds of rows, not
  the millions where a real cluster would matter.
- **SQLite warehouse.** One file, one writer. Fine for this use case,
  unsuitable for concurrent writers or large multi-user analytics.

## Classification

- **Seniority is heuristic.** `derive_seniority` uses a fixed keyword list.
  Unusual title phrasing can fall through to "unspecified".
- **No historical trend analysis yet.** The warehouse stores `loaded_at_utc`
  per row, so the data to do day-over-day trends exists, but the pipeline
  doesn't compute trends — it reports the latest state.

## Testing

- **Only `normalize.py` is unit-tested.** `ingest.py` hits a live API and
  `transform.py` needs a JVM, so neither has automated tests. They're
  verified by running them manually. See `docs/testing.md` and
  `docs/future-improvements.md`.
