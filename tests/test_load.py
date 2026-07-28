import sqlite3

import pandas as pd

from load import write_warehouse


def _job(match_score: int, title: str = "Data Engineer") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "jobId": "job-1",
                "jobTitleClean": title,
                "companyName": "Example",
                "city": "Warsaw",
                "seniority": "mid",
                "contractType": "Full-time",
                "workType": "Hybrid",
                "sector": "Technology",
                "publishedAt": "2026-07-01",
                "keywordMatchScorePercentage": match_score,
                "matchedKeywords": ["Python", "SQL"],
                "unmatchedKeywords": ["Azure"],
                "jobUrl": "https://example.com/job-1",
            }
        ]
    )


def test_reloading_a_job_updates_without_duplicating(tmp_path):
    db_path = tmp_path / "jobs.db"

    assert write_warehouse(_job(60), db_path, "2026-07-01T00:00:00+00:00") == 1
    assert (
        write_warehouse(
            _job(80, "Analytics Engineer"),
            db_path,
            "2026-07-02T00:00:00+00:00",
        )
        == 1
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT jobTitleClean, keywordMatchScorePercentage,
                   matchedKeywords, loaded_at_utc
            FROM jobs
            WHERE jobId = 'job-1'
            """
        ).fetchone()

    assert row == (
        "Analytics Engineer",
        80,
        "Python|SQL",
        "2026-07-02T00:00:00+00:00",
    )
