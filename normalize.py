"""
normalize.py — Pure, dependency-free text-normalization helpers shared by the
pipeline.

These are split out of transform.py on purpose: they contain the only
business logic in the pipeline that's worth unit-testing on its own (title
cleanup, seniority classification, city extraction), and they don't need
PySpark to run. Keeping them here means `tests/test_normalize.py` runs in
milliseconds with no JVM and no Spark install, instead of paying Spark's
startup cost just to check a regex.

transform.py wraps these in PySpark UDFs; it does not duplicate the logic.
"""

import re
from typing import Optional

# Order matters: derive_seniority() checks these in sequence (senior, then
# junior, then mid) so a title like "Senior Associate" resolves to senior.
SENIOR_PATTERN = re.compile(r"\b(senior|staff|principal|lead|expert|architect)\b", re.I)
JUNIOR_PATTERN = re.compile(r"\b(junior|graduate|intern|entry)\b", re.I)
MID_PATTERN = re.compile(r"\b(mid|associate)\b", re.I)

# Used only when the job title itself gives no seniority signal. Keys must be
# lowercase — derive_seniority() lowercases experience_level before lookup.
EXPERIENCE_LEVEL_FALLBACK = {
    "entry level": "junior",
    "internship": "junior",
    "associate": "mid",
    "mid-senior level": "mid",
    "director": "senior",
}


def clean_title(title: Optional[str]) -> str:
    """Strip decorative emoji/pictographic characters and collapse whitespace.

    Some LinkedIn postings wrap their title in emoji (e.g. an orange-circle
    bullet on either side) purely for visual attention-grabbing. Left in,
    these break title-based grouping and search. Returns "" for None so
    callers never have to null-check the result.
    """
    if title is None:
        return ""
    no_emoji = re.sub(r"[\U0001F000-\U0001FFFF←-⯿☀-➿]", "", title)
    return re.sub(r"\s+", " ", no_emoji).strip()


def derive_seniority(title: Optional[str], experience_level: Optional[str]) -> str:
    """Classify a posting into junior / mid / senior / unspecified.

    The job title is checked first because LinkedIn's own `experienceLevel`
    field is blank ("Not Applicable") on roughly a third of postings, while
    the title text is far more reliable ("Senior Data Engineer", "Junior
    Analyst", etc.). `experience_level` is only consulted as a fallback when
    the title carries no seniority signal at all.
    """
    title = title or ""
    if SENIOR_PATTERN.search(title):
        return "senior"
    if JUNIOR_PATTERN.search(title):
        return "junior"
    if MID_PATTERN.search(title):
        return "mid"
    return EXPERIENCE_LEVEL_FALLBACK.get((experience_level or "").strip().lower(), "unspecified")


def extract_city(location: Optional[str]) -> str:
    """Pull a clean city name out of a free-text location string.

    LinkedIn's `location` field is typically "City, Region, Country"
    (e.g. "Warsaw, Mazowieckie, Poland"); this returns just the first
    comma-separated segment.
    """
    if not location:
        return ""
    return location.split(",")[0].strip()
