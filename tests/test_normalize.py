"""
Unit tests for normalize.py — the pure text-normalization logic used by
transform.py. No PySpark/JVM dependency, so this suite runs fast and is what
CI actually executes (see .github/workflows/ci.yml).
"""

from normalize import clean_title, derive_seniority, extract_city


class TestCleanTitle:
    def test_strips_surrounding_emoji(self):
        assert clean_title("🟠 Data Engineer 🟠") == "Data Engineer"

    def test_collapses_internal_whitespace(self):
        assert clean_title("Senior   Data   Engineer") == "Senior Data Engineer"

    def test_none_returns_empty_string(self):
        assert clean_title(None) == ""

    def test_plain_title_unchanged(self):
        assert clean_title("Data Engineer") == "Data Engineer"


class TestDeriveSeniority:
    def test_senior_from_title(self):
        assert derive_seniority("Senior Data Engineer", "") == "senior"
        assert derive_seniority("Staff Software Engineer", "Not Applicable") == "senior"
        assert derive_seniority("Lead Data Architect", "") == "senior"

    def test_junior_from_title(self):
        assert derive_seniority("Junior Data Engineer", "") == "junior"
        assert derive_seniority("Graduate Data Analyst", "") == "junior"

    def test_mid_from_title(self):
        assert derive_seniority("Mid Data Engineer", "") == "mid"
        assert derive_seniority("Associate Data Engineer", "") == "mid"

    def test_falls_back_to_experience_level_when_title_has_no_signal(self):
        assert derive_seniority("Data Engineer", "Entry level") == "junior"
        assert derive_seniority("Data Engineer", "Internship") == "junior"
        assert derive_seniority("Data Engineer", "Associate") == "mid"
        assert derive_seniority("Data Engineer", "Mid-Senior level") == "mid"
        assert derive_seniority("Data Engineer", "Director") == "senior"

    def test_defaults_to_unspecified(self):
        assert derive_seniority("Data Engineer", "Not Applicable") == "unspecified"
        assert derive_seniority("Data Engineer", "") == "unspecified"
        assert derive_seniority("", "") == "unspecified"

    def test_title_signal_takes_priority_over_experience_level(self):
        # LinkedIn's experienceLevel field is unreliable by design choice
        # here (see normalize.py docstring) — title should win on conflict.
        assert derive_seniority("Senior Data Engineer", "Entry level") == "senior"


class TestExtractCity:
    def test_first_segment_of_full_location(self):
        assert extract_city("Warsaw, Mazowieckie, Poland") == "Warsaw"

    def test_single_token_location(self):
        assert extract_city("Remote") == "Remote"

    def test_empty_or_none_returns_empty_string(self):
        assert extract_city("") == ""
        assert extract_city(None) == ""
