"""Ground-truth validation: verify parsers produce expected output for every fixture.

Each fixture pair consists of a raw input file (API JSON / HTML) and a
corresponding expected-output JSON file under ``fixtures/expected/``.
These tests confirm that parser changes don't silently alter field mapping.

PCC-1427: Create test fixtures from real career pages.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.ashby import AshbyParser
from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.lever import LeverParser
from tests.fixture_loader import (
    has_expected,
    list_fixtures,
    load_expected,
    load_raw,
)


def _compare_listing(actual: JobListing, expected: dict, *, idx: int, fixture: str) -> None:
    """Assert that an actual JobListing matches the expected ground truth dict."""
    ctx = f"job[{idx}] in {fixture}"

    assert actual.title == expected["title"], f"{ctx}: title mismatch"
    assert str(actual.url) == expected["url"], f"{ctx}: url mismatch"
    assert actual.location == expected.get("location"), f"{ctx}: location mismatch"
    assert actual.department == expected.get("department"), f"{ctx}: department mismatch"
    assert actual.employment_type == expected.get("employment_type"), (
        f"{ctx}: employment_type mismatch"
    )
    assert actual.requirements == expected.get("requirements", []), f"{ctx}: requirements mismatch"
    assert actual.salary_range == expected.get("salary_range"), f"{ctx}: salary_range mismatch"

    expected_provider = expected.get("ats_provider")
    if expected_provider is not None:
        assert actual.ats_provider == ATSProvider(expected_provider), (
            f"{ctx}: ats_provider mismatch"
        )

    if expected.get("description") is not None:
        assert actual.description == expected["description"], f"{ctx}: description mismatch"

    if expected.get("posted_date") is not None:
        assert actual.posted_date is not None, f"{ctx}: posted_date is None"
        expected_dt = datetime.fromisoformat(expected["posted_date"])
        assert actual.posted_date == expected_dt, f"{ctx}: posted_date mismatch"


# ------------------------------------------------------------------
# Greenhouse ground truth
# ------------------------------------------------------------------

GREENHOUSE_FIXTURES = [
    ("acmecorp.json", "https://boards.greenhouse.io/acmecorp"),
    ("greenleaf.json", "https://boards.greenhouse.io/greenleaf"),
    ("megacorp.json", "https://boards.greenhouse.io/megacorp"),
]


@pytest.mark.verification
class TestGreenhouseGroundTruth:
    """Validate Greenhouse parser output matches expected ground truth."""

    @pytest.mark.parametrize("fixture_name,url", GREENHOUSE_FIXTURES)
    def test_matches_ground_truth(self, fixture_name: str, url: str) -> None:
        parser = GreenhouseParser()
        raw = load_raw("greenhouse_api", fixture_name)
        expected = load_expected("greenhouse_api", fixture_name)

        actual = parser.parse(raw, url=url)

        assert len(actual) == len(expected), (
            f"{fixture_name}: expected {len(expected)} jobs, got {len(actual)}"
        )
        for i, (act, exp) in enumerate(zip(actual, expected, strict=True)):
            _compare_listing(act, exp, idx=i, fixture=fixture_name)

    def test_all_greenhouse_fixtures_have_expected(self) -> None:
        """Every non-edge-case Greenhouse fixture has a ground truth file."""
        for name in ["acmecorp.json", "greenleaf.json", "megacorp.json"]:
            assert has_expected("greenhouse_api", name), (
                f"Missing expected output for greenhouse_api/{name}"
            )

    def test_at_least_three_companies(self) -> None:
        """AC: 3 Greenhouse API responses from different companies."""
        fixtures = list_fixtures("greenhouse_api", suffix=".json")
        company_fixtures = [f for f in fixtures if f != "empty_board.json"]
        assert len(company_fixtures) >= 3


# ------------------------------------------------------------------
# Lever ground truth
# ------------------------------------------------------------------

LEVER_FIXTURES = [
    ("lever_multi_postings.json", "https://jobs.lever.co/acmecorp"),
    ("lever_single_posting.json", "https://jobs.lever.co/acmecorp"),
]


@pytest.mark.verification
class TestLeverGroundTruth:
    """Validate Lever parser output matches expected ground truth."""

    @pytest.mark.parametrize("fixture_name,url", LEVER_FIXTURES)
    def test_matches_ground_truth(self, fixture_name: str, url: str) -> None:
        parser = LeverParser()
        raw = load_raw("lever", fixture_name)
        expected = load_expected("lever", fixture_name)

        actual = parser.parse(raw, url=url)

        assert len(actual) == len(expected), (
            f"{fixture_name}: expected {len(expected)} jobs, got {len(actual)}"
        )
        for i, (act, exp) in enumerate(zip(actual, expected, strict=True)):
            _compare_listing(act, exp, idx=i, fixture=fixture_name)

    def test_all_lever_fixtures_have_expected(self) -> None:
        for name in ["lever_multi_postings.json", "lever_single_posting.json"]:
            assert has_expected("lever", name), (
                f"Missing expected output for lever/{name}"
            )

    def test_at_least_two_fixtures(self) -> None:
        """AC: 2 Lever API responses."""
        fixtures = list_fixtures("lever", suffix=".json")
        assert len(fixtures) >= 2


# ------------------------------------------------------------------
# Ashby ground truth
# ------------------------------------------------------------------

ASHBY_FIXTURES = [
    ("ashby_job_board_response.json", "https://jobs.ashbyhq.com/acmecorp"),
    ("ashby_single_posting.json", "https://jobs.ashbyhq.com/acmecorp"),
]


@pytest.mark.verification
class TestAshbyGroundTruth:
    """Validate Ashby parser output matches expected ground truth."""

    @pytest.mark.parametrize("fixture_name,url", ASHBY_FIXTURES)
    def test_matches_ground_truth(self, fixture_name: str, url: str) -> None:
        parser = AshbyParser()
        raw = load_raw("ashby", fixture_name)
        expected = load_expected("ashby", fixture_name)

        actual = parser.parse(raw, url=url)

        assert len(actual) == len(expected), (
            f"{fixture_name}: expected {len(expected)} jobs, got {len(actual)}"
        )
        for i, (act, exp) in enumerate(zip(actual, expected, strict=True)):
            _compare_listing(act, exp, idx=i, fixture=fixture_name)

    def test_all_ashby_fixtures_have_expected(self) -> None:
        for name in ["ashby_job_board_response.json", "ashby_single_posting.json"]:
            assert has_expected("ashby", name), (
                f"Missing expected output for ashby/{name}"
            )

    def test_at_least_two_fixtures(self) -> None:
        """AC: 2 Ashby responses."""
        fixtures = list_fixtures("ashby", suffix=".json")
        non_edge = [f for f in fixtures if "error" not in f and "empty" not in f]
        assert len(non_edge) >= 2


# ------------------------------------------------------------------
# Raw HTML career pages (no recognized ATS)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestCareerPageFixtures:
    """Validate raw HTML career page fixtures exist with ground truth."""

    def test_at_least_three_no_ats_pages(self) -> None:
        """AC: 3 raw HTML career pages with no recognized ATS."""
        html_files = list_fixtures("career_pages", suffix=".html")
        no_ats = [
            f for f in html_files
            if not any(
                ats in f
                for ats in ("greenhouse", "lever", "ashby", "workday", "icims")
            )
        ]
        assert len(no_ats) >= 3, (
            f"Expected >= 3 no-ATS HTML pages, found {len(no_ats)}: {no_ats}"
        )

    def test_each_no_ats_page_has_expected_output(self) -> None:
        """Each no-ATS career page has a human-annotated ground truth file."""
        expected_pages = [
            "unknown_custom.json",
            "boutique_agency.json",
            "local_restaurant_group.json",
        ]
        for name in expected_pages:
            assert has_expected("career_pages", name), (
                f"Missing expected output for career_pages/{name}"
            )

    def test_career_page_expected_has_titles(self) -> None:
        """Ground truth for career pages includes job titles."""
        for name in ["unknown_custom.json", "boutique_agency.json", "local_restaurant_group.json"]:
            expected = load_expected("career_pages", name)
            assert len(expected) > 0, f"{name}: no expected jobs"
            for job in expected:
                assert "title" in job, f"{name}: job missing title"

    def test_boutique_agency_expected_content(self) -> None:
        expected = load_expected("career_pages", "boutique_agency.json")
        assert len(expected) == 3
        titles = [j["title"] for j in expected]
        assert "Senior Copywriter" in titles
        assert "UX Designer" in titles
        assert "Project Coordinator" in titles

    def test_local_restaurant_expected_content(self) -> None:
        expected = load_expected("career_pages", "local_restaurant_group.json")
        assert len(expected) == 3
        titles = [j["title"] for j in expected]
        assert any("Chef" in t for t in titles)
        assert any("Manager" in t for t in titles)
        assert any("Cook" in t for t in titles)


# ------------------------------------------------------------------
# Fixture loader utility
# ------------------------------------------------------------------


@pytest.mark.verification
class TestFixtureLoader:
    """Verify the fixture_loader utility works correctly."""

    def test_load_raw_greenhouse(self) -> None:
        raw = load_raw("greenhouse_api", "acmecorp.json")
        assert "Senior Software Engineer" in raw
        assert '"jobs"' in raw

    def test_load_raw_lever(self) -> None:
        raw = load_raw("lever", "lever_multi_postings.json")
        assert "Senior Software Engineer" in raw

    def test_load_raw_ashby(self) -> None:
        raw = load_raw("ashby", "ashby_job_board_response.json")
        assert "Senior Backend Engineer" in raw

    def test_load_raw_html(self) -> None:
        raw = load_raw("career_pages", "boutique_agency.html")
        assert "Brightpath Creative" in raw

    def test_load_expected_returns_list(self) -> None:
        expected = load_expected("greenhouse_api", "acmecorp.json")
        assert isinstance(expected, list)
        assert len(expected) == 3

    def test_load_raw_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_raw("greenhouse_api", "nonexistent.json")

    def test_load_expected_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_expected("greenhouse_api", "nonexistent.json")

    def test_list_fixtures_returns_sorted(self) -> None:
        names = list_fixtures("greenhouse_api", suffix=".json")
        assert names == sorted(names)
        assert "acmecorp.json" in names

    def test_list_fixtures_empty_provider(self) -> None:
        names = list_fixtures("nonexistent_provider")
        assert names == []

    def test_has_expected_true(self) -> None:
        assert has_expected("greenhouse_api", "acmecorp.json")

    def test_has_expected_false(self) -> None:
        assert not has_expected("greenhouse_api", "empty_board.json")

    def test_list_fixtures_with_suffix_filter(self) -> None:
        all_files = list_fixtures("career_pages")
        html_only = list_fixtures("career_pages", suffix=".html")
        assert len(html_only) <= len(all_files)
        assert all(f.endswith(".html") for f in html_only)
