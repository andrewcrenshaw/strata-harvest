"""Tests for LeverParser — Lever ATS JSON API parsing."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.models import ATSProvider, FetchResult, JobListing
from strata_harvest.parsers.lever import LeverParser

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "lever"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


# ------------------------------------------------------------------
# parse() — sync JSON parsing
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverParserParse:
    """Core parse() method: JSON content → list[JobListing]."""

    def setup_method(self) -> None:
        self.parser = LeverParser()

    def test_parse_multi_postings(self) -> None:
        content = _load_fixture("lever_multi_postings.json")
        result = self.parser.parse(content, url="https://jobs.lever.co/acmecorp")
        assert len(result) == 3
        assert all(isinstance(j, JobListing) for j in result)

    def test_parse_single_posting_object(self) -> None:
        content = _load_fixture("lever_single_posting.json")
        result = self.parser.parse(content, url="https://jobs.lever.co/acmecorp")
        assert len(result) == 1
        assert result[0].title == "VP of Engineering"

    def test_parse_empty_array(self) -> None:
        result = self.parser.parse("[]", url="https://jobs.lever.co/acmecorp")
        assert result == []

    def test_parse_invalid_json_returns_empty(self) -> None:
        result = self.parser.parse("<html>not json</html>", url="https://jobs.lever.co/x")
        assert result == []

    def test_parse_non_dict_entries_skipped(self) -> None:
        content = json.dumps([
            {"text": "Valid", "hostedUrl": "https://jobs.lever.co/co/123"},
            "not a dict",
            42,
            None,
        ])
        result = self.parser.parse(content, url="https://jobs.lever.co/co")
        assert len(result) == 1
        assert result[0].title == "Valid"

    def test_parse_posting_without_url_or_id_skipped(self) -> None:
        content = json.dumps([{"text": "No URL or ID"}])
        result = self.parser.parse(content, url="https://jobs.lever.co/co")
        assert result == []

    def test_parse_posting_with_id_but_no_url_uses_fallback(self) -> None:
        content = json.dumps([{"id": "fallback-id", "text": "Fallback"}])
        result = self.parser.parse(content, url="https://jobs.lever.co/co")
        assert len(result) == 1
        assert "fallback-id" in str(result[0].url)


# ------------------------------------------------------------------
# Field mapping
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverFieldMapping:
    """Verify each Lever JSON field maps to the correct JobListing field."""

    def setup_method(self) -> None:
        parser = LeverParser()
        content = _load_fixture("lever_multi_postings.json")
        self.listings = parser.parse(content, url="https://jobs.lever.co/acmecorp")
        self.senior_eng = self.listings[0]

    def test_title(self) -> None:
        assert self.senior_eng.title == "Senior Software Engineer"

    def test_url(self) -> None:
        assert str(self.senior_eng.url) == (
            "https://jobs.lever.co/acmecorp/abc-1001-def-2002-ghi-3003"
        )

    def test_location(self) -> None:
        assert self.senior_eng.location == "San Francisco, CA"

    def test_department(self) -> None:
        assert self.senior_eng.department == "Engineering"

    def test_employment_type_from_commitment(self) -> None:
        assert self.senior_eng.employment_type == "Full-time"

    def test_description_uses_plain_text(self) -> None:
        assert "senior engineer" in self.senior_eng.description.lower()
        assert "<div>" not in self.senior_eng.description

    def test_requirements_from_all_list_sections(self) -> None:
        reqs = self.senior_eng.requirements
        assert len(reqs) == 5
        assert "5+ years of professional software development experience" in reqs
        assert "Proficiency in Python or Go" in reqs
        assert "Experience with distributed systems" in reqs
        assert "Experience with Kubernetes" in reqs
        assert "Open source contributions" in reqs

    def test_salary_range_formatted(self) -> None:
        assert self.senior_eng.salary_range == "USD 150,000 - 220,000 per-year"

    def test_posted_date_is_timezone_aware(self) -> None:
        assert self.senior_eng.posted_date is not None
        assert isinstance(self.senior_eng.posted_date, datetime)
        assert self.senior_eng.posted_date.tzinfo is not None

    def test_ats_provider_set(self) -> None:
        assert self.senior_eng.ats_provider == ATSProvider.LEVER

    def test_raw_data_preserved(self) -> None:
        assert self.senior_eng.raw_data["id"] == "abc-1001-def-2002-ghi-3003"
        assert "categories" in self.senior_eng.raw_data

    def test_posting_without_salary_returns_none(self) -> None:
        designer = self.listings[1]
        assert designer.salary_range is None

    def test_posting_with_empty_lists(self) -> None:
        designer = self.listings[1]
        assert designer.requirements == []

    def test_intern_commitment_mapped(self) -> None:
        intern = self.listings[2]
        assert intern.employment_type == "Intern"

    def test_description_fallback_to_html_stripped(self) -> None:
        """When descriptionPlain is missing, HTML description is stripped."""
        parser = LeverParser()
        content = json.dumps({
            "id": "html-only",
            "text": "HTML Only",
            "description": "<div><b>Bold text</b> and normal.</div>",
            "hostedUrl": "https://jobs.lever.co/co/html-only",
        })
        result = parser.parse(content, url="https://jobs.lever.co/co")
        assert len(result) == 1
        assert result[0].description == "Bold text and normal."


# ------------------------------------------------------------------
# Salary formatting
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverSalaryFormatting:

    def test_full_range(self) -> None:
        result = LeverParser._format_salary(
            {"currency": "USD", "interval": "per-year", "min": 100000, "max": 150000}
        )
        assert result == "USD 100,000 - 150,000 per-year"

    def test_min_only(self) -> None:
        result = LeverParser._format_salary(
            {"currency": "EUR", "interval": "per-year", "min": 80000}
        )
        assert result == "EUR 80,000+ per-year"

    def test_max_only(self) -> None:
        result = LeverParser._format_salary(
            {"currency": "GBP", "interval": "per-year", "max": 120000}
        )
        assert result == "GBP up to 120,000 per-year"

    def test_none_input(self) -> None:
        assert LeverParser._format_salary(None) is None

    def test_empty_dict(self) -> None:
        assert LeverParser._format_salary({}) is None

    def test_no_currency_still_formats(self) -> None:
        result = LeverParser._format_salary(
            {"min": 50000, "max": 70000, "interval": "per-year"}
        )
        assert result == "50,000 - 70,000 per-year"

    def test_no_interval(self) -> None:
        result = LeverParser._format_salary({"currency": "USD", "min": 60000, "max": 90000})
        assert result == "USD 60,000 - 90,000"


# ------------------------------------------------------------------
# Requirements extraction
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverRequirementsExtraction:

    def test_extract_from_li_tags(self) -> None:
        lists = [{"text": "Requirements", "content": "<li>Python</li><li>SQL</li>"}]
        result = LeverParser._extract_requirements(lists)
        assert result == ["Python", "SQL"]

    def test_strips_nested_html(self) -> None:
        lists = [{"text": "Req", "content": "<li><b>Strong</b> communication</li>"}]
        result = LeverParser._extract_requirements(lists)
        assert result == ["Strong communication"]

    def test_multiple_sections_combined(self) -> None:
        lists = [
            {"text": "Requirements", "content": "<li>A</li><li>B</li>"},
            {"text": "Bonus", "content": "<li>C</li>"},
        ]
        result = LeverParser._extract_requirements(lists)
        assert result == ["A", "B", "C"]

    def test_empty_list(self) -> None:
        assert LeverParser._extract_requirements([]) == []

    def test_none_input(self) -> None:
        assert LeverParser._extract_requirements(None) == []

    def test_empty_content_skipped(self) -> None:
        lists = [{"text": "Req", "content": ""}]
        assert LeverParser._extract_requirements(lists) == []

    def test_non_dict_entries_skipped(self) -> None:
        lists = [{"text": "Req", "content": "<li>Valid</li>"}, "garbage"]  # type: ignore[list-item]
        result = LeverParser._extract_requirements(lists)
        assert result == ["Valid"]


# ------------------------------------------------------------------
# Timestamp parsing
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverTimestampParsing:

    def test_valid_millisecond_timestamp(self) -> None:
        result = LeverParser._parse_timestamp(1711929600000)
        assert result is not None
        assert result.year == 2024
        assert result.tzinfo is not None

    def test_none_returns_none(self) -> None:
        assert LeverParser._parse_timestamp(None) is None

    def test_string_returns_none(self) -> None:
        assert LeverParser._parse_timestamp("not a number") is None  # type: ignore[arg-type]

    def test_zero_timestamp(self) -> None:
        result = LeverParser._parse_timestamp(0)
        assert result is not None
        assert result.year == 1970


# ------------------------------------------------------------------
# build_api_url
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverBuildApiUrl:

    def test_career_page_url(self) -> None:
        url = LeverParser.build_api_url("https://jobs.lever.co/acmecorp")
        assert url == "https://api.lever.co/v0/postings/acmecorp?mode=json"

    def test_career_page_url_trailing_slash(self) -> None:
        url = LeverParser.build_api_url("https://jobs.lever.co/acmecorp/")
        assert url == "https://api.lever.co/v0/postings/acmecorp?mode=json"

    def test_eu_career_page_url(self) -> None:
        url = LeverParser.build_api_url("https://jobs.eu.lever.co/eucompany")
        assert url == "https://api.eu.lever.co/v0/postings/eucompany?mode=json"

    def test_already_correct_api_url(self) -> None:
        url = LeverParser.build_api_url(
            "https://api.lever.co/v0/postings/acmecorp?mode=json"
        )
        assert url == "https://api.lever.co/v0/postings/acmecorp?mode=json"

    def test_api_url_without_mode_appends_it(self) -> None:
        url = LeverParser.build_api_url("https://api.lever.co/v0/postings/acmecorp")
        assert url == "https://api.lever.co/v0/postings/acmecorp?mode=json"

    def test_api_url_with_existing_params(self) -> None:
        url = LeverParser.build_api_url(
            "https://api.lever.co/v0/postings/acmecorp?limit=10"
        )
        assert url == "https://api.lever.co/v0/postings/acmecorp?limit=10&mode=json"


# ------------------------------------------------------------------
# fetch_all (async, with mocked safe_fetch)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLeverFetchAll:

    async def test_single_page_fetch(self) -> None:
        content = _load_fixture("lever_multi_postings.json")
        mock_result = FetchResult(
            url="https://api.lever.co/v0/postings/acmecorp?mode=json&skip=0&limit=100",
            status_code=200,
            content=content,
            content_type="application/json",
        )
        with patch(
            "strata_harvest.parsers.lever.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            listings = await LeverParser.fetch_all("https://jobs.lever.co/acmecorp")

        assert len(listings) == 3
        mock_fetch.assert_called_once()

    async def test_api_error_returns_empty(self) -> None:
        error_result = FetchResult(
            url="https://api.lever.co/v0/postings/badcorp?mode=json&skip=0&limit=100",
            status_code=404,
            error="HTTP 404: Not Found",
        )
        with patch(
            "strata_harvest.parsers.lever.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await LeverParser.fetch_all("https://jobs.lever.co/badcorp")

        assert listings == []

    async def test_pagination_fetches_multiple_pages(self) -> None:
        page1 = json.dumps([
            {
                "id": f"id-{i}",
                "text": f"Job {i}",
                "hostedUrl": f"https://jobs.lever.co/co/{i}",
            }
            for i in range(3)
        ])
        page2 = json.dumps([
            {
                "id": "id-last",
                "text": "Last Job",
                "hostedUrl": "https://jobs.lever.co/co/last",
            }
        ])

        call_count = 0

        async def mock_fetch(url: str, **_: Any) -> FetchResult:
            nonlocal call_count
            call_count += 1
            content = page1 if call_count == 1 else page2
            return FetchResult(
                url=url,
                status_code=200,
                content=content,
                content_type="application/json",
            )

        with patch("strata_harvest.parsers.lever.safe_fetch", side_effect=mock_fetch):
            listings = await LeverParser.fetch_all(
                "https://jobs.lever.co/co", limit=3
            )

        assert len(listings) == 4
        assert call_count == 2

    async def test_empty_page_stops_pagination(self) -> None:
        with patch(
            "strata_harvest.parsers.lever.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = FetchResult(
                url="https://api.lever.co/v0/postings/co?mode=json&skip=0&limit=100",
                status_code=200,
                content="[]",
                content_type="application/json",
            )
            listings = await LeverParser.fetch_all("https://jobs.lever.co/co")

        assert listings == []
        mock_fetch.assert_called_once()
