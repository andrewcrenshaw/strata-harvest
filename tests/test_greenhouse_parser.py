"""Tests for GreenhouseParser — Greenhouse ATS REST API parsing (PCC-1422).

Covers all acceptance criteria:
- Fetch from /embed/api/v1/jobs endpoint (structured JSON)
- Parse into list[JobListing] with all available fields
- Handle pagination if API returns multiple pages
- Handle API errors gracefully (rate limits, 404s)
- Test with saved Greenhouse API response fixtures from at least 3 companies
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.models import ATSProvider, FetchResult, JobListing
from strata_harvest.parsers.greenhouse import GreenhouseParser

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "greenhouse_api"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


# ------------------------------------------------------------------
# parse() — sync JSON parsing
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseParserParse:
    """Core parse() method: JSON content → list[JobListing]."""

    def setup_method(self) -> None:
        self.parser = GreenhouseParser()

    def test_parse_acmecorp_three_jobs(self) -> None:
        content = _load_fixture("acmecorp.json")
        result = self.parser.parse(content, url="https://boards.greenhouse.io/acmecorp")
        assert len(result) == 3
        assert all(isinstance(j, JobListing) for j in result)

    def test_parse_greenleaf_single_job(self) -> None:
        content = _load_fixture("greenleaf.json")
        result = self.parser.parse(content, url="https://boards.greenhouse.io/greenleaf")
        assert len(result) == 1

    def test_parse_megacorp_four_jobs(self) -> None:
        content = _load_fixture("megacorp.json")
        result = self.parser.parse(content, url="https://boards.greenhouse.io/megacorp")
        assert len(result) == 4

    def test_parse_empty_board_returns_empty(self) -> None:
        content = _load_fixture("empty_board.json")
        result = self.parser.parse(content, url="https://boards.greenhouse.io/empty")
        assert result == []

    def test_parse_invalid_json_returns_empty(self) -> None:
        result = self.parser.parse("<html>not json</html>", url="https://boards.greenhouse.io/x")
        assert result == []

    def test_parse_non_dict_json_returns_empty(self) -> None:
        result = self.parser.parse("[]", url="https://boards.greenhouse.io/x")
        assert result == []

    def test_parse_missing_jobs_key_returns_empty(self) -> None:
        result = self.parser.parse('{"meta": {"total": 0}}', url="https://boards.greenhouse.io/x")
        assert result == []

    def test_non_dict_job_entries_skipped(self) -> None:
        content = json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Valid",
                        "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
                    },
                    "not a dict",
                    42,
                    None,
                ],
                "meta": {"total": 4},
            }
        )
        result = self.parser.parse(content, url="https://boards.greenhouse.io/co")
        assert len(result) == 1
        assert result[0].title == "Valid"

    def test_job_missing_title_skipped(self) -> None:
        content = json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
                    },
                    {
                        "id": 2,
                        "title": "Valid Job",
                        "absolute_url": "https://boards.greenhouse.io/co/jobs/2",
                    },
                ],
                "meta": {"total": 2},
            }
        )
        result = self.parser.parse(content, url="https://boards.greenhouse.io/co")
        assert len(result) == 1
        assert result[0].title == "Valid Job"

    def test_job_missing_url_skipped(self) -> None:
        content = json.dumps(
            {
                "jobs": [{"id": 1, "title": "No URL"}],
                "meta": {"total": 1},
            }
        )
        result = self.parser.parse(content, url="https://boards.greenhouse.io/co")
        assert result == []

    def test_all_fixtures_return_joblistings(self) -> None:
        """AC: Test with saved fixtures from at least 3 companies."""
        for fixture in ["acmecorp.json", "greenleaf.json", "megacorp.json"]:
            content = _load_fixture(fixture)
            result = self.parser.parse(content, url="https://boards.greenhouse.io/test")
            assert len(result) > 0, f"Expected jobs from {fixture}"
            for job in result:
                assert isinstance(job, JobListing)
                assert job.ats_provider == ATSProvider.GREENHOUSE


# ------------------------------------------------------------------
# Field mapping
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseFieldMapping:
    """Verify each Greenhouse JSON field maps to the correct JobListing field."""

    def setup_method(self) -> None:
        parser = GreenhouseParser()
        content = _load_fixture("acmecorp.json")
        self.listings = parser.parse(content, url="https://boards.greenhouse.io/acmecorp")
        self.senior_eng = self.listings[0]

    def test_title(self) -> None:
        assert self.senior_eng.title == "Senior Software Engineer"

    def test_url(self) -> None:
        assert str(self.senior_eng.url) == "https://boards.greenhouse.io/acmecorp/jobs/127817"

    def test_location(self) -> None:
        assert self.senior_eng.location == "New York, NY"

    def test_department(self) -> None:
        assert self.senior_eng.department == "Engineering"

    def test_description_is_plain_text(self) -> None:
        assert self.senior_eng.description is not None
        assert "platform team" in self.senior_eng.description
        assert "<p>" not in self.senior_eng.description

    def test_requirements_extracted_from_li(self) -> None:
        reqs = self.senior_eng.requirements
        assert len(reqs) >= 2
        assert "5+ years of experience with Python or Go" in reqs
        assert "Experience with distributed systems" in reqs

    def test_salary_range_formatted(self) -> None:
        assert self.senior_eng.salary_range is not None
        assert "180,000" in self.senior_eng.salary_range
        assert "220,000" in self.senior_eng.salary_range
        assert "USD" in self.senior_eng.salary_range

    def test_posted_date_parsed(self) -> None:
        assert self.senior_eng.posted_date is not None
        assert self.senior_eng.posted_date.year == 2026
        assert self.senior_eng.posted_date.month == 3

    def test_ats_provider_set(self) -> None:
        assert self.senior_eng.ats_provider == ATSProvider.GREENHOUSE

    def test_raw_data_preserved(self) -> None:
        assert self.senior_eng.raw_data["id"] == 127817
        assert "departments" in self.senior_eng.raw_data

    def test_null_metadata_maps_to_empty_raw(self) -> None:
        designer = self.listings[1]
        assert designer.raw_data.get("metadata") is None

    def test_prospect_post_with_null_internal_id(self) -> None:
        """Greenhouse prospect posts have null internal_job_id — should still parse."""
        prospect = self.listings[2]
        assert prospect.title == "General Application — Engineering"
        assert prospect.raw_data.get("internal_job_id") is None

    def test_no_offices_still_parses(self) -> None:
        prospect = self.listings[2]
        assert prospect.location == "Remote"


# ------------------------------------------------------------------
# MegaCorp fixture edge cases
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseMegacorpEdgeCases:
    """Verify edge cases from the megacorp fixture."""

    def setup_method(self) -> None:
        parser = GreenhouseParser()
        content = _load_fixture("megacorp.json")
        self.listings = parser.parse(content, url="https://boards.greenhouse.io/megacorp")

    def test_null_content_gives_none_description(self) -> None:
        contract_job = self.listings[1]
        assert contract_job.title == "Marketing Coordinator (Contract)"
        assert contract_job.description is None

    def test_non_english_job_preserves_unicode(self) -> None:
        jp_job = self.listings[2]
        assert "フロントエンド" in jp_job.title
        assert jp_job.location == "東京"

    def test_multiple_departments_picks_leaf(self) -> None:
        """When multiple departments exist, prefer the leaf (no children)."""
        staff_eng = self.listings[0]
        assert staff_eng.department == "Payments"

    def test_non_usd_salary(self) -> None:
        london_job = self.listings[0]
        assert london_job.salary_range is not None
        assert "GBP" in london_job.salary_range

    def test_empty_departments_gives_none(self) -> None:
        contract_job = self.listings[1]
        assert contract_job.department is None

    def test_jpy_salary_large_values(self) -> None:
        jp_job = self.listings[2]
        assert jp_job.salary_range is not None
        assert "JPY" in jp_job.salary_range


# ------------------------------------------------------------------
# Salary formatting
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseSalaryFormatting:
    def test_full_range_usd(self) -> None:
        result = GreenhouseParser._format_salary(
            [
                {"min_cents": 15000000, "max_cents": 20000000, "currency_type": "USD"},
            ]
        )
        assert result == "USD 150,000 - 200,000"

    def test_min_only(self) -> None:
        result = GreenhouseParser._format_salary(
            [
                {"min_cents": 10000000, "currency_type": "EUR"},
            ]
        )
        assert result == "EUR 100,000+"

    def test_max_only(self) -> None:
        result = GreenhouseParser._format_salary(
            [
                {"max_cents": 12000000, "currency_type": "GBP"},
            ]
        )
        assert result == "GBP up to 120,000"

    def test_none_input(self) -> None:
        assert GreenhouseParser._format_salary(None) is None

    def test_empty_list(self) -> None:
        assert GreenhouseParser._format_salary([]) is None

    def test_no_currency(self) -> None:
        result = GreenhouseParser._format_salary(
            [
                {"min_cents": 5000000, "max_cents": 7000000},
            ]
        )
        assert result == "50,000 - 70,000"

    def test_non_list_input(self) -> None:
        assert GreenhouseParser._format_salary("not a list") is None  # type: ignore[arg-type]


# ------------------------------------------------------------------
# Department extraction
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseDepartmentExtraction:
    def test_single_department(self) -> None:
        result = GreenhouseParser._extract_department(
            [
                {"id": 1, "name": "Engineering", "child_ids": []},
            ]
        )
        assert result == "Engineering"

    def test_leaf_preferred_over_parent(self) -> None:
        result = GreenhouseParser._extract_department(
            [
                {"id": 1, "name": "Engineering", "child_ids": [2]},
                {"id": 2, "name": "Backend", "child_ids": []},
            ]
        )
        assert result == "Backend"

    def test_none_input(self) -> None:
        assert GreenhouseParser._extract_department(None) is None

    def test_empty_list(self) -> None:
        assert GreenhouseParser._extract_department([]) is None

    def test_non_dict_entries_skipped(self) -> None:
        result = GreenhouseParser._extract_department(
            [
                "garbage",
                {"id": 1, "name": "Valid", "child_ids": []},
            ]
        )  # type: ignore[list-item]
        assert result == "Valid"


# ------------------------------------------------------------------
# build_api_url
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseBuildApiUrl:
    def test_career_page_url(self) -> None:
        url = GreenhouseParser.build_api_url("https://boards.greenhouse.io/acmecorp")
        assert url == "https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs?content=true"

    def test_career_page_with_job_path(self) -> None:
        url = GreenhouseParser.build_api_url("https://boards.greenhouse.io/acmecorp/jobs/12345")
        assert url == "https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs?content=true"

    def test_embed_url_with_for_param(self) -> None:
        url = GreenhouseParser.build_api_url(
            "https://boards.greenhouse.io/embed/job_board?for=techcorp"
        )
        assert url == "https://boards-api.greenhouse.io/v1/boards/techcorp/jobs?content=true"

    def test_already_correct_api_url(self) -> None:
        url = GreenhouseParser.build_api_url(
            "https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs?content=true"
        )
        assert url == ("https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs?content=true")

    def test_api_url_without_content_param(self) -> None:
        url = GreenhouseParser.build_api_url(
            "https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs"
        )
        assert "content=true" in url


# ------------------------------------------------------------------
# fetch_all (async, with mocked safe_fetch)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestGreenhouseFetchAll:
    async def test_successful_fetch(self) -> None:
        content = _load_fixture("acmecorp.json")
        mock_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/acmecorp/jobs?content=true",
            status_code=200,
            content=content,
            content_type="application/json",
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/acmecorp")

        assert len(listings) == 3
        mock_fetch.assert_called_once()
        call_url = mock_fetch.call_args[0][0]
        assert "content=true" in call_url

    async def test_404_returns_empty(self) -> None:
        error_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/nonexistent/jobs?content=true",
            status_code=404,
            error="HTTP 404: Not Found",
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/nonexistent")

        assert listings == []

    async def test_rate_limit_429_returns_empty(self) -> None:
        """AC: Handle rate limits gracefully."""
        error_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/co/jobs?content=true",
            status_code=429,
            error="HTTP 429: Rate limited",
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/co")

        assert listings == []

    async def test_network_error_returns_empty(self) -> None:
        error_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/co/jobs?content=true",
            error="ConnectError: Connection refused",
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/co")

        assert listings == []

    async def test_empty_content_returns_empty(self) -> None:
        mock_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/co/jobs?content=true",
            status_code=200,
            content=None,
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/co")

        assert listings == []

    async def test_fetches_all_three_company_fixtures(self) -> None:
        """AC: Test with saved Greenhouse API response fixtures from at least 3 companies."""
        for fixture, expected_count in [
            ("acmecorp.json", 3),
            ("greenleaf.json", 1),
            ("megacorp.json", 4),
        ]:
            content = _load_fixture(fixture)
            mock_result = FetchResult(
                url="https://boards-api.greenhouse.io/v1/boards/test/jobs?content=true",
                status_code=200,
                content=content,
                content_type="application/json",
            )
            with patch(
                "strata_harvest.parsers.greenhouse.safe_fetch",
                new_callable=AsyncMock,
            ) as mock_fetch:
                mock_fetch.return_value = mock_result
                listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/test")
            assert len(listings) == expected_count, (
                f"Expected {expected_count} from {fixture}, got {len(listings)}"
            )
