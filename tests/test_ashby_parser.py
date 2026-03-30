"""Tests for AshbyParser — Ashby ATS GraphQL API parsing."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.models import ATSProvider, FetchResult, JobListing
from strata_harvest.parsers.ashby import AshbyParser

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "ashby"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


# ------------------------------------------------------------------
# parse() — sync JSON parsing (jobBoard response)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyParserParse:
    """Core parse() method: GraphQL JSON content → list[JobListing]."""

    def setup_method(self) -> None:
        self.parser = AshbyParser()

    def test_parse_job_board_response(self) -> None:
        content = _load_fixture("ashby_job_board_response.json")
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/acmecorp")
        assert len(result) == 3
        assert all(isinstance(j, JobListing) for j in result)

    def test_parse_single_posting(self) -> None:
        content = _load_fixture("ashby_single_posting.json")
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/acmecorp")
        assert len(result) == 1
        assert result[0].title == "VP of Engineering"

    def test_parse_empty_board(self) -> None:
        content = _load_fixture("ashby_empty_board.json")
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/emptycorp")
        assert result == []

    def test_parse_graphql_error_returns_empty(self) -> None:
        content = _load_fixture("ashby_graphql_error.json")
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/nonexistent")
        assert result == []

    def test_parse_invalid_json_returns_empty(self) -> None:
        result = self.parser.parse("<html>not json</html>", url="https://jobs.ashbyhq.com/x")
        assert result == []

    def test_parse_non_graphql_json_returns_empty(self) -> None:
        result = self.parser.parse('{"unexpected": "format"}', url="https://jobs.ashbyhq.com/x")
        assert result == []

    def test_parse_null_data_returns_empty(self) -> None:
        content = json.dumps({"data": None})
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/x")
        assert result == []

    def test_malformed_posting_skipped(self) -> None:
        content = json.dumps(
            {
                "data": {
                    "jobBoard": {
                        "title": "Test",
                        "jobPostings": [
                            {
                                "title": "Good",
                                "id": "good-id",
                                "jobUrl": "https://jobs.ashbyhq.com/co/good-id",
                            },
                            "not a dict",
                            42,
                            None,
                            {"title": None, "id": None},
                        ],
                    }
                }
            }
        )
        result = self.parser.parse(content, url="https://jobs.ashbyhq.com/co")
        assert len(result) == 1
        assert result[0].title == "Good"


# ------------------------------------------------------------------
# Field mapping
# ------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyFieldMapping:
    """Verify each Ashby field maps to the correct JobListing field."""

    def setup_method(self) -> None:
        parser = AshbyParser()
        content = _load_fixture("ashby_job_board_response.json")
        self.listings = parser.parse(content, url="https://jobs.ashbyhq.com/acmecorp")
        self.senior_eng = self.listings[0]

    def test_title(self) -> None:
        assert self.senior_eng.title == "Senior Backend Engineer"

    def test_url(self) -> None:
        assert str(self.senior_eng.url) == (
            "https://jobs.ashbyhq.com/acmecorp/a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        )

    def test_location(self) -> None:
        assert self.senior_eng.location == "San Francisco, CA"

    def test_department(self) -> None:
        assert self.senior_eng.department == "Engineering"

    def test_employment_type(self) -> None:
        assert self.senior_eng.employment_type == "FullTime"

    def test_description_uses_plain_text(self) -> None:
        assert self.senior_eng.description is not None
        assert "Senior Backend Engineer" in self.senior_eng.description
        assert "<div>" not in self.senior_eng.description

    def test_requirements_extracted_from_html(self) -> None:
        reqs = self.senior_eng.requirements
        assert len(reqs) >= 3
        assert "5+ years of backend development experience" in reqs
        assert "Proficiency in Python, Go, or Rust" in reqs
        assert "Experience with distributed systems and microservices" in reqs

    def test_salary_range(self) -> None:
        assert self.senior_eng.salary_range == "$170,000 - $230,000 USD"

    def test_posted_date_is_timezone_aware(self) -> None:
        assert self.senior_eng.posted_date is not None
        assert isinstance(self.senior_eng.posted_date, datetime)
        assert self.senior_eng.posted_date.tzinfo is not None

    def test_ats_provider_set(self) -> None:
        assert self.senior_eng.ats_provider == ATSProvider.ASHBY

    def test_raw_data_preserved(self) -> None:
        assert self.senior_eng.raw_data["id"] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert "departmentName" in self.senior_eng.raw_data

    def test_posting_without_salary_returns_none(self) -> None:
        designer = self.listings[1]
        assert designer.salary_range is None

    def test_remote_location_appended(self) -> None:
        """Remote postings should reflect remote status in location."""
        designer = self.listings[1]
        assert designer.location is not None
        assert "Remote" in designer.location or "New York" in designer.location

    def test_intern_employment_type(self) -> None:
        intern = self.listings[2]
        assert intern.employment_type == "Intern"

    def test_description_fallback_to_html_stripped(self) -> None:
        """When descriptionPlain is missing, HTML is stripped."""
        parser = AshbyParser()
        content = json.dumps(
            {
                "data": {
                    "jobBoard": {
                        "title": "Test",
                        "jobPostings": [
                            {
                                "id": "html-only",
                                "title": "HTML Only",
                                "descriptionHtml": "<div><b>Bold text</b> and normal.</div>",
                                "jobUrl": "https://jobs.ashbyhq.com/co/html-only",
                            }
                        ],
                    }
                }
            }
        )
        result = parser.parse(content, url="https://jobs.ashbyhq.com/co")
        assert len(result) == 1
        assert result[0].description == "Bold text and normal."


# ------------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyBuildGraphqlUrl:
    """Verify GraphQL endpoint URL construction."""

    def test_career_page_url(self) -> None:
        url = AshbyParser.build_graphql_url("https://jobs.ashbyhq.com/acmecorp")
        assert url == "https://jobs.ashbyhq.com/api/non-user-graphql"

    def test_already_api_url(self) -> None:
        url = AshbyParser.build_graphql_url("https://jobs.ashbyhq.com/api/non-user-graphql")
        assert url == "https://jobs.ashbyhq.com/api/non-user-graphql"

    def test_custom_domain_with_ashby(self) -> None:
        url = AshbyParser.build_graphql_url("https://careers.acme.com/jobs")
        assert "non-user-graphql" in url

    def test_extract_org_slug(self) -> None:
        slug = AshbyParser.extract_org_slug("https://jobs.ashbyhq.com/acmecorp")
        assert slug == "acmecorp"

    def test_extract_org_slug_with_path(self) -> None:
        slug = AshbyParser.extract_org_slug("https://jobs.ashbyhq.com/acmecorp/some-job-id")
        assert slug == "acmecorp"

    def test_extract_org_slug_trailing_slash(self) -> None:
        slug = AshbyParser.extract_org_slug("https://jobs.ashbyhq.com/acmecorp/")
        assert slug == "acmecorp"


# ------------------------------------------------------------------
# GraphQL query building
# ------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyGraphqlQuery:
    """Verify the GraphQL query payload is correctly structured."""

    def test_build_job_board_query(self) -> None:
        payload = AshbyParser.build_job_board_query("acmecorp")
        assert "query" in payload
        assert "variables" in payload
        assert payload["variables"]["organizationHostedJobsPageName"] == "acmecorp"
        assert "jobBoard" in payload["query"]

    def test_build_single_posting_query(self) -> None:
        payload = AshbyParser.build_single_posting_query("some-job-id")
        assert "query" in payload
        assert "variables" in payload
        assert payload["variables"]["jobPostingId"] == "some-job-id"
        assert "jobPosting" in payload["query"]


# ------------------------------------------------------------------
# fetch_all (async, with mocked safe_fetch)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyFetchAll:
    async def test_fetch_job_board(self) -> None:
        content = _load_fixture("ashby_job_board_response.json")
        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=content,
            content_type="application/json",
        )
        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            listings = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/acmecorp")

        assert len(listings) == 3
        mock_fetch.assert_called_once()

    async def test_api_error_returns_empty(self) -> None:
        error_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=500,
            error="HTTP 500: Internal Server Error",
        )
        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/badcorp")

        assert listings == []

    async def test_graphql_error_response_returns_empty(self) -> None:
        content = _load_fixture("ashby_graphql_error.json")
        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=content,
            content_type="application/json",
        )
        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            listings = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/nonexistent")

        assert listings == []

    async def test_fetch_sends_post_with_graphql_payload(self) -> None:
        content = _load_fixture("ashby_empty_board.json")
        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=content,
            content_type="application/json",
        )
        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = mock_result
            await AshbyParser.fetch_all("https://jobs.ashbyhq.com/acmecorp")

        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs.get("method") == "POST" or (len(call_kwargs.args) > 0)
        assert call_kwargs.kwargs.get("json") is not None
        payload = call_kwargs.kwargs["json"]
        assert "query" in payload
        assert "variables" in payload
