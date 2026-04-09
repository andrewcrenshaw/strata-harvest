"""Tests for Ashby ATS tenant slug resolver (PCC-1736 / ENH-04).

Covers:
- extract_slug_from_html() — regex extraction of org slug from career page HTML
- fetch_all() with html kwarg — slug resolution + GraphQL query
- Full crawler pipeline for custom-domain Ashby pages (DOM-detected)
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.crawler import create_crawler, harvest
from strata_harvest.models import ATSProvider, FetchResult, JobListing
from strata_harvest.parsers.ashby import AshbyParser
from tests.robots_helpers import patch_all_safe_fetch

CAREER_PAGES_DIR = Path(__file__).parent / "fixtures" / "career_pages"
ASHBY_API_DIR = Path(__file__).parent / "fixtures" / "ashby"


def _load_career_page(name: str) -> str:
    return (CAREER_PAGES_DIR / name).read_text(encoding="utf-8")


def _load_api_fixture(name: str) -> str:
    return (ASHBY_API_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Unit: extract_slug_from_html
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyExtractSlugFromHtml:
    """extract_slug_from_html() pulls the org slug from career page HTML."""

    def test_extracts_from_organization_hosted_jobs_page_name(self) -> None:
        """Primary pattern: JSON blob with organizationHostedJobsPageName."""
        html = _load_career_page("ashby_granola_style.html")
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug == "granola"

    def test_extracts_from_job_board_url_pattern(self) -> None:
        """Fallback pattern: ashbyhq.com/job-board/<slug> link in HTML."""
        html = _load_career_page("ashby_notion_style.html")
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug == "notion"

    def test_inline_json_blob_extraction(self) -> None:
        """Handles organizationHostedJobsPageName with varied whitespace."""
        html = '<script>{"organizationHostedJobsPageName"  :  "retool","other": "data"}</script>'
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug == "retool"

    def test_returns_none_when_no_slug_found(self) -> None:
        """Returns None for HTML with no Ashby slug markers."""
        html = "<html><body><p>No ATS here</p></body></html>"
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug is None

    def test_returns_none_for_empty_string(self) -> None:
        slug = AshbyParser.extract_slug_from_html("")
        assert slug is None

    def test_primary_pattern_takes_precedence_over_fallback(self) -> None:
        """When both patterns match, the JSON blob pattern wins."""
        html = (
            '{"organizationHostedJobsPageName": "loom"} '
            '<a href="https://app.ashbyhq.com/job-board/different">jobs</a>'
        )
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug == "loom"

    def test_job_board_url_with_various_hosts(self) -> None:
        """Matches ashbyhq.com/job-board/<slug> on any subdomain."""
        html = '<script src="https://jobs.ashbyhq.com/job-board/mycompany/embed.js"></script>'
        slug = AshbyParser.extract_slug_from_html(html)
        assert slug == "mycompany"


# ---------------------------------------------------------------------------
# Unit: fetch_all with html kwarg (slug from HTML, not URL)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestAshbyFetchAllWithHtml:
    """fetch_all() uses html kwarg to resolve slug when URL has none."""

    async def test_fetch_all_uses_slug_from_html_when_url_has_no_slug(self) -> None:
        """Custom-domain URL + HTML containing slug → correct GraphQL call."""
        html = _load_career_page("ashby_granola_style.html")
        api_content = _load_api_fixture("ashby_job_board_response.json")

        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=api_content,
            content_type="application/json",
        )

        captured_kwargs: dict = {}

        async def _capture_fetch(url: str, **kwargs: object) -> FetchResult:
            captured_kwargs["url"] = url
            captured_kwargs["json"] = kwargs.get("json")
            return mock_result

        with patch("strata_harvest.parsers.ashby.safe_fetch", side_effect=_capture_fetch):
            listings = await AshbyParser.fetch_all(
                "https://careers.granola.so/",
                html=html,
            )

        assert len(listings) == 3
        assert captured_kwargs["json"]["variables"]["organizationHostedJobsPageName"] == "granola"

    async def test_fetch_all_html_slug_takes_precedence_over_url_slug(self) -> None:
        """When html is provided, its slug wins over the URL path slug."""
        html = _load_career_page("ashby_granola_style.html")
        api_content = _load_api_fixture("ashby_empty_board.json")

        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=api_content,
            content_type="application/json",
        )

        captured_kwargs: dict = {}

        async def _capture_fetch(url: str, **kwargs: object) -> FetchResult:
            captured_kwargs["json"] = kwargs.get("json")
            return mock_result

        with patch("strata_harvest.parsers.ashby.safe_fetch", side_effect=_capture_fetch):
            await AshbyParser.fetch_all(
                "https://careers.example.com/",
                html=html,
            )

        # Should use slug from html ("granola"), not from URL
        assert captured_kwargs["json"]["variables"]["organizationHostedJobsPageName"] == "granola"

    async def test_fetch_all_warns_and_returns_empty_when_no_slug_in_html(self) -> None:
        """When html has no slug markers, returns [] and logs a warning."""
        html = "<html><body>No Ashby here</body></html>"

        with patch("strata_harvest.parsers.ashby.safe_fetch") as mock_fetch:
            listings = await AshbyParser.fetch_all(
                "https://careers.example.com/",
                html=html,
            )

        # Should not even attempt a fetch — no slug to query with
        mock_fetch.assert_not_called()
        assert listings == []

    async def test_fetch_all_without_html_still_works_for_ashbyhq_urls(self) -> None:
        """Existing behavior: jobs.ashbyhq.com/* URLs work without html kwarg."""
        api_content = _load_api_fixture("ashby_job_board_response.json")
        mock_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            status_code=200,
            content=api_content,
            content_type="application/json",
        )

        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            listings = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/acmecorp")

        assert len(listings) == 3


# ---------------------------------------------------------------------------
# Integration: full crawler pipeline for custom-domain Ashby (DOM-detected)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAshbyCustomDomainEndToEnd:
    """Full pipeline: custom Ashby domain → DOM detect → slug extract → GraphQL → jobs."""

    def _make_fetch_side_effect(
        self,
        career_page_html: str,
        api_fixture_name: str = "ashby_job_board_response.json",
    ):
        """Build a side_effect fn: HTML for career page, JSON for GraphQL API calls."""
        api_content = _load_api_fixture(api_fixture_name)

        async def _fetch(url: str, **kwargs: object) -> FetchResult:
            if "non-user-graphql" in url or "graphql" in url:
                return FetchResult(
                    url=url,
                    status_code=200,
                    content=api_content,
                    content_type="application/json",
                    elapsed_ms=30.0,
                )
            if "robots.txt" in url:
                return FetchResult(
                    url=url,
                    status_code=200,
                    content="User-agent: *\nDisallow:\n",
                    content_type="text/plain",
                    elapsed_ms=1.0,
                )
            return FetchResult(
                url=url,
                status_code=200,
                content=career_page_html,
                content_type="text/html",
                elapsed_ms=20.0,
            )

        return AsyncMock(side_effect=_fetch)

    async def test_granola_style_career_page_returns_jobs(self) -> None:
        """Granola AI-style page (organizationHostedJobsPageName) → >0 jobs."""
        html = _load_career_page("ashby_granola_style.html")
        mock = self._make_fetch_side_effect(html)

        with patch_all_safe_fetch(mock), patch("strata_harvest.parsers.ashby.safe_fetch", mock):
            jobs = await harvest("https://careers.granola.so/")

        assert len(jobs) > 0
        assert all(isinstance(j, JobListing) for j in jobs)
        assert all(j.ats_provider == ATSProvider.ASHBY for j in jobs)

    async def test_notion_style_career_page_returns_jobs(self) -> None:
        """Notion-style page (ashbyhq.com/job-board/<slug>) → >0 jobs."""
        html = _load_career_page("ashby_notion_style.html")
        mock = self._make_fetch_side_effect(html)

        with patch_all_safe_fetch(mock), patch("strata_harvest.parsers.ashby.safe_fetch", mock):
            jobs = await harvest("https://www.notion.so/careers")

        assert len(jobs) > 0
        assert all(j.ats_provider == ATSProvider.ASHBY for j in jobs)

    async def test_custom_domain_no_slug_returns_empty_no_error(self) -> None:
        """Custom domain with no extractable slug returns [] without crashing."""
        html = "<html><body><div class='ashby-job-posting'>loading...</div></body></html>"
        mock = self._make_fetch_side_effect(html)

        with patch_all_safe_fetch(mock), patch("strata_harvest.parsers.ashby.safe_fetch", mock):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape("https://careers.example.com/")

        # Should detect Ashby from DOM but return empty jobs (no slug found)
        assert result.ats_info.provider == ATSProvider.ASHBY
        assert result.jobs == []
        assert result.error is None  # Not a hard error, just empty

    async def test_scrape_result_metadata_correct_for_custom_domain(self) -> None:
        """ScrapeResult has correct provider and fetch_ok for custom-domain Ashby."""
        html = _load_career_page("ashby_granola_style.html")
        mock = self._make_fetch_side_effect(html)

        with patch_all_safe_fetch(mock), patch("strata_harvest.parsers.ashby.safe_fetch", mock):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape("https://careers.granola.so/")

        assert result.ats_info.provider == ATSProvider.ASHBY
        assert result.fetch_ok is True
        assert result.error is None


# ---------------------------------------------------------------------------
# Regression: non-Ashby providers unaffected
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestNonAshbyProvidersUnaffected:
    """Slug resolver changes must not regress Greenhouse, Lever, Workday, iCIMS."""

    def test_greenhouse_parser_still_works(self) -> None:
        from strata_harvest.parsers.greenhouse import GreenhouseParser

        content = json.dumps(
            {
                "meta": {"total": 1},
                "jobs": [
                    {
                        "id": 1,
                        "title": "Engineer",
                        "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
                        "location": {"name": "Remote"},
                        "departments": [],
                        "content": "",
                        "updated_at": "2026-01-01T00:00:00Z",
                    }
                ],
            }
        )
        parser = GreenhouseParser()
        result = parser.parse(content, url="https://boards.greenhouse.io/co/jobs")
        assert len(result) == 1
        assert result[0].title == "Engineer"

    def test_lever_parser_still_works(self) -> None:
        from strata_harvest.parsers.lever import LeverParser

        content = json.dumps(
            [
                {
                    "id": "abc",
                    "text": "PM",
                    "hostedUrl": "https://jobs.lever.co/co/abc",
                    "categories": {"location": "NYC", "department": "Prod", "commitment": "FT"},
                    "descriptionPlain": "Drive roadmap.",
                    "createdAt": 1743465600000,
                }
            ]
        )
        parser = LeverParser()
        result = parser.parse(content, url="https://jobs.lever.co/co")
        assert len(result) == 1
        assert result[0].title == "PM"

    def test_ashby_parser_org_slug_extraction_unchanged(self) -> None:
        """Existing extract_org_slug still works for jobs.ashbyhq.com URLs."""
        assert AshbyParser.extract_org_slug("https://jobs.ashbyhq.com/acmecorp") == "acmecorp"
        assert (
            AshbyParser.extract_org_slug("https://jobs.ashbyhq.com/acmecorp/job-id") == "acmecorp"
        )
