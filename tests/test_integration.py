"""Integration tests: full harvest pipeline against fixtures (PCC-1426, PCC-1428).

Uses patched HTTP to serve fixture content so the full pipeline runs
without network access: detect ATS → select parser → extract → return ScrapeResult.

AC (PCC-1428): Integration tests: full harvest pipeline against fixtures
(detect ATS → select parser → extract → return ScrapeResult) for
Greenhouse, Lever, and Ashby providers.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.crawler import create_crawler, harvest
from strata_harvest.models import ATSProvider, FetchResult, JobListing, ScrapeResult
from tests.robots_helpers import make_fetch_with_robots, patch_all_safe_fetch

# ---------------------------------------------------------------------------
# Greenhouse fixtures
# ---------------------------------------------------------------------------

GREENHOUSE_FIXTURE = json.dumps(
    {
        "meta": {"total": 3},
        "jobs": [
            {
                "id": 2001,
                "title": "Backend Engineer",
                "absolute_url": "https://boards.greenhouse.io/testco/jobs/2001",
                "location": {"name": "New York, NY"},
                "departments": [{"name": "Platform"}],
                "content": "<p>Build APIs.</p><ul><li>Go or Python</li><li>PostgreSQL</li></ul>",
                "updated_at": "2026-03-01T09:00:00Z",
            },
            {
                "id": 2002,
                "title": "Frontend Engineer",
                "absolute_url": "https://boards.greenhouse.io/testco/jobs/2002",
                "location": {"name": "Remote"},
                "departments": [{"name": "Platform"}],
                "content": "<p>Build UIs.</p><ul><li>React</li><li>TypeScript</li></ul>",
                "updated_at": "2026-03-10T14:00:00Z",
            },
            {
                "id": 2003,
                "title": "Data Scientist",
                "absolute_url": "https://boards.greenhouse.io/testco/jobs/2003",
                "location": {"name": "San Francisco, CA"},
                "departments": [{"name": "Data"}],
                "content": "<p>Analyze data.</p><ul><li>Python</li><li>ML</li></ul>",
                "updated_at": "2026-03-15T11:00:00Z",
            },
        ],
    }
)

# ---------------------------------------------------------------------------
# Lever fixtures
# ---------------------------------------------------------------------------

LEVER_FIXTURE = json.dumps(
    [
        {
            "id": "lever-001",
            "text": "Software Engineer",
            "hostedUrl": "https://jobs.lever.co/testco/lever-001",
            "categories": {
                "location": "Austin, TX",
                "department": "Engineering",
                "commitment": "Full-time",
            },
            "descriptionPlain": "Build distributed systems.",
            "lists": [
                {"text": "Requirements", "content": "<li>Go or Rust</li><li>3+ years</li>"},
            ],
            "createdAt": 1743465600000,
        },
        {
            "id": "lever-002",
            "text": "Product Manager",
            "hostedUrl": "https://jobs.lever.co/testco/lever-002",
            "categories": {
                "location": "Remote",
                "department": "Product",
                "commitment": "Full-time",
            },
            "descriptionPlain": "Drive product strategy.",
            "createdAt": 1743552000000,
        },
    ]
)

# ---------------------------------------------------------------------------
# Ashby fixtures
# ---------------------------------------------------------------------------

ASHBY_FIXTURE = json.dumps(
    {
        "data": {
            "jobBoard": {
                "title": "TestCorp Careers",
                "jobPostings": [
                    {
                        "id": "ashby-001",
                        "title": "Staff Engineer",
                        "jobUrl": "https://jobs.ashbyhq.com/testcorp/ashby-001",
                        "locationName": "San Francisco, CA",
                        "departmentName": "Engineering",
                        "employmentType": "FullTime",
                        "descriptionPlain": "Lead platform development.",
                        "descriptionHtml": (
                            "<p>Lead platform development.</p>"
                            "<ul><li>10+ years experience</li></ul>"
                        ),
                        "publishedDate": "2026-03-01",
                        "isRemote": False,
                    },
                    {
                        "id": "ashby-002",
                        "title": "Data Analyst",
                        "jobUrl": "https://jobs.ashbyhq.com/testcorp/ashby-002",
                        "locationName": "Remote",
                        "departmentName": "Data",
                        "employmentType": "FullTime",
                        "descriptionPlain": "Analyze business data.",
                        "descriptionHtml": "<p>Analyze business data.</p>",
                        "publishedDate": "2026-03-10",
                        "isRemote": True,
                    },
                ],
            }
        }
    }
)


def _greenhouse_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url,
        status_code=200,
        content=GREENHOUSE_FIXTURE,
        content_type="application/json",
        elapsed_ms=25.0,
    )


def _lever_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url,
        status_code=200,
        content=LEVER_FIXTURE,
        content_type="application/json",
        elapsed_ms=30.0,
    )


def _ashby_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url,
        status_code=200,
        content=ASHBY_FIXTURE,
        content_type="application/json",
        elapsed_ms=35.0,
    )


# ---------------------------------------------------------------------------
# Greenhouse integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGreenhouseEndToEnd:
    """Full pipeline: Greenhouse URL → detect ATS → fetch → parse → JobListings."""

    async def test_harvest_greenhouse_fixture(self) -> None:
        url = "https://boards.greenhouse.io/testco/jobs"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_greenhouse_fetch),
        ):
            jobs = await harvest(url)

        assert isinstance(jobs, list)
        assert len(jobs) == 3
        titles = [j.title for j in jobs]
        assert "Backend Engineer" in titles
        assert "Frontend Engineer" in titles
        assert "Data Scientist" in titles

        for job in jobs:
            assert isinstance(job, JobListing)
            assert job.ats_provider == ATSProvider.GREENHOUSE
            assert job.url is not None

    async def test_harvest_returns_structured_fields(self) -> None:
        url = "https://boards.greenhouse.io/testco/jobs"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_greenhouse_fetch),
        ):
            jobs = await harvest(url)

        backend = next(j for j in jobs if j.title == "Backend Engineer")
        assert backend.location == "New York, NY"
        assert backend.department == "Platform"
        assert len(backend.requirements) >= 1


# ---------------------------------------------------------------------------
# Lever integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLeverEndToEnd:
    """Full pipeline: Lever URL → detect ATS → fetch → parse → JobListings."""

    async def test_harvest_lever_fixture(self) -> None:
        url = "https://jobs.lever.co/testco"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_lever_fetch),
        ):
            jobs = await harvest(url)

        assert isinstance(jobs, list)
        assert len(jobs) == 2
        titles = [j.title for j in jobs]
        assert "Software Engineer" in titles
        assert "Product Manager" in titles

        for job in jobs:
            assert isinstance(job, JobListing)
            assert job.ats_provider == ATSProvider.LEVER
            assert job.url is not None

    async def test_lever_structured_fields(self) -> None:
        url = "https://jobs.lever.co/testco"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_lever_fetch),
        ):
            jobs = await harvest(url)

        swe = next(j for j in jobs if j.title == "Software Engineer")
        assert swe.location == "Austin, TX"
        assert swe.department == "Engineering"
        assert swe.employment_type == "Full-time"
        assert len(swe.requirements) >= 1

    async def test_lever_scrape_result(self) -> None:
        url = "https://jobs.lever.co/testco"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_lever_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert isinstance(result, ScrapeResult)
        assert result.ok is True
        assert result.ats_info.provider == ATSProvider.LEVER
        assert result.content_hash is not None
        assert len(result.jobs) == 2


# ---------------------------------------------------------------------------
# Ashby integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAshbyEndToEnd:
    """Full pipeline: Ashby URL → detect ATS → fetch → parse → JobListings."""

    async def test_harvest_ashby_fixture(self) -> None:
        url = "https://jobs.ashbyhq.com/testcorp"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_ashby_fetch),
        ):
            jobs = await harvest(url)

        assert isinstance(jobs, list)
        assert len(jobs) == 2
        titles = [j.title for j in jobs]
        assert "Staff Engineer" in titles
        assert "Data Analyst" in titles

        for job in jobs:
            assert isinstance(job, JobListing)
            assert job.ats_provider == ATSProvider.ASHBY
            assert job.url is not None

    async def test_ashby_structured_fields(self) -> None:
        url = "https://jobs.ashbyhq.com/testcorp"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_ashby_fetch),
        ):
            jobs = await harvest(url)

        staff = next(j for j in jobs if j.title == "Staff Engineer")
        assert staff.location == "San Francisco, CA"
        assert staff.department == "Engineering"
        assert staff.employment_type == "FullTime"

    async def test_ashby_scrape_result(self) -> None:
        url = "https://jobs.ashbyhq.com/testcorp"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_ashby_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert isinstance(result, ScrapeResult)
        assert result.ok is True
        assert result.ats_info.provider == ATSProvider.ASHBY
        assert result.content_hash is not None
        assert len(result.jobs) == 2


# ---------------------------------------------------------------------------
# Cross-provider batch integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCrawlerBatchIntegration:
    async def test_crawler_scrape_then_batch(self) -> None:
        """create_crawler → scrape single → scrape_batch."""
        urls = [
            "https://boards.greenhouse.io/testco/jobs",
            "https://boards.greenhouse.io/testco2/jobs",
        ]

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_greenhouse_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)

            single = await crawler.scrape(urls[0])
            assert single.ok is True
            assert len(single.jobs) == 3
            assert single.content_hash is not None

            batch_results: list[ScrapeResult] = []
            async for result in crawler.scrape_batch(urls, concurrency=2):
                batch_results.append(result)

        assert len(batch_results) == 2
        assert all(r.ok for r in batch_results)
        assert all(len(r.jobs) == 3 for r in batch_results)

    async def test_change_detection_round_trip(self) -> None:
        """Scrape twice with same fixture — second scrape reports unchanged."""
        url = "https://boards.greenhouse.io/testco/jobs"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_greenhouse_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            first = await crawler.scrape(url)
            second = await crawler.scrape(url, previous_hash=first.content_hash)

        assert first.changed is True
        assert second.changed is False
        assert first.content_hash == second.content_hash

    async def test_mixed_provider_batch(self) -> None:
        """Batch scrape across Greenhouse + Lever URLs."""
        call_count = 0

        async def _mixed_fetch(url: str, **kwargs: object) -> FetchResult:
            nonlocal call_count
            call_count += 1
            if "lever" in url:
                return _lever_fetch(url)
            return _greenhouse_fetch(url)

        urls = [
            "https://boards.greenhouse.io/testco/jobs",
            "https://jobs.lever.co/testco",
        ]
        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_mixed_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            results: list[ScrapeResult] = []
            async for result in crawler.scrape_batch(urls, concurrency=2):
                results.append(result)

        assert len(results) == 2
        assert all(r.ok for r in results)

        total_jobs = sum(len(r.jobs) for r in results)
        assert total_jobs == 5


# ---------------------------------------------------------------------------
# AC1/AC4: API-first fetch strategy — HTML entrypoint → API redirect
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAPIRedirectStrategy:
    """AC1/AC4: Crawler redirects from HTML entrypoint to provider API URL.

    When the detected ATS has a known api_url (e.g. Greenhouse boards-api),
    the crawler re-fetches the API endpoint even if the original URL is an
    HTML career page.  This prevents silent parse failures where the parser
    receives HTML instead of the expected JSON.
    """

    async def test_greenhouse_html_entrypoint_triggers_api_fetch(self) -> None:
        """AC1: Scraping boards.greenhouse.io HTML entrypoint fetches the API JSON."""
        entrypoint_url = "https://boards.greenhouse.io/testco"

        html_content = "<html><body><div id='greenhouse'>Board</div></body></html>"
        api_content = GREENHOUSE_FIXTURE

        fetch_calls: list[str] = []

        async def _tracked_fetch(url: str, **kwargs: object) -> FetchResult:
            fetch_calls.append(url)
            if "boards-api.greenhouse.io" in url:
                return FetchResult(
                    url=url,
                    status_code=200,
                    content=api_content,
                    content_type="application/json",
                    elapsed_ms=20.0,
                )
            return FetchResult(
                url=url,
                status_code=200,
                content=html_content,
                content_type="text/html",
                elapsed_ms=15.0,
            )

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_tracked_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(entrypoint_url)

        # Verify API was fetched (in addition to or instead of the entrypoint)
        api_calls = [u for u in fetch_calls if "boards-api.greenhouse.io" in u]
        assert len(api_calls) >= 1, "API URL must be fetched for Greenhouse entrypoints"
        assert result.ats_info.provider == ATSProvider.GREENHOUSE
        # Jobs should have been parsed from the JSON API response
        assert len(result.jobs) == 3
        assert result.fetch_ok is True

    async def test_lever_html_entrypoint_triggers_api_fetch(self) -> None:
        """AC1: Scraping jobs.lever.co HTML entrypoint fetches the API JSON."""
        entrypoint_url = "https://jobs.lever.co/testco"
        html_content = "<html><body><div class='lever-jobs-container'>Board</div></body></html>"

        fetch_calls: list[str] = []

        async def _tracked_fetch(url: str, **kwargs: object) -> FetchResult:
            fetch_calls.append(url)
            if "api.lever.co" in url:
                return FetchResult(
                    url=url,
                    status_code=200,
                    content=LEVER_FIXTURE,
                    content_type="application/json",
                    elapsed_ms=20.0,
                )
            return FetchResult(
                url=url,
                status_code=200,
                content=html_content,
                content_type="text/html",
                elapsed_ms=15.0,
            )

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_tracked_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(entrypoint_url)

        api_calls = [u for u in fetch_calls if "api.lever.co" in u]
        assert len(api_calls) >= 1, "Lever API URL must be fetched for entrypoints"
        assert result.ats_info.provider == ATSProvider.LEVER
        assert len(result.jobs) == 2
        assert result.fetch_ok is True

    async def test_api_fetch_failure_falls_back_to_entrypoint_content(self) -> None:
        """AC4: If API fetch fails, crawler falls back to entrypoint content (no silent error)."""
        entrypoint_url = "https://boards.greenhouse.io/testco"

        async def _tracked_fetch(url: str, **kwargs: object) -> FetchResult:
            if "boards-api.greenhouse.io" in url:
                return FetchResult(
                    url=url,
                    status_code=503,
                    error="Service Unavailable",
                    elapsed_ms=5.0,
                )
            return FetchResult(
                url=url,
                status_code=200,
                content=GREENHOUSE_FIXTURE,
                content_type="application/json",
                elapsed_ms=15.0,
            )

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_tracked_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(entrypoint_url)

        # The crawler falls back to entrypoint content — no hard error
        assert result.error is None, "Should not propagate API failure as hard error"
        assert result.fetch_ok is True
        assert result.ats_info.provider == ATSProvider.GREENHOUSE

    async def test_fetch_ok_set_on_successful_scrape(self) -> None:
        """AC3: fetch_ok=True is set on a successful scrape with parsed jobs."""
        url = "https://boards.greenhouse.io/testco/jobs"

        with patch(
            "strata_harvest.crawler.safe_fetch",
            new_callable=lambda: AsyncMock(side_effect=_greenhouse_fetch),
        ):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert result.fetch_ok is True
        assert len(result.jobs) > 0
        assert result.ok is True


# ---------------------------------------------------------------------------
# AC2: Workday + iCIMS integration
# ---------------------------------------------------------------------------

WORKDAY_PAGE_FIXTURE = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
[
  {
    "@context": "http://schema.org",
    "@type": "JobPosting",
    "title": "Senior Platform Engineer",
    "url": "https://acme.wd5.myworkdayjobs.com/careers/job/Remote/Senior-Platform-Engineer_R-999",
    "jobLocation": {"@type": "Place", "address": {"addressLocality": "Remote"}}
  },
  {
    "@context": "http://schema.org",
    "@type": "JobPosting",
    "title": "Data Engineering Lead",
    "url": "https://acme.wd5.myworkdayjobs.com/careers/job/NYC/Data-Engineering-Lead_R-888",
    "jobLocation": {
      "@type": "Place",
      "address": {"addressLocality": "New York", "addressRegion": "NY"}
    }
  }
]
</script>
</head>
<body><h1>Open Positions at Acme</h1></body>
</html>"""

ICIMS_PAGE_FIXTURE = """{
  "searchResults": [
    {
      "jobtitle": "Backend Engineer",
      "applyurl": "https://company.icims.com/jobs/2001/job",
      "joblocation": "Seattle, WA",
      "id": "2001"
    },
    {
      "jobtitle": "Security Analyst",
      "applyurl": "https://company.icims.com/jobs/2002/job",
      "joblocation": "Remote",
      "id": "2002"
    },
    {
      "jobtitle": "Marketing Manager",
      "applyurl": "https://company.icims.com/jobs/2003/job",
      "joblocation": "Chicago, IL",
      "id": "2003"
    }
  ],
  "totalCount": 3
}"""


@pytest.mark.integration
class TestWorkdayIntegration:
    """AC2: Full pipeline integration for Workday careers pages."""

    async def test_harvest_workday_json_ld_fixture(self) -> None:
        """AC2: Workday JSON-LD page parsed end-to-end."""
        url = "https://acme.wd5.myworkdayjobs.com/careers"
        page = FetchResult(
            url=url,
            status_code=200,
            content=WORKDAY_PAGE_FIXTURE,
            content_type="text/html",
            elapsed_ms=30.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            jobs = await harvest(url)

        assert len(jobs) == 2
        titles = [j.title for j in jobs]
        assert "Senior Platform Engineer" in titles
        assert "Data Engineering Lead" in titles
        for job in jobs:
            assert isinstance(job, JobListing)
            assert job.ats_provider == ATSProvider.WORKDAY

    async def test_workday_scrape_result_fetch_ok(self) -> None:
        """AC2+AC3: Workday ScrapeResult has fetch_ok=True on success."""
        url = "https://acme.wd5.myworkdayjobs.com/careers"
        page = FetchResult(
            url=url,
            status_code=200,
            content=WORKDAY_PAGE_FIXTURE,
            content_type="text/html",
            elapsed_ms=25.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert result.ats_info.provider == ATSProvider.WORKDAY
        assert result.fetch_ok is True
        assert result.error is None

    async def test_workday_empty_page_fetch_ok_no_error(self) -> None:
        """AC2+AC3: Workday page with no parseable jobs → fetch_ok=True, no error."""
        url = "https://acme.wd5.myworkdayjobs.com/careers"
        empty_html = "<html><body><p>No jobs currently.</p></body></html>"
        page = FetchResult(
            url=url,
            status_code=200,
            content=empty_html,
            content_type="text/html",
            elapsed_ms=20.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert result.error is None
        assert result.fetch_ok is True
        assert result.jobs == []
        assert result.ok is False  # ok=False because zero jobs


@pytest.mark.integration
class TestICIMSIntegration:
    """AC2: Full pipeline integration for iCIMS career pages."""

    async def test_harvest_icims_json_fixture(self) -> None:
        """AC2: iCIMS JSON search API parsed end-to-end."""
        url = "https://company.icims.com/jobs/search"
        page = FetchResult(
            url=url,
            status_code=200,
            content=ICIMS_PAGE_FIXTURE,
            content_type="application/json",
            elapsed_ms=25.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            jobs = await harvest(url)

        assert len(jobs) == 3
        titles = [j.title for j in jobs]
        assert "Backend Engineer" in titles
        assert "Security Analyst" in titles
        assert "Marketing Manager" in titles
        for job in jobs:
            assert isinstance(job, JobListing)
            assert job.ats_provider == ATSProvider.ICIMS

    async def test_icims_scrape_result_fetch_ok(self) -> None:
        """AC2+AC3: iCIMS ScrapeResult has fetch_ok=True on success."""
        url = "https://company.icims.com/jobs/search"
        page = FetchResult(
            url=url,
            status_code=200,
            content=ICIMS_PAGE_FIXTURE,
            content_type="application/json",
            elapsed_ms=20.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            crawler = create_crawler(rate_limit=100.0)
            result = await crawler.scrape(url)

        assert result.ats_info.provider == ATSProvider.ICIMS
        assert result.fetch_ok is True
        assert result.error is None
        assert len(result.jobs) == 3
