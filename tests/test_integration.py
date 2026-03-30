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

# ---------------------------------------------------------------------------
# Greenhouse fixtures
# ---------------------------------------------------------------------------

GREENHOUSE_FIXTURE = json.dumps({
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
})

# ---------------------------------------------------------------------------
# Lever fixtures
# ---------------------------------------------------------------------------

LEVER_FIXTURE = json.dumps([
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
])

# ---------------------------------------------------------------------------
# Ashby fixtures
# ---------------------------------------------------------------------------

ASHBY_FIXTURE = json.dumps({
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
})


def _greenhouse_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url, status_code=200, content=GREENHOUSE_FIXTURE,
        content_type="application/json", elapsed_ms=25.0,
    )


def _lever_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url, status_code=200, content=LEVER_FIXTURE,
        content_type="application/json", elapsed_ms=30.0,
    )


def _ashby_fetch(url: str, **kwargs: object) -> FetchResult:
    return FetchResult(
        url=url, status_code=200, content=ASHBY_FIXTURE,
        content_type="application/json", elapsed_ms=35.0,
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
