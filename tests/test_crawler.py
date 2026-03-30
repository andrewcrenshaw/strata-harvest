"""Tests for the Crawler public API surface (PCC-1426).

Covers:
- Crawler class: scrape(), scrape_batch()
- create_crawler() factory with all configuration options
- harvest() convenience function
- Rate limiter integration
- Change detection via content hash
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.crawler import Crawler, create_crawler, harvest
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, JobListing, ScrapeResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GREENHOUSE_API_RESPONSE = json.dumps(
    {
        "jobs": [
            {
                "id": 1001,
                "title": "Software Engineer",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/1001",
                "location": {"name": "San Francisco, CA"},
                "departments": [{"name": "Engineering"}],
                "content": "<p>Build things.</p><ul><li>Python</li><li>SQL</li></ul>",
                "updated_at": "2026-01-15T10:00:00Z",
            },
            {
                "id": 1002,
                "title": "Product Manager",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/1002",
                "location": {"name": "Remote"},
                "departments": [{"name": "Product"}],
                "content": "<p>Ship products.</p>",
                "updated_at": "2026-02-01T12:00:00Z",
            },
        ]
    }
)


def _ok_fetch(url: str, content: str = GREENHOUSE_API_RESPONSE) -> FetchResult:
    return FetchResult(
        url=url,
        status_code=200,
        content=content,
        content_type="application/json",
        elapsed_ms=42.0,
    )


def _error_fetch(url: str) -> FetchResult:
    return FetchResult(
        url=url,
        status_code=500,
        error="HTTP 500: Internal Server Error",
        elapsed_ms=10.0,
    )


# ---------------------------------------------------------------------------
# create_crawler() factory
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCreateCrawler:
    def test_returns_crawler_instance(self) -> None:
        c = create_crawler()
        assert isinstance(c, Crawler)

    def test_default_rate_limit_is_two_seconds(self) -> None:
        """AC: Rate limiter default 2.0s."""
        c = create_crawler()
        assert c._rate_limiter._interval == 2.0

    def test_custom_rate_limit(self) -> None:
        c = create_crawler(rate_limit=0.5)
        assert c._rate_limiter._interval == pytest.approx(1.0 / 0.5)

    def test_custom_timeout(self) -> None:
        c = create_crawler(timeout=60.0)
        assert c._timeout == 60.0

    def test_custom_user_agent(self) -> None:
        c = create_crawler(user_agent="test-bot/1.0")
        assert c._user_agent == "test-bot/1.0"

    def test_headless_mode_stored(self) -> None:
        """AC: configurable headless mode."""
        c = create_crawler(headless=True)
        assert c._headless is True

    def test_proxy_stored(self) -> None:
        """AC: configurable proxy."""
        c = create_crawler(proxy="http://proxy.example.com:8080")
        assert c._proxy == "http://proxy.example.com:8080"

    def test_llm_provider_stored(self) -> None:
        """AC: configurable llm_provider."""
        c = create_crawler(llm_provider="openai/gpt-4o-mini")
        assert c._llm_provider == "openai/gpt-4o-mini"

    def test_defaults_headless_false(self) -> None:
        c = create_crawler()
        assert c._headless is False

    def test_defaults_proxy_none(self) -> None:
        c = create_crawler()
        assert c._proxy is None

    def test_defaults_llm_provider_none(self) -> None:
        c = create_crawler()
        assert c._llm_provider is None


# ---------------------------------------------------------------------------
# Crawler.scrape()
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerScrape:
    async def test_scrape_returns_scrape_result(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
                detection_method="url_pattern",
            )
            mock_fetch.return_value = _ok_fetch(url)

            c = create_crawler()
            result = await c.scrape(url)

        assert isinstance(result, ScrapeResult)
        assert result.url == url
        assert len(result.jobs) == 2
        assert result.content_hash is not None
        assert result.error is None

    async def test_scrape_change_detection_unchanged(self) -> None:
        """AC: returns ScrapeResult with change detection (content hash comparison)."""
        url = "https://boards.greenhouse.io/acme/jobs"
        content = GREENHOUSE_API_RESPONSE

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
            )
            mock_fetch.return_value = _ok_fetch(url, content)

            c = create_crawler()
            first = await c.scrape(url)
            result = await c.scrape(url, previous_hash=first.content_hash)

        assert result.changed is False
        assert result.content_hash == first.content_hash

    async def test_scrape_change_detection_changed(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
            )
            mock_fetch.return_value = _ok_fetch(url)

            c = create_crawler()
            result = await c.scrape(url, previous_hash="stale-hash-abc123")

        assert result.changed is True

    async def test_scrape_first_scrape_always_changed(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE)
            mock_fetch.return_value = _ok_fetch(url)

            c = create_crawler()
            result = await c.scrape(url)

        assert result.changed is True

    async def test_scrape_fetch_error_returns_error_result(self) -> None:
        url = "https://down.example.com/careers"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo()
            mock_fetch.return_value = _error_fetch(url)

            c = create_crawler()
            result = await c.scrape(url)

        assert result.ok is False
        assert result.error is not None
        assert result.jobs == []

    async def test_scrape_records_duration(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE)
            mock_fetch.return_value = _ok_fetch(url)

            c = create_crawler()
            result = await c.scrape(url)

        assert result.scrape_duration_ms >= 0

    async def test_scrape_uses_rate_limiter(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE)
            mock_fetch.return_value = _ok_fetch(url)

            c = create_crawler()
            with patch.object(c._rate_limiter, "acquire", new_callable=AsyncMock) as mock_acq:
                await c.scrape(url)
                mock_acq.assert_called_once()


# ---------------------------------------------------------------------------
# Crawler.scrape_batch()
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerScrapeBatch:
    async def test_scrape_batch_yields_results(self) -> None:
        """AC: scrape_batch(urls, concurrency) async generator."""
        urls = [
            "https://boards.greenhouse.io/acme/jobs",
            "https://jobs.lever.co/beta",
        ]

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE)
            mock_fetch.return_value = _ok_fetch(urls[0])

            c = create_crawler()
            results = []
            async for result in c.scrape_batch(urls):
                results.append(result)

        assert len(results) == 2
        assert all(isinstance(r, ScrapeResult) for r in results)

    async def test_scrape_batch_respects_concurrency(self) -> None:
        """Concurrency parameter limits parallel requests."""
        urls = [f"https://example.com/page/{i}" for i in range(5)]
        call_count = 0
        max_concurrent = 0
        current_concurrent = 0

        async def tracked_scrape(self: Crawler, url: str, **kwargs: object) -> ScrapeResult:
            nonlocal call_count, max_concurrent, current_concurrent
            current_concurrent += 1
            max_concurrent = max(max_concurrent, current_concurrent)
            call_count += 1
            await asyncio.sleep(0.01)
            current_concurrent -= 1
            return ScrapeResult(url=url, content_hash="abc")

        with patch.object(Crawler, "scrape", tracked_scrape):
            c = create_crawler()
            results = []
            async for result in c.scrape_batch(urls, concurrency=2):
                results.append(result)

        assert len(results) == 5
        assert max_concurrent <= 2

    async def test_scrape_batch_default_concurrency(self) -> None:
        """Default concurrency should be reasonable (e.g. 5)."""
        urls = ["https://example.com/page/1"]

        async def mock_scrape(self: Crawler, url: str, **kwargs: object) -> ScrapeResult:
            return ScrapeResult(url=url)

        with patch.object(Crawler, "scrape", mock_scrape):
            c = create_crawler()
            results = []
            async for result in c.scrape_batch(urls):
                results.append(result)

        assert len(results) == 1

    async def test_scrape_batch_error_doesnt_stop_iteration(self) -> None:
        """Individual scrape errors should yield error results, not stop the generator."""
        urls = [
            "https://ok.example.com",
            "https://fail.example.com",
            "https://ok2.example.com",
        ]

        _fake_job = JobListing(title="Test", url="https://example.com/job/1")

        async def mixed_scrape(self: Crawler, url: str, **kwargs: object) -> ScrapeResult:
            if "fail" in url:
                return ScrapeResult(url=url, error="Fetch failed")
            return ScrapeResult(
                url=url,
                jobs=[_fake_job],
                content_hash="abc",
            )

        with patch.object(Crawler, "scrape", mixed_scrape):
            c = create_crawler()
            results = []
            async for result in c.scrape_batch(urls):
                results.append(result)

        assert len(results) == 3
        assert any(not r.ok for r in results)

    async def test_scrape_batch_empty_urls(self) -> None:
        c = create_crawler()
        results = []
        async for result in c.scrape_batch([]):
            results.append(result)
        assert results == []


# ---------------------------------------------------------------------------
# harvest() convenience function
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestHarvest:
    async def test_harvest_returns_job_listings(self) -> None:
        """AC: harvest(url) convenience function returns structured jobs."""
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
            )
            mock_fetch.return_value = _ok_fetch(url)

            jobs = await harvest(url)

        assert isinstance(jobs, list)
        assert len(jobs) == 2
        assert jobs[0].title == "Software Engineer"
        assert jobs[1].title == "Product Manager"

    async def test_harvest_auto_detects_ats(self) -> None:
        """AC: Auto-detects ATS and uses appropriate parser."""
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
            )
            mock_fetch.return_value = _ok_fetch(url)

            await harvest(url)

        mock_detect.assert_called_once()

    async def test_harvest_empty_on_error(self) -> None:
        url = "https://down.example.com/careers"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo()
            mock_fetch.return_value = _error_fetch(url)

            jobs = await harvest(url)

        assert jobs == []

    async def test_harvest_custom_timeout(self) -> None:
        url = "https://boards.greenhouse.io/acme/jobs"

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE)
            mock_fetch.return_value = _ok_fetch(url)

            await harvest(url, timeout=60.0)

        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs.get("timeout") == 60.0


# ---------------------------------------------------------------------------
# __init__.py exports
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestExports:
    def test_all_public_api_exported(self) -> None:
        """AC: Documented public API lives on strata_harvest root."""
        import strata_harvest

        assert hasattr(strata_harvest, "harvest")
        assert hasattr(strata_harvest, "create_crawler")
        assert hasattr(strata_harvest, "ScrapeResult")
        assert hasattr(strata_harvest, "JobListing")
        assert hasattr(strata_harvest, "ATSInfo")
        assert not hasattr(strata_harvest, "Crawler")
        assert not hasattr(strata_harvest, "ATSProvider")
        assert not hasattr(strata_harvest, "FetchResult")
        assert not hasattr(strata_harvest, "detect_ats")

    def test_harvest_callable(self) -> None:
        import strata_harvest

        assert callable(strata_harvest.harvest)

    def test_create_crawler_callable(self) -> None:
        import strata_harvest

        assert callable(strata_harvest.create_crawler)
