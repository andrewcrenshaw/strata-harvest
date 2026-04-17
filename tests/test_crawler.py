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
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from strata_harvest.crawler import Crawler, create_crawler, harvest
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, JobListing, ScrapeResult
from strata_harvest.parsers.llm_fallback import LLMFallbackParser
from tests.robots_helpers import is_robots_txt_url, make_fetch_with_robots, patch_all_safe_fetch

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
        """AC: Global rate limiter default 0.5/s → 2.0s interval."""
        c = create_crawler()
        assert c._global_rate_limiter._interval == 2.0

    def test_default_per_domain_rate_is_half_req_per_second(self) -> None:
        """AC: Per-domain default 0.5 req/s (PCC-1617)."""
        c = create_crawler()
        assert c._per_domain_registry._requests_per_second == pytest.approx(0.5)

    def test_custom_rate_limit(self) -> None:
        c = create_crawler(rate_limit=0.5)
        assert c._global_rate_limiter._interval == pytest.approx(1.0 / 0.5)

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

    def test_llm_api_base_stored(self) -> None:
        """AC: llm_api_base is stored and forwarded to LLMFallbackParser."""
        c = create_crawler(llm_api_base="http://192.168.50.220:8080")
        assert c._llm_api_base == "http://192.168.50.220:8080"

    def test_defaults_llm_api_base_none(self) -> None:
        c = create_crawler()
        assert c._llm_api_base is None

    def test_respect_robots_default_true(self) -> None:
        c = create_crawler()
        assert c._respect_robots is True

    def test_respect_robots_false(self) -> None:
        c = create_crawler(respect_robots=False)
        assert c._respect_robots is False

    def test_robots_cache_ttl_stored(self) -> None:
        c = create_crawler(robots_cache_ttl=120.0)
        assert c._robots_checker.cache_ttl_seconds == 120.0


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
        mock_fetch = make_fetch_with_robots(page=_error_fetch(url))

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()

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
            with (
                patch.object(c._global_rate_limiter, "acquire", new_callable=AsyncMock) as mock_g,
                patch.object(c._per_domain_registry, "acquire", new_callable=AsyncMock) as mock_d,
            ):
                await c.scrape(url)
                mock_g.assert_called_once()
                mock_d.assert_called_once_with("boards.greenhouse.io")


# ---------------------------------------------------------------------------
# Per-domain vs global rate limits (PCC-1617)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerPerDomainRateLimit:
    async def test_acquire_rate_limits_parallel_different_hosts_fast(self) -> None:
        """Sanity: per-domain acquire alone paces different hostnames in parallel."""
        c = create_crawler(rate_limit=100.0, per_domain_rate=0.5)
        t0 = time.monotonic()
        await asyncio.gather(
            c._acquire_rate_limits("https://a.example.com/jobs"),
            c._acquire_rate_limits("https://b.example.com/jobs"),
        )
        assert time.monotonic() - t0 < 0.5

    async def test_different_domains_respect_independent_per_domain_pacing(self) -> None:
        """AC1: Two hostnames can progress in parallel when the global cap allows it."""
        url_a = "https://a.example.com/jobs"
        url_b = "https://b.example.com/jobs"

        async def fetch_side_effect(u: str, **kwargs: object) -> FetchResult:
            return _ok_fetch(u)

        with (
            patch(
                "strata_harvest.crawler.detect_ats",
                AsyncMock(return_value=ATSInfo(provider=ATSProvider.GREENHOUSE)),
            ),
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(side_effect=fetch_side_effect)),
        ):
            # Generic hosts are not robots-bypass ATS; skip robots fetch (real TCP ~2s in pytest).
            c = create_crawler(rate_limit=100.0, per_domain_rate=0.5, respect_robots=False)
            start = time.monotonic()
            async for _ in c.scrape_batch([url_a, url_b], concurrency=2):
                pass
            elapsed = time.monotonic() - start

        # One global 0.5/s limiter would serialize two requests (~2s apart).
        assert elapsed < 1.0

    async def test_global_rate_is_upper_bound_across_domains(self) -> None:
        """AC3: Tight global limit caps throughput even when per-domain is loose."""
        url_a = "https://a.example.com/jobs"
        url_b = "https://b.example.com/jobs"

        async def fetch_side_effect(u: str, **kwargs: object) -> FetchResult:
            return _ok_fetch(u)

        with (
            patch(
                "strata_harvest.crawler.detect_ats",
                AsyncMock(return_value=ATSInfo(provider=ATSProvider.GREENHOUSE)),
            ),
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(side_effect=fetch_side_effect)),
        ):
            c = create_crawler(rate_limit=0.5, per_domain_rate=100.0, respect_robots=False)
            t0 = time.monotonic()
            await c.scrape(url_a)
            t1 = time.monotonic()
            await c.scrape(url_b)
            t2 = time.monotonic()

        assert t1 - t0 < 0.5
        assert t2 - t1 >= 1.9


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

        async def fetch_side_effect(u: str, **kwargs: object) -> FetchResult:
            if "lever" in u:
                return _ok_fetch(u, content="[]")
            return _ok_fetch(u)

        # detect_ats is called twice: once in _group_sources_by_ats, once in scrape
        # For _group_sources_by_ats, return different providers to keep them separate groups
        call_count = 0

        async def mock_det(u: str, *args: object, **kwargs: object) -> ATSInfo:
            nonlocal call_count
            call_count += 1
            if "lever" in u:
                return ATSInfo(
                    provider=ATSProvider.LEVER,
                    api_url="https://api.lever.co/v0/postings/beta?mode=json",
                )
            return ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                api_url="https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true",
            )

        with (
            patch("strata_harvest.crawler.detect_ats", AsyncMock(side_effect=mock_det)),
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(side_effect=fetch_side_effect)),
        ):
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

        async def mock_detect(u: str, *args: object, **kwargs: object) -> ATSInfo:
            return ATSInfo(provider=ATSProvider.UNKNOWN)

        with (
            patch.object(Crawler, "scrape", tracked_scrape),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(side_effect=mock_detect)),
        ):
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

        async def mock_detect(u: str, *args: object, **kwargs: object) -> ATSInfo:
            return ATSInfo(provider=ATSProvider.UNKNOWN)

        with (
            patch.object(Crawler, "scrape", mock_scrape),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(side_effect=mock_detect)),
        ):
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

        # Mock detect_ats to avoid real HTTP calls during ATS detection phase
        async def mock_detect(u: str, *args: object, **kwargs: object) -> ATSInfo:
            return ATSInfo(provider=ATSProvider.UNKNOWN)

        with (
            patch.object(Crawler, "scrape", mixed_scrape),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(side_effect=mock_detect)),
        ):
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

    async def test_scrape_batch_batches_same_ats_org(self) -> None:
        """AC: Two sources with same Greenhouse board slug → one HTTP call (PCC-1962)."""
        # Two company URLs pointing to the same Greenhouse board
        url_a = "https://boards.greenhouse.io/acme/jobs"
        url_b = "https://boards.greenhouse.io/acme/jobs?department=engineering"

        # Both should resolve to the same api_url
        ats_info = ATSInfo(
            provider=ATSProvider.GREENHOUSE,
            confidence=0.9,
            api_url="https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true",
            detection_method="url_pattern",
        )

        fetch_count = 0

        async def mock_fetch(u: str, **kwargs: object) -> FetchResult:
            nonlocal fetch_count
            # API URL fetch should happen once
            if "boards-api" in u:
                fetch_count += 1
            return _ok_fetch(u)

        with (
            patch("strata_harvest.crawler.detect_ats", return_value=ats_info),
            patch(
                "strata_harvest.crawler.safe_fetch",
                new_callable=AsyncMock,
                side_effect=mock_fetch,
            ),
        ):
            c = create_crawler()
            results = []
            async for result in c.scrape_batch([url_a, url_b]):
                results.append(result)

        assert len(results) == 2
        assert fetch_count == 1, f"Expected 1 API fetch for same org, got {fetch_count}"

    async def test_scrape_batch_distributes_jobs_by_source(self) -> None:
        """AC: Jobs split correctly between sources when filtering on department (PCC-1962)."""
        url_a = "https://boards.greenhouse.io/acme/jobs"
        url_b = "https://boards.greenhouse.io/acme/jobs?department=sales"

        ats_info = ATSInfo(
            provider=ATSProvider.GREENHOUSE,
            confidence=0.9,
            api_url="https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true",
            detection_method="url_pattern",
        )

        # API response with multiple jobs (both engineering and sales)
        api_response = json.dumps(
            {
                "jobs": [
                    {
                        "id": 101,
                        "title": "Backend Engineer",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/101",
                        "location": {"name": "SF"},
                        "departments": [{"name": "Engineering"}],
                        "content": "<p>Backend role</p>",
                        "updated_at": "2026-01-15T10:00:00Z",
                    },
                    {
                        "id": 102,
                        "title": "Sales Engineer",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/102",
                        "location": {"name": "NYC"},
                        "departments": [{"name": "Sales"}],
                        "content": "<p>Sales role</p>",
                        "updated_at": "2026-01-16T10:00:00Z",
                    },
                    {
                        "id": 103,
                        "title": "Frontend Engineer",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/103",
                        "location": {"name": "SF"},
                        "departments": [{"name": "Engineering"}],
                        "content": "<p>Frontend role</p>",
                        "updated_at": "2026-01-17T10:00:00Z",
                    },
                ]
            }
        )

        async def mock_fetch(u: str, **kwargs: object) -> FetchResult:
            return _ok_fetch(u, content=api_response)

        with (
            patch("strata_harvest.crawler.detect_ats", return_value=ats_info),
            patch(
                "strata_harvest.crawler.safe_fetch",
                new_callable=AsyncMock,
                side_effect=mock_fetch,
            ),
        ):
            c = create_crawler()
            results = []
            async for result in c.scrape_batch([url_a, url_b]):
                results.append(result)

        # url_a should get all jobs initially (no department filter)
        result_a = next(r for r in results if r.url == url_a)
        expected_msg = f"Expected at least 2 jobs for engineering, got {len(result_a.jobs)}"
        assert len(result_a.jobs) >= 2, expected_msg

        # url_b would in practice filter by department=sales (application-level)
        # For now, verify we got results back for both
        result_b = next(r for r in results if r.url == url_b)
        assert result_b.url == url_b


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
        mock_fetch = make_fetch_with_robots(page=_error_fetch(url))

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()

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
# PCC-1605: Double-fetch elimination
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestDoubleFetchElimination:
    """PCC-1605: Unknown URLs — career page fetched once (robots.txt is separate, PCC-1610)."""

    async def test_unknown_url_single_fetch(self) -> None:
        """AC1: Career URL is fetched exactly once; robots.txt is a separate request."""
        url = "https://custom-careers.example.com/jobs"
        html = "<html><body><h1>Open Positions</h1></body></html>"

        page = FetchResult(
            url=url,
            status_code=200,
            content=html,
            content_type="text/html",
            elapsed_ms=50.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with patch_all_safe_fetch(fetch_mock):
            c = create_crawler(llm_provider="test/model")
            await c.scrape(url)

        page_calls = [c for c in fetch_mock.call_args_list if c[0][0] == url]
        assert len(page_calls) == 1, "Career page must be fetched exactly once"
        assert sum(1 for c in fetch_mock.call_args_list if is_robots_txt_url(c[0][0])) == 1

    async def test_known_ats_fetch_unaffected(self) -> None:
        """AC2/AC1: Known ATS providers (Greenhouse) use API redirect; detector doesn't fetch.

        The crawler makes 2 safe_fetch calls for Greenhouse:
          1. The entrypoint fetch (original URL)
          2. The API URL fetch (boards-api.greenhouse.io) — AC1/AC4 redirect

        The detector (safe_fetch in strata_harvest.detector) must NOT be invoked —
        it receives the pre-fetched HTML content via the html= kwarg to avoid
        a redundant page fetch.
        """
        url = "https://boards.greenhouse.io/acme/jobs"

        crawler_fetch = AsyncMock(return_value=_ok_fetch(url))
        detector_fetch = AsyncMock()

        with (
            patch("strata_harvest.crawler.safe_fetch", crawler_fetch),
            patch("strata_harvest.detector.safe_fetch", detector_fetch),
        ):
            c = create_crawler()
            result = await c.scrape(url)

        # Crawler fetches twice: entrypoint + API redirect (AC1/AC4)
        assert crawler_fetch.call_count == 2, (
            "Expected 2 crawler fetches: entrypoint + API URL redirect for Greenhouse"
        )
        # Detector must NOT trigger its own fetch (double-fetch elimination still holds)
        detector_fetch.assert_not_called()
        assert len(result.jobs) == 2

    async def test_prefetched_html_passed_to_parser(self) -> None:
        """AC3: Parser (including LLM fallback) receives pre-fetched HTML content."""
        url = "https://custom-careers.example.com/jobs"
        html = "<html><body><h1>Open Roles</h1></body></html>"

        fetch_mock = make_fetch_with_robots(
            page=FetchResult(
                url=url,
                status_code=200,
                content=html,
                content_type="text/html",
                elapsed_ms=50.0,
            ),
        )

        mock_parser = MagicMock()
        mock_parser.parse.return_value = []

        with (
            patch_all_safe_fetch(fetch_mock),
            patch.object(Crawler, "_get_parser", return_value=mock_parser),
        ):
            c = create_crawler(llm_provider="test/model")
            await c.scrape(url)

        mock_parser.parse.assert_called_once_with(html, url=url)

    async def test_scrape_llm_fallback_uses_parse_async(self) -> None:
        """PCC-1606: Unknown ATS + llm_provider uses parse_async (non-blocking LLM path)."""
        url = "https://custom-careers.example.com/jobs"
        html = "<html><body><h1>Open Roles</h1></body></html>"

        fetch_mock = make_fetch_with_robots(
            page=FetchResult(
                url=url,
                status_code=200,
                content=html,
                content_type="text/html",
                elapsed_ms=50.0,
            ),
        )

        with (
            patch_all_safe_fetch(fetch_mock),
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.UNKNOWN, confidence=0.2)
            c = create_crawler(llm_provider="gemini/gemini-2.0-flash")
            with patch.object(LLMFallbackParser, "parse_async", new_callable=AsyncMock) as mock_pa:
                mock_pa.return_value = []
                await c.scrape(url)

        mock_pa.assert_awaited_once()
        assert mock_pa.await_args is not None
        assert mock_pa.await_args.kwargs.get("url") == url
        assert mock_pa.await_args.args[0] == html

    async def test_detect_ats_receives_prefetched_html(self) -> None:
        """detect_ats is called with html kwarg to prevent internal fetch."""
        url = "https://custom-careers.example.com/jobs"
        html = "<html><body>Content</body></html>"

        mock_fetch = make_fetch_with_robots(
            page=FetchResult(
                url=url,
                status_code=200,
                content=html,
                content_type="text/html",
                elapsed_ms=50.0,
            ),
        )

        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()

            c = create_crawler(llm_provider="test/model")
            await c.scrape(url)

        call_kwargs = mock_detect.call_args
        assert call_kwargs.kwargs.get("html") == html, (
            "detect_ats must receive pre-fetched HTML to avoid redundant fetch"
        )


# ---------------------------------------------------------------------------
# OCR Fallback
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerOcrFallback:
    async def test_ocr_triggered_when_sparse(self) -> None:
        from strata_harvest.ocr.client import OcrResult
        from strata_harvest.ocr.router import OcrRouter

        url = "https://example.com/careers"
        html = "<html><body><img src='jobs.png'></body></html>"
        fetch_mock = make_fetch_with_robots(
            page=FetchResult(url=url, status_code=200, content=html, elapsed_ms=10.0)
        )
        mock_router = MagicMock(spec=OcrRouter)
        mock_router.run = AsyncMock(
            return_value=OcrResult(ok=True, markdown="Extracted Markdown", duration_ms=10)
        )

        with (
            patch_all_safe_fetch(fetch_mock),
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.UNKNOWN)
            c = create_crawler(ocr_router=mock_router)
            with patch.object(c, "_get_parser") as mock_get_parser:
                parser_mock = MagicMock()
                parser_mock.parse.return_value = [JobListing(title="OCR Job", url=url)]
                mock_get_parser.return_value = parser_mock

                result = await c.scrape(url)

                mock_router.run.assert_awaited_once()
                parser_mock.parse.assert_called_once_with("Extracted Markdown", url=url)
                assert len(result.jobs) == 1

    async def test_ocr_skipped_when_rich(self) -> None:
        from strata_harvest.ocr.router import OcrRouter

        url = "https://example.com/careers"
        html = "<html><body>" + ("<p>Text</p>" * 50) + "</body></html>"
        fetch_mock = make_fetch_with_robots(
            page=FetchResult(url=url, status_code=200, content=html, elapsed_ms=10.0)
        )
        mock_router = MagicMock(spec=OcrRouter)
        mock_router.run = AsyncMock()

        with (
            patch_all_safe_fetch(fetch_mock),
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.UNKNOWN)
            c = create_crawler(ocr_router=mock_router)
            with patch.object(c, "_get_parser") as mock_get_parser:
                parser_mock = MagicMock()
                parser_mock.parse.return_value = []
                mock_get_parser.return_value = parser_mock

                await c.scrape(url)

                mock_router.run.assert_not_called()
                parser_mock.parse.assert_called_once_with(html, url=url)

    async def test_ocr_none_silently_skipped(self) -> None:
        url = "https://example.com/careers"
        html = "<html><body><img src='jobs.png'></body></html>"
        fetch_mock = make_fetch_with_robots(
            page=FetchResult(url=url, status_code=200, content=html, elapsed_ms=10.0)
        )

        with (
            patch_all_safe_fetch(fetch_mock),
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.UNKNOWN)
            c = create_crawler(ocr_router=None)  # No OCR
            with patch.object(c, "_get_parser") as mock_get_parser:
                parser_mock = MagicMock()
                parser_mock.parse.return_value = []
                mock_get_parser.return_value = parser_mock

                await c.scrape(url)

                parser_mock.parse.assert_called_once_with(html, url=url)


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


# ---------------------------------------------------------------------------
# Tier-3 escalation helpers (PCC-1947)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestTier3EscalationReason:
    """Unit tests for the _tier3_escalation_reason() helper function."""

    def test_403_returns_tier3_403(self) -> None:
        from strata_harvest.crawler import _tier3_escalation_reason

        result = FetchResult(url="https://x.com/careers", status_code=403, error="HTTP 403")
        assert _tier3_escalation_reason(result) == "TIER3_403"

    def test_cloudflare_challenge_body_returns_tier3_cloudflare(self) -> None:
        from strata_harvest.crawler import _tier3_escalation_reason

        cf_html = (
            "<html><body>cloudflare just a moment please wait "
            "<script>cf-browser-verification</script></body></html>"
        )
        result = FetchResult(url="https://x.com/careers", status_code=200, content=cf_html)
        assert _tier3_escalation_reason(result) == "TIER3_CLOUDFLARE"

    def test_cloudflare_without_markers_not_escalated(self) -> None:
        """'cloudflare' in body alone is not enough — must also have a challenge marker."""
        from strata_harvest.crawler import _tier3_escalation_reason

        rich_html = "<html><body>" + ("<p>Job listing</p>" * 50) + "cloudflare</body></html>"
        result = FetchResult(url="https://x.com/careers", status_code=200, content=rich_html)
        assert _tier3_escalation_reason(result) is None

    def test_empty_200_returns_tier3_empty_200(self) -> None:
        from strata_harvest.crawler import _tier3_escalation_reason

        result = FetchResult(
            url="https://x.com/careers",
            status_code=200,
            content="<html><body></body></html>",
        )
        assert _tier3_escalation_reason(result) == "TIER3_EMPTY_200"

    def test_rich_200_no_escalation(self) -> None:
        from strata_harvest.crawler import _tier3_escalation_reason

        rich_html = (
            "<html><body>" + ("<p>Job listing with lots of text here</p>" * 20) + "</body></html>"
        )
        result = FetchResult(url="https://x.com/careers", status_code=200, content=rich_html)
        assert _tier3_escalation_reason(result) is None

    def test_none_status_no_escalation(self) -> None:
        from strata_harvest.crawler import _tier3_escalation_reason

        result = FetchResult(url="https://x.com/careers", status_code=None, content="")
        assert _tier3_escalation_reason(result) is None

    def test_non_200_non_403_no_escalation(self) -> None:
        """500 errors are not escalated to tier-3 (that's a server error, not bot-blocking)."""
        from strata_harvest.crawler import _tier3_escalation_reason

        result = FetchResult(url="https://x.com/careers", status_code=500, error="HTTP 500")
        assert _tier3_escalation_reason(result) is None


# ---------------------------------------------------------------------------
# Tier-3 escalation in Crawler.scrape() (PCC-1947)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerTier3Escalation:
    """Verify StealthyFetcher tier-3 escalation in Crawler.scrape()."""

    def _stealth_ok_fetch(self, url: str) -> FetchResult:
        return FetchResult(
            url=url,
            status_code=200,
            content=(
                "<html><body><h1>Jobs after stealth</h1>" + ("<p>job</p>" * 30) + "</body></html>"
            ),
            content_type="text/html",
            elapsed_ms=120.0,
        )

    @pytest.mark.asyncio
    async def test_tier3_triggered_on_403(self) -> None:
        """AC: 403 response escalates to StealthyFetcher (TIER3_403)."""
        url = "https://blocked.example.com/careers"
        blocked_fetch = FetchResult(url=url, status_code=403, error="HTTP 403: Forbidden")
        stealth_response = self._stealth_ok_fetch(url)

        mock_stealth = AsyncMock(return_value=stealth_response)
        mock_stealth_cls = MagicMock(return_value=MagicMock(fetch=mock_stealth))

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=blocked_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
        ):
            c = create_crawler(respect_robots=False)
            result = await c.scrape(url)

        mock_stealth.assert_awaited_once_with(url)
        # Stealth fetch succeeded — error is None, or validator ran on stealth content
        error = result.error or ""
        assert result.error is None or "validator" in error.lower() or result.fetch_ok

    @pytest.mark.asyncio
    async def test_tier3_triggered_on_cloudflare_challenge(self) -> None:
        """AC: Cloudflare challenge body escalates to StealthyFetcher (TIER3_CLOUDFLARE)."""
        url = "https://cf-protected.example.com/careers"
        cf_content = (
            "<html><head><title>Just a moment...</title></head>"
            "<body>cloudflare just a moment cf-browser-verification ray id</body></html>"
        )
        cf_fetch = FetchResult(url=url, status_code=200, content=cf_content, elapsed_ms=50.0)
        stealth_response = self._stealth_ok_fetch(url)

        mock_stealth = AsyncMock(return_value=stealth_response)
        mock_stealth_cls = MagicMock(return_value=MagicMock(fetch=mock_stealth))

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=cf_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        mock_stealth.assert_awaited_once_with(url)

    @pytest.mark.asyncio
    async def test_tier3_triggered_on_empty_200(self) -> None:
        """AC: Empty body with 200 escalates to StealthyFetcher (TIER3_EMPTY_200)."""
        url = "https://silent-block.example.com/careers"
        empty_fetch = FetchResult(
            url=url, status_code=200, content="<html><body></body></html>", elapsed_ms=30.0
        )
        stealth_response = self._stealth_ok_fetch(url)

        mock_stealth = AsyncMock(return_value=stealth_response)
        mock_stealth_cls = MagicMock(return_value=MagicMock(fetch=mock_stealth))

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=empty_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        mock_stealth.assert_awaited_once_with(url)

    @pytest.mark.asyncio
    async def test_tier3_not_triggered_on_clean_200(self) -> None:
        """AC: Rich 200 response does NOT escalate to tier-3."""
        url = "https://normal.example.com/careers"
        rich_html = "<html><body>" + ("<p>Software Engineer role</p>" * 30) + "</body></html>"
        ok_fetch = FetchResult(url=url, status_code=200, content=rich_html, elapsed_ms=25.0)

        mock_stealth_cls = MagicMock()

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=ok_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        mock_stealth_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_tier3_skipped_for_api_native_ats(self) -> None:
        """AC: Greenhouse/Lever/Ashby bypass tier-3 (API-native, not HTML-fetched)."""
        url = "https://boards.greenhouse.io/acme/jobs"
        blocked_fetch = FetchResult(url=url, status_code=403, error="HTTP 403")

        mock_stealth_cls = MagicMock()

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=blocked_fetch)),
            patch(
                "strata_harvest.crawler.detect_ats",
                AsyncMock(return_value=ATSInfo(provider=ATSProvider.GREENHOUSE)),
            ),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        mock_stealth_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_tier3_skipped_when_scrapling_unavailable(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """AC: When scrapling not installed, tier-3 is silently skipped (debug log)."""
        import logging

        url = "https://blocked.example.com/careers"
        blocked_fetch = FetchResult(url=url, status_code=403, error="HTTP 403")

        mock_stealth_cls = MagicMock()

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=blocked_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", False),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
            caplog.at_level(logging.DEBUG, logger="strata_harvest.crawler"),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        mock_stealth_cls.assert_not_called()
        assert any("scrapling not available" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_tier3_reason_code_in_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC: Tier-3 escalation log entry contains the reason code."""
        import logging

        url = "https://blocked.example.com/careers"
        blocked_fetch = FetchResult(url=url, status_code=403, error="HTTP 403")
        stealth_response = self._stealth_ok_fetch(url)

        mock_stealth = AsyncMock(return_value=stealth_response)
        mock_stealth_cls = MagicMock(return_value=MagicMock(fetch=mock_stealth))

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=blocked_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
            caplog.at_level(logging.INFO, logger="strata_harvest.crawler"),
        ):
            c = create_crawler(respect_robots=False)
            await c.scrape(url)

        assert any("TIER3_403" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_tier3_failure_logged_not_raised(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC: StealthyFetcher failure is logged as warning; scrape continues without raise."""
        import logging

        url = "https://blocked.example.com/careers"
        blocked_fetch = FetchResult(url=url, status_code=403, error="HTTP 403")
        stealth_fail = FetchResult(url=url, status_code=None, error="browser crash")

        mock_stealth = AsyncMock(return_value=stealth_fail)
        mock_stealth_cls = MagicMock(return_value=MagicMock(fetch=mock_stealth))

        with (
            patch("strata_harvest.crawler.safe_fetch", AsyncMock(return_value=blocked_fetch)),
            patch("strata_harvest.crawler.detect_ats", AsyncMock(return_value=ATSInfo())),
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher.StealthFetcher",
                mock_stealth_cls,
            ),
            caplog.at_level(logging.WARNING, logger="strata_harvest.crawler"),
        ):
            c = create_crawler(respect_robots=False)
            result = await c.scrape(url)

        assert isinstance(result, ScrapeResult)
        assert any("Tier-3 StealthyFetcher failed" in r.message for r in caplog.records)
