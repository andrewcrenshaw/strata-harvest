"""Tests for robots.txt compliance (PCC-1610)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.crawler import create_crawler
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult
from strata_harvest.utils.robots import RobotsTxtChecker
from tests.robots_helpers import (
    PERMISSIVE_ROBOTS_TXT,
    is_robots_txt_url,
    make_fetch_with_robots,
    patch_all_safe_fetch,
)


@pytest.mark.verification
class TestRobotsTxtChecker:
    """AC1, AC5: cache and TTL behavior."""

    async def test_fetches_and_caches_per_domain(self) -> None:
        """Same domain: robots.txt fetched once; second URL uses cache."""
        calls: list[str] = []

        async def fetch(url: str, **kwargs: object) -> FetchResult:
            calls.append(url)
            if is_robots_txt_url(url):
                return FetchResult(
                    url=url,
                    status_code=200,
                    content=PERMISSIVE_ROBOTS_TXT,
                    elapsed_ms=1.0,
                )
            return FetchResult(url=url, status_code=200, content="ok", elapsed_ms=1.0)

        with patch("strata_harvest.utils.robots.safe_fetch", AsyncMock(side_effect=fetch)):
            checker = RobotsTxtChecker(ttl_seconds=3600.0)
            assert await checker.can_fetch("https://example.com/a", timeout=5.0) is True
            assert await checker.can_fetch("https://example.com/b", timeout=5.0) is True

        robots_calls = [u for u in calls if is_robots_txt_url(u)]
        assert len(robots_calls) == 1
        assert robots_calls[0] == "https://example.com/robots.txt"

    async def test_ttl_zero_refetches_each_can_fetch(self) -> None:
        """AC5: TTL of zero forces a fresh robots.txt read on each check (expired immediately)."""
        calls: list[str] = []

        async def fetch(url: str, **kwargs: object) -> FetchResult:
            calls.append(url)
            if is_robots_txt_url(url):
                return FetchResult(
                    url=url,
                    status_code=200,
                    content=PERMISSIVE_ROBOTS_TXT,
                    elapsed_ms=1.0,
                )
            return FetchResult(url=url, status_code=200, content="ok", elapsed_ms=1.0)

        with patch("strata_harvest.utils.robots.safe_fetch", AsyncMock(side_effect=fetch)):
            checker = RobotsTxtChecker(ttl_seconds=0.0)
            assert await checker.can_fetch("https://cache.test/foo", timeout=5.0) is True
            assert await checker.can_fetch("https://cache.test/bar", timeout=5.0) is True

        robots_hits = [u for u in calls if is_robots_txt_url(u)]
        assert robots_hits.count("https://cache.test/robots.txt") == 2


@pytest.mark.verification
class TestCrawlerRobotsCompliance:
    """AC2–AC4: crawler integration."""

    async def test_disallowed_url_skipped_with_message(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """AC2: blocked by robots → error result and log line."""
        import logging

        caplog.set_level(logging.INFO)
        disallow_all = "User-agent: *\nDisallow: /\n"
        page = FetchResult(
            url="https://block.example.com/careers",
            status_code=200,
            content="<html/>",
            elapsed_ms=10.0,
        )
        mock_fetch = make_fetch_with_robots(page=page, robots_txt=disallow_all)

        with patch_all_safe_fetch(mock_fetch):
            c = create_crawler()
            result = await c.scrape("https://block.example.com/careers")

        assert result.error is not None
        assert "robots.txt" in (result.error or "").lower()
        assert any("Skipping" in r.message and "robots.txt" in r.message for r in caplog.records)
        # Page fetch should not run when disallowed
        fetched_urls = [c[0][0] for c in mock_fetch.call_args_list]
        assert "https://block.example.com/careers" not in fetched_urls

    async def test_greenhouse_bypasses_robots(self) -> None:
        """AC3: API-oriented board URL does not request robots.txt."""
        urls: list[str] = []

        async def track(url: str, **kwargs: object) -> FetchResult:
            urls.append(url)
            return FetchResult(
                url=url,
                status_code=200,
                content='{"jobs":[]}',
                content_type="application/json",
                elapsed_ms=5.0,
            )

        mock = AsyncMock(side_effect=track)

        with (
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
            patch("strata_harvest.crawler.safe_fetch", mock),
        ):
            mock_detect.return_value = ATSInfo(
                provider=ATSProvider.GREENHOUSE,
                confidence=0.9,
            )
            c = create_crawler()
            await c.scrape("https://boards.greenhouse.io/acme/jobs")

        assert not any(is_robots_txt_url(u) for u in urls)

    async def test_lever_bypasses_robots(self) -> None:
        urls: list[str] = []

        async def track(url: str, **kwargs: object) -> FetchResult:
            urls.append(url)
            return FetchResult(
                url=url,
                status_code=200,
                content="[]",
                content_type="application/json",
                elapsed_ms=5.0,
            )

        mock = AsyncMock(side_effect=track)

        with (
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
            patch("strata_harvest.crawler.safe_fetch", mock),
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.LEVER, confidence=0.9)
            c = create_crawler()
            await c.scrape("https://jobs.lever.co/acme")

        assert not any(is_robots_txt_url(u) for u in urls)

    async def test_respect_robots_false_overrides(self) -> None:
        """AC4: with robots disallowing /, still fetch when respect_robots=False."""
        disallow_all = "User-agent: *\nDisallow: /\n"
        page = FetchResult(
            url="https://override.example.com/jobs",
            status_code=200,
            content="<html><body>x</body></html>",
            content_type="text/html",
            elapsed_ms=10.0,
        )
        mock_fetch = make_fetch_with_robots(page=page, robots_txt=disallow_all)

        with (
            patch("strata_harvest.crawler.detect_ats", new_callable=AsyncMock) as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()
            c = create_crawler(respect_robots=False, llm_provider="openai/gpt-4o-mini")
            result = await c.scrape("https://override.example.com/jobs")

        assert result.error is None
        page_calls = [c[0][0] for c in mock_fetch.call_args_list if c[0][0] == page.url]
        assert len(page_calls) == 1
