"""Unit tests for strata_harvest.browser.crawl4ai_fetcher.

Tests use mocked crawl4ai internals so no real browser is launched.
Integration tests (requiring a live network) are marked ``integration``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from strata_harvest.browser.crawl4ai_fetcher import Crawl4AIFetcher, crawl4ai_fetch

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_crawl_result(*, success: bool, html: str = "", error_message: str = "") -> MagicMock:
    """Build a fake crawl4ai CrawlResult-like object."""
    result = MagicMock()
    result.success = success
    result.html = html
    result.error_message = error_message
    return result


def _patch_crawl4ai(crawl_result: MagicMock):
    """Context-manager that patches the crawl4ai import path used by the fetcher."""
    mock_crawler_instance = AsyncMock()
    mock_crawler_instance.arun = AsyncMock(return_value=crawl_result)
    mock_crawler_instance.__aenter__ = AsyncMock(return_value=mock_crawler_instance)
    mock_crawler_instance.__aexit__ = AsyncMock(return_value=False)

    mock_async_web_crawler = MagicMock(return_value=mock_crawler_instance)
    mock_browser_config = MagicMock()
    mock_run_config = MagicMock()

    return patch.multiple(
        "strata_harvest.browser.crawl4ai_fetcher",
        AsyncWebCrawler=mock_async_web_crawler,
        BrowserConfig=MagicMock(return_value=mock_browser_config),
        CrawlerRunConfig=MagicMock(return_value=mock_run_config),
    )


# ---------------------------------------------------------------------------
# Crawl4AIFetcher unit tests
# ---------------------------------------------------------------------------


class TestCrawl4AIFetcherImportGuard:
    """Ensure ImportError is raised when crawl4ai is absent."""

    def test_raises_import_error_when_unavailable(self) -> None:
        with (
            patch("strata_harvest.browser.crawl4ai_fetcher._CRAWL4AI_AVAILABLE", False),
            pytest.raises(ImportError, match="crawl4ai is not installed"),
        ):
            Crawl4AIFetcher()


class TestCrawl4AIFetcherSuccess:
    """Happy-path fetch scenarios."""

    @pytest.mark.asyncio
    async def test_returns_html_on_success(self) -> None:
        expected_html = "<html><body><h1>Jobs at Rippling</h1></body></html>"
        result = _make_crawl_result(success=True, html=expected_html)

        with _patch_crawl4ai(result):
            fetcher = Crawl4AIFetcher()
            html = await fetcher.fetch("https://ats.rippling.com/rippling/jobs")

        assert html == expected_html

    @pytest.mark.asyncio
    async def test_returns_empty_string_on_failure_result(self) -> None:
        result = _make_crawl_result(success=False, error_message="Navigation timeout")

        with _patch_crawl4ai(result):
            fetcher = Crawl4AIFetcher()
            html = await fetcher.fetch("https://ats.rippling.com/rippling/jobs")

        assert html == ""

    @pytest.mark.asyncio
    async def test_returns_empty_string_when_html_is_none(self) -> None:
        result = _make_crawl_result(success=True, html="")

        with _patch_crawl4ai(result):
            fetcher = Crawl4AIFetcher()
            html = await fetcher.fetch("https://ats.rippling.com/rippling/jobs")

        assert html == ""

    @pytest.mark.asyncio
    async def test_returns_empty_string_on_exception(self) -> None:
        mock_crawler_instance = AsyncMock()
        mock_crawler_instance.arun = AsyncMock(side_effect=RuntimeError("browser crash"))
        mock_crawler_instance.__aenter__ = AsyncMock(return_value=mock_crawler_instance)
        mock_crawler_instance.__aexit__ = AsyncMock(return_value=False)

        with patch.multiple(
            "strata_harvest.browser.crawl4ai_fetcher",
            AsyncWebCrawler=MagicMock(return_value=mock_crawler_instance),
            BrowserConfig=MagicMock(),
            CrawlerRunConfig=MagicMock(),
        ):
            fetcher = Crawl4AIFetcher()
            html = await fetcher.fetch("https://example.com/careers")

        assert html == ""

    @pytest.mark.asyncio
    async def test_timeout_passed_as_milliseconds(self) -> None:
        """CrawlerRunConfig should receive timeout in milliseconds (crawl4ai convention)."""
        result = _make_crawl_result(success=True, html="<html/>")
        mock_run_config_cls = MagicMock()

        with (
            _patch_crawl4ai(result),
            patch(
                "strata_harvest.browser.crawl4ai_fetcher.CrawlerRunConfig",
                mock_run_config_cls,
            ),
        ):
            fetcher = Crawl4AIFetcher(timeout=45)
            await fetcher.fetch("https://example.com/careers")

        mock_run_config_cls.assert_called_once_with(page_timeout=45_000)


# ---------------------------------------------------------------------------
# crawl4ai_fetch convenience helper
# ---------------------------------------------------------------------------


class TestCrawl4AIFetchHelper:
    @pytest.mark.asyncio
    async def test_returns_html_from_helper(self) -> None:
        expected_html = "<html><body>Jobs</body></html>"
        result = _make_crawl_result(success=True, html=expected_html)

        with _patch_crawl4ai(result):
            html = await crawl4ai_fetch("https://ats.rippling.com/rippling/jobs")

        assert html == expected_html

    @pytest.mark.asyncio
    async def test_returns_empty_on_failure(self) -> None:
        result = _make_crawl_result(success=False, error_message="timeout")

        with _patch_crawl4ai(result):
            html = await crawl4ai_fetch("https://example.com/jobs")

        assert html == ""


# ---------------------------------------------------------------------------
# Integration test (live network, marked to be excluded from unit CI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCrawl4AIIntegration:
    """Live browser tests — require a real network and crawl4ai installed."""

    @pytest.mark.asyncio
    async def test_rippling_careers_renders_job_listings(self) -> None:
        """Fetch https://ats.rippling.com/rippling/jobs and verify non-empty HTML.

        AC from PCC-1808:
        - AsyncWebCrawler returns rendered HTML with job listings
        """
        url = "https://ats.rippling.com/rippling/jobs"
        fetcher = Crawl4AIFetcher(timeout=60)
        html = await fetcher.fetch(url)

        assert html, "Expected non-empty HTML from Rippling careers page"
        assert len(html) > 1000, f"HTML suspiciously short ({len(html)} bytes)"

        # Rippling's ATS renders job cards; at minimum the page title should appear
        lower_html = html.lower()
        assert any(
            kw in lower_html for kw in ("job", "role", "career", "opening", "position", "rippling")
        ), "No job-related keywords found in rendered HTML"
