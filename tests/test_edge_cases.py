"""Edge case tests for strata-harvest (PCC-1428).

AC-required scenarios:
- Empty career page
- Page with 0 listings
- ATS API down
- Timeout on slow page
- Malformed HTML
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from strata_harvest.crawler import Crawler, create_crawler
from strata_harvest.detector import detect_ats, detect_from_dom, detect_from_url
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, ScrapeResult
from strata_harvest.parsers.ashby import AshbyParser
from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.lever import LeverParser
from strata_harvest.parsers.llm_fallback import LLMFallbackParser
from strata_harvest.utils.http import safe_fetch
from tests.robots_helpers import make_fetch_with_robots, patch_all_safe_fetch
from tests.test_http import _stream_cm

# ---------------------------------------------------------------------------
# Edge case: Empty career page
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestEmptyCareerPage:
    """AC: empty career page returns graceful empty result."""

    async def test_harvest_empty_html_page(self) -> None:
        """Completely empty HTML body yields no jobs."""
        fetch_result = FetchResult(
            url="https://example.com/careers",
            status_code=200,
            content="<html><body></body></html>",
            content_type="text/html",
            elapsed_ms=50.0,
        )
        mock_fetch = make_fetch_with_robots(page=fetch_result)
        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()
            crawler = create_crawler()
            result = await crawler.scrape("https://example.com/careers")

        assert result.jobs == []
        assert result.error is None
        assert result.content_hash is not None

    async def test_harvest_blank_content(self) -> None:
        """Empty string content yields no jobs."""
        fetch_result = FetchResult(
            url="https://example.com/careers",
            status_code=200,
            content="",
            content_type="text/html",
            elapsed_ms=10.0,
        )
        mock_fetch = make_fetch_with_robots(page=fetch_result)
        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()
            crawler = create_crawler()
            result = await crawler.scrape("https://example.com/careers")

        assert result.jobs == []

    def test_greenhouse_parse_empty_json_object(self) -> None:
        parser = GreenhouseParser()
        result = parser.parse("{}", url="https://boards.greenhouse.io/empty")
        assert result == []

    def test_lever_parse_empty_string(self) -> None:
        parser = LeverParser()
        result = parser.parse("", url="https://jobs.lever.co/empty")
        assert result == []

    def test_ashby_parse_empty_string(self) -> None:
        parser = AshbyParser()
        result = parser.parse("", url="https://jobs.ashbyhq.com/empty")
        assert result == []

    def test_detect_from_dom_empty_string(self) -> None:
        info = detect_from_dom("")
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0

    def test_detect_from_url_empty_string(self) -> None:
        info = detect_from_url("")
        assert info.provider == ATSProvider.UNKNOWN


# ---------------------------------------------------------------------------
# Edge case: Page with 0 listings
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestZeroListings:
    """AC: page with 0 listings returns empty result gracefully."""

    def test_greenhouse_zero_jobs(self) -> None:
        content = json.dumps({"jobs": [], "meta": {"total": 0}})
        parser = GreenhouseParser()
        result = parser.parse(content, url="https://boards.greenhouse.io/co")
        assert result == []

    def test_lever_zero_postings(self) -> None:
        content = json.dumps([])
        parser = LeverParser()
        result = parser.parse(content, url="https://jobs.lever.co/co")
        assert result == []

    def test_ashby_zero_postings(self) -> None:
        content = json.dumps({"data": {"jobBoard": {"title": "Test Board", "jobPostings": []}}})
        parser = AshbyParser()
        result = parser.parse(content, url="https://jobs.ashbyhq.com/co")
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_llm_zero_jobs_response(self, mock_litellm: MagicMock) -> None:
        message = MagicMock()
        message.content = json.dumps({"jobs": []})
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        parser = LLMFallbackParser()
        result = parser.parse(
            "<html><body>No jobs here</body></html>",
            url="https://example.com/careers",
        )
        assert result == []

    async def test_scrape_result_ok_false_when_zero_jobs(self) -> None:
        """ScrapeResult.ok should be False when there are 0 jobs and no error."""
        result = ScrapeResult(url="https://example.com/careers", jobs=[])
        assert result.ok is False


# ---------------------------------------------------------------------------
# Edge case: ATS API down
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestATSAPIDown:
    """AC: ATS API down returns structured error, never raises."""

    async def test_safe_fetch_connection_refused(self) -> None:
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.stream = MagicMock(side_effect=httpx.ConnectError("Connection refused"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://api.down.example.com", retries=0)

        assert result.ok is False
        assert "ConnectError" in (result.error or "")
        assert result.status_code is None

    async def test_crawler_scrape_api_500(self) -> None:
        """500 from the ATS API returns error ScrapeResult, not an exception."""
        url = "https://boards.greenhouse.io/down/jobs"
        error_result = FetchResult(
            url=url,
            status_code=500,
            error="HTTP 500: Internal Server Error",
            elapsed_ms=100.0,
        )
        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch("strata_harvest.crawler.safe_fetch") as mock_fetch,
        ):
            mock_detect.return_value = ATSInfo(provider=ATSProvider.GREENHOUSE, confidence=0.9)
            mock_fetch.return_value = error_result
            crawler = create_crawler()
            result = await crawler.scrape(url)

        assert result.ok is False
        assert result.error is not None
        assert "500" in result.error
        assert result.jobs == []

    async def test_greenhouse_fetch_all_api_unavailable(self) -> None:
        error_result = FetchResult(
            url="https://boards-api.greenhouse.io/v1/boards/down/jobs?content=true",
            error="ConnectError: Connection refused",
        )
        with patch(
            "strata_harvest.parsers.greenhouse.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/down")
        assert listings == []

    async def test_lever_fetch_all_api_unavailable(self) -> None:
        error_result = FetchResult(
            url="https://api.lever.co/v0/postings/down?mode=json&skip=0&limit=100",
            error="ConnectError: Connection refused",
        )
        with patch(
            "strata_harvest.parsers.lever.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await LeverParser.fetch_all("https://jobs.lever.co/down")
        assert listings == []

    async def test_ashby_fetch_all_api_unavailable(self) -> None:
        error_result = FetchResult(
            url="https://jobs.ashbyhq.com/api/non-user-graphql",
            error="ConnectError: Connection refused",
        )
        with patch(
            "strata_harvest.parsers.ashby.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = error_result
            listings = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/downcorp")
        assert listings == []

    async def test_detect_ats_fetch_failure_returns_unknown(self) -> None:
        """When detect_ats needs to fetch HTML and the fetch fails, returns UNKNOWN."""
        fetch_result = FetchResult(
            url="https://example.com/careers",
            error="ConnectError: Connection refused",
        )
        with patch(
            "strata_harvest.detector.safe_fetch",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_fetch.return_value = fetch_result
            info = await detect_ats("https://example.com/careers")
        assert info.provider == ATSProvider.UNKNOWN


# ---------------------------------------------------------------------------
# Edge case: Timeout on slow page
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestTimeoutOnSlowPage:
    """AC: timeout on slow page returns structured error, never raises."""

    async def test_safe_fetch_timeout(self) -> None:
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.stream = MagicMock(side_effect=httpx.TimeoutException("Read timed out"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://slow.example.com", retries=0)

        assert result.ok is False
        assert "TimeoutException" in (result.error or "")
        assert result.elapsed_ms >= 0

    async def test_crawler_timeout_yields_error_result(self) -> None:
        """Timeout during crawler scrape yields ScrapeResult with error."""
        url = "https://slow.example.com/careers"
        timeout_result = FetchResult(
            url=url,
            error="TimeoutException: Read timed out",
            elapsed_ms=30000.0,
        )
        mock_fetch = make_fetch_with_robots(page=timeout_result)
        with (
            patch("strata_harvest.crawler.detect_ats") as mock_detect,
            patch_all_safe_fetch(mock_fetch),
        ):
            mock_detect.return_value = ATSInfo()
            crawler = create_crawler(timeout=1.0)
            result = await crawler.scrape(url)

        assert result.ok is False
        assert "Timeout" in (result.error or "")
        assert result.jobs == []

    async def test_safe_fetch_timeout_retries_then_fails(self) -> None:
        """Timeout with retries exhausted still returns structured error."""
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.stream = MagicMock(side_effect=httpx.TimeoutException("Read timed out"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await safe_fetch("https://slow.example.com", retries=2)

        assert result.ok is False
        assert instance.stream.call_count == 3

    async def test_safe_fetch_timeout_then_success(self) -> None:
        """First attempt times out, retry succeeds."""
        ok_response = httpx.Response(
            200,
            text='{"jobs": []}',
            headers={"content-type": "application/json"},
            request=httpx.Request("GET", "https://slow.example.com"),
        )
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.stream = MagicMock(
                side_effect=[
                    httpx.TimeoutException("Read timed out"),
                    _stream_cm(ok_response),
                ]
            )
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await safe_fetch("https://slow.example.com", retries=1)

        assert result.ok is True
        assert instance.stream.call_count == 2


# ---------------------------------------------------------------------------
# Edge case: Malformed HTML
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestMalformedHTML:
    """AC: malformed HTML is handled gracefully."""

    def test_greenhouse_parse_malformed_html(self) -> None:
        """Non-JSON HTML passed to Greenhouse parser returns empty."""
        parser = GreenhouseParser()
        result = parser.parse(
            "<html><body><p>This is not JSON</p></body></html>",
            url="https://boards.greenhouse.io/co",
        )
        assert result == []

    def test_lever_parse_malformed_html(self) -> None:
        parser = LeverParser()
        result = parser.parse(
            "<html><body>broken html <<<>>>",
            url="https://jobs.lever.co/co",
        )
        assert result == []

    def test_ashby_parse_malformed_html(self) -> None:
        parser = AshbyParser()
        result = parser.parse(
            "<html><body>not graphql json</body></html>",
            url="https://jobs.ashbyhq.com/co",
        )
        assert result == []

    def test_detect_from_dom_malformed_html(self) -> None:
        """DOM detection on garbage HTML doesn't crash."""
        info = detect_from_dom("<<<<>>>>>>><div class='broken>")
        assert info.provider == ATSProvider.UNKNOWN

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_llm_fallback_with_malformed_html(self, mock_litellm: MagicMock) -> None:
        """LLM fallback should handle mangled HTML gracefully."""
        message = MagicMock()
        message.content = json.dumps({"jobs": []})
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        parser = LLMFallbackParser()
        result = parser.parse(
            "<<<broken>>>html<<<&&&>>>",
            url="https://example.com/careers",
        )
        assert isinstance(result, list)

    def test_greenhouse_truncated_json(self) -> None:
        """Truncated JSON (incomplete response) returns empty."""
        parser = GreenhouseParser()
        result = parser.parse(
            '{"jobs": [{"id": 1, "title": "Incomplete',
            url="https://boards.greenhouse.io/co",
        )
        assert result == []

    def test_lever_truncated_json(self) -> None:
        parser = LeverParser()
        result = parser.parse(
            '[{"id": "abc", "text": "Incomplete',
            url="https://jobs.lever.co/co",
        )
        assert result == []

    def test_ashby_truncated_json(self) -> None:
        parser = AshbyParser()
        result = parser.parse(
            '{"data": {"jobBoard": {"jobPostings": [{"title":',
            url="https://jobs.ashbyhq.com/co",
        )
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_llm_whitespace_only_content(self, mock_litellm: MagicMock) -> None:
        """Content with only whitespace should not call the LLM."""
        parser = LLMFallbackParser()
        result = parser.parse("   \n\t  ", url="https://example.com")
        assert result == []
        mock_litellm.completion.assert_not_called()


# ---------------------------------------------------------------------------
# Edge case: Crawler _get_parser with llm_provider
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerGetParser:
    """Verify Crawler._get_parser wiring for LLM fallback provider."""

    def test_unknown_provider_without_llm_uses_default_fallback(self) -> None:
        crawler = create_crawler()
        parser = crawler._get_parser(ATSProvider.UNKNOWN)
        assert isinstance(parser, LLMFallbackParser)

    def test_unknown_provider_with_llm_provider_wires_model(self) -> None:
        crawler = create_crawler(llm_provider="openai/gpt-4o-mini")
        parser = crawler._get_parser(ATSProvider.UNKNOWN)
        assert isinstance(parser, LLMFallbackParser)
        assert parser._model == "openai/gpt-4o-mini"

    def test_known_provider_ignores_llm_provider(self) -> None:
        crawler = create_crawler(llm_provider="openai/gpt-4o-mini")
        parser = crawler._get_parser(ATSProvider.GREENHOUSE)
        assert isinstance(parser, GreenhouseParser)


# ---------------------------------------------------------------------------
# Edge case: scrape_batch exception in worker
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestScrapeBatchWorkerException:
    """Verify scrape_batch handles exceptions in individual workers."""

    async def test_worker_exception_yields_error_result(self) -> None:
        """If a scrape raises an unexpected exception, the batch still returns a result."""
        call_count = 0

        async def exploding_scrape(self: Crawler, url: str, **kwargs: object) -> ScrapeResult:
            nonlocal call_count
            call_count += 1
            if "fail" in url:
                raise RuntimeError("Unexpected internal error")
            return ScrapeResult(url=url, content_hash="ok")

        with patch.object(Crawler, "scrape", exploding_scrape):
            crawler = create_crawler()
            results: list[ScrapeResult] = []
            async for result in crawler.scrape_batch(
                ["https://ok.example.com", "https://fail.example.com"],
                concurrency=2,
            ):
                results.append(result)

        assert len(results) == 2
        errors = [r for r in results if r.error is not None]
        assert len(errors) == 1
        assert "RuntimeError" in errors[0].error
