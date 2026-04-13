"""Unit tests for strata_harvest.parsers.crawl4ai_extractor."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.crawl4ai_extractor import Crawl4AIExtractor

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_crawl_result(
    *, success: bool, extracted_content: str | None = None, error_message: str = ""
) -> MagicMock:
    """Build a fake crawl4ai CrawlResult-like object."""
    result = MagicMock()
    result.success = success
    result.extracted_content = extracted_content
    result.error_message = error_message
    return result


def _patch_crawl4ai(crawl_result: MagicMock):
    """Context-manager that patches the crawl4ai import path used by the extractor."""
    mock_crawler_instance = AsyncMock()
    mock_crawler_instance.arun = AsyncMock(return_value=crawl_result)
    mock_crawler_instance.__aenter__ = AsyncMock(return_value=mock_crawler_instance)
    mock_crawler_instance.__aexit__ = AsyncMock(return_value=False)

    mock_async_web_crawler = MagicMock(return_value=mock_crawler_instance)
    mock_run_config = MagicMock()
    mock_llm_config = MagicMock()
    mock_llm_extraction_strategy = MagicMock()

    return patch.multiple(
        "strata_harvest.parsers.crawl4ai_extractor",
        AsyncWebCrawler=mock_async_web_crawler,
        CrawlerRunConfig=mock_run_config,
        LLMConfig=mock_llm_config,
        LLMExtractionStrategy=mock_llm_extraction_strategy,
    )


# ---------------------------------------------------------------------------
# Crawl4AIExtractor unit tests
# ---------------------------------------------------------------------------


class TestCrawl4AIExtractorImportGuard:
    """Ensure ImportError is raised when crawl4ai is absent."""

    @pytest.mark.asyncio
    async def test_raises_import_error_when_unavailable(self) -> None:
        with patch("strata_harvest.parsers.crawl4ai_extractor._CRAWL4AI_AVAILABLE", False):
            extractor = Crawl4AIExtractor()
            with pytest.raises(ImportError, match="crawl4ai is not installed"):
                await extractor.extract("https://example.com")


class TestCrawl4AIExtractorSuccess:
    """Happy-path extract scenarios."""

    @pytest.mark.asyncio
    async def test_extracts_jobs_on_success(self) -> None:
        json_output = json.dumps(
            {
                "jobs": [
                    {
                        "title": "Software Engineer",
                        "url": "https://example.com/job/1",
                        "location": "Remote",
                        "requirements": ["Python", "AsyncIO"],
                    }
                ]
            }
        )
        result = _make_crawl_result(success=True, extracted_content=json_output)

        with _patch_crawl4ai(result):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/jobs")

        assert len(jobs) == 1
        assert jobs[0].title == "Software Engineer"
        assert str(jobs[0].url) == "https://example.com/job/1"
        assert jobs[0].location == "Remote"
        assert jobs[0].requirements == ["Python", "AsyncIO"]
        assert jobs[0].ats_provider == ATSProvider.UNKNOWN

    @pytest.mark.asyncio
    async def test_returns_empty_list_on_failure_result(self) -> None:
        result = _make_crawl_result(success=False, error_message="Navigation timeout")

        with _patch_crawl4ai(result):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/jobs")

        assert jobs == []

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_content_is_none(self) -> None:
        result = _make_crawl_result(success=True, extracted_content=None)

        with _patch_crawl4ai(result):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/jobs")

        assert jobs == []

    @pytest.mark.asyncio
    async def test_returns_empty_list_on_exception(self) -> None:
        mock_crawler_instance = AsyncMock()
        mock_crawler_instance.arun = AsyncMock(side_effect=RuntimeError("browser crash"))
        mock_crawler_instance.__aenter__ = AsyncMock(return_value=mock_crawler_instance)
        mock_crawler_instance.__aexit__ = AsyncMock(return_value=False)

        with patch.multiple(
            "strata_harvest.parsers.crawl4ai_extractor",
            AsyncWebCrawler=MagicMock(return_value=mock_crawler_instance),
            CrawlerRunConfig=MagicMock(),
            LLMConfig=MagicMock(),
            LLMExtractionStrategy=MagicMock(),
        ):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/careers")

        assert jobs == []

    @pytest.mark.asyncio
    async def test_extracts_jobs_with_list_format(self) -> None:
        json_output = json.dumps([{"title": "Backend Dev", "url": "https://example.com/job/2"}])
        result = _make_crawl_result(success=True, extracted_content=json_output)

        with _patch_crawl4ai(result):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/jobs")

        assert len(jobs) == 1
        assert jobs[0].title == "Backend Dev"
        assert str(jobs[0].url) == "https://example.com/job/2"

    @pytest.mark.asyncio
    async def test_handles_malformed_json(self) -> None:
        result = _make_crawl_result(success=True, extracted_content="{bad_json}")

        with _patch_crawl4ai(result):
            extractor = Crawl4AIExtractor()
            jobs = await extractor.extract("https://example.com/jobs")

        assert jobs == []
