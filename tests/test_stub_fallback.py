"""Tests for stub parser fall-through to LLM extraction (PCC-1598).

Stub parsers (Workday, iCIMS) should not silently return empty results.
Instead, they should fall through to LLM extraction when configured, or
return an informative error when no LLM provider is available.
"""

from unittest.mock import AsyncMock, patch

import pytest

from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, ScrapeResult
from strata_harvest.parsers.base import _REGISTRY, BaseParser
from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.icims import ICIMSParser
from strata_harvest.parsers.llm_fallback import LLMFallbackParser
from strata_harvest.parsers.workday import WorkdayParser
from tests.robots_helpers import make_fetch_with_robots, patch_all_safe_fetch


@pytest.mark.verification
class TestStubFlag:
    """Stub parsers are marked and non-stub parsers are not."""

    def test_workday_is_stub(self) -> None:
        assert WorkdayParser.is_stub is True

    def test_icims_is_stub(self) -> None:
        assert ICIMSParser.is_stub is True

    def test_greenhouse_is_not_stub(self) -> None:
        assert GreenhouseParser.is_stub is False

    def test_llm_fallback_is_not_stub(self) -> None:
        assert LLMFallbackParser.is_stub is False

    def test_base_parser_default_is_not_stub(self) -> None:
        assert BaseParser.is_stub is False


@pytest.mark.verification
class TestStubFallthrough:
    """AC1/AC2: Stub parsers fall through to LLM extraction."""

    def test_workday_falls_through_to_llm_with_provider(self) -> None:
        """AC1: Workday falls through to LLM when llm_provider configured."""
        parser = BaseParser.for_provider(
            ATSProvider.WORKDAY, llm_provider="gemini/gemini-2.0-flash"
        )
        assert isinstance(parser, LLMFallbackParser)
        assert parser._model == "gemini/gemini-2.0-flash"

    def test_icims_falls_through_to_llm_with_provider(self) -> None:
        """AC2: iCIMS falls through to LLM when llm_provider configured."""
        parser = BaseParser.for_provider(ATSProvider.ICIMS, llm_provider="openai/gpt-4o-mini")
        assert isinstance(parser, LLMFallbackParser)
        assert parser._model == "openai/gpt-4o-mini"

    def test_workday_falls_through_to_llm_without_provider(self) -> None:
        """Stub returns LLMFallbackParser even without explicit provider."""
        parser = BaseParser.for_provider(ATSProvider.WORKDAY)
        assert isinstance(parser, LLMFallbackParser)

    def test_icims_falls_through_to_llm_without_provider(self) -> None:
        """Stub returns LLMFallbackParser even without explicit provider."""
        parser = BaseParser.for_provider(ATSProvider.ICIMS)
        assert isinstance(parser, LLMFallbackParser)

    def test_greenhouse_not_affected(self) -> None:
        """Non-stub parsers are returned normally."""
        parser = BaseParser.for_provider(ATSProvider.GREENHOUSE)
        assert isinstance(parser, GreenhouseParser)

    def test_unknown_still_uses_llm(self) -> None:
        """UNKNOWN provider still falls back to LLM (existing behavior)."""
        parser = BaseParser.for_provider(ATSProvider.UNKNOWN)
        assert isinstance(parser, LLMFallbackParser)


@pytest.mark.verification
class TestIsStubProvider:
    """BaseParser.is_stub_provider identifies stub providers."""

    def test_workday_is_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.WORKDAY) is True

    def test_icims_is_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.ICIMS) is True

    def test_greenhouse_is_not_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.GREENHOUSE) is False

    def test_unknown_is_not_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.UNKNOWN) is False


@pytest.mark.verification
class TestStubRegistration:
    """AC4: Stub parsers are still registered for correct ATS detection."""

    def test_workday_still_registered(self) -> None:
        assert ATSProvider.WORKDAY in _REGISTRY
        assert _REGISTRY[ATSProvider.WORKDAY] is WorkdayParser

    def test_icims_still_registered(self) -> None:
        assert ATSProvider.ICIMS in _REGISTRY
        assert _REGISTRY[ATSProvider.ICIMS] is ICIMSParser


@pytest.mark.verification
class TestStubCrawlerError:
    """AC3: Stub parser + no LLM provider → informative error."""

    @pytest.mark.anyio
    async def test_stub_no_llm_returns_error(self) -> None:
        """Scraping a stub-provider URL without llm_provider returns an error."""
        from strata_harvest.crawler import create_crawler

        crawler = create_crawler(llm_provider=None, rate_limit=1000.0)

        mock_ats_info = ATSInfo(
            provider=ATSProvider.WORKDAY,
            confidence=0.9,
            detection_method="url_pattern",
        )

        page = FetchResult(
            url="https://company.wd5.myworkdayjobs.com/careers",
            status_code=200,
            content="<html><body></body></html>",
            content_type="text/html",
            elapsed_ms=1.0,
        )
        fetch_mock = make_fetch_with_robots(page=page)

        with (
            patch(
                "strata_harvest.crawler.detect_ats",
                new_callable=AsyncMock,
                return_value=mock_ats_info,
            ),
            patch_all_safe_fetch(fetch_mock),
        ):
            result = await crawler.scrape("https://company.wd5.myworkdayjobs.com/careers")

        assert isinstance(result, ScrapeResult)
        assert result.error is not None
        assert result.jobs == []
        assert result.ats_info.provider == ATSProvider.WORKDAY
        assert "workday" in result.error.lower()
        assert "llm" in result.error.lower() or "not yet implemented" in result.error.lower()

    @pytest.mark.anyio
    async def test_stub_with_llm_does_not_return_error(self) -> None:
        """Scraping a stub-provider URL WITH llm_provider proceeds to fetch."""
        from strata_harvest.crawler import create_crawler

        crawler = create_crawler(llm_provider="gemini/gemini-2.0-flash", rate_limit=1000.0)

        mock_ats_info = ATSInfo(
            provider=ATSProvider.WORKDAY,
            confidence=0.9,
            detection_method="url_pattern",
        )
        mock_fetch = FetchResult(
            url="https://company.wd5.myworkdayjobs.com/careers",
            status_code=200,
            content="<html><body>Jobs here</body></html>",
            content_type="text/html",
        )

        fetch_mock = make_fetch_with_robots(page=mock_fetch)

        with (
            patch(
                "strata_harvest.crawler.detect_ats",
                new_callable=AsyncMock,
                return_value=mock_ats_info,
            ),
            patch_all_safe_fetch(fetch_mock),
        ):
            result = await crawler.scrape("https://company.wd5.myworkdayjobs.com/careers")

        assert result.error is None
