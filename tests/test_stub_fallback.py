"""Tests for stub parser fall-through to LLM extraction (PCC-1598, PCC-1631).

As of PCC-1631, Workday and iCIMS are no longer stubs — they implement
practical HTML parsing strategies.  These tests verify the stub flag is
correctly updated and that the parsers participate in normal parse flow.
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
    """Verify stub flags: Workday/iCIMS are non-stub as of PCC-1631."""

    def test_workday_is_not_stub(self) -> None:
        """AC2: WorkdayParser.is_stub is False after PCC-1631 implementation."""
        assert WorkdayParser.is_stub is False

    def test_icims_is_not_stub(self) -> None:
        """AC2: ICIMSParser.is_stub is False after PCC-1631 implementation."""
        assert ICIMSParser.is_stub is False

    def test_greenhouse_is_not_stub(self) -> None:
        assert GreenhouseParser.is_stub is False

    def test_llm_fallback_is_not_stub(self) -> None:
        assert LLMFallbackParser.is_stub is False

    def test_base_parser_default_is_not_stub(self) -> None:
        assert BaseParser.is_stub is False


@pytest.mark.verification
class TestParserResolution:
    """AC2: Workday/iCIMS resolve to their own parsers (no longer stub-fallthrough)."""

    def test_workday_resolves_to_workday_parser(self) -> None:
        """AC2: Workday for_provider returns WorkdayParser, not LLMFallbackParser."""
        parser = BaseParser.for_provider(ATSProvider.WORKDAY)
        assert isinstance(parser, WorkdayParser)

    def test_icims_resolves_to_icims_parser(self) -> None:
        """AC2: iCIMS for_provider returns ICIMSParser, not LLMFallbackParser."""
        parser = BaseParser.for_provider(ATSProvider.ICIMS)
        assert isinstance(parser, ICIMSParser)

    def test_workday_parser_returns_list(self) -> None:
        """AC2: WorkdayParser.parse() returns a list (no exception)."""
        parser = WorkdayParser()
        result = parser.parse("<html><body>No jobs</body></html>", url="https://company.wd5.myworkdayjobs.com/careers")
        assert isinstance(result, list)

    def test_icims_parser_returns_list(self) -> None:
        """AC2: ICIMSParser.parse() returns a list (no exception)."""
        parser = ICIMSParser()
        result = parser.parse("<html><body>No jobs</body></html>", url="https://company.icims.com/jobs/search")
        assert isinstance(result, list)

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
    """BaseParser.is_stub_provider — Workday/iCIMS are no longer stub providers (AC2)."""

    def test_workday_is_not_stub_provider(self) -> None:
        """AC2: Workday is no longer a stub provider."""
        assert BaseParser.is_stub_provider(ATSProvider.WORKDAY) is False

    def test_icims_is_not_stub_provider(self) -> None:
        """AC2: iCIMS is no longer a stub provider."""
        assert BaseParser.is_stub_provider(ATSProvider.ICIMS) is False

    def test_greenhouse_is_not_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.GREENHOUSE) is False

    def test_unknown_is_not_stub_provider(self) -> None:
        assert BaseParser.is_stub_provider(ATSProvider.UNKNOWN) is False


@pytest.mark.verification
class TestParserRegistration:
    """Workday/iCIMS parsers are still registered for correct ATS detection."""

    def test_workday_still_registered(self) -> None:
        assert ATSProvider.WORKDAY in _REGISTRY
        assert _REGISTRY[ATSProvider.WORKDAY] is WorkdayParser

    def test_icims_still_registered(self) -> None:
        assert ATSProvider.ICIMS in _REGISTRY
        assert _REGISTRY[ATSProvider.ICIMS] is ICIMSParser


@pytest.mark.verification
class TestWorkdayICIMSCrawlerFlow:
    """AC2: Workday/iCIMS no longer trigger stub error on scrape (PCC-1631)."""

    @pytest.mark.anyio
    async def test_workday_scrape_no_error_without_llm(self) -> None:
        """AC2: Workday scrape proceeds and returns fetch_ok=True without llm_provider."""
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
            content="<html><body><p>No structured jobs here.</p></body></html>",
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
        # No error — Workday is no longer a stub
        assert result.error is None
        assert result.ats_info.provider == ATSProvider.WORKDAY
        # fetch_ok=True because HTTP succeeded
        assert result.fetch_ok is True

    @pytest.mark.anyio
    async def test_icims_scrape_no_error_without_llm(self) -> None:
        """AC2: iCIMS scrape proceeds and returns fetch_ok=True without llm_provider."""
        from strata_harvest.crawler import create_crawler

        crawler = create_crawler(llm_provider=None, rate_limit=1000.0)

        mock_ats_info = ATSInfo(
            provider=ATSProvider.ICIMS,
            confidence=0.9,
            detection_method="url_pattern",
        )

        page = FetchResult(
            url="https://company.icims.com/jobs/search",
            status_code=200,
            content="<html><body><p>No structured jobs here.</p></body></html>",
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
            result = await crawler.scrape("https://company.icims.com/jobs/search")

        assert isinstance(result, ScrapeResult)
        assert result.error is None
        assert result.ats_info.provider == ATSProvider.ICIMS
        assert result.fetch_ok is True
