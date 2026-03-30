"""Tests for parser registry and base class."""

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.base import BaseParser
from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.lever import LeverParser
from strata_harvest.parsers.llm_fallback import LLMFallbackParser


@pytest.mark.verification
class TestParserRegistry:
    def test_greenhouse_registered(self) -> None:
        parser = BaseParser.for_provider(ATSProvider.GREENHOUSE)
        assert isinstance(parser, GreenhouseParser)

    def test_lever_registered(self) -> None:
        parser = BaseParser.for_provider(ATSProvider.LEVER)
        assert isinstance(parser, LeverParser)

    def test_unknown_falls_back_to_llm(self) -> None:
        parser = BaseParser.for_provider(ATSProvider.UNKNOWN)
        assert isinstance(parser, LLMFallbackParser)

    def test_all_parsers_return_list(self) -> None:
        for provider in ATSProvider:
            parser = BaseParser.for_provider(provider)
            result = parser.parse("<html></html>", url="https://example.com")
            assert isinstance(result, list)
