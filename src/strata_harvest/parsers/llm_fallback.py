"""LLM-based fallback parser for unknown ATS providers."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class LLMFallbackParser(BaseParser):
    """Extract job listings using LLM when no known ATS parser matches.

    Uses configurable LLM provider (Gemini Flash by default) to extract
    structured job listing data from raw HTML/text content.

    Requires the `llm` extra: pip install strata-harvest[llm]
    """

    provider = ATSProvider.UNKNOWN

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation in Phase 2A-7
        return []
