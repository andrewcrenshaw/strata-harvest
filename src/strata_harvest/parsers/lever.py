"""Lever ATS parser — JSON API extraction."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class LeverParser(BaseParser):
    """Parse job listings from Lever career pages.

    Lever exposes a JSON API endpoint that returns structured posting data.
    """

    provider = ATSProvider.LEVER

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation in Phase 2A-5
        return []
