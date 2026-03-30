"""Greenhouse ATS parser — REST API extraction."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class GreenhouseParser(BaseParser):
    """Parse job listings from Greenhouse career pages.

    Greenhouse exposes a REST API at /embed/api/v1/jobs that returns
    structured JSON. This parser handles both API and HTML fallback.
    """

    provider = ATSProvider.GREENHOUSE

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation in Phase 2A-4
        return []
