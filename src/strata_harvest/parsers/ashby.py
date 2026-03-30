"""Ashby ATS parser — GraphQL API extraction."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class AshbyParser(BaseParser):
    """Parse job listings from Ashby career pages.

    Ashby uses a GraphQL API for job posting data.
    """

    provider = ATSProvider.ASHBY

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation in Phase 2A-6
        return []
