"""Workday ATS parser."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class WorkdayParser(BaseParser):
    """Parse job listings from Workday career pages."""

    provider = ATSProvider.WORKDAY

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation planned
        return []
