"""iCIMS ATS parser."""

from __future__ import annotations

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser


class ICIMSParser(BaseParser):
    """Parse job listings from iCIMS career pages."""

    provider = ATSProvider.ICIMS

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        # Implementation planned
        return []
