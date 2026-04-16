"""Phenom ATS parser — HTML extraction.

Phenom is white-label heavy; the canonical domain is ``careers.{company}.com``.
The API at ``api.phenom.com/jobs-api/v1/jobs`` is OAuth-gated and not publicly
accessible without credentials, so this parser scrapes the rendered HTML.

Detection signals: ``ph-`` / ``phw-`` CSS class prefixes in the DOM.

URL: careers.{company}.com (white-label), phenompeople.com
"""

from __future__ import annotations

import logging
import re
from typing import Any

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

# Phenom renders jobs inside elements with ph-/phw- prefixed classes
_JOB_BLOCK_RE = re.compile(
    r'<[^>]+class=["\'][^"\']*(?:ph-job|phw-job|ph-card|phw-card)[^"\']*["\'][^>]*>(.*?)</(?:div|article|li)>',
    re.DOTALL | re.IGNORECASE,
)
_TITLE_RE = re.compile(r'<h[1-4][^>]*>(.*?)</h[1-4]>', re.DOTALL | re.IGNORECASE)
_LINK_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


class PhenomParser(BaseParser):
    """Parse job listings from Phenom-powered career pages.

    Phenom's API is OAuth-gated; this parser extracts listings from the
    server-rendered HTML using ``ph-`` / ``phw-`` class-prefix markers.
    """

    provider = ATSProvider.PHENOM

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Extract job listings from Phenom HTML page content."""
        results: list[JobListing] = []
        for match in _JOB_BLOCK_RE.finditer(content):
            block = match.group(0)
            try:
                listing = self._parse_block(block, url)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed Phenom job block")
                continue

        if not results:
            logger.debug("No Phenom job blocks found; page may require JS rendering")

        return results

    def _parse_block(self, block: str, source_url: str) -> JobListing:
        """Extract a JobListing from a single Phenom job card block."""
        title_match = _TITLE_RE.search(block)
        if not title_match:
            msg = "Phenom job block missing title heading"
            raise ValueError(msg)
        title = _TAG_RE.sub("", title_match.group(1)).strip()
        if not title:
            msg = "Phenom job block has empty title"
            raise ValueError(msg)

        link_match = _LINK_RE.search(block)
        job_url = link_match.group(1) if link_match else source_url
        if not job_url.startswith("http"):
            from urllib.parse import urljoin
            job_url = urljoin(source_url, job_url)

        return JobListing(
            title=title,
            url=job_url,
            ats_provider=ATSProvider.PHENOM,
            raw_data={"html_block": block},
        )

    @staticmethod
    def build_api_url(url: str) -> str:
        """Return the OAuth-gated API hint (for reference; requires credentials)."""
        return "https://api.phenom.com/jobs-api/v1/jobs"

    @staticmethod
    def extract_metadata(block: str) -> dict[str, Any]:
        """Extract optional metadata (location, department) from a job block."""
        text = _TAG_RE.sub(" ", block)
        return {"raw_text": text.strip()}
