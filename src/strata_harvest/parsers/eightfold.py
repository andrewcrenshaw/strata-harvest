"""Eightfold AI ATS parser — HTML extraction.

Eightfold has no public API.  Job data is rendered server-side (or via
client-side hydration) on pages at ``{slug}.eightfold.ai/careers`` and
white-label equivalents.

Detection signals: ``eightfold-`` / ``efai-`` CSS class prefixes in the DOM.

URL: {slug}.eightfold.ai/careers (+ white-label)
"""

from __future__ import annotations

import logging
import re

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

# Eightfold renders job cards inside elements with eightfold-/efai- classes
_JOB_BLOCK_RE = re.compile(
    r'<[^>]+class=["\'][^"\']*(?:eightfold-job|efai-job|eightfold-card|efai-card|job-card)[^"\']*["\'][^>]*>(.*?)</(?:div|article|li)>',
    re.DOTALL | re.IGNORECASE,
)
_TITLE_RE = re.compile(r'<h[1-4][^>]*>(.*?)</h[1-4]>', re.DOTALL | re.IGNORECASE)
_LINK_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")

# JSON-LD embedded in the page (some Eightfold pages emit structured data)
_JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


class EightfoldParser(BaseParser):
    """Parse job listings from Eightfold AI career pages.

    Eightfold has no public API.  This parser extracts job listings from
    rendered HTML using ``eightfold-`` / ``efai-`` class-prefix markers.
    Falls back to JSON-LD ``JobPosting`` blocks when present.
    """

    provider = ATSProvider.EIGHTFOLD

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Extract job listings from Eightfold HTML page content."""
        results = self._parse_json_ld(content, url)
        if results:
            return results

        for match in _JOB_BLOCK_RE.finditer(content):
            block = match.group(0)
            try:
                listing = self._parse_block(block, url)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed Eightfold job block")
                continue

        if not results:
            logger.debug("No Eightfold job blocks found; page may require JS rendering")

        return results

    def _parse_json_ld(self, content: str, source_url: str) -> list[JobListing]:
        """Extract JobPosting JSON-LD blocks when embedded in the page."""
        import json

        results: list[JobListing] = []
        for match in _JSON_LD_RE.finditer(content):
            try:
                data = json.loads(match.group(1))
            except (json.JSONDecodeError, TypeError):
                continue

            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "JobPosting":
                    continue
                title = item.get("title") or item.get("name")
                job_url = item.get("url") or source_url
                if not title:
                    continue
                location_obj = item.get("jobLocation") or {}
                location = None
                if isinstance(location_obj, dict):
                    addr = location_obj.get("address") or {}
                    if isinstance(addr, dict):
                        location = addr.get("addressLocality") or addr.get("name")
                    elif isinstance(addr, str):
                        location = addr

                results.append(
                    JobListing(
                        title=title,
                        url=job_url,
                        location=location,
                        department=item.get("occupationalCategory") or None,
                        employment_type=item.get("employmentType") or None,
                        ats_provider=ATSProvider.EIGHTFOLD,
                        raw_data=item,
                    )
                )
        return results

    def _parse_block(self, block: str, source_url: str) -> JobListing:
        """Extract a JobListing from a single Eightfold job card block."""
        title_match = _TITLE_RE.search(block)
        if not title_match:
            msg = "Eightfold job block missing title heading"
            raise ValueError(msg)
        title = _TAG_RE.sub("", title_match.group(1)).strip()
        if not title:
            msg = "Eightfold job block has empty title"
            raise ValueError(msg)

        link_match = _LINK_RE.search(block)
        job_url = link_match.group(1) if link_match else source_url
        if not job_url.startswith("http"):
            from urllib.parse import urljoin
            job_url = urljoin(source_url, job_url)

        return JobListing(
            title=title,
            url=job_url,
            ats_provider=ATSProvider.EIGHTFOLD,
            raw_data={"html_block": block},
        )
