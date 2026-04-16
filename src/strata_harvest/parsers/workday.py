"""Workday ATS parser — JSON-LD and embedded JSON extraction.

Workday career pages are JavaScript-rendered SPAs, but they often embed
structured job data in two parseable formats:

1. ``<script type="application/ld+json">`` blocks with ``JobPosting`` schema.
2. A page-level ``window.__INITIAL_STATE__`` or embedded ``appState`` JSON
   blob that contains job listing arrays.

This parser extracts what it can from those sources without requiring a
headless browser.

JSON-LD extraction uses the shared :mod:`_structured_data` helper (extruct
when available, regex fallback otherwise) for robustness against whitespace,
HTML-entity escaping, and nested script blocks.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urljoin

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers._structured_data import extract_structured_data, salary_to_string
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

_TAG_PATTERN = re.compile(r"<[^>]+")

# Common Workday job link pattern: /jobs/{id}/{slug}
_WORKDAY_JOB_LINK_PATTERN = re.compile(
    r'href=["\']([^"\']*(?:/jobs?/\d+|/wd/plsql/thrcb\.main)[^"\']*)["\']',
    re.IGNORECASE,
)

# Title extraction heuristics for Workday HTML headings near job links
_TITLE_ATTR_PATTERN = re.compile(
    r'(?:aria-label|title|data-automation-id=["\']jobTitle["\'])[^>]*>\s*([^<]+)',
    re.IGNORECASE,
)


class WorkdayParser(BaseParser):
    """Parse job listings from Workday career pages.

    Extracts structured data from:
    - JSON-LD ``<script type="application/ld+json">`` blocks
    - Common Workday SPA embedded JSON blobs

    Returns an empty list if neither format is found — the page likely
    requires JavaScript rendering (configure ``llm_provider`` as fallback).
    """

    provider = ATSProvider.WORKDAY
    is_stub = False

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Workday page content for structured job data.

        Attempts JSON-LD extraction first, then falls back to common SPA
        embedded JSON patterns.  Returns empty list on parse failure.
        """
        if not content:
            return []

        # Strategy 1: JSON-LD blocks
        jobs = self._parse_json_ld(content, base_url=url)
        if jobs:
            return jobs

        # Strategy 2: Embedded JSON blob (window.__INITIAL_STATE__ / appState)
        jobs = self._parse_embedded_json(content, base_url=url)
        if jobs:
            return jobs

        return []

    # ------------------------------------------------------------------
    # Strategy 1: JSON-LD (via extruct or regex fallback)
    # ------------------------------------------------------------------

    def _parse_json_ld(self, html: str, *, base_url: str) -> list[JobListing]:
        """Extract JobPosting items using the shared structured-data helper.

        Uses extruct when installed (``pip install strata-harvest[extract]``)
        for robust handling of HTML-entity-escaped content, nested scripts, and
        malformed whitespace.  Falls back to regex + ``json.loads`` otherwise.
        """
        structured = extract_structured_data(html, base_url=base_url)
        listings: list[JobListing] = []
        for item in structured.job_postings:
            listing = self._json_ld_to_listing(item, base_url=base_url)
            if listing:
                listings.append(listing)
        return listings

    def _json_ld_to_listing(self, item: dict[str, Any], *, base_url: str) -> JobListing | None:
        """Convert a JSON-LD JobPosting dict to a JobListing."""
        title = item.get("title") or item.get("name")
        if not title:
            return None

        job_url = item.get("url") or item.get("sameAs")
        if not job_url:
            identifier = item.get("identifier")
            if isinstance(identifier, dict):
                job_url = identifier.get("url")
            if not job_url:
                job_url = base_url

        # Resolve relative URLs
        if job_url and not job_url.startswith("http"):
            job_url = urljoin(base_url, job_url)

        # Location: prefer jobLocationType=TELECOMMUTE → "Remote"
        job_location_type = item.get("jobLocationType")
        location_obj = item.get("jobLocation")
        location = None
        if job_location_type and str(job_location_type).upper() == "TELECOMMUTE":
            location = "Remote"
        elif isinstance(location_obj, dict):
            addr = location_obj.get("address")
            if isinstance(addr, dict):
                parts = [
                    addr.get("addressLocality"),
                    addr.get("addressRegion"),
                    addr.get("addressCountry"),
                ]
                location = ", ".join(p for p in parts if p) or None
            elif isinstance(addr, str):
                location = addr
        elif isinstance(location_obj, str):
            location = location_obj

        description_raw = item.get("description") or ""
        description = _TAG_PATTERN.sub("", description_raw).strip() or None

        employment_type = item.get("employmentType") or None
        salary_range = salary_to_string(item.get("baseSalary"))

        try:
            return JobListing(
                title=str(title),
                url=str(job_url),
                location=location,
                department=None,
                description=description,
                employment_type=str(employment_type) if employment_type else None,
                salary_range=salary_range,
                ats_provider=ATSProvider.WORKDAY,
                raw_data=item,
            )
        except Exception:
            logger.debug("Skipping malformed Workday JSON-LD job: %s", title)
            return None

    # ------------------------------------------------------------------
    # Strategy 2: Embedded JSON blob
    # ------------------------------------------------------------------

    def _parse_embedded_json(self, html: str, *, base_url: str) -> list[JobListing]:
        """Search for Workday SPA JSON blobs with job arrays."""
        # Look for inline JSON containing "jobPostingInfo" or "jobRequisition"
        pattern = re.compile(
            r'"jobPostings?"\s*:\s*(\[.*?\])',
            re.DOTALL,
        )
        listings: list[JobListing] = []
        for match in pattern.finditer(html):
            raw = match.group(1)
            try:
                jobs_data = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue

            for item in jobs_data:
                if not isinstance(item, dict):
                    continue
                listing = self._blob_item_to_listing(item, base_url=base_url)
                if listing:
                    listings.append(listing)

        return listings

    def _blob_item_to_listing(self, item: dict[str, Any], *, base_url: str) -> JobListing | None:
        """Convert a Workday SPA JSON job item to a JobListing."""
        title = item.get("title") or item.get("jobTitle") or item.get("name")
        if not title:
            return None

        job_url = item.get("externalUrl") or item.get("url") or base_url
        if job_url and not job_url.startswith("http"):
            job_url = urljoin(base_url, job_url)

        location = item.get("locationsText") or item.get("location") or item.get("primaryLocation")

        try:
            return JobListing(
                title=str(title),
                url=str(job_url),
                location=str(location) if location else None,
                ats_provider=ATSProvider.WORKDAY,
                raw_data=item,
            )
        except Exception:
            logger.debug("Skipping malformed Workday embedded job: %s", title)
            return None
