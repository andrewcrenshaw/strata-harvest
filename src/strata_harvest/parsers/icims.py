"""iCIMS ATS parser — JSON-LD and structured HTML extraction.

iCIMS career pages vary widely by client configuration, but two reliable
extraction patterns exist:

1. ``<script type="application/ld+json">`` blocks with ``JobPosting`` schema
   (iCIMS often embeds these for SEO purposes).
2. A structured ``/jobs/search`` JSON API that some iCIMS instances expose
   publicly — the parser will attempt to parse JSON responses directly.
3. HTML link extraction from ``<a>`` elements with common iCIMS path patterns.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers._structured_data import extract_structured_data, salary_to_string
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

_TAG_PATTERN = re.compile(r"<[^>]+")

# iCIMS job link pattern — /jobs/{id}/job or /iims/hrd.aspx?
_ICIMS_JOB_LINK_PATTERN = re.compile(
    r'href=["\']([^"\']*(?:/jobs/\d+/job|module=jobdetail&iis=|JobDetail)[^"\']*)["\']',
    re.IGNORECASE,
)

# iCIMS title near job links
_TITLE_TEXT_PATTERN = re.compile(r'class=["\'][^"\']*job-title[^"\']*["\'][^>]*>\s*([^<]+)')


class ICIMSParser(BaseParser):
    """Parse job listings from iCIMS career pages.

    Extracts structured data from:
    - JSON-LD ``<script type="application/ld+json">`` blocks (most reliable)
    - JSON API responses (for iCIMS API endpoints)
    - HTML link + title extraction as fallback

    Returns an empty list when none of these sources yield data — configure
    ``llm_provider`` for LLM-assisted extraction on complex pages.
    """

    provider = ATSProvider.ICIMS
    is_stub = False

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse iCIMS page content for structured job data.

        Attempts JSON-LD, then JSON API response, then HTML extraction.
        """
        if not content:
            return []

        # Strategy 1: JSON response (iCIMS API or search JSON)
        jobs = self._parse_json_response(content, base_url=url)
        if jobs:
            return jobs

        # Strategy 2: JSON-LD blocks
        jobs = self._parse_json_ld(content, base_url=url)
        if jobs:
            return jobs

        # Strategy 3: HTML link pattern extraction
        jobs = self._parse_html_links(content, base_url=url)
        return jobs

    # ------------------------------------------------------------------
    # Strategy 1: JSON API response
    # ------------------------------------------------------------------

    def _parse_json_response(self, content: str, *, base_url: str) -> list[JobListing]:
        """Attempt to parse content as a JSON API response from iCIMS."""
        content = content.strip()
        if not (content.startswith("{") or content.startswith("[")):
            return []

        try:
            data = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return []

        listings: list[JobListing] = []

        # iCIMS search API: {"searchResults": [...], "totalCount": N}
        if isinstance(data, dict):
            jobs_data = (
                data.get("searchResults")
                or data.get("jobs")
                or data.get("requisitions")
                or data.get("items")
            )
            if isinstance(jobs_data, list):
                for item in jobs_data:
                    listing = self._api_item_to_listing(item, base_url=base_url)
                    if listing:
                        listings.append(listing)

        elif isinstance(data, list):
            for item in data:
                listing = self._api_item_to_listing(item, base_url=base_url)
                if listing:
                    listings.append(listing)

        return listings

    def _api_item_to_listing(self, item: dict[str, Any], *, base_url: str) -> JobListing | None:
        """Convert an iCIMS JSON API result item to a JobListing."""
        if not isinstance(item, dict):
            return None

        title = (
            item.get("jobtitle") or item.get("title") or item.get("jobTitle") or item.get("name")
        )
        if not title:
            return None

        job_url = item.get("applyurl") or item.get("url") or item.get("detailUrl")
        if not job_url:
            job_id = item.get("id") or item.get("requisitionId")
            if job_id:
                parsed = urlparse(base_url)
                job_url = f"{parsed.scheme}://{parsed.netloc}/jobs/{job_id}/job"
            else:
                job_url = base_url

        if job_url and not job_url.startswith("http"):
            job_url = urljoin(base_url, job_url)

        location = item.get("joblocation") or item.get("location") or item.get("locationName")

        try:
            return JobListing(
                title=str(title),
                url=str(job_url),
                location=str(location) if location else None,
                ats_provider=ATSProvider.ICIMS,
                raw_data=item,
            )
        except Exception:
            logger.debug("Skipping malformed iCIMS JSON job: %s", title)
            return None

    # ------------------------------------------------------------------
    # Strategy 2: JSON-LD (via extruct or regex fallback)
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

        job_url = item.get("url") or item.get("sameAs") or base_url
        if job_url and not str(job_url).startswith("http"):
            job_url = urljoin(base_url, str(job_url))

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
                description=description,
                employment_type=str(employment_type) if employment_type else None,
                salary_range=salary_range,
                ats_provider=ATSProvider.ICIMS,
                raw_data=item,
            )
        except Exception:
            logger.debug("Skipping malformed iCIMS JSON-LD job: %s", title)
            return None

    # ------------------------------------------------------------------
    # Strategy 3: HTML link extraction
    # ------------------------------------------------------------------

    def _parse_html_links(self, html: str, *, base_url: str) -> list[JobListing]:
        """Extract job listings from iCIMS HTML link patterns."""
        listings: list[JobListing] = []
        seen_urls: set[str] = set()

        for link_match in _ICIMS_JOB_LINK_PATTERN.finditer(html):
            href = link_match.group(1)
            if not href.startswith("http"):
                href = urljoin(base_url, href)

            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Try to get title from nearby text
            start = max(0, link_match.start() - 200)
            end = min(len(html), link_match.end() + 200)
            context = html[start:end]

            title_match = re.search(
                r"(?:aria-label|title)=[\"']([^\"']+)[\"']", context, re.IGNORECASE
            )
            title = None
            if title_match:
                title = title_match.group(1).strip()

            if not title:
                # Grab the link text
                text_match = re.search(
                    r'href=["\'][^"\']+["\'][^>]*>\s*([^<]{3,80})', context, re.IGNORECASE
                )
                if text_match:
                    title = _TAG_PATTERN.sub("", text_match.group(1)).strip()

            if not title:
                continue

            try:
                listings.append(
                    JobListing(
                        title=title,
                        url=href,
                        ats_provider=ATSProvider.ICIMS,
                        raw_data={"extracted_from": "html_link"},
                    )
                )
            except Exception:
                logger.debug("Skipping malformed iCIMS HTML job link: %s", href)

        return listings
