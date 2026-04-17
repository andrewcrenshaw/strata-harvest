"""Rippling ATS parser — structured data + Next.js __NEXT_DATA__ extraction.

Rippling's career pages embed all job listings in the server-rendered
``__NEXT_DATA__`` JSON block, so no browser execution or LLM is needed.

Extraction strategy:
1. Structured data pass (extruct / regex): extracts any JSON-LD JobPosting
   blocks that may appear for SEO (e.g. individual job detail pages).
2. ``__NEXT_DATA__`` JSON parse: the canonical Rippling-specific payload
   shape (``pageProps.jobs.items``) for listing pages.

Supported URL patterns:
- ``https://www.rippling.com/careers/open-roles``  (Rippling's own careers page)
- ``https://ats.rippling.com/{slug}/jobs``          (companies using Rippling ATS)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers._structured_data import extract_structured_data, salary_to_string
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

_NEXT_DATA_PATTERN = re.compile(
    r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


class RipplingParser(BaseParser):
    """Parse job listings from Rippling career pages via __NEXT_DATA__ JSON.

    Handles both Rippling's own careers page (www.rippling.com/careers/open-roles)
    and standard Rippling ATS pages (ats.rippling.com/{slug}/jobs).

    The full job list is embedded in the server-rendered HTML — no JavaScript
    execution, no LLM, and no pagination required.
    """

    provider = ATSProvider.RIPPLING

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Extract job listings from Rippling page HTML.

        Strategy
        --------
        1. Structured data pass: run extruct (or regex fallback) to extract
           any JSON-LD ``JobPosting`` blocks.  Individual Rippling job-detail
           pages may embed these for SEO.  Returns early if any are found.
        2. ``__NEXT_DATA__`` JSON parse: the canonical Rippling ATS payload
           (``pageProps.jobs.items``) for listing pages.

        Returns an empty list when neither source yields data.
        """
        if not content:
            return []

        # Pass 1: structured data (extruct / regex fallback)
        structured = extract_structured_data(content, base_url=url)
        if structured.job_postings:
            results: list[JobListing] = []
            for item in structured.job_postings:
                listing = self._json_ld_to_listing(item, base_url=url)
                if listing:
                    results.append(listing)
            if results:
                logger.debug(
                    "RipplingParser: extracted %d JSON-LD listings from %s",
                    len(results),
                    url,
                )
                return results

        # Pass 2: __NEXT_DATA__ (Rippling-specific payload shape)
        match = _NEXT_DATA_PATTERN.search(content)
        if not match:
            logger.debug("RipplingParser: no __NEXT_DATA__ found in page at %s", url)
            return []

        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, TypeError):
            logger.debug("RipplingParser: failed to parse __NEXT_DATA__ JSON at %s", url)
            return []

        page_props = data.get("props", {}).get("pageProps", {})
        jobs_blob = page_props.get("jobs")
        if not jobs_blob:
            logger.debug("RipplingParser: no 'jobs' key in pageProps at %s", url)
            return []

        # Standard Rippling ATS shape: {"items": [...]}
        if isinstance(jobs_blob, dict):
            items = jobs_blob.get("items", [])
        elif isinstance(jobs_blob, list):
            items = jobs_blob
        else:
            logger.debug("RipplingParser: unexpected jobs type %s at %s", type(jobs_blob), url)
            return []

        next_data_results: list[JobListing] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                listing = self._parse_item(item)
                next_data_results.append(listing)
            except Exception:
                logger.debug("RipplingParser: skipping malformed item id=%s", item.get("id", "?"))

        logger.debug("RipplingParser: extracted %d listings from %s", len(next_data_results), url)
        return next_data_results

    def _json_ld_to_listing(self, item: dict[str, Any], *, base_url: str) -> JobListing | None:
        """Convert a JSON-LD JobPosting dict to a Rippling JobListing."""
        from urllib.parse import urljoin

        title = item.get("title") or item.get("name")
        if not title:
            return None

        job_url = item.get("url") or item.get("sameAs") or base_url
        if job_url and not str(job_url).startswith("http"):
            job_url = urljoin(base_url, str(job_url))

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

        try:
            return JobListing(
                title=str(title),
                url=str(job_url),
                location=location,
                employment_type=str(item["employmentType"]) if item.get("employmentType") else None,
                salary_range=salary_to_string(item.get("baseSalary")),
                ats_provider=ATSProvider.RIPPLING,
                raw_data=item,
            )
        except Exception:
            logger.debug("RipplingParser: skipping malformed JSON-LD job: %s", title)
            return None

    def _parse_item(self, item: dict[str, Any]) -> JobListing:
        title = item.get("name") or item.get("title")
        if not title:
            msg = "Rippling item missing title"
            raise ValueError(msg)

        job_url = item.get("url") or ""
        if not job_url:
            item_id = item.get("id", "")
            job_url = f"https://ats.rippling.com/unknown/jobs/{item_id}"

        department = item.get("department", {})
        dept_name = department.get("name") if isinstance(department, dict) else None

        location = self._build_location(item.get("locations", []))

        # Extract employment_type from Rippling-specific fields when present.
        employment_type = item.get("employmentType") or item.get("jobType") or None

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=dept_name,
            employment_type=str(employment_type) if employment_type else None,
            ats_provider=ATSProvider.RIPPLING,
            raw_data=item,
        )

    @staticmethod
    def _build_location(locations: list[Any]) -> str | None:
        """Combine first location name and workplace type into a display string."""
        if not locations or not isinstance(locations, list):
            return None
        first = locations[0]
        if not isinstance(first, dict):
            return None

        name = first.get("name") or first.get("city") or ""
        workplace = str(first.get("workplaceType", "")).upper()

        if workplace == "REMOTE":
            return f"{name} (Remote)" if name else "Remote"
        return name or None
