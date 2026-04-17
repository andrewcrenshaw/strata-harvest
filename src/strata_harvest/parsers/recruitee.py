"""Recruitee ATS parser — JSON API extraction.

Recruitee exposes a public JSON API at
``https://{slug}.recruitee.com/api/offers`` that returns an ``offers``
array with job listing objects.

URL: {slug}.recruitee.com
API: {slug}.recruitee.com/api/offers
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class RecruiteeParser(BaseParser):
    """Parse job listings from Recruitee career pages.

    Accepts the ``/api/offers`` JSON response.  The top-level ``offers``
    key contains an array of job objects.
    """

    provider = ATSProvider.RECRUITEE

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Recruitee JSON API response into job listings."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Recruitee parser")
            return []

        raw_jobs = data.get("offers") if isinstance(data, dict) else None
        if not isinstance(raw_jobs, list):
            logger.debug("Recruitee response missing 'offers' key")
            return []

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                listing = self._parse_job(raw, url)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed Recruitee offer: %s", raw.get("id", "?"))
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Recruitee career-page URL to the offers API endpoint."""
        parsed = urlparse(url)
        if "/api/offers" in parsed.path:
            return url
        # {slug}.recruitee.com → same host, /api/offers path
        base = f"https://{parsed.netloc}"
        return f"{base}/api/offers"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        """Map one Recruitee offer object to a JobListing."""
        job_id = raw.get("id", "")
        title = raw.get("title")

        if not title:
            msg = f"Recruitee offer {job_id} missing title"
            raise ValueError(msg)

        # Canonical URL: careers_url or constructed from slug
        slug = raw.get("slug", str(job_id))
        parsed = urlparse(source_url)
        base_host = f"https://{parsed.netloc}"
        job_url = raw.get("careers_url") or f"{base_host}/o/{slug}"

        location = raw.get("location") or raw.get("city") or None
        department = raw.get("department") or None
        employment_type = raw.get("employment_type_code") or raw.get("employment_type") or None
        description = _strip_tags(raw.get("description") or "") or None
        posted_date = _parse_iso(raw.get("created_at") or raw.get("published_at"))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.RECRUITEE,
            raw_data=raw,
        )


def _strip_tags(html: str) -> str:
    import re

    return re.sub(r"<[^>]+>", "", html).strip()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
