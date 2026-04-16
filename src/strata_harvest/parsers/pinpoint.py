"""Pinpoint ATS parser — public JSON feed extraction.

Pinpoint exposes a public JSON feed at
``https://{slug}.pinpointhq.com/jobs.json`` that returns a ``jobs``
array with job listing objects.

URL: {slug}.pinpointhq.com
API: {slug}.pinpointhq.com/jobs.json
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


class PinpointParser(BaseParser):
    """Parse job listings from Pinpoint career pages.

    Accepts the ``/jobs.json`` feed response.  The top-level ``jobs``
    key contains an array of job objects.
    """

    provider = ATSProvider.PINPOINT

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Pinpoint JSON feed into job listings."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Pinpoint parser")
            return []

        # Accept either {"jobs": [...]} wrapper or a bare array
        if isinstance(data, list):
            raw_jobs = data
        elif isinstance(data, dict):
            raw_jobs = data.get("jobs") or data.get("data") or []
        else:
            logger.debug("Pinpoint response has unexpected shape")
            return []

        if not isinstance(raw_jobs, list):
            return []

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                listing = self._parse_job(raw, url)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed Pinpoint job: %s", raw.get("id", "?"))
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Pinpoint career-page URL to the JSON feed endpoint."""
        parsed = urlparse(url)
        if url.endswith(".json") or "/jobs.json" in parsed.path:
            return url
        base = f"https://{parsed.netloc}"
        return f"{base}/jobs.json"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        """Map one Pinpoint job object to a JobListing."""
        job_id = raw.get("id", "")
        title = raw.get("title") or raw.get("job_title")

        if not title:
            msg = f"Pinpoint job {job_id} missing title"
            raise ValueError(msg)

        job_url = raw.get("absolute_url") or raw.get("url") or raw.get("apply_url") or source_url

        location = raw.get("location") or raw.get("office_location") or None
        department = raw.get("department") or raw.get("team") or None
        employment_type = raw.get("employment_type") or raw.get("job_type") or None
        description = _strip_tags(raw.get("description") or "") or None
        posted_date = _parse_iso(raw.get("published_at") or raw.get("created_at"))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.PINPOINT,
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
