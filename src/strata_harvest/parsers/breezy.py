"""Breezy HR ATS parser — JSON extraction.

Breezy HR exposes a public JSON endpoint at
``https://{slug}.breezy.hr/json`` that returns an array of position objects.

URL: {slug}.breezy.hr
API: {slug}.breezy.hr/json
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


class BreezyParser(BaseParser):
    """Parse job listings from Breezy HR career pages.

    Accepts the ``/json`` endpoint response — either a JSON array of
    position objects or a wrapper ``{"positions": [...]}`` dict.
    """

    provider = ATSProvider.BREEZY

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Breezy HR JSON response into job listings."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Breezy parser")
            return []

        if isinstance(data, list):
            raw_jobs = data
        elif isinstance(data, dict):
            raw_jobs = data.get("positions") or data.get("jobs") or data.get("data") or []
        else:
            logger.debug("Breezy response has unexpected shape")
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
                logger.debug("Skipping malformed Breezy position: %s", raw.get("_id", "?"))
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Breezy career-page URL to the JSON endpoint."""
        parsed = urlparse(url)
        if parsed.path.endswith("/json") or parsed.path == "/json":
            return url
        base = f"https://{parsed.netloc}"
        return f"{base}/json"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        """Map one Breezy position object to a JobListing."""
        # Breezy uses _id or id
        job_id = raw.get("_id") or raw.get("id", "")
        title = raw.get("name") or raw.get("title")

        if not title:
            msg = f"Breezy position {job_id} missing title"
            raise ValueError(msg)

        # Friendly URL: {slug}.breezy.hr/p/{position-friendly-id}
        friendly_id = raw.get("friendly_id") or raw.get("slug") or str(job_id)
        parsed = urlparse(source_url)
        base_host = f"https://{parsed.netloc}"
        job_url = raw.get("url") or f"{base_host}/p/{friendly_id}"

        location_obj = raw.get("location") or {}
        if isinstance(location_obj, dict):
            location = location_obj.get("name") or location_obj.get("city") or None
        else:
            location = str(location_obj) if location_obj else None

        department_obj = raw.get("department") or {}
        department = department_obj.get("name") if isinstance(department_obj, dict) else None

        employment_type = raw.get("type") or raw.get("employment_type") or None
        description = _strip_tags(raw.get("description") or "") or None
        posted_date = _parse_ts(raw.get("creation_date") or raw.get("updated_date"))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.BREEZY,
            raw_data=raw,
        )


def _strip_tags(html: str) -> str:
    import re

    return re.sub(r"<[^>]+>", "", html).strip()


def _parse_ts(ts: str | int | None) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        try:
            from datetime import UTC

            return datetime.fromtimestamp(ts / 1000, tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    return None
