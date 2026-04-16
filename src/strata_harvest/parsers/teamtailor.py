"""Teamtailor ATS parser — JSON:API extraction.

Teamtailor exposes a JSON:API at ``https://api.teamtailor.com/v1/jobs``
authenticated with an API key (``X-Api-Key`` header).  The public career
page at ``{slug}.teamtailor.com`` can be detected from the URL; job data
is returned as a ``data`` array of JSON:API resource objects.

URL: {slug}.teamtailor.com  (EU + NA stacks; white-label common)
API: api.teamtailor.com/v1/jobs
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


class TeamtailorParser(BaseParser):
    """Parse job listings from Teamtailor career pages.

    Accepts the JSON:API ``/v1/jobs`` response body.  Each resource object
    in ``data[]`` contains ``attributes`` with the job details.
    """

    provider = ATSProvider.TEAMTAILOR

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Teamtailor JSON:API response into job listings."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Teamtailor parser")
            return []

        # JSON:API: top-level "data" array
        raw_jobs = data.get("data") if isinstance(data, dict) else None
        if not isinstance(raw_jobs, list):
            logger.debug("Teamtailor response missing 'data' array")
            return []

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                listing = self._parse_job(raw, url)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed Teamtailor job: %s", raw.get("id", "?"))
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Return the Teamtailor jobs API URL for the given career-page URL."""
        parsed = urlparse(url)
        if "api.teamtailor.com" in parsed.netloc:
            return url
        # Slug-based: {slug}.teamtailor.com
        slug = parsed.netloc.split(".")[0]
        return f"https://api.teamtailor.com/v1/jobs?filter[company-slug]={slug}"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        """Map one JSON:API resource object to a JobListing."""
        attrs = raw.get("attributes") or {}
        job_id = raw.get("id", "")
        title = attrs.get("title") or attrs.get("name")

        if not title:
            msg = f"Teamtailor job {job_id} missing title"
            raise ValueError(msg)

        links = raw.get("links") or {}
        job_url = links.get("careersite-job-url") or links.get("self") or source_url

        location = attrs.get("location") or attrs.get("human-location")
        department = attrs.get("department") or None
        employment_type = attrs.get("employment-type") or None
        description_html = attrs.get("body") or attrs.get("pitch") or ""
        description = _strip_tags(description_html) or None
        posted_date = _parse_iso(attrs.get("created-at") or attrs.get("updated-at"))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.TEAMTAILOR,
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
