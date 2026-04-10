"""SmartRecruiters ATS parser — REST API extraction.

SmartRecruiters exposes a public Job Board API at
``https://api.smartrecruiters.com/v1/companies/{board}/postings``
that returns structured JSON with job listings.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser
from strata_harvest.utils.http import safe_fetch

logger = logging.getLogger(__name__)

API_BASE = "https://api.smartrecruiters.com/v1/companies"


class SmartRecruitersParser(BaseParser):
    """Parse job listings from SmartRecruiters career pages.

    SmartRecruiters exposes a public JSON API. This parser converts the API response
    into a list of `JobListing` objects.
    """

    provider = ATSProvider.SMARTRECRUITERS

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse SmartRecruiters JSON API response into job listings.

        *content* is the raw JSON string from the `/postings` API.
        """
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for SmartRecruiters parser")
            return []

        if not isinstance(data, dict) or "content" not in data:
            logger.debug("SmartRecruiters response missing 'content' key")
            return []

        raw_jobs: list[dict[str, Any]] = data.get("content", [])
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
                logger.debug(
                    "Skipping malformed SmartRecruiters job: %s",
                    raw.get("id", "?"),
                )
                continue

        expected = data.get("totalFound")
        if expected is not None and len(results) != expected:
            logger.info(
                "SmartRecruiters parsed %d of %d listed jobs (some may have been skipped)",
                len(results),
                expected,
            )

        return results

    @classmethod
    async def fetch_all(
        cls,
        url: str,
        *,
        client: Any = None,
    ) -> list[JobListing]:
        """Fetch all job listings from a SmartRecruiters board.

        Converts *url* to the API endpoint and makes the fetch.
        """
        api_url = cls.build_api_url(url)
        # SmartRecruiters limits to 100 per page, but we will fetch the default or just
        # one page for now unless we paginate. Request an explicit limit=100.
        if "?" not in api_url:
            api_url += "?limit=100"

        result = await safe_fetch(api_url, client=client)

        if not result.ok or not result.content:
            if result.error:
                logger.warning("SmartRecruiters fetch error: %s", result.error)
            return []

        parser = cls()
        jobs = parser.parse(result.content, url=url)
        return jobs

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a SmartRecruiters career-page URL to the postings API endpoint.

        Handles:
        - ``https://jobs.smartrecruiters.com/{board}`` → API endpoint
        - ``https://api.smartrecruiters.com/v1/companies/{board}/postings``
        """
        parsed = urlparse(url)

        if "api.smartrecruiters.com" in parsed.netloc:
            return url

        # Typical career page: jobs.smartrecruiters.com/{board}
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        board = path_parts[0] if path_parts else parsed.netloc.split(".")[0]

        return f"{API_BASE}/{board}/postings"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        job_id = raw.get("id")
        title = raw.get("name")
        company = raw.get("company", {})
        identifier = company.get("identifier")

        if not title or not job_id:
            msg = f"SmartRecruiters job {job_id} missing title or id"
            raise ValueError(msg)

        if identifier:
            job_url = f"https://jobs.smartrecruiters.com/{identifier}/{job_id}"
        else:
            job_url = f"{source_url.rstrip('/')}/{job_id}"

        location_obj = raw.get("location")
        location = None
        if isinstance(location_obj, dict):
            location = location_obj.get("fullLocation") or location_obj.get("city")

        department_obj = raw.get("department")
        department = None
        if isinstance(department_obj, dict):
            department = department_obj.get("label")

        employment_obj = raw.get("typeOfEmployment")
        employment_type = None
        if isinstance(employment_obj, dict):
            employment_type = employment_obj.get("label")

        posted_date = _parse_iso_timestamp(raw.get("releasedDate"))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            # Description is not present in the list endpoint
            posted_date=posted_date,
            ats_provider=ATSProvider.SMARTRECRUITERS,
            raw_data=raw,
        )


def _parse_iso_timestamp(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Some are "YYYY-MM-DDTHH:MM:SS.mmmZ"
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None
