"""SAP SuccessFactors ATS parser — OData v2 JSON extraction.

SAP SuccessFactors is white-labeled; ``career.sap.com`` is the reference
implementation.  The OData v2 endpoint at
``/odata/v2/JobRequisition?$format=json`` returns structured job data.

A hidden sitemap at ``/sitemal.xml`` (typo is intentional in SAP's platform)
lists canonical job URLs.

URL: white-labeled  (career.sap.com as reference)
API: /odata/v2/JobRequisition?$format=json
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

_ODATA_PATH = "/odata/v2/JobRequisition"
_SITEMAP_PATH = "/sitemal.xml"  # SAP's documented typo


class SAPSuccessFactorsParser(BaseParser):
    """Parse job listings from SAP SuccessFactors OData v2 API responses.

    Accepts the ``/odata/v2/JobRequisition?$format=json`` response body.
    The OData envelope wraps results under ``d.results``.
    """

    provider = ATSProvider.SAP_SUCCESSFACTORS

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse SAP SuccessFactors OData JSON into job listings."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for SAP SuccessFactors parser")
            return []

        # OData v2 envelope: {"d": {"results": [...]}}
        raw_jobs: list[dict[str, Any]] = []
        if isinstance(data, dict):
            d_block = data.get("d") or {}
            if isinstance(d_block, dict):
                raw_jobs = d_block.get("results") or []
            elif isinstance(d_block, list):
                raw_jobs = d_block
            elif "results" in data:
                raw_jobs = data["results"]

        if not isinstance(raw_jobs, list):
            logger.debug("SAP SuccessFactors response missing 'd.results' array")
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
                    "Skipping malformed SAP SuccessFactors job: %s",
                    raw.get("jobReqId", "?"),
                )
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Build the OData v2 API URL from a career-page URL.

        Appends the standard OData path to the base host if not already
        present.  Preserves existing OData URLs as-is.
        """
        parsed = urlparse(url)
        if _ODATA_PATH in parsed.path:
            if "$format=json" not in url:
                sep = "&" if "?" in url else "?"
                return f"{url}{sep}$format=json"
            return url
        base = f"{parsed.scheme}://{parsed.netloc}"
        return f"{base}{_ODATA_PATH}?$format=json"

    @staticmethod
    def sitemap_url(url: str) -> str:
        """Return the SAP SuccessFactors sitemap URL (typo ``sitemal.xml`` is intentional)."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        return f"{base}{_SITEMAP_PATH}"

    def _parse_job(self, raw: dict[str, Any], source_url: str) -> JobListing:
        """Map one SAP SuccessFactors OData job object to a JobListing."""
        job_id = raw.get("jobReqId") or raw.get("JobReqId") or raw.get("id", "")
        title = raw.get("jobTitle") or raw.get("JobTitle") or raw.get("externalJobTitle")

        if not title:
            msg = f"SAP SuccessFactors job {job_id} missing title"
            raise ValueError(msg)

        # Construct apply URL; format varies by instance
        parsed = urlparse(source_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        job_url = raw.get("applyUrl") or raw.get("jobUrl") or f"{base}/job/{job_id}"

        location = raw.get("location") or raw.get("primaryLocation") or None
        department = raw.get("department") or raw.get("Division") or None
        employment_type = raw.get("employmentType") or raw.get("EmploymentType") or None
        description = _strip_tags(
            raw.get("jobDescription") or raw.get("extJobDesc") or ""
        ) or None
        posted_date = _parse_odata_date(
            raw.get("postingDate") or raw.get("createdDateTime")
        )

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.SAP_SUCCESSFACTORS,
            raw_data=raw,
        )


def _strip_tags(html: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", html).strip()


def _parse_odata_date(ts: str | None) -> datetime | None:
    """Parse OData date formats: ISO string or /Date(ms)/ ticks."""
    if not ts or not isinstance(ts, str):
        return None
    # OData /Date(1234567890000)/ format
    import re
    ticks_match = re.match(r"/Date\((\d+)(?:[+-]\d+)?\)/", ts)
    if ticks_match:
        try:
            from datetime import UTC
            return datetime.fromtimestamp(int(ticks_match.group(1)) / 1000, tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    # ISO 8601
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
