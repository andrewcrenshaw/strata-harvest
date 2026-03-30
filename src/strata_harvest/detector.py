"""ATS provider detection via URL patterns and DOM probing."""

from __future__ import annotations

import re

from pydantic import BaseModel

from strata_harvest.models import ATSProvider
from strata_harvest.utils.http import safe_fetch

_URL_PATTERNS: list[tuple[re.Pattern[str], ATSProvider]] = [
    (re.compile(r"boards\.greenhouse\.io|greenhouse\.io/embed"), ATSProvider.GREENHOUSE),
    (re.compile(r"jobs\.lever\.co"), ATSProvider.LEVER),
    (re.compile(r"jobs\.ashbyhq\.com"), ATSProvider.ASHBY),
    (re.compile(r"\.myworkdayjobs\.com|workday\.com"), ATSProvider.WORKDAY),
    (re.compile(r"\.icims\.com|icims\.com"), ATSProvider.ICIMS),
]

_DOM_SIGNATURES: list[tuple[str, ATSProvider]] = [
    ("greenhouse", ATSProvider.GREENHOUSE),
    ("lever-jobs-container", ATSProvider.LEVER),
    ("ashby-job-posting", ATSProvider.ASHBY),
    ("workday", ATSProvider.WORKDAY),
    ("icims", ATSProvider.ICIMS),
]


class ATSInfo(BaseModel):
    """Detected ATS provider with confidence score."""

    provider: ATSProvider = ATSProvider.UNKNOWN
    confidence: float = 0.0
    detection_method: str = "none"


async def detect_ats(
    url: str,
    *,
    timeout: float = 15.0,
    user_agent: str | None = None,
) -> ATSInfo:
    """Detect which ATS provider a career page uses."""
    for pattern, provider in _URL_PATTERNS:
        if pattern.search(url):
            return ATSInfo(provider=provider, confidence=0.9, detection_method="url_pattern")

    result = await safe_fetch(url, timeout=timeout, user_agent=user_agent)
    if not result.ok or not result.content:
        return ATSInfo()

    content_lower = result.content.lower()
    for signature, provider in _DOM_SIGNATURES:
        if signature in content_lower:
            return ATSInfo(provider=provider, confidence=0.7, detection_method="dom_probe")

    return ATSInfo()
