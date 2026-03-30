"""ATS provider detection via URL patterns and DOM probing.

Two composable building blocks:
  detect_from_url(url)   — fast, no network, regex on the URL string
  detect_from_dom(html)  — scan fetched HTML for ATS-specific markers

Orchestrator:
  detect_ats(url, *, html=None)  — tries URL first, falls back to DOM
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from strata_harvest.models import ATSInfo, ATSProvider
from strata_harvest.utils.http import safe_fetch

_URL_PATTERNS: list[tuple[re.Pattern[str], ATSProvider, str | None]] = [
    (
        re.compile(r"boards\.greenhouse\.io|greenhouse\.io/embed"),
        ATSProvider.GREENHOUSE,
        "https://boards-api.greenhouse.io/v1/boards/{board}/jobs",
    ),
    (re.compile(r"jobs\.lever\.co"), ATSProvider.LEVER, None),
    (re.compile(r"jobs\.ashbyhq\.com|ashby\.io"), ATSProvider.ASHBY, None),
    (re.compile(r"\.myworkdayjobs\.com|workday\.com"), ATSProvider.WORKDAY, None),
    (re.compile(r"\.icims\.com|icims\.com"), ATSProvider.ICIMS, None),
]

_DOM_SIGNATURES: list[tuple[re.Pattern[str], ATSProvider, float]] = [
    (re.compile(r"boards\.greenhouse\.io|id=['\"]greenhouse", re.I), ATSProvider.GREENHOUSE, 0.85),
    (re.compile(r"lever-jobs-container|jobs\.lever\.co", re.I), ATSProvider.LEVER, 0.85),
    (re.compile(r"ashby-job-posting|ashbyhq\.com", re.I), ATSProvider.ASHBY, 0.80),
    (re.compile(r"myworkdayjobs\.com|workday", re.I), ATSProvider.WORKDAY, 0.70),
    (re.compile(r"icims\.com|class=['\"]iCIMS", re.I), ATSProvider.ICIMS, 0.75),
]


def _extract_api_url(url: str, provider: ATSProvider, template: str | None) -> str | None:
    """Build an API URL from the template by extracting the board slug."""
    if template is None:
        return None
    parsed = urlparse(url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    board = parts[0] if parts else parsed.netloc.split(".")[0]
    return template.format(board=board)


def detect_from_url(url: str) -> ATSInfo:
    """Detect ATS from URL patterns alone. No network call."""
    for pattern, provider, api_template in _URL_PATTERNS:
        if pattern.search(url):
            return ATSInfo(
                provider=provider,
                confidence=0.9,
                api_url=_extract_api_url(url, provider, api_template),
                detection_method="url_pattern",
            )
    return ATSInfo()


def detect_from_dom(html: str) -> ATSInfo:
    """Detect ATS by scanning HTML content for known markers."""
    best: ATSInfo = ATSInfo()
    for pattern, provider, confidence in _DOM_SIGNATURES:
        if pattern.search(html):
            if confidence > best.confidence:
                best = ATSInfo(
                    provider=provider,
                    confidence=confidence,
                    detection_method="dom_probe",
                )
    return best


async def detect_ats(
    url: str,
    *,
    html: str | None = None,
    timeout: float = 15.0,
    user_agent: str | None = None,
) -> ATSInfo:
    """Detect which ATS provider a career page uses.

    Tries URL pattern matching first (instant, no network).
    Falls back to DOM probing on the HTML content.
    Pass *html* to skip the fetch when you already have page content.
    """
    url_result = detect_from_url(url)
    if url_result.provider != ATSProvider.UNKNOWN:
        return url_result

    if html is None:
        fetch_headers = {"User-Agent": user_agent} if user_agent else None
        result = await safe_fetch(url, timeout=timeout, headers=fetch_headers)
        if not result.ok or not result.content:
            return ATSInfo()
        html = result.content

    return detect_from_dom(html)
