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
    (
        re.compile(r"jobs\.lever\.co"),
        ATSProvider.LEVER,
        "https://api.lever.co/v0/postings/{board}?mode=json",
    ),
    (re.compile(r"jobs\.ashbyhq\.com|ashby\.io"), ATSProvider.ASHBY, None),
    (re.compile(r"\.myworkdayjobs\.com|workday\.com"), ATSProvider.WORKDAY, None),
    (re.compile(r"\.icims\.com|icims\.com"), ATSProvider.ICIMS, None),
    (re.compile(r"ats\.rippling\.com", re.I), ATSProvider.RIPPLING, None),
    (re.compile(r"\.workable\.com", re.I), ATSProvider.WORKABLE, None),
    (re.compile(r"\.bamboohr\.com/careers", re.I), ATSProvider.BAMBOOHR, None),
    (
        re.compile(r"jobs\.smartrecruiters\.com", re.I),
        ATSProvider.SMARTRECRUITERS,
        "https://api.smartrecruiters.com/v1/companies/{board}/postings",
    ),
    (re.compile(r"jobs\.personio\.de", re.I), ATSProvider.PERSONIO, None),
    (re.compile(r"jobs\.jobvite\.com", re.I), ATSProvider.JOBVITE, None),
]

_DOM_SIGNATURES: list[tuple[re.Pattern[str], ATSProvider, float]] = [
    (re.compile(r"boards\.greenhouse\.io|id=['\"]greenhouse", re.I), ATSProvider.GREENHOUSE, 0.85),
    (re.compile(r"lever-jobs-container|jobs\.lever\.co", re.I), ATSProvider.LEVER, 0.85),
    (re.compile(r"ashby-job-posting|ashbyhq\.com", re.I), ATSProvider.ASHBY, 0.80),
    (re.compile(r"myworkdayjobs\.com|workday", re.I), ATSProvider.WORKDAY, 0.70),
    (re.compile(r"icims\.com|class=['\"]iCIMS", re.I), ATSProvider.ICIMS, 0.75),
    (re.compile(r"ats\.rippling\.com|rippling-ats", re.I), ATSProvider.RIPPLING, 0.85),
    (re.compile(r"\.workable\.com|workable-board", re.I), ATSProvider.WORKABLE, 0.85),
    (re.compile(r"bamboohr\.com/careers|bamboohr-app", re.I), ATSProvider.BAMBOOHR, 0.85),
    (
        re.compile(r"smartrecruiters\.com|smartrecruiters-app", re.I),
        ATSProvider.SMARTRECRUITERS,
        0.85,
    ),
    (re.compile(r"personio\.de|personio\.com|personio-", re.I), ATSProvider.PERSONIO, 0.85),
    (re.compile(r"jobvite\.com|jobvite-", re.I), ATSProvider.JOBVITE, 0.85),
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
    """Infer ATS from the URL string only (no I/O).

    Parameters
    ----------
    url:
        Career page or board URL to inspect.

    Returns
    -------
    ATSInfo
        High-confidence match when a known host pattern applies; otherwise
        defaults with :attr:`~ATSInfo.provider` ``UNKNOWN``.

    Examples
    --------
    >>> info = detect_from_url("https://boards.greenhouse.io/acme/jobs")
    >>> info.provider.value
    'greenhouse'
    """
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
    """Infer ATS by scanning raw HTML for vendor-specific markers.

    Parameters
    ----------
    html:
        Full page HTML (or a large fragment) to scan.

    Returns
    -------
    ATSInfo
        Best matching signature by confidence; unknown if nothing matched.

    Examples
    --------
    >>> html = '<script src="https://boards.greenhouse.io/embed/job_board"></script>'
    >>> detect_from_dom(html).provider.value
    'greenhouse'
    """
    best: ATSInfo = ATSInfo()
    for pattern, provider, confidence in _DOM_SIGNATURES:
        if pattern.search(html) and confidence > best.confidence:
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
    allow_private: bool = False,
) -> ATSInfo:
    """Detect which ATS powers a career page.

    Order of operations:

    #. :func:`detect_from_url` — instant, no network.
    #. If still unknown and *html* is omitted, :func:`~strata_harvest.utils.http.safe_fetch`.
    #. :func:`detect_from_dom` on the HTML body.

    Parameters
    ----------
    url:
        Page URL (used for URL heuristics and, when needed, fetching).
    html:
        Optional pre-fetched HTML to avoid a network round trip.
    timeout:
        HTTP timeout when this function must fetch the page.
    user_agent:
        Optional ``User-Agent`` header for the internal fetch.
    allow_private:
        Passed to :func:`~strata_harvest.utils.http.safe_fetch` when a fetch is required.

    Returns
    -------
    ATSInfo
        Best-effort detection; may be ``UNKNOWN`` with low confidence.

    Notes
    -----
    Transport failures surface as empty :class:`~strata_harvest.models.ATSInfo`
    results rather than raised exceptions.

    Examples
    --------
    >>> import asyncio
    >>> async def main() -> None:
    ...     info = await detect_ats("https://jobs.lever.co/example")
    ...     assert info.provider.value == "lever"
    >>> asyncio.run(main())  # doctest: +SKIP
    """
    url_result = detect_from_url(url)
    if url_result.provider != ATSProvider.UNKNOWN:
        return url_result

    if html is None:
        fetch_headers = {"User-Agent": user_agent} if user_agent else None
        result = await safe_fetch(
            url,
            timeout=timeout,
            headers=fetch_headers,
            allow_private=allow_private,
        )
        if not result.ok or not result.content:
            return ATSInfo()
        html = result.content

    return detect_from_dom(html)
