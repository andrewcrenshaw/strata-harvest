"""Exa AI-powered career page URL discovery for stale-URL recovery.

When strata-harvest ATS detection returns UNKNOWN for a stale career page URL,
this module queries Exa's semantic search to find the current careers page for a
given company, scoring results by ATS signal strength to pick the best candidate.

Usage (PCC-1807)::

    from strata_harvest.discovery.exa_discovery import find_career_page

    url = await find_career_page("Acme Corp", exa_api_key="exa-...")
    # Returns "https://jobs.ashbyhq.com/acme" or None

Requirements:
    Install the optional ``exa`` extra::

        pip install strata-harvest[exa]
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ATS domain scoring — higher score = stronger signal this is an ATS-hosted
# careers page (preferred over generic company sites).
# ---------------------------------------------------------------------------

_ATS_DOMAIN_SCORES: dict[re.Pattern[str], float] = {
    re.compile(r"boards\.greenhouse\.io|greenhouse\.io/embed", re.I): 1.0,
    re.compile(r"jobs\.lever\.co", re.I): 1.0,
    re.compile(r"jobs\.ashbyhq\.com|ashby\.io", re.I): 1.0,
    re.compile(r"jobs\.smartrecruiters\.com", re.I): 1.0,
    re.compile(r"ats\.rippling\.com", re.I): 0.95,
    re.compile(r"\.myworkdayjobs\.com|workday\.com", re.I): 0.90,
    re.compile(r"\.workable\.com", re.I): 0.90,
    re.compile(r"\.bamboohr\.com/careers", re.I): 0.90,
    re.compile(r"jobs\.personio\.de", re.I): 0.90,
    re.compile(r"jobs\.jobvite\.com", re.I): 0.90,
    re.compile(r"\.icims\.com", re.I): 0.85,
    re.compile(r"/careers|/jobs", re.I): 0.50,  # generic career-page signal
}

# Below this threshold, no result is returned (avoids false positives).
_MIN_CONFIDENCE: float = 0.50

# Maximum number of Exa results to score.
_TOP_N: int = 3


def _score_url(url: str) -> float:
    """Return an ATS confidence score for a candidate URL.

    Evaluates the URL against known ATS domain patterns, returning the highest
    matching score.  Falls back to 0.0 for unrecognised URLs.
    """
    best = 0.0
    for pattern, score in _ATS_DOMAIN_SCORES.items():
        if pattern.search(url) and score > best:
            best = score
    return best


async def find_career_page(
    company_name: str,
    *,
    exa_api_key: str,
    num_results: int = _TOP_N,
    min_confidence: float = _MIN_CONFIDENCE,
) -> str | None:
    """Search Exa for the current careers page URL of *company_name*.

    Performs a semantic search for "{company_name} official careers jobs page",
    scores the top *num_results* by ATS signal strength (prefers known ATS
    domains like Greenhouse, Lever, Ashby, etc.), and returns the
    highest-confidence URL.  Returns ``None`` when no result meets
    *min_confidence* or when Exa is unreachable.

    Parameters
    ----------
    company_name:
        Human-readable company name (e.g. ``"Stripe"``).
    exa_api_key:
        Exa API key (``EXA_API_KEY``).  Required — not optional at call site.
    num_results:
        How many Exa results to retrieve and score (default: 3).
    min_confidence:
        Minimum ATS confidence threshold to accept a result (default: 0.50).

    Returns
    -------
    str | None
        Highest-confidence career page URL, or ``None``.

    Raises
    ------
    ImportError
        When ``exa-py`` is not installed (install ``strata-harvest[exa]``).

    Examples
    --------
    >>> import asyncio
    >>> async def main() -> None:
    ...     url = await find_career_page("Stripe", exa_api_key="exa-test")
    ...     # Returns URL or None
    >>> asyncio.run(main())  # doctest: +SKIP
    """
    try:
        from exa_py import Exa
    except ImportError as exc:
        raise ImportError(
            "exa-py is required for career page discovery. "
            "Install it with: pip install strata-harvest[exa]"
        ) from exc

    query = f"{company_name} official careers jobs page"
    logger.debug("exa_discovery: searching for company=%r query=%r", company_name, query)

    try:
        client = Exa(api_key=exa_api_key)
        # Use neural (semantic) search to find the most relevant career pages.
        response: Any = client.search(
            query,
            num_results=num_results,
            use_autoprompt=False,
            type="neural",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "exa_discovery: Exa search failed for company=%r: %s",
            company_name,
            exc,
        )
        return None

    results: list[Any] = getattr(response, "results", []) or []
    if not results:
        logger.debug("exa_discovery: no results returned for company=%r", company_name)
        return None

    # Score each result URL and pick the best one above threshold.
    best_url: str | None = None
    best_score: float = 0.0

    for result in results[:num_results]:
        url: str = getattr(result, "url", "") or ""
        if not url:
            continue
        score = _score_url(url)
        logger.debug(
            "exa_discovery: candidate url=%r score=%.2f company=%r",
            url,
            score,
            company_name,
        )
        if score > best_score:
            best_score = score
            best_url = url

    if best_url is None or best_score < min_confidence:
        logger.debug(
            "exa_discovery: no result met threshold %.2f for company=%r (best=%.2f, url=%r)",
            min_confidence,
            company_name,
            best_score,
            best_url,
        )
        return None

    logger.info(
        "exa_discovery: selected url=%r score=%.2f for company=%r",
        best_url,
        best_score,
        company_name,
    )
    return best_url
