"""trafilatura-based content extraction for web pages.

Converts HTML to clean Markdown suitable for LLM processing.
Strips navigation, footers, ads, scripts, and styles automatically.

Requires: pip install strata-harvest[extract]
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    import trafilatura
    from trafilatura.settings import Extractor

    _TRAFILATURA_AVAILABLE = True
except ImportError:
    trafilatura = None  # type: ignore[assignment]
    Extractor = None  # type: ignore[assignment,misc]
    _TRAFILATURA_AVAILABLE = False


def extract_markdown(
    html: str,
    *,
    url: str = "",
    include_comments: bool = False,
) -> str | None:
    """Extract clean Markdown from HTML using trafilatura.

    Automatically removes:
    - Navigation bars and menus
    - Footers and sidebars
    - Ads and tracking scripts
    - Boilerplate text
    - Form inputs and buttons
    - Copyright notices

    Parameters
    ----------
    html:
        Raw HTML page content.
    url:
        Page URL (helps with relative link resolution).
    include_comments:
        Whether to preserve HTML comments in output.

    Returns
    -------
    Optional[str]
        Clean Markdown, or None if extraction failed/html is empty.
        Never raises — always returns None on errors.

    Examples
    --------
    >>> md = extract_markdown(html, url="https://example.com/careers")
    >>> if md:
    ...     print(f"Extracted {len(md)} characters")
    """
    if not html or not html.strip():
        return None

    if not _TRAFILATURA_AVAILABLE:
        logger.warning("trafilatura not installed: pip install strata-harvest[extract]")
        return None

    try:
        # Extract to Markdown with structural preservation
        result = trafilatura.extract(
            html,
            output_format="markdown",
            include_comments=include_comments,
            favor_precision=True,  # stricter boilerplate removal
        )
        return result if result and result.strip() else None
    except Exception as exc:
        logger.debug("trafilatura extraction failed: %s", exc)
        return None


def extract_json_ld(html: str) -> list[dict[str, object]]:
    """Extract JSON-LD structured data from HTML.

    Minimal parser for JSON-LD blocks in <script type="application/ld+json">.
    Used when trafilatura extraction is unavailable or as a fast path.

    Parameters
    ----------
    html:
        Raw HTML page content.

    Returns
    -------
    list[dict]
        All JSON-LD objects found (may be empty).
        Never raises.

    Examples
    --------
    >>> postings = extract_json_ld(html)
    >>> for p in postings:
    ...     if p.get("@type") == "JobPosting":
    ...         print(p.get("title"))
    """
    import json
    import re

    if not html:
        return []

    pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    results: list[dict[str, object]] = []

    for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        try:
            obj = json.loads(match.group(1))
            if isinstance(obj, list):
                results.extend(o for o in obj if isinstance(o, dict))
            elif isinstance(obj, dict):
                results.append(obj)
        except (json.JSONDecodeError, ValueError):
            continue

    return results
