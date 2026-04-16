"""Shared structured-data extraction helper for ATS parsers.

Uses extruct (Zyte) when available for robust JSON-LD / microdata / OpenGraph
extraction from HTML.  Falls back to a lightweight regex + ``json.loads`` path
so parsers work in environments where the ``[extract]`` extra is not installed.

Usage
-----
>>> from strata_harvest.parsers._structured_data import extract_structured_data
>>> data = extract_structured_data(html, base_url="https://example.com/jobs")
>>> for posting in data.job_postings:
...     print(posting["title"])
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

try:
    import extruct as _extruct

    _EXTRUCT_AVAILABLE = True
except ImportError:
    _EXTRUCT_AVAILABLE = False

_JSON_LD_SCRIPT_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


@dataclass
class StructuredData:
    """Structured data extracted from an HTML page.

    Attributes
    ----------
    json_ld:
        All JSON-LD objects found on the page (any ``@type``).
    job_postings:
        Subset of ``json_ld`` where ``@type == "JobPosting"``.
    microdata:
        Microdata items extracted by extruct (empty when extruct is absent).
    opengraph:
        First OpenGraph metadata dict, or empty dict.
    """

    json_ld: list[dict[str, Any]] = field(default_factory=list)
    job_postings: list[dict[str, Any]] = field(default_factory=list)
    microdata: list[dict[str, Any]] = field(default_factory=list)
    opengraph: dict[str, Any] = field(default_factory=dict)


def extract_structured_data(html: str, *, base_url: str = "") -> StructuredData:
    """Extract structured data from *html* using extruct or regex fallback.

    Parameters
    ----------
    html:
        Raw HTML page content.
    base_url:
        Passed to extruct for resolving relative URLs in structured data.

    Returns
    -------
    StructuredData
        Never raises — returns an empty :class:`StructuredData` on failures.
    """
    if not html:
        return StructuredData()

    if _EXTRUCT_AVAILABLE:
        return _extract_with_extruct(html, base_url=base_url)
    return _extract_with_regex(html)


def _extract_with_extruct(html: str, *, base_url: str) -> StructuredData:
    """Use extruct for robust multi-syntax extraction."""
    try:
        raw = _extruct.extract(
            html,
            base_url=base_url or "",
            syntaxes=["json-ld", "microdata", "opengraph"],
            uniform=True,
        )
    except Exception as exc:
        logger.debug("extruct extraction failed: %s — falling back to regex", exc)
        return _extract_with_regex(html)

    json_ld: list[dict[str, Any]] = [
        item for item in (raw.get("json-ld") or []) if isinstance(item, dict)
    ]
    job_postings = [item for item in json_ld if item.get("@type") == "JobPosting"]
    microdata: list[dict[str, Any]] = [
        item for item in (raw.get("microdata") or []) if isinstance(item, dict)
    ]
    og_list = raw.get("opengraph") or []
    opengraph: dict[str, Any] = og_list[0] if og_list else {}

    return StructuredData(
        json_ld=json_ld,
        job_postings=job_postings,
        microdata=microdata,
        opengraph=opengraph,
    )


def _extract_with_regex(html: str) -> StructuredData:
    """Regex fallback: extract JSON-LD from ``<script type="application/ld+json">`` tags."""
    json_ld: list[dict[str, Any]] = []

    for match in _JSON_LD_SCRIPT_RE.finditer(html):
        try:
            obj = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, list):
            json_ld.extend(item for item in obj if isinstance(item, dict))
        elif isinstance(obj, dict):
            json_ld.append(obj)

    job_postings = [item for item in json_ld if item.get("@type") == "JobPosting"]
    return StructuredData(json_ld=json_ld, job_postings=job_postings)


def salary_to_string(base_salary: Any) -> str | None:
    """Convert a JSON-LD ``baseSalary`` object to a human-readable string.

    Handles both ``MonetaryAmount`` (ranged) and bare numeric values.

    Examples
    --------
    >>> salary_to_string({"@type": "MonetaryAmount", "currency": "USD",
    ...     "value": {"minValue": 120000, "maxValue": 160000, "unitText": "YEAR"}})
    '$120,000\u2013$160,000/year'
    >>> salary_to_string({"@type": "MonetaryAmount", "currency": "USD",
    ...     "value": {"value": 80000, "unitText": "YEAR"}})
    '$80,000/year'
    >>> salary_to_string(None)
    """
    if not base_salary or not isinstance(base_salary, dict):
        return None

    currency = base_salary.get("currency", "")
    symbol = "$" if currency == "USD" else (currency + " " if currency else "")
    value = base_salary.get("value")

    if isinstance(value, (int, float)):
        return f"{symbol}{value:,.0f}"

    if isinstance(value, dict):
        unit = str(value.get("unitText", "")).lower()
        unit_label = f"/{unit}" if unit else ""
        min_v = value.get("minValue")
        max_v = value.get("maxValue")
        single = value.get("value")
        if min_v is not None and max_v is not None:
            return f"{symbol}{min_v:,.0f}\u2013{symbol}{max_v:,.0f}{unit_label}"
        if single is not None:
            return f"{symbol}{single:,.0f}{unit_label}"

    return None
