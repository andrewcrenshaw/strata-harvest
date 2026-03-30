"""strata-harvest: Career page scraping and ATS parsing library.

Public API (stable)
-------------------
* :func:`harvest` — one-shot scrape; returns job listings
* :func:`create_crawler` — factory for a reusable crawler with rate limiting
* :class:`JobListing` — structured fields for one posting
* :class:`ScrapeResult` — full scrape outcome (listings, ATS metadata, errors)
* :class:`ATSInfo` — detected ATS provider and confidence

Advanced use (submodules) — e.g. ``from strata_harvest.detector import detect_ats``,
``from strata_harvest.crawler import Crawler``, ``from strata_harvest.models import ATSProvider``.
"""

from __future__ import annotations

from strata_harvest.crawler import create_crawler, harvest
from strata_harvest.models import ATSInfo, JobListing, ScrapeResult

__version__ = "0.1.1"

__all__ = [
    "ATSInfo",
    "JobListing",
    "ScrapeResult",
    "__version__",
    "create_crawler",
    "harvest",
]
