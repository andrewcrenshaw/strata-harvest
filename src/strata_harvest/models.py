"""Public data models for strata-harvest."""

from __future__ import annotations

from datetime import datetime  # noqa: TCH003 — Pydantic needs this at runtime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class ATSProvider(StrEnum):
    """Supported ATS vendors recognized by parsers and detection.

    Use enum members when comparing :attr:`JobListing.ats_provider` or
    :attr:`ATSInfo.provider` values.

    Examples
    --------
    >>> ATSProvider.GREENHOUSE.value
    'greenhouse'
    """

    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    WORKDAY = "workday"
    ICIMS = "icims"
    UNKNOWN = "unknown"


class ATSInfo(BaseModel):
    """Detected ATS vendor and confidence for a career page.

    Populated by detection helpers (URL patterns and optional DOM probes).
    Used inside :class:`ScrapeResult` and returned by
    ``strata_harvest.detector`` functions.

    Attributes
    ----------
    provider:
        Identified ATS, or :attr:`ATSProvider.UNKNOWN` when unsure.
    confidence:
        Score in ``[0.0, 1.0]``; higher means stronger evidence.
    api_url:
        When applicable, a stable API URL for listings (provider-specific).
    detection_method:
        Short label such as ``url_pattern``, ``dom_probe``, or ``none``.

    Examples
    --------
    >>> info = ATSInfo(provider=ATSProvider.LEVER, confidence=0.85, detection_method="url_pattern")
    >>> info.provider == ATSProvider.LEVER
    True
    """

    provider: ATSProvider = ATSProvider.UNKNOWN
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    api_url: str | None = None
    detection_method: str = "none"


class JobListing(BaseModel):
    """One job posting extracted from an ATS career page.

    Fields are normalized across providers; optional attributes are ``None``
    when the source did not expose them. ``raw_data`` holds provider-specific
    payloads for debugging or extensions.

    Attributes
    ----------
    title:
        Job title as shown on the listing.
    url:
        Canonical posting URL (validated ``HttpUrl``).
    location, department, employment_type:
        Optional human-readable metadata.
    description:
        Long-form description when parsed.
    requirements:
        Bullet requirements when the parser extracted them.
    salary_range:
        Free-text compensation when available.
    posted_date:
        Posting timestamp when parseable.
    ats_provider:
        Source ATS when known.
    raw_data:
        Arbitrary provider-specific key/values.

    Examples
    --------
    >>> j = JobListing(title="Backend Engineer", url="https://jobs.example.com/1")
    >>> j.title
    'Backend Engineer'
    """

    title: str
    url: HttpUrl
    location: str | None = None
    department: str | None = None
    employment_type: str | None = None
    description: str | None = None
    requirements: list[str] = Field(default_factory=list)
    salary_range: str | None = None
    posted_date: datetime | None = None
    ats_provider: ATSProvider | None = None
    raw_data: dict[str, Any] = Field(default_factory=dict)


class FetchResult(BaseModel):
    """Result of an HTTP fetch — never raises, always returns structured data."""

    url: str
    status_code: int | None = None
    content: str | None = None
    content_type: str | None = None
    data: Any = None
    error: str | None = None
    elapsed_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 400


class ScrapeResult(BaseModel):
    """Outcome of scraping one career-page URL.

    Always inspect :attr:`error`, :attr:`fetch_ok`, and :attr:`jobs` together:
    a successful HTTP response with no matching parser rows yields an empty
    ``jobs`` list without setting ``error``, but with :attr:`fetch_ok` True.
    A hard failure sets ``error`` and has :attr:`fetch_ok` False.

    Attributes
    ----------
    url:
        The URL that was requested.
    jobs:
        Parsed :class:`JobListing` rows (possibly empty).
    content_hash:
        Stable hash of raw page bytes when fetched successfully.
    changed:
        When a prior hash was supplied to the crawler, whether content differs.
    ats_info:
        Detected ATS metadata from :mod:`strata_harvest.detector`.
    scrape_duration_ms:
        Wall time spent on the scrape path in milliseconds.
    error:
        Human-readable failure when the scrape could not complete; ``None`` on success.
    fetch_ok:
        ``True`` when the HTTP fetch itself succeeded (status 2xx/3xx), even if
        zero jobs were parsed.  ``False`` on transport errors, non-2xx responses,
        or when the scrape was aborted before any fetch.
        Use this to distinguish *silent empty parse* from *hard failure*.

    Examples
    --------
    >>> r = ScrapeResult(url="https://example.com/jobs", error="HTTP 404")
    >>> r.error
    'HTTP 404'
    >>> r.ok
    False
    >>> r.fetch_ok
    False
    >>> zero = ScrapeResult(url="https://example.com/jobs", fetch_ok=True)
    >>> zero.ok
    False
    >>> zero.fetch_ok
    True
    """

    url: str
    jobs: list[JobListing] = Field(default_factory=list)
    content_hash: str | None = None
    changed: bool = False
    ats_info: ATSInfo = Field(default_factory=ATSInfo)
    scrape_duration_ms: float = 0.0
    error: str | None = None
    fetch_ok: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None and len(self.jobs) > 0
