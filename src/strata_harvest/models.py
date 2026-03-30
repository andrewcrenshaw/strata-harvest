"""Public data models for strata-harvest."""

from __future__ import annotations

from datetime import datetime  # noqa: TCH003 — Pydantic needs this at runtime
from enum import StrEnum

from pydantic import BaseModel, Field, HttpUrl


class ATSProvider(StrEnum):
    """Known ATS providers."""

    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    WORKDAY = "workday"
    ICIMS = "icims"
    UNKNOWN = "unknown"


class JobListing(BaseModel):
    """A single parsed job listing."""

    title: str
    url: HttpUrl
    location: str | None = None
    department: str | None = None
    description: str | None = None
    requirements: list[str] = Field(default_factory=list)
    salary_range: str | None = None
    employment_type: str | None = None
    external_id: str | None = None
    posted_at: datetime | None = None
    raw_data: dict | None = Field(default=None, exclude=True)


class FetchResult(BaseModel):
    """Result of an HTTP fetch — never raises, always returns structured data."""

    url: str
    status_code: int | None = None
    content: str | None = None
    content_type: str | None = None
    error: str | None = None
    elapsed_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 400


class ScrapeResult(BaseModel):
    """Result of scraping a single career page."""

    url: str
    provider: ATSProvider = ATSProvider.UNKNOWN
    listings: list[JobListing] = Field(default_factory=list)
    content_hash: str | None = None
    error: str | None = None
    elapsed_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return self.error is None and len(self.listings) > 0
