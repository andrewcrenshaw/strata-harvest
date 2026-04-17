"""Pydantic schema for structured job posting extraction.

Designed for compatibility with instructor + local LLM constrained decoding.
Fields map directly to JobListing for zero-friction conversion.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 — Pydantic needs this at runtime

from pydantic import BaseModel, Field, HttpUrl


class JobPostingSchema(BaseModel):
    """Job posting extracted by LLM or structured data extraction.

    All fields are optional except title and url, which the pipeline validates.
    Maps directly to JobListing for database storage.

    Attributes
    ----------
    title:
        Job title (required for valid extraction).
    url:
        Canonical posting URL (required for valid extraction).
    company:
        Hiring company name.
    location:
        Job location (e.g., "San Francisco, CA" or "Remote").
    employment_type:
        One of: "Full-time", "Part-time", "Contract", "Internship", "Temporary", etc.
    remote_policy:
        One of: "Remote", "Hybrid", "On-site".
    description:
        Long-form job description or requirements summary.
    salary_min:
        Minimum annual salary in base currency.
    salary_max:
        Maximum annual salary in base currency.
    salary_currency:
        ISO 4217 currency code (e.g., "USD", "EUR").
    posted_at:
        ISO 8601 timestamp when posted (if available).
    apply_url:
        Direct link to application form (if distinct from canonical url).
    """

    title: str = Field(..., description="Job title (required)")
    url: HttpUrl | str = Field(..., description="Canonical job posting URL (required)")
    company: str | None = Field(None, description="Hiring company name")
    location: str | None = Field(None, description="Job location")
    employment_type: str | None = Field(
        None, description="Employment type (Full-time, Part-time, etc.)"
    )
    remote_policy: str | None = Field(None, description="Remote policy (Remote, Hybrid, On-site)")
    description: str | None = Field(None, description="Job description or requirements")
    salary_min: float | None = Field(None, description="Minimum salary")
    salary_max: float | None = Field(None, description="Maximum salary")
    salary_currency: str | None = Field(None, description="Salary currency (USD, EUR, etc.)")
    posted_at: datetime | None = Field(None, description="Posting date/time")
    apply_url: HttpUrl | str | None = Field(None, description="Direct application URL")

    model_config = {
        "json_schema_extra": {
            "example": {
                "title": "Senior Backend Engineer",
                "url": "https://company.com/jobs/senior-backend",
                "company": "Example Corp",
                "location": "San Francisco, CA",
                "employment_type": "Full-time",
                "remote_policy": "Hybrid",
                "description": "Build scalable backend services...",
                "salary_min": 150000,
                "salary_max": 200000,
                "salary_currency": "USD",
                "posted_at": "2026-04-16T00:00:00Z",
            }
        }
    }
