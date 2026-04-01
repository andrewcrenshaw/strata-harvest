"""Exercised by mypy via test_packaging (PCC-1612); not collected by pytest."""

from __future__ import annotations

from strata_harvest import JobListing


def _sample() -> JobListing:
    return JobListing(title="Engineer", url="https://jobs.example.com/1")
