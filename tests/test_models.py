"""Tests for strata_harvest data models."""

import pytest

from strata_harvest.models import ATSProvider, FetchResult, JobListing, ScrapeResult


@pytest.mark.verification
class TestJobListing:
    def test_minimal_listing(self) -> None:
        listing = JobListing(title="Engineer", url="https://example.com/jobs/1")
        assert listing.title == "Engineer"
        assert str(listing.url) == "https://example.com/jobs/1"
        assert listing.location is None
        assert listing.requirements == []

    def test_full_listing(self) -> None:
        listing = JobListing(
            title="Senior Engineer",
            url="https://example.com/jobs/2",
            location="Remote",
            department="Engineering",
            description="Build things.",
            requirements=["Python", "FastAPI"],
            salary_range="$150k-$200k",
            employment_type="Full-time",
            external_id="job-123",
        )
        assert listing.department == "Engineering"
        assert len(listing.requirements) == 2


@pytest.mark.verification
class TestFetchResult:
    def test_ok_result(self) -> None:
        result = FetchResult(url="https://example.com", status_code=200, content="<html>")
        assert result.ok is True
        assert result.error is None

    def test_error_result(self) -> None:
        result = FetchResult(url="https://example.com", error="Connection refused")
        assert result.ok is False

    def test_http_error_status(self) -> None:
        result = FetchResult(url="https://example.com", status_code=500)
        assert result.ok is False


@pytest.mark.verification
class TestScrapeResult:
    def test_ok_with_listings(self) -> None:
        listing = JobListing(title="Engineer", url="https://example.com/jobs/1")
        result = ScrapeResult(url="https://example.com", listings=[listing])
        assert result.ok is True

    def test_empty_listings(self) -> None:
        result = ScrapeResult(url="https://example.com")
        assert result.ok is False

    def test_error_result(self) -> None:
        result = ScrapeResult(url="https://example.com", error="Timeout")
        assert result.ok is False

    def test_provider_default(self) -> None:
        result = ScrapeResult(url="https://example.com")
        assert result.provider == ATSProvider.UNKNOWN
