"""Tests for strata_harvest data models — serialization and deserialization."""

from datetime import UTC, datetime

import pytest

from strata_harvest.models import (
    ATSInfo,
    ATSProvider,
    FetchResult,
    JobListing,
    ScrapeResult,
)


# ---------------------------------------------------------------------------
# ATSInfo
# ---------------------------------------------------------------------------
@pytest.mark.verification
class TestATSInfo:
    def test_defaults(self) -> None:
        info = ATSInfo()
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0
        assert info.api_url is None
        assert info.detection_method == "none"

    def test_full_construction(self) -> None:
        info = ATSInfo(
            provider=ATSProvider.GREENHOUSE,
            confidence=0.9,
            api_url="https://boards-api.greenhouse.io/v1/boards/acme/jobs",
            detection_method="url_pattern",
        )
        assert info.provider == ATSProvider.GREENHOUSE
        assert info.confidence == 0.9
        assert info.api_url is not None

    def test_confidence_bounds(self) -> None:
        ATSInfo(confidence=0.0)
        ATSInfo(confidence=1.0)
        with pytest.raises(ValueError):
            ATSInfo(confidence=-0.1)
        with pytest.raises(ValueError):
            ATSInfo(confidence=1.1)

    def test_round_trip_json(self) -> None:
        original = ATSInfo(
            provider=ATSProvider.LEVER,
            confidence=0.85,
            api_url="https://api.lever.co/v0/postings/company",
            detection_method="url_pattern",
        )
        data = original.model_dump_json()
        restored = ATSInfo.model_validate_json(data)
        assert restored == original

    def test_round_trip_dict(self) -> None:
        original = ATSInfo(provider=ATSProvider.ASHBY, confidence=0.7)
        data = original.model_dump()
        restored = ATSInfo.model_validate(data)
        assert restored == original


# ---------------------------------------------------------------------------
# JobListing
# ---------------------------------------------------------------------------
@pytest.mark.verification
class TestJobListing:
    def test_minimal_listing(self) -> None:
        listing = JobListing(title="Engineer", url="https://example.com/jobs/1")
        assert listing.title == "Engineer"
        assert str(listing.url) == "https://example.com/jobs/1"
        assert listing.location is None
        assert listing.requirements == []
        assert listing.raw_data == {}
        assert listing.ats_provider is None
        assert listing.posted_date is None

    def test_full_listing(self) -> None:
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        listing = JobListing(
            title="Senior Engineer",
            url="https://example.com/jobs/2",
            location="Remote",
            department="Engineering",
            description="Build things.",
            requirements=["Python", "FastAPI"],
            salary_range="$150k-$200k",
            employment_type="Full-time",
            posted_date=ts,
            ats_provider=ATSProvider.GREENHOUSE,
            raw_data={"source_id": "gh-123", "internal_code": "ENG-42"},
        )
        assert listing.department == "Engineering"
        assert len(listing.requirements) == 2
        assert listing.posted_date == ts
        assert listing.ats_provider == ATSProvider.GREENHOUSE
        assert listing.raw_data["source_id"] == "gh-123"

    def test_round_trip_json(self) -> None:
        ts = datetime(2026, 1, 20, 8, 30, 0, tzinfo=UTC)
        original = JobListing(
            title="Data Scientist",
            url="https://boards.greenhouse.io/acme/jobs/456",
            location="New York, NY",
            department="Data",
            employment_type="Full-time",
            description="Analyze data and build models.",
            requirements=["Python", "SQL", "ML"],
            salary_range="$130k-$180k",
            posted_date=ts,
            ats_provider=ATSProvider.GREENHOUSE,
            raw_data={"id": 456, "updated_at": "2026-01-20"},
        )
        json_str = original.model_dump_json()
        restored = JobListing.model_validate_json(json_str)
        assert restored.title == original.title
        assert str(restored.url) == str(original.url)
        assert restored.requirements == original.requirements
        assert restored.posted_date == original.posted_date
        assert restored.ats_provider == original.ats_provider
        assert restored.raw_data == original.raw_data

    def test_round_trip_dict(self) -> None:
        original = JobListing(
            title="DevOps",
            url="https://jobs.lever.co/company/789",
            requirements=["AWS", "Terraform"],
        )
        data = original.model_dump()
        restored = JobListing.model_validate(data)
        assert restored.title == original.title
        assert restored.requirements == original.requirements

    def test_raw_data_serialized(self) -> None:
        """raw_data must appear in serialized output (not excluded)."""
        listing = JobListing(
            title="Test",
            url="https://example.com/j/1",
            raw_data={"key": "value"},
        )
        data = listing.model_dump()
        assert "raw_data" in data
        assert data["raw_data"] == {"key": "value"}


# ---------------------------------------------------------------------------
# FetchResult
# ---------------------------------------------------------------------------
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

    def test_round_trip_json(self) -> None:
        original = FetchResult(
            url="https://example.com/api",
            status_code=200,
            content='{"jobs": []}',
            content_type="application/json",
            data={"jobs": []},
            elapsed_ms=142.5,
        )
        restored = FetchResult.model_validate_json(original.model_dump_json())
        assert restored.url == original.url
        assert restored.status_code == original.status_code
        assert restored.elapsed_ms == original.elapsed_ms


# ---------------------------------------------------------------------------
# ScrapeResult
# ---------------------------------------------------------------------------
@pytest.mark.verification
class TestScrapeResult:
    def test_ok_with_jobs(self) -> None:
        job = JobListing(title="Engineer", url="https://example.com/jobs/1")
        result = ScrapeResult(url="https://example.com", jobs=[job])
        assert result.ok is True

    def test_empty_jobs(self) -> None:
        result = ScrapeResult(url="https://example.com")
        assert result.ok is False

    def test_error_result(self) -> None:
        result = ScrapeResult(url="https://example.com", error="Timeout")
        assert result.ok is False

    def test_defaults(self) -> None:
        result = ScrapeResult(url="https://example.com")
        assert result.ats_info.provider == ATSProvider.UNKNOWN
        assert result.changed is False
        assert result.content_hash is None
        assert result.scrape_duration_ms == 0.0
        assert result.jobs == []

    def test_full_construction(self) -> None:
        jobs = [
            JobListing(title="SWE", url="https://example.com/j/1"),
            JobListing(title="PM", url="https://example.com/j/2"),
        ]
        ats = ATSInfo(provider=ATSProvider.GREENHOUSE, confidence=0.9)
        result = ScrapeResult(
            url="https://example.com/careers",
            jobs=jobs,
            content_hash="a" * 64,
            changed=True,
            ats_info=ats,
            scrape_duration_ms=1234.5,
        )
        assert len(result.jobs) == 2
        assert result.changed is True
        assert result.ats_info.provider == ATSProvider.GREENHOUSE
        assert result.scrape_duration_ms == 1234.5

    def test_round_trip_json(self) -> None:
        ts = datetime(2026, 2, 10, 14, 0, 0, tzinfo=UTC)
        jobs = [
            JobListing(
                title="Backend Engineer",
                url="https://boards.greenhouse.io/acme/jobs/100",
                location="Remote",
                requirements=["Python", "PostgreSQL"],
                posted_date=ts,
                ats_provider=ATSProvider.GREENHOUSE,
                raw_data={"id": 100},
            ),
        ]
        ats = ATSInfo(
            provider=ATSProvider.GREENHOUSE,
            confidence=0.9,
            api_url="https://boards-api.greenhouse.io/v1/boards/acme/jobs",
            detection_method="url_pattern",
        )
        original = ScrapeResult(
            url="https://boards.greenhouse.io/acme",
            jobs=jobs,
            content_hash="b" * 64,
            changed=True,
            ats_info=ats,
            scrape_duration_ms=850.0,
        )
        json_str = original.model_dump_json()
        restored = ScrapeResult.model_validate_json(json_str)

        assert len(restored.jobs) == 1
        assert restored.jobs[0].title == "Backend Engineer"
        assert restored.jobs[0].requirements == ["Python", "PostgreSQL"]
        assert restored.jobs[0].posted_date == ts
        assert restored.jobs[0].ats_provider == ATSProvider.GREENHOUSE
        assert restored.content_hash == "b" * 64
        assert restored.changed is True
        assert restored.ats_info.provider == ATSProvider.GREENHOUSE
        assert restored.ats_info.api_url is not None
        assert restored.scrape_duration_ms == 850.0
        assert restored.error is None

    def test_round_trip_dict(self) -> None:
        original = ScrapeResult(
            url="https://example.com",
            jobs=[JobListing(title="Test", url="https://example.com/j/1")],
            changed=True,
        )
        data = original.model_dump()
        restored = ScrapeResult.model_validate(data)
        assert len(restored.jobs) == 1
        assert restored.changed is True

    def test_content_hash_sha256_format(self) -> None:
        """Content hash should be a valid SHA-256 hex string when present."""
        result = ScrapeResult(
            url="https://example.com",
            content_hash="abcdef1234567890" * 4,
        )
        assert result.content_hash is not None
        assert len(result.content_hash) == 64
        assert all(c in "0123456789abcdef" for c in result.content_hash)

    # ------------------------------------------------------------------
    # AC3: fetch_ok — distinguish zero-job-success from hard failure
    # ------------------------------------------------------------------

    def test_fetch_ok_defaults_false(self) -> None:
        """AC3: fetch_ok defaults to False (no fetch was attempted)."""
        result = ScrapeResult(url="https://example.com")
        assert result.fetch_ok is False

    def test_fetch_ok_true_with_zero_jobs(self) -> None:
        """AC3: fetch_ok=True + zero jobs = successful fetch, no parseable listings."""
        result = ScrapeResult(url="https://example.com", fetch_ok=True)
        assert result.fetch_ok is True
        assert result.ok is False  # ok still False (no jobs)
        assert result.error is None

    def test_fetch_ok_true_with_jobs(self) -> None:
        """AC3: fetch_ok=True + jobs = fully successful result."""
        job = JobListing(title="Engineer", url="https://example.com/jobs/1")
        result = ScrapeResult(url="https://example.com", jobs=[job], fetch_ok=True)
        assert result.fetch_ok is True
        assert result.ok is True

    def test_fetch_ok_false_on_error(self) -> None:
        """AC3: Hard failures have fetch_ok=False."""
        result = ScrapeResult(url="https://example.com", error="HTTP 500")
        assert result.fetch_ok is False
        assert result.ok is False

    def test_fetch_ok_differentiates_zero_jobs_from_failure(self) -> None:
        """AC3: An empty-but-successful scrape is distinguishable from hard failure."""
        empty_ok = ScrapeResult(url="https://example.com", fetch_ok=True)
        failure = ScrapeResult(url="https://example.com", error="Connection refused")

        assert empty_ok.fetch_ok is True  # HTTP succeeded
        assert empty_ok.ok is False  # but no jobs parsed

        assert failure.fetch_ok is False  # HTTP failed
        assert failure.ok is False  # and no jobs

    def test_fetch_ok_round_trip_json(self) -> None:
        """AC3: fetch_ok serializes and deserializes correctly."""
        original = ScrapeResult(url="https://example.com", fetch_ok=True)
        json_str = original.model_dump_json()
        restored = ScrapeResult.model_validate_json(json_str)
        assert restored.fetch_ok is True

    def test_fetch_ok_round_trip_dict(self) -> None:
        """AC3: fetch_ok appears in dict serialization."""
        result = ScrapeResult(url="https://example.com", fetch_ok=True)
        data = result.model_dump()
        assert "fetch_ok" in data
        assert data["fetch_ok"] is True
        restored = ScrapeResult.model_validate(data)
        assert restored.fetch_ok is True
