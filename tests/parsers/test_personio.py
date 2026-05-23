"""Tests for the Personio ATS parser against a recorded live fixture (PCC-2725).

The fixture in ``tests/fixtures/personio/personio_feed.xml`` was captured
verbatim from the real Personio public feed at
``https://personio.jobs.personio.com/xml``. It is a one-position
``<workzag-jobs>`` payload covering the canonical Personio schema (id,
subcompany, office, department, recruitingCategory, name, jobDescriptions,
employmentType, seniority, schedule, yearsOfExperience, occupation,
occupationCategory, createdAt). This satisfies PCC-2704 AC2:
"Personio parser extracts >0 listings from a recorded fixture".
"""

from pathlib import Path

import pytest

from strata_harvest.models import ATSProvider, ScrapeResult
from strata_harvest.parsers.base import BaseParser
from strata_harvest.parsers.personio import PersonioParser

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "personio"
LIVE_FIXTURE = FIXTURE_DIR / "personio_feed.xml"


@pytest.fixture(scope="module")
def recorded_feed() -> str:
    return LIVE_FIXTURE.read_text(encoding="utf-8")


@pytest.mark.verification
def test_recorded_fixture_is_present_and_non_empty() -> None:
    # Guard against the fixture being accidentally truncated or deleted.
    assert LIVE_FIXTURE.is_file(), f"recorded fixture missing: {LIVE_FIXTURE}"
    assert LIVE_FIXTURE.stat().st_size > 0
    content = LIVE_FIXTURE.read_text(encoding="utf-8")
    assert "<workzag-jobs>" in content
    assert "<position>" in content


@pytest.mark.verification
def test_parser_extracts_greater_than_zero_listings_from_recorded_feed(
    recorded_feed: str,
) -> None:
    parser = PersonioParser()
    jobs = parser.parse(recorded_feed, url="https://personio.jobs.personio.com")

    # The core AC: a recorded fixture yields > 0 listings.
    assert len(jobs) > 0, "Personio parser produced zero listings from recorded feed"

    job = jobs[0]
    # Required JobListing fields populated from the canonical Personio schema.
    assert job.title  # name → title
    assert str(job.url).startswith("https://personio.jobs.personio.com/job/")
    assert job.ats_provider == ATSProvider.PERSONIO
    # Optional but commonly-present fields from the recorded position.
    assert job.location is not None  # office
    assert job.department is not None
    assert job.employment_type is not None  # employmentType or schedule
    assert job.posted_date is not None  # createdAt


@pytest.mark.verification
def test_parser_is_registered_and_not_a_stub() -> None:
    # Resolving the PERSONIO provider returns the real parser, not the LLM stub.
    parser = BaseParser.for_provider(ATSProvider.PERSONIO)
    assert isinstance(parser, PersonioParser)
    assert BaseParser.is_stub_provider(ATSProvider.PERSONIO) is False


@pytest.mark.verification
def test_build_api_url() -> None:
    # Career page URL → /xml feed
    assert (
        PersonioParser.build_api_url("https://acme.jobs.personio.de")
        == "https://acme.jobs.personio.de/xml"
    )
    # Trailing slash handled
    assert (
        PersonioParser.build_api_url("https://acme.jobs.personio.de/")
        == "https://acme.jobs.personio.de/xml"
    )
    # Already a feed URL is returned unchanged
    assert (
        PersonioParser.build_api_url("https://acme.jobs.personio.de/xml")
        == "https://acme.jobs.personio.de/xml"
    )


@pytest.mark.verification
def test_parse_empty_and_invalid_inputs() -> None:
    parser = PersonioParser()
    assert parser.parse("", url="https://acme.jobs.personio.de") == []
    assert parser.parse("   ", url="https://acme.jobs.personio.de") == []
    assert parser.parse("<!DOCTYPE html><html></html>", url="https://x.personio.de") == []
    assert parser.parse("<workzag-jobs></workzag-jobs>", url="https://x.personio.de") == []


@pytest.mark.verification
def test_scrape_result_extraction_empty_semantics() -> None:
    # Hard failure: not a zero-yield gap.
    failure = ScrapeResult(url="https://x/jobs", error="HTTP 404")
    assert failure.extraction_empty is False

    # Fetched cleanly but zero listings: the distinct unhealthy outcome.
    zero = ScrapeResult(url="https://x/jobs", fetch_ok=True, extraction_empty=True)
    assert zero.extraction_empty is True
    assert zero.ok is False
