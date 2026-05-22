"""Tests for the Personio ATS parser (PCC-2704)."""

import pytest

from strata_harvest.models import ATSProvider, ScrapeResult
from strata_harvest.parsers.base import BaseParser
from strata_harvest.parsers.personio import PersonioParser

MOCK_PERSONIO_XML = """<?xml version="1.0" encoding="utf-8"?>
<workzag-jobs>
  <position>
    <id>123456</id>
    <subcompany></subcompany>
    <office>Berlin</office>
    <department>Engineering</department>
    <recruitingCategory></recruitingCategory>
    <name>Senior Backend Engineer (m/f/d)</name>
    <jobDescriptions>
      <jobDescription>
        <name>Your Tasks</name>
        <value><![CDATA[<p>Build delightful APIs.</p>]]></value>
      </jobDescription>
      <jobDescription>
        <name>Your Profile</name>
        <value><![CDATA[<ul><li>5+ years Python</li></ul>]]></value>
      </jobDescription>
    </jobDescriptions>
    <employmentType>permanent</employmentType>
    <schedule>full-time</schedule>
    <createdAt>2026-01-15T10:00:00+01:00</createdAt>
  </position>
  <position>
    <id>789012</id>
    <office>Remote</office>
    <department>Sales</department>
    <name>Account Executive</name>
    <schedule>full-time</schedule>
  </position>
</workzag-jobs>
"""


@pytest.mark.verification
def test_parse_valid_feed() -> None:
    parser = PersonioParser()
    jobs = parser.parse(MOCK_PERSONIO_XML, url="https://acme.jobs.personio.de")

    assert len(jobs) == 2

    job1 = jobs[0]
    assert job1.title == "Senior Backend Engineer (m/f/d)"
    assert str(job1.url) == "https://acme.jobs.personio.de/job/123456"
    assert job1.location == "Berlin"
    assert job1.department == "Engineering"
    assert job1.employment_type == "permanent"
    assert job1.description is not None
    assert "Build delightful APIs." in job1.description
    assert "5+ years Python" in job1.description
    assert job1.posted_date is not None
    assert job1.ats_provider == ATSProvider.PERSONIO

    job2 = jobs[1]
    assert job2.title == "Account Executive"
    assert str(job2.url) == "https://acme.jobs.personio.de/job/789012"
    assert job2.location == "Remote"
    # No employmentType element → falls back to schedule.
    assert job2.employment_type == "full-time"
    assert job2.posted_date is None


@pytest.mark.verification
def test_build_api_url() -> None:
    # Career page URL → /xml feed
    url = PersonioParser.build_api_url("https://acme.jobs.personio.de")
    assert url == "https://acme.jobs.personio.de/xml"

    # Trailing slash handled
    url_slash = PersonioParser.build_api_url("https://acme.jobs.personio.de/")
    assert url_slash == "https://acme.jobs.personio.de/xml"

    # Already a feed URL is returned unchanged
    url_feed = PersonioParser.build_api_url("https://acme.jobs.personio.de/xml")
    assert url_feed == "https://acme.jobs.personio.de/xml"


@pytest.mark.verification
def test_parse_empty_and_invalid() -> None:
    parser = PersonioParser()
    # Empty / whitespace
    assert parser.parse("", url="https://acme.jobs.personio.de") == []
    assert parser.parse("   ", url="https://acme.jobs.personio.de") == []
    # Not XML
    assert parser.parse("<!DOCTYPE html><html></html>", url="https://x.personio.de") == []
    # Well-formed XML but no positions
    assert parser.parse("<workzag-jobs></workzag-jobs>", url="https://x.personio.de") == []


@pytest.mark.verification
def test_position_without_name_is_skipped() -> None:
    parser = PersonioParser()
    xml = (
        "<workzag-jobs>"
        "<position><id>1</id><office>Berlin</office></position>"
        "<position><id>2</id><name>Valid Role</name></position>"
        "</workzag-jobs>"
    )
    jobs = parser.parse(xml, url="https://acme.jobs.personio.de")
    assert len(jobs) == 1
    assert jobs[0].title == "Valid Role"


@pytest.mark.verification
def test_parser_is_registered_and_not_stub() -> None:
    # Resolving the PERSONIO provider returns the real parser, not the LLM stub.
    parser = BaseParser.for_provider(ATSProvider.PERSONIO)
    assert isinstance(parser, PersonioParser)
    assert BaseParser.is_stub_provider(ATSProvider.PERSONIO) is False


@pytest.mark.verification
def test_scrape_result_extraction_empty_semantics() -> None:
    # Hard failure: not flagged as a zero-extraction yield gap.
    failure = ScrapeResult(url="https://x/jobs", error="HTTP 404")
    assert failure.extraction_empty is False

    # Successful extraction: not empty.
    from strata_harvest.models import JobListing

    healthy = ScrapeResult(
        url="https://x/jobs",
        jobs=[JobListing(title="Eng", url="https://x/jobs/1")],
        fetch_ok=True,
        extraction_empty=False,
    )
    assert healthy.extraction_empty is False

    # Fetched cleanly but zero listings: the distinct unhealthy outcome.
    zero = ScrapeResult(url="https://x/jobs", fetch_ok=True, extraction_empty=True)
    assert zero.extraction_empty is True
    assert zero.ok is False
