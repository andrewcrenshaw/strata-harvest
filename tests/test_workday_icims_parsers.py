"""Tests for Workday and iCIMS parsers (PCC-1631, AC2).

Validates that both parsers:
- Parse JSON-LD JobPosting blocks
- Parse JSON API responses
- Handle plain HTML gracefully (return empty list, no exception)
- Produce well-formed JobListing objects
"""

from __future__ import annotations

import json

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.icims import ICIMSParser
from strata_harvest.parsers.workday import WorkdayParser

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

WORKDAY_JSON_LD_HTML = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
{
  "@context": "http://schema.org",
  "@type": "JobPosting",
  "title": "Senior Software Engineer",
  "url": "https://company.wd5.myworkdayjobs.com/careers/job/New-York-NY/Senior-Software-Engineer_R-12345",
  "description": "<p>Build cloud infrastructure for our platform.</p>",
  "jobLocation": {
    "@type": "Place",
    "address": {
      "@type": "PostalAddress",
      "addressLocality": "New York",
      "addressRegion": "NY",
      "addressCountry": "US"
    }
  }
}
</script>
</head>
<body><h1>Open Positions</h1></body>
</html>"""

WORKDAY_JSON_LD_MULTI_HTML = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
[
  {
    "@context": "http://schema.org",
    "@type": "JobPosting",
    "title": "Data Engineer",
    "url": "https://company.wd5.myworkdayjobs.com/careers/job/Remote/Data-Engineer_R-222",
    "jobLocation": {"@type": "Place", "address": {"addressLocality": "Remote"}}
  },
  {
    "@context": "http://schema.org",
    "@type": "JobPosting",
    "title": "Product Manager",
    "url": "https://company.wd5.myworkdayjobs.com/careers/job/SF/Product-Manager_R-333"
  }
]
</script>
</head>
<body></body>
</html>"""

WORKDAY_EMBEDDED_JSON_HTML = """<!DOCTYPE html>
<html>
<body>
<script>
var appState = {"jobPostings":[
  {"title":"Infrastructure Engineer",
   "externalUrl":"https://company.wd5.myworkdayjobs.com/careers/job/job1",
   "locationsText":"Austin, TX"},
  {"title":"DevOps Lead",
   "externalUrl":"https://company.wd5.myworkdayjobs.com/careers/job/job2",
   "locationsText":"Remote"}
]};
</script>
</body>
</html>"""

ICIMS_JSON_LD_HTML = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
{
  "@context": "http://schema.org",
  "@type": "JobPosting",
  "title": "Compliance Analyst",
  "url": "https://careers-company.icims.com/jobs/5678/compliance-analyst/job",
  "description": "Analyze compliance requirements.",
  "jobLocation": {
    "@type": "Place",
    "address": {
      "@type": "PostalAddress",
      "addressLocality": "Chicago",
      "addressRegion": "IL"
    }
  }
}
</script>
</head>
<body></body>
</html>"""

ICIMS_JSON_RESPONSE = json.dumps(
    {
        "searchResults": [
            {
                "jobtitle": "Software Developer",
                "applyurl": "https://company.icims.com/jobs/1001/job",
                "joblocation": "Boston, MA",
                "id": "1001",
            },
            {
                "jobtitle": "QA Engineer",
                "applyurl": "https://company.icims.com/jobs/1002/job",
                "joblocation": "Remote",
                "id": "1002",
            },
        ],
        "totalCount": 2,
    }
)

EMPTY_HTML = "<html><body><p>No jobs currently open.</p></body></html>"


# ---------------------------------------------------------------------------
# WorkdayParser
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestWorkdayParser:
    """AC2: WorkdayParser extracts structured job data from common page patterns."""

    def test_parse_json_ld_single(self) -> None:
        """AC2: JSON-LD single JobPosting extraction."""
        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_JSON_LD_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        assert len(results) == 1
        job = results[0]
        assert job.title == "Senior Software Engineer"
        assert "myworkdayjobs.com" in str(job.url)
        assert job.ats_provider == ATSProvider.WORKDAY

    def test_parse_json_ld_location(self) -> None:
        """AC2: JSON-LD location fields are extracted."""
        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_JSON_LD_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        assert len(results) == 1
        assert results[0].location is not None
        assert "New York" in results[0].location

    def test_parse_json_ld_multiple(self) -> None:
        """AC2: JSON-LD array of multiple JobPostings."""
        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_JSON_LD_MULTI_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        assert len(results) == 2
        titles = [r.title for r in results]
        assert "Data Engineer" in titles
        assert "Product Manager" in titles
        for r in results:
            assert r.ats_provider == ATSProvider.WORKDAY

    def test_parse_embedded_json(self) -> None:
        """AC2: Embedded SPA JSON jobPostings array extraction."""
        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_EMBEDDED_JSON_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        assert len(results) == 2
        titles = [r.title for r in results]
        assert "Infrastructure Engineer" in titles
        assert "DevOps Lead" in titles

    def test_parse_embedded_json_location(self) -> None:
        """AC2: Embedded JSON location is preserved."""
        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_EMBEDDED_JSON_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        infra = next(r for r in results if r.title == "Infrastructure Engineer")
        assert infra.location == "Austin, TX"

    def test_parse_empty_html_returns_empty(self) -> None:
        """AC2: No parseable data → empty list, no exception."""
        parser = WorkdayParser()
        results = parser.parse(
            EMPTY_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        assert results == []

    def test_parse_empty_content_returns_empty(self) -> None:
        """Edge case: empty content string."""
        parser = WorkdayParser()
        results = parser.parse("", url="https://company.wd5.myworkdayjobs.com/careers")
        assert results == []

    def test_parse_does_not_raise_on_malformed_json_ld(self) -> None:
        """AC2: Malformed JSON in ld+json block → gracefully skipped."""
        html = """<html><head>
        <script type="application/ld+json">{broken json</script>
        </head><body></body></html>"""
        parser = WorkdayParser()
        results = parser.parse(html, url="https://company.wd5.myworkdayjobs.com/careers")
        assert results == []

    def test_parse_ignores_non_job_posting_json_ld(self) -> None:
        """AC2: Non-JobPosting JSON-LD blocks (e.g. Organization) are skipped."""
        html = """<html><head>
        <script type="application/ld+json">
        {"@type": "Organization", "name": "Acme Corp"}
        </script>
        </head><body></body></html>"""
        parser = WorkdayParser()
        results = parser.parse(html, url="https://company.wd5.myworkdayjobs.com/careers")
        assert results == []

    def test_all_results_are_job_listings(self) -> None:
        """AC2: All parsed results are proper JobListing objects."""
        from strata_harvest.models import JobListing

        parser = WorkdayParser()
        results = parser.parse(
            WORKDAY_JSON_LD_HTML,
            url="https://company.wd5.myworkdayjobs.com/careers",
        )
        for r in results:
            assert isinstance(r, JobListing)
            assert r.title
            assert r.url


# ---------------------------------------------------------------------------
# ICIMSParser
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestICIMSParser:
    """AC2: ICIMSParser extracts structured job data from common page patterns."""

    def test_parse_json_api_response(self) -> None:
        """AC2: JSON search API response is parsed."""
        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_RESPONSE,
            url="https://company.icims.com/jobs/search",
        )
        assert len(results) == 2
        titles = [r.title for r in results]
        assert "Software Developer" in titles
        assert "QA Engineer" in titles

    def test_parse_json_api_ats_provider(self) -> None:
        """AC2: Parsed iCIMS JSON results have correct ats_provider."""
        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_RESPONSE,
            url="https://company.icims.com/jobs/search",
        )
        for r in results:
            assert r.ats_provider == ATSProvider.ICIMS

    def test_parse_json_api_location(self) -> None:
        """AC2: iCIMS JSON location field is preserved."""
        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_RESPONSE,
            url="https://company.icims.com/jobs/search",
        )
        dev = next(r for r in results if r.title == "Software Developer")
        assert dev.location == "Boston, MA"

    def test_parse_json_ld(self) -> None:
        """AC2: JSON-LD JobPosting extraction from iCIMS page."""
        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_LD_HTML,
            url="https://careers-company.icims.com/jobs/search",
        )
        assert len(results) == 1
        job = results[0]
        assert job.title == "Compliance Analyst"
        assert job.ats_provider == ATSProvider.ICIMS

    def test_parse_json_ld_location(self) -> None:
        """AC2: JSON-LD location is parsed from iCIMS page."""
        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_LD_HTML,
            url="https://careers-company.icims.com/jobs/search",
        )
        assert len(results) == 1
        assert results[0].location is not None
        assert "Chicago" in results[0].location

    def test_parse_empty_html_returns_empty(self) -> None:
        """AC2: No parseable data → empty list, no exception."""
        parser = ICIMSParser()
        results = parser.parse(
            EMPTY_HTML,
            url="https://company.icims.com/jobs/search",
        )
        assert results == []

    def test_parse_empty_content_returns_empty(self) -> None:
        """Edge case: empty content."""
        parser = ICIMSParser()
        results = parser.parse("", url="https://company.icims.com/jobs/search")
        assert results == []

    def test_parse_malformed_json_does_not_raise(self) -> None:
        """AC2: Malformed JSON body → gracefully falls through to HTML strategy."""
        parser = ICIMSParser()
        results = parser.parse("{bad json", url="https://company.icims.com/jobs/search")
        assert isinstance(results, list)

    def test_parse_json_api_array_format(self) -> None:
        """AC2: JSON array (some iCIMS instances return bare arrays)."""
        data = json.dumps([
            {
                "title": "Dev Manager",
                "applyurl": "https://example.icims.com/jobs/500/job",
                "joblocation": "Dallas",
            },
            {
                "title": "UX Designer",
                "applyurl": "https://example.icims.com/jobs/501/job",
            },
        ])
        parser = ICIMSParser()
        results = parser.parse(data, url="https://example.icims.com/jobs/search")
        assert len(results) == 2
        titles = [r.title for r in results]
        assert "Dev Manager" in titles
        assert "UX Designer" in titles

    def test_all_results_are_job_listings(self) -> None:
        """AC2: All parsed results are proper JobListing objects."""
        from strata_harvest.models import JobListing

        parser = ICIMSParser()
        results = parser.parse(
            ICIMS_JSON_RESPONSE,
            url="https://company.icims.com/jobs/search",
        )
        for r in results:
            assert isinstance(r, JobListing)
            assert r.title
            assert r.url
