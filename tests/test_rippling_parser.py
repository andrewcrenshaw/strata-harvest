"""Tests for RipplingParser — Rippling ATS __NEXT_DATA__ extraction."""

from __future__ import annotations

import json

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.rippling import RipplingParser


def _make_next_data(jobs_items: list[dict]) -> str:
    """Wrap job items in the Rippling __NEXT_DATA__ HTML structure."""
    data = {
        "props": {
            "pageProps": {
                "jobs": {"items": jobs_items},
                "departments": [],
                "locations": [],
            }
        },
        "page": "/careers/open-roles",
        "buildId": "test-build-id",
    }
    return f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(data)}</script>'


_SAMPLE_ITEM = {
    "id": "abc-123",
    "name": "Senior Software Engineer",
    "url": "https://ats.rippling.com/rippling/jobs/abc-123",
    "department": {"name": "Engineering"},
    "locations": [
        {
            "name": "San Francisco, CA",
            "workplaceType": "ON_SITE",
        }
    ],
    "language": "en-US",
}

_REMOTE_ITEM = {
    "id": "def-456",
    "name": "Product Manager",
    "url": "https://ats.rippling.com/rippling/jobs/def-456",
    "department": {"name": "Product"},
    "locations": [
        {
            "name": "New York",
            "workplaceType": "REMOTE",
        }
    ],
}


@pytest.mark.verification
class TestRipplingParserParse:
    def setup_method(self) -> None:
        self.parser = RipplingParser()
        self.url = "https://www.rippling.com/careers/open-roles"

    def test_parses_single_item(self) -> None:
        html = _make_next_data([_SAMPLE_ITEM])
        result = self.parser.parse(html, url=self.url)
        assert len(result) == 1
        job = result[0]
        assert job.title == "Senior Software Engineer"
        assert str(job.url) == "https://ats.rippling.com/rippling/jobs/abc-123"
        assert job.department == "Engineering"
        assert job.location == "San Francisco, CA"
        assert job.ats_provider == ATSProvider.RIPPLING

    def test_parses_multiple_items(self) -> None:
        html = _make_next_data([_SAMPLE_ITEM, _REMOTE_ITEM])
        result = self.parser.parse(html, url=self.url)
        assert len(result) == 2

    def test_remote_location(self) -> None:
        html = _make_next_data([_REMOTE_ITEM])
        result = self.parser.parse(html, url=self.url)
        assert result[0].location == "New York (Remote)"

    def test_empty_items_list(self) -> None:
        html = _make_next_data([])
        result = self.parser.parse(html, url=self.url)
        assert result == []

    def test_no_next_data_returns_empty(self) -> None:
        result = self.parser.parse("<html><body>no data</body></html>", url=self.url)
        assert result == []

    def test_invalid_json_returns_empty(self) -> None:
        html = '<script id="__NEXT_DATA__">not-json{</script>'
        result = self.parser.parse(html, url=self.url)
        assert result == []

    def test_missing_jobs_key_returns_empty(self) -> None:
        data = json.dumps({"props": {"pageProps": {}}})
        html = f'<script id="__NEXT_DATA__">{data}</script>'
        result = self.parser.parse(html, url=self.url)
        assert result == []

    def test_empty_content_returns_empty(self) -> None:
        result = self.parser.parse("", url=self.url)
        assert result == []

    def test_item_without_url_uses_fallback(self) -> None:
        item = {**_SAMPLE_ITEM, "url": None, "id": "xyz-789"}
        html = _make_next_data([item])
        result = self.parser.parse(html, url=self.url)
        assert len(result) == 1
        assert "xyz-789" in str(result[0].url)

    def test_item_missing_title_is_skipped(self) -> None:
        bad_item = {"id": "no-title", "url": "https://example.com/jobs/1"}
        html = _make_next_data([bad_item, _SAMPLE_ITEM])
        result = self.parser.parse(html, url=self.url)
        # bad item skipped, good item parsed
        assert len(result) == 1
        assert result[0].title == "Senior Software Engineer"

    def test_jobs_as_list_not_dict(self) -> None:
        """pageProps.jobs can also be a plain list."""
        data = json.dumps(
            {
                "props": {
                    "pageProps": {
                        "jobs": [_SAMPLE_ITEM],
                    }
                }
            }
        )
        html = f'<script id="__NEXT_DATA__">{data}</script>'
        result = self.parser.parse(html, url=self.url)
        assert len(result) == 1

    def test_provider_is_rippling(self) -> None:
        assert RipplingParser.provider == ATSProvider.RIPPLING


@pytest.mark.verification
class TestRipplingDetector:
    """Verify the detector correctly identifies Rippling URLs."""

    def test_ats_rippling_url_detected(self) -> None:
        from strata_harvest.detector import detect_from_url

        info = detect_from_url("https://ats.rippling.com/acme/jobs")
        assert info.provider == ATSProvider.RIPPLING

    def test_rippling_careers_url_detected(self) -> None:
        from strata_harvest.detector import detect_from_url

        info = detect_from_url("https://www.rippling.com/careers/open-roles")
        assert info.provider == ATSProvider.RIPPLING


# ---------------------------------------------------------------------------
# Enriched fixture tests — PCC-1949 AC: richer field extraction
# ---------------------------------------------------------------------------

# __NEXT_DATA__ item with employmentType field
_RICH_ITEM = {
    "id": "rich-001",
    "name": "Principal Platform Engineer",
    "url": "https://ats.rippling.com/acme/jobs/rich-001",
    "department": {"name": "Infrastructure"},
    "locations": [{"name": "Austin, TX", "workplaceType": "HYBRID"}],
    "employmentType": "FULL_TIME",
}

# JSON-LD JobPosting for a Rippling individual job detail page
_RIPPLING_JSON_LD_HTML = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "JobPosting",
  "title": "Senior DevOps Engineer",
  "url": "https://ats.rippling.com/acme/jobs/devops-999",
  "employmentType": "FULL_TIME",
  "jobLocationType": "TELECOMMUTE",
  "baseSalary": {
    "@type": "MonetaryAmount",
    "currency": "USD",
    "value": {
      "@type": "QuantitativeValue",
      "minValue": 150000,
      "maxValue": 200000,
      "unitText": "YEAR"
    }
  },
  "description": "Build and maintain cloud infrastructure."
}
</script>
</head>
<body><p>Rippling job page</p></body>
</html>"""


@pytest.mark.verification
class TestRipplingRicherExtraction:
    """PCC-1949 AC: Richer field extraction from __NEXT_DATA__ and JSON-LD."""

    def setup_method(self) -> None:
        self.parser = RipplingParser()
        self.url = "https://ats.rippling.com/acme/jobs"

    def test_employment_type_from_next_data(self) -> None:
        """employmentType in __NEXT_DATA__ item is extracted."""
        html = _make_next_data([_RICH_ITEM])
        results = self.parser.parse(html, url=self.url)
        assert len(results) == 1
        assert results[0].employment_type == "FULL_TIME"

    def test_json_ld_job_extracted_when_present(self) -> None:
        """JSON-LD JobPosting block (e.g. detail page) is parsed via extruct/regex."""
        results = self.parser.parse(
            _RIPPLING_JSON_LD_HTML,
            url="https://ats.rippling.com/acme/jobs/devops-999",
        )
        assert len(results) == 1
        assert results[0].title == "Senior DevOps Engineer"
        assert results[0].ats_provider == ATSProvider.RIPPLING

    def test_json_ld_remote_location(self) -> None:
        results = self.parser.parse(
            _RIPPLING_JSON_LD_HTML,
            url="https://ats.rippling.com/acme/jobs/devops-999",
        )
        assert len(results) == 1
        assert results[0].location == "Remote"

    def test_json_ld_salary_extracted(self) -> None:
        results = self.parser.parse(
            _RIPPLING_JSON_LD_HTML,
            url="https://ats.rippling.com/acme/jobs/devops-999",
        )
        assert len(results) == 1
        assert results[0].salary_range is not None
        assert "150,000" in (results[0].salary_range or "")
        assert "200,000" in (results[0].salary_range or "")

    def test_json_ld_employment_type(self) -> None:
        results = self.parser.parse(
            _RIPPLING_JSON_LD_HTML,
            url="https://ats.rippling.com/acme/jobs/devops-999",
        )
        assert len(results) == 1
        assert results[0].employment_type == "FULL_TIME"

    def test_next_data_preferred_over_empty_json_ld(self) -> None:
        """When there are no JSON-LD job postings, __NEXT_DATA__ is used."""
        html = _make_next_data([_SAMPLE_ITEM])
        results = self.parser.parse(html, url=self.url)
        assert len(results) == 1
        assert results[0].title == "Senior Software Engineer"
