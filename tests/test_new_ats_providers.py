"""Tests for new ATS providers: Teamtailor, Recruitee, Pinpoint, Breezy,
Phenom, Eightfold, and SAP SuccessFactors.

PCC-1952 — one detection test + one fixture-based parser test per provider.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from strata_harvest.ats_fingerprints import FINGERPRINT_BY_PROVIDER, FINGERPRINTS
from strata_harvest.detector import detect_from_dom, detect_from_url
from strata_harvest.models import ATSProvider
from strata_harvest.parsers.breezy import BreezyParser
from strata_harvest.parsers.eightfold import EightfoldParser
from strata_harvest.parsers.phenom import PhenomParser
from strata_harvest.parsers.pinpoint import PinpointParser
from strata_harvest.parsers.recruitee import RecruiteeParser
from strata_harvest.parsers.sap_successfactors import SAPSuccessFactorsParser
from strata_harvest.parsers.teamtailor import TeamtailorParser

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "career_pages"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# ATSFingerprint catalog
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestATSFingerprintCatalog:
    def test_all_new_providers_in_catalog(self) -> None:
        new_providers = {
            ATSProvider.TEAMTAILOR,
            ATSProvider.RECRUITEE,
            ATSProvider.PINPOINT,
            ATSProvider.BREEZY,
            ATSProvider.PHENOM,
            ATSProvider.EIGHTFOLD,
            ATSProvider.SAP_SUCCESSFACTORS,
        }
        catalog_providers = {fp.provider for fp in FINGERPRINTS}
        assert new_providers.issubset(catalog_providers)

    def test_lookup_by_provider(self) -> None:
        for provider in (
            ATSProvider.TEAMTAILOR,
            ATSProvider.RECRUITEE,
            ATSProvider.PINPOINT,
            ATSProvider.BREEZY,
            ATSProvider.PHENOM,
            ATSProvider.EIGHTFOLD,
            ATSProvider.SAP_SUCCESSFACTORS,
        ):
            fp = FINGERPRINT_BY_PROVIDER[provider]
            assert fp.provider == provider
            assert fp.confidence > 0.5

    def test_each_fingerprint_has_url_or_dom_signals(self) -> None:
        for fp in FINGERPRINTS:
            assert fp.url_patterns or fp.dom_selectors, (
                f"{fp.provider} has no url_patterns or dom_selectors"
            )


# ---------------------------------------------------------------------------
# Detection — URL patterns
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestNewProviderURLDetection:
    def test_teamtailor_url(self) -> None:
        info = detect_from_url("https://acme.teamtailor.com/jobs")
        assert info.provider == ATSProvider.TEAMTAILOR
        assert info.confidence == 0.9
        assert info.detection_method == "url_pattern"

    def test_teamtailor_api_url(self) -> None:
        info = detect_from_url("https://api.teamtailor.com/v1/jobs")
        assert info.provider == ATSProvider.TEAMTAILOR

    def test_recruitee_url(self) -> None:
        info = detect_from_url("https://acme.recruitee.com/api/offers")
        assert info.provider == ATSProvider.RECRUITEE
        assert info.confidence == 0.9

    def test_pinpoint_url(self) -> None:
        info = detect_from_url("https://acme.pinpointhq.com/jobs.json")
        assert info.provider == ATSProvider.PINPOINT
        assert info.confidence == 0.9

    def test_breezy_url(self) -> None:
        info = detect_from_url("https://acme.breezy.hr/json")
        assert info.provider == ATSProvider.BREEZY
        assert info.confidence == 0.9

    def test_phenom_url(self) -> None:
        info = detect_from_url("https://careers.acme.phenompeople.com/jobs")
        assert info.provider == ATSProvider.PHENOM

    def test_eightfold_url(self) -> None:
        info = detect_from_url("https://acme.eightfold.ai/careers")
        assert info.provider == ATSProvider.EIGHTFOLD
        assert info.confidence == 0.9

    def test_sap_successfactors_url(self) -> None:
        info = detect_from_url("https://career.sap.com/careers")
        assert info.provider == ATSProvider.SAP_SUCCESSFACTORS
        assert info.confidence == 0.9

    def test_sap_successfactors_subdomain(self) -> None:
        info = detect_from_url("https://acme.successfactors.com/odata/v2/JobRequisition")
        assert info.provider == ATSProvider.SAP_SUCCESSFACTORS

    def test_no_false_positives(self) -> None:
        """New patterns must not match unrelated URLs."""
        safe_urls = [
            "https://example.com/careers",
            "https://jobs.lever.co/acme",
            "https://boards.greenhouse.io/acme",
        ]
        new_providers = {
            ATSProvider.TEAMTAILOR,
            ATSProvider.RECRUITEE,
            ATSProvider.PINPOINT,
            ATSProvider.BREEZY,
            ATSProvider.PHENOM,
            ATSProvider.EIGHTFOLD,
            ATSProvider.SAP_SUCCESSFACTORS,
        }
        for url in safe_urls:
            info = detect_from_url(url)
            assert info.provider not in new_providers, f"{url} falsely matched {info.provider}"


# ---------------------------------------------------------------------------
# Detection — DOM signatures
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestNewProviderDOMDetection:
    def test_teamtailor_fixture(self) -> None:
        html = _load_fixture("teamtailor_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.TEAMTAILOR
        assert info.confidence > 0.5
        assert info.detection_method == "dom_probe"

    def test_recruitee_fixture(self) -> None:
        html = _load_fixture("recruitee_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.RECRUITEE
        assert info.confidence > 0.5

    def test_pinpoint_fixture(self) -> None:
        html = _load_fixture("pinpoint_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.PINPOINT
        assert info.confidence > 0.5

    def test_breezy_fixture(self) -> None:
        html = _load_fixture("breezy_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.BREEZY
        assert info.confidence > 0.5

    def test_phenom_fixture(self) -> None:
        html = _load_fixture("phenom_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.PHENOM
        assert info.confidence > 0.5

    def test_eightfold_fixture(self) -> None:
        html = _load_fixture("eightfold_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.EIGHTFOLD
        assert info.confidence > 0.5

    def test_sap_successfactors_fixture(self) -> None:
        html = _load_fixture("sap_successfactors_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.SAP_SUCCESSFACTORS
        assert info.confidence > 0.5

    # Inline DOM signal tests (no fixture file needed)
    def test_teamtailor_inline_dom(self) -> None:
        html = '<div class="tt-job" data-teamtailor>Software Engineer</div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.TEAMTAILOR

    def test_recruitee_inline_dom(self) -> None:
        html = '<div class="recruitee-job-board"><a href="/o/job">Role</a></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.RECRUITEE

    def test_pinpoint_inline_dom(self) -> None:
        html = '<div data-pinpoint="board"><ul class="pinpoint-job-list"></ul></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.PINPOINT

    def test_breezy_inline_dom(self) -> None:
        html = '<ul class="breezy-position-list"><li class="breezy-position">Role</li></ul>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.BREEZY

    def test_phenom_inline_dom(self) -> None:
        html = '<div class="ph-jobs-container"><div class="ph-card">Job</div></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.PHENOM

    def test_eightfold_inline_dom(self) -> None:
        html = '<div class="eightfold-job-listing"><div class="efai-card">Job</div></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.EIGHTFOLD

    def test_sap_inline_dom(self) -> None:
        html = '<div class="sfsf-job-list">SAP SuccessFactors careers</div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.SAP_SUCCESSFACTORS


# ---------------------------------------------------------------------------
# Parser tests — Teamtailor
# ---------------------------------------------------------------------------

TEAMTAILOR_FIXTURE = {
    "data": [
        {
            "id": "1234",
            "type": "jobs",
            "attributes": {
                "title": "Backend Engineer",
                "location": "Stockholm, Sweden",
                "employment-type": "Full-time",
                "body": "<p>Join us to build great things.</p>",
                "created-at": "2026-04-01T10:00:00Z",
            },
            "links": {
                "careersite-job-url": "https://acme.teamtailor.com/jobs/1234-backend-engineer",
            },
        },
        {
            "id": "1235",
            "type": "jobs",
            "attributes": {
                "title": "Product Designer",
                "location": None,
                "employment-type": None,
                "body": "",
                "created-at": "2026-03-15T09:00:00Z",
            },
            "links": {
                "self": "https://api.teamtailor.com/v1/jobs/1235",
            },
        },
    ]
}


@pytest.mark.verification
class TestTeamtailorParser:
    def test_parse_valid_response(self) -> None:
        parser = TeamtailorParser()
        jobs = parser.parse(json.dumps(TEAMTAILOR_FIXTURE), url="https://acme.teamtailor.com")
        assert len(jobs) == 2

        j0 = jobs[0]
        assert j0.title == "Backend Engineer"
        assert str(j0.url) == "https://acme.teamtailor.com/jobs/1234-backend-engineer"
        assert j0.location == "Stockholm, Sweden"
        assert j0.employment_type == "Full-time"
        assert j0.description == "Join us to build great things."
        assert j0.posted_date is not None
        assert j0.ats_provider == ATSProvider.TEAMTAILOR

        j1 = jobs[1]
        assert j1.title == "Product Designer"

    def test_build_api_url(self) -> None:
        url = TeamtailorParser.build_api_url("https://acme.teamtailor.com")
        assert "teamtailor.com" in url
        assert "acme" in url

    def test_parse_invalid_json(self) -> None:
        parser = TeamtailorParser()
        assert parser.parse("not json", url="https://acme.teamtailor.com") == []

    def test_parse_missing_data_key(self) -> None:
        parser = TeamtailorParser()
        assert parser.parse('{"meta": {}}', url="https://acme.teamtailor.com") == []

    def test_skips_job_missing_title(self) -> None:
        bad = {"data": [{"id": "99", "type": "jobs", "attributes": {}, "links": {}}]}
        parser = TeamtailorParser()
        jobs = parser.parse(json.dumps(bad), url="https://acme.teamtailor.com")
        assert jobs == []


# ---------------------------------------------------------------------------
# Parser tests — Recruitee
# ---------------------------------------------------------------------------

RECRUITEE_FIXTURE = {
    "offers": [
        {
            "id": 5001,
            "title": "Senior Frontend Developer",
            "slug": "senior-frontend-developer",
            "location": "Berlin, Germany",
            "department": "Engineering",
            "employment_type_code": "fulltime",
            "description": "<p>Work on the frontend.</p>",
            "created_at": "2026-04-05T08:00:00Z",
        },
        {
            "id": 5002,
            "title": "UX Researcher",
            "slug": "ux-researcher",
            "careers_url": "https://acme.recruitee.com/o/ux-researcher",
        },
    ]
}


@pytest.mark.verification
class TestRecruiteeParser:
    def test_parse_valid_response(self) -> None:
        parser = RecruiteeParser()
        jobs = parser.parse(
            json.dumps(RECRUITEE_FIXTURE),
            url="https://acme.recruitee.com",
        )
        assert len(jobs) == 2

        j0 = jobs[0]
        assert j0.title == "Senior Frontend Developer"
        assert j0.location == "Berlin, Germany"
        assert j0.department == "Engineering"
        assert j0.employment_type == "fulltime"
        assert j0.description == "Work on the frontend."
        assert j0.posted_date is not None
        assert j0.ats_provider == ATSProvider.RECRUITEE

        j1 = jobs[1]
        assert j1.title == "UX Researcher"
        assert str(j1.url) == "https://acme.recruitee.com/o/ux-researcher"

    def test_build_api_url(self) -> None:
        url = RecruiteeParser.build_api_url("https://acme.recruitee.com")
        assert url == "https://acme.recruitee.com/api/offers"

    def test_build_api_url_already_api(self) -> None:
        url = RecruiteeParser.build_api_url("https://acme.recruitee.com/api/offers")
        assert url == "https://acme.recruitee.com/api/offers"

    def test_parse_invalid_json(self) -> None:
        parser = RecruiteeParser()
        assert parser.parse("garbage", url="https://acme.recruitee.com") == []

    def test_parse_missing_offers_key(self) -> None:
        parser = RecruiteeParser()
        assert parser.parse('{"data": []}', url="https://acme.recruitee.com") == []


# ---------------------------------------------------------------------------
# Parser tests — Pinpoint
# ---------------------------------------------------------------------------

PINPOINT_FIXTURE = {
    "jobs": [
        {
            "id": 301,
            "title": "Growth Marketing Manager",
            "absolute_url": "https://acme.pinpointhq.com/jobs/301",
            "location": "London, UK",
            "department": "Marketing",
            "employment_type": "Full-time",
            "description": "<p>Drive growth initiatives.</p>",
            "published_at": "2026-04-10T12:00:00Z",
        },
        {
            "id": 302,
            "title": "Backend Developer",
        },
    ]
}


@pytest.mark.verification
class TestPinpointParser:
    def test_parse_valid_response(self) -> None:
        parser = PinpointParser()
        jobs = parser.parse(
            json.dumps(PINPOINT_FIXTURE),
            url="https://acme.pinpointhq.com",
        )
        assert len(jobs) == 2

        j0 = jobs[0]
        assert j0.title == "Growth Marketing Manager"
        assert str(j0.url) == "https://acme.pinpointhq.com/jobs/301"
        assert j0.location == "London, UK"
        assert j0.department == "Marketing"
        assert j0.employment_type == "Full-time"
        assert j0.description == "Drive growth initiatives."
        assert j0.ats_provider == ATSProvider.PINPOINT

    def test_parse_bare_array(self) -> None:
        """Pinpoint may return a bare JSON array instead of a wrapper dict."""
        bare = [
            {"id": 1, "title": "Designer", "absolute_url": "https://acme.pinpointhq.com/jobs/1"}
        ]
        parser = PinpointParser()
        jobs = parser.parse(json.dumps(bare), url="https://acme.pinpointhq.com")
        assert len(jobs) == 1
        assert jobs[0].title == "Designer"

    def test_build_api_url(self) -> None:
        url = PinpointParser.build_api_url("https://acme.pinpointhq.com")
        assert url == "https://acme.pinpointhq.com/jobs.json"

    def test_parse_invalid_json(self) -> None:
        parser = PinpointParser()
        assert parser.parse("not json", url="https://acme.pinpointhq.com") == []


# ---------------------------------------------------------------------------
# Parser tests — Breezy
# ---------------------------------------------------------------------------

BREEZY_FIXTURE = [
    {
        "_id": "abc123",
        "name": "DevOps Engineer",
        "friendly_id": "devops-engineer",
        "location": {"name": "Austin, TX"},
        "department": {"name": "Infrastructure"},
        "type": "full-time",
        "description": "<p>Maintain cloud infra.</p>",
        "creation_date": 1712000000000,
    },
    {
        "_id": "abc124",
        "name": "iOS Developer",
        "friendly_id": "ios-developer",
        "location": {"city": "New York"},
    },
]


@pytest.mark.verification
class TestBreezyParser:
    def test_parse_valid_response(self) -> None:
        parser = BreezyParser()
        jobs = parser.parse(json.dumps(BREEZY_FIXTURE), url="https://acme.breezy.hr")
        assert len(jobs) == 2

        j0 = jobs[0]
        assert j0.title == "DevOps Engineer"
        assert str(j0.url) == "https://acme.breezy.hr/p/devops-engineer"
        assert j0.location == "Austin, TX"
        assert j0.department == "Infrastructure"
        assert j0.employment_type == "full-time"
        assert j0.description == "Maintain cloud infra."
        assert j0.ats_provider == ATSProvider.BREEZY

        j1 = jobs[1]
        assert j1.title == "iOS Developer"
        assert j1.location == "New York"

    def test_parse_wrapped_response(self) -> None:
        wrapped = {"positions": BREEZY_FIXTURE}
        parser = BreezyParser()
        jobs = parser.parse(json.dumps(wrapped), url="https://acme.breezy.hr")
        assert len(jobs) == 2

    def test_build_api_url(self) -> None:
        url = BreezyParser.build_api_url("https://acme.breezy.hr")
        assert url == "https://acme.breezy.hr/json"

    def test_build_api_url_already_json(self) -> None:
        url = BreezyParser.build_api_url("https://acme.breezy.hr/json")
        assert url == "https://acme.breezy.hr/json"

    def test_parse_invalid_json(self) -> None:
        parser = BreezyParser()
        assert parser.parse("bad data", url="https://acme.breezy.hr") == []


# ---------------------------------------------------------------------------
# Parser tests — Phenom (HTML)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestPhenomParser:
    def test_parse_fixture_html(self) -> None:
        html = _load_fixture("phenom_embedded.html")
        parser = PhenomParser()
        jobs = parser.parse(html, url="https://careers.acme.com")
        assert len(jobs) >= 1
        titles = [j.title for j in jobs]
        assert any(t for t in titles)
        for job in jobs:
            assert job.ats_provider == ATSProvider.PHENOM

    def test_parse_inline_phenom_block(self) -> None:
        html = """
        <div class="ph-job-card">
          <h2><a href="https://careers.acme.com/jobs/42">Analytics Lead</a></h2>
        </div>
        """
        parser = PhenomParser()
        jobs = parser.parse(html, url="https://careers.acme.com")
        assert len(jobs) == 1
        assert jobs[0].title == "Analytics Lead"
        assert "careers.acme.com" in str(jobs[0].url)

    def test_parse_no_jobs_returns_empty(self) -> None:
        parser = PhenomParser()
        jobs = parser.parse("<html><body>No jobs here</body></html>", url="https://example.com")
        assert jobs == []

    def test_build_api_url_returns_hint(self) -> None:
        url = PhenomParser.build_api_url("https://careers.acme.com")
        assert "phenom.com" in url


# ---------------------------------------------------------------------------
# Parser tests — Eightfold (HTML + JSON-LD)
# ---------------------------------------------------------------------------

EIGHTFOLD_JSON_LD_HTML = """<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
[
  {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    "title": "Principal Engineer",
    "url": "https://acme.eightfold.ai/careers?job=500",
    "jobLocation": {
      "@type": "Place",
      "address": {"@type": "PostalAddress", "addressLocality": "Seattle, WA"}
    },
    "employmentType": "FULL_TIME"
  }
]
</script>
</head>
<body><div class="eightfold-job-listing"></div></body>
</html>
"""


@pytest.mark.verification
class TestEightfoldParser:
    def test_parse_fixture_html(self) -> None:
        html = _load_fixture("eightfold_embedded.html")
        parser = EightfoldParser()
        jobs = parser.parse(html, url="https://acme.eightfold.ai/careers")
        assert len(jobs) >= 1
        for job in jobs:
            assert job.ats_provider == ATSProvider.EIGHTFOLD

    def test_parse_json_ld(self) -> None:
        parser = EightfoldParser()
        jobs = parser.parse(EIGHTFOLD_JSON_LD_HTML, url="https://acme.eightfold.ai/careers")
        assert len(jobs) == 1
        assert jobs[0].title == "Principal Engineer"
        assert jobs[0].location == "Seattle, WA"
        assert jobs[0].employment_type == "FULL_TIME"
        assert jobs[0].ats_provider == ATSProvider.EIGHTFOLD

    def test_parse_inline_block(self) -> None:
        html = """
        <div class="eightfold-job-card">
          <h3><a href="https://acme.eightfold.ai/careers?job=99">Data Scientist</a></h3>
        </div>
        """
        parser = EightfoldParser()
        jobs = parser.parse(html, url="https://acme.eightfold.ai/careers")
        assert len(jobs) == 1
        assert jobs[0].title == "Data Scientist"

    def test_parse_no_jobs_returns_empty(self) -> None:
        parser = EightfoldParser()
        jobs = parser.parse("<html><body>Nothing here</body></html>", url="https://example.com")
        assert jobs == []


# ---------------------------------------------------------------------------
# Parser tests — SAP SuccessFactors
# ---------------------------------------------------------------------------

SAP_SF_FIXTURE = {
    "d": {
        "results": [
            {
                "jobReqId": "REQ-001",
                "jobTitle": "SAP Basis Consultant",
                "location": "Walldorf, Germany",
                "department": "Technology",
                "employmentType": "Regular",
                "jobDescription": "<p>Manage SAP landscape.</p>",
                "postingDate": "2026-04-01T00:00:00Z",
            },
            {
                "jobReqId": "REQ-002",
                "jobTitle": "Cloud Architect",
                "location": "Remote",
                "postingDate": "/Date(1712000000000)/",
            },
        ]
    }
}


@pytest.mark.verification
class TestSAPSuccessFactorsParser:
    def test_parse_valid_odata_response(self) -> None:
        parser = SAPSuccessFactorsParser()
        jobs = parser.parse(
            json.dumps(SAP_SF_FIXTURE),
            url="https://career.sap.com",
        )
        assert len(jobs) == 2

        j0 = jobs[0]
        assert j0.title == "SAP Basis Consultant"
        assert j0.location == "Walldorf, Germany"
        assert j0.department == "Technology"
        assert j0.employment_type == "Regular"
        assert j0.description == "Manage SAP landscape."
        assert j0.posted_date is not None
        assert j0.ats_provider == ATSProvider.SAP_SUCCESSFACTORS

        j1 = jobs[1]
        assert j1.title == "Cloud Architect"
        assert j1.posted_date is not None  # parsed from /Date(...)/ format

    def test_build_api_url(self) -> None:
        url = SAPSuccessFactorsParser.build_api_url("https://career.sap.com")
        assert "/odata/v2/JobRequisition" in url
        assert "$format=json" in url

    def test_build_api_url_already_odata(self) -> None:
        existing = "https://career.sap.com/odata/v2/JobRequisition?$format=json"
        url = SAPSuccessFactorsParser.build_api_url(existing)
        assert url == existing

    def test_sitemap_url(self) -> None:
        url = SAPSuccessFactorsParser.sitemap_url("https://career.sap.com/jobs")
        assert url == "https://career.sap.com/sitemal.xml"

    def test_parse_invalid_json(self) -> None:
        parser = SAPSuccessFactorsParser()
        assert parser.parse("not json", url="https://career.sap.com") == []

    def test_parse_missing_results(self) -> None:
        parser = SAPSuccessFactorsParser()
        assert parser.parse('{"d": {}}', url="https://career.sap.com") == []

    def test_odata_ticks_date_parsed(self) -> None:
        """OData /Date(ms)/ format must parse to a valid datetime."""
        from strata_harvest.parsers.sap_successfactors import _parse_odata_date

        dt = _parse_odata_date("/Date(1712000000000)/")
        assert dt is not None

    def test_odata_iso_date_parsed(self) -> None:
        from strata_harvest.parsers.sap_successfactors import _parse_odata_date

        dt = _parse_odata_date("2026-04-01T00:00:00Z")
        assert dt is not None
        assert dt.year == 2026
