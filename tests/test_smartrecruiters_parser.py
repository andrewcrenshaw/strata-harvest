"""Tests for the SmartRecruiters ATS parser."""

import json

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.smartrecruiters import SmartRecruitersParser

MOCK_SMARTRECRUITERS_API_RESPONSE = {
    "offset": 0,
    "limit": 10,
    "totalFound": 2,
    "content": [
        {
            "id": "744000119411078",
            "name": "Demand Marketing & Events Specialist",
            "company": {"identifier": "smartrecruiters", "name": "SmartRecruiters Inc"},
            "releasedDate": "2026-04-08T15:42:09.135Z",
            "location": {
                "city": "Remote",
                "region": "Remote",
                "country": "gb",
                "remote": True,
                "fullLocation": "United Kingdom, Remote",
            },
            "department": {"id": "18554", "label": "Marketing"},
            "typeOfEmployment": {"id": "contract", "label": "Contract"},
        },
        {
            "id": "744000165432123",
            "name": "Software Engineer",
            "company": {},
            "releasedDate": "2026-04-01T10:00:00.000Z",
            "location": {"city": "San Francisco"},
        },
    ],
}


@pytest.mark.verification
def test_parse_valid_response() -> None:
    parser = SmartRecruitersParser()
    content = json.dumps(MOCK_SMARTRECRUITERS_API_RESPONSE)
    jobs = parser.parse(content, url="https://jobs.smartrecruiters.com/smartrecruiters")

    assert len(jobs) == 2

    job1 = jobs[0]
    assert job1.title == "Demand Marketing & Events Specialist"
    assert str(job1.url) == "https://jobs.smartrecruiters.com/smartrecruiters/744000119411078"
    assert job1.location == "United Kingdom, Remote"
    assert job1.department == "Marketing"
    assert job1.employment_type == "Contract"
    assert job1.posted_date is not None
    assert job1.ats_provider == ATSProvider.SMARTRECRUITERS

    job2 = jobs[1]
    assert job2.title == "Software Engineer"
    assert str(job2.url) == "https://jobs.smartrecruiters.com/smartrecruiters/744000165432123"
    assert job2.location == "San Francisco"
    assert job2.department is None


@pytest.mark.verification
def test_build_api_url() -> None:
    # Career page URL
    url = SmartRecruitersParser.build_api_url("https://jobs.smartrecruiters.com/company")
    assert url == "https://api.smartrecruiters.com/v1/companies/company/postings"

    # Already an API URL
    url2 = SmartRecruitersParser.build_api_url(
        "https://api.smartrecruiters.com/v1/companies/acme/postings"
    )
    assert url2 == "https://api.smartrecruiters.com/v1/companies/acme/postings"


@pytest.mark.verification
def test_parse_invalid_response() -> None:
    parser = SmartRecruitersParser()
    # Bad JSON
    assert parser.parse("<!DOCTYPE html><html>", url="https://example.com") == []
    # Missing content
    assert parser.parse('{"status": "ok"}', url="https://example.com") == []
