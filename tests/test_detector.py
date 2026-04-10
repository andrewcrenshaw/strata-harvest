"""Tests for ATS provider detection.

Covers:
- URL pattern matching (detect_from_url — no network)
- DOM probing with fixture HTML files (detect_from_dom — no network)
- ATSInfo model bounds and defaults
- Orchestrator (detect_ats with pre-supplied html — no network)
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from strata_harvest.detector import detect_ats, detect_from_dom, detect_from_url
from strata_harvest.models import ATSInfo, ATSProvider

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "career_pages"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@pytest.mark.verification
class TestATSInfo:
    def test_default_unknown(self) -> None:
        info = ATSInfo()
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0
        assert info.detection_method == "none"

    def test_with_provider(self) -> None:
        info = ATSInfo(provider=ATSProvider.GREENHOUSE, confidence=0.9)
        assert info.provider == ATSProvider.GREENHOUSE

    def test_confidence_bounds(self) -> None:
        info = ATSInfo(confidence=0.0)
        assert info.confidence == 0.0
        info = ATSInfo(confidence=1.0)
        assert info.confidence == 1.0

    def test_confidence_rejects_out_of_range(self) -> None:
        with pytest.raises(ValidationError):
            ATSInfo(confidence=1.5)
        with pytest.raises(ValidationError):
            ATSInfo(confidence=-0.1)

    def test_serialization_round_trip(self) -> None:
        info = ATSInfo(
            provider=ATSProvider.LEVER,
            confidence=0.85,
            api_url="https://api.example.com",
            detection_method="url_pattern",
        )
        data = info.model_dump()
        restored = ATSInfo(**data)
        assert restored == info


@pytest.mark.verification
class TestURLPatternMatching:
    def test_greenhouse_boards(self) -> None:
        info = detect_from_url("https://boards.greenhouse.io/company/jobs")
        assert info.provider == ATSProvider.GREENHOUSE
        assert info.confidence == 0.9
        assert info.detection_method == "url_pattern"
        assert info.api_url is not None

    def test_greenhouse_embed(self) -> None:
        info = detect_from_url("https://greenhouse.io/embed/job_board/iframe?for=acme")
        assert info.provider == ATSProvider.GREENHOUSE

    def test_lever(self) -> None:
        info = detect_from_url("https://jobs.lever.co/company")
        assert info.provider == ATSProvider.LEVER
        assert info.detection_method == "url_pattern"

    def test_ashby_ashbyhq(self) -> None:
        info = detect_from_url("https://jobs.ashbyhq.com/company")
        assert info.provider == ATSProvider.ASHBY

    def test_ashby_io(self) -> None:
        info = detect_from_url("https://ashby.io/company/jobs")
        assert info.provider == ATSProvider.ASHBY

    def test_workday_myworkdayjobs(self) -> None:
        info = detect_from_url("https://company.myworkdayjobs.com/en-US/careers")
        assert info.provider == ATSProvider.WORKDAY

    def test_workday_subdomain(self) -> None:
        info = detect_from_url("https://corp.workday.com/jobs")
        assert info.provider == ATSProvider.WORKDAY

    def test_icims(self) -> None:
        info = detect_from_url("https://careers-company.icims.com/jobs")
        assert info.provider == ATSProvider.ICIMS

    def test_unknown_url(self) -> None:
        info = detect_from_url("https://example.com/careers")
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0

    def test_rippling(self) -> None:
        info = detect_from_url("https://ats.rippling.com/company/jobs")
        assert info.provider == ATSProvider.RIPPLING

    def test_workable(self) -> None:
        info = detect_from_url("https://company.workable.com/")
        assert info.provider == ATSProvider.WORKABLE

    def test_bamboohr(self) -> None:
        info = detect_from_url("https://company.bamboohr.com/careers")
        assert info.provider == ATSProvider.BAMBOOHR

    def test_smartrecruiters(self) -> None:
        info = detect_from_url("https://jobs.smartrecruiters.com/company")
        assert info.provider == ATSProvider.SMARTRECRUITERS

    def test_personio(self) -> None:
        info = detect_from_url("https://jobs.personio.de/company")
        assert info.provider == ATSProvider.PERSONIO

    def test_jobvite(self) -> None:
        info = detect_from_url("https://jobs.jobvite.com/company")
        assert info.provider == ATSProvider.JOBVITE
        info = detect_from_url("https://example.com/careers")
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0

    def test_no_false_positive_on_substring(self) -> None:
        info = detect_from_url("https://example.com/greenhouse-supplies")
        assert info.provider == ATSProvider.UNKNOWN


@pytest.mark.verification
class TestDOMProbing:
    def test_greenhouse_fixture(self) -> None:
        html = _load_fixture("greenhouse_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.GREENHOUSE
        assert info.detection_method == "dom_probe"
        assert info.confidence > 0.5

    def test_lever_fixture(self) -> None:
        html = _load_fixture("lever_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.LEVER
        assert info.confidence > 0.5

    def test_ashby_fixture(self) -> None:
        html = _load_fixture("ashby_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.ASHBY
        assert info.confidence > 0.5

    def test_workday_fixture(self) -> None:
        html = _load_fixture("workday_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.WORKDAY
        assert info.confidence > 0.5

    def test_icims_fixture(self) -> None:
        html = _load_fixture("icims_embedded.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.ICIMS
        assert info.confidence > 0.5

    def test_unknown_fixture(self) -> None:
        html = _load_fixture("unknown_custom.html")
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.UNKNOWN

    def test_rippling_dom(self) -> None:
        html = '<script src="ats.rippling.com/js"></script>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.RIPPLING

    def test_workable_dom(self) -> None:
        html = '<div id="workable-board"></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.WORKABLE

    def test_bamboohr_dom(self) -> None:
        html = '<div id="bamboohr-app"></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.BAMBOOHR

    def test_smartrecruiters_dom(self) -> None:
        html = '<script src="smartrecruiters-app.js"></script>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.SMARTRECRUITERS

    def test_personio_dom(self) -> None:
        html = '<div class="personio-jobs"></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.PERSONIO

    def test_jobvite_dom(self) -> None:
        html = '<div class="jobvite-container"></div>'
        info = detect_from_dom(html)
        assert info.provider == ATSProvider.JOBVITE

    def test_empty_html(self) -> None:
        info = detect_from_dom("")
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0

    def test_best_match_wins(self) -> None:
        html = (
            '<div class="lever-jobs-container"></div><script src="boards.greenhouse.io/x"></script>'
        )
        info = detect_from_dom(html)
        assert info.provider in (ATSProvider.GREENHOUSE, ATSProvider.LEVER)
        assert info.confidence > 0.7


@pytest.mark.verification
class TestDetectATS:
    """Test the orchestrator with pre-supplied html (no network)."""

    async def test_url_match_skips_fetch(self) -> None:
        info = await detect_ats("https://boards.greenhouse.io/acme/jobs")
        assert info.provider == ATSProvider.GREENHOUSE
        assert info.detection_method == "url_pattern"

    async def test_dom_fallback_with_html(self) -> None:
        html = _load_fixture("lever_embedded.html")
        info = await detect_ats("https://example.com/careers", html=html)
        assert info.provider == ATSProvider.LEVER
        assert info.detection_method == "dom_probe"

    async def test_unknown_url_and_html(self) -> None:
        info = await detect_ats(
            "https://example.com/careers",
            html="<html><body>No ATS here</body></html>",
        )
        assert info.provider == ATSProvider.UNKNOWN
