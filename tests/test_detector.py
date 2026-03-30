"""Tests for ATS provider detection."""

import pytest

from strata_harvest.detector import ATSInfo, detect_ats
from strata_harvest.models import ATSProvider


@pytest.mark.verification
class TestATSInfo:
    def test_default_unknown(self) -> None:
        info = ATSInfo()
        assert info.provider == ATSProvider.UNKNOWN
        assert info.confidence == 0.0

    def test_with_provider(self) -> None:
        info = ATSInfo(provider=ATSProvider.GREENHOUSE, confidence=0.9)
        assert info.provider == ATSProvider.GREENHOUSE


@pytest.mark.verification
class TestDetectATSUrlPatterns:
    async def test_greenhouse_url(self) -> None:
        info = await detect_ats("https://boards.greenhouse.io/company/jobs")
        assert info.provider == ATSProvider.GREENHOUSE
        assert info.confidence == 0.9
        assert info.detection_method == "url_pattern"

    async def test_lever_url(self) -> None:
        info = await detect_ats("https://jobs.lever.co/company")
        assert info.provider == ATSProvider.LEVER
        assert info.detection_method == "url_pattern"

    async def test_ashby_url(self) -> None:
        info = await detect_ats("https://jobs.ashbyhq.com/company")
        assert info.provider == ATSProvider.ASHBY

    async def test_workday_url(self) -> None:
        info = await detect_ats("https://company.myworkdayjobs.com/en-US/careers")
        assert info.provider == ATSProvider.WORKDAY

    async def test_icims_url(self) -> None:
        info = await detect_ats("https://careers-company.icims.com/jobs")
        assert info.provider == ATSProvider.ICIMS
