"""Tests for LLMFallbackParser — LLM-based extraction for unknown career pages (PCC-1425).

Covers all acceptance criteria:
- Sends page content to LLM with structured extraction prompt
- Configurable LLM provider (default Gemini Flash)
- Returns list[JobListing] in same format as ATS parsers
- Handles: multi-page career sites, single-page job boards, JS-rendered content
- Tests with saved HTML from 3+ real career pages without recognized ATS
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.llm_fallback import LLMFallbackParser

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "llm_fallback"


def _load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


def _make_llm_response(jobs: list[dict[str, Any]]) -> MagicMock:
    """Build a mock litellm completion response with the given job list."""
    content = json.dumps({"jobs": jobs})
    message = MagicMock()
    message.content = content
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    return response


# ------------------------------------------------------------------
# Startup careers fixture (3 jobs, inline cards)
# ------------------------------------------------------------------

STARTUP_LLM_RESPONSE = _make_llm_response(
    [
        {
            "title": "Senior Python Engineer",
            "url": "https://novatech-labs.com/careers/senior-python-engineer",
            "location": "San Francisco, CA (Hybrid)",
            "department": "Engineering",
            "employment_type": "Full-time",
            "description": "Lead backend development for our battery management platform.",
            "salary_range": "$180,000 - $220,000",
        },
        {
            "title": "Product Designer",
            "url": "https://novatech-labs.com/careers/product-designer",
            "location": "Remote (US)",
            "department": "Design",
            "employment_type": "Full-time",
            "description": "Own the end-to-end design process for our monitoring dashboard.",
        },
        {
            "title": "Data Scientist Intern",
            "url": "https://novatech-labs.com/careers/data-scientist-intern",
            "location": "Austin, TX",
            "department": "Data Science",
            "employment_type": "Internship",
            "description": (
                "Summer 2026 internship. Build predictive models for battery degradation."
            ),
        },
    ]
)

# ------------------------------------------------------------------
# Table careers fixture (4 jobs, tabular layout)
# ------------------------------------------------------------------

TABLE_LLM_RESPONSE = _make_llm_response(
    [
        {
            "title": "DevOps Engineer",
            "url": "https://meridianhealth.com/jobs/devops-engineer",
            "location": "Chicago, IL",
            "department": "Infrastructure",
            "employment_type": "Full-time",
        },
        {
            "title": "Clinical Data Analyst",
            "url": "https://meridianhealth.com/jobs/clinical-data-analyst",
            "location": "Remote",
            "department": "Analytics",
            "employment_type": "Full-time",
        },
        {
            "title": "Mobile Developer (iOS)",
            "url": "https://meridianhealth.com/jobs/ios-developer",
            "location": "Boston, MA",
            "department": "Engineering",
            "employment_type": "Contract",
        },
        {
            "title": "VP of Product",
            "url": "https://meridianhealth.com/jobs/vp-product",
            "location": "New York, NY",
            "department": "Product",
            "employment_type": "Full-time",
        },
    ]
)

# ------------------------------------------------------------------
# SPA-rendered careers fixture (4 jobs, JS-rendered)
# ------------------------------------------------------------------

SPA_LLM_RESPONSE = _make_llm_response(
    [
        {
            "title": "Staff ML Infrastructure Engineer",
            "url": "https://cloudburst.ai/careers/eng-001",
            "location": "San Francisco, CA",
            "department": "Engineering",
            "employment_type": "Full-time",
            "salary_range": "$250,000 - $320,000 + equity",
            "description": "Design and scale distributed training infrastructure.",
            "requirements": [
                "7+ years in distributed systems or ML infrastructure",
                "Deep experience with CUDA, NCCL, or similar GPU programming",
            ],
        },
        {
            "title": "Senior Frontend Engineer",
            "url": "https://cloudburst.ai/careers/eng-002",
            "location": "Remote (US/EU)",
            "department": "Engineering",
            "employment_type": "Full-time",
            "salary_range": "$180,000 - $230,000 + equity",
        },
        {
            "title": "Solutions Architect",
            "url": "https://cloudburst.ai/careers/gtm-001",
            "location": "New York, NY or Remote",
            "department": "Go-to-Market",
            "employment_type": "Full-time",
        },
        {
            "title": "Technical Writer",
            "url": "https://cloudburst.ai/careers/gtm-002",
            "location": "Remote",
            "department": "Go-to-Market",
            "employment_type": "Part-time",
        },
    ]
)


# ------------------------------------------------------------------
# parse() — core extraction with mocked LLM
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackParse:
    """Core parse() method: HTML content → LLM → list[JobListing]."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_parse_startup_inline_careers(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")

        result = parser.parse(content, url="https://novatech-labs.com/careers")

        assert len(result) == 3
        assert all(isinstance(j, JobListing) for j in result)

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_parse_table_careers(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = TABLE_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("table_careers.html")

        result = parser.parse(content, url="https://meridianhealth.com/careers")

        assert len(result) == 4
        assert all(isinstance(j, JobListing) for j in result)

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_parse_spa_rendered_careers(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = SPA_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("spa_rendered_careers.html")

        result = parser.parse(content, url="https://cloudburst.ai/careers")

        assert len(result) == 4
        assert all(isinstance(j, JobListing) for j in result)

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_all_three_fixtures_return_joblistings(self, mock_litellm: MagicMock) -> None:
        """AC: Tests with saved HTML from 3+ real career pages without recognized ATS."""
        fixtures_and_responses = [
            ("startup_inline_careers.html", STARTUP_LLM_RESPONSE, 3),
            ("table_careers.html", TABLE_LLM_RESPONSE, 4),
            ("spa_rendered_careers.html", SPA_LLM_RESPONSE, 4),
        ]
        for fixture_name, llm_response, expected_count in fixtures_and_responses:
            mock_litellm.completion.return_value = llm_response
            parser = LLMFallbackParser()
            content = _load_fixture(fixture_name)
            result = parser.parse(content, url="https://example.com/careers")

            assert len(result) == expected_count, (
                f"Expected {expected_count} jobs from {fixture_name}, got {len(result)}"
            )
            for job in result:
                assert isinstance(job, JobListing)
                assert job.ats_provider == ATSProvider.UNKNOWN

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_sends_content_to_llm(self, mock_litellm: MagicMock) -> None:
        """Verify the parser actually sends page content to the LLM."""
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Hello</body></html>", url="https://example.com")

        mock_litellm.completion.assert_called_once()
        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        assert any("Hello" in str(m.get("content", "")) for m in messages)


# ------------------------------------------------------------------
# parse_async() — PCC-1606 non-blocking LLM path
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackParseAsync:
    """parse_async runs litellm in a thread pool so the event loop stays free."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    async def test_parse_async_matches_sync_parse(self, mock_litellm: MagicMock) -> None:
        """AC3: Same extraction results as sync parse() for identical LLM output."""
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        url = "https://novatech-labs.com/careers"

        sync_jobs = parser.parse(content, url=url)
        mock_litellm.completion.reset_mock()
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        async_jobs = await parser.parse_async(content, url=url)

        assert len(sync_jobs) == len(async_jobs)
        for a, b in zip(sync_jobs, async_jobs, strict=True):
            assert a.model_dump() == b.model_dump()

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    async def test_parse_async_allows_other_coroutines_during_llm(
        self,
        mock_litellm: MagicMock,
    ) -> None:
        """AC1: Another task runs while the (slow) sync completion executes in a thread."""
        order: list[str] = []

        def slow_completion(*_a: object, **_kw: object) -> MagicMock:
            time.sleep(0.2)
            order.append("llm_done")
            return STARTUP_LLM_RESPONSE

        mock_litellm.completion.side_effect = slow_completion
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")

        async def buddy() -> None:
            await asyncio.sleep(0.02)
            order.append("buddy_done")

        await asyncio.gather(
            parser.parse_async(content, url="https://novatech-labs.com/careers"),
            buddy(),
        )
        assert order.index("buddy_done") < order.index("llm_done")


# ------------------------------------------------------------------
# Field mapping
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackFieldMapping:
    """Verify LLM-extracted fields map correctly to JobListing."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_title(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].title == "Senior Python Engineer"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_url(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert str(result[0].url) == "https://novatech-labs.com/careers/senior-python-engineer"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_location(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].location == "San Francisco, CA (Hybrid)"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_department(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].department == "Engineering"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_employment_type(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].employment_type == "Full-time"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_salary_range(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].salary_range == "$180,000 - $220,000"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_description(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert result[0].description is not None
        assert "battery management" in result[0].description

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_requirements_list(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = SPA_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("spa_rendered_careers.html")
        result = parser.parse(content, url="https://cloudburst.ai/careers")
        assert len(result[0].requirements) >= 2
        assert "7+ years in distributed systems or ML infrastructure" in result[0].requirements

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_ats_provider_is_unknown(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = STARTUP_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("startup_inline_careers.html")
        result = parser.parse(content, url="https://novatech-labs.com/careers")
        assert all(j.ats_provider == ATSProvider.UNKNOWN for j in result)

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_missing_optional_fields_default_none(self, mock_litellm: MagicMock) -> None:
        """Jobs without optional fields should still parse with None defaults."""
        mock_litellm.completion.return_value = TABLE_LLM_RESPONSE
        parser = LLMFallbackParser()
        content = _load_fixture("table_careers.html")
        result = parser.parse(content, url="https://meridianhealth.com/careers")
        devops = result[0]
        assert devops.salary_range is None
        assert devops.description is None


# ------------------------------------------------------------------
# Configurable LLM provider
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackProvider:
    """AC: Configurable LLM provider via llm_provider parameter."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_default_provider_is_gemini_flash(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Open positions here</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        model = call_kwargs.kwargs.get("model") or call_kwargs[1].get("model")
        assert "gemini" in model.lower()

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_custom_provider(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser(llm_provider="openai/gpt-4o-mini")
        parser.parse("<html><body>Open positions here</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        model = call_kwargs.kwargs.get("model") or call_kwargs[1].get("model")
        assert model == "openai/gpt-4o-mini"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_provider_stored_on_instance(self, mock_litellm: MagicMock) -> None:
        parser = LLMFallbackParser(llm_provider="anthropic/claude-3-haiku")
        assert parser._model == "anthropic/claude-3-haiku"


# ------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackErrorHandling:
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_llm_exception_returns_empty(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.side_effect = Exception("API error")
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_malformed_json_response_returns_empty(self, mock_litellm: MagicMock) -> None:
        message = MagicMock()
        message.content = "This is not JSON at all"
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_json_without_jobs_key_returns_empty(self, mock_litellm: MagicMock) -> None:
        message = MagicMock()
        message.content = '{"unexpected": "format"}'
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_empty_choices_returns_empty(self, mock_litellm: MagicMock) -> None:
        response = MagicMock()
        response.choices = []
        mock_litellm.completion.return_value = response
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert result == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_job_missing_title_skipped(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response(
            [
                {"url": "https://example.com/job1"},
                {"title": "Valid Job", "url": "https://example.com/job2"},
            ]
        )
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert len(result) == 1
        assert result[0].title == "Valid Job"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_job_missing_url_skipped(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response(
            [
                {"title": "No URL Job"},
                {"title": "Valid Job", "url": "https://example.com/job2"},
            ]
        )
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")
        assert len(result) == 1
        assert result[0].title == "Valid Job"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_empty_content_returns_empty(self, mock_litellm: MagicMock) -> None:
        parser = LLMFallbackParser()
        result = parser.parse("", url="https://example.com")
        assert result == []
        mock_litellm.completion.assert_not_called()


# ------------------------------------------------------------------
# HTML cleaning
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackHTMLCleaning:
    """Verify HTML is cleaned/reduced before sending to LLM."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_strips_script_tags(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        html = "<html><script>alert('x')</script><body>Jobs here</body></html>"
        parser.parse(html, url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "alert" not in user_content
        assert "Jobs here" in user_content

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_strips_style_tags(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        html = "<html><style>.x{color:red}</style><body>Jobs here</body></html>"
        parser.parse(html, url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "color:red" not in user_content

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_preserves_link_urls(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        html = '<html><body><a href="https://example.com/apply">Apply</a></body></html>'
        parser.parse(html, url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "https://example.com/apply" in user_content


# ------------------------------------------------------------------
# Structured extraction prompt
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackPrompt:
    """Verify the extraction prompt is well-structured."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_system_prompt_requests_json(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        system_msg = next(m["content"] for m in messages if m["role"] == "system")
        assert "json" in system_msg.lower() or "JSON" in system_msg

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_user_prompt_includes_url(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com/careers")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        user_msg = next(m["content"] for m in messages if m["role"] == "user")
        assert "https://example.com/careers" in user_msg

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_prompt_specifies_required_fields(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        all_content = " ".join(m["content"] for m in messages)
        assert "title" in all_content
        assert "url" in all_content
