"""Tests for LLMFallbackParser — LLM-based extraction for unknown career pages (PCC-1425).

Covers all acceptance criteria:
- Sends page content to LLM with structured extraction prompt
- Configurable LLM provider (default Gemini Flash)
- Returns list[JobListing] in same format as ATS parsers
- Handles: multi-page career sites, single-page job boards, JS-rendered content
- Tests with saved HTML from 3+ real career pages without recognized ATS

PCC-1969 additions:
- TruncatedCompletionError on finish_reason == "length"
- Retry with doubled max_tokens on truncation
- json-repair salvage on malformed JSON
- ParseStatus tracking (clean / salvaged / truncated / failed)
- salvage_rate > 10% emits WARN
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from strata_harvest.models import ATSProvider, JobListing, ParseStatus
from strata_harvest.parsers.llm_fallback import (
    _DEFAULT_MAX_TOKENS,
    LLMFallbackParser,
    ParseResult,
    ParseStatusTracker,
    TruncatedCompletionError,
    get_parse_tracker,
)

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
# api_base forwarding (PCC-1685)
# ------------------------------------------------------------------


@pytest.mark.verification
class TestLLMFallbackApiBase:
    """AC: api_base and api_key are forwarded to litellm.completion when set."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_api_base_forwarded_to_litellm(self, mock_litellm: MagicMock) -> None:
        """When api_base is provided, litellm.completion receives api_base and api_key."""
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser(api_base="http://192.168.50.220:8080")
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
        assert kwargs.get("api_base") == "http://192.168.50.220:8080"
        assert kwargs.get("api_key") == "not-required"

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_api_base_none_no_kwarg(self, mock_litellm: MagicMock) -> None:
        """When api_base is not set, api_base and api_key are NOT sent to litellm."""
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        call_kwargs = mock_litellm.completion.call_args
        kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
        assert "api_base" not in kwargs
        assert "api_key" not in kwargs


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


# -----------------------------------------------------------------------
# PCC-1969: Truncation detection
# -----------------------------------------------------------------------


def _make_truncated_response(content: str = '{"jobs": [{"title": "Eng"') -> MagicMock:
    """Build a mock litellm response with finish_reason='length'."""
    message = MagicMock()
    message.content = content
    choice = MagicMock()
    choice.message = message
    choice.finish_reason = "length"
    response = MagicMock()
    response.choices = [choice]
    return response


@pytest.mark.verification
class TestTruncationDetection:
    """finish_reason == 'length' raises TruncatedCompletionError."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_finish_reason_length_raises(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_truncated_response()
        parser = LLMFallbackParser()
        with pytest.raises(TruncatedCompletionError):
            parser._completion_sync("some content", "https://example.com")

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_finish_reason_stop_does_not_raise(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        # Should not raise — MagicMock finish_reason != "length"
        parser._completion_sync("some content", "https://example.com")

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_truncation_error_message_includes_url(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_truncated_response()
        parser = LLMFallbackParser()
        with pytest.raises(TruncatedCompletionError, match="https://example.com"):
            parser._completion_sync("content", "https://example.com")

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_max_tokens_forwarded_to_litellm(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser._completion_sync("content", "https://example.com", max_tokens=2048)

        kw = mock_litellm.completion.call_args.kwargs
        assert kw.get("max_tokens") == 2048

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_no_max_tokens_arg_omitted(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        parser._completion_sync("content", "https://example.com")

        kw = mock_litellm.completion.call_args.kwargs
        assert "max_tokens" not in kw


# -----------------------------------------------------------------------
# PCC-1969: Truncation remediation
# -----------------------------------------------------------------------


@pytest.mark.verification
class TestTruncationRemediation:
    """Retry with doubled tokens; fall back to chunked parse on second truncation."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_truncation_triggers_retry(self, mock_litellm: MagicMock) -> None:
        """First call truncated → second call succeeds with jobs."""
        good_response = _make_llm_response([{"title": "Eng", "url": "https://example.com/eng"}])
        mock_litellm.completion.side_effect = [
            _make_truncated_response(),
            good_response,
        ]
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        assert len(result) == 1
        assert result[0].title == "Eng"
        assert mock_litellm.completion.call_count == 2

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_retry_uses_doubled_tokens(self, mock_litellm: MagicMock) -> None:
        """Retry call must pass max_tokens == DEFAULT_MAX_TOKENS * 2."""
        good_response = _make_llm_response([])
        mock_litellm.completion.side_effect = [
            _make_truncated_response(),
            good_response,
        ]
        parser = LLMFallbackParser()
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        retry_call = mock_litellm.completion.call_args_list[1]
        assert retry_call.kwargs.get("max_tokens") == _DEFAULT_MAX_TOKENS * 2

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_retry_uses_custom_max_tokens_doubled(self, mock_litellm: MagicMock) -> None:
        """When max_tokens set on parser, retry uses that value * 2."""
        good_response = _make_llm_response([])
        mock_litellm.completion.side_effect = [
            _make_truncated_response(),
            good_response,
        ]
        parser = LLMFallbackParser(max_tokens=1024)
        parser.parse("<html><body>Jobs</body></html>", url="https://example.com")

        retry_call = mock_litellm.completion.call_args_list[1]
        assert retry_call.kwargs.get("max_tokens") == 2048

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_second_truncation_returns_truncated_status(self, mock_litellm: MagicMock) -> None:
        """Both calls truncated → chunked parse attempts; if all chunks fail, TRUNCATED."""
        mock_litellm.completion.side_effect = TruncatedCompletionError("truncated")
        parser = LLMFallbackParser()
        result = parser.parse_with_status(
            "<html><body>Jobs</body></html>", url="https://example.com"
        )
        assert result.parse_status == ParseStatus.TRUNCATED
        assert result.jobs == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_truncation_parse_status_is_clean_after_recovery(self, mock_litellm: MagicMock) -> None:
        """Successful retry → parse_status reflects actual JSON quality (CLEAN)."""
        good_response = _make_llm_response([{"title": "Eng", "url": "https://example.com/eng"}])
        mock_litellm.completion.side_effect = [
            _make_truncated_response(),
            good_response,
        ]
        parser = LLMFallbackParser()
        result = parser.parse_with_status(
            "<html><body>Jobs</body></html>", url="https://example.com"
        )
        assert result.parse_status == ParseStatus.CLEAN

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    async def test_async_truncation_retries(self, mock_litellm: MagicMock) -> None:
        """Async path also triggers retry on truncation."""
        good_response = _make_llm_response([{"title": "Eng", "url": "https://example.com/eng"}])
        mock_litellm.completion.side_effect = [
            _make_truncated_response(),
            good_response,
        ]
        parser = LLMFallbackParser()
        result = await parser.parse_async(
            "<html><body>Jobs</body></html>", url="https://example.com"
        )
        assert len(result) == 1
        assert mock_litellm.completion.call_count == 2


# -----------------------------------------------------------------------
# PCC-1969: json-repair salvage
# -----------------------------------------------------------------------


def _make_malformed_response(bad_json: str) -> MagicMock:
    message = MagicMock()
    message.content = bad_json
    choice = MagicMock()
    choice.message = message
    choice.finish_reason = "stop"
    response = MagicMock()
    response.choices = [choice]
    return response


@pytest.mark.verification
class TestJsonRepairSalvage:
    """json-repair parses malformed JSON and tags result as SALVAGED."""

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_trailing_comma_salvaged(self, mock_litellm: MagicMock, mock_repair: MagicMock) -> None:
        bad_json = '{"jobs": [{"title": "Eng", "url": "https://ex.com/e"}],}'
        mock_litellm.completion.return_value = _make_malformed_response(bad_json)
        mock_repair.loads.return_value = {"jobs": [{"title": "Eng", "url": "https://ex.com/e"}]}
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.SALVAGED
        assert len(result.jobs) == 1
        assert result.jobs[0].title == "Eng"

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_unclosed_string_salvaged(
        self, mock_litellm: MagicMock, mock_repair: MagicMock
    ) -> None:
        bad_json = '{"jobs": [{"title": "Eng", "url": "https://ex.com/e"}'
        mock_litellm.completion.return_value = _make_malformed_response(bad_json)
        mock_repair.loads.return_value = {"jobs": [{"title": "Eng", "url": "https://ex.com/e"}]}
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.SALVAGED

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_malformed_json_without_repair_returns_failed(self, mock_litellm: MagicMock) -> None:
        """When json_repair is None, malformed JSON → FAILED."""
        mock_litellm.completion.return_value = _make_malformed_response('{"jobs": [{"title": "Eng"')
        parser = LLMFallbackParser()
        with patch("strata_harvest.parsers.llm_fallback.json_repair", None):
            result = parser.parse_with_status(
                "<html><body>Jobs</body></html>", url="https://ex.com"
            )
        assert result.parse_status == ParseStatus.FAILED
        assert result.jobs == []

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_clean_json_not_salvaged(self, mock_litellm: MagicMock, mock_repair: MagicMock) -> None:
        """Valid JSON does NOT call json_repair."""
        mock_litellm.completion.return_value = _make_llm_response(
            [{"title": "Eng", "url": "https://ex.com/e"}]
        )
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.CLEAN
        mock_repair.loads.assert_not_called()

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_json_repair_failure_returns_failed(
        self, mock_litellm: MagicMock, mock_repair: MagicMock
    ) -> None:
        """If json_repair also raises, result is FAILED."""
        mock_litellm.completion.return_value = _make_malformed_response("not json at all")
        mock_repair.loads.side_effect = Exception("can't repair")
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.FAILED


# -----------------------------------------------------------------------
# PCC-1969: ParseStatusTracker
# -----------------------------------------------------------------------


@pytest.mark.verification
class TestParseStatusTracker:
    """Unit tests for ParseStatusTracker rolling-window logic."""

    def test_records_and_counts(self) -> None:
        t = ParseStatusTracker()
        t.record("example.com", "m", ParseStatus.CLEAN)
        t.record("example.com", "m", ParseStatus.SALVAGED)
        counts = t.status_counts("example.com", "m")
        assert counts["clean"] == 1
        assert counts["salvaged"] == 1
        assert counts["truncated"] == 0
        assert counts["failed"] == 0

    def test_salvage_rate_calculation(self) -> None:
        t = ParseStatusTracker()
        for _ in range(9):
            t.record("s", "m", ParseStatus.CLEAN)
        t.record("s", "m", ParseStatus.SALVAGED)
        assert abs(t.salvage_rate("s", "m") - 0.10) < 1e-9

    def test_salvage_rate_zero_when_empty(self) -> None:
        t = ParseStatusTracker()
        assert t.salvage_rate("s", "m") == 0.0

    def test_purges_old_events(self) -> None:
        t = ParseStatusTracker()
        old_ts = time.time() - 90_000  # 25h ago
        t._events["s|m"].append((old_ts, ParseStatus.SALVAGED))
        t.record("s", "m", ParseStatus.CLEAN)
        counts = t.status_counts("s", "m")
        assert counts["salvaged"] == 0
        assert counts["clean"] == 1

    def test_different_sources_isolated(self) -> None:
        t = ParseStatusTracker()
        t.record("a.com", "m", ParseStatus.FAILED)
        t.record("b.com", "m", ParseStatus.CLEAN)
        assert t.status_counts("a.com", "m")["failed"] == 1
        assert t.status_counts("b.com", "m")["failed"] == 0

    def test_different_models_isolated(self) -> None:
        t = ParseStatusTracker()
        t.record("s", "model-a", ParseStatus.FAILED)
        t.record("s", "model-b", ParseStatus.CLEAN)
        assert t.status_counts("s", "model-a")["failed"] == 1
        assert t.status_counts("s", "model-b")["failed"] == 0

    def test_get_parse_tracker_returns_singleton(self) -> None:
        assert get_parse_tracker() is get_parse_tracker()


# -----------------------------------------------------------------------
# PCC-1969: Salvage-rate warning telemetry
# -----------------------------------------------------------------------


@pytest.mark.verification
class TestSalvageRateWarning:
    """salvage_rate > 10% over 24h emits logger.warning with source name."""

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_high_salvage_rate_emits_warning(
        self, mock_litellm: MagicMock, mock_repair: MagicMock
    ) -> None:
        bad_json = '{"jobs": [{"title": "J", "url": "https://hi.com/j"}],}'
        mock_repair.loads.return_value = {"jobs": [{"title": "J", "url": "https://hi.com/j"}]}

        # Fresh tracker so rate is deterministic
        fresh_tracker = ParseStatusTracker()

        parser = LLMFallbackParser()
        with patch("strata_harvest.parsers.llm_fallback._tracker", fresh_tracker):
            # Record 9 clean + 1 salvaged = exactly 10% (not > threshold)
            for _ in range(9):
                mock_litellm.completion.return_value = _make_llm_response(
                    [{"title": "J", "url": "https://hi.com/j"}]
                )
                parser.parse("<html><body>J</body></html>", url="https://hi.com")

            # 11th call is salvaged → now 1/10 = 10% — still not above threshold
            mock_litellm.completion.return_value = _make_malformed_response(bad_json)
            parser.parse("<html><body>J</body></html>", url="https://hi.com")

            # 12th call is also salvaged → 2/11 ≈ 18% > 10%
            with patch("strata_harvest.parsers.llm_fallback.logger") as mock_logger:
                mock_litellm.completion.return_value = _make_malformed_response(bad_json)
                parser.parse("<html><body>J</body></html>", url="https://hi.com")

        mock_logger.warning.assert_called()
        warning_args = mock_logger.warning.call_args_list[-1]
        # Source hostname should appear in warning
        assert "hi.com" in str(warning_args)

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_low_salvage_rate_no_warning(
        self, mock_litellm: MagicMock, mock_repair: MagicMock
    ) -> None:
        bad_json = '{"jobs": [],}'
        mock_repair.loads.return_value = {"jobs": []}

        fresh_tracker = ParseStatusTracker()
        parser = LLMFallbackParser()

        with patch("strata_harvest.parsers.llm_fallback._tracker", fresh_tracker):
            # 1 salvaged + 99 clean = 1% — well below threshold
            mock_litellm.completion.return_value = _make_malformed_response(bad_json)
            parser.parse("<html><body>J</body></html>", url="https://low.com")
            for _ in range(99):
                mock_litellm.completion.return_value = _make_llm_response([])
                with patch("strata_harvest.parsers.llm_fallback.logger") as mock_logger:
                    parser.parse("<html><body>J</body></html>", url="https://low.com")

        # No warning about salvage rate should have been emitted in final iteration
        for c in mock_logger.warning.call_args_list:
            assert "salvage" not in str(c).lower()


# -----------------------------------------------------------------------
# PCC-1969: All four parse_status outcomes
# -----------------------------------------------------------------------


@pytest.mark.verification
class TestAllFourParseStatuses:
    """Unit tests ensuring each ParseStatus outcome can be produced."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_clean_status(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response(
            [{"title": "Eng", "url": "https://ex.com/eng"}]
        )
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.CLEAN
        assert len(result.jobs) == 1

    @patch("strata_harvest.parsers.llm_fallback.json_repair")
    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_salvaged_status(self, mock_litellm: MagicMock, mock_repair: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_malformed_response(
            '{"jobs": [{"title": "Eng", "url": "https://ex.com/eng"}]'  # missing }
        )
        mock_repair.loads.return_value = {"jobs": [{"title": "Eng", "url": "https://ex.com/eng"}]}
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.SALVAGED

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_truncated_status(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.side_effect = TruncatedCompletionError("truncated")
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.TRUNCATED
        assert result.jobs == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_failed_status_on_exception(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.side_effect = Exception("network error")
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert result.parse_status == ParseStatus.FAILED
        assert result.jobs == []

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_failed_status_on_unrepaired_json(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_malformed_response("not json")
        parser = LLMFallbackParser()
        with patch("strata_harvest.parsers.llm_fallback.json_repair", None):
            result = parser.parse_with_status(
                "<html><body>Jobs</body></html>", url="https://ex.com"
            )
        assert result.parse_status == ParseStatus.FAILED

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_parse_result_is_named_tuple(self, mock_litellm: MagicMock) -> None:
        mock_litellm.completion.return_value = _make_llm_response([])
        parser = LLMFallbackParser()
        result = parser.parse_with_status("<html><body>Jobs</body></html>", url="https://ex.com")
        assert isinstance(result, ParseResult)
        assert isinstance(result.jobs, list)
        assert isinstance(result.parse_status, ParseStatus)

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_parse_with_status_parse_backward_compat(self, mock_litellm: MagicMock) -> None:
        """parse() still returns list[JobListing] (backward compat)."""
        mock_litellm.completion.return_value = _make_llm_response(
            [{"title": "Eng", "url": "https://ex.com/eng"}]
        )
        parser = LLMFallbackParser()
        result = parser.parse("<html><body>Jobs</body></html>", url="https://ex.com")
        assert isinstance(result, list)
        assert len(result) == 1
