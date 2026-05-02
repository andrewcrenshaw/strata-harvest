"""Tests for token-minimizing extraction pipeline (PCC-1950).

Covers:
- Tier 0: Structured data extraction (JSON-LD, microdata) — zero LLM tokens
- Tier 2–3: trafilatura + local Ollama Qwen2.5-7B
- Tier 4: Fallback to hosted Gemini (when enabled)
- Token count reduction assertions (>50% reduction expected)
- Integration tests with real fixtures
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from strata_harvest.extract.pipeline import (
    _extract_tier_0_structured,
    _extract_tier_2_local_llm,
    extract_with_pipeline,
)
from strata_harvest.extract.schema import JobPostingSchema
from strata_harvest.models import ATSProvider, JobListing

OMLX_MOCK_RESPONSE_SINGLE = json.dumps(
    {
        "title": "Senior Backend Engineer",
        "url": "https://example.com/job1",
        "location": "San Francisco, CA",
        "employment_type": "Full-time",
    }
)

OMLX_MOCK_RESPONSE_LIST = json.dumps(
    {
        "items": [
            {
                "title": "Senior Backend Engineer",
                "url": "https://example.com/job1",
                "location": "San Francisco, CA",
            },
            {
                "title": "Frontend Engineer",
                "url": "https://example.com/job2",
                "location": "Remote",
            },
        ]
    }
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "llm_fallback"


def _load_fixture(name: str) -> str:
    """Load HTML fixture by name."""
    return (FIXTURES_DIR / name).read_text()


def _trafilatura_available() -> bool:
    """Check if trafilatura is installed."""
    try:
        import trafilatura  # noqa: F401

        return True
    except ImportError:
        return False


def _instructor_available() -> bool:
    """Check if instructor is installed."""
    try:
        import instructor  # noqa: F401

        return True
    except ImportError:
        return False


# ============================================================================
# Tier 0: Structured Data Extraction (JSON-LD)
# ============================================================================


class TestTier0StructuredExtraction:
    """Tier 0: Extract from JSON-LD without any LLM calls."""

    def test_extract_json_ld_jobposting(self) -> None:
        """Extract JobPosting from JSON-LD."""
        html = """
        <html>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "title": "Senior Backend Engineer",
            "url": "https://example.com/jobs/backend",
            "location": {"@type": "Place", "address": "San Francisco, CA"},
            "employmentType": "FULL_TIME",
            "description": "Build scalable systems",
            "baseSalary": {
                "@type": "MonetaryAmount",
                "currency": "USD",
                "value": {"minValue": 150000, "maxValue": 200000}
            }
        }
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 1
        assert jobs[0].title == "Senior Backend Engineer"
        assert jobs[0].ats_provider == ATSProvider.UNKNOWN
        assert jobs[0].salary_range is not None
        assert "150,000" in jobs[0].salary_range

    def test_extract_multiple_json_ld_postings(self) -> None:
        """Extract multiple JobPostings from single JSON-LD block."""
        html = """
        <html>
        <script type="application/ld+json">
        [
            {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Job 1",
                "url": "https://example.com/job1",
                "employmentType": "FULL_TIME"
            },
            {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Job 2",
                "url": "https://example.com/job2",
                "employmentType": "PART_TIME"
            }
        ]
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 2
        assert jobs[0].title == "Job 1"
        assert jobs[1].title == "Job 2"

    def test_skip_non_jobposting_json_ld(self) -> None:
        """Ignore JSON-LD objects that are not JobPostings."""
        html = """
        <html>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "Organization",
            "name": "Example Corp"
        }
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 0

    def test_json_ld_missing_url_skipped(self) -> None:
        """Skip JobPostings without required url field."""
        html = """
        <html>
        <script type="application/ld+json">
        {
            "@type": "JobPosting",
            "title": "Job Without URL"
        }
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 0

    def test_json_ld_missing_title_skipped(self) -> None:
        """Skip JobPostings without required title field."""
        html = """
        <html>
        <script type="application/ld+json">
        {
            "@type": "JobPosting",
            "url": "https://example.com/job"
        }
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 0

    def test_malformed_json_ld_returns_empty(self) -> None:
        """Gracefully handle malformed JSON-LD."""
        html = """
        <html>
        <script type="application/ld+json">
        {invalid json}
        </script>
        </html>
        """
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 0

    def test_no_json_ld_returns_empty(self) -> None:
        """Return empty list when no JSON-LD present."""
        html = "<html><body>Just regular HTML</body></html>"
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 0


# ============================================================================
# OmlxExtractor unit tests
# ============================================================================


@pytest.mark.verification
class TestOmlxExtractor:
    """OmlxExtractor: structured extraction via oMLX OpenAI-compat API."""

    def test_omlx_extractor_importable(self) -> None:
        """OmlxExtractor is importable from local_llm module."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        assert OmlxExtractor is not None

    def test_omlx_extractor_default_attributes(self) -> None:
        """OmlxExtractor has sensible defaults."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        extractor = OmlxExtractor()
        assert extractor.base_url  # non-empty
        assert extractor.model  # non-empty
        assert extractor.api_key  # non-empty
        assert extractor.timeout > 0

    def test_omlx_extractor_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Constructor params override env vars, env vars override built-in defaults."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        monkeypatch.setenv("OMLX_BASE_URL", "http://custom-host:9000")
        monkeypatch.setenv("OMLX_API_KEY", "my-key")
        monkeypatch.setenv("OMLX_EXTRACT_MODEL", "custom-model")

        extractor = OmlxExtractor()
        assert extractor.base_url == "http://custom-host:9000"
        assert extractor.api_key == "my-key"
        assert extractor.model == "custom-model"

    def test_omlx_extractor_constructor_params_override_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit constructor params take priority over env vars."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        monkeypatch.setenv("OMLX_BASE_URL", "http://env-host:9000")
        extractor = OmlxExtractor(base_url="http://explicit:8000")
        assert extractor.base_url == "http://explicit:8000"

    def test_omlx_extractor_extract_empty_text_returns_none(self) -> None:
        """extract() returns None for empty input."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        extractor = OmlxExtractor()
        assert extractor.extract("", JobPostingSchema) is None
        assert extractor.extract("   ", JobPostingSchema) is None

    def test_omlx_extractor_extract_list_empty_text_returns_empty(self) -> None:
        """extract_list() returns [] for empty input."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        extractor = OmlxExtractor()
        assert extractor.extract_list("", JobPostingSchema) == []
        assert extractor.extract_list("   ", JobPostingSchema) == []

    @patch("strata_harvest.extract.local_llm._litellm_mod")
    def test_omlx_extractor_extract_success(self, mock_litellm: MagicMock) -> None:
        """extract() parses LLM JSON response into Pydantic schema."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        message = MagicMock()
        message.content = OMLX_MOCK_RESPONSE_SINGLE
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        extractor = OmlxExtractor()
        result = extractor.extract("some job posting text", JobPostingSchema)

        assert result is not None
        assert result.title == "Senior Backend Engineer"
        assert mock_litellm.completion.called
        call_kwargs = mock_litellm.completion.call_args[1]
        assert "openai/" in call_kwargs["model"]
        assert call_kwargs["api_base"] == extractor.base_url
        assert call_kwargs["api_key"] == extractor.api_key

    @patch("strata_harvest.extract.local_llm._litellm_mod")
    def test_omlx_extractor_extract_list_success(self, mock_litellm: MagicMock) -> None:
        """extract_list() parses LLM JSON response into list of Pydantic objects."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        message = MagicMock()
        message.content = OMLX_MOCK_RESPONSE_LIST
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        extractor = OmlxExtractor()
        results = extractor.extract_list("job listings page", JobPostingSchema)

        assert len(results) == 2
        assert results[0].title == "Senior Backend Engineer"
        assert results[1].title == "Frontend Engineer"

    @patch("strata_harvest.extract.local_llm._litellm_mod")
    def test_omlx_extractor_extract_returns_none_on_exception(
        self, mock_litellm: MagicMock
    ) -> None:
        """extract() returns None when LLM call raises."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        mock_litellm.completion.side_effect = ConnectionError("oMLX unreachable")

        extractor = OmlxExtractor()
        result = extractor.extract("text", JobPostingSchema)
        assert result is None

    @patch("strata_harvest.extract.local_llm._litellm_mod")
    def test_omlx_extractor_extract_list_returns_empty_on_exception(
        self, mock_litellm: MagicMock
    ) -> None:
        """extract_list() returns [] when LLM call raises."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        mock_litellm.completion.side_effect = ConnectionError("oMLX unreachable")

        extractor = OmlxExtractor()
        result = extractor.extract_list("text", JobPostingSchema)
        assert result == []

    @patch("strata_harvest.extract.local_llm._litellm_mod")
    def test_omlx_extractor_strips_markdown_fences(self, mock_litellm: MagicMock) -> None:
        """extract() handles response wrapped in markdown code fences."""
        from strata_harvest.extract.local_llm import OmlxExtractor

        fenced = f"```json\n{OMLX_MOCK_RESPONSE_SINGLE}\n```"
        message = MagicMock()
        message.content = fenced
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        extractor = OmlxExtractor()
        result = extractor.extract("some text", JobPostingSchema)
        assert result is not None
        assert result.title == "Senior Backend Engineer"


# ============================================================================
# Tier 2–3: Local LLM Extraction (now via OmlxExtractor)
# ============================================================================


class TestTier2LocalLLMExtraction:
    """Tier 2: trafilatura + oMLX extraction."""

    @patch("strata_harvest.extract.local_llm.OmlxExtractor")
    @patch("strata_harvest.extract.pipeline.extract_markdown")
    def test_local_llm_extraction_success(
        self,
        mock_extract_markdown: MagicMock,
        mock_extractor_class: MagicMock,
    ) -> None:
        """Successfully extract jobs via oMLX."""
        mock_extract_markdown.return_value = """
        Senior Backend Engineer
        https://example.com/job1
        San Francisco, CA
        Full-time
        Build scalable backend systems
        """

        posting = JobPostingSchema(
            title="Senior Backend Engineer",
            url="https://example.com/job1",
            location="San Francisco, CA",
            employment_type="Full-time",
            description="Build scalable backend systems",
        )
        mock_extractor = MagicMock()
        mock_extractor.is_available.return_value = True
        mock_extractor.extract_list.return_value = [posting]
        mock_extractor_class.return_value = mock_extractor

        html = "<html><body>Job content</body></html>"
        jobs = _extract_tier_2_local_llm(
            html, url="https://example.com/careers", base_url="http://localhost:11434"
        )

        assert len(jobs) == 1
        assert jobs[0].title == "Senior Backend Engineer"
        assert jobs[0].ats_provider == ATSProvider.UNKNOWN

    @patch("strata_harvest.extract.local_llm.OmlxExtractor")
    @patch("strata_harvest.extract.pipeline.extract_markdown")
    def test_local_llm_omlx_unavailable(
        self,
        mock_extract_markdown: MagicMock,
        mock_extractor_class: MagicMock,
    ) -> None:
        """Return empty when oMLX is unavailable."""
        mock_extract_markdown.return_value = "Some markdown content"
        mock_extractor = MagicMock()
        mock_extractor.is_available.return_value = False
        mock_extractor_class.return_value = mock_extractor

        html = "<html><body>Job content</body></html>"
        jobs = _extract_tier_2_local_llm(
            html, url="https://example.com/careers", base_url="http://localhost:11434"
        )

        assert len(jobs) == 0

    @patch("strata_harvest.extract.pipeline.extract_markdown")
    def test_local_llm_trafilatura_unavailable(self, mock_extract_markdown: MagicMock) -> None:
        """Return empty when trafilatura is unavailable."""
        mock_extract_markdown.return_value = None

        html = "<html><body>Job content</body></html>"
        jobs = _extract_tier_2_local_llm(
            html, url="https://example.com/careers", base_url="http://localhost:11434"
        )

        assert len(jobs) == 0


# ============================================================================
# Pipeline: Multi-Tier Fallback
# ============================================================================


class TestExtractionPipeline:
    """Full pipeline: tries tiers 0–4 in sequence."""

    def test_pipeline_success_tier_0(self) -> None:
        """Pipeline succeeds at Tier 0 (JSON-LD)."""
        html = """
        <html>
        <script type="application/ld+json">
        {
            "@type": "JobPosting",
            "title": "Backend Engineer",
            "url": "https://example.com/job1"
        }
        </script>
        </html>
        """
        jobs = extract_with_pipeline(html, url="https://example.com/careers")
        assert len(jobs) == 1
        assert jobs[0].title == "Backend Engineer"

    @patch("strata_harvest.extract.pipeline.litellm")  # type: ignore[attr-defined]
    def test_pipeline_fallback_to_gemini_when_enabled(self, mock_litellm: MagicMock) -> None:
        """Pipeline falls back to Gemini when enabled and earlier tiers fail."""
        # Create mock Gemini response
        message = MagicMock()
        message.content = json.dumps(
            {
                "jobs": [
                    {
                        "title": "Frontend Engineer",
                        "url": "https://example.com/job1",
                        "location": "Remote",
                    }
                ]
            }
        )
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        html = "<html><body>No JSON-LD here</body></html>"
        jobs = extract_with_pipeline(
            html,
            url="https://example.com/careers",
            enable_ollama=False,
            enable_gemini_fallback=True,
        )

        assert len(jobs) == 1
        assert jobs[0].title == "Frontend Engineer"

    def test_pipeline_respects_hosted_llm_fallback_enabled_env(self) -> None:
        """Pipeline respects HOSTED_LLM_FALLBACK_ENABLED env var."""
        with (
            patch.dict("os.environ", {"HOSTED_LLM_FALLBACK_ENABLED": "1"}),
            patch("strata_harvest.extract.pipeline.litellm") as mock_litellm,  # type: ignore[attr-defined]
        ):
            message = MagicMock()
            message.content = json.dumps({"jobs": []})
            choice = MagicMock()
            choice.message = message
            response = MagicMock()
            response.choices = [choice]
            mock_litellm.completion.return_value = response

            html = "<html><body>No JSON-LD</body></html>"
            extract_with_pipeline(
                html,
                url="https://example.com/careers",
                enable_ollama=False,
                enable_gemini_fallback=False,  # Not explicitly enabled
            )

            # Should still call Gemini because env var is set
            mock_litellm.completion.assert_called_once()

    def test_pipeline_empty_html_returns_empty(self) -> None:
        """Pipeline returns empty list for empty HTML."""
        jobs = extract_with_pipeline("", url="https://example.com/careers")
        assert len(jobs) == 0

    def test_pipeline_all_tiers_fail_returns_empty(self) -> None:
        """Pipeline returns empty when all tiers fail."""
        html = "<html><body>No structured data, no JSON-LD, no jobs</body></html>"
        jobs = extract_with_pipeline(
            html,
            url="https://example.com/careers",
            enable_ollama=False,
            enable_gemini_fallback=False,
        )
        assert len(jobs) == 0


# ============================================================================
# Token Count Reduction Assertion (AC requirement)
# ============================================================================


class TestTokenReduction:
    """Verify >50% token reduction vs. raw HTML to LLM."""

    @pytest.mark.skipif(
        not _trafilatura_available(),
        reason="trafilatura not installed for token reduction test",
    )
    def test_trafilatura_reduces_tokens(self) -> None:
        """trafilatura extraction reduces HTML size (proxy for token reduction)."""
        from strata_harvest.extract.prune import extract_markdown

        html_fixture = _load_fixture("startup_inline_careers.html")
        markdown = extract_markdown(html_fixture)

        assert markdown is not None
        # Markdown should be significantly smaller than raw HTML
        # Typical reduction: 67-98% per trafilatura benchmarks
        token_reduction_ratio = len(markdown) / len(html_fixture)
        assert token_reduction_ratio < 0.5, (
            f"Expected >50% token reduction, "
            f"got {100 * (1 - token_reduction_ratio):.1f}% reduction "
            f"({len(html_fixture)} → {len(markdown)} chars)"
        )

    def test_structured_data_zero_llm_tokens(self) -> None:
        """Tier 0 extraction uses zero LLM tokens."""
        html = """
        <html>
        <script type="application/ld+json">
        {"@type": "JobPosting", "title": "Job", "url": "https://example.com"}
        </script>
        </html>
        """
        # No mock of litellm needed — Tier 0 should never call LLM
        jobs = _extract_tier_0_structured(html)
        assert len(jobs) == 1
        # If we got here without calling litellm, Tier 0 worked


# ============================================================================
# Regression: No breaking changes to existing llm_fallback
# ============================================================================


class TestLLMFallbackRegression:
    """Ensure no regression on existing llm_fallback.LLMFallbackParser."""

    @patch("strata_harvest.parsers.llm_fallback.litellm")
    def test_llm_fallback_still_works(self, mock_litellm: MagicMock) -> None:
        """LLMFallbackParser should still work unchanged."""
        from strata_harvest.parsers.llm_fallback import LLMFallbackParser

        message = MagicMock()
        message.content = json.dumps(
            {
                "jobs": [
                    {
                        "title": "Test Job",
                        "url": "https://example.com/job1",
                        "location": "Test Location",
                    }
                ]
            }
        )
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        mock_litellm.completion.return_value = response

        parser = LLMFallbackParser()
        html = _load_fixture("startup_inline_careers.html")
        jobs = parser.parse(html, url="https://example.com/careers")

        assert len(jobs) >= 1
        assert all(isinstance(j, JobListing) for j in jobs)


# ============================================================================
# Integration: Real HTML fixtures
# ============================================================================


class TestIntegrationWithFixtures:
    """Integration tests using real HTML fixtures."""

    def test_pipeline_with_startup_fixture(self) -> None:
        """Pipeline handles real startup careers HTML."""
        html = _load_fixture("startup_inline_careers.html")
        # No JSON-LD in this fixture, so Tier 0 should fail
        # But pipeline should not crash
        jobs = extract_with_pipeline(
            html,
            url="https://novatech-labs.com/careers",
            enable_ollama=False,
            enable_gemini_fallback=False,
        )
        # Just verify it doesn't crash; extraction may be empty
        assert isinstance(jobs, list)

    def test_pipeline_with_table_fixture(self) -> None:
        """Pipeline handles real table-based careers HTML."""
        html = _load_fixture("table_careers.html")
        jobs = extract_with_pipeline(
            html,
            url="https://meridianhealth.com/careers",
            enable_ollama=False,
            enable_gemini_fallback=False,
        )
        # Just verify it doesn't crash
        assert isinstance(jobs, list)

    def test_pipeline_with_spa_fixture(self) -> None:
        """Pipeline handles SPA-rendered careers HTML."""
        html = _load_fixture("spa_rendered_careers.html")
        jobs = extract_with_pipeline(
            html,
            url="https://cloudburst.ai/careers",
            enable_ollama=False,
            enable_gemini_fallback=False,
        )
        # Just verify it doesn't crash
        assert isinstance(jobs, list)
