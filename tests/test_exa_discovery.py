"""Unit tests for strata_harvest.discovery.exa_discovery.

All tests mock the exa_py module — no network calls made.

Since ``exa_py`` is imported lazily inside ``find_career_page``, we patch it via
``sys.modules`` injection rather than a module-level attribute patch.

Covers:
- Scoring logic (_score_url)
- find_career_page happy-path: returns best ATS URL above threshold
- find_career_page: returns None when all scores below threshold
- find_career_page: returns None when Exa returns empty results
- find_career_page: returns None on Exa exception (non-raising)
- find_career_page: raises ImportError when exa-py not installed
- find_career_page: correctly picks best URL among multiple candidates
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from strata_harvest.discovery.exa_discovery import _score_url, find_career_page

if TYPE_CHECKING:
    from collections.abc import Generator

# ---------------------------------------------------------------------------
# Helper to inject a fake exa_py module into sys.modules
# ---------------------------------------------------------------------------


def _make_exa_result(url: str) -> Any:
    """Build a minimal mock Exa result object with a .url attribute."""
    result = MagicMock()
    result.url = url
    return result


def _make_exa_response(urls: list[str]) -> Any:
    """Build a minimal mock Exa response with a .results list."""
    response = MagicMock()
    response.results = [_make_exa_result(u) for u in urls]
    return response


@contextmanager
def _fake_exa(
    response: Any, side_effect: Exception | None = None
) -> Generator[MagicMock, None, None]:
    """Context manager that injects a fake exa_py module into sys.modules.

    Yields the mock Exa *instance* so callers can inspect call args.
    """
    fake_module = MagicMock()
    mock_instance = MagicMock()
    if side_effect is not None:
        mock_instance.search.side_effect = side_effect
    else:
        mock_instance.search.return_value = response
    fake_module.Exa.return_value = mock_instance

    old = sys.modules.get("exa_py")
    sys.modules["exa_py"] = fake_module
    try:
        yield mock_instance
    finally:
        if old is None:
            sys.modules.pop("exa_py", None)
        else:
            sys.modules["exa_py"] = old


# ---------------------------------------------------------------------------
# _score_url unit tests
# ---------------------------------------------------------------------------


class TestScoreUrl:
    def test_greenhouse_boards(self) -> None:
        score = _score_url("https://boards.greenhouse.io/acme/jobs")
        assert score == 1.0

    def test_lever(self) -> None:
        score = _score_url("https://jobs.lever.co/acme")
        assert score == 1.0

    def test_ashby(self) -> None:
        score = _score_url("https://jobs.ashbyhq.com/stripe")
        assert score == 1.0

    def test_smartrecruiters(self) -> None:
        score = _score_url("https://jobs.smartrecruiters.com/acme")
        assert score == 1.0

    def test_rippling(self) -> None:
        score = _score_url("https://ats.rippling.com/acme/jobs")
        assert score >= 0.90

    def test_workday(self) -> None:
        score = _score_url("https://acme.myworkdayjobs.com/en-US/careers")
        assert score >= 0.85

    def test_bamboohr(self) -> None:
        score = _score_url("https://acme.bamboohr.com/careers")
        assert score >= 0.85

    def test_generic_careers_path(self) -> None:
        score = _score_url("https://www.example.com/careers")
        assert 0.0 < score < 1.0

    def test_unknown_domain(self) -> None:
        score = _score_url("https://www.example.com/about")
        assert score == 0.0

    def test_icims(self) -> None:
        score = _score_url("https://careers.company.icims.com/jobs/search")
        assert score > 0.0


# ---------------------------------------------------------------------------
# find_career_page tests
# ---------------------------------------------------------------------------


class TestFindCareerPage:
    """Tests for find_career_page with mocked exa_py module."""

    async def test_returns_best_ats_url(self) -> None:
        """Happy path: Exa returns an Ashby URL → returned as top result."""
        response = _make_exa_response(
            [
                "https://jobs.ashbyhq.com/acme",
                "https://www.acme.com/about",
            ]
        )
        with _fake_exa(response):
            result = await find_career_page("Acme Corp", exa_api_key="exa-test")

        assert result == "https://jobs.ashbyhq.com/acme"

    async def test_returns_none_when_all_below_threshold(self) -> None:
        """All candidates are generic pages — nothing meets min_confidence."""
        response = _make_exa_response(
            [
                "https://www.acme.com/about",
                "https://linkedin.com/company/acme",
            ]
        )
        with _fake_exa(response):
            result = await find_career_page("Acme Corp", exa_api_key="exa-test")

        assert result is None

    async def test_returns_none_for_empty_results(self) -> None:
        """Exa returns no results → None."""
        response = _make_exa_response([])
        with _fake_exa(response):
            result = await find_career_page("Ghost Corp", exa_api_key="exa-test")

        assert result is None

    async def test_returns_none_on_exa_exception(self) -> None:
        """Exa client raises an exception → returns None without raising."""
        with _fake_exa(None, side_effect=RuntimeError("connection refused")):
            result = await find_career_page("Acme Corp", exa_api_key="exa-test")

        assert result is None

    async def test_raises_import_error_when_exa_not_installed(self) -> None:
        """ImportError from exa_py propagates as ImportError with helpful message."""
        old = sys.modules.get("exa_py")
        sys.modules["exa_py"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ImportError, match="exa-py is required"):
                await find_career_page("Acme Corp", exa_api_key="exa-test")
        finally:
            if old is None:
                sys.modules.pop("exa_py", None)
            else:
                sys.modules["exa_py"] = old

    async def test_picks_highest_score_among_candidates(self) -> None:
        """When multiple high-signal URLs are returned, pick the highest-scored."""
        response = _make_exa_response(
            [
                "https://www.acme.com/careers",  # generic (0.50)
                "https://jobs.ashbyhq.com/acme",  # ATS king (1.0)
                "https://jobs.lever.co/acme",  # also ATS king (1.0)
            ]
        )
        with _fake_exa(response):
            result = await find_career_page("Acme", exa_api_key="exa-test")

        # Either Ashby or Lever is fine — both score 1.0; the first wins
        assert result in (
            "https://jobs.ashbyhq.com/acme",
            "https://jobs.lever.co/acme",
        )

    async def test_greenhouse_url_is_accepted(self) -> None:
        """Greenhouse board URL is returned when it's the best result."""
        response = _make_exa_response(["https://boards.greenhouse.io/acme/jobs"])
        with _fake_exa(response):
            result = await find_career_page("Acme Corp", exa_api_key="exa-test")

        assert result == "https://boards.greenhouse.io/acme/jobs"

    async def test_custom_min_confidence_raises_threshold(self) -> None:
        """A result passing default threshold is rejected by stricter custom threshold."""
        # Generic /careers URL scores 0.50 — passes default but fails 0.80
        response = _make_exa_response(["https://www.acme.com/careers"])
        with _fake_exa(response):
            result = await find_career_page(
                "Acme Corp",
                exa_api_key="exa-test",
                min_confidence=0.80,
            )

        assert result is None

    async def test_result_with_empty_url_is_skipped(self) -> None:
        """Results that return an empty URL are gracefully skipped."""
        response = MagicMock()
        empty_result = MagicMock()
        empty_result.url = ""
        valid_result = _make_exa_result("https://jobs.ashbyhq.com/acme")
        response.results = [empty_result, valid_result]

        with _fake_exa(response):
            result = await find_career_page("Acme Corp", exa_api_key="exa-test")

        assert result == "https://jobs.ashbyhq.com/acme"

    async def test_exa_client_called_with_correct_query(self) -> None:
        """Verify the Exa client is called with the expected semantic query."""
        response = _make_exa_response([])

        fake_module = MagicMock()
        mock_instance = MagicMock()
        mock_instance.search.return_value = response
        fake_module.Exa.return_value = mock_instance

        old = sys.modules.get("exa_py")
        sys.modules["exa_py"] = fake_module
        try:
            await find_career_page("Stripe", exa_api_key="exa-abc123")
        finally:
            if old is None:
                sys.modules.pop("exa_py", None)
            else:
                sys.modules["exa_py"] = old

        fake_module.Exa.assert_called_once_with(api_key="exa-abc123")
        mock_instance.search.assert_called_once()
        call_args = mock_instance.search.call_args
        assert "Stripe" in call_args[0][0]
        query = call_args[0][0].lower()
        assert "careers" in query or "jobs" in query
