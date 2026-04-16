"""Unit tests for strata_harvest.utils.stealth_fetcher (PCC-1947).

Tests use mocked scrapling internals so no real browser is launched.
Integration tests (requiring a live network and scrapling installed) are
marked ``integration`` and excluded from CI.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from strata_harvest.utils.stealth_fetcher import StealthFetcher, stealth_fetch

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_scrapling_page(
    *,
    status: int = 200,
    html_content: str = "<html><body>Jobs</body></html>",
) -> MagicMock:
    """Build a fake scrapling Adaptor-like page object."""
    page = MagicMock()
    page.status = status
    page.html_content = html_content
    return page


def _patch_scrapling(page: MagicMock):
    """Context-manager that patches scrapling at the fetcher module level.

    Uses ``create=True`` because scrapling may not be installed in CI, which
    means ``_ScraplingStealthyFetcher`` may not exist as a module attribute.
    The ``_SCRAPLING_AVAILABLE`` flag is also patched to True so that
    ``StealthFetcher.__init__`` passes without raising ImportError.
    """
    mock_fetcher_instance = MagicMock()
    mock_fetcher_instance.async_fetch = AsyncMock(return_value=page)
    mock_fetcher_cls = MagicMock(return_value=mock_fetcher_instance)

    from contextlib import contextmanager

    @contextmanager
    def _combined():
        with (
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher._ScraplingStealthyFetcher",
                mock_fetcher_cls,
                create=True,
            ),
        ):
            yield mock_fetcher_cls

    return _combined()


# ---------------------------------------------------------------------------
# Import guard
# ---------------------------------------------------------------------------


class TestStealthFetcherImportGuard:
    """Ensure ImportError is raised when scrapling is absent."""

    def test_raises_import_error_when_unavailable(self) -> None:
        with (
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", False),
            pytest.raises(ImportError, match="scrapling is not installed"),
        ):
            StealthFetcher()


# ---------------------------------------------------------------------------
# StealthFetcher.fetch() — success paths
# ---------------------------------------------------------------------------


class TestStealthFetcherSuccess:
    """Happy-path fetch scenarios."""

    @pytest.mark.asyncio
    async def test_returns_fetch_result_on_success(self) -> None:
        page = _make_scrapling_page(status=200, html_content="<html>Jobs</html>")
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://cloudflare.example.com/careers")

        assert result.ok is True
        assert result.status_code == 200
        assert result.content == "<html>Jobs</html>"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_elapsed_ms_is_positive(self) -> None:
        page = _make_scrapling_page()
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://example.com/careers")

        assert result.elapsed_ms >= 0

    @pytest.mark.asyncio
    async def test_content_type_is_text_html(self) -> None:
        page = _make_scrapling_page()
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://example.com/careers")

        assert result.content_type == "text/html"

    @pytest.mark.asyncio
    async def test_empty_html_content_returns_none_content(self) -> None:
        """scrapling returning empty html_content maps to content=None."""
        page = _make_scrapling_page(html_content="")
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://example.com/careers")

        assert result.content is None

    @pytest.mark.asyncio
    async def test_timeout_passed_to_scrapling_constructor_as_ms(self) -> None:
        """StealthyFetcher should receive timeout in milliseconds."""
        page = _make_scrapling_page()
        mock_fetcher_cls = MagicMock()
        mock_fetcher_instance = MagicMock()
        mock_fetcher_instance.async_fetch = AsyncMock(return_value=page)
        mock_fetcher_cls.return_value = mock_fetcher_instance

        with (
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher._ScraplingStealthyFetcher",
                mock_fetcher_cls,
                create=True,
            ),
        ):
            fetcher = StealthFetcher(timeout=45)
            await fetcher.fetch("https://example.com/careers")

        mock_fetcher_cls.assert_called_once_with(timeout=45_000)


# ---------------------------------------------------------------------------
# StealthFetcher.fetch() — error paths
# ---------------------------------------------------------------------------


class TestStealthFetcherErrors:
    """Error and failure scenarios."""

    @pytest.mark.asyncio
    async def test_http_403_returns_error_result(self) -> None:
        page = _make_scrapling_page(status=403, html_content="Forbidden")
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://blocked.example.com/careers")

        assert result.ok is False
        assert result.status_code == 403
        assert result.error is not None
        assert "403" in result.error

    @pytest.mark.asyncio
    async def test_exception_returns_error_result_not_raise(self) -> None:
        mock_fetcher_cls = MagicMock()
        mock_fetcher_instance = MagicMock()
        mock_fetcher_instance.async_fetch = AsyncMock(
            side_effect=RuntimeError("browser crash")
        )
        mock_fetcher_cls.return_value = mock_fetcher_instance

        with (
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", True),
            patch(
                "strata_harvest.utils.stealth_fetcher._ScraplingStealthyFetcher",
                mock_fetcher_cls,
                create=True,
            ),
        ):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://example.com/careers")

        assert result.ok is False
        assert result.error is not None
        assert "RuntimeError" in result.error
        assert "browser crash" in result.error

    @pytest.mark.asyncio
    async def test_http_500_returns_error_result(self) -> None:
        page = _make_scrapling_page(status=500, html_content="")
        with _patch_scrapling(page):
            fetcher = StealthFetcher()
            result = await fetcher.fetch("https://example.com/careers")

        assert result.ok is False
        assert result.status_code == 500


# ---------------------------------------------------------------------------
# stealth_fetch() convenience helper
# ---------------------------------------------------------------------------


class TestStealthFetchHelper:
    @pytest.mark.asyncio
    async def test_returns_fetch_result_from_helper(self) -> None:
        page = _make_scrapling_page(status=200, html_content="<html>Careers</html>")
        with _patch_scrapling(page):
            result = await stealth_fetch("https://example.com/careers")

        assert result.ok is True
        assert result.content == "<html>Careers</html>"

    @pytest.mark.asyncio
    async def test_helper_raises_on_missing_scrapling(self) -> None:
        with (
            patch("strata_harvest.utils.stealth_fetcher._SCRAPLING_AVAILABLE", False),
            pytest.raises(ImportError, match="scrapling is not installed"),
        ):
            await stealth_fetch("https://example.com/careers")


# ---------------------------------------------------------------------------
# Integration test (live network, marked to be excluded from unit CI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStealthFetcherIntegration:
    """Live browser tests — require scrapling installed and camoufox/playwright set up.

    AC from PCC-1947:
    - Integration test against a known Cloudflare-protected sample (public test service)
    """

    @pytest.mark.asyncio
    async def test_nowsecure_passes_bot_detection(self) -> None:
        """Fetch https://nowsecure.nl/ which tests browser stealth / bot detection.

        AC: StealthFetcher returns non-empty HTML and HTTP 200 from a page that
        would normally challenge or block a standard httpx client.
        """
        url = "https://nowsecure.nl/"
        fetcher = StealthFetcher(timeout=60)
        result = await fetcher.fetch(url)

        assert result.ok is True, f"Expected HTTP 200, got error: {result.error}"
        assert result.content, "Expected non-empty HTML from nowsecure.nl"
        assert len(result.content) > 500, (
            f"HTML suspiciously short ({len(result.content)} bytes)"
        )
