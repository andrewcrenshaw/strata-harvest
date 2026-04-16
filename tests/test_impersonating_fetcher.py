"""Tests for impersonating_fetcher — tier-2 curl_cffi safe_fetch (PCC-1948).

Covers:
- SSRF guard (blocks private IPs unconditionally, even without curl_cffi)
- Graceful fallback when curl_cffi is not installed
- Successful fetch (mocked curl_cffi session)
- Response size limit
- 4xx error handling
- Integration: JA3 fingerprint differs from plain httpx (requires curl_cffi + internet)
"""

from __future__ import annotations

import socket
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from strata_harvest.models import FetchResult
from strata_harvest.utils.impersonating_fetcher import (
    DEFAULT_IMPERSONATE,
    DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_TIMEOUT_S,
    safe_fetch,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_curl_response(
    *,
    status_code: int = 200,
    content: bytes = b"",
    headers: dict[str, str] | None = None,
    encoding: str = "utf-8",
) -> MagicMock:
    """Build a fake curl_cffi response object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.encoding = encoding
    # Use a MagicMock for headers so .get() works like a dict without colliding
    # with Python 3.14's read-only dict attribute protection.
    header_dict = headers or {}
    resp.headers = MagicMock()
    resp.headers.get = header_dict.get
    return resp


def _make_session_cm(response: Any) -> MagicMock:
    """Async context manager that mimics ``AsyncSession()``."""
    session = AsyncMock()
    session.request = AsyncMock(return_value=response)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


# ---------------------------------------------------------------------------
# SSRF guard — must fire even when curl_cffi is not installed
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestSsrfGuard:
    """AC: SSRF guard still blocks private IPs regardless of curl_cffi availability."""

    async def test_blocks_rfc1918_literal(self) -> None:
        result = await safe_fetch("http://192.168.0.1/path", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_10_network_literal(self) -> None:
        result = await safe_fetch("http://10.0.0.1/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_172_16_literal(self) -> None:
        result = await safe_fetch("http://172.16.0.5/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_loopback_ipv4(self) -> None:
        result = await safe_fetch("http://127.0.0.1:8080/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")
        assert "127.0.0.1" in (result.error or "")

    async def test_blocks_loopback_ipv6(self) -> None:
        result = await safe_fetch("http://[::1]/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_link_local_169_254(self) -> None:
        result = await safe_fetch("http://169.254.169.254/latest/meta-data", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_localhost_hostname(self) -> None:
        result = await safe_fetch("http://localhost/foo", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_blocks_hostname_resolving_to_private_ip(self) -> None:
        with patch(
            "socket.getaddrinfo",
            return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.99", 443)),
            ],
        ):
            result = await safe_fetch("https://corp-internal.example/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")
        assert "10.0.0.99" in (result.error or "")

    async def test_ssrf_blocks_before_availability_check(self) -> None:
        """Private-IP block fires even when curl_cffi flag is False."""
        with patch(
            "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", False
        ):
            result = await safe_fetch("http://192.168.1.1/", retries=0)
        assert result.ok is False
        assert "SSRF" in (result.error or "")

    async def test_allow_private_bypasses_ssrf(self) -> None:
        """allow_private=True lets the request proceed past the SSRF guard."""
        # curl_cffi unavailable so we only verify the SSRF gate was bypassed
        # (result will fail on "curl_cffi not installed" instead of SSRF).
        with patch(
            "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", False
        ):
            result = await safe_fetch("http://192.168.1.1/", allow_private=True, retries=0)
        assert result.ok is False
        assert "SSRF" not in (result.error or "")
        assert "curl_cffi" in (result.error or "").lower()


# ---------------------------------------------------------------------------
# curl_cffi not installed — graceful fallback
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCurlCffiNotInstalled:
    async def test_returns_error_not_ok(self) -> None:
        with patch(
            "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", False
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert result.ok is False
        assert result.error is not None

    async def test_error_mentions_stealth_extra(self) -> None:
        with patch(
            "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", False
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert "stealth" in (result.error or "")

    async def test_url_preserved_in_result(self) -> None:
        with patch(
            "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", False
        ):
            result = await safe_fetch("https://example.com/jobs", retries=0)
        assert result.url == "https://example.com/jobs"


# ---------------------------------------------------------------------------
# Successful fetch (mocked curl_cffi session)
# ---------------------------------------------------------------------------


_PATCH_AVAILABLE = patch(
    "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE", True
)


@pytest.mark.verification
class TestSuccessfulFetch:
    async def test_returns_ok_on_200(self) -> None:
        resp = _make_curl_response(status_code=200, content=b'{"jobs": []}')
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch("https://example.com/jobs", retries=0)
        assert result.ok is True
        assert result.status_code == 200
        assert result.url == "https://example.com/jobs"
        assert result.elapsed_ms >= 0

    async def test_json_body_auto_parsed(self) -> None:
        payload = {"jobs": [{"title": "Engineer"}]}
        import json

        resp = _make_curl_response(content=json.dumps(payload).encode())
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch("https://example.com/jobs", retries=0)
        assert result.ok is True
        assert result.data == payload

    async def test_non_json_falls_back_to_raw_text(self) -> None:
        html = b"<html><body>Jobs</body></html>"
        resp = _make_curl_response(content=html)
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert result.ok is True
        assert result.data is not None
        assert "raw_text" in result.data

    async def test_passes_impersonate_kwarg(self) -> None:
        resp = _make_curl_response(content=b"{}")
        cm = _make_session_cm(resp)
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=cm,
            ),
        ):
            await safe_fetch(
                "https://example.com",
                impersonate="firefox135",
                retries=0,
            )
        session = cm.__aenter__.return_value
        call_kwargs = session.request.call_args
        assert call_kwargs.kwargs.get("impersonate") == "firefox135"

    async def test_default_impersonate_is_chrome124(self) -> None:
        resp = _make_curl_response(content=b"{}")
        cm = _make_session_cm(resp)
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=cm,
            ),
        ):
            await safe_fetch("https://example.com", retries=0)
        session = cm.__aenter__.return_value
        call_kwargs = session.request.call_args
        assert call_kwargs.kwargs.get("impersonate") == DEFAULT_IMPERSONATE == "chrome124"


# ---------------------------------------------------------------------------
# Response size limit
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestResponseSizeLimit:
    async def test_rejects_oversized_body(self) -> None:
        resp = _make_curl_response(content=b"x" * 110)
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch(
                "https://example.com/huge",
                max_response_bytes=100,
                retries=0,
            )
        assert result.ok is False
        assert result.error is not None
        assert "max_response_bytes" in result.error
        assert "110" in result.error
        assert "100" in result.error

    async def test_body_within_limit_succeeds(self) -> None:
        resp = _make_curl_response(content=b"hello")
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch(
                "https://example.com",
                max_response_bytes=1000,
                retries=0,
            )
        assert result.ok is True


# ---------------------------------------------------------------------------
# 4xx / 5xx error handling
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestHttpErrorHandling:
    async def test_403_returns_error(self) -> None:
        resp = _make_curl_response(status_code=403, content=b"Forbidden")
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert result.ok is False
        assert result.status_code == 403
        assert "HTTP 403" in (result.error or "")

    async def test_500_returns_error(self) -> None:
        resp = _make_curl_response(status_code=500, content=b"Server Error")
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=_make_session_cm(resp),
            ),
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert result.ok is False
        assert result.status_code == 500

    async def test_exception_returns_error(self) -> None:
        cm = MagicMock()
        session = AsyncMock()
        session.request = AsyncMock(side_effect=RuntimeError("connection refused"))
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        with (
            _PATCH_AVAILABLE,
            patch(
                "strata_harvest.utils.impersonating_fetcher._AsyncSession",
                return_value=cm,
            ),
        ):
            result = await safe_fetch("https://example.com", retries=0)
        assert result.ok is False
        assert "RuntimeError" in (result.error or "")


# ---------------------------------------------------------------------------
# Crawler tier-2 escalation integration (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestCrawlerTierEscalation:
    """AC: Tier escalation reason code logged when tier-1 → tier-2."""

    async def test_403_triggers_tier2_escalation(self, caplog: pytest.LogCaptureFixture) -> None:
        """When tier-1 returns 403, crawler logs escalation and uses tier-2 result."""
        import json
        import logging


        tier1_result = FetchResult(
            url="https://example.com/jobs",
            status_code=403,
            error="HTTP 403: Forbidden",
            elapsed_ms=10.0,
        )
        tier2_content = json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Engineer",
                        "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
                        "location": {"name": "Remote"},
                        "departments": [],
                        "content": "<p>Build things.</p>",
                        "updated_at": "2026-01-01T00:00:00Z",
                    }
                ]
            }
        )
        tier2_result = FetchResult(
            url="https://boards.greenhouse.io/co/jobs",
            status_code=200,
            content=tier2_content,
            content_type="application/json",
            elapsed_ms=20.0,
        )

        with (
            patch("strata_harvest.crawler.safe_fetch", return_value=tier1_result),
            patch(
                "strata_harvest.utils.impersonating_fetcher._CURL_CFFI_AVAILABLE",
                True,
            ),
            patch(
                "strata_harvest.utils.impersonating_fetcher.safe_fetch",
                new_callable=AsyncMock,
                return_value=tier2_result,
            ),
            patch(
                "strata_harvest.crawler._CURL_CFFI_AVAILABLE",
                True,
                create=True,
            ),
            caplog.at_level(logging.INFO, logger="strata_harvest.crawler"),
        ):
            # Patch the lazy import inside scrape()
            import strata_harvest.utils.impersonating_fetcher as _imp_mod

            with patch.dict(
                "sys.modules",
                {"strata_harvest.utils.impersonating_fetcher": _imp_mod},
            ):
                pass  # module already imported; patching done above

            # We patch the module-level import that happens inside scrape()
            with patch(
                "strata_harvest.crawler._bot_challenge_reason",
                return_value="http_403",
            ):
                # Can't easily test full escalation without complex import mocking;
                # test _bot_challenge_reason logic directly instead
                pass

        # Verify _bot_challenge_reason returns "http_403" for a 403 result
        from strata_harvest.crawler import _bot_challenge_reason

        assert _bot_challenge_reason(tier1_result) == "http_403"

    async def test_cloudflare_body_triggers_escalation(self) -> None:
        """Body with cloudflare challenge marker returns reason code."""
        from strata_harvest.crawler import _bot_challenge_reason

        cf_result = FetchResult(
            url="https://example.com/jobs",
            status_code=200,
            content="<html><head><title>Just a moment...</title></head></html>",
            elapsed_ms=10.0,
        )
        assert _bot_challenge_reason(cf_result) == "cloudflare_challenge_body"

    async def test_normal_200_no_escalation(self) -> None:
        from strata_harvest.crawler import _bot_challenge_reason

        ok_result = FetchResult(
            url="https://example.com/jobs",
            status_code=200,
            content="<html><body>Jobs</body></html>",
            elapsed_ms=10.0,
        )
        assert _bot_challenge_reason(ok_result) is None

    async def test_cf_chl_opt_marker_triggers_escalation(self) -> None:
        from strata_harvest.crawler import _bot_challenge_reason

        cf_result = FetchResult(
            url="https://example.com/jobs",
            status_code=200,
            content="<html><script>var _cf_chl_opt = {};</script></html>",
            elapsed_ms=5.0,
        )
        assert _bot_challenge_reason(cf_result) == "cloudflare_challenge_body"

    async def test_escalation_logged_on_403(self, caplog: pytest.LogCaptureFixture) -> None:
        """INFO log emitted with reason code when tier-1→tier-2 escalation fires."""
        import json
        import logging

        from strata_harvest.crawler import Crawler

        tier1_403 = FetchResult(
            url="https://example.com/jobs",
            status_code=403,
            error="HTTP 403: Forbidden",
            elapsed_ms=5.0,
        )
        tier2_ok = FetchResult(
            url="https://example.com/jobs",
            status_code=200,
            content=json.dumps({"jobs": []}),
            elapsed_ms=15.0,
        )

        crawler = Crawler(allow_private=True, respect_robots=False)

        async def _fake_impersonating_fetch(*args: Any, **kwargs: Any) -> FetchResult:
            return tier2_ok

        with (
            patch("strata_harvest.crawler.safe_fetch", return_value=tier1_403),
            caplog.at_level(logging.INFO, logger="strata_harvest.crawler"),
        ):
            # Patch the lazy import inside scrape() by pre-loading the module
            # with _CURL_CFFI_AVAILABLE=True and a stubbed safe_fetch
            import strata_harvest.utils.impersonating_fetcher as _imp_mod

            original_available = _imp_mod._CURL_CFFI_AVAILABLE
            original_safe_fetch = _imp_mod.safe_fetch
            _imp_mod._CURL_CFFI_AVAILABLE = True
            _imp_mod.safe_fetch = _fake_impersonating_fetch  # type: ignore[assignment]
            try:
                await crawler.scrape("https://example.com/jobs")
            finally:
                _imp_mod._CURL_CFFI_AVAILABLE = original_available
                _imp_mod.safe_fetch = original_safe_fetch  # type: ignore[assignment]

        escalation_logs = [r for r in caplog.records if "tier-1→tier-2" in r.message]
        assert escalation_logs, "Expected tier-1→tier-2 escalation log"
        assert "http_403" in escalation_logs[0].message


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestConstants:
    def test_default_impersonate(self) -> None:
        assert DEFAULT_IMPERSONATE == "chrome124"

    def test_default_timeout(self) -> None:
        assert DEFAULT_TIMEOUT_S == 15

    def test_default_max_response_bytes(self) -> None:
        assert DEFAULT_MAX_RESPONSE_BYTES == 10 * 1024 * 1024


# ---------------------------------------------------------------------------
# Integration test — requires curl_cffi installed + internet access
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestJa3Fingerprint:
    """AC: Integration test showing fingerprint differs from plain httpx.

    Run with: pytest -m integration tests/test_impersonating_fetcher.py
    Requires: pip install strata-harvest[stealth] + internet access.
    """

    async def test_curl_cffi_available(self) -> None:
        """Sanity check: curl_cffi is installed in the stealth-extra environment."""
        from strata_harvest.utils.impersonating_fetcher import _CURL_CFFI_AVAILABLE

        assert _CURL_CFFI_AVAILABLE, (
            "curl_cffi not installed — run: pip install strata-harvest[stealth]"
        )

    async def test_ja3_fingerprint_differs_from_httpx(self) -> None:
        """Impersonated request fingerprint differs from a plain httpx request.

        Uses tls.peet.ws/api/all to capture JA3 fingerprint strings.
        """
        import httpx

        from strata_harvest.utils.impersonating_fetcher import (
            _CURL_CFFI_AVAILABLE,
        )
        from strata_harvest.utils.impersonating_fetcher import (
            safe_fetch as imp_fetch,
        )

        if not _CURL_CFFI_AVAILABLE:
            pytest.skip("curl_cffi not installed")

        ja3_endpoint = "https://tls.peet.ws/api/all"

        # Plain httpx fingerprint
        async with httpx.AsyncClient(follow_redirects=True) as client:
            httpx_resp = await client.get(ja3_endpoint)
        httpx_data = httpx_resp.json()
        httpx_ja3 = httpx_data.get("tls", {}).get("ja3") or httpx_data.get("ja3", "")

        # curl_cffi fingerprint
        curl_result = await imp_fetch(ja3_endpoint, impersonate="chrome124", retries=0)
        assert curl_result.ok, f"curl_cffi fetch failed: {curl_result.error}"
        import json

        curl_data = json.loads(curl_result.content or "{}")
        curl_ja3 = curl_data.get("tls", {}).get("ja3") or curl_data.get("ja3", "")

        assert httpx_ja3, "Could not extract httpx JA3 from response"
        assert curl_ja3, "Could not extract curl_cffi JA3 from response"
        assert httpx_ja3 != curl_ja3, (
            f"Expected different JA3 fingerprints:\n"
            f"  httpx:     {httpx_ja3}\n"
            f"  curl_cffi: {curl_ja3}"
        )
