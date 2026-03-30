"""Tests for safe_fetch() — resilient HTTP client (TDS §5.1)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from strata_harvest.models import FetchResult
from strata_harvest.utils.http import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_S,
    DEFAULT_USER_AGENT,
    safe_fetch,
)


def _mock_response(
    *,
    status_code: int = 200,
    json_data: dict[str, Any] | None = None,
    text: str = "",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Build a mock httpx.Response."""
    resp_headers = headers or {}
    if json_data is not None:
        import json

        text = json.dumps(json_data)
        resp_headers.setdefault("content-type", "application/json")

    request = httpx.Request("GET", "https://example.com")
    return httpx.Response(
        status_code=status_code,
        text=text,
        headers=resp_headers,
        request=request,
    )


# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestDefaults:
    def test_default_timeout(self) -> None:
        assert DEFAULT_TIMEOUT_S == 15

    def test_default_retries(self) -> None:
        assert DEFAULT_RETRIES == 1

    def test_default_user_agent(self) -> None:
        assert DEFAULT_USER_AGENT == "strata-harvest/0.1"


# ---------------------------------------------------------------------------
# Successful fetch
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestSuccessfulFetch:
    async def test_get_json_response(self) -> None:
        """JSON responses are auto-parsed into FetchResult.data."""
        json_payload = {"jobs": [{"title": "Engineer"}]}
        mock_resp = _mock_response(json_data=json_payload)

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://api.example.com/jobs")

        assert result.ok is True
        assert result.data == json_payload
        assert result.status_code == 200
        assert result.url == "https://api.example.com/jobs"
        assert result.elapsed_ms >= 0

    async def test_get_text_response(self) -> None:
        """Non-JSON responses fallback to {"raw_text": text[:500]}."""
        html = "<html><body>Hello World</body></html>"
        mock_resp = _mock_response(text=html)

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://example.com")

        assert result.ok is True
        assert result.data == {"raw_text": html}
        assert result.status_code == 200

    async def test_text_fallback_truncates_at_500(self) -> None:
        """raw_text fallback is capped at 500 characters."""
        long_text = "x" * 1000
        mock_resp = _mock_response(text=long_text)

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://example.com")

        assert result.data is not None
        assert len(result.data["raw_text"]) == 500


# ---------------------------------------------------------------------------
# Error handling — never raises
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestNeverRaises:
    async def test_timeout_returns_fetch_result(self) -> None:
        """Timeout returns FetchResult(ok=False), never raises."""
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://slow.example.com", retries=0)

        assert result.ok is False
        assert result.error is not None
        assert "TimeoutException" in result.error
        assert result.url == "https://slow.example.com"

    async def test_connect_error_returns_fetch_result(self) -> None:
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://down.example.com", retries=0)

        assert result.ok is False
        assert "ConnectError" in (result.error or "")

    async def test_read_error_returns_fetch_result(self) -> None:
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=httpx.ReadError("read error"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://broken.example.com", retries=0)

        assert result.ok is False
        assert "ReadError" in (result.error or "")


# ---------------------------------------------------------------------------
# 4xx/5xx error handling with retry
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestHTTPErrorRetry:
    async def test_4xx_retries_then_returns_error(self) -> None:
        """4xx responses retry, then return structured error."""
        mock_resp = _mock_response(status_code=429, text="Rate limited")

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                result = await safe_fetch("https://api.example.com", retries=1)

            assert result.ok is False
            assert result.status_code == 429
            assert "HTTP 429" in (result.error or "")
            mock_sleep.assert_called_once()

    async def test_5xx_retries_then_returns_error(self) -> None:
        mock_resp = _mock_response(status_code=500, text="Internal Server Error")

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await safe_fetch("https://api.example.com", retries=1)

        assert result.ok is False
        assert result.status_code == 500

    async def test_4xx_recovery_on_retry(self) -> None:
        """First attempt gets 429, retry succeeds with 200."""
        error_resp = _mock_response(status_code=429, text="Rate limited")
        ok_resp = _mock_response(json_data={"ok": True})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=[error_resp, ok_resp])
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await safe_fetch("https://api.example.com", retries=1)

        assert result.ok is True
        assert result.data == {"ok": True}


# ---------------------------------------------------------------------------
# Retry logic & exponential backoff
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestRetryLogic:
    async def test_retries_on_timeout(self) -> None:
        """Retries the specified number of times on timeout."""
        json_payload = {"success": True}
        ok_resp = _mock_response(json_data=json_payload)

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=[httpx.TimeoutException("timeout"), ok_resp])
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await safe_fetch("https://api.example.com", retries=1)

        assert result.ok is True
        assert result.data == json_payload
        assert instance.request.call_count == 2

    async def test_exponential_backoff_timing(self) -> None:
        """Backoff follows 2*(attempt+1) pattern: 2s, 4s."""
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                result = await safe_fetch("https://slow.example.com", retries=2)

            assert result.ok is False
            sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
            assert sleep_args == [2.0, 4.0]

    async def test_no_retry_when_retries_zero(self) -> None:
        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch("https://slow.example.com", retries=0)

        assert result.ok is False
        assert instance.request.call_count == 1


# ---------------------------------------------------------------------------
# Shared client support
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestSharedClient:
    async def test_uses_provided_client(self) -> None:
        """When a client is passed, uses it instead of creating a new one."""
        json_payload = {"jobs": []}
        mock_resp = _mock_response(json_data=json_payload)

        shared_client = AsyncMock(spec=httpx.AsyncClient)
        shared_client.request = AsyncMock(return_value=mock_resp)

        result = await safe_fetch("https://api.example.com", client=shared_client)

        assert result.ok is True
        shared_client.request.assert_called_once()
        shared_client.aclose.assert_not_called()

    async def test_closes_owned_client(self) -> None:
        """When no client is passed, creates and closes one."""
        mock_resp = _mock_response(json_data={"ok": True})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            await safe_fetch("https://api.example.com")

        instance.aclose.assert_called_once()


# ---------------------------------------------------------------------------
# POST support
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestPostSupport:
    async def test_post_with_json_body(self) -> None:
        """POST with json parameter sends JSON payload."""
        mock_resp = _mock_response(json_data={"results": []})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch(
                "https://api.example.com/search",
                method="POST",
                json={"query": "engineer"},
            )

        assert result.ok is True
        call_kwargs = instance.request.call_args
        assert call_kwargs.args[0] == "POST"
        assert call_kwargs.kwargs.get("json") == {"query": "engineer"}

    async def test_post_with_raw_body(self) -> None:
        """POST with body parameter sends raw bytes."""
        mock_resp = _mock_response(json_data={"ok": True})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            result = await safe_fetch(
                "https://api.example.com/upload",
                method="POST",
                body=b"raw payload",
            )

        assert result.ok is True
        call_kwargs = instance.request.call_args
        assert call_kwargs.kwargs.get("content") == b"raw payload"


# ---------------------------------------------------------------------------
# Configurable headers
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestConfigurableHeaders:
    async def test_custom_headers_merged(self) -> None:
        """Custom headers are merged with default User-Agent."""
        mock_resp = _mock_response(json_data={"ok": True})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            await safe_fetch(
                "https://api.example.com",
                headers={"Authorization": "Bearer token123"},
            )

        call_kwargs = instance.request.call_args
        sent_headers = call_kwargs.kwargs.get("headers", {})
        assert sent_headers["Authorization"] == "Bearer token123"
        assert sent_headers["User-Agent"] == DEFAULT_USER_AGENT

    async def test_custom_user_agent_overrides_default(self) -> None:
        """Custom User-Agent in headers overrides the default."""
        mock_resp = _mock_response(json_data={"ok": True})

        with patch("strata_harvest.utils.http.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.request = AsyncMock(return_value=mock_resp)
            instance.aclose = AsyncMock()
            mock_client.return_value = instance

            await safe_fetch(
                "https://api.example.com",
                headers={"User-Agent": "custom-bot/1.0"},
            )

        call_kwargs = instance.request.call_args
        sent_headers = call_kwargs.kwargs.get("headers", {})
        assert sent_headers["User-Agent"] == "custom-bot/1.0"


# ---------------------------------------------------------------------------
# FetchResult structure
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestFetchResultStructure:
    def test_ok_property_true_for_2xx(self) -> None:
        result = FetchResult(url="https://example.com", status_code=200)
        assert result.ok is True

    def test_ok_property_true_for_3xx(self) -> None:
        result = FetchResult(url="https://example.com", status_code=301)
        assert result.ok is True

    def test_ok_property_false_for_4xx(self) -> None:
        result = FetchResult(url="https://example.com", status_code=404)
        assert result.ok is False

    def test_ok_property_false_when_no_status(self) -> None:
        result = FetchResult(url="https://example.com", error="Timeout")
        assert result.ok is False

    def test_data_field_exists(self) -> None:
        result = FetchResult(url="https://example.com", data={"jobs": []})
        assert result.data == {"jobs": []}

    def test_data_defaults_to_none(self) -> None:
        result = FetchResult(url="https://example.com")
        assert result.data is None
