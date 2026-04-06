from unittest import mock

import httpx
import pytest
import respx

from strata_harvest.ocr.router import OcrEndpoint, OcrRouter


@pytest.fixture
def endpoints() -> list[OcrEndpoint]:
    return [
        OcrEndpoint(name="primary", base_url="http://node01:8000/process"),
        OcrEndpoint(name="secondary", base_url="http://node02:8000/process"),
    ]


@pytest.mark.asyncio
@respx.mock
async def test_first_endpoint_used_when_healthy(endpoints: list[OcrEndpoint]) -> None:
    respx.get("http://node01:8000/health").mock(return_value=httpx.Response(200))
    respx.post("http://node01:8000/process").mock(
        return_value=httpx.Response(200, json={"markdown": "primary ok"})
    )

    router = OcrRouter(endpoints=endpoints)
    async with httpx.AsyncClient() as client:
        res = await router.run(b"test", client=client)

    assert res.ok is True
    assert res.endpoint_used == "http://node01:8000/process"
    assert res.markdown == "primary ok"


@pytest.mark.asyncio
@respx.mock
async def test_fallback_on_probe_fail(endpoints: list[OcrEndpoint]) -> None:
    respx.get("http://node01:8000/health").mock(return_value=httpx.Response(500))
    respx.get("http://node02:8000/health").mock(return_value=httpx.Response(200))
    respx.post("http://node02:8000/process").mock(
        return_value=httpx.Response(200, json={"markdown": "secondary ok"})
    )

    router = OcrRouter(endpoints=endpoints)
    async with httpx.AsyncClient() as client:
        res = await router.run(b"test", client=client)

    assert res.ok is True
    assert res.endpoint_used == "http://node02:8000/process"
    assert res.markdown == "secondary ok"


@pytest.mark.asyncio
@respx.mock
async def test_cache_health_30s(endpoints: list[OcrEndpoint]) -> None:
    route = respx.get("http://node01:8000/health").mock(return_value=httpx.Response(200))

    router = OcrRouter(endpoints=endpoints)
    async with httpx.AsyncClient() as client:
        with mock.patch("time.monotonic", return_value=100.0):
            # First check hits network
            healthy = await router._check_health(endpoints[0], client)
            assert healthy is True
            assert route.call_count == 1

        with mock.patch("time.monotonic", return_value=120.0):
            # 20s later hits cache
            healthy = await router._check_health(endpoints[0], client)
            assert healthy is True
            assert route.call_count == 1

        with mock.patch("time.monotonic", return_value=135.0):
            # 35s later expires, network hit again
            healthy = await router._check_health(endpoints[0], client)
            assert healthy is True
            assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_all_down_returns_ok_false(endpoints: list[OcrEndpoint]) -> None:
    respx.get("http://node01:8000/health").mock(return_value=httpx.Response(500))
    respx.get("http://node02:8000/health").mock(
        side_effect=httpx.RequestError("Connection refused")
    )

    router = OcrRouter(endpoints=endpoints)
    async with httpx.AsyncClient() as client:
        res = await router.run(b"test", client=client)

    assert res.ok is False
    assert res.error is not None
    assert "No healthy endpoints" in res.error
    assert res.markdown == ""
