from pathlib import Path

import httpx
import pytest
import respx

from strata_harvest.ocr.client import ocr_image


@pytest.mark.asyncio
@respx.mock
async def test_bytes_encoded() -> None:
    endpoint = "http://192.168.50.200:5002/process"
    image_bytes = b"fake image content"

    # Mock endpoint
    respx.post(endpoint).mock(
        return_value=httpx.Response(200, json={"markdown": "Here is the parsed text"})
    )

    async with httpx.AsyncClient() as client:
        result = await ocr_image(
            image=image_bytes,
            client=client,
            endpoint=endpoint,
            timeout=5.0
        )

    assert result.ok is True
    assert result.markdown == "Here is the parsed text"
    assert result.error is None
    assert result.duration_ms >= 0
    assert result.endpoint_used == endpoint

@pytest.mark.asyncio
@respx.mock
async def test_path_read(tmp_path: Path) -> None:
    endpoint = "http://192.168.50.200:5002/process"
    image_bytes = b"another fake image"

    test_file = tmp_path / "test.png"
    test_file.write_bytes(image_bytes)

    respx.post(endpoint).mock(
        return_value=httpx.Response(200, json={"markdown": "Output from file"})
    )

    async with httpx.AsyncClient() as client:
        result = await ocr_image(
            image=test_file,
            client=client,
            endpoint=endpoint,
        )

    assert result.ok is True
    assert result.markdown == "Output from file"

@pytest.mark.asyncio
@respx.mock
async def test_http_error_ok_false() -> None:
    endpoint = "http://192.168.50.200:5002/process"
    image_str = "http://example.com/image.png"

    # Mock endpoint mapping to error
    respx.post(endpoint).mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    async with httpx.AsyncClient() as client:
        result = await ocr_image(
            image=image_str,
            client=client,
            endpoint=endpoint,
        )

    assert result.ok is False
    assert result.markdown == ""
    assert result.error is not None
    assert "500" in result.error
    assert result.endpoint_used == endpoint
