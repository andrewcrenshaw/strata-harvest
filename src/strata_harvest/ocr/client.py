import base64
import time
from dataclasses import dataclass
from pathlib import Path

import httpx


@dataclass
class OcrResult:
    ok: bool
    markdown: str = ""
    error: str | None = None
    duration_ms: int = 0
    endpoint_used: str = ""

async def ocr_image(
    image: bytes | Path | str,
    *,
    client: httpx.AsyncClient,
    endpoint: str,
    timeout: float = 60.0,
) -> OcrResult:
    """Send an image to the GLM-OCR endpoint and get the markdown back."""
    start_time = time.monotonic()

    try:
        # Prepare the payload
        payload = {}
        if isinstance(image, Path):
            data = image.read_bytes()
            b64_data = base64.b64encode(data).decode('utf-8')
            payload["image"] = f"data:image/png;base64,{b64_data}"
        elif isinstance(image, bytes):
            b64_data = base64.b64encode(image).decode('utf-8')
            payload["image"] = f"data:image/png;base64,{b64_data}"
        elif isinstance(image, str):
            if image.startswith("http://") or image.startswith("https://"):
                payload["image_url"] = image
            else:
                # Assume raw base64 string
                payload["image"] = image
        else:
            return OcrResult(
                ok=False,
                error=f"Unsupported image type: {type(image)}",
                endpoint_used=endpoint,
            )

        resp = await client.post(
            endpoint,
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()

        data = resp.json()

        # Try to extract the markdown, accepting various common JSON structures
        markdown_text = data.get("markdown") or data.get("text") or data.get("content") or ""

        # If wrapped in OpenAI-like response
        if not markdown_text and "choices" in data and len(data["choices"]) > 0:
            choice = data["choices"][0]
            if "message" in choice and "content" in choice["message"]:
                markdown_text = choice["message"]["content"]

        duration_ms = int((time.monotonic() - start_time) * 1000)
        return OcrResult(
            ok=True,
            markdown=str(markdown_text),
            duration_ms=duration_ms,
            endpoint_used=endpoint,
        )

    except httpx.HTTPStatusError as e:
        duration_ms = int((time.monotonic() - start_time) * 1000)
        error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
        return OcrResult(
            ok=False,
            error=error_msg,
            duration_ms=duration_ms,
            endpoint_used=endpoint,
        )
    except httpx.RequestError as e:
        duration_ms = int((time.monotonic() - start_time) * 1000)
        return OcrResult(
            ok=False,
            error=f"{type(e).__name__}: {str(e)}",
            duration_ms=duration_ms,
            endpoint_used=endpoint,
        )
    except Exception as e:
        duration_ms = int((time.monotonic() - start_time) * 1000)
        return OcrResult(
            ok=False,
            error=f"{type(e).__name__}: {str(e)}",
            duration_ms=duration_ms,
            endpoint_used=endpoint,
        )
