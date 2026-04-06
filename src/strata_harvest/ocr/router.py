import time
from dataclasses import dataclass, field
from urllib.parse import urljoin

import httpx

from strata_harvest.ocr.client import OcrResult, ocr_image


@dataclass
class OcrEndpoint:
    name: str
    base_url: str
    timeout_s: float = 60.0


@dataclass
class OcrRouter:
    endpoints: list[OcrEndpoint]
    health_probe_timeout_s: float = 3.0

    _health_cache: dict[str, tuple[bool, float]] = field(default_factory=dict, init=False)

    async def _check_health(self, endpoint: OcrEndpoint, client: httpx.AsyncClient) -> bool:
        now = time.monotonic()

        # Check cache (valid for 30s)
        if endpoint.name in self._health_cache:
            is_healthy, timestamp = self._health_cache[endpoint.name]
            if now - timestamp < 30.0:
                return is_healthy

        try:
            health_url = urljoin(endpoint.base_url, "/health")
            resp = await client.get(health_url, timeout=self.health_probe_timeout_s)
            is_healthy = resp.status_code == 200
        except (httpx.RequestError, httpx.HTTPStatusError):
            is_healthy = False

        self._health_cache[endpoint.name] = (is_healthy, now)
        return is_healthy

    async def resolve_endpoint(self, client: httpx.AsyncClient) -> OcrEndpoint | None:
        """Find the first healthy endpoint."""
        for ep in self.endpoints:
            if await self._check_health(ep, client):
                return ep
        return None

    async def run(self, image: bytes, *, client: httpx.AsyncClient) -> OcrResult:
        """Resolve a healthy endpoint and run OCR."""
        ep = await self.resolve_endpoint(client)
        if not ep:
            return OcrResult(
                ok=False,
                error="No healthy endpoints available",
            )

        return await ocr_image(
            image=image,
            client=client,
            endpoint=ep.base_url,
            timeout=ep.timeout_s,
        )
