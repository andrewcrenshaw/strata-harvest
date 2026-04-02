import ipaddress
import socket
from typing import Any

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _stub_dns_for_ssrf(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-IP hostnames resolve to a public IP; avoids flaky real DNS in tests."""

    def _fake_getaddrinfo(
        host: Any,
        port: Any,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", int(port or 0)))]
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo)
