"""Shared fixture loader utility for strata-harvest tests.

Provides a uniform API for loading raw fixtures (API responses, HTML pages)
and their corresponding expected parsed output (ground truth).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def load_raw(provider: str, name: str) -> str:
    """Load a raw fixture file (API response JSON or HTML) as a string.

    Args:
        provider: Subdirectory under fixtures/ (e.g. "greenhouse_api", "lever", "career_pages").
        name: Filename within the provider directory.

    Raises:
        FileNotFoundError: If the fixture file does not exist.
    """
    path = FIXTURES_ROOT / provider / name
    return path.read_text(encoding="utf-8")


def load_expected(provider: str, name: str) -> list[dict[str, Any]]:
    """Load the ground-truth expected output for a fixture.

    Expected files live under ``fixtures/expected/<provider>/<name>`` and contain
    a JSON array of objects matching the ``JobListing`` model (minus ``raw_data``).

    Args:
        provider: Provider subdirectory (e.g. "greenhouse_api").
        name: Fixture filename (must be ``.json``).

    Raises:
        FileNotFoundError: If the expected output file does not exist.
    """
    path = FIXTURES_ROOT / "expected" / provider / name
    return json.loads(path.read_text(encoding="utf-8"))


def fixture_path(provider: str, name: str) -> Path:
    """Return the absolute path to a fixture file."""
    return FIXTURES_ROOT / provider / name


def expected_path(provider: str, name: str) -> Path:
    """Return the absolute path to an expected-output file."""
    return FIXTURES_ROOT / "expected" / provider / name


def list_fixtures(provider: str, *, suffix: str | None = None) -> list[str]:
    """List fixture filenames for a provider, optionally filtered by suffix.

    Args:
        provider: Provider subdirectory.
        suffix: Optional file extension filter (e.g. ".json", ".html").

    Returns:
        Sorted list of filenames (excluding dotfiles).
    """
    provider_dir = FIXTURES_ROOT / provider
    if not provider_dir.is_dir():
        return []
    names = sorted(
        f.name for f in provider_dir.iterdir() if f.is_file() and not f.name.startswith(".")
    )
    if suffix:
        names = [n for n in names if n.endswith(suffix)]
    return names


def has_expected(provider: str, name: str) -> bool:
    """Check whether a ground-truth expected output file exists for a fixture."""
    return expected_path(provider, name).is_file()
