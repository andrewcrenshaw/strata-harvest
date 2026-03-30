"""Package-level public API contract (PCC-1517)."""

from __future__ import annotations

import inspect

import pytest


@pytest.mark.verification
def test_all_matches_documented_exports() -> None:
    """Stable names exported from strata_harvest root."""
    import strata_harvest

    assert set(strata_harvest.__all__) == {
        "ATSInfo",
        "JobListing",
        "ScrapeResult",
        "__version__",
        "create_crawler",
        "harvest",
    }


@pytest.mark.verification
def test_help_lists_only_public_names() -> None:
    """``help(strata_harvest)`` exposes a small, intentional surface."""
    import strata_harvest

    names = {x[0] for x in inspect.getmembers(strata_harvest)}
    assert "detect_ats" not in names
    assert "Crawler" not in names
    assert "FetchResult" not in names
    assert "ATSProvider" not in names
    assert "harvest" in names
    assert "create_crawler" in names


@pytest.mark.verification
def test_version_is_semver() -> None:
    import strata_harvest

    parts = strata_harvest.__version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
