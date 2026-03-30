"""Packaging metadata contract (PCC-1518)."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.verification
def test_pyproject_name_version_python_requires() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    proj = data["project"]
    assert proj["name"] == "strata-harvest"
    assert proj["version"] == "0.1.1"
    assert proj["requires-python"] == ">=3.11"


@pytest.mark.verification
def test_pyproject_license_mit_and_dev_optional() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    proj = data["project"]
    assert proj["license"] == {"text": "MIT"}
    opt = proj["optional-dependencies"]
    assert "dev" in opt
    dev_joined = " ".join(opt["dev"])
    assert "pytest" in dev_joined


@pytest.mark.verification
def test_changelog_has_initial_release() -> None:
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "0.1.0" in text
    assert "2026" in text or "Initial" in text
