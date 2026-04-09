"""Packaging metadata contract (PCC-1518)."""

from __future__ import annotations

import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.verification
def test_pyproject_name_version_python_requires() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    proj = data["project"]
    assert proj["name"] == "strata-harvest"
    assert proj["version"] == "0.1.2"
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


@pytest.mark.verification
def test_built_wheel_contains_py_typed() -> None:
    """PEP 561: downstream mypy/pyright need py.typed in the wheel (PCC-1612)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        dist = Path(tmp) / "dist"
        dist.mkdir(parents=True)
        r = subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist)],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        assert r.returncode == 0, r.stdout + r.stderr
        wheels = list(dist.glob("strata_harvest-*.whl"))
        assert len(wheels) == 1, f"expected one wheel, got {wheels}"
        with zipfile.ZipFile(wheels[0]) as zf:
            names = zf.namelist()
        assert any(n.endswith("strata_harvest/py.typed") for n in names), names


@pytest.mark.verification
def test_mypy_strict_on_typing_consumer() -> None:
    """Installed / editable package is type-checkable under --strict (PCC-1612)."""
    consumer = ROOT / "tests" / "mypy_typing_consumer.py"
    assert consumer.is_file()
    r = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", str(consumer)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stdout + r.stderr
