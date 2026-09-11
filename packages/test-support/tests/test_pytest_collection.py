"""Keep every two-level Python test suite in pytest's explicit collection roots."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# A suite outside pytest's roots must be an intentional exception, named with
# the reason it is safe to leave uncollected. There are no such exceptions today.
COLLECTION_EXCLUSIONS: dict[str, str] = {}


def _audit_test_directories(
    repo_root: Path,
    exclusions: Mapping[str, str] = COLLECTION_EXCLUSIONS,
) -> tuple[str, ...]:
    """Return two-level test directories absent from pytest's testpaths."""
    with (repo_root / "pyproject.toml").open("rb") as config_file:
        config = tomllib.load(config_file)

    testpaths = config["tool"]["pytest"]["ini_options"]["testpaths"]
    assert isinstance(testpaths, list)
    assert all(isinstance(testpath, str) for testpath in testpaths)

    discovered = tuple(
        path.relative_to(repo_root).as_posix()
        for path in sorted(repo_root.glob("*/*/tests"))
        if path.is_dir()
    )
    assert all(reason.strip() for reason in exclusions.values()), (
        "pytest collection exclusions need a non-empty reason: "
        f"{', '.join(sorted(path for path, reason in exclusions.items() if not reason.strip()))}"
    )

    return tuple(path for path in discovered if path not in testpaths and path not in exclusions)


def _write_pyproject(repo_root: Path, testpaths: Sequence[str]) -> None:
    entries = "\n".join(f'    "{testpath}",' for testpath in testpaths)
    (repo_root / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\n"
        "testpaths = [\n"
        f"{entries}\n"
        "]\n",
        encoding="utf-8",
    )


def _make_test_directory(repo_root: Path, relative_path: str) -> None:
    (repo_root / relative_path).mkdir(parents=True)


def test_all_test_directories_are_collected() -> None:
    missing = _audit_test_directories(REPO_ROOT)

    assert not missing, f"pytest testpaths omit: {', '.join(missing)}"


def test_audit_does_not_report_a_listed_test_directory(tmp_path: Path) -> None:
    _make_test_directory(tmp_path, "packages/alpha/tests")
    _write_pyproject(tmp_path, ["packages/alpha/tests"])

    assert _audit_test_directories(tmp_path) == ()


def test_audit_reports_an_unlisted_sibling_test_directory(tmp_path: Path) -> None:
    _make_test_directory(tmp_path, "packages/alpha/tests")
    _make_test_directory(tmp_path, "packages/beta/tests")
    _write_pyproject(tmp_path, ["packages/alpha/tests"])

    assert _audit_test_directories(tmp_path) == ("packages/beta/tests",)


def test_audit_accepts_a_discovered_exclusion_with_a_reason(tmp_path: Path) -> None:
    _make_test_directory(tmp_path, "packages/alpha/tests")
    _write_pyproject(tmp_path, [])

    assert _audit_test_directories(
        tmp_path,
        {"packages/alpha/tests": "The fixture package is intentionally not collected."},
    ) == ()


def test_audit_rejects_an_exclusion_without_a_reason(tmp_path: Path) -> None:
    _make_test_directory(tmp_path, "packages/alpha/tests")
    _write_pyproject(tmp_path, [])

    with pytest.raises(AssertionError, match="need a non-empty reason"):
        _audit_test_directories(tmp_path, {"packages/alpha/tests": ""})
