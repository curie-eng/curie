"""The repository's service failures must be eligible for factory delegation."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import runpy
import socket
from pathlib import Path

import pytest
from curie_runner.verification import (
    _install,
    _verify,
    load_verification_declaration,
)

REPOSITORY = Path(__file__).resolve().parents[2]
SERVICE_PATHS = (
    "apps/api/src/curie_api/main.py",
    "apps/api/tests/test_actions_worker_contract.py",
    "apps/worker/src/curie_worker/config.py",
    "apps/worker/tests/binding/test_approval_grant.py",
    "apps/dispatcher/src/curie_dispatcher/queue.py",
    "apps/mail-adapter/src/curie_mail_adapter/server.py",
    "packages/test-support/src/curie_test_support/postgres.py",
    "tests/vectors/channel-port-refusal.json",
    "conftest.py",
    "pyproject.toml",
    "uv.lock",
    ".curie/verification.json",
)
RUNNER_PATH = "runner/src/curie_runner/approval.py"
COMMANDS = {
    "api": (
        "env",
        "CI_REQUIRE_POSTGRES_TESTS=1",
        "uv",
        "run",
        "pytest",
        "apps/api/tests",
        "-x",
        "-q",
    ),
    "worker": (
        "env",
        "CI_REQUIRE_POSTGRES_TESTS=1",
        "uv",
        "run",
        "pytest",
        "apps/worker/tests",
        "-x",
        "-q",
    ),
}


def test_repository_verification_declaration(tmp_path: Path) -> None:
    declaration = load_verification_declaration(tmp_path, REPOSITORY)
    assert declaration.source == "repository"
    assert declaration.unreadable is None
    assert [check.id for check in declaration.checks] == ["api", "worker"]
    gate = runpy.run_path(str(REPOSITORY / "examples/dark-factory/hooks/review_gate.py"))
    gate_checks = gate["_declared_checks_list"](
        REPOSITORY / ".curie/verification.json", repository=True
    )
    assert gate_checks is not None
    assert [check["id"] for check in gate_checks] == [check.id for check in declaration.checks]
    for check in declaration.checks:
        assert check.delegated_to == "Python (ruff + mypy + pytest)"
        assert check.command == COMMANDS[check.id]
        assert check.install == ("uv", "sync", "--locked")
        assert _install(list(check.install)) == check.install
        for dependency in SERVICE_PATHS:
            assert any(fnmatch.fnmatchcase(dependency, pattern) for pattern in check.paths)
        assert not any(fnmatch.fnmatchcase(RUNNER_PATH, pattern) for pattern in check.paths)
        for independent in ("cli/src/main.rs", "apps/ui/src/App.tsx"):
            assert not any(fnmatch.fnmatchcase(independent, pattern) for pattern in check.paths)


def test_empty_bundle_keeps_repository_checks_and_locked_installs(tmp_path: Path) -> None:
    bundle = tmp_path / "verification"
    bundle.mkdir()
    (bundle / "checks.json").write_text(
        json.dumps({"lockfile_installs": True, "checks": []}),
        encoding="utf-8",
    )

    declaration = load_verification_declaration(tmp_path, REPOSITORY)

    assert declaration.source == "repository"
    assert declaration.lockfile_installs is True
    assert declaration.unreadable is None
    assert [check.id for check in declaration.checks] == ["api", "worker"]


@pytest.mark.parametrize("suite", ["api", "worker"])
def test_postgres_failure_is_delegatable(suite: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as closed_port:
        closed_port.bind(("127.0.0.1", 0))
        port = closed_port.getsockname()[1]
        url = f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{port}/postgres"
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("TEST_DATABASE_URL", url)
        monkeypatch.setenv("PYTEST_ADDOPTS", "")
        declaration = load_verification_declaration(REPOSITORY / ".absent-plugin", REPOSITORY)
        check = next(check for check in declaration.checks if check.id == suite)
        result, installed = asyncio.run(_verify(check, REPOSITORY, lockfile_installs=False))
    assert not installed
    assert result.outcome == "unavailable"
    assert result.missing == ()
    assert result.blocked == ("postgres",)
