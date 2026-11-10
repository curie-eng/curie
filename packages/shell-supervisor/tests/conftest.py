from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from curie_test_support.static_shell import ShellTls, SupervisorProcess, make_shell_tls


@pytest.fixture(scope="session")
def tls(tmp_path_factory: pytest.TempPathFactory) -> ShellTls:
    return make_shell_tls(tmp_path_factory.mktemp("tls"))


@pytest.fixture
def records(tmp_path: Path) -> Path:
    path = tmp_path / "records"
    path.mkdir()
    return path


@pytest.fixture
def supervisor(tls: ShellTls, tmp_path: Path) -> Iterator[SupervisorProcess]:
    shell = SupervisorProcess(
        slot="slot-a",
        tls=tls,
        root=tmp_path / "shell",
        runner_command=f"{sys.executable} -m curie_test_support.fake_shell_runner",
    )
    try:
        yield shell.start()
    finally:
        shell.stop()
