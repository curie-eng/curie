"""The real-SDK skip decision counts the SDK's packaged CLI (#5204).

Hosted CI installs no global `claude`, yet the locked claude-agent-sdk ships an
executable one and spawns it ahead of anything on PATH. The prerequisite for the
real-SDK catalogue tests must follow that discovery, not `which("claude")`.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from sdk_cli_support import sdk_cli_available, sdk_cli_path


@pytest.fixture
def cli_free_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert shutil.which("claude") is None


def test_packaged_cli_counts_as_prerequisite_without_global_claude(
    cli_free_path: None,
) -> None:
    assert sdk_cli_available(), "the SDK's packaged CLI must satisfy the prerequisite"


def test_discovered_cli_is_a_real_executable_the_sdk_would_spawn(
    cli_free_path: None,
) -> None:
    path = sdk_cli_path()
    assert path is not None
    assert "_bundled" in Path(path).parts
    result = subprocess.run(
        [path, "--version"], capture_output=True, text=True, timeout=60, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()
