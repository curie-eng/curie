"""The parity ladder must run under the bash macOS ships.

``curie dev e2e-ladder`` runs ``bash cli/scripts/e2e-ladder.sh`` with whatever
``bash`` PATH names first, which on a stock Mac is ``/bin/bash`` 3.2. Two things
3.2 refuses and bash 4.4+ accepts reached the local rung: ``[[ -v NAME ]]`` is a
parse error that kills the whole script before any rung starts, and expanding an
empty array under ``set -u`` is an ``unbound variable`` error at the point it
runs. These tests need a 3.x interpreter, so they run where one exists: macOS
``/bin/bash``, or any interpreter named by ``CURIE_TEST_BASH3``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LADDER_PATH = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"


def _bash3() -> str | None:
    for candidate in (os.environ.get("CURIE_TEST_BASH3"), "/bin/bash"):
        if not candidate or not Path(candidate).is_file():
            continue
        major = subprocess.run(
            [candidate, "-c", 'printf %s "${BASH_VERSINFO[0]}"'],
            text=True,
            capture_output=True,
            check=False,
        ).stdout
        if major == "3":
            return candidate
    return None


BASH3 = _bash3()
needs_bash3 = pytest.mark.skipif(
    BASH3 is None,
    reason="no bash 3.x here: macOS /bin/bash, or set CURIE_TEST_BASH3",
)


def _shell_function(source: str, name: str) -> str:
    start_marker = f"{name}() {{"
    assert start_marker in source, f"{LADDER_PATH}: missing {name}"
    start = source.index(start_marker)
    end = source.index("\n}\n", start) + len("\n}\n")
    return source[start:end]


@needs_bash3
def test_ladder_parses_under_bash_3_2() -> None:
    result = subprocess.run(
        [BASH3, "-n", str(LADDER_PATH)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"{LADDER_PATH} does not parse under {BASH3}, so every rung dies "
        f"before it starts: {result.stderr}"
    )


@needs_bash3
def test_approval_seed_mints_its_principal_under_bash_3_2(tmp_path: Path) -> None:
    """The local rung's approval seed reaches the CLI with no scope arguments."""

    seed = _shell_function(LADDER_PATH.read_text(), "seed_approval_resume_turn")
    invocations = tmp_path / "invocations"
    stub_bin = tmp_path / "curie"
    stub_bin.write_text(
        "#!/bin/sh\n"
        'printf \'%s\\n\' "$*" >> "$STUB_INVOCATIONS"\n'
        "exit 1\n"
    )
    stub_bin.chmod(0o700)
    script = f"""set -euo pipefail
LIVE=0
WORKDIR="$1"
BIN="$2"
prepare_approval_seed_fixture() {{ APPROVAL_SEED_AGENT_ID=acme-approval-agent; }}
{seed}
seed_approval_resume_turn local acme-bot
"""
    result = subprocess.run(
        [BASH3, "-c", script, "bash", str(tmp_path), str(stub_bin)],
        env={**os.environ, "STUB_INVOCATIONS": str(invocations)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert "unbound variable" not in result.stderr, result.stderr
    assert invocations.is_file(), (
        f"the approval seed never reached the CLI under {BASH3}: {result.stderr}"
    )
    assert invocations.read_text().splitlines() == [
        "--json local approvals acme-approval-agent "
        "--mint-operator-principal U0EXAMPLE1"
    ]
    # The stub refuses the mint, so the seed must stop at its own refusal.
    assert result.returncode == 1
    assert "could not mint deterministic approval principal" in result.stderr
