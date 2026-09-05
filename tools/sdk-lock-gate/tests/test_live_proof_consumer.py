"""Fixture-controlled guard executions; these never claim a live SDK proof."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
CONSUMER = ROOT / "tools/sdk-lock-gate/run-proof.py"
START = "=== case: a live gated tool call is denied and the turn parks (#2094) ==="
PARK = "the gated turn parked: status=awaiting-approval finalized=false approval_summary=present"
ABSENT = "the gated command did not run: /tmp/curie-2094-canary is absent"


def run(
    tmp_path: Path, lines: list[str], exit_code: int = 0, live: str = "1"
) -> subprocess.CompletedProcess[str]:
    ladder = tmp_path / "ladder.sh"
    ladder.write_text(
        "#!/bin/bash\ncat <<'OUTPUT'\n" + "\n".join(lines) + f"\nOUTPUT\nexit {exit_code}\n"
    )
    return subprocess.run(
        [sys.executable, str(CONSUMER), "--ladder", str(ladder)],
        env=dict(os.environ, CURIE_E2E_LIVE=live, CURIE_E2E_TIERS="skill"),
        text=True,
        capture_output=True,
        check=False,
    )


def test_observed_park_and_no_effect_with_success_are_accepted(tmp_path: Path) -> None:
    result = run(tmp_path, [START, PARK, ABSENT])
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("missing", [START, PARK, ABSENT])
def test_missing_case_or_assertion_cannot_succeed(tmp_path: Path, missing: str) -> None:
    result = run(tmp_path, [line for line in [START, PARK, ABSENT] if line != missing])
    assert result.returncode != 0
    assert "::error title=" in result.stderr


@pytest.mark.parametrize("exit_code", [0, 1])
def test_reset_failure_before_case_is_named_failure(tmp_path: Path, exit_code: int) -> None:
    result = run(tmp_path, ["runner reset returned HTTP 500"], exit_code)
    assert result.returncode != 0
    assert "setup/reset failed before approval-gate case" in result.stderr


def test_a_later_ladder_failure_is_not_hidden_by_successful_case(tmp_path: Path) -> None:
    assert run(tmp_path, [START, PARK, ABSENT], 1).returncode != 0


def test_non_live_mode_refuses_before_launching_ladder(tmp_path: Path) -> None:
    result = run(tmp_path, [START, PARK, ABSENT], live="0")
    assert result.returncode != 0
    assert START not in result.stdout
