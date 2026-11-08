"""A failed restore must not log a private value, and every scenario module imports first.

Covers the ``Evidence.hold_private`` short-value refusal,
``identity_scenarios_requester._quietly`` and the import order of the scenario modules.
Synthetic values only.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import identity_scenarios_requester as requester
import pytest
from identity_e2e import HarnessError
from identity_scenarios import Evidence

HARNESS = Path(__file__).resolve().parents[1]
SEG = "A1b2C3d4E5f6G7h8_-9z"
TICKET = f"cat1.{SEG}.{SEG}"
SUBJECT = "synthetic-subject-0002"
SHORT = "abc1234"


def held(*values: str) -> Evidence:
    evidence = Evidence("zz-restore-log")
    evidence.hold_private(values)
    return evidence


# --- Evidence.hold_private --------------------------------------------------------


def test_hold_private_refuses_a_short_value() -> None:
    with pytest.raises(HarnessError) as caught:
        held(SHORT)
    assert "shorter than 8 characters" in str(caught.value)
    assert SHORT not in str(caught.value)


# --- _quietly ---------------------------------------------------------------------


def test_quietly_logs_the_failure_without_private_values(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def call() -> None:
        raise HarnessError(f"sql failed params=({SUBJECT!r}, {TICKET!r})")

    assert requester._quietly(held(SUBJECT), call) is None
    captured = capsys.readouterr()
    assert captured.err.strip().endswith("restore failed: HarnessError")
    assert SUBJECT not in captured.err
    assert TICKET not in captured.err
    assert SEG not in captured.err


def test_quietly_returns_the_value_and_logs_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    assert requester._quietly(held(SUBJECT), lambda: {"ok": 1}) == {"ok": 1}
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_quietly_propagates_other_exceptions() -> None:
    def call() -> None:
        raise KeyError("boom")

    with pytest.raises(KeyError):
        requester._quietly(held(SUBJECT), call)


# --- import order -----------------------------------------------------------------


@pytest.mark.parametrize(
    "module",
    [
        "identity_scenarios_attestation",
        "identity_scenarios_stored_card",
        "identity_scenarios_requester",
        "identity_scenarios",
    ],
)
def test_every_scenario_module_imports_first(module: str) -> None:
    code = (
        f"import {module}\n"
        "import identity_scenarios\n"
        "names = identity_scenarios.SCENARIOS\n"
        "assert 'grant-attestation' in names, sorted(names)\n"
        "assert 'stored-arguments-card' in names, sorted(names)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=HARNESS,
        env={"PYTHONPATH": str(HARNESS), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-800:]
