"""Prompt baseline admission through the shipped hook commands (#4813)."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

_GIT_SHIM = r"""
import os
import signal
import sqlite3
import sys
import time

args = sys.argv[1:]
capture = args[-3:] == ["rev-parse", "--show-toplevel", "HEAD"]
db = sqlite3.connect(os.environ["FACTORY_FINGERPRINT_DB"])
db.execute("INSERT INTO calls (capture) VALUES (?)", (int(capture),))
attempt = db.execute("SELECT count(*) FROM calls WHERE capture = 1").fetchone()[0]
db.commit()
db.close()
case = os.environ["FACTORY_FINGERPRINT_CASE"]
if capture:
    if case == "fail" or (case == "fail_first" and attempt == 1):
        sys.exit(1)
    if case in ("interrupt_first", "kill_first") and attempt == 1:
        os.kill(os.getpid(), signal.SIGTERM if case == "interrupt_first" else signal.SIGKILL)
    if case in ("interrupt_hook", "kill_hook"):
        os.kill(os.getppid(), signal.SIGTERM if case == "interrupt_hook" else signal.SIGKILL)
        sys.exit(1)
    if case == "delayed" and attempt == 1:
        time.sleep(2)
        sys.exit(1)
elif case == "delayed":
    time.sleep(2)
os.execv(os.environ["FACTORY_FINGERPRINT_REAL_GIT"], ["git", *args])
"""


class CaptureRun:
    def __init__(self, tmp_path: Path, helpers: Any) -> None:
        self.helpers = helpers
        self.session = helpers.Session(tmp_path)
        self.run = helpers.HooksJsonRuntime(self.session)
        self.database = tmp_path / "calls.sqlite3"
        with sqlite3.connect(self.database) as db:
            db.execute("CREATE TABLE calls (capture INTEGER NOT NULL)")
        real_git = shutil.which("git", path=self.session.env.get("PATH"))
        assert real_git is not None, "Git is required for the hook runtime"
        shim_dir = tmp_path / "bin"
        shim_dir.mkdir()
        shim = shim_dir / "git"
        shim.write_text(f"#!{sys.executable}\n" + _GIT_SHIM)
        shim.chmod(0o755)
        self.run.env.update(
            PATH=str(shim_dir) + os.pathsep + self.session.env.get("PATH", ""),
            PYTHONDONTWRITEBYTECODE="1",
            FACTORY_FINGERPRINT_DB=str(self.database),
            FACTORY_FINGERPRINT_REAL_GIT=real_git,
            FACTORY_FINGERPRINT_CASE="healthy",
        )

    def case(self, name: str) -> None:
        self.run.env["FACTORY_FINGERPRINT_CASE"] = name

    def prompt(self) -> str:
        outs = self.run.dispatch("UserPromptSubmit", prompt="Implement the issue")
        assert len(outs) == 1
        return str(self.helpers.context_of(outs[0]))

    def state(self) -> dict[str, Any]:
        path = Path(self.session.env["DARK_FACTORY_STATE_DIR"]) / "sess-fresh.json"
        state: dict[str, Any] = json.loads(path.read_text())
        return state

    def calls(self, *, captures_only: bool = False) -> int:
        with sqlite3.connect(self.database) as db:
            query = "SELECT count(*) FROM calls"
            if captures_only:
                query += " WHERE capture = 1"
            return int(db.execute(query).fetchone()[0])

    def plan(self, tool: str = "Agent") -> dict[str, Any]:
        out = self.run.pre(
            tool,
            {
                "subagent_type": self.helpers.PLAN,
                "description": "plan-reviewer",
                "prompt": "Review the plan",
                "model": "sonnet",
                "isolation": "worktree",
                "run_in_background": True,
            },
        )
        assert out is not None
        decision: dict[str, Any] = out["hookSpecificOutput"]
        return decision


@pytest.fixture
def capture_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CaptureRun:
    monkeypatch.setattr(sys, "dont_write_bytecode", True)
    helper_path = (
        Path(__file__).resolve().parents[2] / "examples/tests/test_dark_factory_review_gate.py"
    )
    spec = importlib.util.spec_from_file_location("factory_fingerprint_helpers", helper_path)
    assert spec is not None and spec.loader is not None
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    return CaptureRun(tmp_path, helpers)


def test_missing_initial_fingerprint_refuses_review_without_spending_round(
    capture_run: CaptureRun,
) -> None:
    c = capture_run
    c.case("fail")
    context = c.prompt()

    assert context.startswith("STOP.")
    assert "fingerprint" in context.lower()
    assert "new prompt" in context.lower()
    assert c.calls(captures_only=True) == 2
    assert c.state()["fingerprint"] is None

    # Bash may change the checkout, so restored Git access cannot create a baseline now.
    c.case("healthy")
    assert c.run.bash("printf '\nCHANGED = True\n' >> pkg/calc.py") == 0
    calls = c.calls()
    for tool in ("Agent", "Task", "Agent"):
        decision = c.plan(tool)
        assert decision["permissionDecision"] == "deny"
        assert "checkout_unverifiable" in decision["permissionDecisionReason"]
        assert "updatedInput" not in decision
        assert c.state()["plan"] == {"round": 0, "verdict": None}
    assert c.calls() == calls
    assert c.state()["fingerprint"] is None
    assert c.session.phases() == []
    assert c.run.decision(c.helpers.PUBLISH) == "deny"


@pytest.mark.parametrize("case", ["fail_first", "interrupt_first", "kill_first"])
def test_transient_capture_failure_recovers_before_review(
    capture_run: CaptureRun, case: str
) -> None:
    c = capture_run
    c.case(case)
    assert c.helpers.contract() in c.prompt()
    assert c.calls(captures_only=True) == 2
    assert c.state()["fingerprint"] is not None
    assert c.state()["plan"]["round"] == 0

    decision = c.plan()
    assert decision["permissionDecision"] == "allow"
    updated = decision["updatedInput"]
    assert updated["run_in_background"] is False
    assert "model" not in updated and "isolation" not in updated
    result = c.run.dispatch(
        "PostToolUse",
        "Agent",
        tool_input=updated,
        tool_response={
            "content": [{"type": "text", "text": c.helpers.reply(c.helpers.PLAN, "APPROVE")}]
        },
    )
    assert result[0] is not None
    assert "APPROVED" in result[0]["hookSpecificOutput"]["additionalContext"]
    assert c.state()["plan"] == {"round": 1, "verdict": "APPROVE"}
    assert c.calls(captures_only=True) == 3


def test_capture_attempts_share_the_seven_second_deadline(capture_run: CaptureRun) -> None:
    c = capture_run
    c.case("delayed")
    started = time.monotonic()
    context = c.prompt()
    elapsed = time.monotonic() - started

    assert 6.5 <= elapsed < 9.0, elapsed
    assert c.calls(captures_only=True) == 2
    assert context.startswith("STOP.")
    assert c.state()["fingerprint"] is None
    assert c.plan()["permissionDecision"] == "deny"
    assert c.state()["plan"]["round"] == 0


@pytest.mark.parametrize("case", ["interrupt_hook", "kill_hook"])
def test_capture_interruption_saves_reset_and_cannot_capture_later(
    capture_run: CaptureRun, case: str
) -> None:
    c = capture_run
    c.prompt()
    assert "APPROVED" in c.run.review(c.helpers.PLAN, "APPROVE")
    assert "APPROVED" in c.run.review(c.helpers.DIFF, "APPROVE")
    c.case(case)
    with pytest.raises(subprocess.CalledProcessError):
        c.prompt()

    state = c.state()
    assert state["fingerprint"] is None
    assert state["plan"] == {"round": 0, "verdict": None}
    assert state["diff"] == {"round": 0, "verdict": None}
    assert state["workflow"] is False
    assert state["stage"] is None
    c.case("healthy")
    calls = c.calls()
    assert c.plan()["permissionDecision"] == "deny"
    assert c.run.decision(c.helpers.PUBLISH) == "deny"
    assert c.calls() == calls
    assert c.state()["plan"]["round"] == 0


@pytest.mark.parametrize("mutate", [False, True])
def test_a_fresh_prompt_recovers_round_one_and_still_checks_for_changes(
    capture_run: CaptureRun, mutate: bool
) -> None:
    c = capture_run
    c.case("fail")
    assert c.prompt().startswith("STOP.")
    assert c.plan()["permissionDecision"] == "deny"
    assert c.run.bash("printf '\nREPAIRED = True\n' >> pkg/calc.py") == 0
    c.case("healthy")
    assert c.helpers.contract() in c.prompt()
    assert c.state()["plan"] == {"round": 0, "verdict": None}
    if mutate:
        assert c.run.bash("printf '\nLATER = True\n' >> pkg/calc.py") == 0

    context = c.run.review(c.helpers.PLAN, "APPROVE")
    assert c.state()["plan"]["round"] == 1
    if mutate:
        assert context.startswith("STOP.")
        assert "checkout_changed_before_plan_approval" in context
        assert c.state()["plan"]["verdict"] is None
        assert c.run.decision(c.helpers.PUBLISH) == "deny"
    else:
        assert "APPROVED" in context
        assert c.state()["plan"]["verdict"] == "APPROVE"


def test_task_notification_preserves_the_baseline_without_recapturing(
    capture_run: CaptureRun,
) -> None:
    c = capture_run
    c.prompt()
    assert "APPROVED" in c.run.review(c.helpers.PLAN, "APPROVE")
    before, calls = c.state(), c.calls()
    c.case("fail")

    assert c.run.dispatch("UserPromptSubmit", prompt=c.helpers._TASK_NOTIFICATION) == [None]
    assert c.state() == before
    assert c.calls() == calls
    denied = c.run.pre(
        "Agent",
        {
            "subagent_type": c.helpers.DIFF,
            "description": "diff-reviewer",
            "prompt": "Review the diff",
        },
    )
    assert denied is not None
    assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert c.state() == before
    c.case("healthy")
    assert "APPROVED" in c.run.review(c.helpers.DIFF, "APPROVE")


def test_ci_fix_round_needs_no_initial_fingerprint(capture_run: CaptureRun) -> None:
    c = capture_run
    c.case("fail")
    context = c.run.dispatch("UserPromptSubmit", prompt=c.helpers._ci_prompt(2))

    assert c.helpers.contract() in c.helpers.context_of(context[0])
    assert c.calls() == 0
    assert c.state()["stage"] == "implement"
    assert c.state()["fingerprint"] is None
    assert c.plan()["permissionDecision"] == "deny"
    denied = c.run.pre(
        "Agent",
        {
            "subagent_type": c.helpers.DIFF,
            "description": "diff-reviewer",
            "prompt": "Review the diff",
        },
    )
    assert denied is not None
    assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert c.state()["diff"]["round"] == 0
    assert c.state()["stopped"] is None
    assert c.state()["fingerprint"] is None
    c.case("healthy")
    assert "APPROVED" in c.run.review(c.helpers.DIFF, "APPROVE")
    assert c.state()["fingerprint"] is None
