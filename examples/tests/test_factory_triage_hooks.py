"""Exercise the deployed hook configuration with a real checkout (#5587)."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_independent
ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/factory-triage"
SUBMIT = "mcp__curie__submit_tracker_result"


class Session:
    def __init__(self, path: Path):
        self.cwd = path
        self.cwd.mkdir()
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "Example")
        (path / "calc.py").write_text("def add(a, b): return a - b\n")
        self.git("add", ".")
        self.git("commit", "-qm", "Seed defect")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.env.pop("CURIE_SESSION_ID", None)
        self.env["CLAUDE_PLUGIN_ROOT"] = str(BUNDLE)
        self.config = json.loads((BUNDLE / "hooks/hooks.json").read_text())["hooks"]
        self.serial = 0

    def git(self, *args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=self.cwd, text=True)

    def event(self, event: str, tool: str, inputs: dict, **extra: object) -> dict:
        payload = dict(
            session_id="triage-session",
            cwd=str(self.cwd),
            hook_event_name=event,
            tool_name=tool,
            tool_input=inputs,
        )
        payload.update(extra)
        result = {}
        for group in self.config.get(event, []):
            if not re.fullmatch(group.get("matcher", ".*"), tool):
                continue
            for hook in group["hooks"]:
                command = hook["command"].replace("${CLAUDE_PLUGIN_ROOT}", str(BUNDLE))
                done = subprocess.run(
                    command,
                    shell=True,
                    input=json.dumps(payload),
                    text=True,
                    capture_output=True,
                    cwd=BUNDLE,
                    env=self.env,
                    timeout=12,
                )
                assert done.returncode == 0, done.stderr
                result = json.loads(done.stdout)
                if denied(result):
                    return result
        return result

    def run(self, command: str) -> dict:
        self.serial += 1
        call = f"call-{self.serial}"
        pre = self.event("PreToolUse", "Bash", {"command": command}, tool_use_id=call)
        assert not denied(pre), pre
        done = subprocess.run(command, shell=True, cwd=self.cwd, capture_output=True, text=True)
        # The SDK Bash response has stdout/stderr/interrupted; failed calls expose
        # an Exit code prefix. See https://code.claude.com/docs/en/hooks#posttooluse-input
        response = {"stdout": done.stdout, "stderr": done.stderr, "interrupted": False}
        if done.returncode:
            return self.event(
                "PostToolUseFailure",
                "Bash",
                {"command": command},
                tool_use_id=call,
                error=f"Exit code {done.returncode}\n" + done.stdout + done.stderr,
            )
        return self.event(
            "PostToolUse", "Bash", {"command": command}, tool_use_id=call, tool_response=response
        )

    def ticket(self, command: str = "python3 -c 'print(2 - 3)'", observed: str = "-1") -> dict:
        return dict(
            kind="ticket",
            title="Addition subtracts",
            body="Implement addition.",
            reproduction=dict(command=command, observed=observed, revision=self.revision),
        )

    def submit(self, proposal: dict) -> dict:
        return self.event("PreToolUse", SUBMIT, proposal)


def denied(result: dict) -> bool:
    return result.get("hookSpecificOutput", {}).get("permissionDecision") == "deny"


@pytest.fixture
def session(tmp_path: Path) -> Session:
    return Session(tmp_path / "repo")


def test_clean_recorded_probe_allows_ticket_and_untracked_test(session: Session) -> None:
    command = "python3 -c 'print(2 - 3)'"
    session.run(command)
    (session.cwd / "probe.py").write_text("assert False\n")
    assert not denied(session.submit(session.ticket(command, "Observed output:\n-1")))


@pytest.mark.parametrize(
    "fault",
    [
        "unrecorded",
        "invented_output",
        "dirty_record",
        "changed_revision",
        "dirty_submit",
        "missing_state",
        "corrupt_state",
        "wrong_session",
        "changed_during_command",
    ],
)
def test_gate_refuses_unsupported_ticket_but_allows_findings(session: Session, fault: str) -> None:
    command = "python3 -c 'print(2 - 3)'"
    if fault == "dirty_record":
        (session.cwd / "calc.py").write_text("def add(a, b): return a + b\n")
    if fault == "changed_during_command":
        command = "printf 'def add(a, b): return a + b\\n' > calc.py; echo -1"
    session.run(command)
    proposal = session.ticket(command)
    if fault in {"dirty_record", "changed_during_command"}:
        session.git("restore", "calc.py")
    elif fault == "unrecorded":
        proposal["reproduction"]["command"] = "echo -1"
    elif fault == "invented_output":
        proposal["reproduction"]["observed"] = "imaginary failure"
    elif fault == "changed_revision":
        session.git("commit", "--allow-empty", "-qm", "Different revision")
    elif fault == "dirty_submit":
        (session.cwd / "calc.py").write_text("changed\n")
    elif fault in {"missing_state", "corrupt_state"}:
        state = next((session.cwd / ".curie/factory-triage").glob("*.json"))
        if fault == "missing_state":
            state.unlink()
        else:
            state.write_text("invalid json")
    elif fault == "wrong_session":
        session.env["CURIE_SESSION_ID"] = "another-session"
    refusal = session.submit(proposal)
    assert denied(refusal), fault
    assert refusal["hookSpecificOutput"]["permissionDecisionReason"]
    assert not denied(
        session.submit(
            dict(
                kind="findings",
                reason="not_reproduced",
                tried="Ran probe",
                missing="Matching failure",
            )
        )
    )


def test_removing_gate_from_configuration_defeats_refusal(session: Session) -> None:
    assert denied(session.submit(session.ticket()))
    session.config["PreToolUse"] = []
    assert not denied(session.submit(session.ticket()))


@pytest.mark.parametrize("raw", [b"", b"{", b"\xff", b"null", b'{"cwd":"\\ud800"}'])
def test_malformed_input_denies_with_successful_hook_exit(raw: bytes) -> None:
    done = subprocess.run(
        [sys.executable, str(BUNDLE / "hooks/reproduction_gate.py")],
        input=raw,
        capture_output=True,
        timeout=12,
    )
    assert done.returncode == 0, done.stderr
    assert denied(json.loads(done.stdout))


def test_own_exception_denies_instead_of_failing_open(session: Session) -> None:
    (session.cwd / ".curie").write_text("not a directory")
    assert denied(session.event("PreToolUse", "Bash", {"command": "echo probe"}))


def test_internal_deadline_denies_and_live_probe_still_works(
    session: Session, tmp_path: Path
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    git = fake_bin / "git"
    git.write_text(f"#!{sys.executable}\nimport signal\nsignal.pause()\n")
    git.chmod(0o755)
    session.env["PATH"] = str(fake_bin) + os.pathsep + os.environ["PATH"]
    refusal = session.event("PreToolUse", "Bash", {"command": "echo probe"}, tool_use_id="slow-git")
    assert denied(refusal)
    assert "deadline" in refusal["hookSpecificOutput"]["permissionDecisionReason"]
    session.env["PATH"] = os.environ["PATH"]
    session.run("echo probe")
    assert not denied(session.submit(session.ticket("echo probe", "probe")))


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        ("Write", {"file_path": ".curie/factory-triage/forged.json", "content": "{}"}),
        ("Edit", {"file_path": "hooks/reproduction_gate.py", "old_string": "x", "new_string": "y"}),
        ("Bash", {"command": "rm -rf .curie"}),
        ("Bash", {"command": 'python3 -c \'open(".curie/factory-triage/x", "w").write("x")\''}),
        ("Bash", {"command": "git push origin HEAD"}),
        ("mcp__curie__publish_changes", {}),
        ("Agent", {"prompt": "Change the recorder"}),
    ],
)
def test_protected_mutations_are_refused(session: Session, tool: str, inputs: dict) -> None:
    assert denied(session.event("PreToolUse", tool, inputs))


def test_post_without_matching_pre_cannot_invent_evidence(session: Session) -> None:
    assert denied(
        session.event(
            "PostToolUse",
            "Bash",
            {"command": "echo -1"},
            tool_use_id="invented",
            tool_response={"stdout": "-1"},
        )
    )
    assert denied(session.submit(session.ticket("echo -1")))


def test_recorder_retains_failure_exit_status(session: Session) -> None:
    command = "python3 -c 'print(\"expected 5 got -1\"); raise SystemExit(1)'"
    session.run(command)
    assert not denied(session.submit(session.ticket(command, "expected 5 got -1")))
    state = json.loads(next((session.cwd / ".curie/factory-triage").glob("*.json")).read_text())
    assert state["runs"][-1]["exit_status"] == 1


def test_successful_output_cannot_invent_a_failure_exit_status(session: Session) -> None:
    session.run("python3 -c 'print(\"Exit code 7\\nprobe\")'")
    state = json.loads(next((session.cwd / ".curie/factory-triage").glob("*.json")).read_text())
    assert state["runs"][-1]["exit_status"] == 0


def test_lock_is_held_through_check_and_save(session: Session) -> None:
    import fcntl

    session.run("echo before")
    lock = next((session.cwd / ".curie/factory-triage").glob("*.lock"))
    with lock.open("r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        refusal = session.event("PreToolUse", "Bash", {"command": "echo after"})
        assert denied(refusal)
    session.run("echo after")
    assert not denied(session.submit(session.ticket("echo after", "after")))


@pytest.mark.parametrize(
    "field",
    [
        "command",
        "exit_status",
        "revision",
        "clean",
        "output",
        "output_digest",
        "entry_digest",
    ],
)
def test_accidental_entry_corruption_refuses_ticket_and_preserves_findings(
    session: Session,
    field: str,
) -> None:
    session.run("echo probe")
    path = next((session.cwd / ".curie/factory-triage").glob("*.json"))
    state = json.loads(path.read_text())
    state["runs"][0][field] = "hand edited"
    path.write_text(json.dumps(state))
    assert denied(session.submit(session.ticket("echo probe", "probe")))
    assert not denied(
        session.submit(
            dict(kind="findings", reason="not_reproduced", tried="probe", missing="valid evidence")
        )
    )


def test_documented_limit_deliberate_shell_state_forgery_is_not_detected(session: Session) -> None:
    """Document the accepted limit, not proof of protection against deliberate forgery."""
    code = (
        "from pathlib import Path; import hashlib,json; "
        "p=next(Path('.').glob('.*/*/*.json')); s=json.loads(p.read_text()); "
        "e=dict(command='echo fabricated',exit_status=0,"
        "revision=s['pending']['call-1']['revision'],clean=True,output='fabricated'); "
        "e['output_digest']=hashlib.sha256(e['output'].encode()).hexdigest(); "
        "e['entry_digest']=hashlib.sha256(json.dumps(e,sort_keys=True,"
        "separators=(',',':'),ensure_ascii=False).encode()).hexdigest(); "
        "s['runs']=[e]; p.write_text(json.dumps(s))"
    )
    session.run("python3 -c " + shlex.quote(code))
    assert not denied(session.submit(session.ticket("echo fabricated", "fabricated")))


def test_undecodable_state_refuses_ticket_but_findings_needs_no_checkout(session: Session) -> None:
    session.run("echo probe")
    path = next((session.cwd / ".curie/factory-triage").glob("*.json"))
    path.write_bytes(b"\xff")
    assert denied(session.submit(session.ticket("echo probe", "probe")))
    assert not denied(session.event("PreToolUse", SUBMIT, dict(kind="findings"), cwd="unused"))


def test_input_read_deadline_returns_deny_before_runner_timeout() -> None:
    process = subprocess.Popen(
        [sys.executable, str(BUNDLE / "hooks/reproduction_gate.py")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.wait(timeout=12) == 0
        assert process.stdout is not None
        response = json.loads(process.stdout.read())
        assert denied(response)
        assert "deadline" in response["hookSpecificOutput"]["permissionDecisionReason"]
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
