"""Subprocess proof of the factory selector hook.

Payloads use the SDK CLI shape recorded in test_dark_factory_review_gate.py
and the GitHub issue JSON that server-github 2025.4.8 returns, whose labels
are objects with a name. Each refusal is paired with an allowed call.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
HOOK = REPO / "examples" / "factory-selector" / "hooks" / "selector_gate.py"
GET = "mcp__plugin_factory-selector_github__get_issue"
UPDATE = "mcp__plugin_factory-selector_github__update_issue"
LABEL = "curie-factory"

# recorded, requested, extra fields, reason needle, labels for the allowed
# call, and whether that allow is the same session, a read-then-allow, or a
# fresh session (a blocked label cannot be repaired in place).
_REFUSALS = [
    (None, ["bug", LABEL], {}, "not read", ["bug"], "after-read"),
    (["bug", "good first issue"], ["bug", LABEL], {}, "drops", ["bug", "good first issue"], "same"),
    (
        ["bug", "good first issue"],
        ["bug", "good first issue", LABEL, "extra"],
        {},
        "other than the policy label",
        ["bug", "good first issue"],
        "same",
    ),
    (["bug"], [LABEL, "bug"], {"title": "Renamed"}, "title", ["bug"], "same"),
    (["bug"], [LABEL, "bug"], {"body": "rewritten"}, "body", ["bug"], "same"),
    (["bug"], [LABEL, "bug"], {"state": "closed"}, "state", ["bug"], "same"),
    (["bug", LABEL], ["bug", LABEL], {}, "policy label", ["bug"], "fresh"),
    (
        ["bug", "curie-factory:queued"],
        ["bug", "curie-factory:queued", LABEL],
        {},
        "curie-factory:",
        ["bug"],
        "fresh",
    ),
    (
        ["curie:intake"],
        ["curie:intake", LABEL],
        {},
        "curie:",
        ["bug"],
        "fresh",
    ),
    (
        ["bug"],
        ["bug", LABEL],
        {"owner": "other-org", "repo": "other-bot"},
        "repository",
        ["bug"],
        "same",
    ),
]


def _shipped() -> dict[str, Any]:
    return json.loads((REPO / "examples" / "factory-selector" / "policy.json").read_text())


def _blocks(labels: list[str]) -> list[dict[str, str]]:
    issue = {
        "number": 12,
        "title": "Widget flaky on cold start",
        "state": "open",
        "labels": [{"id": i + 1, "name": name, "color": "ededed"} for i, name in enumerate(labels)],
    }
    return [{"type": "text", "text": json.dumps(issue)}]


class Gate:
    def __init__(
        self,
        root: Path,
        policy: dict[str, Any] | None = None,
        session: str | None = "sess-1",
    ) -> None:
        self.state = root / "state"
        self.cwd = root / "workspace"
        self.cwd.mkdir(parents=True, exist_ok=True)
        self.policy_path = root / "policy.json"
        self.policy_path.write_text(json.dumps(_shipped() if policy is None else policy))
        self.env = {
            "PATH": os.environ.get("PATH", ""),
            "FACTORY_SELECTOR_STATE_DIR": str(self.state),
            "FACTORY_SELECTOR_POLICY": str(self.policy_path),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        if session is not None:
            self.env["CURIE_SESSION_ID"] = session

    def send(self, event: str, **fields: Any) -> dict[str, Any] | None:
        payload = {
            "session_id": "payload-session",
            "hook_event_name": event,
            "cwd": str(self.cwd),
            **fields,
        }
        done = subprocess.run(
            [sys.executable, str(HOOK)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            env=self.env,
            check=False,
        )
        assert done.returncode == 0, done.stderr
        return json.loads(done.stdout) if done.stdout.strip() else None

    def raw(self, payload: bytes) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [sys.executable, str(HOOK)],
            input=payload,
            capture_output=True,
            env=self.env,
            check=False,
        )

    def read(self, number: int, labels: list[str], **identity: str) -> None:
        self.send(
            "PostToolUse",
            tool_name=GET,
            tool_input={
                "owner": identity.get("owner", "acme-corp"),
                "repo": identity.get("repo", "acme-bot"),
                "issue_number": number,
            },
            tool_response=_blocks(labels),
        )

    def update(self, number: int, labels: list[str], **extra: Any) -> dict[str, Any]:
        tool_input = {
            "owner": extra.pop("owner", "acme-corp"),
            "repo": extra.pop("repo", "acme-bot"),
            "issue_number": number,
            "labels": labels,
            **extra,
        }
        out = self.send("PreToolUse", tool_name=UPDATE, tool_input=tool_input)
        assert out is not None
        return out["hookSpecificOutput"]


def _allow(gate: Gate, number: int, labels: list[str]) -> None:
    gate.read(number, labels)
    decision = gate.update(number, [*labels, LABEL])
    assert decision["permissionDecision"] == "allow", decision


def test_three_preserving_writes_then_the_fourth_names_the_cap(tmp_path: Path) -> None:
    gate = Gate(tmp_path)
    batches = ((12, ["bug", "good first issue"]), (15, ["docs"]), (18, ["help wanted"]))
    for number, labels in batches:
        _allow(gate, number, labels)
    stored = json.loads((gate.state / "sess-1.json").read_text())
    assert stored["reads"]["acme-corp/acme-bot/12"] == ["bug", "good first issue"]
    assert stored["allowed"] == [
        "acme-corp/acme-bot/12",
        "acme-corp/acme-bot/15",
        "acme-corp/acme-bot/18",
    ]
    gate.read(21, ["bug"])
    denied = gate.update(21, ["bug", LABEL])
    assert denied["permissionDecision"] == "deny"
    assert "cap of 3" in denied["permissionDecisionReason"]


def test_an_out_of_range_cap_and_a_smaller_cap_refuse(tmp_path: Path) -> None:
    wide = Gate(tmp_path / "wide", policy={**_shipped(), "cap": 4})
    wide.read(12, ["bug"])
    assert "out of range" in wide.update(12, ["bug", LABEL])["permissionDecisionReason"]

    narrow = Gate(tmp_path / "narrow", policy={**_shipped(), "cap": 2})
    _allow(narrow, 12, ["bug"])
    _allow(narrow, 15, ["docs"])
    narrow.read(18, ["help wanted"])
    denied = narrow.update(18, ["help wanted", LABEL])
    assert "cap of 2" in denied["permissionDecisionReason"]

    one = Gate(tmp_path / "one", policy={**_shipped(), "cap": 1})
    _allow(one, 12, ["bug"])
    assert one.update(12, ["bug", LABEL])["permissionDecision"] == "allow"
    one.read(15, ["docs"])
    assert "cap of 1" in one.update(15, ["docs", LABEL])["permissionDecisionReason"]


@pytest.mark.parametrize(
    "recorded,requested,extra,needle,allow_labels,mode",
    _REFUSALS,
)
def test_each_refusal_is_paired_with_an_allowed_call(
    tmp_path: Path,
    recorded: list[str] | None,
    requested: list[str],
    extra: dict[str, Any],
    needle: str,
    allow_labels: list[str],
    mode: str,
) -> None:
    gate = Gate(tmp_path)
    if recorded is not None:
        gate.read(12, recorded)
    denied = gate.update(12, requested, **extra)
    assert denied["permissionDecision"] == "deny"
    assert needle in denied["permissionDecisionReason"]
    if mode == "fresh":
        gate = Gate(tmp_path / "fresh")
    if mode != "same":
        gate.read(12, allow_labels)
    allowed = gate.update(12, [*allow_labels, LABEL])
    assert allowed["permissionDecision"] == "allow", allowed


def test_a_resumed_session_keeps_its_count_and_a_new_one_starts_at_zero(tmp_path: Path) -> None:
    first = Gate(tmp_path, session="sess-keep")
    for number, labels in ((12, ["bug"]), (15, ["docs"]), (18, ["help wanted"])):
        _allow(first, number, labels)
    resumed = Gate(tmp_path, session="sess-keep")
    resumed.read(21, ["bug"])
    denied = resumed.update(21, ["bug", LABEL])
    assert "cap of 3" in denied["permissionDecisionReason"]
    other = Gate(tmp_path, session="sess-new")
    _allow(other, 21, ["bug"])


def test_missing_policy_malformed_state_and_unreadable_state_refuse(tmp_path: Path) -> None:
    missing = Gate(tmp_path / "missing")
    missing.policy_path.unlink()
    missing.read(12, ["bug"])
    reason = missing.update(12, ["bug", LABEL])["permissionDecisionReason"]
    assert "policy file is missing" in reason

    malformed = Gate(tmp_path / "malformed")
    malformed.read(12, ["bug"])
    (malformed.state / "sess-1.json").write_text("{", encoding="utf-8")
    assert "malformed" in malformed.update(12, ["bug", LABEL])["permissionDecisionReason"]

    unreadable = Gate(tmp_path / "unreadable")
    unreadable.read(12, ["bug"])
    state_file = unreadable.state / "sess-1.json"
    state_file.write_text("nope", encoding="utf-8")
    state_file.chmod(0)
    try:
        reason = unreadable.update(12, ["bug", LABEL])["permissionDecisionReason"]
    finally:
        state_file.chmod(0o600)
    assert "unreadable" in reason or "malformed" in reason


def test_state_lives_under_the_session_workspace_when_no_directory_is_set(tmp_path: Path) -> None:
    gate = Gate(tmp_path, session="")
    del gate.env["FACTORY_SELECTOR_STATE_DIR"]
    _allow(gate, 12, ["bug"])
    stored = json.loads((gate.cwd / ".factory-selector" / "payload-session.json").read_text())
    assert stored["allowed"] == ["acme-corp/acme-bot/12"]


def _reason(done: subprocess.CompletedProcess[bytes]) -> str:
    assert done.returncode == 0, done.stderr
    output = json.loads(done.stdout)["hookSpecificOutput"]
    assert output["permissionDecision"] == "deny"
    assert output["hookEventName"] == "PreToolUse"
    return output["permissionDecisionReason"]


def _decision_of(payload: dict[str, Any] | None) -> dict[str, Any]:
    assert payload is not None
    return payload["hookSpecificOutput"]


def test_a_state_reset_is_denied_and_the_fourth_write_stays_refused(tmp_path: Path) -> None:
    gate = Gate(tmp_path)
    for number, labels in ((12, ["bug"]), (15, ["docs"]), (18, ["help wanted"])):
        _allow(gate, number, labels)
    state_path = gate.state / "sess-1.json"
    before = state_path.read_bytes()
    reset = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="Write",
            tool_input={
                "file_path": str(state_path),
                "content": json.dumps({"reads": {}, "allowed": []}),
            },
        )
    )
    assert reset["permissionDecision"] == "deny"
    assert "session cap state" in reset["permissionDecisionReason"]
    assert state_path.read_bytes() == before

    edited = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="Edit",
            tool_input={
                "file_path": str(state_path),
                "old_string": "allowed",
                "new_string": "cleared",
            },
        )
    )
    assert edited["permissionDecision"] == "deny"
    assert state_path.read_bytes() == before

    nested = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="MultiEdit",
            tool_input={
                "edits": [{"file_path": str(state_path), "old_string": "a", "new_string": "b"}]
            },
        )
    )
    assert nested["permissionDecision"] == "deny"
    assert state_path.read_bytes() == before

    shell = _decision_of(
        gate.send("PreToolUse", tool_name="Bash", tool_input={"command": "rm -rf ."})
    )
    assert "shell" in shell["permissionDecisionReason"]
    other = _decision_of(
        gate.send("PreToolUse", tool_name="ApplyPatch", tool_input={"command": "patch the state"})
    )
    assert "shell" in other["permissionDecisionReason"]
    assert state_path.read_bytes() == before

    outside = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="Write",
            tool_input={"file_path": str(gate.cwd / "notes.txt"), "content": "leave the cap alone"},
        )
    )
    assert outside["permissionDecision"] == "deny"
    assert (
        gate.send(
            "PreToolUse",
            tool_name="Read",
            tool_input={"file_path": str(state_path)},
        )
        is None
    )

    gate.read(21, ["bug"])
    denied = gate.update(21, ["bug", LABEL])
    assert denied["permissionDecision"] == "deny"
    assert "cap of 3" in denied["permissionDecisionReason"]
    stored = json.loads(state_path.read_text())
    assert stored["allowed"] == [
        "acme-corp/acme-bot/12",
        "acme-corp/acme-bot/15",
        "acme-corp/acme-bot/18",
    ]


def test_overwriting_the_hook_is_denied_and_the_fourth_write_stays_refused(
    tmp_path: Path,
) -> None:
    gate = Gate(tmp_path)
    for number, labels in ((12, ["bug"]), (15, ["docs"]), (18, ["help wanted"])):
        _allow(gate, number, labels)
    bundle = HOOK.parent.parent
    targets = (
        HOOK,
        HOOK.parent / "hooks.json",
        bundle / ".claude-plugin" / "plugin.json",
        bundle / "policy.json",
    )
    before = {path: path.read_bytes() for path in targets}
    overwrite = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="Write",
            tool_input={"file_path": str(HOOK), "content": "pass\n"},
        )
    )
    assert overwrite["permissionDecision"] == "deny"
    assert "file write" in overwrite["permissionDecisionReason"]
    for path in (
        HOOK.parent / "hooks.json",
        bundle / ".claude-plugin" / "plugin.json",
        bundle / "policy.json",
    ):
        decision = _decision_of(
            gate.send(
                "PreToolUse",
                tool_name="Edit",
                tool_input={"file_path": str(path), "old_string": "{", "new_string": "pass"},
            )
        )
        assert decision["permissionDecision"] == "deny"
    pushed = _decision_of(
        gate.send(
            "PreToolUse",
            tool_name="mcp__plugin_factory-selector_github__push_files",
            tool_input={"files": [{"path": str(HOOK), "content": "pass\n"}]},
        )
    )
    assert pushed["permissionDecision"] == "deny"
    for path, original in before.items():
        assert path.read_bytes() == original
    gate.read(21, ["bug"])
    denied = gate.update(21, ["bug", LABEL])
    assert denied["permissionDecision"] == "deny"
    assert "cap of 3" in denied["permissionDecisionReason"]
    stored = json.loads((gate.state / "sess-1.json").read_text())
    assert stored["allowed"] == [
        "acme-corp/acme-bot/12",
        "acme-corp/acme-bot/15",
        "acme-corp/acme-bot/18",
    ]


def _blocked_on_state_lock(pid: int) -> bool:
    try:
        wchan = Path(f"/proc/{pid}/wchan").read_text(encoding="utf-8")
    except OSError:
        return False
    return "locks_" in wchan


def test_concurrent_updates_admit_exactly_three(tmp_path: Path) -> None:
    gate = Gate(tmp_path)
    batches = ((12, ["bug"]), (15, ["docs"]), (18, ["help wanted"]), (21, ["chore"]))
    for number, labels in batches:
        gate.read(number, labels)
    lock_path = gate.state / "sess-1.json.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    processes: list[subprocess.Popen[bytes]] = []
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        for number, labels in batches:
            payload = json.dumps(
                {
                    "session_id": "payload-session",
                    "hook_event_name": "PreToolUse",
                    "cwd": str(gate.cwd),
                    "tool_name": UPDATE,
                    "tool_input": {
                        "owner": "acme-corp",
                        "repo": "acme-bot",
                        "issue_number": number,
                        "labels": [*labels, LABEL],
                    },
                }
            ).encode()
            proc = subprocess.Popen(
                [sys.executable, str(HOOK)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=gate.env,
                close_fds=True,
            )
            assert proc.stdin is not None
            proc.stdin.write(payload)
            proc.stdin.close()
            processes.append(proc)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all(_blocked_on_state_lock(proc.pid) for proc in processes):
                break
            exited = [proc for proc in processes if proc.poll() is not None]
            if exited:
                stderr = exited[0].stderr.read() if exited[0].stderr is not None else b""
                raise AssertionError(stderr.decode(errors="replace"))
        else:
            raise AssertionError("updates did not wait on the state lock")
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
    try:
        allows = 0
        reasons: list[str] = []
        for proc in processes:
            stdout, stderr = proc.communicate(timeout=5)
            assert proc.returncode == 0, stderr
            decision = json.loads(stdout)["hookSpecificOutput"]
            if decision["permissionDecision"] == "allow":
                allows += 1
            else:
                reasons.append(decision["permissionDecisionReason"])
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
    assert allows == 3
    assert len(reasons) == 1
    assert "cap of 3" in reasons[0]
    stored = json.loads((gate.state / "sess-1.json").read_text())
    assert len(stored["allowed"]) == 3
    assert set(stored["allowed"]) <= {
        "acme-corp/acme-bot/12",
        "acme-corp/acme-bot/15",
        "acme-corp/acme-bot/18",
        "acme-corp/acme-bot/21",
    }


def test_missing_malformed_and_undecodable_input_fails_closed(tmp_path: Path) -> None:
    gate = Gate(tmp_path)
    assert "hook input is missing" in _reason(gate.raw(b""))
    assert "hook input is missing" in _reason(gate.raw(b" \n\t"))
    assert "hook input is malformed" in _reason(gate.raw(b"{"))
    assert "hook input is malformed" in _reason(gate.raw(b"[]"))
    assert "hook input is not valid UTF-8" in _reason(gate.raw(b"\xff"))
    gate.state.write_text("occupied", encoding="utf-8")
    failed = gate.update(12, ["bug", LABEL])
    assert failed["permissionDecision"] == "deny"
    assert "hook failed closed" in failed["permissionDecisionReason"]


def test_a_lock_wait_denies_before_the_runner_timeout(tmp_path: Path) -> None:
    spec = importlib.util.spec_from_file_location("selector_gate_deadline", HOOK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module._HOOK_DEADLINE_SECONDS < 60
    os.environ["FACTORY_SELECTOR_DEADLINE_SECONDS"] = "120"
    try:
        assert module._deadline_seconds() == module._HOOK_DEADLINE_SECONDS
        assert module._deadline_seconds() < 60
    finally:
        os.environ.pop("FACTORY_SELECTOR_DEADLINE_SECONDS", None)

    gate = Gate(tmp_path)
    gate.read(12, ["bug"])
    gate.env["FACTORY_SELECTOR_DEADLINE_SECONDS"] = "0.2"
    descriptor = os.open(gate.state / "sess-1.json.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    started = time.monotonic()
    try:
        done = subprocess.run(
            [sys.executable, str(HOOK)],
            input=json.dumps(
                {
                    "session_id": "payload-session",
                    "hook_event_name": "PreToolUse",
                    "cwd": str(gate.cwd),
                    "tool_name": UPDATE,
                    "tool_input": {
                        "owner": "acme-corp",
                        "repo": "acme-bot",
                        "issue_number": 12,
                        "labels": ["bug", LABEL],
                    },
                }
            ),
            capture_output=True,
            text=True,
            env=gate.env,
            check=False,
            timeout=5,
        )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
    elapsed = time.monotonic() - started
    assert done.returncode == 0, done.stderr
    output = json.loads(done.stdout)["hookSpecificOutput"]
    assert output["permissionDecision"] == "deny"
    assert "deadline" in output["permissionDecisionReason"]
    assert elapsed < 60
    stored = json.loads((gate.state / "sess-1.json").read_text())
    assert stored["allowed"] == []


# The runner image WORKDIR (runner/Dockerfile). ClaudeAgentOptions.cwd is
# omitted when /workspace has no git checkout, so the CLI hook payload cwd
# is this path. It is not a temporary directory standing in for that path.
_SDK_CWD = "/app"
_SESSION_WORKSPACE = "/workspace"

_READONLY_ROOT_DRIVER = r"""
import json
import os
import subprocess
import sys
from pathlib import Path

HOOK = "/opt/selector_gate.py"
POLICY = "/opt/policy.json"
SESSION = "sess-keep"
LABEL = "curie-factory"
GET = "mcp__plugin_factory-selector_github__get_issue"
UPDATE = "mcp__plugin_factory-selector_github__update_issue"


def issue(labels):
    body = {
        "number": 1,
        "title": "t",
        "state": "open",
        "labels": [
            {"id": i + 1, "name": name, "color": "ededed"} for i, name in enumerate(labels)
        ],
    }
    return [{"type": "text", "text": json.dumps(body)}]


def send(event, **fields):
    payload = {
        "session_id": "payload-session",
        "hook_event_name": event,
        "cwd": "/app",
        **fields,
    }
    env = {
        "PATH": "/usr/bin:/bin",
        "FACTORY_SELECTOR_POLICY": POLICY,
        "CURIE_SESSION_ID": SESSION,
        "PYTHONDONTWRITEBYTECODE": "1",
        "HOME": "/tmp",
    }
    done = subprocess.run(
        [sys.executable, HOOK],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if done.returncode != 0:
        sys.stderr.write(done.stderr)
        raise SystemExit(f"hook exit {done.returncode}")
    if not done.stdout.strip():
        return None
    return json.loads(done.stdout)


def read(number, labels):
    send(
        "PostToolUse",
        tool_name=GET,
        tool_input={"owner": "acme-corp", "repo": "acme-bot", "issue_number": number},
        tool_response=issue(labels),
    )


def update(number, labels):
    out = send(
        "PreToolUse",
        tool_name=UPDATE,
        tool_input={
            "owner": "acme-corp",
            "repo": "acme-bot",
            "issue_number": number,
            "labels": labels,
        },
    )
    return out["hookSpecificOutput"]


def allow(number, labels):
    read(number, labels)
    decision = update(number, [*labels, LABEL])
    if decision["permissionDecision"] != "allow":
        raise SystemExit(f"allow failed: {decision}")


def state_path():
    workspace = Path("/workspace")
    if workspace.is_dir() and os.access(workspace, os.W_OK):
        return workspace / ".factory-selector" / f"{SESSION}.json"
    return Path("/tmp/.factory-selector") / f"{SESSION}.json"


if sys.argv[1:] == ["resume"]:
    read(21, ["bug"])
    denied = update(21, ["bug", LABEL])
    reason = denied["permissionDecisionReason"]
    if denied["permissionDecision"] != "deny" or "cap of 3" not in reason:
        raise SystemExit(f"resumed session did not keep the cap: {denied}")
    state = state_path()
    if not state.is_file():
        raise SystemExit(f"state missing at {state}")
    resolved = str(state.resolve())
    if resolved == "/app" or resolved.startswith("/app/"):
        raise SystemExit(f"state path is under the read-only root: {resolved}")
    if Path("/app/.factory-selector").exists():
        raise SystemExit("state directory was created under /app")
    saved = json.loads(state.read_text())
    if saved["allowed"] != [
        "acme-corp/acme-bot/12",
        "acme-corp/acme-bot/15",
        "acme-corp/acme-bot/18",
    ]:
        raise SystemExit(f"count not kept: {saved}")
    print(f"STATE={resolved}")
    raise SystemExit(0)

for number, labels in ((12, ["bug"]), (15, ["docs"]), (18, ["help wanted"])):
    allow(number, labels)
child = subprocess.run([sys.executable, __file__, "resume"], check=False)
raise SystemExit(child.returncode)
"""

_READONLY_ROOT_SETUP = r"""
import subprocess
import sys
from pathlib import Path

new = Path(sys.argv[1])
mount_workspace = sys.argv[2] == "1"


def mount(args):
    done = subprocess.run(args, capture_output=True, text=True, check=False)
    if done.returncode != 0:
        sys.stderr.write(done.stderr)
        raise SystemExit(done.returncode)


for src, name in (("/usr", "usr"), ("/lib", "lib"), ("/bin", "bin"), ("/etc", "etc")):
    if Path(src).exists():
        mount(["mount", "--bind", src, str(new / name)])
if Path("/lib64").exists():
    mount(["mount", "--bind", "/lib64", str(new / "lib64")])
mount(["mount", "-t", "tmpfs", "-o", "ro,mode=555", "tmpfs", str(new / "app")])
mount(["mount", "-t", "tmpfs", "-o", "rw,mode=1777", "tmpfs", str(new / "tmp")])
if mount_workspace:
    mount(["mount", "-t", "tmpfs", "-o", "rw,mode=755", "tmpfs", str(new / "workspace")])
raise SystemExit(
    subprocess.run(
        ["chroot", str(new), "/usr/bin/python3", "/opt/drive.py"],
        check=False,
    ).returncode
)
"""


def _dispatch_under_readonly_image_root(tmp_path: Path, *, workspace: bool) -> str:
    """Run the hook with payload cwd /app inside a read-only image root."""
    new = tmp_path / "newroot"
    for name in ("app", "tmp", "usr", "lib", "lib64", "bin", "etc", "opt"):
        (new / name).mkdir(parents=True)
    if workspace:
        (new / "workspace").mkdir()
    (new / "opt" / "selector_gate.py").write_bytes(HOOK.read_bytes())
    (new / "opt" / "policy.json").write_bytes(
        (REPO / "examples" / "factory-selector" / "policy.json").read_bytes()
    )
    (new / "opt" / "drive.py").write_text(_READONLY_ROOT_DRIVER)
    setup = tmp_path / "setup.py"
    setup.write_text(_READONLY_ROOT_SETUP)
    done = subprocess.run(
        [
            "unshare",
            "--user",
            "--map-root-user",
            "--mount",
            sys.executable,
            str(setup),
            str(new),
            "1" if workspace else "0",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr or done.stdout
    return done.stdout


def test_a_read_only_image_root_keeps_the_cap_off_that_root(tmp_path: Path) -> None:
    """Three label writes are allowed and the fourth is denied on resume.

    The negative control is the state path. Putting it back under /app fails
    this test: that directory is the read-only image root, so the writes
    fail closed, and a file created there fails the path check.
    """
    with_workspace = _dispatch_under_readonly_image_root(tmp_path / "workspace", workspace=True)
    assert f"STATE={_SESSION_WORKSPACE}/.factory-selector/sess-keep.json" in with_workspace
    assert _SDK_CWD not in with_workspace.split("STATE=", 1)[1].splitlines()[0]

    without_workspace = _dispatch_under_readonly_image_root(tmp_path / "temp", workspace=False)
    assert "STATE=/tmp/.factory-selector/sess-keep.json" in without_workspace
    assert _SDK_CWD not in without_workspace.split("STATE=", 1)[1].splitlines()[0]
