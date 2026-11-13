"""Subprocess proof of the factory selector hook.

Payloads use the SDK CLI shape recorded in test_dark_factory_review_gate.py
and the GitHub issue JSON that server-github 2025.4.8 returns, whose labels
are objects with a name. Each refusal is paired with an allowed call.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
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
