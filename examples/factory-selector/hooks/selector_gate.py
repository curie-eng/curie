#!/usr/bin/env python3
"""Allow one factory label add per issue, and refuse every other update_issue.

PostToolUse on get_issue records that issue's labels. PreToolUse on
update_issue allows the call only when it adds the policy label to those
labels and this session is still under the cap. State is one JSON file per
session. A missing policy, an out-of-range cap, or a bad state file refuses
every label write.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

_CHANGED = ("title", "body", "state", "assignees", "milestone")


def _env_path(name: str) -> Path | None:
    value = (os.environ.get(name) or "").strip()
    return Path(value) if value else None


def session_key(data: dict[str, Any]) -> str:
    """Curie session when set, otherwise the hook payload session id."""
    curie = (os.environ.get("CURIE_SESSION_ID") or "").strip()
    return curie or str(data.get("session_id") or "")


def policy_path() -> Path:
    override = _env_path("FACTORY_SELECTOR_POLICY")
    if override is not None:
        return override
    root = _env_path("CLAUDE_PLUGIN_ROOT") or Path(__file__).resolve().parent.parent
    return root / "policy.json"


def state_file(data: dict[str, Any]) -> Path:
    directory = _env_path("FACTORY_SELECTOR_STATE_DIR")
    if directory is None:
        directory = Path(str(data.get("cwd") or "")) / ".factory-selector"
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", session_key(data) or "default")
    return directory / f"{name}.json"


def _decision(kind: str, reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": kind,
            "permissionDecisionReason": reason,
        }
    }


def load_policy(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.is_file():
        return None, "policy file is missing"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return None, "policy file is unreadable"
    except json.JSONDecodeError:
        return None, "policy file is malformed"
    if not isinstance(data, dict):
        return None, "policy file is malformed"
    repository, query, label, cap = (
        data.get("repository"),
        data.get("query"),
        data.get("label"),
        data.get("cap"),
    )
    owner, _, repo = repository.partition("/") if isinstance(repository, str) else ("", "", "")
    if not owner or not repo or "/" in repo:
        return None, "policy repository is invalid"
    if not isinstance(query, str) or not query.strip():
        return None, "policy query is invalid"
    if not isinstance(label, str) or not label.strip():
        return None, "policy label is invalid"
    if type(cap) is not int or not 1 <= cap <= 3:
        return None, "cap is out of range"
    return {"repository": repository, "query": query, "label": label, "cap": cap}, None


def _valid_state(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    reads, allowed = data.get("reads"), data.get("allowed")
    if not isinstance(reads, dict) or not isinstance(allowed, list):
        return False
    return all(isinstance(item, str) for item in allowed) and all(
        isinstance(key, str)
        and isinstance(labels, list)
        and all(isinstance(label, str) for label in labels)
        for key, labels in reads.items()
    )


def load_state(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return {"reads": {}, "allowed": []}, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return None, "state file is unreadable"
    except json.JSONDecodeError:
        return None, "state file is malformed"
    if not _valid_state(data):
        return None, "state file is malformed"
    return data, None


def save_state(path: Path, state: dict[str, Any]) -> str | None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state), encoding="utf-8")
        temporary.replace(path)
    except OSError:
        return "state file is unreadable"
    return None


def _identity(tool_input: object) -> tuple[str, str] | None:
    if not isinstance(tool_input, dict):
        return None
    owner, repo, number = (
        tool_input.get("owner"),
        tool_input.get("repo"),
        tool_input.get("issue_number"),
    )
    if not isinstance(owner, str) or not owner or not isinstance(repo, str) or not repo:
        return None
    if isinstance(number, str) and number.isdigit():
        number = int(number)
    if isinstance(number, bool) or not isinstance(number, int):
        return None
    repository = f"{owner}/{repo}"
    return repository, f"{repository}/{number}"


def _issue_object(response: object) -> dict[str, Any] | None:
    blocks = response if isinstance(response, list) else None
    if isinstance(response, dict) and isinstance(response.get("content"), list):
        blocks = response["content"]
    elif isinstance(response, dict) and "labels" in response:
        return response
    if blocks is None:
        return None
    for block in blocks:
        if not isinstance(block, dict) or not isinstance(block.get("text"), str):
            continue
        try:
            parsed = json.loads(block["text"])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and "labels" in parsed:
            return parsed
    return None


def _label_names(issue: dict[str, Any]) -> list[str] | None:
    labels = issue.get("labels")
    if not isinstance(labels, list):
        return None
    names: list[str] = []
    for label in labels:
        if isinstance(label, str):
            names.append(label)
        elif isinstance(label, dict) and isinstance(label.get("name"), str):
            names.append(label["name"])
        else:
            return None
    return names


def _record_read(data: dict[str, Any], path: Path) -> None:
    identity = _identity(data.get("tool_input"))
    issue = _issue_object(data.get("tool_response"))
    if identity is None or issue is None:
        return
    names = _label_names(issue)
    if names is None:
        return
    state, error = load_state(path)
    if error is not None or state is None:
        return
    state["reads"][identity[1]] = names
    save_state(path, state)


def _label_refusal(recorded: list[str], requested: object, policy_label: str) -> str | None:
    for name in recorded:
        if name == policy_label:
            return "issue already carries the policy label"
        if name.startswith("curie-factory:"):
            return "issue carries a curie-factory: label"
        if name.startswith("curie:"):
            return "issue carries a curie: label"
    if not isinstance(requested, list) or not all(isinstance(item, str) for item in requested):
        return "label set is not the recorded labels plus the policy label"
    expected = set(recorded) | {policy_label}
    got = set(requested)
    if not set(recorded) <= got:
        return "label set drops an existing label"
    if got - expected:
        return "label set adds a label other than the policy label"
    if got != expected:
        return "label set is not the recorded labels plus the policy label"
    return None


def _decide_update(data: dict[str, Any], path: Path) -> dict[str, Any]:
    policy, error = load_policy(policy_path())
    if error is not None or policy is None:
        return _decision("deny", error or "policy file is missing")
    state, error = load_state(path)
    if error is not None or state is None:
        return _decision("deny", error or "state file is malformed")
    tool_input = data.get("tool_input")
    identity = _identity(tool_input)
    if not isinstance(tool_input, dict) or identity is None:
        return _decision("deny", "call does not name the issue")
    repository, key = identity
    if repository != policy["repository"]:
        return _decision("deny", "repository is not the policy repository")
    recorded = state["reads"].get(key)
    if not isinstance(recorded, list):
        return _decision("deny", "issue was not read in this session")
    refusal = _label_refusal(recorded, tool_input.get("labels"), policy["label"])
    if refusal is not None:
        return _decision("deny", refusal)
    changed = next(
        (field for field in _CHANGED if field in tool_input and tool_input[field] is not None),
        None,
    )
    if changed is not None:
        return _decision("deny", f"call changes {changed}")
    allowed: list[str] = state["allowed"]
    if key not in allowed and len(allowed) >= policy["cap"]:
        return _decision(
            "deny",
            f"cap of {policy['cap']} distinct issues already allowed in this session",
        )
    if key not in allowed:
        allowed.append(key)
    error = save_state(path, state)
    if error is not None:
        return _decision("deny", error)
    return _decision(
        "allow",
        "label add keeps the recorded labels and is within the session cap",
    )


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        return
    if not isinstance(data, dict):
        return
    path = state_file(data)
    event = str(data.get("hook_event_name") or "")
    tool = str(data.get("tool_name") or "")
    if event == "PostToolUse" and tool.endswith("get_issue"):
        _record_read(data, path)
        return
    if event == "PreToolUse" and tool.endswith("update_issue"):
        json.dump(_decide_update(data, path), sys.stdout)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
