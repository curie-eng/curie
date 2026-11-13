#!/usr/bin/env python3
"""Allow one factory label add per issue, and refuse every other update_issue.

PostToolUse on get_issue records that issue's labels. PreToolUse on
update_issue allows the call only when it adds the policy label to those
labels and this session is still under the cap. The read and the allow
share one file lock for the whole state transaction, so concurrent calls
cannot pass the cap. The runner allows a tool when this hook is still
running at 60 seconds, so the hook denies itself first. State is one JSON
file per session. A missing policy, an out-of-range cap, or a bad state
file refuses every label write. PreToolUse also refuses a shell command
and any write that lands in the state directory, so the model cannot clear
the cap. Missing, malformed, or undecodable hook input, and any other
failure, deny.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

_CHANGED = ("title", "body", "state", "assignees", "milestone")
# The runner's hook limit is 60s and a timed-out hook is allowed through.
# This deadline is also under the 10s command timeout declared for the hook.
_HOOK_DEADLINE_SECONDS = 8.0
_SHELL_TOOLS = frozenset({"bash", "shell", "powershell", "exec"})
_WRITE_TOOLS = frozenset({"write", "edit", "multiedit", "notebookedit", "applypatch"})
_READ_TOOLS = frozenset({"read", "glob", "grep"})
_PATH_KEYS = frozenset({"file_path", "path", "notebook_path", "target_file", "target_notebook"})
_COMMAND_KEYS = ("command", "cmd")


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


class _Deadline(Exception):
    """The hook's own clock ran out while a tool call could still be allowed."""


def _deadline_seconds() -> float:
    """A positive wait no longer than the built-in deadline.

    An environment override can only shorten the wait. A longer value, zero,
    or garbage keeps the built-in deadline, so a caller cannot outlast the
    runner timeout that would allow the tool.
    """
    raw = (os.environ.get("FACTORY_SELECTOR_DEADLINE_SECONDS") or "").strip()
    if not raw:
        return _HOOK_DEADLINE_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _HOOK_DEADLINE_SECONDS
    if not value > 0:
        return _HOOK_DEADLINE_SECONDS
    return min(value, _HOOK_DEADLINE_SECONDS)


def _on_deadline(_signum: int, _frame: object) -> None:
    raise _Deadline()


def _arm_deadline() -> None:
    signal.signal(signal.SIGALRM, _on_deadline)
    signal.setitimer(signal.ITIMER_REAL, _deadline_seconds())


def _disarm_deadline() -> None:
    signal.setitimer(signal.ITIMER_REAL, 0.0)


def _decision(kind: str, reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": kind,
            "permissionDecisionReason": reason,
        }
    }


def _with_state_lock(path: Path, body: Callable[[], Any]) -> Any:
    """Hold the session lock for one read-modify-write of the state file.

    The directory is created here, before the lock, so two callers cannot
    both observe a missing file and then write. A symlink in the lock's
    place fails the open instead of being followed. A waiter does not block
    until the runner gives up: the deadline armed around the hook raises
    first, and that failure denies the tool.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        str(path) + ".lock",
        os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
    )
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return body()
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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


def _read_text(path: Path) -> str:
    descriptor = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        chunks: list[bytes] = []
        while True:
            block = os.read(descriptor, 65536)
            if not block:
                break
            chunks.append(block)
    finally:
        os.close(descriptor)
    return b"".join(chunks).decode("utf-8")


def load_state(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if path.is_symlink():
        return None, "state file is unreadable"
    if not path.exists():
        return {"reads": {}, "allowed": []}, None
    try:
        data = json.loads(_read_text(path))
    except OSError:
        return None, "state file is unreadable"
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, "state file is malformed"
    if not _valid_state(data):
        return None, "state file is malformed"
    return data, None


def save_state(path: Path, state: dict[str, Any]) -> str | None:
    temporary = path.with_suffix(".tmp")
    payload = json.dumps(state).encode("utf-8")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            str(temporary),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
        )
        try:
            os.write(descriptor, payload)
        finally:
            os.close(descriptor)
        os.replace(str(temporary), str(path))
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
    def body() -> None:
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

    _with_state_lock(path, body)


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
    def body() -> dict[str, Any]:
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

    return _with_state_lock(path, body)


def _leaf(tool: str) -> str:
    name = tool.strip()
    if "__" in name:
        name = name.rsplit("__", 1)[-1]
    return name.casefold()


def _path_strings(tool_input: object) -> list[str]:
    if not isinstance(tool_input, dict):
        return []
    found: list[str] = []
    for key, value in tool_input.items():
        if key in _PATH_KEYS and isinstance(value, str) and value.strip():
            found.append(value)
        elif isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    continue
                for child_key, child in item.items():
                    if child_key in _PATH_KEYS and isinstance(child, str) and child.strip():
                        found.append(child)
    return found


def _lands_in_state(candidate: Path, directory: Path) -> bool:
    try:
        candidate.resolve().relative_to(directory.resolve())
    except ValueError:
        return False
    except (OSError, RuntimeError):
        return True
    return True


def _state_write_refusal(data: dict[str, Any], path: Path) -> str | None:
    tool_input = data.get("tool_input")
    leaf = _leaf(str(data.get("tool_name") or ""))
    if leaf in _READ_TOOLS:
        return None
    if leaf in _SHELL_TOOLS or (
        isinstance(tool_input, dict)
        and any(isinstance(tool_input.get(key), str) for key in _COMMAND_KEYS)
    ):
        return "a shell command can rewrite the session cap state"
    raw_paths = _path_strings(tool_input)
    if leaf in _WRITE_TOOLS and not raw_paths:
        return "a write can rewrite the session cap state"
    cwd = Path(str(data.get("cwd") or ""))
    for raw in raw_paths:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = cwd / candidate
        if _lands_in_state(candidate, path.parent):
            return "a write targets the session cap state"
    return None


def _dispatch(data: dict[str, Any]) -> dict[str, Any] | None:
    path = state_file(data)
    event = str(data.get("hook_event_name") or "")
    tool = str(data.get("tool_name") or "")
    if event == "PostToolUse" and tool.endswith("get_issue"):
        _record_read(data, path)
        return None
    if event != "PreToolUse":
        return None
    if tool.endswith("update_issue"):
        return _decide_update(data, path)
    refusal = _state_write_refusal(data, path)
    if refusal is None:
        return None
    return _decision("deny", refusal)


def _emit(decision: dict[str, Any]) -> None:
    json.dump(decision, sys.stdout)
    sys.stdout.write("\n")


def _body() -> dict[str, Any] | None:
    raw = sys.stdin.buffer.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return _decision("deny", "hook input is not valid UTF-8")
    if not text.strip():
        return _decision("deny", "hook input is missing")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return _decision("deny", "hook input is malformed")
    if not isinstance(data, dict):
        return _decision("deny", "hook input is malformed")
    return _dispatch(data)


def _safe_deny(reason: str) -> None:
    try:
        _disarm_deadline()
    except Exception:  # noqa: BLE001 - disarm must not hide the deny
        pass
    try:
        _emit(_decision("deny", reason))
    except Exception:  # noqa: BLE001 - the last resort is still a deny
        sys.stdout.write(
            '{"hookSpecificOutput":{"hookEventName":"PreToolUse",'
            '"permissionDecision":"deny",'
            '"permissionDecisionReason":"hook failed closed"}}\n'
        )


def main() -> None:
    try:
        _arm_deadline()
        try:
            decision = _body()
        finally:
            _disarm_deadline()
        if decision is not None:
            _emit(decision)
    except _Deadline:
        _safe_deny("hook deadline exceeded")
    except Exception:  # noqa: BLE001 - any hook failure must deny
        _safe_deny("hook failed closed")


if __name__ == "__main__":
    main()
