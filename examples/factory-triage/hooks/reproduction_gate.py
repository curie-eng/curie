#!/usr/bin/env python3
"""Record shell observations and require them before submitting a ticket."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

DEADLINE_SECONDS = 8
MAX_INPUT = 2 * 1024 * 1024
MAX_OUTPUT = 32 * 1024
MAX_RUNS = 128
RESULT_TOOL = "mcp__curie__submit_tracker_result"
STATE_DIRECTORY = ".curie/factory-triage"
READ_TOOLS = {"Read", "Glob", "Grep", "Skill", "TodoWrite"}
EDIT_TOOLS = {"Write", "Edit", "MultiEdit", "NotebookEdit"}


class Refusal(Exception):
    pass


def expired(_signal: int, _frame: Any) -> None:
    raise Refusal("The reproduction gate reached its internal deadline; try a bounded probe.")


def decode(raw: bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise Refusal("Duplicate input keys are refused.")
            value[key] = item
        return value

    value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise Refusal("The hook input must be an object.")
    json.dumps(value, ensure_ascii=False).encode("utf-8", "strict")
    return value


def required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Refusal(f"Missing or invalid {label}.")
    return value


def git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(cwd), *args],
        capture_output=True,
        timeout=3,
        check=True,
        env={key: value for key, value in os.environ.items() if not key.startswith("GIT_")},
    )
    return result.stdout.decode("utf-8", "strict").strip()


def snapshot(cwd: Path) -> dict[str, Any]:
    revision = git(cwd, "rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise Refusal("The checkout has no 40-character commit.")
    return {
        "revision": revision,
        "clean": not git(
            cwd,
            "status",
            "--porcelain=v1",
            "--untracked-files=no",
            "--ignore-submodules=none",
        ),
    }


def protected(text: str, cwd: Path) -> bool:
    normalized = text.replace("\\", "/").lower()
    root = str(Path(__file__).resolve().parents[1]).lower()
    return any(
        part in normalized
        for part in (
            ".curie",
            "factory-triage",
            "reproduction_gate",
            "hooks/",
            "hooks.json",
            "claude_plugin_root",
            root,
            str(cwd / STATE_DIRECTORY).lower(),
        )
    )


def mutation_guard(tool: str, inputs: dict[str, Any], cwd: Path) -> None:
    if (
        tool == RESULT_TOOL
        or tool in READ_TOOLS
        or tool.endswith(("__get_issue", "__report_progress"))
    ):
        return
    if tool in EDIT_TOOLS:
        raw = inputs.get("file_path", inputs.get("notebook_path"))
        path = Path(required_text(raw, "edit path"))
        target = (cwd / path).resolve()
        if protected(str(target), cwd):
            raise Refusal("The hook, its configuration and its evidence state cannot be edited.")
        if not target.is_relative_to(cwd) or ".git" in target.relative_to(cwd).parts:
            raise Refusal("Investigation edits must stay in the checkout, outside git metadata.")
        return
    if tool == "Bash":
        command = required_text(inputs.get("command"), "shell command")
        if protected(command, cwd):
            raise Refusal("Shell commands cannot access the hook or its evidence state.")
        if inputs.get("run_in_background"):
            raise Refusal("Reproduction commands must finish in the foreground.")
        if re.search(r"\b(?:git\s+(?:push|remote)|gh|curl|wget|ssh|eval|exec)\b", command):
            raise Refusal(
                "Use local reproduction tools; direct publication and remote shell access "
                "are refused."
            )
        return
    raise Refusal(
        "This investigation tool is not permitted; use the local shell or submit findings."
    )


def load(path: Path, session: str, cwd: Path, *, create: bool) -> dict[str, Any]:
    if not path.exists() and create:
        return {"session": session, "workspace": str(cwd), "runs": [], "pending": {}}
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_INPUT:
        raise Refusal(
            "Reproduction state is missing or unreadable; run a fresh probe or submit findings."
        )
    state = decode(path.read_bytes())
    if (
        state.get("session") != session
        or state.get("workspace") != str(cwd)
        or not isinstance(state.get("runs"), list)
        or not isinstance(state.get("pending"), dict)
    ):
        raise Refusal("Reproduction state does not belong to this session and checkout.")
    return state


def save(path: Path, state: dict[str, Any]) -> None:
    pending = path.with_suffix(".pending")
    with pending.open("x", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False)
    os.replace(pending, path)


def output(response: Any, *, failed: bool) -> tuple[int, str]:
    # Claude Code documents stdout/stderr/interrupted, rather than an exit code,
    # in successful Bash hook responses: https://code.claude.com/docs/en/hooks#posttooluse-input
    # The CLI includes "Exit code N" in failed Bash output. Unfinished and
    # interrupted calls cannot supply a completed reproduction observation.
    if failed:
        if not isinstance(response, str):
            raise Refusal("The failed shell call has no observable exit status.")
        match = re.match(r"Exit code (\d+)\n", response)
        if not match:
            raise Refusal("The failed shell call has no observable exit status.")
        return int(match.group(1)), response[match.end() :][:MAX_OUTPUT]
    if not isinstance(response, dict) or response.get("interrupted") is not False:
        raise Refusal("The shell did not return a completed observation.")
    stdout = response.get("stdout")
    stderr = response.get("stderr")
    if not isinstance(stdout, str) or not isinstance(stderr, str):
        raise Refusal("The shell returned an undecodable observation.")
    text = stdout + "\n" + stderr
    if re.search(r"running in (?:the )?background|Background task|Process running", text, re.I):
        raise Refusal("The shell command is unfinished; it cannot be reproduction evidence.")
    match = re.match(r"Exit code (\d+)\n", stdout)
    status = int(match.group(1)) if match else 0
    return status, text[:MAX_OUTPUT]


def record(state: dict[str, Any], event: str, data: dict[str, Any], cwd: Path) -> None:
    call = required_text(data.get("tool_use_id"), "tool call identity")
    command = required_text(data["tool_input"].get("command"), "shell command")
    if event == "PreToolUse":
        if len(state["pending"]) >= MAX_RUNS:
            raise Refusal("Too many unfinished shell calls; submit findings.")
        state["pending"][call] = {"command": command, **snapshot(cwd)}
        return
    before = state["pending"].pop(call, None)
    if not isinstance(before, dict) or before.get("command") != command:
        raise Refusal("No matching admitted shell call exists for this observation.")
    after = snapshot(cwd)
    failed = event == "PostToolUseFailure"
    status, text = output(data.get("error") if failed else data.get("tool_response"), failed=failed)
    state["runs"].append(
        {
            "command": command,
            "revision": before["revision"],
            "clean": before["clean"] and after["clean"] and before["revision"] == after["revision"],
            "exit_status": status,
            "output": text,
        }
    )
    state["runs"] = state["runs"][-MAX_RUNS:]


def check_ticket(state: dict[str, Any], proposal: dict[str, Any], cwd: Path) -> None:
    reproduction = proposal.get("reproduction")
    if not isinstance(reproduction, dict):
        raise Refusal("A ticket needs a reproduction command, observed output and revision.")
    command = required_text(reproduction.get("command"), "reproduction.command")
    observed = required_text(reproduction.get("observed"), "reproduction.observed")
    revision = required_text(reproduction.get("revision"), "reproduction.revision")
    current = snapshot(cwd)
    if revision != current["revision"]:
        raise Refusal("The submitted revision differs from the checkout revision.")
    if not current["clean"]:
        raise Refusal("Tracked files are modified at submission; revert candidate patches first.")
    runs = [run for run in state["runs"] if run.get("command") == command]
    if not runs:
        raise Refusal("The reproduction command was not recorded in this session.")
    runs = [run for run in runs if run.get("revision") == revision and run.get("clean") is True]
    if not runs:
        raise Refusal(
            "The command was not recorded at this revision with tracked files unmodified."
        )
    if not any(
        any(line.strip() and line in observed for line in run["output"].splitlines())
        for run in runs
    ):
        raise Refusal("The observed result must quote a nonempty recorded output line verbatim.")


def handle(data: dict[str, Any]) -> dict[str, Any]:
    event = required_text(data.get("hook_event_name"), "hook event")
    tool = required_text(data.get("tool_name"), "tool name")
    inputs = data.get("tool_input")
    if not isinstance(inputs, dict):
        raise Refusal("Tool input must be an object.")
    if event not in {"PreToolUse", "PostToolUse", "PostToolUseFailure"}:
        raise Refusal("Unknown hook event.")
    if tool == RESULT_TOOL and event == "PreToolUse" and inputs.get("kind") == "findings":
        return {}
    cwd = Path(required_text(data.get("cwd"), "checkout path")).resolve(strict=True)
    if event == "PreToolUse":
        mutation_guard(tool, inputs, cwd)
    if tool != "Bash" and tool != RESULT_TOOL:
        return {}
    session = required_text(
        os.environ.get("CURIE_SESSION_ID") or data.get("session_id"), "session identity"
    )
    directory = cwd / STATE_DIRECTORY
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or directory.resolve() != directory:
        raise Refusal("Reproduction state cannot be redirected by a symlink.")
    key = hashlib.sha256(session.encode("utf-8")).hexdigest()
    path = directory / f"{key}.json"
    lock_path = directory / f"{key}.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = load(path, session, cwd, create=tool == "Bash" and event == "PreToolUse")
        if tool == "Bash":
            record(state, event, data, cwd)
            save(path, state)
        elif event == "PreToolUse":
            if inputs.get("kind") != "ticket":
                raise Refusal("Submit exactly one typed ticket or findings proposal.")
            check_ticket(state, inputs, cwd)
    return {}


def main() -> None:
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, DEADLINE_SECONDS)
    try:
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise Refusal("The hook input exceeds its size limit.")
        result = handle(decode(raw))
    except BaseException as exc:  # noqa: BLE001
        reason = (
            str(exc)
            if isinstance(exc, Refusal)
            else "The reproduction gate could not validate its input or state."
        )
        if isinstance(exc, subprocess.TimeoutExpired):
            reason = "The reproduction gate reached its internal git deadline."
        result = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        }
    signal.setitimer(signal.ITIMER_REAL, 0)
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()
