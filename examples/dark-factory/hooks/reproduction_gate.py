#!/usr/bin/env python3
"""Record shell observations and ticket questions; enforce the ask and cite rules.

Rules, per session:

1. A Write, Edit, MultiEdit or NotebookEdit whose path, or a Bash command whose
   text, names this hook, ``hooks.json`` or the hook's state is refused.
2. Each completed Bash call is recorded as an observation.
3. A ``get_issue`` result containing the factory question heading is recorded.
4. A successful ``ask_questions`` call is recorded.
5. ``mcp__curie__ask_questions`` is refused until one Bash observation exists.
6. ``mcp__curie__publish_changes`` is refused after a recorded successful ask.
7. ``mcp__curie__publish_changes`` is refused when a question heading was
   recorded and the body's Decisions section has no line starting
   ``Answered on the ticket by `` (after an optional ``n. `` entry number).

Every other tool passes. The state lives in the checkout under
``.curie/factory-investigation``, which the hook lists in the repository's
``info/exclude`` so it never joins the publishable candidate.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

DEADLINE_SECONDS = 8
MAX_INPUT = 2 * 1024 * 1024
MAX_OUTPUT = 32 * 1024
MAX_RUNS = 128
MAX_STATE = 8 * 1024 * 1024
LOCK_SECONDS = 2
ASK_TOOL = "mcp__curie__ask_questions"
PUBLISH_TOOL = "mcp__curie__publish_changes"
QUESTION_HEADING = "### Questions from the factory"
CITATION = re.compile(r"^(?:[1-9][0-9]*\. )?Answered on the ticket by ")
STATE_NAME = "factory-investigation"
STATE_DIRECTORY = ".curie/" + STATE_NAME
EDIT_TOOLS = {"Write", "Edit", "MultiEdit", "NotebookEdit"}


class Refusal(Exception):
    pass


def expired(_signal: int, _frame: Any) -> None:
    raise Refusal("The investigation gate reached its internal deadline; try a bounded probe.")


def decode(raw: bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise Refusal("Duplicate input keys are refused.")
            value[key] = item
        return value

    def constant(_name: str) -> None:
        raise Refusal("Nonfinite JSON values are refused.")

    value = json.loads(
        raw.decode("utf-8", "strict"), object_pairs_hook=pairs, parse_constant=constant
    )
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
        ["git", "--no-optional-locks", "-c", f"safe.directory={cwd}", "-C", str(cwd), *args],
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
            STATE_NAME,
            "reproduction_gate",
            "hooks.json",
            "claude_plugin_root",
            root,
            str(cwd / STATE_DIRECTORY).lower(),
        )
    )


def mutation_guard(tool: str, inputs: dict[str, Any], cwd: Path) -> None:
    """Rule 1: no edit or shell command may name the hook, its registration or its state."""
    if tool in EDIT_TOOLS:
        raw = inputs.get("file_path", inputs.get("notebook_path"))
        path = Path(required_text(raw, "edit path"))
        if protected(str((cwd / path).resolve()), cwd) or protected(str(path), cwd):
            raise Refusal("The hook, its configuration and its evidence state cannot be edited.")
    elif tool == "Bash":
        command = required_text(inputs.get("command"), "shell command")
        if protected(command, cwd):
            raise Refusal("This command names the hook or its evidence state; use a local probe.")


def digest(value: dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_entry(entry: Any) -> None:
    fields = {"command", "exit_status", "revision", "clean", "output", "output_digest"}
    if not isinstance(entry, dict) or set(entry) != fields | {"entry_digest"}:
        raise Refusal("A reproduction entry is missing its integrity fields.")
    if (
        not isinstance(entry["command"], str)
        or not entry["command"].strip()
        or type(entry["exit_status"]) is not int
        or not 0 <= entry["exit_status"] <= 255
        or not isinstance(entry["revision"], str)
        or not re.fullmatch(r"[0-9a-f]{40}", entry["revision"])
        or type(entry["clean"]) is not bool
        or not isinstance(entry["output"], str)
        or len(entry["output"]) > MAX_OUTPUT
    ):
        raise Refusal("A reproduction entry has corrupt integrity fields.")
    if entry["output_digest"] != hashlib.sha256(
        entry["output"].encode("utf-8")
    ).hexdigest() or entry["entry_digest"] != digest({key: entry[key] for key in fields}):
        raise Refusal("A reproduction entry no longer matches its digest.")


def load_base_revision(path: Path, cwd: Path, *, create: bool) -> str:
    if not path.exists() and create:
        save(path, {"base_revision": snapshot(cwd)["revision"]})
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_STATE:
        raise Refusal("The investigated base revision is missing or unreadable.")
    revision = decode(path.read_bytes()).get("base_revision")
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise Refusal("The investigated base revision is corrupt.")
    return revision


def load(
    path: Path, session: str, cwd: Path, *, create: bool, base_revision: str
) -> dict[str, Any]:
    if not path.exists() and create:
        return {
            "session": session,
            "workspace": str(cwd),
            "base_revision": base_revision,
            "runs": [],
            "pending": {},
            "question_heading": False,
            "asked": False,
        }
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_STATE:
        raise Refusal("Investigation state is missing or unreadable; run a fresh probe.")
    state = decode(path.read_bytes())
    if (
        state.get("session") != session
        or state.get("workspace") != str(cwd)
        or not isinstance(state.get("runs"), list)
        or not isinstance(state.get("pending"), dict)
        or type(state.get("question_heading")) is not bool
        or type(state.get("asked")) is not bool
    ):
        raise Refusal("Reproduction state does not belong to this session and checkout.")
    if not isinstance(state.get("base_revision"), str) or not re.fullmatch(
        r"[0-9a-f]{40}", state["base_revision"]
    ):
        raise Refusal("Reproduction state has a missing or corrupt base revision.")
    if state["base_revision"] != base_revision:
        raise Refusal("Reproduction state differs from the investigated base revision.")
    if len(state["runs"]) > MAX_RUNS or len(state["pending"]) > MAX_RUNS:
        raise Refusal("Reproduction state exceeds its entry limit.")
    for entry in state["runs"]:
        validate_entry(entry)
    return state


def save(path: Path, state: dict[str, Any]) -> None:
    fd, name = tempfile.mkstemp(prefix=path.stem + ".", suffix=".pending", dir=path.parent)
    pending = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)


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
    return 0, text[:MAX_OUTPUT]


def record(state: dict[str, Any], event: str, data: dict[str, Any], cwd: Path) -> None:
    call = required_text(data.get("tool_use_id"), "tool call identity")
    command = required_text(data["tool_input"].get("command"), "shell command")
    if event == "PreToolUse":
        # A call another hook refused never completes; drop the oldest instead of
        # refusing every later shell call.
        while len(state["pending"]) >= MAX_RUNS:
            state["pending"].pop(next(iter(state["pending"])))
        state["pending"][call] = {"command": command, **snapshot(cwd)}
        return
    before = state["pending"].pop(call, None)
    if not isinstance(before, dict) or before.get("command") != command:
        raise Refusal("No matching admitted shell call exists for this observation.")
    after = snapshot(cwd)
    failed = event == "PostToolUseFailure"
    status, text = output(data.get("error") if failed else data.get("tool_response"), failed=failed)
    entry = {
        "command": command,
        "revision": before["revision"],
        "clean": before["clean"] and after["clean"] and before["revision"] == after["revision"],
        "exit_status": status,
        "output": text,
        "output_digest": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }
    entry["entry_digest"] = digest(entry)
    state["runs"].append(entry)
    state["runs"] = state["runs"][-MAX_RUNS:]


def cited(body: Any) -> bool:
    """Whether the body's Decisions section has a line citing an answer on the ticket."""
    if not isinstance(body, str):
        return False
    lines = body.splitlines()
    for index, line in enumerate(lines):
        if line != "## Decisions":
            continue
        for entry in lines[index + 1 :]:
            if entry.startswith("## "):
                break
            if CITATION.match(entry):
                return True
    return False


def exclude_state(cwd: Path) -> None:
    """Keep the state out of the publishable candidate through ``info/exclude``."""
    info = Path(git(cwd, "rev-parse", "--git-path", "info/exclude"))
    exclude = info if info.is_absolute() else cwd / info
    pattern = f"/{STATE_DIRECTORY}/"
    text = exclude.read_text("utf-8") if exclude.is_file() else ""
    if pattern not in text.splitlines():
        exclude.parent.mkdir(parents=True, exist_ok=True)
        prefix = "" if not text or text.endswith("\n") else "\n"
        with exclude.open("a", encoding="utf-8") as handle:
            handle.write(f"{prefix}{pattern}\n")


def acquire(lock: Any) -> None:
    """Wait briefly for a concurrent call of this hook, then refuse."""
    waited = time.monotonic() + LOCK_SECONDS
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= waited:
                raise Refusal("Another call holds the investigation state; retry it.") from None
            time.sleep(0.02)


def handle(data: dict[str, Any]) -> dict[str, Any]:
    event = required_text(data.get("hook_event_name"), "hook event")
    tool = required_text(data.get("tool_name"), "tool name")
    inputs = data.get("tool_input")
    if not isinstance(inputs, dict):
        raise Refusal("Tool input must be an object.")
    if event not in {"PreToolUse", "PostToolUse", "PostToolUseFailure"}:
        raise Refusal("Unknown hook event.")
    cwd = Path(required_text(data.get("cwd"), "checkout path")).resolve(strict=True)
    if event == "PreToolUse":
        mutation_guard(tool, inputs, cwd)
    heading = (
        event == "PostToolUse"
        and tool.endswith("get_issue")
        and QUESTION_HEADING in json.dumps(data.get("tool_response"), ensure_ascii=False)
    )
    response = data.get("tool_response")
    asked = (
        event == "PostToolUse"
        and tool.endswith("ask_questions")
        and not (isinstance(response, dict) and response.get("isError"))
    )
    gated = event == "PreToolUse" and tool in {ASK_TOOL, PUBLISH_TOOL}
    if tool != "Bash" and not heading and not asked and not gated:
        return {}
    session = required_text(
        os.environ.get("CURIE_SESSION_ID") or data.get("session_id"), "session identity"
    )
    directory = cwd / STATE_DIRECTORY
    exclude_state(cwd)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or directory.resolve() != directory:
        raise Refusal("Investigation state cannot be redirected by a symlink.")
    key = hashlib.sha256(session.encode("utf-8")).hexdigest()
    path = directory / f"{key}.json"
    lock_path = directory / f"{key}.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as lock:
        acquire(lock)
        anchor_directory = Path(git(cwd, "rev-parse", "--absolute-git-dir")) / STATE_NAME
        anchor_directory.mkdir(mode=0o700, exist_ok=True)
        base_revision = load_base_revision(
            anchor_directory / f"{key}.json", cwd, create=not path.exists()
        )
        state = load(path, session, cwd, create=True, base_revision=base_revision)
        if tool == "Bash":
            record(state, event, data, cwd)
        elif heading:
            state["question_heading"] = True
        elif asked:
            state["asked"] = True
        elif tool == ASK_TOOL:
            if not state["runs"]:
                raise Refusal(
                    "Investigate before asking: run at least one bounded shell probe "
                    "that observes the current behaviour."
                )
            return {}
        elif state["asked"]:
            raise Refusal(
                "This session asked questions on the ticket; end the session without "
                "publishing. The next attempt reads the answers."
            )
        elif state["question_heading"] and not cited(inputs.get("body")):
            raise Refusal(
                "The ticket carries factory questions: add one Decisions entry per answer "
                "relied on, starting `Answered on the ticket by <author>: <the decision>`."
            )
        else:
            return {}
        save(path, state)
    return {}


def main() -> None:
    try:
        try:
            signal.signal(signal.SIGALRM, expired)
            signal.setitimer(signal.ITIMER_REAL, DEADLINE_SECONDS)
            raw = sys.stdin.buffer.read(MAX_INPUT + 1)
            if len(raw) > MAX_INPUT:
                raise Refusal("The hook input exceeds its size limit.")
            result = handle(decode(raw))
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
    except BaseException as exc:  # noqa: BLE001
        reason = (
            str(exc)
            if isinstance(exc, Refusal)
            else "The investigation gate could not validate its input or state."
        )
        if isinstance(exc, subprocess.TimeoutExpired):
            reason = "The investigation gate reached its internal git deadline."
        result = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        }
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()
