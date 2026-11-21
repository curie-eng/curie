"""Exercise the investigation gate with a real checkout, directly and through hooks.json."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_independent
ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/dark-factory"
HOOK = BUNDLE / "hooks/reproduction_gate.py"
ASK = "mcp__curie__ask_questions"
PUBLISH = "mcp__curie__publish_changes"
ISSUE_READ = "mcp__curie__get_issue"
STATE = ".curie/factory-investigation"
HEADING = "### Questions from the factory (round 1 of 3)"
QUESTIONS = {"summary": "add(2, 3) returned -1.", "questions": ["Which result is intended?"]}
CITED_BODY = (
    "Restore addition.\n\nCloses #7\n\n## Decisions\n"
    "1. Keep integer addition. Rejected: float addition, because the tests compare integers. "
    "(plan) Evidence: tests/test_calc.py.\n"
    "Answered on the ticket by a maintainer: add returns the integer sum.\n\n"
    "## Acceptance checklist\n1. Done.\n"
)
UNCITED_BODY = (
    "Restore addition.\n\nCloses #7\n\n## Decisions\n"
    "None: the change follows directly from the acceptance criteria.\n\n"
    "## Acceptance checklist\n1. Answered on the ticket by a maintainer: elsewhere.\n"
)


def denied(result: dict) -> bool:
    return result.get("hookSpecificOutput", {}).get("permissionDecision") == "deny"


def reason(result: dict) -> str:
    return str(result["hookSpecificOutput"]["permissionDecisionReason"])


def issue_response(ticket: str, *, has_questions: bool = False) -> list[dict[str, str]]:
    """The recorded PostToolUse shape of get_issue: one text block holding the JSON payload."""
    payload = {
        "issue": "#7",
        "url": "https://github.com/Acme/bot/issues/7",
        "ticket": ticket,
        "has_question_history": has_questions,
    }
    return [{"type": "text", "text": json.dumps(payload, indent=2)}]


class Session:
    """A real git checkout and the hook called directly as a subprocess."""

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
        self.serial = 0

    def git(self, *args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=self.cwd, text=True)

    def payload(self, event: str, tool: str, inputs: dict, **extra: object) -> dict:
        payload = dict(
            session_id="investigation-session",
            transcript_path=str(self.cwd / "transcript.jsonl"),
            cwd=str(self.cwd),
            permission_mode="bypassPermissions",
            hook_event_name=event,
            tool_name=tool,
            tool_input=inputs,
        )
        payload.update(extra)
        return payload

    def raw(self, data: bytes) -> dict:
        done = subprocess.run(
            [sys.executable, str(HOOK)],
            input=data,
            capture_output=True,
            cwd=BUNDLE,
            env=self.env,
            timeout=12,
        )
        assert done.returncode == 0, done.stderr
        return json.loads(done.stdout)

    def event(self, event: str, tool: str, inputs: dict, **extra: object) -> dict:
        return self.raw(json.dumps(self.payload(event, tool, inputs, **extra)).encode())

    def run(self, command: str) -> dict:
        self.serial += 1
        call = f"call-{self.serial}"
        pre = self.event("PreToolUse", "Bash", {"command": command}, tool_use_id=call)
        assert not denied(pre), pre
        done = subprocess.run(
            command, shell=True, cwd=self.cwd, env=self.env, capture_output=True, text=True
        )
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

    def ask(self) -> dict:
        return self.event("PreToolUse", ASK, QUESTIONS)

    def asked(self, response: object = None) -> dict:
        reply = response if response is not None else [{"type": "text", "text": '{"posted":1}'}]
        return self.event("PostToolUse", ASK, QUESTIONS, tool_response=reply)

    def read_issue(self, ticket: str, *, has_questions: bool = False) -> dict:
        return self.event(
            "PostToolUse",
            ISSUE_READ,
            {},
            tool_response=issue_response(ticket, has_questions=has_questions),
        )

    def publish(self, body: str = CITED_BODY) -> dict:
        return self.event("PreToolUse", PUBLISH, {"title": "Restore addition (#7)", "body": body})

    def state(self) -> dict:
        return json.loads(next((self.cwd / STATE).glob("*.json")).read_text())


@pytest.fixture
def session(tmp_path: Path) -> Session:
    return Session(tmp_path / "repo")


# --- rules 5 to 7: ask before evidence, publish after asking, cite answers ---------


def test_rules_ask_is_refused_without_a_bash_observation_and_allowed_after_one(
    session: Session,
) -> None:
    refusal = session.ask()
    assert denied(refusal)
    assert "probe" in reason(refusal)
    session.run("python3 -c 'from calc import add; print(add(2, 3))'")
    assert not denied(session.ask())


def test_rules_an_admitted_but_unfinished_bash_call_is_not_an_observation(
    session: Session,
) -> None:
    pre = session.event("PreToolUse", "Bash", {"command": "echo probe"}, tool_use_id="open")
    assert not denied(pre)
    assert denied(session.ask())
    session.event(
        "PostToolUse",
        "Bash",
        {"command": "echo probe"},
        tool_use_id="open",
        tool_response={"stdout": "probe\n", "stderr": "", "interrupted": False},
    )
    assert not denied(session.ask())


def test_rules_a_failed_probe_still_counts_as_an_observation(session: Session) -> None:
    session.run("python3 -c 'raise SystemExit(3)'")
    assert session.state()["runs"][-1]["exit_status"] == 3
    assert not denied(session.ask())


def test_rules_a_quiet_failed_probe_without_output_counts(session: Session) -> None:
    command = "python3 -c 'raise SystemExit(3)'"
    pre = session.event("PreToolUse", "Bash", {"command": command}, tool_use_id="quiet")
    assert not denied(pre)
    # The CLI reports a failed call with no output as just "Exit code N".
    done = session.event(
        "PostToolUseFailure", "Bash", {"command": command}, tool_use_id="quiet", error="Exit code 3"
    )
    assert not denied(done), done
    assert session.state()["runs"][-1]["exit_status"] == 3
    assert not denied(session.ask())


def test_rules_a_shell_cd_does_not_split_the_session_state(session: Session) -> None:
    sub = session.cwd / "pkg"
    sub.mkdir()
    session.run("echo probe")
    # The reported cwd follows a shell `cd`; the state still belongs to the checkout.
    asked = session.event(
        "PostToolUse", ASK, QUESTIONS, cwd=str(sub), tool_response=[{"type": "text", "text": "{}"}]
    )
    assert not denied(asked), asked
    assert not (sub / ".curie").exists()
    refusal = session.publish()
    assert denied(refusal)
    assert "asked questions" in reason(refusal)
    from_sub = session.event("PreToolUse", PUBLISH, {"title": "t", "body": "b"}, cwd=str(sub))
    assert denied(from_sub)


def test_rules_publish_is_refused_after_a_successful_ask_in_the_session(
    session: Session,
) -> None:
    session.run("echo probe")
    assert not denied(session.publish())
    assert not denied(session.ask())
    assert not denied(session.asked())
    refusal = session.publish()
    assert denied(refusal)
    assert "asked questions" in reason(refusal)


def test_rules_an_ask_the_tool_reported_as_an_error_is_not_recorded(session: Session) -> None:
    session.run("echo probe")
    assert not denied(session.asked({"isError": True, "content": []}))
    assert session.state()["asked"] is False
    assert not denied(session.publish())


def test_rules_publish_needs_a_citation_line_after_a_question_heading(session: Session) -> None:
    assert not denied(session.read_issue(f"# Fix add\n\n{HEADING}"))
    assert not denied(session.publish(UNCITED_BODY))
    assert not denied(session.read_issue("# Fix add", has_questions=True))
    refusal = session.publish(UNCITED_BODY)
    assert denied(refusal)
    assert "Answered on the ticket by" in reason(refusal)
    indented = CITED_BODY.replace("\nAnswered", "\n  Answered")
    assert denied(session.publish(indented))
    numbered = CITED_BODY.replace("\nAnswered", "\n2. Answered")
    assert denied(session.publish(numbered))
    assert not denied(session.publish(CITED_BODY))
    # One line per answer relied on, with no limit of seven.
    many = CITED_BODY.replace(
        "\n\n## Acceptance",
        "".join(f"\nAnswered on the ticket by a maintainer: {n}." for n in range(9))
        + "\n\n## Acceptance",
    )
    assert not denied(session.publish(many))


def test_rules_the_heading_is_remembered_for_the_session(session: Session) -> None:
    session.read_issue("# Fix add", has_questions=True)
    session.read_issue("# Fix add\n\nNo questions in this read.")
    assert denied(session.publish(UNCITED_BODY))
    session.env["CURIE_SESSION_ID"] = "another-session"
    assert not denied(session.publish(UNCITED_BODY))


# --- rule 1: protection, and the removed triage refusals ---------------------------


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        ("Write", {"file_path": f"{STATE}/x.json", "content": "{}"}),
        ("Edit", {"file_path": "hooks/reproduction_gate.py", "old_string": "x", "new_string": "y"}),
        ("MultiEdit", {"file_path": "plugin/hooks/hooks.json", "edits": []}),
        ("NotebookEdit", {"notebook_path": f"{STATE}/n.ipynb", "new_source": ""}),
        ("Bash", {"command": f"rm -rf {STATE}"}),
        ("Bash", {"command": "cat ${CLAUDE_PLUGIN_ROOT}/hooks/hooks.json"}),
        ("Bash", {"command": "sed -i s/x/y/ reproduction_gate.py"}),
    ],
)
def test_protected_paths_are_refused(session: Session, tool: str, inputs: dict) -> None:
    assert denied(session.event("PreToolUse", tool, inputs, tool_use_id="guarded"))


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        ("Write", {"file_path": "src/hooks/use_add.py", "content": "x = 1\n"}),
        ("Edit", {"file_path": "calc.py", "old_string": "a - b", "new_string": "a + b"}),
        ("Bash", {"command": "git push origin HEAD"}),
        ("Bash", {"command": "gh pr view 1 || curl -sS example.invalid || true"}),
        ("Bash", {"command": "sleep 1", "run_in_background": True}),
        ("Bash", {"command": "cat .curie/verification.json || true"}),
        ("Agent", {"prompt": "Review the plan"}),
        ("mcp__curie__report_progress", {"phase": "investigate"}),
    ],
)
def test_an_implementation_run_keeps_its_tools(session: Session, tool: str, inputs: dict) -> None:
    assert not denied(session.event("PreToolUse", tool, inputs, tool_use_id="allowed"))


def test_state_is_kept_out_of_the_candidate(session: Session) -> None:
    session.run("echo probe")
    assert list((session.cwd / STATE).glob("*.json"))
    assert session.git("status", "--porcelain", "--untracked-files=all") == ""
    exclude = Path(session.git("rev-parse", "--git-path", "info/exclude").strip())
    lines = (session.cwd / exclude).read_text().splitlines()
    session.run("echo again")
    assert lines.count(f"/{STATE}/") == 1
    assert (session.cwd / exclude).read_text().splitlines() == lines


# --- kept hardening: input decoding, size, state, lock, anchor and deadline --------


def test_duplicate_input_keys_are_refused(session: Session) -> None:
    payload = json.dumps(session.payload("PreToolUse", "Write", {"file_path": "calc.py"}))
    assert not denied(session.raw(payload.encode()))
    duplicated = payload[:-1] + ', "tool_name": "Write"}'
    refusal = session.raw(duplicated.encode())
    assert denied(refusal)
    assert "Duplicate" in reason(refusal)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_json_is_refused(session: Session, constant: str) -> None:
    payload = session.payload("PreToolUse", "Write", {"file_path": "calc.py"}, extra=1)
    text = json.dumps(payload)
    assert not denied(session.raw(text.encode()))
    refusal = session.raw(text.replace('"extra": 1', f'"extra": {constant}').encode())
    assert denied(refusal)
    assert "Nonfinite" in reason(refusal)


def test_oversize_input_is_refused(session: Session) -> None:
    payload = session.payload("PreToolUse", "Write", {"file_path": "calc.py"}, padding="")
    limit = 2 * 1024 * 1024
    base = len(json.dumps(payload).encode())
    payload["padding"] = "x" * (limit - base)
    assert not denied(session.raw(json.dumps(payload).encode()))
    payload["padding"] += "x"
    refusal = session.raw(json.dumps(payload).encode())
    assert denied(refusal)
    assert "size limit" in reason(refusal)


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        ("Write", {"file_path": f"{STATE}/forged.json", "content": '{"runs": []}'}),
        ("Edit", {"file_path": f"{STATE}/forged.json", "old_string": "[]", "new_string": "[1]"}),
        ("Bash", {"command": f"printf '{{}}' > {STATE}/forged.json"}),
        ("Bash", {"command": 'python3 -c \'open(".curie/factory-investigation/x", "w")\''}),
    ],
)
def test_forged_state_write_is_refused(session: Session, tool: str, inputs: dict) -> None:
    session.run("echo probe")
    assert denied(session.event("PreToolUse", tool, inputs, tool_use_id="forge"))
    honest = {"file_path": ".curie/notes.json", "content": "{}"}
    assert not denied(session.event("PreToolUse", "Write", honest))


def test_forged_shell_observation_without_an_admitted_call_is_refused(session: Session) -> None:
    forged = session.event(
        "PostToolUse",
        "Bash",
        {"command": "echo -1"},
        tool_use_id="invented",
        tool_response={"stdout": "-1", "stderr": "", "interrupted": False},
    )
    assert denied(forged)
    assert denied(session.ask())


@pytest.mark.parametrize("field", ["command", "exit_status", "output_digest", "entry_digest"])
def test_hand_edited_entries_refuse_the_ask(session: Session, field: str) -> None:
    session.run("echo probe")
    path = next((session.cwd / STATE).glob("*.json"))
    state = json.loads(path.read_text())
    state["runs"][0][field] = "hand edited"
    path.write_text(json.dumps(state))
    assert denied(session.ask())


@pytest.mark.parametrize("fault", ["corrupt", "undecodable", "different_revision", "symlink"])
def test_an_unavailable_base_anchor_refuses_the_ask(session: Session, fault: str) -> None:
    session.run("echo probe")
    assert not denied(session.ask())
    git_dir = Path(session.git("rev-parse", "--absolute-git-dir").strip())
    anchor = (
        git_dir
        / "factory-investigation"
        / (hashlib.sha256(b"investigation-session").hexdigest() + ".json")
    )
    assert anchor.is_file()
    if fault == "corrupt":
        anchor.write_text("{")
    elif fault == "undecodable":
        anchor.write_bytes(b"\xff")
    elif fault == "different_revision":
        anchor.write_text(json.dumps(dict(base_revision="0" * 40)))
    else:
        anchor.unlink()
        anchor.symlink_to(session.cwd / "calc.py")
    assert denied(session.ask())


def test_lock_is_held_through_check_and_save(session: Session) -> None:
    session.run("echo before")
    lock = next((session.cwd / STATE).glob("*.lock"))
    with lock.open("r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        assert denied(session.event("PreToolUse", "Bash", {"command": "echo after"}))
    session.run("echo after")
    assert not denied(session.ask())


def test_mounted_checkout_ownership_still_records(session: Session, tmp_path: Path) -> None:
    real_git = shutil.which("git")
    assert real_git is not None
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    global_config = tmp_path / "gitconfig"
    global_config.touch()
    launcher = bin_dir / "git"
    # Git's ownership tests use this switch to exercise the real trust check:
    # https://github.com/git/git/blob/v2.39.5/t/t0033-safe-directory.sh
    launcher.write_text(
        "#!/bin/sh\n"
        f"HOME={shlex.quote(str(home))} "
        f"GIT_CONFIG_GLOBAL={shlex.quote(str(global_config))} "
        "GIT_CONFIG_NOSYSTEM=1 GIT_TEST_ASSUME_DIFFERENT_OWNER=1 exec "
        f'{shlex.quote(real_git)} "$@"\n'
    )
    launcher.chmod(0o755)
    session.env.update(PATH=str(bin_dir) + os.pathsep + session.env["PATH"])
    session.run("echo probe")
    assert not denied(session.ask())


@pytest.mark.parametrize("raw", [b"", b"{", b"\xff", b"null", b'{"cwd":"\\ud800"}'])
def test_malformed_input_denies_with_successful_hook_exit(session: Session, raw: bytes) -> None:
    assert denied(session.raw(raw))


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
    refusal = session.event("PreToolUse", "Bash", {"command": "echo probe"}, tool_use_id="slow")
    assert denied(refusal)
    assert "deadline" in reason(refusal)
    session.env["PATH"] = os.environ["PATH"]
    session.run("echo probe")
    assert not denied(session.ask())


def test_input_read_deadline_returns_deny_before_runner_timeout() -> None:
    process = subprocess.Popen(
        [sys.executable, str(HOOK)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.wait(timeout=12) == 0
        assert process.stdout is not None
        response = json.loads(process.stdout.read())
        assert denied(response)
        assert "deadline" in reason(response)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()


@pytest.mark.parametrize("ending", ["completion", "refusal"])
def test_deadline_during_completion_or_refusal_cannot_fail_open(
    session: Session, ending: str
) -> None:
    code = """
import runpy, signal, sys
ending, hook = sys.argv[1:]
visited = False
def trace(frame, event, arg):
    global visited
    if not visited and event == 'line' and frame.f_code.co_name == 'main':
        ready = 'result' in frame.f_locals if ending == 'completion' else 'exc' in frame.f_locals
        if ready:
            visited = True
            sys.stderr.write('deadline checkpoint reached\\n')
            if signal.getitimer(signal.ITIMER_REAL)[0] > 0:
                signal.raise_signal(signal.SIGALRM)
    return trace
sys.settrace(trace)
runpy.run_path(hook, run_name='__main__')
assert visited
"""
    payload = (
        dict(
            hook_event_name="PreToolUse",
            tool_name="Read",
            tool_input=dict(file_path="calc.py"),
            cwd=str(session.cwd),
        )
        if ending == "completion"
        else {}
    )
    done = subprocess.run(
        [sys.executable, "-c", code, ending, str(HOOK)],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        timeout=12,
    )
    assert "deadline checkpoint reached" in done.stderr
    assert done.returncode == 0, done.stderr
    assert denied(json.loads(done.stdout))


# --- registration: the hook as Claude Code runs it from hooks.json ------------------


class Registered:
    """Selects hooks.json entries by ``re.fullmatch`` and runs this hook's command.

    The review gate's entries are skipped by command name; ``invocations`` lists
    each (event, tool) this hook actually ran for.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.hooks = json.loads((BUNDLE / "hooks/hooks.json").read_text())["hooks"]
        self.invocations: list[tuple[str, str]] = []
        self.serial = 0

    def dispatch(self, event: str, tool: str, inputs: dict, **extra: object) -> dict | None:
        outputs = []
        for entry in self.hooks.get(event, []):
            if not re.fullmatch(entry.get("matcher") or ".*", tool):
                continue
            for hook in entry["hooks"]:
                if "reproduction_gate.py" not in hook["command"]:
                    continue
                assert hook["timeout"] == 10
                command = hook["command"].replace("${CLAUDE_PLUGIN_ROOT}", str(BUNDLE))
                done = subprocess.run(
                    command,
                    shell=True,
                    input=json.dumps(self.session.payload(event, tool, inputs, **extra)),
                    text=True,
                    capture_output=True,
                    cwd=BUNDLE,
                    env=self.session.env,
                    timeout=12,
                )
                assert done.returncode == 0, done.stderr
                self.invocations.append((event, tool))
                outputs.append(json.loads(done.stdout))
        assert len(outputs) <= 1, outputs
        return outputs[0] if outputs else None

    def bash(self, command: str) -> None:
        self.serial += 1
        call = f"registered-{self.serial}"
        pre = self.dispatch("PreToolUse", "Bash", {"command": command}, tool_use_id=call)
        assert pre is not None and not denied(pre), pre
        done = subprocess.run(command, shell=True, cwd=self.session.cwd, capture_output=True)
        assert done.returncode == 0
        post = self.dispatch(
            "PostToolUse",
            "Bash",
            {"command": command},
            tool_use_id=call,
            tool_response={"stdout": done.stdout.decode(), "stderr": "", "interrupted": False},
        )
        assert post is not None and not denied(post), post


@pytest.fixture
def registered(session: Session) -> Registered:
    return Registered(session)


def test_registered_entries_are_appended_after_the_review_gate() -> None:
    hooks = json.loads((BUNDLE / "hooks/hooks.json").read_text())["hooks"]
    command = "python3 ${CLAUDE_PLUGIN_ROOT}/hooks/reproduction_gate.py"
    expected = {
        "PreToolUse": "Bash|Write|Edit|MultiEdit|NotebookEdit|.*ask_questions|.*publish_changes",
        "PostToolUse": "Bash|.*get_issue|.*ask_questions",
        "PostToolUseFailure": "Bash",
    }
    for event, matcher in expected.items():
        first, last = hooks[event][0], hooks[event][-1]
        assert "review_gate.py" in first["hooks"][0]["command"], event
        assert last == {
            "matcher": matcher,
            "hooks": [{"type": "command", "command": command, "timeout": 10}],
        }, event
    assert all("reproduction_gate" not in str(entry) for entry in hooks["UserPromptSubmit"])


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        ("Write", {"file_path": f"{STATE}/forged.json", "content": "{}"}),
        ("Edit", {"file_path": f"{STATE}/forged.json", "old_string": "a", "new_string": "b"}),
        ("Bash", {"command": f"rm -rf {STATE}"}),
    ],
)
def test_registered_hook_refuses_writes_to_its_state(
    registered: Registered, tool: str, inputs: dict
) -> None:
    out = registered.dispatch("PreToolUse", tool, inputs, tool_use_id="forge")
    assert out is not None and denied(out)
    honest = {"file_path": "calc.py", "content": "def add(a, b): return a + b\n"}
    out = registered.dispatch("PreToolUse", tool if tool != "Bash" else "Write", honest)
    assert out is not None and not denied(out)


def test_registered_hook_gates_ask_then_publish(registered: Registered) -> None:
    out = registered.dispatch("PreToolUse", ASK, QUESTIONS)
    assert out is not None and denied(out)
    registered.bash("python3 -c 'from calc import add; print(add(2, 3))'")
    out = registered.dispatch("PreToolUse", ASK, QUESTIONS)
    assert out is not None and not denied(out)
    out = registered.dispatch(
        "PostToolUse", ASK, QUESTIONS, tool_response=[{"type": "text", "text": "posted"}]
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and denied(out)


def test_registered_hook_records_the_question_heading_from_get_issue(
    registered: Registered,
) -> None:
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Fix add", has_questions=True)
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_holds_the_citation_rule_for_an_oversize_ticket_read(
    registered: Registered,
) -> None:
    # The runner returns up to 4 MiB of ticket, past the hook's 2 MiB input limit.
    ticket = f"{HEADING}\n\n1. Which?\n\n" + "comment text. " * (3 * 1024 * 1024 // 14)
    assert len(json.dumps(issue_response(ticket))) > 2 * 1024 * 1024
    out = registered.dispatch("PostToolUse", ISSUE_READ, {}, tool_response=issue_response(ticket))
    assert out is not None and "size limit" in reason(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_leaves_publication_open_after_an_oversize_non_ticket_result(
    registered: Registered,
) -> None:
    big = {"stdout": "x" * (3 * 1024 * 1024), "stderr": "", "interrupted": False}
    out = registered.dispatch(
        "PostToolUse", "Bash", {"command": "cat big"}, tool_use_id="big", tool_response=big
    )
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_keeps_citations_when_the_cli_replaces_a_large_ticket_with_its_notice(
    registered: Registered,
) -> None:
    # Claude Code replaces a tool result over its size limit with this notice, so the hook
    # sees the preview and never the body (shape observed in a session transcript).
    notice = (
        "<persisted-output>\nOutput too large (3.0MB). Full output saved to: "
        "/tmp/tool-results/get_issue.txt\n\nPreview (first 2KB):\n"
        '{\n  "issue": "#7",\n  "url": "https://github.com/Acme/bot/issues/7",\n'
        '  "ticket": "# Title\\n'
    )
    out = registered.dispatch("PostToolUse", ISSUE_READ, {}, tool_response=notice)
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_leaves_publication_open_after_a_decodable_ticket_without_questions(
    registered: Registered,
) -> None:
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Title\n\nFix add.")
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and not denied(out)


def test_only_the_platform_issue_read_sets_question_history(session: Session) -> None:
    session.event(
        "PostToolUse", "mcp__notes__get_issue", {},
        tool_response=issue_response("# Notes", has_questions=True),
    )
    assert not denied(session.publish(UNCITED_BODY))


def oversize_ticket_read(registered: Registered) -> None:
    ticket = f"{HEADING}\n\n1. Which?\n\n" + "comment text. " * (3 * 1024 * 1024 // 14)
    out = registered.dispatch("PostToolUse", ISSUE_READ, {}, tool_response=issue_response(ticket))
    assert out is not None and "size limit" in reason(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)


def test_registered_hook_clears_an_overflow_heading_when_a_retry_decodes_without_questions(
    registered: Registered,
) -> None:
    oversize_ticket_read(registered)
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Title\n\nFix add.")
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_still_requires_the_citation_when_a_retry_decodes_with_an_answer(
    registered: Registered,
) -> None:
    oversize_ticket_read(registered)
    answered = f"{HEADING}\n\n1. Which?\n\nUse the second."
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response(answered, has_questions=True)
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_keeps_a_seen_heading_through_an_overflow_and_a_clean_retry(
    registered: Registered,
) -> None:
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Fix add", has_questions=True)
    )
    assert out is not None and not denied(out)
    oversize_ticket_read(registered)
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Title\n\nFix add.")
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)


def test_registered_hook_keeps_citations_through_a_checkout_cleanup(
    registered: Registered,
) -> None:
    out = registered.dispatch(
        "PostToolUse", ISSUE_READ, {}, tool_response=issue_response("# Fix add", has_questions=True)
    )
    assert out is not None and not denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    registered.session.git("clean", "-fdx", "-q")
    assert not (registered.session.cwd / STATE).exists()
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and denied(out)
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": CITED_BODY})
    assert out is not None and not denied(out)


def test_registered_hook_does_not_mistake_a_refused_first_ask_for_lost_state(
    registered: Registered,
) -> None:
    out = registered.dispatch("PreToolUse", ASK, QUESTIONS)
    assert out is not None and denied(out)
    registered.bash("python3 -c 'from calc import add; print(add(2, 3))'")
    out = registered.dispatch("PreToolUse", PUBLISH, {"title": "t", "body": UNCITED_BODY})
    assert out is not None and not denied(out)


@pytest.mark.parametrize(
    ("event", "tool", "inputs"),
    [
        ("PreToolUse", "Read", {"file_path": "calc.py"}),
        ("PostToolUse", "Read", {"file_path": "calc.py"}),
        ("PreToolUse", "Agent", {"prompt": "Review the plan"}),
        ("PostToolUse", "Agent", {"prompt": "Review the plan"}),
        ("PostToolUseFailure", "Agent", {"prompt": "Review the plan"}),
        ("PreToolUse", "mcp__curie__report_progress", {"phase": "investigate"}),
        ("PostToolUse", "mcp__curie__report_progress", {"phase": "investigate"}),
    ],
)
def test_registered_hook_never_runs_for_unnamed_tools(
    registered: Registered, event: str, tool: str, inputs: dict
) -> None:
    assert registered.dispatch(event, tool, inputs) is None
    assert registered.invocations == []
