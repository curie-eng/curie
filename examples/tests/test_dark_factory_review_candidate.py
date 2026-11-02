"""The review gate binds review and publication to the run's candidate (#5230).

The candidate is the final publishable contents: the net change from the base
commit, the checkout's HEAD when the run started, to the final working tree
files. One shared builder (``curie_runner.workspace_snapshot.build_candidate``)
computes it for both the runner capture and the gate. The gate fingerprints its
tree and, at diff review admission, writes its rendered patch to a file the
CANDIDATE block names, so the reviewer reads exactly what will publish.

Every case drives ``examples/dark-factory/hooks/review_gate.py`` as the real
hook subprocess, one process per event, against a real git checkout, through
the shared fixture in ``test_dark_factory_review_gate.py``. A local commit must
not redefine the base or keep a stale approval alive, and committing the
approved files must not by itself be refused. Neither an index flag, a git stat
cache nor a diff driver may make the reviewed candidate differ from the published
one, and a changed path under a filter attribute is refused rather than built.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shlex
import stat
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app, workspace_snapshot
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_worker.publication_validation import (
    publication_git_environment,
    validate_snapshot_against_base,
)
from curie_worker.runner_client import RunnerClient


def _load_gate_helpers() -> Any:
    """The review gate test module, loaded by path: ``examples/tests`` is no package."""
    path = Path(__file__).resolve().with_name("test_dark_factory_review_gate.py")
    spec = importlib.util.spec_from_file_location("dark_factory_review_candidate_helpers", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


gate = _load_gate_helpers()
Session = gate.Session
PLAN, DIFF, PUBLISH, SOURCE, ISSUE = gate.PLAN, gate.DIFF, gate.PUBLISH, gate.SOURCE, gate.ISSUE
TEST = gate.TEST
reply, contract, candidate_block = gate.reply, gate.contract, gate.candidate_block

DIFF_INPUT = {"subagent_type": DIFF, "description": "diff-reviewer", "prompt": "p"}
FRESH_REVIEW = "Fresh diff review required: checkout changed or is unverifiable."
UNVERIFIABLE = "Diff checkout is unverifiable; restore it before review."
FRESH_REVIEW_PREFIX = "Fresh diff review required:"
PATCH_CHANGED = "candidate patch is missing or changed"
FILTER_REFUSAL = "has a filter attribute"
CAPTURE_REPO = "acme-corp/acme-calc"
GITHUB = "https://github.com"


def _git(s: Any, *args: str) -> str:
    """One git command in the session's checkout, with a placeholder identity; its stdout."""
    return _git_bytes(s, *args).decode("utf-8")


def _git_bytes(s: Any, *args: str) -> bytes:
    done = subprocess.run(
        [
            "git",
            "-c",
            "user.email=acme-bot@example.com",
            "-c",
            "user.name=acme-bot",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=s.cwd,
        env=gate._clean_env(),
        capture_output=True,
        check=True,
    )
    return done.stdout


def _head(s: Any) -> str:
    return _git(s, "rev-parse", "HEAD").strip()


def _commit(s: Any, message: str) -> None:
    """What an agent runs to commit everything: ``git add -A && git commit``."""
    _git(s, "add", "-A")
    _git(s, "commit", "-q", "-m", message)


def _clean(s: Any) -> bool:
    """Nothing staged, unstaged or untracked, and ``git diff HEAD`` is empty."""
    status = _git(s, "status", "--porcelain=v1", "--untracked-files=all")
    return status == "" and _git(s, "diff", "HEAD") == ""


def _change_source(s: Any, label: str) -> None:
    """A real source change made outside the edit tools."""
    (s.cwd / SOURCE).write_text(f"def add(a, b):\n    return a + b  # {label}\n")


def _prompted(tmp_path: Path) -> tuple[Any, str]:
    """A run started by its first prompt; the session and the checkout's HEAD right after it."""
    s = Session(tmp_path)
    assert contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    return s, _head(s)


def _planned(tmp_path: Path) -> tuple[Any, str]:
    """A started run whose plan review approved; the session and its base commit."""
    s, base = _prompted(tmp_path)
    s.approve_plan()
    return s, base


def _diff_review(s: Any) -> dict[str, Any]:
    """The PreToolUse decision on one diff reviewer call."""
    decision: dict[str, Any] = s.pre("Agent", dict(DIFF_INPUT))
    return decision


@dataclass(frozen=True)
class _Admitted:
    """One admitted diff review: the prompt and its patch file's bytes at admission."""

    prompt: str
    patch: bytes


def _admitted(prompt: str) -> _Admitted:
    """Read the patch file an admitted diff reviewer prompt names, while the review runs.

    The file must be readable by the run alone.
    """
    path = Path(_block_line(prompt, "candidate patch"))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600, oct(path.stat().st_mode)
    return _Admitted(prompt, path.read_bytes())


def _admit_diff(s: Any) -> tuple[dict[str, Any], _Admitted]:
    """An admitted diff reviewer call: its PreToolUse decision and what it was sent."""
    pre = _diff_review(s)
    assert pre["permissionDecision"] == "allow", pre
    return pre, _admitted(str(pre["updatedInput"]["prompt"]))


def _deliver_diff(s: Any, pre: dict[str, Any], verdict: str) -> str:
    """The admitted reviewer returns ``verdict``; the PostToolUse context the agent gets."""
    post = s.fire(
        "PostToolUse",
        tool_name="Agent",
        tool_input=pre["updatedInput"],
        tool_response={"content": [{"type": "text", "text": reply(DIFF, verdict)}]},
    )
    assert post is not None
    return str(post["hookSpecificOutput"]["additionalContext"])


def _review_diff(s: Any, verdict: str) -> _Admitted:
    """One full diff reviewer call that returns ``verdict``."""
    pre, admitted = _admit_diff(s)
    context = _deliver_diff(s, pre, verdict)
    assert ("APPROVED" if verdict == "APPROVE" else verdict) in context, context
    return admitted


def _approve_diff(s: Any) -> _Admitted:
    """One admitted diff review that approves."""
    return _review_diff(s, "APPROVE")


def _publish(s: Any) -> dict[str, Any]:
    decision: dict[str, Any] = s.pre(PUBLISH, {"title": "t", "body": "b"})
    return decision


def _assert_denied(decision: dict[str, Any], reason: str) -> None:
    assert decision["permissionDecision"] == "deny", decision
    assert decision["permissionDecisionReason"] == reason, decision


def _state_file(s: Any) -> Path:
    return Path(s.env["DARK_FACTORY_STATE_DIR"]) / "sess-1.json"


def _drop_base(s: Any, base: str) -> None:
    """Rewrite the saved run as a state file without a recorded base commit."""
    path = _state_file(s)
    state = json.loads(path.read_text())
    assert state["base"] == base, state.get("base")
    del state["base"]
    path.write_text(json.dumps(state))


def _plan_run(s: Any) -> str:
    """Start the run on the session's current checkout and approve its plan; the base."""
    assert contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    base = _head(s)
    s.approve_plan()
    return base


def _block_line(prompt: str, label: str) -> str:
    """The value of one ``<label> <value>`` line of the prompt's CANDIDATE block."""
    block = prompt.rpartition("\n\nCANDIDATE:\n")[2]
    (line,) = [line for line in block.splitlines() if line.startswith(f"{label} ")]
    return line.removeprefix(f"{label} ")


def _reviewed_patch(s: Any, base: str, reviewed: _Admitted) -> bytes:
    """Check the diff reviewer got the checkout's candidate; the patch file it was sent.

    The prompt ends with the CANDIDATE block for the candidate the shared
    builder computes for the unchanged checkout, after the contract, and the
    file it named held that candidate's rendered patch.
    """
    built = gate.build_candidate(s.cwd, base)
    path = gate.candidate_patch_path(s.env["DARK_FACTORY_STATE_DIR"], built.tree)
    prompt = reviewed.prompt
    assert prompt.endswith(contract() + candidate_block(base, built.tree, path)), prompt[-900:]
    assert prompt.count("CANDIDATE:") == 1
    assert reviewed.patch == built.patch
    return reviewed.patch


def _section(patch: bytes, path: str) -> bytes:
    """The part of a rendered patch that changes ``path``."""
    header = f"diff --git a/{path} b/{path}\n".encode()
    start = patch.index(header)
    end = patch.find(b"\ndiff --git ", start + len(header))
    return patch[start:] if end < 0 else patch[start : end + 1]


# ``git ls-files -v`` tags: skip-worktree is ``S``, assume-unchanged lowercases the tag.
_FLAG_TAGS = {"assume-unchanged": "h", "skip-worktree": "S"}


def _flag(s: Any, flag: str, *paths: str, tag: str | None = None) -> None:
    """Set an index flag (``assume-unchanged``, ``skip-worktree``) or clear one (``no-...``).

    git stops reading a flagged entry's file. ``tag`` is the ``ls-files -v`` tag
    each path must carry afterwards; by default the tag ``flag`` alone gives.
    """
    _git(s, "update-index", f"--{flag}", "--", *paths)
    expected = tag or _FLAG_TAGS[flag]
    for path in paths:
        found = _git(s, "ls-files", "-v", "--", path)[:1]
        assert found == expected, (path, found)


def _with_dropped_escaping_link(tmp_path: Path) -> Any:
    """A session whose checkout is prepared the way the worker prepares one.

    The base commits ``out -> ../outside``; preparation unlinks that escaping
    link and marks it skip-worktree, so the file stays absent and git status is
    clean.
    """
    s = Session(tmp_path)
    os.symlink("../outside", s.cwd / "out")
    _commit(s, "Escaping link fixture")
    (s.cwd / "out").unlink()
    _flag(s, "skip-worktree", "out")
    assert _clean(s)
    return s


# --- A commit keeps the approval only when the final files are the approved ones ---


@pytest.mark.parametrize("edited", [False, True], ids=["commit_only", "edit_then_commit"])
def test_a_commit_after_diff_approval_keeps_it_only_when_the_files_are_unchanged(
    tmp_path: Path, edited: bool
) -> None:
    s, _ = _planned(tmp_path)
    _change_source(s, "fix")
    _approve_diff(s)
    if edited:
        _change_source(s, "edited after approval")
    _commit(s, "Fix add")
    # The committed change no longer shows in git diff HEAD.
    assert _clean(s)

    decision = _publish(s)

    if not edited:
        # The final files are the approved candidate, so committing them changes nothing.
        assert decision["permissionDecision"] == "allow", decision
        return
    _assert_denied(decision, FRESH_REVIEW)
    refused = [e for e in s.gate_events() if e["curie_gate"] == "diff_approval_refused"]
    assert [e["cause"] for e in refused] == ["checkout_changed_or_unverifiable"]
    # Fresh review, not a stop: the committed checkout is reviewed again and publishes.
    _approve_diff(s)
    assert _publish(s)["permissionDecision"] == "allow"


def test_a_change_committed_before_the_diff_review_is_reviewed_and_published(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _change_source(s, "fix")
    _commit(s, "Fix add")
    assert _head(s) != base and _clean(s)

    reviewed = _approve_diff(s)

    # The committed change is in the patch the reviewer reads.
    patch = _reviewed_patch(s, base, reviewed)
    assert b"+    return a + b  # fix\n" in _section(patch, SOURCE)
    assert _publish(s)["permissionDecision"] == "allow"
    assert not [e for e in s.gate_events() if e["curie_gate"] == "diff_approval_refused"]


# --- A base file removed from the index and ignored is still part of the candidate --


@pytest.mark.parametrize("untracking", ["staged", "committed"])
def test_an_edit_to_a_base_file_removed_from_the_index_and_ignored_needs_a_fresh_review(
    tmp_path: Path, untracking: str
) -> None:
    s, base = _planned(tmp_path)
    _git(s, "rm", "-q", "--cached", SOURCE)
    gitignore = s.cwd / ".gitignore"
    gitignore.write_text(gitignore.read_text() + f"{SOURCE}\n")
    if untracking == "committed":
        _commit(s, "Stop tracking the source")
    # The base tracks the file; it is still on disk, outside the index, and ignored.
    assert SOURCE in _git(s, "ls-tree", "-r", "--name-only", base).splitlines()
    assert _git(s, "ls-files", "--", SOURCE) == ""
    assert _git(s, "ls-files", "--others", "--exclude-standard") == ""
    assert (s.cwd / SOURCE).is_file()
    reviewed = _approve_diff(s)
    # Kept with its bytes on disk, which equal the base: not a deletion.
    assert f"diff --git a/{SOURCE} ".encode() not in _reviewed_patch(s, base, reviewed)

    (s.cwd / SOURCE).write_text("def add(a, b):\n    return a - b\n")

    _assert_denied(_publish(s), FRESH_REVIEW)


@pytest.mark.parametrize("ignored_by", ["base", "agent"])
def test_an_edit_to_an_ignored_file_the_base_never_tracked_keeps_the_approval(
    tmp_path: Path, ignored_by: str
) -> None:
    s, base = _planned(tmp_path)
    _change_source(s, "fix")
    if ignored_by == "base":
        # The fixture's base ignores .venv/.
        scratch = s.cwd / ".venv" / "cache.txt"
        scratch.parent.mkdir()
    else:
        gitignore = s.cwd / ".gitignore"
        gitignore.write_text(gitignore.read_text() + "build.log\n")
        scratch = s.cwd / "build.log"
        _commit(s, "Ignore the build log")
    scratch.write_text("one\n")
    relative = scratch.relative_to(s.cwd).as_posix()
    assert relative not in _git(s, "ls-tree", "-r", "--name-only", base).splitlines()
    _approve_diff(s)

    scratch.write_text("two\n")

    assert _publish(s)["permissionDecision"] == "allow"


# --- A committed .github change stops the diff review --------------------------------


@pytest.mark.parametrize("directory", [".github", ".GitHub"])
@pytest.mark.parametrize("change", ["added", "modified", "deleted"])
def test_a_committed_github_change_stops_the_diff_review(
    tmp_path: Path, directory: str, change: str
) -> None:
    s = Session(tmp_path)
    path = f"{directory}/workflows/ci.yml"
    target = s.cwd / path
    if change != "added":
        target.parent.mkdir(parents=True)
        target.write_text("on: push\n")
        _commit(s, "Workflow fixture")
    assert contract() in gate.context_of(s.fire("UserPromptSubmit", prompt=ISSUE))
    s.approve_plan()
    s.report("implement", 1)
    if change == "deleted":
        target.unlink()
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("on: [push, pull_request]\n")
    _commit(s, "Change the workflow")
    assert _clean(s)

    out = s.fire("PreToolUse", tool_name="Agent", tool_input=dict(DIFF_INPUT))

    # The same stop an uncommitted .github change gets (#4435).
    gate._assert_github_path_stops_run(s, out, path)


@pytest.mark.parametrize("path", [SOURCE, "pkg/.github/workflows/ci.yml"])
def test_a_committed_change_outside_root_github_reaches_the_diff_reviewer(
    tmp_path: Path, path: str
) -> None:
    s, _ = _planned(tmp_path)
    s.report("implement", 1)
    target = s.cwd / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# changed and committed by the agent\n")
    _commit(s, "Agent change")
    assert _clean(s)

    _approve_diff(s)

    assert _publish(s)["permissionDecision"] == "allow"
    assert not [e for e in s.gate_events() if e["curie_gate"] == "github_path_refused"]


# --- The diff reviewer's prompt names the base and the candidate's patch file -------


def test_the_diff_reviewer_prompt_names_the_base_recorded_when_the_run_started(
    tmp_path: Path,
) -> None:
    s, base = _prompted(tmp_path)
    s.load_workflow()
    plan, context = s.review(PLAN, reply(PLAN, "APPROVE"))
    assert "APPROVED" in context
    # Control: the plan reviewer gets the contract and no CANDIDATE block.
    plan_prompt = plan["updatedInput"]["prompt"]
    assert plan_prompt.endswith(contract())
    assert "CANDIDATE:" not in plan_prompt

    _change_source(s, "first try")
    _commit(s, "First try")
    first_head = _head(s)
    assert first_head != base
    first = _review_diff(s, "CHANGES")
    first_patch = _reviewed_patch(s, base, first)
    _change_source(s, "second try")
    _commit(s, "Second try")
    second_head = _head(s)
    second = _approve_diff(s)
    second_patch = _reviewed_patch(s, base, second)

    # Agent commits never redefine the base: both rounds name the run's first HEAD.
    for prompt in (first.prompt, second.prompt):
        assert prompt.startswith("p\n\n"), prompt[:40]
        assert _block_line(prompt, "base commit") == base
        assert first_head not in prompt and second_head not in prompt
    # Each round's patch file is that round's candidate, measured from the base.
    assert b"+    return a + b  # first try\n" in first_patch
    assert b"+    return a + b  # second try\n" in second_patch
    assert _block_line(first.prompt, "candidate patch") != _block_line(
        second.prompt, "candidate patch"
    )
    assert _publish(s)["permissionDecision"] == "allow"


def test_a_ci_fix_round_in_a_fresh_state_directory_records_its_base(tmp_path: Path) -> None:
    s = Session(tmp_path)
    assert not Path(s.env["DARK_FACTORY_STATE_DIR"]).exists()
    context = gate.context_of(s.fire("UserPromptSubmit", prompt=gate._ci_prompt(2)))
    assert context.startswith("CI fix round")
    base = _head(s)
    _change_source(s, "ci fix")
    _commit(s, "Fix the failing check")

    decision = _diff_review(s)

    # Admitted, not unverifiable: the CI round start recorded the base.
    assert decision["permissionDecision"] == "allow", decision
    prompt = decision["updatedInput"]["prompt"]
    assert b"+    return a + b  # ci fix\n" in _reviewed_patch(s, base, _admitted(prompt))
    assert _head(s) not in prompt


# --- A reverted change is an empty candidate, not a refusal --------------------------


def test_a_change_reverted_to_the_base_content_is_still_reviewed_and_published(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    original = (s.cwd / SOURCE).read_text()
    _change_source(s, "attempt")
    _commit(s, "Attempt")
    _git(s, "revert", "--no-edit", "HEAD")
    assert (s.cwd / SOURCE).read_text() == original
    assert _head(s) != base and _clean(s)
    assert _git(s, "diff", "--name-only", base) == ""

    decision = _diff_review(s)

    assert decision["permissionDecision"] == "allow", decision
    assert _reviewed_patch(s, base, _admitted(decision["updatedInput"]["prompt"])) == b""


# --- A run state without a base fails closed -----------------------------------------


@pytest.mark.parametrize("dropped", [True, False], ids=["no_base", "control"])
def test_a_run_state_without_a_base_refuses_the_diff_review_as_unverifiable(
    tmp_path: Path, dropped: bool
) -> None:
    s, base = _planned(tmp_path)
    _change_source(s, "fix")
    if dropped:
        _drop_base(s, base)
    else:
        assert json.loads(_state_file(s).read_text())["base"] == base

    decision = _diff_review(s)

    if not dropped:
        assert decision["permissionDecision"] == "allow", decision
        return
    _assert_denied(decision, UNVERIFIABLE)
    assert ("review_diff", 1) not in s.phases()
    assert json.loads(_state_file(s).read_text())["diff"]["round"] == 0


@pytest.mark.parametrize("dropped", [True, False], ids=["no_base", "control"])
def test_a_run_state_that_lost_its_base_after_approval_cannot_publish(
    tmp_path: Path, dropped: bool
) -> None:
    s, base = _planned(tmp_path)
    _change_source(s, "fix")
    _approve_diff(s)
    if dropped:
        _drop_base(s, base)

    decision = _publish(s)

    if dropped:
        _assert_denied(decision, FRESH_REVIEW)
    else:
        assert decision["permissionDecision"] == "allow", decision


# --- An index flag does not hide an edit from the approval ---------------------------


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree"])
@pytest.mark.parametrize("order", ["flag_after_approval", "flag_before_review"])
def test_an_edit_to_a_flagged_base_file_after_diff_approval_needs_a_fresh_review(
    tmp_path: Path, flag: str, order: str
) -> None:
    s, _ = _planned(tmp_path)
    _change_source(s, "fix")
    if order == "flag_before_review":
        _flag(s, flag, TEST)
    _approve_diff(s)
    if order == "flag_after_approval":
        _flag(s, flag, TEST)

    (s.cwd / TEST).write_text("def test_add():\n    assert True\n")
    # git reads the index entry, not the file: neither status nor diff shows the edit.
    assert TEST not in _git(s, "status", "--porcelain=v1", "--untracked-files=all")
    assert _git(s, "diff", "HEAD", "--", TEST) == ""

    _assert_denied(_publish(s), FRESH_REVIEW)
    refused = [e for e in s.gate_events() if e["curie_gate"] == "diff_approval_refused"]
    assert [e["cause"] for e in refused] == ["checkout_changed_or_unverifiable"]
    # Fresh review, not a stop: the edited checkout is reviewed again and publishes.
    _approve_diff(s)
    assert _publish(s)["permissionDecision"] == "allow"


@pytest.mark.parametrize("cleared", [True, False], ids=["skip_worktree_cleared", "control_reset"])
def test_changing_only_an_index_flag_after_diff_approval_needs_a_fresh_review(
    tmp_path: Path, cleared: bool
) -> None:
    s, _ = _planned(tmp_path)
    # A base file is absent on disk with both flags set: git shows nothing for it,
    # and the candidate keeps the base entry of an absent skip-worktree path.
    (s.cwd / TEST).unlink()
    _flag(s, "assume-unchanged", TEST)
    _flag(s, "skip-worktree", TEST, tag="s")
    _change_source(s, "fix")
    _approve_diff(s)

    if cleared:
        # Still assume-unchanged, so status and diff stay silent, but the absent file
        # is now a deletion in the candidate.
        _flag(s, "no-skip-worktree", TEST, tag="h")
    else:
        # Control: setting the flag it already has changes nothing.
        _flag(s, "skip-worktree", TEST, tag="s")
    assert TEST not in _git(s, "status", "--porcelain=v1", "--untracked-files=all")

    decision = _publish(s)

    if not cleared:
        assert decision["permissionDecision"] == "allow", decision
        return
    _assert_denied(decision, FRESH_REVIEW)


@pytest.mark.parametrize("chmod", [True, False], ids=["chmod_after_approval", "control"])
def test_an_executable_bit_change_after_approval_needs_a_fresh_review_despite_core_filemode(
    tmp_path: Path, chmod: bool
) -> None:
    s, _ = _planned(tmp_path)
    # The checkout's own config tells git to ignore executable bits.
    _git(s, "config", "core.fileMode", "false")
    _change_source(s, "fix")
    _approve_diff(s)

    if chmod:
        target = s.cwd / TEST
        target.chmod(target.stat().st_mode | 0o111)
        assert TEST not in _git(s, "status", "--porcelain=v1", "--untracked-files=all")
        assert _git(s, "diff", "HEAD", "--", TEST) == ""

    decision = _publish(s)

    if not chmod:
        assert decision["permissionDecision"] == "allow", decision
        return
    _assert_denied(decision, FRESH_REVIEW)


def test_a_prepared_escaping_link_that_stays_absent_keeps_the_approval(tmp_path: Path) -> None:
    # Liveness control for the flag refusals above: the skip-worktree entry
    # preparation leaves behind stays the base entry, consistently.
    s = _with_dropped_escaping_link(tmp_path)
    base = _plan_run(s)
    _change_source(s, "fix")

    reviewed = _approve_diff(s)

    patch = _reviewed_patch(s, base, reviewed)
    assert b"diff --git a/out " not in patch
    assert b"+    return a + b  # fix\n" in _section(patch, SOURCE)
    assert not (s.cwd / "out").exists() and not (s.cwd / "out").is_symlink()
    assert _publish(s)["permissionDecision"] == "allow"


# --- The reviewed patch file carries what git diff does not show ---------------------


@pytest.mark.parametrize(
    "change",
    ["flagged_edit", "index_removed_ignored_edit", "untracked_file", "deleted_assume_unchanged"],
)
def test_the_reviewed_patch_file_carries_changes_git_diff_does_not_show(
    tmp_path: Path, change: str
) -> None:
    s, base = _planned(tmp_path)
    if change == "flagged_edit":
        _flag(s, "assume-unchanged", TEST)
        (s.cwd / TEST).write_text("def test_add():\n    assert True\n")
        path, expected = TEST, b"+    assert True\n"
    elif change == "index_removed_ignored_edit":
        _git(s, "rm", "-q", "--cached", SOURCE)
        gitignore = s.cwd / ".gitignore"
        gitignore.write_text(gitignore.read_text() + f"{SOURCE}\n")
        (s.cwd / SOURCE).write_text("def add(a, b):\n    return a - b\n")
        path, expected = SOURCE, b"+    return a - b\n"
        # git diff shows a deletion; the candidate is the edited file.
        assert "deleted file mode" in _git(s, "diff", base, "--", SOURCE)
    elif change == "untracked_file":
        target = s.cwd / "notes" / "todo.txt"
        target.parent.mkdir()
        target.write_text("remember the acme release\n")
        path, expected = "notes/todo.txt", b"+remember the acme release\n"
    else:
        _flag(s, "assume-unchanged", TEST)
        (s.cwd / TEST).unlink()
        path, expected = TEST, b"deleted file mode 100644\n"
    if change != "index_removed_ignored_edit":
        assert path not in _git(s, "diff", "--name-only", base)

    reviewed = _approve_diff(s)

    section = _section(_reviewed_patch(s, base, reviewed), path)
    assert expected in section
    if change == "index_removed_ignored_edit":
        assert b"deleted file mode" not in section


def test_many_flagged_edits_are_admitted_and_every_one_is_in_the_patch_file(
    tmp_path: Path,
) -> None:
    s = Session(tmp_path)
    notes = [f"docs/note-{n:02d}.txt" for n in range(51)]
    for note in notes:
        target = s.cwd / note
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{note}\n")
    _commit(s, "Notes fixture")
    base = _plan_run(s)
    _flag(s, "assume-unchanged", *notes)
    for note in notes:
        (s.cwd / note).write_text(f"{note} edited\n")

    # Admitted: there is no cap on changes git diff does not show.
    reviewed = _approve_diff(s)

    patch = _reviewed_patch(s, base, reviewed)
    for note in notes:
        assert f"+{note} edited\n".encode() in _section(patch, note)
    assert json.loads(_state_file(s).read_text())["stopped"] is None


@pytest.mark.parametrize(
    "path",
    [".github/workflows/ci.yml", "pkg/.github/workflows/ci.yml"],
    ids=["root_github", "control_nested"],
)
def test_a_flagged_github_edit_hidden_from_git_diff_stops_the_diff_review(
    tmp_path: Path, path: str
) -> None:
    s = Session(tmp_path)
    target = s.cwd / path
    target.parent.mkdir(parents=True)
    target.write_text("on: push\n")
    _commit(s, "Workflow fixture")
    base = _plan_run(s)
    s.report("implement", 1)
    _flag(s, "assume-unchanged", path)
    target.write_text("on: [push, pull_request]\n")
    assert _clean(s)

    out = s.fire("PreToolUse", tool_name="Agent", tool_input=dict(DIFF_INPUT))

    if path.startswith(".github/"):
        # The same stop a visible .github change gets (#4435).
        gate._assert_github_path_stops_run(s, out, path)
        return
    # Control: outside the root .github the diff review is admitted and reviews the edit.
    assert out is not None
    decision = out["hookSpecificOutput"]
    assert decision["permissionDecision"] == "allow", decision
    patch = _reviewed_patch(s, base, _admitted(decision["updatedInput"]["prompt"]))
    assert b"+on: [push, pull_request]\n" in _section(patch, path)
    assert not [e for e in s.gate_events() if e["curie_gate"] == "github_path_refused"]


# --- The reviewed candidate is the published one -------------------------------------

# A comment in both Python and YAML, so it changes no behavior where it lands.
HIDDEN_LINE = "# HIDDEN: never shown by git diff\n"


def _publishable(s: Any) -> None:
    """Give the checkout the credential-free origin the runner capture requires."""
    _git(s, "remote", "add", "origin", f"{GITHUB}/{CAPTURE_REPO}.git")


def _capture(s: Any, base: str) -> Any:
    """The real runner capture of the session's checkout against the run's base."""
    return workspace_snapshot.capture_workspace_snapshot(
        s.cwd,
        base_sha=base,
        expected_repo=CAPTURE_REPO,
        trusted_origin=GITHUB,
        repository_path=CAPTURE_REPO,
    )


def _strip_hidden_lines(s: Any, path: str, marker: Path) -> None:
    """A checkout-local clean filter on ``path`` that drops every HIDDEN line from git's view.

    The filter program creates ``marker`` whenever git runs it.
    """
    program = f"touch {shlex.quote(str(marker))}; grep -v HIDDEN || true"
    _git(s, "config", "filter.strip.clean", program)
    _git(s, "config", "filter.strip.smudge", "cat")
    attributes = s.cwd / ".git" / "info" / "attributes"
    attributes.parent.mkdir(exist_ok=True)
    attributes.write_text(f"{path} filter=strip\n")


def _trust_stat(s: Any, path: str) -> int:
    """Make git trust ``path``'s cached stat; the mtime it caches, in nanoseconds.

    ``core.checkStat=minimal`` and ``core.trustctime=false`` leave whole-second
    mtime and size as git's only checks, and the index is refreshed after an
    mtime a minute in the past, so the entry is not racy. Every tracked file
    must equal its index entry when this runs.
    """
    _git(s, "config", "core.checkStat", "minimal")
    _git(s, "config", "core.trustctime", "false")
    target = s.cwd / path
    past = target.stat().st_mtime_ns - 60 * 1_000_000_000
    os.utime(target, ns=(past, past))
    _git(s, "update-index", "--refresh")
    return past


def _rewrite_keeping_stat(s: Any, path: str, old: bytes, new: bytes, mtime_ns: int) -> None:
    """Rewrite ``path`` with ``old`` replaced by ``new``, same size, and put its mtime back."""
    target = s.cwd / path
    data = target.read_bytes()
    changed = data.replace(old, new)
    assert changed != data and len(changed) == len(data)
    target.write_bytes(changed)
    os.utime(target, ns=(mtime_ns, mtime_ns))


def _unsupported_filter() -> type[Exception]:
    """The runner's refusal of a changed path under a filter attribute."""
    from curie_runner.workspace_snapshot import UnsupportedFilterError

    assert issubclass(UnsupportedFilterError, workspace_snapshot.WorkspaceSnapshotError)
    return UnsupportedFilterError


def test_a_clean_filter_added_after_approval_denies_the_publish_and_refuses_the_capture(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    _change_source(s, "fix")
    _reviewed_patch(s, base, _approve_diff(s))
    marker = tmp_path / "filter-ran"
    _strip_hidden_lines(s, SOURCE, marker)
    with (s.cwd / SOURCE).open("a") as handle:
        handle.write(HIDDEN_LINE)
    # The checkout's own git diff runs the filter and hides the line.
    assert "HIDDEN" not in _git(s, "diff", base)
    assert marker.exists(), "git diff did not run the configured filter"
    marker.unlink()

    decision = _publish(s)

    # The approved candidate can no longer be built: neither published nor reviewed.
    assert decision["permissionDecision"] == "deny", decision
    assert decision["permissionDecisionReason"].startswith(FRESH_REVIEW_PREFIX), decision
    with pytest.raises(_unsupported_filter(), match=FILTER_REFUSAL) as refused:
        _capture(s, base)
    assert SOURCE in str(refused.value)
    assert not marker.exists(), "the gate or the capture ran a filter program"


@pytest.mark.parametrize(
    "filtered", [SOURCE, TEST], ids=["edited_filtered_path", "control_unchanged_filtered_path"]
)
def test_a_diff_review_of_an_edited_path_under_a_filter_is_denied_naming_the_path(
    tmp_path: Path, filtered: str
) -> None:
    s, _ = _planned(tmp_path)
    marker = tmp_path / "filter-ran"
    _strip_hidden_lines(s, filtered, marker)
    _change_source(s, "fix")

    decision = _diff_review(s)

    assert not marker.exists(), "the gate ran a filter program"
    if filtered != SOURCE:
        # Control: a filter attribute on a path the candidate does not change refuses nothing.
        assert decision["permissionDecision"] == "allow", decision
        return
    assert decision["permissionDecision"] == "deny", decision
    reason = decision["permissionDecisionReason"]
    assert SOURCE in reason and FILTER_REFUSAL in reason, reason
    assert reason != UNVERIFIABLE
    assert ("review_diff", 1) not in s.phases()
    assert json.loads(_state_file(s).read_text())["diff"]["round"] == 0


@pytest.mark.parametrize("patch_file", ["deleted", "overwritten", "control_unchanged"])
def test_a_diff_verdict_on_a_candidate_patch_that_went_missing_or_changed_is_not_counted(
    tmp_path: Path, patch_file: str
) -> None:
    """The verdict counts only while the patch file still holds the admitted bytes.

    The gate records the patch's sha256 at admission and checks it when the
    verdict arrives. That catches a patch file deleted or left changed during
    the review. It does not defeat a process that replaces the file and puts the
    admitted bytes back before the verdict, nor one that forges the gate's own
    state: both run as the same user as the gate, inside its trust boundary.
    """
    s, _ = _planned(tmp_path)
    _change_source(s, "fix")
    pre, admitted = _admit_diff(s)
    path = Path(_block_line(admitted.prompt, "candidate patch"))
    assert path.read_bytes() == admitted.patch
    if patch_file == "deleted":
        path.unlink()
    elif patch_file == "overwritten":
        changed = admitted.patch.replace(b"# fix", b"# not the reviewed fix")
        assert changed != admitted.patch
        path.write_bytes(changed)

    context = _deliver_diff(s, pre, "APPROVE")

    if patch_file == "control_unchanged":
        assert "APPROVED" in context, context
        # Publication reads the candidate, not the patch file, after approval.
        path.unlink()
        assert _publish(s)["permissionDecision"] == "allow"
        return
    assert context.startswith(FRESH_REVIEW_PREFIX), context
    assert PATCH_CHANGED in context, context
    assert "APPROVED" not in context, context
    state = json.loads(_state_file(s).read_text())
    assert state["diff"]["verdict"] != "APPROVE"
    assert state["diff"]["round"] == 0
    assert _publish(s)["permissionDecision"] == "deny"
    # Fresh review, not a stop: the unchanged checkout is reviewed again and publishes.
    _approve_diff(s)
    assert _publish(s)["permissionDecision"] == "allow"


def test_a_stale_stat_rewrite_of_an_unedited_file_after_approval_needs_a_fresh_review(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    past = _trust_stat(s, TEST)
    _change_source(s, "fix")
    _approve_diff(s)
    _rewrite_keeping_stat(s, TEST, b"== 3", b"== 4", past)
    # git trusts the cached stat: git diff <base> does not list the rewritten file.
    assert _git(s, "diff", "--name-only", base) == f"{SOURCE}\n"

    decision = _publish(s)
    published = _capture(s, base)

    _assert_denied(decision, FRESH_REVIEW)
    # Final bytes win over the stat cache: the publication carries the rewrite.
    assert published.changed_paths == (SOURCE, TEST)
    assert b"+    assert add(1, 2) == 4\n" in _section(published.patch, TEST)
    # A fresh review reads the rewrite in its patch file, then publishes.
    reviewed = _reviewed_patch(s, base, _approve_diff(s))
    assert reviewed == published.patch
    assert _publish(s)["permissionDecision"] == "allow"


def test_a_stale_stat_rewrite_of_a_committed_file_after_approval_needs_a_fresh_review(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    _change_source(s, "fix")
    _commit(s, "Fix add")
    past = _trust_stat(s, SOURCE)
    _approve_diff(s)
    _rewrite_keeping_stat(s, SOURCE, b"a + b", b"a - b", past)
    # git diff HEAD misses the rewrite.
    assert _git(s, "diff", "HEAD") == ""

    decision = _publish(s)
    published = _capture(s, base)

    _assert_denied(decision, FRESH_REVIEW)
    # The deny is needed: the publication carries the rewrite.
    assert published.changed_paths == (SOURCE,)
    assert b"+    return a - b  # fix\n" in published.patch
    # A fresh review reads the rewrite in its patch file, then publishes.
    reviewed = _reviewed_patch(s, base, _approve_diff(s))
    assert reviewed == published.patch
    assert _publish(s)["permissionDecision"] == "allow"


def test_external_diff_and_textconv_drivers_change_neither_the_reviewed_nor_the_published_patch(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    _change_source(s, "fix")
    plain = _capture(s, base).patch
    external = tmp_path / "acme-external-diff"
    external.write_text("#!/bin/sh\necho ACME EXTERNAL DIFF\n")
    external.chmod(0o755)
    textconv = tmp_path / "acme-textconv"
    textconv.write_text('#!/bin/sh\necho ACME TEXTCONV\ncat "$1"\n')
    textconv.chmod(0o755)
    _git(s, "config", "diff.external", str(external))
    _git(s, "config", "diff.acme.textconv", str(textconv))
    attributes = s.cwd / ".git" / "info" / "attributes"
    attributes.parent.mkdir(exist_ok=True)
    attributes.write_text(f"{SOURCE} diff=acme\n")
    # The checkout's own git diff now runs both drivers.
    assert "ACME EXTERNAL DIFF" in _git(s, "diff")
    assert "ACME TEXTCONV" in _git(s, "diff", "--no-ext-diff")

    reviewed = _reviewed_patch(s, base, _approve_diff(s))
    published = _capture(s, base).patch

    assert reviewed == published == plain
    assert b"ACME" not in published
    assert b"+    return a + b  # fix\n" in published


def test_the_reviewed_patch_file_is_byte_identical_to_the_runner_capture(tmp_path: Path) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    # Committed, staged, deleted, mode, binary and untracked changes together.
    _change_source(s, "committed")
    _commit(s, "Commit the source change")
    (s.cwd / "pkg" / "staged.py").write_text("STAGED = True\n")
    _git(s, "add", "pkg/staged.py")
    (s.cwd / TEST).unlink()
    (s.cwd / ".gitignore").chmod(0o755)
    (s.cwd / "asset.bin").write_bytes(b"\x00acme asset\xff")
    (s.cwd / "notes.md").write_text("untracked note\n")

    reviewed = _reviewed_patch(s, base, _approve_diff(s))
    published = _capture(s, base)

    assert published.base_sha == base
    assert published.patch == reviewed
    assert set(published.changed_paths) == {
        ".gitignore",
        "asset.bin",
        "notes.md",
        "pkg/staged.py",
        SOURCE,
        TEST,
    }


# --- End to end: the reviewed patch is what the worker validates and publishes -------

TOKEN = "runner-token-value"
THREAD = "1700000000.000100"


def _runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="dark-factory-review-candidate",
    )


def _snapshot_over_http(workspace: Path, base_sha: str) -> Any:
    """Ask the real runner route for a snapshot through the real worker client."""

    async def go() -> Any:
        runner = _runner()
        await runner.start()
        app = create_app(
            runner,
            token=TOKEN,
            snapshotter=workspace_snapshot.workspace_snapshotter(
                workspace,
                trusted_origin=GITHUB,
                repository_path=CAPTURE_REPO,
                proposal=lambda: ("Fix add", "Publish the reviewed candidate."),
            ),
        )
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            return await client.snapshot(
                f"http://127.0.0.1:{server.port}", token=TOKEN, base_sha=base_sha
            )
        finally:
            await client.close()
            await server.close()

    return asyncio.run(go())


def _coordinator(base_sha: str, archive: bytes) -> SimpleNamespace:
    """The retained facts the worker holds for this thread: P and P's archive."""
    return SimpleNamespace(
        preparer=SimpleNamespace(limits=SimpleNamespace(max_archive_bytes=len(archive) + 1)),
        current=lambda _thread: SimpleNamespace(repo_full_name=CAPTURE_REPO, base_sha=base_sha),
        stream_current_base=lambda _thread: [archive],
    )


def _published_tree(root: Path, archive: bytes, patch: bytes) -> tuple[str, Path]:
    """The published result: P's archive with the patch applied, as a tree id."""
    root.mkdir(parents=True)
    checkout = root / "checkout"
    checkout.mkdir()
    with tarfile.open(fileobj=BytesIO(archive), mode="r:gz") as source:
        source.extractall(checkout, filter="data")
    env = publication_git_environment(root)

    def git(*args: str) -> bytes:
        return subprocess.run(
            ["git", *args], cwd=checkout, env=env, check=True, capture_output=True
        ).stdout

    git("init", "--quiet")
    git("add", "-A")
    patch_file = root / "candidate.patch"
    patch_file.write_bytes(patch)
    git("apply", "--binary", "--index", str(patch_file))
    return git("write-tree").decode("ascii").strip(), checkout


def test_a_reviewed_candidate_publishes_through_the_runner_route_and_worker_validation(
    tmp_path: Path,
) -> None:
    s, base = _planned(tmp_path)
    _publishable(s)
    archive = _git_bytes(s, "archive", "--format=tar.gz", base)
    _change_source(s, "fix")
    _commit(s, "Fix add")
    (s.cwd / TEST).write_text(
        "from pkg.calc import add\n\n\ndef test_add():\n    assert add(2, 3) == 5\n"
    )
    _git(s, "add", TEST)
    (s.cwd / "docs").mkdir()
    (s.cwd / "docs" / "notes.md").write_text("untracked note\n")
    approved = _approve_diff(s)
    reviewed = _reviewed_patch(s, base, approved)
    assert _publish(s)["permissionDecision"] == "allow"

    snapshot = _snapshot_over_http(s.cwd, base)

    assert snapshot.base_sha == base
    assert snapshot.patch == reviewed
    scratch = tmp_path / "validation"
    scratch.mkdir()
    validate_snapshot_against_base(
        _coordinator(base, archive),
        thread_key=THREAD,
        snapshot=snapshot,
        scratch_root=scratch,
        protected_paths=(),
        allow_dependency_additions=False,
    )
    tree, checkout = _published_tree(tmp_path / "result", archive, snapshot.patch)
    # The published tree is the candidate tree the reviewer was told about.
    assert tree == _block_line(approved.prompt, "candidate tree")
    for path in (SOURCE, TEST, "docs/notes.md"):
        assert (checkout / path).read_bytes() == (s.cwd / path).read_bytes(), path
