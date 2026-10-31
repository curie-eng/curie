"""The review gate binds review and publication to the run's candidate (#5230).

The candidate is the net change from the base commit, the checkout's HEAD when
the run started, to the final working tree files. Every case drives
``examples/dark-factory/hooks/review_gate.py`` as the real hook subprocess, one
process per event, against a real git checkout, through the shared fixture in
``test_dark_factory_review_gate.py``. A local commit must not redefine the base,
hide a change from the diff reviewer or the ``.github`` stop, or keep a stale
approval alive, and committing locally must not by itself be refused. An index
flag (skip-worktree, assume-unchanged) must not hide an edit from the approval or
from the ``.github`` stop either, and the diff reviewer is told which candidate
paths git diff does not show.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


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
hidden_paths_line = gate.hidden_paths_line

DIFF_INPUT = {"subagent_type": DIFF, "description": "diff-reviewer", "prompt": "p"}
FRESH_REVIEW = "Fresh diff review required: checkout changed or is unverifiable."
UNVERIFIABLE = "Diff checkout is unverifiable; restore it before review."


def _git(s: Any, *args: str) -> str:
    """One git command in the session's checkout, with a placeholder identity; its stdout."""
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
        text=True,
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


def _approve_diff(s: Any) -> str:
    """One admitted diff review that approves; the prompt the reviewer was sent."""
    pre, context = s.review(DIFF, reply(DIFF, "APPROVE"))
    assert "APPROVED" in context, context
    return str(pre["updatedInput"]["prompt"])


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


def _flag(s: Any, flag: str, *paths: str) -> None:
    """Mark index entries ``assume-unchanged`` or ``skip-worktree``: git stops reading them."""
    _git(s, "update-index", f"--{flag}", "--", *paths)
    expected = "S" if flag == "skip-worktree" else "h"
    for path in paths:
        tag = _git(s, "ls-files", "-v", "--", path)[:1]
        assert tag == expected, (path, tag)


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


# --- A commit after approval invalidates it; a commit before review does not -------


@pytest.mark.parametrize("committed", [True, False], ids=["commit_after_approval", "control"])
def test_a_commit_after_diff_approval_requires_a_fresh_review(
    tmp_path: Path, committed: bool
) -> None:
    s, _ = _planned(tmp_path)
    _change_source(s, "fix")
    _approve_diff(s)

    if committed:
        _commit(s, "Fix add")
        # The committed change no longer shows in git diff HEAD.
        assert _clean(s)

    decision = _publish(s)

    if not committed:
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

    _approve_diff(s)

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
    _approve_diff(s)

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


# --- The diff reviewer's prompt names the base recorded when the run started --------


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
    pre, context = s.review(DIFF, reply(DIFF, "CHANGES"))
    assert "CHANGES" in context
    first = pre["updatedInput"]["prompt"]
    _change_source(s, "second try")
    _commit(s, "Second try")
    second_head = _head(s)
    second = _approve_diff(s)

    # Agent commits never redefine the base: both rounds name the run's first HEAD.
    for prompt in (first, second):
        assert prompt.startswith("p\n\n"), prompt[:40]
        assert prompt.endswith(contract() + candidate_block(base)), prompt[-600:]
        assert prompt.count("CANDIDATE:") == 1
        assert first_head not in prompt and second_head not in prompt
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
    assert prompt.endswith(contract() + candidate_block(base)), prompt[-600:]
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

    assert _diff_review(s)["permissionDecision"] == "allow"


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


def test_a_prepared_escaping_link_that_stays_absent_keeps_the_approval(tmp_path: Path) -> None:
    # Liveness control for the flag refusals above: the skip-worktree entry
    # preparation leaves behind is hashed as absent, consistently.
    s = _with_dropped_escaping_link(tmp_path)
    base = _plan_run(s)
    _change_source(s, "fix")

    prompt = _approve_diff(s)

    # Admitted, and nothing hidden is on disk: the block is exactly the plain one.
    assert prompt.endswith(contract() + candidate_block(base)), prompt[-600:]
    assert "git diff does not show them" not in prompt
    assert not (s.cwd / "out").exists() and not (s.cwd / "out").is_symlink()
    assert _publish(s)["permissionDecision"] == "allow"


# --- The diff reviewer is told which candidate paths git diff hides ------------------


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree"])
@pytest.mark.parametrize("on_disk", [True, False], ids=["on_disk", "control_absent"])
def test_candidate_paths_git_diff_hides_are_named_after_the_candidate_block(
    tmp_path: Path, flag: str, on_disk: bool
) -> None:
    s, base = _planned(tmp_path)
    # A base file leaves the index and is ignored; a base file is flagged.
    _git(s, "rm", "-q", "--cached", SOURCE)
    gitignore = s.cwd / ".gitignore"
    gitignore.write_text(gitignore.read_text() + f"{SOURCE}\n")
    _flag(s, flag, TEST)
    if on_disk:
        (s.cwd / TEST).write_text("def test_add():\n    assert True\n")
    else:
        (s.cwd / SOURCE).unlink()
        (s.cwd / TEST).unlink()

    prompt = _approve_diff(s)

    expected = contract() + candidate_block(base)
    if on_disk:
        expected += hidden_paths_line([TEST, SOURCE])
    else:
        # Control: an absent path is a deletion git diff shows, so the block is unchanged.
        assert "git diff does not show them" not in prompt
    assert prompt.endswith(expected), prompt[-800:]
    assert prompt.count("CANDIDATE:") == 1


def test_the_hidden_paths_line_names_fifty_paths_then_counts_the_rest(tmp_path: Path) -> None:
    s = Session(tmp_path)
    notes = [f"docs/note-{n:02d}.txt" for n in range(53)]
    for note in notes:
        target = s.cwd / note
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{note}\n")
    _commit(s, "Notes fixture")
    base = _plan_run(s)
    _flag(s, "assume-unchanged", *notes)

    prompt = _approve_diff(s)

    assert prompt.endswith(contract() + candidate_block(base) + hidden_paths_line(notes))
    assert prompt.endswith(", docs/note-49.txt and 3 more"), prompt[-200:]
    assert "docs/note-50.txt" not in prompt


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
    # Control: outside the root .github the diff review is admitted, naming the path.
    assert out is not None
    decision = out["hookSpecificOutput"]
    assert decision["permissionDecision"] == "allow", decision
    prompt = decision["updatedInput"]["prompt"]
    assert prompt.endswith(contract() + candidate_block(base) + hidden_paths_line([path]))
    assert not [e for e in s.gate_events() if e["curie_gate"] == "github_path_refused"]
