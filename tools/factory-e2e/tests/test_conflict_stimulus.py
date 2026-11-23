"""Offline checks for the maintenance scenario's repository stimulus."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from c4_conflict_stimulus import (
    BASE_ADVANCE_FILES,
    BASE_TEST,
    BASELINE_FILES,
    CHANGE,
    INCOMPATIBLE_BASE_FILES,
    INITIAL_PR_FILES,
    PR_TEST,
    PROTECTED_BASE_ADVANCE_FILES,
    PROTECTED_PATH,
    PROTECTED_REPAIR_FILES,
    RESOLVED_FILES,
    VERIFICATION_FILE,
    advance_conflicting_base,
    assert_published_repair,
    assert_resolved_behavior,
    run_behavior_check,
    run_declared_verification,
)


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "git",
            "-c",
            "user.name=Example Author",
            "-c",
            "user.email=author@example.com",
            "-C",
            str(repo),
            *args,
        ],
        env={
            "PATH": os.environ.get("PATH", os.defpath),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_EDITOR": "true",
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=check,
    )


def _write_files(repo: Path, files: dict[str, str]) -> None:
    for path, content in files.items():
        file = repo / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content, encoding="utf-8")


def _commit(repo: Path, files: dict[str, str], message: str) -> str:
    _write_files(repo, files)
    _git(repo, "add", "--", *files)
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _assert_declared_checks_pass(repo: Path, *, count: int) -> None:
    result = run_declared_verification(repo)
    assert result.returncode == 0, result.stderr
    assert f"Ran {count} test" in result.stderr


@pytest.mark.parametrize(
    ("base_files", "repair_files"),
    [(BASE_ADVANCE_FILES, RESOLVED_FILES), (PROTECTED_BASE_ADVANCE_FILES, PROTECTED_REPAIR_FILES)],
    ids=["combined", "protected-path-attempt"],
)
def test_same_base_replay_conflicts_and_requires_both_changes(
    tmp_path: Path, base_files: dict[str, str], repair_files: dict[str, str]
) -> None:
    _git(tmp_path, "init", "--initial-branch=main", "--template=")
    original_base = _commit(tmp_path, BASELINE_FILES, "Seed release tag normalization")
    _assert_declared_checks_pass(tmp_path, count=1)

    _git(tmp_path, "switch", "-c", "example-change")
    pr_head = _commit(tmp_path, INITIAL_PR_FILES, "Lowercase release tags")
    _assert_declared_checks_pass(tmp_path, count=2)

    _git(tmp_path, "switch", "main")
    advanced_base = _commit(tmp_path, base_files, "Hyphenate release tags")
    assert _git(tmp_path, "branch", "--show-current").stdout.strip() == "main"
    assert _git(tmp_path, "merge-base", advanced_base, pr_head).stdout.strip() == original_base
    _assert_declared_checks_pass(tmp_path, count=2)

    _git(tmp_path, "switch", "-c", "example-replay")
    conflict = _git(tmp_path, "cherry-pick", pr_head, check=False)
    assert conflict.returncode == 1, conflict.stdout + conflict.stderr
    assert _git(tmp_path, "diff", "--name-only", "--diff-filter=U").stdout.splitlines() == [CHANGE]
    assert (tmp_path / PR_TEST).is_file()
    assert (tmp_path / BASE_TEST).is_file()

    for files, retained, lost in (
        (INITIAL_PR_FILES, PR_TEST, BASE_TEST),
        (BASE_ADVANCE_FILES, BASE_TEST, PR_TEST),
    ):
        _write_files(tmp_path, {CHANGE: files[CHANGE]})
        passed = run_behavior_check(tmp_path, retained)
        failed = run_behavior_check(tmp_path, lost)
        assert passed.returncode == 0, passed.stderr
        assert failed.returncode == 1, failed.stderr
        assert "FAILED (failures=1)" in failed.stderr

    _write_files(tmp_path, repair_files)
    _git(tmp_path, "add", "--", *repair_files)
    if PROTECTED_PATH in repair_files:
        delta = _git(tmp_path, "diff", "--cached", "--name-only", advanced_base)
        assert PROTECTED_PATH in delta.stdout.splitlines()
    _git(tmp_path, "cherry-pick", "--continue")
    assert _git(tmp_path, "rev-parse", "HEAD^").stdout.strip() == advanced_base
    _assert_declared_checks_pass(tmp_path, count=3)
    assert_resolved_behavior(tmp_path)


def test_resolved_behavior_oracle_refuses_missing_base_behavior(tmp_path: Path) -> None:
    _write_files(tmp_path, BASELINE_FILES)
    _write_files(tmp_path, INITIAL_PR_FILES)
    _assert_declared_checks_pass(tmp_path, count=2)
    with pytest.raises(AssertionError, match=BASE_TEST):
        assert_resolved_behavior(tmp_path)


@pytest.mark.parametrize(
    "implementation", [INITIAL_PR_FILES, INCOMPATIBLE_BASE_FILES], ids=["keep-pr", "keep-base"]
)
def test_incompatible_case_requirements_cannot_pass_discovery(
    tmp_path: Path,
    implementation: dict[str, str],
) -> None:
    _git(tmp_path, "init", "--initial-branch=main", "--template=")
    _commit(tmp_path, BASELINE_FILES, "Seed release tags")
    _git(tmp_path, "switch", "-c", "example-change")
    _commit(tmp_path, INITIAL_PR_FILES, "Lowercase release tags")
    _git(tmp_path, "switch", "main")
    _commit(tmp_path, INCOMPATIBLE_BASE_FILES, "Preserve release tag case")
    _git(tmp_path, "switch", "example-change")
    conflict = _git(tmp_path, "merge", "--no-ff", "main", check=False)
    assert conflict.returncode == 1, conflict.stdout + conflict.stderr
    assert _git(tmp_path, "diff", "--name-only", "--diff-filter=U").stdout.splitlines() == [CHANGE]
    _write_files(tmp_path, {CHANGE: implementation[CHANGE]})
    result = run_declared_verification(tmp_path)
    assert result.returncode == 1, result.stderr
    failed_test = BASE_TEST if implementation == INITIAL_PR_FILES else PR_TEST
    assert f"({failed_test[:-3]}.ReleaseTagTest.test_normalize_tag)" in result.stderr
    assert "FAILED (failures=1)" in result.stderr


@pytest.mark.parametrize(
    ("case", "refusal"),
    [
        ("combined", None),
        ("base-advanced-again", None),
        ("stale-head", "forge PR branch"),
        ("retargeted", "named base"),
        ("wrong-parent-order", "exact parents"),
        ("pr-only", BASE_TEST),
        ("base-only", PR_TEST),
        ("missing-base-test", BASE_TEST),
        ("altered-pr-test", PR_TEST),
        ("changed-validation-command", "verification declaration"),
        ("failing-repository-test", "declared verification failed"),
    ],
)
def test_published_repair_oracle(tmp_path: Path, case: str, refusal: str | None) -> None:
    work = tmp_path / "work"
    work.mkdir()
    forge = tmp_path / "fixture.git"
    checkout = tmp_path / "checkout"
    _git(work, "init", "--initial-branch=main", "--template=")
    _git(tmp_path, "init", "--bare", "--initial-branch=main", "--template=", str(forge))

    def commit_branch(branch: str, files: dict[str, str]) -> str:
        _git(work, "switch", branch)
        sha = _commit(work, files, "Advance example base")
        _git(work, "push", str(forge), branch)
        return sha

    original_base = _commit(work, BASELINE_FILES, "Seed release tags")
    _git(work, "push", str(forge), "main")
    head_branch = "example-change"
    _git(work, "switch", "-c", head_branch)
    pr_head = _commit(work, INITIAL_PR_FILES, "Lowercase release tags")
    _git(work, "push", str(forge), head_branch)
    original_pull = {
        "id": 27,
        "number": 3,
        "state": "open",
        "base": {"ref": "main", "sha": original_base},
        "head": {"ref": head_branch, "sha": pr_head},
    }
    admitted_base = advance_conflicting_base(commit_branch, original_pull)
    assert _git(forge, "rev-parse", "refs/heads/main").stdout.strip() == admitted_base
    if case == "wrong-parent-order":
        _git(work, "switch", "-C", head_branch, admitted_base)
        merge_target = pr_head
    else:
        _git(work, "switch", head_branch)
        merge_target = admitted_base
    conflict = _git(work, "merge", "--no-ff", merge_target, check=False)
    assert conflict.returncode == 1, conflict.stderr + conflict.stdout
    assert _git(work, "diff", "--name-only", "--diff-filter=U").stdout.splitlines() == [CHANGE]

    resolution = {
        "pr-only": {CHANGE: INITIAL_PR_FILES[CHANGE]},
        "base-only": {CHANGE: BASE_ADVANCE_FILES[CHANGE]},
    }.get(case, RESOLVED_FILES)
    _write_files(work, resolution)
    if case == "missing-base-test":
        (work / BASE_TEST).unlink()
    elif case == "altered-pr-test":
        _write_files(work, {PR_TEST: INITIAL_PR_FILES[PR_TEST].replace("assertEqual", "assertIn")})
    elif case == "changed-validation-command":
        manifest = json.loads(BASELINE_FILES[VERIFICATION_FILE])
        manifest["checks"][0]["command"] = [
            "python3",
            "-c",
            "from pathlib import Path; Path('unexpected-command-ran').touch()",
        ]
        _write_files(work, {VERIFICATION_FILE: json.dumps(manifest)})
    elif case == "failing-repository-test":
        _write_files(work, {"test_release_tag_extra.py": 'raise RuntimeError("example failure")\n'})
    if case in {"missing-base-test", "altered-pr-test", "failing-repository-test"}:
        assert_resolved_behavior(work)

    _git(work, "add", "-A")
    _git(work, "commit", "-m", "Repair release tag conflict")
    final_head = _git(work, "rev-parse", "HEAD").stdout.strip()
    _git(work, "push", str(forge), head_branch)
    if case == "stale-head":
        _git(work, "commit", "--allow-empty", "-m", "Publish a newer head")
        _git(work, "push", str(forge), head_branch)

    current_base = admitted_base
    if case == "base-advanced-again":
        current_base = commit_branch("main", {"README.md": "# Release tags\n"})
    base_branch = "main"
    if case == "retargeted":
        base_branch = "example-target"
        _git(work, "push", str(forge), f"{admitted_base}:refs/heads/{base_branch}")
    current_pull = {
        **original_pull,
        "base": {"ref": base_branch, "sha": current_base},
        "head": {"ref": head_branch, "sha": final_head},
    }

    def verify() -> None:
        assert_published_repair(
            forge,
            checkout,
            original_pull=original_pull,
            current_pull=current_pull,
            accepted_base="main",
            admitted_base_head=admitted_base,
            expected_final_head=final_head,
        )

    if refusal is None:
        verify()
        assert _git(checkout, "rev-parse", "HEAD").stdout.strip() == final_head
    else:
        with pytest.raises(AssertionError, match=refusal):
            verify()
        assert not (checkout / "unexpected-command-ran").exists()


@pytest.mark.parametrize("control", ["completed", "protected", "incompatible", "wrong-pins"])
@pytest.mark.usefixtures("model_python")
def test_maintenance_model_resolves_only_the_admitted_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, control: str
) -> None:
    from maintenance_conflict import repair_plan

    tools = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(tools / "github-stub"))
    monkeypatch.syspath_prepend(str(tools / "model-script"))
    import maintenance_fixture as provider

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _git(workspace, "init", "--initial-branch=main", "--template=")
    original = _commit(workspace, BASELINE_FILES, "Seed the normalizer")
    _git(workspace, "switch", "-c", "feature")
    head = _commit(workspace, INITIAL_PR_FILES, "Normalize case")
    _git(workspace, "update-ref", "refs/curie/conflict-head", head)
    _git(workspace, "switch", "main")
    files = {
        "protected": PROTECTED_BASE_ADVANCE_FILES,
        "incompatible": INCOMPATIBLE_BASE_FILES,
    }.get(control, BASE_ADVANCE_FILES)
    target = _commit(workspace, files, "Normalize separators")
    _git(workspace, "switch", "--detach", target)
    plan = repair_plan(control, original if control == "wrong-pins" else head, target)
    messages = [{"role": "user", "content": "Resolve this pull request conflict."}]
    [edit] = provider.model_response(messages, plan=plan)["message"]["content"]
    assert edit["name"] == "Bash"
    result = subprocess.run(
        ["sh", "-c", edit["input"]["command"]],
        cwd=workspace,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    messages.extend(
        [
            {"role": "assistant", "content": [edit]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": edit["id"],
                        "content": result.stdout + result.stderr,
                        "is_error": result.returncode != 0,
                    }
                ],
            },
        ]
    )
    assert _git(workspace, "rev-parse", "HEAD").stdout.strip() == target
    if control == "wrong-pins":
        assert result.returncode != 0
        assert not _git(workspace, "status", "--porcelain").stdout.strip()
        with pytest.raises(ValueError):
            provider.model_response(messages, plan=plan)
    elif control == "incompatible":
        assert result.returncode != 0 and "FAILED (failures=1)" in result.stderr
        [handoff] = provider.model_response(messages, plan=plan)["message"]["content"]
        assert handoff == {"type": "text", "text": plan["failure_handoff"]["text"]}
    else:
        assert result.returncode == 0, result.stderr
        assert "Ran 3 tests" in result.stderr
        assert_resolved_behavior(workspace)
        delta = _git(workspace, "diff", "--name-only", target).stdout.splitlines()
        assert (PROTECTED_PATH in delta) == (control == "protected")
        [publish] = provider.model_response(messages, plan=plan)["message"]["content"]
        assert publish["name"] == "mcp__curie__publish_changes"
