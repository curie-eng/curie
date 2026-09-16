"""Executable contract for labeling issues fixed on next (issue #2250).

GitHub closing keywords fire only on the default branch. This helper is what
the push-to-next workflow runs: parse merged PR bodies, apply `fixed-on-next`,
and close those issues when `next` merges to `main`. Tests drive the script as
a subprocess with a fake `gh`, matching tools/fix-pin-ci.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
HELPER = REPO_ROOT / "tools" / "close-on-next" / "close.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "close-on-next.yaml"
REPO = "curie-eng/curie"
LABEL = "fixed-on-next"


def _helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("curie_close_on_next", HELPER)
    assert spec is not None and spec.loader is not None, f"cannot load {HELPER}"
    module = importlib.util.module_from_spec(spec)
    # Dataclasses on 3.14 look up the module in sys.modules while the class
    # body runs; spec_from_file_location does not register it on its own.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _parse(body: str, repository: str = REPO) -> tuple[int, ...]:
    return _helper().parse_closing_issues(body, repository)


def _write_event(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / "event.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_fake_gh(tmp_path: Path, fixture: Mapping[str, Any]) -> tuple[Path, Path]:
    """Install a fake ``gh`` that logs argv and serves scripted list/view JSON."""
    gh_bin = tmp_path / "gh-bin"
    gh_bin.mkdir()
    log_path = tmp_path / "gh-argv.json"
    fixture_path = tmp_path / "gh-fixture.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
    fake = gh_bin / "gh"
    fake.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import json, os, sys",
                "from pathlib import Path",
                f"log = Path({str(log_path)!r})",
                f"fixture = json.loads(Path({str(fixture_path)!r}).read_text())",
                "calls = json.loads(log.read_text()) if log.exists() else []",
                "calls.append(sys.argv[1:])",
                "log.write_text(json.dumps(calls))",
                "argv = sys.argv[1:]",
                "joined = ' '.join(argv)",
                "if os.environ.get('CLOSE_ON_NEXT_GH_EXIT', '0') != '0':",
                "    sys.stderr.write(os.environ.get('CLOSE_ON_NEXT_GH_ERR', 'gh failed'))",
                "    raise SystemExit(int(os.environ['CLOSE_ON_NEXT_GH_EXIT']))",
                "if argv[:1] == ['pr'] and 'list' in argv:",
                "    print(json.dumps(fixture.get('prs', [])))",
                "    raise SystemExit(0)",
                "if argv[:1] == ['issue'] and 'list' in argv:",
                "    if '--label' in argv:",
                "        print(json.dumps(fixture.get('labeled', [])))",
                "    else:",
                "        print(json.dumps(fixture.get('open_issues', [])))",
                "    raise SystemExit(0)",
                "if 'commits' in joined and joined.rstrip('/').endswith('pulls'):",
                "    print(json.dumps(fixture.get('associated', [])))",
                "    raise SystemExit(0)",
                "mutating = (",
                "    '--method' in argv or '-X' in argv",
                "    or (argv[:1] == ['label'] and 'create' in argv)",
                "    or 'issue' in argv and ("
                "'comment' in argv or 'edit' in argv or 'close' in argv)",
                ")",
                "if mutating:",
                "    print('{}')",
                "    raise SystemExit(0)",
                "sys.stderr.write('unexpected gh invocation: ' + joined)",
                "raise SystemExit(2)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    fake.chmod(0o755)
    return gh_bin, log_path


def _run(
    tmp_path: Path,
    *,
    event: dict[str, Any] | None = None,
    extra: list[str] | None = None,
    fixture: Mapping[str, Any] | None = None,
    gh_on_path: bool = True,
    dry_run: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    args = [sys.executable, str(HELPER), "--repo", REPO]
    if event is not None:
        args.extend(["--event", str(_write_event(tmp_path, event))])
    if dry_run:
        args.append("--dry-run")
    if extra:
        args.extend(extra)
    environment = {**os.environ, "GITHUB_REPOSITORY": REPO}
    log_path = tmp_path / "gh-argv.json"
    if gh_on_path:
        gh_bin, log_path = _write_fake_gh(tmp_path, fixture or {})
        environment["PATH"] = f"{gh_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    else:
        environment["PATH"] = ""
    completed = subprocess.run(
        args,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    return completed, log_path


def _calls(log_path: Path) -> list[list[str]]:
    if not log_path.exists():
        return []
    return json.loads(log_path.read_text(encoding="utf-8"))


def _mutating_calls(log_path: Path) -> list[list[str]]:
    mutating: list[list[str]] = []
    for argv in _calls(log_path):
        joined = " ".join(argv)
        if (
            "--method" in argv
            or "-X" in argv
            or argv[:1] == ["label"]
            or "comment" in argv
            or "close" in argv
            or (argv[:1] == ["issue"] and "edit" in argv)
        ):
            mutating.append(argv)
        elif "unexpected" in joined:
            mutating.append(argv)
    return mutating


# GitHub merge messages are `from <owner>/<branch>`. Naming this org as the
# owner makes a branch look like a downstream repository slug to gitleaks.
NEXT_HEAD_MESSAGE = "Merge pull request #2217 from example/task/example"
NEXT_MERGE_MESSAGE = "Merge pull request #99 from example/next"
HOTFIX_MERGE_MESSAGE = "Merge pull request #50 from example/task/hotfix"

NEXT_EVENT = {
    "ref": "refs/heads/next",
    "before": "a" * 40,
    "after": "b" * 40,
    "head_commit": {"message": NEXT_HEAD_MESSAGE},
    "repository": {"full_name": REPO},
}

MAIN_NEXT_MERGE_EVENT = {
    "ref": "refs/heads/main",
    "before": "c" * 40,
    "after": "d" * 40,
    "head_commit": {"message": NEXT_MERGE_MESSAGE},
    "repository": {"full_name": REPO},
}

MAIN_HOTFIX_EVENT = {
    "ref": "refs/heads/main",
    "before": "c" * 40,
    "after": "d" * 40,
    "head_commit": {"message": HOTFIX_MERGE_MESSAGE},
    "repository": {"full_name": REPO},
}


# --- parser -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("Fixes #2201\n", (2201,)),
        ("Closes #2123.\n", (2123,)),
        ("Closes #2173\n", (2173,)),
        ("Closes: #12\n", (12,)),
        ("Resolved #12\n", (12,)),
        ("This mentions #12 without a keyword.\n", ()),
        ("Closes #12, #13\n", (12,)),
        ("Closes #12 and #13\n", (12,)),
        ("Closes #12, closes #13\n", (12, 13)),
        ("Fixes #12\nFixes #13\n", (12, 13)),
        ("<!-- Closes #12 -->\n", ()),
        ("Closes another-owner" + "/some-repo#12\n", ()),
        (f"Closes https://github.com/{REPO}/issues/12\n", (12,)),
        ("Closes https://github.com/other/other/issues/12\n", ()),
        ("", ()),
    ],
)
def test_parse_closing_issues_matches_github_same_repo_semantics(
    body: str, expected: tuple[int, ...]
) -> None:
    assert _parse(body) == expected


def test_parse_strips_html_comments_before_scanning() -> None:
    body = "Summary\n\n<!-- Closes #99 -->\n\nCloses #12\n"
    assert _parse(body) == (12,)


# --- planner ----------------------------------------------------------------


def test_plan_next_labels_open_unlabeled_issues_and_skips_the_rest() -> None:
    module = _helper()
    prs = [
        module.PullRequest(
            number=2217,
            body="Fixes #2201\n",
            url=f"https://github.com/{REPO}/pull/2217",
            merged=True,
            base="next",
            head="task/example",
        ),
        module.PullRequest(
            number=9,
            body="Closes #12, closes #13\n",
            url=f"https://github.com/{REPO}/pull/9",
            merged=True,
            base="next",
            head="task/other",
        ),
    ]
    open_issues = {
        2201: module.Issue(number=2201, state="open", labels=()),
        12: module.Issue(number=12, state="open", labels=(LABEL,)),
        # 13 is closed / missing from the open map
    }
    actions = module.plan_next_actions(prs, open_issues, repository=REPO)
    kinds = [(action.kind, action.issue) for action in actions]
    assert ("ensure_label", None) in kinds
    assert ("add_label", 2201) in kinds
    assert ("comment", 2201) in kinds
    assert ("add_label", 12) not in kinds
    assert ("add_label", 13) not in kinds
    comment = next(action for action in actions if action.kind == "comment")
    assert "#2217" in (comment.body or "")
    assert "next" in (comment.body or "")


def test_plan_next_is_empty_when_nothing_is_open_and_unlabeled() -> None:
    module = _helper()
    prs = [
        module.PullRequest(
            number=1,
            body="Closes #12\n",
            url=f"https://github.com/{REPO}/pull/1",
            merged=True,
            base="next",
            head="task/x",
        )
    ]
    actions = module.plan_next_actions(prs, {}, repository=REPO)
    assert actions == []


def test_plan_main_closes_labeled_issues_only_on_next_merge() -> None:
    module = _helper()
    labeled = [module.Issue(number=12, state="open", labels=(LABEL,))]
    close_actions = module.plan_main_actions(should_close=True, labeled_open=labeled)
    assert [(action.kind, action.issue) for action in close_actions] == [
        ("comment", 12),
        ("close", 12),
    ]
    assert module.plan_main_actions(should_close=False, labeled_open=labeled) == []


@pytest.mark.parametrize(
    ("ref", "event_name", "message", "associated", "after_has", "before_has", "expected"),
    [
        (
            "refs/heads/main",
            "push",
            NEXT_MERGE_MESSAGE,
            False,
            None,
            None,
            True,
        ),
        ("refs/heads/main", "push", "Merge branch 'next'", False, None, None, True),
        (
            "refs/heads/main",
            "push",
            HOTFIX_MERGE_MESSAGE,
            False,
            None,
            None,
            False,
        ),
        ("refs/heads/main", "push", "hotfix", True, None, None, True),
        ("refs/heads/main", "push", "hotfix", False, True, False, True),
        ("refs/heads/main", "push", "hotfix", False, True, True, False),
        ("refs/heads/main", "workflow_dispatch", "n/a", False, True, True, True),
        (
            "refs/heads/next",
            "push",
            NEXT_MERGE_MESSAGE,
            False,
            None,
            None,
            False,
        ),
    ],
)
def test_should_close_labeled_issues_detects_next_to_main_merge(
    ref: str,
    event_name: str,
    message: str,
    associated: bool,
    after_has: bool | None,
    before_has: bool | None,
    expected: bool,
) -> None:
    assert (
        _helper().should_close_labeled_issues(
            ref=ref,
            event_name=event_name,
            head_message=message,
            associated_is_next_merge=associated,
            next_is_ancestor_of_after=after_has,
            next_is_ancestor_of_before=before_has,
        )
        is expected
    )


# --- CLI / gh ---------------------------------------------------------------


def test_self_test_passes_without_gh() -> None:
    completed = subprocess.run(
        [sys.executable, str(HELPER), "--self-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PATH": ""},
    )
    assert completed.returncode == 0, completed.stderr
    assert "self-test passed" in completed.stdout


def test_dry_run_on_next_prints_label_plan_and_does_not_mutate(tmp_path: Path) -> None:
    fixture = {
        "prs": [
            {
                "number": 2217,
                "url": f"https://github.com/{REPO}/pull/2217",
                "body": "Fixes #2201\n",
                "title": "fix mcp",
                "mergedAt": "2026-09-02T16:52:01Z",
                "headRefName": "task/example",
                "baseRefName": "next",
            }
        ],
        "open_issues": [{"number": 2201, "state": "OPEN", "labels": []}],
        "labeled": [],
        "associated": [],
    }
    completed, log_path = _run(
        tmp_path, event=NEXT_EVENT, fixture=fixture, dry_run=True
    )
    shown = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, shown
    assert "2201" in completed.stdout
    assert "add_label" in completed.stdout
    assert _mutating_calls(log_path) == []
    assert any("pr" in argv and "list" in argv for argv in _calls(log_path))


def test_apply_on_next_adds_label_and_comments(tmp_path: Path) -> None:
    fixture = {
        "prs": [
            {
                "number": 2217,
                "url": f"https://github.com/{REPO}/pull/2217",
                "body": "Fixes #2201\n",
                "title": "fix mcp",
                "mergedAt": "2026-09-02T16:52:01Z",
                "headRefName": "task/example",
                "baseRefName": "next",
            }
        ],
        "open_issues": [{"number": 2201, "state": "OPEN", "labels": []}],
        "labeled": [],
        "associated": [],
    }
    completed, log_path = _run(tmp_path, event=NEXT_EVENT, fixture=fixture)
    shown = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, shown
    mutating = _mutating_calls(log_path)
    assert mutating, shown
    joined = " ".join(" ".join(argv) for argv in mutating)
    assert "2201" in joined
    assert "label" in joined or "labels" in joined


def test_dry_run_on_main_next_merge_plans_close_without_mutating(tmp_path: Path) -> None:
    fixture = {
        "prs": [],
        "open_issues": [],
        "labeled": [
            {
                "number": 2201,
                "state": "OPEN",
                "labels": [{"name": LABEL}],
                "title": "mcp",
                "url": f"https://github.com/{REPO}/issues/2201",
            }
        ],
        "associated": [],
    }
    completed, log_path = _run(
        tmp_path, event=MAIN_NEXT_MERGE_EVENT, fixture=fixture, dry_run=True
    )
    shown = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, shown
    assert "close" in completed.stdout
    assert "2201" in completed.stdout
    assert _mutating_calls(log_path) == []


def test_main_hotfix_does_not_close_labeled_issues(tmp_path: Path) -> None:
    fixture = {
        "prs": [],
        "open_issues": [],
        "labeled": [
            {
                "number": 2201,
                "state": "OPEN",
                "labels": [{"name": LABEL}],
                "title": "mcp",
                "url": f"https://github.com/{REPO}/issues/2201",
            }
        ],
        "associated": [],
    }
    completed, log_path = _run(tmp_path, event=MAIN_HOTFIX_EVENT, fixture=fixture)
    shown = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, shown
    assert "close" not in completed.stdout.splitlines() or "no actions" in completed.stdout
    assert _mutating_calls(log_path) == []


def test_missing_gh_fails_closed(tmp_path: Path) -> None:
    completed, _log = _run(tmp_path, event=NEXT_EVENT, gh_on_path=False)
    assert completed.returncode != 0
    assert "gh" in f"{completed.stdout}\n{completed.stderr}".lower()


# --- workflow wiring --------------------------------------------------------


def _workflow() -> dict[str, Any]:
    loaded = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_workflow_documents_the_label_path_and_triggers_on_next_and_main() -> None:
    header = WORKFLOW.read_text(encoding="utf-8")
    assert "label path" in header.lower()
    assert LABEL in header
    assert "close-on-merge-to-next" in header or "not close" in header.lower()

    workflow = _workflow()
    trigger = workflow.get(True, workflow.get("on"))
    assert isinstance(trigger, dict)
    push = trigger.get("push")
    assert isinstance(push, dict)
    assert set(push.get("branches") or []) == {"next", "main"}
    dispatch = trigger.get("workflow_dispatch")
    assert isinstance(dispatch, dict)
    inputs = dispatch.get("inputs")
    assert isinstance(inputs, dict)
    dry_run = inputs.get("dry_run")
    assert isinstance(dry_run, dict)
    assert dry_run.get("type") == "boolean"
    assert dry_run.get("default") is True

    permissions = workflow.get("permissions")
    assert isinstance(permissions, dict)
    assert permissions.get("contents") == "read"
    assert permissions.get("issues") == "write"
    assert permissions.get("pull-requests") == "read"


def test_workflow_runs_self_test_before_reconcile_and_supports_dry_run() -> None:
    workflow = _workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    job = jobs.get("reconcile")
    assert isinstance(job, dict)
    steps = job.get("steps")
    assert isinstance(steps, list)
    run_bodies = [step.get("run") for step in steps if isinstance(step.get("run"), str)]
    assert any(
        isinstance(body, str) and "close.py --self-test" in body for body in run_bodies
    ), "the workflow must run --self-test before mutating GitHub"
    reconcile_runs = [
        body
        for body in run_bodies
        if isinstance(body, str) and "close.py" in body and "--self-test" not in body
    ]
    assert len(reconcile_runs) == 1
    assert "--event" in reconcile_runs[0]
    assert "--dry-run" in reconcile_runs[0]
    checkout = [
        step
        for step in steps
        if isinstance(step, dict)
        and isinstance(step.get("uses"), str)
        and step["uses"].split("@", 1)[0] == "actions/checkout"
    ]
    assert checkout
    persist = checkout[0].get("with", {}).get("persist-credentials")
    assert persist is False
    assert all(
        step.get("continue-on-error") is not True
        for step in steps
        if isinstance(step, dict)
    )
