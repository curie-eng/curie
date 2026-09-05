"""Executable contract for the P0 / release-blocker / sre-bot-e2e-demo Closes reminder.

Issue #2306: a PR that closes one of those labels must spec-vs-impl each issue
AC against the diff. The implement skill is the trigger. The optional CI helper
comments; it must not fail the pull request and must not raise GitHub required
reviews. Ordinary bugfixes that close neither label stay skipped.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "tools" / "p0-closes-ci" / "check.py"
SKILL = REPO_ROOT / ".claude" / "skills" / "implement" / "SKILL.md"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "p0-closes.yaml"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
PYPROJECT = REPO_ROOT / "pyproject.toml"

TRIGGER_LABELS = ("P0", "release-blocker", "sre-bot-e2e-demo")
COMMENT_MARKER = "<!-- curie-p0-closes-spec-vs-impl -->"


def _write_event(
    tmp_path: Path,
    body: str | None,
    *,
    include_body: bool = True,
    repository: str = "curie-eng/curie",
) -> Path:
    pull_request: dict[str, object] = {}
    if include_body:
        pull_request["body"] = body
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            {
                "action": "opened",
                "pull_request": pull_request,
                "repository": {"full_name": repository},
            }
        ),
        encoding="utf-8",
    )
    return event_path


def _write_fake_gh(tmp_path: Path) -> Path:
    directory = tmp_path / "gh-bin"
    directory.mkdir()
    fake_binary = directory / "gh"
    fake_binary.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import json",
                "import os",
                "import sys",
                "from pathlib import Path",
                "log = Path(os.environ['P0_CLOSES_GH_CALLS'])",
                "calls = json.loads(log.read_text()) if log.exists() else []",
                "calls.append(sys.argv[1:])",
                "log.write_text(json.dumps(calls))",
                "code = int(os.environ.get('P0_CLOSES_GH_EXIT', '0'))",
                "if code != 0:",
                "    raise SystemExit(code)",
                "path = sys.argv[2]",
                "number = path.rsplit('/', 1)[-1]",
                "by_issue = json.loads(os.environ.get('P0_CLOSES_GH_LABELS_BY_ISSUE', '{}'))",
                "if number in by_issue:",
                "    print(json.dumps(by_issue[number]))",
                "else:",
                "    print(os.environ.get('P0_CLOSES_GH_LABELS', '[]'))",
                "",
            ]
        ),
        encoding="utf-8",
    )
    fake_binary.chmod(0o755)
    return directory


def _run_checker(
    tmp_path: Path,
    body: str | None,
    *,
    include_body: bool = True,
    gh_labels: str = "[]",
    gh_labels_by_issue: dict[str, list[str]] | None = None,
    gh_exit: int = 0,
    gh_on_path: bool = True,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    event_path = _write_event(tmp_path, body, include_body=include_body)
    comment_file = tmp_path / "comment.md"
    gh_calls = tmp_path / "gh-calls.json"
    binaries = _write_fake_gh(tmp_path)
    environment = {
        **os.environ,
        "P0_CLOSES_GH_CALLS": str(gh_calls),
        "P0_CLOSES_GH_LABELS": gh_labels,
        "P0_CLOSES_GH_LABELS_BY_ISSUE": json.dumps(gh_labels_by_issue or {}),
        "P0_CLOSES_GH_EXIT": str(gh_exit),
        "GITHUB_REPOSITORY": "curie-eng/curie",
        "PATH": f"{binaries}{os.pathsep}{os.environ.get('PATH', '')}" if gh_on_path else "",
    }
    completed = subprocess.run(
        [
            sys.executable,
            str(CHECKER),
            "--event",
            str(event_path),
            "--comment-file",
            str(comment_file),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        env=environment,
        text=True,
    )
    return completed, comment_file, gh_calls


def _load_workflow(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(document, dict), f"{path.name} must be a YAML mapping"
    return document


def _string(step: dict[str, Any], key: str) -> str:
    value = step.get(key)
    return value if isinstance(value, str) else ""


def _single_step(
    steps: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool], description: str
) -> dict[str, Any]:
    matches = [step for step in steps if predicate(step)]
    assert len(matches) == 1, f"expected exactly one {description}, found {len(matches)}"
    return matches[0]


def test_implement_skill_states_the_p0_closes_trigger_and_ac_vs_diff_rule() -> None:
    text = SKILL.read_text(encoding="utf-8")
    for label in TRIGGER_LABELS:
        assert f"`{label}`" in text, f"implement skill must name the `{label}` Closes trigger"
    assert "Closes" in text
    assert "spec-vs-impl" in text
    assert "acceptance criterion" in text.lower() or "acceptance criteria" in text.lower()
    assert "diff" in text
    assert "Fix pin" in text
    assert "e2e" in text.lower()


def test_implement_skill_documents_the_2209_message_only_pin_as_insufficient() -> None:
    text = SKILL.read_text(encoding="utf-8")
    assert "#2209" in text
    assert "#2248" in text
    combined = text.lower()
    assert "routing" in combined
    assert "message-only" in combined or "refusal-text" in combined or "string" in combined


def test_implement_skill_leaves_ordinary_bugfixes_unchanged() -> None:
    text = SKILL.read_text(encoding="utf-8")
    lowered = text.lower()
    assert "ordinary" in lowered
    assert "unchanged" in lowered


def test_pytest_collects_this_suite_from_committed_testpaths() -> None:
    assert '    "tools/p0-closes-ci/tests",' in PYPROJECT.read_text(encoding="utf-8")


@pytest.mark.parametrize("label", TRIGGER_LABELS)
def test_closing_a_trigger_label_writes_the_advisory_comment_and_exits_zero(
    tmp_path: Path, label: str
) -> None:
    completed, comment_file, gh_calls = _run_checker(
        tmp_path, "Closes #12\n", gh_labels=json.dumps([label, "bug"])
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("COMMENT:")
    assert "#12" in completed.stdout
    body = comment_file.read_text(encoding="utf-8")
    assert COMMENT_MARKER in body
    assert "#12" in body
    assert "confirm each" in body.lower() or "each issue" in body.lower()
    assert "diff" in body
    assert "#2209" in body
    assert "#2248" in body
    calls = json.loads(gh_calls.read_text(encoding="utf-8"))
    assert calls == [["api", "repos/curie-eng/curie/issues/12", "--jq", "[.labels[].name]"]]


def test_ordinary_bug_close_skips_without_commenting(tmp_path: Path) -> None:
    completed, comment_file, gh_calls = _run_checker(
        tmp_path, "Closes #12\n", gh_labels=json.dumps(["bug"])
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("SKIPPED:")
    assert comment_file.read_text(encoding="utf-8") == ""
    assert json.loads(gh_calls.read_text(encoding="utf-8"))


def test_no_closing_keyword_skips_without_calling_gh(tmp_path: Path) -> None:
    completed, comment_file, gh_calls = _run_checker(
        tmp_path, "Ref #12\nThis is an ordinary bugfix.\n"
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("SKIPPED:")
    assert comment_file.read_text(encoding="utf-8") == ""
    assert not gh_calls.exists()


def test_html_comment_closes_does_not_trigger(tmp_path: Path) -> None:
    completed, comment_file, gh_calls = _run_checker(
        tmp_path, "<!-- Closes #12 -->\n", gh_labels=json.dumps(["P0"])
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("SKIPPED:")
    assert comment_file.read_text(encoding="utf-8") == ""
    assert not gh_calls.exists()


def test_fixes_colon_form_triggers(tmp_path: Path) -> None:
    completed, comment_file, _gh_calls = _run_checker(
        tmp_path, "Fixes: #12\n", gh_labels=json.dumps(["sre-bot-e2e-demo"])
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("COMMENT:")
    assert "#12" in comment_file.read_text(encoding="utf-8")


def test_mixed_closes_comments_only_the_trigger_issues(tmp_path: Path) -> None:
    completed, comment_file, _gh_calls = _run_checker(
        tmp_path,
        "Closes #12 and #13\n",
        gh_labels_by_issue={"12": ["bug"], "13": ["P0"]},
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("COMMENT:")
    body = comment_file.read_text(encoding="utf-8")
    assert "#13" in body
    assert "#12" not in body
    assert "#13" in completed.stdout
    assert "#12" not in completed.stdout


def test_gh_api_error_fails_open_without_blocking(tmp_path: Path) -> None:
    completed, comment_file, _gh_calls = _run_checker(
        tmp_path, "Closes #12\n", gh_labels=json.dumps(["P0"]), gh_exit=1
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("SKIPPED:")
    assert comment_file.read_text(encoding="utf-8") == ""


def test_missing_gh_with_closed_issues_fails_open(tmp_path: Path) -> None:
    completed, comment_file, _gh_calls = _run_checker(tmp_path, "Closes #12\n", gh_on_path=False)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().startswith("SKIPPED:")
    assert comment_file.read_text(encoding="utf-8") == ""


def test_malformed_event_exits_nonzero(tmp_path: Path) -> None:
    event_path = tmp_path / "event.json"
    event_path.write_text("{", encoding="utf-8")
    comment_file = tmp_path / "comment.md"
    completed = subprocess.run(
        [
            sys.executable,
            str(CHECKER),
            "--event",
            str(event_path),
            "--comment-file",
            str(comment_file),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert completed.returncode != 0
    assert "event" in completed.stderr.lower() or "json" in completed.stderr.lower()


def test_commenter_is_not_wired_into_the_required_python_job() -> None:
    ci = CI_WORKFLOW.read_text(encoding="utf-8")
    assert "p0-closes" not in ci
    assert "tools/p0-closes-ci/check.py" not in ci


def test_workflow_comments_on_body_edits_and_never_requires_a_review() -> None:
    workflow = _load_workflow(WORKFLOW)
    trigger = workflow.get("on", workflow.get(True))
    assert isinstance(trigger, dict)
    pull_request = trigger.get("pull_request")
    assert isinstance(pull_request, dict)
    assert set(pull_request.get("types") or []) == {
        "opened",
        "reopened",
        "synchronize",
        "edited",
    }
    assert set(pull_request.get("branches") or []) == {"main", "next"}

    workflow_permissions = workflow.get("permissions")
    assert workflow_permissions == {"contents": "read"} or (
        isinstance(workflow_permissions, dict) and workflow_permissions.get("contents") == "read"
    )

    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    assert len(jobs) == 1
    job = next(iter(jobs.values()))
    assert isinstance(job, dict)
    permissions = job.get("permissions")
    assert isinstance(permissions, dict)
    assert permissions.get("contents") == "read"
    assert permissions.get("issues") == "read"
    assert permissions.get("pull-requests") == "write"
    assert "reviews" not in permissions
    assert job.get("continue-on-error") is not True

    steps = [step for step in job.get("steps") or [] if isinstance(step, dict)]
    checkout = _single_step(
        steps,
        lambda step: _string(step, "uses").startswith("actions/checkout@"),
        "checkout",
    )
    assert checkout.get("with", {}).get("persist-credentials") is False

    checker = _single_step(
        steps,
        lambda step: "tools/p0-closes-ci/check.py" in _string(step, "run"),
        "commenter caller",
    )
    assert "python3" in _string(checker, "run") or "python" in _string(checker, "run")
    assert "--event" in _string(checker, "run")
    assert "$GITHUB_EVENT_PATH" in _string(checker, "run")
    assert "${{ github.event.pull_request.body }}" not in _string(checker, "run")
    checker_env = checker.get("env")
    assert isinstance(checker_env, dict)
    assert checker_env.get("GH_TOKEN")

    poster = _single_step(
        steps,
        lambda step: (
            isinstance(step.get("uses"), str)
            and str(step.get("uses")).startswith("actions/github-script@")
            and "createComment" in str(step.get("with", {}).get("script", ""))
        ),
        "comment poster",
    )
    script = str(poster.get("with", {}).get("script", ""))
    assert COMMENT_MARKER in script or "curie-p0-closes-spec-vs-impl" in script
    assert "github.event.pull_request.body" not in script
    assert "createComment" in script
    assert "updateComment" in script
    assert poster.get("continue-on-error") is not True
    assert "catch" in script
    assert "core.warning" in script
