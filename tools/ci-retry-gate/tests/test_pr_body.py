"""Executable regressions for the pull request body guards.

Three rules share this checker: bodies that defeat GitHub auto-close (#1713),
bodies that claim AI authorship (#962), and patch-release bodies that leave
Trigger or Live proof empty (#2251). The attribution half exists because
AGENTS.md forbade attribution on both surfaces while only the commit half was
enforced, so footers kept reaching merged pull requests. The release half exists
because the v0.8.x patch PRs named no defect trigger and no live re-verification.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "scripts" / "check-pr-body.sh"


def _check_body(
    tmp_path: Path, body: str, *, title: str | None = None
) -> subprocess.CompletedProcess[str]:
    body_path = tmp_path / "pull-request-body.md"
    body_path.write_text(body, encoding="utf-8")
    command = ["bash", str(CHECKER), str(body_path)]
    if title is not None:
        title_path = tmp_path / "pull-request-title.txt"
        title_path.write_text(title, encoding="utf-8")
        command.extend(["--title-file", str(title_path)])
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _diagnostics(completed: subprocess.CompletedProcess[str]) -> str:
    return (
        f"exit code: {completed.returncode}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


@pytest.mark.parametrize(
    "body",
    (
        "Describe a maintenance change without a closing keyword.\n",
        "Describe a fix.\n\nCloses #1713\n",
    ),
)
def test_pr_body_check_accepts_plain_text_and_real_newlines(tmp_path: Path, body: str) -> None:
    completed = _check_body(tmp_path, body)

    assert completed.returncode == 0, _diagnostics(completed)


def test_pr_body_check_rejects_literal_backslash_n_before_it_inerts_a_closing_keyword(
    tmp_path: Path,
) -> None:
    completed = _check_body(tmp_path, "Describe a fix.\\n\\nCloses #1713\n")
    diagnostics = _diagnostics(completed)

    assert completed.returncode != 0, diagnostics
    assert r"\n" in diagnostics, diagnostics
    assert "replace" in diagnostics.lower(), diagnostics


FOOTER = "\N{ROBOT FACE} Generated with [Claude Code](https://claude.com/claude-code)"


@pytest.mark.parametrize(
    "body",
    (
        # The exact footer that reached merged pull requests.
        f"Describe a fix.\n\n{FOOTER}\n",
        # The same footer with no trailing newline, which is the shape a body
        # pasted straight out of a tool actually has. A `while read` loop drops
        # that last line unless it is written to handle it.
        f"Describe a fix.\n\n{FOOTER}",
        "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n",
        "This patch was generated with Claude\n",
    ),
)
def test_pr_body_check_rejects_ai_attribution(tmp_path: Path, body: str) -> None:
    completed = _check_body(tmp_path, body)

    assert completed.returncode != 0, _diagnostics(completed)
    assert "AI" in completed.stderr, _diagnostics(completed)


@pytest.mark.parametrize(
    "body",
    (
        # Attribution, not mention. These are this repo's ordinary vocabulary and
        # must stay mergeable, or the gate gets switched off.
        "Update apps/ui/CLAUDE.md and the harness.claude import boundary.\n",
        "Pin claude-agent-sdk==0.2.110 so claude_agent_sdk spans stay schema-clean.\n",
        "Describe the Claude harness port and its registry.\n\nCloses #1713\n",
    ),
)
def test_pr_body_check_accepts_technical_references_to_the_harness(
    tmp_path: Path, body: str
) -> None:
    completed = _check_body(tmp_path, body)

    assert completed.returncode == 0, _diagnostics(completed)


def test_pr_body_guard_delegates_to_one_matcher() -> None:
    """Both surfaces must share a matcher, or the vendor lists drift apart."""
    checker = CHECKER.read_text(encoding="utf-8")

    assert "check-commit-messages.sh" in checker
    assert "--message-file" in checker


def test_pr_body_self_test_covers_the_attribution_half() -> None:
    """CI trusts --self-test before running the gate, so it must not be vacuous."""
    completed = subprocess.run(
        ["bash", str(CHECKER), "--self-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, _diagnostics(completed)
    assert "AI attribution" in completed.stdout, _diagnostics(completed)


def test_pr_body_gate_inherits_the_commit_matcher_boundary(tmp_path: Path) -> None:
    """A bare name followed by `.` is out of scope, by the shared matcher's design.

    BARE_ASSISTANT excludes a trailing `[._-]` so `harness.claude`, `CLAUDE.md`
    and `claude-sonnet-5` never trip rule C. A sentence that ends "generated with
    Claude." rides that same exclusion. Widening it belongs to the commit gate
    and its false-positive budget, not here; this pins the boundary so the next
    reader knows it is inherited rather than overlooked. The footer forms that
    actually reach pull requests are caught by rule B regardless.
    """
    completed = _check_body(tmp_path, "This patch was generated with Claude.\n")

    assert completed.returncode == 0, _diagnostics(completed)


PATCH_RELEASE_TITLE = "Prepare the v0.8.4 release"
FEATURE_RELEASE_TITLE = "Prepare the v0.9.0 release"

# Shape of #2218: release mechanics, no Trigger, no Live proof, every product
# tier marked n/a. The gate must refuse this on a patch-release title.
EMPTY_PATCH_RELEASE_BODY = """## Summary

Prepare the frozen v0.8.4 patch release on `main`. This registers the
immutable architecture-atlas snapshot and aligns CLI and chart versions.

## Related issue

Milestone: v0.8.4

## Fix pin verification

This is release preparation, not a behavior fix, and it closes no bug.

## End-to-end verification

This change updates release identity. Product behavior is unchanged.

| Tier | Required / n/a | Reason |
| --- | --- | --- |
| skill | n/a | unchanged |
| local | n/a | unchanged |
| local-release | required | released identity changes |
| cluster | n/a | unchanged |
| live provider | n/a | unchanged |
| external integration | n/a | unchanged |

## Checklist

- [x] Tests pass for the area touched.
"""

FILLED_TRIGGER = "## Trigger\n\n#2202, #2203, #2205, #2194\n"
FILLED_LIVE_PROOF_URL = (
    "## Live proof\n\n"
    "https://github.com/curie-eng/curie/actions/runs/1\n"
)
FILLED_LIVE_PROOF_WAIVER = "## Live proof\n\nwaiver: no live cluster in this cut\n"
COMMENT_ONLY_TRIGGER = (
    "## Trigger\n\n"
    "<!-- List the issue numbers of the defects that triggered this patch. -->\n"
)


def test_pr_body_rejects_patch_release_without_trigger_or_live_proof(tmp_path: Path) -> None:
    """#2251: the v0.8.x patch PRs are the negative examples this gate exists for."""
    completed = _check_body(
        tmp_path, EMPTY_PATCH_RELEASE_BODY, title=PATCH_RELEASE_TITLE
    )
    diagnostics = _diagnostics(completed)

    assert completed.returncode != 0, diagnostics
    assert "Trigger" in completed.stderr, diagnostics


def test_pr_body_rejects_patch_release_with_empty_trigger(tmp_path: Path) -> None:
    body = EMPTY_PATCH_RELEASE_BODY + "\n" + COMMENT_ONLY_TRIGGER + FILLED_LIVE_PROOF_URL
    completed = _check_body(tmp_path, body, title=PATCH_RELEASE_TITLE)
    diagnostics = _diagnostics(completed)

    assert completed.returncode != 0, diagnostics
    assert "Trigger" in completed.stderr, diagnostics


def test_pr_body_rejects_patch_release_with_empty_live_proof(tmp_path: Path) -> None:
    body = (
        EMPTY_PATCH_RELEASE_BODY
        + "\n"
        + FILLED_TRIGGER
        + "## Live proof\n\n<!-- Name a run URL or an explicit waiver. -->\n"
    )
    completed = _check_body(tmp_path, body, title=PATCH_RELEASE_TITLE)
    diagnostics = _diagnostics(completed)

    assert completed.returncode != 0, diagnostics
    assert "Live proof" in completed.stderr, diagnostics


@pytest.mark.parametrize(
    "live_proof",
    (FILLED_LIVE_PROOF_URL, FILLED_LIVE_PROOF_WAIVER),
)
def test_pr_body_accepts_patch_release_with_trigger_and_live_proof(
    tmp_path: Path, live_proof: str
) -> None:
    body = EMPTY_PATCH_RELEASE_BODY + "\n" + FILLED_TRIGGER + live_proof
    completed = _check_body(tmp_path, body, title=PATCH_RELEASE_TITLE)

    assert completed.returncode == 0, _diagnostics(completed)
    assert "Trigger" in completed.stdout or "Live proof" in completed.stdout, _diagnostics(
        completed
    )


@pytest.mark.parametrize(
    "title",
    (
        FEATURE_RELEASE_TITLE,
        "Prepare the v1.0.0 release",
        "Fix the dispatcher readiness probe",
    ),
)
def test_pr_body_skips_release_sections_unless_title_is_a_patch_release(
    tmp_path: Path, title: str
) -> None:
    completed = _check_body(tmp_path, EMPTY_PATCH_RELEASE_BODY, title=title)

    assert completed.returncode == 0, _diagnostics(completed)


def test_pr_body_gates_two_digit_patch_versions(tmp_path: Path) -> None:
    completed = _check_body(
        tmp_path, EMPTY_PATCH_RELEASE_BODY, title="Prepare the v0.8.10 release"
    )
    diagnostics = _diagnostics(completed)

    assert completed.returncode != 0, diagnostics
    assert "Trigger" in completed.stderr, diagnostics


def test_pr_template_has_required_patch_release_sections() -> None:
    """#2251: the template the release PRs actually use must carry both sections."""
    template = (REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md").read_text(encoding="utf-8")

    assert "## Trigger" in template
    assert "## Live proof" in template
    assert "waiver:" in template


def test_pr_body_self_test_covers_the_patch_release_half() -> None:
    """CI trusts --self-test before running the gate, so the #2251 cases must be in it."""
    completed = subprocess.run(
        ["bash", str(CHECKER), "--self-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, _diagnostics(completed)
    combined = f"{completed.stdout}\n{completed.stderr}"
    assert "Trigger" in combined, _diagnostics(completed)
    assert "Live proof" in combined, _diagnostics(completed)
