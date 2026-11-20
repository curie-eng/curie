"""ADR number prefixes are unique in-tree and across trains / open PRs.

In-tree uniqueness (#541): two files claiming the same ``NNNN`` in one tree.
Cross-train uniqueness (#1957): the same number with different filenames on
``origin/main``, ``origin/next``, the working tree, or an open PR. Remote
collisions this candidate adds or renames into fail the gate; other remote
collisions and scan failures without a candidate ADR claim are warnings.

Test cases:

1. ``test_duplicate_adr_prefix_fails`` -- two working-tree ADRs sharing one
   ``NNNN`` fail the lint, and the report names both filenames.
2. ``test_unique_adr_prefixes_pass`` -- distinct prefixes beside the fixture
   stay silent (false-positive guard after regenerating the ADR index).
3. ``test_next_free_number_is_one_past_max`` -- empty set yields ``0001``;
   otherwise the result is one past the highest claimed number (gaps ignored).
4. ``test_same_filename_across_sources_is_not_a_collision`` -- the same
   filename claiming one number on several sources is not a collision.
5. ``test_cross_train_collision_names_holders_and_next_free`` -- different
   filenames with the same number on ``main`` and ``next`` produce one
   finding that names every holder and the next free number.
6. ``test_open_pr_collision_message_shape`` -- ``format_collision_reason``
   includes PR and train holders plus the supplied next-free value.
7. ``test_check_adr_uniqueness_fails_closed_when_remote_scan_breaks`` -- a
   failing remote runner with a candidate ADR claim yields a fail-closed error.
8. ``test_check_adr_uniqueness_remote_union_detects_cross_ref`` -- the union
   of working tree, ``origin/main``, and ``origin/next`` detects a shared
   number this candidate claims and reports next free as an error.
9. ``test_open_pr_claims_include_added_adr_only`` -- open-PR claims keep only
   files with status ``added`` under ``docs/adr/NNNN-*.md``; modified ADRs
   and non-ADR paths are ignored.
10. ``test_require_remote_flag_on_cli_fails_closed_when_diff_base_missing``
    -- ``--require-remote-adr-scan`` on a fixture with no usable git base
    exits non-zero (candidate ADR discovery fails closed).
11. ``test_claims_from_filenames_ignores_readme`` -- ``README.md`` is skipped;
    only numbered ADR basenames become claims.
12. ``test_adr_scan_error_message_prefix`` -- ``AdrScanError.as_finding``
    reasons start with the shared fail-closed prefix before the detail.
13. ``test_remote_only_collision_is_warning_without_candidate_claim`` -- a
    main/next collision with no candidate ADR add/rename is a warning, not an
    error (unrelated PRs must not be blocked).
14. ``test_train_repair_renumber_is_warning_not_error`` -- renumbering away
    from a main/next collision leaves a warning for the old number so the
    repair can merge.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from curie_doclint.adr_uniqueness import (
    AdrClaim,
    AdrScanError,
    allocation_findings,
    check_adr_uniqueness,
    claims_from_filenames,
    collision_groups,
    format_collision_reason,
    next_free_number,
    open_pr_adr_claims,
)
from curie_doclint import main

from .conftest import Regenerate, RunLint, write

_BODY = "# ADR (fixture)\n\nStatus: Accepted\n"


def test_duplicate_adr_prefix_fails(clean_repo: Path, run_lint: RunLint) -> None:
    """Two working-tree ADRs sharing ``0002`` fail the lint naming both files.

    Both filenames must appear: the remedy is choosing which one moves, and a
    report that only says ``0002`` is duplicated does not tell you that.
    """
    write(clean_repo, "docs/adr/0002-first-decision.md", _BODY)
    write(clean_repo, "docs/adr/0002-second-decision.md", _BODY)
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "0002-first-decision.md" in out
    assert "0002-second-decision.md" in out


def test_unique_adr_prefixes_pass(
    clean_repo: Path, regenerate: Regenerate, run_lint: RunLint
) -> None:
    """Distinct prefixes beside the fixture's ``0001`` pass after index regen.

    False-positive guard: a check that flags every ADR directory blocks every
    PR. The index is regenerated first because these two new ADRs belong in it;
    leaving the index stale is real drift covered by another test.
    """
    write(clean_repo, "docs/adr/0002-second-decision.md", _BODY)
    write(clean_repo, "docs/adr/0003-third-decision.md", _BODY)
    regenerate(clean_repo)
    code, out = run_lint(clean_repo)
    assert code == 0, out


def test_next_free_number_is_one_past_max() -> None:
    """Empty set yields ``0001``; otherwise one past the highest claim (gaps ignored)."""
    assert next_free_number(set()) == "0001"
    assert next_free_number({"0001", "0003"}) == "0004"
    assert next_free_number({"0099"}) == "0100"


def test_same_filename_across_sources_is_not_a_collision() -> None:
    """The same ADR filename on working tree, main, and next is not a collision."""
    claims = [
        AdrClaim("0042", "0042-shared.md", "working-tree"),
        AdrClaim("0042", "0042-shared.md", "origin/main"),
        AdrClaim("0042", "0042-shared.md", "origin/next"),
    ]
    assert collision_groups(claims) == {}
    assert allocation_findings(claims) == []


def test_cross_train_collision_names_holders_and_next_free() -> None:
    """Different filenames sharing a number on main and next name holders and next free."""
    claims = [
        AdrClaim("0042", "0042-foo.md", "origin/main"),
        AdrClaim("0042", "0042-bar.md", "origin/next"),
        AdrClaim("0043", "0043-only.md", "working-tree"),
    ]
    findings = allocation_findings(
        claims, candidate_adr_filenames={"0042-foo.md"}
    )
    assert len(findings) == 1
    assert findings[0].severity == "error"
    reason = findings[0].reason
    assert "0042-foo.md (origin/main)" in reason
    assert "0042-bar.md (origin/next)" in reason
    assert "next free number is 0044" in reason


def test_open_pr_collision_message_shape() -> None:
    """``format_collision_reason`` includes train and PR holders plus the next-free value."""
    reason = format_collision_reason(
        "0042",
        [
            AdrClaim("0042", "0042-foo.md", "origin/main"),
            AdrClaim("0042", "0042-baz.md", "PR #1847"),
        ],
        "0190",
    )
    assert "0042-baz.md (PR #1847)" in reason
    assert "0042-foo.md (origin/main)" in reason
    assert "next free number is 0190" in reason


def test_check_adr_uniqueness_fails_closed_when_remote_scan_breaks(
    clean_repo: Path,
) -> None:
    """A failing remote runner with a candidate ADR claim yields a fail-closed error."""
    def boom(cmd: list[str] | tuple[str, ...], cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            list(cmd), returncode=1, stdout="", stderr="network unreachable"
        )

    findings = check_adr_uniqueness(
        clean_repo,
        require_remote=True,
        runner=boom,
        candidate_adr_filenames={"0042-new.md"},
    )
    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert "cannot verify open PRs to determine a new unique ADR number" in findings[0].reason
    assert "network unreachable" in findings[0].reason


def test_check_adr_uniqueness_remote_union_detects_cross_ref(
    clean_repo: Path,
) -> None:
    """Working tree plus main and next detect a shared number this candidate claims."""
    write(clean_repo, "docs/adr/0042-local.md", _BODY)

    def fake_runner(
        cmd: list[str] | tuple[str, ...], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        args = list(cmd)
        if args[:3] == ["git", "ls-remote", "--heads"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=(
                    "abc\trefs/heads/main\n"
                    "def\trefs/heads/next\n"
                ),
                stderr="",
            )
        if args[:2] == ["git", "fetch"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["git", "ls-tree", "-r"]:
            ref = args[4]
            if ref == "origin/main":
                files = "docs/adr/0042-from-main.md\n"
            elif ref == "origin/next":
                files = "docs/adr/0042-from-next.md\n"
            else:
                files = ""
            return subprocess.CompletedProcess(args, 0, stdout=files, stderr="")
        if args[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(
                args, 0, stdout="git@github.com:curie-eng/curie.git\n", stderr=""
            )
        if args[:2] == ["gh", "api"] and "pulls?" in " ".join(args):
            return subprocess.CompletedProcess(args, 0, stdout="[]", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    findings = check_adr_uniqueness(
        clean_repo,
        require_remote=True,
        runner=fake_runner,
        candidate_adr_filenames={"0042-local.md"},
    )
    assert len(findings) == 1
    assert findings[0].severity == "error"
    reason = findings[0].reason
    assert "0042-local.md (working-tree)" in reason
    assert "0042-from-main.md (origin/main)" in reason
    assert "0042-from-next.md (origin/next)" in reason
    assert "next free number is 0043" in reason


def test_remote_only_collision_is_warning_without_candidate_claim(
    clean_repo: Path,
) -> None:
    """A main/next collision with no candidate ADR add/rename is a warning."""

    def fake_runner(
        cmd: list[str] | tuple[str, ...], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        args = list(cmd)
        if args[:3] == ["git", "ls-remote", "--heads"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=(
                    "abc\trefs/heads/main\n"
                    "def\trefs/heads/next\n"
                ),
                stderr="",
            )
        if args[:2] == ["git", "fetch"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["git", "ls-tree", "-r"]:
            ref = args[4]
            if ref == "origin/main":
                files = "docs/adr/0042-from-main.md\n"
            elif ref == "origin/next":
                files = "docs/adr/0042-from-next.md\n"
            else:
                files = ""
            return subprocess.CompletedProcess(args, 0, stdout=files, stderr="")
        if args[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(
                args, 0, stdout="git@github.com:curie-eng/curie.git\n", stderr=""
            )
        if args[:2] == ["gh", "api"] and "pulls?" in " ".join(args):
            return subprocess.CompletedProcess(args, 0, stdout="[]", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    findings = check_adr_uniqueness(
        clean_repo,
        require_remote=True,
        runner=fake_runner,
        candidate_adr_filenames=set(),
    )
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert "0042-from-main.md (origin/main)" in findings[0].reason
    assert "0042-from-next.md (origin/next)" in findings[0].reason


def test_train_repair_renumber_is_warning_not_error(clean_repo: Path) -> None:
    """Renumbering away from a main/next collision leaves a warning, not an error."""
    write(clean_repo, "docs/adr/0043-repaired.md", _BODY)

    def fake_runner(
        cmd: list[str] | tuple[str, ...], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        args = list(cmd)
        if args[:3] == ["git", "ls-remote", "--heads"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=(
                    "abc\trefs/heads/main\n"
                    "def\trefs/heads/next\n"
                ),
                stderr="",
            )
        if args[:2] == ["git", "fetch"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["git", "ls-tree", "-r"]:
            ref = args[4]
            if ref == "origin/main":
                files = "docs/adr/0042-from-main.md\n"
            elif ref == "origin/next":
                files = "docs/adr/0042-from-next.md\n"
            else:
                files = ""
            return subprocess.CompletedProcess(args, 0, stdout=files, stderr="")
        if args[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(
                args, 0, stdout="git@github.com:curie-eng/curie.git\n", stderr=""
            )
        if args[:2] == ["gh", "api"] and "pulls?" in " ".join(args):
            return subprocess.CompletedProcess(args, 0, stdout="[]", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    findings = check_adr_uniqueness(
        clean_repo,
        require_remote=True,
        runner=fake_runner,
        # Repair renamed 0042-from-main.md -> 0043-repaired.md; the old train
        # collision must not block the merge.
        candidate_adr_filenames={"0043-repaired.md"},
    )
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].citation == "0042"
    assert "next free number is 0044" in findings[0].reason


def test_open_pr_claims_include_added_adr_only(tmp_path: Path) -> None:
    """Open-PR claims keep only ``added`` ``docs/adr/NNNN-*.md`` paths; others are ignored."""
    def fake_runner(
        cmd: list[str] | tuple[str, ...], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        args = list(cmd)
        if args[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(
                args, 0, stdout="https://github.com/curie-eng/curie.git\n", stderr=""
            )
        joined = " ".join(args)
        if "pulls?" in joined:
            payload = [{"number": 1847, "title": "adr"}]
            return subprocess.CompletedProcess(
                args, 0, stdout=json.dumps(payload), stderr=""
            )
        if "pulls/1847/files" in joined:
            payload = [
                {"filename": "docs/adr/0042-new.md", "status": "added"},
                {"filename": "docs/adr/0001-example.md", "status": "modified"},
                {"filename": "README.md", "status": "added"},
            ]
            return subprocess.CompletedProcess(
                args, 0, stdout=json.dumps(payload), stderr=""
            )
        raise AssertionError(f"unexpected command: {args}")

    claims = open_pr_adr_claims(tmp_path, runner=fake_runner)
    assert claims == [
        AdrClaim("0042", "0042-new.md", "PR #1847"),
    ]


def test_require_remote_flag_on_cli_fails_closed_when_diff_base_missing(
    clean_repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--require-remote-adr-scan`` fails closed when candidate ADR discovery cannot run.

    A fixture tree has no usable git base, so ``git diff`` against the PR base
    fails. That must not be treated as "no ADR claim" (which would soft-warn).
    """
    code = main(["--repo-root", str(clean_repo), "--require-remote-adr-scan"])
    captured = capsys.readouterr()
    out = captured.out + captured.err
    assert code != 0
    assert "cannot verify open PRs to determine a new unique ADR number" in out
    assert "diffing ADR files against" in out


def test_claims_from_filenames_ignores_readme() -> None:
    """``README.md`` is skipped; only numbered ADR basenames become claims."""
    claims = claims_from_filenames(
        ["docs/adr/README.md", "docs/adr/0007-adopt.md"], "origin/main"
    )
    assert claims == [AdrClaim("0007", "0007-adopt.md", "origin/main")]


def test_adr_scan_error_message_prefix() -> None:
    """``AdrScanError.as_finding`` reasons start with the shared fail-closed prefix."""
    finding = AdrScanError("gh: Not Found").as_finding()
    assert finding.reason.startswith(
        "cannot verify open PRs to determine a new unique ADR number:"
    )
