"""The SRE demo runbook's command gate (#2247).

`examples/sre-bot/DEMO.md` tells an operator which `curie` commands to run.
Nothing recomputed those names, so a renamed or dropped flag would leave the
runbook naming a switch the CLI no longer has. This gate resolves every
`curie ...` invocation the runbook names against the committed
`cli/command-manifest.json`, the same way `docs/agents.md` is already gated.

Four failure modes are asserted here: the runbook is missing, it names a flag
the manifest does not declare, it names a subcommand the manifest does not
declare, and it names no command at all. Every test drives
`main(["--repo-root", str(root)])` through `run_lint` and asserts on exit code
and output text only.
"""

from __future__ import annotations

from pathlib import Path

from .conftest import RunLint, write

_RUNBOOK = "examples/sre-bot/DEMO.md"


def _write_runbook(root: Path, body: str) -> None:
    write(root, _RUNBOOK, body)


def test_missing_sre_demo_runbook_fails(clean_repo: Path, run_lint: RunLint) -> None:
    # Absence must be louder than a skip. Filtering a missing required file
    # with `.is_file()` is what lets it be deleted for zero findings.
    runbook = clean_repo / _RUNBOOK
    if runbook.is_file():
        runbook.unlink()
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _RUNBOOK in out


def test_sre_demo_runbook_bogus_flag_fails(clean_repo: Path, run_lint: RunLint) -> None:
    # THE ticket's own defect class: the runbook names a flag the CLI does not
    # declare. Mutating the doc, not the manifest, is the operator-facing
    # direction (an editor invents `--workpace`).
    _write_runbook(
        clean_repo,
        "# Fixture runbook\n\n```bash\ncurie schema --not-a-real-flag\n```\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "--not-a-real-flag" in out
    assert _RUNBOOK in out


def test_sre_demo_runbook_bogus_subcommand_fails(
    clean_repo: Path, run_lint: RunLint
) -> None:
    _write_runbook(
        clean_repo,
        "# Fixture runbook\n\nRun `curie skill bogusverb --json` next.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert "bogusverb" in out
    assert _RUNBOOK in out


def test_sre_demo_runbook_without_commands_fails(
    clean_repo: Path, run_lint: RunLint
) -> None:
    # Vacuity guard: a reword that drops the backticks would otherwise leave a
    # green gate over an unverified runbook.
    _write_runbook(
        clean_repo,
        "# Fixture runbook\n\nProse about the curie binary with no command.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _RUNBOOK in out
    assert "no `curie` command appears" in out


def test_sre_demo_runbook_valid_command_passes(
    clean_repo: Path, run_lint: RunLint
) -> None:
    # Positive control. `curie schema` is hidden in the fixture manifest and
    # must still resolve: hidden means unadvertised, not nonexistent.
    _write_runbook(
        clean_repo,
        "# Fixture runbook\n\n```bash\ncurie schema\n```\n",
    )
    code, out = run_lint(clean_repo)
    assert code == 0, out
