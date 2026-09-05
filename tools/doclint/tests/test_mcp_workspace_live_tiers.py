"""The #2305 live-tier rule is pinned through the docs gate.

A docs-only AGENTS.md edit skips pytest, so the pin has to live in
``curie_doclint``: the docs step always runs. Tests drive ``main`` and
assert on exit code and message text only.
"""

from __future__ import annotations

from pathlib import Path

from .conftest import RunLint, write

_AGENTS = "AGENTS.md"
_SKILL = ".claude/skills/implement/SKILL.md"
_TEMPLATE = ".github/PULL_REQUEST_TEMPLATE.md"
_REASON = "MCP/workspace live-tier rule is missing this required sentence"

_WORKFLOW_RULE = """
The path set is runner MCP catalog projection, unscoped PreToolUse,
in-process platform MCP tools, workspace publication, and
built-in coding-tool session capability. A behavior-bearing change that
reaches any of those reaches both live-provider and Slack external-integration.
Those two rows are required on that path. "No model routing change" is not a valid n/a reason.
Fake-model kind, skill ladder, and helper-only tests remain useful and are
not sufficient for those acceptance criteria. Leave the required-tier item
open when the evidence is missing; do not close it by marking the row n/a.
"""

_TEMPLATE_RULE = """
A change that reaches runner MCP catalog projection, unscoped PreToolUse,
in-process platform MCP tools, workspace publication, or
built-in coding-tool session capability must record live-provider plus
Slack external-integration evidence, or leave those required-tier rows open.
"No model routing change" is not a valid n/a reason.
"""

_FORBIDDEN_NA = '"No model routing change" is not a valid n/a reason'
_LIVE_SLACK = (
    "A behavior-bearing change that reaches any of those reaches both "
    "live-provider and Slack external-integration"
)


def test_fixture_without_workflow_docs_still_passes(
    clean_repo: Path, run_lint: RunLint
) -> None:
    # The miniature tree has no AGENTS.md / implement skill / PR template.
    # Absence is a skip, not a finding: those files are not fixture artifacts.
    code, out = run_lint(clean_repo)
    assert code == 0, out
    assert _REASON not in out


def test_complete_workflow_docs_pass(clean_repo: Path, run_lint: RunLint) -> None:
    write(clean_repo, _AGENTS, _WORKFLOW_RULE)
    write(clean_repo, _SKILL, _WORKFLOW_RULE)
    write(clean_repo, _TEMPLATE, _TEMPLATE_RULE)
    code, out = run_lint(clean_repo)
    assert code == 0, out


def test_agents_md_missing_the_forbidden_na_sentence_fails(
    clean_repo: Path, run_lint: RunLint
) -> None:
    # THE ticket's own defect: the path set is named, live-provider and Slack
    # are named, but "No model routing change" is no longer forbidden.
    write(clean_repo, _AGENTS, _WORKFLOW_RULE.replace(_FORBIDDEN_NA, "", 1))
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _AGENTS in out
    assert _FORBIDDEN_NA in out
    assert _REASON in out


def test_scattered_nouns_without_the_contiguous_rule_fail(
    clean_repo: Path, run_lint: RunLint
) -> None:
    # Independent substrings are not enough. A doc that keeps every noun and
    # re-allows the n/a reason must fail the contiguous live-provider sentence.
    write(
        clean_repo,
        _AGENTS,
        "The path set is runner MCP catalog projection, unscoped PreToolUse, "
        "in-process platform MCP tools, workspace publication, and "
        "built-in coding-tool session capability. "
        "No model routing change is a valid n/a reason for live-provider "
        "and Slack external-integration. Fake-model kind, skill ladder, and "
        "helper-only tests remain useful and are not sufficient. Leave the "
        "required-tier item open when the evidence is missing.\n",
    )
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _AGENTS in out
    assert _LIVE_SLACK in out
    assert _REASON in out


def test_implement_skill_missing_the_rule_fails(
    clean_repo: Path, run_lint: RunLint
) -> None:
    write(clean_repo, _SKILL, "Classify the end-to-end tiers before the first edit.\n")
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _SKILL in out
    assert _REASON in out


def test_pr_template_missing_the_reminder_fails(
    clean_repo: Path, run_lint: RunLint
) -> None:
    write(clean_repo, _TEMPLATE, "## End-to-end verification\n")
    code, out = run_lint(clean_repo)
    assert code != 0
    assert _TEMPLATE in out
    assert _REASON in out
