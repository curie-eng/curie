"""The #2305 live-tier rule in the implement workflow docs.

A pull request can change how the runner advertises MCP tools, how PreToolUse
classifies them, or how a mounted workspace exposes coding and publication
tools, then mark live-provider and Slack external-integration n/a because
"model routing did not change." The implement skill and AGENTS.md now forbid
that classification. This check pins the contiguous rule sentences so a
reword cannot keep the nouns and drop the prohibition.

The files are optional in the miniature fixture tree. When a file is present,
every required sentence must appear after whitespace is squashed. The real
repo carries all three files, and the docs gate always runs, including on
docs-only diffs that skip pytest.
"""

from __future__ import annotations

from pathlib import Path

from .finding import Finding

WORKFLOW_DOCS = (
    "AGENTS.md",
    ".claude/skills/implement/SKILL.md",
)

# Contiguous claims, not a bag of nouns. A document that names the path set
# and then re-allows "No model routing change" as n/a must fail.
WORKFLOW_SENTENCES = (
    "runner MCP catalog projection, unscoped PreToolUse, "
    "in-process platform MCP tools, workspace publication, and "
    "built-in coding-tool session capability",
    "A behavior-bearing change that reaches any of those reaches both "
    "live-provider and Slack external-integration",
    '"No model routing change" is not a valid n/a reason',
    "Fake-model kind, skill ladder, and helper-only tests remain useful "
    "and are not sufficient",
    "Leave the required-tier item open when the evidence is missing",
)

TEMPLATE_REL = ".github/PULL_REQUEST_TEMPLATE.md"
TEMPLATE_SENTENCES = (
    "runner MCP catalog projection, unscoped PreToolUse, "
    "in-process platform MCP tools, workspace publication",
    "built-in coding-tool session capability must record live-provider plus "
    "Slack external-integration evidence, or leave those required-tier "
    "rows open",
    '"No model routing change" is not a valid n/a reason',
)


def _squashed(text: str) -> str:
    return " ".join(text.split())


def _missing_sentences(text: str, sentences: tuple[str, ...]) -> list[str]:
    haystack = _squashed(text)
    return [sentence for sentence in sentences if sentence not in haystack]


def check_mcp_workspace_live_tiers(repo_root: Path) -> list[Finding]:
    """Return a finding for each required sentence a present workflow doc dropped."""

    findings: list[Finding] = []
    targets: tuple[tuple[str, tuple[str, ...]], ...] = tuple(
        (rel, WORKFLOW_SENTENCES) for rel in WORKFLOW_DOCS
    ) + ((TEMPLATE_REL, TEMPLATE_SENTENCES),)
    for rel, sentences in targets:
        path = repo_root / rel
        if not path.is_file():
            continue
        for sentence in _missing_sentences(path.read_text(encoding="utf-8"), sentences):
            findings.append(
                Finding(
                    rel,
                    sentence,
                    "MCP/workspace live-tier rule is missing this required sentence",
                )
            )
    return findings
