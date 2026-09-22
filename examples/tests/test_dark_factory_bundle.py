"""The default dark-factory agent bundle validates and holds its discipline (#2576).

Pins the parts of ``examples/dark-factory`` that must not drift: the bundle
validates, its only MCP server is GitHub, its toolPolicy (classified by the real
plugin_format classifier) grants exactly ``get_issue``, the one skill states the
factory discipline, the evals are falsifiable, and no private identifier ships.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    PluginManifest,
    ToolPolicyDecision,
    classify_tool,
    load_tool_policy,
    validate_bundle,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "examples" / "dark-factory"
DRIVER = REPO_ROOT / "tools" / "factory-e2e" / "factory_e2e.py"


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def _policy():
    manifest = PluginManifest.model_validate(_manifest())
    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)
    assert policy is not None
    return policy


def _skill_files() -> list[Path]:
    return sorted((BUNDLE / "skills").glob("*/SKILL.md"))


def _skill_parts() -> tuple[dict, str]:
    (skill,) = _skill_files()
    text = skill.read_text()
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.DOTALL)
    assert match, "SKILL.md must open with YAML frontmatter"
    return yaml.safe_load(match.group(1)) or {}, match.group(2)


def test_bundle_validates() -> None:
    result = validate_bundle(BUNDLE)
    assert result.valid, result.errors
    assert result.errors == []


def test_manifest_identity_secrets_and_policy() -> None:
    manifest = _manifest()
    assert manifest["name"] == "dark-factory"
    assert manifest["secrets"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
    assert manifest["toolPolicy"]["enforcement"] == TOOL_POLICY_ENFORCEMENT


def test_mcp_declares_only_github() -> None:
    mcp = json.loads((BUNDLE / ".mcp.json").read_text())
    servers = mcp["mcpServers"]
    assert list(servers) == ["github"]
    github = servers["github"]
    assert github["command"] == "mcp-server-github"
    assert github["env"] == {"GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_PERSONAL_ACCESS_TOKEN}"}


def test_get_issue_is_allowed() -> None:
    assert classify_tool(_policy(), "github/get_issue") == ToolPolicyDecision.ALLOW


DENIED_TOOLS = [
    "add_issue_comment",
    "create_branch",
    "create_issue",
    "create_or_update_file",
    "create_pull_request",
    "create_pull_request_review",
    "create_repository",
    "fork_repository",
    "merge_pull_request",
    "push_files",
    "update_issue",
    "update_pull_request_branch",
    "search_code",
    "search_repositories",
    "search_users",
    "list_issues",
    "search_issues",
    "get_file_contents",
    "list_commits",
    "get_pull_request",
    "get_pull_request_comments",
    "get_pull_request_files",
    "get_pull_request_reviews",
    "get_pull_request_status",
    "list_pull_requests",
    "delete_repository",
]


@pytest.mark.parametrize("tool", DENIED_TOOLS)
def test_everything_else_is_denied(tool: str) -> None:
    assert classify_tool(_policy(), f"github/{tool}") == ToolPolicyDecision.DENY


def test_exactly_one_skill_without_allowed_tools() -> None:
    assert len(_skill_files()) == 1
    front, _ = _skill_parts()
    assert front.get("name")
    assert front.get("description")
    assert "allowed-tools" not in front


DISCIPLINE = {
    "reads-issue-with-get-issue": r"get_issue",
    "acceptance-criteria": r"acceptance criteri",
    "plan-before-editing": r"(written|write (a|the|down)|state (a|the)) plan|plan (before|first)",
    "failing-test-first": r"failing test|test (that )?fails|red test",
    "runs-repo-checks": r"(run|execute)s? (the )?(repository|repo|project)'?s? (own )?(checks|tests|test suite|linters?)",
    "self-review-against-every-criterion": r"(review|re-read|reread|check)\w* (the |your |its )?(own )?diff.{0,120}(every|each) acceptance criteri",
    "publishes-through-publish-changes": r"mcp__curie__publish_changes",
    "ends-with-reason-not-pr": r"(stop|end|finish)\w*.{0,80}(stated |clear |written )?reason|reason.{0,80}instead of (a |opening a )?pull request",
    "time-budget-1800": r"1800",
    "untrusted-input": r"untrusted",
    "stop-on-ambiguity": r"ambigu\w*.{0,160}(stop|do not guess|don'?t guess|never guess)|(stop|do not guess|never guess).{0,160}ambigu",
    "never-git-push": r"(never|do not|don'?t|must not)\s+(run\s+)?`?git push|(never|do not|don'?t|must not) push\w* with git",
    "no-sub-agents": r"(no|never|do not|don'?t|must not)\s+(use\s+)?(the\s+)?(`?task`?\s+tool|sub-?agents?)",
    "no-workflow-edits": r"\.github/",
}


@pytest.mark.parametrize("pattern", list(DISCIPLINE.values()), ids=list(DISCIPLINE))
def test_skill_states_the_discipline(pattern: str) -> None:
    assert re.search(pattern, "", re.IGNORECASE | re.DOTALL) is None
    _, body = _skill_parts()
    assert re.search(pattern, body, re.IGNORECASE | re.DOTALL), pattern


def test_untrusted_covers_issue_and_repository_and_instructions() -> None:
    _, body = _skill_parts()
    text = body.lower()
    assert "untrusted" in text
    assert re.search(r"issue", text) and re.search(r"repositor", text)
    assert re.search(r"instructions", text)


def test_workflow_edits_need_an_explicit_ask() -> None:
    _, body = _skill_parts()
    assert re.search(
        r"\.github/.{0,200}(unless|only if|only when).{0,80}(explicit|issue)",
        body,
        re.IGNORECASE | re.DOTALL,
    )


def test_evals_are_falsifiable() -> None:
    cases = json.loads((BUNDLE / "evals" / "cases.json").read_text())["cases"]
    assert len(cases) >= 4
    for case in cases:
        grader = case["grader"]
        text = case["input"]
        if grader["kind"] == "contains":
            assert grader["expected"].lower() not in text.lower(), case["id"]
        elif grader["kind"] == "regex":
            flags = 0 if grader.get("case_sensitive") else re.IGNORECASE
            assert re.search(grader["expected"], text, flags) is None, case["id"]


FORBIDDEN = [
    "the" + "connman",
    "curie-factory-" + "fixture",
    "curie-factory-" + "test",
    "/ho" + "me/",
    ".claude/" + "skills",
]


def _shipped_files() -> list[Path]:
    files = [p for p in BUNDLE.rglob("*") if p.is_file()]
    return [*files, DRIVER]


@pytest.mark.parametrize("needle", FORBIDDEN)
def test_no_private_identifiers(needle: str) -> None:
    hits = [
        str(p.relative_to(REPO_ROOT))
        for p in _shipped_files()
        if needle in p.read_text(errors="ignore").lower()
    ]
    assert hits == []


def test_operations_doc_points_at_the_bundle() -> None:
    assert "examples/dark-factory" in (REPO_ROOT / "docs" / "operations.md").read_text()
