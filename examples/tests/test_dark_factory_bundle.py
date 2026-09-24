"""The default dark-factory agent bundle validates and holds its discipline (#2576).

Pins the parts of ``examples/dark-factory`` that must not drift: the bundle
validates, its only MCP server is GitHub, its toolPolicy (classified by the real
plugin_format classifier) grants exactly ``get_issue`` and ``add_issue_comment``
(the review gate hook narrows the comment to capped or failed reviews, #3092),
the one skill states the factory discipline and its nine phases, the evals are
falsifiable, and no private identifier ships.
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
    # The platform's deploy path validates with the enforcing contract; a
    # toolPolicy bundle is refused by the non-enforcing default.
    result = validate_bundle(BUNDLE, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
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


@pytest.mark.parametrize("tool", ["get_issue", "add_issue_comment"])
def test_allowed_tools(tool: str) -> None:
    assert classify_tool(_policy(), f"github/{tool}") == ToolPolicyDecision.ALLOW


DENIED_TOOLS = [
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
    "runs-repo-checks": (
        r"(run"
        r"|execute)s? (the )?(repository"
        r"|repo"
        r"|project)'?s? (own )?(checks"
        r"|tests"
        r"|test suite"
        r"|linters?)"
    ),
    "diff-review-against-every-criterion": (
        r"diff-reviewer.{0,200}(every"
        r"|each"
        r"|numbered) acceptance criteri"
    ),
    "publishes-through-publish-changes": r"mcp__curie__publish_changes",
    "ends-with-reason-not-pr": (
        r"(stop"
        r"|end"
        r"|finish)\w*.{0,80}(stated "
        r"|clear "
        r"|written )?reason"
        r"|reason.{0,80}instead of (a "
        r"|opening a )?pull request"
    ),
    "time-budget-10800": r"10800",
    "untrusted-input": r"untrusted",
    "stop-on-ambiguity": (
        r"ambigu\w*.{0,160}(stop"
        r"|do not guess"
        r"|don'?t guess"
        r"|never guess)"
        r"|(stop"
        r"|do not guess"
        r"|never guess).{0,160}ambigu"
    ),
    "never-git-push": (
        r"(never"
        r"|do not"
        r"|don'?t"
        r"|must not)\s+(run\s+)?`?git push"
        r"|(never"
        r"|do not"
        r"|don'?t"
        r"|must not) push\w* with git"
    ),
    "only-the-reviewer-sub-agents": r"only sub-agents you may start",
    "review-loops-capped-at-3": r"at most 3 rounds",
    "no-workflow-edits": r"\.github/",
}


@pytest.mark.parametrize("pattern", list(DISCIPLINE.values()), ids=list(DISCIPLINE))
def test_skill_states_the_discipline(pattern: str) -> None:
    assert re.search(pattern, "", re.IGNORECASE | re.DOTALL) is None
    _, body = _skill_parts()
    assert re.search(pattern, body, re.IGNORECASE | re.DOTALL), pattern


PHASES = [
    "read_issue",
    "pin_criteria",
    "plan",
    "plan_review",
    "failing_test",
    "implement",
    "review_diff",
    "publish",
    "wait_ci",
]


def test_skill_defines_the_nine_phases_in_order() -> None:
    _, body = _skill_parts()
    headings = re.findall(r"^## \d+\. .*\(phase `(\w+)`", body, re.MULTILINE)
    assert headings == PHASES


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


def test_example_deploys_as_dark_factory_on_the_default_model() -> None:
    """The shipped example deploys under the product name on GLM 5.3 Flash (#3075)."""

    readme = (BUNDLE / "README.md").read_text()
    operations = (REPO_ROOT / "docs" / "operations.md").read_text()
    assert "--agent dark-factory " in readme
    assert "surfaces dark-factory " in readme
    assert "publication-policy dark-factory " in readme
    assert "agentSandbox.connectorEgress.dark-factory[" in readme
    assert "--agent factory " not in readme
    assert "agentSandbox.runner.model=z-ai/glm-5.3-flash" in readme
    assert "agent `dark-factory`" in operations
    assert "`z-ai/glm-5.3-flash`" in operations


# --- #3097: wait_ci loops back to implement -----------------------------------------


def _section(body: str, number: int) -> str:
    match = re.search(rf"^## {number}\. .*?(?=^## \d+\. |\Z)", body, re.MULTILINE | re.DOTALL)
    assert match, f"section {number} is missing"
    return match.group(0)


def test_wait_ci_section_loops_a_failed_check_back_to_implement() -> None:
    _, body = _skill_parts()
    section = _section(body, 9)
    assert "(phase `wait_ci`)" in section.splitlines()[0]
    assert "Curie wait_ci round" in section
    assert "implement" in section
    assert "untrusted" in section.lower()
    assert ".github/" in section
    assert "Could not complete:" in section
    assert "1800" in section
    assert "does not act on them yet" not in section


def test_skill_names_three_review_loops_including_wait_ci() -> None:
    _, body = _skill_parts()
    assert "Two\npairs loop" not in body
    assert re.search(r"`wait_ci`.{0,80}`implement`", body, re.DOTALL)


def test_readme_describes_the_ci_wait_and_fix_loop() -> None:
    readme = (BUNDLE / "README.md").read_text()
    assert "does not act on the checks yet" not in readme
    assert re.search(r"`wait_ci`.{0,80}`implement`", readme, re.DOTALL)
    assert "unverified" in readme.lower()
