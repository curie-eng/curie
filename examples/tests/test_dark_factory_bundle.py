"""Bundle validation, progress declarations, eval graders and publication hygiene.

Factory decisions are exercised by the review gate tests and graded eval cases.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    validate_bundle,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "examples" / "dark-factory"
DRIVER = REPO_ROOT / "tools" / "factory-e2e" / "factory_e2e.py"


def _manifest() -> dict:
    return json.loads((BUNDLE / ".claude-plugin" / "plugin.json").read_text())


def _skill_files() -> list[Path]:
    return sorted((BUNDLE / "skills").glob("*/SKILL.md"))


def _skill_parts() -> tuple[dict, str]:
    (skill,) = _skill_files()
    text = skill.read_text()
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.DOTALL)
    assert match, "SKILL.md must open with YAML frontmatter"
    return yaml.safe_load(match.group(1)) or {}, match.group(2)


def _only_the_unbuilt_runner_layer(errors: list) -> bool:
    """The shipped bundle's one intake error is its unbuilt runner layer (#3420).

    Its stdio MCP servers live in the layer `curie build` builds and locks for
    the operator's own registry, so the checkout carries no lock and intake
    refuses it until that build runs. Anything else is a real defect."""

    return [(e.code, e.message.split(":", 1)[0]) for e in errors] == [
        ("connectors.lock_missing", "runner")
    ]


def test_bundle_validates() -> None:
    # The platform's deploy path validates with the enforcing contract; a
    # toolPolicy bundle is refused by the non-enforcing default.
    result = validate_bundle(BUNDLE, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert _only_the_unbuilt_runner_layer(result.errors), result.errors


def test_manifest_declares_no_github_credential() -> None:
    # ADR 0187: the platform reads the issue, so the bundle holds no PAT.
    manifest = _manifest()
    assert manifest["name"] == "dark-factory"
    assert "secrets" not in manifest
    assert "toolPolicy" not in manifest


def test_bundle_ships_no_mcp_server() -> None:
    assert not (BUNDLE / ".mcp.json").exists()
    assert "server-github" not in (BUNDLE / "runner.Dockerfile").read_text()
    shipped = "\n".join(p.read_text(errors="ignore") for p in BUNDLE.rglob("*") if p.is_file())
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" not in shipped
    assert "add_issue_comment" not in shipped


def test_exactly_one_skill_without_allowed_tools() -> None:
    assert len(_skill_files()) == 1
    front, _ = _skill_parts()
    assert front.get("name")
    assert front.get("description")
    assert "allowed-tools" not in front


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


def _phase_declaration() -> dict:
    return json.loads((BUNDLE / "progress" / "phases.json").read_text())


def test_progress_declaration_defines_ordered_phases_labels_loops_and_stages() -> None:
    declared = _phase_declaration()
    assert [phase["id"] for phase in declared["phases"]] == PHASES
    for phase in declared["phases"]:
        assert 1 <= len(phase["label"]) <= 40, phase
    assert declared["loops"] == [
        {"start": "plan", "review": "plan_review", "cap": 3},
        {"start": "implement", "review": "review_diff", "cap": 3},
        {"start": "implement", "review": "wait_ci", "cap": 3},
    ]
    assert declared["stages"] == [
        {"id": "plan", "label": "Plan", "phases": ["read_issue", "pin_criteria", "plan"]},
        {"id": "plan_review", "label": "Plan review", "phases": ["plan_review"]},
        {"id": "implement", "label": "Implement", "phases": ["failing_test", "implement"]},
        {"id": "review_diff", "label": "Review diff", "phases": ["review_diff", "publish"]},
        {"id": "wait_ci", "label": "Wait for CI", "phases": ["wait_ci"]},
    ]


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


def test_evals_cover_an_approve_with_notes_verdict() -> None:
    cases = json.loads((BUNDLE / "evals" / "cases.json").read_text())["cases"]
    assert any(
        "approve" in case["input"].lower() and "notes" in case["input"].lower() for case in cases
    ), "no eval case covers an approve-with-notes verdict"


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


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


CONTRACT = BUNDLE / "verification" / "contract.md"


def _contract() -> str:
    """The one verification contract the hook hands every reader (#3874)."""
    assert CONTRACT.is_file(), "examples/dark-factory/verification/contract.md is missing"
    return _flat(CONTRACT.read_text())


def test_the_contract_keeps_the_service_ci_path_and_red_on_base_procedure() -> None:
    """A change whose tests need Postgres or Valkey still reaches publication (#3755).

    The #3755 path moved from the skill into the verification contract (#3874):
    the pull request's required CI is the delegated test's run, and the
    red-on-base procedure keeps the new test and restores only the base source.
    """
    contract = _contract()
    assert "keep the new test file" in contract
    assert "git checkout <base-sha> -- <changed source files>" in contract
    assert "must fail on the bug, not at import" in contract
    assert "red-on-base was not observed in the sandbox" in contract
    assert "in-sandbox verification was unavailable and CI is pending proof" in contract
    assert "a failure there returns the run to `implement`" in contract
    # Every step that used to restate the rules now defers to the contract.
    _, body = _skill_parts()
    flat = _flat(body)
    failing_test = flat.split("(phase `failing_test`)", 1)[1].split("(phase `implement`", 1)[0]
    implement = flat.split("(phase `implement`", 1)[1].split("(phase `review_diff`", 1)[0]
    review = flat.split("(phase `review_diff`", 1)[1].split("## Loop cap", 1)[0]
    publish = flat.split("(phase `publish`)", 1)[1].split("(phase `wait_ci`)", 1)[0]
    for name, step in (("failing_test", failing_test), ("implement", implement)):
        assert "verification contract" in step, name
    # The diff review prompt carries the current table the contract defines.
    assert "verification table" in review
    assert "verification contract" in publish


def test_both_reviewers_defer_to_one_contract_that_relaxes_only_service_evidence() -> None:
    """Neither reviewer demands evidence a valid delegation makes unobtainable (#3755, #3874)."""
    contract = _contract()
    assert (
        "neither reviewer asks for that test's local run results, real-service evidence, "
        "or an observed red-on-base run"
    ) in contract
    # The relaxation is bounded: these always block, at plan review and diff review alike.
    for reason in (
        "missing_coverage",
        "invalid_test",
        "failing_check",
        "missing_ci_route",
        "package_dependency",
    ):
        assert f"`{reason}`" in contract, reason
    assert "A missing package dependency is never a service gap." in contract
    assert (
        "Undeclared CI coverage and a failed check are never reclassified as a service gap."
    ) in contract
    assert (
        "A valid delegation removes only the demand for evidence this sandbox cannot "
        "produce, never another blocking finding."
    ) in contract
    for name in ("plan-reviewer", "diff-reviewer"):
        assert "verification contract" in _flat((BUNDLE / "agents" / f"{name}.md").read_text())


def test_delegation_binds_to_the_resolved_declaration_and_plan_review_judges_the_proposal() -> None:
    """A route is the resolved declaration's, and the plan's test is judged as proposed (#3874)."""
    contract = _contract()
    # The bundle's checks, when it declares any, shadow the repository's file.
    assert (
        "The resolved declaration is the bundle's `verification/checks.json` when it "
        "declares any checks"
    ) in contract
    assert "otherwise the repository's `.curie/verification.json`" in contract
    assert "whose startup result was `unavailable`" in contract
    assert ("A row relying on a repository declaration shadowed by bundle checks") in contract
    # At plan review the test does not exist yet.
    assert "the reviewer judges the proposed test as the plan describes it" in contract
    assert "never demands the written test" in contract
    assert "At diff review the reviewer reads the written test." in contract
