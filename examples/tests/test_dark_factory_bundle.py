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
