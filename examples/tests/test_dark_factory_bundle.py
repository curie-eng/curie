"""The default dark-factory agent bundle validates and holds its discipline (#2576).

Pins the parts of ``examples/dark-factory`` that must not drift: the bundle
validates, it declares no MCP server, secret or toolPolicy and reads its
issue through the platform's ``mcp__curie__get_issue`` (ADR 0187), the one
skill states the factory discipline and its nine phases, the evals are
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


LOOPED_PHASES = {"plan", "plan_review", "implement", "review_diff"}


def _phase_declaration() -> dict:
    return json.loads((BUNDLE / "progress" / "phases.json").read_text())


def test_progress_declaration_lists_the_skill_phases_in_order() -> None:
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


def _step_sections(body: str) -> dict[str, str]:
    """Each numbered step's text, keyed by its phase id, up to the next heading."""

    sections: dict[str, str] = {}
    parts = re.split(r"^(## .*)$", body, flags=re.MULTILINE)
    for index in range(1, len(parts), 2):
        heading = parts[index]
        match = re.match(r"^## \d+\. .*\(phase `(\w+)`", heading)
        if match:
            sections[match.group(1)] = parts[index + 1]
    return sections


def test_every_step_reports_its_own_phase_through_report_progress() -> None:
    _, body = _skill_parts()
    sections = _step_sections(body)
    assert list(sections) == PHASES
    for phase, text in sections.items():
        if phase == "wait_ci":
            # The agent's turn ends at publication; the platform reports it (#3179).
            assert "report_progress" not in text
            assert re.search(r"platform reports `wait_ci`", text)
            continue
        assert "report_progress" in text, phase
        assert re.search(rf"report_progress[^\n]*\b{phase}\b", text), phase
        if phase in LOOPED_PHASES:
            assert re.search(r"report_progress[^\n]*\bround\b", text), phase


def test_phases_section_names_the_platform_progress_tool() -> None:
    _, body = _skill_parts()
    phases = body.split("## Phases", 1)[1].split("\n## ", 1)[0]
    assert "mcp__curie__report_progress" in phases
    assert re.search(r"never blocks|continue the work", phases, re.IGNORECASE)


# --- #3195: report each phase once; never end a message without a tool call ---------


def _phases_section() -> str:
    """The `## Phases` section, whitespace-normalized so line wrapping cannot
    break a sentence-level assertion (#3195)."""
    _, body = _skill_parts()
    return re.sub(r"\s+", " ", body.split("## Phases", 1)[1].split("\n## ", 1)[0])


def test_phases_section_reports_each_phase_once_on_entry() -> None:
    """One report_progress per phase entry, not one per turn while working (#3195)."""
    assert re.search(
        r"report_progress`? once when you enter a phase, not while you work in it",
        _phases_section(),
    )


def test_phases_section_bars_a_message_without_a_tool_call() -> None:
    """A message without a tool call ends the run; only the stop may be all text (#3195)."""
    assert re.search(
        r"Every message you send must include a tool call"
        r".{0,220}publish_changes"
        r".{0,220}Could not complete:"
        r".{0,220}A message without a tool call ends the run",
        _phases_section(),
    )


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


# --- #3196: approve with non-blocking notes ---------------------------------------


def test_skill_carries_approved_notes_into_implement() -> None:
    _, body = _skill_parts()
    assert "## Reviewer notes" in body
    section = body.split("## Reviewer notes", 1)[1].split("\n## ", 1)[0]
    assert "NOTES:" in section
    assert "approval ends that review loop" in section
    # Plan notes ride into implement; diff notes never edit approved code.
    assert re.search(r"plan review.{0,120}?`implement`", section, re.IGNORECASE | re.DOTALL)
    assert "Never apply diff-review notes to the code" in section
    assert re.search(r"List\s+diff-review notes in the pull request body", section)
    assert "Never start another review round only to address notes" in section


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


def test_operations_doc_points_at_the_bundle() -> None:
    assert "examples/dark-factory" in (REPO_ROOT / "docs" / "operations.md").read_text()


def test_example_deploys_as_dark_factory_on_the_default_model() -> None:
    """The shipped example deploys under the product name on GLM 5.3 Flash (#3075)."""

    readme = (BUNDLE / "README.md").read_text()
    operations = (REPO_ROOT / "docs" / "operations.md").read_text()
    assert "--agent dark-factory " in readme
    assert "surfaces dark-factory " in readme
    assert "publication-policy dark-factory " in readme
    assert "--github-api-egress" not in readme
    assert "--agent factory " not in readme
    assert "cluster up --model z-ai/glm-5.3-flash" in readme
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
    assert "10800" in section
    assert "does not act on them yet" not in section


# --- #3194: publish reads the repository's PR conventions ---------------------------


def test_publish_section_reads_repository_pr_conventions() -> None:
    _, body = _skill_parts()
    section = _section(body, 8)
    assert "(phase `publish`)" in section.splitlines()[0]
    # The conventions are read before the publication is requested.
    assert section.index("AGENTS.md") < section.index("mcp__curie__publish_changes")
    assert "CONTRIBUTING.md" in section
    assert re.search(r"pull\s+request\s+template", section)
    assert "CI job" in section
    assert re.search(r"pull\s+request\s+bod", section)
    # ...and followed, including required trailers and selectors.
    assert re.search(
        r"follow (them|those conventions).{0,80}(trailer|selector)",
        section,
        re.IGNORECASE | re.DOTALL,
    )
    # The skill stays repository-agnostic: it names where conventions live,
    # never a specific repository's rules (#3194).
    assert "Fix pin" not in body


def test_skill_names_three_review_loops_including_wait_ci() -> None:
    _, body = _skill_parts()
    assert "Two\npairs loop" not in body
    assert re.search(r"`wait_ci`.{0,80}`implement`", body, re.DOTALL)


def test_readme_describes_the_ci_wait_and_fix_loop() -> None:
    readme = (BUNDLE / "README.md").read_text()
    assert "does not act on the checks yet" not in readme
    assert re.search(r"`wait_ci`.{0,80}`implement`", readme, re.DOTALL)
    assert "unverified" in readme.lower()


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
