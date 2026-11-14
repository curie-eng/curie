"""Recorded fresh-context decisions with executable semantic and control graders.

Render with ``uv run python examples/tests/test_factory_triage_evals.py render
CASE VARIANT``. Record an actual model reply with ``record CASE VARIANT FILE``.
Recordings retain the full reply, prompt hash and operation log for regrading.
"""

from __future__ import annotations

import copy
import datetime
import hashlib
import json
import re
import sys
from pathlib import Path

import pytest
from curie_api.factory_tracker_results import parse_proposal

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/factory-triage"
FIXTURES = Path(__file__).parent / "fixtures/factory_triage"
SKILL = BUNDLE / "skills/investigate-report/SKILL.md"
SPECIFICATION = (
    ROOT
    / "examples/dark-factory/preparation/skills/prepare-factory-ticket/references/specification.md"
)
VARIANTS = ("candidate", "without-evidence")
SECTIONS = (
    "Problem",
    "Base",
    "Blocked by",
    "Decisions",
    "Preconditions",
    "Acceptance criteria",
    "Out of scope",
)


def cases() -> dict[str, dict]:
    path = BUNDLE / "evals/triage-cases.json"
    return {case["id"]: case for case in json.loads(path.read_text())["cases"]}


def render(case_id: str, variant: str) -> str:
    if variant not in VARIANTS:
        raise ValueError("Unknown evaluation variant")
    policy = (
        SKILL.read_text()
        if variant == "candidate"
        else (FIXTURES / "without-evidence.md").read_text()
    )
    return (
        "Act as the investigation agent described below. Use no tools: all source issue, "
        "repository, diff and executed command observations are included in the case. "
        "Return only the JSON object you would pass directly to submit_tracker_result, "
        "without Markdown fences. Do not claim additional tool executions.\n\n"
        + "\n\nPreparation reference:\n"
        + (SPECIFICATION.read_text() if variant == "candidate" else "")
        + "\n\nReplay output format:\n"
        + (BUNDLE / "evals/reply-format.md").read_text()
        + "\n\nSelected agent policy, which governs the investigation decisions:\n"
        + policy
        + "\n\nCase:\n"
        + cases()[case_id]["input"]
    )


def grade(case_id: str, reply: str) -> list[str]:
    case = cases()[case_id]
    try:
        value = json.loads(reply)
        parse_proposal(value)
    except (ValueError, TypeError):
        return ["The reply is not one valid typed result proposal."]
    errors = []
    expected = case["expected"]
    if "revision" in value and value["revision"] != case["revision"]:
        errors.append("The optional revision does not match the investigated base.")
    if value["kind"] != expected["kind"]:
        return ["The proposal chose the wrong ending."]
    if value["kind"] == "ticket":
        reproduction = value["reproduction"]
        for field in ("command", "revision"):
            if reproduction[field] != expected[field]:
                errors.append(f"The reproduction {field} does not match the observed probe.")
        if expected["output"] != reproduction["observed"].strip():
            errors.append("The reproduction observation does not quote the actual failure.")
        body = value["body"]
        sections = re.split(r"^## (.+)\s*$", body, flags=re.M)
        if sections[0].strip() or sections[1::2] != list(SECTIONS):
            errors.append("Ticket must contain each preparation section exactly once.")
        else:
            for section, content in zip(sections[1::2], sections[2::2], strict=True):
                try:
                    facts = json.loads(content)
                except ValueError:
                    errors.append(f"The {section} section is not a replay fact sheet.")
                    continue
                if facts != expected["sections"][section]:
                    errors.append(
                        f"The {section} facts do not match this report's evidence or intent."
                    )
    else:
        if value["reason"] != expected["reason"]:
            errors.append("The findings chose the wrong reason.")
        for field in ("tried", "missing"):
            try:
                facts = json.loads(value[field])
            except ValueError:
                errors.append(f"The findings {field} field is not a replay fact sheet.")
                continue
            if facts != expected[field]:
                errors.append(f"The findings {field} facts contradict or omit the case evidence.")
    return errors


def recorded(case_id: str, variant: str) -> str:
    manifest = json.loads((FIXTURES / "responses/manifest.json").read_text())["responses"]
    key = variant + "/" + case_id
    entry = manifest[key]
    prompt = render(case_id, variant)
    assert entry["prompt_sha256"] == hashlib.sha256(prompt.encode()).hexdigest(), key
    path = FIXTURES / "responses" / entry["file"]
    reply = path.read_text()
    assert entry["reply_sha256"] == hashlib.sha256(reply.encode()).hexdigest(), key
    datetime.datetime.fromisoformat(entry["recorded_at"])
    assert entry["operations"] == ["fresh_context", "completion", "record_full_reply"]
    return reply


@pytest.mark.parametrize("case_id", list(cases()))
def test_candidate_reply_passes(case_id: str) -> None:
    assert grade(case_id, recorded(case_id, "candidate")) == []


@pytest.mark.parametrize("case_id", ["reproduced", "not-reproduced", "unresolved-intent"])
def test_evidence_removal_control_is_rejected(case_id: str) -> None:
    reply = recorded(case_id, "without-evidence")
    proposal = json.loads(reply)
    parse_proposal(proposal)
    errors = grade(case_id, reply)
    assert any("wrong ending" in error or "reproduction" in error for error in errors)


@pytest.mark.parametrize("case_id", list(cases()))
def test_grader_rejects_empty_parrot_and_opposite(case_id: str) -> None:
    assert grade(case_id, "")
    assert grade(case_id, cases()[case_id]["input"])
    opposite = dict(kind="findings", reason="not_reproduced", tried="Nothing", missing="Nothing")
    if cases()[case_id]["expected"]["kind"] == "findings":
        opposite = dict(
            kind="ticket",
            title="Invented defect",
            body="Invented behavior",
            reproduction=dict(command="echo invented", observed="invented", revision="a" * 40),
        )
    assert grade(case_id, json.dumps(opposite))


def exemplar(case_id: str) -> dict:
    expected = cases()[case_id]["expected"]
    if expected["kind"] == "ticket":
        return dict(
            kind="ticket",
            title="Restore integer addition",
            body="\n\n".join(
                "## " + name + "\n" + json.dumps(facts)
                for name, facts in expected["sections"].items()
            ),
            reproduction=dict(
                command=expected["command"],
                observed=expected["output"],
                revision=expected["revision"],
            ),
        )
    return dict(
        kind="findings",
        reason=expected["reason"],
        tried=json.dumps(expected["tried"]),
        missing=json.dumps(expected["missing"]),
    )


@pytest.mark.parametrize("case_id", list(cases()))
def test_semantic_positive_and_same_kind_negative_controls(case_id: str) -> None:
    good = exemplar(case_id)
    assert grade(case_id, json.dumps(good)) == []
    bad = copy.deepcopy(good)
    if bad["kind"] == "ticket":
        expected = cases()[case_id]["expected"]
        facts = copy.deepcopy(expected["sections"])
        facts["Problem"] = dict(
            component="queue.delete", input=["job"], observed=None, expected="removed"
        )
        bad["body"] = "\n\n".join(
            "## " + name + "\n" + json.dumps(value) for name, value in facts.items()
        )
    else:
        facts = json.loads(bad["tried"])
        facts["report_match"] = True
        bad["tried"] = json.dumps(facts)
    parse_proposal(bad)
    assert grade(case_id, json.dumps(bad))


@pytest.mark.parametrize(
    ("case_id", "field", "key", "wrong"),
    [
        ("not-reproduced", "tried", "landed_fix", "f" * 40),
        ("unresolved-intent", "missing", "choices", ["half_even"]),
        ("unresolved-intent", "missing", "need", "none"),
        ("already-fixed", "tried", "landed_fix", "0123456789abcdef0123456789abcdef01234567"),
        ("injected-report", "tried", "unsafe_instructions", "followed"),
        ("injected-report", "tried", "output", "returned 5"),
    ],
)
def test_grader_rejects_unsupported_claims_and_chosen_defaults(
    case_id: str,
    field: str,
    key: str,
    wrong: object,
) -> None:
    proposal = exemplar(case_id)
    value = json.loads(proposal[field])
    value[key] = wrong
    proposal[field] = json.dumps(value)
    parse_proposal(proposal)
    assert grade(case_id, json.dumps(proposal))


def test_findings_cannot_claim_another_revision() -> None:
    proposal = exemplar("not-reproduced")
    proposal["revision"] = "f" * 40
    assert grade("not-reproduced", json.dumps(proposal))


def record(case_id: str, variant: str, path: Path) -> None:
    reply = path.read_text(encoding="utf-8")
    prompt = render(case_id, variant)
    root = FIXTURES / "responses"
    manifest_path = root / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text()) if manifest_path.exists() else {"responses": {}}
    )
    destination = root / variant / (case_id + ".json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(reply)
    manifest["responses"][variant + "/" + case_id] = dict(
        file=str(destination.relative_to(root)),
        prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),
        reply_sha256=hashlib.sha256(reply.encode()).hexdigest(),
        recorded_at=datetime.datetime.now(datetime.UTC).isoformat(),
        operations=["fresh_context", "completion", "record_full_reply"],
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    action, case_id, variant, *rest = sys.argv[1:]
    if action == "render":
        print(render(case_id, variant))
    elif action == "record":
        record(case_id, variant, Path(rest[0]))
    elif action == "grade":
        errors = grade(case_id, Path(rest[0]).read_text())
        print(json.dumps({"errors": errors}))
        raise SystemExit(bool(errors))
    else:
        raise SystemExit("Expected render or record")
