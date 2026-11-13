"""Recorded fresh-context decisions with executable semantic and control graders.

Render with ``uv run python examples/tests/test_factory_triage_evals.py render
CASE VARIANT``. Record an actual model reply with ``record CASE VARIANT FILE``.
Recordings retain the full reply, prompt hash and operation log for regrading.
"""

from __future__ import annotations

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
    return {case["id"]: case for case in json.loads((FIXTURES / "cases.json").read_text())["cases"]}


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
        + policy
        + "\n\nPreparation reference:\n"
        + SPECIFICATION.read_text()
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
    if value["kind"] != expected["kind"]:
        return ["The proposal chose the wrong ending."]
    if value["kind"] == "ticket":
        reproduction = value["reproduction"]
        for field in ("command", "revision"):
            if reproduction[field] != expected[field]:
                errors.append(f"The reproduction {field} does not match the observed probe.")
        if expected["output"] not in reproduction["observed"]:
            errors.append("The reproduction observation does not quote the actual failure.")
        body = value["body"]
        for section in SECTIONS:
            if not re.search(r"^#{2,3} " + re.escape(section) + r"\s*$", body, re.M | re.I):
                errors.append(f"Ticket lacks the substantive {section} section.")
        if "Verified by:" not in body or "Runs in:" not in body:
            errors.append("Ticket lacks executable verification and its route.")
        if expected["revision"] not in body or "python3 -m pytest" not in body:
            errors.append("Ticket lacks the inspected base or feasible repository test route.")
    else:
        if value["reason"] != expected["reason"]:
            errors.append("The findings chose the wrong reason.")
        evidence = value["tried"].lower()
        missing = value["missing"].lower()
        if not any(token.lower() in evidence for token in expected["evidence"]):
            errors.append("Findings omit the actual probe or source evidence.")
        if not any(token.lower() in missing for token in expected["missing"]):
            errors.append("Findings omit the remaining evidence or behavioral choice.")
        if expected.get("no_fixed_claim") and re.search(
            r"\b(?:already fixed|was fixed)\b", evidence
        ):
            errors.append("A passing probe was incorrectly claimed to prove a landed fix.")
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
    assert grade(case_id, recorded(case_id, "without-evidence"))


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
    else:
        raise SystemExit("Expected render or record")
