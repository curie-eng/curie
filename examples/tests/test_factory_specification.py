"""Shared authoring/form vectors prove shape, never deployment readiness."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from curie_factory_preparation.specification import CONTRACT_VERSION, parse_specification
from curie_factory_preparation.ticket_set import parse_ticket_set

VECTORS = Path(__file__).resolve().parents[2] / "tests/vectors/factory-specification"
SECTIONS = (
    "Problem",
    "Base",
    "Blocked by",
    "Decisions",
    "Preconditions",
    "Acceptance criteria",
    "Out of scope",
)


def body(name: str = "agent.md") -> str:
    return (VECTORS / name).read_text()


def envelope() -> dict[str, Any]:
    return json.loads((VECTORS / "complete-set.json").read_text())


def children() -> dict[str, str]:
    return {"storage": body(), "integration": body()}


def codes(result: Any) -> set[str]:
    assert not result.valid
    assert result.diagnostics
    return {item.code for item in result.diagnostics}


def test_agent_and_issue_form_have_one_contract() -> None:
    assert CONTRACT_VERSION == 1
    agent = parse_specification(body())
    form = parse_specification(body("issue-form.md"))
    assert agent.valid and form.valid
    assert agent.diagnostics == form.diagnostics == ()
    assert agent.specification is not None and form.specification is not None
    assert agent.specification.sections == form.specification.sections
    assert set(agent.specification.sections) == set(SECTIONS)
    assert agent.specification.criteria == form.specification.criteria
    assert [row.number for row in agent.specification.criteria] == [1, 2]
    first = agent.specification.criteria[0]
    assert first.text == "Reopening preserves the counter."
    assert first.verified_by == "uv run pytest tests/test_counter.py::test_reopen -q"
    assert first.runs_in == "sandbox"
    assert agent.specification.fingerprint == hashlib.sha256(body().encode()).hexdigest()
    assert agent.specification.fingerprint != form.specification.fingerprint


def test_exact_body_fingerprint_including_line_endings() -> None:
    document = body().replace("\n", "\r\n")
    result = parse_specification(document)
    assert result.valid and result.specification is not None
    assert result.specification.fingerprint == hashlib.sha256(document.encode()).hexdigest()


@pytest.mark.parametrize("style", ["inside", "outside"])
def test_case_closing_hashes_and_bold_fields(style: str) -> None:
    document = body()
    for section in SECTIONS:
        document = document.replace(f"## {section}\n", f"### {section.upper()} ###\n")
    for label in ("Verified by", "Runs in"):
        bold = f"**{label}:**" if style == "inside" else f"**{label}**:"
        document = document.replace(f"{label}:", bold)
    parsed = parse_specification(document)
    assert parsed.valid and parsed.specification is not None
    original = parse_specification(body()).specification
    assert original is not None
    assert parsed.specification.criteria == original.criteria


@pytest.mark.parametrize("section", SECTIONS)
def test_missing_and_duplicate_sections_are_field_specific(section: str) -> None:
    missing = body().replace(f"## {section}\n", "## Commentary\n")
    result = parse_specification(missing)
    assert result.specification is None
    assert "spec_missing_section" in codes(result)
    assert any(d.section == section for d in result.diagnostics)
    duplicated = body() + f"\n## {section}\nRepeated body\n"
    result = parse_specification(duplicated)
    assert "spec_duplicate_section" in codes(result)
    assert any(d.section == section for d in result.diagnostics)


@pytest.mark.parametrize(
    "old,new,code",
    [
        ("next", "", "spec_empty_section"),
        ("next", "None", "spec_invalid_none"),
        ("Counter UI changes.", "None", "spec_invalid_none"),
        ("2. The integration", "3. The integration", "spec_invalid_criteria"),
        ("1. Reopening", "0. Reopening", "spec_invalid_criteria"),
        (
            "   Verified by: uv run pytest tests/test_counter.py::test_reopen -q\n",
            "",
            "spec_missing_field",
        ),
        ("   Runs in: sandbox", "   Runs in: sandbox\n   Runs in: sandbox", "spec_duplicate_field"),
        ("Verified by: counter-integration required check", "Verified by:", "spec_empty_field"),
        ("Runs in: sandbox", "Runs in: laptop", "spec_invalid_runs_in"),
        ("Runs in: ci:counter-integration", "Runs in: ci:", "spec_invalid_runs_in"),
        (
            "Runs in: ci:counter-integration",
            "Runs in: ci:<required check name>",
            "spec_invalid_runs_in",
        ),
    ],
)
def test_malformed_fields_have_stable_codes(old: str, new: str, code: str) -> None:
    document = body().replace(old, new)
    result = parse_specification(document)
    assert result.specification is None
    assert code in codes(result)
    assert result.diagnostics == parse_specification(document).diagnostics


def test_literal_blocks_cannot_forge_structure() -> None:
    literal = "\n```markdown\n## Base\n1. forged\n   Runs in: laptop\n```\n"
    document = body().replace("## Base", literal + "\n## Base", 1)
    document = document.replace(
        "## Out of scope", "\n    ## Base\n    99. forged\n\n## Out of scope"
    )
    result = parse_specification(document)
    assert result.valid and result.specification is not None
    assert len(result.specification.criteria) == 2
    # Numbered verification examples nested beneath a criterion are not new ACs.
    document = body().replace(
        "   Runs in: sandbox", "     1. Nested command example\n   Runs in: sandbox"
    )
    result = parse_specification(document)
    assert result.valid and result.specification is not None
    assert len(result.specification.criteria) == 2


def test_injection_is_inert_and_diagnostics_and_repr_are_safe(tmp_path: Path) -> None:
    marker = tmp_path / "must-not-exist"
    secret = "SYNTHETIC_CREDENTIAL_DO_NOT_ECHO_0123456789"
    document = (
        body()
        .replace("next", secret)
        .replace(
            "uv run pytest tests/test_counter.py::test_reopen -q",
            f"$(touch {marker}); echo {secret}",
        )
    )
    result = parse_specification(document)
    assert result.valid
    assert not marker.exists()
    assert secret not in repr(result)
    result = parse_specification(document.replace("Runs in: sandbox", f"Runs in: {secret}"))
    assert "spec_invalid_runs_in" in codes(result)
    assert secret not in repr(result)
    for diagnostic in result.diagnostics:
        assert secret not in diagnostic.message
        assert diagnostic.path.startswith("/")
    assert not marker.exists()


def test_published_malformed_vector() -> None:
    result = parse_specification(body("malformed.md"))
    assert "spec_missing_field" in codes(result)
    assert any(d.section == "Acceptance criteria" and d.criterion == 1 for d in result.diagnostics)


@pytest.mark.parametrize("literal", ["```\ncommand\n```", "    command"])
def test_literal_command_does_not_rescue_empty_verified_by(literal: str) -> None:
    document = body().replace(
        "Verified by: uv run pytest tests/test_counter.py::test_reopen -q",
        "Verified by:\n" + literal,
    )
    assert "spec_empty_field" in codes(parse_specification(document))


def test_complete_set_roundtrips_and_order_is_canonical() -> None:
    data = envelope()
    result = parse_ticket_set(json.dumps(data), children())
    assert result.valid and result.ticket_set is not None
    assert result.diagnostics == ()
    canonical = result.ticket_set.canonical_json()
    assert isinstance(canonical, str)
    assert result.ticket_set.fingerprint == hashlib.sha256(canonical.encode()).hexdigest()
    again = parse_ticket_set(canonical, children())
    assert again.valid and again.ticket_set is not None
    assert again.ticket_set.canonical_json() == canonical
    for key in ("requirements", "children", "coverage"):
        data[key].reverse()
    reordered = parse_ticket_set(json.dumps(data), children())
    assert reordered.valid and reordered.ticket_set is not None
    assert reordered.ticket_set.canonical_json() == canonical


@pytest.mark.parametrize(
    "kind,code",
    [
        ("duplicate_id", "set_duplicate_id"),
        ("dangling", "set_unknown_reference"),
        ("duplicate_edge", "set_duplicate_dependency"),
        ("cycle", "set_dependency_cycle"),
        ("self_cycle", "set_dependency_cycle"),
        ("uncovered", "set_uncovered_requirement"),
        ("criterion", "set_invalid_criterion"),
        ("integration_criterion", "set_invalid_criterion"),
        ("missing_integration", "set_missing_integration"),
        ("fingerprint", "set_fingerprint_mismatch"),
        ("unreviewed", "set_unreviewed_exclusion"),
        ("extra_field", "set_invalid_schema"),
        ("bool_version", "set_invalid_schema"),
        ("bool_criterion", "set_invalid_schema"),
        ("empty_requirements", "set_invalid_schema"),
        ("bad_id", "set_invalid_schema"),
    ],
)
def test_invalid_set_structure(kind: str, code: str) -> None:
    data = envelope()
    if kind == "duplicate_id":
        data["children"].append(copy.deepcopy(data["children"][0]))
    elif kind == "dangling":
        data["coverage"][0]["child"] = "missing"
    elif kind == "duplicate_edge":
        data["dependencies"].append(copy.deepcopy(data["dependencies"][0]))
    elif kind == "cycle":
        data["dependencies"].append({"child": "storage", "prerequisite": "integration"})
    elif kind == "self_cycle":
        data["dependencies"][0]["prerequisite"] = "integration"
    elif kind == "uncovered":
        data["coverage"].pop()
    elif kind == "criterion":
        data["coverage"][0]["criterion"] = 999
    elif kind == "integration_criterion":
        data["integration"]["criterion"] = 999
    elif kind == "missing_integration":
        del data["integration"]
    elif kind == "fingerprint":
        data["children"][0]["specification_fingerprint"] = "0" * 64
    elif kind == "unreviewed":
        data["coverage"].pop()
        data["scope_exclusions"] = [
            {"requirement": "integrate", "reason": "Deferred", "reviewed": False}
        ]
    elif kind == "extra_field":
        data["source"]["unexpected"] = "unexpected"
    elif kind == "bool_version":
        data["version"] = True
    elif kind == "bool_criterion":
        data["coverage"][0]["criterion"] = True
    elif kind == "empty_requirements":
        data["requirements"] = []
    elif kind == "bad_id":
        data["children"][0]["id"] = "../file.md"
    result = parse_ticket_set(json.dumps(data), children())
    assert result.ticket_set is None
    assert code in codes(result)
    assert result.diagnostics == parse_ticket_set(json.dumps(data), children()).diagnostics


def test_reviewed_exclusion_is_explicit_and_structural() -> None:
    data = envelope()
    data["coverage"].pop()
    data["scope_exclusions"] = [
        {
            "requirement": "integrate",
            "reason": "Developer deferred this requirement",
            "reviewed": True,
        }
    ]
    result = parse_ticket_set(json.dumps(data), children())
    assert result.valid
    assert not hasattr(result, "ready")


def test_missing_or_invalid_child_body_is_rejected() -> None:
    supplied = children()
    del supplied["storage"]
    assert "set_invalid_child_specification" in codes(
        parse_ticket_set(json.dumps(envelope()), supplied)
    )
    supplied = children()
    supplied["storage"] = body("malformed.md")
    assert "set_invalid_child_specification" in codes(
        parse_ticket_set(json.dumps(envelope()), supplied)
    )
    supplied = children()
    supplied["storage"] += "\n"
    assert "set_fingerprint_mismatch" in codes(parse_ticket_set(json.dumps(envelope()), supplied))


def test_set_errors_are_safe_even_for_attacker_keys_and_ids() -> None:
    secret = "SYNTHETIC_CREDENTIAL_DO_NOT_ECHO_0123456789"
    data = envelope()
    data["coverage"][0]["child"] = secret
    result = parse_ticket_set(json.dumps(data), children())
    assert "set_unknown_reference" in codes(result)
    assert secret not in repr(result)
    data = envelope()
    data[secret] = secret
    result = parse_ticket_set(json.dumps(data), children())
    assert "set_invalid_schema" in codes(result)
    assert secret not in repr(result)
    result = parse_ticket_set('{"' + secret, children())
    assert "set_invalid_json" in codes(result)
    assert secret not in repr(result)


def test_comment_marker_inside_fence_cannot_hide_later_sections() -> None:
    document = body().replace(
        "## Base", "```text\n<!-- an inert example of an unclosed comment\n```\n\n## Base", 1
    )
    assert parse_specification(document).valid


def test_second_comment_opener_on_one_line_hides_commented_section() -> None:
    document = body().replace("## Base\nnext", "<!-- closed --> <!-- open\n## Base\nnext\n-->")
    result = parse_specification(document)
    assert "spec_missing_section" in codes(result)
    assert any(d.section == "Base" for d in result.diagnostics)


def test_indented_comment_closer_restores_following_sections() -> None:
    document = body().replace("## Base", "<!-- open\n    -->\n## Base", 1)
    assert parse_specification(document).valid


@pytest.mark.parametrize(
    "old,new,code",
    [
        ("## Problem", "#<!-- --># Problem", "spec_missing_section"),
        (
            "Verified by: uv run pytest tests/test_counter.py::test_reopen -q",
            "Verified<!-- --> by: uv run pytest tests/test_counter.py::test_reopen -q",
            "spec_missing_field",
        ),
    ],
)
def test_comment_lines_cannot_synthesize_structural_tokens(old: str, new: str, code: str) -> None:
    assert code in codes(parse_specification(body().replace(old, new)))


def test_invalid_utf8_has_safe_diagnostic() -> None:
    result = parse_specification(body() + "\ud800")
    assert "spec_invalid_encoding" in codes(result)
    assert result.specification is None
    assert "\\ud800" not in repr(result)


def test_duplicate_json_keys_are_refused() -> None:
    document = json.dumps(envelope()).replace('"version": 1', '"version": 1, "version": 1')
    assert "set_invalid_json" in codes(parse_ticket_set(document, children()))


def test_three_child_cycle_is_refused() -> None:
    data = envelope()
    data["children"].append(
        {
            "id": "third",
            "specification_fingerprint": data["children"][0]["specification_fingerprint"],
        }
    )
    data["dependencies"] = [
        {"child": "storage", "prerequisite": "integration"},
        {"child": "integration", "prerequisite": "third"},
        {"child": "third", "prerequisite": "storage"},
    ]
    supplied = children() | {"third": body()}
    assert "set_dependency_cycle" in codes(parse_ticket_set(json.dumps(data), supplied))


def test_unknown_integration_owner_is_refused() -> None:
    data = envelope()
    data["integration"]["child"] = "unknown"
    result = parse_ticket_set(json.dumps(data), children())
    assert "set_unknown_reference" in codes(result)
    assert any(d.path == "/integration/child" for d in result.diagnostics)


def test_duplicate_coverage_and_exclusions_are_refused() -> None:
    data = envelope()
    data["coverage"].append(copy.deepcopy(data["coverage"][0]))
    assert "set_duplicate_coverage" in codes(parse_ticket_set(json.dumps(data), children()))
    data = envelope()
    data["coverage"].pop()
    exclusion = {"requirement": "integrate", "reason": "Explicitly deferred", "reviewed": True}
    data["scope_exclusions"] = [exclusion, copy.deepcopy(exclusion)]
    assert "set_duplicate_exclusion" in codes(parse_ticket_set(json.dumps(data), children()))
