"""Grader and result-rollup unit tests (pure, no server)."""

from __future__ import annotations

from pathlib import Path

import pytest
from curie_worker.eval import (
    EvalCase,
    EvalCaseResult,
    EvalOutcome,
    EvalRunResult,
    EvalSuite,
    ExpectedStatus,
    Grader,
    GraderKind,
)
from pydantic import ValidationError


def test_exact_grader_trims_and_ignores_case_by_default() -> None:
    g = Grader(kind=GraderKind.EXACT, expected="Paris")
    assert g.grade("  paris ") is True
    assert g.grade("Paris, France") is False


def test_contains_grader() -> None:
    g = Grader(kind=GraderKind.CONTAINS, expected="4")
    assert g.grade("the answer is 4") is True
    assert g.grade("five") is False


def test_regex_grader() -> None:
    g = Grader(kind=GraderKind.REGEX, expected=r"\b4\b")
    assert g.grade("result: 4 units") is True
    assert g.grade("result: 42") is False


def test_tool_called_grader_reads_the_trajectory_not_the_text() -> None:
    # ADR-0022 Phase 1: a tool_called grader asserts a named tool was invoked,
    # read from the observed trajectory -- NOT grepped from the answer text.
    g = Grader(kind=GraderKind.TOOL_CALLED, expected="DeterministicEngine")
    # GREEN: the tool appears in the trajectory.
    assert g.grade("", ["DeterministicEngine"]) is True
    assert g.grade("", ["Bash", "DeterministicEngine", "Read"]) is True
    # RED: the tool was never called, whatever the final text claims.
    assert g.grade("I ran the DeterministicEngine tool", []) is False
    assert g.grade("DeterministicEngine", ["Bash", "Read"]) is False
    # Tool names are exact identifiers; the case_sensitive flag does not fold them.
    assert Grader(kind=GraderKind.TOOL_CALLED, expected="Bash").grade("", ["bash"]) is False


def test_tool_called_grader_rejects_an_empty_tool_name() -> None:
    # An empty tool name can never be satisfied, so it is a suite-authoring
    # mistake rejected at construction (deny-by-default).
    with pytest.raises(ValidationError):
        Grader(kind=GraderKind.TOOL_CALLED, expected="  ")


def test_case_sensitivity_is_honored() -> None:
    insensitive = Grader(kind=GraderKind.CONTAINS, expected="PARIS")
    assert insensitive.grade("paris") is True
    sensitive = Grader(kind=GraderKind.CONTAINS, expected="PARIS", case_sensitive=True)
    assert sensitive.grade("paris") is False


def test_run_result_rollups() -> None:
    result = EvalRunResult(
        version="v1",
        suite="basics",
        results=[
            EvalCaseResult(case_id="a", outcome=EvalOutcome.PASS, output="4", latency_ms=1.0),
            EvalCaseResult(case_id="b", outcome=EvalOutcome.FAIL, output="x", latency_ms=1.0),
            EvalCaseResult(case_id="c", outcome=EvalOutcome.PASS, output="ok", latency_ms=1.0),
        ],
    )
    assert result.total == 3
    assert result.passed_count == 2
    assert result.summary() == "2/3 passed"
    assert result.all_passed() is False


def test_all_passed_is_false_for_empty_suite() -> None:
    assert EvalRunResult(version="v", suite="s", results=[]).all_passed() is False


def _row(case_id: str, outcome: EvalOutcome) -> EvalCaseResult:
    return EvalCaseResult(case_id=case_id, outcome=outcome, output="all done", latency_ms=1.0)


def test_passed_is_derived_from_outcome_and_a_plumbing_row_is_neither() -> None:
    # `passed` is a read-only view of `outcome`, so no caller can ever set the two
    # inconsistently, and a non-graded row reports null: not a pass, not a fail.
    assert _row("p", EvalOutcome.PASS).passed is True
    assert _row("f", EvalOutcome.FAIL).passed is False
    assert _row("k", EvalOutcome.PLUMBING_OK).passed is None
    # The derived value survives serialization, which is what the recorder writes
    # into trace metadata and what the matrix reads back.
    assert _row("k", EvalOutcome.PLUMBING_OK).model_dump()["passed"] is None
    assert _row("k", EvalOutcome.PLUMBING_OK).model_dump()["outcome"] == "plumbing_ok"


def test_an_all_plumbing_run_is_never_a_pass_but_is_a_clean_completion() -> None:
    run = EvalRunResult(
        version="v1",
        suite="s",
        results=[_row("a", EvalOutcome.PLUMBING_OK), _row("b", EvalOutcome.PLUMBING_OK)],
    )
    # Nothing was graded, so nothing passed: `all_passed` stays a pure grading
    # predicate and must not go green on rows no grader ever judged.
    assert run.passed_count == 0
    assert run.all_passed() is False
    # ...but nothing broke either, so the eval Job's process exit is success.
    assert run.completed_without_failure() is True
    # The operator-facing rollup must read as plumbing, never as "0/2 passed" --
    # that number is a lie in the other direction (no case failed).
    summary = run.summary()
    assert "plumbing" in summary.lower()
    assert "0/2" not in summary


def test_a_plumbing_run_carrying_a_failure_is_not_a_clean_completion() -> None:
    # The fake tier's ONLY assertion is that the turn completed; when one did not,
    # the plumbing is genuinely broken and the run must still be operationally red.
    run = EvalRunResult(
        version="v1",
        suite="s",
        results=[_row("a", EvalOutcome.PLUMBING_OK), _row("b", EvalOutcome.FAIL)],
    )
    assert run.completed_without_failure() is False
    assert run.all_passed() is False


# --- Frozen eval-case contract (issue #8) -------------------------------------
# These lock Section 3 of the plan: a suite must carry at least one case, a regex
# grader compiles eagerly, the committed cross-language fixture is
# platform-loadable, and the retired array form no longer loads. They fail
# against current main because models.py lacks the min_length and the regex
# validator.


def test_empty_suite_is_invalid() -> None:
    """A suite with zero cases can never pass, so the frozen schema rejects it."""
    with pytest.raises(ValidationError):
        EvalSuite(name="s", cases=[])


def test_invalid_regex_grader_rejected_at_parse() -> None:
    """An uncompilable regex pattern is rejected eagerly when the grader is built."""
    with pytest.raises(ValidationError):
        Grader(kind=GraderKind.REGEX, expected="(unclosed")


def test_valid_regex_grader_constructs_and_grades() -> None:
    """A valid regex grader builds and searches the output."""
    grader = Grader(kind=GraderKind.REGEX, expected="wea.her")
    assert grader.grade("weather") is True


def test_committed_fixture_parses_and_grades(eval_cases_example_path: Path) -> None:
    """The committed cross-language fixture loads as an EvalSuite, proving the
    scaffold output is platform-loadable (the latent bug in issue #8). Its smoke
    grader now asserts the agent named itself (#527), so it is falsifiable: an
    empty/off-topic turn fails, an introduction naming the agent passes."""
    suite = EvalSuite.model_validate_json(
        eval_cases_example_path.read_text(encoding="utf-8")
    )
    assert len(suite.cases) == 1
    grader = suite.cases[0].grader
    assert grader.kind == GraderKind.CONTAINS
    assert grader.expected == "example"
    assert grader.grade("I am the example agent.") is True
    assert grader.grade("literally anything") is False
    assert grader.grade("") is False


def test_expect_status_defaults_to_done() -> None:
    """A case with no expect_status defaults to `done`, keeping every pre-existing
    case byte-identical in behavior (issue #262)."""
    case = EvalCase(id="c", input="i", grader=Grader(kind=GraderKind.CONTAINS, expected="x"))
    assert case.expect_status is ExpectedStatus.DONE


def test_expect_status_awaiting_approval_round_trips() -> None:
    """An `awaiting-approval` case (the gate-blocked assertion) constructs and
    survives a JSON round-trip through the frozen schema."""
    case = EvalCase(
        id="c",
        input="i",
        grader=Grader(kind=GraderKind.CONTAINS, expected="x"),
        expect_status=ExpectedStatus.AWAITING_APPROVAL,
    )
    reloaded = EvalCase.model_validate_json(case.model_dump_json())
    assert reloaded.expect_status is ExpectedStatus.AWAITING_APPROVAL


def test_old_array_form_is_rejected() -> None:
    """The retired CLI array form does not load as an EvalSuite, so exactly one
    schema is loadable anywhere."""
    old = '[{"name": "a", "input": "b", "expect_contains": ["c"]}]'
    with pytest.raises(ValidationError):
        EvalSuite.model_validate_json(old)


@pytest.mark.parametrize("unknown_key", ["requires", "note", "expect_stauts"])
def test_case_schema_rejects_unknown_keys(unknown_key: str) -> None:
    """A case author must not be able to add an unenforced requirement or typo."""
    document = {
        "name": "strict-case-keys",
        "cases": [
            {
                "id": "c",
                "input": "i",
                "grader": {"kind": "contains", "expected": "x"},
                unknown_key: "portable",
            }
        ],
    }
    with pytest.raises(ValidationError, match=unknown_key):
        EvalSuite.model_validate(document)
