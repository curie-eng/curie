"""Exercise canary triage graders, not model adherence. @spec SRE-ALERT-4"""

import json
from pathlib import Path

import pytest
from curie_worker.eval.models import EvalSuite

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "case_id,expected",
    [
        ("canary-delivered-mismatch", "Delivery: observed; response: mismatch; user impact: unverified."),
        ("canary-legacy-failure", "Delivery: unverified; response: mismatch; user impact: unverified."),
        ("canary-delivery-timeout", "Delivery: failed; cause: unverified; user impact: unverified."),
    ],
)
def test_canary_classification_controls(case_id: str, expected: str) -> None:
    """@spec SRE-ALERT-4"""
    suite = EvalSuite.model_validate_json((REPO / "examples/sre-bot/evals/cases.json").read_text())
    cases = {case.id: case for case in suite.cases}
    assert case_id in cases, f"missing canary triage regression: {case_id}"
    case = cases[case_id]
    assert case.grader.grade(expected)
    assert not case.grader.grade(case.input)
    fixtures = json.loads((REPO / "cli/tests/data/eval_falsifiability_fixtures.json").read_text())
    key = f"sre-bot/{case_id}"
    assert case.grader.grade(fixtures["exemplars"][key])
    for answer in fixtures["counterexamples"][key]:
        assert not case.grader.grade(answer)
