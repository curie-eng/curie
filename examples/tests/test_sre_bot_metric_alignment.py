"""Exercise numeric eval oracles with independently derived answer controls.

These tests qualify the graders, not model adherence to the skill. A model that
pairs independent peaks or sums overlapping estimates must fail the evals.
"""

import json
from pathlib import Path

import pytest
from curie_worker.eval.models import EvalSuite

REPO = Path(__file__).resolve().parents[2]
CONTROLS = [
    ("sre-same-time-peak-denominator", "7 | 20 | 35%", True),
    ("sre-same-time-peak-denominator", "7 | 900 | 0.78%", False),
    ("sre-same-time-peak-denominator", "7 | 40 | 17.5%", False),
    ("sre-missing-time-denominator", "7 | unknown | unknown", True),
    ("sre-missing-time-denominator", "7 | 40 | 17.5%", False),
    ("sre-different-window-denominator", "7 | unknown | unknown", True),
    ("sre-different-window-denominator", "7 | 20 | 35%", False),
    ("sre-overlapping-window-total", "unknown", True),
    ("sre-overlapping-window-total", "12.5", False),
    ("sre-overlapping-window-total", "13", False),
]


@pytest.mark.parametrize("case_id,answer,accepted", CONTROLS)
def test_numeric_oracles_reject_cross_time_and_window_inferences(
    case_id: str, answer: str, accepted: bool
) -> None:
    suite = EvalSuite.model_validate_json(
        (REPO / "examples/sre-bot/evals/cases.json").read_text()
    )
    case = next((case for case in suite.cases if case.id == case_id), None)
    assert case is not None, f"missing executable regression: {case_id}"
    assert case.grader.grade(answer) is accepted
    assert not case.grader.grade(case.input), "input parroting must not pass"
    assert not case.grader.grade("all done"), "unconditional completion must not pass"


def test_shared_numeric_controls_exercise_the_python_production_grader() -> None:
    suite = EvalSuite.model_validate_json(
        (REPO / "examples/sre-bot/evals/cases.json").read_text()
    )
    fixtures = json.loads((REPO / "cli/tests/data/eval_falsifiability_fixtures.json").read_text())
    for case in suite.cases:
        if not any(case.id == row[0] for row in CONTROLS):
            continue
        key = f"sre-bot/{case.id}"
        assert case.grader.grade(fixtures["exemplars"][key])
        for answer in fixtures["paraphrases"][key]:
            assert case.grader.grade(answer)
        for answer in fixtures["counterexamples"][key]:
            assert not case.grader.grade(answer)
