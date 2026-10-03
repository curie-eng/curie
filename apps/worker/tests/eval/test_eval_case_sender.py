"""EvalCase accepts an optional sender (#3818)."""

from __future__ import annotations

from curie_worker.eval.models import EvalCase


def _case(sender: str | None = None) -> dict[str, object]:
    body: dict[str, object] = {
        "id": "reports-the-eval-sender",
        "input": "who sent this",
        "grader": {"kind": "contains", "expected": "eval-sender-acme"},
    }
    if sender is not None:
        body["sender"] = sender
    return body


def test_eval_case_accepts_sender_and_omits_it_as_none() -> None:
    present = EvalCase.model_validate(_case("eval-sender-acme"))
    assert present.sender == "eval-sender-acme"
    absent = EvalCase.model_validate(_case())
    assert absent.sender is None
