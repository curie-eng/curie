"""API half of the frozen remediation predicate vector.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-17
@spec AUTOMATED-REMEDIATION-26. The runner's pointer extraction, the worker's
sample report and the API's evaluator ship in three images, so all three read
``tests/vectors/remediation-predicate.json``
(``runner/tests/test_runner_execute_vector.py`` drives the extraction cases
through the runner's ``read`` phase, and
``apps/worker/tests/test_remediation_predicate_vector.py`` is the worker half).

The readers here:

* ``curie_api.remediation_predicate``: ``evaluate_sample(predicate, sample)``
  returns ``satisfied``, ``unsatisfied`` or ``unsuccessful`` for a declared
  ``{"comparator", "value"}`` (no ``value`` for ``absent``) against a reported
  ``{"sample", "value"}``; ``SAMPLE_KINDS``, ``SUCCESSFUL_SAMPLES`` and
  ``RESULTS`` are the closed sets, and ``ORDERING_COMPARATORS`` read a JSON number
  or a string matching ``NUMERIC_STRING_GRAMMAR`` as numeric;
* ``curie_api.schemas.action_executions.ExecutionSample``: the closed body of
  ``POST /action-executions/{id}/samples`` (no ``index``: one read execution is
  one sample, maintainer ruling M2);
* ``curie_api.remediation_policy_document`` (the policy store): the comparator
  set, the ``in`` list cap and the pointer grammar refused at policy write.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_VECTOR = json.loads((_VECTORS / "remediation-predicate.json").read_text("utf-8"))
_POLICY = json.loads((_VECTORS / "remediation-policy.json").read_text("utf-8"))
_KEYS = {
    "comment",
    "comparators",
    "ordering_comparators",
    "numeric_string_grammar",
    "in_list_max",
    "value_max_chars",
    "sample_kinds",
    "successful",
    "results",
    "extractions",
    "invalid_pointers",
    "sample_report",
    "evaluations",
}


def _evaluator() -> Any:  # noqa: ANN401 - the production module under test
    from curie_api import remediation_predicate

    return remediation_predicate


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-predicate.json: {sorted(unknown)}. Teach them to this "
        "test, apps/worker/tests/test_remediation_predicate_vector.py and "
        "runner/tests/test_runner_execute_vector.py."
    )
    assert set(_VECTOR) == _KEYS


def test_the_evaluator_closes_the_frozen_sets() -> None:
    """@spec AUTOMATED-REMEDIATION-17: sample kinds, successful kinds and results."""

    module = _evaluator()
    assert set(module.SAMPLE_KINDS) == set(_VECTOR["sample_kinds"])
    assert set(module.SUCCESSFUL_SAMPLES) == set(_VECTOR["successful"])
    assert set(module.RESULTS) == set(_VECTOR["results"])
    assert set(module.ORDERING_COMPARATORS) == set(_VECTOR["ordering_comparators"])
    assert module.NUMERIC_STRING_GRAMMAR == _VECTOR["numeric_string_grammar"]


@pytest.mark.parametrize("case", _VECTOR["evaluations"], ids=lambda case: case["name"])
def test_each_evaluation_gives_the_frozen_result(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-17: one pointer, a closed comparator set, no coercion."""

    predicate = {"comparator": case["comparator"]}
    if "value" in case:
        predicate["value"] = case["value"]
    assert _evaluator().evaluate_sample(predicate, case["sample"]) == case["result"]


@pytest.mark.parametrize("case", _VECTOR["extractions"], ids=lambda case: case["name"])
def test_every_extracted_sample_is_evaluable(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-12: whatever the runner answers, the API can judge.

    An unsuccessful sample is ``unsuccessful`` for every comparator; a
    successful one never is.
    """

    module = _evaluator()
    sample = case["sample"]
    result = module.evaluate_sample({"comparator": "absent"}, sample)
    if sample["sample"] in _VECTOR["successful"]:
        assert result in {"satisfied", "unsatisfied"}
    else:
        assert result == "unsuccessful"


@pytest.mark.parametrize("comparator", sorted(_VECTOR["comparators"]))
def test_a_skipped_sample_is_unsuccessful_for_every_comparator(comparator: str) -> None:
    """@spec AUTOMATED-REMEDIATION-12: a sample that missed its slot is recorded
    ``skipped`` by the API (never reported by a worker), an unsuccessful sample.
    """

    predicate: dict[str, Any] = {"comparator": comparator}
    if comparator == "in":
        predicate["value"] = [1]
    elif comparator != "absent":
        predicate["value"] = 1
    skipped = {"sample": "skipped", "value": None}
    assert _evaluator().evaluate_sample(predicate, skipped) == "unsuccessful"


def test_the_sample_route_body_is_the_worker_report() -> None:
    """@spec AUTOMATED-REMEDIATION-12: the fenced, closed body the worker posts."""

    from curie_api.schemas.action_executions import ExecutionSample

    report = _VECTOR["sample_report"]
    assert set(ExecutionSample.model_fields) == set(report["keys"])
    for case in _VECTOR["extractions"]:
        body = {**report["fence"], **case["sample"]}
        parsed = ExecutionSample.model_validate(body)
        assert parsed.sample == case["sample"]["sample"], case["name"]
    with pytest.raises(ValueError):
        ExecutionSample.model_validate(
            {**report["fence"], "sample": "example_unknown", "value": None}
        )
    with pytest.raises(ValueError):
        ExecutionSample.model_validate({**report["fence"], "sample": "value", "value": {"x": 1}})


def _policy_module() -> Any:  # noqa: ANN401 - the policy store's validator
    from curie_api import remediation_policy_document

    return remediation_policy_document


def test_the_policy_validator_declares_the_frozen_comparators() -> None:
    """@spec AUTOMATED-REMEDIATION-17: the comparator set is closed at policy write."""

    module = _policy_module()
    assert set(module.COMPARATORS) == set(_VECTOR["comparators"])


@pytest.mark.parametrize("pointer", _VECTOR["invalid_pointers"])
def test_an_invalid_pointer_is_refused_at_policy_write(pointer: str) -> None:
    """@spec AUTOMATED-REMEDIATION-17: the runner and the policy agree on the grammar."""

    module = _policy_module()
    document = copy.deepcopy(_POLICY["valid"][0]["document"])
    document["actions"][0]["verifier"]["pointer"] = pointer
    with pytest.raises(module.PolicyRefused) as refused:
        module.validate_document(document)
    assert refused.value.code == "policy_document_invalid"
    assert refused.value.path == "/actions/0/verifier/pointer"


def test_the_in_list_cap_is_the_frozen_one() -> None:
    """@spec AUTOMATED-REMEDIATION-17: an ``in`` list holds at most ``in_list_max`` scalars."""

    module = _policy_module()
    cap = _VECTOR["in_list_max"]
    document = copy.deepcopy(_POLICY["valid"][0]["document"])
    precondition = document["actions"][0]["precondition"]
    precondition.update(comparator="in", value=list(range(cap)))
    module.validate_document(document)
    precondition["value"] = list(range(cap + 1))
    with pytest.raises(module.PolicyRefused):
        module.validate_document(document)
