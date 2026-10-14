"""API half of the frozen remediation policy vector.

@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-10
@spec AUTOMATED-REMEDIATION-17 @spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-26.
The API validates a policy document and the CLI mirrors that validation in Rust,
so both read ``tests/vectors/remediation-policy.json``
(``cli/tests/remediation_vectors.rs`` is the CLI half).

The reader is the policy store's pure validator,
``curie_api.remediation_policy_document.validate_document(document)``: it
returns a valid document unchanged and raises ``PolicyRefused`` with the frozen
``code`` and ``path`` for an invalid one.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_VECTOR = json.loads((_VECTORS / "remediation-policy.json").read_text("utf-8"))
_CODES = json.loads((_VECTORS / "remediation-codes.json").read_text("utf-8"))
_KEYS = {"comment", "refusal_codes", "valid", "invalid", "invalid_texts", "numeric_texts"}


def _validator() -> Any:  # noqa: ANN401 - the production module under test
    from curie_api import remediation_policy_document

    return remediation_policy_document


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-policy.json: {sorted(unknown)}. Teach them to this test "
        "and cli/tests/remediation_vectors.rs."
    )
    assert set(_VECTOR) == _KEYS
    for case in _VECTOR["valid"]:
        assert set(case) == {"name", "document"}, case["name"]
    for case in _VECTOR["invalid"]:
        assert set(case) == {"name", "document", "code", "path"}, case["name"]
    for case in _VECTOR["invalid_texts"] + _VECTOR["numeric_texts"]:
        assert set(case) == {"name", "text", "code", "path"}, case["name"]


def test_the_document_codes_are_policy_refusals() -> None:
    """@spec AUTOMATED-REMEDIATION-26: the document codes sit in the closed code set."""

    cases = _VECTOR["invalid"] + _VECTOR["invalid_texts"] + _VECTOR["numeric_texts"]
    used = {case["code"] for case in cases}
    assert used == set(_VECTOR["refusal_codes"])
    assert set(_VECTOR["refusal_codes"]) <= set(_CODES["policy_refusals"])


@pytest.mark.parametrize("case", _VECTOR["valid"], ids=lambda case: case["name"])
def test_each_valid_document_is_accepted_unchanged(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-2: a closed document within every bound."""

    document = copy.deepcopy(case["document"])
    assert _validator().validate_document(document) == case["document"]


@pytest.mark.parametrize("case", _VECTOR["invalid"], ids=lambda case: case["name"])
def test_each_invalid_document_is_refused_with_its_code_and_path(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24.

    Unknown keys, loosened limits, missing reads, delta bounds, automatic
    ``prevent`` and ``tune``, and the verifier grammar, each with a named code.
    """

    module = _validator()
    with pytest.raises(module.PolicyRefused) as refused:
        module.validate_document(copy.deepcopy(case["document"]))
    assert (refused.value.code, refused.value.path) == (case["code"], case["path"])


@pytest.mark.parametrize("case", _VECTOR["invalid_texts"], ids=lambda case: case["name"])
def test_a_non_finite_number_is_refused_before_any_check(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-2: NaN and infinities never reach a digest."""

    module = _validator()
    with pytest.raises(module.PolicyRefused) as refused:
        module.validate_document(json.loads(case["text"]))
    assert (refused.value.code, refused.value.path) == (case["code"], case["path"])


@pytest.mark.parametrize("case", _VECTOR["numeric_texts"], ids=lambda case: case["name"])
def test_a_number_a_json_value_can_change_is_refused_at_its_path(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.

    An integer outside the signed 64-bit range and an exponent beyond a double
    are refused at the number's own path, so the CLI's mirror (which reads
    numbers into a native JSON value) and the API agree on code and path.
    """

    module = _validator()
    with pytest.raises(module.PolicyRefused) as refused:
        module.validate_document(json.loads(case["text"]))
    assert (refused.value.code, refused.value.path) == (case["code"], case["path"])
