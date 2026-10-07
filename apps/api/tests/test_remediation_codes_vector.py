"""API half of the frozen remediation codes vector.

@spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-14
@spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-26.
The API stores and returns the closed remediation vocabularies, and the worker's
receipt renderer and the CLI render them in other images, so all three read
``tests/vectors/remediation-codes.json``
(``apps/worker/tests/test_remediation_codes_vector.py`` and
``cli/tests/remediation_vectors.rs`` are the other halves).

The reader is ``curie_api.remediation_codes``: one frozenset per closed list
(upper-cased key names) and ``ADMISSION_CHECKS``, a mapping from each
AUTOMATED-REMEDIATION-8 check number (an int) to the codes that check reports.
The policy store's ``remediation_policy_document.KINDS`` is cross-checked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-codes.json"
    ).read_text("utf-8")
)
_LISTS = [
    "nomination_states",
    "nomination_refusals",
    "submission_refusals",
    "approval_reasons",
    "approval_resolution_refusals",
    "verification_outcomes",
    "receipt_stages",
    "kinds",
    "authorities",
    "authority_kinds",
    "actor_kinds",
    "policy_refusals",
]
_KEYS = {"comment", "admission_checks", *_LISTS}


def _codes() -> Any:  # noqa: ANN401 - the production module under test
    from curie_api import remediation_codes

    return remediation_codes


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-codes.json: {sorted(unknown)}. Teach them to this test, "
        "apps/worker/tests/test_remediation_codes_vector.py and cli/tests/remediation_vectors.rs."
    )
    assert set(_VECTOR) == _KEYS
    for name in _LISTS:
        assert len(_VECTOR[name]) == len(set(_VECTOR[name])), name


@pytest.mark.parametrize("name", _LISTS)
def test_each_closed_set_matches(name: str) -> None:
    """@spec AUTOMATED-REMEDIATION-26: a code added or renamed on one side fails here."""

    assert set(getattr(_codes(), name.upper())) == set(_VECTOR[name])


def test_each_admission_check_reports_the_frozen_codes() -> None:
    """@spec AUTOMATED-REMEDIATION-8: the card names the check that failed."""

    expected = {int(check): set(codes) for check, codes in _VECTOR["admission_checks"].items()}
    actual = {check: set(codes) for check, codes in _codes().ADMISSION_CHECKS.items()}
    assert actual == expected


def test_the_policy_store_declares_the_frozen_kinds() -> None:
    """@spec AUTOMATED-REMEDIATION-24: the kinds the policy validator accepts."""

    from curie_api import remediation_policy_document

    assert set(remediation_policy_document.KINDS) == set(_VECTOR["kinds"])
