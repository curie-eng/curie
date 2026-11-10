"""API half of the frozen sealed reply vector (ACTION-EXECUTOR-9).

@spec ACTION-EXECUTOR-9. The API decides whether a stored ``prior_state`` is a
sealed envelope (``undoable``, ACTION-EXECUTOR-11); the worker's ``_snapshot``
and the runner's redactor judge the same grammar in other images, so all three
read ``tests/vectors/sealed-snapshot-reply.json``. ``envelope_valid`` is the
verdict on ``reply.prior`` alone.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from curie_api import sealed_snapshot

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "sealed-snapshot-reply.json"
    ).read_text("utf-8")
)
_CASE_KEYS = {
    "name",
    "reply",
    "held_secrets",
    "expand_ciphertext",
    "envelope_valid",
    "runner",
    "worker",
}


def _reply(case: dict[str, Any]) -> dict[str, Any]:
    reply = copy.deepcopy(case["reply"])
    expand = case.get("expand_ciphertext")
    if expand is not None:
        reply["prior"]["ciphertext"] = expand["fill"] * expand["length"] + expand["suffix"]
    return reply


def test_the_vector_has_only_known_keys() -> None:
    assert set(_VECTOR) == {"comment", "envelope", "vectors"}
    for case in _VECTOR["vectors"]:
        unknown = set(case) - _CASE_KEYS
        assert not unknown, f"{case['name']}: unknown keys {sorted(unknown)}"


def test_the_frozen_grammar_constants_are_the_apis() -> None:
    envelope = _VECTOR["envelope"]
    assert envelope["sealed"] == sealed_snapshot.SEALED_CONSTANT
    assert envelope["redaction_placeholder_prefix"] == sealed_snapshot.REDACTION_PLACEHOLDER_PREFIX


@pytest.mark.parametrize("case", _VECTOR["vectors"], ids=lambda case: case["name"])
def test_the_api_judges_each_envelope_as_frozen(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-9: the API's grammar verdict on ``prior``."""

    assert sealed_snapshot.is_sealed_envelope(_reply(case).get("prior")) is case["envelope_valid"]
