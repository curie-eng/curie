"""Runner half of the frozen sealed reply vector (ACTION-EXECUTOR-10).

@spec ACTION-EXECUTOR-10 @spec ACTION-EXECUTOR-9. ``OutboundRedactor`` treats
the replay inputs of a ``side_effect_flag`` result (``prior`` when it validates
as an envelope, ``version`` and ``target``) as verbatim or withheld, never
altered: pattern rules skip a valid envelope's ciphertext but run over its
``kid``, ``target`` and ``version``, where a match withholds all three; a held
literal anywhere in them, ciphertext included, withholds all three; every
other field keeps today's scrubbing. The worker reads the frame this emits
(``apps/worker/tests/test_sealed_snapshot_reply_vector.py``), so both read
``tests/vectors/sealed-snapshot-reply.json``. Read through the production
``OutboundRedactor.push`` on a real ``side_effect_flag`` line.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from curie_runner.redact import OutboundRedactor

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[2] / "tests" / "vectors" / "sealed-snapshot-reply.json"
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
_REPLAY_INPUTS = ("prior", "version", "target")


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
        assert not unknown, (
            f"{case['name']}: unknown keys {sorted(unknown)}. Teach them to this test, "
            "apps/api/tests/test_sealed_snapshot_reply_vector.py and "
            "apps/worker/tests/test_sealed_snapshot_reply_vector.py."
        )
        assert set(case["runner"]) <= {"replay_inputs", "redacted", "scrubbed"}


@pytest.mark.parametrize("case", _VECTOR["vectors"], ids=lambda case: case["name"])
def test_the_redactor_treats_replay_inputs_as_frozen(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-10: verbatim or withheld, never altered."""

    reply = _reply(case)
    line = json.dumps(
        {
            "type": "side_effect_flag",
            "tool": "mcp__example-scale__scale",
            "call_id": "toolu_example",
            "arguments": {"replicas": 5},
            "result": reply,
        }
    )
    redactor = OutboundRedactor(frozenset(case["held_secrets"]))
    (emitted,) = redactor.push(line)
    frame = json.loads(emitted)
    result = frame["result"]
    expected = case["runner"]

    if expected["replay_inputs"] == "verbatim":
        for key in _REPLAY_INPUTS:
            assert result.get(key) == reply.get(key), key
        if isinstance(reply.get("prior"), dict) and case["envelope_valid"]:
            # Byte-identical, not merely equal after a round trip.
            assert json.dumps(result["prior"], sort_keys=True) == json.dumps(
                reply["prior"], sort_keys=True
            )
    else:
        for key in _REPLAY_INPUTS:
            assert result[key] is None, key

    assert bool(frame.get("redacted")) is expected["redacted"]
    scrubbed = set(expected.get("scrubbed", []))
    for key, value in reply.items():
        if key in _REPLAY_INPUTS:
            continue
        if key in scrubbed:
            assert result[key] != value, key
        else:
            assert result[key] == value, key
