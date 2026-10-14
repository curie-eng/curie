"""The delivered-reply failure marker stays aligned with the frozen vector (#3401)."""

from __future__ import annotations

import json
from pathlib import Path

from curie_worker.kernel.constants import TURN_FAILURE_REPLY_PREFIX
from curie_worker.kernel.failures import failure_class_from_reply, turn_failure_reply

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "turn-failure-reply.json"
_KEYS = {"comment", "reply_prefix", "factory_class_line_prefix", "examples"}


def test_turn_failure_reply_matches_the_frozen_vector() -> None:
    vector = json.loads(_VECTOR.read_text())
    unknown = set(vector) - _KEYS
    assert not unknown, f"unknown keys in {_VECTOR}: {sorted(unknown)}"
    assert vector["reply_prefix"] == TURN_FAILURE_REPLY_PREFIX
    for example in vector["examples"]:
        reply = turn_failure_reply(example["classification"], "The run failed.")
        assert reply.splitlines()[0] == example["reply_first_line"]
        assert failure_class_from_reply(reply) == example["classification"]
        assert example["factory_class_line"].startswith(vector["factory_class_line_prefix"])
