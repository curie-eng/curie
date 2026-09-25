"""API half of the frozen thread-reset SET vector (#1534, ADR-0168 decision 4)."""

from __future__ import annotations

import json
from pathlib import Path

from curie_api.threadkeys import route_thread_key
from curie_api.threadreset import THREAD_RESET_INFLIGHT_SET, THREAD_RESET_SET

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "thread-reset-set.json"
)
_EXPECTED_KEYS = {
    "comment",
    "thread_reset_set",
    "thread_reset_inflight_set",
    "thread_key_examples",
}
_EXAMPLE_KEYS = {"kind", "adapter", "channel", "conversation_id", "thread_key"}


def _parsed() -> dict[str, object]:
    parsed: dict[str, object] = json.loads(_VECTOR.read_text())
    return parsed


def test_thread_reset_keys_match_the_frozen_vector() -> None:
    parsed = _parsed()
    unknown = set(parsed) - _EXPECTED_KEYS
    assert not unknown, (
        f"unknown keys in tests/vectors/thread-reset-set.json: {sorted(unknown)}. "
        "Teach them to this test, apps/worker/tests/test_thread_reset_vector.py, "
        "and the CLI queue.rs vector test."
    )
    assert parsed["thread_reset_set"] == THREAD_RESET_SET
    assert parsed["thread_reset_inflight_set"] == THREAD_RESET_INFLIGHT_SET


def test_the_api_builds_every_frozen_thread_key() -> None:
    examples = _parsed()["thread_key_examples"]
    assert isinstance(examples, list) and examples
    for example in examples:
        assert set(example) <= _EXAMPLE_KEYS, sorted(set(example) - _EXAMPLE_KEYS)
        assert (
            route_thread_key(
                example["kind"],
                example.get("adapter"),
                example["channel"],
                example["conversation_id"],
            )
            == example["thread_key"]
        ), example


def test_the_vector_freezes_the_identity_cases_decision_4_names() -> None:
    examples = _parsed()["thread_key_examples"]
    assert isinstance(examples, list)
    routes = {(e["kind"], e.get("adapter")) for e in examples}
    assert {
        ("slack", "second-bot"),
        ("slack", "default"),
        ("slack", None),
        ("slack", ""),
        ("email", "agentmail-sandbox"),
        ("email", "mail:sandbox"),
    } <= routes
