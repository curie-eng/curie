"""Worker half of the frozen thread-reset SET vector (#1534, ADR-0168 decision 4)."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

from aci_protocol import QueuedTurn, ReplyHandle
from aci_protocol.turn import DEFAULT_IDENTITY, route_identity
from curie_worker.consumer import THREAD_RESET_INFLIGHT_SET, THREAD_RESET_SET
from curie_worker.kernel import _thread_key_for

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "thread-reset-set.json"
_EXPECTED_KEYS = {
    "comment",
    "thread_reset_set",
    "thread_reset_inflight_set",
    "thread_key_examples",
}
_EXAMPLE_KEYS = {"kind", "adapter", "channel", "conversation_id", "thread_key"}


def test_thread_reset_keys_match_the_frozen_vector() -> None:
    parsed = json.loads(_VECTOR.read_text())
    unknown = set(parsed) - _EXPECTED_KEYS
    assert not unknown, (
        f"unknown keys in tests/vectors/thread-reset-set.json: {sorted(unknown)}. "
        "Teach them to this test, apps/api/tests/test_thread_reset_vector.py, "
        "and the CLI queue.rs vector test."
    )
    assert parsed["thread_reset_set"] == THREAD_RESET_SET
    assert parsed["thread_reset_inflight_set"] == THREAD_RESET_INFLIGHT_SET
    examples = parsed["thread_key_examples"]
    assert examples, "the vector must freeze at least one scoped thread-key example"
    for example in examples:
        assert set(example) <= _EXAMPLE_KEYS, sorted(set(example) - _EXAMPLE_KEYS)
        adapter = example.get("adapter")
        turn = QueuedTurn(
            event_id="EvSIM-vector",
            conversation_id=example["conversation_id"],
            author="U1",
            text="ping",
            reply_handle=ReplyHandle(
                kind=example["kind"],
                channel=example["channel"],
                placeholder="p-1",
                adapter=adapter,
            ),
            received_at="2026-07-05T00:00:00+00:00",
        )
        assert _thread_key_for(turn) == example["thread_key"], example
        identity = route_identity(example["kind"], adapter)
        named = [] if identity in (None, DEFAULT_IDENTITY) else [identity]
        assert example["thread_key"] == ":".join(
            quote(part, safe="")
            for part in (
                example["kind"],
                *named,
                example["channel"],
                example["conversation_id"],
            )
        )
