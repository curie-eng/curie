"""Worker half of the frozen restore and ``observe_version`` calls vector.

@spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-20. In one
executor sandbox the worker has the runner call ``observe_version`` with the
recorded target only, then ``restore`` with ``target`` and ``prior_state``,
adding ``expected_version`` only when the connector's ``restore`` schema
declares it. Before adding it the worker checks the ruling's
``arguments_sha256`` over the two-key canonical form and refuses
``arguments_mismatch`` on any difference. The connector's reply to the call
maps onto the execution's terminal state and code. The runner and the
reference connector read ``tests/vectors/executor-restore-calls.json`` in other
images.

The readers are ``curie_worker.action_executor``'s ``observe_arguments``,
``restore_call`` (the exact canonical text the ``call`` phase sends, or
``ExecutorRefusal`` with its pre-dispatch ``code``) and ``call_outcome``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "executor-restore-calls.json"
    ).read_text("utf-8")
)


def _executor() -> Any:
    from curie_worker import action_executor

    return action_executor


def test_the_vector_has_only_known_keys() -> None:
    expected = {
        "comment",
        "observe_tool",
        "restore_tool",
        "observe_arguments",
        "observe_replies",
        "restore_calls",
        "call_replies",
    }
    unknown = set(_VECTOR) - expected
    assert not unknown, (
        f"unknown keys in executor-restore-calls.json: {sorted(unknown)}. Teach them to this "
        "test, apps/api/tests/test_action_canonical_arguments_vector.py and "
        "runner/tests/test_runner_execute_vector.py."
    )


def test_the_worker_observes_with_the_recorded_target_only() -> None:
    """@spec ACTION-EXECUTOR-15: ``observe_version`` gets ``{"target"}`` and nothing else."""

    target = _VECTOR["observe_arguments"]["target"]
    assert _executor().observe_arguments(target) == _VECTOR["observe_arguments"]


@pytest.mark.parametrize("call", _VECTOR["restore_calls"], ids=lambda call: call["name"])
def test_the_worker_sends_the_frozen_restore_text(call: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-15: ``expected_version`` only when the schema declares it."""

    text = _executor().restore_call(
        target=call["target"],
        prior_state=call["prior_state"],
        recorded_version=call["recorded_version"],
        restore_input_schema=call["restore_input_schema"],
        arguments_sha256=call["ruling_sha256"],
    )
    assert text == call["canonical"]
    assert json.loads(text) == call["arguments"]


@pytest.mark.parametrize("call", _VECTOR["restore_calls"], ids=lambda call: call["name"])
def test_a_ruling_digest_for_other_arguments_is_refused(call: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: any difference refuses ``arguments_mismatch`` before dispatch."""

    executor = _executor()
    with pytest.raises(executor.ExecutorRefusal) as refused:
        executor.restore_call(
            target=call["target"],
            prior_state=call["prior_state"],
            recorded_version=call["recorded_version"],
            restore_input_schema=call["restore_input_schema"],
            arguments_sha256="0" * 64,
        )
    assert refused.value.code == "arguments_mismatch"


@pytest.mark.parametrize("reply", _VECTOR["call_replies"], ids=lambda reply: reply["name"])
def test_a_call_reply_maps_to_the_frozen_outcome(reply: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-20: unknown codes are normalized."""

    state, code = _executor().call_outcome(
        is_error=reply["is_error"], structured=reply["structured"]
    )
    assert (state, code) == (reply["state"], reply["code"])
