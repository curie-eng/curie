"""API half of the frozen canonical arguments vector (ACTION-EXECUTOR-7).

@spec ACTION-EXECUTOR-7. The undo ruling is the restore's creator, so it stores
``arguments_sha256`` over the restore call's two-key canonical form. The worker
recomputes it and the caller proxy re-canonicalizes in another image, so all
three read ``tests/vectors/action-canonical-arguments.json``; the ruling digest
the worker checks before adding ``expected_version`` is frozen in
``tests/vectors/executor-restore-calls.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api.routers.actions import restore_arguments_sha256

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_CANONICAL = json.loads((_VECTORS / "action-canonical-arguments.json").read_text("utf-8"))
_CALLS = json.loads((_VECTORS / "executor-restore-calls.json").read_text("utf-8"))

_CANONICAL_KEYS = {
    "comment",
    "form",
    "vectors",
    "restore",
    "non_canonical_texts",
    "not_an_object_texts",
    "refusal",
}


def test_the_canonical_vector_has_only_known_keys() -> None:
    unknown = set(_CANONICAL) - _CANONICAL_KEYS
    assert not unknown, (
        f"unknown keys in action-canonical-arguments.json: {sorted(unknown)}. Teach them to "
        "this test, apps/worker/tests/test_action_canonical_arguments_vector.py and "
        "runner/tests/test_runner_execute_vector.py."
    )
    assert set(_CANONICAL) == _CANONICAL_KEYS


@pytest.mark.parametrize("case", _CANONICAL["restore"], ids=lambda case: case["name"])
def test_the_ruling_digest_is_the_frozen_restore_digest(case: dict[str, object]) -> None:
    """@spec ACTION-EXECUTOR-7: the ruling stores the digest of the vector's bytes."""

    assert set(case) == {"name", "target", "prior_state", "canonical", "sha256"}
    assert restore_arguments_sha256(case["target"], case["prior_state"]) == case["sha256"]  # type: ignore[arg-type]


@pytest.mark.parametrize("call", _CALLS["restore_calls"], ids=lambda call: call["name"])
def test_the_ruling_digest_matches_what_the_worker_checks(call: dict[str, object]) -> None:
    """@spec ACTION-EXECUTOR-15: ``expected_version`` never enters the ruling digest."""

    assert restore_arguments_sha256(call["target"], call["prior_state"]) == call["ruling_sha256"]  # type: ignore[arg-type]
