"""Runner half of the ErrorEvent.classification allowlist-constrain vector (#2556)."""

from __future__ import annotations

import json
from pathlib import Path

from curie_runner.translate import map_error_classification

_VECTOR = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "vectors"
    / "error-event-classification.json"
)
_TOP_LEVEL_KEYS = frozenset({"comment", "unclassified", "platform", "vectors"})
_VECTOR_KEYS = frozenset({"name", "input", "expected"})
_PLATFORM = (
    "rate-limit",
    "runner-error",
    "runner-timeout",
    "workspace-error",
    "budget-exceeded",
    "server-error",
    "ledger-error",
    "model-credential-rejected",
    "approval-not-acted",
    "false-completion",
    "publication-unrecorded",
)


def _load_payload() -> dict:
    payload = json.loads(_VECTOR.read_text(encoding="utf-8"))
    extra = set(payload) - _TOP_LEVEL_KEYS
    missing = _TOP_LEVEL_KEYS - set(payload)
    assert not extra and not missing, (
        f"tests/vectors/error-event-classification.json has unexpected keys "
        f"{sorted(extra)} and is missing {sorted(missing)}. A new input is "
        "rejected on purpose: one this lane cannot see would pass vacuously. "
        "Teach it to this test and apps/worker/tests/kernel/"
        "test_vector_error_classification.py."
    )
    assert payload["comment"]
    return payload


def _assert_known_vector_keys(vector: dict[str, object]) -> None:
    keys = set(vector)
    if keys != _VECTOR_KEYS:
        raise AssertionError(
            f"vector {vector.get('name')!r} in {_VECTOR} has unexpected keys "
            f"{sorted(keys - _VECTOR_KEYS)} and is missing "
            f"{sorted(_VECTOR_KEYS - keys)}. A new input is rejected on "
            "purpose: one this lane cannot see would pass vacuously. Teach the "
            "new key to both Python loaders."
        )


def test_vector_rejects_unknown_fields() -> None:
    payload = _load_payload()
    assert payload["vectors"], f"no vectors parsed from {_VECTOR}"
    for vector in payload["vectors"]:
        _assert_known_vector_keys(vector)


def test_mapper_matches_every_classification_vector() -> None:
    payload = _load_payload()
    assert payload["unclassified"] == "unclassified"
    assert payload["platform"] == list(_PLATFORM)
    vectors = payload["vectors"]
    assert vectors, f"no vectors parsed from {_VECTOR}"

    passthrough = {
        vector["input"]
        for vector in vectors
        if vector["input"] in _PLATFORM and vector["expected"] == vector["input"]
    }
    assert passthrough == set(_PLATFORM), (
        "every platform vocabulary token must have a pass-through vector "
        f"(expected == input). missing={sorted(set(_PLATFORM) - passthrough)}"
    )

    for vector in vectors:
        _assert_known_vector_keys(vector)
        raw = vector["input"]
        if raw is not None and not isinstance(raw, str):
            raise AssertionError(
                f"vector {vector['name']!r} input must be str or null, got {type(raw)}"
            )
        got = map_error_classification(raw)
        assert got == vector["expected"], vector["name"]
        assert got != "unknown"
        assert got != "rate_limit"
        assert got != "error_during_execution"
        assert got != "server_error"
