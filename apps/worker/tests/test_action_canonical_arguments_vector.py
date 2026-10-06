"""Worker and caller proxy halves of the frozen canonical arguments vector.

@spec ACTION-EXECUTOR-7. Canonical form is the proxy's: sorted keys, ``,`` and
``:`` separators, ``ensure_ascii=False``. The worker canonicalizes the
arguments it sends, recomputes ``arguments_sha256`` over that text (refusing
``arguments_mismatch`` on any difference) and mints the ``ccg`` grant over it;
the proxy's production parser re-canonicalizes the forwarded arguments and
compares them with the grant's. The API ruling and the runner preflight read
the same ``tests/vectors/action-canonical-arguments.json`` in other images.

The worker's canonicalizer and digest are ``curie_worker.connector_grant``'s
``canonical_arguments(arguments) -> str`` and ``arguments_sha256(text) -> str``,
beside ``mint``, whose ``args`` they produce.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_connector_proxy import server as proxy_server
from curie_worker import connector_grant

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "vectors"
        / "action-canonical-arguments.json"
    ).read_text("utf-8")
)


def test_the_vector_has_only_known_keys() -> None:
    expected = {
        "comment",
        "form",
        "vectors",
        "restore",
        "non_canonical_texts",
        "not_an_object_texts",
        "non_finite_texts",
        "refusal",
    }
    unknown = set(_VECTOR) - expected
    assert not unknown, (
        f"unknown keys in action-canonical-arguments.json: {sorted(unknown)}. Teach them to "
        "this test, apps/api/tests/test_action_canonical_arguments_vector.py and "
        "runner/tests/test_runner_execute_vector.py."
    )
    for case in _VECTOR["vectors"]:
        assert set(case) == {"name", "arguments", "canonical", "sha256"}


@pytest.mark.parametrize(
    "case", _VECTOR["vectors"] + _VECTOR["restore"], ids=lambda case: case["name"]
)
def test_the_proxy_parser_produces_the_frozen_bytes(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: the proxy's production parser, unchanged."""

    arguments = (
        case["arguments"]
        if "arguments" in case
        else {"target": case["target"], "prior_state": case["prior_state"]}
    )
    assert proxy_server._canonical_arguments(arguments) == case["canonical"]


@pytest.mark.parametrize("case", _VECTOR["vectors"], ids=lambda case: case["name"])
def test_the_worker_canonicalizer_produces_the_frozen_bytes(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: the worker's text equals the proxy's, byte for byte."""

    text = connector_grant.canonical_arguments(case["arguments"])
    assert text == case["canonical"]
    assert text.encode("utf-8") == case["canonical"].encode("utf-8")


@pytest.mark.parametrize(
    "case", _VECTOR["vectors"] + _VECTOR["restore"], ids=lambda case: case["name"]
)
def test_the_worker_digest_is_the_frozen_digest(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: the digest the worker compares with ``arguments_sha256``."""

    assert connector_grant.arguments_sha256(case["canonical"]) == case["sha256"]


@pytest.mark.parametrize("case", _VECTOR["non_canonical_texts"], ids=lambda case: case["name"])
def test_a_non_canonical_text_never_digests_as_its_canonical_form(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: the digest is over bytes, so a re-spelling is a mismatch."""

    canonical = connector_grant.canonical_arguments(json.loads(case["text"]))
    assert canonical == case["canonical"]
    assert connector_grant.arguments_sha256(case["text"]) != connector_grant.arguments_sha256(
        canonical
    )


@pytest.mark.parametrize("case", _VECTOR["non_finite_texts"], ids=lambda case: case["name"])
def test_the_worker_and_the_proxy_refuse_non_finite_numbers(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-7: NaN and infinities have no exact JSON form, on any side."""

    arguments = json.loads(case["text"])  # Python's permissive reader accepts them
    assert proxy_server._canonical_arguments(arguments) is None
    with pytest.raises(ValueError):
        connector_grant.canonical_arguments(arguments)


@pytest.mark.parametrize(
    "case",
    _VECTOR["vectors"] + _VECTOR["non_finite_texts"],
    ids=lambda case: case["name"],
)
def test_the_worker_and_the_proxy_canonicalize_as_one_implementation(
    case: dict[str, Any],
) -> None:
    """@spec ACTION-EXECUTOR-7: one canonicalizer, so the two never disagree on any input."""

    arguments = case["arguments"] if "arguments" in case else json.loads(case["text"])
    proxied = proxy_server._canonical_arguments(arguments)
    try:
        worker: str | None = connector_grant.canonical_arguments(arguments)
    except ValueError:
        worker = None
    assert worker == proxied
