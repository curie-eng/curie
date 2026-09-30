"""Contract tests for canonical, rendering-neutral conversation identity."""

from __future__ import annotations

import json
from inspect import Parameter, signature
from pathlib import Path
from typing import get_type_hints

import pytest
from aci_protocol import turn as aci_turn
from channel_protocol import (
    ScopedConversation,
    parse_scoped_conversation_id,
    scoped_conversation_id,
)
from channel_protocol.identity import DEFAULT_IDENTITY


@pytest.mark.parametrize(
    ("kind", "address", "conversation_id", "expected"),
    [
        (
            "slack",
            "C0EXAMPLE1",
            "1700000000.000100",
            "slack:C0EXAMPLE1:1700000000.000100",
        ),
        (
            "email",
            "agent@example.test",
            "thread/9",
            "email:agent%40example.test:thread%2F9",
        ),
    ],
)
def test_scoped_conversation_id_has_exact_adapter_neutral_output(
    kind: str,
    address: str,
    conversation_id: str,
    expected: str,
) -> None:
    assert scoped_conversation_id(kind, address, conversation_id) == expected


def test_scoped_conversation_id_encodes_components_before_joining() -> None:
    assert scoped_conversation_id(
        "slack:bridge",
        "C0EXAMPLE1%archive",
        "topic:✓",
    ) == "slack%3Abridge:C0EXAMPLE1%25archive:topic%3A%E2%9C%93"


@pytest.mark.parametrize(
    ("left", "right"),
    [
        pytest.param(
            ("slack", "C0EXAMPLE1", "1700000000.000100:child"),
            ("slack", "C0EXAMPLE1:1700000000.000100", "child"),
            id="delimiter-moved-between-components",
        ),
        pytest.param(
            ("slack", "C0EXAMPLE1", "literal%3Avalue"),
            ("slack", "C0EXAMPLE1", "literal:value"),
            id="literal-percent-escape-versus-delimiter",
        ),
        pytest.param(
            ("släck", "C0EXAMPLE1", "thread"),
            ("sl%C3%A4ck", "C0EXAMPLE1", "thread"),
            id="unicode-versus-literal-percent-encoding",
        ),
        pytest.param(
            ("", ":C0EXAMPLE1", "thread"),
            (":", "C0EXAMPLE1", "thread"),
            id="empty-component-with-moved-delimiter",
        ),
    ],
)
def test_scoped_conversation_id_isolates_ambiguous_component_tuples(
    left: tuple[str, str, str],
    right: tuple[str, str, str],
) -> None:
    assert left != right
    assert scoped_conversation_id(*left) != scoped_conversation_id(*right)


def test_scoped_conversation_id_is_a_typed_deterministic_package_export() -> None:
    assert scoped_conversation_id.__module__ == "channel_protocol.identity"
    helper_signature = signature(scoped_conversation_id)
    assert list(helper_signature.parameters) == [
        "kind",
        "address",
        "conversation_id",
        "identity",
    ]
    assert helper_signature.parameters["identity"].kind is Parameter.KEYWORD_ONLY
    assert helper_signature.parameters["identity"].default is None
    assert get_type_hints(scoped_conversation_id)["return"] is str

    identity = ("slack", "C0EXAMPLE1", "1700000000.000100")
    assert scoped_conversation_id(*identity) == scoped_conversation_id(*identity)


@pytest.mark.parametrize("identity", [None, "default"])
def test_no_identity_and_the_default_identity_keep_the_pre_identity_key(
    identity: str | None,
) -> None:
    """ADR-0168 decision 4: every existing Slack key is unchanged, whether the
    caller passes no identity or the default app's own name."""
    assert (
        scoped_conversation_id(
            "slack", "C0EXAMPLE1", "1700000000.000100", identity=identity
        )
        == "slack:C0EXAMPLE1:1700000000.000100"
    )


@pytest.mark.parametrize(
    ("kind", "identity", "address", "conversation_id", "expected"),
    [
        (
            "slack",
            "second-bot",
            "C0EXAMPLE1",
            "1700000000.000100",
            "slack:second-bot:C0EXAMPLE1:1700000000.000100",
        ),
        (
            "email",
            "agentmail-sandbox",
            "agent@example.test",
            "thread/9",
            "email:agentmail-sandbox:agent%40example.test:thread%2F9",
        ),
        ("slack", "bot:a", "C0EXAMPLE1", "t", "slack:bot%3Aa:C0EXAMPLE1:t"),
    ],
)
def test_a_named_identity_follows_the_kind_as_its_own_encoded_segment(
    kind: str, identity: str, address: str, conversation_id: str, expected: str
) -> None:
    assert (
        scoped_conversation_id(kind, address, conversation_id, identity=identity)
        == expected
    )


def test_two_identities_in_one_thread_get_two_keys() -> None:
    first = scoped_conversation_id(
        "slack", "C0EXAMPLE1", "1700000000.000100", identity="first-bot"
    )
    second = scoped_conversation_id(
        "slack", "C0EXAMPLE1", "1700000000.000100", identity="second-bot"
    )
    assert first != second


def test_the_default_identity_is_the_aci_protocol_default() -> None:
    """A copy, because this package does not depend on aci-protocol."""
    assert DEFAULT_IDENTITY == aci_turn.DEFAULT_IDENTITY


_AWKWARD = (
    "",
    ":",
    "%",
    "%3A",
    "a:b",
    "default",
    "släck",
    "C0EXAMPLE1",
    "1700000000.000100",
)


def test_the_identity_form_and_the_pre_identity_form_never_collide() -> None:
    """The two forms differ in segment count, so no key is both.

    An encoded segment never contains ':', so a pre-identity key has exactly
    two separators and an identity key exactly three.
    """
    three = {
        scoped_conversation_id(k, a, c): (k, a, c)
        for k in _AWKWARD
        for a in _AWKWARD
        for c in _AWKWARD
    }
    four = {
        scoped_conversation_id(k, a, c, identity=i): (k, i, a, c)
        for k in _AWKWARD
        for i in _AWKWARD
        if i != "default"
        for a in _AWKWARD
        for c in _AWKWARD
    }
    assert len(three) == len(_AWKWARD) ** 3
    assert len(four) == len(_AWKWARD) ** 3 * (len(_AWKWARD) - 1)
    assert not set(three) & set(four)
    assert {key.count(":") for key in three} == {2}
    assert {key.count(":") for key in four} == {3}


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (
            "slack:C0EXAMPLE1:1700000000.000100",
            ScopedConversation("slack", None, "C0EXAMPLE1", "1700000000.000100"),
        ),
        (
            "slack:second-bot:C0EXAMPLE1:1700000000.000100",
            ScopedConversation("slack", "second-bot", "C0EXAMPLE1", "1700000000.000100"),
        ),
        (
            "email:agentmail-sandbox:agent%40example.test:thread%2F9",
            ScopedConversation("email", "agentmail-sandbox", "agent@example.test", "thread/9"),
        ),
        (
            "slack:C0EXAMPLE1:eval%3A1720000000.000100",
            ScopedConversation("slack", None, "C0EXAMPLE1", "eval:1720000000.000100"),
        ),
    ],
)
def test_parse_reads_back_a_canonical_key(key: str, expected: ScopedConversation) -> None:
    assert parse_scoped_conversation_id(key) == expected


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("1700000000.000100", id="bare-conversation-id"),
        pytest.param("slack:C0EXAMPLE1", id="two-segments"),
        pytest.param("a:b:c:d:e", id="five-segments"),
        pytest.param("slack:default:C0EXAMPLE1:t", id="default-is-never-written"),
        pytest.param("email:agent%40example.test:thread%2f9", id="lowercase-escape"),
        pytest.param("email:agent%zzexample.test:t", id="invalid-escape"),
        pytest.param("email:agent%FFexample.test:t", id="not-utf8"),
    ],
)
def test_parse_refuses_a_key_the_builder_could_not_have_written(key: str) -> None:
    assert parse_scoped_conversation_id(key) is None


def test_parse_inverts_the_builder_over_awkward_components() -> None:
    identities: tuple[str | None, ...] = (
        None,
        *(i for i in _AWKWARD if i != "default"),
    )
    for kind in _AWKWARD:
        for identity in identities:
            for address in _AWKWARD:
                for conversation_id in _AWKWARD:
                    key = scoped_conversation_id(
                        kind, address, conversation_id, identity=identity
                    )
                    assert parse_scoped_conversation_id(key) == ScopedConversation(
                        kind, identity, address, conversation_id
                    ), key


_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "thread-reset-set.json"


def test_parse_round_trips_every_frozen_vector_example() -> None:
    """The earlier parse tests only cover hand-written keys and the
    ``_AWKWARD`` product, never the one corpus the API, the worker, and the
    CLI all freeze together (``tests/vectors/thread-reset-set.json``). A
    parser and a builder that agree on invented keys but disagree on the
    shared vector would still pass every other test in this file."""
    examples = json.loads(_VECTOR.read_text())["thread_key_examples"]
    assert examples
    for example in examples:
        identity = aci_turn.route_identity(example["kind"], example.get("adapter"))
        expected_identity = None if identity == DEFAULT_IDENTITY else identity
        assert parse_scoped_conversation_id(example["thread_key"]) == ScopedConversation(
            example["kind"], expected_identity, example["channel"], example["conversation_id"]
        ), example
