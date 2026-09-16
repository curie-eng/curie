"""Contract tests for canonical, rendering-neutral conversation identity."""

from __future__ import annotations

from inspect import signature
from typing import get_type_hints

import pytest
from channel_protocol import scoped_conversation_id


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
    ]
    assert get_type_hints(scoped_conversation_id)["return"] is str

    identity = ("slack", "C0EXAMPLE1", "1700000000.000100")
    assert scoped_conversation_id(*identity) == scoped_conversation_id(*identity)
