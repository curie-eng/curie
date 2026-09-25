"""The worker's thread key carries the route's identity (ADR-0168 decision 4).

It names the sandbox route, the Valkey thread lock, the order lock, the claim
name, the session id and the history ref, so two identities in one thread
must get two keys, and every pre-ADR Slack key must be unchanged.
"""

from __future__ import annotations

import pytest
from aci_protocol import QueuedTurn, ReplyHandle
from curie_worker.kernel import _thread_key_for


def _turn(
    kind: str,
    channel: str,
    adapter: str | None,
    *,
    conversation_id: str = "1700000000.000100",
    endpoint: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id="EvSIM-identity",
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text="ping",
        reply_handle=ReplyHandle(
            kind=kind,
            channel=channel,
            placeholder="p-1",
            endpoint=endpoint,
            adapter=adapter,
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


@pytest.mark.parametrize("adapter", [None, "default"])
def test_a_default_slack_turn_keeps_its_pre_identity_key(adapter: str | None) -> None:
    assert (
        _thread_key_for(_turn("slack", "C0EXAMPLE1", adapter))
        == "slack:C0EXAMPLE1:1700000000.000100"
    )


def test_two_identities_in_one_slack_thread_get_two_keys() -> None:
    first = _thread_key_for(_turn("slack", "C0EXAMPLE1", "first-bot"))
    second = _thread_key_for(_turn("slack", "C0EXAMPLE1", "second-bot"))
    assert first == "slack:first-bot:C0EXAMPLE1:1700000000.000100"
    assert second == "slack:second-bot:C0EXAMPLE1:1700000000.000100"


def test_a_cluster_message_relay_turn_keeps_its_pre_identity_key() -> None:
    # The relay adapter selects a delivery substitution, not an identity: the
    # turn is still the channel's own Slack turn, so its thread key must not
    # gain a segment (ADR-0168 decision 4).
    assert (
        _thread_key_for(_turn("slack", "C0EXAMPLE1", "curie-cluster-message"))
        == "slack:C0EXAMPLE1:1700000000.000100"
    )


def test_a_named_mail_route_keys_its_thread_by_its_adapter() -> None:
    turn = _turn(
        "email",
        "agent@example.test",
        "agentmail-sandbox",
        conversation_id="thread/9",
        endpoint="http://curie-mail-adapter:8080/",
    )
    assert (
        _thread_key_for(turn)
        == "email:agentmail-sandbox:agent%40example.test:thread%2F9"
    )
