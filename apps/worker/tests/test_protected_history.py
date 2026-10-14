"""Pure protected transcript identity, @spec PROTECTED-HOOK-LANE-7.

Uses real frozen admission records and ACI turns; no broker or database fixture.
The complete proof vectors follow test_admission_records/test_worker_verification.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json

import pytest
from aci_protocol import QueuedTurn
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER, ReplyHandle
from channel_protocol import parse_scoped_conversation_id, scoped_conversation_id
from curie_protected_hooks.admission_records import (
    execution_session_key,
    parse_envelope,
    parse_intent,
)
from curie_protected_hooks.worker_verification import Dispatchable

RUNTIME = "33333333-3333-4333-8333-333333333333"
OTHER_RUNTIME = "33333333-3333-4333-8333-333333333334"


def canonical(value: object) -> bytes:
    """Independent ASCII wire oracle, @spec PROTECTED-HOOK-LANE-7."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def dispatchable(
    *,
    runtime: str = RUNTIME,
    generation: str = "1",
    conversation: str = "conversation/example",
    kind: str = "slack",
    channel: str = "channel/example",
    adapter: str | None = "acme-bot",
    identity: str | None = None,
    absent_reply: bool = False,
    event: str = "event/example",
    stream_id: str = "1000-0",
) -> Dispatchable:
    """Real closed immutable proof vectors, @spec PROTECTED-HOOK-LANE-7."""
    turn = QueuedTurn.model_validate(
        dict(
            event_id=event,
            conversation_id=conversation,
            author="hook/example",
            text="anonymous prompt",
            received_at="2026-10-03T00:00:00Z",
            source="cron" if absent_reply else "webhook",
            attachments=[],
            hook_run=dict(
                agent_id="11111111-1111-4111-8111-111111111111",
                name="incident",
                slot_utc="2026-10-03T00:00:00Z",
            )
            if absent_reply
            else None,
            tool_access="read-only",
            reply_handle=None
            if absent_reply
            else dict(
                kind=kind,
                channel=channel,
                placeholder=None,
                adapter=adapter,
                identity=identity,
                endpoint=None,
            ),
        )
    )
    payload_hash = hashlib.sha256(turn.model_dump_json().encode()).hexdigest()
    envelope = dict(
        schema_version=1,
        event_id=event,
        source_revision="1",
        runtime_id=runtime,
        runtime_generation=generation,
        manifest_digest="b" * 64,
        qualification_id="44444444-4444-4444-8444-444444444444",
        runner_image_digest="sha256:" + "e" * 64,
        bundle_digest="f" * 64,
        execution_config_digest="1" * 64,
        logical_conversation_key=conversation,
        execution_session_key=execution_session_key(runtime, generation, conversation),
        payload_sha256=payload_hash,
    )
    intent = dict(
        schema_version=1,
        identity=dict(
            agent_id="11111111-1111-4111-8111-111111111111",
            hook="incident",
            delivery_id="delivery/example",
        ),
        requested_tool_access=None,
        effective_tool_access="read-only",
        request_body_sha256="a" * 64,
        source_generation="1",
        source_operation_id="22222222-2222-4222-8222-222222222222",
        policy_fingerprint="c" * 64,
        manifest_digest=envelope["manifest_digest"],
        runtime_id=runtime,
        runtime_generation=generation,
        qualification_id=envelope["qualification_id"],
        event_id=event,
        conversation_id=conversation,
        payload_sha256=payload_hash,
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
        reserved_stream_id=stream_id,
        created_at_ms="1000",
        deadline_ms="301000",
    )
    return Dispatchable(turn, parse_envelope(canonical(envelope)), parse_intent(canonical(intent)))


def history_key(value: Dispatchable) -> str:
    """Missing implementation is RED in the body, @spec PROTECTED-HOOK-LANE-7."""
    name = "curie_worker.protected_history"
    assert importlib.util.find_spec(name) is not None, "protected_history module is absent"
    product = importlib.import_module(name)
    assert hasattr(product, "protected_history_key"), "protected_history_key is absent"
    return product.protected_history_key(value)


def expected_key(value: Dispatchable, resolved_identity: str | None) -> str:
    """Independent complete canonical vector, @spec PROTECTED-HOOK-LANE-7."""
    envelope = value.envelope.as_dict()
    handle = value.turn.reply_handle
    assert handle is not None
    address = hashlib.sha256(
        canonical(
            [
                "protected-history-v1",
                envelope["runtime_id"],
                envelope["runtime_generation"],
                handle.kind,
                handle.channel,
                resolved_identity,
            ]
        )
    ).hexdigest()
    return scoped_conversation_id("@protected", address, envelope["execution_session_key"])


def test_exact_protected_key_vector_and_canonical_round_trip():
    """@spec PROTECTED-HOOK-LANE-7."""
    value = dispatchable()
    result = history_key(value)
    assert result == expected_key(value, "acme-bot")
    parsed = parse_scoped_conversation_id(result)
    assert parsed is not None
    assert parsed.kind == "@protected"
    assert parsed.identity is None
    assert len(parsed.address) == 64
    assert parsed.conversation_id == value.envelope.as_dict()["execution_session_key"]
    assert result.isascii()


@pytest.mark.parametrize(
    "change",
    [
        {"channel": "other/channel"},
        {"kind": "email"},
        {"adapter": "acme-other"},
        {"generation": "2"},
        {"runtime": OTHER_RUNTIME},
        {"conversation": "other/conversation"},
    ],
)
def test_actual_route_runtime_and_logical_conversation_are_disjoint(change):
    """@spec PROTECTED-HOOK-LANE-7."""
    assert history_key(dispatchable()) != history_key(dispatchable(**change))


def test_copied_delivery_and_retry_do_not_change_history():
    """Event/PEL identities do not partition history, @spec PROTECTED-HOOK-LANE-7."""
    original = dispatchable()
    retry = dispatchable(event="event/another-delivery", stream_id="1001-0")
    assert original.envelope.canonical_bytes != retry.envelope.canonical_bytes
    assert history_key(original) == history_key(retry)
    assert history_key(original) == history_key(original)


@pytest.mark.parametrize("adapter", [None, "default", CLUSTER_MESSAGE_ADAPTER])
def test_default_slack_binding_survives_delivery_adapter_selection(adapter):
    """External delivery selector is preserved, @spec PROTECTED-HOOK-LANE-7."""
    value = dispatchable(adapter=adapter)
    before = value.turn.model_dump_json()
    assert history_key(value) == expected_key(value, "default")
    assert value.turn.model_dump_json() == before
    assert value.turn.reply_handle.adapter == adapter


def test_cluster_message_uses_named_binding_identity_without_rewriting_handle():
    """@spec PROTECTED-HOOK-LANE-7 WORKER-CANARY-1."""
    value = dispatchable(adapter=CLUSTER_MESSAGE_ADAPTER, identity="acme-bot")
    before = value.turn.model_dump_json()
    assert history_key(value) == expected_key(value, "acme-bot")
    assert history_key(value) == history_key(dispatchable(adapter="acme-bot"))
    assert history_key(value) != history_key(dispatchable(adapter=CLUSTER_MESSAGE_ADAPTER))
    assert value.turn.model_dump_json() == before
    assert value.turn.reply_handle.adapter == CLUSTER_MESSAGE_ADAPTER
    assert value.turn.reply_handle.identity == "acme-bot"


def test_non_slack_null_adapter_is_not_slack_default_identity():
    """@spec PROTECTED-HOOK-LANE-7."""
    value = dispatchable(kind="email", adapter=None)
    assert history_key(value) == expected_key(value, None)
    assert history_key(value) != history_key(dispatchable(kind="email", adapter="default"))


@pytest.mark.parametrize(
    "left,right",
    [
        ({"channel": "a:b", "adapter": "c"}, {"channel": "a", "adapter": "b:c"}),
        ({"kind": "a:b", "channel": "c"}, {"kind": "a", "channel": "b:c"}),
        ({"channel": "例:é/%40"}, {"channel": "例:é/@"}),
        ({"channel": "é"}, {"channel": "e\u0301"}),
    ],
)
def test_colon_percent_and_unicode_boundaries_are_not_normalized_or_concatenated(left, right):
    """@spec PROTECTED-HOOK-LANE-7."""
    first = dispatchable(kind="email", **left) if "kind" not in left else dispatchable(**left)
    second = dispatchable(kind="email", **right) if "kind" not in right else dispatchable(**right)
    assert history_key(first) != history_key(second)
    assert history_key(first) == expected_key(first, first.turn.reply_handle.adapter)
    assert history_key(second) == expected_key(second, second.turn.reply_handle.adapter)


def test_absent_reply_refuses_without_inventing_route():
    """@spec PROTECTED-HOOK-LANE-7."""
    value = dispatchable(absent_reply=True)
    # Product module absence must not accidentally satisfy the refusal assertion.
    history_key(dispatchable())
    with pytest.raises(ValueError):
        history_key(value)
    assert value.turn.reply_handle is None


def test_history_derivation_does_not_rewrite_any_proof_or_external_identity():
    """@spec PROTECTED-HOOK-LANE-7."""
    value = dispatchable(kind="email", channel="a:例", adapter="adapter:é")
    before = (
        value.turn.model_dump_json(),
        value.envelope.canonical_bytes,
        value.intent.canonical_bytes,
    )
    history_key(value)
    assert before == (
        value.turn.model_dump_json(),
        value.envelope.canonical_bytes,
        value.intent.canonical_bytes,
    )
    assert isinstance(value.turn.reply_handle, ReplyHandle)
