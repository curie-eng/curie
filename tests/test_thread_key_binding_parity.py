"""#3767: every thread key the worker produces maps to the binding its credential names.

The API scopes a sandbox's transcript reach by mapping the transcript key (a
thread key) back to its binding with ``threadkeys.transcript_binding`` and
comparing that with the credential's ``binding`` claim (plan section 4). The
worker builds the key in ``kernel._thread_key_for`` and the claim in
``ChannelBinding.boot_env``; neither imports the API. If the two drift for any
key form, the sandbox's own history read is refused and the conversation
silently loses its past. So this repo-level test runs every producer the
worker has (Slack, named identity, the cluster-message relay, hook, targeted
and targetless cron, eval-isolated threads, a non-Slack named route) through
the real worker code and checks the API maps each key to the claim the same
boot env carries.
"""

from __future__ import annotations

import uuid
from typing import Any
from urllib.parse import unquote

import pytest
from aci_protocol import HookRunRef, QueuedTurn, ReplyHandle
from aci_protocol.turn import TurnSource
from channel_protocol import hook_conversation_id, scoped_conversation_id
from curie_api import threadkeys
from curie_internal.sandbox_token import decode
from curie_worker.binding import (
    HISTORY_REF_ENV,
    HISTORY_TOKEN_ENV,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from curie_worker.kernel.routing import _thread_key_for

_AGENT = uuid.UUID("33333333-3333-4333-8333-333333333333")
_OTHER_AGENT = uuid.UUID("44444444-4444-4444-8444-444444444444")
_KEY = "curie-dev-key"
_CHANNEL_A = "C0EXAMPLE1"
_CHANNEL_B = "C0EXAMPLE2"
_TS = "1700000000.000100"


def _resolved() -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name="parity-agent",
        agent_id=_AGENT,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        memory_writes=True,
    )


def _resolver() -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key=_KEY)  # type: ignore[attr-defined]
    return resolver


def _routed(
    kind: str,
    channel: str,
    conversation_id: str,
    *,
    adapter: str | None = None,
    identity: str | None = None,
    endpoint: str | None = None,
    hook_run: HookRunRef | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=f"EvSIM-{uuid.uuid4().hex[:8]}",
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text="ping",
        reply_handle=ReplyHandle(
            kind=kind,
            channel=channel,
            placeholder="p-1",
            endpoint=endpoint,
            adapter=adapter,
            identity=identity,
        ),
        received_at="2026-07-05T00:00:00+00:00",
        hook_run=hook_run,
    )


def _targetless_cron(agent_id: uuid.UUID = _AGENT) -> QueuedTurn:
    # The shape the scheduler mints for a cron with no target (#2963), built
    # like ``kernel/test_targetless_cron.py`` does.
    return QueuedTurn.model_construct(
        event_id=uuid.uuid4().hex,
        conversation_id=f"cron-{uuid.uuid4().hex}",
        author="cron",
        text="run the nightly report",
        reply_handle=None,
        received_at="2026-09-22T03:00:00+00:00",
        source=TurnSource.CRON,
        attachments=[],
        hook_run=HookRunRef(
            agent_id=str(agent_id), name="nightly", slot_utc="2026-09-22T03:00:00+00:00"
        ),
    )


def _boot(qevent: QueuedTurn) -> tuple[str, dict[str, str], str | None, str | None]:
    """The thread key and boot env exactly as the kernel builds them.

    A routed turn passes its handle's kind and channel (``kernel.py`` routing
    block); a targetless cron passes neither. An ``eval:`` conversation also
    passes ``isolate_memory``."""

    key = _thread_key_for(qevent)
    handle = qevent.reply_handle
    if handle is None:
        return key, _resolver().boot_env(_resolved(), key), None, None
    env = _resolver().boot_env(
        _resolved(),
        key,
        kind=handle.kind,
        address=handle.channel,
        isolate_memory=qevent.conversation_id.startswith("eval:"),
    )
    return key, env, handle.kind, handle.channel


def _claims(token: str) -> dict[str, Any]:
    payload = decode(token, _KEY, agent=str(_AGENT), scope="state")
    assert payload is not None, "the boot env history token did not verify"
    return payload


def _admits(claim: str | None, key: str, agent_id: uuid.UUID) -> bool:
    """Plan section 4's transcript rule for a non-legacy sandbox credential."""

    key_binding = threadkeys.transcript_binding(key)
    if claim is None:
        return key_binding == f"@cron:{agent_id}"
    return key_binding is not None and key_binding == claim


# Every routed key form the kernel produces. Each id names the risk it guards.
_ROUTED: list[tuple[str, dict[str, Any]]] = [
    ("slack-default", {"kind": "slack", "channel": _CHANNEL_A, "conversation_id": _TS}),
    (
        "slack-explicit-default-identity",
        {"kind": "slack", "channel": _CHANNEL_A, "conversation_id": _TS, "adapter": "default"},
    ),
    (
        "slack-named-identity",
        {"kind": "slack", "channel": _CHANNEL_A, "conversation_id": _TS, "adapter": "first-bot"},
    ),
    (
        "slack-cluster-message-relay",
        {
            "kind": "slack",
            "channel": _CHANNEL_A,
            "conversation_id": _TS,
            "adapter": "curie-cluster-message",
        },
    ),
    (
        "slack-named-cluster-message-relay",
        {
            "kind": "slack",
            "channel": _CHANNEL_A,
            "conversation_id": _TS,
            "adapter": "curie-cluster-message",
            "identity": "sre-bot",
        },
    ),
    (
        "email-named-route",
        {
            "kind": "email",
            "channel": "agent@example.test",
            "conversation_id": "thread/9",
            "adapter": "agentmail-sandbox",
            "endpoint": "http://curie-mail-adapter:8080/",
        },
    ),
    (
        "email-default-route",
        {
            "kind": "email",
            "channel": "agent+ops@example.test",
            "conversation_id": "<msg:1@example.test>",
            "endpoint": "http://curie-mail-adapter:8080/",
        },
    ),
    (
        "hook-unpartitioned",
        {
            "kind": "slack",
            "channel": _CHANNEL_A,
            "conversation_id": hook_conversation_id(_AGENT, "deploys"),
        },
    ),
    (
        "hook-partitioned",
        {
            "kind": "slack",
            "channel": _CHANNEL_A,
            "conversation_id": hook_conversation_id(_AGENT, "pr-review", "curie-eng/curie#12"),
        },
    ),
    (
        "cron-targeted",
        {
            "kind": "slack",
            "channel": _CHANNEL_A,
            "conversation_id": "cron-nightly",
            "hook_run": HookRunRef(
                agent_id=str(_AGENT), name="nightly", slot_utc="2026-09-22T03:00:00+00:00"
            ),
        },
    ),
    (
        "eval-isolate",
        {"kind": "slack", "channel": _CHANNEL_A, "conversation_id": "eval:1720000000.000100"},
    ),
]


@pytest.mark.parametrize("fields", [f for _, f in _ROUTED], ids=[i for i, _ in _ROUTED])
def test_routed_thread_key_maps_to_the_boot_env_binding_claim(fields: dict[str, Any]) -> None:
    key, env, kind, address = _boot(_routed(**fields))
    claim = _claims(env[HISTORY_TOKEN_ENV])["binding"]

    assert claim == f"{kind}:{address}"
    assert threadkeys.transcript_binding(key) == claim
    assert _admits(claim, key, _AGENT)
    # The runner reads its history at the ref the boot env carries; the API
    # sees that path segment decoded once, which must be the same key.
    ref = env[HISTORY_REF_ENV]
    segment = ref.rsplit("/state/transcript/", 1)[1]
    assert unquote(segment) == key
    assert threadkeys.transcript_binding(unquote(segment)) == claim


@pytest.mark.parametrize("fields", [f for _, f in _ROUTED], ids=[i for i, _ in _ROUTED])
def test_turn_memory_token_names_the_same_binding(fields: dict[str, Any]) -> None:
    qevent = _routed(**fields)
    key, _env, kind, address = _boot(qevent)
    token = _resolver().turn_memory_token(
        _resolved(),
        kind=kind,
        address=address,
        thread_key=key,
        sender="U0EXAMPLE1",
        turn=qevent.event_id,
        ttl_s=300.0,
    )
    if token is None:
        # The kernel gives an eval turn no write credential (it passes
        # isolate_memory instead); nothing else to compare.
        assert qevent.conversation_id.startswith("eval:")
        return
    assert threadkeys.transcript_binding(key) == _claims(token)["binding"]


def test_targetless_cron_key_is_admitted_only_for_its_unbound_credential() -> None:
    key, env, kind, address = _boot(_targetless_cron())
    assert (kind, address) == (None, None)
    claim = _claims(env[HISTORY_TOKEN_ENV])["binding"]

    assert claim is None
    assert threadkeys.transcript_binding(key) == f"@cron:{_AGENT}"
    assert _admits(claim, key, _AGENT)
    # Not another agent's cron thread, and never a bound credential.
    other_key, *_ = _boot(_targetless_cron(_OTHER_AGENT))
    assert not _admits(claim, other_key, _AGENT)
    assert not _admits(f"slack:{_CHANNEL_A}", key, _AGENT)


def test_another_channels_key_maps_to_another_binding() -> None:
    a_key, a_env, *_ = _boot(_routed("slack", _CHANNEL_A, _TS))
    b_key, *_ = _boot(_routed("slack", _CHANNEL_B, _TS))
    b_named, *_ = _boot(_routed("slack", _CHANNEL_B, _TS, adapter="first-bot"))
    claim = _claims(a_env[HISTORY_TOKEN_ENV])["binding"]

    assert threadkeys.transcript_binding(b_key) == f"slack:{_CHANNEL_B}"
    assert not _admits(claim, b_key, _AGENT)
    assert not _admits(claim, b_named, _AGENT)
    # The unbound credential reaches no channel's thread.
    assert not _admits(None, a_key, _AGENT)


@pytest.mark.parametrize(
    "key",
    [
        "not-a-thread-key",
        "eval:1720000000.000100",
        "",
        "slack:C0EXAMPLE1:a:b:c",
        # A lowercase escape does not rebuild byte for byte.
        "email:agent%40example.test:thread%2f9",
    ],
)
def test_a_key_no_producer_builds_maps_to_no_binding(key: str) -> None:
    assert threadkeys.transcript_binding(key) is None
    assert not _admits(f"slack:{_CHANNEL_A}", key, _AGENT)
    assert not _admits(None, key, _AGENT)


@pytest.mark.parametrize(
    ("kind", "adapter", "address", "conversation"),
    [
        ("slack", None, _CHANNEL_A, _TS),
        ("slack", "first-bot", _CHANNEL_A, _TS),
        ("email", "agentmail-sandbox", "agent@example.test", "thread/9"),
    ],
)
def test_api_route_keys_and_pre_identity_keys_map_to_the_route_binding(
    kind: str, adapter: str | None, address: str, conversation: str
) -> None:
    # The API's own builders (used by work items and pre-identity adoption)
    # must land on the same binding as the worker's key for that route.
    current = threadkeys.route_thread_key(kind, adapter, address, conversation)
    assert threadkeys.transcript_binding(current) == f"{kind}:{address}"
    old = threadkeys.pre_identity_thread_key(kind, adapter, address, conversation)
    if old is not None:
        assert old == scoped_conversation_id(kind, address, conversation)
        assert threadkeys.transcript_binding(old) == f"{kind}:{address}"
