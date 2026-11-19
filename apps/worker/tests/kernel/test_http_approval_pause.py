"""HTTP creation carry through the actual consumer and approval pause.

Real Valkey, substrate and in-process runner; API boundaries are synthetic.
These cases prove worker ownership/plumbing, never core admission or activation.
@spec HTTP-APPROVAL-CONTINUITY-3/4/10/12.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import inspect
import json
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from aci_protocol import Final, SessionStatus
from curie_internal.identity_approval import ApprovalCreationIdentityV1
from curie_worker.approvals import (
    TICKET_RETRY_REPLY,
    ApprovalClient,
    ApprovalContinuityRefused,
    CreatedApproval,
)
from curie_worker.binding import GRANT_ARGUMENTS_ENV, GRANT_TOOL_ENV
from curie_worker.capacity_wait import WAIT_REPLY_REF_FIELD
from curie_worker.consumer import Consumer, refuse_unconfigured_reminder
from curie_worker.delivery_lease import DeliveryLeaseStore, LeaseLostError
from curie_worker.identity_authority import TurnAuthorityUnavailable
from curie_worker.kernel import constants, identity
from curie_worker.reply_sink import CLUSTER_MESSAGE_ADAPTER
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

from ..queue_fixtures import qevent
from .test_approval_lifecycle import RecordingApprovals
from .test_http_identity_authority import CAPSULE, Binding, Core, expected_scope, pending_entry


def _identity_approval_contract() -> Any:
    """Load the root ``tests/test_identity_approval.py`` module by its path.

    The repository root ``tests`` directory is not an importable package, so
    ``from tests.test_identity_approval import ...`` resolved only when the same
    pytest process had already collected that file. Run alone, or in a shard
    that does not select it, the import failed with ModuleNotFoundError.
    """
    path = Path(__file__).resolve().parents[4] / "tests" / "test_identity_approval.py"
    spec = importlib.util.spec_from_file_location("_worker_identity_approval_contract", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def raw_payload(event: Any) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-4: preserved whitespace defeats reconstruction."""
    return " \n" + json.dumps(event.model_dump(mode="json"), ensure_ascii=False, indent=1) + "\n"


def digest(raw: str) -> str:
    """@spec HTTP-APPROVAL-CONTINUITY-4."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def awaiting(*, tool: str = "synthetic_read") -> list[Final]:
    """@spec HTTP-APPROVAL-CONTINUITY-4: explicit runner-held effect, no summary fallback."""
    return [Final(
        text="", status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Approve the synthetic operation", approval_gate_kind="permission",
        approval_granted_tool=tool, approval_granted_arguments={},
    )]


async def process(
    harness: Any, event: Any, *, raw: str | None, token: str = CAPSULE,
    original_digest: str | None = None,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4: old ABI reaches an actual missing-data assertion."""
    kwargs: dict[str, Any] = {
        "http_identity_context": token,
        "original_payload_sha256": original_digest or digest(raw or raw_payload(event)),
    }
    if "original_payload" in inspect.signature(harness.kernel.process_event).parameters:
        kwargs["original_payload"] = raw
    await harness.kernel.process_event(event, **kwargs)


class Creator:
    """Recording external API boundary, with transport outcomes selected by the test.

    @spec HTTP-APPROVAL-CONTINUITY-3/4.
    """

    def __init__(self, *, outcome: str = "pending") -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        self.outcome = outcome
        self.calls: list[tuple[Any, dict[str, Any]]] = []
        self.observe: Any = None
        self.at_create: list[Any] = []

    async def create(self, request: Any, **kwargs: Any) -> CreatedApproval:
        """@spec HTTP-APPROVAL-CONTINUITY-3: a missing identity records real legacy transport."""
        self.calls.append((request, kwargs))
        if self.observe is not None:
            self.at_create.append(self.observe())
        # On the old kernel this returns normally, exposing its actual unsafe
        # suspension/card instead of failing because a future exception is absent.
        if kwargs.get("identity") is not None:
            if self.outcome == "refuse":
                raise ApprovalContinuityRefused()
            if self.outcome == "unavailable":
                raise TurnAuthorityUnavailable()
            if self.outcome == "invalid":
                raise ValueError("invalid_approval_creation_request")
        return CreatedApproval(id=str(uuid.uuid4()), status="pending")


def assert_identity(
    creator: Creator, binding: Binding, raw: str, *, token: str = CAPSULE,
) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3/4: assert the actual create arguments, not helper output."""
    assert len(creator.calls) == 1
    request, kwargs = creator.calls[0]
    carried = kwargs.get("identity")
    assert type(carried) is ApprovalCreationIdentityV1, "actual approval create lost HTTP identity"
    assert carried.context == token
    assert carried.payload == raw
    assert carried.scope.model_dump() == expected_scope(binding)
    assert digest(carried.payload) == digest(raw)
    assert set(kwargs) == {"budget_s", "identity"}
    assert "identity" not in request.model_dump()
    assert "original_payload" not in request.model_dump()
    return request


def assert_no_pause_effects(harness: Any, creator: Creator, *, calls: int = 0) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3/4: no new durable pause or unverified delivery."""
    assert len(creator.calls) == calls
    assert all(item.operating_mode != "Suspended" for item in harness.fake_k8s.sandboxes.values())
    assert harness.sink.events == []
    assert harness.sink.posts == []


def test_park_wake_reclaim_keeps_original_bytes_through_actual_approval_pause(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4: original payload and rewritten reply ref are distinct."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4."""
        binding, core, creator = Binding(), Core(), Creator()
        async with make_harness(binding=binding, identity_authority=core, approvals=creator) as h:
            h.runner.default_script = awaiting()
            event = qevent("original café payload", placeholder=None)
            raw = raw_payload(event)
            fields = {"payload": raw, "curie_identity_context_v1": CAPSULE,
                      "synthetic-extra": "retained"}
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
                reminders=refuse_unconfigured_reminder,
            )
            entry_id, observed = await pending_entry(h, consumer, fields)
            lease = await leases.acquire(
                h.config.stream, h.config.consumer_group, entry_id, consumer=h.config.consumer_name,
            )
            assert lease is not None
            try:
                parked = await consumer._waits.park(entry_id, observed, event.event_id, lease)
                notice = await consumer._waits.claim_notice(event.event_id, parked.generation)
                assert notice is not None
                await consumer._waits.finish_notice(
                    event.event_id, parked.generation, notice, delivered=True,
                    reply_ref="synthetic-existing-placeholder",
                )
            finally:
                await leases.release(
                    h.config.stream, h.config.consumer_group, entry_id,
                    owner=lease.owner, resume_event_id=None,
                )
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            assert await consumer._waits.wake_due() == 1
            read = await h.async_redis.xreadgroup(
                h.config.consumer_group, "synthetic-prior-owner", {h.config.stream: ">"}, count=1,
            )
            awakened_id, awakened = read[0][1][0]
            await asyncio.sleep(0.02)
            claimed = await h.async_redis.xclaim(
                h.config.stream, h.config.consumer_group, h.config.consumer_name,
                min_idle_time=1, message_ids=[awakened_id],
            )
            assert len(claimed) == 1
            assert all(claimed[0][1][key] == value for key, value in fields.items())
            assert awakened[WAIT_REPLY_REF_FIELD] == "synthetic-existing-placeholder"
            observed_process = []
            original = h.kernel.process_event

            async def observing(turn: Any, **kwargs: Any) -> None:
                """@spec HTTP-APPROVAL-CONTINUITY-4: delegate the real kernel without repair."""
                observed_process.append((turn, kwargs))
                await original(turn, **kwargs)

            monkeypatch.setattr(h.kernel, "process_event", observing)
            creator.observe = lambda: (
                identity.CURRENT.get(), list(h.sink.events),
                [item.operating_mode for item in h.fake_k8s.sandboxes.values()],
            )
            await consumer._handle(awakened_id, dict(claimed[0][1]))
            assert len(observed_process) == 1
            turn, kwargs = observed_process[0]
            assert kwargs.get("original_payload") == raw, "consumer discarded original wire bytes"
            assert kwargs["original_payload_sha256"] == digest(raw)
            assert turn.reply_handle.placeholder == "synthetic-existing-placeholder"
            assert turn.model_dump_json() != raw
            request = assert_identity(creator, binding, raw)
            assert request.reply_placeholder == "synthetic-existing-placeholder"
            assert request.author == event.author and request.dedupe_key == event.event_id
            carry, sent_before_create, modes_before_create = creator.at_create[0]
            assert carry is not None and carry.scope.model_dump() == expected_scope(binding)
            assert sent_before_create == [] and "Suspended" not in modes_before_create
            assert not identity.is_live(carry), "completed carry retained authority"
            assert [item.operating_mode for item in h.fake_k8s.sandboxes.values()] == ["Suspended"]
            assert len(h.sink.posts) == 1
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert pending["pending"] == 0
    asyncio.run(run())


def test_expired_resume_notification_keeps_wire_author_and_original_invoker_separate(
    make_harness: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4/12: synthetic current-core boundary, no grant proof."""
    from curie_internal import identity_approval, identity_context

    contract = _identity_approval_contract()
    KEY, KEY_ID, context_body = contract.KEY, contract.KEY_ID, contract.context_body

    class NotificationBinding(Binding):
        """@spec HTTP-APPROVAL-CONTINUITY-12: external SQL result is a non-grant notification."""

        async def approval_grant_snapshot(self, *_args: Any) -> None:
            """@spec HTTP-APPROVAL-CONTINUITY-12."""
            return None

    class NotificationCore(Core):
        """@spec HTTP-APPROVAL-CONTINUITY-12: synthetic acceptance, never an API claim."""

        async def check(self, request: Any) -> Any:
            """@spec HTTP-APPROVAL-CONTINUITY-12."""
            result = await super().check(request)
            return result.model_copy(update={"decision": "continue", "reason": "current_authority"})

    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4/12."""
        binding, core, creator = NotificationBinding(), NotificationCore(), Creator()
        body = context_body("resume", status="expired")
        body["scope"].update(expected_scope(binding))
        body["scope"].update(provider="slack", native_id="UOriginalInvoker")
        body["scope"]["namespace"].update(kind="slack_workspace", key="T0EXAMPLE1")
        body["independent_human"] = True
        event = qevent(
            "The earlier approval expired", thread=body["conversation_id"],
            event_id=body["event_id"],
        ).model_copy(update={"author": "system"})
        raw = raw_payload(event)
        body["payload_sha256"] = digest(raw)
        capsule = identity_approval.ResumeContextV1.model_validate(body)
        ring = identity_context.build_identity_keyring("context", ((KEY_ID, 3, KEY),))
        token = identity_approval.seal_resume_context(capsule, ring)
        async with make_harness(binding=binding, identity_authority=core, approvals=creator) as h:
            h.runner.default_script = awaiting()
            await process(h, event, raw=raw, token=token)
            request = assert_identity(creator, binding, raw, token=token)
            assert request.author == "system"
            assert json.loads(creator.calls[0][1]["identity"].payload)["author"] == "system"
            original = identity_approval.open_resume_context(token, ring)
            assert original.scope.native_id == "UOriginalInvoker"
            assert original.original_event_id != event.event_id
            assert core.requests and all(
                item.approval_id == body["approval_id"] for item in core.requests
            )
            assert all(
                GRANT_TOOL_ENV not in env and GRANT_ARGUMENTS_ENV not in env
                for env in h.fake_k8s.claim_envs
            )
    asyncio.run(run())


@pytest.mark.parametrize("defect", ["missing", "mismatched-digest"])
def test_missing_or_changed_original_payload_refuses_before_create_suspend_or_card(
    make_harness: Any, defect: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4: a valid earlier check cannot replace missing wire bytes."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4."""
        creator = Creator()
        async with make_harness(
            binding=Binding(), identity_authority=Core(), approvals=creator,
        ) as h:
            h.runner.default_script = awaiting()
            event = qevent("invalid creation carry")
            raw = None if defect == "missing" else raw_payload(event)
            await process(h, event, raw=raw, original_digest="0" * 64)
            assert_no_pause_effects(h, creator)
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


@pytest.mark.parametrize("cancelled", [False, True], ids=["lost-owner", "cancelled-owner"])
def test_pause_cannot_use_a_lost_or_cancelled_owning_task(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, cancelled: bool,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4: retained context is not live creation authority."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4."""
        creator = Creator()
        async with make_harness(
            binding=Binding(), identity_authority=Core(), approvals=creator,
        ) as h:
            h.runner.default_script = awaiting()
            entered, release = asyncio.Event(), asyncio.Event()
            original = h.kernel._pause_for_approval

            async def pause(*args: Any, **kwargs: Any) -> Any:
                """@spec HTTP-APPROVAL-CONTINUITY-4: expose the actual pause to stale ownership."""
                carry = identity.CURRENT.get()
                assert carry is not None
                if not cancelled:
                    carry.owner = None
                    return await original(*args, **kwargs)
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    try:
                        await original(*args, **kwargs)
                    except (TurnAuthorityUnavailable, identity.HttpIdentityRefused):
                        pass
                    raise
                raise AssertionError("cancelled pause unexpectedly released")

            monkeypatch.setattr(h.kernel, "_pause_for_approval", pause)
            event = qevent("ownership lost before creation")
            task = asyncio.create_task(process(h, event, raw=raw_payload(event)))
            if cancelled:
                try:
                    await asyncio.wait_for(entered.wait(), 3)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            else:
                try:
                    await task
                except TurnAuthorityUnavailable:
                    pass
            assert_no_pause_effects(h, creator)
            assert len(h.runner.opened) == 1
    asyncio.run(run())


def test_selected_publication_gate_cannot_create_or_escalate_through_legacy_pause(
    make_harness: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/10: unsupported selected purpose is terminal and silent."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-1/10."""
        creator = Creator()
        async with make_harness(
            binding=Binding(), identity_authority=Core(), approvals=creator,
        ) as h:
            h.runner.default_script = awaiting(tool=PLATFORM_PUBLISH_TOOL_NAME)
            event = qevent("synthetic unsupported publication")
            await process(h, event, raw=raw_payload(event))
            assert_no_pause_effects(h, creator)
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert len(h.fake_k8s.claim_envs) == 1
    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["refuse", "unavailable", "invalid"])
def test_identity_creation_outcome_never_uses_legacy_escalation_or_unverified_reply(
    make_harness: Any, outcome: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3/12: terminal denial ACKs; unknown remains in real PEL."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3/12."""
        binding, creator = Binding(), Creator(outcome=outcome)
        async with make_harness(binding=binding, identity_authority=Core(), approvals=creator) as h:
            h.runner.default_script = awaiting()
            event = qevent("synthetic creation outcome")
            raw = raw_payload(event)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
                reminders=refuse_unconfigured_reminder,
            )
            entry_id, observed = await pending_entry(h, consumer, {
                "payload": raw, "curie_identity_context_v1": CAPSULE,
            })
            await consumer._handle(entry_id, observed)
            assert_identity(creator, binding, raw)
            assert_no_pause_effects(h, creator, calls=1)
            assert len(h.runner.opened) == len(h.fake_k8s.claim_envs) == 1
            assert len(binding.boots) == 1
            pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert pending["pending"] == (1 if outcome == "unavailable" else 0)
            assert bool(await h.async_redis.exists(h.config.done_key(event.event_id))) is (
                outcome in {"refuse", "invalid"}
            )
            assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0
    asyncio.run(run())


def test_explicit_local_approval_keeps_the_legacy_creator_signature_and_pause(
    make_harness: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-1/3: local legacy creators receive no identity argument."""
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-1/3."""
        legacy = RecordingApprovals()  # This seam deliberately has no identity keyword.
        async with make_harness(binding=Binding(required=False), approvals=legacy) as h:
            h.runner.default_script = awaiting()
            event = qevent("explicit local approval")
            await h.kernel.process_event(event)
            assert legacy.create_calls == 1 and len(legacy.requests) == 1
            assert legacy.requests[0].dedupe_key == event.event_id
            assert [item.operating_mode for item in h.fake_k8s.sandboxes.values()] == ["Suspended"]
            assert len(h.sink.posts) == 1
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
    asyncio.run(run())


def test_selected_email_turn_without_approver_emails_reaches_identity_creation(
    make_harness: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4/15: the approver role, not a route list, decides.

    A legacy email turn with no route approver emails escalates; a selected one
    creates through the identity transport, whose answer owner applies the role.
    """
    async def run() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4/15."""
        binding, creator = Binding(), Creator()
        binding._by_route[("email", "acme-agent@example.com")] = binding.resolved
        async with make_harness(binding=binding, identity_authority=Core(), approvals=creator) as h:
            h.runner.default_script = awaiting()
            event = qevent(
                "synthetic email approval", kind="email", channel="acme-agent@example.com",
                adapter="acme-mail", endpoint="https://mail.example.com/reply",
            )
            raw = raw_payload(event)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=leases,
                reminders=refuse_unconfigured_reminder,
            )
            entry_id, observed = await pending_entry(h, consumer, {
                "payload": raw, "curie_identity_context_v1": CAPSULE,
            })
            await consumer._handle(entry_id, observed)
            request = assert_identity(creator, binding, raw)
            assert request.reply_kind == "email"
            assert request.route is None
            assert request.card_channel == "acme-agent@example.com"
    asyncio.run(run())


@pytest.mark.parametrize(
    ("status", "detail", "settled", "told"),
    [
        (422, "approval_ticket_preparation_expired", True, True),
        (422, "approval_ticket_invalid_ticket", True, True),
        (422, "approval_ticket_arguments_mismatch", True, True),
        (409, "approval_ticket_replayed", True, True),
        (422, "approval_ticket_arguments_unstable", True, False),
        (422, "approval_ticket_requester_mismatch", True, False),
        (422, "invalid_approval_context", True, False),
        (403, "caller_not_allowed", True, False),
        (422, "approver_dm_recipient_count", True, False),
        (503, "authority_unavailable", False, False),
    ],
)
def test_definite_creation_refusal_settles_the_turn(
    make_harness: Any, status: int, detail: str, settled: bool, told: bool,
) -> None:
    """A coded creation refusal ACKs the turn. Availability failure stays pending.

    Draft ADR 0288 decision 3: a refusal of the prepared ticket itself replaces
    the placeholder with one plain sentence; every refusal about a person, a
    route or availability stays silent.
    """

    async def run() -> None:
        async def handle(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, json={"detail": detail})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            approvals = ApprovalClient(
                api_base_url="https://core.example.test",
                api_key="SyntheticPlatformKey",
                client=http,
                read_timeout_s=30,
                worker_token="SyntheticPrivateWorkerToken",
            )
            async with make_harness(
                binding=Binding(), identity_authority=Core(), approvals=approvals,
            ) as h:
                h.runner.default_script = awaiting()
                event = qevent("synthetic creation refusal")
                raw = raw_payload(event)
                consumer = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=DeliveryLeaseStore(h.async_redis, h.config),
                    reminders=refuse_unconfigured_reminder,
                )
                entry_id, observed = await pending_entry(h, consumer, {
                    "payload": raw, "curie_identity_context_v1": CAPSULE,
                })
                # Whether the terminal marker existed when each event left.
                marked_at_emit: list[bool] = []
                emit = h.sink.emit

                async def observing_emit(*args: Any, **kwargs: Any) -> Any:
                    done_key = h.config.done_key(event.event_id)
                    marked_at_emit.append(bool(await h.async_redis.exists(done_key)))
                    return await emit(*args, **kwargs)

                h.sink.emit = observing_emit
                await consumer._handle(entry_id, observed)
                pending = await h.async_redis.xpending(
                    h.config.stream, h.config.consumer_group,
                )
                done = bool(await h.async_redis.exists(h.config.done_key(event.event_id)))
                assert pending["pending"] == (0 if settled else 1)
                assert done is settled
                assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0
                if told:
                    assert [event.event for event, _, _ in h.sink.events] == ["reply.update"]
                    assert h.sink.updates == [
                        (event.reply_handle.channel, event.reply_handle.placeholder,
                         TICKET_RETRY_REPLY),
                    ]
                    assert detail not in TICKET_RETRY_REPLY
                    assert marked_at_emit == [True], "the notice follows the terminal marker"
                else:
                    assert h.sink.events == []
                assert h.sink.posts == []
                assert all(
                    item.operating_mode != "Suspended"
                    for item in h.fake_k8s.sandboxes.values()
                )

    asyncio.run(run())


def test_an_undelivered_ticket_refusal_notice_keeps_the_terminal_outcome(
    make_harness: Any,
) -> None:
    """Draft ADR 0288 decision 3: the notice is best effort, the refusal stays terminal."""

    async def run() -> None:
        async def handle(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(422, json={"detail": "approval_ticket_invalid_ticket"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            approvals = ApprovalClient(
                api_base_url="https://core.example.test",
                api_key="SyntheticPlatformKey",
                client=http,
                read_timeout_s=30,
                worker_token="SyntheticPrivateWorkerToken",
            )
            async with make_harness(
                binding=Binding(), identity_authority=Core(), approvals=approvals,
            ) as h:
                h.runner.default_script = awaiting()
                h.sink.fail_events.add("reply.update")
                event = qevent("synthetic undelivered notice")
                consumer = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=DeliveryLeaseStore(h.async_redis, h.config),
                    reminders=refuse_unconfigured_reminder,
                )
                entry_id, observed = await pending_entry(h, consumer, {
                    "payload": raw_payload(event), "curie_identity_context_v1": CAPSULE,
                })
                await consumer._handle(entry_id, observed)
                pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                assert pending["pending"] == 0
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
                assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0
                assert h.sink.events == []

    asyncio.run(run())


@pytest.mark.parametrize(
    ("adapter", "told"),
    [(None, True), (CLUSTER_MESSAGE_ADAPTER, False)],
    ids=["slack", "relay"],
)
def test_the_ticket_refusal_notice_leaves_the_relay_to_its_own_settlement(
    make_harness: Any, adapter: str | None, told: bool,
) -> None:
    """Draft ADR 0288 decision 3: the relay waiter gets only its existing refusal reply."""

    async def run() -> None:
        async with make_harness(binding=Binding(), identity_authority=Core()) as h:
            event = qevent("synthetic notice", adapter=adapter)
            await h.kernel._notice_after_refusal(event, TICKET_RETRY_REPLY)
            texts = [text for _, _, text in h.sink.updates]
            assert texts == ([TICKET_RETRY_REPLY] if told else [])

    asyncio.run(run())


@pytest.mark.parametrize("how", ["lost", "expired"])
def test_a_lease_lost_while_the_marker_is_written_sends_no_ticket_notice(
    make_harness: Any, how: str,
) -> None:
    """Draft ADR 0288 decision 3: a stale owner never tells the requester.

    The lease is still held at the lifecycle's lease check and is lost, or its
    confirmed window runs out before the heartbeat notices, while the done
    marker is written. Another owner may now hold the event, so this one sends
    nothing.
    """

    async def run() -> None:
        async def handle(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(422, json={"detail": "approval_ticket_invalid_ticket"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            approvals = ApprovalClient(
                api_base_url="https://core.example.test",
                api_key="SyntheticPlatformKey",
                client=http,
                read_timeout_s=30,
                worker_token="SyntheticPrivateWorkerToken",
            )
            async with make_harness(
                binding=Binding(), identity_authority=Core(), approvals=approvals,
            ) as h:
                h.runner.default_script = awaiting()
                event = qevent("synthetic lease lost at the marker")
                consumer = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=DeliveryLeaseStore(h.async_redis, h.config),
                    reminders=refuse_unconfigured_reminder,
                )
                entry_id, observed = await pending_entry(h, consumer, {
                    "payload": raw_payload(event), "curie_identity_context_v1": CAPSULE,
                })
                mark_done = h.kernel._markers.mark_done
                lost: list[bool] = []

                async def losing_mark_done(*args: Any, **kwargs: Any) -> Any:
                    result = await mark_done(*args, **kwargs)
                    lease = constants._DELIVERY_LEASE.get()
                    assert lease is not None, "the refusal runs under a delivery lease"
                    if how == "lost":
                        lease.lost.set()
                    else:
                        lease.local_deadline_monotonic = time.monotonic() - 1
                    lost.append(True)
                    return result

                h.kernel._markers.mark_done = losing_mark_done
                # The stale owner's settlement refuses to ACK: a lost lease is
                # logged and left pending, an expired window raises at the ACK.
                try:
                    await consumer._handle(entry_id, observed)
                except LeaseLostError:
                    assert how == "expired"
                assert lost == [True], "the refusal reached the done marker"
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
                assert h.sink.updates == []
                assert h.sink.posts == []

    asyncio.run(run())
