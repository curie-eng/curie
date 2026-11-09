"""ADR 0272 approver DM delivery: one selected recipient, one card, one registration.

The kernel cases run the actual consumer-free kernel with real Valkey, the real
``ApprovalCardStore`` and an in-process runner. The API is a recording fake at
the ``ApprovalClient`` seam; the Slack transport is a recording fake at the
``ReplySink`` seam whose acks replay the shape ``chat.postMessage`` answers with.
The client cases drive the actual ``ApprovalClient`` over an HTTPX mock
transport; the Slack sink case drives the actual ``SlackReplyAdapter`` with a
recorded ``chat.postMessage`` response.

@spec APPROVER-DM-1 APPROVER-DM-2 APPROVER-DM-3
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from aci_protocol import Final, SessionStatus
from channel_protocol import ConfirmIntent
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
)
from curie_worker.approval_cards import DEFAULT_CARD_TTL_S
from curie_worker.approvals import ApprovalContinuityRefused, CreatedApproval, SettledApproval
from curie_worker.identity_authority import TurnAuthorityUnavailable
from curie_worker.kernel import approval as kernel_approval
from curie_worker.kernel import log as kernel_log
from curie_worker.reply_sink import TargetRoute

# ruff: noqa: F811 (the shared harness fixture is imported and then requested)
from apps.worker.tests import test_http_approval_clients as clients
from apps.worker.tests.kernel.conftest import FakeSink, make_harness  # noqa: F401
from apps.worker.tests.kernel.test_approval_lifecycle import RecordingApprovals
from apps.worker.tests.kernel.test_http_approval_pause import process, raw_payload
from apps.worker.tests.kernel.test_http_identity_authority import Binding, Core
from apps.worker.tests.queue_fixtures import qevent

APPROVAL_ID = "77777777-7777-4777-8777-777777777777"
NEIGHBOUR_ID = "88888888-8888-4888-8888-888888888888"
INSTALLATION = "22222222-2222-4222-8222-222222222222"
RECIPIENT = "U0EXAMPLE2"
DM_CHANNEL = "D0EXAMPLE1"
CARD_TS = "1700000000.000200"
ROUTE = "managers"
TICKET = {
    "connector": "example-ticket",
    "tool": "approve",
    "key_id": "key-1",
    "public_key": "AAAA-example-public-key",
    "ceiling": {
        "approver_role_version": 1,
        "approver_role": '{"role":"example-approver"}',
        "users": ["U0EXAMPLE1"],
    },
}


def dm_route(identity: Any = "default", **extra: Any) -> dict[str, Any]:
    """A dynamic route whose resolution is one selected approver's DM."""
    return {
        "resolution": {"mode": "approver_dm", "identity": identity},
        "approver_ticket": TICKET,
        "separation": {"exclude_requester": True},
        **extra,
    }


def dm_binding(identity: str = "default", *, required: bool = True) -> Binding:
    """The synthetic HTTP-identity binding, its deployment bound to the DM route."""
    binding = Binding(required=required)
    binding.resolved = binding.resolved.model_copy(
        update={"approval_routes": {ROUTE: dm_route(identity)}}
    )
    binding._by_route[("slack", "C1")] = binding.resolved
    return binding


def awaiting_on_route() -> list[Final]:
    """A runner-held effect that names the DM route."""
    return [
        Final(
            text="Requesting sign-off",
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary="Approve the synthetic operation",
            approval_gate_kind="permission",
            approval_granted_tool="synthetic_read",
            approval_granted_arguments={},
            approval_route=ROUTE,
        )
    ]


def dm_delivery(identity: str = "default") -> Any:
    """The pinned ``ApprovalDmDeliveryV1`` the API returns for a DM creation."""
    from curie_internal.identity_approval import ApprovalDmDeliveryV1

    return ApprovalDmDeliveryV1(
        mode="approver_dm",
        identity=identity,
        provider_installation_id=INSTALLATION,
        recipient=RECIPIENT,
    )


def registration_response(approval_id: str, *, status: str, channel: str | None) -> Any:
    """The pinned registration response as the API would return it."""
    from curie_internal.identity_approval import ApprovalCardRegistrationResponseV1

    return ApprovalCardRegistrationResponseV1.model_validate(
        {
            "schema_version": 1,
            "request_id": str(uuid.uuid4()),
            "approval_id": approval_id,
            "status": status,
            "card_channel": channel,
            "duplicate": False,
        }
    )


class DmCreator:
    """Recording API boundary for a selected DM creation and its card registration."""

    def __init__(
        self,
        *,
        identity: str = "default",
        duplicate: bool = False,
        registration_status: str = "pending",
    ) -> None:
        # Built eagerly: a missing wire model or CreatedApproval field fails here,
        # in the test body, rather than as a swallowed error inside the kernel.
        self.created = CreatedApproval(
            id=APPROVAL_ID,
            status="pending",
            requested_by=None,
            requester_known=True,
            delivery=dm_delivery(identity),
            duplicate=duplicate,
        )
        self.registration_status = registration_status
        self.calls: list[tuple[Any, dict[str, Any]]] = []
        self.registrations: list[dict[str, Any]] = []

    async def create(self, request: Any, **kwargs: Any) -> CreatedApproval:
        self.calls.append((request, kwargs))
        return self.created

    async def register_card(
        self,
        *,
        approval_id: str,
        dedupe_key: str,
        identity: Any,
        card_identity: str,
        recipient: str,
        channel: str,
        **extra: Any,
    ) -> Any:
        self.registrations.append(
            {
                "approval_id": approval_id,
                "dedupe_key": dedupe_key,
                "identity": identity,
                "card_identity": card_identity,
                "recipient": recipient,
                "channel": channel,
                "extra": extra,
            }
        )
        terminal = self.registration_status != "pending"
        return registration_response(
            approval_id,
            status=self.registration_status,
            channel=None if terminal else channel,
        )


class DmSink(FakeSink):
    """The kernel's reply sink, answering a post to the recipient's user id the way
    the Slack adapter does once it reads ``chat.postMessage``'s ``channel``.

    ``mode`` selects the provider outcome: ``ok`` (the DM conversation id),
    ``raise`` (transport failure), ``missing`` (an ack with no channel), or any
    other string, which is returned verbatim as the ack channel.
    """

    def __init__(self, mode: str = "ok") -> None:
        super().__init__()
        self.mode = mode
        self.dm_attempts: list[tuple[ReplyPost, TargetRoute]] = []

    async def emit(
        self, event: Any, *, route: TargetRoute, best_effort_unreachable: bool = False
    ) -> ReplyAck:
        if isinstance(event, ReplyPost) and event.target.address == RECIPIENT:
            self.dm_attempts.append((event, route))
            if self.mode == "raise":
                raise RuntimeError("synthetic Slack transport failure")
            await super().emit(event, route=route, best_effort_unreachable=best_effort_unreachable)
            if self.mode == "missing":
                return ReplyAck(ref=CARD_TS)
            channel = DM_CHANNEL if self.mode == "ok" else self.mode
            return ReplyAck.model_validate({"ref": CARD_TS, "channel": channel})
        return await super().emit(
            event, route=route, best_effort_unreachable=best_effort_unreachable
        )


class Reader:
    """An ApprovalReader with a per-approval record the test can change."""

    def __init__(self, **records: SettledApproval | None) -> None:
        self.records: dict[str, SettledApproval | None] = dict(records)
        self.reads: list[str] = []

    async def get(self, approval_id: str) -> SettledApproval | None:
        self.reads.append(approval_id)
        return self.records.get(approval_id, SettledApproval("pending", None, None, None))


def pending() -> SettledApproval:
    return SettledApproval("pending", None, None, None)


def approved(at: datetime) -> SettledApproval:
    return SettledApproval("approved", RECIPIENT, None, at)


def interactive_posts(sink: FakeSink) -> list[tuple[ReplyPost, TargetRoute]]:
    return [
        (event, route)
        for event, route, _ in sink.events
        if isinstance(event, ReplyPost)
        and event.message is not None
        and isinstance(event.message.interaction, ConfirmIntent)
    ]


def metric_recorder(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    seen: list[tuple[str, dict[str, Any]]] = []
    original = kernel_log.record_metric

    def record(name: str, *args: Any, **kwargs: Any) -> Any:
        seen.append((name, dict(kwargs.get("attributes") or {})))
        return original(name, *args, **kwargs)

    monkeypatch.setattr(kernel_log, "record_metric", record)
    return seen


# --- Item 1: the worker's route parser --------------------------------------------


@pytest.mark.parametrize("identity", ["default", "ops-bot"])
def test_dm_resolution_on_a_dynamic_route_parses_to_the_selected_identity(identity: str) -> None:
    """@spec APPROVER-DM-1: a third resolution form, not a fixed target or the thread."""
    targets = kernel_approval._parse_approval_targets(dm_route(identity))
    assert targets is not None, "the approver_dm resolution was refused"
    target, notification = targets
    assert notification is None
    assert type(target).__name__ == "ApproverDmTarget"
    assert dataclasses.is_dataclass(target) and target.__dataclass_params__.frozen
    assert target.identity == identity


def test_dm_mode_constant_is_pinned() -> None:
    """@spec APPROVER-DM-1."""
    from curie_worker.kernel import constants

    assert constants._APPROVER_DM_MODE == "approver_dm"


@pytest.mark.parametrize(
    "binding",
    [
        pytest.param(
            dm_route(notification={"kind": "slack", "address": "C0EXAMPLE3"}),
            id="with-notification",
        ),
        pytest.param(
            {**dm_route(), "resolution": {"mode": "approver_dm", "identity": "default",
                                          "address": "C0EXAMPLE3"}},
            id="extra-resolution-key",
        ),
        pytest.param(
            {**dm_route(), "resolution": {"mode": "approver_dm", "identity": "default",
                                          "kind": "slack"}},
            id="mixed-with-fixed-target",
        ),
        pytest.param({**dm_route(), "resolution": {"mode": "approver_dm"}}, id="missing-identity"),
        pytest.param(dm_route(None), id="null-identity"),
        pytest.param(dm_route(""), id="empty-identity"),
        pytest.param(dm_route("Ops-Bot"), id="uppercase-identity"),
        pytest.param(dm_route("ops bot"), id="space-identity"),
        pytest.param(dm_route("ops--bot"), id="double-separator-identity"),
        pytest.param(dm_route(7), id="non-string-identity"),
        pytest.param(
            {**dm_route(), "resolution": {"mode": "approver-dm", "identity": "default"}},
            id="misspelled-mode",
        ),
    ],
)
def test_malformed_dm_resolution_fails_closed(binding: dict[str, Any]) -> None:
    """@spec APPROVER-DM-1: out-of-band JSONB cannot widen a DM route."""
    assert kernel_approval._parse_approval_targets(binding) is None


# --- Item 2: selected creation, one card to the recipient, one registration -------


@pytest.mark.parametrize("identity", ["default", "ops-bot"])
def test_selected_dm_pause_posts_one_card_to_the_recipient_and_registers_the_ack_channel(
    make_harness: Any, identity: str
) -> None:
    """@spec APPROVER-DM-2: card_channel unset at creation; the provider's DM id is
    what gets registered and remembered, never the user id posted to."""

    async def run() -> None:
        creator, sink = DmCreator(identity=identity), DmSink()
        async with make_harness(
            binding=dm_binding(identity),
            identity_authority=Core(),
            approvals=creator,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("selected approver DM")
            await process(h, event, raw=raw_payload(event))

            assert len(creator.calls) == 1
            request, kwargs = creator.calls[0]
            assert request.card_channel is None
            assert request.route == ROUTE
            assert request.reply_channel == "C1"

            assert len(sink.dm_attempts) == 1
            card, card_route = sink.dm_attempts[0]
            assert card.target == ReplyTarget(
                kind="slack", address=RECIPIENT, conversation_id=None, reply_ref=None
            )
            assert isinstance(card.message.interaction, ConfirmIntent)
            assert card.message.interaction.id == APPROVAL_ID
            assert card_route.adapter == (None if identity == "default" else identity)
            assert card_route.endpoint is None
            assert interactive_posts(sink) == [(card, card_route)]

            assert len(creator.registrations) == 1
            registration = creator.registrations[0]
            assert registration["approval_id"] == APPROVAL_ID
            assert registration["dedupe_key"] == event.event_id
            assert (
                registration["identity"].model_dump(mode="json")
                == kwargs["identity"].model_dump(mode="json")
            )
            assert registration["card_identity"] == identity
            assert registration["recipient"] == RECIPIENT
            assert registration["channel"] == DM_CHANNEL

            entry = await h.card_store.read(APPROVAL_ID)
            assert entry is not None, "the DM card ref was not remembered"
            ref, _raw = entry
            assert (ref.channel, ref.ts, ref.kind, ref.adapter, ref.endpoint) == (
                DM_CHANNEL,
                CARD_TS,
                "slack",
                None if identity == "default" else identity,
                None,
            )

            # The requesting thread gets the plain pending notice and no card.
            thread_texts = [text for address, _ref, text in sink.updates if address == "C1"]
            assert any(f"Awaiting approval ({APPROVAL_ID})" in text for text in thread_texts)
            assert not [
                event
                for event, _route, _ in sink.events
                if isinstance(event, ReplyPost) and event.target.address == "C1"
            ]
            assert [
                item.operating_mode for item in h.fake_k8s.sandboxes.values()
            ] == ["Suspended"]

    asyncio.run(run())


# --- Item 3: registration racing a decision ---------------------------------------


@pytest.mark.parametrize("decision", ["approved", "expired"])
def test_terminal_registration_settles_the_known_dm_card_immediately(
    make_harness: Any, decision: str
) -> None:
    """@spec APPROVER-DM-2: registration that answers a terminal status settles the
    card it just posted, through the stored ref, in the DM it landed in."""

    async def run() -> None:
        now = datetime.now(UTC)
        record = (
            approved(now)
            if decision == "approved"
            else SettledApproval("expired", None, None, None, now)
        )
        creator, sink = DmCreator(registration_status=decision), DmSink()
        reader = Reader(**{APPROVAL_ID: record})
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            approval_reader=reader,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("decided before registration")
            await process(h, event, raw=raw_payload(event))

            assert len(creator.registrations) == 1
            assert APPROVAL_ID in reader.reads
            assert len(sink.card_updates) == 1
            address, ts, _message, endpoint, settled = sink.card_updates[0]
            assert (address, ts, endpoint) == (DM_CHANNEL, CARD_TS, None)
            assert settled is not None
            assert settled.decision == (decision if decision == "approved" else None)
            updates = [
                (event, route)
                for event, route, _ in sink.events
                if isinstance(event, ReplyUpdate) and event.settled is not None
            ]
            assert len(updates) == 1
            assert updates[0][0].target.kind == "slack"
            assert updates[0][1].adapter is None
            assert await h.card_store.read(APPROVAL_ID) is None, "settled ref was not consumed"

    asyncio.run(run())


def test_pending_registration_leaves_the_dm_card_live(make_harness: Any) -> None:
    """@spec APPROVER-DM-2: a pending registration never stamps the card."""

    async def run() -> None:
        creator, sink = DmCreator(), DmSink()
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            approval_reader=Reader(),
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("pending registration")
            await process(h, event, raw=raw_payload(event))
            assert len(creator.registrations) == 1
            assert sink.card_updates == []
            assert await h.card_store.read(APPROVAL_ID) is not None

    asyncio.run(run())


# --- Item 4: duplicate creation and failed delivery -------------------------------


def test_duplicate_dm_creation_never_reposts_or_reregisters(make_harness: Any) -> None:
    """@spec APPROVER-DM-2: a creation retry adopts the row; it never posts a card."""

    async def run() -> None:
        creator, sink = DmCreator(duplicate=True), DmSink()
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("redelivered DM creation")
            await process(h, event, raw=raw_payload(event))
            assert len(creator.calls) == 1
            assert sink.dm_attempts == []
            assert interactive_posts(sink) == []
            assert creator.registrations == []
            assert await h.card_store.read(APPROVAL_ID) is None

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("raise", id="post-raised"),
        pytest.param("missing", id="ack-without-channel"),
        pytest.param(RECIPIENT, id="ack-echoes-user-id"),
        pytest.param("C0EXAMPLE9", id="ack-public-channel"),
    ],
)
def test_failed_or_uncertain_dm_post_is_recorded_and_never_falls_back(
    make_harness: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    mode: str,
) -> None:
    """@spec APPROVER-DM-2: one attempt, no registration, no other card anywhere."""

    async def run() -> None:
        metrics = metric_recorder(monkeypatch)
        creator, sink = DmCreator(), DmSink(mode)
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("failed DM delivery")
            await process(h, event, raw=raw_payload(event))

            assert len(creator.calls) == 1
            assert len(sink.dm_attempts) == 1, "the DM card was retried"
            assert creator.registrations == []
            assert await h.card_store.read(APPROVAL_ID) is None
            # No fallback: the only interactive post ever attempted is the DM one.
            assert [
                event
                for event, _route, _ in sink.events
                if isinstance(event, ReplyPost) and event.target.address != RECIPIENT
            ] == []
            assert all(
                post_event.target.address == RECIPIENT
                for post_event, _route in interactive_posts(sink)
            )
            assert any(
                "approver DM delivery failed" in record.getMessage()
                for record in caplog.records
            )
            assert (
                "curie.approval.lifecycle",
                {"service.name": "curie-worker", "operation": "dm_delivery",
                 "outcome": "failure"},
            ) in [
                (name, {key: attrs.get(key) for key in ("service.name", "operation", "outcome")})
                for name, attrs in metrics
            ]

    caplog.set_level(logging.INFO)
    asyncio.run(run())


# --- Item 5: legacy turns never create a DM approval ------------------------------


def test_legacy_turn_on_a_dm_route_escalates_and_creates_nothing(
    make_harness: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec APPROVER-DM-1: only a selected (HTTP identity) turn can create a DM
    approval; a legacy turn escalates as an unsupported route."""

    async def run() -> None:
        legacy = RecordingApprovals()
        sink = DmSink()
        async with make_harness(
            binding=dm_binding(required=False),
            approvals=legacy,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("legacy DM route")
            await h.kernel.process_event(event)
            assert legacy.create_calls == 0
            assert sink.dm_attempts == []
            assert interactive_posts(sink) == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    caplog.set_level(logging.WARNING)
    asyncio.run(run())
    assert any(
        "approval-route-unsupported" in record.getMessage() for record in caplog.records
    ), "the legacy DM turn did not escalate as approval-route-unsupported"


# --- Item 5b (plan review R5): pending retention is a prerequisite ---------------


@pytest.mark.parametrize("retention", [False, True], ids=["retention-off", "retention-on"])
def test_selected_dm_turn_requires_pending_retention(
    make_harness: Any, caplog: pytest.LogCaptureFixture, retention: bool
) -> None:
    """@spec APPROVER-DM-1: a DM card ref must outlive the 14-day terminal window, so
    without ``approval_pending_retention_enabled`` a selected turn on a DM route
    escalates ``approval-route-unsupported`` before creating anything; with it the
    same turn creates, posts one DM card and registers it."""

    async def run() -> None:
        creator, sink = DmCreator(), DmSink()
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            sink=sink,
            approval_pending_retention_enabled=retention,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent(f"selected DM turn, retention {retention}")
            await process(h, event, raw=raw_payload(event))
            if retention:
                assert len(creator.calls) == 1
                assert len(sink.dm_attempts) == 1
                assert len(creator.registrations) == 1
                assert await h.card_store.read(APPROVAL_ID) is not None
            else:
                assert creator.calls == []
                assert creator.registrations == []
                assert sink.dm_attempts == []
                assert interactive_posts(sink) == []
                assert await h.card_store.read(APPROVAL_ID) is None

    caplog.set_level(logging.WARNING)
    asyncio.run(run())
    escalated = any(
        "approval-route-unsupported" in record.getMessage() for record in caplog.records
    )
    assert escalated is not retention, (
        "retention off must escalate approval-route-unsupported; retention on must not"
    )


# --- Item 6: the ApprovalClient wire ---------------------------------------------


def _creation_handler(
    seen: list[httpx.Request], *, status: int, delivery: dict[str, Any] | None,
    version: int | None = None,
) -> Any:
    """A DM receipt is the dynamic V2 receipt; an ordinary one stays V1."""

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        sent = json.loads(request.content)
        dynamic = (version or (1 if delivery is None else 2)) == 2
        body = clients.v2_body(sent) if dynamic else clients.response_body(sent)
        body.update(duplicate=status == 200)
        if delivery is not None:
            body["delivery"] = delivery
        return httpx.Response(status, stream=clients.Stream(clients.canonical(body)))

    return handle


DELIVERY_WIRE = {
    "mode": "approver_dm",
    "identity": "default",
    "provider_installation_id": INSTALLATION,
    "recipient": RECIPIENT,
}


@pytest.mark.parametrize("status", [201, 200])
def test_creation_response_with_delivery_is_parsed_into_created_approval(status: int) -> None:
    """@spec APPROVER-DM-2: the server-verified recipient reaches the worker."""

    async def go() -> None:
        seen: list[httpx.Request] = []
        transport = httpx.MockTransport(
            _creation_handler(seen, status=status, delivery=DELIVERY_WIRE)
        )
        async with httpx.AsyncClient(transport=transport) as http:
            result = await clients.invoke(clients.client(http))
        assert result.delivery == dm_delivery()
        assert result.delivery.recipient == RECIPIENT
        assert result.duplicate is (status == 200)
        assert len(seen) == 1

    asyncio.run(go())


def test_a_v1_receipt_carrying_delivery_is_unavailable() -> None:
    """@spec APPROVER-DM-2: delivery rides only the dynamic V2 receipt."""

    async def go() -> None:
        seen: list[httpx.Request] = []
        transport = httpx.MockTransport(
            _creation_handler(seen, status=201, delivery=DELIVERY_WIRE, version=1)
        )
        async with httpx.AsyncClient(transport=transport) as http:
            with pytest.raises(TurnAuthorityUnavailable):
                await clients.invoke(clients.client(http))

    asyncio.run(go())


def test_creation_response_without_delivery_parses_to_none() -> None:
    """@spec APPROVER-DM-2: every non-DM creation keeps its exact response."""

    async def go() -> None:
        seen: list[httpx.Request] = []
        transport = httpx.MockTransport(_creation_handler(seen, status=201, delivery=None))
        async with httpx.AsyncClient(transport=transport) as http:
            result = await clients.invoke(clients.client(http))
        assert result.delivery is None
        assert result.duplicate is False

    asyncio.run(go())


def _registration_owner(http: httpx.AsyncClient) -> Any:
    owner = clients.client(http)
    assert callable(getattr(owner, "register_card", None)), "register_card is not implemented"
    return owner


async def _register(owner: Any) -> Any:
    return await owner.register_card(
        approval_id=clients.OTHER,
        dedupe_key="event-1",
        identity=clients.identity(),
        card_identity="default",
        recipient=RECIPIENT,
        channel=DM_CHANNEL,
    )


def _registration_body(request: httpx.Request, **changes: Any) -> dict[str, Any]:
    wire = json.loads(request.content)
    body = {
        "schema_version": 1,
        "request_id": wire["request_id"],
        "approval_id": clients.OTHER,
        "status": "pending",
        "card_channel": DM_CHANNEL,
        "duplicate": False,
    }
    body.update(changes)
    return body


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({}, ("pending", DM_CHANNEL, False)),
        ({"duplicate": True}, ("pending", DM_CHANNEL, True)),
        ({"status": "approved", "card_channel": None}, ("approved", None, False)),
    ],
)
def test_register_card_posts_the_pinned_request_and_parses_the_response(
    changes: dict[str, Any], expected: tuple[Any, ...]
) -> None:
    """@spec APPROVER-DM-2: worker token only, exact body, pinned response."""

    async def go() -> None:
        seen: list[httpx.Request] = []

        async def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            body = clients.canonical(_registration_body(request, **changes))
            return httpx.Response(200, stream=clients.Stream(body))

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            headers={"Authorization": "Bearer Ambient", "X-API-Key": "Ambient"},
        ) as http:
            result = await _register(_registration_owner(http))
        assert (result.status, result.card_channel, result.duplicate) == expected
        assert result.approval_id == clients.OTHER
        assert len(seen) == 1
        request = seen[0]
        clients.assert_request(
            request, f"/v1/internal/identity/approvals/{clients.OTHER}/card", 2_097_152
        )
        wire = json.loads(request.content)
        assert set(wire) == {
            "schema_version", "request_id", "approval_id", "dedupe_key", "identity",
            "card_identity", "recipient", "channel",
        }
        assert wire["schema_version"] == 1
        assert str(uuid.UUID(wire["request_id"])) == wire["request_id"]
        assert wire["approval_id"] == clients.OTHER
        assert wire["dedupe_key"] == "event-1"
        assert wire["identity"] == clients.identity().model_dump(mode="json")
        assert (wire["card_identity"], wire["recipient"], wire["channel"]) == (
            "default", RECIPIENT, DM_CHANNEL,
        )

    asyncio.run(go())


def test_register_card_conflict_is_a_definitive_refusal() -> None:
    """@spec APPROVER-DM-2: a second, different channel is refused, never retried."""

    async def go() -> None:
        seen: list[httpx.Request] = []

        async def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            body = clients.canonical({"detail": "approval_card_conflict"})
            return httpx.Response(409, stream=clients.Stream(body))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            with pytest.raises(ApprovalContinuityRefused):
                await _register(_registration_owner(http))
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    ("status", "body"),
    [
        pytest.param(200, b"not json", id="non-json"),
        pytest.param(200, None, id="missing-field"),
        pytest.param(200, "extra", id="extra-field"),
        pytest.param(200, "request-id", id="other-request-id"),
        pytest.param(500, b'{"detail":"internal"}', id="server-error"),
        pytest.param(409, b'{"detail":"something_else"}', id="unknown-conflict"),
    ],
)
def test_malformed_registration_response_is_unavailable(status: int, body: Any) -> None:
    """@spec APPROVER-DM-2: an unknown registration outcome is not a decision."""

    async def go() -> None:
        async def handle(request: httpx.Request) -> httpx.Response:
            if body is None:
                payload = _registration_body(request)
                del payload["card_channel"]
                raw = clients.canonical(payload)
            elif body == "extra":
                raw = clients.canonical(_registration_body(request, surprise=1))
            elif body == "request-id":
                raw = clients.canonical(_registration_body(request, request_id=clients.UUID))
            else:
                raw = body
            return httpx.Response(status, stream=clients.Stream(raw))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            with pytest.raises(TurnAuthorityUnavailable):
                await _register(_registration_owner(http))

    asyncio.run(go())


# --- Item 7: the Slack sink reads the DM channel off chat.postMessage -------------


def test_slack_sink_top_level_post_to_a_user_id_acks_the_dm_conversation_id() -> None:
    """@spec APPROVER-DM-2.

    Provider contract: https://docs.slack.dev/reference/methods/chat.postMessage/
    Posting with a user id as ``channel`` begins a conversation in that user's App
    Home; the response's ``channel`` is the DM conversation id (``D...``), not the
    user id that was posted to. The recorded body below is that documented shape.
    """
    from curie_worker.slack_sink import SlackReplyAdapter
    from slack_sdk.web.async_slack_response import AsyncSlackResponse

    from apps.worker.tests.test_slack_sink import _approval_message

    sink = SlackReplyAdapter("xoxb-test")
    client = sink._client_for(None)
    calls: list[dict[str, Any]] = []
    recorded = {
        "ok": True,
        "channel": DM_CHANNEL,
        "ts": CARD_TS,
        "message": {
            "type": "message",
            "subtype": "bot_message",
            "text": "Approve the synthetic operation",
            "bot_id": "B0EXAMPLE1",
            "user": "U0EXAMPLEBOT",
            "app_id": "A0EXAMPLE1",
            "team": "T0EXAMPLE1",
            "ts": CARD_TS,
            "blocks": [],
        },
    }

    async def fake_post(**kwargs: Any) -> AsyncSlackResponse:
        calls.append(kwargs)
        return AsyncSlackResponse(
            client=client,
            http_verb="POST",
            api_url="https://slack.com/api/chat.postMessage",
            req_args={"json": kwargs},
            data=recorded,
            headers={},
            status_code=200,
        )

    client.chat_postMessage = fake_post  # type: ignore[method-assign]
    ack = asyncio.run(
        sink.emit(
            ReplyPost(
                version=REPLY_WIRE_VERSION,
                event="reply.post",
                target=ReplyTarget(
                    kind="slack", address=RECIPIENT, conversation_id=None, reply_ref=None
                ),
                message=_approval_message(APPROVAL_ID, "Approve the synthetic operation"),
                requested_by="U0EXAMPLE1",
            ),
            route=TargetRoute(),
        )
    )
    assert len(calls) == 1
    assert calls[0]["channel"] == RECIPIENT
    assert calls[0].get("thread_ts") is None
    assert ack == ReplyAck.model_validate({"ref": CARD_TS, "channel": DM_CHANNEL})


# --- Item 7b (review r1 F2): no text-only DM card through the real Slack adapter --
#
# Provider contract: https://docs.slack.dev/reference/methods/chat.postMessage/
# A rejected call answers HTTP 200 with ``{"ok": false, "error": <code>}``, which
# the SDK raises as ``SlackApiError``. ``invalid_blocks`` is a definite rejection
# of the Block Kit payload; ``internal_error`` is documented as possibly having
# partly succeeded, so it is ambiguous. In both cases a text-only DM card has no
# buttons and cannot be answered, so the adapter must not post one and the worker
# must not register its channel.

POSTED_DM = {
    "ok": True,
    "channel": DM_CHANNEL,
    "ts": CARD_TS,
    "message": {
        "type": "message",
        "subtype": "bot_message",
        "text": "Requesting sign-off",
        "bot_id": "B0EXAMPLE1",
        "user": "U0EXAMPLEBOT",
        "app_id": "A0EXAMPLE1",
        "team": "T0EXAMPLE1",
        "ts": CARD_TS,
    },
}


class RecordedSlack:
    """A fake ``AsyncWebClient.chat_postMessage``: the blocks post answers ``error``
    (a recorded ``ok: false`` body raised as ``SlackApiError``) or, with ``error``
    None, the recorded DM post; a text-only post answers the same DM body, so a
    fallback would succeed and be visible."""

    def __init__(self, client: Any, error: str | None) -> None:
        self.client, self.error = client, error
        self.calls: list[dict[str, Any]] = []

    def response(self, kwargs: dict[str, Any], data: dict[str, Any]) -> Any:
        from slack_sdk.web.async_slack_response import AsyncSlackResponse

        return AsyncSlackResponse(
            client=self.client,
            http_verb="POST",
            api_url="https://slack.com/api/chat.postMessage",
            req_args={"json": kwargs},
            data=data,
            headers={},
            status_code=200,
        )

    async def chat_postMessage(self, **kwargs: Any) -> Any:  # noqa: N802 (SDK name)
        from slack_sdk.errors import SlackApiError

        self.calls.append(kwargs)
        if kwargs.get("blocks") is not None and self.error is not None:
            raise SlackApiError(
                f"The request to the Slack API failed. (url: chat.postMessage) "
                f"The server responded with: {{'ok': False, 'error': '{self.error}'}}",
                self.response(kwargs, {"ok": False, "error": self.error}),
            )
        return self.response(kwargs, POSTED_DM)


class RealSlackDmSink(FakeSink):
    """The kernel's reply sink with the DM card delivered by the REAL
    ``SlackReplyAdapter`` (Block Kit rendering, block fallback policy, ack
    channel); every other event is recorded by ``FakeSink`` as usual."""

    def __init__(self, error: str | None) -> None:
        from curie_worker.slack_sink import SlackReplyAdapter

        super().__init__()
        self.adapter = SlackReplyAdapter("xoxb-test")
        client = self.adapter._client_for(None)
        self.slack = RecordedSlack(client, error)
        client.chat_postMessage = self.slack.chat_postMessage  # type: ignore[method-assign]
        self.dm_attempts: list[tuple[ReplyPost, TargetRoute]] = []

    async def emit(
        self, event: Any, *, route: TargetRoute, best_effort_unreachable: bool = False
    ) -> ReplyAck:
        if isinstance(event, ReplyPost) and event.target.address == RECIPIENT:
            self.dm_attempts.append((event, route))
            return await self.adapter.emit(
                event, route=route, best_effort_unreachable=best_effort_unreachable
            )
        return await super().emit(
            event, route=route, best_effort_unreachable=best_effort_unreachable
        )


def _dm_card_through_the_adapter(
    make_harness: Any, error: str | None
) -> tuple[DmCreator, RealSlackDmSink, Any]:
    """Run one selected DM pause; returns the creator, the sink and the stored ref."""
    creator, sink = DmCreator(), RealSlackDmSink(error)
    stored: list[Any] = []

    async def run() -> None:
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("DM card through the Slack adapter")
            await process(h, event, raw=raw_payload(event))
            stored.append(await h.card_store.read(APPROVAL_ID))

    asyncio.run(run())
    return creator, sink, stored[0]


def test_a_dm_card_posted_by_the_slack_adapter_registers_its_dm_channel(
    make_harness: Any,
) -> None:
    """@spec APPROVER-DM-2: liveness for the refusals below; one blocks post, one
    registration of the DM id Slack answered with."""
    creator, sink, stored = _dm_card_through_the_adapter(make_harness, None)
    assert len(sink.dm_attempts) == 1
    assert len(sink.slack.calls) == 1, sink.slack.calls
    assert sink.slack.calls[0]["channel"] == RECIPIENT
    assert sink.slack.calls[0].get("blocks"), "the DM card was posted without its buttons"
    assert [item["channel"] for item in creator.registrations] == [DM_CHANNEL]
    assert stored is not None and stored[0].channel == DM_CHANNEL


@pytest.mark.parametrize(
    "error",
    [
        pytest.param("invalid_blocks", id="blocks-rejected"),
        pytest.param("internal_error", id="ambiguous-provider-error"),
    ],
)
def test_a_rejected_dm_card_never_falls_back_to_a_text_only_dm(
    make_harness: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: str,
) -> None:
    """@spec APPROVER-DM-2 (review r1 F2): a text-only DM card cannot be answered.

    Slack refuses the Block Kit post. The adapter must not retry the DM as plain
    text (no ``chat.postMessage`` without blocks), and the worker must record a
    DM delivery failure and never register a channel or remember a card ref.
    """
    caplog.set_level(logging.INFO)
    metrics = metric_recorder(monkeypatch)
    creator, sink, stored = _dm_card_through_the_adapter(make_harness, error)

    assert len(sink.dm_attempts) == 1, "the DM card was retried by the worker"
    text_only = [call for call in sink.slack.calls if call.get("blocks") is None]
    assert text_only == [], f"a text-only DM card was posted: {text_only}"
    assert all(call["channel"] == RECIPIENT for call in sink.slack.calls)
    assert creator.registrations == [], "a text-only or failed DM card was registered"
    assert stored is None, "a failed DM card ref was remembered"
    assert any("approver DM delivery failed" in record.getMessage() for record in caplog.records)
    assert (
        "curie.approval.lifecycle",
        {"service.name": "curie-worker", "operation": "dm_delivery", "outcome": "failure"},
    ) in [
        (name, {key: attrs.get(key) for key in ("service.name", "operation", "outcome")})
        for name, attrs in metrics
    ]


def test_an_ordinary_card_keeps_its_text_only_fallback() -> None:
    """Existing behavior: a channel approval card rejected for its blocks still
    falls back to a text-only post in the thread (the API resolve path works
    regardless). Only the DM card loses the fallback."""
    from curie_worker.slack_sink import SlackReplyAdapter

    from apps.worker.tests.test_slack_sink import _approval_message

    sink = SlackReplyAdapter("xoxb-test")
    client = sink._client_for(None)
    slack = RecordedSlack(client, "invalid_blocks")
    client.chat_postMessage = slack.chat_postMessage  # type: ignore[method-assign]
    ack = asyncio.run(
        sink.emit(
            ReplyPost(
                version=REPLY_WIRE_VERSION,
                event="reply.post",
                target=ReplyTarget(
                    kind="slack",
                    address="C0EXAMPLE1",
                    conversation_id="1700000000.000100",
                    reply_ref=None,
                ),
                message=_approval_message(APPROVAL_ID, "Approve the synthetic operation"),
                requested_by="U0EXAMPLE1",
            ),
            route=TargetRoute(),
        )
    )
    assert len(slack.calls) == 2, slack.calls
    assert slack.calls[0].get("blocks") and slack.calls[1].get("blocks") is None
    assert {call["channel"] for call in slack.calls} == {"C0EXAMPLE1"}
    assert slack.calls[1]["thread_ts"] == "1700000000.000100"
    assert ack.ref == CARD_TS


# --- Item 8 (AC4): ADR 0273 decision 4 retention applied to the DM ref ------------


def test_dm_card_ref_persists_while_pending_then_gets_bounded_retention_and_exact_delete(
    make_harness: Any,
) -> None:
    """@spec APPROVER-DM-3 / ADR 0273 decision 4.

    A pending DM ref carries no TTL, so it outlives the 14-day terminal window no
    matter how long the approval stays pending; a reconcile pass that reads the
    record as pending keeps it that way. After settlement it carries the bounded
    terminal window, and the exact compare-and-delete consumes only that ref.
    """

    async def run() -> None:
        creator, sink = DmCreator(), DmSink()
        reader = Reader()
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            approval_reader=reader,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("long pending DM")
            await process(h, event, raw=raw_payload(event))
            await h.card_store.remember(
                NEIGHBOUR_ID, channel="C0EXAMPLE1", ts="1700000000.000001",
                summary="Neighbour", endpoint=None, kind="slack",
            )
            key = h.config.approval_card_key(APPROVAL_ID)
            neighbour = h.config.approval_card_key(NEIGHBOUR_ID)
            assert await h.async_redis.pttl(key) == -1
            reader.records[APPROVAL_ID] = pending()
            await h.card_store.reconcile_pending(reader)
            assert await h.async_redis.pttl(key) == -1
            entry = await h.card_store.read(APPROVAL_ID)
            assert entry is not None and entry[0].channel == DM_CHANNEL

            reader.records[APPROVAL_ID] = approved(datetime.now(UTC))
            await h.card_store.reconcile_pending(reader)
            ttl = await h.async_redis.pttl(key)
            assert 0 < ttl <= DEFAULT_CARD_TTL_S * 1000
            assert await h.async_redis.pttl(neighbour) == -1

            entry = await h.card_store.read(APPROVAL_ID)
            assert entry is not None
            ref, raw = entry
            assert ref.channel == DM_CHANNEL and ref.ts == CARD_TS
            assert not await h.card_store.consume(APPROVAL_ID, "stale-value")
            assert await h.card_store.consume(APPROVAL_ID, raw)
            assert not await h.async_redis.exists(key)
            assert await h.card_store.read(NEIGHBOUR_ID) is not None

    asyncio.run(run())


def test_dm_card_ref_settled_past_the_terminal_window_is_deleted_alone(
    make_harness: Any,
) -> None:
    """@spec ADR 0273 decision 4: a decision 15 days old leaves no DM reference behind
    and touches no other approval's reference."""

    async def run() -> None:
        creator, sink = DmCreator(), DmSink()
        reader = Reader()
        async with make_harness(
            binding=dm_binding(),
            identity_authority=Core(),
            approvals=creator,
            approval_reader=reader,
            sink=sink,
            approval_pending_retention_enabled=True,
        ) as h:
            h.runner.default_script = awaiting_on_route()
            event = qevent("DM decided long ago")
            await process(h, event, raw=raw_payload(event))
            await h.card_store.remember(
                NEIGHBOUR_ID, channel="C0EXAMPLE1", ts="1700000000.000001",
                summary="Neighbour", endpoint=None, kind="slack",
            )
            assert await h.card_store.read(APPROVAL_ID) is not None
            reader.records[APPROVAL_ID] = approved(datetime.now(UTC) - timedelta(days=15))
            await h.card_store.reconcile_pending(reader)
            assert not await h.async_redis.exists(h.config.approval_card_key(APPROVAL_ID))
            assert await h.async_redis.pttl(h.config.approval_card_key(NEIGHBOUR_ID)) == -1

    asyncio.run(run())
