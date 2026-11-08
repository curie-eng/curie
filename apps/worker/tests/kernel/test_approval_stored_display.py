"""ADR 0271 decision 8: the card describes the stored values the creation returned.

Same harness discipline as the approval lifecycle suite: real Valkey, the real
substrate over a fake Kubernetes client, an in-process fake ACI runner. Only the
model, the reply transport and the approval API (a recording fake at the
``ApprovalClient`` seam returning ``CreatedApproval``) are faked.

A V2 creation receipt hands back the row's stored ``granted_tool`` and
``granted_arguments``. The card text, the card prompt, the pending notice, the
notification and the summary remembered for settlement then all equal
``describe_approval(stored_tool, stored_arguments)``: the runner's
bundle-authored ``approval_display`` and the model's own arguments never reach
them, and the opaque ``approval_ticket`` is never shown. A V1 receipt keeps
today's rendering exactly. Synthetic identifiers only.

@spec REQUEST-APPROVAL-4 REQUEST-APPROVAL-8.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import uuid
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta
from channel_protocol import ConfirmIntent
from curie_worker import approvals as approvals_module
from curie_worker.approval_wording import approval_display, describe_approval

from ..queue_fixtures import qevent
from .test_approval_lifecycle import RoutedBinding, _resolution_route, _split_approval_routes
from .test_http_approval_pause import Creator, process, raw_payload
from .test_http_identity_authority import Binding, Core

AWAITING = SessionStatus.AWAITING_APPROVAL
DONE = SessionStatus.DONE
RECEIVED_AT = "2026-07-14T00:00:00+00:00"
TOOL = "mcp__access__execute_approved"
TICKET = "cat1." + "SyntheticTicketBody" * 40 + ".SyntheticTicketSignature"
MODEL_ARGUMENTS: dict[str, Any] = {"note": "model-authored-note"}
STORED_ARGUMENTS: dict[str, Any] = {
    "account": "acct-example-1",
    "approval_ticket": TICKET,
    "note": "stored-note-value",
}
BUNDLE_DISPLAY = "Bundle-authored display sentence for this gate"
MACHINE = "Tool call awaiting approval: " + TOOL + " " + json.dumps(MODEL_ARGUMENTS)
SELECTED = (
    {"tenant_id": "00000000-0000-4000-8000-000000000001",
     "issuer": "https://idp.example.test/directory", "subject": "SyntheticApproverA"},
)
EXPECTED = "Approve execute approved. Account: acct-example-1; Note: stored-note-value"
PREVIEW_NOTICE = "Details exceed this preview; review the original request before approving."
POLICY_CHANNEL = "C0EXAMPLE1"


def require_stored_fields() -> None:
    """Absence of the stored creation values is an execution assertion, not a crash."""
    names = {field.name for field in dataclasses.fields(approvals_module.CreatedApproval)}
    missing = sorted({"granted_tool", "granted_arguments", "selected_approvers"} - names)
    assert not missing, f"CreatedApproval does not carry stored values: {missing}"


def created(approval_id: str, stored: dict[str, Any] | None) -> Any:
    """A ``CreatedApproval`` as a V2 (stored values) or V1 (none) receipt produces it."""
    fields: dict[str, Any] = {"id": approval_id, "status": "pending"}
    if stored is not None:
        fields.update(
            requested_by=None, requester_known=True, granted_tool=TOOL,
            granted_arguments=dict(stored), selected_approvers=SELECTED,
        )
    return approvals_module.CreatedApproval(**fields)


class StoredApprovals:
    """A recording ``ApprovalClient`` fake whose receipt carries ``stored`` (or nothing)."""

    def __init__(self, stored: dict[str, Any] | None) -> None:
        self.stored = stored
        self.requests: list[Any] = []

    async def create(self, request: Any, *, budget_s: float = 120) -> Any:
        self.requests.append(request)
        return created(f"appr-{len(self.requests)}", self.stored)


class StoredCreator(Creator):
    """The HTTP identity creation boundary, answering with a V2 receipt's stored values."""

    approval_id = "33333333-3333-4333-8333-333333333333"

    async def create(self, request: Any, **kwargs: Any) -> Any:
        """@spec REQUEST-APPROVAL-4: a selected turn's create returns stored values."""
        await super().create(request, **kwargs)
        return created(self.approval_id, STORED_ARGUMENTS)


def awaiting(display: str | None, *, route: str | None = "managers") -> list[Any]:
    """A permission gate whose model arguments and bundle display differ from storage."""
    return [
        TextDelta(text="Requesting sign-off"),
        Final(
            text="Requesting sign-off",
            status=AWAITING,
            approval_summary=MACHINE,
            approval_display=display,
            approval_route=route,
            approval_gate_kind="permission",
            approval_granted_tool=TOOL,
            approval_granted_arguments=dict(MODEL_ARGUMENTS),
        ),
    ]


def turn(text: str, thread: str) -> QueuedTurn:
    return qevent(text, thread=thread, event_id="ev-" + uuid.uuid4().hex, received_at=RECEIVED_AT)


def expiry(thread: str, approval_id: str) -> QueuedTurn:
    """The API's expiry resume turn, which settles the card from remembered memory."""
    return QueuedTurn(
        event_id=f"approval-{approval_id}-resolved",
        conversation_id=thread,
        author="system",
        text="[approval expired] not approved in time",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1", endpoint=None),
        received_at=RECEIVED_AT,
    )


def card(h: Any) -> tuple[str, Any]:
    """The one interactive card the pause posted, with its address."""
    cards = [
        (address, message) for address, message, *_ in h.sink.posts
        if isinstance(message.interaction, ConfirmIntent)
    ]
    assert len(cards) == 1, h.sink.posts
    return cards[0]


def pending_notice(h: Any, approval_id: str) -> str:
    """The requester-thread notice carrying the approval summary."""
    marker = f"Awaiting approval ({approval_id})"
    notices = [text for _, _, text in h.sink.updates if marker in text]
    assert notices, h.sink.updates
    return notices[-1]


async def remembered(h: Any, approval_id: str) -> str:
    """The summary the card store keeps for settlement."""
    entry = await h.card_store.read(approval_id)
    assert entry is not None, "the approval card was not remembered"
    return entry[0].summary


def assert_stored_only(*texts: str) -> None:
    """Stored values show; the bundle display, model arguments and ticket never do."""
    for text in texts:
        assert "stored-note-value" in text and "acct-example-1" in text, text
        assert BUNDLE_DISPLAY not in text, text
        assert "model-authored-note" not in text, text
        assert TICKET not in text and "SyntheticTicketBody" not in text, text
        assert "approval_ticket" not in text and "Approval ticket" not in text, text


@pytest.mark.parametrize("display", [BUNDLE_DISPLAY, None], ids=["bundle-display", "no-display"])
def test_v2_stored_values_render_the_card_notice_notification_and_card_memory(
    make_harness: Any, display: str | None,
) -> None:
    """@spec REQUEST-APPROVAL-8: the stored values replace every runner or model summary."""
    require_stored_fields()
    assert describe_approval(TOOL, STORED_ARGUMENTS) == EXPECTED

    async def go() -> None:
        approvals = StoredApprovals(STORED_ARGUMENTS)
        binding = RoutedBinding(_split_approval_routes())
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = awaiting(display)
            await h.kernel.process_event(turn("please execute", "th-stored-display"))

            # The durable request still carries what the model asked for.
            request = approvals.requests[0]
            assert request.summary == MACHINE
            assert (request.granted_tool, request.granted_arguments) == (TOOL, MODEL_ARGUMENTS)

            address, message = card(h)
            assert address == POLICY_CHANNEL
            assert message.text == EXPECTED
            assert message.interaction.prompt == EXPECTED
            notice = pending_notice(h, "appr-1")
            assert f"Awaiting approval (appr-1): {EXPECTED}\n" in notice
            notifications = [
                message.text for _, message, *_ in h.sink.posts if message.interaction is None
            ]
            assert len(notifications) == 1
            assert f"Approval appr-1 requires review: {EXPECTED}." in notifications[0]
            summary = await remembered(h, "appr-1")
            assert summary == EXPECTED
            assert_stored_only(message.text, message.interaction.prompt, notice,
                               notifications[0], summary)

    asyncio.run(go())


@pytest.mark.parametrize("display", [BUNDLE_DISPLAY, None], ids=["bundle-display", "no-display"])
def test_v1_receipt_keeps_todays_rendering_exactly(
    make_harness: Any, display: str | None,
) -> None:
    """@spec REQUEST-APPROVAL-8 neighbour: without stored values nothing changes.

    The runner's ``approval_display`` still wins when present; otherwise the
    model's own call is described, as today.
    """
    expected = display or approval_display(MACHINE, TOOL, MODEL_ARGUMENTS)
    assert expected == (display or "Approve execute approved. Note: model-authored-note")

    async def go() -> None:
        approvals = StoredApprovals(None)
        binding = RoutedBinding({"managers": _resolution_route(POLICY_CHANNEL)})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = awaiting(display)
            await h.kernel.process_event(turn("please execute", "th-v1-display"))
            address, message = card(h)
            assert address == POLICY_CHANNEL
            assert message.text == expected
            assert message.interaction.prompt == expected
            assert f"Awaiting approval (appr-1): {expected}\n" in pending_notice(h, "appr-1")
            assert await remembered(h, "appr-1") == expected

    asyncio.run(go())


def test_settlement_shows_the_stored_values_not_the_runner_display(make_harness: Any) -> None:
    """@spec REQUEST-APPROVAL-8: settlement rebuilds the card from the remembered stored values."""
    require_stored_fields()

    async def go() -> None:
        approvals = StoredApprovals(STORED_ARGUMENTS)
        thread = "th-stored-settle"
        async with make_harness(approvals=approvals) as h:
            h.runner.default_script = awaiting(BUNDLE_DISPLAY, route=None)
            await h.kernel.process_event(turn("please execute", thread))
            _, message = card(h)
            assert message.text == EXPECTED
            assert await remembered(h, "appr-1") == EXPECTED

            h.runner.default_script = [Final(text="Acknowledged the expiry.", status=DONE)]
            await h.kernel.process_event(expiry(thread, "appr-1"))
            assert len(h.sink.card_updates) == 1
            _channel, _ts, settled_message, _endpoint, settled = h.sink.card_updates[0]
            assert settled is not None and settled.decision is None
            assert settled_message.text == EXPECTED
            assert_stored_only(settled_message.text)

    asyncio.run(go())


def test_long_stored_values_keep_the_bounded_preview_and_its_notice(make_harness: Any) -> None:
    """@spec REQUEST-APPROVAL-8: the describer's preview bound and truncation notice hold."""
    require_stored_fields()
    long_arguments = {"approval_ticket": TICKET, "note": "stored-" + "n" * 3000}
    expected = describe_approval(TOOL, long_arguments)
    assert expected.endswith("… " + PREVIEW_NOTICE)
    assert len(expected) == 2400 + len("… " + PREVIEW_NOTICE)

    async def go() -> None:
        approvals = StoredApprovals(long_arguments)
        binding = RoutedBinding({"managers": _resolution_route(POLICY_CHANNEL)})
        async with make_harness(approvals=approvals, binding=binding) as h:
            h.runner.default_script = awaiting(BUNDLE_DISPLAY)
            await h.kernel.process_event(turn("please execute", "th-stored-long"))
            _, message = card(h)
            assert message.text == expected
            assert message.interaction.prompt == expected
            notice = pending_notice(h, "appr-1")
            assert f"Awaiting approval (appr-1): {expected}\n" in notice
            summary = await remembered(h, "appr-1")
            assert summary == expected
            for text in (message.text, notice, summary):
                assert PREVIEW_NOTICE in text
                assert BUNDLE_DISPLAY not in text
                assert TICKET not in text and "SyntheticTicketBody" not in text

    asyncio.run(go())


def test_selected_identity_creation_renders_the_receipts_stored_values(make_harness: Any) -> None:
    """@spec REQUEST-APPROVAL-4/8: the HTTP identity path, where a V2 receipt arrives."""
    require_stored_fields()

    async def go() -> None:
        creator = StoredCreator()
        async with make_harness(
            binding=Binding(), identity_authority=Core(), approvals=creator,
        ) as h:
            h.runner.default_script = awaiting(BUNDLE_DISPLAY, route=None)
            event = qevent("please execute", received_at=RECEIVED_AT)
            await process(h, event, raw=raw_payload(event))
            assert len(creator.calls) == 1
            request, kwargs = creator.calls[0]
            assert kwargs.get("identity") is not None
            assert request.granted_arguments == MODEL_ARGUMENTS
            _, message = card(h)
            assert message.text == EXPECTED
            assert message.interaction.prompt == EXPECTED
            summary = await remembered(h, StoredCreator.approval_id)
            assert summary == EXPECTED
            assert_stored_only(message.text, message.interaction.prompt, summary)

    asyncio.run(go())
