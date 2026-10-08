from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from aci_protocol import (
    GateKind,
    QueuedTurn,
    ReplyHandle,
)
from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND, route_identity
from channel_protocol import (
    MESSAGE_VERSION,
    Action,
    ConfirmIntent,
    OutboundMessage,
)
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
)
from curie_telemetry.redact import redact_text
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import ValidationError

from ..approval_wording import approval_display
from ..approvals import (
    ApprovalBackendError,
    ApprovalRefused,
    ApprovalRequest,
    CreatedApproval,
    PublicationCreateRequest,
    SettledApproval,
    approver_fields,
    decided_field,
)
from ..reply_sink import (
    CLUSTER_MESSAGE_ADAPTER,
    TargetRoute,
)
from ..sandbox.types import (
    SandboxError,
)
from ..turn_progress import (
    link_progress_resume,
)
from ..workitem_dispatch import (
    parse_work_item_event_id,
)
from ..workspace import (
    WorkspaceSelectionRefused,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import constants, delivery, failures, log, publication, routing, workspace
from .log import logger


@dataclass(frozen=True)
class _ApprovalPause:
    """What ``_pause_for_approval`` did: created the approval, or not and why.

    ``failure_detail`` is the API's redacted, clipped coded refusal (#3617)
    when the create was refused with one; the factory run reports it.
    """

    created: bool
    failure_detail: str | None = None

    @classmethod
    def refused(cls, detail: str | None) -> _ApprovalPause:
        clipped = redact_text(detail)[: constants._ESCALATION_DETAIL_MAX] if detail else None
        return cls(created=False, failure_detail=clipped or None)


def _valid_notification_endpoint(endpoint: Any) -> bool:
    """Mirror the API's absolute HTTP(S), host, and no-userinfo endpoint gate."""

    if not isinstance(endpoint, str):
        return False
    try:
        parsed = urlsplit(endpoint)
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
    )


def _approver_emails(binding: Any) -> list[str]:
    """The approver email addresses a route binding lists (ADR-0177 amendment).

    Without them, nobody can answer an approval shown in an email thread: the
    requester is no longer admitted by default. The API validates the list's
    entries when it is written and re-reads it at resolve time; this answers
    only the raise-time questions "is there anyone at all" and "who, to name on
    the card", so it asks for a non-empty list of strings and fails closed to
    an empty list on any other shape.
    """

    if not isinstance(binding, dict):
        return []
    approvers = binding.get("approvers")
    if not isinstance(approvers, dict):
        return []
    emails = approvers.get("emails")
    if not isinstance(emails, list) or not all(isinstance(e, str) for e in emails):
        return []
    return list(emails)


def _parse_approval_targets(
    binding: Any,
) -> tuple[tuple[str, str] | None, tuple[str, str, TargetRoute] | None] | None:
    """Parse one stored approval binding, or fail closed with ``None``.

    The first element is the fixed card target, or ``None`` when the route's
    resolution is ``{"mode": "requesting_surface"}`` (ADR-0177): the card then
    goes to the requesting turn's own conversation. That form is exactly the
    one key, and it carries no notification, since the card already joins the
    thread that asked; a mix of the mode and a fixed target is refused.

    The API owns complete config validation. This worker boundary independently
    reasserts the authority-bearing envelope and Slack resolution-ID shape
    because JSONB may be written out of band: the retired ``channel`` key, a
    non-Slack resolver, an unknown target field, a half-configured transport,
    or duplicate targets must not produce a durable approval with ambiguous
    authority.

    ADR-0168 decision 3: a Slack notification names its identity in
    ``adapter``, or none for the default, and has no endpoint; any other kind
    needs both. A Slack notification duplicates the resolution when it names
    the same identity (``route_identity``) on the same channel; any other kind
    duplicates it on the raw ``(kind, address)`` pair.
    """

    if not isinstance(binding, dict) or set(binding) - {
        "resolution",
        "notification",
        "approvers",
    }:
        return None

    resolution = binding.get("resolution")
    if resolution == constants._REQUESTING_SURFACE:
        if binding.get("notification") is not None:
            return None
        return None, None
    if (
        not isinstance(resolution, dict)
        or set(resolution) != {"kind", "address"}
        or resolution.get("kind") != constants.POLICY_CARD_KIND
        or not isinstance(resolution.get("address"), str)
        or constants._POLICY_CARD_ADDRESS.fullmatch(resolution["address"]) is None
    ):
        return None
    resolution_pair = (constants.POLICY_CARD_KIND, resolution["address"])

    notification = binding.get("notification")
    if notification is None:
        return resolution_pair, None
    if not isinstance(notification, dict) or set(notification) - {
        "kind",
        "address",
        "endpoint",
        "adapter",
    }:
        return None

    kind = notification.get("kind")
    address = notification.get("address")
    endpoint = notification.get("endpoint")
    adapter = notification.get("adapter")
    address_shape = (
        constants._NOTIFICATION_ADDRESS_SHAPES.get(kind) if isinstance(kind, str) else None
    )
    not_slack = kind != constants.POLICY_CARD_KIND
    if (
        not isinstance(kind, str)
        or constants._CHANNEL_SLUG.fullmatch(kind) is None
        or not isinstance(address, str)
        or not address
        or constants._ROUTE_WHITESPACE.search(address) is not None
        or (address_shape is not None and address_shape.fullmatch(address) is None)
        or (
            adapter is not None
            and (not isinstance(adapter, str) or constants._CHANNEL_SLUG.fullmatch(adapter) is None)
        )
        or (endpoint is None if not_slack else endpoint is not None)
        or (not_slack and adapter is None)
        or (endpoint is not None and not _valid_notification_endpoint(endpoint))
        or (
            (kind, address) == resolution_pair
            if not_slack
            else (kind, route_identity(kind, adapter), address)
            == (constants.POLICY_CARD_KIND, DEFAULT_IDENTITY, resolution_pair[1])
        )
    ):
        return None
    return resolution_pair, (kind, address, TargetRoute(endpoint=endpoint, adapter=adapter))


def _settled_outcome(
    record: SettledApproval,
) -> tuple[SettledOutcome, datetime | None] | None:
    """A resolved record's outcome to stamp and its decision time (#1084).

    None for any record that is not approved or rejected: a pending one has no
    verdict, and an expiry names no decision, so its caller settles it apart.
    """

    if record.status not in ("approved", "rejected"):
        return None
    return (
        SettledOutcome(
            requested_by="",
            decision=record.status,
            resolver=record.resolved_by,
            note=record.resolution_note,
        ),
        record.resolved_at,
    )


def _approval_id_from_resume_event(event_id: str) -> str | None:
    """The approval id inside a resume turn's deterministic event id (#1084).

    The inverse of ``resumequeue.resume_event_id``: ``approval-<id>-resolved``,
    a key the API documents as frozen because the worker's done-marker dedupes
    on it, and which the expiry and resolve paths deliberately share. Reading
    the id off it beats parsing the platform-authored prose, which is written
    for a model rather than for a parser. Returns None on any other shape, so a
    non-resume event never reaches the reader.
    """

    if not event_id.startswith("approval-") or not event_id.endswith("-resolved"):
        return None
    middle = event_id[len("approval-") : -len("-resolved")]
    return middle or None


async def _finalize_settled_card(self: Kernel, qevent: QueuedTurn, route: TargetRoute) -> None:
    """Settle the approval card when its approval resumes (#419, #1084).

    Approval id is the only identity used for the remembered ref. A resolved
    resume reads its durable verdict first, so an unavailable record defers
    settlement without touching the ref. Expiry needs no verdict read.

    The card ref is read without mutation and the Slack edit happens while
    the original value remains stored. Only confirmed delivery is followed
    by an atomic comparison and deletion of that exact value. A failed edit
    leaves the ref available to a reclaimed pass only when that delivery
    also fails before mark_done. Otherwise it remains until TTL.

    Two worker replicas may read the same ref and emit equivalent edits
    because both use the shared renderer. Only one can consume the matching
    value. Sequential redelivery finds no ref after successful cleanup. If a
    newer ref replaces the value during delivery, comparison preserves it.

    This remains fully best effort and never fails the resume. There is
    still no RUNTIME dual read: this path reads exactly one key, the
    approval id's. Refs keyed by thread before the approval id layout are
    instead rekeyed onto their approval id by a one-shot boot migration
    (``ApprovalCardStore.migrate_legacy_thread_keyed_refs``, #1751), so by
    the time a resume arrives they are ordinary entries this read finds. A
    pre-#1199 ref that never recorded its approval id is not migratable and
    still lapses with its TTL.
    """

    if self._card_store is None or not self._is_approval_resume(qevent.event_id):
        return
    approval_id = _approval_id_from_resume_event(qevent.event_id)
    if approval_id is None:
        return
    # Computed once, and the only thing the two forms disagree about. It is
    # an explicit flag rather than "no outcome to stamp" because those two
    # facts coincide only by way of the early return below: soften that
    # return and an APPROVED card whose record blipped would render EXPIRED.
    is_expiry = qevent.text.startswith(constants._EXPIRY_RESUME_MARKER)
    # The card ref is worker-internal state, so it is filed under the scoped
    # thread key like the sandbox and the lock: two channels sharing a
    # conversation id must not pop each other's card. Outside the try because
    # the failure log below names it.
    handle = routing._reply_handle_for(qevent)
    thread_key = routing._thread_key_for(qevent)
    try:
        # Expiry states only that nobody decided, so it needs no record read.
        # A resolve states what was decided, and that comes from the durable
        # record before the card ref is touched.
        outcome: SettledOutcome | None = None
        decided: datetime | None = None
        if not is_expiry:
            read = await self._settled_from_record(approval_id)
            if read is not None:
                outcome, decided = read
            if outcome is None:
                logger.info(
                    "no readable approval outcome for thread %s -- "
                    "leaving its card ref in place, not stamped",
                    thread_key,
                )
                return
        await self._settle_remembered_card(
            approval_id,
            is_expiry=is_expiry,
            outcome=outcome,
            decided=decided,
            conversation_id=qevent.conversation_id,
            handle=handle,
        )
    except Exception as exc:  # noqa: BLE001 - card teardown is best-effort
        logger.warning(
            "approval card teardown failed for thread %s: %s",
            thread_key,
            exc,
        )


async def _settle_remembered_card(
    self: Kernel,
    approval_id: str,
    *,
    is_expiry: bool,
    outcome: SettledOutcome | None,
    decided: datetime | None,
    conversation_id: str,
    handle: ReplyHandle,
) -> None:
    """Edit the remembered card into its settled form, then consume its ref.

    Shared by the resume (``_finalize_settled_card``) and by the pause once
    it registers a card whose record is already settled
    (``_settle_if_decided_before_registration``, #3637). Either may run
    first, or both at once: each emits the same edit, and the atomic
    ``consume`` of the exact value read lets only one of them remove it.
    Raises on failure; each caller owns its own best-effort handling.
    """

    assert self._card_store is not None
    entry = await self._card_store.read(approval_id)
    if entry is None:
        return
    ref, raw_ref = entry
    if is_expiry:
        # An expiry says only that nobody decided, so no outcome is stamped.
        settled = SettledOutcome(requested_by=ref.requested_by)
    else:
        # A caller settles a resolve only once it has read an outcome.
        assert outcome is not None
        settled = SettledOutcome(
            requested_by=ref.requested_by,
            decision=outcome.decision,
            resolver=outcome.resolver,
            note=outcome.note,
        )
    # Emit the channel-neutral summary (ADR-0020) plus the semantic
    # outcome; the adapter renders the buttonless settled card below the
    # seam.
    # The card's own address and ref, over the transport it was posted
    # through. ``settled`` carries the whole difference between an
    # expired card and a resolved one; the adapter renders the form.
    await self._sink.emit(
        ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=ReplyTarget(
                # The card's OWN destination, remembered at post time. A
                # policy-routed card lives in a channel this resume turn
                # may not share a kind or a transport with, so rebuilding
                # either from the turn addresses the wrong place; ``kind``
                # empty is the pre-upgrade entry, which falls back to the
                # turn's kind but NOT its identity: the historical card
                # was posted by the default transport.
                kind=ref.kind or handle.kind,
                address=ref.channel,
                conversation_id=conversation_id,
                reply_ref=ref.ts,
            ),
            message=OutboundMessage(
                version=MESSAGE_VERSION,
                text=ref.summary,
                # When it was decided, as data (ADR-0179); the adapter
                # chooses how to show it.
                fields=[decided_field(decided)] if decided is not None else [],
            ),
            settled=settled,
        ),
        route=TargetRoute(
            endpoint=ref.endpoint,
            adapter=ref.adapter if ref.kind else None,
        ),
    )
    consumed = await self._card_store.consume(approval_id, raw_ref)
    if consumed:
        logger.info("settled approval card for thread %s", conversation_id)
    else:
        logger.info(
            "approval card ref changed or vanished before cleanup for approval %s",
            approval_id,
        )


async def _settle_if_decided_before_registration(
    self: Kernel, approval_id: str, qevent: QueuedTurn
) -> None:
    """Settle a card whose approval was decided before it was registered (#3637).

    The record precedes every delivery, so an operator can resolve it, or
    the sweeper expire it, while the card post is still in flight. Its
    resume then finds no card ref and finishes, and nothing else would
    settle the card this pause registers afterwards. So once the card is
    registered, the pause reads its record back: a decided record settles
    the card now, and a pending one leaves it to the resume.

    Neither order is lost. A verdict recorded before this read is seen
    here, and one recorded after it reaches a resume that runs after the
    card was registered and settles it there. Best-effort, like the
    resume's settle: an unreadable record leaves the card as it is.
    """

    if self._approval_reader is None or self._card_store is None:
        return
    try:
        record = await self._approval_reader.get(approval_id)
        if record is None:
            return
        if record.status == "expired":
            # ``_settled_outcome`` answers only a resolve. An expiry names
            # no decision and stamps none, exactly as its resume would.
            is_expiry, outcome, decided = True, None, None
        else:
            read = _settled_outcome(record)
            if read is None:
                return
            is_expiry = False
            outcome, decided = read
        await self._settle_remembered_card(
            approval_id,
            is_expiry=is_expiry,
            outcome=outcome,
            decided=decided,
            conversation_id=qevent.conversation_id,
            handle=routing._reply_handle_for(qevent),
        )
    except Exception as exc:  # noqa: BLE001 - the pause stands without the settle
        logger.warning("settling approval card %s after registration failed: %s", approval_id, exc)


async def _settled_from_record(
    self: Kernel, approval_id: str
) -> tuple[SettledOutcome, datetime | None] | None:
    """The resolved outcome to stamp and its decision time, from the record.

    Read, not parsed. The resume turn does state the decision, the resolver
    and the note, but it states them in a sentence written for a language
    model. Reconstructing them by regex would make the card's correctness
    depend on that wording.

    None means "do not stamp": no reader configured, a record that could not
    be read, or a record that is somehow not resolved. Leaving a live looking
    card is a smaller wrong than stamping a verdict nobody confirmed.
    """

    if self._approval_reader is None:
        return None
    record = await self._approval_reader.get(approval_id)
    if record is None:
        return None
    return _settled_outcome(record)


async def _place_the_resumed_reply(self: Kernel, qevent: QueuedTurn) -> QueuedTurn:
    """Choose where a resumed approval turn answers (ADR-0179 decision 3).

    When the pause remembered that its card sits in this thread below the
    notice, the turn drops the placeholder the API replays, so its first
    delivery posts a new message after the card (the ADR-0079 path) and the
    rest of the turn edits that message. Otherwise the answer stays on the
    pending notice, adopting its remembered ref when the row carries none.
    The returned turn replaces the caller's for the rest of the turn; the
    durable row and the stream entry are untouched.
    """

    approval_id = _approval_id_from_resume_event(qevent.event_id)
    handle = qevent.reply_handle
    if self._card_store is not None and approval_id is not None and handle is not None:
        try:
            below = await self._card_store.replies_below_card(approval_id)
        except Exception as exc:  # noqa: BLE001 - never fail the resume
            logger.warning(
                "reading the reply placement failed for approval %s: %s", approval_id, exc
            )
            below = False
        if below:
            return qevent.model_copy(
                update={"reply_handle": handle.model_copy(update={"placeholder": None})}
            )
    await self._adopt_remembered_notice_ref(qevent)
    return qevent


async def _adopt_remembered_notice_ref(self: Kernel, qevent: QueuedTurn) -> None:
    """Adopt the pending notice's ref on a ref-less approval resume (#2721).

    The approval row is persisted before any delivery, so a placeholderless
    turn's record replays no ref. The notice's minted ref was remembered
    under the approval id instead; adopting it before the turn streams keeps
    the resumed answer on that same message (#1640). Best-effort: a miss
    only means the answer lands on a new message.
    """

    if self._card_store is None or self._target_for(qevent).reply_ref is not None:
        return
    approval_id = _approval_id_from_resume_event(qevent.event_id)
    if approval_id is None:
        return
    try:
        ref = await self._card_store.read_notice_ref(approval_id)
    except Exception as exc:  # noqa: BLE001 - never fail the resume
        logger.warning("reading notice ref failed for approval %s: %s", approval_id, exc)
        return
    if ref:
        self._adopt_ref(qevent, ReplyAck(ref=ref))


async def _pause_for_approval(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    outcome: failures.TurnOutcome,
    agent_id: uuid.UUID | None,
    approval_routes: dict[str, Any] | None = None,
    *,
    deployment_id: uuid.UUID | None = None,
) -> _ApprovalPause:
    """Persist the approval, suspend the session, and leave the pending notice.

    Ordering is deliberate: the durable record exists before the sandbox is
    suspended, so there is never a suspended session without a record that
    can wake it. The converse crash (record created, suspend or notice
    lost) self-heals -- creation is idempotent on the event id, and the
    resume path cold-claims a fresh sandbox regardless (ADR-0003).

    The record also precedes EVERY delivery (#2721). A placeholderless turn
    persists whatever ref it already holds, possibly None, and the pending
    notice is best-effort: a dead transport must not strand a turn whose row
    and suspension already happened. A ref the notice mints for a ref-less
    row is remembered in the card store so the resume edits that message
    (#1640).

    ``approval_routes`` is the agent's per-deployment route-binding map
    (#247/#1460): ``resolution`` owns the sole interactive card and verified
    identity path; ``notification`` may receive a separate text-only ping.
    A named but UNBOUND or malformed route is ESCALATED loudly rather than
    routed to the requesting channel (#544, Decision B, reversing #247):
    silently widening authority to whoever happens to be in the requesting
    channel is exactly the failure AC2 closes. No approval is created in
    that case.

    Returns whether the approval was created and, when it was not, the
    API's coded refusal (#3617) for the factory run's detail, if any.
    """

    handle = routing._reply_handle_for(qevent)
    # Both identities are live in this function and they are not
    # interchangeable: ``thread`` is the BARE adapter conversation id used
    # only for replies; ``thread_key`` is the canonical server identity that
    # owns the workspace, publication lineage, sandbox, and card slot.
    thread = qevent.conversation_id
    thread_key = routing._thread_key_for(qevent)
    summary = outcome.approval_summary or outcome.text or "Approval requested"
    display_summary = (
        outcome.approval_display
        if outcome.approval_display
        else approval_display(
            summary, outcome.approval_granted_tool, outcome.approval_granted_arguments
        )
    )

    # Resolve the manifest route NAME (#247) to its workspace channel. A named
    # route that resolves to no binding escalates instead of widening (#544).
    # Named ``route_name``, not ``route``: this is the approval manifest's
    # route identifier, a different concept from the turn's ``TargetRoute``
    # above, and one word for the two is what forced the old ``route_``.
    route_name = outcome.approval_route
    is_publication = publication._is_publish_provenance(
        outcome.approval_gate_kind,
        outcome.approval_granted_tool,
    )
    # The card's destination is a (kind, address) PAIR, never an address on
    # its own: the schema permits the same address string under two kinds,
    # so an address-only comparison misreads an email turn whose address
    # happens to equal a Slack policy channel as "the requesting channel".
    card_kind = handle.kind
    card_channel = handle.channel
    notification_target: tuple[str, str, TargetRoute] | None = None
    # The listed approver addresses an email card names (ADR-0177 amendment
    # A5), so the mail adapter can tell the requester who can approve. Set
    # only on the email branch below, where the list was just required.
    card_approver_emails: list[str] = []
    if route_name:
        binding = (approval_routes or {}).get(route_name)
        targets = _parse_approval_targets(binding)
        if targets is None:
            logger.warning(
                "approval route %r is not bound or is malformed for agent %s; escalating "
                "rather than routing the card to the requesting channel",
                route_name,
                agent_id,
            )
            await self._escalate(
                qevent,
                route,
                f"The run requested approval via route {route_name!r}, but that "
                "route is not bound to a valid resolution target for this "
                "agent; flagging for a human instead of widening the request "
                "to this channel.",
                failure_class="approval-route-unbound",
            )
            return _ApprovalPause(created=False)
        fixed_target, notification_target = targets
        if fixed_target is not None:
            card_kind, card_channel = fixed_target
        elif handle.kind in constants._APPROVER_EMAIL_KINDS:
            card_approver_emails = _approver_emails(binding)
            if not card_approver_emails:
                await self._escalate_unanswerable_email_approval(
                    qevent, route, agent_id, route_name
                )
                return _ApprovalPause(created=False)
        elif (
            handle.kind != SLACK_KIND
            and isinstance(binding, dict)
            and binding.get("approvers") is not None
        ):
            # ADR-0177 decision 3: approver lists hold Slack users, and
            # nobody on a non-Slack conversation can prove to be one yet.
            # Creating the approval would leave it for nobody to answer.
            logger.warning(
                "approval route %r lists approvers but its card would land on a "
                "%s conversation for agent %s; escalating",
                route_name,
                handle.kind,
                agent_id,
            )
            await self._escalate(
                qevent,
                route,
                f"The run requested approval via route {route_name!r}, which "
                f"lists approvers, but its card would be shown here, on {handle.kind}, "
                "where approvers cannot be verified yet; flagging for a human "
                "instead of creating an approval nobody here can answer.",
                failure_class="approval-approvers-unverifiable",
            )
            return _ApprovalPause(created=False)
    elif handle.kind in constants._APPROVER_EMAIL_KINDS:
        # ADR-0177 amendment A3: a routeless approval has no binding, so no
        # approver emails, and on email nobody else may answer it.
        await self._escalate_unanswerable_email_approval(qevent, route, agent_id, None)
        return _ApprovalPause(created=False)

    if not is_publication and self._approvals is None:
        await self._escalate(
            qevent,
            route,
            "The run requested an approval, but no approval backend is "
            "configured on this worker; flagging for a human instead of pausing.",
            failure_class="approval-backend-missing",
        )
        return _ApprovalPause(created=False)

    if is_publication and self._publication_creator is None:
        await self._escalate(
            qevent,
            route,
            "Repository publication is unavailable on this installation; nothing was "
            "published and no approval was created.",
            failure_class="publication-unavailable",
        )
        return _ApprovalPause(created=False)

    base = outcome.text.strip()
    # #2659: the inferred repository is a reply block only. It is composed
    # here, beside the text, and never into ``summary`` or ``outcome.text``,
    # so the durable record, the card and the publication request stay free
    # of it.
    inference = workspace._workspace_inference_notice(outcome.workspace_inferred_repo)
    parsed_publication = parse_work_item_event_id(qevent.event_id)
    publication_run = (
        self._work_item_runs.get(parsed_publication.request_id)
        if parsed_publication is not None and parsed_publication.kind in {"execute", "ci"}
        else None
    )
    try:
        if is_publication:
            publication_creator = self._publication_creator
            assert publication_creator is not None
            snapshot = outcome.publication_snapshot
            if outcome.publication_snapshot_error is not None:
                # #4121: carry the message as the refusal so the factory run
                # shows why the snapshot failed, not only the cause.
                snapshot_failure = outcome.publication_snapshot_error
                raise ApprovalBackendError(snapshot_failure, refusal=snapshot_failure)
            if deployment_id is None or snapshot is None:
                raise ApprovalBackendError(
                    "publication requires a deployment-managed repository workspace"
                )
            observation = constants._PUBLICATION_CONTEXT.get() if not snapshot.patch else None
            if not snapshot.patch and (
                observation is None
                or observation.expected_head != snapshot.base_sha
                or publication_run is None
                or observation.execution_request_id != publication_run.request_id
                or observation.runtime_epoch != publication_run.runtime_epoch
            ):
                raise ApprovalBackendError(
                    "metadata-only publication requires current factory observation"
                )
            summary = publication._publication_approval_summary(snapshot)
            display_summary = summary
            published = await publication_creator.create_publication(
                PublicationCreateRequest(
                    deployment_id=deployment_id,
                    conversation_id=thread_key,
                    reply_conversation_id=thread,
                    repo_full_name=snapshot.repo_full_name,
                    author=qevent.author,
                    summary=summary,
                    reply_kind=handle.kind,
                    reply_channel=handle.channel,
                    reply_placeholder=self._target_for(qevent).reply_ref,
                    reply_endpoint=handle.endpoint,
                    reply_adapter=handle.adapter,
                    dedupe_key=qevent.event_id,
                    base_sha=snapshot.base_sha,
                    patch=snapshot.patch,
                    changed_paths=snapshot.changed_paths,
                    expires_in_seconds=constants._PUBLICATION_EXPIRES_IN_SECONDS,
                    title=snapshot.publication_title,
                    body=snapshot.publication_body,
                    max_patch_bytes=self._config.publication_patch_max_bytes,
                    review_origin_key=outcome.review_origin_key,
                    route=route_name,
                    work_item_request_id=(
                        parsed_publication.request_id
                        if parsed_publication is not None
                        and parsed_publication.kind in {"execute", "ci"}
                        and publication_run is not None
                        and publication_run.started
                        else None
                    ),
                    work_item_runtime_epoch=(
                        publication_run.runtime_epoch
                        if publication_run is not None and publication_run.started
                        else None
                    ),
                    observed_title=(
                        observation.observed_title if observation is not None else None
                    ),
                    observed_body_sha256=(
                        observation.observed_body_sha256 if observation is not None else None
                    ),
                    observed_lineage_id=(
                        observation.lineage_id if observation is not None else None
                    ),
                    observed_lineage_version=(
                        observation.lineage_version if observation is not None else None
                    ),
                ),
                budget_s=routing._api_write_budget_s(),
            )
            created = CreatedApproval(
                id=published.approval_id,
                status=published.status,
            )
        else:
            assert self._approvals is not None
            created = await self._approvals.create(
                ApprovalRequest(
                    agent_id=agent_id,
                    # BARE, with the pair below it: the API replays this value as
                    # the resume turn's conversation_id and the adapter threads
                    # the reply on it. The worker re-derives its scoped key from
                    # the same triple when that turn arrives.
                    conversation_id=thread,
                    author=qevent.author,
                    summary=summary,
                    # The durable twin of this turn's routing pair and egress
                    # selector (ADR-0096 phase 2). Copied off THIS turn at
                    # creation time, never looked up at resume: an operator may
                    # re-bind the address between suspension and resume, and the
                    # persisted values are facts about the original turn.
                    reply_kind=handle.kind,
                    reply_channel=handle.channel,
                    # The ref this turn actually DELIVERED on, not the one the wire
                    # carried. On a placeholder-less turn (ADR-0079) the two differ:
                    # the wire says null and the turn has since posted its own
                    # message. Persisting the null would leave the resume with
                    # nothing to edit, so the approval's outcome would land on a
                    # second message beside the request it answers.
                    reply_placeholder=self._target_for(qevent).reply_ref,
                    reply_endpoint=handle.endpoint,
                    reply_adapter=handle.adapter,
                    dedupe_key=qevent.event_id,
                    route=route_name,
                    card_channel=card_channel,
                    # The ACI ``final`` frame types this as a bare ``str``, so an
                    # unrecognized value only fails when the shared model
                    # validates it (#492/#544: it is authority-bearing, so it is
                    # rejected, never degraded to None). The cast defers to that
                    # validation; ValidationError below is the rejection path.
                    gate_kind=cast("GateKind | None", outcome.approval_gate_kind),
                    granted_tool=outcome.approval_granted_tool,
                    granted_arguments=outcome.approval_granted_arguments,
                    expires_in_seconds=constants._SESSION_APPROVAL_EXPIRES_IN_SECONDS,
                ),
                budget_s=routing._api_write_budget_s(),
            )
    except WorkspaceSelectionRefused as exc:
        logger.info(
            "publication refused for agent=%s deployment=%s thread=%s: %s",
            agent_id,
            deployment_id,
            thread_key,
            exc.public_detail,
        )
        await self._reply_for(qevent, route, exc.public_detail)
        return _ApprovalPause.refused(exc.public_detail)
    except ApprovalRefused as exc:
        # #2885: a person rejected this approval in this thread and nobody
        # has asked since. The API refused it and audited the refusal; the
        # turn reports that instead of pausing, escalating, or retrying.
        logger.info(
            "approval re-raise refused for agent=%s thread=%s event=%s",
            agent_id,
            thread_key,
            qevent.event_id,
        )
        await self._reply_for(qevent, route, exc.public_detail)
        return _ApprovalPause.refused(exc.public_detail)
    except (ApprovalBackendError, ValidationError) as exc:
        # ValidationError: the shared model rejected the payload at
        # construction (#492) -- an unknown gate_kind, or an empty
        # conversation_id/author/dedupe_key, which the wire's QueuedTurn does
        # not constrain. The API rejected these with a 422 before the model
        # was shared, which surfaced here as ApprovalBackendError; both still
        # escalate to a human rather than stranding the turn.
        refusal = exc.refusal if isinstance(exc, ApprovalBackendError) else None
        if refusal is not None and refusal.startswith("publication.work_item_cancelled:"):
            logger.info("approval create refused for cancelled work item %s", qevent.event_id)
            return _ApprovalPause.refused(refusal)
        logger.warning("approval create failed for %s: %s", qevent.event_id, exc)
        await self._escalate(
            qevent,
            route,
            "The run requested an approval, but the approval record could "
            "not be created; flagging for a human instead of pausing.",
            failure_class="approval-create-failed",
        )
        return _ApprovalPause.refused(refusal)

    progress_plan = constants._TURN_PROGRESS.get()
    if progress_plan is not None and self._progress is not None:
        # The resume continues this chain and its milestone budget (ADR 0130).
        await link_progress_resume(self._progress, progress_plan, created.id)

    if self._workspace is not None:
        async with self._lock.hold(self._config.lock_key(thread_key)):
            retain_workspace = not is_publication
            if is_publication:
                try:
                    await asyncio.to_thread(self._workspace.release, thread_key)
                except Exception as exc:  # noqa: BLE001 - patch is already durable
                    retain_workspace = True
                    logger.warning(
                        "publication base-object cleanup failed for thread %s: %s",
                        thread_key,
                        exc,
                    )
            if retain_workspace:
                await asyncio.to_thread(
                    self._workspace.touch,
                    thread_key,
                    ttl_seconds=self._suspended_route_ttl_seconds,
                )
    log.record_metric(
        "curie.approval.lifecycle",
        attributes={
            "service.name": "curie-worker",
            "operation": "request",
            "outcome": "requested",
        },
    )

    suspend_error: SandboxError | None = None
    with log.operation_span(
        "curie.approval.suspend",
        kind=SpanKind.INTERNAL,
        attributes={
            "service.name": "curie-worker",
            "operation": "suspend",
        },
    ) as approval_span:
        try:
            await asyncio.to_thread(self._substrate.suspend, thread_key, history_ref=None)
        except SandboxError as exc:
            suspend_error = exc
            if hasattr(approval_span, "set_status"):
                approval_span.set_status(StatusCode.ERROR)
            approval_span.add_event(
                "approval.suspend.failed",
                {"outcome": "failure", "error.class": type(exc).__name__},
            )
        else:
            approval_span.add_event("approval.suspended", {"outcome": "suspended"})
    log.record_metric(
        "curie.approval.lifecycle",
        attributes={
            "service.name": "curie-worker",
            "operation": "suspend",
            "outcome": "failure" if suspend_error is not None else "suspended",
        },
    )
    if suspend_error is not None:
        # Non-fatal: the record is durable and the resume path cold-claims a
        # fresh sandbox either way; a still-live sandbox is just reaped when
        # its route expires.
        logger.warning(
            "suspend failed for thread %s (%s)",
            thread_key,
            type(suspend_error).__name__,
        )

    # The card's destination -- kind AND route -- is selected from the
    # channel it POSTS TO, never from the turn that requested it. In the
    # requesting channel the card joins the thread and rides the trigger's
    # own transport. A route-bound channel has no such thread and is policy,
    # not a per-turn reply: it posts top-level over the worker's configured
    # Slack transport, because ``ApprovalRouteBinding.resolution`` is
    # Slack-only by construction (``schemas.approvals`` validates the explicit
    # pair), and the authorizer proves membership of that channel through a
    # verified Slack card click. Notification transport never feeds this
    # comparison or these route fields.
    #
    # Keeping the requesting turn's kind and adapter here was a fail-closed
    # bug: an email-originated approval routed to a Slack policy channel kept
    # ``kind=email`` with an email adapter and NO endpoint, so the egress
    # raised and the channel the policy exists to notify never saw the card.
    #
    # The comparison is the full PAIR, because an address is only unique
    # within its kind: two bindings may carry the same address string under
    # different kinds, and comparing addresses alone would hand a non-Slack
    # turn's transport to a Slack policy card that merely shares its address.
    in_requesting_channel = (card_kind, card_channel) == (
        handle.kind,
        handle.channel,
    )
    card_endpoint = handle.endpoint if in_requesting_channel else None
    # A policy route names only a channel. A Slack turn lends its adapter
    # so a named identity posts the card under that same identity. An adapter
    # from another kind cannot carry a Slack policy card.
    card_adapter = (
        None if not in_requesting_channel and handle.kind != SLACK_KIND else route.adapter
    )
    # The relay (#2883) carries the card on the turn's own ref, so no card
    # message follows the notice there, and its reader parses the approval id
    # out of the notice text.
    card_rides_the_turn = card_adapter == CLUSTER_MESSAGE_ADAPTER
    # ADR-0179 decision 2: a card that lands in this thread as a message of its
    # own gets one plain line above it. The line is chosen here, before the card
    # is posted, and never rewritten: a buffering channel (email) replaces its
    # reply text on each update and appends the card, so a rewrite would drop
    # the card from that reply.
    card_in_thread = not is_publication and in_requesting_channel and not card_rides_the_turn

    # The notice is a control string the CLI parses by splitting on blank
    # lines and requiring the marker-leading block (cli/src/chat.rs
    # parse_approval_id, the #766 keep-alive). A model-authored blank line or
    # newline in ``summary`` would break that delimiter and strand the
    # resumed reply (#817), so collapse the interpolated summary to one
    # logical line -- the notice is always a single clean block. The durable
    # ``Approval`` record and the Block Kit card keep the original summary.
    # The inferred repository announcement (#2659) is its own block before
    # the notice and never starts with the marker, so the notice stays the
    # single marker-leading, trailing block the CLI expects.
    notice_summary = " ".join(display_summary.split())
    if card_in_thread:
        notice = constants._IN_THREAD_APPROVAL_NOTICE
    elif is_publication:
        notice = (
            f"Awaiting approval ({created.id}): {notice_summary}\n"
            "The session is paused. The platform will publish or decline the "
            "captured patch and report the result here; the sandbox will not "
            "receive a publication credential."
        )
    else:
        notice = (
            f"Awaiting approval ({created.id}): {notice_summary}\n"
            "The session is paused and will resume once an authorized member "
            "resolves this request."
        )
    # Best-effort (#2721): the row exists and the session is suspended, so a
    # transport failure here must not reclaim the turn into a second model
    # run. Cancellation still propagates.
    persisted_ref = self._target_for(qevent).reply_ref
    try:
        await self._reply_for(qevent, route, delivery._join_reply_blocks(base, inference, notice))
    except Exception as exc:  # noqa: BLE001 - the approval is already durable
        logger.warning("pending notice delivery failed for approval %s: %s", created.id, exc)
    else:
        minted_ref = self._target_for(qevent).reply_ref
        if persisted_ref is None and minted_ref is not None and self._card_store is not None:
            # The row carries no ref, so remember the one the notice minted
            # for the resume turn to adopt (#1640 under #2721 ordering).
            try:
                await self._card_store.remember_notice_ref(created.id, minted_ref)
            except Exception as exc:  # noqa: BLE001 - resume falls back to a new message
                logger.warning(
                    "remembering notice ref failed for approval %s: %s",
                    created.id,
                    exc,
                )

    if is_publication:
        # The atomic Approval+Publication insert is also the durable initial
        # card outbox. The publication reconciler posts and remembers that
        # card independently, so this turn can be acknowledged now and is
        # never reclaimed through the model merely because Slack is down.
        logger.info(
            "thread %s suspended awaiting publication approval %s; card queued",
            thread_key,
            created.id,
        )
        return _ApprovalPause(created=True)

    # Display attribution is distinct from the resume actor and the durable
    # approval author. An older API cannot prove a continuation's origin.
    if created.requester_known:
        requested_by = created.requested_by or ""
    elif self._is_approval_resume(qevent.event_id):
        requested_by = ""
    else:
        requested_by = qevent.author

    # The approval interaction (#246, ADR-0010/0020): a channel-neutral
    # Confirm intent (Approve/Reject) emitted WITHOUT any Block Kit -- the
    # Slack adapter renders it into the approval card's buttons below the
    # seam. The confirm/cancel actions carry the durable record id so a click
    # resolves exactly this approval through the API's server-side authorizer.
    # Building the message is inside the best-effort try (the channel-neutral
    # models validate on construction, unlike the old blocks builder) so the
    # pause -- the durable record and the resume path -- stands with or without
    # the card, exactly as before.
    try:
        card_message = OutboundMessage(
            version=MESSAGE_VERSION,
            text=display_summary,
            # ADR-0177 amendment A5: an email card in the asking thread names
            # who can approve. Display only; the platform decides who may.
            fields=approver_fields(card_approver_emails),
            interaction=ConfirmIntent(
                kind="confirm",
                id=created.id,
                prompt=display_summary,
                confirm=Action(label="Approve", value=created.id),
                cancel=Action(label="Reject", value=created.id),
                # An approval decision may carry a reason (#1053). This says
                # only that, semantically; each adapter decides how to
                # collect it (ADR-0020: the interaction is the port, the
                # widget is the adapter). Slack renders a dialog on the
                # click, the terminal renderer a typed reply. The note is
                # optional in every rendering, and the note the approver
                # leaves already reaches the requester -- the API stores it
                # on the record and build_resume_turn interpolates it into
                # the platform-authored resume text.
                #
                # UNCONDITIONAL, and that is the decision rather than an
                # oversight (#1076). It costs an approver who wants no note
                # one extra click, on every approval, in every deployment,
                # so it was worth stating rather than leaving as the only
                # reachable arm of a branch that looked configurable.
                #
                # Rejected: exposing a per-agent or env toggle. A reason is
                # the half of a rejection the requester actually needs, and
                # the dialog is what makes leaving one the default rather
                # than a thing you remember to do from the CLI. A toggle
                # would also keep TWO settled-card render paths alive
                # permanently, which is what #1073 and #1084 exist to
                # collapse into one. And the evidence for whether operators
                # want the opt-out does not exist yet: discussion #1061
                # settles that the approval plane's plumbing gets fixed
                # first and governance knobs are decided from evidence
                # after, which applies to this knob as much as to #1054's.
                #
                # If that evidence arrives, this field is still the one flip
                # -- but the flip needs an operator-written source, never a
                # bundle-declared one (the #520 anti-hollow-out rule: an
                # agent must not widen how its own approvals are collected).
                allow_free_text=True,
            ),
        )
        # The card's own target: a policy-routed card belongs to no
        # conversation (it posts top-level in a channel that never asked),
        # and it mints its own ref, so it carries neither. The one exception
        # is the cluster-message relay (#2883): it has no card message to
        # mint and addresses the caller's session bucket by the turn's ref,
        # exactly as the publication card outbox does (#2757).
        card_reply_ref = self._target_for(qevent).reply_ref if card_rides_the_turn else None
        card_ack = await self._sink.emit(
            ReplyPost(
                version=REPLY_WIRE_VERSION,
                event="reply.post",
                target=ReplyTarget(
                    kind=card_kind,
                    address=card_channel,
                    conversation_id=thread if in_requesting_channel else None,
                    reply_ref=card_reply_ref,
                ),
                message=card_message,
                requested_by=requested_by,
            ),
            route=TargetRoute(endpoint=card_endpoint, adapter=card_adapter),
        )
        card_ts = card_ack.ref
    except Exception as exc:  # noqa: BLE001 - the pause stands without the card
        logger.warning("approval card post failed for %s: %s", created.id, exc)
    else:
        # Remember where the card landed so an EXPIRY -- which, unlike a
        # resolve, carries no click to locate the card -- can disable it
        # later (#419). Best-effort: a lost memory only means the card is not
        # auto-disabled, and the resolve-click path still heals it.
        if card_ts and self._card_store is not None:
            registered = False
            try:
                await self._card_store.remember(
                    str(created.id),
                    channel=card_channel,
                    ts=card_ts,
                    summary=display_summary,
                    endpoint=card_endpoint,
                    # The whole destination, not just the endpoint: the
                    # settle path posts to THIS card, so it must re-use the
                    # kind and adapter the card was posted through rather
                    # than rebuilding them from the resume turn (which, for
                    # a policy-routed card, is a different channel entirely).
                    kind=card_kind,
                    adapter=card_adapter,
                    # The settled rebuild shows the same "Requested by" line
                    # the live card did, and once the sandbox is gone this
                    # is the worker's only copy of it (#1084).
                    requested_by=requested_by,
                )
                registered = True
            except Exception as exc:  # noqa: BLE001 - best-effort memory
                logger.warning("remembering approval card for %s failed: %s", created.id, exc)
            if registered:
                await self._settle_if_decided_before_registration(str(created.id), qevent)
        # ADR-0179 decision 3: the card is now a message of its own below the
        # notice, so the resume answers below it. Remembered apart from the
        # card ref because settling consumes that ref, and a redelivered
        # resume must choose the same place. A lost memory only means the
        # answer edits the notice, as it did before.
        if card_in_thread and card_ts and self._card_store is not None:
            try:
                await self._card_store.remember_reply_below_card(str(created.id))
            except Exception as exc:  # noqa: BLE001 - best-effort memory
                logger.warning("remembering the reply placement for %s failed: %s", created.id, exc)
    # Visibility is independent of card delivery. This is a second post, not
    # a second card: there is deliberately no ConfirmIntent, action value, or
    # remembered card ref. A failed notification cannot invalidate or move
    # the durable approval, and a failed resolution-card transport must not
    # suppress the notification attempt.
    if notification_target is not None:
        notification_kind, notification_address, notification_route = notification_target
        try:
            await self._sink.emit(
                ReplyPost(
                    version=REPLY_WIRE_VERSION,
                    event="reply.post",
                    target=ReplyTarget(
                        kind=notification_kind,
                        address=notification_address,
                        conversation_id=None,
                        reply_ref=None,
                    ),
                    message=OutboundMessage(
                        version=MESSAGE_VERSION,
                        text=(
                            f"Approval {created.id} requires review: {notice_summary}. "
                            "Resolve in the configured approval channel."
                        ),
                        interaction=None,
                    ),
                    requested_by=requested_by,
                ),
                route=notification_route,
            )
        except Exception as exc:  # noqa: BLE001 - the durable pause stands
            logger.warning("approval notification post failed for %s: %s", created.id, exc)
    logger.info("thread %s suspended awaiting approval %s", thread_key, created.id)
    return _ApprovalPause(created=True)


async def _escalate_unanswerable_email_approval(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    agent_id: uuid.UUID | None,
    route_name: str | None,
) -> None:
    """Escalate an approval raised in an email thread that nobody may answer.

    ADR-0177 amendment A3: on email, only an address on the route's approver
    list may answer, and there is no requester-only default. Creating the
    approval would leave a card that can only expire, so the turn is
    flagged for a human with the reason, as ADR-0177 decision 3 already does
    for approvers that cannot be verified on the asking channel.

    Args:
        qevent: the turn that raised the approval.
        route: the turn's reply route.
        agent_id: the agent, for the log line.
        route_name: the route the approval named, or None when routeless.
    """

    logger.warning(
        "approval route %r has no approver emails but its card would land on an "
        "email conversation for agent %s; escalating",
        route_name,
        agent_id,
    )
    if route_name is None:
        why = "The run requested an approval without naming a route"
    else:
        why = (
            f"The run requested approval via route {route_name!r}, which lists no "
            "approver email addresses"
        )
    await self._escalate(
        qevent,
        route,
        f"{why}, and on email only an address on a route's approver list may "
        "answer; flagging for a human instead of creating an approval nobody "
        "here can answer.",
        failure_class="approval-no-email-approvers",
    )
