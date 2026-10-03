"""A reusable conformance kit for channel adapters.

A channel adapter is conformant if it does what the conformance floor of
``docs/guides/building-a-channel-adapter.md`` (sections 5 and 7) says: it
normalizes an upstream message into a ``POST /channels/turns`` body with a
``delivery_id`` stable across retries, serves the attachments it declares,
serves the four reply-wire events the way its declared streaming mode promises,
dedupes ``turn.completed`` durably and never repeats an ambiguous send, keeps
progress from ever changing the answer, refuses an unauthenticated or foreign
event before any side effect, and reports a failed provider call as a loud
delivery failure so the worker retries it.

The kit is plain data and plain async functions, with no test runner in it, so
an adapter author outside this repository can import it from the runtime
package and drive it from pytest, a CLI or CI however they like. To register an
adapter, wrap it in an object satisfying ``ChannelAdapterSubject``: declare its
``Capabilities``, make an upstream message arrive, deliver reply events through
the same egress path the platform uses, and report the provider-visible
``Effect`` list. Then run every check ``applicable_checks`` returns for those
capabilities:

    for check in applicable_checks(subject.capabilities):
        await check.run(subject, CheckContext(validate_turn=my_validator))

A check that finds a violation raises ``ConformanceFailure`` naming the check,
what it expected and what it observed (the effects recorded since the check
started), so a CI log alone is enough to diagnose it. A check never skips: one
that does not apply to a declaration is simply not returned for it.

Every check opens its own conversation under a fresh upstream id, so checks are
independent of each other and of their order.
"""

import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Protocol

from pydantic import TypeAdapter, ValidationError

from .models import MESSAGE_VERSION, Action, ConfirmIntent, OutboundMessage
from .progress import ProgressCard, ProgressState, progress_text
from .reply import (
    PROGRESS_REPLY_WIRE_VERSION,
    REPLY_WIRE_VERSION,
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyTarget,
    ReplyUpdate,
    SettledOutcome,
    TurnCompleted,
    TurnStatus,
)

__all__ = [
    "CHECKS",
    "Capabilities",
    "ChannelAdapterSubject",
    "Check",
    "CheckArea",
    "CheckContext",
    "ConformanceFailure",
    "Effect",
    "EffectOp",
    "IngressTurn",
    "Streaming",
    "Upstream",
    "UpstreamAttachment",
    "applicable_checks",
]


class Streaming(StrEnum):
    """How an adapter makes a turn's reply visible."""

    EDIT = "edit"
    """``reply.update`` edits the visible reply at ``target.reply_ref``."""
    BUFFERED = "buffered"
    """Answer text is held and sent once, on ``turn.completed``."""
    SILENT = "silent"
    """Every event is acked and nothing is made visible."""
    RELAY = "relay"
    """Every event is forwarded verbatim to a far end."""


@dataclass(frozen=True)
class Capabilities:
    """What an adapter declares about itself; it decides which checks apply."""

    kind: str
    streaming: Streaming
    ingress: bool
    """The adapter normalizes upstream messages into channel turns."""
    authenticates: bool
    """Egress verifies a per-adapter credential."""
    refuses_foreign_targets: bool
    """An event addressed to another channel kind is refused."""
    serves_attachments: bool
    """The adapter answers ``GET {endpoint}/attachments/{id}`` (ADR-0153)."""
    settles_approval_cards: bool
    """An approval card post acks a ref, and a settled update at it shows the decision."""


EffectOp = Literal["post", "edit", "delete", "send", "forward"]


@dataclass(frozen=True)
class Effect:
    """One provider-visible side effect, in the order it happened."""

    op: EffectOp
    ref: str | None
    text: str


@dataclass(frozen=True)
class UpstreamAttachment:
    """A file arriving with an upstream message."""

    name: str
    content: bytes


@dataclass(frozen=True)
class Upstream:
    """A message arriving from the channel's own users."""

    id: str
    text: str
    attachments: tuple[UpstreamAttachment, ...] = ()


@dataclass(frozen=True)
class IngressTurn:
    """One turn an adapter sent to the platform for an upstream message.

    ``body`` is the exact ``POST /channels/turns`` body, or None when the
    adapter's ingress is not that route.
    """

    kind: str
    address: str
    delivery_id: str
    conversation_id: str
    reply_ref: str | None
    attachments: tuple[Mapping[str, Any], ...] = ()
    body: Mapping[str, Any] | None = None


class ChannelAdapterSubject(Protocol):
    """An adapter wired for the kit, with only its external seams faked."""

    capabilities: Capabilities

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        """Make ``message`` arrive; return every turn the adapter sent for it, in order.

        A subject whose adapter retries ingress makes the platform refuse the
        first attempt transiently so the retry is included. A subject without
        ingress returns one stand-in turn addressing the far end.
        """
        ...

    async def emit(self, event: ReplyEvent, *, best_effort: bool = False) -> ReplyAck:
        """Deliver ``event`` the way the platform does; raise on a delivery failure.

        ``best_effort`` is the worker's best-effort turn (#708): an UNREACHABLE
        adapter is acked instead of raising, while a rejection still raises.
        """
        ...

    async def emit_unauthenticated(self, event: ReplyEvent) -> ReplyAck:
        """Deliver ``event`` the same way with a wrong egress credential."""
        ...

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        """``GET {endpoint}/attachments/{id}`` with the egress secret.

        Only called for an adapter declaring ``serves_attachments``.
        """
        ...

    async def restart(self) -> None:
        """Reopen the adapter on the same durable state; a no-op when it keeps none."""
        ...

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        """Make the provider (or far end) fail the next side effect, once.

        ``accepted`` makes it a lost acknowledgement instead: the provider
        performs the side effect and the adapter observes a failure.
        """
        ...

    def effects(self) -> Sequence[Effect]:
        """The provider-visible effects so far, in order, ingress placeholders excluded."""
        ...


class ConformanceFailure(AssertionError):
    """An adapter violated a check; the message names the check and the evidence."""


@dataclass(frozen=True)
class CheckContext:
    """What a caller supplies to the checks.

    ``validate_turn`` raises on a ``POST /channels/turns`` body the platform
    would refuse. None skips that validation (the kit itself is app-free, so the
    platform's own validator is the caller's to pass).
    """

    validate_turn: Callable[[Mapping[str, Any]], None] | None = None


CheckArea = Literal[
    "ingress", "delivery", "streaming", "attachments", "approvals", "progress", "errors"
]


@dataclass(frozen=True)
class Check:
    """One named conformance check and the declarations it applies to."""

    name: str
    area: CheckArea
    applies: Callable[[Capabilities], bool]
    run: Callable[[ChannelAdapterSubject, CheckContext], Awaitable[None]]


# --- helpers ------------------------------------------------------------------

_EVENT: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)
_REQUESTER = "conformance-requester"
_RESOLVER = "conformance-resolver"
_DECISION = "approve"
# A redelivered completion is retried this many times after an ambiguous attempt
# before the adapter is judged never to ack it.
_AMBIGUOUS_RETRIES = 3


def _token() -> str:
    return uuid.uuid4().hex[:12]


def _require(condition: bool, check: str, expected: str, observed: object) -> None:
    if not condition:
        raise ConformanceFailure(f"{check}: expected {expected}; observed {observed!r}")


def _since(subject: ChannelAdapterSubject, mark: int) -> list[Effect]:
    return list(subject.effects())[mark:]


async def _open(
    subject: ChannelAdapterSubject,
    check: str,
    attachments: tuple[UpstreamAttachment, ...] = (),
) -> tuple[list[IngressTurn], Upstream]:
    message = Upstream(
        id=str(uuid.uuid4()), text=f"conformance message {_token()}", attachments=attachments
    )
    turns = await subject.open_conversation(message)
    _require(bool(turns), check, "at least one ingress turn for an upstream message", turns)
    return turns, message


async def _target(subject: ChannelAdapterSubject, check: str) -> ReplyTarget:
    """A fresh conversation's reply target, built from the last turn ingress sent."""

    turns, _ = await _open(subject, check)
    turn = turns[-1]
    return ReplyTarget(
        kind=turn.kind,
        address=turn.address,
        conversation_id=turn.conversation_id,
        reply_ref=turn.reply_ref,
    )


def _at(target: ReplyTarget, reply_ref: str | None) -> ReplyTarget:
    return target.model_copy(update={"reply_ref": reply_ref})


def _update(target: ReplyTarget, text: str) -> ReplyUpdate:
    return ReplyUpdate(version=REPLY_WIRE_VERSION, event="reply.update", target=target, text=text)


def _completed(target: ReplyTarget, outcome: str = "delivered") -> TurnCompleted:
    return TurnCompleted.model_validate(
        {
            "version": REPLY_WIRE_VERSION,
            "event": "turn.completed",
            "target": target.model_dump(),
            "event_id": f"conformance-{uuid.uuid4()}",
            "outcome": outcome,
        }
    )


def _card(summary: str, revision: int) -> ProgressCard:
    return ProgressCard(
        kind="card",
        state=ProgressState.INVESTIGATING,
        summary=summary,
        revision=revision,
        terminal=False,
    )


def _progress_post(target: ReplyTarget, summary: str) -> ReplyPost:
    card = _card(summary, 1)
    return ReplyPost(
        version=PROGRESS_REPLY_WIRE_VERSION,
        event="reply.post",
        target=target,
        message=OutboundMessage(version=MESSAGE_VERSION, text=progress_text(card)),
        requested_by=_REQUESTER,
        delivery_id=str(uuid.uuid4()),
        progress=card,
    )


def _progress_edit(target: ReplyTarget, summary: str) -> ReplyUpdate:
    return ReplyUpdate(
        version=PROGRESS_REPLY_WIRE_VERSION,
        event="reply.update",
        target=target,
        delivery_id=str(uuid.uuid4()),
        progress=_card(summary, 2),
    )


def _forwards_equal(effects: Sequence[Effect], events: Sequence[ReplyEvent]) -> bool:
    """Each effect is a ``forward`` whose body decodes to the matching event."""

    if len(effects) != len(events):
        return False
    for effect, event in zip(effects, events, strict=True):
        if effect.op != "forward":
            return False
        try:
            decoded = _EVENT.validate_json(effect.text)
        except ValidationError:
            return False
        if decoded.model_dump(mode="json") != event.model_dump(mode="json"):
            return False
    return True


def _edits_at(effects: Sequence[Effect], ref: str | None) -> list[Effect]:
    return [effect for effect in effects if effect.op == "edit" and effect.ref == ref]


def _sends(effects: Sequence[Effect]) -> list[Effect]:
    return [effect for effect in effects if effect.op == "send"]


def _names_the_settlement(text: str) -> bool:
    """The settled card's text names its resolver or decision, case-insensitively."""

    lowered = text.lower()
    return _RESOLVER in lowered or _DECISION in lowered


def _side_effecting(
    subject: ChannelAdapterSubject, target: ReplyTarget, answer: str
) -> tuple[list[ReplyEvent], ReplyEvent]:
    """The prefix and the one event that makes the answer visible.

    EDIT and RELAY: the update itself. BUFFERED: an update (no effect) then the
    completion that sends it.
    """

    update = _update(target, answer)
    if subject.capabilities.streaming is Streaming.BUFFERED:
        return [update], _completed(target)
    return [], update


async def _expect_refused(
    delivery: Awaitable[ReplyAck],
    subject: ChannelAdapterSubject,
    mark: int,
    check: str,
    expected: str,
) -> None:
    """Await ``delivery``, which must raise; an ack fails ``check``.

    The failure names the ack and the effects recorded since ``mark``.
    """

    try:
        ack = await delivery
    except Exception:  # noqa: BLE001 - the expected failure is the pass
        return
    raise ConformanceFailure(
        f"{check}: expected {expected}; observed it acked with {ack!r} and effects "
        f"{_since(subject, mark)!r}"
    )


def _lands_the_answer(
    subject: ChannelAdapterSubject, effect: Effect, event: ReplyEvent, answer: str
) -> bool:
    if subject.capabilities.streaming is Streaming.RELAY:
        return _forwards_equal([effect], [event])
    return answer in effect.text


# --- the checks -----------------------------------------------------------------


async def ingress_normalizes_to_a_channel_turn(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 4 and floor rule 1: an upstream message becomes a channel turn.

    Every turn names the adapter's own kind, a whitespace-free address and a
    non-empty delivery and conversation id; the posted body carries the
    message's text, is one the platform accepts, and says what the turn says.
    """

    name = "ingress_normalizes_to_a_channel_turn"
    turns, message = await _open(subject, name)
    for turn in turns:
        _require(
            turn.kind == subject.capabilities.kind,
            name,
            f"kind {subject.capabilities.kind!r}",
            turn,
        )
        _require(
            bool(turn.address) and not any(ch.isspace() for ch in turn.address),
            name,
            "a non-empty address without whitespace",
            turn,
        )
        _require(bool(turn.delivery_id), name, "a non-empty delivery_id", turn)
        _require(bool(turn.conversation_id), name, "a non-empty conversation_id", turn)
        body = turn.body
        if body is None:
            continue
        text = body.get("text")
        _require(
            isinstance(text, str) and message.text in text,
            name,
            f"the body text to carry the upstream text {message.text!r}",
            body,
        )
        if ctx.validate_turn is not None:
            try:
                ctx.validate_turn(body)
            except Exception as exc:
                raise ConformanceFailure(
                    f"{name}: expected a body POST /channels/turns accepts; observed {body!r} "
                    f"refused with {type(exc).__name__}: {exc}"
                ) from exc
        for field in ("kind", "address", "delivery_id", "conversation_id", "reply_ref"):
            _require(
                body.get(field) == getattr(turn, field),
                name,
                f"body {field} equal to the turn's {getattr(turn, field)!r}",
                body,
            )
        posted = [dict(ref) for ref in body.get("attachments") or ()]
        _require(
            posted == [dict(ref) for ref in turn.attachments],
            name,
            f"body attachments equal to the turn's {turn.attachments!r}",
            body,
        )


async def ingress_delivery_id_is_stable(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide section 4 and floor rules 1 and 2: one upstream message, one delivery_id.

    A retry carries the delivery_id of the attempt it retries (never a freshly
    minted one, which escapes the platform's idempotency receipt), and a
    different upstream message gets a different one.
    """

    del ctx
    name = "ingress_delivery_id_is_stable"
    turns, _ = await _open(subject, name)
    ids = [turn.delivery_id for turn in turns]
    _require(len(set(ids)) == 1, name, "every turn for one upstream to share its delivery_id", ids)
    other, _ = await _open(subject, name)
    other_ids = {turn.delivery_id for turn in other}
    _require(
        ids[0] not in other_ids,
        name,
        "a distinct upstream message to get a different delivery_id",
        {"first": ids, "second": sorted(other_ids)},
    )


async def attachments_match_declared_capability(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """ADR-0153 via guide section 4: attachment refs exactly when the adapter serves them.

    An upstream message carries one file. An adapter not declaring
    ``serves_attachments`` sends no attachment refs (the platform would fetch
    them from an endpoint that does not exist). One that does sends a ref named
    for the file, and fetching that ref returns the file's exact bytes.
    """

    del ctx
    name = "attachments_match_declared_capability"
    upload = UpstreamAttachment(
        name=f"conformance-{_token()}.txt", content=f"conformance bytes {_token()}".encode()
    )
    turns, _ = await _open(subject, name, (upload,))
    for turn in turns:
        refs = list(turn.attachments)
        if turn.body is not None:
            refs.extend(turn.body.get("attachments") or ())
        if not subject.capabilities.serves_attachments:
            _require(not refs, name, "no attachment refs from an adapter serving none", turn)
            continue
        _require(bool(refs), name, f"a ref for the upstream attachment {upload.name!r}", turn)
        for ref in refs:
            _require(
                bool(ref.get("id")) and ref.get("name") == upload.name,
                name,
                f"every attachment ref to carry a non-empty id and the name {upload.name!r}",
                ref,
            )
            served = await subject.fetch_attachment(str(ref["id"]))
            _require(
                served == upload.content,
                name,
                f"GET attachments/{ref['id']} to return the upload's {len(upload.content)} bytes",
                served[:200],
            )


async def turn_status_is_tolerated(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide section 5 (``turn.status``) and floor rule 5: tolerate events it does not use.

    A status caption is acked. A relay forwards it verbatim. Any other mode may
    render the caption, but a liveness caption never creates or removes a
    message: no post, send or delete.
    """

    del ctx
    name = "turn_status_is_tolerated"
    target = await _target(subject, name)
    status = TurnStatus(
        version=REPLY_WIRE_VERSION, event="turn.status", target=target, status="working"
    )
    mark = len(subject.effects())
    await subject.emit(status)
    effects = _since(subject, mark)
    if subject.capabilities.streaming is Streaming.RELAY:
        _require(
            _forwards_equal(effects, [status]),
            name,
            "one forward equal to the turn.status",
            effects,
        )
    else:
        _require(
            not any(e.op in ("post", "send", "delete") for e in effects),
            name,
            "no post, send or delete from a turn.status",
            effects,
        )


@dataclass(frozen=True)
class _StreamedTurn:
    target: ReplyTarget
    partial: str
    final: str
    completed: TurnCompleted
    acks: list[ReplyAck]
    after_partial: list[Effect]
    after_final: list[Effect]
    effects: list[Effect]
    events: list[ReplyEvent]


async def _stream_reply(subject: ChannelAdapterSubject, name: str) -> _StreamedTurn:
    """Section 5's typical turn: two updates, then a delivered completion."""

    target = await _target(subject, name)
    partial, final = f"partial-{_token()}", f"final-{_token()}"
    first, second = _update(target, partial), _update(target, final)
    completed = _completed(target)
    mark = len(subject.effects())
    acks = [await subject.emit(first)]
    after_partial = _since(subject, mark)
    acks.append(await subject.emit(second))
    after_final = _since(subject, mark)
    await subject.emit(completed)
    return _StreamedTurn(
        target=target,
        partial=partial,
        final=final,
        completed=completed,
        acks=acks,
        after_partial=after_partial,
        after_final=after_final,
        effects=_since(subject, mark),
        events=[first, second, completed],
    )


async def reply_streams_per_declared_mode(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 (``reply.update`` and ``turn.completed``): the reply streams as declared.

    EDIT edits the reply at ``reply_ref`` in place as each update arrives, so it
    is visible while streaming, and completion adds nothing; BUFFERED shows
    nothing until completion and then sends the final text once; SILENT shows
    nothing; RELAY forwards each event verbatim.
    """

    del ctx
    name = "reply_streams_per_declared_mode"
    turn = await _stream_reply(subject, name)
    ref = turn.target.reply_ref
    streaming = subject.capabilities.streaming
    if streaming is Streaming.EDIT:
        _require(ref is not None, name, "ingress to hand back a reply_ref to edit", turn.target)
        for seen, text in ((turn.after_partial, turn.partial), (turn.after_final, turn.final)):
            _require(
                bool(seen)
                and all(e.op == "edit" and e.ref == ref for e in seen)
                and seen[-1].text == text,
                name,
                f"only edits at reply_ref {ref!r}, the latest reading {text!r}, "
                "as soon as the update arrives",
                seen,
            )
        _require(
            turn.effects == turn.after_final,
            name,
            "turn.completed to add nothing after the streamed edits",
            turn.effects,
        )
        _require(
            all(ack.ref == ref for ack in turn.acks),
            name,
            f"every update acked with reply_ref {ref!r}",
            turn.acks,
        )
    elif streaming is Streaming.BUFFERED:
        _require(not turn.after_final, name, "nothing visible before turn.completed", turn.effects)
        _require(
            len(turn.effects) == 1
            and turn.effects[0].op == "send"
            and turn.final in turn.effects[0].text
            and turn.partial not in turn.effects[0].text,
            name,
            f"one send containing {turn.final!r} and not {turn.partial!r}",
            turn.effects,
        )
    elif streaming is Streaming.SILENT:
        _require(not turn.effects, name, "no effect", turn.effects)
    else:
        _require(
            _forwards_equal(turn.effects, turn.events),
            name,
            "three forwards equal to the events",
            turn.effects,
        )


async def completion_is_deduplicated(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide section 5 (at-least-once delivery) and floor rule 6: dedupe ``event_id`` durably.

    A redelivered ``turn.completed`` for a conversation already finished is
    acked and makes nothing visible again, and that still holds after the
    adapter restarts on its durable state.
    """

    del ctx
    name = "completion_is_deduplicated"
    completed = (await _stream_reply(subject, name)).completed
    mark = len(subject.effects())
    await subject.emit(completed)
    effects = _since(subject, mark)
    _require(not effects, name, "no effect from a duplicate turn.completed", effects)
    await subject.restart()
    await subject.emit(completed)
    effects = _since(subject, mark)
    _require(
        not effects,
        name,
        "no effect from a turn.completed replayed after a restart",
        effects,
    )


async def dropped_completion_without_answer_is_silent(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 (``turn.completed``): a dropped turn with no answer owes no message.

    A relay still forwards the event; any other mode makes nothing visible.
    """

    del ctx
    name = "dropped_completion_without_answer_is_silent"
    target = await _target(subject, name)
    dropped = _completed(target, "dropped")
    mark = len(subject.effects())
    await subject.emit(dropped)
    effects = _since(subject, mark)
    if subject.capabilities.streaming is Streaming.RELAY:
        _require(
            _forwards_equal(effects, [dropped]), name, "one forward equal to the event", effects
        )
    else:
        _require(not effects, name, "no effect from a dropped completion with no answer", effects)


async def approval_card_is_delivered(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide section 5 (``reply.post`` and settled ``reply.update``): the approval card works.

    EDIT posts the card; BUFFERED sends it with the awaiting-approval
    completion; SILENT shows nothing; RELAY forwards it with its interaction.
    An adapter declaring ``settles_approval_cards`` acks the card with a ref,
    and a settled update at that ref makes the decision visible: an edit of the
    card naming the resolver or decision (EDIT), or exactly one new send
    (BUFFERED).
    """

    del ctx
    name = "approval_card_is_delivered"
    target = await _target(subject, name)
    card_text = f"Allow conformance action {_token()}?"
    message = OutboundMessage(
        version=MESSAGE_VERSION,
        text=card_text,
        interaction=ConfirmIntent(
            kind="confirm",
            id=f"approval-{uuid.uuid4().hex}",
            prompt=card_text,
            confirm=Action(label="Approve", value="approve"),
            cancel=Action(label="Reject", value="reject"),
        ),
    )
    post = ReplyPost(
        version=REPLY_WIRE_VERSION,
        event="reply.post",
        target=target,
        message=message,
        requested_by=_REQUESTER,
    )
    mark = len(subject.effects())
    ack = await subject.emit(post)
    effects = _since(subject, mark)
    streaming = subject.capabilities.streaming
    settles = subject.capabilities.settles_approval_cards
    if settles:
        _require(ack.ref is not None, name, "the card acked with a ref to settle it by", ack)
    if streaming is Streaming.EDIT:
        _require(
            len(effects) == 1 and effects[0].op == "post" and card_text in effects[0].text,
            name,
            f"one post containing {card_text!r}",
            effects,
        )
        if settles:
            _require(
                ack.ref == effects[0].ref,
                name,
                f"the card acked with its post's ref {effects[0].ref!r}",
                ack,
            )
    elif streaming is Streaming.BUFFERED:
        _require(not effects, name, "nothing visible before turn.completed", effects)
        await subject.emit(_completed(target, "awaiting-approval"))
        effects = _since(subject, mark)
        _require(
            len(effects) == 1 and effects[0].op == "send" and card_text in effects[0].text,
            name,
            f"the awaiting-approval completion to send one message containing {card_text!r}",
            effects,
        )
    elif streaming is Streaming.SILENT:
        _require(not effects, name, "no effect", effects)
    else:
        _require(_forwards_equal(effects, [post]), name, "one forward equal to the event", effects)
    if not settles or streaming not in (Streaming.EDIT, Streaming.BUFFERED):
        return
    settled = ReplyUpdate(
        version=REPLY_WIRE_VERSION,
        event="reply.update",
        target=_at(target, ack.ref),
        message=message,
        settled=SettledOutcome(requested_by=_REQUESTER, decision=_DECISION, resolver=_RESOLVER),
    )
    mark = len(subject.effects())
    await subject.emit(settled)
    effects = _since(subject, mark)
    if streaming is Streaming.EDIT:
        _require(
            len(effects) == 1
            and effects[0].op == "edit"
            and effects[0].ref == ack.ref
            and _names_the_settlement(effects[0].text),
            name,
            f"one edit of the card at {ack.ref!r} naming the resolver or the decision",
            effects,
        )
    else:
        _require(
            len(effects) == 1
            and effects[0].op == "send"
            and _names_the_settlement(effects[0].text),
            name,
            f"the settled card at {ack.ref!r} to send exactly one new message naming the "
            "resolver or the decision",
            effects,
        )


async def progress_never_changes_the_answer(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 (progress) and floor rule 8: a progress body never changes answer text.

    After the answer, a progress card is posted and then edited (revision 2).
    EDIT leaves the answer as the reply's last edit and puts no progress at the
    reply; BUFFERED sends the answer without either progress summary; SILENT
    shows nothing; RELAY forwards each event verbatim.
    """

    del ctx
    name = "progress_never_changes_the_answer"
    target = await _target(subject, name)
    answer_text = f"answer-{_token()}"
    summaries = (f"conformance progress {_token()}", f"conformance progress {_token()}")
    answer = _update(target, answer_text)
    progress = _progress_post(target, summaries[0])
    completed = _completed(target)
    mark = len(subject.effects())
    await subject.emit(answer)
    post_ack = await subject.emit(progress)
    card_edit = _progress_edit(_at(target, post_ack.ref or target.reply_ref), summaries[1])
    await subject.emit(card_edit)
    await subject.emit(completed)
    effects = _since(subject, mark)
    streaming = subject.capabilities.streaming
    if streaming is Streaming.EDIT:
        edits = _edits_at(effects, target.reply_ref)
        _require(
            bool(edits) and edits[-1].text == answer_text,
            name,
            f"the last edit at reply_ref {target.reply_ref!r} to read {answer_text!r}",
            effects,
        )
        _require(
            not any(
                e.ref == target.reply_ref and any(s in e.text for s in summaries) for e in effects
            ),
            name,
            f"no progress effect at reply_ref {target.reply_ref!r}",
            effects,
        )
    elif streaming is Streaming.BUFFERED:
        sends = _sends(effects)
        _require(
            len(sends) == 1
            and answer_text in sends[0].text
            and not any(s in sends[0].text for s in summaries),
            name,
            f"one send containing {answer_text!r} and neither progress summary {summaries!r}",
            effects,
        )
    elif streaming is Streaming.SILENT:
        _require(not effects, name, "no effect", effects)
    else:
        _require(
            _forwards_equal(effects, [answer, progress, card_edit, completed]),
            name,
            "four forwards equal to the events",
            effects,
        )


async def progress_post_is_idempotent_on_delivery_id(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 (progress, post idempotently) and floor rule 8: key a post on delivery_id.

    A repeated 1.1 post is answered with the first attempt's ref and posts
    nothing new.
    """

    del ctx
    name = "progress_post_is_idempotent_on_delivery_id"
    target = await _target(subject, name)
    post = _progress_post(target, f"conformance progress {_token()}")
    first = await subject.emit(post)
    mark = len(subject.effects())
    second = await subject.emit(post)
    effects = _since(subject, mark)
    _require(
        second.ref == first.ref,
        name,
        f"the repeat acked with the first ref {first.ref!r}",
        second,
    )
    _require(not effects, name, "no effect from a repeated delivery_id", effects)


async def unauthenticated_egress_takes_no_effect(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 and floor rule 3: verify the secret before any side effect.

    The event that would make the answer visible, sent with a wrong credential,
    is refused and changes nothing.
    """

    del ctx
    name = "unauthenticated_egress_takes_no_effect"
    target = await _target(subject, name)
    prefix, event = _side_effecting(subject, target, f"answer-{_token()}")
    for earlier in prefix:
        await subject.emit(earlier)
    mark = len(subject.effects())
    await _expect_refused(
        subject.emit_unauthenticated(event),
        subject,
        mark,
        name,
        f"an unauthenticated {event.event} to be refused",
    )
    effects = _since(subject, mark)
    _require(not effects, name, "no effect from an unauthenticated event", effects)


async def provider_failure_surfaces_as_delivery_failure(
    subject: ChannelAdapterSubject, ctx: CheckContext
) -> None:
    """Guide section 5 (any status at or above 400 is a delivery failure) and floor rule 4.

    Every emit here is best-effort. When the provider fails the side effect the
    delivery still fails loudly (a rejection is never an unreachable host, which
    a best-effort turn would ack, #708), and the identical retry acks with the
    effect landed exactly once.
    """

    del ctx
    name = "provider_failure_surfaces_as_delivery_failure"
    target = await _target(subject, name)
    answer = f"answer-{_token()}"
    prefix, event = _side_effecting(subject, target, answer)
    for earlier in prefix:
        await subject.emit(earlier, best_effort=True)
    mark = len(subject.effects())
    subject.fail_next_delivery()
    await _expect_refused(
        subject.emit(event, best_effort=True),
        subject,
        mark,
        name,
        f"a best-effort {event.event} whose provider call failed to fail delivery loudly",
    )
    try:
        await subject.emit(event, best_effort=True)
    except Exception as exc:
        raise ConformanceFailure(
            f"{name}: expected the identical retry to succeed; observed {type(exc).__name__}: "
            f"{exc} with effects {_since(subject, mark)!r}"
        ) from exc
    effects = _since(subject, mark)
    _require(
        len(effects) == 1 and _lands_the_answer(subject, effects[0], event, answer),
        name,
        f"the retry to land the answer {answer!r} exactly once",
        effects,
    )


async def foreign_target_is_refused(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide section 5 (``target.kind``): an event for another channel kind is refused.

    The kind is never ``github`` or ``slack``, which the platform routes to
    other sinks, so the event reaches this adapter and must be refused there.
    """

    del ctx
    name = "foreign_target_is_refused"
    target = await _target(subject, name)
    foreign = target.model_copy(update={"kind": f"foreign-to-{subject.capabilities.kind}"})
    event = _update(foreign, f"answer-{_token()}")
    mark = len(subject.effects())
    await _expect_refused(
        subject.emit(event),
        subject,
        mark,
        name,
        f"an update for kind {foreign.kind!r} to be refused",
    )
    effects = _since(subject, mark)
    _require(not effects, name, "no effect from a foreign-kind event", effects)


async def ambiguous_send_is_not_repeated(subject: ChannelAdapterSubject, ctx: CheckContext) -> None:
    """Guide sections 5 and 6 and floor rule 6: read a witness before repeating a send.

    The provider performs the send but its acknowledgement is lost, so the
    completion may fail and is redelivered. Before sending again the adapter
    consults an independent provider-visible witness, so the answer is sent
    exactly once however many redeliveries it takes to ack.
    """

    del ctx
    name = "ambiguous_send_is_not_repeated"
    target = await _target(subject, name)
    answer = f"answer-{_token()}"
    completed = _completed(target)
    await subject.emit(_update(target, answer))
    mark = len(subject.effects())
    subject.fail_next_delivery(accepted=True)
    errors: list[str] = []
    for _ in range(1 + _AMBIGUOUS_RETRIES):
        try:
            await subject.emit(completed)
        except Exception as exc:  # noqa: BLE001 - record the failure and retry
            errors.append(type(exc).__name__)
            continue
        break
    else:
        raise ConformanceFailure(
            f"{name}: expected a redelivered turn.completed to ack within "
            f"{_AMBIGUOUS_RETRIES} retries; observed {errors} with effects "
            f"{_since(subject, mark)!r}"
        )
    effects = _since(subject, mark)
    _require(
        len(effects) == 1 and effects[0].op == "send" and answer in effects[0].text,
        name,
        f"exactly one send containing {answer!r} after a lost acknowledgement",
        effects,
    )


def _always(capabilities: Capabilities) -> bool:
    del capabilities
    return True


def _ingress(capabilities: Capabilities) -> bool:
    return capabilities.ingress


def _not_relay(capabilities: Capabilities) -> bool:
    return capabilities.streaming is not Streaming.RELAY


def _not_silent(capabilities: Capabilities) -> bool:
    return capabilities.streaming is not Streaming.SILENT


def _buffered(capabilities: Capabilities) -> bool:
    return capabilities.streaming is Streaming.BUFFERED


def _authenticates(capabilities: Capabilities) -> bool:
    return capabilities.authenticates


def _refuses_foreign(capabilities: Capabilities) -> bool:
    return capabilities.refuses_foreign_targets


CHECKS: tuple[Check, ...] = (
    Check(
        "ingress_normalizes_to_a_channel_turn",
        "ingress",
        _ingress,
        ingress_normalizes_to_a_channel_turn,
    ),
    Check("ingress_delivery_id_is_stable", "ingress", _ingress, ingress_delivery_id_is_stable),
    Check(
        "attachments_match_declared_capability",
        "attachments",
        _ingress,
        attachments_match_declared_capability,
    ),
    Check("turn_status_is_tolerated", "delivery", _always, turn_status_is_tolerated),
    Check(
        "reply_streams_per_declared_mode",
        "streaming",
        _always,
        reply_streams_per_declared_mode,
    ),
    Check("completion_is_deduplicated", "delivery", _not_relay, completion_is_deduplicated),
    Check(
        "dropped_completion_without_answer_is_silent",
        "delivery",
        _always,
        dropped_completion_without_answer_is_silent,
    ),
    Check("approval_card_is_delivered", "approvals", _always, approval_card_is_delivered),
    Check(
        "progress_never_changes_the_answer",
        "progress",
        _always,
        progress_never_changes_the_answer,
    ),
    Check(
        "progress_post_is_idempotent_on_delivery_id",
        "progress",
        _not_relay,
        progress_post_is_idempotent_on_delivery_id,
    ),
    Check(
        "unauthenticated_egress_takes_no_effect",
        "errors",
        _authenticates,
        unauthenticated_egress_takes_no_effect,
    ),
    Check(
        "provider_failure_surfaces_as_delivery_failure",
        "errors",
        _not_silent,
        provider_failure_surfaces_as_delivery_failure,
    ),
    Check("foreign_target_is_refused", "errors", _refuses_foreign, foreign_target_is_refused),
    Check(
        "ambiguous_send_is_not_repeated",
        "delivery",
        _buffered,
        ambiguous_send_is_not_repeated,
    ),
)
"""Every check, in a stable order."""


def applicable_checks(capabilities: Capabilities) -> tuple[Check, ...]:
    """The checks an adapter declaring ``capabilities`` is held to, in ``CHECKS`` order."""

    return tuple(check for check in CHECKS if check.applies(capabilities))
