"""The channel adapter conformance kit, proven against adapters whose behavior is known.

Two halves, and both are needed. A reference adapter per streaming mode, built
the way ``docs/guides/building-a-channel-adapter.md`` says a conforming adapter
behaves, must pass every check its capabilities make applicable: otherwise the
kit refuses honest adapters. And for each check, an adapter broken in exactly
the way that check names must FAIL it with ``ConformanceFailure``, and fail
nothing else it was not built to break: otherwise the check has no teeth, or
its teeth bite the wrong defect, and a green suite proves nothing.

No app is imported. The kit ships in this package so an adapter author outside
the repository can run it, so its own tests may lean on nothing else either.
"""

from __future__ import annotations

import asyncio
import itertools
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest
from channel_protocol import (
    ConfirmIntent,
    ReplyAck,
    ReplyEvent,
    ReplyPost,
    ReplyUpdate,
    TurnCompleted,
    TurnStatus,
    progress_text,
)
from channel_protocol.conformance import (
    CHECKS,
    Capabilities,
    Check,
    CheckContext,
    ConformanceFailure,
    Effect,
    IngressTurn,
    Streaming,
    Upstream,
    applicable_checks,
)

# The planned checks and their areas. Pinned so a renamed, dropped or re-homed
# check is a visible change to this file, not a silent one.
EXPECTED_CHECKS = {
    "ingress_normalizes_to_a_channel_turn": "ingress",
    "ingress_delivery_id_is_stable": "ingress",
    "attachments_match_declared_capability": "attachments",
    "turn_status_is_tolerated": "delivery",
    "reply_streams_per_declared_mode": "streaming",
    "completion_is_deduplicated": "delivery",
    "dropped_completion_without_answer_is_silent": "delivery",
    "approval_card_is_delivered": "approvals",
    "progress_never_changes_the_answer": "progress",
    "progress_post_is_idempotent_on_delivery_id": "progress",
    "unauthenticated_egress_takes_no_effect": "errors",
    "provider_failure_surfaces_as_delivery_failure": "errors",
    "foreign_target_is_refused": "errors",
    "ambiguous_send_is_not_repeated": "delivery",
}

_TURN_FIELDS = ("kind", "address", "delivery_id", "conversation_id", "author", "text", "reply_ref")


def _validate_turn(body: Mapping[str, Any]) -> None:
    """A stand-in for the platform's ``TurnIn``: the required fields, as strings.

    The kit is app-free, so the real validator is the root suite's to pass; this
    one only has to refuse the shapes ``POST /channels/turns`` refuses.
    """

    for name in _TURN_FIELDS:
        if not isinstance(body.get(name), str):
            raise ValueError(f"{name} must be a string")
    for ref in body.get("attachments", []):
        if not ref.get("id") or not ref.get("name"):
            raise ValueError("an attachment reference needs an id and a name")


CONTEXT = CheckContext(validate_turn=_validate_turn)


class ProviderUnavailableError(RuntimeError):
    """The in-memory provider refused one side effect: a loud delivery failure."""


class ProviderLostAckError(RuntimeError):
    """The provider performed the side effect, but its answer never arrived."""


class AdapterUnreachableError(ConnectionError):
    """What the worker reads as "the endpoint's host did not answer"."""


class CredentialRefusedError(PermissionError):
    """The egress credential did not verify; nothing was read or done."""


@dataclass
class _Durable:
    """What a conforming adapter keeps across a restart (guide section 6)."""

    completed: set[str] = field(default_factory=set)
    posted: dict[str, str] = field(default_factory=dict)
    buffers: dict[str, str] = field(default_factory=dict)
    cards: dict[str, str] = field(default_factory=dict)  # card ref -> conversation


class ReferenceAdapter:
    """An in-memory adapter that does what the guide says, in one streaming mode.

    EDIT behaves like Discord (a placeholder per conversation, edited in place;
    posts keyed on the 1.1 ``delivery_id``; a card settled by editing it),
    BUFFERED like mail (answer text held per conversation and sent once on the
    first ``turn.completed``, a provider-visible marker read before any resend,
    progress silent, a settled card answered with one follow-up send), SILENT
    like GitHub (acks everything, shows nothing, no egress credential), RELAY
    like the generic HTTP relay (every event forwarded verbatim, no ingress).

    ``_durable`` survives ``restart``; ``_memory`` does not. Each broken
    subclass overrides ONE hook below.
    """

    def __init__(self, streaming: Streaming, *, serves_attachments: bool = False) -> None:
        self.capabilities = Capabilities(
            kind=f"reference-{streaming.value}",
            streaming=streaming,
            ingress=streaming is not Streaming.RELAY,
            authenticates=streaming is not Streaming.SILENT,
            refuses_foreign_targets=streaming in (Streaming.EDIT, Streaming.BUFFERED),
            serves_attachments=serves_attachments,
            settles_approval_cards=streaming in (Streaming.EDIT, Streaming.BUFFERED),
        )
        self._effects: list[Effect] = []
        self._ids = itertools.count(1)
        self._durable = _Durable()
        self._memory: set[str] = set()
        self._files: dict[str, bytes] = {}
        self._fail_next: type[Exception] | None = None

    # -- the subject protocol ------------------------------------------------

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        kind = self.capabilities.kind
        conversation = f"conversation-{message.id}"
        if not self.capabilities.ingress:
            return [
                IngressTurn(
                    kind=kind,
                    address="reference-far-end",
                    delivery_id=f"relay-{message.id}",
                    conversation_id=conversation,
                    reply_ref=None,
                )
            ]
        reply_ref: str | None = None  # SILENT: nothing addressable to hand back
        if self.capabilities.streaming is Streaming.EDIT:
            reply_ref = self._placeholder()
        elif self.capabilities.streaming is Streaming.BUFFERED:
            reply_ref = message.id  # email replies to the upstream message
        # The platform refuses the first attempt transiently; the retry follows.
        return [
            self._turn(message, conversation, reply_ref, message.id),
            self._turn(message, conversation, reply_ref, self._retry_delivery_id(message)),
        ]

    async def emit(self, event: ReplyEvent, *, best_effort: bool = False) -> ReplyAck:
        try:
            return self._deliver(event)
        except AdapterUnreachableError:
            if best_effort:
                # The worker's #708 rule: an UNREACHABLE adapter is acked on a
                # best-effort turn. Only a broken adapter ever lands here.
                return ReplyAck(ref=None)
            raise

    async def emit_unauthenticated(self, event: ReplyEvent) -> ReplyAck:
        if self.capabilities.authenticates:
            raise CredentialRefusedError("missing or invalid credential")
        return self._deliver(event)

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        if not self.capabilities.serves_attachments:
            raise AssertionError("this adapter serves no attachments")
        return self._files[attachment_id]

    async def restart(self) -> None:
        self._memory = set()

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        self._fail_next = ProviderLostAckError if accepted else ProviderUnavailableError

    def effects(self) -> Sequence[Effect]:
        return list(self._effects)

    # -- hooks a broken adapter overrides ------------------------------------

    def _retry_delivery_id(self, message: Upstream) -> str:
        return message.id

    def _attachments(self, message: Upstream) -> tuple[Mapping[str, Any], ...]:
        if not self.capabilities.serves_attachments:
            return ()
        refs = []
        for index, attachment in enumerate(message.attachments):
            ref_id = f"{message.id}/att-{index}"
            self._files[ref_id] = attachment.content
            refs.append({"id": ref_id, "name": attachment.name})
        return tuple(refs)

    def _refuses(self, event: ReplyEvent) -> bool:
        return (
            self.capabilities.refuses_foreign_targets
            and event.target.kind != self.capabilities.kind
        )

    def _side_effect(self, op: Any, ref: str | None, text: str) -> None:
        failure, self._fail_next = self._fail_next, None
        if failure is ProviderUnavailableError:
            raise ProviderUnavailableError("provider answered 503")
        self._effects.append(Effect(op=op, ref=ref, text=text))
        if failure is ProviderLostAckError:
            raise ProviderLostAckError("the provider's answer was lost")

    def _show_answer(self, reply_ref: str, text: str) -> str:
        self._side_effect("edit", reply_ref, text)
        return reply_ref

    def _complete_edit(self, event: TurnCompleted) -> None:
        del event  # streamed edits already made the answer visible

    def _show_status(self, event: TurnStatus) -> None:
        del event  # a caption is optional; showing nothing is conforming

    def _known_post(self, delivery_id: str | None) -> str | None:
        return self._durable.posted.get(delivery_id) if delivery_id is not None else None

    def _buffered_progress(self, event: ReplyPost | ReplyUpdate) -> None:
        del event  # silent: a progress body never touches the buffered answer

    def _is_completed(self, event_id: str) -> bool:
        return event_id in self._durable.completed

    def _settle(self, event: TurnCompleted) -> None:
        self._durable.completed.add(event.event_id)
        self._durable.buffers.pop(event.target.conversation_id or "", None)

    def _owed_text(self, event: TurnCompleted) -> str | None:
        return self._durable.buffers.get(event.target.conversation_id or "")

    def _already_sent(self, event_id: str) -> bool:
        """The provider-visible witness: a send carrying this event's marker."""

        marker = self._marker(event_id)
        return any(effect.op == "send" and marker in effect.text for effect in self._effects)

    def _card_ack(self, ref: str) -> ReplyAck:
        return ReplyAck(ref=ref)

    def _settle_card(self, event: ReplyUpdate, card_ref: str, text: str) -> None:
        if self.capabilities.streaming is Streaming.EDIT:
            self._side_effect("edit", card_ref, text)
        else:
            self._side_effect("send", card_ref, text)

    def _provider_failure(self, failure: ProviderUnavailableError) -> Exception:
        return failure

    # -- the behavior ----------------------------------------------------------

    @staticmethod
    def _marker(event_id: str) -> str:
        return f"[curie-event {event_id}]"

    def _placeholder(self) -> str:
        # Posted by ingress, before the turn exists: not a reply-wire effect.
        return f"msg-{next(self._ids)}"

    def _turn(
        self, message: Upstream, conversation: str, reply_ref: str | None, delivery_id: str
    ) -> IngressTurn:
        attachments = self._attachments(message)
        body: dict[str, Any] | None = None
        if self.capabilities.streaming is not Streaming.SILENT:
            body = {
                "kind": self.capabilities.kind,
                "address": "reference-address",
                "delivery_id": delivery_id,
                "conversation_id": conversation,
                "author": "someone@example.com",
                "text": f"Subject\n\n{message.text}",
                "reply_ref": reply_ref,
            }
            if attachments:
                body["attachments"] = [dict(ref) for ref in attachments]
        return IngressTurn(
            kind=self.capabilities.kind,
            address="reference-address",
            delivery_id=delivery_id,
            conversation_id=conversation,
            reply_ref=reply_ref,
            attachments=attachments,
            body=body,
        )

    def _deliver(self, event: ReplyEvent) -> ReplyAck:
        if self._refuses(event):
            raise ValueError(f"cannot render kind {event.target.kind!r}")
        try:
            return self._render(event)
        except ProviderUnavailableError as failure:
            raise self._provider_failure(failure) from failure

    def _render(self, event: ReplyEvent) -> ReplyAck:
        streaming = self.capabilities.streaming
        if streaming is Streaming.RELAY:
            self._side_effect("forward", None, event.model_dump_json())
            return ReplyAck(ref="relay-ref")
        if streaming is Streaming.SILENT:
            return ReplyAck(ref=None)
        if isinstance(event, ReplyUpdate) and event.settled is not None:
            return self._settled(event)
        if streaming is Streaming.EDIT:
            return self._deliver_edit(event)
        return self._deliver_buffered(event)

    @staticmethod
    def _answer_text(event: ReplyUpdate) -> str:
        text = event.text if event.text is not None else ""
        if event.text is None and event.message is not None:
            text = event.message.text
        return text

    def _settled(self, event: ReplyUpdate) -> ReplyAck:
        settled = event.settled
        assert settled is not None
        card_ref = event.target.reply_ref
        if card_ref is None or card_ref not in self._durable.cards:
            return ReplyAck(ref=card_ref)
        text = self._answer_text(event)
        if settled.decision is None:
            text = f"{text}\n\nApproval expired."
        else:
            resolver = settled.resolver or "an authorized operator"
            text = f"{text}\n\n{settled.decision.title()} by {resolver}."
        self._settle_card(event, card_ref, text)
        return ReplyAck(ref=card_ref)

    def _post_card(self, event: ReplyPost) -> ReplyAck:
        conversation = event.target.conversation_id or ""
        if self.capabilities.streaming is Streaming.EDIT:
            ref = f"msg-{next(self._ids)}"
            self._side_effect("post", ref, event.message.text)
        else:
            ref = f"card-{next(self._ids)}"
            held = self._durable.buffers.get(conversation)
            self._durable.buffers[conversation] = (
                f"{held}\n\n{event.message.text}" if held else event.message.text
            )
        self._durable.cards[ref] = conversation
        return self._card_ack(ref)

    def _deliver_edit(self, event: ReplyEvent) -> ReplyAck:
        if isinstance(event, TurnStatus):
            self._show_status(event)
            return ReplyAck(ref=None)
        if isinstance(event, TurnCompleted):
            if not self._is_completed(event.event_id):
                self._complete_edit(event)
                self._durable.completed.add(event.event_id)
            return ReplyAck(ref=None)
        if isinstance(event, ReplyPost):
            if isinstance(event.message.interaction, ConfirmIntent) and event.progress is None:
                return self._post_card(event)
            known = self._known_post(event.delivery_id)
            if known is not None:
                return ReplyAck(ref=known)
            text = progress_text(event.progress) if event.progress else event.message.text
            ref = f"msg-{next(self._ids)}"
            self._side_effect("post", ref, text)
            if event.delivery_id is not None:
                self._durable.posted[event.delivery_id] = ref
            return ReplyAck(ref=ref)
        reply_ref = event.target.reply_ref
        if reply_ref is None:
            raise ValueError("an edit needs the reply_ref it edits")
        if event.progress is not None:
            self._side_effect("edit", reply_ref, progress_text(event.progress))
            return ReplyAck(ref=reply_ref)
        return ReplyAck(ref=self._show_answer(reply_ref, self._answer_text(event)))

    def _deliver_buffered(self, event: ReplyEvent) -> ReplyAck:
        conversation = event.target.conversation_id or ""
        if isinstance(event, TurnStatus):
            return ReplyAck(ref=None)
        if isinstance(event, ReplyUpdate | ReplyPost) and event.progress is not None:
            self._buffered_progress(event)
            return ReplyAck(ref=None)
        if isinstance(event, ReplyUpdate):
            self._durable.buffers[conversation] = self._answer_text(event)
            return ReplyAck(ref=None)
        if isinstance(event, ReplyPost):
            if isinstance(event.message.interaction, ConfirmIntent):
                return self._post_card(event)
            held = self._durable.buffers.get(conversation)
            self._durable.buffers[conversation] = (
                f"{held}\n\n{event.message.text}" if held else event.message.text
            )
            return ReplyAck(ref=None)
        if self._is_completed(event.event_id):
            return ReplyAck(ref=None)
        text = self._owed_text(event)
        if text and not self._already_sent(event.event_id):
            self._side_effect(
                "send", event.target.reply_ref, f"{text}\n\n{self._marker(event.event_id)}"
            )
        self._settle(event)
        return ReplyAck(ref=None)


# -- broken adapters, one violation each -------------------------------------


class PostsEveryUpdate(ReferenceAdapter):
    """Streams by posting a new message per update instead of editing reply_ref."""

    def _show_answer(self, reply_ref: str, text: str) -> str:
        ref = f"msg-{next(self._ids)}"
        self._side_effect("post", ref, text)
        return ref


class EditsOnlyOnCompletion(ReferenceAdapter):
    """Acks every update silently and edits the answer in once, on completion."""

    def __init__(self, streaming: Streaming) -> None:
        super().__init__(streaming)
        self._pending: dict[str, str] = {}

    def _show_answer(self, reply_ref: str, text: str) -> str:
        self._pending[reply_ref] = text
        return reply_ref

    def _complete_edit(self, event: TurnCompleted) -> None:
        reply_ref = event.target.reply_ref
        if reply_ref is not None and reply_ref in self._pending:
            self._side_effect("edit", reply_ref, self._pending.pop(reply_ref))


class ResendsDuplicateCompletion(ReferenceAdapter):
    """Keeps no record of completions, so a redelivered one sends the answer again."""

    def _settle(self, event: TurnCompleted) -> None:
        del event

    def _already_sent(self, event_id: str) -> bool:
        del event_id
        return False


class ForgetsCompletionsOnRestart(ReferenceAdapter):
    """Dedupes completions in memory only, so a restart loses the receipt."""

    def _is_completed(self, event_id: str) -> bool:
        return event_id in self._memory

    def _settle(self, event: TurnCompleted) -> None:
        self._memory.add(event.event_id)  # the answer stays held, the receipt does not

    def _already_sent(self, event_id: str) -> bool:
        del event_id
        return False


class ResendsAfterLostAck(ReferenceAdapter):
    """Repeats an ambiguous send instead of reading the provider-visible witness."""

    def _already_sent(self, event_id: str) -> bool:
        del event_id
        return False


class AnswersADroppedTurn(ReferenceAdapter):
    """Sends a notice for a dropped turn that never said anything."""

    def _owed_text(self, event: TurnCompleted) -> str | None:
        return super()._owed_text(event) or "Sorry, I could not answer that."


class ProgressOverwritesAnswer(ReferenceAdapter):
    """Reads a progress post's fallback text as answer text, replacing the answer."""

    def _buffered_progress(self, event: ReplyPost | ReplyUpdate) -> None:
        if isinstance(event, ReplyPost):
            self._durable.buffers[event.target.conversation_id or ""] = event.message.text


class ProgressCardClearsAnswer(ReferenceAdapter):
    """Reads a card edit's absent text as an empty answer and clears the buffer."""

    def _buffered_progress(self, event: ReplyPost | ReplyUpdate) -> None:
        if isinstance(event, ReplyUpdate):
            self._durable.buffers.pop(event.target.conversation_id or "", None)


class CardWithoutRef(ReferenceAdapter):
    """Declares it settles cards but acks the card with no ref to settle it by."""

    def _card_ack(self, ref: str) -> ReplyAck:
        del ref
        return ReplyAck(ref=None)


class IgnoresSettlement(ReferenceAdapter):
    """Acks a settled card update and never shows the decision."""

    def _settle_card(self, event: ReplyUpdate, card_ref: str, text: str) -> None:
        del event, card_ref, text


class SettlesWithTheBareCardText(ReferenceAdapter):
    """Answers a settlement with a send that repeats the card and names no outcome.

    Something visible happens, so a check that only counts effects passes it;
    the person still cannot tell who decided or what was decided.
    """

    def _settle_card(self, event: ReplyUpdate, card_ref: str, text: str) -> None:
        del text
        super()._settle_card(event, card_ref, self._answer_text(event))


class RepostsOnRepeatedDeliveryId(ReferenceAdapter):
    """Ignores the 1.1 delivery_id, so a retried post creates a second message."""

    def _known_post(self, delivery_id: str | None) -> str | None:
        del delivery_id
        return None


class AcceptsUnauthenticated(ReferenceAdapter):
    """Never verifies the egress secret."""

    async def emit_unauthenticated(self, event: ReplyEvent) -> ReplyAck:
        return self._deliver(event)


class MintsFreshDeliveryIdOnRetry(ReferenceAdapter):
    """Escapes an ingress error by minting a new delivery_id, defeating the receipt."""

    def _retry_delivery_id(self, message: Upstream) -> str:
        del message
        return str(uuid.uuid4())


class SwallowsProviderFailure(ReferenceAdapter):
    """Acks an event whose provider call failed, so the worker never retries it."""

    def _side_effect(self, op: Any, ref: str | None, text: str) -> None:
        try:
            super()._side_effect(op, ref, text)
        except ProviderUnavailableError:
            return


class FailsLikeAnUnreachableHost(ReferenceAdapter):
    """Answers a provider failure the way the worker classifies as unreachable.

    Uvicorn's bare 500 used to close the socket under the worker's pooled
    session; the worker then saw a dead host, and a best-effort turn acked
    an event nobody delivered.
    """

    def _provider_failure(self, failure: ProviderUnavailableError) -> Exception:
        return AdapterUnreachableError(str(failure))


class SendsUnservedAttachments(ReferenceAdapter):
    """Sends attachment refs while declaring it serves no attachment endpoint."""

    def _attachments(self, message: Upstream) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            {"id": f"{message.id}/att-{index}", "name": attachment.name}
            for index, attachment in enumerate(message.attachments)
        )


class DropsServedAttachments(ReferenceAdapter):
    """Declares it serves attachments, then sends the turn without their refs."""

    def _attachments(self, message: Upstream) -> tuple[Mapping[str, Any], ...]:
        del message
        return ()


class ServesWrongBytes(ReferenceAdapter):
    """Sends honest refs but answers the fetch with some other file's bytes."""

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        return (await super().fetch_attachment(attachment_id)) + b"-stale"


class AcceptsForeignTargets(ReferenceAdapter):
    """Renders an event addressed to another channel kind."""

    def _refuses(self, event: ReplyEvent) -> bool:
        del event
        return False


class CaptionsStatusAsEdit(ReferenceAdapter):
    """Shows the liveness caption by editing the placeholder: legitimate."""

    def _show_status(self, event: TurnStatus) -> None:
        if event.target.reply_ref is not None and event.status:
            self._side_effect("edit", event.target.reply_ref, f"_{event.status}_")


class PostsStatus(ReferenceAdapter):
    """Posts a new message for every liveness caption."""

    def _show_status(self, event: TurnStatus) -> None:
        self._side_effect("post", f"msg-{next(self._ids)}", event.status)


def _run(subject: ReferenceAdapter, check: Check) -> None:
    asyncio.run(check.run(subject, CONTEXT))


def _check(name: str) -> Check:
    return next(check for check in CHECKS if check.name == name)


def _failing(subject_factory: Callable[[], ReferenceAdapter]) -> set[str]:
    """Every applicable check a fresh instance fails with ``ConformanceFailure``."""

    failed: set[str] = set()
    for check in applicable_checks(subject_factory().capabilities):
        try:
            _run(subject_factory(), check)
        except ConformanceFailure:
            failed.add(check.name)
    return failed


# -- the check set and its selection ------------------------------------------


def test_the_kit_carries_exactly_the_planned_checks() -> None:
    names = [check.name for check in CHECKS]
    assert len(names) == len(set(names)), "a check name appears twice"
    assert {check.name: check.area for check in CHECKS} == EXPECTED_CHECKS


def _fully_capable(streaming: Streaming) -> Capabilities:
    return Capabilities(
        kind="anything",
        streaming=streaming,
        ingress=True,
        authenticates=True,
        refuses_foreign_targets=True,
        serves_attachments=True,
        settles_approval_cards=True,
    )


def test_a_registered_adapter_is_held_to_every_check_its_mode_owes() -> None:
    """Registering is all it takes: no check is reachable only by hand-picking it."""

    covered = {
        check for streaming in Streaming for check in applicable_checks(_fully_capable(streaming))
    }
    assert covered == set(CHECKS)
    assert set(applicable_checks(_fully_capable(Streaming.BUFFERED))) == set(CHECKS)


def _names(capabilities: Capabilities) -> set[str]:
    return {check.name for check in applicable_checks(capabilities)}


def test_a_relay_owes_no_dedupe_idempotency_or_ingress() -> None:
    relay = ReferenceAdapter(Streaming.RELAY).capabilities
    excluded = set(EXPECTED_CHECKS) - _names(relay)
    assert excluded == {
        "completion_is_deduplicated",
        "progress_post_is_idempotent_on_delivery_id",
        "ambiguous_send_is_not_repeated",
        "ingress_normalizes_to_a_channel_turn",
        "ingress_delivery_id_is_stable",
        "attachments_match_declared_capability",
        "foreign_target_is_refused",
    }


def test_an_adapter_without_an_egress_credential_owes_no_unauthenticated_check() -> None:
    silent = ReferenceAdapter(Streaming.SILENT).capabilities
    assert not silent.authenticates
    excluded = set(EXPECTED_CHECKS) - _names(silent)
    assert excluded == {
        "unauthenticated_egress_takes_no_effect",
        "provider_failure_surfaces_as_delivery_failure",
        "foreign_target_is_refused",
        "ambiguous_send_is_not_repeated",
    }


@pytest.mark.parametrize("streaming", list(Streaming))
def test_only_a_buffered_adapter_owes_the_ambiguous_send_check(streaming: Streaming) -> None:
    owed = "ambiguous_send_is_not_repeated" in _names(_fully_capable(streaming))
    assert owed is (streaming is Streaming.BUFFERED)


@pytest.mark.parametrize(
    ("capability", "check_name"),
    [
        ("authenticates", "unauthenticated_egress_takes_no_effect"),
        ("refuses_foreign_targets", "foreign_target_is_refused"),
        ("ingress", "ingress_delivery_id_is_stable"),
    ],
)
def test_each_capability_flag_alone_decides_its_check(capability: str, check_name: str) -> None:
    base = dict(
        kind="flagged",
        streaming=Streaming.EDIT,
        ingress=True,
        authenticates=True,
        refuses_foreign_targets=True,
        serves_attachments=False,
        settles_approval_cards=True,
    )
    assert check_name in _names(Capabilities(**base))
    assert check_name not in _names(Capabilities(**{**base, capability: False}))


# -- honest adapters pass ------------------------------------------------------

_REFERENCES: dict[str, Callable[[], ReferenceAdapter]] = {
    "edit": lambda: ReferenceAdapter(Streaming.EDIT),
    "edit-attachments": lambda: ReferenceAdapter(Streaming.EDIT, serves_attachments=True),
    "buffered": lambda: ReferenceAdapter(Streaming.BUFFERED),
    "silent": lambda: ReferenceAdapter(Streaming.SILENT),
    "relay": lambda: ReferenceAdapter(Streaming.RELAY),
    # A caption rendered as an edit of the placeholder is a legitimate liveness
    # signal; the status check must not refuse it.
    "edit-captions-status": lambda: CaptionsStatusAsEdit(Streaming.EDIT),
}


@pytest.mark.parametrize(
    ("reference", "check"),
    [
        pytest.param(name, check, id=f"{name}-{check.name}")
        for name, make in _REFERENCES.items()
        for check in applicable_checks(make().capabilities)
    ],
)
def test_a_conforming_adapter_passes(reference: str, check: Check) -> None:
    _run(_REFERENCES[reference](), check)


# -- broken adapters fail the check that names their defect, and only it ------


@dataclass(frozen=True)
class _Broken:
    name: str
    make: Callable[[], ReferenceAdapter]
    check: str
    # Checks the same defect necessarily also breaks, each named on purpose so a
    # check that starts biting an unrelated defect shows up as a diff here.
    collateral: frozenset[str] = frozenset()


_BROKEN = [
    _Broken(
        "posts-every-update",
        lambda: PostsEveryUpdate(Streaming.EDIT),
        "reply_streams_per_declared_mode",
        # The answer is never at reply_ref, which the progress check reads too.
        frozenset({"progress_never_changes_the_answer"}),
    ),
    _Broken(
        "edits-only-on-completion",
        lambda: EditsOnlyOnCompletion(Streaming.EDIT),
        "reply_streams_per_declared_mode",
        # Nothing is visible when the provider fails mid-stream, so the
        # provider-failure check sees no effect to fail or land.
        frozenset({"provider_failure_surfaces_as_delivery_failure"}),
    ),
    _Broken(
        "resends-duplicate-completion",
        lambda: ResendsDuplicateCompletion(Streaming.BUFFERED),
        "completion_is_deduplicated",
        # Without a witness, a lost acknowledgement is resent too.
        frozenset({"ambiguous_send_is_not_repeated"}),
    ),
    _Broken(
        "forgets-completions-on-restart",
        lambda: ForgetsCompletionsOnRestart(Streaming.BUFFERED),
        "completion_is_deduplicated",
        frozenset({"ambiguous_send_is_not_repeated"}),
    ),
    _Broken(
        "resends-after-lost-ack",
        lambda: ResendsAfterLostAck(Streaming.BUFFERED),
        "ambiguous_send_is_not_repeated",
    ),
    _Broken(
        "answers-a-dropped-turn",
        lambda: AnswersADroppedTurn(Streaming.BUFFERED),
        "dropped_completion_without_answer_is_silent",
    ),
    _Broken(
        "progress-overwrites-answer",
        lambda: ProgressOverwritesAnswer(Streaming.BUFFERED),
        "progress_never_changes_the_answer",
    ),
    _Broken(
        "progress-card-clears-answer",
        lambda: ProgressCardClearsAnswer(Streaming.BUFFERED),
        "progress_never_changes_the_answer",
    ),
    _Broken(
        "card-without-ref",
        lambda: CardWithoutRef(Streaming.EDIT),
        "approval_card_is_delivered",
    ),
    _Broken(
        "buffered-card-without-ref",
        lambda: CardWithoutRef(Streaming.BUFFERED),
        "approval_card_is_delivered",
    ),
    _Broken(
        "ignores-settlement",
        lambda: IgnoresSettlement(Streaming.EDIT),
        "approval_card_is_delivered",
    ),
    _Broken(
        "buffered-ignores-settlement",
        lambda: IgnoresSettlement(Streaming.BUFFERED),
        "approval_card_is_delivered",
    ),
    _Broken(
        "buffered-settles-with-the-bare-card-text",
        lambda: SettlesWithTheBareCardText(Streaming.BUFFERED),
        "approval_card_is_delivered",
    ),
    _Broken(
        "reposts-on-repeated-delivery-id",
        lambda: RepostsOnRepeatedDeliveryId(Streaming.EDIT),
        "progress_post_is_idempotent_on_delivery_id",
    ),
    _Broken(
        "accepts-unauthenticated",
        lambda: AcceptsUnauthenticated(Streaming.EDIT),
        "unauthenticated_egress_takes_no_effect",
    ),
    _Broken(
        "mints-fresh-delivery-id-on-retry",
        lambda: MintsFreshDeliveryIdOnRetry(Streaming.EDIT),
        "ingress_delivery_id_is_stable",
    ),
    _Broken(
        "swallows-provider-failure",
        lambda: SwallowsProviderFailure(Streaming.EDIT),
        "provider_failure_surfaces_as_delivery_failure",
    ),
    _Broken(
        "relay-swallows-provider-failure",
        lambda: SwallowsProviderFailure(Streaming.RELAY),
        "provider_failure_surfaces_as_delivery_failure",
    ),
    _Broken(
        "fails-like-an-unreachable-host",
        lambda: FailsLikeAnUnreachableHost(Streaming.EDIT),
        "provider_failure_surfaces_as_delivery_failure",
    ),
    _Broken(
        "buffered-fails-like-an-unreachable-host",
        lambda: FailsLikeAnUnreachableHost(Streaming.BUFFERED),
        "provider_failure_surfaces_as_delivery_failure",
    ),
    _Broken(
        "sends-unserved-attachments",
        lambda: SendsUnservedAttachments(Streaming.EDIT),
        "attachments_match_declared_capability",
    ),
    _Broken(
        "drops-served-attachments",
        lambda: DropsServedAttachments(Streaming.EDIT, serves_attachments=True),
        "attachments_match_declared_capability",
    ),
    _Broken(
        "serves-wrong-bytes",
        lambda: ServesWrongBytes(Streaming.EDIT, serves_attachments=True),
        "attachments_match_declared_capability",
    ),
    _Broken(
        "accepts-foreign-targets",
        lambda: AcceptsForeignTargets(Streaming.EDIT),
        "foreign_target_is_refused",
    ),
    _Broken(
        "posts-status",
        lambda: PostsStatus(Streaming.EDIT),
        "turn_status_is_tolerated",
    ),
]


@pytest.mark.parametrize("broken", [pytest.param(b, id=b.name) for b in _BROKEN])
def test_a_broken_adapter_fails_the_check_naming_its_defect(broken: _Broken) -> None:
    check = _check(broken.check)
    assert check.applies(broken.make().capabilities), "the defect must be one it is held to"
    with pytest.raises(ConformanceFailure):
        _run(broken.make(), check)


@pytest.mark.parametrize("broken", [pytest.param(b, id=b.name) for b in _BROKEN])
def test_a_broken_adapter_fails_nothing_its_defect_does_not_explain(broken: _Broken) -> None:
    assert _failing(broken.make) == {broken.check} | broken.collateral


def test_a_body_the_platform_refuses_fails_ingress_normalization() -> None:
    def refuse(body: Mapping[str, Any]) -> None:
        raise ValueError("the platform refuses this body")

    with pytest.raises(ConformanceFailure):
        asyncio.run(
            _check("ingress_normalizes_to_a_channel_turn").run(
                ReferenceAdapter(Streaming.EDIT), CheckContext(validate_turn=refuse)
            )
        )
