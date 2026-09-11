"""The verb results: frozen dataclasses that survive a JSON round trip.

Every verb on :class:`~curie_test_support.interaction.harness.InteractionHarness`
returns one of these. Two properties are load bearing and are the reason they are
not plain dicts:

* **Frozen.** An agent (or a test) holds a result across several steps and
  compares it against a later one. A mutable result lets a later verb quietly
  rewrite the evidence an earlier assertion was made from.
* **``to_dict()`` yields ONLY JSON primitives.** The ``python -m`` entry point
  writes these over a pipe, so a nested dataclass, a ``datetime``, a ``UUID`` or
  a tuple-keyed dict survives every in-process assertion and dies at the process
  boundary. The serialiser is written by hand, per class, rather than derived
  with ``dataclasses.asdict``: ``asdict`` recurses into nested dataclasses but
  leaves a ``UUID`` or a ``datetime`` exactly as it found it, which is the one
  mistake this file exists to make impossible.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "ActResult",
    "ApprovalRecord",
    "AuditEntry",
    "AuditResult",
    "CapturedCard",
    "FaultResult",
    "ResumeTurn",
    "ResumeTurnsResult",
    "CapturedAction",
    "CapturedMessage",
    "MessagesResult",
    "OutcomeResult",
    "ResetResult",
    "SendResult",
    "Snapshot",
]


@dataclass(frozen=True)
class CapturedAction:
    """One actionable element of a captured card, as the card rendered it.

    ``action_id`` and ``value`` are read off the REAL rendered Block Kit
    element, never composed here: ``act`` refuses anything that did not come
    from a capture, and this object is the token of that provenance.
    ``message_id`` rides along because two cards in one channel can carry
    byte-identical buttons, and without it ``act`` cannot tell them apart.

    ``handle`` is the pipe's form of that provenance. In process, ``act``
    matches on object identity, which a caller cannot fabricate. Over the
    ``python -m`` pipe there are no objects, so the CLI has to find the stored
    action from what the request names -- and every OTHER field is
    reconstructible without ever rendering a card (the action id is an
    importable renderer constant, the message id follows the harness's
    ``1700.%04d`` scheme, the value is the approval id). The handle is minted
    with ``secrets`` when the action is captured and is the one field that is
    not: a caller who never called ``messages()`` cannot produce one, so the
    pipe refuses the same forgery the identity check refuses. It is unguessable,
    not merely longer -- length is not the property, unpredictability is.
    """

    action_id: str
    message_id: str
    value: str
    text: str
    handle: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "message_id": self.message_id,
            "value": self.value,
            "text": self.text,
            "handle": self.handle,
        }


@dataclass(frozen=True)
class CapturedMessage:
    """One message the harness observed on the channel edge."""

    message_id: str
    thread: str
    text: str
    approval_id: str | None
    actions: tuple[CapturedAction, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "thread": self.thread,
            "text": self.text,
            "approval_id": self.approval_id,
            "actions": [action.to_dict() for action in self.actions],
        }


@dataclass(frozen=True)
class SendResult:
    """What one inbound turn created."""

    message_id: str
    thread: str
    run_id: str
    event_id: str
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "thread": self.thread,
            "run_id": self.run_id,
            "event_id": self.event_id,
            "text": self.text,
        }


@dataclass(frozen=True)
class CapturedCard:
    """A captured message that carries actions, paired with those actions.

    A projection of :class:`CapturedMessage`, not a second source of truth: the
    same object appears under ``message``. It exists because "the cards" and
    "the messages" are different questions -- a run posts plain replies too, and
    a caller that wants the actionable ones should not have to filter and then
    re-establish that what it filtered really did carry actions.
    """

    message_id: str
    approval_id: str | None
    actions: tuple[CapturedAction, ...]
    message: CapturedMessage

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "approval_id": self.approval_id,
            "actions": [action.to_dict() for action in self.actions],
            "message": self.message.to_dict(),
        }


@dataclass(frozen=True)
class MessagesResult:
    """Everything captured on the channel edge so far, newest last."""

    messages: tuple[CapturedMessage, ...]
    cards: tuple[CapturedCard, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages": [message.to_dict() for message in self.messages],
            "cards": [card.to_dict() for card in self.cards],
        }


@dataclass(frozen=True)
class ApprovalRecord:
    """One approval row, as the REAL ``GET /approvals/{id}`` answered it.

    ``raw`` keeps the whole body so a case can assert on a field this dataclass
    does not name, without the dataclass having to mirror the API schema (and
    silently drift from it). The named fields are the ones every case reads.
    """

    approval_id: str
    status: str
    resolved_by: str | None
    resolution_note: str | None
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "status": self.status,
            "resolved_by": self.resolved_by,
            "resolution_note": self.resolution_note,
            "raw": self.raw,
        }


@dataclass(frozen=True)
class AuditEntry:
    """One audit row, as the REAL ``GET /approvals/{id}/audit`` answered it."""

    action: str
    actor: str | None
    authorized: bool | None
    authorizer: str | None
    principal_kind: str | None
    actor_channel: str | None
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "actor": self.actor,
            "authorized": self.authorized,
            "authorizer": self.authorizer,
            "principal_kind": self.principal_kind,
            "actor_channel": self.actor_channel,
            "raw": self.raw,
        }


@dataclass(frozen=True)
class AuditResult:
    """One approval's audit trail, in the order the API returned it."""

    approval_id: str
    entries: tuple[AuditEntry, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "entries": [entry.to_dict() for entry in self.entries],
        }


@dataclass(frozen=True)
class ResumeTurn:
    """One resume turn read back off the REAL runs stream.

    Decoded from the stream the API's real ``ResumeQueue.enqueue`` xadded to
    (resumequeue.py:266). Hand-building one and feeding it to the kernel would
    satisfy an event-id assertion while the API had xadded nothing at all, which
    is why this type only ever comes from a read.
    """

    entry_id: str
    event_id: str
    conversation_id: str
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "event_id": self.event_id,
            "conversation_id": self.conversation_id,
            "text": self.text,
        }


@dataclass(frozen=True)
class ResumeTurnsResult:
    """Every resume turn currently on the run's stream."""

    stream: str
    turns: tuple[ResumeTurn, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"stream": self.stream, "turns": [turn.to_dict() for turn in self.turns]}


@dataclass(frozen=True)
class ActResult:
    """The settled outcome of driving one captured action.

    ``accepted`` is about the INTERACTION (the system took the click and
    settled it), while ``outcome`` is the approval's resulting status. The two
    are kept apart because a refused actor is an accepted interaction with an
    unchanged outcome, and collapsing them would make a refusal indistinguishable
    from a transport failure.
    """

    action_id: str
    message_id: str
    actor: str
    accepted: bool
    outcome: str
    note: str | None
    detail: str
    # The body Bolt acked the view with: ``None`` closes the dialog and is the
    # ACCEPTED submission, while ``{"response_action": "errors", ...}`` keeps it
    # open with the reason attached to the note field (handlers.py:637-645).
    # Kept whole, rather than collapsed into ``accepted``, because a case has to
    # assert WHICH field the refusal was attached to.
    response_action: dict[str, Any] | None = None
    # What the dispatcher asked Slack to stamp onto the card. A ``chat_update``
    # call on a mock, so it proves the dispatcher ASKED, not that Slack
    # rendered -- the same honest labelling stage 2 carries.
    rendered_text: str | None = None
    # Which approval this result is about. ``action_id``/``message_id`` identify
    # a CLICK, so the card-less ``resolve`` verb leaves them empty -- and left
    # the whole result unidentifiable, which is the bug this field closes. Both
    # verbs fill it.
    approval_id: str | None = None
    # The REAL ``ResolveOutcome.status_code`` when the verb owns the resolve hop
    # directly (``resolve``); None on the click path, where the resolve happens
    # inside the dispatcher and the outcome is observable as ``response_action``
    # instead. ``0`` is the product's explicit UNKNOWN outcome -- the request may
    # or may not have been delivered (approval_actions.py:239-244) -- and a case
    # about a broken transport has to be able to say so rather than inferring it
    # from a falsy ``accepted``.
    status_code: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "message_id": self.message_id,
            "actor": self.actor,
            "accepted": self.accepted,
            "outcome": self.outcome,
            "note": self.note,
            "detail": self.detail,
            "response_action": self.response_action,
            "rendered_text": self.rendered_text,
            "approval_id": self.approval_id,
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class Snapshot:
    """What a ``await_outcome`` predicate is handed on each poll.

    Deliberately a VALUE, rebuilt per poll: a predicate closing over the live
    harness could observe a half-applied state mid-capture, and the resulting
    flake would be indistinguishable from a real ordering bug.
    """

    run_id: str
    messages: tuple[CapturedMessage, ...]
    stream_entries: int
    # The status of the approval this run is currently about, or None before one
    # exists. Read over the real API, briefly cached: a predicate polls tens of
    # times a second and an uncached read would turn every wait into a load test
    # of the loopback server rather than of the behavior.
    approval_status: str | None = None
    approval_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "messages": [message.to_dict() for message in self.messages],
            "stream_entries": self.stream_entries,
            "approval_status": self.approval_status,
            "approval_id": self.approval_id,
        }


@dataclass(frozen=True)
class OutcomeResult:
    """A satisfied wait, and how long it actually took.

    ``elapsed_s`` is REPORTED rather than merely measured: an agent scripting
    the harness has no other way to tell "returned immediately" from "waited
    29s", and that difference is the difference between a healthy run and a
    flake that is one slow box away from failing.
    """

    satisfied: bool
    elapsed_s: float
    deadline_s: float
    stream_entries: int
    message_count: int
    approval_id: str | None = None
    approval_status: str | None = None
    resolved_by: str | None = None
    # The resume turns on the stream at the moment the predicate was satisfied.
    # The COUNT is not enough for the reconciliation cases: "exactly one resume,
    # and it is the one for THIS approval" is the property, and a count cannot
    # distinguish a second resume for the same approval from the first.
    resume_turns: tuple[ResumeTurn, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "satisfied": self.satisfied,
            "elapsed_s": self.elapsed_s,
            "deadline_s": self.deadline_s,
            "stream_entries": self.stream_entries,
            "message_count": self.message_count,
            "approval_id": self.approval_id,
            "approval_status": self.approval_status,
            "resolved_by": self.resolved_by,
            "resume_turns": [turn.to_dict() for turn in self.resume_turns],
        }


@dataclass(frozen=True)
class ResetResult:
    """The run's stream namespace after a reset."""

    stream: str
    entries_remaining: int

    def to_dict(self) -> dict[str, Any]:
        return {"stream": self.stream, "entries_remaining": self.entries_remaining}


@dataclass(frozen=True)
class FaultResult:
    """What is armed after one ``arm_fault`` / ``disarm_fault``.

    Carries the WHOLE armed set, not just the name that changed: an agent on the
    pipe has no other way to see what a previous line left armed, and a fault it
    does not know about is exactly the thing that makes a later verb's result
    unreadable.
    """

    fault: str
    armed: bool
    armed_faults: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "fault": self.fault,
            "armed": self.armed,
            "armed_faults": list(self.armed_faults),
        }
