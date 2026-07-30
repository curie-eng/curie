"""Approval-resolution fan-out: enqueue the resume turn onto the runs stream.

Resolving a pending approval (#244, ADR-0010) must wake the suspended session.
Rather than a bespoke resume channel, the API appends a normal ``QueuedTurn``
onto the same ``curie:runs`` stream the dispatcher feeds, so the resume walks
the identical consumer -> kernel -> claim path a Slack mention takes: the kernel
finds the thread's route suspended and rehydrates it (ADR-0003), and the
platform-authored resolution text becomes the turn that continues the run.

Wire encoding mirrors the dispatcher's seam exactly (a single ``payload`` field
holding the model's JSON). The turn's ``event_id`` is deterministic per
approval, so the worker's done-marker dedupes any double-enqueue.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle

from .config import get_settings
from .models import Approval, ApprovalStatus


def resume_event_id(approval_id: object) -> str:
    """The deterministic idempotency key of an approval's resume turn."""

    return f"approval-{approval_id}-resolved"


def parse_resume_event_id(event_id: str) -> uuid.UUID | None:
    """The inverse of ``resume_event_id``: recover the approval id, or None.

    Strips the ``approval-`` prefix and the ``-resolved`` suffix (the historical
    shared key both the resolve and expiry paths use -- see
    ``build_expiry_resume_turn``) and parses the middle as a UUID. Returns None
    when the shape does not match or the middle is not a valid UUID, so the
    dead-letter scan never mistakes a non-resume graveyard row for an approval.
    """

    if not event_id.startswith("approval-") or not event_id.endswith("-resolved"):
        return None
    middle = event_id[len("approval-") : -len("-resolved")]
    try:
        return uuid.UUID(middle)
    except ValueError:
        return None


def _build_turn(approval: Approval, *, author: str, text: str) -> QueuedTurn:
    """Assemble the resume turn shared by the resolve and expiry paths: same
    deterministic event_id, conversation, reply handle, and timestamp; only the
    author and platform-authored text differ.
    """

    return QueuedTurn(
        event_id=resume_event_id(approval.id),
        conversation_id=approval.conversation_id,
        author=author,
        text=text,
        reply_handle=ReplyHandle(
            channel=approval.reply_channel,
            placeholder=approval.reply_placeholder,
            endpoint=approval.reply_endpoint,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )


def build_resume_turn(approval: Approval) -> QueuedTurn:
    """The turn that continues a suspended session with the human decision.

    The text is platform-authored (not user input): it tells the model what was
    decided, by whom, and to carry on accordingly. The reply handle replays the
    requesting turn's placeholder so the resumed run streams into the same
    message the "awaiting approval" notice was left on.
    """

    decision = approval.status
    note = f" Note: {approval.resolution_note}." if approval.resolution_note else ""
    text = (
        f'[approval resolved] The request "{approval.summary}" was {decision} '
        f"by {approval.resolved_by}.{note} Continue the task accordingly: proceed "
        "with the approved action, or acknowledge the rejection and stop."
    )
    return _build_turn(approval, author=approval.resolved_by or "approver", text=text)


def build_expiry_resume_turn(approval: Approval) -> QueuedTurn:
    """The turn that continues a suspended session after its approval EXPIRED (#412).

    Same shape as ``build_resume_turn`` but platform-authored for an SLA lapse
    with no human decision: the text states the request timed out, that nobody
    approved or rejected it, and that the agent must proceed down its timeout
    branch WITHOUT performing the gated action. ``author`` is "system" because
    no person acted (``resolved_by`` is None on expiry).

    The ``event_id`` reuses ``resume_event_id`` -- the SAME deterministic key
    the resolve path uses. This is by design, so a redelivery of an
    ALREADY-FINISHED turn for this approval is recognized by the worker's
    done-marker and not re-run; the ``-resolved`` suffix in the key is
    historical and must NOT be forked, or that redelivery guard silently
    breaks. This is not a mid-turn dedupe: the single-wakeup guarantee for the
    expiry vs. resolve race comes from the CAS in ``crud.expire_approval``, not
    from this shared key.
    """

    text = (
        f'[approval expired] The request "{approval.summary}" was not approved '
        "or rejected before its deadline and has expired. Do not perform the "
        "gated action. Continue the task down its timeout path: acknowledge the "
        "expiry and proceed or stop accordingly."
    )
    return _build_turn(approval, author="system", text=text)


def resume_turn_for(approval: Approval) -> QueuedTurn:
    """The resume turn owed for a resolved-or-expired approval, by status.

    The single status -> builder mapping, kept beside the builders so a new
    resumable status cannot be added without extending it, and so the one
    status-agnostic caller (the reconciler, #418) stays mapping-free. Raises
    ValueError for a status that owes no wake (pending), mirroring how
    ``crud._RESUMABLE_STATUSES`` fences the reconciler's finder and its per-row
    claim.

    The three inline callers do NOT come through here: each just performed the
    CAS that established its record's status, so it names its builder directly.
    """

    if approval.status == ApprovalStatus.expired:
        return build_expiry_resume_turn(approval)
    if approval.status in (ApprovalStatus.approved, ApprovalStatus.rejected):
        return build_resume_turn(approval)
    raise ValueError(f"approval {approval.id} status {approval.status} owes no resume turn")


class ResumeQueue:
    """Producer half of the resume seam (the worker's runs consumer is the other)."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str | None = None,
        *,
        dead_letter_stream: str | None = None,
    ) -> None:
        # The runs stream is declared once, on the settings object (#492): the
        # module constant this used to default to was a second declaration of
        # the same name. Settings.runs_stream defaults to the shared
        # RUNS_STREAM_DEFAULT and stays overridable via RUNS_STREAM. Resolved at
        # construction rather than as a default arg so the env override is read
        # when the queue is built, not at import time.
        self._client = client
        self._stream = stream if stream is not None else get_settings().runs_stream
        # The graveyard the #532 backstop scans (READ-ONLY here; the API never
        # mutates it). The derivation MUST match the worker's
        # WorkerConfig.dead_letter_stream_name() (`<stream>:dead`). An operator
        # who overrides the worker's CURIE_DEAD_LETTER_STREAM or CURIE_STREAM
        # must set the API's override to match, or the backstop reads the wrong
        # graveyard.
        self._dead_letter_stream = dead_letter_stream or f"{self._stream}:dead"

    async def enqueue(self, turn: QueuedTurn) -> str:
        fields: dict[Any, Any] = {STREAM_PAYLOAD_FIELD: turn.model_dump_json()}
        stream_id = await self._client.xadd(self._stream, fields)
        return stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)

    async def read_dead_letter(self, *, count: int) -> list[tuple[str, dict[str, str]]]:
        """Read up to ``count`` MOST-RECENT graveyard rows, newest-first
        (READ-ONLY; never XDEL/XACK).

        Scans with ``xrevrange`` so a freshly dead-lettered resume turn (which
        lands at the newest end of the graveyard) is seen promptly even when
        older graveyard rows have not yet evicted and the scan cap sits below
        the worker's graveyard MAXLEN.

        Normalizes each entry id and every field key/value from bytes-or-str to
        str (the same bytes-guard ``enqueue`` applies to the xadd id), so the
        caller sees plain strings regardless of the client's decode_responses
        setting.
        """

        entries = await self._client.xrevrange(self._dead_letter_stream, count=count)
        result: list[tuple[str, dict[str, str]]] = []
        for entry_id, fields in entries or []:
            entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
            fields_str = {
                (k.decode() if isinstance(k, bytes) else str(k)): (
                    v.decode() if isinstance(v, bytes) else str(v)
                )
                for k, v in (fields or {}).items()
            }
            result.append((entry_id_str, fields_str))
        return result
