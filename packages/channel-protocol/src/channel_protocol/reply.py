"""The neutral reply wire: what the platform sends a channel adapter (ADR-0096).

Four versioned events -- ``turn.status``, ``reply.update``, ``reply.post`` and
``turn.completed`` -- carried as a discriminated union so an adapter switches on
one field instead of trying each model in turn. Every event names its
``ReplyTarget`` (kind, address, conversation, opaque reply ref) and NOTHING about
where the platform is delivering it: the endpoint and the egress-credential
selector travel as a transport-level argument, never as a wire field, so a
published body can never tell an adapter (or anyone who reads one) where the
platform's authenticated egress points.

Two contracts an adapter must build against:

- **Delivery is AT-LEAST-ONCE.** The worker's completion outbox is durable and
  separately retryable, so a confirmed-but-uncleared record is re-emitted. Every
  duplicate carries the SAME ``event_id``.
- **``turn.completed`` is idempotent per ``event_id``, and may arrive for a
  conversation the adapter already considers finished** (a redelivery of an
  earlier terminal turn, or a sweeper draining a record after an outage). An
  adapter MUST dedupe on ``TurnCompleted.event_id`` -- on a best-effort in-memory
  basis at minimum, which is the conformance floor. Durable dedupe across a
  restart is the adapter's own quality bar and cannot be enforced by the
  platform: an in-memory-only adapter can double-send after a restart.

``reply_ref`` is OPAQUE and adapter-minted: Slack's is the placeholder ``ts``,
email's is the upstream message id. The platform never parses it.

Two wire versions, and a body carries the one it needs (ADR-0130 section 4):

- **1.0** (``REPLY_WIRE_VERSION``) is every form above. The models are closed,
  so an adapter built against 1.0 refuses any body carrying a field it does not
  model. Every 1.0 body therefore serializes exactly as it did before 1.1
  existed: an absent 1.1 field is omitted from the body, never sent as null.
- **1.1** (``PROGRESS_REPLY_WIRE_VERSION``) adds two optional fields to
  ``reply.update`` and ``reply.post`` and changes nothing else.
  ``delivery_id`` is a canonical lowercase UUID the platform mints for one
  externally visible operation (one post, or one edit of a posted message), and
  it is the adapter's idempotency key for that operation: a retry of an
  ambiguous attempt carries the same ``delivery_id``, so the adapter adopts the
  earlier result instead of posting twice. It flows OUTBOUND, platform to
  adapter, and is unrelated to the inbound ``delivery_id`` an adapter sends to
  ``POST /channels/turns`` to name its own upstream message. ``progress`` is
  the rendering-free progress payload from ``channel_protocol.progress``: a
  ``ProgressCard`` on ``reply.update`` or ``reply.post``, or a
  ``ProgressMilestone`` on ``reply.post``.

The version is ``"1.1"`` exactly when the body carries ``delivery_id``, and a
body carrying ``progress`` carries ``delivery_id``. ``turn.status`` and
``turn.completed`` have no 1.1 field and are 1.0 only. A body that breaks either
rule is refused at validation, so no producer can put a 1.1 version on a body a
1.0 adapter could otherwise have read, or a 1.1 field on a body labelled 1.0.

A progress body is never an answer (ADR-0130 section 5). A ``reply.update``
carrying ``progress`` carries no ``text``, ``message``, ``settled`` or ``nav``,
so an adapter that switches on ``progress`` first never mistakes a card edit for
answer text. A ``reply.post`` carrying ``progress`` still carries its
``message``, whose ``text`` is the mandatory plain-text fallback, and that
message has no ``interaction``: the approval card remains the only actionable
platform message (ADR-0130 section 3).
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .models import OutboundMessage

ReplyWireVersion = Literal["1.0"]
REPLY_WIRE_VERSION: ReplyWireVersion = "1.0"
_STRICT = ConfigDict(extra="forbid")


class ReplyTarget(BaseModel):
    """Where a reply belongs, in the channel's own terms.

    ``conversation_id`` is None for a message that belongs to no conversation
    (a policy-routed approval card posted top-level in a channel that never
    asked). ``reply_ref`` is None when the channel has no addressable handle to
    edit yet.
    """

    model_config = _STRICT

    kind: str
    address: str
    conversation_id: str | None
    reply_ref: str | None


class NavAffordance(BaseModel):
    """The no-dead-ends way back, in wire terms.

    The worker-local ``NavPack`` cannot cross into this package without
    inverting the dependency, so the worker's sink layer maps an ENABLED pack to
    this and a disabled one to ``None``: absence is the disabled form, and there
    is no dead affordance on the wire.
    """

    model_config = _STRICT

    label: str
    command: str


class SettledOutcome(BaseModel):
    """How an approval ended, for the settled-card render.

    ``decision`` is None when nobody decided, which is the expiry case; a
    non-None value is the resolved case and brings ``resolver`` with it.
    """

    model_config = _STRICT

    requested_by: str
    decision: str | None = None
    resolver: str | None = None
    note: str | None = None


class _ReplyEventBase(BaseModel):
    model_config = _STRICT

    version: ReplyWireVersion
    target: ReplyTarget


class TurnStatus(_ReplyEventBase):
    """The channel's liveness caption. An EMPTY status is the clear."""

    event: Literal["turn.status"]
    status: str


class ReplyUpdate(_ReplyEventBase):
    """The turn's reply, edited in place where the channel supports it.

    ``text`` is a streamed or final reply. ``message`` plus ``settled`` is the
    other form: an already-posted platform message being settled (an approval
    card that expired or was resolved).
    """

    event: Literal["reply.update"]
    text: str | None = None
    message: OutboundMessage | None = None
    settled: SettledOutcome | None = None
    nav: NavAffordance | None = None


class ReplyPost(_ReplyEventBase):
    """A NEW platform-owned message (the approval card), acked with its ref."""

    event: Literal["reply.post"]
    message: OutboundMessage
    requested_by: str


class TurnCompleted(_ReplyEventBase):
    """The turn reached a terminal outcome. The adapter's delivery trigger.

    ``event_id`` is the dedupe key: at-least-once delivery means a duplicate is
    unavoidable, so it is made identifiable rather than pretended away.
    """

    event: Literal["turn.completed"]
    event_id: str
    outcome: Literal["delivered", "dropped", "escalated", "awaiting-approval"]


ReplyEvent = Annotated[
    TurnStatus | ReplyUpdate | ReplyPost | TurnCompleted,
    Field(discriminator="event"),
]


class ReplyAck(BaseModel):
    """What an adapter answers with. ``ref`` is None when it mints no handle."""

    model_config = _STRICT

    ref: str | None = None
