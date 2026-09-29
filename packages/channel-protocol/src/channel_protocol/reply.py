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

from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    StringConstraints,
    model_serializer,
    model_validator,
)

from .models import OutboundMessage
from .progress import ProgressCard, ProgressMilestone

ReplyWireVersion = Literal["1.0", "1.1"]
REPLY_WIRE_VERSION: Literal["1.0"] = "1.0"
PROGRESS_REPLY_WIRE_VERSION: Literal["1.1"] = "1.1"
_STRICT = ConfigDict(extra="forbid")

DeliveryId = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
"""A UUID in its canonical form, the one ``str(uuid.UUID(...))`` produces."""

# The fields 1.1 added. Absent, each is left out of the serialized body rather
# than sent as null: a 1.0-built adapter's closed models refuse an unknown key
# even when its value is null.
_PROGRESS_WIRE_FIELDS = ("delivery_id", "progress")
_DELIVERY_ID_DESCRIPTION = (
    "Reply wire 1.1. The platform-minted idempotency key of this one post or edit; "
    "a retry of an ambiguous attempt carries the same value. Outbound, and unrelated "
    "to the inbound delivery_id an adapter sends to POST /channels/turns."
)


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


class _DeliveredEvent(_ReplyEventBase):
    """The shared 1.1 rules of the two events that carry a delivery identity.

    The fields themselves are declared on each subclass, after every 1.0 field,
    because a field declared here would be serialized ahead of ``event``.
    """

    def _progress_wire(self) -> tuple[str | None, ProgressCard | ProgressMilestone | None]:
        """This body's ``(delivery_id, progress)``."""
        raise NotImplementedError

    @model_validator(mode="after")
    def _version_names_the_wire_the_body_uses(self) -> Self:
        delivery_id, progress = self._progress_wire()
        if self.version == REPLY_WIRE_VERSION:
            if delivery_id is not None or progress is not None:
                raise ValueError(
                    "delivery_id and progress need reply wire 1.1; a 1.0 body carries neither"
                )
        elif delivery_id is None:
            if progress is not None:
                raise ValueError("progress needs a delivery_id")
            raise ValueError(
                "a reply wire 1.1 body carries a delivery_id; send a body without one as 1.0"
            )
        return self

    @model_serializer(mode="wrap")
    def _omit_absent_progress_wire_fields(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        body: dict[str, Any] = handler(self)
        for name in _PROGRESS_WIRE_FIELDS:
            if getattr(self, name) is None:
                body.pop(name, None)
        return body


class TurnStatus(_ReplyEventBase):
    """The channel's liveness caption. An EMPTY status is the clear."""

    version: Literal["1.0"]
    event: Literal["turn.status"]
    status: str


class ReplyUpdate(_DeliveredEvent):
    """The turn's reply, edited in place where the channel supports it.

    ``text`` is a streamed or final reply. ``message`` plus ``settled`` is the
    other form: an already-posted platform message being settled (an approval
    card that expired or was resolved). ``progress`` is the third, 1.1 only: an
    edit of the progress card ``target.reply_ref`` names, carrying none of the
    answer fields.
    """

    event: Literal["reply.update"]
    text: str | None = None
    message: OutboundMessage | None = None
    settled: SettledOutcome | None = None
    nav: NavAffordance | None = None
    delivery_id: DeliveryId | None = Field(default=None, description=_DELIVERY_ID_DESCRIPTION)
    progress: ProgressCard | None = Field(
        default=None,
        description=(
            "Reply wire 1.1. An edit of the progress card target.reply_ref names; "
            "the body then carries no text, message, settled or nav."
        ),
    )

    def _progress_wire(self) -> tuple[str | None, ProgressCard | None]:
        return self.delivery_id, self.progress

    @model_validator(mode="after")
    def _progress_is_not_an_answer(self) -> Self:
        if self.progress is not None and any(
            value is not None for value in (self.text, self.message, self.settled, self.nav)
        ):
            raise ValueError(
                "a progress update carries no text, message, settled or nav: a card "
                "edit is never answer text or an approval settlement"
            )
        return self


class ReplyPost(_DeliveredEvent):
    """A NEW platform-owned message, acked with its ref.

    The approval card, or, 1.1 only, a progress card's first revision or a
    milestone. ``message.text`` is the complete plain-text fallback either way.
    """

    event: Literal["reply.post"]
    message: OutboundMessage
    requested_by: str
    delivery_id: DeliveryId | None = Field(default=None, description=_DELIVERY_ID_DESCRIPTION)
    progress: Annotated[ProgressCard | ProgressMilestone, Field(discriminator="kind")] | None = (
        Field(
            default=None,
            description=(
                "Reply wire 1.1. A progress card's first revision or a milestone; "
                "message.text is its plain-text fallback and carries no interaction."
            ),
        )
    )

    def _progress_wire(self) -> tuple[str | None, ProgressCard | ProgressMilestone | None]:
        return self.delivery_id, self.progress

    @model_validator(mode="after")
    def _progress_is_not_actionable(self) -> Self:
        if self.progress is not None and self.message.interaction is not None:
            raise ValueError(
                "a progress post's message carries no interaction: the approval "
                "card stays the only actionable platform message"
            )
        return self


class TurnCompleted(_ReplyEventBase):
    """The turn reached a terminal outcome. The adapter's delivery trigger.

    ``event_id`` is the dedupe key: at-least-once delivery means a duplicate is
    unavoidable, so it is made identifiable rather than pretended away.
    """

    version: Literal["1.0"]
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
