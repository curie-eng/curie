"""ACI channel events: inbound messages and outbound NDJSON response events.

Mirrors the ACI contract v0.1 (docs/reference/detailed-architecture.md section 0):

    CHANNEL (while claimed):
      -> event      {type: message|job|eval_case, text, user, ts}
      -> interrupt  {reason}
      <- response   NDJSON: {type: text_delta|tool_note|final|error|side_effect_flag, ...}

Inbound messages are modelled as a discriminated union on a ``kind`` tag so a
single control channel can carry both event and interrupt frames self
describingly. Outbound events are a discriminated union on ``type``; every
outbound event carries a ``version`` equal to PROTOCOL_VERSION.
"""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator

from .version import PROTOCOL_VERSION, SEMVER_PATTERN

# Reader-context flag. The decoder passes ``context={_READER_CONTEXT_KEY: True}``
# so a consumer decoding the wire tolerates unknown fields; direct construction
# does not, so a producer building a model with a stray field is rejected. It
# lives here (not in a new module) because every wire model shares it and the
# tests import it from ``aci_protocol.events``.
_READER_CONTEXT_KEY = "aci_reader"

# The one reader context every sanctioned consumer decode threads. Public so a
# consuming lane whose decode cannot go through a ``parse_*`` helper (FastAPI
# validates a request body itself) can thread the same flag instead of inventing
# a second tolerance mechanism.
READER_CONTEXT: Mapping[str, bool] = MappingProxyType({_READER_CONTEXT_KEY: True})


class _AciModel(BaseModel):
    """Base for every ACI wire model: strict producers, tolerant consumers.

    ``extra="ignore"`` drops unknown keys, but the before-validator rejects them
    on construction UNLESS the caller passes the reader context flag. So a
    producer that builds an event with a field the contract does not define is
    caught at the source, while a consumer decoding a newer producer's payload
    ignores fields it does not model. Pydantic propagates the validation context
    into nested models, so a nested model (``ReplyHandle`` inside ``QueuedTurn``)
    gets the same tolerant read without threading the flag by hand.
    """

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_keys_on_construction(cls, data: Any, info: ValidationInfo) -> Any:
        # Aliases are not used anywhere in aci-protocol, so comparing raw keys
        # against ``model_fields`` is exact. If an alias is ever added here, this
        # would need to compare against alias-aware keys instead.
        if isinstance(data, dict) and not (info.context or {}).get(_READER_CONTEXT_KEY):
            unknown = data.keys() - cls.model_fields.keys()
            if unknown:
                raise ValueError(
                    f"unexpected field(s) {sorted(unknown)}; the ACI wire is strict on "
                    "construction (a consumer decoding the wire tolerates them, a producer "
                    "does not)"
                )
        return data


class SessionStatus(StrEnum):
    """Terminal or awaiting status of a session, from the output contract.

    Wire tokens follow the section 0 spelling; ``classified failure`` in prose
    becomes the token ``classified-failure`` on the wire.
    """

    DONE = "done"
    IDLE_AWAITING_INPUT = "idle-awaiting-input"
    CLASSIFIED_FAILURE = "classified-failure"
    # The turn ended pending a human decision (ADR-0010, epic #22): the platform
    # suspends the session on this status and resumes it when the durable
    # approval record is resolved. Additive value; consumers that only handle
    # the original three still parse every pre-existing payload.
    AWAITING_APPROVAL = "awaiting-approval"


# --- Inbound channel messages -------------------------------------------------


class Event(_AciModel):
    """An inbound event delivered into a live session (initial or follow-up)."""

    kind: Literal["event"] = "event"
    type: Literal["message", "job", "eval_case"]
    text: str
    user: str
    ts: str


class Interrupt(_AciModel):
    """A hard stop delivered on the control channel, distinct from a steer."""

    kind: Literal["interrupt"] = "interrupt"
    reason: str


InboundMessage = Annotated[Event | Interrupt, Field(discriminator="kind")]


# --- Outbound NDJSON response events ------------------------------------------


class _OutboundBase(_AciModel):
    # ``version`` is a semver-constrained string (not a Literal const): the wire
    # accepts any compatible version, so pinning it to one value would defeat the
    # compatibility range. The NDJSON decoder enforces compatibility; the pattern
    # here rejects a structurally malformed value on construction.
    version: str = Field(default=PROTOCOL_VERSION, pattern=SEMVER_PATTERN)


class TextDelta(_OutboundBase):
    """A streamed chunk of assistant text."""

    type: Literal["text_delta"] = "text_delta"
    text: str


class ToolNote(_OutboundBase):
    """A human readable note about a tool call the harness is making."""

    type: Literal["tool_note"] = "tool_note"
    text: str
    tool: str | None = None


class Final(_OutboundBase):
    """The terminal response event, carrying the session status.

    ``approval_summary`` accompanies an ``awaiting-approval`` status (ADR-0010):
    the human-readable statement of what needs approval, captured from the
    run's approval request so the platform can persist it on the durable
    ``Approval`` record and show it to the approver. ``approval_route`` names
    the approval route the request targets (#247): declared in the bundle
    manifest's ``approvalPolicy`` (versioned with the agent), bound to a
    workspace channel per deployment by the worker. Both ``None`` on every
    other status; ``approval_route`` also ``None`` when the request named no
    route (the platform falls back to the requesting channel).

    ``approval_gate_kind`` records which gate produced the request (#544):
    ``'permission'`` when the runner's tool-permission gate denied a real tool
    call, ``'policy'`` when the model asked for a business-decision approval.
    It is the durable provenance the worker branches on instead of sniffing the
    summary prefix. ``approval_granted_tool`` carries the tool name the
    permission gate denied -- the trusted ``can_use_tool`` value the resume-turn
    grant is bound to -- and is only ever set for ``approval_gate_kind =
    'permission'``; a policy gate never authorizes a tool, so it stays ``None``.
    Both ``None`` on every other status, and ``None`` from an older runner that
    predates these fields (the worker falls back to the prefix parse then).

    ``input_tokens``/``output_tokens`` carry the turn's model token usage when
    the harness reported it (#390): the runner stamps them from the SDK result's
    ``usage`` so a consumer can attribute a dollar cost to the turn (model
    pricing lives with the consumer, not on the wire). Both ``None`` when usage
    was unavailable -- the fake-model path, a provider that reports no usage, or
    an older runner that predates these fields -- so a consumer computing cost
    leaves it unknown rather than counting the turn as free. Additive optional
    scalars: a tolerant consumer decoding an older producer's ``final`` simply
    sees them absent.
    """

    type: Literal["final"] = "final"
    text: str
    status: SessionStatus = SessionStatus.DONE
    approval_summary: str | None = None
    approval_route: str | None = None
    approval_gate_kind: str | None = None
    approval_granted_tool: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None


class ErrorEvent(_OutboundBase):
    """A classified failure surfaced to the platform."""

    type: Literal["error"] = "error"
    message: str
    classification: str | None = None


class SideEffectFlag(_OutboundBase):
    """Marks that a non-idempotent tool call executed during the run.

    Its presence gates the no-retry-after-side-effects rule (section 2b): a
    failed run carrying this flag escalates to a human instead of retrying.
    """

    type: Literal["side_effect_flag"] = "side_effect_flag"
    tool: str | None = None
    detail: str | None = None
    # The call's arguments and the tool's structured reply. Both are optional and
    # default to None, so a reader that predates them is unaffected (ADR-0036).
    # They exist because the information is already in hand when this frame is
    # built and was previously replaced by a constant string: without them a
    # consumer can know that something mutated but never what, which is the gap
    # an action ledger has to close.
    arguments: dict[str, Any] | None = None
    result: dict[str, Any] | None = None


OutboundEvent = Annotated[
    TextDelta | ToolNote | Final | ErrorEvent | SideEffectFlag,
    Field(discriminator="type"),
]

# The concrete outbound model classes, in a fixed order used by schema and code
# generation so the committed artifacts are deterministic.
OUTBOUND_EVENT_TYPES: tuple[type[_OutboundBase], ...] = (
    TextDelta,
    ToolNote,
    Final,
    ErrorEvent,
    SideEffectFlag,
)
