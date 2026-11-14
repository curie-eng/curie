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

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Any, Literal
from typing import Final as TypingFinal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationInfo,
    field_validator,
    model_validator,
)

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


class ToolAccess(StrEnum):
    """Which tools one turn may execute (TOOL-ACCESS-1, TOOL-ACCESS-3).

    Carried as ``QueuedTurn.tool_access`` and ``Event.tool_access``. Null means
    the turn runs exactly as it always has. On a server that advertises it
    under ``TOOL_ACCESS_STATUS_FIELD`` (TOOL-ACCESS-4), ``READ_ONLY`` restricts
    the turn to tools the server explicitly classifies as read-only, denies
    every other tool before it executes, and never requests an approval. A
    worker or server that does not implement the contract decodes the value and
    drops it (TOOL-ACCESS-6).

    An enum, not free text (TOOL-ACCESS-2): an unknown value is refused on the
    wire, never read as null, because reading it as null would run a restricted
    turn unrestricted. A second value is a breaking change, decided on its own.
    """

    READ_ONLY = "read-only"


#: The ``GET /status`` and ``GET /v1/status`` key under which an ACI server
#: lists the ``ToolAccess`` values it enforces (TOOL-ACCESS-4). A server that
#: omits it enforces none, so a consumer must not send it such a turn.
TOOL_ACCESS_STATUS_FIELD: TypingFinal = "tool_access"


#: A server advertises literal boolean true at this status key only after it
#: enforces the channel read contract. Missing, false or malformed means
#: unsupported; a producer must refuse a granted turn on an unsupported server.
#: This prerequisite defines the marker but advertises no enforcement.
CHANNEL_READ_STATUS_FIELD: TypingFinal = "channel_read"

# Advertise literal true only when scoped attachment tools, lifecycle and
# revocation are enforced. A field's presence alone is not runtime support.
ATTACHMENT_ACCESS_STATUS_FIELD: TypingFinal = "attachment_access"


class AttachmentAccessCapability(_AciModel):
    """Platform attachment authority for one admitted logical turn (ADR 0244).

    Only the kernel sends this on authenticated event/steer requests after the
    runner advertises enforcement. The URL is a trusted configured platform
    facade, never a channel or arbitrary download destination. Implementations
    verify the origin independently of HTTP syntax. The capability authorizes
    only catalog membership and requested reads for its agent, canonical thread,
    active logical turn, current sandbox route generation and finite lifetime.

    A steer renews the opener's logical authority without extending file scope.
    Missing/null context clears local authority; a new logical turn replaces
    it. Completion from an older turn must not clear its successor. Terminal,
    reset and route-replacement paths revoke reads, including local-cache hits.
    Provider credentials remain in the worker. Tokens never enter environment,
    model/tool text, subprocesses, logs, transcripts or persisted checkpoints.
    These transport models implement none of those authorization behaviors.
    """

    url: str = Field(min_length=1, strict=True, json_schema_extra={"format": "uri"})
    token: str = Field(min_length=1, strict=True, repr=False)

    @field_validator("url")
    @classmethod
    def _validate_facade_url(cls, value: str) -> str:
        HttpUrl(value)
        return value


class ChannelReadCapability(_AciModel):
    """Dedicated proxy authority for one admitted logical turn under ADR 0100.

    Only the kernel may issue and send it, over authenticated event or steer
    control requests to a server advertising enforcement. The configured URL
    identifies the trusted platform proxy, not a provider endpoint. HTTP URL
    syntax alone establishes no trust; the implementing lanes must verify the
    trusted configuration. The provider credential remains in the platform.

    The signed token binds audience ``channel.read``, agent, deployment, bundle
    grant, logical turn, credential generation and finite expiry. Every read
    requires a live matching turn, current binding and adapter membership, and
    atomic successful page accounting. Renewal preserves the logical turn and
    its budget, revokes the old generation, and never extends channel scope.
    A steer cannot change the original inbound binding or authenticated scope.
    Terminal paths revoke and clear the capability; null on a steer clears it.

    Tokens must stay out of environment variables, hooks, subprocesses, model
    arguments, logs, telemetry, transcripts, checkpoints and persisted state.
    This model declares the transport contract. It issues no token and supplies
    no read operation, authorization, renewal or runtime revocation behavior.
    """

    url: str = Field(min_length=1, strict=True, json_schema_extra={"format": "uri"})
    token: str = Field(min_length=1, strict=True, repr=False)

    @field_validator("url")
    @classmethod
    def _validate_proxy_url(cls, value: str) -> str:
        HttpUrl(value)
        return value


class PublicationContext(_AciModel):
    """API issued authority and observation for one execution's publication read.

    The capability authorizes only the publication precheck endpoint. It must
    stay outside model text, transcripts, workspace files and logs. The queued
    event identifier is a correlation value, not proof of an active turn.
    Observation metadata records the API read at dispatch; consumers must make
    a fresh comparison before handling an empty publication proposal.
    """

    agent_id: UUID
    deployment_id: UUID
    work_item_id: UUID
    execution_request_id: UUID
    runtime_epoch: int = Field(gt=0, strict=True)
    conversation_id: str = Field(min_length=1)
    lineage_id: UUID
    lineage_version: int = Field(gt=0, strict=True)
    expected_head: str = Field(pattern=r"^[0-9a-f]{40}$")
    queued_event_id: str = Field(min_length=1)
    precheck_url: str = Field(min_length=1, json_schema_extra={"format": "uri"})
    capability: str = Field(min_length=1, repr=False)
    observed_title: str
    observed_body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: datetime

    @field_validator("precheck_url")
    @classmethod
    def _validate_precheck_url(cls, value: str) -> str:
        HttpUrl(value)
        return value

    @field_validator("observed_at")
    @classmethod
    def _require_observation_timezone(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("publication observation time must include a timezone")
        return value


class Event(_AciModel):
    """An inbound event delivered into a live session (initial or follow-up).

    ``session_id`` and ``history_ref`` carry conversation-scoped identity to a
    runner after its sandbox is bound. Both remain optional so older producers
    can omit them and tolerant consumers can adopt the additive wire shape.

    ``tool_access`` asks a server to restrict what this turn may execute
    (TOOL-ACCESS-1, TOOL-ACCESS-3); null is an unrestricted turn. It is sent
    only to a server that advertises the value under
    ``TOOL_ACCESS_STATUS_FIELD`` (TOOL-ACCESS-4), because a server that does
    not enforce it ignores it.

    ``memory_token`` is the per-turn signed state credential the runner's
    memory tools present for writes (MEMORY-TOKEN-1). Null means the turn
    carries no write credential. It is never a BootEnv key or an env var, and
    it is sent only on the runner POST (``/v1/event``, ``/v1/steer``;
    MEMORY-TOKEN-2). ``repr=False`` keeps the value out of ``repr(event)`` and
    log ``%r``; a consumer must not place it in env, logs, hook or subprocess
    input, or persisted state (MEMORY-TOKEN-3).

    ``channel_read`` pairs a trusted platform proxy URL with a dedicated
    ephemeral signed credential for one logical turn. Only the kernel sends it
    on authenticated ``/v1/event`` and ``/v1/steer`` requests after the server
    advertises actual enforcement under ``CHANNEL_READ_STATUS_FIELD``. Missing
    or null means no capability, and a steer clears any previous capability.
    It is not a BootEnv or SessionConfig field and reuses no other credential.
    The nested type defines the required scope and lifecycle; these models do
    not advertise enforcement or implement that lifecycle.
    """

    kind: Literal["event"] = "event"
    type: Literal["message", "job", "eval_case"]
    text: str
    user: str
    ts: str
    session_id: str | None = None
    history_ref: str | None = None
    publication_context: PublicationContext | None = None
    tool_access: ToolAccess | None = None  # @spec TOOL-ACCESS-1 TOOL-ACCESS-2
    memory_token: str | None = Field(default=None, repr=False)  # @spec MEMORY-TOKEN-1
    channel_read: ChannelReadCapability | None = Field(default=None, repr=False)
    # Optional for tolerant older readers. Never overload attachments_manifest:
    # references available to fetch are not files already present on disk.
    attachment_access: AttachmentAccessCapability | None = Field(default=None, repr=False)


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

    ``approval_display`` is the optional human sentence a bundle-authored
    permission-gate template rendered (#2565). ``None`` means the card and
    notice use ``approval_summary`` (today's machine string). It is never the
    grant-provenance signal; that stays ``approval_gate_kind`` /
    ``approval_granted_tool``. Additive optional scalar: a tolerant consumer
    decoding an older producer's ``final`` simply sees it absent.

    ``approval_granted_arguments`` carries the canonical JSON object passed to
    the permission gate for the denied call (#3255). It is set only with a
    permission gate's ``approval_granted_tool``; a policy gate carries no tool
    arguments. An older producer omits it, and an empty object is distinct from
    an absent argument carrier. The worker persists this object with the
    approval so the resume boot can bind a later grant to the same call.

    ``input_tokens``/``output_tokens`` carry the turn's model token usage when
    the harness reported it (#390): the runner stamps them from the SDK result's
    ``usage`` so a consumer can attribute a dollar cost to the turn (model
    pricing lives with the consumer, not on the wire). Both ``None`` when usage
    was unavailable -- the fake-model path, a provider that reports no usage, or
    an older runner that predates these fields -- so a consumer computing cost
    leaves it unknown rather than counting the turn as free. Additive optional
    scalars: a tolerant consumer decoding an older producer's ``final`` simply
    sees them absent.

    ``text_mode`` is ``"verbatim"`` only on a ``done`` final whose text a
    bundle-declared validator approved in this turn (ADR 0227). A caller that
    negotiated exact output delivers that text literally, without receipts,
    notices or Markdown rendering. ``None`` keeps the established rendering,
    including for an older producer that predates the field.
    """

    type: Literal["final"] = "final"
    text: str
    status: SessionStatus = SessionStatus.DONE
    approval_summary: str | None = None
    approval_route: str | None = None
    approval_gate_kind: str | None = None
    approval_granted_tool: str | None = None
    approval_granted_arguments: dict[str, Any] | None = None
    approval_display: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    text_mode: Literal["verbatim"] | None = None


class RetryReason(StrEnum):
    """Why a provider asked the platform to wait before another attempt.

    Carried only inside ``RetryCondition``. Each value names the provider's
    signal, not a platform decision: ``rate_limited`` is a request rate limit,
    ``usage_reset`` is a usage allowance that resets at a provider time,
    ``credit_exhausted`` is a spent credit balance, ``credential_rejected`` is
    a credential the provider refused, and ``transient_service`` is a temporary
    provider service failure. These are deliberately not the hyphenated
    ``ErrorEvent.classification`` tokens, and neither vocabulary maps onto the
    other.

    A closed enum, not free text: an unknown value is refused on the wire,
    never read as an absent condition. Adding a value is a breaking change
    under ADR 0036.
    """

    RATE_LIMITED = "rate_limited"
    USAGE_RESET = "usage_reset"
    CREDIT_EXHAUSTED = "credit_exhausted"
    CREDENTIAL_REJECTED = "credential_rejected"
    TRANSIENT_SERVICE = "transient_service"


#: The only wire spelling of ``RetryCondition.scope_key``: an opaque SHA-256
#: digest of a credential scope. ``[0-9a-f]`` (not ``\w``) so the pattern means
#: the same thing in Python, in the exported JSON Schema and in the Rust guard.
RETRY_SCOPE_KEY_PATTERN: TypingFinal = r"^sha256:[0-9a-f]{64}$"

#: The only wire spelling of ``RetryCondition.retry_at``: RFC 3339 with an
#: uppercase ``T``, required seconds, at most six fractional digits and the
#: ``Z`` designator. Matched with ``fullmatch`` so the trailing newline the
#: ``$`` anchor would tolerate in Python is refused, as it is in the Rust guard.
RETRY_AT_PATTERN: TypingFinal = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$"
)

_RETRY_AT_RE = re.compile(RETRY_AT_PATTERN)

# The exported schema branch is ``{"type": "string", "format": "date-time",
# "pattern": RETRY_AT_PATTERN}``; the validators below enforce the same grammar.
_UtcRetryAt = Annotated[datetime, Field(json_schema_extra={"pattern": RETRY_AT_PATTERN})]

# Refusal texts name the rule and never quote the refused value.
_RETRY_AT_GRAMMAR_ERROR = (
    "retry_at must be an RFC 3339 UTC timestamp spelled YYYY-MM-DDTHH:MM:SS[.ffffff]Z"
)
_RETRY_AT_OFFSET_ERROR = "retry_at must be a timezone-aware datetime with a zero UTC offset"
_RETRY_AT_CALENDAR_ERROR = "retry_at names an impossible calendar date or time"


def _parse_retry_at(value: str) -> datetime:
    """Build the UTC instant a grammar-checked ``retry_at`` string names.

    The grammar has already fixed every character position, so plain slicing
    is exact. ``datetime`` refuses impossible calendar values (year zero, a day
    past the end of its month, hour 24, a leap second), which is the same
    calendar check the generated Rust guard applies, so both lanes refuse the
    same strings.
    """

    fraction = value[20:-1]
    try:
        return datetime(
            int(value[0:4]),
            int(value[5:7]),
            int(value[8:10]),
            int(value[11:13]),
            int(value[14:16]),
            int(value[17:19]),
            int(fraction.ljust(6, "0")) if fraction else 0,
            tzinfo=UTC,
        )
    except ValueError:
        raise ValueError(_RETRY_AT_CALENDAR_ERROR) from None


class RetryCondition(_AciModel):
    """Provider-derived evidence that a failed turn should wait before a retry.

    Carried as ``ErrorEvent.retry_condition``. A missing or null carrier means
    no trusted retry condition, and a consumer must never read that as retry
    now. A present carrier is evidence for a wait. It is never authorization,
    an approval, an increased limit, or permission to replay an effect: the
    no-retry-after-side-effects rule and every other gate still apply.

    ``reason`` is the closed ``RetryReason`` the producer derived from the
    provider's own signal. Nothing infers a reset from
    ``ErrorEvent.classification``; a classification token alone never creates
    a retry condition.

    ``retry_at`` is optional. When present it is an absolute, provider-derived
    UTC instant spelled as ``RETRY_AT_PATTERN`` describes. Naive values,
    numeric offsets (``+00:00`` included), unix numbers and every other
    spelling are refused rather than coerced, so every lane reads the same
    instant. A past instant is valid; consumers judge time themselves. The
    scheduler adds its own separate 60 second reset grace; the producer reports
    the provider's time without it. A Python ``datetime`` is accepted only with
    a zero UTC offset and is normalized to ``UTC``, so every Python producer
    writes the ``Z`` form. Python also normalizes the fraction spelling on
    re-encode (``.5Z`` becomes ``.500000Z`` and a zero fraction is dropped)
    while the generated Rust keeps the received string; both name the same
    instant.

    ``scope_key`` is optional. When present it is an opaque digest of the
    credential scope the condition applies to, ``sha256:`` followed by 64
    lowercase hex digits (``RETRY_SCOPE_KEY_PATTERN``). It is never a
    credential value, an account name or a provider message. The grammar check
    is the only guard on it and is not a secret detector, so a producer must
    derive the value from a digest rather than rely on validation to catch a
    leak.
    """

    reason: RetryReason
    retry_at: _UtcRetryAt | None = None
    scope_key: str | None = Field(default=None, pattern=RETRY_SCOPE_KEY_PATTERN)

    @field_validator("retry_at", mode="before")
    @classmethod
    def _require_utc_retry_at(cls, value: object) -> datetime | None:
        # Runs before pydantic's lax datetime parsing, which would otherwise
        # accept unix numbers, a space separator, lowercase designators,
        # missing seconds, naive values and silently truncated fractions.
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.utcoffset() != timedelta(0):
                raise ValueError(_RETRY_AT_OFFSET_ERROR)
            return value
        if isinstance(value, str) and _RETRY_AT_RE.fullmatch(value):
            return _parse_retry_at(value)
        raise ValueError(_RETRY_AT_GRAMMAR_ERROR)

    @field_validator("retry_at")
    @classmethod
    def _normalize_retry_at_to_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else value.astimezone(UTC)


class ErrorEvent(_OutboundBase):
    """A classified failure surfaced to the platform.

    ``message`` and ``classification`` keep their existing meaning, and the
    classification token vocabulary is unchanged.

    ``retry_condition`` is optional provider-derived evidence for a wait (see
    ``RetryCondition``). Missing or null means no trusted retry condition, never
    retry now. A malformed present carrier fails the whole frame on decode
    rather than degrading to null. A reader that predates the field ignores it.
    """

    type: Literal["error"] = "error"
    message: str
    classification: str | None = None
    retry_condition: RetryCondition | None = None


class SideEffectFlag(_OutboundBase):
    """Marks that a non-idempotent tool call executed during the run.

    Its presence gates the no-retry-after-side-effects rule (section 2b): a
    failed run carrying this flag escalates to a human instead of retrying.

    It also reports WHAT the call did (ADR-0117). The information was already in
    hand where the frame is built and was being replaced by a constant ``detail``
    string, so a consumer could know that something mutated and never what. Every
    field below is optional with a ``None`` default, which is why carrying them
    is a patch under ADR-0036: a reader that predates them decodes the frame
    unchanged.

    One CALL produces two frames -- an opening one when the call is made, so the
    no-retry rule latches even if the turn dies mid-call, and a closing one
    carrying what came back. ``call_id`` is what joins them into one record; a
    turn that calls the same tool twice is otherwise indistinguishable from one
    that called it once.
    """

    type: Literal["side_effect_flag"] = "side_effect_flag"
    tool: str | None = None
    detail: str | None = None
    # The harness's own id for the call. The join key, not a display value.
    call_id: str | None = None
    # What the call was made with, and what it answered. ``result`` is present
    # only for a structured reply: a connector that answers in prose has none,
    # deliberately, because guessing structure out of a sentence is how something
    # downstream restores a guess.
    arguments: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    # Whether the tool reported failure. ``None`` means unknown (an opening
    # frame, or a producer that predates this). A record is undoable only on a
    # successful outcome, so the outcome has to travel with the result.
    failed: bool | None = None
    # Whether outbound redaction replaced anything inside ``result``. A scrubbed
    # snapshot is no longer what the connector reported, and replaying it would
    # write a placeholder over the live value, so a consumer must not treat a
    # redacted ``result`` as restorable state (#1873). ``None`` means nothing was
    # replaced or the producer predates this field.
    redacted: bool | None = None


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
