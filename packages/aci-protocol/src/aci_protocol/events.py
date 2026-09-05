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

import json
from collections.abc import Callable, Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
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


# Opaque per-conversation runner credential. Character cap, not a product
# secret format: long enough for a typical bearer, short enough to reject a
# dump. Realizing servers must not echo this value in errors, traces, or prompts.
ADOPTION_CREDENTIAL_MAX_CHARS = 4096


def _malformed_adoption_credential() -> ValueError:
    """Reject without attaching the presented material to the exception."""

    return ValueError("malformed adoption credential")


def admit_bounded_credential(
    value: Any, *, max_chars: int, error: Callable[[], ValueError]
) -> str | None:
    """Admit an optional opaque credential, or raise ``error()`` without echoing it.

    The one admission rule every credential-bearing ACI field shares
    (``Event.adoption_credential``, ``BootEnv.runner_bootstrap_token``):
    ``None`` is "not presented"; any other well-formed value is a non-empty
    string no longer than ``max_chars`` that is not whitespace-only. The
    presented material is never copied onto the raised error, so a caller that
    interpolates the exception into an HTTP body or a log line cannot leak it
    from here. Control characters are deliberately not policed at this layer;
    a realizing consumer compares the value and fails closed on a mismatch.
    """

    if value is None:
        return None
    if not isinstance(value, str):
        raise error()
    if not value or value.strip() == "" or len(value) > max_chars:
        raise error()
    return value


def parse_adoption_credential(value: Any) -> str | None:
    """Admit an optional adoption credential, or raise without echoing it.

    ``None`` is the legacy cold path (omit or JSON null). Any other well-formed
    value is a non-empty string no longer than ``ADOPTION_CREDENTIAL_MAX_CHARS``
    that is not whitespace-only. The presented material is never copied onto
    the raised error; a realizing server that interpolates the exception into
    an HTTP body therefore cannot leak it from this helper.
    """

    return admit_bounded_credential(
        value, max_chars=ADOPTION_CREDENTIAL_MAX_CHARS, error=_malformed_adoption_credential
    )


def _malformed_credential_error(title: str, field: str, message: str) -> ValidationError:
    """A material-free ValidationError naming only the field and a fixed message."""

    return ValidationError.from_exception_data(
        title,
        [
            {
                "type": "value_error",
                "loc": (field,),
                "input": None,
                "ctx": {"error": message},
            }
        ],
    )


def _malformed_event_error() -> ValidationError:
    """A credential-free ValidationError for a bad adoption_credential."""

    return _malformed_credential_error(
        "Event", "adoption_credential", "malformed adoption credential"
    )


def _scrub_credential_input(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return {
            key: None if key == field else _scrub_credential_input(item, field)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_scrub_credential_input(item, field) for item in value]
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return "<redacted>"
        return "<redacted>" if field in text else value
    if isinstance(value, str) and field in value:
        return "<redacted>"
    return value


def redact_credential_error(
    exc: ValidationError, *, title: str, field: str, message: str
) -> ValidationError:
    """Return a ValidationError whose inputs cannot contain ``field``'s material.

    Errors whose loc is the credential field collapse to a closed
    malformed-credential failure (``title``/``field``/``message``). Unrelated
    failures (a missing ``text``, for example) keep their diagnosis; only the
    credential material is scrubbed from their inputs. A ``json_invalid`` error
    keeps its parser diagnosis but loses its input entirely, because unparsed
    raw JSON cannot be scrubbed by field name.
    """

    errors = exc.errors()
    credential_failed = False
    for err in errors:
        loc = err.get("loc", ())
        loc_text = ".".join(str(part) for part in loc) if isinstance(loc, tuple) else ""
        msg = str(err.get("msg", ""))
        if field in loc_text or msg == message:
            credential_failed = True
            break
    if credential_failed:
        return _malformed_credential_error(title, field, message)
    scrubbed: list[Any] = []
    for err in errors:
        input_value = err.get("input")
        item = dict(err)
        if err.get("type") == "json_invalid":
            # Raw JSON that never parsed: pydantic attaches the WHOLE input, and
            # no field-name match can locate a credential inside it (an escaped
            # key such as ``"adoption_credenti\u0061l"`` defeats a literal
            # scrub). Drop the raw input outright; the parser's position-only
            # message keeps the invalid-JSON diagnosis truthful.
            item["input"] = "<redacted>"
            scrubbed.append(item)
            continue
        if isinstance(input_value, str) and field in input_value:
            return _malformed_credential_error(title, field, message)
        item["input"] = _scrub_credential_input(input_value, field)
        scrubbed.append(item)
    try:
        return ValidationError.from_exception_data(exc.title, scrubbed)
    except (TypeError, ValueError):
        return _malformed_credential_error(title, field, message)


def redact_adoption_credential_error(exc: ValidationError) -> ValidationError:
    """``redact_credential_error`` bound to ``Event.adoption_credential``."""

    return redact_credential_error(
        exc,
        title="Event",
        field="adoption_credential",
        message="malformed adoption credential",
    )


class Event(_AciModel):
    """An inbound event delivered into a live session (initial or follow-up).

    ``session_id`` and ``history_ref`` carry conversation-scoped identity to a
    runner after its sandbox is bound. Both remain optional so older producers
    can omit them and tolerant consumers can adopt the additive wire shape.

    ``adoption_credential`` is the optional per-conversation runner credential
    ADR-0122 delivers on this same authenticated ``Event`` (never a new
    lifecycle route, and never by overloading ``text`` / ``user`` / ``ts`` /
    ``history_ref``). Omitted or JSON ``null`` is the legacy cold path: no
    adoption is requested and a current credential must stay in force. A
    malformed value is a hard decode error; the model is not constructed, so
    there is no partial adoption at the wire layer. The field is secret
    material: it is omitted from ``repr`` / ``str``, and decode errors must
    not echo it.

    This field does not retire a bootstrap token, persist a route, or enable a
    warm pool. Those are realizing-runner/worker behaviors. Because inbound
    ``Event`` carries no ``version`` and a tolerant consumer ignores unknown
    keys, a successful turn is not proof of adoption. A consumer that applies
    the credential MUST set ``adoption_applied=True`` on the outbound frames of
    that turn; a producer that sent a credential MUST treat a missing or
    non-true ack as "not adopted".
    """

    kind: Literal["event"] = "event"
    type: Literal["message", "job", "eval_case"]
    text: str
    user: str
    ts: str
    session_id: str | None = None
    history_ref: str | None = None
    adoption_credential: str | None = Field(
        default=None,
        repr=False,
        min_length=1,
        max_length=ADOPTION_CREDENTIAL_MAX_CHARS,
        pattern=r".*\S.*",
    )

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        **kwargs: Any,
    ) -> "Event":
        # Raise outside the handler: ``from None`` hides traceback display but
        # leaves the original raw-input error reachable through ``__context__``.
        redacted: ValidationError | None = None
        try:
            return super().model_validate_json(json_data, **kwargs)
        except ValidationError as exc:
            redacted = redact_adoption_credential_error(exc)
        except json.JSONDecodeError:
            redacted = _malformed_event_error()
        raise redacted

    @model_validator(mode="wrap")
    @classmethod
    def _admit_adoption_credential(cls, data: Any, handler: Any) -> Any:
        # Validate and (on failure) drop the presented value before pydantic
        # records ``input_value``. The wrap path is what keeps HTTP 400 bodies
        # that interpolate ``ValidationError`` from echoing the credential.
        if isinstance(data, Mapping) and "adoption_credential" in data:
            incoming = dict(data)
            raw = incoming.pop("adoption_credential")
            try:
                incoming["adoption_credential"] = parse_adoption_credential(raw)
            except ValueError:
                raise _malformed_event_error() from None
            data = incoming
        try:
            return handler(data)
        except ValidationError as exc:
            raise redact_adoption_credential_error(exc) from None


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
    # Ack that the consumer applied ``Event.adoption_credential`` for this turn.
    # ``None`` (omitted or JSON null) is the pre-ack shape: either the consumer
    # predates the field, or no adoption was requested. ``True`` means the
    # credential was installed and the bootstrap must no longer be accepted
    # against that pod. ``False`` means the consumer understood the field and
    # did not apply it. A producer that sent a credential must require
    # ``True``; a missing ack is not successful adoption.
    adoption_applied: bool | None = None

    @field_validator("adoption_applied", mode="before")
    @classmethod
    def _strict_adoption_applied(cls, value: Any) -> bool | None:
        # JSON true/false only. Coercing "true"/1 to True would let a malformed
        # ack look like successful adoption.
        if value is None:
            return None
        if value is True or value is False:
            return value
        raise ValueError("adoption_applied must be a JSON boolean")


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
