"""The typed tracker result proposal a triage run submits (ADR 0277 decision 4, #5585).

A tracker result run ends in exactly one of two shapes. This module freezes
them for the later stages (#5586 adds the runner result tool that submits one,
#5587 the first bundle that produces one, #5588 the handoff that reads a
delivered ticket). Nothing in production imports this module yet.

Proposal shape. Every model is strict, frozen and refuses unknown fields.

``ticket``:

1. ``kind``: the string ``ticket``.
2. ``title``: 1 to 256 characters.
3. ``body``: 1 to 60,000 characters.
4. ``reproduction``: an object with ``command`` (1 to 2,000 characters),
   ``observed`` (1 to 4,000 characters) and ``revision`` (40 lowercase hex
   characters, the investigated commit).

``findings``:

1. ``kind``: the string ``findings``.
2. ``reason``: one of ``not_reproduced``, ``already_fixed``,
   ``product_decision_needed``, ``capability_unavailable``.
3. ``tried``: 1 to 4,000 characters.
4. ``missing``: 1 to 4,000 characters.
5. ``revision``: optional, 40 lowercase hex characters.

Lengths are Python character counts. A text field also refuses a value with
no non-whitespace character and a value holding NUL (Postgres JSONB cannot
store it, and the transport artifact is JSONB). A proposal never carries a
repository, label, agent, assignee or destination: those come from the grant
(`curie_api.factory_tracker_result_delivery`), so each such key is an unknown
field.

The stored reason vocabulary (`FindingsReason`) has a fifth value,
``budget_exhausted``, that only the platform produces (`PlatformFindings`,
for #5586). A submitted proposal carrying it is refused on ``reason`` with
code ``value``.

Refusals. `parse_proposal` raises `ProposalInvalid`, whose ``issues`` are
(field path, code) pairs and nothing else. Field paths are dotted
(``reproduction.revision``; ``""`` for the top level). Only schema-owned
field names survive in a path: any key the submitter invented is reported as
the fixed placeholder ``*`` (``reproduction.*``), so no submitted text, key or
value, can reach an exception, a log line or a row. Codes (`PROPOSAL_ISSUE_CODES`):

1. ``not_object``: the proposal is not a JSON object.
2. ``kind``: ``kind`` is missing or not ``ticket`` or ``findings``.
3. ``unknown_field``: a key the shape does not have.
4. ``missing_field``: a required key is absent.
5. ``type``: a value of the wrong JSON type.
6. ``length``: a text outside its bounds.
7. ``text``: a text that is blank or holds NUL.
8. ``value``: a value outside its vocabulary (``reason``).
9. ``revision``: a revision that is not 40 lowercase hex characters.

Digest. `proposal_sha256` is the SHA-256 of the canonical JSON
(`curie_api.tracker_result_declaration.canonical_json`: keys sorted, UTF-8,
no insignificant whitespace) of ``model_dump(mode="json", exclude_none=True)``,
so an omitted optional ``revision`` and an explicit null hash the same.
``tests/vectors/tracker-result-proposal.json`` freezes valid inputs with
their digests and invalid inputs with their expected issues for every reader.
"""

from __future__ import annotations

import enum
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Strict,
    StringConstraints,
    ValidationError,
)
from pydantic_core import PydanticCustomError

from curie_api.tracker_result_declaration import canonical_json


class SubmittedReason(enum.StrEnum):
    """A findings reason a run may submit."""

    NOT_REPRODUCED = "not_reproduced"
    ALREADY_FIXED = "already_fixed"
    PRODUCT_DECISION_NEEDED = "product_decision_needed"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"


class FindingsReason(enum.StrEnum):
    """The stored findings reason vocabulary."""

    NOT_REPRODUCED = "not_reproduced"
    ALREADY_FIXED = "already_fixed"
    PRODUCT_DECISION_NEEDED = "product_decision_needed"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    # Platform only (#5586); never parsed from a submission.
    BUDGET_EXHAUSTED = "budget_exhausted"


TITLE_MAX = 256
BODY_MAX = 60_000
COMMAND_MAX = 2_000
OBSERVED_MAX = 4_000
TRIED_MAX = 4_000
MISSING_MAX = 4_000
REVISION_PATTERN = r"^[0-9a-f]{40}$"

PROPOSAL_ISSUE_CODES = frozenset(
    {
        "not_object",
        "kind",
        "unknown_field",
        "missing_field",
        "type",
        "length",
        "text",
        "value",
        "revision",
    }
)


def _plain_text(value: str) -> str:
    if "\x00" in value or not value.strip():
        raise PydanticCustomError("text", "the text is blank or holds NUL")
    return value


# One alias per bounded text field (mypy needs a literal type in each annotation).
Title = Annotated[
    str, StringConstraints(min_length=1, max_length=TITLE_MAX), AfterValidator(_plain_text)
]
Body = Annotated[
    str, StringConstraints(min_length=1, max_length=BODY_MAX), AfterValidator(_plain_text)
]
Command = Annotated[
    str, StringConstraints(min_length=1, max_length=COMMAND_MAX), AfterValidator(_plain_text)
]
Observed = Annotated[
    str, StringConstraints(min_length=1, max_length=OBSERVED_MAX), AfterValidator(_plain_text)
]
Tried = Annotated[
    str, StringConstraints(min_length=1, max_length=TRIED_MAX), AfterValidator(_plain_text)
]
Missing = Annotated[
    str, StringConstraints(min_length=1, max_length=MISSING_MAX), AfterValidator(_plain_text)
]
Revision = Annotated[str, StringConstraints(pattern=REVISION_PATTERN)]

_STRICT = ConfigDict(extra="forbid", strict=True, frozen=True)


class Reproduction(BaseModel):
    """The observed reproduction a ticket carries."""

    model_config = _STRICT

    command: Command
    observed: Observed
    revision: Revision


class TicketProposal(BaseModel):
    """One implementable ticket, published as a new linked issue."""

    model_config = _STRICT

    kind: Literal["ticket"]
    title: Title
    body: Body
    reproduction: Reproduction


class FindingsProposal(BaseModel):
    """Why no ticket was produced, posted as the source's result comment."""

    model_config = _STRICT

    kind: Literal["findings"]
    # A JSON string names the member; strict mode would demand an enum instance.
    reason: Annotated[SubmittedReason, Strict(False)]
    tried: Tried
    missing: Missing
    revision: Revision | None = None


class PlatformFindings(BaseModel):
    """The findings the platform itself records when an investigation's budget runs out.

    Constructor path for #5586: platform code builds it directly,
    ``PlatformFindings(kind="findings", reason=FindingsReason.BUDGET_EXHAUSTED,
    tried=..., missing=...)``, from facts it holds, never from submitted JSON.
    `parse_proposal` never builds one (a submitted ``budget_exhausted`` is
    refused on ``reason``), and it is not part of `ResultProposal`, so
    `submit_result` does not accept it either; #5586 adds its own sealing path.
    """

    model_config = _STRICT

    kind: Literal["findings"]
    reason: Literal[FindingsReason.BUDGET_EXHAUSTED]
    tried: Tried
    missing: Missing
    revision: Revision | None = None


ResultProposal = TicketProposal | FindingsProposal


@dataclass(frozen=True)
class ProposalIssue:
    """One refusal: a dotted field path and a code from `PROPOSAL_ISSUE_CODES`."""

    field: str
    code: str


class ProposalInvalid(ValueError):
    """The proposal failed validation. Carries field paths and codes, never values."""

    def __init__(self, issues: tuple[ProposalIssue, ...]) -> None:
        self.issues = issues
        listed = ", ".join(f"{issue.field or '<top>'}:{issue.code}" for issue in issues)
        super().__init__(f"the tracker result proposal failed validation: {listed}")


# Every field name the shapes own; any other key is shown as ``*``.
_SCHEMA_FIELDS = frozenset(
    name
    for model in (Reproduction, TicketProposal, FindingsProposal)
    for name in model.model_fields
)

_CODES = {
    "extra_forbidden": "unknown_field",
    "missing": "missing_field",
    "string_too_short": "length",
    "string_too_long": "length",
    "text": "text",
    "enum": "value",
    "literal_error": "value",
    "string_pattern_mismatch": "revision",
}


def _field(loc: tuple[int | str, ...]) -> str:
    parts = []
    for segment in loc:
        if isinstance(segment, int):
            parts.append(str(segment))
        else:
            parts.append(segment if segment in _SCHEMA_FIELDS else "*")
    return ".".join(parts)


def _code(error_type: str) -> str:
    if error_type in _CODES:
        return _CODES[error_type]
    # string_type, dict_type, model_type, is_instance_of and the like.
    return "type"


def parse_proposal(raw: object) -> ResultProposal:
    """The validated proposal; `ProposalInvalid` with every issue found."""

    if not isinstance(raw, Mapping):
        raise ProposalInvalid((ProposalIssue("", "not_object"),))
    kind = raw.get("kind")
    model: type[TicketProposal] | type[FindingsProposal]
    if kind == "ticket":
        model = TicketProposal
    elif kind == "findings":
        model = FindingsProposal
    else:
        raise ProposalInvalid((ProposalIssue("kind", "kind"),))
    try:
        return model.model_validate(dict(raw))
    except ValidationError as exc:
        # Only ``loc`` and ``type``: pydantic's ``input`` and ``msg`` can echo
        # submitted text and are dropped here.
        issues = tuple(
            dict.fromkeys(
                ProposalIssue(_field(error["loc"]), _code(error["type"]))
                for error in exc.errors(include_input=False, include_url=False)
            )
        )
    # Raised outside the handler, so the refusal does not chain the
    # ValidationError (and the input it holds) as its context.
    raise ProposalInvalid(issues)


def proposal_content(proposal: ResultProposal) -> dict[str, object]:
    """The JSON object the transport artifact stores and the digest covers."""

    return proposal.model_dump(mode="json", exclude_none=True)


def proposal_sha256(proposal: ResultProposal) -> str:
    """SHA-256 of the canonical JSON of `proposal_content`."""

    return hashlib.sha256(canonical_json(proposal_content(proposal))).hexdigest()
