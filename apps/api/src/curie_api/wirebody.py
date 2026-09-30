"""Tolerant request-body types for the shared ACI wire models (#492).

The wire is strict producers, tolerant consumers (``packages/CLAUDE.md``): a
consumer decoding the wire ignores fields it does not model, which is what makes
a new optional field a genuine patch. Every other consumer gets that from an
``aci_protocol.parse_*`` helper, which threads the reader context. A FastAPI
route cannot: FastAPI validates the request body itself and has no seam to pass
a validation context, so a bare ``data: ApprovalRequest`` annotation decodes
strictly and 422s a forward-compatible payload from a newer worker.

These aliases restore tolerance at that seam. The ``BeforeValidator`` runs ahead
of the model's own strict-on-construction check and performs the decode itself
with the SAME ``READER_CONTEXT`` the ``parse_*`` helpers use -- not a second
tolerance mechanism -- returning a validated instance the outer schema then
accepts as-is. Two properties this preserves deliberately:

- **The OpenAPI schema is unchanged.** ``BeforeValidator`` does not alter the
  wrapped model's JSON schema, so the committed ``openapi.json`` still describes
  the request body by the model's own name. No duplicated model, which is the
  whole point of #492.
- **A genuinely invalid body is still a 422** with per-field ``loc``: the inner
  ``ValidationError`` propagates out of ``validate_python`` unchanged.

Producers are untouched: they construct these models directly, where an unknown
field is still an error at the source.

The module is the API's request-body seam generally, so ``read_bounded_body``
lives here too: the pre-auth size guard is one implementation every router that
reads its own body calls, rather than a copy per router that has to be audited
separately.
"""

from typing import Annotated, Any

from aci_protocol import READER_CONTEXT, ApprovalRequest, EvalReport, QueuedTurn
from fastapi import HTTPException, Request, status
from pydantic import BaseModel, BeforeValidator


async def read_bounded_body(request: Request, max_bytes: int, *, subject: str) -> bytes:
    """Read the request body, rejecting anything over ``max_bytes`` early.

    The bound is enforced BEFORE the whole body is buffered, parsed, or
    authenticated (#633), so an unauthenticated oversized request cannot make
    the app hold an unbounded body in memory. A declared ``Content-Length`` over
    the bound is refused without reading a byte; then the body is read in chunks
    and refused the moment the accumulated size crosses the bound, so an absent
    or lying ``Content-Length`` (including a chunked/streamed request) is held to
    the same limit. Raises 413 on an oversized body.

    ``subject`` names the surface in the 413 detail (``"webhook body"``,
    ``"channel turn body"``): the message is the only thing that differed between
    the per-router copies of this function, and a message is a parameter, not a
    reason for a second implementation of the pre-auth memory guard.
    """

    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            declared_len = int(declared)
        except ValueError:
            declared_len = -1  # unparseable: fall through to streamed enforcement
        if declared_len > max_bytes:
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                f"{subject} exceeds the maximum size",
            )

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                f"{subject} exceeds the maximum size",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _reader_decode[M: BaseModel](model: type[M]) -> Any:
    """Build a before-validator that decodes ``model`` with the reader context."""

    def _decode(value: Any) -> Any:
        # Only a raw mapping is a wire decode. Anything else (an already-built
        # instance) is passed through for the model schema to judge.
        if isinstance(value, dict):
            return model.model_validate(value, context=READER_CONTEXT)
        return value

    return BeforeValidator(_decode)


ApprovalRequestBody = Annotated[ApprovalRequest, _reader_decode(ApprovalRequest)]
EvalReportBody = Annotated[EvalReport, _reader_decode(EvalReport)]
# A worker echoes a queued turn back to the API (the GitHub feedback checks); a
# newer worker's turn can carry an optional field this API predates
# (TOOL-ACCESS-7).
QueuedTurnBody = Annotated[QueuedTurn, _reader_decode(QueuedTurn)]


def stored_turn_matches(turn: QueuedTurn, stored: Any) -> bool:
    """Whether ``turn`` is the turn ``stored`` holds, compared as decoded models.

    @spec TOOL-ACCESS-7: a row written before an optional turn field existed
    lacks its key, and a turn decoded after carries it as its default, so raw
    JSON equality would refuse the same turn across an upgrade. Both sides are
    read tolerantly; an unreadable stored value matches nothing.
    """

    try:
        return QueuedTurn.model_validate(stored, context=READER_CONTEXT) == turn
    except ValueError:
        return False


__all__ = [
    "ApprovalRequestBody",
    "EvalReportBody",
    "QueuedTurnBody",
    "read_bounded_body",
    "stored_turn_matches",
]
