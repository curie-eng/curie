"""Pure ADR 0271 ticket encoding for connector signers and API readers."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from typing import Annotated, Any, Literal

from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

from .identity_context import CanonicalUUID, ContextNamespaceV1, Identifier, Issuer

MAX_TICKET_CHARS = 16_384
MAX_ARGUMENT_BYTES = 16_384
MAX_GRANT_CHARS = 4096
MAX_TIMESTAMP = 9223372036854775807
VERIFIED_GRANT_HEADER = "X-Curie-Grant-Verified"
_SEGMENT = re.compile(r"[A-Za-z0-9_-]+")
_DIGEST = re.compile(r"[0-9a-f]{64}")
Timestamp = Annotated[int, Field(ge=0, le=MAX_TIMESTAMP)]


class TicketError(ValueError):
    """A ticket refusal with a stable machine-readable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode(text: str) -> bytes:
    if not _SEGMENT.fullmatch(text):
        raise TicketError("invalid_ticket")
    try:
        raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (ValueError, binascii.Error):
        raise TicketError("invalid_ticket") from None
    if _b64(raw) != text:
        raise TicketError("invalid_ticket")
    return raw


def _clean_text(value: str) -> str:
    value.encode("utf-8")
    if value != value.strip() or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value):
        raise ValueError("invalid ticket string")
    return value


def _short_text(value: str) -> str:
    if not 1 <= len(value.encode("utf-8")) <= 256:
        raise ValueError("invalid ticket string length")
    return value


ShortText = Annotated[str, AfterValidator(_short_text)]


class _Closed(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @field_validator("*")
    @classmethod
    def _text(cls, value: Any) -> Any:
        return _clean_text(value) if isinstance(value, str) else value


class CanonicalPrincipal(_Closed):
    tenant_id: CanonicalUUID
    issuer: Issuer
    subject: Identifier


class RecipientNamespace(ContextNamespaceV1):
    @field_validator("authority", "kind", "key")
    @classmethod
    def _text(cls, value: str) -> str:
        return _clean_text(value)


class RecipientAccount(_Closed):
    provider: Identifier
    installation_id: CanonicalUUID
    namespace: RecipientNamespace
    account_id: Identifier


class SelectedApprover(CanonicalPrincipal):
    recipient_accounts: (
        Annotated[list[RecipientAccount], Field(min_length=1, max_length=16)] | None
    ) = None

    @field_validator("recipient_accounts", mode="before")
    @classmethod
    def _present_accounts(cls, value: object) -> object:
        if value is None:
            raise ValueError("omit absent recipient accounts")
        return value


class ApprovalTicket(_Closed):
    version: Literal[1]
    purpose: Literal["approval_request"]
    tenant_id: CanonicalUUID
    agent_id: CanonicalUUID
    connector: ShortText
    tool: ShortText
    route: ShortText
    key_id: ShortText
    requester: CanonicalPrincipal
    nonce: str
    issued_at: Timestamp
    expires_at: Timestamp
    arguments_sha256: str
    approvers: Annotated[list[SelectedApprover], Field(min_length=1, max_length=16)]

    @field_validator("version", mode="before")
    @classmethod
    def _version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("version must be an integer")
        return value

    @field_validator("nonce")
    @classmethod
    def _nonce(cls, value: str) -> str:
        if len(_decode(value)) != 32:
            raise ValueError("nonce must contain 32 random bytes")
        return value

    @field_validator("arguments_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("invalid argument digest")
        return value

    @model_validator(mode="after")
    def _selection(self) -> ApprovalTicket:
        if not 0 < self.expires_at - self.issued_at <= 300:
            raise ValueError("invalid preparation lifetime")
        people = [(p.tenant_id, p.issuer, p.subject) for p in self.approvers]
        if len(set(people)) != len(people):
            raise ValueError("duplicate approver")
        if self.requester.tenant_id != self.tenant_id or any(
            p[0] != self.tenant_id for p in people
        ):
            raise ValueError("principal tenant mismatch")
        return self


def _json(value: object, *, ascii_only: bool = True) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=ascii_only, allow_nan=False
    )


def execution_arguments_sha256(arguments: dict[str, Any]) -> str:
    """Production argument bytes, omitting only top-level approval_ticket."""
    try:
        if type(arguments) is not dict:
            raise TypeError
        value = {k: v for k, v in arguments.items() if k != "approval_ticket"}
        return hashlib.sha256(_json(value, ascii_only=False).encode("utf-8")).hexdigest()
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_arguments") from None


def sign_ticket(payload: dict[str, Any], *, signing_key: SigningKey) -> str:
    """Mint a ticket from closed claims; callers supply a fresh random nonce."""
    try:
        claims = ApprovalTicket.model_validate(payload)
        body = _json(claims.model_dump(exclude_none=True)).encode("ascii")
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_ticket") from None
    message = "cat1." + _b64(body)
    token = message + "." + _b64(signing_key.sign(message.encode("ascii")).signature)
    if len(token) > MAX_TICKET_CHARS:
        raise TicketError("ticket_too_large")
    return token


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise TicketError("invalid_ticket")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise TicketError("invalid_ticket")


def _ticket_envelope(token: str) -> tuple[bytes, bytes, bytes]:
    try:
        if type(token) is not str or len(token) > MAX_TICKET_CHARS or not token.isascii():
            raise ValueError
        prefix, payload, signature = token.split(".")
        if prefix != "cat1":
            raise ValueError
        body, sig = _decode(payload), _decode(signature)
        if len(sig) != 64:
            raise ValueError
        return ("cat1." + payload).encode("ascii"), body, sig
    except (ValueError, TypeError):
        raise TicketError("invalid_ticket") from None


def _ticket_claims(body: bytes) -> ApprovalTicket:
    try:
        parsed = json.loads(
            body.decode("ascii"), object_pairs_hook=_pairs, parse_constant=_constant
        )
        claims = ApprovalTicket.model_validate(parsed)
        if _json(claims.model_dump(exclude_none=True)).encode("ascii") != body:
            raise ValueError
        return claims
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_ticket") from None


def unverified_key_id(token: str) -> str:
    """Read an UNTRUSTED key selector; never proves registration or signature.

    Bounded framing, closed shape and canonical JSON are checked. Always verify
    the original ticket with the registered key selected by this untrusted id.
    """
    _, body, _ = _ticket_envelope(token)
    return _ticket_claims(body).key_id


def parse_ticket(token: str, *, public_key: VerifyKey) -> ApprovalTicket:
    """Verify signature and closed wire shape, without preparation freshness."""
    message, body, signature = _ticket_envelope(token)
    try:
        public_key.verify(message, signature)
    except (ValueError, TypeError, BadSignatureError):
        raise TicketError("invalid_ticket") from None
    return _ticket_claims(body)


def verify_preparation(
    token: str, *, public_key: VerifyKey, key_id: str, now: int, arguments_sha256: str
) -> ApprovalTicket:
    """Verify wire facts only; no route, registration, identity or nonce authority."""
    claims = parse_ticket(token, public_key=public_key)
    if claims.key_id != key_id:
        raise TicketError("key_mismatch")
    if type(now) is not int or not claims.issued_at <= now < claims.expires_at:
        raise TicketError("preparation_expired")
    if claims.arguments_sha256 != arguments_sha256:
        raise TicketError("arguments_mismatch")
    return claims


def check_execution_size(
    arguments: dict[str, Any], *, agent: str, connector: str, tool: str
) -> int:
    """Bound a future ccg using the largest signed 64-bit expiry and a UUID jti.

    Call before approval creation with the complete persisted arguments and the
    exact agent name, connector and upstream tool the worker will mint.
    """
    try:
        if type(arguments) is not dict:
            raise TypeError
        text = _json(arguments, ascii_only=False)
        if len(text.encode("utf-8")) > MAX_ARGUMENT_BYTES:
            raise TicketError("arguments_too_large")
        payload = _json(
            dict(
                agent=agent,
                connector=connector,
                tool=tool,
                args=text,
                exp=MAX_TIMESTAMP,
                jti="00000000-0000-0000-0000-000000000000",
            )
        )
        size = len("ccg.") + len(_b64(payload.encode("ascii"))) + 1 + 86
        if size > MAX_GRANT_CHARS:
            raise TicketError("grant_too_large")
        return size
    except TicketError:
        raise
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_arguments") from None
