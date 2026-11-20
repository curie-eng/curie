import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ConsoleLoginCodeMint(BaseModel):
    """Administrative request for one immutable subject-bound console login."""

    subject: str = Field(min_length=1)

    @field_validator("subject")
    @classmethod
    def _nonblank_subject(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("subject must not be blank")
        return value


class ConsoleLoginCodeOut(BaseModel):
    """A freshly minted login code, returned exactly once.

    The code is plaintext here because this response IS the one delivery: the CLI
    prints it for the operator to copy. Only its hash is stored.
    """

    code: str
    subject: str
    expires_at: datetime


class ConsoleSessionExchange(BaseModel):
    """The browser's exchange request: a login code and nothing else."""

    code: str = Field(min_length=1)


class ConsoleSessionPrincipal(BaseModel):
    """The person a principal console session names. No token, no issuer."""

    id: uuid.UUID
    display_name: str | None
    email: str | None


class ConsoleSessionOut(BaseModel):
    """The result of an exchange or a session probe. Deliberately carries NO token.

    The session token travels only as an `HttpOnly` cookie, so page script cannot
    read it -- putting it in the body would hand the credential straight back to
    the JavaScript this design exists to keep it away from.

    ``kind`` is ``operator`` for a login-code session and ``principal`` for an
    OIDC session. ``principal`` is populated only for the latter. A principal
    session's ``subject`` is ``principal:<id>``, the same subject
    ``authenticate_administrator`` already records.
    """

    subject: str | None
    expires_at: datetime
    kind: Literal["operator", "principal"]
    principal: ConsoleSessionPrincipal | None


class ConsoleSignInMethodsOut(BaseModel):
    """Which unauthenticated sign-in paths this install offers."""

    oidc: bool


class PrincipalOut(BaseModel):
    """The principal an OIDC console session authenticates (#2908).

    Identity and display attributes only. ``idp_issuer`` and the authorization
    version are server bookkeeping the console has no use for, and the session
    token never appears here for the same reason it is absent from
    ``ConsoleSessionOut``.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    idp_subject: str
    display_name: str | None
    email: str | None
    status: str
