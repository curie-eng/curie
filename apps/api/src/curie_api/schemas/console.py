from datetime import datetime

from pydantic import BaseModel, Field, field_validator


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


class ConsoleSessionOut(BaseModel):
    """The result of an exchange. Deliberately carries NO token.

    The session token travels only as an `HttpOnly` cookie, so page script cannot
    read it -- putting it in the body would hand the credential straight back to
    the JavaScript this design exists to keep it away from.
    """

    subject: str | None
    expires_at: datetime
