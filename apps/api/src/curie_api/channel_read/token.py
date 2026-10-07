"""The API issued channel read capability (ADR 0100).

It follows ``curie_api.issue_read_token``: an HMAC signed claim set the sandbox
presents to the channel read route and its canvas sibling (ADR 0200). It names
one agent, one deployment, the grant digest it was minted under and which
grants that bundle declares, one logical turn and its generation. It carries no
channel list: the bound set is read again on every request.
"""

from __future__ import annotations

import hmac
import json
import time
import uuid
from typing import Literal, Self

from curie_internal import sandbox_token
from curie_internal.sandbox_token import b64url, b64url_decode
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

_PREFIX = "chr"
# Mirrors the worker's SANDBOX_TOKEN_TTL_SECONDS: no capability outlives a day.
MAX_TTL_SECONDS = 24 * 60 * 60
_MAX_TOKEN_LENGTH = 4096

# The platform Slack grant names (plugin_format.PLATFORM_SLACK_GRANT_FIELDS).
GrantName = Literal["channelRead", "canvasList", "canvasRead", "canvasEdit"]


class ChannelPair(BaseModel):
    """A channel kind and address, as the turn's default channel."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    kind: str = Field(min_length=1, max_length=64)
    address: str = Field(min_length=1, max_length=255)


class ChannelReadClaims(BaseModel):
    """Strict claims authenticated before any ledger or database lookup."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    aud: Literal["channel.read"]
    agent: uuid.UUID
    deployment: uuid.UUID
    grant: str = Field(pattern=r"^[0-9a-f]{64}$")
    # Which platform Slack grants the bundle behind ``grant`` declares (ADR 0200).
    grants: tuple[GrantName, ...] = Field(min_length=1, max_length=4)
    turn: str = Field(min_length=1, max_length=512)
    gen: int = Field(gt=0)
    default: ChannelPair | None
    iat: int = Field(ge=0)
    exp: int = Field(gt=0)

    @model_validator(mode="after")
    def _grants_are_distinct(self) -> Self:
        if len(set(self.grants)) != len(self.grants):
            raise ValueError("a grant is named once")
        return self


def mint(api_key: str, claims: ChannelReadClaims) -> str:
    payload = json.dumps(
        claims.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    signed = f"{_PREFIX}.{b64url(payload)}"
    return f"{signed}.{sandbox_token.signature(api_key, signed)}"


def verify_claims(token: str, api_key: str) -> ChannelReadClaims | None:
    """Reject malformed, foreign or expired credentials without saying why."""

    if len(token) > _MAX_TOKEN_LENGTH:
        return None
    try:
        prefix, payload, signature = token.split(".")
        if prefix != _PREFIX or not hmac.compare_digest(
            signature, sandbox_token.signature(api_key, f"{prefix}.{payload}")
        ):
            return None
        claims = ChannelReadClaims.model_validate_json(b64url_decode(payload))
    except (ValueError, TypeError, ValidationError):
        return None
    now = time.time()
    if (
        claims.iat > now
        or claims.exp <= now
        or claims.exp <= claims.iat
        or claims.exp - claims.iat > MAX_TTL_SECONDS
    ):
        return None
    return claims
