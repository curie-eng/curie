"""Client an HTTP identity provider uses to ask the gateway for a Slack user's email.

@spec GATEWAY-USER-EMAIL-4, GATEWAY-USER-EMAIL-5.

The provider holds only the gateway identity-caller credential; the Slack token
stays in the gateway. Settings refuse any Slack token, so a provider configured
for gateway lookup cannot also carry one. Every failure raises
``UserEmailLookupError`` (``UserEmailUnknown`` for a user Slack has no usable
email for), so a caller refuses rather than falling back to a default. Nothing
here logs.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Self

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .read_transport import (
    UNKNOWN_USER_EMAIL_CODE,
    GatewayClient,
    GatewayError,
    GatewayNotFoundError,
    validate_endpoint,
)
from .tls import gateway_ca_context
from .token_custody import refuse_slack_tokens
from .user_email_models import UserEmailLookupError, UserEmailUnknown


class GatewayUserEmailSettings(BaseSettings):
    """Identity-provider configuration for gateway email lookup."""

    model_config = SettingsConfigDict(extra="ignore", hide_input_in_errors=True)

    url: str = Field(validation_alias="CURIE_CHANNEL_GATEWAY_URL")
    token: str = Field(validation_alias="CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN", repr=False)
    slack_identity: str = Field(
        default="default", min_length=1, validation_alias="CURIE_CHANNEL_GATEWAY_SLACK_IDENTITY"
    )
    # Longer than the gateway's own 5 second Slack bound, so a slow Slack
    # surfaces as the gateway's 504 rather than a client-side give-up.
    timeout_s: float = Field(
        default=8.0, gt=0, validation_alias="CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S"
    )
    ca_file: str = Field(default="", validation_alias="CURIE_CHANNEL_GATEWAY_CA_FILE")
    allow_insecure_http: bool = Field(
        default=False, validation_alias="CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE"
    )

    @model_validator(mode="before")
    @classmethod
    def _no_slack_tokens(cls, values: Any) -> Any:
        return refuse_slack_tokens(values)


class _Answer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    user_id: str
    email: str


class GatewayUserEmails:
    """``email_of(user_id, workspace)`` through the gateway's ``user_email`` read."""

    def __init__(
        self,
        client: GatewayClient,
        *,
        slack_identity: str = "default",
        timeout_s: float = 8.0,
        owned_http: httpx.AsyncClient | None = None,
    ) -> None:
        self._client = client
        self._identity = slack_identity
        self._timeout_s = timeout_s
        self._owned_http = owned_http

    @classmethod
    def from_settings(cls, settings: GatewayUserEmailSettings) -> Self:
        validate_endpoint(
            settings.url, settings.token, allow_insecure_http=settings.allow_insecure_http
        )
        verify = gateway_ca_context(settings.ca_file) if settings.ca_file else True
        http = httpx.AsyncClient(timeout=settings.timeout_s, verify=verify, follow_redirects=False)
        client = GatewayClient(
            http, settings.url, settings.token, allow_insecure_http=settings.allow_insecure_http
        )
        return cls(
            client,
            slack_identity=settings.slack_identity,
            timeout_s=settings.timeout_s,
            owned_http=http,
        )

    async def email_of(self, user_id: str, workspace: str) -> str:
        if not user_id or not workspace:
            raise UserEmailUnknown("slack user unknown")
        try:
            async with asyncio.timeout(self._timeout_s):
                answer = await self._client.call(
                    self._identity,
                    "user_email",
                    {"user_id": user_id, "workspace": workspace},
                    TypeAdapter(_Answer),
                )
        except GatewayNotFoundError as error:
            # Only the gateway's own code means "unknown"; any other 404 (a
            # wrong path or proxy) is an outage, never a silent unresolved.
            if error.code == UNKNOWN_USER_EMAIL_CODE:
                raise UserEmailUnknown("slack user unknown") from None
            raise UserEmailLookupError("gateway user email unavailable") from None
        except (GatewayError, TimeoutError):
            raise UserEmailLookupError("gateway user email unavailable") from None
        if answer.user_id != user_id or not answer.email.strip():
            raise UserEmailLookupError("gateway user email answer invalid")
        return answer.email

    async def aclose(self) -> None:
        if self._owned_http is not None:
            await self._owned_http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


__all__ = [
    "GatewayUserEmailSettings",
    "GatewayUserEmails",
    "UserEmailLookupError",
    "UserEmailUnknown",
]
