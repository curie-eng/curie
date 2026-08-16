"""Typed configuration for the mail adapter, read from the process environment.

``MailAdapterConfig`` is a frozen ``pydantic_settings.BaseSettings`` (the house
pattern, see ``apps/dispatcher/src/curie_dispatcher/config.py``): construct it
with no arguments and it reads the environment on init, falling back to the
defaults below for anything absent.

Every aliased field reads ONLY its alias, via ``AliasOnlyEnvSource``. That is
load-bearing rather than cosmetic: the adapter's own knobs live under
``CURIE_MAIL_*`` precisely so a stray generic ``PORT`` or ``POLL_INTERVAL`` in a
pod environment cannot reach them.

Env mapping:
    AGENTMAIL_API_KEY                       -> agentmail_api_key
    AGENTMAIL_INBOX                         -> agentmail_inbox
    AGENTMAIL_BASE_URL                      -> agentmail_base_url
    CURIE_API_URL                           -> api_base_url
                                               (CURIE_API_BASE_URL: deprecated alias)
    CURIE_CHANNEL_TOKEN                     -> channel_token
    CURIE_EGRESS_SECRET                     -> egress_secret
    ADAPTER_INGRESS_ENABLED                 -> ingress_enabled
    CURIE_MAIL_POLL_INTERVAL_SECONDS        -> poll_interval_seconds
    CURIE_MAIL_INGRESS_ATTEMPTS             -> ingress_attempts
    CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS  -> ingress_retry_delay_seconds
    CURIE_MAIL_PORT                         -> port
    CURIE_MAIL_ALLOWED_SENDERS              -> allowed_senders

The adapter holds no platform API key, no queue credential, and no database
access: ``CURIE_CHANNEL_TOKEN`` and ``CURIE_EGRESS_SECRET`` are its only Curie
credentials, and ``AGENTMAIL_API_KEY`` its only provider one.
"""

from __future__ import annotations

from typing import Annotated, Any

from aci_protocol.service_config import (
    AliasOnlyEnvSource,
    api_url_validation_alias,
    warn_if_deprecated_api_url_env,
)
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from pydantic_settings.sources import PydanticBaseSettingsSource


class MailAdapterConfig(BaseSettings):
    """Everything the mail adapter needs to run, in one typed object."""

    model_config = SettingsConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Swap the env source so aliased fields read only their alias."""
        # Surface the CURIE_API_BASE_URL -> CURIE_API_URL rename (#496).
        warn_if_deprecated_api_url_env()
        return (
            init_settings,
            AliasOnlyEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

    agentmail_api_key: str = Field(default="", validation_alias="AGENTMAIL_API_KEY")
    agentmail_inbox: str = Field(default="", validation_alias="AGENTMAIL_INBOX")
    agentmail_base_url: str = Field(
        default="https://api.agentmail.to/v0", validation_alias="AGENTMAIL_BASE_URL"
    )

    # The platform's channel ingress. The adapter is a client of it and nothing
    # more: it presents the scoped channel token, never a platform API key.
    api_base_url: str = Field(
        default="http://localhost:8000", validation_alias=api_url_validation_alias()
    )
    channel_token: str = Field(default="", validation_alias="CURIE_CHANNEL_TOKEN")
    egress_secret: str = Field(default="", validation_alias="CURIE_EGRESS_SECRET")

    # The neutral adapter-pattern name, documented in
    # docs/guides/building-a-channel-adapter.md; it gates the poller only, never
    # the egress server, so a staged cutover can serve replies before ingesting.
    ingress_enabled: bool = Field(default=True, validation_alias="ADAPTER_INGRESS_ENABLED")

    poll_interval_seconds: float = Field(
        default=5.0, validation_alias="CURIE_MAIL_POLL_INTERVAL_SECONDS"
    )
    ingress_attempts: int = Field(default=3, validation_alias="CURIE_MAIL_INGRESS_ATTEMPTS")
    ingress_retry_delay_seconds: float = Field(
        default=2.0, validation_alias="CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS"
    )
    port: int = Field(default=8080, validation_alias="CURIE_MAIL_PORT")

    # A comma-separated list of full addresses, bare domains, or the single
    # literal "*". NoDecode keeps pydantic-settings from JSON-parsing it, so the
    # validator below owns the whole parse.
    allowed_senders: Annotated[tuple[str, ...], NoDecode] = Field(
        default=(), validation_alias="CURIE_MAIL_ALLOWED_SENDERS"
    )

    @field_validator("allowed_senders", mode="before")
    @classmethod
    def _split_allowed_senders(cls, value: Any) -> Any:
        """Parse the comma-separated form, dropping empty segments.

        A trailing comma is the common operator typo, and treating the empty
        segment it produces as an entry would turn a configured allow-list into
        an open one.
        """
        entries = value.split(",") if isinstance(value, str) else value
        if not isinstance(entries, (list, tuple)):
            return value
        return tuple(stripped for entry in entries if (stripped := str(entry).strip()))
