"""Typed configuration for the dispatcher, read from the process environment.

The dispatcher needs two Slack tokens (an app-level token for Socket Mode and a
bot token for Web API calls), a Valkey connection, and the stream/dedupe/backoff
knobs. ``DispatcherConfig`` is a ``pydantic_settings.BaseSettings`` (the house
pattern, see ``apps/api``): construct it with no arguments and it reads the
environment on init, falling back to the defaults below for anything absent.

Env mapping:
    SLACK_APP_TOKEN            -> slack_app_token   (xapp-..., Socket Mode)
    SLACK_BOT_TOKEN            -> slack_bot_token   (xoxb-..., Web API)
    SLACK_SIGNING_SECRET       -> slack_signing_secret (optional; unused in
                                  Socket Mode, kept for Bolt App construction)
    VALKEY_HOST                -> valkey_host
    VALKEY_PORT                -> valkey_port
    VALKEY_PASSWORD            -> valkey_password
    VALKEY_DB                  -> valkey_db
    VALKEY_TLS                 -> valkey_tls
    CURIE_STREAM             -> stream
    CURIE_DEDUPE_PREFIX      -> dedupe_prefix
    CURIE_DEDUPE_TTL_SECONDS -> dedupe_ttl_seconds
    CURIE_PLACEHOLDER_TEXT   -> placeholder_text
    CURIE_BACKOFF_INITIAL_SECONDS -> backoff_initial_seconds
    CURIE_BACKOFF_MAX_SECONDS     -> backoff_max_seconds
    CURIE_BACKOFF_MULTIPLIER      -> backoff_multiplier
    CURIE_API_URL            -> api_base_url  (CURIE_API_BASE_URL: deprecated alias)
    CURIE_API_KEY            -> api_key
    CURIE_APPROVAL_CHAT_ATTESTER_SECRET -> approval_chat_attester_secret
    CURIE_API_PREFLIGHT_TIMEOUT_SECONDS -> api_preflight_timeout_s
    CURIE_HEARTBEAT_FILE             -> heartbeat_file
    CURIE_HEARTBEAT_INTERVAL_SECONDS -> heartbeat_interval_s
"""

from aci_protocol.service_config import (
    API_KEY_ENV,
    HEARTBEAT_FILE_ENV,
    HEARTBEAT_INTERVAL_ENV,
    RUNS_STREAM_DEFAULT,
    STREAM_ENV,
    AliasOnlyEnvSource,
    api_url_validation_alias,
    warn_if_deprecated_api_url_env,
)
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import (
    PydanticBaseSettingsSource,
)


class DispatcherConfig(BaseSettings):
    """Everything the dispatcher needs to run, in one typed object."""

    model_config = SettingsConfigDict(
        frozen=True, populate_by_name=True, extra="ignore"
    )

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

    slack_app_token: str = ""
    slack_bot_token: str = ""
    slack_signing_secret: str = ""

    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_password: str = ""
    valkey_db: int = 0
    # TLS transport for a BYO Valkey, rendered by the chart from valkey.tls.
    # Off by default: the in-chart store and the compose lane are both cleartext
    # by design (#2315).
    valkey_tls: bool = False

    stream: str = Field(default=RUNS_STREAM_DEFAULT, validation_alias=STREAM_ENV)
    dedupe_prefix: str = Field(
        default="curie:dedupe:", validation_alias="CURIE_DEDUPE_PREFIX"
    )
    dedupe_ttl_seconds: int = Field(
        default=3600, validation_alias="CURIE_DEDUPE_TTL_SECONDS"
    )

    # Platform API for the approval click-to-resolve flow (#246). Defaults match
    # the API's dev stack, mirroring the worker's settings for the same seam.
    api_base_url: str = Field(
        default="http://localhost:8000", validation_alias=api_url_validation_alias()
    )
    api_key: str = Field(default="curie-dev-key", validation_alias=API_KEY_ENV)
    # Socket Mode proves which Slack user acted, but the platform API accepts
    # that fact only when the dispatcher attests it with a credential held by
    # no other caller class (ADR-0106). This is deliberately required: falling
    # back to the widely shared platform key would let any holder mint a chat
    # identity and recreate the caller-asserted boundary under a signed shape.
    approval_chat_attester_secret: str = Field(
        validation_alias="CURIE_APPROVAL_CHAT_ATTESTER_SECRET"
    )
    # Deadline for the boot-time gate on that wiring (see preflight.py). Long
    # enough to absorb the API's own startup. Must be positive: the gate is the
    # AC2 requirement, so a non-positive value is a config error at boot rather
    # than a silent way to turn it off. Non-finite is rejected for the same
    # reason: `inf` passes `gt=0` but hangs the boot gate forever, so the pod
    # never exits, never crash-loops, and the operator never gets the signal the
    # gate exists to produce.
    api_preflight_timeout_s: float = Field(
        default=30.0,
        gt=0,
        allow_inf_nan=False,
        validation_alias="CURIE_API_PREFLIGHT_TIMEOUT_SECONDS",
    )

    placeholder_text: str = Field(
        default="On it. Working on your request.",
        validation_alias="CURIE_PLACEHOLDER_TEXT",
    )
    # CURIE_SHIMMER and CURIE_STATUS_TEXT are deliberately NOT read here (#1312).
    # The dispatcher no longer touches the Slack assistant-thread status: setting
    # it from this side put a best-effort cosmetic call in front of the durable
    # enqueue, and split set/clear across two processes. Both env names are
    # unchanged and now read by the worker alone -- see
    # apps/worker/src/curie_worker/config.py.
    #
    # One token does change meaning, and it is worth naming rather than claiming
    # blanket compatibility: the deleted parser here accepted `on` as truthy and
    # the worker's does not. `CURIE_SHIMMER=on` therefore used to mean "the
    # dispatcher raises the caption and the worker never lowers it", which is the
    # stranded shimmer #1312 was filed to kill, not a working configuration. It
    # was produced by nothing in the repo, documented nowhere, and rendered by
    # neither chart Deployment.

    backoff_initial_seconds: float = Field(
        default=1.0, gt=0, validation_alias="CURIE_BACKOFF_INITIAL_SECONDS"
    )
    backoff_max_seconds: float = Field(
        default=30.0, gt=0, validation_alias="CURIE_BACKOFF_MAX_SECONDS"
    )
    backoff_multiplier: float = Field(
        default=2.0, gt=1, validation_alias="CURIE_BACKOFF_MULTIPLIER"
    )

    # A daemon thread touches heartbeat_file every heartbeat_interval_s so an exec
    # liveness probe can restart the pod if the process fully wedges.
    heartbeat_file: str = Field(
        default="/tmp/curie-dispatcher.heartbeat",
        validation_alias=HEARTBEAT_FILE_ENV,
    )
    heartbeat_interval_s: float = Field(
        default=10.0, validation_alias=HEARTBEAT_INTERVAL_ENV
    )

    @model_validator(mode="after")
    def _require_independent_chat_attester(self) -> "DispatcherConfig":
        """Fail boot when the Slack identity attester is absent or shared."""

        if not self.approval_chat_attester_secret.strip():
            raise ValueError("CURIE_APPROVAL_CHAT_ATTESTER_SECRET must be non-blank")
        if self.approval_chat_attester_secret == self.api_key:
            raise ValueError(
                "CURIE_APPROVAL_CHAT_ATTESTER_SECRET must differ from CURIE_API_KEY"
            )
        return self

    def dedupe_key(self, slack_event_id: str) -> str:
        """The Valkey key that guards a single Slack event id against retries."""
        return f"{self.dedupe_prefix}{slack_event_id}"
