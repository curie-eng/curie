"""Standalone Slack gateway process; application services hold only caller credentials."""

import logging
import ssl
from importlib.metadata import version
from pathlib import Path

import httpx
from aci_protocol.slack_identities import declared_slack_identity_names
from aiohttp import web
from curie_telemetry import bootstrap_service_telemetry
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..lookup_identities import (
    LOOKUP_IDENTITIES_ENV,
    DirectoryEmails,
    LookupIdentities,
    LookupSourceFactory,
    lookup_email_sources,
)
from ..read_transport import GatewayProviders, gateway_application
from ..reply_delivery import ReplyDeliveryStore
from ..request_replay import RequestReplayStore
from .account_profiles import SlackAccountProfiles
from .canvas import SlackCanvasReader
from .files import SlackFileClient
from .groups import SlackUserGroupClient
from .reads import SlackChannelReader
from .senders import SlackSenderIdentities
from .slack_sink import SlackReplyAdapter
from .tokens import SlackProviderConfig, slack_bot_tokens
from .user_emails import SlackUserEmails


class GatewayConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", hide_input_in_errors=True)
    api_token: str = Field(
        default="", validation_alias="CURIE_CHANNEL_GATEWAY_API_TOKEN", repr=False
    )
    worker_token: str = Field(
        default="", validation_alias="CURIE_CHANNEL_GATEWAY_WORKER_TOKEN", repr=False
    )
    # Optional third caller: an external HTTP identity provider that may only
    # ask which email belongs to a Slack user (@spec GATEWAY-USER-EMAIL-1).
    identity_token: str = Field(
        default="", validation_alias="CURIE_CHANNEL_GATEWAY_IDENTITY_TOKEN", repr=False
    )
    user_email_timeout_s: float = Field(
        default=5.0, gt=0, validation_alias="CURIE_CHANNEL_GATEWAY_USER_EMAIL_TIMEOUT_S"
    )
    # Credentials for directory reads alone (@spec GATEWAY-USER-EMAIL-7).
    lookup_identities: LookupIdentities = Field(default=(), validation_alias=LOOKUP_IDENTITIES_ENV)
    host: str = Field(default="0.0.0.0", validation_alias="CURIE_CHANNEL_GATEWAY_HOST")
    port: int = Field(default=8081, validation_alias="CURIE_CHANNEL_GATEWAY_PORT")
    allow_insecure_http: bool = Field(
        default=False, validation_alias="CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE"
    )
    tls_cert: str = Field(default="", validation_alias="CURIE_CHANNEL_GATEWAY_TLS_CERT")
    tls_key: str = Field(default="", validation_alias="CURIE_CHANNEL_GATEWAY_TLS_KEY", repr=False)
    replay_db: str = Field(
        default="/var/lib/curie-gateway/requests.sqlite3",
        validation_alias="CURIE_CHANNEL_GATEWAY_REPLAY_DB",
    )
    max_file_bytes: int = Field(
        default=32 * 1024 * 1024, gt=0, validation_alias="CURIE_CHANNEL_GATEWAY_MAX_FILE_BYTES"
    )
    group_cache_ttl_s: float = Field(default=60.0, validation_alias="SLACK_USERGROUP_CACHE_TTL_S")
    slack_api_base_url: str = ""
    slack_trusted_origins: str = Field(default="", validation_alias="CURIE_SLACK_TRUSTED_ORIGINS")


def gateway_callers(config: GatewayConfig) -> dict[str, str]:
    """The caller credentials; the identity caller exists only when configured."""
    callers = {"api": config.api_token, "worker": config.worker_token}
    if config.identity_token:
        callers["identity"] = config.identity_token
    return callers


def lookup_registry(
    config: GatewayConfig, http: httpx.AsyncClient
) -> dict[str, LookupSourceFactory]:
    """Each lookup identity kind's directory adapter (@spec GATEWAY-USER-EMAIL-7)."""

    def slack(name: str, token: str) -> SlackUserEmails:
        return SlackUserEmails(
            http,
            {name: token},
            base_url=config.slack_api_base_url or None,
            timeout_s=config.user_email_timeout_s,
        )

    return {"slack": slack}


async def application(config: GatewayConfig, slack: SlackProviderConfig) -> web.Application:
    tokens = slack_bot_tokens(slack)
    http = httpx.AsyncClient(timeout=30.0, follow_redirects=False)
    try:
        # @spec GATEWAY-USER-EMAIL-8, -9: only the user email read sees these.
        lookups = lookup_email_sources(
            config.lookup_identities,
            registry=lookup_registry(config, http),
            declared=declared_slack_identity_names(slack.slack_identities),
        )
    except Exception:
        await http.aclose()
        raise
    sink = SlackReplyAdapter(
        tokens.get("default", ""),
        identity_tokens=tokens,
        base_url=config.slack_api_base_url or None,
        trusted_origins=tuple(
            origin.strip() for origin in config.slack_trusted_origins.split(",") if origin.strip()
        ),
    )
    try:
        app = gateway_application(
            tokens=gateway_callers(config),
            replay=RequestReplayStore(config.replay_db),
            max_file_bytes=config.max_file_bytes,
            groups={
                name: SlackUserGroupClient(
                    http,
                    token=token,
                    ttl_s=config.group_cache_ttl_s,
                    base_url=config.slack_api_base_url or "https://slack.com/api/",
                )
                for name, token in tokens.items()
                if token
            },
            files={name: SlackFileClient(token=token) for name, token in tokens.items() if token},
            providers=GatewayProviders(
                channel=SlackChannelReader(http, tokens),
                canvas=SlackCanvasReader(http, tokens),
                senders=SlackSenderIdentities(tokens, base_url=config.slack_api_base_url or None),
                emails=DirectoryEmails(
                    SlackUserEmails(
                        http,
                        tokens,
                        base_url=config.slack_api_base_url or None,
                        timeout_s=config.user_email_timeout_s,
                    ),
                    lookups,
                ),
                profiles=SlackAccountProfiles(
                    http, tokens, base_url=config.slack_api_base_url or None
                ),
            ),
            reply=sink,
            reply_state=ReplyDeliveryStore(Path(config.replay_db)),
            allow_insecure_http=config.allow_insecure_http,
        )
    except Exception:
        await http.aclose()
        raise

    async def cleanup(_app: web.Application) -> None:
        await http.aclose()

    app.on_cleanup.append(cleanup)
    return app


def main() -> None:
    config = GatewayConfig()
    context = None
    if config.tls_cert and config.tls_key:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(config.tls_cert, config.tls_key)
    elif not config.allow_insecure_http:
        raise ValueError("gateway requires TLS certificate and key or explicit insecure opt-out")
    telemetry = bootstrap_service_telemetry(
        "curie-channel-gateway",
        service_version=version("curie-channel-gateway"),
        logger=logging.getLogger("curie_channel_gateway"),
    )
    try:
        web.run_app(
            application(config, SlackProviderConfig()),
            host=config.host,
            port=config.port,
            ssl_context=context,
            access_log=None,
        )
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
