"""Standalone Slack gateway process; application services hold only caller credentials."""

import ssl
from pathlib import Path

import httpx
from aiohttp import web
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..read_transport import GatewayProviders, gateway_application
from ..reply_delivery import ReplyDeliveryStore
from ..request_replay import RequestReplayStore
from .canvas import SlackCanvasReader
from .files import SlackFileClient
from .groups import SlackUserGroupClient
from .reads import SlackChannelReader
from .senders import SlackSenderIdentities
from .slack_sink import SlackReplyAdapter
from .tokens import SlackProviderConfig, slack_bot_tokens


class GatewayConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", hide_input_in_errors=True)
    api_token: str = Field(
        default="", validation_alias="CURIE_CHANNEL_GATEWAY_API_TOKEN", repr=False
    )
    worker_token: str = Field(
        default="", validation_alias="CURIE_CHANNEL_GATEWAY_WORKER_TOKEN", repr=False
    )
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


async def application(config: GatewayConfig, slack: SlackProviderConfig) -> web.Application:
    tokens = slack_bot_tokens(slack)
    http = httpx.AsyncClient(timeout=30.0, follow_redirects=False)
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
            tokens={"api": config.api_token, "worker": config.worker_token},
            replay=RequestReplayStore(config.replay_db),
            max_file_bytes=config.max_file_bytes,
            groups={
                name: SlackUserGroupClient(http, token=token, ttl_s=config.group_cache_ttl_s)
                for name, token in tokens.items()
                if token
            },
            files={name: SlackFileClient(token=token) for name, token in tokens.items() if token},
            providers=GatewayProviders(
                channel=SlackChannelReader(http, tokens),
                canvas=SlackCanvasReader(http, tokens),
                senders=SlackSenderIdentities(tokens, base_url=config.slack_api_base_url or None),
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
    web.run_app(
        application(config, SlackProviderConfig()),
        host=config.host,
        port=config.port,
        ssl_context=context,
        access_log=None,
    )


if __name__ == "__main__":
    main()
