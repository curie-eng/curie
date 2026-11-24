"""Fixture setup and native Socket Mode delivery inside the scripted install."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sys
from types import SimpleNamespace
from typing import Any

TEAM = "T0EXAMPLE1"
USER = "U0EXAMPLE1"
BOT_USER = "U0EXAMPLE9"
BOT = "B0EXAMPLE1"


async def seed_sender(user: str = USER) -> dict[str, str]:
    from curie_api.db import create_engine, create_sessionmaker
    from curie_api.identity.service import find_or_create_namespace
    from curie_api.models import (
        DEFAULT_TENANT_ID,
        ChannelIdentity,
        IdentityLink,
        Principal,
        ProviderInstallation,
    )
    from sqlalchemy import select

    engine = create_engine()
    try:
        async with create_sessionmaker(engine).begin() as session:
            receiving = (
                await session.scalars(
                    select(ChannelIdentity).where(
                        ChannelIdentity.tenant_id == DEFAULT_TENANT_ID,
                        ChannelIdentity.name == "default",
                        ChannelIdentity.provider == "slack",
                    )
                )
            ).one()
            installation = await session.scalar(
                select(ProviderInstallation).where(
                    ProviderInstallation.tenant_id == DEFAULT_TENANT_ID,
                    ProviderInstallation.provider == "slack",
                    ProviderInstallation.external_account_id == TEAM,
                )
            )
            if installation is None:
                installation = ProviderInstallation(
                    tenant_id=DEFAULT_TENANT_ID, provider="slack", external_account_id=TEAM
                )
                session.add(installation)
                await session.flush()
            receiving.provider_installation_id = installation.id
            namespace = await find_or_create_namespace(
                session,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                authority=installation.authority,
                kind="slack_workspace",
                key=TEAM,
            )
            principal = Principal(
                tenant_id=DEFAULT_TENANT_ID,
                idp_subject=f"acme-steering-author-{user}",
                type="human",
            )
            session.add(principal)
            await session.flush()
            session.add(
                IdentityLink(
                    tenant_id=DEFAULT_TENANT_ID,
                    identity_namespace_id=namespace.id,
                    provider_native_id=user,
                    principal_id=principal.id,
                    created_by_principal_id=principal.id,
                    verification_source="admin_mapped",
                )
            )
            return {"tenant_id": str(DEFAULT_TENANT_ID), "principal_id": str(principal.id)}
    finally:
        await engine.dispose()


def _config() -> Any:
    from curie_channel_gateway.slack.config import DispatcherConfig

    return DispatcherConfig(
        api_base_url="http://127.0.0.1:8000",
        api_key=os.environ["API_KEY"],
        approval_chat_attester_secret=os.environ.get("CURIE_APPROVAL_CHAT_ATTESTER_SECRET")
        or secrets.token_hex(32),
    )


def mention(data: dict[str, Any]) -> dict[str, Any]:
    from curie_channel_gateway.slack.app import build_app
    from curie_channel_gateway.slack.identities import SlackIdentityCredentials
    from curie_channel_gateway.slack.valkey import build_redis
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    from slack_bolt.authorization import AuthorizeResult
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.web import WebClient

    config = _config()
    credentials = SlackIdentityCredentials(
        name="default", app_token="xapp-EXAMPLE", bot_token=data["bot_token"]
    )
    web = WebClient(token=credentials.bot_token, base_url=data["slack"])
    redis = build_redis(config)

    def authorize(**_kwargs: Any) -> AuthorizeResult:
        auth = web.auth_test()
        return AuthorizeResult(
            enterprise_id=None,
            team_id=auth["team_id"],
            bot_token=credentials.bot_token,
            bot_id=auth["bot_id"],
            bot_user_id=auth["user_id"],
        )

    app = build_app(
        config, web_client=web, redis_client=redis, identity=credentials, authorize=authorize
    )
    errors: list[str] = []
    acknowledgements: list[Any] = []

    @app.error
    def capture_error(error: Exception) -> None:
        errors.append(type(error).__name__)

    event_id = data["event_id"]
    try:
        handler = SocketModeHandler(app, app_token=credentials.app_token)
        handler.handle(
            SimpleNamespace(
                logger=logging.getLogger(__name__),
                send_socket_mode_response=acknowledgements.append,
            ),
            SocketModeRequest(
                type="events_api",
                envelope_id=event_id,
                payload={
                    "type": "event_callback",
                    "event_id": event_id,
                    "team_id": TEAM,
                    "api_app_id": "A0EXAMPLE1",
                    "is_ext_shared_channel": False,
                    "authorizations": [{"team_id": TEAM, "user_id": BOT_USER, "is_bot": True}],
                    "event": data.get("event") or {
                        "type": "app_mention",
                        "channel": data["channel"],
                        "thread_ts": data["root"],
                        "ts": data["ts"],
                        "user": USER,
                        "user_team": TEAM,
                        "team": TEAM,
                        "text": f"<@{BOT_USER}> {data['text']}",
                    },
                },
            ),
        )
        app.listener_runner.listener_executor.shutdown(wait=True)
        if errors or [ack.envelope_id for ack in acknowledgements] != [event_id]:
            raise RuntimeError("Socket Mode delivery failed")
        from curie_internal.slack_mention_receipt import receipt_key

        raw = redis.get(receipt_key(event_id))
        return {
            "acked": True,
            "claimed": bool(redis.exists(config.dedupe_key(event_id))),
            "receipt_version": json.loads(raw)["version"] if raw else None,
            "placeholder_text": config.placeholder_text,
        }
    finally:
        redis.close()


def expire_receipt(data: dict[str, str]) -> dict[str, bool]:
    from curie_channel_gateway.slack.valkey import build_redis
    from curie_internal.slack_mention_receipt import receipt_key

    redis = build_redis(_config())
    try:
        key = receipt_key(data["event_id"])
        return {"expired": bool(redis.pexpire(key, 0)) and not redis.exists(key)}
    finally:
        redis.close()


if __name__ == "__main__":
    try:
        if sys.argv[1] == "seed":
            result = asyncio.run(seed_sender(json.loads(sys.argv[2]).get("user", USER)))
        elif sys.argv[1] == "expire":
            result = expire_receipt(json.loads(sys.argv[2]))
        else:
            result = mention(json.loads(sys.argv[2]))
        print(json.dumps(result))
    except Exception as exc:  # noqa: BLE001 - database and SDK errors may include private inputs
        raise SystemExit(f"scripted Slack ingress failed ({type(exc).__name__})") from None
