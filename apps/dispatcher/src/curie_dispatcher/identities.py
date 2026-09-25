"""The Slack identities this dispatcher connects (ADR-0168 decision 2).

`aci_protocol.slack_identities` parses which identities the chart declares and
where each one's tokens are; this module reads the tokens and holds the rules
every identity's app shares. The identity a turn carries is always the one whose
Bolt app the delivery arrived on, never a field of the delivery.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from aci_protocol.turn import DEFAULT_IDENTITY

from .config import DispatcherConfig


@dataclass(frozen=True)
class SlackIdentityCredentials:
    """One identity's name and the secrets its Bolt app is built from."""

    name: str
    app_token: str = field(repr=False)
    bot_token: str = field(repr=False)
    signing_secret: str = field(default="", repr=False)


@dataclass(frozen=True)
class SlackBotIds:
    """What ``auth.test`` reported for one identity's bot token."""

    team_id: str | None
    app_id: str | None
    bot_id: str | None
    bot_user_id: str | None


def default_identity_credentials(config: DispatcherConfig) -> SlackIdentityCredentials:
    """``default``, from the same ``SLACK_*`` settings a stock install reads."""

    return SlackIdentityCredentials(
        name=DEFAULT_IDENTITY,
        app_token=config.slack_app_token,
        bot_token=config.slack_bot_token,
        signing_secret=config.slack_signing_secret,
    )


def resolve_identity_credentials(
    config: DispatcherConfig,
    *,
    logger: logging.Logger,
    environ: Mapping[str, str] | None = None,
) -> tuple[SlackIdentityCredentials, ...]:
    """Every identity this dispatcher connects, in declared order.

    With no declaration this is ``default`` alone, and no environment is read
    beyond the settings above. ``default`` always comes from those settings,
    because the parser pins it to the legacy names. Any other identity's tokens
    come from the env names its declaration gives; one whose app or bot token
    is blank is logged and left out, so it cannot keep the others from
    connecting.
    """

    if not config.slack_identities:
        return (default_identity_credentials(config),)
    env: Mapping[str, str] = os.environ if environ is None else environ
    resolved: list[SlackIdentityCredentials] = []
    for declared in config.slack_identities:
        if declared.name == DEFAULT_IDENTITY:
            resolved.append(default_identity_credentials(config))
            continue
        app_token = env.get(declared.app_token_env) or ""
        bot_token = env.get(declared.bot_token_env) or ""
        empty = [
            name
            for name, value in (
                (declared.app_token_env, app_token),
                (declared.bot_token_env, bot_token),
            )
            if not value.strip()
        ]
        if empty:
            logger.error(
                "Slack identity %s will not connect: %s is empty",
                declared.name,
                " and ".join(empty),
            )
            continue
        signing = (
            env.get(declared.signing_secret_env) or ""
            if declared.signing_secret_env is not None
            else ""
        )
        resolved.append(
            SlackIdentityCredentials(
                name=declared.name,
                app_token=app_token,
                bot_token=bot_token,
                signing_secret=signing,
            )
        )
    return tuple(resolved)


def minted_adapter(slack_identity: str) -> str | None:
    """The ``ReplyHandle.adapter`` a turn arriving on ``slack_identity`` carries.

    ``default`` still mints none: every reader resolves a missing Slack adapter
    to ``default`` (``aci_protocol.turn.route_identity``), and a worker from
    before the route triple resolves only that form. #3146 stores the name and
    moves this writer to it.
    """

    return None if slack_identity == DEFAULT_IDENTITY else slack_identity


def delivery_key(slack_delivery_id: str, slack_identity: str) -> str:
    """The idempotency key for one delivery on one identity's connection.

    ``default`` keeps the bare Slack id, so a stock install claims and enqueues
    exactly the keys it always did. Any other identity's key carries its name,
    so one message delivered to two identities is two turns whether or not
    Slack's ids differ across apps. The name goes last, so no key starts with
    ``approval-``, the prefix of an approval resume id.
    """

    if slack_identity == DEFAULT_IDENTITY:
        return slack_delivery_id
    return f"{slack_delivery_id}:{slack_identity}"


def bot_ids_from_auth_test(response: object) -> SlackBotIds | None:
    """The ids an ``ok`` ``auth.test`` answer reports, or None for any other answer."""

    getter = getattr(response, "get", None)
    if not callable(getter):
        return None
    try:
        if getter("ok") is not True:
            return None
        values = {key: getter(key) for key in ("team_id", "app_id", "bot_id", "user_id")}
    except Exception:
        return None

    def text(key: str) -> str | None:
        value = values[key]
        return value if isinstance(value, str) and value else None

    return SlackBotIds(
        team_id=text("team_id"),
        app_id=text("app_id"),
        bot_id=text("bot_id"),
        bot_user_id=text("user_id"),
    )
