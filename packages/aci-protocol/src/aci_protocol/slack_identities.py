"""The Slack identities an installation declares (ADR-0168 decision 1).

The chart renders ``CURIE_SLACK_IDENTITIES`` into the dispatcher, the worker
and the API from one helper (``charts/curie/templates/_slack-identities.tpl``).
It names each identity and the env vars holding its tokens, never a token. An
absent or blank value means the one Slack app, ``default``, under the legacy
``SLACK_*`` names.

The env names are pinned, not free: ``default`` uses the legacy names and every
other identity an indexed ``CURIE_SLACK_*__<n>`` name. That is what lets the
worker's sandbox filter drop every Slack token by exact name and prefix without
reading this declaration.
"""

from __future__ import annotations

import json
import re
from typing import Annotated

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field
from pydantic_settings import NoDecode

from .turn import CLUSTER_MESSAGE_ADAPTER, DEFAULT_IDENTITY

SLACK_IDENTITIES_ENV = "CURIE_SLACK_IDENTITIES"

LEGACY_APP_TOKEN_ENV = "SLACK_APP_TOKEN"
LEGACY_BOT_TOKEN_ENV = "SLACK_BOT_TOKEN"
LEGACY_SIGNING_SECRET_ENV = "SLACK_SIGNING_SECRET"

APP_TOKEN_ENV_PREFIX = "CURIE_SLACK_APP_TOKEN__"
BOT_TOKEN_ENV_PREFIX = "CURIE_SLACK_BOT_TOKEN__"
SIGNING_SECRET_ENV_PREFIX = "CURIE_SLACK_SIGNING_SECRET__"
#: Every indexed Slack credential env name starts with one of these.
SLACK_CREDENTIAL_ENV_PREFIXES: tuple[str, ...] = (
    APP_TOKEN_ENV_PREFIX,
    BOT_TOKEN_ENV_PREFIX,
    SIGNING_SECRET_ENV_PREFIX,
)

#: Hyphen-separated lowercase runs: the binding schema's ``adapter`` rule,
#: without its underscore, so every declared name is one a binding can carry.
#: ``charts/curie/templates/_slack-identities.tpl`` spells the same pattern,
#: and ``charts/curie/ci/slack-identities-assertions.sh`` checks that it does.
IDENTITY_NAME_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
IDENTITY_NAME_MAX_LENGTH = 40

_INDEX = r"(0|[1-9][0-9]*)"


class SlackIdentity(BaseModel):
    """One declared Slack identity: its name and where its tokens are."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(
        strict=True, pattern=IDENTITY_NAME_PATTERN, max_length=IDENTITY_NAME_MAX_LENGTH
    )
    app_token_env: str = Field(strict=True)
    bot_token_env: str = Field(strict=True)
    signing_secret_env: str | None = Field(default=None, strict=True)


def _check_env_names(identity: SlackIdentity) -> None:
    if identity.name == DEFAULT_IDENTITY:
        allowed = {
            "app_token_env": (LEGACY_APP_TOKEN_ENV,),
            "bot_token_env": (LEGACY_BOT_TOKEN_ENV,),
            "signing_secret_env": (None, LEGACY_SIGNING_SECRET_ENV),
        }
        for field, names in allowed.items():
            value = getattr(identity, field)
            if value not in names:
                raise ValueError(
                    f"Slack identity 'default' must read {field} from "
                    f"{' or '.join(repr(n) for n in names)}, got {value!r}"
                )
        return
    patterns = {
        "app_token_env": APP_TOKEN_ENV_PREFIX,
        "bot_token_env": BOT_TOKEN_ENV_PREFIX,
        "signing_secret_env": SIGNING_SECRET_ENV_PREFIX,
    }
    for field, prefix in patterns.items():
        value = getattr(identity, field)
        if value is None and field == "signing_secret_env":
            continue
        if value is None or not re.fullmatch(re.escape(prefix) + _INDEX, value):
            raise ValueError(
                f"Slack identity {identity.name!r} must read {field} from "
                f"{prefix}<index>, got {value!r}"
            )


def _check_declarations(identities: tuple[SlackIdentity, ...]) -> tuple[SlackIdentity, ...]:
    if not identities:
        return identities
    names = [identity.name for identity in identities]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(f"{SLACK_IDENTITIES_ENV} repeats identity names {repeated}")
    if CLUSTER_MESSAGE_ADAPTER in names:
        raise ValueError(
            f"{SLACK_IDENTITIES_ENV} declares an identity named {CLUSTER_MESSAGE_ADAPTER!r}; "
            f"{CLUSTER_MESSAGE_ADAPTER!r} is a reserved delivery selector, not an identity"
        )
    if DEFAULT_IDENTITY not in names:
        raise ValueError(
            f"{SLACK_IDENTITIES_ENV} declares no {DEFAULT_IDENTITY!r} identity; a Slack "
            f"route that names none means {DEFAULT_IDENTITY!r}"
        )
    env_names: list[str] = []
    for identity in identities:
        _check_env_names(identity)
        env_names += [identity.app_token_env, identity.bot_token_env]
        if identity.signing_secret_env is not None:
            env_names.append(identity.signing_secret_env)
    shared = sorted({name for name in env_names if env_names.count(name) > 1})
    if shared:
        raise ValueError(f"{SLACK_IDENTITIES_ENV} gives two identities the env names {shared}")
    return identities


def _decode(value: object) -> object:
    if isinstance(value, str):
        if not value.strip():
            return ()
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            raise ValueError(f"{SLACK_IDENTITIES_ENV} must be a JSON list")
        return decoded
    return value


#: The Settings field type all three services declare for ``CURIE_SLACK_IDENTITIES``.
SlackIdentities = Annotated[
    tuple[SlackIdentity, ...],
    NoDecode,
    BeforeValidator(_decode),
    AfterValidator(_check_declarations),
]


def declared_slack_identity_names(identities: tuple[SlackIdentity, ...]) -> frozenset[str]:
    """The identity names a Slack binding may use: ``default`` alone when none are declared."""

    if not identities:
        return frozenset({DEFAULT_IDENTITY})
    return frozenset(identity.name for identity in identities)
