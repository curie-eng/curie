"""Credentials the gateway holds for directory reads alone (@spec GATEWAY-USER-EMAIL-7..9).

``CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES`` declares each lookup identity's
name, provider kind and the env var holding its credential, never the
credential itself. Its env names are pinned to an indexed prefix so custody
checks and the worker's sandbox filter drop them without reading this
declaration (@spec GATEWAY-USER-EMAIL-10).

Which kinds exist is the composition root's adapter registry, not this
module: a new kind adds an adapter and a registry entry and leaves this
declaration's shape unchanged.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Collection, Mapping
from typing import Annotated

from aci_protocol.slack_identities import IDENTITY_NAME_MAX_LENGTH, IDENTITY_NAME_PATTERN
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER, DEFAULT_IDENTITY
from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field
from pydantic_settings import NoDecode

from .ports.reads import UserEmailSource
from .token_custody import LOOKUP_TOKEN_ENV_PREFIX

LOOKUP_IDENTITIES_ENV = "CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES"
_RESERVED = frozenset({DEFAULT_IDENTITY, CLUSTER_MESSAGE_ADAPTER})
_TOKEN_ENV = re.compile(re.escape(LOOKUP_TOKEN_ENV_PREFIX) + r"(0|[1-9][0-9]*)")


class LookupIdentity(BaseModel):
    """One lookup identity: its name, provider kind and where its credential is."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(
        strict=True, pattern=IDENTITY_NAME_PATTERN, max_length=IDENTITY_NAME_MAX_LENGTH
    )
    kind: str = Field(strict=True, min_length=1)
    token_env: str = Field(strict=True)


def _check(identities: tuple[LookupIdentity, ...]) -> tuple[LookupIdentity, ...]:
    names: set[str] = set()
    envs: set[str] = set()
    for identity in identities:
        if identity.name in _RESERVED:
            raise ValueError(f"{LOOKUP_IDENTITIES_ENV} may not name {identity.name!r}")
        if identity.name in names:
            raise ValueError(f"{LOOKUP_IDENTITIES_ENV} repeats the name {identity.name!r}")
        if not _TOKEN_ENV.fullmatch(identity.token_env):
            raise ValueError(
                f"lookup identity {identity.name!r} must read its credential from "
                f"{LOOKUP_TOKEN_ENV_PREFIX}<index>"
            )
        if identity.token_env in envs:
            raise ValueError(f"{LOOKUP_IDENTITIES_ENV} repeats the env {identity.token_env}")
        names.add(identity.name)
        envs.add(identity.token_env)
    return identities


def _decode(value: object) -> object:
    if isinstance(value, str):
        if not value.strip():
            return ()
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            raise ValueError(f"{LOOKUP_IDENTITIES_ENV} must be a JSON list")
        return decoded
    return value


#: The gateway Settings field type for ``CURIE_CHANNEL_GATEWAY_LOOKUP_IDENTITIES``.
LookupIdentities = Annotated[
    tuple[LookupIdentity, ...],
    NoDecode,
    BeforeValidator(_decode),
    AfterValidator(_check),
]

#: Builds a kind's directory source for one lookup identity from its name and credential.
LookupSourceFactory = Callable[[str, str], UserEmailSource]


def lookup_email_sources(
    identities: tuple[LookupIdentity, ...],
    *,
    registry: Mapping[str, LookupSourceFactory],
    declared: Collection[str],
    environ: Mapping[str, str] | None = None,
) -> dict[str, UserEmailSource]:
    """Each lookup identity's directory source, or ValueError at boot.

    ``declared`` holds the Slack identity names a lookup identity may not
    reuse. A declared credential that is unset or blank refuses rather than
    being skipped: the operator asked for it by name. No error carries a
    credential.
    """

    env: Mapping[str, str] = os.environ if environ is None else environ
    sources: dict[str, UserEmailSource] = {}
    for identity in identities:
        factory = registry.get(identity.kind)
        if factory is None:
            raise ValueError(
                f"lookup identity {identity.name!r} has unsupported kind {identity.kind!r}; "
                f"supported kinds: {', '.join(sorted(registry))}"
            )
        if identity.name in declared:
            raise ValueError(
                f"lookup identity {identity.name!r} repeats a declared Slack identity's name"
            )
        token = env.get(identity.token_env) or ""
        if not token.strip():
            raise ValueError(f"lookup identity {identity.name!r}: {identity.token_env} is empty")
        sources[identity.name] = factory(identity.name, token)
    return sources


class DirectoryEmails:
    """Routes a user email read to a lookup identity's source, else the declared one."""

    def __init__(self, declared: UserEmailSource, lookups: Mapping[str, UserEmailSource]) -> None:
        self._declared = declared
        self._lookups = dict(lookups)

    async def email_of(self, *, identity: str, user_id: str, workspace: str) -> str:
        source = self._lookups.get(identity, self._declared)
        return await source.email_of(identity=identity, user_id=user_id, workspace=workspace)


_CONFORMS: type[UserEmailSource] = DirectoryEmails
