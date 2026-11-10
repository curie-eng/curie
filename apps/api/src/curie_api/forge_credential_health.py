"""Whether each configured forge accepts its credential now (#4630).

`credential_health` probes every code host and tracker entry concurrently with
one existing read through the real adapter, reading the credential as it is
now (a static token file is read again by the adapter the probe builds):

1. a ``static_token`` code host resolves the first repository a binding names
   on it (`CodeHost.resolve_repository`);
2. a ``static_token`` binding-scoped tracker (GitLab, Jira Cloud) verifies its
   credential on its first binding (``verify_credential``, a read);
3. an entry no binding uses has nothing to read: ``unknown``, ``no_probe``;
4. a re-minting credential (GitHub App, OAuth client credentials) is not
   probed: ``unknown``, ``not_probed``.

A successful read is ``accepted``; a refused credential is ``rejected``
(``unauthorized``); anything else is ``unknown`` with a fixed reason, so an
entry is never reported accepted without an authenticated read. The probes
only read: no cursor, comment, label or other write. Nothing in a report or a
log carries credential bytes, a header or a response body; an unexpected
failure is logged by its class name alone.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

import httpx
from pydantic import BaseModel

from .forges.config import BindingConfig, CodeHostConfig, RepositoryBindingConfig, TrackerConfig
from .forges.connections import BindingTrackerConnection
from .forges.errors import CredentialUnreadable, NotFound, Unauthorized, Unavailable
from .forges.registry import ForgeRegistry

logger = logging.getLogger(__name__)

Section = Literal["code_host", "tracker"]
Authentication = Literal["accepted", "rejected", "unknown"]

STATIC_TOKEN = "static_token"


class ForgeCredentialHealth(BaseModel):
    """One configured entry's credential, as its forge answered a read now."""

    section: Section
    name: str
    kind: str
    credential_type: str
    authentication: Authentication
    # Why the authentication is not accepted: ``unauthorized``,
    # ``credential_unreadable``, ``not_found``, ``unavailable``, ``error``,
    # ``no_probe`` or ``not_probed``. None when accepted.
    reason: str | None
    observed_at: datetime


class ForgeCredentialHealthReport(BaseModel):
    """``GET /forges/credentials/health``: every configured entry's probe."""

    observed_at: datetime
    entries: list[ForgeCredentialHealth]


@runtime_checkable
class _ClientSource(Protocol):
    def client(self) -> httpx.AsyncClient: ...


@runtime_checkable
class _CredentialVerifier(Protocol):
    async def verify_credential(self) -> None: ...


class _Read(Protocol):
    async def __call__(self) -> None: ...


class _NotProbed(Exception):
    """The adapter has no read that checks the credential alone."""


def _now() -> datetime:
    return datetime.now(UTC)


def _entry(
    section: Section,
    config: CodeHostConfig | TrackerConfig,
    authentication: Authentication,
    reason: str | None,
) -> ForgeCredentialHealth:
    return ForgeCredentialHealth(
        section=section,
        name=config.name,
        kind=config.kind,
        credential_type=config.credential.type,
        authentication=authentication,
        reason=reason,
        observed_at=_now(),
    )


async def _classified(
    section: Section, config: CodeHostConfig | TrackerConfig, read: _Read
) -> ForgeCredentialHealth:
    """Run ``read`` and classify its outcome; never raises."""

    authentication: Authentication = "unknown"
    reason: str | None
    try:
        await read()
    except _NotProbed:
        reason = "not_probed"
    except CredentialUnreadable:
        reason = "credential_unreadable"
    except Unauthorized:
        authentication, reason = "rejected", "unauthorized"
    except NotFound:
        reason = "not_found"
    except Unavailable:
        reason = "unavailable"
    except Exception as error:  # noqa: BLE001  a probe never fails the report
        # The class alone: an exception's text could quote a request.
        failure = type(error).__name__
        reason = "error"
        logger.warning("forge credential probe of %s %r failed: %s", section, config.name, failure)
    else:
        authentication, reason = "accepted", None
    return _entry(section, config, authentication, reason)


def _first_repository(registry: ForgeRegistry, code_host: str) -> RepositoryBindingConfig | None:
    for binding in registry.config.bindings:
        for repository in binding.repos:
            if repository.code_host == code_host:
                return repository
    return None


def _first_binding(registry: ForgeRegistry, tracker: str) -> BindingConfig | None:
    return next(
        (binding for binding in registry.config.bindings if binding.tracker == tracker), None
    )


async def _code_host(registry: ForgeRegistry, config: CodeHostConfig) -> ForgeCredentialHealth:
    if config.credential.type != STATIC_TOKEN:
        return _entry("code_host", config, "unknown", "not_probed")
    connection = registry.code_host(config.name)
    repository = _first_repository(registry, config.name)
    if repository is None:
        return _entry("code_host", config, "unknown", "no_probe")
    if not isinstance(connection, _ClientSource):
        return _entry("code_host", config, "unknown", "not_probed")
    project_id = repository.project_id

    async def read() -> None:
        async with connection.client() as client:
            await connection.adapter(client).resolve_repository(project_id)

    return await _classified("code_host", config, read)


async def _tracker(registry: ForgeRegistry, config: TrackerConfig) -> ForgeCredentialHealth:
    if config.credential.type != STATIC_TOKEN:
        return _entry("tracker", config, "unknown", "not_probed")
    connection = registry.tracker(config.name)
    if not isinstance(connection, BindingTrackerConnection) or not isinstance(
        connection, _ClientSource
    ):
        return _entry("tracker", config, "unknown", "not_probed")
    binding = _first_binding(registry, config.name)
    if binding is None:
        return _entry("tracker", config, "unknown", "no_probe")
    bound = binding

    async def read() -> None:
        async with connection.client() as client:
            tracker = connection.for_binding(client, bound)
            if not isinstance(tracker, _CredentialVerifier):
                raise _NotProbed
            await tracker.verify_credential()

    return await _classified("tracker", config, read)


async def credential_health(registry: ForgeRegistry) -> ForgeCredentialHealthReport:
    """Every configured entry's credential health, probed concurrently."""

    probes = [_code_host(registry, entry) for entry in registry.config.code_hosts]
    probes += [_tracker(registry, entry) for entry in registry.config.trackers]
    entries = list(await asyncio.gather(*probes))
    return ForgeCredentialHealthReport(observed_at=_now(), entries=entries)
