"""Provider installations: connected external accounts (#2909, ADR 0155 step 4).

A provider installation is one connected external account, such as one Slack
workspace. Its credential lives in the deployment's secret store; the row holds
only a reference to it (``env:NAME`` or ``k8s-secret:name/key``). This module
and the table's CHECK both enforce that grammar and refuse the shapes of
well-known credentials, and the router's 422s never echo a submitted value.
That is best effort against a pasted token, not a proof: no syntax can show
a string is not a secret.

Today's single static Slack app is represented by one row created at API boot
(``bootstrap_static_slack``), gated on the same ``slack_bot_token`` setting the
approver usergroup client already reads, so a Slack-free install gets no row
and an existing self-host install needs no manual step. Its
``external_account_id`` is the placeholder ``static`` until an administrator
PATCHes in the real Slack ``team_id``: no service learns it today, and finding
it out would mean a Slack call at boot.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .models import (
    DEFAULT_TENANT_ID,
    PROVIDER_REFERENCE_DENY_PATTERN,
    PROVIDER_REFERENCE_MAX_LENGTH,
    PROVIDER_REFERENCE_PATTERN,
    ProviderInstallation,
)
from .schemas import ProviderInstallationCreate, ProviderInstallationUpdate

_LOG = logging.getLogger("curie_api.provider_installations")

# Fixed, like the default tenant, so replicas booting together collide on the
# primary key rather than each inserting a row, and so the row keeps its
# identity after an administrator renames ``external_account_id``.
STATIC_SLACK_INSTALLATION_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
STATIC_SLACK_EXTERNAL_ACCOUNT_ID = "static"
STATIC_SLACK_CREDENTIAL_REF = "env:SLACK_BOT_TOKEN"
# How often boot re-checks for the table when it started below 0053.
BOOTSTRAP_RETRY_INTERVAL_S = 2.0

REFERENCE_RE = re.compile(PROVIDER_REFERENCE_PATTERN.removeprefix("^").removesuffix("$"))
_REFERENCE_DENY_RE = re.compile(PROVIDER_REFERENCE_DENY_PATTERN)
_REFERENCE_FIELDS = ("credential_ref", "webhook_verification_ref")


class InstallationConflict(Exception):
    """The (tenant, provider, external account) key is already taken."""


class InstallationInvalid(Exception):
    """A request the table rejects; the message never includes a submitted value."""


def is_reference(value: str) -> bool:
    """The API's side of the table's reference CHECK: a pointer, never a value."""
    return (
        len(value) <= PROVIDER_REFERENCE_MAX_LENGTH
        and REFERENCE_RE.fullmatch(value) is not None
        and _REFERENCE_DENY_RE.search(value) is None
    )


def check_reference(field: str, value: str | None) -> None:
    if value is None:
        return
    if not is_reference(value):
        raise InstallationInvalid(
            f"{field} must be a secret reference (env:NAME or k8s-secret:name/key); "
            "the submitted value is not echoed"
        )


# Only named constraints are translated; anything else is a bug and re-raises.
_CONSTRAINT_ERRORS: dict[str, tuple[type[Exception], str]] = {
    "provider_installations_tenant_provider_external_key": (
        InstallationConflict,
        "a provider installation for this tenant, provider and external account already exists",
    ),
    "provider_installations_tenant_id_fkey": (
        InstallationInvalid,
        "tenant_id does not name a tenant",
    ),
    "provider_installations_installer_fkey": (
        InstallationInvalid,
        "installed_by_principal_id is not a principal of this tenant",
    ),
    # A binding arriving through this installation keeps it: unbind first.
    "agent_channels_provider_installation_fkey": (
        InstallationConflict,
        "a channel binding still arrives through this provider installation",
    ),
}


def _translate(exc: IntegrityError) -> Exception | None:
    constraint = getattr(exc.orig.__cause__, "constraint_name", None) if exc.orig else None
    if constraint not in _CONSTRAINT_ERRORS:
        return None
    kind, message = _CONSTRAINT_ERRORS[constraint]
    return kind(message)


async def _commit(session: AsyncSession) -> None:
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        translated = _translate(exc)
        if translated is None:
            raise
        # `from None`: the IntegrityError carries the statement's parameters.
        raise translated from None


async def create_installation(
    session: AsyncSession, data: ProviderInstallationCreate
) -> ProviderInstallation:
    for field in _REFERENCE_FIELDS:
        check_reference(field, getattr(data, field))
    installation = ProviderInstallation(
        tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
        provider=data.provider,
        external_account_id=data.external_account_id,
        display_name=data.display_name,
        credential_ref=data.credential_ref,
        scopes=list(data.scopes),
        webhook_verification_ref=data.webhook_verification_ref,
        status=data.status,
        installed_by_principal_id=data.installed_by_principal_id,
        disconnected_at=datetime.now(UTC) if data.status == "disconnected" else None,
    )
    session.add(installation)
    await _commit(session)
    await session.refresh(installation)
    return installation


async def list_installations(
    session: AsyncSession,
    *,
    provider: str | None = None,
    tenant_id: uuid.UUID | None = None,
) -> list[ProviderInstallation]:
    query = select(ProviderInstallation).order_by(
        ProviderInstallation.installed_at, ProviderInstallation.id
    )
    if provider is not None:
        query = query.where(ProviderInstallation.provider == provider)
    if tenant_id is not None:
        query = query.where(ProviderInstallation.tenant_id == tenant_id)
    return list((await session.scalars(query)).all())


async def get_installation(
    session: AsyncSession, installation_id: uuid.UUID
) -> ProviderInstallation | None:
    return await session.get(ProviderInstallation, installation_id)


async def update_installation(
    session: AsyncSession,
    installation: ProviderInstallation,
    data: ProviderInstallationUpdate,
) -> ProviderInstallation:
    fields = data.model_fields_set
    for field in _REFERENCE_FIELDS:
        if field in fields:
            check_reference(field, getattr(data, field))
    for field in (
        "display_name",
        "external_account_id",
        "credential_ref",
        "scopes",
        "webhook_verification_ref",
    ):
        if field in fields:
            setattr(installation, field, getattr(data, field))
    if "status" in fields and data.status != installation.status:
        # disconnected_at marks exactly the disconnected span.
        installation.disconnected_at = datetime.now(UTC) if data.status == "disconnected" else None
        installation.status = data.status  # type: ignore[assignment]
    await _commit(session)
    await session.refresh(installation)
    return installation


async def delete_installation(session: AsyncSession, installation: ProviderInstallation) -> None:
    await session.delete(installation)
    await _commit(session)


# --- the static Slack app --------------------------------------------------

_TABLE_EXISTS = text("SELECT to_regclass('curie.provider_installations') IS NOT NULL")

# Keyed on "any Slack row in the default tenant" rather than on the placeholder
# account id: a renamed static row, or one an administrator created in its
# place, means the static app is already represented. A disconnected row still
# counts, so disconnecting it sticks; deleting every Slack row does not, and
# the next boot recreates it while the token is still configured.
_INSERT_STATIC_SLACK = text(
    """
    INSERT INTO curie.provider_installations
        (id, tenant_id, provider, external_account_id, credential_ref, status)
    SELECT :id, :tenant_id, 'slack', :external_account_id, :credential_ref, 'connected'
    WHERE NOT EXISTS (
        SELECT 1 FROM curie.provider_installations
        WHERE tenant_id = :tenant_id AND provider = 'slack'
    )
    ON CONFLICT DO NOTHING
    RETURNING id
    """
)


async def bootstrap_static_slack(
    sessionmaker: async_sessionmaker[AsyncSession], settings: Settings
) -> bool:
    """Ensure the static Slack app has its installation row.

    Returns False only when the table does not exist yet, which is the one
    case worth retrying: this image may serve any schema from its
    ``schema_min``, and a fresh install applies migrations one transaction at
    a time, so boot can land between 0052 and 0053.
    """

    if not settings.slack_bot_token:
        return True
    async with sessionmaker() as session:
        if not (await session.execute(_TABLE_EXISTS)).scalar_one():
            return False
        inserted = (
            await session.execute(
                _INSERT_STATIC_SLACK,
                {
                    "id": STATIC_SLACK_INSTALLATION_ID,
                    "tenant_id": DEFAULT_TENANT_ID,
                    "external_account_id": STATIC_SLACK_EXTERNAL_ACCOUNT_ID,
                    "credential_ref": STATIC_SLACK_CREDENTIAL_REF,
                },
            )
        ).scalar_one_or_none()
        await session.commit()
    if inserted is not None:
        _LOG.info("bootstrapped static Slack provider installation id=%s", inserted)
    return True


class _Warned:
    """Whether this process has logged a bootstrap failure yet."""

    def __init__(self) -> None:
        self.done = False


async def _attempt(
    sessionmaker: async_sessionmaker[AsyncSession], settings: Settings, warned: _Warned
) -> bool:
    try:
        return await bootstrap_static_slack(sessionmaker, settings)
    except Exception as exc:  # noqa: BLE001 -- boot must not fail on this row
        # One warning per process, whenever the first failure happens: a
        # persistent one retries quietly rather than logging every interval.
        # The class only: nothing here needs the statement's parameters.
        if not warned.done:
            warned.done = True
            _LOG.warning(
                "static Slack provider installation bootstrap failed: %s",
                type(exc).__name__,
            )
        return False


async def _retry_until_done(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    interval_s: float,
    warned: _Warned,
) -> None:
    while not await _attempt(sessionmaker, settings, warned):
        await asyncio.sleep(interval_s)


async def start_static_slack_bootstrap(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    interval_s: float = BOOTSTRAP_RETRY_INTERVAL_S,
) -> asyncio.Task[Any] | None:
    """Try once inline; if that cannot finish, keep retrying in the background.

    Returns the retry task for the lifespan to cancel on shutdown, or None when
    the inline attempt finished.
    """

    warned = _Warned()
    if await _attempt(sessionmaker, settings, warned):
        return None
    # Covers both causes: the table not there yet (no other log line) and a
    # failed attempt (already logged above, by class).
    _LOG.warning(
        "static Slack provider installation not yet recorded; retrying every %ss",
        interval_s,
    )
    return asyncio.create_task(_retry_until_done(sessionmaker, settings, interval_s, warned))
