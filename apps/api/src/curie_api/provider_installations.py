"""Provider installations: one row per channel identity (#2909, ADR 0166 step 4).

A provider installation is one channel identity -- one bot speaking through
one connected account -- not one connected account (ADR 0168 decision 1). Its
credential lives in the deployment's secret store; the row holds only a
reference to it (``env:NAME`` or ``k8s-secret:name/key``). This module and the
table's CHECK both enforce that grammar and refuse the shapes of well-known
credentials, and the router's 422s never echo a submitted value. That is best
effort against a pasted token, not a proof: no syntax can show a string is not
a secret.

Today's statically configured Slack identities are each represented by one row
created at API boot (``bootstrap_static_slack``): the legacy single app, named
``default`` and gated on ``settings.slack_bot_token``, and every identity
``settings.slack_identities`` declares (ADR 0168 decision 1), each gated on its
own bot token env var being non-blank. A Slack-free install gets no rows, and
an existing self-host install needs no manual step. Every bootstrapped row's
``external_account_id`` is the placeholder ``static`` until an administrator
PATCHes in the real Slack ``team_id``: no service learns it today, and finding
it out would mean a Slack call at boot.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from aci_protocol.slack_identities import (
    LEGACY_APP_TOKEN_ENV,
    LEGACY_BOT_TOKEN_ENV,
    LEGACY_SIGNING_SECRET_ENV,
)
from aci_protocol.turn import DEFAULT_IDENTITY
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .config import Settings
from .models import (
    PROVIDER_REFERENCE_DENY_PATTERN,
    PROVIDER_REFERENCE_MAX_LENGTH,
    PROVIDER_REFERENCE_PATTERN,
    ProviderInstallation,
)
from .schemas import ProviderInstallationCreate, ProviderInstallationUpdate

_LOG = logging.getLogger("curie_api.provider_installations")

# Migration 0051's auto-provisioned tenant.
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
# Fixed, like the default tenant, so replicas booting together collide on the
# primary key rather than each inserting a row, and so the row keeps its
# identity after an administrator renames ``external_account_id``. Only the
# "default" identity gets a fixed id; any other declared identity gets a fresh
# one, since the (tenant, provider, name) unique key is what makes a race
# between replicas safe for it (see ``_INSERT_SLACK_IDENTITY``).
STATIC_SLACK_INSTALLATION_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
STATIC_SLACK_EXTERNAL_ACCOUNT_ID = "static"
# How often boot re-checks for the table when it started below this migration,
# and how often a persistent failure re-warns rather than retrying silently.
BOOTSTRAP_RETRY_INTERVAL_S = 2.0
BOOTSTRAP_WARN_PERIOD_S = 60.0

REFERENCE_RE = re.compile(PROVIDER_REFERENCE_PATTERN.removeprefix("^").removesuffix("$"))
_REFERENCE_DENY_RE = re.compile(PROVIDER_REFERENCE_DENY_PATTERN)
_REFERENCE_FIELDS = ("credential_ref", "webhook_verification_ref")

# PROVIDER_REFERENCE_DENY_PATTERN only refuses a known prefix after a ``:`` or
# ``/`` (a reference is always ``env:NAME`` or ``k8s-secret:name/key``), so a
# bare pasted token -- no scheme, nothing before it -- would not match. This
# drops that requirement, since ``attributes`` has no fixed grammar to anchor
# the known prefixes to.
_ATTRIBUTES_DENY_RE = re.compile(
    r"(xox[a-z]-|xoxe\.|xapp-|gh[pousr]_|github_pat_|sk-|sk_live_|rk_live_|lin_api_|AIza|eyJ)"
    r"|(AKIA|ASIA)[A-Z0-9]{16}"
    r"|[0-9a-fA-F]{32}"
)


class InstallationConflict(Exception):
    """The (tenant, provider, name) key is already taken."""


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


def _check_attributes_deny(value: Any) -> None:
    """Refuse a well-known credential shape anywhere inside ``attributes``.

    Unlike ``credential_ref``/``webhook_verification_ref``, a value here is not
    always a reference -- ADR 0168 decision 1 also puts plain identifiers here
    (a Slack team, app or bot user id), so this cannot require the reference
    grammar. It can still refuse the same credential-shaped substrings, minus
    the reference grammar's requirement that a known prefix follow a ``:`` or
    ``/`` -- a bare pasted token has neither -- recursively, so a value pasted
    anywhere in this JSON bag is refused rather than stored and echoed back by
    every read.
    """

    if isinstance(value, str):
        if len(value) > PROVIDER_REFERENCE_MAX_LENGTH or _ATTRIBUTES_DENY_RE.search(value):
            raise InstallationInvalid(
                "attributes must not contain a well-known credential shape; "
                "the submitted value is not echoed"
            )
    elif isinstance(value, dict):
        for item in value.values():
            _check_attributes_deny(item)
    elif isinstance(value, list):
        for item in value:
            _check_attributes_deny(item)


# Only named constraints are translated; anything else is a bug and re-raises.
_CONSTRAINT_ERRORS: dict[str, tuple[type[Exception], str]] = {
    "provider_installations_tenant_provider_name_key": (
        InstallationConflict,
        "a provider installation for this tenant, provider and name already exists",
    ),
    "provider_installations_tenant_id_fkey": (
        InstallationInvalid,
        "tenant_id does not name a tenant",
    ),
    "provider_installations_installer_fkey": (
        InstallationInvalid,
        "installed_by_principal_id is not a principal of this tenant",
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
    _check_attributes_deny(data.attributes)
    installation = ProviderInstallation(
        tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
        provider=data.provider,
        name=data.name,
        external_account_id=data.external_account_id,
        display_name=data.display_name,
        credential_ref=data.credential_ref,
        scopes=list(data.scopes),
        webhook_verification_ref=data.webhook_verification_ref,
        attributes=dict(data.attributes),
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
    if "attributes" in fields:
        _check_attributes_deny(data.attributes)
    for field in (
        "name",
        "display_name",
        "external_account_id",
        "credential_ref",
        "scopes",
        "webhook_verification_ref",
        "attributes",
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
    await session.commit()


# --- the declared Slack identities ------------------------------------------


class _DeclaredSlackIdentity:
    """One Slack identity this install has a bot token configured for."""

    __slots__ = ("name", "bot_token_env", "app_token_env", "signing_secret_env")

    def __init__(
        self, name: str, bot_token_env: str, app_token_env: str, signing_secret_env: str | None
    ) -> None:
        self.name = name
        self.bot_token_env = bot_token_env
        self.app_token_env = app_token_env
        self.signing_secret_env = signing_secret_env


def _declared_slack_identities(settings: Settings) -> list[_DeclaredSlackIdentity]:
    """Every Slack identity this install actually has a bot token for.

    The legacy ``default`` reads ``Settings`` directly (it predates
    ``CURIE_SLACK_IDENTITIES`` and keeps its own fixed env names). Every other
    declared identity's presence is read from the real environment the same
    way ``identities.slack_bot_tokens`` does, so a row is bootstrapped exactly
    when the dispatcher and worker would treat that identity as configured.
    """

    declared: list[_DeclaredSlackIdentity] = []
    if settings.slack_bot_token:
        declared.append(
            _DeclaredSlackIdentity(
                DEFAULT_IDENTITY,
                LEGACY_BOT_TOKEN_ENV,
                LEGACY_APP_TOKEN_ENV,
                LEGACY_SIGNING_SECRET_ENV,
            )
        )
    for identity in settings.slack_identities:
        if identity.name == DEFAULT_IDENTITY:
            continue  # Already covered above, under the legacy env names.
        token = os.environ.get(identity.bot_token_env) or ""
        if not token.strip():
            continue  # A blank declared token is not configured; no row for it.
        declared.append(
            _DeclaredSlackIdentity(
                identity.name,
                identity.bot_token_env,
                identity.app_token_env,
                identity.signing_secret_env,
            )
        )
    return declared


_TABLE_EXISTS = text("SELECT to_regclass('curie.provider_installations') IS NOT NULL")

# Keyed on "a row for this (tenant, provider, name)" rather than on the
# placeholder account id: a renamed row, or one an administrator created in its
# place, means that identity is already represented. A disconnected row still
# counts, so disconnecting it sticks; deleting it does not, and the next boot
# recreates it while its token is still configured. The conflict target is the
# same unique key, so two replicas racing to bootstrap this identity collide
# there instead of each inserting a row.
#
# Only safe for a declared identity whose id is freshly generated on every
# bootstrap attempt (every identity but ``default``, see ``bootstrap_static_slack``):
# a FRESH id never collides with a prior row's primary key, so the only
# conflict this INSERT can hit is the one this statement's ON CONFLICT already
# targets.
_INSERT_DECLARED_SLACK_IDENTITY = text(
    """
    INSERT INTO curie.provider_installations
        (id, tenant_id, provider, name, external_account_id, credential_ref,
         webhook_verification_ref, attributes, status)
    SELECT :id, :tenant_id, 'slack', :name, :external_account_id, :credential_ref,
           :webhook_verification_ref, CAST(:attributes AS jsonb), 'connected'
    WHERE NOT EXISTS (
        SELECT 1 FROM curie.provider_installations
        WHERE tenant_id = :tenant_id AND provider = 'slack' AND name = :existing_name
    )
    ON CONFLICT (tenant_id, provider, name) DO NOTHING
    RETURNING id
    """
)
# asyncpg's extended protocol infers one type per PARAMETER NAME, not per
# occurrence: reusing `:name` for both the inserted value and the `WHERE`
# comparison above made it deduce `character varying` at one site and `text`
# at the other, raising `AmbiguousParameterError` on every real execution.
# `:existing_name` is a distinct bind carrying the identical Python value,
# never a different one (see its single caller below).

# ``default`` reuses the SAME id (``STATIC_SLACK_INSTALLATION_ID``) on every
# bootstrap attempt, so a name-keyed dedup check is not enough: once an
# administrator renames that row, no row is named "default" any more, this
# would try to INSERT a second row at the SAME primary key, and the ON CONFLICT
# target above does not cover a conflict on a different index -- Postgres
# raises a raw IntegrityError that aborts the whole bootstrap call (#3040
# review). Checking `id = :id` as well as `name` covers both a prior
# bootstrap's row (however it has since been renamed) and an administrator's
# own pre-created "default" row (a different id); the ON CONFLICT target is
# the primary key, so two replicas racing to bootstrap this identity with the
# SAME fixed id collide there instead of each inserting a row. The one
# remaining race this does not cover -- an administrator's own POST for this
# exact name landing in the same instant as a boot's bootstrap attempt --
# would still raise; accepted as a lower-probability race than two pods
# booting together, which happens on every multi-replica rollout.
_INSERT_DEFAULT_SLACK_IDENTITY = text(
    """
    INSERT INTO curie.provider_installations
        (id, tenant_id, provider, name, external_account_id, credential_ref,
         webhook_verification_ref, attributes, status)
    SELECT :id, :tenant_id, 'slack', :name, :external_account_id, :credential_ref,
           :webhook_verification_ref, CAST(:attributes AS jsonb), 'connected'
    WHERE NOT EXISTS (
        SELECT 1 FROM curie.provider_installations
        WHERE id = :id
           OR (tenant_id = :tenant_id AND provider = 'slack' AND name = :existing_name)
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id
    """
)


async def bootstrap_static_slack(
    sessionmaker: async_sessionmaker[AsyncSession], settings: Settings
) -> bool:
    """Ensure every declared Slack identity has its installation row.

    Returns False only when the table does not exist yet, which is the one
    case worth retrying: this image may serve any schema from its
    ``schema_min``, and a fresh install applies migrations one transaction at
    a time, so boot can land between 0073 and 0074.
    """

    declared = _declared_slack_identities(settings)
    if not declared:
        return True
    async with sessionmaker() as session:
        if not (await session.execute(_TABLE_EXISTS)).scalar_one():
            return False
        inserted_ids: list[Any] = []
        for identity in declared:
            is_default = identity.name == DEFAULT_IDENTITY
            installation_id = STATIC_SLACK_INSTALLATION_ID if is_default else uuid.uuid4()
            statement = (
                _INSERT_DEFAULT_SLACK_IDENTITY if is_default else _INSERT_DECLARED_SLACK_IDENTITY
            )
            webhook_ref = (
                f"env:{identity.signing_secret_env}" if identity.signing_secret_env else None
            )
            attributes = json.dumps({"app_token_ref": f"env:{identity.app_token_env}"})
            result = (
                await session.execute(
                    statement,
                    {
                        "id": installation_id,
                        "tenant_id": DEFAULT_TENANT_ID,
                        "name": identity.name,
                        "existing_name": identity.name,
                        "external_account_id": STATIC_SLACK_EXTERNAL_ACCOUNT_ID,
                        "credential_ref": f"env:{identity.bot_token_env}",
                        "webhook_verification_ref": webhook_ref,
                        "attributes": attributes,
                    },
                )
            ).scalar_one_or_none()
            if result is not None:
                inserted_ids.append(result)
        await session.commit()
    for installation_id in inserted_ids:
        _LOG.info("bootstrapped Slack provider installation id=%s", installation_id)
    return True


class _Warned:
    """When this process last logged a bootstrap failure, if ever."""

    def __init__(self) -> None:
        self._last_warned_at: float | None = None

    def should_warn(self, period_s: float) -> bool:
        """True at most once per ``period_s``, so a persistent failure stays
        visible without flooding the log at ``interval_s``."""

        now = time.monotonic()
        if self._last_warned_at is None or now - self._last_warned_at >= period_s:
            self._last_warned_at = now
            return True
        return False


async def _attempt(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    warned: _Warned,
    warn_period_s: float,
) -> bool:
    try:
        return await bootstrap_static_slack(sessionmaker, settings)
    except Exception as exc:  # noqa: BLE001 -- boot must not fail on this row
        # A transient schema gap (expected to self-resolve, see the docstring
        # above) and a genuinely permanent misconfiguration (e.g. a missing
        # default tenant) raise the same way here and cannot be told apart
        # from the exception alone, so both keep retrying at ``interval_s``.
        # What changes is how often that gets logged: once per
        # ``warn_period_s``, not once ever and not every interval, so a
        # failure that never clears stays visible without flooding the log.
        # The class only: nothing here needs the statement's parameters.
        if warned.should_warn(warn_period_s):
            _LOG.warning(
                "static Slack provider installation bootstrap failed: %s",
                type(exc).__name__,
            )
        return False


async def _retry_until_done(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    interval_s: float,
    warn_period_s: float,
    warned: _Warned,
) -> None:
    while not await _attempt(sessionmaker, settings, warned, warn_period_s):
        await asyncio.sleep(interval_s)


async def start_static_slack_bootstrap(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    interval_s: float = BOOTSTRAP_RETRY_INTERVAL_S,
    warn_period_s: float = BOOTSTRAP_WARN_PERIOD_S,
) -> asyncio.Task[Any] | None:
    """Try once inline; if that cannot finish, keep retrying in the background.

    Returns the retry task for the lifespan to cancel on shutdown, or None when
    the inline attempt finished.
    """

    warned = _Warned()
    if await _attempt(sessionmaker, settings, warned, warn_period_s):
        return None
    # Covers both causes: the table not there yet (no other log line) and a
    # failed attempt (already logged above, by class).
    _LOG.warning(
        "static Slack provider installations not yet recorded; retrying every %ss",
        interval_s,
    )
    return asyncio.create_task(
        _retry_until_done(sessionmaker, settings, interval_s, warn_period_s, warned)
    )
