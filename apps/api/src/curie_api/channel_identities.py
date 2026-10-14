"""Channel identities: who Curie speaks as (#2909, ADR 0168 decision 1).

A channel identity is one bot speaking through one connected account, not the
connected account itself -- that is :mod:`.provider_installations`, keyed
separately per ADR 0193. Its credential lives in the deployment's secret
store; the row holds only a reference to it (``env:NAME`` or
``k8s-secret:name/key``). This module and the table's CHECK both enforce that
grammar and refuse the shapes of well-known credentials, and the router's
422s never echo a submitted value. That is best effort against a pasted
token, not a proof: no syntax can show a string is not a secret.

Today's statically configured Slack identities are each represented by one
row created at API boot (``bootstrap_static_slack``): the legacy single app,
named ``default`` and gated on ``settings.slack_bot_token``, and every
identity ``settings.slack_identities`` declares (ADR 0168 decision 1), each
gated on its own bot token env var being non-blank. A Slack-free install gets
no rows, and an existing self-host install needs no manual step. Every
bootstrapped row is created unattached (``provider_installation_id`` is NULL,
ADR 0193 decision 4): only the identity's own credential can later report
which installation it belongs to (#3039), so attaching it -- finding or
creating the matching ``provider_installations`` row -- is not this module's
job until then. Until #3039 lands, an operator attaches it through the admin
routes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
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
from .identity.slack import AUTH_TEST_KEY
from .models import (
    PROVIDER_REFERENCE_DENY_PATTERN,
    PROVIDER_REFERENCE_MAX_LENGTH,
    PROVIDER_REFERENCE_PATTERN,
    ChannelIdentity,
    ProviderInstallation,
)
from .schemas.channel_identities import ChannelIdentityCreate, ChannelIdentityUpdate

_LOG = logging.getLogger("curie_api.channel_identities")

# Migration 0051's auto-provisioned tenant.
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
# Fixed, like the default tenant, so the row keeps its identity after an
# administrator renames it, and two replicas booting together attempt the
# SAME row rather than two different ones. Only the "default" identity gets a
# fixed id; any other declared identity gets a fresh one every attempt.
STATIC_SLACK_IDENTITY_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
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


class IdentityConflict(Exception):
    """The (tenant, provider, name) key is already taken."""


class IdentityInvalid(Exception):
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
        raise IdentityInvalid(
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
            raise IdentityInvalid(
                "attributes must not contain a well-known credential shape; "
                "the submitted value is not echoed"
            )
    elif isinstance(value, dict):
        for item in value.values():
            _check_attributes_deny(item)
    elif isinstance(value, list):
        for item in value:
            _check_attributes_deny(item)


def check_attributes(attributes: dict[str, Any]) -> None:
    """The deny check for a whole ``attributes`` bag, for writers outside this module."""
    _check_attributes_deny(attributes)


# Only named constraints are translated; anything else is a bug and re-raises.
_CONSTRAINT_ERRORS: dict[str, tuple[type[Exception], str]] = {
    "channel_identities_tenant_provider_name_key": (
        IdentityConflict,
        "a channel identity for this tenant, provider and name already exists",
    ),
    "channel_identities_tenant_id_fkey": (
        IdentityInvalid,
        "tenant_id does not name a tenant",
    ),
    "channel_identities_installation_fkey": (
        IdentityInvalid,
        "provider_installation_id does not name an installation of this tenant and provider",
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


async def create_identity(
    session: AsyncSession, data: ChannelIdentityCreate
) -> ChannelIdentity:
    for field in _REFERENCE_FIELDS:
        check_reference(field, getattr(data, field))
    _check_attributes_deny(data.attributes)
    identity = ChannelIdentity(
        tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
        provider=data.provider,
        name=data.name,
        credential_ref=data.credential_ref,
        scopes=list(data.scopes),
        webhook_verification_ref=data.webhook_verification_ref,
        attributes=dict(data.attributes),
        status=data.status,
        provider_installation_id=data.provider_installation_id,
    )
    session.add(identity)
    await _commit(session)
    await session.refresh(identity)
    return identity


async def list_identities(
    session: AsyncSession,
    *,
    provider: str | None = None,
    tenant_id: uuid.UUID | None = None,
) -> list[ChannelIdentity]:
    query = select(ChannelIdentity).order_by(ChannelIdentity.created_at, ChannelIdentity.id)
    if provider is not None:
        query = query.where(ChannelIdentity.provider == provider)
    if tenant_id is not None:
        query = query.where(ChannelIdentity.tenant_id == tenant_id)
    return list((await session.scalars(query)).all())


async def get_identity(session: AsyncSession, identity_id: uuid.UUID) -> ChannelIdentity | None:
    return await session.get(ChannelIdentity, identity_id)


async def update_identity(
    session: AsyncSession,
    identity: ChannelIdentity,
    data: ChannelIdentityUpdate,
) -> ChannelIdentity:
    """Apply a partial update under the lock order every writer shares.

    The target installation (when the body names one) FOR SHARE, then the
    identity FOR UPDATE, re-read, so a concurrent Slack identity report
    (:mod:`.identity.attach`) and this update serialize rather than one
    overwriting the other's attributes. The reserved ``slack_auth_test``
    evidence is the report's alone: replacing ``attributes`` keeps it, a new
    ``credential_ref`` or ``name`` drops it (the token it described is gone
    until the dispatcher restarts and reports again), and a reattach recomputes
    ``installation_mismatch`` against it.
    """

    fields = data.model_fields_set
    for field in _REFERENCE_FIELDS:
        if field in fields:
            check_reference(field, getattr(data, field))
    if "attributes" in fields:
        _check_attributes_deny(data.attributes)

    target: ProviderInstallation | None = None
    if "provider_installation_id" in fields and data.provider_installation_id is not None:
        target = await session.scalar(
            select(ProviderInstallation)
            .where(ProviderInstallation.id == data.provider_installation_id)
            .with_for_update(read=True)
        )
    locked = await session.scalar(
        select(ChannelIdentity)
        .where(ChannelIdentity.id == identity.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked is None:
        await session.rollback()
        raise IdentityInvalid("the channel identity no longer exists")
    identity = locked

    attributes = dict(identity.attributes or {})
    evidence = attributes.get(AUTH_TEST_KEY)
    if "attributes" in fields:
        attributes = dict(data.attributes or {})
        if evidence is not None:
            attributes[AUTH_TEST_KEY] = evidence
    credential_changed = (
        "credential_ref" in fields and data.credential_ref != identity.credential_ref
    )
    # The evidence was reported under the old name, by the token it held.
    renamed = "name" in fields and data.name != identity.name
    if credential_changed or renamed:
        attributes.pop(AUTH_TEST_KEY, None)
        evidence = None
    for field in ("name", "credential_ref", "scopes", "webhook_verification_ref", "status"):
        if field in fields:
            setattr(identity, field, getattr(data, field))
    identity.attributes = attributes

    if "provider_installation_id" in fields:
        identity.provider_installation_id = data.provider_installation_id
        if data.provider_installation_id is None:
            # Detached: there is nothing left to mismatch.
            identity.installation_mismatch = False
        elif target is not None and isinstance(evidence, dict):
            # The evidence names ('', team_id); without evidence the flag stays.
            identity.installation_mismatch = (target.authority, target.external_account_id) != (
                "",
                evidence.get("team_id"),
            )
    if renamed and identity.provider_installation_id is not None:
        # The row may now stand for another token, so its attachment is
        # unproven until a report under the new name matches it. Interactions
        # need no stored evidence and would otherwise still pass the
        # installation check against the old attachment.
        identity.installation_mismatch = True
    await _commit(session)
    await session.refresh(identity)
    return identity


async def delete_identity(session: AsyncSession, identity: ChannelIdentity) -> None:
    await session.delete(identity)
    await _commit(session)


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


_TABLE_EXISTS = text("SELECT to_regclass('curie.channel_identities') IS NOT NULL")

# A plain INSERT, no WHERE NOT EXISTS or ON CONFLICT: Postgres's ON CONFLICT
# clause only arbitrates a conflict on the ONE index it names, and a row that
# also violates a DIFFERENT unique constraint still raises -- confirmed by a
# real CI run (#3040 review round 2): two replicas concurrently bootstrapping
# "default" (same fixed id, same name) can still hit a raw UniqueViolationError
# on the NAME key even with `ON CONFLICT (id) DO NOTHING`, because genuinely
# concurrent inserts resolve their unique indexes in an order Postgres does
# not guarantee follows the declared arbiter. There is no single arbiter that
# covers every way this row can already be represented (a prior boot's row
# at the same id, a renamed row, an operator-created row with the same name
# but a different id, or a second replica racing this exact attempt), so
# catching the IntegrityError here, in a SAVEPOINT that rolls back only this
# identity's own attempt, replaces trying to express all of that in SQL.
_INSERT_SLACK_IDENTITY = text(
    """
    INSERT INTO curie.channel_identities
        (id, tenant_id, provider, name, credential_ref, webhook_verification_ref,
         attributes, status)
    VALUES (:id, :tenant_id, 'slack', :name, :credential_ref,
            :webhook_verification_ref, CAST(:attributes AS jsonb), 'active')
    """
)


async def bootstrap_static_slack(
    sessionmaker: async_sessionmaker[AsyncSession], settings: Settings
) -> bool:
    """Ensure every declared Slack identity has its channel identity row.

    Returns False only when the table does not exist yet, which is the one
    case worth retrying: this image may serve any schema from its
    ``schema_min``, and a fresh install applies migrations one transaction at
    a time, so boot can land between 0077 and 0078.
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
            identity_id = STATIC_SLACK_IDENTITY_ID if is_default else uuid.uuid4()
            webhook_ref = (
                f"env:{identity.signing_secret_env}" if identity.signing_secret_env else None
            )
            attributes = json.dumps({"app_token_ref": f"env:{identity.app_token_env}"})
            try:
                async with session.begin_nested():
                    await session.execute(
                        _INSERT_SLACK_IDENTITY,
                        {
                            "id": identity_id,
                            "tenant_id": DEFAULT_TENANT_ID,
                            "name": identity.name,
                            "credential_ref": f"env:{identity.bot_token_env}",
                            "webhook_verification_ref": webhook_ref,
                            "attributes": attributes,
                        },
                    )
            except IntegrityError:
                # Already represented under this (tenant, provider, name) or
                # at this fixed id -- a prior boot, a rename, an
                # operator-created row, or a replica racing this exact
                # attempt. The SAVEPOINT rolled back only this attempt, so
                # the next identity in the loop is unaffected.
                continue
            inserted_ids.append(identity_id)
        await session.commit()
    for identity_id in inserted_ids:
        _LOG.info("bootstrapped Slack channel identity id=%s", identity_id)
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
                "static Slack channel identity bootstrap failed: %s",
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
        "static Slack channel identities not yet recorded; retrying every %ss",
        interval_s,
    )
    return asyncio.create_task(
        _retry_until_done(sessionmaker, settings, interval_s, warn_period_s, warned)
    )
