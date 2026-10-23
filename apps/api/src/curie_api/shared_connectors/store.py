"""Shared connector applications, recipients, members and bindings (ADR 0239).

Lock order, everywhere: the application row, then the binding row, then the
binding's refresh intent row (plan decision 15). The recipient row is a leaf
locked last: the binding and database-role triggers lock it FOR SHARE and
refuse a retired recipient, and retirement locks it FOR UPDATE before counting
references, so a writer and a retirement serialize (decision 16). Where a
write also needs an agent row it locks that first, the order an agent delete
takes through its cascade into memberships and the policy generation bump, so
the two cannot deadlock.

Disable takes only its own row FOR UPDATE, so it waits out every open resolver
FOR SHARE or FOR KEY SHARE lock on that row, and the route acknowledges only
after the commit (ADR 0239 "Static sources" 6).

Every refusal is a ``SharedRefusal`` carrying a fixed code and nothing the
caller sent. A failed write that bound ciphertext never chains the driver
error, whose text would carry the bound parameters.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from typing import Any, Literal

from curie_internal.shared_declarations import (
    DSN_SECRET_KEY,
    DSN_SECRET_PREFIX,
    KEY_SECRET_KEY,
    KEY_SECRET_PREFIX,
    dsn_secret_name,
    key_secret_name,
)
from fastapi import HTTPException, status
from plugin_format.shared_connector_render import SharedApplication
from sqlalchemy import Select, delete, func, null, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer
from sqlalchemy.sql.dml import Delete, Insert, Update

from ..config import get_settings
from ..models import (
    Agent,
    SharedConnectorApplication,
    SharedConnectorBinding,
    SharedConnectorDatabaseRole,
    SharedConnectorMembership,
    SharedConnectorRecipient,
    SharedConnectorRefreshIntent,
)
from ..schemas.shared_connectors import (
    SharedAliasIn,
    SharedAliasOut,
    SharedApplicationDisabled,
    SharedApplicationOut,
    SharedApplicationRegistration,
    SharedBindingCheck,
    SharedBindingCheckOut,
    SharedBindingDisabled,
    SharedBindingImport,
    SharedBindingOut,
    SharedBindingReimport,
    SharedMembershipOut,
    SharedRecipientRegistered,
    SharedRecipientRegistration,
    SharedRecipientRetired,
    SharedSourceMetadata,
)
from . import sealing

NO_STORE = {"Cache-Control": "no-store"}
_UNIQUE_VIOLATION = "23505"

Lock = Literal["update", "no_key_update", "share", "key_share"]

# The API writes ciphertext and sealed DSNs but never reads them back: these
# loader options leave the columns unloaded and raise on any access. The one
# exception is ``database_roles.sealed_dsn_for_worker``, an explicit column read
# for the worker-authenticated application render, which hands the box unopened
# to the worker for its DSN Secret.
SEALED_BINDING = defer(SharedConnectorBinding.ciphertext, raiseload=True)
SEALED_DSN = defer(SharedConnectorDatabaseRole.sealed_dsn, raiseload=True)


class SharedRefusal(HTTPException):
    """A refusal answered as ``{"detail": "<code>"}`` and never cached."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(status_code, detail=code, headers=dict(NO_STORE))


def with_lock[S: Select[Any]](statement: S, lock: Lock | None) -> S:
    # populate_existing so a row read after waiting on its lock replaces any
    # copy this session already holds.
    statement = statement.execution_options(populate_existing=True)
    if lock == "update":
        return statement.with_for_update()
    if lock == "no_key_update":
        return statement.with_for_update(key_share=True)
    if lock == "share":
        return statement.with_for_update(read=True)
    if lock == "key_share":
        return statement.with_for_update(read=True, key_share=True)
    return statement


async def guarded_write(
    session: AsyncSession,
    statement: Insert | Update | Delete,
    *,
    unique_conflict: tuple[int, str] | None = None,
) -> Any:
    """Run a write whose parameters carry ciphertext or a sealed DSN.

    The refusal is raised outside the ``except`` block so it carries no
    context: SQLAlchemy's error text includes every bound parameter.
    """

    refusal: tuple[int, str]
    try:
        return await session.execute(statement)
    except DBAPIError as error:
        # SQLAlchemy's asyncpg adapter copies the driver's SQLSTATE onto ``orig``.
        conflict = (
            isinstance(error, IntegrityError)
            and getattr(error.orig, "sqlstate", None) == _UNIQUE_VIOLATION
        )
        refusal = (
            unique_conflict
            if conflict and unique_conflict is not None
            else (status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_store_write_failed")
        )
    raise SharedRefusal(*refusal)


# --- reads and locks -----------------------------------------------------------


async def lock_application(
    session: AsyncSession, application_id: uuid.UUID, lock: Lock | None = None
) -> SharedConnectorApplication:
    statement = select(SharedConnectorApplication).where(
        SharedConnectorApplication.id == application_id
    )
    application: SharedConnectorApplication | None = await session.scalar(
        with_lock(statement, lock)
    )
    if application is None:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_application_not_found")
    return application


async def _lock_agent(session: AsyncSession, agent_id: uuid.UUID) -> bool:
    """FOR KEY SHARE on the agent row: an agent delete waits for this write."""

    statement = select(Agent.id).where(Agent.id == agent_id)
    return await session.scalar(with_lock(statement, "key_share")) is not None


async def current_recipient(
    session: AsyncSession, application_id: uuid.UUID, lock: Lock | None = None
) -> SharedConnectorRecipient | None:
    """The live recipient registered last; new seals go to it.

    A writer about to seal to it passes ``lock="share"``: the leaf lock, so a
    concurrent retirement waits for the write instead of failing it.
    """

    statement = (
        select(SharedConnectorRecipient)
        .where(
            SharedConnectorRecipient.application_id == application_id,
            SharedConnectorRecipient.retired_at.is_(None),
        )
        .order_by(SharedConnectorRecipient.ordinal.desc())
        .limit(1)
    )
    recipient: SharedConnectorRecipient | None = await session.scalar(with_lock(statement, lock))
    return recipient


async def _lock_recipient(
    session: AsyncSession, application_id: uuid.UUID, recipient_id: str, lock: Lock
) -> SharedConnectorRecipient | None:
    statement = select(SharedConnectorRecipient).where(
        SharedConnectorRecipient.application_id == application_id,
        SharedConnectorRecipient.recipient_id == recipient_id,
    )
    row: SharedConnectorRecipient | None = await session.scalar(with_lock(statement, lock))
    return row


async def _live_recipient(
    session: AsyncSession, application_id: uuid.UUID, recipient_id: str
) -> SharedConnectorRecipient | None:
    """The named recipient FOR SHARE, when it is live: the leaf lock, taken last."""

    row = await _lock_recipient(session, application_id, recipient_id, "share")
    return row if row is not None and row.retired_at is None else None


async def _lock_binding(
    session: AsyncSession, binding_id: uuid.UUID, lock: Lock | None = None
) -> SharedConnectorBinding:
    statement = (
        select(SharedConnectorBinding)
        .where(SharedConnectorBinding.id == binding_id)
        .options(SEALED_BINDING)
    )
    binding: SharedConnectorBinding | None = await session.scalar(with_lock(statement, lock))
    if binding is None:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_binding_not_found")
    return binding


async def database_role(
    session: AsyncSession, application_id: uuid.UUID, lock: Lock | None = None
) -> SharedConnectorDatabaseRole | None:
    statement = (
        select(SharedConnectorDatabaseRole)
        .where(SharedConnectorDatabaseRole.application_id == application_id)
        .options(SEALED_DSN)
    )
    row: SharedConnectorDatabaseRole | None = await session.scalar(with_lock(statement, lock))
    return row


async def _lock_intent(
    session: AsyncSession, binding_id: uuid.UUID, lock: Lock | None = None
) -> SharedConnectorRefreshIntent | None:
    statement = select(SharedConnectorRefreshIntent).where(
        SharedConnectorRefreshIntent.binding_id == binding_id
    )
    intent: SharedConnectorRefreshIntent | None = await session.scalar(with_lock(statement, lock))
    return intent


async def _policy_generation(session: AsyncSession, application_id: uuid.UUID) -> int:
    generation = await session.scalar(
        select(SharedConnectorApplication.policy_generation).where(
            SharedConnectorApplication.id == application_id
        )
    )
    return int(generation or 0)


# --- applications ----------------------------------------------------------------


def usable_installation() -> str:
    """The configured installation ID, when shared registration may use it.

    Blank, invalid under the frozen ``SharedApplication`` rule, and not yet
    observed by the chart all refuse, in that order (plan decision 5).
    """

    settings = get_settings()
    configured = settings.installation_id
    if not configured.strip():
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_installation_unavailable")
    try:
        SharedApplication.valid_installation(configured)
    except ValueError:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_installation_invalid") from None
    if not settings.installation_id_observed:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_installation_unobserved")
    return configured


def _require_worker_secret_refs(spec: SharedApplication) -> None:
    """Pin the worker-managed Secret names, so the two kinds of Secret never cross.

    The worker creates the key Secret, and for a bundled database the DSN
    Secret, once and never patches them; the admission policy admits only
    these names. So the spec must name exactly those, and an operator Secret
    (a managed DSN, the grant store, the snapshot seal) must never carry a
    worker-managed prefix, or the worker would create or adopt it.
    """

    application = spec.application
    worker_prefixes = (KEY_SECRET_PREFIX, DSN_SECRET_PREFIX)
    operator_refs = [spec.grant_store, spec.snapshot_seal]
    if spec.dsn_encoding == "bundled-sealed":
        dsn_pinned = (spec.database_dsn.name, spec.database_dsn.key) == (
            dsn_secret_name(application),
            DSN_SECRET_KEY,
        )
    else:
        dsn_pinned = True
        operator_refs.append(spec.database_dsn)
    if (
        (spec.encryption_key.name, spec.encryption_key.key)
        != (key_secret_name(application), KEY_SECRET_KEY)
        or not dsn_pinned
        or any(ref is not None and ref.name.startswith(worker_prefixes) for ref in operator_refs)
    ):
        raise SharedRefusal(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "shared_application_secret_ref_invalid"
        )


def _pin_digest(spec: dict[str, Any], policy: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"spec": spec, "policy": policy},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def register_application(
    session: AsyncSession, body: SharedApplicationRegistration
) -> tuple[SharedApplicationOut, bool]:
    """Register an immutable application; answer its record and whether it is new.

    An identical registration replays the stored record. The same UUID with any
    other pins is refused, never updated.
    """

    installation = usable_installation()
    try:
        # JSON mode: the frozen model is strict and declares tuple fields.
        spec = SharedApplication.model_validate_json(json.dumps(body.application))
    except ValueError:
        raise SharedRefusal(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "shared_application_invalid"
        ) from None
    custody = body.policy.custody
    if (custody == "platform") != bool(spec.credential_slots):
        raise SharedRefusal(status.HTTP_422_UNPROCESSABLE_CONTENT, "shared_application_invalid")
    if spec.installation != installation:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_installation_mismatch")
    _require_worker_secret_refs(spec)

    spec_json = spec.model_dump(mode="json")
    # The tool subsets are stored only when nonempty, so a policy without them
    # keeps the JSON and pin digest it had before they existed.
    empty_subsets = {
        name
        for name, values in (
            ("run_tools", body.policy.run_tools),
            ("gated_tools", body.policy.gated_tools),
        )
        if not values
    }
    policy_json = body.policy.model_dump(mode="json", exclude=empty_subsets)
    pin_digest = _pin_digest(spec_json, policy_json)
    application_id = uuid.UUID(spec.application)
    inserted = await session.scalar(
        insert(SharedConnectorApplication)
        .values(
            id=application_id,
            installation_id=installation,
            spec=spec_json,
            server_image=spec.server_image,
            credential_slots=list(spec.credential_slots),
            provider_origins=list(spec.provider_origins),
            snapshot_seal=null() if spec.snapshot_seal is None else spec_json["snapshot_seal"],
            policy=policy_json,
            pin_digest=pin_digest,
            custody=custody,
            policy_generation=1,
            enabled=True,
        )
        .on_conflict_do_nothing()
        .returning(SharedConnectorApplication.id)
    )
    if inserted is None:
        stored = await session.scalar(
            select(SharedConnectorApplication.pin_digest).where(
                SharedConnectorApplication.id == application_id
            )
        )
        if stored != pin_digest:
            raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_application_immutable")
    record = await application_record(session, application_id)
    await session.commit()
    return record, inserted is not None


async def application_record(
    session: AsyncSession, application_id: uuid.UUID
) -> SharedApplicationOut:
    """The operator view of one application: never ciphertext or a sealed DSN."""

    application = await lock_application(session, application_id)
    recipients = (
        await session.scalars(
            with_lock(
                select(SharedConnectorRecipient)
                .where(SharedConnectorRecipient.application_id == application_id)
                .order_by(SharedConnectorRecipient.ordinal),
                None,
            )
        )
    ).all()
    # Ordered by ordinal, so the last live row is the current recipient.
    live = [row for row in recipients if row.retired_at is None]
    current = live[-1].recipient_id if live else None
    database = await database_role(session, application_id)
    return SharedApplicationOut.model_validate(
        {
            "application_id": application.id,
            "installation_id": application.installation_id,
            "server_image": application.server_image,
            "credential_slots": list(application.credential_slots),
            "provider_origins": list(application.provider_origins),
            "snapshot_seal": application.snapshot_seal,
            "policy": {
                "tools": application.policy["tools"],
                "custody": application.policy["custody"],
                # Stored only when nonempty (``register_application``).
                "run_tools": application.policy.get("run_tools", []),
                "gated_tools": application.policy.get("gated_tools", []),
            },
            "custody": application.custody,
            "policy_generation": application.policy_generation,
            "enabled": application.enabled,
            "recipients": [
                {
                    "recipient_id": row.recipient_id,
                    "algorithm": row.algorithm,
                    "current": row.recipient_id == current,
                    "created_at": row.created_at,
                    "retired_at": row.retired_at,
                }
                for row in recipients
            ],
            "database": None if database is None else _database_view(database),
            "created_at": application.created_at,
        }
    )


def _database_view(row: SharedConnectorDatabaseRole) -> dict[str, Any]:
    secret = (
        None
        if row.dsn_secret_name is None
        else {"name": row.dsn_secret_name, "key": row.dsn_secret_key}
    )
    return {
        "mode": row.mode,
        "role_name": row.role_name,
        "status": row.status,
        "status_reason": row.status_reason,
        "dsn_recipient_id": row.dsn_recipient_id,
        "dsn_revision": row.dsn_revision,
        "dsn_secret": secret,
    }


async def disable_application(
    session: AsyncSession, application_id: uuid.UUID
) -> SharedApplicationDisabled:
    """Disable under FOR UPDATE and commit; the caller acknowledges afterwards."""

    application = await lock_application(session, application_id, "update")
    if application.enabled:
        await session.execute(
            update(SharedConnectorApplication)
            .where(SharedConnectorApplication.id == application_id)
            .values(enabled=False, disabled_at=func.now(), updated_at=func.now())
            .execution_options(synchronize_session=False)
        )
    await session.commit()
    return SharedApplicationDisabled(
        application_id=application_id, enabled=False, acknowledged=True
    )


# --- members -------------------------------------------------------------------


async def _stored_aliases(
    session: AsyncSession, application_id: uuid.UUID, agent_id: uuid.UUID
) -> dict[str, list[str] | None]:
    rows = await session.execute(
        select(SharedConnectorMembership.alias, SharedConnectorMembership.tools).where(
            SharedConnectorMembership.application_id == application_id,
            SharedConnectorMembership.agent_id == agent_id,
        )
    )
    return {row.alias: None if row.tools is None else list(row.tools) for row in rows}


def _membership_out(
    application_id: uuid.UUID,
    agent_id: uuid.UUID,
    aliases: dict[str, list[str] | None],
    generation: int,
) -> SharedMembershipOut:
    return SharedMembershipOut(
        application_id=application_id,
        agent_id=agent_id,
        aliases=[SharedAliasOut(alias=alias, tools=aliases[alias]) for alias in sorted(aliases)],
        policy_generation=generation,
    )


async def replace_members(
    session: AsyncSession,
    application_id: uuid.UUID,
    agent_id: uuid.UUID,
    aliases: Sequence[SharedAliasIn],
) -> SharedMembershipOut:
    """Replace one agent's aliases, writing only when the stored set differs.

    The membership triggers raise the policy generation on any change, so an
    identical replace leaves it unchanged and a refused widening writes nothing.
    """

    agent_found = await _lock_agent(session, agent_id)
    # NO KEY UPDATE: the membership trigger updates this row, so take the
    # stronger lock now rather than upgrade a share lock later.
    application = await lock_application(session, application_id, "no_key_update")
    if not agent_found:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_agent_not_found")
    if not application.enabled:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_application_disabled")
    allowed = set(application.policy["tools"])
    requested = {item.alias: None if item.tools is None else sorted(item.tools) for item in aliases}
    if any(tools is not None and not set(tools) <= allowed for tools in requested.values()):
        raise SharedRefusal(status.HTTP_422_UNPROCESSABLE_CONTENT, "shared_policy_widening")
    if await _stored_aliases(session, application_id, agent_id) != requested:
        await session.execute(
            delete(SharedConnectorMembership)
            .where(
                SharedConnectorMembership.application_id == application_id,
                SharedConnectorMembership.agent_id == agent_id,
            )
            .execution_options(synchronize_session=False)
        )
        for alias, tools in requested.items():
            await session.execute(
                insert(SharedConnectorMembership).values(
                    application_id=application_id,
                    agent_id=agent_id,
                    alias=alias,
                    tools=null() if tools is None else tools,
                )
            )
    stored = await _stored_aliases(session, application_id, agent_id)
    generation = await _policy_generation(session, application_id)
    await session.commit()
    return _membership_out(application_id, agent_id, stored, generation)


async def remove_member(
    session: AsyncSession, application_id: uuid.UUID, agent_id: uuid.UUID
) -> SharedMembershipOut:
    agent_found = await _lock_agent(session, agent_id)
    await lock_application(session, application_id, "no_key_update")
    removed: Sequence[Any] = []
    if agent_found:
        result = await session.execute(
            delete(SharedConnectorMembership)
            .where(
                SharedConnectorMembership.application_id == application_id,
                SharedConnectorMembership.agent_id == agent_id,
            )
            .returning(SharedConnectorMembership.alias)
            .execution_options(synchronize_session=False)
        )
        removed = result.all()
    if not removed:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_member_not_found")
    generation = await _policy_generation(session, application_id)
    await session.commit()
    return _membership_out(application_id, agent_id, {}, generation)


# --- recipients ----------------------------------------------------------------


async def register_recipient(
    session: AsyncSession, application_id: uuid.UUID, body: SharedRecipientRegistration
) -> tuple[SharedRecipientRegistered, bool]:
    """Register a worker-generated public key; answer the result and whether it is new.

    A replay of any live recipient, current or older, answers it again; a
    retired one is refused. A new row is read back in the same transaction
    and compared byte for byte before commit.
    """

    await lock_application(session, application_id)
    if body.application_id != application_id:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_application_mismatch")
    key = body.public_key.raw()
    digest = sealing.recipient_id(key)
    if body.recipient_id != digest:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_id_mismatch")
    if body.readback.public_key_sha256 != digest:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_readback_failed")

    inserted = await session.scalar(
        insert(SharedConnectorRecipient)
        .values(
            application_id=application_id,
            recipient_id=digest,
            algorithm=sealing.ALGORITHM,
            public_key=key,
        )
        .on_conflict_do_nothing()
        .returning(SharedConnectorRecipient.recipient_id)
    )
    stored = (
        await session.execute(
            select(
                SharedConnectorRecipient.algorithm,
                SharedConnectorRecipient.public_key,
                SharedConnectorRecipient.retired_at,
            ).where(
                SharedConnectorRecipient.application_id == application_id,
                SharedConnectorRecipient.recipient_id == digest,
            )
        )
    ).one_or_none()
    if stored is None or stored.algorithm != sealing.ALGORITHM or bytes(stored.public_key) != key:
        raise SharedRefusal(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_recipient_readback_failed"
        )
    if stored.retired_at is not None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_retired")
    current = await current_recipient(session, application_id)
    await session.commit()
    registered = SharedRecipientRegistered(
        application_id=application_id,
        recipient_id=digest,
        current=current is not None and current.recipient_id == digest,
    )
    return registered, inserted is not None


async def retire_recipient(
    session: AsyncSession, application_id: uuid.UUID, recipient_id: str
) -> SharedRecipientRetired:
    """Retire a recipient nothing references and nothing will seal to next.

    FOR UPDATE on the recipient row first, then the reference count: a writer
    holding it FOR SHARE through a binding or DSN write finishes first, and a
    writer arriving later sees it retired.
    """

    row = await _lock_recipient(session, application_id, recipient_id, "update")
    if row is None:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_recipient_not_found")
    retired_at = row.retired_at
    if retired_at is None:
        current = await current_recipient(session, application_id)
        if current is not None and current.recipient_id == recipient_id:
            raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_current")
        referenced = await session.scalar(
            select(
                or_(
                    select(SharedConnectorBinding.id)
                    .where(
                        SharedConnectorBinding.application_id == application_id,
                        SharedConnectorBinding.recipient_id == recipient_id,
                    )
                    .exists(),
                    select(SharedConnectorDatabaseRole.application_id)
                    .where(
                        SharedConnectorDatabaseRole.application_id == application_id,
                        SharedConnectorDatabaseRole.dsn_recipient_id == recipient_id,
                    )
                    .exists(),
                )
            )
        )
        if referenced:
            raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_referenced")
        retired_at = await session.scalar(
            update(SharedConnectorRecipient)
            .where(
                SharedConnectorRecipient.application_id == application_id,
                SharedConnectorRecipient.recipient_id == recipient_id,
            )
            .values(retired_at=func.now())
            .returning(SharedConnectorRecipient.retired_at)
            .execution_options(synchronize_session=False)
        )
    await session.commit()
    assert retired_at is not None
    return SharedRecipientRetired(
        application_id=application_id, recipient_id=recipient_id, retired_at=retired_at
    )


# --- bindings ------------------------------------------------------------------


def _binding_out(
    binding: SharedConnectorBinding, intent: SharedConnectorRefreshIntent | None
) -> SharedBindingOut:
    source = (
        None
        if binding.source_kind is None
        else {
            field: getattr(binding, f"source_{field}")
            for field in SharedSourceMetadata.model_fields
        }
    )
    refresh = (
        None
        if intent is None
        else {
            "grant_id": intent.grant_id,
            "client_config_fingerprint": intent.client_config_fingerprint,
            "state": intent.state,
            "epoch": intent.epoch,
        }
    )
    return SharedBindingOut.model_validate(
        {
            "binding_id": binding.id,
            "application_id": binding.application_id,
            "agent_id": binding.agent_id,
            "provider_account": binding.provider_account,
            "custody": binding.custody,
            "enabled": binding.enabled,
            "credential_revision": binding.credential_revision,
            "recipient_id": binding.recipient_id,
            "source": source,
            "refresh": refresh,
            "created_at": binding.created_at,
            "updated_at": binding.updated_at,
        }
    )


async def binding_record(session: AsyncSession, binding_id: uuid.UUID) -> SharedBindingOut:
    binding = await _lock_binding(session, binding_id)
    return _binding_out(binding, await _lock_intent(session, binding_id))


async def import_binding(
    session: AsyncSession, application_id: uuid.UUID, body: SharedBindingImport
) -> SharedBindingOut:
    """Create a binding at revision 1, and its refresh intent when a grant is named.

    At most one enabled binding per (application, agent): account replacement
    disables the old binding first. A connector-custody binding stores no
    credential, source or grant.
    """

    agent_found = await _lock_agent(session, body.agent_id)
    # FOR SHARE: a concurrent disable waits for this import, and two imports
    # into one application do not wait on each other.
    application = await lock_application(session, application_id, "share")
    if not agent_found:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_agent_not_found")
    if not application.enabled:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_application_disabled")
    if application.custody == "platform":
        mismatch = body.credential is None or body.source is None
    else:
        mismatch = any(
            part is not None for part in (body.credential, body.source, body.refresh_grant)
        )
    if mismatch:
        raise SharedRefusal(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "shared_binding_custody_mismatch"
        )
    existing = await session.scalar(
        select(SharedConnectorBinding.id).where(
            SharedConnectorBinding.application_id == application_id,
            SharedConnectorBinding.agent_id == body.agent_id,
            SharedConnectorBinding.enabled.is_(True),
        )
    )
    if existing is not None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_exists")
    grant = body.refresh_grant
    if grant is not None and await session.scalar(
        select(SharedConnectorRefreshIntent.binding_id).where(
            SharedConnectorRefreshIntent.installation_id == application.installation_id,
            SharedConnectorRefreshIntent.grant_id == grant.grant_id,
        )
    ):
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_refresh_grant_exists")

    binding_id = uuid.uuid4()
    values: dict[str, Any] = {
        "id": binding_id,
        "application_id": application_id,
        "agent_id": body.agent_id,
        "provider_account": body.provider_account,
        "custody": application.custody,
        "enabled": True,
        "credential_revision": 1,
    }
    if body.credential is not None and body.source is not None:
        if await _live_recipient(session, application_id, body.credential.recipient_id) is None:
            raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_unknown")
        values |= {
            "recipient_id": body.credential.recipient_id,
            "ciphertext": body.credential.sealed(),
            **_source_columns(body.source),
        }
    await guarded_write(
        session,
        insert(SharedConnectorBinding).values(**values),
        unique_conflict=(status.HTTP_409_CONFLICT, "shared_binding_exists"),
    )
    if grant is not None:
        await guarded_write(
            session,
            insert(SharedConnectorRefreshIntent).values(
                installation_id=application.installation_id,
                grant_id=grant.grant_id,
                application_id=application_id,
                binding_id=binding_id,
                client_config_fingerprint=grant.client_config_fingerprint,
                state="ready",
                epoch=0,
            ),
            unique_conflict=(status.HTTP_409_CONFLICT, "shared_refresh_grant_exists"),
        )
    record = await binding_record(session, binding_id)
    await session.commit()
    return record


def _source_columns(source: SharedSourceMetadata) -> dict[str, str]:
    return {f"source_{field}": value for field, value in source.model_dump().items()}


async def reimport_binding(
    session: AsyncSession, binding_id: uuid.UUID, body: SharedBindingReimport
) -> SharedBindingOut:
    """CAS on the credential revision; the explicit reset of the refresh intent.

    Binding FOR UPDATE, then intent FOR UPDATE, then the recipient (the leaf).
    The intent returns to ``ready`` at a higher epoch whatever its state, so a
    pre-reset intent can no longer commit and ``recovery_required`` clears only
    here (ADR 0239 "Static sources" 5).
    """

    binding = await _lock_binding(session, binding_id, "update")
    if not binding.enabled:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_disabled")
    if binding.custody != "platform":
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_connector_custody")
    intent = await _lock_intent(session, binding_id, "update")
    recipient_id = body.credential.recipient_id
    if await _live_recipient(session, binding.application_id, recipient_id) is None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_unknown")
    if binding.credential_revision != body.expected_revision:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_revision_conflict")
    result = await guarded_write(
        session,
        update(SharedConnectorBinding)
        .where(
            SharedConnectorBinding.id == binding_id,
            SharedConnectorBinding.credential_revision == body.expected_revision,
        )
        .values(
            credential_revision=SharedConnectorBinding.credential_revision + 1,
            recipient_id=recipient_id,
            ciphertext=body.credential.sealed(),
            source_stale=False,
            updated_at=func.now(),
            **_source_columns(body.source),
        )
        .returning(SharedConnectorBinding.id)
        .execution_options(synchronize_session=False),
    )
    if result.first() is None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_revision_conflict")
    if intent is not None:
        await session.execute(
            update(SharedConnectorRefreshIntent)
            .where(SharedConnectorRefreshIntent.binding_id == binding_id)
            .values(
                state="ready",
                epoch=SharedConnectorRefreshIntent.epoch + 1,
                intent_id=None,
                owner=None,
                lease_expires_at=None,
                recovery_reason=None,
                updated_at=func.now(),
            )
            .execution_options(synchronize_session=False)
        )
    record = await binding_record(session, binding_id)
    await session.commit()
    return record


async def check_binding(
    session: AsyncSession, binding_id: uuid.UUID, body: SharedBindingCheck
) -> SharedBindingCheckOut:
    """Compare the recorded source version and persist a mismatch.

    The resolver cannot read the source Secret, so the flag this sets is the only
    way it refuses a stale binding; only a reimport clears it (ADR 0239 "Static
    sources" 1). Never reads or returns material.
    """

    binding = await _lock_binding(session, binding_id, "no_key_update")
    if binding.custody != "platform":
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_binding_connector_custody")
    recorded = {"uid": binding.source_uid, "resource_version": binding.source_resource_version}
    current = recorded == {"uid": body.source.uid, "resource_version": body.source.resource_version}
    stale = binding.source_stale or not current
    if stale and not binding.source_stale:
        await session.execute(
            update(SharedConnectorBinding)
            .where(SharedConnectorBinding.id == binding_id)
            .values(source_stale=True, updated_at=func.now())
            .execution_options(synchronize_session=False)
        )
    result = SharedBindingCheckOut.model_validate(
        {
            "binding_id": binding_id,
            "status": "source_stale" if stale else "current",
            "credential_revision": binding.credential_revision,
            "recorded_source": recorded,
        }
    )
    await session.commit()
    return result


async def disable_binding(session: AsyncSession, binding_id: uuid.UUID) -> SharedBindingDisabled:
    """Disable under FOR UPDATE and commit; the caller acknowledges afterwards."""

    binding = await _lock_binding(session, binding_id, "update")
    if binding.enabled:
        await session.execute(
            update(SharedConnectorBinding)
            .where(SharedConnectorBinding.id == binding_id)
            .values(enabled=False, disabled_at=func.now(), updated_at=func.now())
            .execution_options(synchronize_session=False)
        )
    await session.commit()
    return SharedBindingDisabled(binding_id=binding_id, enabled=False, acknowledged=True)
