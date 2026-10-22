"""The per-resolve admission transaction (ADR 0239).

"Static sources" 6: each resolve locks and checks the current application and
binding status, and "Bundle" 5 makes the resolve transaction's commit the
admission boundary. ``admit`` runs inside a transaction the caller opened on
the application's own role and reads, in cp1's lock order (application, then
binding):

1. The application row ``FOR SHARE``. Row level security hides every other
   application's row, so a foreign one is simply absent. ``FOR SHARE`` and not
   ``FOR KEY SHARE`` because it must conflict with the ``NO KEY UPDATE`` that
   both an application disable and the membership trigger's
   ``policy_generation`` bump take: a disable or membership change waits for
   in-flight resolves, and every resolve after its commit sees it.
2. The membership row for the exact agent and alias ("Bundle" 7: membership,
   alias narrowing and policy generation are read authoritatively per call).
3. The agent's binding ``FOR SHARE``, without an ``enabled`` filter, so a
   binding disabled while this resolve waited on its lock is re-read and
   refused as disabled rather than skipped as missing.
4. The refresh intent, unlocked. That is safe because both transitions into
   ``recovery_required`` (``shared_connector_refresh_mark_recovery`` and a
   ``shared_connector_refresh_begin`` on a lapsed lease) lock the binding
   ``FOR NO KEY UPDATE``, which conflicts with this transaction's binding
   ``FOR SHARE``. A recovery therefore waits for an admission already holding
   the binding to commit; and when a recovery committed first, this
   transaction waited for it on the binding lock, and the intent read, a new
   statement under READ COMMITTED after that lock was granted, sees it. The
   coordinator's own CAS functions fence a refresh, never inside this
   transaction.

Nothing here writes. A refusal is a ``ResolveRefusal`` the caller turns into a
rollback.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from .handles import InvocationContext
from .wire import ResolveRefusal

# What a lost, refused or slow database raises. Every one of them is the
# fail-closed ``store_unavailable``, never a release.
STORE_ERRORS: tuple[type[BaseException], ...] = (
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    asyncpg.InternalClientError,
    OSError,
    TimeoutError,
)
# Bounds the wait for a pooled connection.
ACQUIRE_TIMEOUT_S = 5.0

_APPLICATION_SQL = """
SELECT a.installation_id, a.enabled, a.policy_generation, a.credential_slots,
       a.provider_origins, a.custody
FROM curie.shared_connector_applications a
WHERE a.id = $1
FOR SHARE
"""

_MEMBERSHIP_SQL = """
SELECT 1 FROM curie.shared_connector_memberships m
WHERE m.application_id = $1 AND m.agent_id = $2 AND m.alias = $3
"""

_BINDING_SQL = """
SELECT b.id::text AS id, b.enabled, b.custody, b.provider_account, b.credential_revision,
       b.recipient_id, b.ciphertext, b.source_stale
FROM curie.shared_connector_bindings b
WHERE b.application_id = $1 AND b.agent_id = $2
ORDER BY b.enabled DESC, b.created_at DESC
LIMIT 1
FOR SHARE
"""

_INTENT_SQL = """
SELECT i.grant_id, i.state, i.epoch
FROM curie.shared_connector_refresh_intents i
WHERE i.binding_id = $1
"""


@dataclass(frozen=True)
class RefreshIntent:
    grant_id: str
    state: str
    epoch: int


@dataclass(frozen=True)
class Admitted:
    """One admitted call: the rows it was admitted on, as they were at commit."""

    application: str
    installation: str
    policy_generation: int
    origins: tuple[str, ...]
    slots: tuple[str, ...]
    custody: str
    binding: str
    agent_id: str
    account: str
    credential_revision: int
    recipient_id: str | None
    ciphertext: bytes | None = field(repr=False)
    source_stale: bool
    # Read without a lock, after the binding FOR SHARE was granted. Every move
    # into recovery_required locks that binding FOR NO KEY UPDATE, so it either
    # waits for this transaction or committed before this read, which sees it.
    refresh: RefreshIntent | None


def _json_strings(value: Any) -> tuple[str, ...]:
    """A JSONB list of strings (asyncpg answers JSONB as text); anything else refuses."""

    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ResolveRefusal("source_unavailable")
    return tuple(parsed)


def slot_accepted(slot: str, slots: tuple[str, ...]) -> bool:
    """The identity slot rule: with no declared slots, exactly ``""`` is accepted.

    cp1 registers connector custody with no credential slots and platform
    custody with at least one, so identity-only admission asks for the empty
    slot, which no declared slot (an identifier) can equal.
    """

    if not slots:
        return slot == ""
    return slot in slots


async def admit(connection: Any, context: InvocationContext, slot: str) -> Admitted:
    """Admit ``context`` for ``slot`` inside the caller's open transaction.

    ``connection`` is an asyncpg connection (the package ships no types).
    """

    principal = context.principal
    application_id = uuid.UUID(principal.application)
    agent_id = uuid.UUID(principal.agent_id)

    application = await connection.fetchrow(_APPLICATION_SQL, application_id)
    if application is None:
        raise ResolveRefusal("application_unknown")
    if application["installation_id"] != principal.installation:
        raise ResolveRefusal("installation_mismatch")
    if not application["enabled"]:
        raise ResolveRefusal("application_disabled")
    if int(application["policy_generation"]) != principal.policy_generation:
        raise ResolveRefusal("policy_generation_mismatch")
    custody = str(application["custody"])
    slots = _json_strings(application["credential_slots"])
    if not slot_accepted(slot, slots):
        raise ResolveRefusal("slot_unknown")
    origins = _json_strings(application["provider_origins"])

    member = await connection.fetchval(_MEMBERSHIP_SQL, application_id, agent_id, context.alias)
    if member is None:
        raise ResolveRefusal("not_member")

    binding = await connection.fetchrow(_BINDING_SQL, application_id, agent_id)
    if binding is None:
        raise ResolveRefusal("binding_missing")
    if not binding["enabled"]:
        raise ResolveRefusal("binding_disabled")
    if binding["custody"] != custody:
        raise ResolveRefusal("custody_mismatch")

    intent = await connection.fetchrow(_INTENT_SQL, uuid.UUID(str(binding["id"])))
    refresh = (
        None
        if intent is None
        else RefreshIntent(
            grant_id=str(intent["grant_id"]), state=str(intent["state"]), epoch=int(intent["epoch"])
        )
    )
    recipient = binding["recipient_id"]
    ciphertext = binding["ciphertext"]
    return Admitted(
        application=principal.application,
        installation=str(application["installation_id"]),
        policy_generation=int(application["policy_generation"]),
        origins=origins,
        slots=slots,
        custody=custody,
        binding=str(binding["id"]),
        agent_id=principal.agent_id,
        account=str(binding["provider_account"]),
        credential_revision=int(binding["credential_revision"]),
        recipient_id=None if recipient is None else str(recipient),
        ciphertext=None if ciphertext is None else bytes(ciphertext),
        source_stale=bool(binding["source_stale"]),
        refresh=refresh,
    )
