"""Refresh-capable sources and the fenced refresh protocol (ADR 0239).

"Static sources" 4 and 5: a grant's durable intent (ready, refreshing,
recovery_required), its intent ID and epoch serialize refresh; the replacement
is persisted before the access token is released; and an uncertain outcome
enters ``recovery_required``, which is never stolen or replayed automatically
and is cleared only by an explicit operator reimport.

A ``RefreshCapableSource`` decides from its material whether the material is
fresh (``release``) and performs the external exchange (``refresh``). It never
touches the database. ``RefreshCoordinator`` owns the protocol over cp1's
SECURITY DEFINER CAS functions, each step in its own short transaction on the
application role and never inside the admission transaction, so no row lock is
held across the provider call and an operator reimport never waits on it:

1. ``shared_connector_refresh_begin`` takes the intent (``busy``,
   ``recovery_required``, ``disabled`` and ``not_found`` refuse) and answers the
   ciphertext as of that moment.
2. ``source.refresh`` runs, bounded by the source's lease.
3. The replacement is sealed to the application's current recipient (the live
   recipient row with the highest ordinal), which the key ring must hold.
4. ``shared_connector_refresh_commit`` fences on intent and epoch: ``stale``
   means a reimport or another writer moved the row on, so the provider's token
   is dropped, never stored and never released.

Anything that fails after ``began`` and before a definite commit outcome may
have consumed a rotating refresh token at the provider, so it is marked
``recovery_required`` (``uncertain_outcome``). When the database is
unreachable even for that mark, the answer is ``store_unavailable`` and the
intent stays ``refreshing`` until its lease lapses; the next begin then turns
it into ``recovery_required`` (``lease_expired``) itself.

The coordinator releases nothing. The service re-admits from scratch after a
commit, so a disable, membership change or reimport committed meanwhile is
honored before the new token leaves the resolver.
"""

from __future__ import annotations

import asyncio
import uuid
from abc import abstractmethod
from typing import Any, ClassVar

from .admission import ACQUIRE_TIMEOUT_S, STORE_ERRORS, Admitted
from .keys import KeyRing, KeyUnavailable, seal
from .sources import KIND_REFRESH, CredentialSource, SourceRefusal

MAX_LEASE_SECONDS = 300
_MAX_OWNER_BYTES = 128
# A sealed box adds 48 bytes; the stored ciphertext is at most 64 KiB.
_MAX_PLAINTEXT_BYTES = 65536 - 48

_BEGIN_SQL = "SELECT * FROM curie.shared_connector_refresh_begin($1, $2, $3)"
_COMMIT_SQL = "SELECT * FROM curie.shared_connector_refresh_commit($1, $2, $3, $4, $5)"
_MARK_SQL = "SELECT curie.shared_connector_refresh_mark_recovery($1, $2, $3)"
_CURRENT_RECIPIENT_SQL = """
SELECT r.recipient_id
FROM curie.shared_connector_recipients r
WHERE r.application_id = $1 AND r.retired_at IS NULL
ORDER BY r.ordinal DESC
LIMIT 1
"""

_BEGIN_REFUSALS = {
    "busy": "refresh_busy",
    "recovery_required": "recovery_required",
    "disabled": "binding_disabled",
    "not_found": "source_unavailable",
}


class RefreshCapableSource(CredentialSource):
    """A source whose material can expire and be renewed at the provider."""

    kind = KIND_REFRESH
    # Passed to shared_connector_refresh_begin, which accepts 1 to 300.
    lease_seconds: ClassVar[int] = 60

    @abstractmethod
    async def refresh(self, admitted: Admitted, slot: str, material: bytes) -> bytes:
        """Exchange ``material`` at the provider; answer the complete replacement plaintext.

        Any exception is an uncertain outcome: the provider may have consumed
        the refresh token.
        """


class _Uncertain(Exception):
    """A failure after ``began`` and before a definite commit outcome."""


class RefreshCoordinator:
    """Runs one fenced refresh for a resolve; never releases anything itself."""

    def __init__(self, *, owner: str) -> None:
        if not owner or len(owner.encode("utf-8")) > _MAX_OWNER_BYTES:
            raise ValueError("a refresh owner is 1 to 128 UTF-8 bytes")
        self._owner = owner

    async def _step(self, pool: Any, sql: str, *args: Any) -> Any:
        """One statement in its own transaction; any database failure raises it as is."""

        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as connection:
            async with connection.transaction():
                return await connection.fetchrow(sql, *args)

    async def _mark_recovery(self, pool: Any, grant_id: str, intent: uuid.UUID, epoch: int) -> str:
        try:
            row = await self._step(pool, _MARK_SQL, grant_id, intent, epoch)
        except STORE_ERRORS:
            raise SourceRefusal("store_unavailable") from None
        return "" if row is None else str(row[0])

    async def run(
        self,
        pool: Any,
        source: RefreshCapableSource,
        admitted: Admitted,
        slot: str,
        keys: KeyRing,
    ) -> None:
        """Refresh ``admitted``'s grant and commit the replacement, or refuse."""

        lease = source.lease_seconds
        if admitted.refresh is None or type(lease) is not int or not 1 <= lease <= 300:
            raise SourceRefusal("source_unavailable")
        grant_id = admitted.refresh.grant_id
        try:
            began = await self._step(pool, _BEGIN_SQL, grant_id, self._owner, lease)
        except STORE_ERRORS:
            raise SourceRefusal("store_unavailable") from None
        outcome = None if began is None else str(began["outcome"])
        if outcome != "began":
            raise SourceRefusal(_BEGIN_REFUSALS.get(outcome or "", "source_unavailable"))
        assert began is not None
        intent = uuid.UUID(str(began["intent_id"]))
        epoch = int(began["epoch"])

        try:
            sealed, recipient = await self._exchange(pool, source, admitted, slot, keys, began)
            committed = await self._step(
                pool, _COMMIT_SQL, grant_id, intent, epoch, recipient, sealed
            )
        except asyncio.CancelledError:
            # Shutdown: the lease lapses and the next begin records recovery.
            raise
        except Exception:  # noqa: BLE001 - every failure here is an uncertain outcome
            marked = await self._mark_recovery(pool, grant_id, intent, epoch)
            if marked == "recovery_required":
                raise SourceRefusal("recovery_required") from None
            # Another writer moved the intent on; this attempt's outcome is moot.
            raise SourceRefusal("refresh_stale") from None

        result = None if committed is None else str(committed["outcome"])
        if result == "committed":
            return
        if result == "stale":
            raise SourceRefusal("refresh_stale")
        marked = await self._mark_recovery(pool, grant_id, intent, epoch)
        if marked == "recovery_required" or result == "recipient_unknown":
            raise SourceRefusal("recovery_required")
        raise SourceRefusal("source_unavailable")

    async def _exchange(
        self,
        pool: Any,
        source: RefreshCapableSource,
        admitted: Admitted,
        slot: str,
        keys: KeyRing,
        began: Any,
    ) -> tuple[bytes, str]:
        """Steps 2 and 3: the provider exchange, then the seal to the current recipient."""

        try:
            material = keys.open(str(began["recipient_id"]), bytes(began["ciphertext"]))
        except (KeyUnavailable, TypeError) as error:
            raise _Uncertain("the material does not open") from error
        replacement = await asyncio.wait_for(
            source.refresh(admitted, slot, material), timeout=source.lease_seconds
        )
        if not isinstance(replacement, bytes) or not 0 < len(replacement) <= _MAX_PLAINTEXT_BYTES:
            raise _Uncertain("the replacement is not a storable plaintext")
        current = await self._step(pool, _CURRENT_RECIPIENT_SQL, uuid.UUID(admitted.application))
        if current is None or not keys.holds(str(current["recipient_id"])):
            raise _Uncertain("the current recipient is not held")
        recipient = str(current["recipient_id"])
        return seal(keys.public_key(recipient), replacement), recipient
