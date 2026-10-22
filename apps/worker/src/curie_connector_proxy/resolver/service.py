"""The resolver: one handle, one slot, one admission, one release (ADR 0239).

"Bundle, identity and request contract" 4 and 5. Order matters and is the
point of this module:

1. Refusals that need no database run first and leave ``admissions``
   unchanged: an empty handle, a handle the registry does not hold or that has
   expired, a context for another installation or application, and a slot the
   application does not declare. So an unknown slot or a missing or foreign
   handle refuses before any credential is read.
2. One admission transaction on the application's role (``admission.admit``)
   locks and checks the application, membership and binding. A stale source or
   a grant in ``recovery_required`` refuses before any source runs, so a still
   fresh access token is never released during recovery ("Static sources" 5).
3. The binding's source releases inside that transaction, and the success body
   is built only after the transaction COMMITs: the commit is the admission
   boundary ("Bundle" 5). Nothing is cached between calls.
4. A refresh-capable source may instead ask for a refresh. The admission
   transaction ends first, the coordinator runs its own fenced steps, and the
   call is re-admitted from scratch, handle included, before the new token can
   leave. A second request to refresh refuses ``refresh_failed``.

Every database failure, timeout and pool exhaustion is ``store_unavailable``.
The audit line names the binding, revision and agent, and the purpose only by
a SHA-256 prefix and byte length; a purpose is caller text and may carry
anything, including a handle. No log line carries a handle, credential
material, ciphertext or the DSN, and refusals log their code alone.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
import socket
from collections.abc import Mapping
from typing import Any

from .admission import ACQUIRE_TIMEOUT_S, STORE_ERRORS, Admitted, admit, slot_accepted
from .config import ResolverConfig
from .handles import HandleRegistry, InvocationContext, RegistryRefusal
from .refresh import RefreshCapableSource, RefreshCoordinator
from .sources import (
    CredentialSource,
    Field,
    RefreshNeeded,
    Release,
    SourceRefusal,
    SourceRegistry,
    classify,
)
from .wire import ResolveRefusal

logger = logging.getLogger("curie_connector_proxy.resolver")

_PURPOSE_DIGEST_HEX = 16


def default_owner() -> str:
    """A refresh owner naming this process, within the intent's 128-byte bound."""

    host = socket.gethostname().encode("ascii", "replace").decode("ascii")[:64]
    return f"curie-shared-resolver:{host}:{os.getpid()}:{secrets.token_hex(4)}"


def purpose_digest(purpose: str) -> tuple[str, int]:
    """The audit form of a purpose: a SHA-256 prefix and the UTF-8 byte length."""

    encoded = purpose.encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:_PURPOSE_DIGEST_HEX], len(encoded)


def _field_json(value: Field) -> dict[str, str]:
    if value.text is not None:
        return {"text": value.text}
    assert value.data is not None
    return {"bytes": base64.b64encode(value.data).decode("ascii")}


class Resolver:
    """Admits and releases one call per resolve; holds no authorization between calls."""

    def __init__(
        self,
        *,
        config: ResolverConfig,
        handles: HandleRegistry,
        sources: SourceRegistry,
        pool: Any,
        owner: str | None = None,
    ) -> None:
        self._config = config
        self._handles = handles
        self._sources = sources
        # An asyncpg pool on the application role (the package ships no types).
        self._pool = pool
        self._coordinator = RefreshCoordinator(owner=owner or default_owner())
        # Admission transactions opened, and successful releases: the counters
        # that show a refusal happened before any credential read.
        self.admissions = 0
        self.releases = 0

    async def resolve(self, handle: str, slot: str, purpose: str) -> dict[str, object]:
        """The 200 body for one call, or a ``ResolveRefusal``."""

        try:
            return await self._resolve(handle, slot, purpose)
        except ResolveRefusal as refusal:
            logger.info("resolve refused code=%s", refusal.code)
            raise ResolveRefusal(refusal.code) from None

    def _context(self, handle: str, slot: str) -> InvocationContext:
        """Every check that needs no database; none of them counts as an admission."""

        if not handle:
            raise ResolveRefusal("handle_missing")
        try:
            context = self._handles.admit(handle)
        except RegistryRefusal as refusal:
            raise ResolveRefusal(refusal.code) from None
        principal = context.principal
        if (
            principal.installation != self._config.installation
            or principal.application != self._config.application
        ):
            raise ResolveRefusal("foreign_principal")
        if not slot_accepted(slot, self._config.credential_slots):
            raise ResolveRefusal("slot_unknown")
        return context

    async def _resolve(self, handle: str, slot: str, purpose: str) -> dict[str, object]:
        context = self._context(handle, slot)
        admitted, source, outcome = await self._admit_once(context, slot)
        if isinstance(outcome, RefreshNeeded):
            assert isinstance(source, RefreshCapableSource)
            await self._coordinator.run(self._pool, source, admitted, slot, self._config.keys)
            # From scratch: the handle may have been removed while the provider ran.
            context = self._context(handle, slot)
            admitted, source, outcome = await self._admit_once(context, slot)
            if isinstance(outcome, RefreshNeeded):
                raise ResolveRefusal("refresh_failed")
        self.releases += 1
        body = self._body(context, admitted, slot, outcome.fields)
        digest, size = purpose_digest(purpose)
        logger.info(
            "resolve released admission=%s binding=%s revision=%d agent_id=%s "
            "purpose_sha256=%s purpose_bytes=%d",
            body["admission"],
            admitted.binding,
            admitted.credential_revision,
            admitted.agent_id,
            digest,
            size,
        )
        return body

    async def _admit_once(
        self, context: InvocationContext, slot: str
    ) -> tuple[Admitted, CredentialSource, Release | RefreshNeeded]:
        """One admission transaction; answers only after it committed."""

        self.admissions += 1
        try:
            async with self._pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as connection:
                async with connection.transaction():
                    admitted = await admit(connection, context, slot)
                    source, outcome = self._release(admitted, slot)
        except ResolveRefusal:
            raise
        except STORE_ERRORS:
            raise ResolveRefusal("store_unavailable") from None
        return admitted, source, outcome

    def _release(
        self, admitted: Admitted, slot: str
    ) -> tuple[CredentialSource, Release | RefreshNeeded]:
        """Run the binding's source inside the admission transaction."""

        if admitted.origins != self._config.provider_origins:
            # The rendered origins are not the application's: a mis-rendered Pod.
            raise SourceRefusal("source_unavailable")
        if admitted.custody == "platform":
            if admitted.source_stale:
                raise ResolveRefusal("source_stale")
            if admitted.refresh is not None and admitted.refresh.state == "recovery_required":
                raise ResolveRefusal("recovery_required")
        source = self._sources.get(classify(admitted))
        try:
            outcome = source.release(admitted, slot, self._config.keys)
        except ResolveRefusal:
            raise
        except Exception as error:  # noqa: BLE001 - a failing source refuses closed
            logger.warning("credential source failed: %s", type(error).__name__)
            raise SourceRefusal("source_unavailable") from None
        if isinstance(outcome, RefreshNeeded):
            if not isinstance(source, RefreshCapableSource):
                raise SourceRefusal("source_unavailable")
            return source, outcome
        if not isinstance(outcome, Release) or not _release_fits(admitted, outcome):
            raise SourceRefusal("source_unavailable")
        return source, outcome

    def _body(
        self,
        context: InvocationContext,
        admitted: Admitted,
        slot: str,
        fields: Mapping[str, Field] | None,
    ) -> dict[str, object]:
        body: dict[str, object] = {
            "admission": "identity" if fields is None else "credential",
            "binding": admitted.binding,
            "account": admitted.account,
            "revision": admitted.credential_revision,
            "origins": list(admitted.origins),
            "expires_at": context.expires_at,
            "slot": slot,
        }
        if fields is not None:
            body["credential"] = {
                "fields": {name: _field_json(value) for name, value in fields.items()}
            }
        return body


def _release_fits(admitted: Admitted, release: Release) -> bool:
    """Identity-only for connector custody; at least one field for platform custody."""

    if admitted.custody == "connector":
        return release.fields is None
    fields = release.fields
    return (
        isinstance(fields, Mapping)
        and bool(fields)
        and all(
            isinstance(name, str) and isinstance(value, Field) for name, value in fields.items()
        )
    )
