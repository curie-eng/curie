"""Administrative source broker composition, @spec PROTECTED-HOOK-SOURCE-3/6/7/10.

Administrative broker work runs on its own executor of two threads, separate
from the support probe's, and fails rather than queueing when no slot is free.
A mutation keeps its slot through publication; GET takes one only after
releasing the gate. A slot is released when its last thread call finishes,
not when the request ends.

The source writer cannot see the live run_id, so every writer effect is
bracketed by control reader reads on the same pinned endpoint: one precedes
it, and one follows it and must show the effect. A reader identity failure is
reported on this path as ``broker_unavailable``. The gate phase has one five
second deadline over the reader and writer connections opened for it; ordinary
tombstone publication after gate release opens fresh connections under its own
deadline. A source key never appears in any value, log or error here.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, TypeVar

from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable
from curie_protected_hooks.broker_transport import (
    AuthenticatedMetadataReader,
    AuthenticatedSourceWriter,
    metadata_reader_budget,
)
from curie_protected_hooks.source_fence import (
    SourceFenceConflict,
    SourceFenceExhausted,
    SourceFenceInvalid,
    SourceState,
)
from curie_protected_hooks.source_policy_sql import SourcePolicySnapshot

from .hook_source_admin import SourceActivation, SourceAdminError
from .hook_source_mutation import (
    DesiredSourceTarget,
    SourceControlSession,
    SourceIdentity,
    committed_policy_fingerprint,
)
from .protected_runtime_files import (
    AdministrationRuntime,
    RuntimeBootstrap,
    RuntimeFilesInvalid,
    load_bootstrap,
)

# @spec PROTECTED-HOOK-SOURCE-3/6/10
_ADMIN_THREADS = 2
_DEADLINE_SECONDS = 5.0
_ADMIN_SLOTS = threading.BoundedSemaphore(_ADMIN_THREADS)
_ADMIN_EXECUTOR = ThreadPoolExecutor(
    max_workers=_ADMIN_THREADS, thread_name_prefix="curie-source-admin"
)
_BROKER_FAILURES = (
    BrokerMetadataUnavailable,
    SourceFenceConflict,
    SourceFenceExhausted,
    SourceFenceInvalid,
)

_T = TypeVar("_T")


def _broker_unavailable() -> SourceAdminError:
    """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
    return SourceAdminError("broker_unavailable", 503)


class AdminSlot:
    """One of the two administrative executor slots, @spec PROTECTED-HOOK-SOURCE-3/6/10.

    Calls run on the administrative executor; there are as many threads as
    slots, so a call never queues. The semaphore is released once the holder
    finished and no call is still running on a thread.
    """

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        self._lock = threading.Lock()
        self._running = 0
        self._finished = False
        self._released = False

    def _release_if_idle(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        with self._lock:
            if self._released or not self._finished or self._running:
                return
            self._released = True
        _ADMIN_SLOTS.release()

    def _done(self, _future: Future[Any]) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        with self._lock:
            self._running -= 1
        self._release_if_idle()

    def finish(self) -> None:
        """The holder is done; the slot frees after its last call. @spec PROTECTED-HOOK-SOURCE-10"""
        with self._lock:
            self._finished = True
        self._release_if_idle()

    async def run(self, call: Callable[..., _T], *args: Any) -> _T:
        """Run one blocking call on this slot's thread.

        Cancellation of the awaiting request never abandons a running call:
        the caller (and any gate it holds) waits until the call returns or its
        deadline ends, then the cancellation proceeds. @spec PROTECTED-HOOK-SOURCE-6/10.
        """
        with self._lock:
            if self._finished:
                raise _broker_unavailable()
            self._running += 1
        try:
            submitted = _ADMIN_EXECUTOR.submit(call, *args)
        except BaseException:
            with self._lock:
                self._running -= 1
            raise
        submitted.add_done_callback(self._done)
        waiting = asyncio.wrap_future(submitted)
        try:
            return await asyncio.shield(waiting)
        except asyncio.CancelledError:
            while not waiting.done():
                try:
                    await asyncio.shield(waiting)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            raise


@asynccontextmanager
async def admin_slot() -> AsyncIterator[AdminSlot]:
    """Take a free administrative slot or refuse with ``broker_unavailable``.

    @spec PROTECTED-HOOK-SOURCE-3/10.
    """
    if not _ADMIN_SLOTS.acquire(blocking=False):
        raise _broker_unavailable()
    slot = AdminSlot()
    try:
        yield slot
    finally:
        slot.finish()


def _remaining(deadline: float) -> float:
    """Seconds left before ``deadline`` or the safe refusal, @spec PROTECTED-HOOK-SOURCE-6."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise BrokerMetadataUnavailable()
    return left


def _connect_reader(bootstrap: RuntimeBootstrap, deadline: float) -> AuthenticatedMetadataReader:
    """A reader whose watchdog ends at ``deadline``, @spec PROTECTED-HOOK-SOURCE-6.

    Its connection checks the live run_id against the manifest's.
    """
    with metadata_reader_budget(_remaining(deadline)):
        return AuthenticatedMetadataReader.connect(
            bootstrap.manifest, bootstrap.credential, bootstrap.ca_pem
        )


def _connect_writer(runtime: AdministrationRuntime, deadline: float) -> AuthenticatedSourceWriter:
    """A writer whose watchdog ends at ``deadline``, @spec PROTECTED-HOOK-SOURCE-6."""
    with metadata_reader_budget(_remaining(deadline)):
        return AuthenticatedSourceWriter.connect(
            runtime.bootstrap.manifest, runtime.writer, runtime.bootstrap.ca_pem
        )


def _close(connection: AuthenticatedMetadataReader | AuthenticatedSourceWriter | None) -> None:
    """Best effort close, @spec PROTECTED-HOOK-SOURCE-6."""
    if connection is None:
        return
    try:
        connection.close()
    except BrokerMetadataUnavailable:
        pass


def _holds(state: SourceState, generation: int, operation_id: str) -> bool:
    """Whether the record still carries this reservation, @spec PROTECTED-HOOK-SOURCE-6/7."""
    return state["floor"] == generation and state["operation_id"] == operation_id


def _published(state: SourceState, generation: int, operation_id: str, fingerprint: str) -> bool:
    """Whether the record is exactly this ordinary publication, @spec PROTECTED-HOOK-SOURCE-6."""
    active = state["active"]
    return (
        _holds(state, generation, operation_id)
        and active is not None
        and active["generation"] == generation
        and active["operation_id"] == operation_id
        and active["mode"] == "ordinary"
        and active["policy_fingerprint"] == fingerprint
    )


class _GateReader:
    """SourceControlReader over the gate phase reader, @spec PROTECTED-HOOK-SOURCE-6/7/10."""

    def __init__(
        self, slot: AdminSlot, source: SourceIdentity, reader: AuthenticatedMetadataReader
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        self._slot = slot
        self._source = source
        self._reader = reader

    async def read_reconciled_floor(self) -> int:
        """The source floor from a connection whose live run_id is the manifest's.

        A missing key reads as zero; allocation above every durable ledger
        generation keeps that from reusing a generation. @spec PROTECTED-HOOK-SOURCE-6/7/10.
        """
        try:
            state = await self._slot.run(
                self._reader.read_source, self._source.agent_id, self._source.hook
            )
        except _BROKER_FAILURES:
            raise _broker_unavailable() from None
        floor: int = state["floor"]
        return floor


class _GateWriter:
    """SourceControlWriter: reader bracketed reservation, fresh connection publication.

    @spec PROTECTED-HOOK-SOURCE-3/6/7.
    """

    def __init__(
        self,
        slot: AdminSlot,
        source: SourceIdentity,
        runtime: AdministrationRuntime,
        reader: AuthenticatedMetadataReader,
        writer: AuthenticatedSourceWriter,
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        self._slot = slot
        self._source = source
        self._runtime = runtime
        self._reader = reader
        self._writer = writer

    def _reserve(self, expected_floor: int, operation_id: str, min_generation: int) -> int:
        """Reader read, reservation, reader confirmation, @spec PROTECTED-HOOK-SOURCE-6/7."""
        agent, hook = self._source.agent_id, self._source.hook
        self._reader.read_source(agent, hook)
        reserved = self._writer.reserve_and_revoke(
            agent, hook, expected_floor, operation_id, min_generation
        )
        if not _holds(self._reader.read_source(agent, hook), reserved, operation_id):
            # A readable confirmation without the effect is an uncertain effect.
            raise BrokerMetadataUnavailable()
        return reserved

    async def reserve(self, *, expected_floor: int, operation_id: str, min_generation: int) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        try:
            return await self._slot.run(self._reserve, expected_floor, operation_id, min_generation)
        except _BROKER_FAILURES:
            raise _broker_unavailable() from None

    def _publish(self, generation: int, operation_id: str, fingerprint: str) -> bool:
        """Fresh reader and writer under their own deadline; never a reconnect.

        @spec PROTECTED-HOOK-SOURCE-3/6/7.
        """
        agent, hook = self._source.agent_id, self._source.hook
        deadline = time.monotonic() + _DEADLINE_SECONDS
        reader = writer = None
        try:
            reader = _connect_reader(self._runtime.bootstrap, deadline)
            if not _holds(reader.read_source(agent, hook), generation, operation_id):
                raise SourceAdminError("source_reservation_lost", 503)
            writer = _connect_writer(self._runtime, deadline)
            published = writer.publish_ordinary(agent, hook, generation, operation_id, fingerprint)
            confirmed = reader.read_source(agent, hook)
            if _published(confirmed, generation, operation_id, fingerprint):
                return True
            if not _holds(confirmed, generation, operation_id):
                raise SourceAdminError("source_reservation_lost", 503)
            if not published:
                raise SourceAdminError("source_closed", 503)
            raise BrokerMetadataUnavailable()
        finally:
            _close(writer)
            _close(reader)

    async def publish_ordinary(
        self, *, generation: int, operation_id: str, fingerprint: str
    ) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-3/6/7."""
        try:
            return await self._slot.run(self._publish, generation, operation_id, fingerprint)
        except _BROKER_FAILURES:
            raise _broker_unavailable() from None


class ProvisionedSourceAuthority:
    """SourceAuthorityResolver from the runtime files the route loaded once.

    @spec PROTECTED-HOOK-SOURCE-3/6/7/10.
    """

    def __init__(self, runtime: AdministrationRuntime, slot: AdminSlot) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        self._runtime = runtime
        self._slot = slot
        manifest = runtime.bootstrap.manifest.as_dict()
        self._references = (
            manifest["runtime_id"],
            manifest["qualification_id"],
            manifest["bundle_digest"]["sha256"],
        )

    def check_references(
        self, runtime_id: str | None, qualification_id: str | None, bundle_digest: str | None
    ) -> None:
        """The deployment's one runtime or 422 ``unknown_source_reference``.

        @spec PROTECTED-HOOK-SOURCE-3/10.
        """
        if (runtime_id, qualification_id, bundle_digest) != self._references:
            raise SourceAdminError("unknown_source_reference", 422)

    def check_target(self, target: DesiredSourceTarget) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        self.check_references(target.runtime_id, target.qualification_id, target.bundle_digest)

    @asynccontextmanager
    async def resolve(
        self,
        source: SourceIdentity,
        target: DesiredSourceTarget,
        *,
        durable_generation_highwater: int,
    ) -> AsyncIterator[SourceControlSession]:
        """Open reader then writer under one gate phase deadline, before registration.

        @spec PROTECTED-HOOK-SOURCE-6/7/10.
        """
        deadline = time.monotonic() + _DEADLINE_SECONDS
        reader: AuthenticatedMetadataReader | None = None
        writer: AuthenticatedSourceWriter | None = None
        try:
            try:
                reader = await self._slot.run(_connect_reader, self._runtime.bootstrap, deadline)
                writer = await self._slot.run(_connect_writer, self._runtime, deadline)
            except _BROKER_FAILURES:
                raise _broker_unavailable() from None
            yield SourceControlSession(
                source=source,
                target=target,
                reader=_GateReader(self._slot, source, reader),
                writer=_GateWriter(self._slot, source, self._runtime, reader, writer),
            )
        finally:
            if reader is not None or writer is not None:
                await self._slot.run(_close_pair, reader, writer)


def _close_pair(
    reader: AuthenticatedMetadataReader | None, writer: AuthenticatedSourceWriter | None
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    _close(writer)
    _close(reader)


def _tombstone_activation(
    policy: SourcePolicySnapshot, fingerprint: str, directory: str
) -> SourceActivation:
    """One authenticated reader session over the three bootstrap files.

    Blocking; never opens the writer file. @spec PROTECTED-HOOK-SOURCE-3/6.
    """
    try:
        bootstrap = load_bootstrap(directory)
    except RuntimeFilesInvalid:
        return "closed", "runtime_unavailable"
    reader = None
    try:
        reader = _connect_reader(bootstrap, time.monotonic() + _DEADLINE_SECONDS)
        state = reader.read_source(str(policy.agent_id), policy.hook)
    except BrokerMetadataUnavailable:
        return "closed", "broker_unavailable"
    finally:
        _close(reader)
    if _published(state, policy.generation, str(policy.operation_id), fingerprint):
        return "active", None
    return "closed", "source_closed"


async def tombstone_activation(
    policy: SourcePolicySnapshot, directory: str | None
) -> SourceActivation:
    """Activation of a committed ordinary tombstone; the gate is already released.

    Reasons in order: ``authority_unavailable`` (no fingerprint),
    ``runtime_unavailable`` (setting unset or a file invalid),
    ``broker_unavailable`` (connection, identity or read failure, a full
    executor or an exhausted budget) and ``source_closed`` (any record
    mismatch). @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    try:
        fingerprint = committed_policy_fingerprint(policy)
    except Exception:
        return "closed", "authority_unavailable"
    if not directory:
        return "closed", "runtime_unavailable"
    try:
        async with admin_slot() as slot:
            return await slot.run(_tombstone_activation, policy, fingerprint, directory)
    except SourceAdminError:
        return "closed", "broker_unavailable"
