"""Protected and tombstone ingress broker work, @spec PROTECTED-HOOK-SOURCE-2/6/8 LANE-4.

Signed delivery ingress reaches the protected broker only through here. Each
call opens one fresh ``AuthenticatedEnqueueClient`` connection from the
provisioner's ``enqueue.json`` (read through ``load_ingress``) under one five
second budget covering the connection and every call, and always closes it;
it never reconnects. Admission and the tombstone read share one ingress
executor of two threads that fails rather than queueing, so at most two gate
connections wait on broker I/O for ingress at any time. A request cancelled
during a broker call keeps waiting (and so keeps its gate) until that call
returns or its budget ends.

The global protected quota limit is a fixed 64 members in this release, equal
to the ordinary per agent default: capacity, not authority, so an API
constant rather than a provisioner file member. A queued protected turn above
the ADMISSION-2 limit of 262144 bytes is refused before any broker I/O.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Literal, TypeVar

from curie_protected_hooks.admission_records import (
    AdmissionRequest,
    AdmissionResult,
    AdmissionUnavailable,
    DeliveryIdentity,
)
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable
from curie_protected_hooks.broker_transport import (
    AuthenticatedEnqueueClient,
    metadata_reader_budget,
)
from curie_protected_hooks.source_policy_sql import SourcePolicySnapshot

from .hook_source_mutation import committed_policy_fingerprint
from .protected_runtime_files import IngressRuntime, RuntimeFilesInvalid, load_ingress

# @spec PROTECTED-HOOK-LANE-4
BACKLOG_LIMIT = 64
# @spec PROTECTED-HOOK-ADMISSION-2 @spec PROTECTED-HOOK-LANE-4
TURN_LIMIT = 262144
# @spec PROTECTED-HOOK-SOURCE-2
BUDGET_SECONDS = 5.0
_INGRESS_THREADS = 2
_SLOTS = threading.BoundedSemaphore(_INGRESS_THREADS)
_EXECUTOR = ThreadPoolExecutor(max_workers=_INGRESS_THREADS, thread_name_prefix="curie-ingress")

_T = TypeVar("_T")

TombstoneOutcome = Literal["open", "source_closed", "delivery_conflict"]


class IngressBrokerUnavailable(Exception):
    """A full executor, an exhausted budget or any broker failure, @spec PROTECTED-HOOK-SOURCE-2."""


async def read_ingress_runtime(directory: str | None) -> IngressRuntime | None:
    """The runtime and enqueue files read afresh, or None when unset or invalid.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-8.
    """
    if not directory:
        return None
    try:
        return await asyncio.to_thread(load_ingress, directory)
    except RuntimeFilesInvalid:
        return None


class IngressSlot:
    """One of the two ingress executor slots, held from acquisition to release.

    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-8.
    """

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self._lock = threading.Lock()
        self._running = 0
        self._finished = False
        self._released = False

    def _release_if_idle(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        with self._lock:
            if self._released or not self._finished or self._running:
                return
            self._released = True
        _SLOTS.release()

    def _done(self, _future: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        with self._lock:
            self._running -= 1
        self._release_if_idle()

    def finish(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        with self._lock:
            self._finished = True
        self._release_if_idle()

    async def run(self, call: Callable[..., _T], *args: Any) -> _T:
        """Run one blocking broker call; cancellation waits for it to end.

        @spec PROTECTED-HOOK-SOURCE-2.
        """
        with self._lock:
            if self._finished:
                raise IngressBrokerUnavailable()
            self._running += 1
        try:
            submitted = _EXECUTOR.submit(call, *args)
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
                except BaseException:  # noqa: BLE001  A cancelled wait drains on any outcome before cancellation re-raises.
                    break
            raise


@asynccontextmanager
async def ingress_slot() -> AsyncIterator[IngressSlot]:
    """Take a free ingress slot or raise ``IngressBrokerUnavailable`` at once.

    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-8.
    """
    if not _SLOTS.acquire(blocking=False):
        raise IngressBrokerUnavailable()
    slot = IngressSlot()
    try:
        yield slot
    finally:
        slot.finish()


def _connect(runtime: IngressRuntime) -> AuthenticatedEnqueueClient:
    """@spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-2."""
    return AuthenticatedEnqueueClient.connect(
        runtime.bootstrap.manifest, runtime.enqueue, runtime.bootstrap.ca_pem
    )


def _close(client: AuthenticatedEnqueueClient | None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if client is None:
        return
    try:
        client.close()
    except BrokerMetadataUnavailable:
        pass


def _admit(runtime: IngressRuntime, request: AdmissionRequest) -> AdmissionResult:
    """One enqueue connection, one atomic admission, always closed.

    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-ADMISSION-1/4.
    """
    client = None
    try:
        with metadata_reader_budget(BUDGET_SECONDS):
            client = _connect(runtime)
            facade = client.admission(
                runtime.bootstrap.manifest,
                trusted_max_readiness_ms=runtime.bootstrap.max_readiness_ms,
                backlog_limit=BACKLOG_LIMIT,
            )
            return facade.admit(request)
    except (AdmissionUnavailable, BrokerMetadataUnavailable):
        raise IngressBrokerUnavailable() from None
    finally:
        _close(client)


async def admit(
    slot: IngressSlot, runtime: IngressRuntime, request: AdmissionRequest
) -> AdmissionResult:
    """Admit one protected delivery on the held ingress slot, @spec PROTECTED-HOOK-SOURCE-2/8."""
    return await slot.run(_admit, runtime, request)


def _tombstone(
    runtime: IngressRuntime, policy: SourcePolicySnapshot, fingerprint: str, delivery_id: str
) -> TombstoneOutcome:
    """The source record and the delivery's private intent key on one enqueue connection.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-6.
    """
    client = None
    try:
        identity = DeliveryIdentity(
            agent_id=str(policy.agent_id), hook=policy.hook, delivery_id=delivery_id
        )
    except ValueError:
        identity = None
    try:
        with metadata_reader_budget(BUDGET_SECONDS):
            client = _connect(runtime)
            state = client.read_source(str(policy.agent_id), policy.hook)
            active = state["active"]
            if not (
                state["floor"] == policy.generation
                and state["operation_id"] == str(policy.operation_id)
                and active is not None
                and active["generation"] == policy.generation
                and active["operation_id"] == str(policy.operation_id)
                and active["mode"] == "ordinary"
                and active["policy_fingerprint"] == fingerprint
            ):
                return "source_closed"
            # A delivery ID the protected store cannot name has no private intent.
            if identity is not None and client.intent_present(identity):
                return "delivery_conflict"
            return "open"
    except Exception:  # noqa: BLE001  Any broker read failure, expected or not, is unavailable.
        raise IngressBrokerUnavailable() from None
    finally:
        _close(client)


async def tombstone_check(
    slot: IngressSlot, runtime: IngressRuntime, policy: SourcePolicySnapshot, delivery_id: str
) -> TombstoneOutcome:
    """Whether a tombstone's ordinary publication is active and the delivery is not private.

    @spec PROTECTED-HOOK-SOURCE-8.
    """
    try:
        fingerprint = committed_policy_fingerprint(policy)
    except Exception:  # noqa: BLE001  A row whose fingerprint fails admits nothing.
        raise IngressBrokerUnavailable() from None
    return await slot.run(_tombstone, runtime, policy, fingerprint, delivery_id)
