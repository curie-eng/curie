"""The API admission reconciliation owner, @spec PROTECTED-HOOK-LANE-4 PROTECTED-HOOK-ADMISSION-5.

One ``ProtectedAdmissionReconciler`` per API process owns preparing intents
that hold capacity. The lifespan starts it inside the source resource scope
and stops it before the Valkey client and engines close: a stop signal, then
cancellation and a ten second join. Each tick starts five seconds after the
previous one ends. With the runtime setting unset a tick does nothing;
otherwise it loads the runtime and enqueue files afresh (``load_ingress``) and
an invalid file skips the tick without broker I/O. A valid tick opens one
enqueue connection under a five second budget, lists preparing intents through
the admission facade (every quota member, the set never exceeding the 64
member limit) and calls ``recover`` for each until the budget ends, leaving
the rest to the next tick. One intent's ``AdmissionUnavailable`` is counted and
the tick continues; a connection or budget failure ends the tick. The loop is
supervised: an unexpected exception is logged safely and the next tick runs.

Broker calls run on the reconciler's single dedicated thread, never an ingress
or probe thread, and an in flight call finishes under its own budget. It holds
no SQL gate and fabricates no HTTP authentication: ``recover`` checks the
original source authority in the broker. Every replica runs its own
reconciler; the facade's script and torn read retry decide races with ingress
retries and with other reconcilers. Each tick logs only counts: quota
occupancy, committed members parked without a worker, and preparing,
recovered, failed and skipped intents; never an identity, payload or
credential. An intent interrupted before its quota member holds no capacity
and is left to a caller retry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from curie_protected_hooks.admission_records import AdmissionUnavailable
from curie_protected_hooks.atomic_admission import quota_occupancy
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable
from curie_protected_hooks.broker_transport import (
    AuthenticatedEnqueueClient,
    metadata_reader_budget,
)

from .protected_ingress import BACKLOG_LIMIT, BUDGET_SECONDS
from .protected_runtime_files import IngressRuntime, RuntimeFilesInvalid, load_ingress

logger = logging.getLogger(__name__)

# @spec PROTECTED-HOOK-LANE-4
TICK_SECONDS = 5.0
JOIN_SECONDS = 10.0


@dataclass
class TickCounts:
    """One tick's safe counts, @spec PROTECTED-HOOK-LANE-4."""

    quota: int = 0
    parked: int = 0
    preparing: int = 0
    recovered: int = 0
    failed: int = 0
    skipped: int = 0


def _tick(runtime: IngressRuntime) -> TickCounts:
    """One budgeted enqueue session: list, then recover each until the budget ends.

    Raises on a connection or budget failure, which ends the tick.
    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5.
    """
    counts = TickCounts()
    deadline = time.monotonic() + BUDGET_SECONDS
    client = None
    try:
        with metadata_reader_budget(BUDGET_SECONDS):
            client = AuthenticatedEnqueueClient.connect(
                runtime.bootstrap.manifest, runtime.enqueue, runtime.bootstrap.ca_pem
            )
            facade = client.admission(
                runtime.bootstrap.manifest,
                trusted_max_readiness_ms=runtime.bootstrap.max_readiness_ms,
                backlog_limit=BACKLOG_LIMIT,
            )
            identities = facade.preparing(BACKLOG_LIMIT)
            counts.quota = quota_occupancy(facade)
            counts.preparing = len(identities)
            counts.parked = max(0, counts.quota - counts.preparing)
            for identity in identities:
                if time.monotonic() >= deadline:
                    break
                try:
                    result = facade.recover(identity)
                except AdmissionUnavailable:
                    counts.skipped += 1
                    continue
                if result.status in ("accepted", "duplicate"):
                    counts.recovered += 1
                elif result.status == "failed":
                    counts.failed += 1
    finally:
        if client is not None:
            try:
                client.close()
            except BrokerMetadataUnavailable:
                pass
    return counts


class ProtectedAdmissionReconciler:
    """Supervised five second loop over preparing intents, @spec PROTECTED-HOOK-LANE-4."""

    def __init__(self, directory: Callable[[], str | None]) -> None:
        """``directory`` reads the runtime setting afresh each tick, @spec PROTECTED-HOOK-LANE-4."""
        self._directory = directory
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="curie-protected-reconciler"
        )

    def start(self) -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="protected-admission-reconciler")

    async def stop(self) -> None:
        """Stop signal, cancellation and a ten second join, @spec PROTECTED-HOOK-LANE-4.

        A broker call in flight keeps running on the dedicated thread until its
        own budget ends; the join waits for it within the same bound.
        """
        started = time.monotonic()
        self._stop.set()
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), JOIN_SECONDS)
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001  Shutdown never fails on a reconciler fault.
                logger.warning("protected admission reconciler stopped with an error")
        remaining = max(0.0, JOIN_SECONDS - (time.monotonic() - started))
        await asyncio.to_thread(self._drain, remaining)

    def _drain(self, seconds: float) -> None:
        """Wait up to ``seconds`` for the dedicated thread, @spec PROTECTED-HOOK-LANE-4."""
        future = self._executor.submit(lambda: None)
        try:
            future.result(timeout=seconds)
        except Exception:  # noqa: BLE001  A slow broker call ends under its own budget.
            pass
        self._executor.shutdown(wait=False)

    async def _run(self) -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), TICK_SECONDS)
                return
            except TimeoutError:
                pass
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001  The supervised loop resumes on the next tick.
                logger.warning(
                    "protected admission reconciler tick failed: %s", type(error).__name__
                )

    async def tick(self) -> TickCounts | None:
        """One tick; None when the setting is unset or a runtime file invalid.

        @spec PROTECTED-HOOK-LANE-4.
        """
        directory = self._directory()
        if not directory:
            return None
        loop = asyncio.get_running_loop()
        try:
            runtime = await loop.run_in_executor(self._executor, load_ingress, directory)
        except RuntimeFilesInvalid:
            return None
        try:
            counts = await loop.run_in_executor(self._executor, _tick, runtime)
        except (AdmissionUnavailable, BrokerMetadataUnavailable):
            logger.info("protected admission reconciler tick outcome=broker_unavailable")
            return None
        busy = counts.quota or counts.preparing or counts.skipped
        logger.log(
            logging.INFO if busy else logging.DEBUG,
            "protected admission reconciler tick outcome=ok quota=%d parked=%d "
            "preparing=%d recovered=%d failed=%d skipped=%d",
            counts.quota,
            counts.parked,
            counts.preparing,
            counts.recovered,
            counts.failed,
            counts.skipped,
        )
        return counts
