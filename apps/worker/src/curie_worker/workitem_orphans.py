"""Recover WorkItem runs orphaned by a worker restart (#3076).

A run is orphaned when its runtime owner is provably gone. A row carrying this
process's own name is orphaned when the kernel does not hold it: a restarted
container reuses its name, so the previous incarnation's runs look like ours.
A peer row is orphaned only after two absent liveness observations at least
absence_proof_s apart, so one missed heartbeat never kills a live run. The
sweeper asks the API to declare the owner lost; the existing terminate chain
then tears down the sandbox and fails the run.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from .workitem_dispatch import WorkItemConflict, WorkItemTransportError

logger = logging.getLogger(__name__)


class _RuntimeOwner(Protocol):
    @property
    def request_id(self) -> uuid.UUID: ...
    @property
    def runtime_owner(self) -> str: ...
    @property
    def runtime_epoch(self) -> int: ...


class OrphanClient(Protocol):
    async def runtime_owners(
        self, after: uuid.UUID | None = None
    ) -> Sequence[_RuntimeOwner]: ...

    async def declare_owner_lost(
        self, request_id: uuid.UUID, *, owner: str, runtime_epoch: int
    ) -> None: ...


class WorkItemOrphanSweeper:
    def __init__(
        self,
        client: OrphanClient,
        liveness: Callable[[str], Awaitable[bool]],
        *,
        self_name: str,
        locally_owned: Callable[[uuid.UUID], bool],
        absence_proof_s: float,
        interval_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._liveness = liveness
        self._self_name = self_name
        self._locally_owned = locally_owned
        self._absence_proof_s = absence_proof_s
        self._interval_s = interval_s
        self._clock = clock
        # Peer name to the time it was first observed absent.
        self._first_absent: dict[str, float] = {}

    async def _peer_gone(self, owner: str, observed: dict[str, bool]) -> bool:
        # One liveness observation per owner per sweep.
        if owner not in observed:
            if await self._liveness(owner):
                self._first_absent.pop(owner, None)
                observed[owner] = False
            else:
                now = self._clock()
                first = self._first_absent.setdefault(owner, now)
                observed[owner] = now - first >= self._absence_proof_s
        return observed[owner]

    async def _orphaned(self, row: _RuntimeOwner, observed: dict[str, bool]) -> bool:
        if row.runtime_owner == self._self_name:
            return not self._locally_owned(row.request_id)
        return await self._peer_gone(row.runtime_owner, observed)

    async def sweep(self) -> int:
        """Declare every orphaned run once; return how many were declared."""

        declared = 0
        observed: dict[str, bool] = {}
        after: uuid.UUID | None = None
        while True:
            try:
                rows = await self._client.runtime_owners(after=after)
            except WorkItemTransportError:
                logger.warning(
                    "work-item orphan sweep could not list runtime owners", exc_info=True
                )
                return declared
            if not rows:
                return declared
            after = rows[-1].request_id
            for row in rows:
                try:
                    if not await self._orphaned(row, observed):
                        continue
                    await self._client.declare_owner_lost(
                        row.request_id,
                        owner=row.runtime_owner,
                        runtime_epoch=row.runtime_epoch,
                    )
                except WorkItemConflict as exc:
                    logger.info(
                        "work-item owner-lost refused for %s: %s", row.request_id, exc.code
                    )
                    continue
                except WorkItemTransportError:
                    logger.warning(
                        "work-item orphan sweep stopped at %s", row.request_id, exc_info=True
                    )
                    return declared
                declared += 1
                logger.warning(
                    "declared work-item %s orphaned (owner %s, epoch %d)",
                    row.request_id,
                    row.runtime_owner,
                    row.runtime_epoch,
                )

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval_s)
                return
            except TimeoutError:
                pass
            await self.sweep()
