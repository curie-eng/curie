"""Campaign supervisor of the commissioning control process.

@spec PROTECTED-COMMISSIONING-3 PROTECTED-COMMISSIONING-8. The supervisor owns
cancellation after expiry, exhaustion, revocation or broker loss. It watches
the campaign journal and stops at the first of:

* ``expired``: broker ``TIME`` or this host's clock reached the permit's
  ``expires_at_ms``;
* ``exhausted``: the journal is ``closing`` with an exhausted counter;
* ``revoked``: the operator revoked the campaign in this process;
* ``closing``: the journal left ``running`` for any other reason, which is
  also how a restarted control process finds a campaign it must resume;
* ``broker_lost``: the journal could not be read, including a broker whose
  ``run_id`` changed.

Stopping fences the mediator first, so no later launch is charged or created.
An expired or revoked campaign is moved to ``closing`` (exhaustion already is;
a lost broker cannot be written). Then every registered SDK and runner session
is cancelled and joined, and only then is every recorded workload retired from
durable ownership, which confirms the original runner container terminated
before the protected finalizer is removed and the workload then absent.

The report never assumes success. A session that resists cancellation, a
launch whose ownership is not ``retired`` or an unknown remainder after broker
loss leaves the cleanup incomplete, which blocks acceptance. Nothing here
grants launch, turn or provider authority, writes a closed record or confirms
the journal ``closed``.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal, Protocol

__all__ = [
    "CampaignLaunches",
    "CampaignSupervisor",
    "CampaignView",
    "CloseReport",
    "StopReason",
    "SupervisorRefused",
]

StopReason = Literal["expired", "exhausted", "revoked", "closing", "broker_lost"]


class SupervisorRefused(Exception):
    def __init__(self) -> None:
        super().__init__("Commissioning supervisor refused")


class CampaignState(Protocol):
    @property
    def state(self) -> str: ...
    @property
    def exhausted(self) -> str | None: ...
    @property
    def broker_time_ms(self) -> int: ...


class CampaignView(Protocol):
    """The control principal's journal view of one campaign."""

    @property
    def expires_at_ms(self) -> int: ...

    async def status(self) -> CampaignState:
        """The journal, read with broker ``TIME``; available after expiry."""
        ...

    async def begin_close(self) -> None:
        """``prepared`` or ``running`` to ``closing``; inert otherwise."""
        ...


class CampaignLaunches(Protocol):
    """The campaign mediator's cleanup surface."""

    def fence(self) -> None:
        """Refuse every later create."""
        ...

    async def close_campaign(self) -> tuple[str, ...]:
        """Retire every owned launch; the launch IDs whose ownership is not ``retired``."""
        ...


@dataclass(frozen=True, slots=True)
class CloseReport:
    """What one closing observed; complete only with nothing left unknown or owned."""

    reason: StopReason
    joined: bool
    unsettled: tuple[str, ...] | None

    @property
    def complete(self) -> bool:
        return self.joined and self.unsettled == ()


class CampaignSupervisor:
    """Watch one campaign and close it once, @spec PROTECTED-COMMISSIONING-3/8."""

    def __init__(
        self,
        campaign: CampaignView,
        launches: CampaignLaunches,
        *,
        interval_s: float = 1.0,
        join_s: float = 10.0,
        close_s: float = 120.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        for value in (interval_s, join_s, close_s):
            if type(value) not in (int, float) or not 0 < value <= 3600:
                raise SupervisorRefused()
        self._campaign, self._launches = campaign, launches
        self._interval_s, self._join_s, self._close_s = interval_s, join_s, close_s
        self._clock = clock
        self._sessions: set[asyncio.Task[Any]] = set()
        self._stopping = False
        self._revoked = False
        self._ran = False
        self._wake = asyncio.Event()

    @property
    def stopping(self) -> bool:
        return self._stopping

    def own(self, work: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Register one SDK or runner session before it runs; refused once stopping.

        No await separates the check from the registration, so the session is
        cancellable and joined by the supervisor from its first step.
        """
        if self._stopping:
            work.close()
            raise SupervisorRefused()
        task = asyncio.create_task(work, name="commissioning-owned-session")
        self._sessions.add(task)
        task.add_done_callback(self._sessions.discard)
        return task

    def revoke(self) -> None:
        """Stop the campaign at the next observation."""
        self._revoked = True
        self._wake.set()

    async def _cause(self) -> StopReason | None:
        expires = self._campaign.expires_at_ms
        if self._revoked:
            return "revoked"
        if int(self._clock() * 1000) >= expires:
            return "expired"
        try:
            state = await self._campaign.status()
        except Exception:  # noqa: BLE001 - an unreadable journal is broker loss, never retried as authority
            return "broker_lost"
        if state.broker_time_ms >= expires:
            return "expired"
        if state.state == "closing" and state.exhausted is not None:
            return "exhausted"
        if state.state not in {"prepared", "running"}:
            return "closing"
        return None

    async def run(self) -> CloseReport:
        """Watch until the first stop cause, then close the campaign once."""
        if self._ran:
            raise SupervisorRefused()
        self._ran = True
        while (reason := await self._cause()) is None:
            try:
                await asyncio.wait_for(self._wake.wait(), self._interval_s)
            except TimeoutError:
                pass
            self._wake.clear()
        return await self._close(reason)

    async def _close(self, reason: StopReason) -> CloseReport:
        self._stopping = True
        self._launches.fence()
        if reason in {"expired", "revoked"}:
            try:
                await self._campaign.begin_close()
            except Exception:  # noqa: BLE001 - cleanup never waits on the broker
                pass
        sessions = tuple(self._sessions)
        for task in sessions:
            task.cancel()
        joined = True
        if sessions:
            _done, pending = await asyncio.wait(sessions, timeout=self._join_s)
            joined = not pending
        unsettled: tuple[str, ...] | None = None
        deadline = asyncio.get_running_loop().time() + self._close_s
        while True:
            try:
                unsettled = tuple(await self._launches.close_campaign())
            except Exception:  # noqa: BLE001 - what is left is unknown, never assumed retired
                unsettled = None
            if unsettled == () or asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(self._interval_s)
        return CloseReport(reason, joined, unsettled)
