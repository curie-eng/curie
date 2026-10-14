"""Explicit mediator runner wrapper; no generic URL, bearer or steer forwarding."""

from __future__ import annotations

from collections.abc import Mapping

from aci_protocol import Event

from .protected_launch_client import ProtectedLaunchClient, _refuse
from .protected_launch_records import WorkerLaunchCapability
from .protected_substrate import ProtectedSandboxSubstrate
from .runner_client import TimeoutResult, TurnStream
from .sandbox.types import SandboxHandle


class ProtectedRunnerClient:
    """Preserve ACI stream/epoch semantics using only privately owned targets."""

    def __init__(self, client: ProtectedLaunchClient, substrate: ProtectedSandboxSubstrate) -> None:
        if (
            type(client) is not ProtectedLaunchClient
            or type(substrate) is not ProtectedSandboxSubstrate
        ):
            _refuse()
        if substrate._client is not client:
            _refuse()
        self._client, self._substrate = client, substrate

    def _capability(self, target: SandboxHandle | str, token: str | None) -> WorkerLaunchCapability:
        if token:
            _refuse()
        if type(target) is SandboxHandle:
            return self._substrate.capability_for(target)
        if type(target) is str:
            return self._substrate.capability_for_route(target)
        _refuse()
        raise AssertionError("unreachable")

    def turn_deadline_s(self, remaining_s: float | None) -> float:
        return self._client.turn_deadline_s(remaining_s)

    async def start_turn(
        self,
        target: SandboxHandle | str,
        event: Event,
        token: str | None = None,
        *,
        remaining_s: float | None = None,
        capacity_admission: bool = False,
        progress: Mapping[str, str] | None = None,
    ) -> TurnStream:
        if progress:
            _refuse()
        return await self._client.start_turn(
            self._capability(target, token),
            event,
            remaining_s=remaining_s,
            capacity_admission=capacity_admission,
        )

    async def admit_turn(
        self,
        target: SandboxHandle | str,
        turn_epoch: str,
        *,
        allow: bool,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> None:
        await self._client.admit_turn(
            self._capability(target, token), turn_epoch, allow=allow, remaining_s=remaining_s
        )

    async def steer(self, *_args: object, **_kwargs: object) -> None:
        _refuse()

    async def interrupt(
        self, target: SandboxHandle | str, reason: str, token: str | None = None
    ) -> None:
        # Server owns the fixed reason; user-controlled reason is never sent.
        _ = reason
        await self._client.interrupt(self._capability(target, token))

    async def timeout_turn(
        self, target: SandboxHandle | str, turn_epoch: str, token: str | None = None
    ) -> TimeoutResult:
        return await self._client.timeout_turn(self._capability(target, token), turn_epoch)

    async def status(
        self,
        target: SandboxHandle | str,
        *,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        return await self._client.status(self._capability(target, token), remaining_s=remaining_s)

    async def capacity_status(
        self,
        target: SandboxHandle | str,
        *,
        epoch: str | None = None,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        return await self._client.capacity_status(
            self._capability(target, token), epoch=epoch, remaining_s=remaining_s
        )

    async def close(self) -> None:
        await self._client.close()

    async def __aenter__(self) -> ProtectedRunnerClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
