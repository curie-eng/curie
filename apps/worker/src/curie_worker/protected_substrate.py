"""Cold-only protected substrate; kernel handles never carry runner secrets.

The sync kernel bridge submits to one captured explicit owning loop. A caller
must establish its original claim scope and install only ProtectedRunnerClient
for these handles. No warm lookup or generic substrate
method forwarding confers execution permission. Retained lookup is stop-only.
"""

from __future__ import annotations

import asyncio
import contextvars
import math
import time
import uuid
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

from curie_protected_hooks.worker_verification import Dispatchable

from .kernel.routing import _thread_key_for
from .protected_binding import _CURRENT_DELIVERY
from .protected_launch_client import ProtectedLaunchClient, _refuse
from .protected_launch_records import LaunchRequest, WorkerLaunchCapability, canonical_json
from .protected_retirement import CleanupEvidence
from .sandbox.types import SandboxHandle


@dataclass(slots=True, repr=False)
class _HandleTarget:
    handle: SandboxHandle
    capability: WorkerLaunchCapability


@dataclass(slots=True)
class _ClaimBudget:
    deadline: float
    tasks: set[asyncio.Task[Any]]


_CLAIM_BUDGET: contextvars.ContextVar[_ClaimBudget | None] = contextvars.ContextVar(
    "protected_original_claim_budget", default=None
)
_T = TypeVar("_T")


@asynccontextmanager
async def protected_claim_scope(deadline: float) -> AsyncIterator[None]:
    """Carry original absolute monotonic budget and join canceled owned claims.

    This local clock is no execution proof. Parent runtime must derive it once
    from its actual delivery lease; nested scope must never extend that budget.
    """
    prior = _CLAIM_BUDGET.get()
    if prior is not None:
        deadline = min(deadline, prior.deadline)
    if not math.isfinite(deadline) or deadline <= time.monotonic():
        _refuse()
    budget = _ClaimBudget(deadline, set())
    token = _CLAIM_BUDGET.set(budget)
    try:
        yield
    finally:
        _CLAIM_BUDGET.reset(token)
        tasks = [task for task in budget.tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            try:
                async with asyncio.timeout(5):
                    await asyncio.gather(*tasks, return_exceptions=True)
            except TimeoutError:
                pass  # issuer retains independent cleanup ownership/deadline


class ProtectedSandboxSubstrate:
    """Only this instance maps its actual kernel handles to issued capabilities."""

    def __init__(self, client: ProtectedLaunchClient) -> None:
        if type(client) is not ProtectedLaunchClient:
            _refuse()
        self._client = client
        self._loop = asyncio.get_running_loop()
        self._handles: dict[str, _HandleTarget] = {}
        self._claims: set[str] = set()

    async def claim_async(
        self, thread_key: str, *, remaining_s: float, delivery: Dispatchable | None = None
    ) -> SandboxHandle:
        reserved = False
        try:
            selected = _CURRENT_DELIVERY.get()
            if (
                type(selected) is not Dispatchable
                or (delivery is not None and delivery is not selected)
                or thread_key != _thread_key_for(selected.turn)
                or thread_key in self._handles
                or thread_key in self._claims
            ):
                _refuse()
            assert isinstance(selected, Dispatchable)
            self._claims.add(thread_key)
            reserved = True
            envelope = selected.envelope.as_dict()
            request = LaunchRequest(
                canonical_json(
                    {
                        "schema_version": 1,
                        "request_id": str(uuid.uuid4()),
                        **{
                            key: envelope[key]
                            for key in (
                                "runtime_id",
                                "runtime_generation",
                                "manifest_digest",
                                "event_id",
                                "payload_sha256",
                                "execution_session_key",
                            )
                        },
                        "committed_stream_id": selected.intent.as_dict()["reserved_stream_id"],
                        "source_fields": dict(selected.source_fields),
                    }
                )
            )
            capability = await self._client.create(
                request, delivery=selected, remaining_s=remaining_s
            )
            # Deliberately unroutable marker, never the trusted mediator origin or
            # an actual Pod address. Only this wrapper's private map resolves it.
            handle = SandboxHandle(
                thread_key=thread_key,
                claim_name=capability.launch_id,
                sandbox_name=capability.launch_id,
                namespace="",
                service_fqdn=capability.launch_id + ".invalid",
                port=0,
                session_id=envelope["execution_session_key"],
                token="",
            )
            if thread_key in self._handles:
                await self._client.retire(capability)
                _refuse()
            self._handles[thread_key] = _HandleTarget(handle, capability)
            return handle
        except Exception:  # noqa: BLE001 - never expose selected source fields
            _refuse()
        finally:
            if reserved:
                self._claims.discard(thread_key)
        raise AssertionError("unreachable")

    def _submit(self, coroutine: Coroutine[Any, Any, _T], timeout: float) -> _T:
        try:
            if asyncio.get_running_loop() is self._loop:
                coroutine.close()
                _refuse()
        except RuntimeError:
            pass  # expected: kernel calls the sync seam through to_thread
        if not self._loop.is_running():
            coroutine.close()
            _refuse()
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except Exception:  # noqa: BLE001 - detach background transport details
            future.cancel()
            _refuse()
        raise AssertionError("unreachable")

    def claim(
        self,
        thread_key: str,
        *,
        env: dict[str, str] | None = None,
        agent_name: str | None = None,
        runner_resources: dict[str, Any] | None = None,
        fresh_only: bool = False,
        caller_run: str | None = None,
    ) -> SandboxHandle:
        """Sync kernel bridge; same explicit loop, no new client or fallback."""
        _ = agent_name, fresh_only
        budget = _CLAIM_BUDGET.get()
        if budget is None or env or runner_resources or caller_run:
            _refuse()
        assert budget is not None
        remaining = budget.deadline - time.monotonic()
        if remaining <= 0:
            _refuse()

        async def owned_claim() -> SandboxHandle:
            task = asyncio.current_task()
            assert task is not None
            budget.tasks.add(task)
            try:
                return await self.claim_async(thread_key, remaining_s=remaining)
            finally:
                budget.tasks.discard(task)

        return self._submit(owned_claim(), min(remaining, 120.0) + 5.0)

    def capability_for(self, handle: SandboxHandle) -> WorkerLaunchCapability:
        if type(handle) is not SandboxHandle:
            _refuse()
        target = self._handles.get(handle.thread_key)
        if target is None or target.handle is not handle:
            _refuse()
        assert target is not None
        return target.capability

    def capability_for_route(self, route: str) -> WorkerLaunchCapability:
        """Compatibility routing for explicit runner wrapper, never URL dialing."""
        matches = [target for target in self._handles.values() if target.handle.base_url == route]
        if len(matches) != 1:
            _refuse()
        return matches[0].capability

    def lookup(self, thread_key: str) -> SandboxHandle | None:
        """Retained owned stop target only; cold claim never adopts this handle."""
        target = self._handles.get(thread_key)
        return target.handle if target else None

    async def release_async(self, handle: SandboxHandle) -> bool:
        capability = self.capability_for(handle)
        retired = await self._client.retire(capability)
        if retired:
            del self._handles[handle.thread_key]
        return retired

    async def retire_delivery(self, delivery: Dispatchable) -> CleanupEvidence | None:
        """Only this substrate's exact delivery and issued handle can settle."""
        if type(delivery) is not Dispatchable:
            _refuse()
        thread_key = _thread_key_for(delivery.turn)
        if thread_key in self._claims:
            return None
        target = self._handles.get(thread_key)
        if target is None:
            return self._client.owned_retirement_for_delivery(delivery)
        owned = self._client._target(target.capability)
        if owned.delivery is not delivery:
            _refuse()
        proof = await self._client.retire_verified(target.capability)
        if proof is not None:
            if not self._client.is_owned_retirement(proof, delivery=delivery):
                _refuse()
            if self._handles.get(thread_key) is target:
                del self._handles[thread_key]
        return proof

    def release(self, thread_key: str) -> bool:
        target = self._handles.get(thread_key)
        if target is None:
            return False
        return self._submit(self.release_async(target.handle), 5.1)

    def release_if_claim(self, thread_key: str, claim_name: str) -> bool:
        target = self._handles.get(thread_key)
        if target is None or target.handle.claim_name != claim_name:
            return False
        return self.release(thread_key)

    def adopt(self, _thread_key: str) -> None:
        _refuse()

    def resume(self, *_args: object, **_kwargs: object) -> None:
        _refuse()

    def handoff(self, *_args: object, **_kwargs: object) -> None:
        _refuse()

    @property
    def claim_timeout_seconds(self) -> float:
        return 120.0

    @property
    def namespace(self) -> str:
        return ""

    async def close(self) -> None:
        for target in list(self._handles.values()):
            await self.release_async(target.handle)
