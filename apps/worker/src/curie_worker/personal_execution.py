"""Authorize and launch personal subscription executions (#5378, ADR 0268).

A WorkItem execution's mode is its ExecutionRequest's snapshot, never the
installation setting or anything a requester says. ``PersonalExecution`` turns
that snapshot into either nothing (a service request: the claim path is
unchanged) or a ``PersonalLaunch`` the kernel carries for one delivery.

The launch has two separated steps. ``authorize`` is SQL only and runs fresh
at every claim, adopt, steer and turn start: the owner tuple, the owner's
tenant, principal and enrollment rows, and this worker's authority over the
request (its acquire for a waiting request, the run's runtime epoch for a
started one). ``grant_env`` repeats ``authorize`` and only then reads custody,
and only when a new runtime is about to be created. The substrate calls it
inside the owner's launch lease (``PersonalCreate``), after all preparation
and after the owner's claim inventory, so the grant is never read for a
create that cannot happen. The create fence repeats ``authorize`` once more
after the predecessor teardown, immediately before the create write.

No log line, exception or repr here carries a grant. Refusals carry their
code only.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import Any, Literal, Protocol, TypeVar

from .binding import ExecutionAuthority, FactoryContext
from .personal_custody import SECRET_READ_TIMEOUT_S, GrantRefusal, PersonalGrant
from .sandbox.types import (
    AUTHENTICATION_REQUIRED,
    ClaimProvenance,
    KubeTransientError,
    OwnerBusy,
    PersonalLaunchRefused,
)
from .threadlock import LockAcquireTimeout, LockLease, LockLeaseLost, ThreadLock

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Structural refusals: the request can never run as it stands. A started
#: request that meets one finishes ``runner_escalated``.
PERSONAL_MODE_REQUIRES_KUBERNETES = "personal_mode_requires_kubernetes"
PERSONAL_OWNER_REQUIRED = "personal_owner_required"
EXECUTION_OWNER_UNKNOWN = "execution_owner_unknown"
OWNER_MISMATCH = "owner_mismatch"
OWNER_TENANT_INACTIVE = "owner_tenant_inactive"
OWNER_PRINCIPAL_INACTIVE = "owner_principal_inactive"
EXECUTION_AUTHORITY_LOST = "execution_authority_lost"
WORK_ITEM_CANCELLED = "work_item_cancelled"
OWNER_PROVENANCE_MISMATCH = "owner_provenance_mismatch"
#: Enrollment fences, classified authentication-required like a grant refusal:
#: the owner reconnects or replaces the enrollment, then the request resumes.
ENROLLMENT_INACTIVE = "enrollment_inactive"
ENROLLMENT_GENERATION_MISMATCH = "enrollment_generation_mismatch"

#: Bounds on one bridge from the substrate's worker thread to the kernel's
#: loop. A timeout is transient (the create is retried), never a refusal.
LEASE_BRIDGE_TIMEOUT_S = 5.0
GRANT_BRIDGE_TIMEOUT_S = SECRET_READ_TIMEOUT_S + 10.0
#: The create fence reads the authority rows and then the lease.
FENCE_BRIDGE_TIMEOUT_S = 2 * LEASE_BRIDGE_TIMEOUT_S
#: Margin the owner lease TTL keeps over the claim budget it spans.
OWNER_LEASE_MARGIN_S = 30.0

InstallationMode = Literal["service", "personal"]


class AuthoritySource(Protocol):
    async def resolve_execution_authority(
        self, request_id: uuid.UUID
    ) -> ExecutionAuthority | None: ...


class GrantSource(Protocol):
    async def read_grant(
        self, tenant_id: uuid.UUID, principal_id: uuid.UUID, generation: int
    ) -> PersonalGrant | GrantRefusal: ...


class RunView(Protocol):
    """What a launch reads, live, from the kernel's ``WorkItemRun``."""

    request_id: uuid.UUID
    owner: str
    generation: int
    started: bool
    runtime_epoch: int | None
    context: FactoryContext | None


def refused(code: str) -> PersonalLaunchRefused:
    """A structural refusal, or an enrollment fence (authentication-required)."""

    classification = (
        AUTHENTICATION_REQUIRED
        if code in (ENROLLMENT_INACTIVE, ENROLLMENT_GENERATION_MISMATCH)
        else None
    )
    return PersonalLaunchRefused(code, classification=classification)


def personal_owner_required() -> PersonalLaunchRefused:
    """A lane with no owner on a personal installation (ADR 0268 decision 1)."""

    return PersonalLaunchRefused(PERSONAL_OWNER_REQUIRED)


def _provenance_of(authority: ExecutionAuthority) -> ClaimProvenance | None:
    if (
        authority.tenant_id is None
        or authority.owner_principal_id is None
        or authority.enrollment_generation is None
    ):
        return None
    return ClaimProvenance(
        owner_tenant_id=str(authority.tenant_id),
        owner_principal_id=str(authority.owner_principal_id),
        enrollment_generation=authority.enrollment_generation,
    )


def _authority_lost(authority: ExecutionAuthority, run: RunView) -> str | None:
    """Whether this worker no longer holds the request it is launching."""

    if authority.work_item_cancelled:
        return WORK_ITEM_CANCELLED
    if run.started:
        lost = (
            authority.status != "running"
            or run.runtime_epoch is None
            or authority.runtime_epoch != run.runtime_epoch
            or not authority.deadline_live
        )
    else:
        lost = (
            authority.status != "waiting"
            or authority.acquire_owner != run.owner
            or authority.acquired_generation != run.generation
            or not authority.acquire_live
        )
    return EXECUTION_AUTHORITY_LOST if lost else None


class PersonalExecution:
    """The worker's personal execution authority (one per Kernel)."""

    def __init__(
        self,
        *,
        authority: AuthoritySource,
        custody: GrantSource,
        owner_lock: ThreadLock,
        owner_key: Callable[[ClaimProvenance], str],
        kubernetes: bool,
    ) -> None:
        self._authority = authority
        self._custody = custody
        self._owner_lock = owner_lock
        self._owner_key = owner_key
        self._kubernetes = kubernetes

    async def launch_for(self, run: RunView) -> PersonalLaunch | None:
        """None for a service request; a launch for a personal one.

        Raises ``PersonalLaunchRefused`` for an unknown or foreign request,
        an owner that does not match the acquired context, and every
        personal refusal ``authorize`` names.
        """

        authority = await self._read(run)
        if authority.model_auth_mode != "personal":
            return None
        provenance = _provenance_of(authority)
        if provenance is None:
            raise refused(PERSONAL_OWNER_REQUIRED)
        launch = PersonalLaunch(self, run, provenance)
        launch.check(authority)
        return launch

    async def _read(self, run: RunView) -> ExecutionAuthority:
        authority = await self._authority.resolve_execution_authority(run.request_id)
        if authority is None:
            raise refused(EXECUTION_OWNER_UNKNOWN)
        context = run.context
        if context is None or authority.agent_id != context.agent_id:
            raise refused(OWNER_MISMATCH)
        return authority


class PersonalLaunch:
    """One delivery's personal launch. Expectations are read live from the run."""

    def __init__(
        self, service: PersonalExecution, run: RunView, provenance: ClaimProvenance
    ) -> None:
        self._service = service
        self._run = run
        self.provenance = provenance

    @property
    def request_id(self) -> uuid.UUID:
        return self._run.request_id

    async def authorize(self) -> None:
        """Fresh SQL: raise ``PersonalLaunchRefused`` unless this launch may run."""

        self.check(await self._service._read(self._run))

    def check(self, authority: ExecutionAuthority) -> None:
        run = self._run
        assert run.context is not None
        if authority.model_auth_mode != "personal":
            raise refused(EXECUTION_AUTHORITY_LOST)
        provenance = _provenance_of(authority)
        if provenance is None:
            raise refused(PERSONAL_OWNER_REQUIRED)
        if authority.tenant_id != run.context.tenant_id:
            raise refused(OWNER_MISMATCH)
        if not self._service._kubernetes:
            raise refused(PERSONAL_MODE_REQUIRES_KUBERNETES)
        # The tuple a request snapshotted changes only through revalidation,
        # which the API refuses while this worker holds the request.
        if provenance != self.provenance:
            raise refused(EXECUTION_AUTHORITY_LOST)
        if code := _authority_lost(authority, run):
            raise refused(code)
        if authority.tenant_status != "active":
            raise refused(OWNER_TENANT_INACTIVE)
        if authority.principal_status != "active":
            raise refused(OWNER_PRINCIPAL_INACTIVE)
        if authority.enrollment_active is not True:
            raise refused(ENROLLMENT_INACTIVE)
        if authority.enrollment_current_generation != authority.enrollment_generation:
            raise refused(ENROLLMENT_GENERATION_MISMATCH)

    async def grant_env(self) -> dict[str, str]:
        """Authorize again, then read custody: the grant's claim env.

        A ``GrantRefusal`` raises authentication-required with the custody
        code. The returned mapping is the only place the grant appears.
        """

        await self.authorize()
        grant = await self._service._custody.read_grant(
            uuid.UUID(self.provenance.owner_tenant_id),
            uuid.UUID(self.provenance.owner_principal_id),
            self.provenance.enrollment_generation,
        )
        if isinstance(grant, GrantRefusal):
            raise PersonalLaunchRefused(grant.code, classification=grant.classification)
        return grant.claim_env()

    def guard(self, loop: asyncio.AbstractEventLoop) -> OwnerLaunchGuard:
        """The ``PersonalCreate`` one claim, resume or handoff runs under."""

        return OwnerLaunchGuard(self, loop)


class OwnerLaunchGuard:
    """``sandbox.types.PersonalCreate`` bridged to the kernel's event loop.

    The owner launch lease is the kernel's token-checked ``ThreadLock`` under
    its own key per tenant and principal. It is held by the loop (its renewer
    runs there) and taken with one non-blocking attempt, so a contended owner
    answers ``OwnerBusy`` at once and nothing ever waits on it. The kernel
    already holds the thread lock; nothing takes a thread lock while holding
    an owner lease, so the two are always taken in that order. ``close``
    releases the lease and is idempotent; after it every step refuses.
    """

    def __init__(self, launch: PersonalLaunch, loop: asyncio.AbstractEventLoop) -> None:
        self._launch = launch
        self._loop = loop
        self._stack: contextlib.AsyncExitStack | None = None
        self._lease: LockLease | None = None
        self._closed = False

    @property
    def provenance(self) -> ClaimProvenance:
        return self._launch.provenance

    # -- PersonalCreate (called on the substrate's worker thread) -------------

    def begin(self) -> None:
        self._bridge(self._begin, LEASE_BRIDGE_TIMEOUT_S, timeout_error=OwnerBusy)

    def grant_env(self) -> Mapping[str, str]:
        return self._bridge(
            self._launch.grant_env, GRANT_BRIDGE_TIMEOUT_S, timeout_error=_grant_unavailable
        )

    def fence(self) -> None:
        self._bridge(self._fence, FENCE_BRIDGE_TIMEOUT_S, timeout_error=OwnerBusy)

    # -- loop side -------------------------------------------------------------

    async def close(self) -> None:
        self._closed = True
        stack, self._stack = self._stack, None
        self._lease = None
        if stack is None:
            return
        try:
            await stack.aclose()
        except Exception as exc:  # noqa: BLE001 - a lost lease is already fenced
            logger.info("owner launch lease ended without a clean release (%s)", type(exc).__name__)

    async def _begin(self) -> None:
        if self._closed:
            raise OwnerBusy()
        if self._lease is not None:
            # A resume after a claim found the thread suspended: one lease
            # serves every create of this call, while it is still held.
            if self._lease.lost:
                raise OwnerBusy()
            return
        stack = contextlib.AsyncExitStack()
        lock = self._launch._service._owner_lock
        key = self._launch._service._owner_key(self.provenance)
        try:
            lease = await stack.enter_async_context(lock.hold(key))
        except LockAcquireTimeout:
            raise OwnerBusy() from None
        self._stack = stack
        self._lease = lease

    async def _fence(self) -> None:
        lease = self._lease
        if self._closed or lease is None:
            raise OwnerBusy()
        # The same SQL authority check again, after the predecessor teardown
        # and every other blocking step since the grant read: a disconnect,
        # an advanced enrollment or a cancellation then creates nothing.
        await self._launch.authorize()
        try:
            await lease.ensure_owned()
        except LockLeaseLost:
            raise OwnerBusy() from None
        if lease.lost:
            raise OwnerBusy()

    def _bridge(
        self,
        step: Callable[[], Awaitable[T]],
        timeout_s: float,
        *,
        timeout_error: Callable[[], Exception],
    ) -> T:
        """Run ``step`` on the kernel's loop from the substrate's thread.

        Safe because the kernel awaits the substrate through ``to_thread``,
        so its loop is free. Refusals and ``OwnerBusy`` pass through; any
        other failure is transient and names its class only.
        """

        with contextlib.suppress(RuntimeError):
            if asyncio.get_running_loop() is self._loop:
                raise RuntimeError("a personal launch step ran on its own event loop")

        async def run() -> T:
            return await step()

        coroutine: Coroutine[Any, Any, T] = run()
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout_s)
        except TimeoutError:
            future.cancel()
            raise timeout_error() from None
        except (PersonalLaunchRefused, OwnerBusy):
            raise
        except Exception as exc:  # noqa: BLE001 - transient, class name only
            raise KubeTransientError(
                f"personal launch step failed ({type(exc).__name__})"
            ) from None


def _grant_unavailable() -> Exception:
    return KubeTransientError("personal grant read timed out")


def owner_lease_ttl_ms(*, thread_lock_ttl_ms: int, claim_timeout_seconds: float) -> int:
    """An owner lease TTL that outlives the claim budget it spans."""

    return max(thread_lock_ttl_ms, int((claim_timeout_seconds + OWNER_LEASE_MARGIN_S) * 1000))
