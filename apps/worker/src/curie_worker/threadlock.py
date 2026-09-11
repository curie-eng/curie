"""A Valkey-backed per-thread lock: one live session per thread across workers.

The routing decision (is there a live route? steer it, or open a new turn) plus
the turn opening must be atomic per thread, or two workers racing two events for
the same thread could each open a turn and violate one-live-session-per-thread.
This is a standard single-instance Redis lock: ``SET key token NX PX ttl`` to
acquire, and a Lua compare-and-delete to release only our own token (so a lock
that expired and was re-taken by another worker is never released by us).

The lock is held only for the bounded critical section (decision + turn start),
never for the whole stream, so a follow-up can steer the live turn.

A force-killed holder cannot release or renew (#2500). Tokens are
``{consumer}{RS}{boot}{RS}{nonce}`` so a replacement can CAS-steal when that
consumer's alive lease is gone, or when the same consumer name is back with a
new boot id (replicas:1 kubelet restart). Opaque legacy values are never
stolen: they stay on TTL, matching #849's foreign-holder timeout. Live capable
owners still serialize. The TTL itself is unchanged and still outlives a live
cold claim.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Protocol

from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from redis.asyncio import Redis

# ASCII RS: consumer names are hostname-pid and do not contain this.
_OWNER_SEP = "\x1e"

# Release only if we still own the lock (token match); avoids deleting a lock a
# later holder acquired after ours expired.
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

_RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end
"""

_STEAL_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('set', KEYS[1], ARGV[2], 'PX', ARGV[3])
    return 1
else
    return 0
end
"""


class OwnerLiveness(Protocol):
    """Consumer heartbeat view used to steal a crashed owner's lock (#2500)."""

    async def is_alive(self, owner: str) -> bool: ...

    async def is_capable(self, owner: str) -> bool: ...


def _lock_token(owner: str | None, boot_id: str | None) -> str:
    nonce = uuid.uuid4().hex
    if owner and boot_id:
        return f"{owner}{_OWNER_SEP}{boot_id}{_OWNER_SEP}{nonce}"
    return nonce


def _parse_lock_parts(value: str) -> tuple[str, str] | None:
    owner, sep, rest = value.partition(_OWNER_SEP)
    if not sep or not owner or not rest:
        return None
    boot_id, sep, nonce = rest.partition(_OWNER_SEP)
    if not sep or not boot_id or not nonce:
        return None
    return owner, boot_id


class LockAcquireTimeout(TimeoutError):
    """The per-thread lock was not acquired within the configured timeout.

    A ``TimeoutError`` subclass on purpose (#849): it genuinely is a timeout, and
    the kernel's turn path treats a failed turn start as a retryable outcome by
    catching ``TimeoutError`` among the transient errors. As a plain
    ``Exception`` this escaped that catch, so a contended lock left the stream
    entry pending for the whole reclaim window instead of retrying in process.
    Subclassing also makes the turn path uniform with the reset path, where the
    outer ``asyncio.wait_for`` bound already raises ``TimeoutError``. Note that
    since builtin ``TimeoutError`` subclasses ``OSError``, this exception is now
    also an ``OSError``.
    """


class LockLeaseLost(TimeoutError):
    """The lock token expired or was replaced while its holder was working."""


class LockLease:
    """A renewable token-fenced lease returned by ``ThreadLock.hold``."""

    def __init__(self, lock: ThreadLock, key: str, token: str) -> None:
        self._lock = lock
        self.key = key
        self.token = token
        self._lost = False

    def mark_lost(self) -> None:
        self._lost = True

    async def ensure_owned(self) -> None:
        """Fence an irreversible final write with a compare-token renewal."""

        if self._lost:
            raise LockLeaseLost(self.key)
        try:
            owned = await self._lock._renew(self.key, self.token)
        except Exception as exc:
            self._lost = True
            raise LockLeaseLost(self.key) from exc
        if not owned:
            self._lost = True
            raise LockLeaseLost(self.key)


class ThreadLock:
    """Acquire/release a per-thread lock keyed in Valkey."""

    def __init__(
        self,
        redis: Redis,
        *,
        ttl_ms: int,
        acquire_timeout_s: float,
        poll_interval_s: float,
        owner: str | None = None,
        owner_liveness: OwnerLiveness | None = None,
        dead_owner_proof_s: float = 0.0,
    ) -> None:
        self._redis = redis
        self._ttl_ms = ttl_ms
        self._acquire_timeout_s = acquire_timeout_s
        self._poll_interval_s = poll_interval_s
        self._owner = owner
        self._boot_id = uuid.uuid4().hex
        self._owner_liveness = owner_liveness
        self._dead_owner_proof_s = dead_owner_proof_s
        # Tokens this process currently holds or is trying to acquire. Register
        # before SET NX so a sibling task on the same instance cannot treat a
        # just-won token as a prior-incarnation leftover (#2500 replicas:1).
        self._held_tokens: set[str] = set()

    async def _cas_steal(self, key: str, held: str, token: str) -> bool:
        stolen = await self._redis.eval(_STEAL_LUA, 1, key, held, token, self._ttl_ms)
        return bool(stolen)

    async def _try_steal(self, key: str, token: str, deadline: float) -> bool:
        """CAS-steal a crashed owner's lock, or a same-name leftover we do not hold."""

        held = await self._redis.get(key)
        if not isinstance(held, str):
            return False
        if held in self._held_tokens:
            return False
        parts = _parse_lock_parts(held)
        if parts is None:
            return False
        owner, boot_id = parts
        if self._owner and owner == self._owner:
            if boot_id == self._boot_id:
                return False
            return await self._cas_steal(key, held, token)
        if self._owner_liveness is None:
            return False
        try:
            capable = await self._owner_liveness.is_capable(owner)
            alive = await self._owner_liveness.is_alive(owner)
        except Exception:
            return False
        if not capable or alive:
            return False
        proof = self._dead_owner_proof_s
        if proof > 0:
            remaining = deadline - time.monotonic()
            if remaining <= proof:
                return False
            await asyncio.sleep(proof)
            if time.monotonic() >= deadline:
                return False
            held_again = await self._redis.get(key)
            if held_again != held:
                return False
            try:
                if await self._owner_liveness.is_alive(owner):
                    return False
                if not await self._owner_liveness.is_capable(owner):
                    return False
            except Exception:
                return False
        return await self._cas_steal(key, held, token)

    async def acquire(self, key: str) -> str:
        """Block until the lock is held (returns the owner token) or time out."""
        token = _lock_token(self._owner, self._boot_id)
        self._held_tokens.add(token)
        deadline = time.monotonic() + self._acquire_timeout_s
        started = time.monotonic()
        contended = False
        error: LockAcquireTimeout | None = None
        acquired = False
        outcome = "acquired"
        try:
            with operation_span(
                "curie.thread.lock",
                kind=SpanKind.INTERNAL,
                attributes={"service.name": "curie-worker", "source": "worker"},
            ) as span:
                while True:
                    if await self._redis.set(key, token, nx=True, px=self._ttl_ms):
                        acquired = True
                        outcome = "contended" if contended else "acquired"
                        span.add_event("thread.lock.acquired", {"outcome": outcome})
                        break
                    contended = True
                    if await self._try_steal(key, token, deadline):
                        acquired = True
                        outcome = "contended"
                        span.add_event("thread.lock.stolen", {"outcome": "stolen"})
                        break
                    if time.monotonic() >= deadline:
                        outcome = "timeout"
                        error = LockAcquireTimeout(key)
                        if hasattr(span, "set_status"):
                            span.set_status(StatusCode.ERROR)
                        span.add_event("thread.lock.timeout", {"outcome": "timeout"})
                        break
                    await asyncio.sleep(self._poll_interval_s)
        finally:
            if not acquired:
                self._held_tokens.discard(token)

        record_metric(
            "curie.thread.lock.wait.duration",
            max(0.0, time.monotonic() - started),
            attributes={
                "service.name": "curie-worker",
                "source": "worker",
                "outcome": outcome,
            },
        )
        if error is not None:
            raise error
        assert acquired
        return token

    async def release(self, key: str, token: str) -> None:
        try:
            await self._redis.eval(_RELEASE_LUA, 1, key, token)
        finally:
            self._held_tokens.discard(token)

    async def _renew(self, key: str, token: str) -> bool:
        renewed = await self._redis.eval(_RENEW_LUA, 1, key, token, self._ttl_ms)
        return bool(renewed)

    async def _renew_until_lost(self, lease: LockLease) -> None:
        interval_seconds = max(0.001, self._ttl_ms / 3000.0)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                if not await self._renew(lease.key, lease.token):
                    lease.mark_lost()
                    return
            except Exception:
                lease.mark_lost()
                return

    @asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[LockLease]:
        token = await self.acquire(key)
        lease = LockLease(self, key, token)
        renewer = asyncio.create_task(self._renew_until_lost(lease))
        body_failed = False
        try:
            yield lease
        except BaseException:
            body_failed = True
            raise
        finally:
            renewer.cancel()
            with suppress(asyncio.CancelledError):
                await renewer
            try:
                if not body_failed:
                    await lease.ensure_owned()
            finally:
                await self.release(key, token)
