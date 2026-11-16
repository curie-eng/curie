"""The startup window in which a dependency that is not ready yet is expected (#3079).

On a fresh install postgres, Valkey and the object store come up alongside the
worker, and the object-store bucket is created by a post-install Job a little
later. Inside this window a failure caused by such a dependency is logged as a
one-line warning rather than a traceback. The supervisor and the boot sweeps in
``run.py`` use it, and so does the consumer's maintenance tick, whose
``reap_orphans`` step lists the object store before the bucket exists.

It lives in its own module because ``run.py`` imports ``consumer.py``, so the
consumer cannot import it from there.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Callable
from dataclasses import dataclass

# How long after boot a dependency that is not reachable yet is expected rather
# than a fault (#3079). On a fresh install postgres, Valkey and the object store
# come up alongside the worker; for this window a supervised task that fails on
# them logs a one-line warning and retries instead of a full traceback.
BOOT_GRACE_S = 120.0

# Object-store error codes a fresh install returns until its bucket is created.
_NOT_READY_S3_CODES = frozenset({"NoSuchBucket"})


def is_dependency_not_ready(exc: BaseException) -> bool:
    """True when ``exc`` (or anything in its cause chain) means a dependency is
    not reachable or not provisioned yet, rather than a bug in the task."""

    import redis.exceptions
    import sqlalchemy.exc

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        # Network failures only: a PermissionError or FileNotFoundError is a
        # local fault, not a dependency warming up.
        if isinstance(
            current,
            socket.gaierror
            | ConnectionError
            | TimeoutError
            | redis.exceptions.ConnectionError
            | redis.exceptions.TimeoutError
            | sqlalchemy.exc.OperationalError
            | sqlalchemy.exc.InterfaceError,
        ):
            return True
        # botocore's ClientError, matched by shape so boto3 stays a lazy import.
        response = getattr(current, "response", None)
        if isinstance(response, dict):
            code = response.get("Error", {}).get("Code")
            if code in _NOT_READY_S3_CODES:
                return True
        current = current.__cause__ or current.__context__
    return False


@dataclass(frozen=True)
class BootGrace:
    """The #3079 startup window, anchored once when the worker boots."""

    booted: float
    window_s: float = BOOT_GRACE_S
    clock: Callable[[], float] = time.monotonic

    @classmethod
    def starting_now(cls, clock: Callable[[], float] = time.monotonic) -> BootGrace:
        """A window anchored at ``clock()`` now."""

        return cls(booted=clock(), clock=clock)

    def excuses(self, exc: BaseException) -> bool:
        """True while inside the window and ``exc`` is a dependency not ready yet."""

        return self.clock() - self.booted < self.window_s and is_dependency_not_ready(exc)
