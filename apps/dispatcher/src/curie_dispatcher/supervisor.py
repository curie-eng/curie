"""Reconnect supervision for a long-lived connection, with backoff and shutdown.

Socket Mode connections drop. The builtin Slack client self-heals transient
websocket drops on its own; this supervisor is the outer safety net for the
failures it cannot recover from (the connection factory raising on connect, an
unrecoverable client exit) and the owner of graceful shutdown.

The logic here is deliberately transport-agnostic: it drives a ``Connection``
(anything with ``run`` that blocks until the link is lost and ``close`` that
unblocks it), so it is unit-tested against a fake connection with an injected
sleep, no real socket required. The Socket Mode adapter that satisfies this
protocol lives in ``app.py``.
"""

import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol


class Connection(Protocol):
    """A live connection the supervisor keeps up."""

    def run(self) -> None:
        """Establish and block until the connection is lost or ``close`` is called."""

    def close(self) -> None:
        """Tear the connection down and unblock ``run``."""


@dataclass(frozen=True)
class BackoffPolicy:
    """Exponential backoff between reconnect attempts, capped at a maximum."""

    initial_seconds: float = 1.0
    max_seconds: float = 30.0
    multiplier: float = 2.0

    def delay(self, attempt: int) -> float:
        """Delay before reconnect ``attempt`` (0-based): initial * multiplier**attempt, capped.

        Never raises: a float power overflows after about a thousand attempts,
        which a revoked token reaches within hours, and by then the cap applies.
        """
        try:
            raw = self.initial_seconds * (self.multiplier**attempt)
        except OverflowError:
            return self.max_seconds
        return min(self.max_seconds, raw)


class Supervisor:
    """Keeps a Connection alive across drops until asked to stop.

    ``connect`` is a factory that returns a fresh Connection each attempt (a
    dropped connection is not reused). On any drop the supervisor sleeps for the
    backoff delay, then reconnects; the attempt counter grows monotonically so
    repeated rapid failures back off further, capped by the policy.

    ``label`` names this supervisor in its log lines when several run in one
    process (ADR-0168 decision 2). Without it the lines are unchanged.
    """

    def __init__(
        self,
        connect: Callable[[], Connection],
        *,
        backoff: BackoffPolicy | None = None,
        sleep: Callable[[float], None] | None = None,
        logger: logging.Logger | None = None,
        label: str | None = None,
    ) -> None:
        self._connect = connect
        self._backoff = backoff or BackoffPolicy()
        self._stop = threading.Event()
        # The default sleep waits on the stop event, so a stop request ends a
        # backoff at once instead of after it.
        self._sleep = sleep if sleep is not None else self._stop.wait
        self._logger = logger or logging.getLogger(__name__)
        self._prefix = f"{label}: " if label else ""
        self._current: Connection | None = None
        self._lock = threading.Lock()

    def request_stop(self) -> None:
        """Ask the supervisor to shut down and unblock the current connection.

        Safe to call from another thread. Not from a signal handler: this takes
        the supervisor's lock and the stop event's, which the interrupted
        thread may hold, so a handler defers the call to a thread.
        """
        self._stop.set()
        with self._lock:
            current = self._current
        if current is not None:
            try:
                current.close()
            except Exception:  # close is best-effort on shutdown
                self._logger.exception("%serror closing connection during shutdown", self._prefix)

    def wait_for_stop(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for a stop request; True once one arrives."""
        return self._stop.wait(timeout)

    def run(self) -> None:
        """Run the supervise loop until ``request_stop`` is called. Blocks."""
        attempt = 0
        while not self._stop.is_set():
            try:
                connection = self._connect()
                with self._lock:
                    # A stop that arrived while connecting found nothing to
                    # close; checked under the lock, it cannot be missed.
                    stopped = self._stop.is_set()
                    if not stopped:
                        self._current = connection
                if stopped:
                    connection.close()
                    break
                connection.run()
            except Exception as exc:
                self._logger.warning("%sconnection failed: %s", self._prefix, exc)
            finally:
                with self._lock:
                    self._current = None

            if self._stop.is_set():
                break

            delay = self._backoff.delay(attempt)
            attempt += 1
            self._logger.info("%sreconnecting in %.1fs (attempt %d)", self._prefix, delay, attempt)
            self._sleep(delay)


class SupervisorGroup:
    """Runs one supervisor per member at once, and stops them together.

    Each member keeps its own connection, backoff and attempt counter, so one
    member failing to connect never delays or stops another (ADR-0168 decision
    2: one Slack identity can be down while the rest serve). A group of one
    runs its member on the calling thread, exactly as a lone supervisor ran.
    """

    def __init__(
        self,
        members: Mapping[str, Supervisor],
        *,
        join_interval_s: float = 1.0,
        stop_timeout_s: float = 10.0,
        restart_backoff: BackoffPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not members:
            raise ValueError("a supervisor group needs at least one member")
        self._members = dict(members)
        self._join_interval_s = join_interval_s
        self._stop_timeout_s = stop_timeout_s
        self._restart_backoff = restart_backoff or BackoffPolicy()
        self._logger = logger or logging.getLogger(__name__)

    @property
    def members(self) -> Mapping[str, Supervisor]:
        return dict(self._members)

    def request_stop(self) -> None:
        """Ask every member to stop, closing their connections at once.

        Closing one Socket Mode connection takes most of a second, so members
        are closed concurrently: shutdown then costs one close, not one per
        identity. Waits at most ``stop_timeout_s`` for the closes in total;
        a close still running past it is left to the process exit. Safe from
        another thread; not from a signal handler, which must defer to one.
        """
        members = list(self._members.values())
        if len(members) == 1:
            members[0].request_stop()
            return
        closers = [
            threading.Thread(target=member.request_stop, name=f"stop-{name}", daemon=True)
            for name, member in self._members.items()
        ]
        for closer in closers:
            closer.start()
        deadline = time.monotonic() + self._stop_timeout_s
        for closer in closers:
            closer.join(max(0.0, deadline - time.monotonic()))

    def run(self) -> None:
        """Run every member until each has stopped. Blocks."""
        if len(self._members) == 1:
            next(iter(self._members.values())).run()
            return
        threads = [
            threading.Thread(
                target=self._run_member, args=(name, member), name=f"supervisor-{name}", daemon=True
            )
            for name, member in self._members.items()
        ]
        for thread in threads:
            thread.start()
        # Join in bounded slices so the main thread keeps returning to the
        # interpreter, where the SIGTERM handler that calls request_stop runs.
        for thread in threads:
            while thread.is_alive():
                thread.join(self._join_interval_s)

    def _run_member(self, name: str, member: Supervisor) -> None:
        """Run one member on its own thread, restarting it if it raises.

        A lone supervisor that raises ends the process, and the restart brings
        it back. A member thread that raised would end alone and leave its
        identity down while the others serve, so it is logged and restarted
        after a backoff that a stop request interrupts.
        """
        restarts = 0
        while True:
            try:
                member.run()
                return
            except Exception:
                delay = self._restart_backoff.delay(restarts)
                restarts += 1
                self._logger.exception(
                    "supervisor for %s stopped unexpectedly; restarting in %.1fs", name, delay
                )
            if member.wait_for_stop(delay):
                return
