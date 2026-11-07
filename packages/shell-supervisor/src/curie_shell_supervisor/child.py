"""The runner child: spawn it in its own process group, and stop all of it.

The child is the unchanged runner. It is spawned as a session leader, so its
process group holds every descendant that did not leave it deliberately, with
the parent-death signal armed so it cannot outlive a supervisor that dies
without cleaning up (outside a container, where the supervisor is not PID 1).
A root supervisor runs the child as another uid; then a process of that uid in
any group is the child's, and stopping sweeps those too.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import logging
import os
import signal
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_PR_SET_PDEATHSIG = 1
_PR_SET_CHILD_SUBREAPER = 36
# Resolved before any fork: the preexec hook below runs between fork and exec
# and must not import or load anything.
_LIBC = ctypes.CDLL(None, use_errno=True)
_SWEEP_SECONDS = 5.0


def become_subreaper() -> None:
    """Adopt orphaned descendants so stopping can reap them (Linux)."""

    _LIBC.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)


def _arm_parent_death(parent: int) -> Callable[[], None]:
    def arm() -> None:
        # After any uid change (subprocess applies user/group first), because a
        # credential change clears the parent-death signal.
        if _LIBC.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
            os._exit(127)
        if os.getppid() != parent:
            os._exit(127)

    return arm


@dataclass(frozen=True)
class _Proc:
    pid: int
    pgrp: int
    uid: int
    zombie: bool


def _processes() -> list[_Proc]:
    found: list[_Proc] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            status = (entry / "status").read_text()
        except OSError:
            continue
        # The comm field is parenthesized and may hold spaces or parentheses.
        fields = stat[stat.rfind(")") + 2 :].split()
        uid = next(
            (int(line.split()[1]) for line in status.splitlines() if line.startswith("Uid:")),
            -1,
        )
        found.append(_Proc(int(entry.name), int(fields[2]), uid, fields[0] in {"Z", "X"}))
    return found


def reap_orphans(exclude: int | None = None) -> None:
    """Collect exited adopted descendants without stealing ``exclude``'s status."""

    while True:
        try:
            info = os.waitid(os.P_ALL, 0, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except ChildProcessError:
            return
        if info is None or info.si_pid == 0 or info.si_pid == exclude:
            return
        with contextlib.suppress(ChildProcessError):
            os.waitpid(info.si_pid, 0)


class RunnerChild:
    """One spawned runner process and the group it leads."""

    def __init__(
        self, process: asyncio.subprocess.Process, *, uid: int | None, sweep_uid: bool
    ) -> None:
        self._process = process
        self.pid = process.pid
        self._uid = uid
        self._sweep_uid = sweep_uid

    @classmethod
    async def spawn(
        cls,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        cwd: Path,
        uid: int | None,
        gid: int | None,
    ) -> RunnerChild:
        extra: dict[str, object] = {}
        if uid is not None:
            extra = {"user": uid, "group": gid, "extra_groups": []}
        process = await asyncio.create_subprocess_exec(
            *argv,
            env=dict(env),
            cwd=str(cwd),
            stdin=asyncio.subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            preexec_fn=_arm_parent_death(os.getpid()),
            **extra,  # type: ignore[arg-type]
        )
        return cls(process, uid=uid, sweep_uid=uid is not None and uid != os.geteuid())

    @property
    def returncode(self) -> int | None:
        return self._process.returncode

    async def wait(self) -> int:
        return await self._process.wait()

    def _members(self) -> list[_Proc]:
        return [
            proc
            for proc in _processes()
            if proc.pid != os.getpid()
            and (proc.pgrp == self.pid or (self._sweep_uid and proc.uid == self._uid))
        ]

    def _signal_group(self, signum: int) -> None:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self.pid, signum)

    async def stop(self, grace_s: float) -> bool:
        """Stop the child and every descendant; True once none is left alive."""

        if self._process.returncode is None:
            self._signal_group(signal.SIGTERM)
            try:
                await asyncio.wait_for(self._process.wait(), timeout=grace_s)
            except TimeoutError:
                pass
        self._signal_group(signal.SIGKILL)
        await self._process.wait()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _SWEEP_SECONDS
        while True:
            reap_orphans()
            alive = [proc for proc in self._members() if not proc.zombie]
            if not alive:
                return True
            if loop.time() >= deadline:
                logger.error("%d runner descendants survived SIGKILL", len(alive))
                return False
            for proc in alive:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(proc.pid, signal.SIGKILL)
            await asyncio.sleep(0.05)
