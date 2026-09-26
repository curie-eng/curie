#!/usr/bin/env python3
"""GNU coreutils ``timeout``, util-linux ``setsid`` and ``flock``, for hosts without them.

    gnu-process.py timeout [--foreground] DURATION COMMAND [ARG]...
    gnu-process.py setsid COMMAND [ARG]...
    gnu-process.py flock -n FD

A stock Mac has none of these tools, and the host scripts that need them test
GNU's exit statuses, so this reproduces those statuses and the process-group
and lock behaviour they rest on rather than approximating them. It accepts only
the options those scripts use. compose/tests/test_gnu_process.py pins each status
against GNU's own tools. Python 3.9 is the floor, because that is the
``python3`` a stock Mac ships.
"""

from __future__ import annotations

import errno
import fcntl
import os
import re
import resource
import signal
import subprocess
import sys

EXIT_TIMEDOUT = 124
EXIT_CANCELED = 125
EXIT_CANNOT_INVOKE = 126
EXIT_ENOENT = 127
# util-linux flock's: a held lock, then the sysexits.h codes it uses.
EXIT_CONFLICT = 1
EX_USAGE = 64
EX_DATAERR = 65
EX_OSERR = 71
UNITS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
# What GNU timeout passes on to the command it runs.
FORWARDED_SIGNALS = (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM)


def _cannot_run(tool: str, command: str, error: OSError) -> int:
    print(f"{tool}: failed to run command '{command}': {error.strerror}", file=sys.stderr)
    return EXIT_ENOENT if error.errno == errno.ENOENT else EXIT_CANNOT_INVOKE


def _signal_quietly(pid: int, signum: int) -> None:
    try:
        os.kill(pid, signum)
    except OSError:
        pass


def _die_like(signum: int) -> int:
    """Exit by the signal that killed the command, as GNU does, core dump off."""

    try:
        _, hard = resource.getrlimit(resource.RLIMIT_CORE)
        resource.setrlimit(resource.RLIMIT_CORE, (0, hard))
    except (OSError, ValueError):
        return 128 + signum
    signal.signal(signum, signal.SIG_DFL)
    # timeout() keeps the signals it holds blocked once the command has
    # exited, and a blocked one would only be left pending.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signum})
    os.kill(os.getpid(), signum)
    return 128 + signum


def timeout(arguments: list[str]) -> int:
    foreground = arguments[:1] == ["--foreground"]
    if foreground:
        arguments = arguments[1:]
    if len(arguments) < 2 or arguments[0].startswith("-"):
        print("usage: timeout [--foreground] DURATION COMMAND [ARG]...", file=sys.stderr)
        return EXIT_CANCELED
    duration, command = arguments[0], arguments[1:]
    match = re.fullmatch(r"(\d+\.?\d*|\.\d+)([smhd]?)", duration)
    if match is None:
        print(f"timeout: invalid time interval '{duration}'", file=sys.stderr)
        return EXIT_CANCELED
    seconds = float(match.group(1)) * UNITS[match.group(2)]

    if not foreground:
        # Lead a new group, so the bound reaches everything the command starts.
        try:
            os.setpgid(0, 0)
        except OSError:
            pass
    state = {"child": None, "timed_out": False}

    def forward(signum: int, _frame: object) -> None:
        if signum == signal.SIGALRM:
            state["timed_out"] = True
            signum = signal.SIGTERM
        child = state["child"]
        if child is None:
            os._exit(128 + signum)
        _signal_quietly(child.pid, signum)
        if not foreground:
            # Ignored first, or signalling our own group would signal us.
            signal.signal(signum, signal.SIG_IGN)
            _signal_quietly(0, signum)
            if signum != signal.SIGKILL:
                _signal_quietly(child.pid, signal.SIGCONT)
                _signal_quietly(0, signal.SIGCONT)

    for signum in (signal.SIGALRM, *FORWARDED_SIGNALS):
        signal.signal(signum, forward)
    # Handled, so the command's exit ends the wait below. It also clears an
    # inherited SIG_IGN, under which the kernel would reap the command itself.
    signal.signal(signal.SIGCHLD, lambda _signum, _frame: None)
    # CPython runs a Python handler only between bytecodes, so a signal that
    # lands just as a blocking wait starts would be handled only once the wait
    # returned. The C level handler writes a byte here for every signal Python
    # handles, and the wait is a read of it, so any such signal ends the wait.
    # os.pipe() is not inherited, so neither end reaches the command.
    wake_read, wake_write = os.pipe()
    os.set_blocking(wake_write, False)
    signal.set_wakeup_fd(wake_write, warn_on_full_buffer=False)
    # Held across Popen: a command can start before Popen returns, and a
    # signal that arrived then would otherwise be lost with it.
    held = {signal.SIGALRM, signal.SIGCHLD, *FORWARDED_SIGNALS}
    original = signal.pthread_sigmask(signal.SIG_BLOCK, held)
    try:
        # The command starts with the mask this process started with, less
        # SIGCHLD and SIGALRM, which GNU 9.5 unblocks before it saves the mask.
        state["child"] = subprocess.Popen(
            command,
            close_fds=False,
            preexec_fn=lambda: signal.pthread_sigmask(
                signal.SIG_SETMASK, original - {signal.SIGCHLD, signal.SIGALRM}
            ),
        )
    except OSError as error:
        status = _cannot_run("timeout", command[0], error)
        # One that arrived meanwhile exits 128 + sig, as GNU's child does before exec.
        signal.pthread_sigmask(signal.SIG_UNBLOCK, held)
        return status
    # Not stopped when a command in a background group touches the terminal.
    signal.signal(signal.SIGTTIN, signal.SIG_IGN)
    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    if seconds:
        signal.setitimer(signal.ITIMER_REAL, seconds)
    # Polled only while held, so no handler can signal a pid already reaped:
    # handlers run from the unblock through the block that follows it, never
    # after the poll that reaps the command. The signals stay held after the
    # loop, as GNU keeps them after its waitpid.
    while state["child"].poll() is None:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, held)
        os.read(wake_read, 512)
        signal.pthread_sigmask(signal.SIG_BLOCK, held)
    returncode = state["child"].returncode

    if returncode >= 0:
        return EXIT_TIMEDOUT if state["timed_out"] else returncode
    signum = -returncode
    if not state["timed_out"]:
        return _die_like(signum)
    # A command killed outright on expiry keeps that status, so it is visible.
    return 128 + signum if signum == signal.SIGKILL else EXIT_TIMEDOUT


def setsid(arguments: list[str]) -> int:
    if not arguments:
        print("usage: setsid COMMAND [ARG]...", file=sys.stderr)
        return 1
    if os.getpgrp() == os.getpid() and os.fork():
        # A group leader cannot start a session, so the command runs in a
        # child, and the leader returns without waiting for it.
        return 0
    os.setsid()
    # Python ignores these at startup, and an exec would pass that on.
    for name in ("SIGPIPE", "SIGXFSZ"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), signal.SIG_DFL)
    try:
        os.execvp(arguments[0], arguments)
    except OSError as error:
        return _cannot_run("setsid", arguments[0], error)
    return 0


def flock(arguments: list[str]) -> int:
    if len(arguments) != 2 or arguments[0] != "-n":
        print("usage: flock -n FD", file=sys.stderr)
        return EX_USAGE
    if re.fullmatch(r"[0-9]+", arguments[1]) is None:
        print(f"flock: bad file descriptor: '{arguments[1]}'", file=sys.stderr)
        return EX_USAGE
    fd = int(arguments[1])
    try:
        # flock(2), never fcntl's per-process record locks: it locks the open
        # file description the shell shares, so the lock outlives this process.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return EXIT_CONFLICT
    except OSError as error:
        print(f"flock: {fd}: {error.strerror}", file=sys.stderr)
        return EX_OSERR if error.errno in (errno.ENOLCK, errno.ENOMEM) else EX_DATAERR
    return 0


TOOLS = {"timeout": timeout, "setsid": setsid, "flock": flock}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in TOOLS:
        print(f"usage: {sys.argv[0]} {{{','.join(TOOLS)}}} ...", file=sys.stderr)
        sys.exit(EXIT_CANCELED)
    sys.exit(TOOLS[sys.argv[1]](sys.argv[2:]))
