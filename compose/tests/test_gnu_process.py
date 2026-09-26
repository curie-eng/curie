"""GNU ``timeout`` and util-linux ``setsid`` and ``flock`` on a host that ships none.

A stock Mac has no ``timeout``, no ``setsid`` and no ``flock`` (measured
2026-09-24 on Darwin 25.6: ``timeout``, ``gtimeout``, ``setsid`` and ``flock``
are all absent from PATH), so the host scripts run them through
``cli/scripts/gnu-process.py``. Their callers test GNU's statuses: 124 once the
bound fires, the command's own status otherwise, a new session led by the
``$!`` the script recorded, and a lock taken or refused on the descriptor the
script opened.

Every expectation here was measured against GNU coreutils 9.1 ``timeout`` and
util-linux 2.38.1 ``setsid`` and ``flock`` on Debian 12, in the ``curie-runner``
image. Each case also runs against GNU's own tool wherever it is on PATH, as on
Linux CI, so a divergence from GNU fails there even though a Mac cannot see it.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "cli" / "scripts" / "gnu-process.py"
GNU_VERSION_MARKERS = {
    "timeout": "GNU coreutils",
    "setsid": "util-linux",
    "flock": "util-linux",
}
# Exists and is not executable on both macOS and Linux.
NOT_EXECUTABLE = "/etc/passwd"
NOT_FOUND = "/nonexistent/acme-command"
# 141 when SIGPIPE is at its default, 1 when an inherited SIG_IGN leaves `yes`
# to fail on EPIPE instead. A pipeline under either tool must see the default.
SIGPIPE_PROBE = ["bash", "-c", 'yes | head -n 1 >/dev/null; exit "${PIPESTATUS[0]}"']
# The lock as the drill scripts take it, on a descriptor the shell opened.
TAKE_THE_LOCK = 'exec 9>"$1"; shift; "$@" -n 9'


def _is_gnu(tool: str) -> bool:
    path = shutil.which(tool)
    if path is None:
        return False
    version = subprocess.run(
        [path, "--version"], capture_output=True, text=True, check=False
    ).stdout
    return GNU_VERSION_MARKERS[tool] in version


def _implementations(tool: str) -> list[object]:
    return [
        pytest.param([str(HELPER), tool], id="gnu-process"),
        pytest.param(
            [tool],
            marks=pytest.mark.skipif(not _is_gnu(tool), reason=f"no GNU {tool} on PATH"),
            id=f"gnu-{tool}",
        ),
    ]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # Linux answers `kill -0` for a zombie until something reaps it.
    stat = Path(f"/proc/{pid}/stat")
    try:
        if stat.exists():
            return stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        # The child exited between kill and the procfs read.
        return False
    return True


def test_proc_stat_disappearing_after_kill_probe_means_process_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def disappeared(_path: Path) -> bool:
        raise ProcessLookupError(3, "process exited")

    with monkeypatch.context() as patch:
        patch.setattr(os, "kill", lambda _pid, _signal: None)
        patch.setattr(Path, "exists", disappeared)
        assert not _alive(12345)


def _gone_within(pid: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


def _asleep(pid: int) -> bool:
    """Whether the process is in an interruptible sleep, as in a wait."""

    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        return stat.read_text().rsplit(")", 1)[1].split()[0] == "S"
    state = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    # macOS reports a sleep of more than about 20 seconds as idle.
    return state[:1] in ("S", "I")


def _wait_until_asleep(pid: int) -> None:
    deadline = time.monotonic() + 10
    while not _asleep(pid):
        assert time.monotonic() < deadline, "the tool never started waiting"
        time.sleep(0.01)


def _kill_quietly(pid: int) -> None:
    try:
        os.kill(pid, 9)
    except ProcessLookupError:
        pass


def _command_group(implementation: list[str], options: list[str]) -> tuple[int, int]:
    """The tool's pid, and the process group of the command it runs."""

    probe = subprocess.Popen(
        [
            *implementation,
            *options,
            "5",
            sys.executable,
            "-c",
            "import os; print(os.getpgrp())",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    stdout, _ = probe.communicate(timeout=30)
    return probe.pid, int(stdout)


def _require_a_group_of_its_own(implementation: list[str]) -> None:
    """Fail before any bound fires if firing it would signal pytest.

    Without --foreground the tool signals its whole process group on expiry,
    and a tool that never left pytest's group kills the test run itself.
    """

    pid, group = _command_group(implementation, [])
    assert group == pid, "the tool did not lead a process group of its own"


@pytest.mark.parametrize("implementation", _implementations("timeout"))
@pytest.mark.parametrize(
    ("options", "leads_its_own_group"),
    [([], True), (["--foreground"], False)],
    ids=["own-group", "foreground-stays-in-the-callers-group"],
)
def test_timeout_runs_the_command_in_a_group_it_leads_unless_foreground(
    implementation: list[str], options: list[str], leads_its_own_group: bool
) -> None:
    pid, group = _command_group(implementation, options)
    assert group == (pid if leads_its_own_group else os.getpgrp())


@pytest.mark.parametrize("implementation", _implementations("timeout"))
@pytest.mark.parametrize(
    ("arguments", "status"),
    [
        pytest.param(["5", "sh", "-c", "exit 3"], 3, id="the-commands-own-status"),
        pytest.param(["0.3", "sleep", "5"], 124, id="expired"),
        pytest.param(["0.3s", "sleep", "5"], 124, id="expired-with-a-unit"),
        pytest.param(
            ["--foreground", "0.3", "sleep", "5"], 124, id="expired-in-the-foreground"
        ),
        pytest.param(
            ["0.3", "sh", "-c", "trap 'exit 7' TERM; sleep 5 & wait"],
            124,
            id="expired-though-the-command-exits-on-term",
        ),
        pytest.param(["0", "sh", "-c", "sleep 0.3; exit 4"], 4, id="zero-never-expires"),
        pytest.param(["5", "sh", "-c", "kill -TERM $$"], -15, id="killed-by-a-signal"),
        pytest.param(["5", NOT_FOUND], 127, id="not-found"),
        pytest.param(["5", NOT_EXECUTABLE], 126, id="not-executable"),
        pytest.param(["bogus", "true"], 125, id="bad-duration"),
        pytest.param(["5", *SIGPIPE_PROBE], 141, id="sigpipe-at-its-default"),
    ],
)
def test_timeout_exits_with_gnus_status(
    implementation: list[str], arguments: list[str], status: int
) -> None:
    _require_a_group_of_its_own(implementation)
    started = time.monotonic()
    result = subprocess.run(
        [*implementation, *arguments],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == status, result.stderr
    # An expired bound must not have waited out the command it bounds.
    assert time.monotonic() - started < 4, result.stderr


@pytest.mark.parametrize("implementation", _implementations("timeout"))
@pytest.mark.parametrize(
    ("options", "grandchild_survives"),
    [([], False), (["--foreground"], True)],
    ids=["kills-the-commands-group", "foreground-spares-the-grandchild"],
)
def test_timeout_on_expiry_reaches_the_command_group_unless_foreground(
    implementation: list[str], options: list[str], grandchild_survives: bool
) -> None:
    _require_a_group_of_its_own(implementation)
    result = subprocess.run(
        [
            *implementation,
            *options,
            "0.5",
            "sh",
            "-c",
            "sleep 30 >/dev/null 2>&1 & echo $!; wait",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 124, result.stderr
    grandchild = int(result.stdout)
    try:
        if grandchild_survives:
            time.sleep(0.5)
            assert _alive(grandchild)
        else:
            assert _gone_within(grandchild, 3)
    finally:
        _kill_quietly(grandchild)


@pytest.mark.parametrize("implementation", _implementations("timeout"))
@pytest.mark.parametrize(
    ("command", "shell_status"),
    [
        (["sh", "-c", "echo ready; exec sleep 30"], 128 + 15),
        (["sh", "-c", "trap 'exit 7' TERM; echo ready; sleep 30 & wait"], 7),
    ],
    ids=["the-command-dies-of-it", "the-command-traps-it"],
)
def test_timeout_passes_a_term_it_receives_to_the_command(
    implementation: list[str], command: list[str], shell_status: int
) -> None:
    """Every caller is a shell, so this is the status a shell reads.

    When the command dies of the TERM passed to it, GNU coreutils 9.1 and 9.7
    in Debian images re-raise it (a returncode of -15). GNU timeout on a loaded
    ubuntu-24.04 Actions runner once exited 143 instead, without a warning (CI
    run 36041307028), for a reason not established: timeout.c is the same
    there in 9.1 and 9.4. A shell reads 143 from both.
    """

    _require_a_group_of_its_own(implementation)
    process = subprocess.Popen(
        [*implementation, "30", *command], stdout=subprocess.PIPE, text=True
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == "ready\n"
        process.terminate()
        returncode = process.wait(timeout=10)
        assert (128 - returncode if returncode < 0 else returncode) == shell_status
    finally:
        process.kill()
        process.wait()


# gnu-process.py with its Popen slowed down, so a signal is sure to arrive after
# the command has started and before the helper has recorded it.
SLOW_START = """
import importlib.util, subprocess, sys, time
spec = importlib.util.spec_from_file_location("gnu_process", sys.argv[1])
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
class SlowPopen(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        time.sleep(1)
helper.subprocess.Popen = SlowPopen
sys.exit(helper.timeout(sys.argv[2:]))
"""


def _group_gone_within(group: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while True:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return True
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)


def test_timeout_passes_on_a_term_that_arrives_while_the_command_starts() -> None:
    """A TERM that arrives before the child is recorded must still reach it.

    Otherwise the helper exits 143 and leaves the command running. On a
    loaded CI runner a command can print before Popen has even returned.
    """

    _require_a_group_of_its_own([str(HELPER), "timeout"])
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            SLOW_START,
            str(HELPER),
            "30",
            "sh",
            "-c",
            "echo ready; exec sleep 30",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == "ready\n"
        process.terminate()
        assert process.wait(timeout=10) == -15, "the TERM never reached the command"
        assert _group_gone_within(process.pid, 3), "the command outlived the helper"
    finally:
        process.kill()
        process.wait()
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, 9)


# gnu-process.py with every Python handler it installs set to restart the call
# it interrupts, so a handler that fires during the wait runs only once the
# wait returns. That is where a signal already stands when it lands just before
# the wait starts: CPython runs a Python handler only between bytecodes.
RESTARTING_HANDLERS = """
import importlib.util, signal, sys
spec = importlib.util.spec_from_file_location("gnu_process", sys.argv[1])
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
install = signal.signal
def restarting(signum, handler):
    previous = install(signum, handler)
    if callable(handler):
        signal.siginterrupt(signum, False)
    return previous
signal.signal = restarting
sys.exit(helper.timeout(sys.argv[2:]))
"""


def test_timeout_passes_on_a_term_that_lands_as_it_starts_waiting() -> None:
    """A TERM that lands as the helper starts its wait must not wait with it.

    Otherwise the command runs on until it exits of its own accord. On a
    loaded CI runner a TERM can land between the helper's last bytecode and
    the wait it then blocks in.
    """

    _require_a_group_of_its_own([str(HELPER), "timeout"])
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            RESTARTING_HANDLERS,
            str(HELPER),
            "30",
            "sh",
            "-c",
            "echo ready; exec sleep 30",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == "ready\n"
        _wait_until_asleep(process.pid)
        process.terminate()
        assert process.wait(timeout=10) == -15, "the TERM never reached the command"
        assert _group_gone_within(process.pid, 3), "the command outlived the helper"
    finally:
        process.kill()
        process.wait()
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, 9)


@pytest.mark.parametrize("implementation", _implementations("timeout"))
def test_timeout_passes_its_stdin_and_descriptors_through(
    implementation: list[str], tmp_path: Path
) -> None:
    extra = tmp_path / "extra"
    with extra.open("w") as handle:
        result = subprocess.run(
            [
                *implementation,
                "5",
                "bash",
                "-c",
                'cat; echo through >&"$1"',
                "bash",
                str(handle.fileno()),
            ],
            input="piped\n",
            pass_fds=(handle.fileno(),),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "piped\n"
    assert extra.read_text() == "through\n"


@pytest.mark.parametrize("implementation", _implementations("setsid"))
def test_setsid_leads_a_new_session_under_the_pid_the_shell_recorded(
    implementation: list[str],
) -> None:
    """A script signals the group `$!` names, so `$!` must lead it."""

    probe = "import os; print(os.getpid(), os.getpgrp(), os.getsid(0))"
    result = subprocess.run(
        [
            "bash",
            "-c",
            '"$@" & echo "$!"; wait "$!"',
            "bash",
            *implementation,
            sys.executable,
            "-c",
            probe,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    recorded, observed = result.stdout.splitlines()
    pid, group, session = observed.split()
    assert pid == group == session == recorded, result.stdout


@pytest.mark.parametrize("implementation", _implementations("setsid"))
@pytest.mark.parametrize(
    ("command", "status"),
    [
        pytest.param(["sh", "-c", "exit 5"], 5, id="the-commands-own-status"),
        pytest.param(["sh", "-c", "kill -TERM $$"], -15, id="killed-by-a-signal"),
        pytest.param([NOT_FOUND], 127, id="not-found"),
        pytest.param([NOT_EXECUTABLE], 126, id="not-executable"),
        pytest.param(SIGPIPE_PROBE, 141, id="sigpipe-at-its-default"),
    ],
)
def test_setsid_exits_with_util_linuxs_status(
    implementation: list[str], command: list[str], status: int
) -> None:
    result = subprocess.run(
        [*implementation, *command],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == status, result.stderr


@pytest.mark.parametrize("implementation", _implementations("setsid"))
def test_setsid_forks_when_it_already_leads_a_group(
    implementation: list[str], tmp_path: Path
) -> None:
    """A group leader cannot start a session, so util-linux forks and returns."""

    record = tmp_path / "record"
    probe = (
        "import os, sys, time; time.sleep(0.3); "
        "open(sys.argv[1], 'w').write(f'{os.getpid()} {os.getsid(0)}')"
    )
    process = subprocess.Popen(
        [*implementation, sys.executable, "-c", probe, str(record)],
        start_new_session=True,
    )
    assert process.wait(timeout=10) == 0
    assert not record.exists(), "setsid waited for the command it forked"
    deadline = time.monotonic() + 5
    while not record.exists() or not record.read_text():
        assert time.monotonic() < deadline, "the forked command never ran"
        time.sleep(0.05)
    pid, session = record.read_text().split()
    assert pid == session
    assert int(pid) != process.pid


@contextlib.contextmanager
def _held_elsewhere(lock: Path, operation: int | None) -> Iterator[None]:
    """The lock, held on an open file description of the test's own."""

    if operation is None:
        yield
        return
    with lock.open("a") as handle:
        fcntl.flock(handle, operation | fcntl.LOCK_NB)
        yield


def _free(lock: Path) -> bool:
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
    return True


@pytest.mark.parametrize("implementation", _implementations("flock"))
@pytest.mark.parametrize(
    ("script", "held_elsewhere", "status", "stderr"),
    [
        pytest.param(TAKE_THE_LOCK, None, 0, "", id="takes-a-free-lock"),
        pytest.param(
            TAKE_THE_LOCK, fcntl.LOCK_EX, 1, "", id="refuses-a-lock-held-elsewhere"
        ),
        # Exclusive, so two drills can never both hold it.
        pytest.param(
            TAKE_THE_LOCK, fcntl.LOCK_SH, 1, "", id="refuses-a-lock-shared-elsewhere"
        ),
        pytest.param(
            f'{TAKE_THE_LOCK} && "$@" -n 9', None, 0, "", id="retakes-its-own-lock"
        ),
        pytest.param(
            'shift; "$@" -n 9',
            None,
            65,
            "flock: 9: Bad file descriptor\n",
            id="descriptor-not-open",
        ),
        pytest.param('shift; "$@" -n', None, 64, None, id="no-descriptor"),
        pytest.param('shift; "$@" -n nine', None, 64, None, id="not-a-descriptor"),
        pytest.param('shift; "$@"', None, 64, None, id="no-arguments"),
    ],
)
def test_flock_exits_with_util_linuxs_status(
    implementation: list[str],
    script: str,
    held_elsewhere: int | None,
    status: int,
    stderr: str | None,
    tmp_path: Path,
) -> None:
    """The scripts test only `if ! flock -n 9`, and print their own refusal."""

    lock = tmp_path / "lock"
    with _held_elsewhere(lock, held_elsewhere):
        result = subprocess.run(
            ["bash", "-c", script, "bash", str(lock), *implementation],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    assert result.returncode == status, result.stderr
    if stderr is not None:
        assert result.stderr == stderr


@pytest.mark.parametrize("implementation", _implementations("flock"))
def test_flock_leaves_the_lock_with_the_shell_until_it_closes_the_descriptor(
    implementation: list[str], tmp_path: Path
) -> None:
    """A drill holds its lock for its whole run, long after the tool has exited."""

    lock = tmp_path / "lock"
    script = (
        f"{TAKE_THE_LOCK} || exit; echo taken; read -r _; "
        "exec 9>&-; echo closed; read -r _"
    )
    process = subprocess.Popen(
        ["bash", "-c", script, "bash", str(lock), *implementation],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stdout.readline() == "taken\n"
        assert not _free(lock), "the lock went with the tool"
        process.stdin.write("\n")
        process.stdin.flush()
        assert process.stdout.readline() == "closed\n"
        assert _free(lock), "closing the descriptor left the lock held"
        process.stdin.write("\n")
        process.stdin.close()
        assert process.wait(timeout=10) == 0
    finally:
        process.kill()
        process.wait()
