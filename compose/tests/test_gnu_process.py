"""GNU ``timeout`` and util-linux ``setsid`` on a host that ships neither.

A stock Mac has no ``timeout`` and no ``setsid`` (measured 2026-09-24 on
Darwin 25.6: ``timeout``, ``gtimeout`` and ``setsid`` are all absent from
PATH), so the host scripts run both through ``cli/scripts/gnu-process.py``.
Their callers test GNU's statuses: 124 once the bound fires, the command's own
status otherwise, and a new session led by the ``$!`` the script recorded.

Every expectation here was measured against GNU coreutils 9.1 ``timeout`` and
util-linux 2.38.1 ``setsid`` on Debian 12, in the ``curie-runner`` image. Each
case also runs against GNU's own tool wherever it is on PATH, as on Linux CI,
so a divergence from GNU fails there even though a Mac cannot see it.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "cli" / "scripts" / "gnu-process.py"
GNU_VERSION_MARKERS = {"timeout": "GNU coreutils", "setsid": "util-linux"}
# Exists and is not executable on both macOS and Linux.
NOT_EXECUTABLE = "/etc/passwd"
NOT_FOUND = "/nonexistent/acme-command"
# 141 when SIGPIPE is at its default, 1 when an inherited SIG_IGN leaves `yes`
# to fail on EPIPE instead. A pipeline under either tool must see the default.
SIGPIPE_PROBE = ["bash", "-c", 'yes | head -n 1 >/dev/null; exit "${PIPESTATUS[0]}"']


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
    if stat.exists():
        return stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"
    return True


def _gone_within(pid: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


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
