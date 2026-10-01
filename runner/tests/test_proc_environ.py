"""Same user reads of process environ stay closed once a process has locked it."""

from __future__ import annotations

import os
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from curie_runner.subprocess_env import BASH_CREDENTIAL_PRELUDE, cli_parent_env

_RUNNER_SENTINEL = "runner-sentinel-proc-environ"
_PARENT_SENTINEL = "sk-ant-proc-parent-sentinel"
_STATE_SENTINEL = "state-token-proc-sentinel"
_COMM_TIMEOUT_SECONDS = 2.0

_BOOT_CHILD = f"""
import os
import time

os.environ["CURIE_RUNNER_TOKEN"] = {_RUNNER_SENTINEL!r}
import curie_runner.__main__ as boot

def _noop(*_args, **_kwargs):
    return None

class _QuietTelemetry:
    def shutdown(self):
        return None

def _serve():
    held = os.environ.get("CURIE_RUNNER_TOKEN") == {_RUNNER_SENTINEL!r}
    print("has_sentinel=yes" if held else "has_sentinel=no", flush=True)
    print("ready", flush=True)
    time.sleep(30)

boot.install_stdout_redaction = _noop
boot.bootstrap_service_telemetry = lambda *_args, **_kwargs: _QuietTelemetry()
boot._serve = _serve
boot.main()
"""


def _kill(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=5)


def _wait_until_comm(pid: int, name: str) -> None:
    deadline = time.monotonic() + _COMM_TIMEOUT_SECONDS
    last = ""
    while True:
        try:
            last = Path(f"/proc/{pid}/comm").read_text(encoding="ascii").strip()
        except FileNotFoundError:
            last = ""
        if last == name:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"process command was {last!r}")
        time.sleep(0.01)


def _expect_environ_unreadable(pid: int) -> None:
    # The command name changes before startup finishes.
    deadline = time.monotonic() + _COMM_TIMEOUT_SECONDS
    while True:
        try:
            with open(f"/proc/{pid}/environ", "rb"):
                pass
        except PermissionError:
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    with pytest.raises(PermissionError):
        with open(f"/proc/{pid}/environ", "rb"):
            pass


def _read_until_ready(proc: subprocess.Popen[bytes]) -> str:
    assert proc.stdout is not None
    deadline = time.monotonic() + 30.0
    chunks = bytearray()
    stream = proc.stdout.fileno()
    while b"ready" not in chunks.splitlines():
        if time.monotonic() >= deadline:
            detail = chunks.decode(errors="replace")
            raise AssertionError(detail or "boot did not become ready")
        remaining = deadline - time.monotonic()
        readable, _, _ = select.select([stream], [], [], min(0.2, remaining))
        if not readable:
            if proc.poll() is not None:
                detail = chunks.decode(errors="replace")
                raise AssertionError(detail or "boot exited before ready")
            continue
        piece = os.read(stream, 4096)
        if not piece:
            detail = chunks.decode(errors="replace")
            raise AssertionError(detail or "boot closed stdout")
        chunks.extend(piece)
    return chunks.decode()


def test_runner_boot_proc_environ_is_unreadable_to_the_same_user() -> None:
    """The runner keeps its sentinel, and a same user open of environ fails."""

    proc = subprocess.Popen(
        [sys.executable, "-c", _BOOT_CHILD],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        output = _read_until_ready(proc)
        assert "has_sentinel=yes" in output.splitlines()
        with pytest.raises(PermissionError):
            with open(f"/proc/{proc.pid}/environ", "rb"):
                pass
    finally:
        _kill(proc)


def test_cli_parent_proc_environ_is_unreadable_to_the_same_user() -> None:
    """A process started from the cli parent env is not readable after exec."""

    env = cli_parent_env({**os.environ, "ANTHROPIC_API_KEY": _PARENT_SENTINEL})
    proc = subprocess.Popen(
        ["/bin/sleep", "30"],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_until_comm(proc.pid, "sleep")
        _expect_environ_unreadable(proc.pid)
    finally:
        _kill(proc)


def test_unfiltered_sleep_environ_is_readable_and_contains_the_sentinel() -> None:
    """Sleep without the cli parent env stays readable and keeps the sentinel."""

    env = os.environ.copy()
    env["ANTHROPIC_API_KEY"] = _PARENT_SENTINEL
    proc = subprocess.Popen(
        ["/bin/sleep", "30"],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_until_comm(proc.pid, "sleep")
        with open(f"/proc/{proc.pid}/environ", "rb") as environ:
            assert _PARENT_SENTINEL.encode() in environ.read()
    finally:
        _kill(proc)


def test_prelude_unsets_token_names_when_proc_environ_is_unreadable(tmp_path: Path) -> None:
    """The prelude clears credential names when proc environ cannot be read."""

    source = tmp_path / "proc_dumpable.c"
    library = tmp_path / "libproc_dumpable.so"
    source.write_text(
        "#include <sys/prctl.h>\n"
        "__attribute__((constructor)) static void lock_dumpable(void) {\n"
        "    prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);\n"
        "}\n",
        encoding="utf-8",
    )
    if shutil.which("gcc") is None:
        raise RuntimeError("gcc is missing")
    compiled = subprocess.run(
        ["gcc", "-shared", "-fPIC", "-O2", "-o", str(library), str(source)],
        check=False,
        capture_output=True,
        text=True,
    )
    if compiled.returncode != 0:
        raise RuntimeError(compiled.stderr.strip() or "gcc failed")
    env = os.environ.copy()
    env.update(
        {
            "BASH_ENV": str(BASH_CREDENTIAL_PRELUDE),
            "LD_PRELOAD": str(library),
            "ANTHROPIC_API_KEY": _PARENT_SENTINEL,
            "CURIE_STATE_TOKEN": _STATE_SENTINEL,
        }
    )
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'printf "key=%s tok=%s\\n" "${ANTHROPIC_API_KEY-}" "${CURIE_STATE_TOKEN-}"',
        ],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout == "key= tok=\n"
