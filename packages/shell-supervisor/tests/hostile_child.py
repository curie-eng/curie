"""A runner child that tries to leave state for the next claim on its shell.

Shipped in a claim's bundle by ``test_shell_container.py`` and started by the
supervisor in place of the runner, as the runner's uid. Standard library
only, because it runs in the built image.

``HOSTILE_PHASE=plant`` leaves a regular file, a symlink to ``/`` and a
symlink to a file in every directory it can write, creates a named System V
shared memory segment, semaphore set and message queue and a POSIX message
queue, starts a detached process, and tries to read the supervisor's
environment and the shell's private key. ``HOSTILE_PHASE=probe`` looks for
all of it. Either way ``GET /report`` answers what it found, ``GET /flood``
streams without end, and ``HOSTILE_BAD_STATUS`` makes ``/v1/status`` answer
a JSON array.
"""

from __future__ import annotations

import ctypes
import hmac
import json
import os
import resource
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

TOKEN = os.environ["CURIE_RUNNER_TOKEN"]
PHASE = os.environ["HOSTILE_PHASE"]
MARKER = os.environ["HOSTILE_MARKER"]
IPC_KEY = 0x5C1A1
QUEUE = b"/hostile-queue"
SKIP = ("/proc", "/sys")
SCRATCH = ("/tmp", "/var/tmp", "/dev/shm", "/run/lock", "/dev/mqueue")
LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.shmat.restype = ctypes.c_void_p
LIBC.shmat.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
IPC_CREAT = 0o1000
O_RDONLY, O_RDWR, O_CREAT = 0, 2, 0o100


def _directories() -> list[str]:
    found: list[str] = []
    for parent, dirs, _files in os.walk("/"):
        dirs[:] = [name for name in dirs if os.path.join(parent, name) not in SKIP]
        found.append(parent)
    return found


def _readable(path: str) -> bool:
    try:
        with open(path, "rb") as handle:
            handle.read(1)
    except OSError:
        return False
    return True


def _scratch_modes() -> dict[str, list[int]]:
    modes: dict[str, list[int]] = {}
    for path in SCRATCH:
        info = os.stat(path)
        modes[path] = [info.st_mode & 0o7777, info.st_uid, info.st_gid]
    return modes


def _no_new_privs() -> bool:
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("NoNewPrivs:"):
            return line.split()[1] == "1"
    return False


def plant() -> dict[str, object]:
    written: list[str] = []
    for directory in _directories():
        try:
            Path(directory, f".hostile-{MARKER}").write_text(MARKER)
        except OSError:
            continue
        written.append(directory)
        for name, target in (("dir", "/"), ("file", "/etc/passwd")):
            try:
                Path(directory, f".hostile-{MARKER}-{name}").symlink_to(target)
            except OSError:
                pass
    segment = LIBC.shmget(IPC_KEY, 4096, IPC_CREAT | 0o666)
    if segment >= 0:
        address = LIBC.shmat(segment, None, 0)
        ctypes.memmove(address, MARKER.encode(), len(MARKER))
        LIBC.shmdt(ctypes.c_void_p(address))
    semaphores = LIBC.semget(IPC_KEY, 1, IPC_CREAT | 0o666)
    messages = LIBC.msgget(IPC_KEY, IPC_CREAT | 0o666)
    queue = LIBC.mq_open(QUEUE, O_RDWR | O_CREAT, 0o666, None)
    lingering = subprocess.Popen(["sleep", "600"], start_new_session=True)  # noqa: S607
    return {
        "written": written,
        "ipc_created": [segment >= 0, semaphores >= 0, messages >= 0, queue >= 0],
        "lingering_pid": lingering.pid,
        "supervisor_environ_readable": _readable("/proc/1/environ"),
        "key_readable": _readable(os.environ["HOSTILE_KEY_PATH"]),
        "scratch_modes": _scratch_modes(),
        "core_limit": list(resource.getrlimit(resource.RLIMIT_CORE)),
        "no_new_privs": _no_new_privs(),
    }


def probe() -> dict[str, object]:
    looked_for = f".hostile-{os.environ['HOSTILE_LOOK_FOR']}"
    names = (looked_for, f"{looked_for}-dir", f"{looked_for}-file")
    markers = [
        os.path.join(directory, name)
        for directory in _directories()
        for name in names
        if os.path.lexists(os.path.join(directory, name))
    ]
    ipc: list[str] = []
    for kind in ("shm", "sem", "msg"):
        ipc += Path("/proc/sysvipc", kind).read_text().splitlines()[1:]
    queue = LIBC.mq_open(QUEUE, O_RDONLY, 0, None)
    pid = os.environ.get("HOSTILE_LINGERING_PID", "")
    return {
        "markers": markers,
        "sysv_ipc": ipc,
        "posix_queue_left": queue >= 0,
        "lingering_alive": bool(pid) and os.path.exists(f"/proc/{pid}"),
        "scratch_modes": _scratch_modes(),
    }


REPORT = plant() if PHASE == "plant" else probe()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        return

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 - the http.server interface
        if self.path == "/healthz":
            self._send(200, {"ok": True})
            return
        presented = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if not hmac.compare_digest(presented.encode(), TOKEN.encode()):
            self._send(401, {"error": "invalid token"})
        elif self.path == "/v1/status":
            if os.environ.get("HOSTILE_BAD_STATUS"):
                self._send(200, [])
            else:
                self._send(200, {"turn_active": False, "history_durable": True})
        elif self.path == "/flood":
            self.send_response(200)
            self.end_headers()
            chunk = b"x" * 65536
            while True:
                self.wfile.write(chunk)
        elif self.path == "/report":
            self._send(200, REPORT)
        else:
            self._send(404, {})


ThreadingHTTPServer(("127.0.0.1", int(os.environ["CURIE_RUNNER_PORT"])), Handler).serve_forever()
