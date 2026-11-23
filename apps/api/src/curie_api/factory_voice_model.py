"""Pinned local model availability and the API's one-child recognition budget."""

from __future__ import annotations

import asyncio
import hashlib
import os
import signal
import stat
import sys
import threading
from contextlib import suppress
from functools import lru_cache
from pathlib import Path
from typing import BinaryIO, Final

from .factory_voice_contract import Refusal, Refused
from .factory_voice_decoder import MAX_ENCODED_BYTES, canonical_media_type

MODEL_SHA256 = "a03779c86df3323075f5e796cb2ce5029f00ec8869eee3fdfb897afe36c6d002"
MODEL_BYTES = 147_964_211
CHILD_WALL_SECONDS = 180
CHILD_ADDRESS_SPACE = 2 * 1024**3
CHILD_MEDIA_LIMIT_EXIT = 12
_CHILD_SLOT = threading.Lock()
_MODEL_CHECK = threading.Lock()
_THREAD_LIMITS = {
    name: "1"
    for name in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "BLIS_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "OMP_THREAD_LIMIT",
    )
}
CHILD_REFUSALS: Final[dict[int, Refusal]] = {
    10: "disabled",
    11: "invalid_media",
    CHILD_MEDIA_LIMIT_EXIT: "media_limit",
    13: "no_speech",
    14: "unavailable",
}
MAX_RESULT_BYTES = 64 * 1024


def open_model(path: Path) -> BinaryIO:
    return open(path, "rb", opener=_open_nonblocking)


def _open_nonblocking(path: str, flags: int) -> int:
    return os.open(path, flags | os.O_NONBLOCK)


def _identity(st: os.stat_result) -> tuple[int, ...]:
    return st.st_dev, st.st_ino, st.st_size, st.st_ctime_ns, st.st_mtime_ns


def pinned_model(file: BinaryIO) -> bool:
    st = os.fstat(file.fileno())
    if not stat.S_ISREG(st.st_mode) or st.st_size != MODEL_BYTES:
        return False
    remaining = MODEL_BYTES
    digest = hashlib.sha256()
    while remaining:
        chunk = file.read(min(1024 * 1024, remaining))
        if not chunk:
            return False
        digest.update(chunk)
        remaining -= len(chunk)
    unchanged = _identity(st) == _identity(os.fstat(file.fileno()))
    return unchanged and digest.hexdigest() == MODEL_SHA256


@lru_cache(maxsize=4)
def _checked_model(path: Path, identity: tuple[int, ...]) -> bool:
    with open_model(path) as file:
        return _identity(os.fstat(file.fileno())) == identity and pinned_model(file)


def _available(path: Path) -> bool:
    try:
        with _MODEL_CHECK:
            return _checked_model(path, _identity(path.stat()))
    except OSError:
        return False


async def _reap_and_release(launch: asyncio.Task[asyncio.subprocess.Process]) -> None:
    try:
        proc = await launch
        if proc.returncode is None:
            with suppress(ProcessLookupError):
                proc.kill()
            await proc.communicate()
    finally:
        _CHILD_SLOT.release()


class LocalVoiceRecognizer:
    def __init__(self, model_path: Path) -> None:
        self.model_path = model_path

    async def available(self) -> bool:
        return await asyncio.to_thread(_available, self.model_path)

    async def recognize(self, declared_type: str, data: bytes) -> str | Refused:
        mime = canonical_media_type(declared_type)
        if len(data) > MAX_ENCODED_BYTES:
            return Refused(reason="media_limit")
        if mime is None or not data:
            return Refused(reason="invalid_media")
        if not _CHILD_SLOT.acquire(blocking=False):
            return Refused(reason="unavailable")
        try:
            async with asyncio.timeout(CHILD_WALL_SECONDS):
                return await self._run(mime, data)
        except TimeoutError:
            return Refused(reason="media_limit")
        except OSError:
            return Refused(reason="unavailable")

    async def _run(self, mime: str, data: bytes) -> str | Refused:
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "curie_api.factory_voice_child",
                str(self.model_path),
                mime,
                str(CHILD_ADDRESS_SPACE),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env={"PYTHONPATH": os.environ.get("PYTHONPATH", ""), **_THREAD_LIMITS},
            )
        )
        try:
            proc = await asyncio.shield(launch)
            result, _ = await proc.communicate(data)
        finally:
            # Cancellation can interrupt spawn or recur during shutdown. Only the reaper
            # releases the shared slot, after the child has exited.
            await asyncio.shield(_reap_and_release(launch))
        # Native allocation failure can abort or fault before the child can return a refusal.
        if proc.returncode in (
            -signal.SIGALRM,
            -signal.SIGXCPU,
            -signal.SIGKILL,
            -signal.SIGABRT,
            -signal.SIGSEGV,
        ):
            return Refused(reason="media_limit")
        if proc.returncode != 0:
            return Refused(reason=CHILD_REFUSALS.get(proc.returncode or -1, "unavailable"))
        if not 0 < len(result) <= MAX_RESULT_BYTES:
            return Refused(reason="unavailable")
        try:
            return result.decode("utf-8")
        except UnicodeDecodeError:
            return Refused(reason="unavailable")
