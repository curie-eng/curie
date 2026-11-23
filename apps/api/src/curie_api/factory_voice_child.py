"""One offline recognition attempt. The parent sends only the attachment on stdin."""

from __future__ import annotations

import importlib
import os
import resource
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from .factory_voice_contract import Refused


def _cap(kind: int, value: int) -> None:
    _, hard = resource.getrlimit(kind)
    cap = value if hard == resource.RLIM_INFINITY else min(hard, value)
    resource.setrlimit(kind, (cap, cap))


def prepare_speech(pcm: bytes) -> bytes | Refused:
    """Normalize quiet edges and retain up to 160 ms on each side of long internal gaps."""
    import numpy as np

    from .factory_voice_contract import Refused
    from .factory_voice_decoder import PCM_RATE

    samples = np.frombuffer(pcm, dtype="<i2")
    if not samples.size:
        return Refused(reason="no_speech")
    hop = PCM_RATE // 50
    audible = (samples >= 16) | (samples <= -16)
    if not audible.any():
        return Refused(reason="no_speech")
    # Align the analysis windows to the recording's audible onset. Otherwise an
    # identical utterance can receive different model input after silence is added.
    first = int(audible.argmax())
    last = samples.size - int(audible[::-1].argmax())
    audible = audible[first:last]
    pcm = pcm[first * 2 : last * 2]
    active = np.flatnonzero(np.logical_or.reduceat(audible, np.arange(0, audible.size, hop)))
    breaks = np.flatnonzero(np.diff(active) > 75) + 1
    parts = []
    for run in np.split(active, breaks):
        start = max(0, (int(run[0]) - 8) * hop)
        end = (int(run[-1]) + 1 + 8) * hop
        parts.append(pcm[start * 2 : end * 2])
    margin = bytes(2 * PCM_RATE * 40 // 1000)
    return (margin + b"".join(parts) + margin).ljust(PCM_RATE * 2 // 10, b"\0")


def _transcribe(model: BinaryIO, pcm: bytes) -> str | Refused:
    import numpy as np

    from .factory_voice_contract import Refused
    from .factory_voice_model import MAX_RESULT_BYTES

    speech = prepare_speech(pcm)
    if isinstance(speech, Refused):
        return speech
    audio = np.frombuffer(speech, dtype="<i2").astype(np.float32) / np.float32(32768)
    # The native file loader accepts the verified descriptor and has no downloader.
    whisper = importlib.import_module("_pywhispercpp")
    prefs = whisper.whisper_context_default_params()
    prefs.use_gpu = False
    ctx = whisper.whisper_init_from_file_with_params(f"/proc/self/fd/{model.fileno()}", prefs)
    try:
        params = whisper.whisper_full_default_params(
            whisper.whisper_sampling_strategy.WHISPER_SAMPLING_GREEDY
        )
        params.n_threads = 1
        params.language = "en"
        params.no_context = True
        params.print_progress = False
        params.print_timestamps = False
        params.print_realtime = False
        params.print_special = False
        params.suppress_blank = True
        params.suppress_nst = True
        params.no_speech_thold = 0.6
        params.logprob_thold = -1.0
        params.temperature = 0.0
        params.temperature_inc = 0.0
        if whisper.whisper_full(ctx, params, audio, audio.size) != 0:
            return Refused(reason="unavailable")
        text = b" ".join(
            whisper.whisper_full_get_segment_text(ctx, index).strip()
            for index in range(whisper.whisper_full_n_segments(ctx))
        ).strip()
        if len(text) > MAX_RESULT_BYTES:
            return Refused(reason="unavailable")
        return text.decode("utf-8") if text else Refused(reason="no_speech")
    finally:
        whisper.whisper_free(ctx)


def _recognize(path: Path, mime: str) -> str | Refused:
    from .factory_voice_contract import Refused
    from .factory_voice_decoder import DECODE_SECONDS, MAX_ENCODED_BYTES, decode_audio
    from .factory_voice_model import open_model, pinned_model

    try:
        model = open_model(path)
    except OSError:
        return Refused(reason="disabled")
    with model:
        stamp = os.fstat(model.fileno())
        if not pinned_model(model):
            return Refused(reason="disabled")
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.alarm(DECODE_SECONDS)
        try:
            pcm = decode_audio(mime, sys.stdin.buffer.read(MAX_ENCODED_BYTES + 1))
        finally:
            signal.alarm(0)
        if isinstance(pcm, Refused):
            return pcm
        result = _transcribe(model, pcm)
        if stamp.st_ctime_ns != os.fstat(model.fileno()).st_ctime_ns:
            return Refused(reason="disabled")
        return result


def main() -> int:
    from .factory_voice_contract import Refused
    from .factory_voice_model import CHILD_ADDRESS_SPACE, CHILD_REFUSALS, CHILD_WALL_SECONDS

    _cap(resource.RLIMIT_CORE, 0)
    _cap(resource.RLIMIT_AS, CHILD_ADDRESS_SPACE)
    _cap(resource.RLIMIT_CPU, CHILD_WALL_SECONDS)
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    with os.fdopen(os.dup(1), "wb") as reply:
        os.dup2(2, 1)
        result = _recognize(Path(sys.argv[1]), sys.argv[2])
        if isinstance(result, Refused):
            return next(code for code, reason in CHILD_REFUSALS.items() if reason == result.reason)
        reply.write(result.encode("utf-8"))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
