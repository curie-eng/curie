"""Decode a single in-memory voice attachment inside the recognition child."""

from __future__ import annotations

import io
import re
from typing import TYPE_CHECKING, Never

from .factory_voice_contract import Refused

if TYPE_CHECKING:
    from av.audio.frame import AudioFrame

MAX_ENCODED_BYTES = 10 * 1024 * 1024
MAX_SECONDS = 300
PCM_RATE = 16_000
DECODE_SECONDS = 30
_MIME = {
    "audio/wave": "audio/wav",
    "audio/wav": "audio/wav",
    "audio/x-wav": "audio/wav",
    "audio/vnd.wave": "audio/wav",
    "audio/mp3": "audio/mpeg",
    "audio/mpeg": "audio/mpeg",
    "audio/mp4": "audio/mp4",
    "audio/x-m4a": "audio/mp4",
    "video/mp4": "audio/mp4",
    "audio/webm": "audio/webm",
    "video/webm": "audio/webm",
}
_DEMUXERS = {"audio/wav": "wav", "audio/mpeg": "mp3", "audio/mp4": "mov", "audio/webm": "webm"}
_CODECS = {
    "audio/wav": {"pcm_u8", "pcm_s16le", "pcm_s24le", "pcm_s32le", "pcm_f32le", "pcm_f64le"},
    "audio/mpeg": {"mp3"},
    "audio/mp4": {"aac"},
    "audio/webm": {"opus"},
}
_DECLARED_CODEC = {
    "audio/wav": r"(?:1|pcm)",
    "audio/mpeg": "mp3",
    "audio/mp4": r"(?:aac|mp4a\.40\.\d+)",
    "audio/webm": "opus",
}


def canonical_media_type(declared_type: str) -> str | None:
    if len(declared_type) > 128:
        return None
    base, separator, params = declared_type.lower().partition(";")
    mime = _MIME.get(base.strip())
    if not separator or mime is None:
        return mime
    codec = _DECLARED_CODEC[mime]
    return mime if re.fullmatch(rf'\s*codecs\s*=\s*(?:"{codec}"|{codec})\s*', params) else None


def supports_media_type(declared_type: str) -> bool:
    return canonical_media_type(declared_type) is not None


def _vint(data: bytes, offset: int, *, element_id: bool) -> tuple[int, int]:
    if offset >= len(data) or data[offset] == 0:
        raise ValueError
    length = 9 - data[offset].bit_length()
    end = offset + length
    if end > len(data) or length > (4 if element_id else 8):
        raise ValueError
    value = int.from_bytes(data[offset:end], "big")
    if not element_id:
        value ^= 1 << (7 * length)
    return value, end


def _webm_doctype(data: bytes) -> bool:
    # The Matroska and WebM demuxer is shared; only the EBML DocType distinguishes them.
    if not data.startswith(b"\x1a\x45\xdf\xa3"):
        return False
    length, offset = _vint(data, 4, element_id=False)
    if length > 4096 or offset + length > len(data):
        return False
    header = data[offset : offset + length]
    offset = 0
    doc_type = None
    while offset < len(header):
        tag, offset = _vint(header, offset, element_id=True)
        size, offset = _vint(header, offset, element_id=False)
        if offset + size > len(header):
            return False
        if tag == 0x4282:
            if doc_type is not None:
                return False
            doc_type = header[offset : offset + size]
        offset += size
    return doc_type == b"webm"


def _mp4_brand(data: bytes) -> bool:
    if data[4:8] != b"ftyp":
        return False
    size = int.from_bytes(data[:4], "big")
    offset = 8
    if size == 1:
        size = int.from_bytes(data[8:16], "big")
        offset = 16
    if not offset + 8 <= size <= len(data):
        return False
    brand = data[offset : offset + 4]
    return brand in {b"isom", b"mp41", b"mp42", b"M4A ", b"dash"} or bool(
        re.fullmatch(rb"iso[2-9]", brand)
    )


def _deny_secondary_io(*_args: object) -> Never:
    raise PermissionError("external media is unavailable")


def _append_pcm(pcm: bytearray, frames: list[AudioFrame]) -> None:
    for frame in frames:
        nbytes = 2 * frame.samples
        if len(pcm) + nbytes > 2 * PCM_RATE * MAX_SECONDS:
            raise _MediaLimit
        pcm.extend(memoryview(frame.planes[0])[:nbytes])


class _MediaLimit(Exception):
    pass


def _check_layout(rate: int, channels: int) -> None:
    if rate <= 0 or channels <= 0:
        raise ValueError
    if rate > 96_000 or channels > 2:
        raise _MediaLimit


def decode_audio(declared_type: str, data: bytes) -> bytes | Refused:
    import av

    mime = canonical_media_type(declared_type)
    if len(data) > MAX_ENCODED_BYTES:
        return Refused(reason="media_limit")
    if mime is None or not data:
        return Refused(reason="invalid_media")
    try:
        if mime == "audio/webm" and not _webm_doctype(data):
            return Refused(reason="invalid_media")
        if mime == "audio/mp4" and not _mp4_brand(data):
            return Refused(reason="invalid_media")
        codecs = _CODECS[mime]
        decoders = codecs | ({"mp3float"} if mime == "audio/mpeg" else set())
        with av.open(
            io.BytesIO(data),
            mode="r",
            io_open=_deny_secondary_io,
            options={
                "format_whitelist": _DEMUXERS[mime],
                "codec_whitelist": ",".join(sorted(decoders)),
                "protocol_whitelist": "",
                "enable_drefs": "0",
                "use_absolute_path": "0",
                "max_streams": "1",
                "threads": "1",
                "err_detect": "explode",
            },
        ) as container:
            if len(container.streams) != 1 or len(container.streams.audio) != 1:
                return Refused(reason="invalid_media")
            stream = container.streams.audio[0]
            context = stream.codec_context
            if context is None or context.codec.canonical_name not in codecs:
                return Refused(reason="invalid_media")
            rate = context.sample_rate
            _check_layout(rate, context.channels)
            context.thread_count = 1
            resampler = av.AudioResampler(format="s16", layout="mono", rate=PCM_RATE, threads=1)
            pcm = bytearray()
            samples = 0
            for frame in container.decode(stream):
                _check_layout(frame.sample_rate, frame.layout.nb_channels)
                if frame.is_corrupt or frame.sample_rate != rate:
                    return Refused(reason="invalid_media")
                samples += frame.samples
                if samples > MAX_SECONDS * rate:
                    raise _MediaLimit
                _append_pcm(pcm, resampler.resample(frame))
            _append_pcm(pcm, resampler.resample(None))
            return bytes(pcm) if pcm else Refused(reason="invalid_media")
    except _MediaLimit:
        return Refused(reason="media_limit")
    except (av.FFmpegError, OSError, ValueError, OverflowError):
        return Refused(reason="invalid_media")
