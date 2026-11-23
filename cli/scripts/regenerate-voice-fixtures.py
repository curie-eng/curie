# /// script
# requires-python = "==3.13.*"
# dependencies = ["av==19.0.2", "espeakng-loader==0.2.4"]
# ///
"""Generate the synthetic factory voice fixtures with pinned local tools."""

import argparse
import ctypes
import json
import os
import sys
import wave
from fractions import Fraction
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import av
from espeakng_loader import get_data_path, get_library_path

FIXTURES = Path(__file__).resolve().parents[2] / "tests/vectors/factory-voice"
SAMPLE_RATE = 22_050
SAMPLE_BYTES = 2
AUDIO_OUTPUT_SYNCHRONOUS = 2
POSITION_CHARACTER = 1
CHARS_UTF8 = 1


def speech() -> tuple[bytes, bytes, bytes]:
    engine = ctypes.CDLL(get_library_path())
    callback_type = ctypes.CFUNCTYPE(
        ctypes.c_int, ctypes.POINTER(ctypes.c_short), ctypes.c_int, ctypes.c_void_p
    )
    engine.espeak_Initialize.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
    engine.espeak_SetSynthCallback.argtypes = [callback_type]
    engine.espeak_SetSynthCallback.restype = None
    engine.espeak_SetVoiceByName.argtypes = [ctypes.c_char_p]
    engine.espeak_ng_SetRandSeed.argtypes = [ctypes.c_long]
    engine.espeak_ng_SetRandSeed.restype = None
    engine.espeak_Synth.argtypes = [
        ctypes.c_char_p,
        ctypes.c_size_t,
        ctypes.c_uint,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.c_uint,
        ctypes.POINTER(ctypes.c_uint),
        ctypes.c_void_p,
    ]
    chunks: list[bytes] = []

    @callback_type
    def collect(samples, count: int, _events) -> int:
        if samples and count:
            chunks.append(ctypes.string_at(samples, count * SAMPLE_BYTES))
        return 0

    def select_voice(voice: bytes) -> None:
        if engine.espeak_SetVoiceByName(voice) != 0:
            raise RuntimeError(f"Could not select eSpeak NG voice {voice.decode()}")

    def synthesize(text: str) -> bytes:
        chunks.clear()
        utf8 = text.encode("utf-8")
        result = engine.espeak_Synth(
            utf8, len(utf8) + 1, 0, POSITION_CHARACTER, 0, CHARS_UTF8, None, None
        )
        if result != 0 or not chunks:
            raise RuntimeError(f"Could not synthesize {text!r}")
        return b"".join(chunks)

    data_parent = os.fsencode(Path(get_data_path()).parent)
    rate = engine.espeak_Initialize(AUDIO_OUTPUT_SYNCHRONOUS, 0, data_parent, 0)
    try:
        if rate != SAMPLE_RATE:
            raise RuntimeError(f"Expected eSpeak NG at {SAMPLE_RATE} Hz, got {rate}")
        engine.espeak_SetSynthCallback(collect)
        select_voice(b"en-us")
        retry_label = synthesize("Change the button label to Retry")
        preserve_timeout = synthesize("Keep the existing timeout. Do not change it")
        select_voice(b"en-us+f2")
        engine.espeak_ng_SetRandSeed(1)
        issue_number = synthesize("Change issue forty two")
    finally:
        engine.espeak_Terminate()
    return retry_label, preserve_timeout, issue_number


def wav(pcm: bytes) -> bytes:
    output = BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(SAMPLE_BYTES)
        audio.setframerate(SAMPLE_RATE)
        audio.writeframes(pcm)
    return output.getvalue()


def transcode(pcm: bytes, container: str, codec: str, rate: int) -> bytes:
    source = av.AudioFrame(format="s16", layout="mono", samples=len(pcm) // SAMPLE_BYTES)
    source.sample_rate = SAMPLE_RATE
    source.time_base = Fraction(1, SAMPLE_RATE)
    source.pts = 0
    source.planes[0].update(pcm)
    output = BytesIO()
    with av.open(output, "w", format=container) as audio:
        audio.flags |= av.container.Flags.bitexact.value
        stream = audio.add_stream(codec, rate=rate)
        stream.codec_context.layout = "mono"
        stream.codec_context.thread_count = 1
        stream.codec_context.flags |= av.codec.context.Flags.bitexact.value
        resampler = av.AudioResampler(
            format=stream.codec_context.format.name, layout="mono", rate=rate
        )
        for frame in resampler.resample(source) + resampler.resample(None):
            audio.mux(stream.encode(frame))
        audio.mux(stream.encode(None))
    return output.getvalue()


def render() -> dict[str, bytes]:
    retry_label, preserve_timeout, issue_number = speech()
    padding = bytes(2 * SAMPLE_RATE * SAMPLE_BYTES)
    fixtures = {
        "retry-label.wav": wav(retry_label),
        "preserve-timeout.wav": wav(preserve_timeout),
        "preserve-timeout-padded.wav": wav(padding + preserve_timeout + padding),
        "silence.wav": wav(bytes(3 * SAMPLE_RATE * SAMPLE_BYTES)),
        "issue-number.wav": wav(issue_number),
    }
    for stem, pcm in (("retry-label", retry_label), ("preserve-timeout", preserve_timeout)):
        fixtures[f"{stem}.mp3"] = transcode(pcm, "mp3", "libmp3lame", SAMPLE_RATE)
        fixtures[f"{stem}.m4a"] = transcode(pcm, "mp4", "aac", SAMPLE_RATE)
        fixtures[f"{stem}.webm"] = transcode(pcm, "webm", "libopus", 48_000)
    return fixtures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check bytes without writing files")
    args = parser.parse_args()
    fixtures = render()
    if args.check:
        changed = [
            name
            for name, content in fixtures.items()
            if not (FIXTURES / name).is_file() or (FIXTURES / name).read_bytes() != content
        ]
        if changed:
            print(f"Missing or different fixtures: {', '.join(changed)}", file=sys.stderr)
            return 1
    else:
        with TemporaryDirectory(prefix=".voice-fixtures-", dir=FIXTURES) as staging:
            for name, content in fixtures.items():
                (Path(staging) / name).write_bytes(content)
            for name in fixtures:
                (Path(staging) / name).replace(FIXTURES / name)
    print(json.dumps({"fixtures": sorted(fixtures), "checked": args.check}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
