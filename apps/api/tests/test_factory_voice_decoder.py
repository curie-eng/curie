import array
import io
import wave
from pathlib import Path

import av
import pytest
from curie_api.factory_voice_contract import Refused
from curie_api.factory_voice_decoder import MAX_ENCODED_BYTES, decode_audio

VECTORS = Path(__file__).resolve().parents[3] / "tests/vectors/factory-voice"


def _wav(rate: int, channels: int, frames: bytes) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wav:
        wav.setparams((channels, 2, rate, 0, "NONE", ""))
        wav.writeframes(frames)
    return out.getvalue()


@pytest.mark.parametrize("stem", ["retry-label", "preserve-timeout"])
@pytest.mark.parametrize(
    ("suffix", "declared"),
    [
        ("wav", "audio/wav"),
        ("mp3", "audio/mpeg"),
        ("m4a", "audio/mp4"),
        ("webm", "audio/webm; codecs=opus"),
    ],
)
def test_pcm_has_the_duration_and_energy_of_the_spoken_fixture(
    stem: str,
    suffix: str,
    declared: str,
) -> None:
    with wave.open(str(VECTORS / f"{stem}.wav")) as wav:
        seconds = wav.getnframes() / wav.getframerate()
    result = decode_audio(declared, (VECTORS / f"{stem}.{suffix}").read_bytes())
    assert isinstance(result, bytes)
    samples = array.array("h", result)
    assert len(samples) / 16_000 == pytest.approx(seconds, abs=0.08)
    assert min(samples) < -1000 and max(samples) > 1000


def test_resamples_the_two_input_channels_to_mono_pcm() -> None:
    stereo = array.array("h", [10_000, -10_000] * 48_000)
    result = decode_audio("audio/x-wav", _wav(48_000, 2, stereo.tobytes()))
    assert result == bytes(16_000 * 2)


@pytest.mark.parametrize(
    ("declared", "data", "reason"),
    [
        ("application/octet-stream", b"audio", "invalid_media"),
        ("audio/wav", b"", "invalid_media"),
        ("audio/wav", b"RIFF" + bytes(180), "invalid_media"),
        ("audio/wav", b"#EXTM3U\n#EXTINF:1,\nsegment.wav\n", "invalid_media"),
        ("audio/wav", _wav(192_000, 1, bytes(4096)), "media_limit"),
        ("audio/wav", _wav(16_000, 3, bytes(6000)), "media_limit"),
        ("audio/wav", _wav(1000, 1, bytes(301 * 1000 * 2)), "media_limit"),
    ],
)
def test_refuses_unreadable_unsupported_or_over_budget_audio(
    declared: str,
    data: bytes,
    reason: str,
) -> None:
    result = decode_audio(declared, data)
    assert isinstance(result, Refused) and result.reason == reason


def test_encoded_limit_precedes_demux() -> None:
    assert decode_audio("audio/mpeg", bytes(MAX_ENCODED_BYTES + 1)) == Refused(reason="media_limit")


def test_refuses_a_valid_recording_under_another_declaration() -> None:
    result = decode_audio("audio/webm", (VECTORS / "retry-label.m4a").read_bytes())
    assert result == Refused(reason="invalid_media")


def test_refuses_a_wav_with_a_non_pcm_codec() -> None:
    out = io.BytesIO()
    with av.open(out, mode="w", format="wav") as writer:
        stream = writer.add_stream("pcm_alaw", rate=8000)
        stream.layout = "mono"
        frame = av.AudioFrame(format="s16", layout="mono", samples=8000)
        frame.sample_rate = 8000
        frame.planes[0].update(bytes(frame.planes[0].buffer_size))
        writer.mux(stream.encode(frame))
        writer.mux(stream.encode(None))
    assert decode_audio("audio/wav", out.getvalue()) == Refused(reason="invalid_media")


@pytest.mark.parametrize(("container", "mime"), [("mp4", "audio/mp4"), ("matroska", "audio/webm")])
def test_opus_stream_in_a_different_container_is_refused(container: str, mime: str) -> None:
    out = io.BytesIO()
    with av.open(str(VECTORS / "retry-label.webm"), "r") as source:
        stream = source.streams.audio[0]
        with av.open(out, "w", format=container) as writer:
            output = writer.add_stream_from_template(stream)
            for packet in source.demux(stream):
                if packet.dts is not None:
                    packet.stream = output
                    writer.mux(packet)
    assert decode_audio(mime, out.getvalue()) == Refused(reason="invalid_media")
