"""Run the offline fixture oracle directly with a path to the pinned model."""

import argparse
import array
import asyncio
import json
import os
import re
import resource
import signal
import time
import wave
from pathlib import Path

import pytest
from curie_api import factory_voice_model
from curie_api.factory_voice_child import prepare_speech
from curie_api.factory_voice_contract import Refused
from curie_api.factory_voice_model import LocalVoiceRecognizer
from factory_voice_fakes import FixtureVoiceRecognizer

VECTORS = Path(__file__).resolve().parents[3] / "tests/vectors/factory-voice"
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.parametrize("exists", [False, True])
async def test_missing_and_mismatched_weights_disable_recognition(
    tmp_path: Path,
    exists: bool,
) -> None:
    path = tmp_path / "weights.bin"
    if exists:
        with path.open("wb") as mount:
            mount.truncate(147_964_211)
    recognizer = LocalVoiceRecognizer(path)
    assert not await recognizer.available()
    result = await recognizer.recognize("audio/wav", (VECTORS / "retry-label.wav").read_bytes())
    assert result == Refused(reason="disabled")


async def test_fifo_model_path_is_never_opened_for_a_blocking_read(tmp_path: Path) -> None:
    path = tmp_path / "weights.bin"
    os.mkfifo(path)
    recognizer = LocalVoiceRecognizer(path)
    async with asyncio.timeout(5):
        assert not await recognizer.available()


@pytest.mark.parametrize("end", ["cancel", "deadline"])
async def test_concurrent_service_instances_reap_the_real_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    end: str,
) -> None:
    children = Path("/proc/thread-self/children")
    initial = set(children.read_text().split())
    data = (VECTORS / "retry-label.wav").read_bytes()
    path = tmp_path / "without-model.bin"
    if end == "deadline":
        monkeypatch.setattr(factory_voice_model, "CHILD_WALL_SECONDS", 1)
    first = asyncio.create_task(LocalVoiceRecognizer(path).recognize("audio/wav", data))
    try:
        async with asyncio.timeout(5):
            while not (new := set(children.read_text().split()) - initial):
                await asyncio.sleep(0)
        assert len(new) == 1
        child = int(new.pop())
        os.kill(child, signal.SIGSTOP)
        assert await LocalVoiceRecognizer(path).recognize("audio/wav", data) == Refused(
            reason="unavailable"
        )
        if end == "deadline":
            assert await first == Refused(reason="unavailable")
    finally:
        if not first.done():
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
    assert not Path(f"/proc/{child}").exists()
    monkeypatch.undo()
    again = await LocalVoiceRecognizer(path).recognize("audio/wav", data)
    assert again == Refused(reason="disabled")


async def test_fake_is_keyed_by_fixture_bytes() -> None:
    data = (VECTORS / "retry-label.wav").read_bytes()
    recognizer = FixtureVoiceRecognizer({data: "Change the button label to Retry"})
    assert await recognizer.recognize("audio/wav", data) == "Change the button label to Retry"
    assert await recognizer.recognize("audio/wav", b"different") == Refused(reason="invalid_media")


def test_trims_quiet_edges_and_long_gaps_preserving_both_speech_bursts() -> None:
    quiet = bytes(16_000 * 2 * 3)
    first = array.array("h", [-24, 24] * 320).tobytes()
    second = array.array("h", [-32768, 24] * 160).tobytes()
    raw = quiet + first + quiet + second + quiet
    margin = bytes(16_000 * 2 * 160 // 1000)
    edge = bytes(16_000 * 2 * 40 // 1000)
    expected = edge + first + margin + margin + second + edge
    assert prepare_speech(raw) == expected
    assert prepare_speech(bytes(174) + raw + bytes(26)) == expected


def test_keeps_short_pauses_and_pads_a_word_below_the_native_minimum() -> None:
    word = array.array("h", [-100, 100] * 400).tobytes()
    normal_pause = bytes(16_000 * 2 // 2)
    edge = bytes(16_000 * 2 * 40 // 1000)
    assert prepare_speech(word + normal_pause + word) == edge + word + normal_pause + word + edge
    short = prepare_speech(word)
    assert isinstance(short, bytes) and short.startswith(edge + word)
    assert len(short) >= 16_000 * 2 // 10


def test_near_zero_quantization_noise_has_no_speech() -> None:
    assert prepare_speech(array.array("h", [1, -1] * 1600).tobytes()) == Refused(reason="no_speech")


def _word_errors(expected: list[str], actual: list[str]) -> int:
    row = list(range(len(actual) + 1))
    for word in expected:
        current = [row[0] + 1]
        for index, other in enumerate(actual):
            current.append(min(row[index + 1] + 1, current[-1] + 1, row[index] + (word != other)))
        row = current
    return row[-1]


async def _oracle(model: Path) -> bool:
    recognizer = LocalVoiceRecognizer(model)
    if not await recognizer.available():
        print(json.dumps({"error": "model_missing_or_hash_mismatch"}))
        return False
    cases = {
        "retry-label": "change the button label to retry",
        "preserve-timeout": "keep the existing timeout do not change it",
        "preserve-timeout-padded": "keep the existing timeout do not change it",
        "issue-number": "change issue forty two",
        "silence": "",
    }
    passed = True
    errors = total = 0
    for stem, reference in cases.items():
        path = VECTORS / f"{stem}.wav"
        with wave.open(str(path)) as wav:
            seconds = wav.getnframes() / wav.getframerate()
        audio = path.read_bytes()
        start = time.monotonic()
        result = await recognizer.recognize("audio/wav", audio)
        wall = time.monotonic() - start
        heard = re.findall(r"[a-z0-9']+", result.lower()) if isinstance(result, str) else []
        distance = _word_errors(reference.split(), heard)
        errors += distance
        total += len(reference.split())
        phrase = " ".join(heard).replace("time out", "timeout").replace("don't", "do not")
        checks: dict[str, bool] = {}
        if stem == "silence":
            checks["no_speech"] = result == Refused(reason="no_speech")
        elif stem.startswith("preserve"):
            checks["timeout"] = "keep the existing timeout" in phrase
            checks["negation"] = "do not change it" in phrase
            if stem.endswith("-padded"):
                checks["no_added_words"] = phrase == "keep the existing timeout do not change it"
        elif stem == "issue-number":
            checks["identifier"] = bool(re.fullmatch(r"change issue (?:42|forty two)", phrase))
        else:
            checks["label_and_value"] = "change the button label to retry" in phrase
        passed &= all(checks.values())
        print(
            json.dumps(
                {
                    "fixture": stem,
                    "reference": reference,
                    "transcript": result if isinstance(result, str) else None,
                    "refusal": result.reason if isinstance(result, Refused) else None,
                    "checks": checks,
                    "word_errors": distance,
                    "reference_words": len(reference.split()),
                    "audio_seconds": round(seconds, 3),
                    "wall_seconds": round(wall, 3),
                    "wall_per_audio_second": round(wall / seconds, 3),
                }
            ),
            flush=True,
        )
    print(
        json.dumps(
            {
                "cpus": len(os.sched_getaffinity(0)),
                "peak_child_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "word_error_rate": round(errors / total, 4),
                "passed": passed,
            }
        )
    )
    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model", type=Path)
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    raise SystemExit(0 if asyncio.run(_oracle(parser.parse_args().model)) else 1)
