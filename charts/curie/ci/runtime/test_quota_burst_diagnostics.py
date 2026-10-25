"""@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/2/3: bounded, observation-only diagnostics."""

from __future__ import annotations

import ast
import calendar
import importlib.util
import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "idle-capacity-reclamation-runtime.sh"
HELPER = HERE / "quota_burst_diagnostics.py"
PREFIX = "Quota burst diagnostic: "
FALLBACK = PREFIX + '{"schema_version":1,"diagnostic_unavailable":true}'
SECRET = "SYNTHETIC_SECRET_MUST_NEVER_APPEAR"
KEYS = {
    "schema_version", "elapsed_seconds", "prior_completed_count", "prior_sample_age_seconds",
    "fresh_exited_count", "fresh_running_count", "last_observation_seconds",
    "max_observation_seconds", "processes", "diagnostic_unavailable",
}


def load_helper() -> Any:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: missing implementation fails in a test body."""
    assert HELPER.is_file(), "the bounded diagnostic helper has not been implemented"
    spec = importlib.util.spec_from_file_location("quota_burst_diagnostics_under_test", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inputs(**changes: Any) -> dict[str, Any]:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: independent literal serializer input."""
    return {
        "elapsed_seconds": 320.12349,
        "prior_completed_count": 15,
        "prior_sample_age_seconds": 40.98765,
        "last_observation_seconds": 7.12345,
        "max_observation_seconds": 40.98765,
        "returncodes": [0] * 14 + [-9, None],
        **changes,
    }


def decode(line: str) -> dict[str, Any]:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: validate the public line boundary."""
    assert line.startswith(PREFIX)
    assert "\n" not in line and "\r" not in line
    assert len(line.encode("utf-8")) < 4096
    assert SECRET not in line
    return json.loads(line[len(PREFIX):])


def driver_tree() -> ast.Module:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: parse exactly the actual driver heredoc."""
    drivers = re.findall(r'cat >"\$WORKDIR/driver\.py" <<\'PY\'\n(.*?)\nPY\n',
                         SCRIPT.read_text(), re.DOTALL)
    assert len(drivers) == 1, "expected exactly one embedded driver heredoc"
    return ast.parse(drivers[0], filename=str(SCRIPT))


class Clock:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: no real sleeps or clock races."""

    def __init__(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3."""
        self.sleeps.append(seconds)
        self.now += seconds


class Process:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2/3: owned poll boundary, guarded payloads."""

    def __init__(self, code: int | None) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3."""
        self.code = code
        self.polls = 0
        self.poll_error: Exception | None = None
        self.cleanup = False
        self.actions: list[str] = []
        self.wait_timeout = False

    def __getattr__(self, name: str) -> Any:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: reject args/PID/output inspection."""
        raise AssertionError(f"unexpected process attribute {name}: {SECRET}")

    def poll(self) -> int | None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2/3."""
        self.polls += 1
        if self.poll_error is not None:
            raise self.poll_error
        return self.code

    def terminate(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: only original cleanup may terminate."""
        assert self.cleanup, "diagnostics must not terminate a listener"
        self.actions.append("terminate")

    def wait(self, timeout: int) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: original cleanup retains its timeout."""
        assert self.cleanup and timeout == 5
        self.actions.append("wait")
        if self.wait_timeout:
            self.wait_timeout = False
            raise subprocess.TimeoutExpired("synthetic-owned-listener", timeout)

    def kill(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: only original cleanup may kill."""
        assert self.cleanup, "diagnostics must not kill a listener"
        self.actions.append("kill")


class Burst:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: execute the real function, no driver setup."""

    def __init__(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3."""
        self.clock = Clock()
        self.processes = [Process(0) for _ in range(15)] + [Process(None)]
        self.turns: list[dict[str, Any]] = []
        self.observations = 0
        self.observation_durations = [321.0]
        self.observation_error: Exception | None = None
        self.listener_error: Exception | None = None
        self.exit_on_observation = True
        self.followups: list[str] = []
        self.logs = (
            '{"timestamp":"2026-01-01T00:00:00.000000Z",'
            '"message":"capacity wait parked event_id=synthetic-event"}\n'
            '{"timestamp":"2026-01-01T00:00:01.000000Z",'
            '"message":"capacity wait woke event_id=synthetic-event"}'
        )
        self.emitter_error: Exception | None = None
        self.globals: dict[str, Any] = {
            "time": SimpleNamespace(monotonic=self.clock.monotonic, sleep=self.clock.sleep,
                                    time=self.clock.monotonic, strftime=time.strftime,
                                    gmtime=time.gmtime, strptime=time.strptime),
            "calendar": calendar, "json": json, "sys": sys,
            "start": self.start, "finished": self.finished, "get": self.get,
            "counts_against_quota": lambda pod, now: True,
            "observe": self.observe, "run": self.run,
            "kube": ["synthetic-kubectl"], "fullname": "synthetic-release",
            "emit_summary": self.emit_summary,
            "quota_burst_diagnostics": SimpleNamespace(emit_summary=self.emit_summary),
        }

    def start(self, name: str, text: str) -> dict[str, Any]:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: no process is launched."""
        index = len(self.turns)
        assert name == f"burst16-{index}"
        assert text == "held-open capacity probe burst16 " + str(index)
        turn = {"process": self.processes[index], "finished": None,
                "name": SECRET, "args": SECRET, "output": SECRET, "error": SECRET,
                "thread": SECRET, "text": SECRET, "started": 0.0}
        self.turns.append(turn)
        return turn

    def finished(self, turn: dict[str, Any]) -> bool:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: deterministic listener boundary."""
        if self.listener_error is not None:
            raise self.listener_error
        complete = turn["process"].poll() is not None
        if complete:
            turn["finished"] = self.clock.now
        return complete

    def get(self, kind: str, *args: str) -> dict[str, Any]:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: synthetic existing Kubernetes reads."""
        if kind == "resourcequota":
            assert args == ("synthetic-release-sandbox-quota",)
            self.clock.now += self.observation_durations[self.observations] / 2
            self.observations += 1
            if self.exit_on_observation:
                self.processes[-1].code = 0
            return {"spec": {"hard": {"pods": "2"}, "scopeSelector": {
                "matchExpressions": [{"values": ["synthetic-priority"]}]}},
                "status": {"used": {"pods": "2"}}}
        assert kind == "pods" and args == ()
        self.clock.now += self.observation_durations[self.observations - 1] / 2
        if self.observation_error is not None:
            raise self.observation_error
        return {"items": []}

    def observe(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: retain post-loop observation."""
        self.followups.append("observe")

    def run(self, args: list[str], *, timeout: int) -> str:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: no subprocess or output file reads."""
        assert args[:3] == ["synthetic-kubectl", "logs", "deployment/synthetic-release-worker"]
        assert len(args) == 4 and args[3].startswith("--since-time=") and timeout == 60
        self.followups.append("logs")
        return self.logs

    def emit_summary(self, **kwargs: Any) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2/3: resolve the actual helper lazily."""
        if self.emitter_error is not None:
            raise self.emitter_error
        load_helper().emit_summary(**kwargs)

    def invoke(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-3: compile no surrounding setup/body."""
        definitions = [node for node in driver_tree().body
                       if isinstance(node, ast.FunctionDef) and node.name == "burst_sixteen"]
        assert len(definitions) == 1, "expected exactly one burst_sixteen definition"
        code = ast.Module(body=definitions, type_ignores=[])
        exec(compile(code, str(SCRIPT), "exec"), self.globals)
        self.globals["burst_sixteen"]()

    def cleanup(self) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: execute the original driver finally."""
        cleanups = [node.finalbody for node in driver_tree().body
                    if isinstance(node, ast.Try) and node.finalbody]
        assert len(cleanups) == 1
        for process in self.processes:
            process.cleanup = True
            process.poll_error = None
        code = ast.Module(body=cleanups[0], type_ignores=[])
        exec(compile(code, str(SCRIPT), "exec"),
             {"children": self.processes, "subprocess": subprocess})


def summary(capsys: pytest.CaptureFixture[str], *, markers: tuple[str, ...] = ()) -> dict[str, Any]:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: one diagnostic across both streams."""
    captured = capsys.readouterr()
    lines = (captured.out + captured.err).splitlines()
    diagnostics = [line for line in lines if line.startswith(PREFIX)]
    assert len(diagnostics) == 1, "expected one diagnostic on loop exit"
    assert all(SECRET not in line for line in lines)
    assert all(marker in lines for marker in markers)
    return decode(diagnostics[0])


def test_summary_allowlists_fields_rounds_durations_and_keeps_exit_codes() -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: reject payload leakage and success inference."""
    result = decode(load_helper().render_summary(**inputs()))
    assert set(result) == KEYS
    assert type(result["schema_version"]) is int and result["schema_version"] == 1
    assert result["diagnostic_unavailable"] is False
    assert result["elapsed_seconds"] == 320.123
    assert result["prior_completed_count"] == 15
    assert result["prior_sample_age_seconds"] == result["max_observation_seconds"] == 40.988
    assert result["last_observation_seconds"] == 7.123
    assert result["fresh_exited_count"] == 15 and result["fresh_running_count"] == 1
    assert all(type(result[key]) is int for key in (
        "prior_completed_count", "fresh_exited_count", "fresh_running_count"))
    assert len(result["processes"]) == 16
    for index, record in enumerate(result["processes"]):
        assert set(record) == {"index", "state", "returncode"}
        assert type(record["index"]) is int and record["index"] == index
        assert record["state"] == ("running" if index == 15 else "exited")
        assert record["returncode"] == (None if index == 15 else -9 if index == 14 else 0)
        assert record["returncode"] is None or type(record["returncode"]) is int


def test_summary_without_prior_sample_uses_nulls() -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: an incomplete sample is not zero completed."""
    result = decode(load_helper().render_summary(**inputs(
        prior_completed_count=None, prior_sample_age_seconds=None, returncodes=[None] * 16)))
    assert result["prior_completed_count"] is result["prior_sample_age_seconds"] is None
    assert result["fresh_exited_count"] == 0 and result["fresh_running_count"] == 16


def test_invalid_nonfinite_and_oversize_inputs_use_only_fixed_fallback() -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: never stringify rejected arbitrary inputs."""
    helper = load_helper()
    invalid = [
        {"elapsed_seconds": float("nan")}, {"prior_sample_age_seconds": float("inf")},
        {"last_observation_seconds": -0.001}, {"max_observation_seconds": True},
        {"prior_completed_count": True}, {"prior_completed_count": 17},
        {"prior_completed_count": -1}, {"elapsed_seconds": SECRET},
        {"returncodes": [0] * 15}, {"returncodes": [0] * 17},
        {"returncodes": [0] * 15 + [True]}, {"returncodes": [0] * 15 + [0.0]},
        {"returncodes": [0] * 15 + [{"token": SECRET}]},
        {"returncodes": [10**1000] * 16},
    ]
    for changes in invalid:
        assert helper.render_summary(**inputs(**changes)) == FALLBACK


def test_serializer_failure_uses_fixed_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: a serialization failure leaks no exception."""
    helper = load_helper()

    def fail(*args: Any, **kwargs: Any) -> str:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
        raise ValueError(SECRET)

    monkeypatch.setattr(json, "dumps", fail)
    assert helper.render_summary(**inputs()) == FALLBACK


def test_emitter_attempts_one_delivery_even_when_stream_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: output failure cannot fail the scenario."""
    helper = load_helper()
    attempts: list[str] = []

    def broken_write(text: str) -> None:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: one real stream delivery boundary."""
        attempts.append(text)
        raise OSError(SECRET)

    with monkeypatch.context() as patch:
        stream = SimpleNamespace(write=broken_write, flush=lambda: None)
        patch.setattr(sys, "stdout", stream)
        patch.setattr(sys, "stderr", stream)
        helper.emit_summary(**inputs())
    assert len(attempts) == 1
    assert set(decode(attempts[0].rstrip("\n"))) == KEYS
    assert capsys.readouterr() == ("", "")


def test_helper_import_has_no_operational_side_effects(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: importing diagnostics must stay inert."""
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
        pytest.fail("diagnostic import attempted an operational side effect")

    for target, name in ((subprocess, "Popen"), (subprocess, "run"), (os, "getenv"),
                         (socket, "create_connection"), (Path, "write_text")):
        monkeypatch.setattr(target, name, forbidden)
    load_helper()
    assert capsys.readouterr() == ("", "")


def test_renderer_exception_is_delivered_as_fixed_fallback_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: construction failure has one fixed delivery."""
    helper = load_helper()

    def fail(**kwargs: Any) -> str:
        """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
        raise ValueError(SECRET)

    monkeypatch.setattr(helper, "render_summary", fail)
    helper.emit_summary(**inputs())
    captured = capsys.readouterr()
    assert (captured.out + captured.err).splitlines() == [FALLBACK]


def test_blocking_observation_keeps_original_timeout_despite_sixteen_fresh_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: fresh exits never overwrite stale verdict."""
    burst = Burst()
    with pytest.raises(AssertionError, match="^sixteen-thread burst exceeded its bounded runtime$"):
        burst.invoke()
    result = summary(capsys)
    assert result["prior_completed_count"] == 15
    assert result["fresh_exited_count"] == 16 and result["fresh_running_count"] == 0
    assert result["elapsed_seconds"] == result["prior_sample_age_seconds"] == 321.0
    assert result["last_observation_seconds"] == result["max_observation_seconds"] == 321.0
    assert len(burst.turns) == 16 and burst.turns[-1]["finished"] is None
    assert burst.followups == [] and burst.clock.sleeps == []
    assert all(process.actions == [] and process.polls == 2 for process in burst.processes)


def test_observation_error_records_duration_and_preserves_same_exception(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: timing finally cannot mask observation failure."""
    burst = Burst()
    original = RuntimeError(SECRET)
    burst.observation_error = original
    burst.observation_durations = [7.5]
    with pytest.raises(RuntimeError) as raised:
        burst.invoke()
    assert raised.value is original
    result = summary(capsys)
    assert result["last_observation_seconds"] == result["max_observation_seconds"] == 7.5
    assert burst.followups == []


def test_last_observation_and_max_observation_are_separate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: retain original sleep/sample loop ordering."""
    burst = Burst()
    burst.observation_durations = [4.0, 1.0]
    burst.invoke()
    result = summary(capsys)
    assert result["prior_completed_count"] == 16
    assert result["last_observation_seconds"] == 1.0 and result["max_observation_seconds"] == 4.0
    assert result["elapsed_seconds"] == 5.2 and result["prior_sample_age_seconds"] == 1.0
    assert burst.clock.sleeps == [0.2] and burst.followups == ["observe", "logs"]


def test_listener_error_retains_original_exception_and_null_sample(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: partial finished() evaluation is not a sample."""
    burst = Burst()
    original = AssertionError("a quota-pressure CLI listener failed")
    burst.listener_error = original
    with pytest.raises(AssertionError) as raised:
        burst.invoke()
    assert raised.value is original
    result = summary(capsys)
    assert result["prior_completed_count"] is result["prior_sample_age_seconds"] is None
    assert result["last_observation_seconds"] == result["max_observation_seconds"] == 0
    assert result["fresh_exited_count"] == 15 and result["fresh_running_count"] == 1
    assert burst.observations == 0 and burst.followups == []


def test_polling_failure_uses_fixed_fallback_and_preserves_original_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2/3: diagnostic polling error is private."""
    burst = Burst()
    original = AssertionError("a quota-pressure CLI listener failed")
    burst.listener_error = original
    burst.processes[0].poll_error = RuntimeError(SECRET)
    with pytest.raises(AssertionError) as raised:
        burst.invoke()
    assert raised.value is original
    assert summary(capsys) == {"schema_version": 1, "diagnostic_unavailable": True}


def test_failed_diagnostic_does_not_replace_error_or_take_cleanup_ownership() -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: original driver cleanup remains effective."""
    burst = Burst()
    original = AssertionError("a quota-pressure CLI listener failed")
    burst.listener_error = original
    burst.emitter_error = OSError(SECRET)
    with pytest.raises(AssertionError) as raised:
        try:
            burst.invoke()
        finally:
            assert all(process.actions == [] for process in burst.processes)
            burst.processes[-1].wait_timeout = True
            burst.cleanup()
    assert raised.value is original
    assert burst.processes[-1].actions == ["terminate", "wait", "kill", "wait"]
    assert all(process.actions == ["wait"] for process in burst.processes[:-1])


@pytest.mark.parametrize("missing_wake", [False, True])
def test_completed_loop_preserves_followup_verdicts(
    missing_wake: bool, capsys: pytest.CaptureFixture[str],
) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-1/3: summary cannot skip park/wake assertions."""
    burst = Burst()
    burst.processes[-1].code = 0
    burst.observation_durations = [321.0]
    if missing_wake:
        burst.logs = burst.logs.splitlines()[0]
        with pytest.raises(AssertionError, match="^a park had no later wake$"):
            burst.invoke()
    else:
        burst.invoke()
    markers = () if missing_wake else (
        "parked_turn_burst_16", "finalized_16", "park_observed", "unmatched_parks_0",
        "max_park_wake_s",
    )
    result = summary(capsys, markers=markers)
    assert result["prior_completed_count"] == result["fresh_exited_count"] == 16
    assert result["elapsed_seconds"] == 321.0 and burst.clock.sleeps == []
    assert burst.followups == ["observe", "logs"]


def test_output_failure_cannot_fail_a_successful_burst() -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2/3: best-effort delivery retains success."""
    burst = Burst()
    burst.processes[-1].code = 0
    burst.observation_durations = [0.5]
    burst.emitter_error = OSError(SECRET)
    burst.invoke()
    assert burst.followups == ["observe", "logs"]
