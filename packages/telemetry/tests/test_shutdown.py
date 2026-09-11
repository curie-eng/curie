"""ServiceTelemetry.shutdown spends one deadline, not one per provider call.

Regression coverage for curie-eng/curie#2362 — "ServiceTelemetry.shutdown's worst
case equals the default pod termination grace period". The pre-fix implementation
force-flushed every provider sequentially and then shut every provider down
sequentially, each of the six calls independently bounded, so worst case was
``2 x N x timeout_millis``. These tests pin the post-fix contract: one shared
wall-clock deadline for the whole call, flush-before-shutdown preserved per
provider, and the two calls never overlapping on the same provider.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from typing import Any

from curie_telemetry.bootstrap import ServiceTelemetry
from opentelemetry.sdk.metrics import MeterProvider

# Fake-provider tests assert ratios against this budget, never absolute seconds:
# the box running the suite is heavily loaded and a wall-clock band would flake.
# 300ms keeps the negative control (which must burn a full deadline) fast while
# leaving the 6x sequential cost — 1.8s — far outside every band below.
_TIMEOUT_MILLIS = 300
_TIMEOUT_SECONDS = _TIMEOUT_MILLIS / 1000


class _CallRecorder:
    """Shared event log. Provider workers append concurrently, so it locks."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[str, str, str]] = []
        # AC 3 violations are *recorded*, never raised: ``_drain_provider``
        # wraps every provider call in ``except BaseException: pass``, so an
        # AssertionError raised inside a fake is swallowed by production and
        # never reaches pytest. The test thread reads this list instead.
        self.violations: list[str] = []

    def record(self, provider: str, method: str, phase: str) -> None:
        with self._lock:
            self.events.append((provider, method, phase))

    def record_violation(self, message: str) -> None:
        with self._lock:
            self.violations.append(message)

    def events_for(self, provider: str) -> list[tuple[str, str]]:
        """(method, phase) pairs recorded for one provider, in order."""

        with self._lock:
            return [
                (method, phase)
                for name, method, phase in self.events
                if name == provider
            ]

    def phases_for(self, provider: str, method: str) -> list[str]:
        """Ordered phases recorded for one provider/method pair."""

        return [
            phase
            for recorded_method, phase in self.events_for(provider)
            if recorded_method == method
        ]

    def methods_for(self, provider: str) -> list[str]:
        """Ordered method names for one provider, one entry per call."""

        return [
            method
            for method, phase in self.events_for(provider)
            if phase == "enter"
        ]


class _FakeProvider:
    """Base fake matching how ``_drain_provider`` invokes a real provider.

    ``_drain_provider`` owns one provider for the whole call: it flushes, then
    shuts down, sequentially, on a single daemon worker thread.

    ``force_flush`` is called as ``force_flush(timeout_millis=...)`` and
    ``shutdown`` with no arguments. Both accept ``**kwargs`` so that the fix
    passing a *computed remaining budget* rather than the raw timeout — or any
    other incidental signature change — does not turn a behavioural test into a
    signature test.
    """

    def __init__(self, name: str, recorder: _CallRecorder) -> None:
        self.name = name
        self._recorder = recorder
        # Set for the duration of force_flush; shutdown records a violation if
        # it is still set on entry. This is the direct proof of AC 3 (no
        # concurrent flush/shutdown on one provider) — an ordering-only check
        # would pass a racy implementation silently. It must *record* rather
        # than raise: production swallows every exception a provider throws.
        self._flush_in_progress = False

    def force_flush(self, timeout_millis: int | None = None, **kwargs: Any) -> bool:
        self._recorder.record(self.name, "force_flush", "enter")
        self._flush_in_progress = True
        try:
            self._flush(timeout_millis)
        finally:
            self._flush_in_progress = False
            self._recorder.record(self.name, "force_flush", "exit")
        return True

    def shutdown(self, **kwargs: Any) -> None:
        if self._flush_in_progress:
            self._recorder.record_violation(
                f"{self.name}: shutdown entered while force_flush in flight"
            )
        self._recorder.record(self.name, "shutdown", "enter")
        try:
            self._shutdown()
        finally:
            self._recorder.record(self.name, "shutdown", "exit")

    def _flush(self, timeout_millis: int | None) -> None:
        return None

    def _shutdown(self) -> None:
        return None


class _FastProvider(_FakeProvider):
    """Returns immediately from both calls."""


class _BlockingProvider(_FakeProvider):
    """Blocks both calls until released.

    The release Event is always set from the test's ``finally``, so a failing
    assertion cannot wedge worker threads and hang the suite.
    """

    def __init__(self, name: str, recorder: _CallRecorder) -> None:
        super().__init__(name, recorder)
        self.release = threading.Event()

    def _flush(self, timeout_millis: int | None) -> None:
        self.release.wait()

    def _shutdown(self) -> None:
        self.release.wait()


class _RaisingProvider(_FakeProvider):
    """Raises from ``force_flush``, and from ``shutdown`` unless told otherwise."""

    def __init__(
        self, name: str, recorder: _CallRecorder, *, raise_on_shutdown: bool = True
    ) -> None:
        super().__init__(name, recorder)
        self._raise_on_shutdown = raise_on_shutdown

    def _flush(self, timeout_millis: int | None) -> None:
        raise RuntimeError(f"{self.name}: force_flush exploded")

    def _shutdown(self) -> None:
        if self._raise_on_shutdown:
            raise RuntimeError(f"{self.name}: shutdown exploded")


def _telemetry(*providers: Any) -> ServiceTelemetry:
    """Build ServiceTelemetry directly — the dataclass takes the three providers
    positionally, so no bootstrap (and no real exporter) is involved."""

    tracer, logger, meter = providers
    return ServiceTelemetry(tracer, logger, meter)


def _await_shutdowns(
    recorder: _CallRecorder, names: list[str], *, budget_seconds: float = 2.0
) -> None:
    """Bounded wait until every named provider's shutdown() has exited.

    Workers abandoned at the deadline finish asynchronously once their release
    Event is set, so violations recorded inside shutdown() are not visible the
    instant ``ServiceTelemetry.shutdown`` returns. The wait is bounded and never
    asserts: it exists so the caller's own assertions are not vacuous, and a
    genuinely stuck worker surfaces as that caller's failure, not as a hang.
    """

    limit = time.monotonic() + budget_seconds
    while time.monotonic() < limit:
        if all(recorder.phases_for(name, "shutdown") == ["enter", "exit"] for name in names):
            return
        time.sleep(0.01)


def test_all_blocked_providers_complete_within_one_deadline() -> None:
    """The negative control: three providers that never return must still cost
    one deadline, not six.

    Sequentially bounded, this costs 6 x timeout. Bounded by one shared
    deadline, it costs ~1 x timeout. The bands sit either side of that gap.
    """

    recorder = _CallRecorder()
    providers = [_BlockingProvider(f"p{index}", recorder) for index in range(3)]
    telemetry = _telemetry(*providers)
    try:
        started = time.monotonic()
        telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
        elapsed = time.monotonic() - started
    finally:
        for provider in providers:
            provider.release.set()

    # 2x leaves a full extra deadline of slack for thread spawn on a loaded box,
    # and is the only band needed: the old sequential shape cost ~6x for fakes
    # that block both calls (2 calls x 3 providers, each bounded separately), so
    # anything above 2x is already unambiguously the per-call-timeout shape.
    assert elapsed < 2 * _TIMEOUT_SECONDS, (
        f"shutdown took {elapsed:.3f}s for a {_TIMEOUT_SECONDS:.3f}s budget"
    )

    # The workers were abandoned at the deadline mid-flush; they only reach
    # shutdown() once the release above lands. Wait for them (bounded, so a
    # regression is a failure rather than a hang) before reading violations —
    # otherwise the AC 3 check below is vacuous because no shutdown ran yet.
    _await_shutdowns(recorder, [provider.name for provider in providers])
    # AC 3 in the case that actually stresses it: a racy implementation that
    # started shutdown() while force_flush() was still in flight would land here
    # and nowhere else, and elapsed alone would not notice.
    assert recorder.violations == [], recorder.violations


def test_raising_provider_does_not_propagate_and_does_not_block_siblings() -> None:
    """One exploding provider neither escapes nor starves the others."""

    recorder = _CallRecorder()
    raising = _RaisingProvider("raising", recorder)
    fast_a = _FastProvider("fast_a", recorder)
    fast_b = _FastProvider("fast_b", recorder)
    telemetry = _telemetry(raising, fast_a, fast_b)

    started = time.monotonic()
    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
    elapsed = time.monotonic() - started

    assert recorder.methods_for("fast_a") == ["force_flush", "shutdown"]
    assert recorder.methods_for("fast_b") == ["force_flush", "shutdown"]
    assert recorder.violations == [], recorder.violations
    # Nothing blocks here, so the whole call should land well inside one budget.
    assert elapsed < _TIMEOUT_SECONDS


def test_flush_failure_still_calls_shutdown() -> None:
    """A provider that fails only its flush must still be shut down.

    Flush and shutdown need independent failure handling; folding them into one
    try block would silently skip the shutdown of any provider whose flush
    raised, leaking its export threads.
    """

    recorder = _CallRecorder()
    flush_only_raiser = _RaisingProvider(
        "flush_only", recorder, raise_on_shutdown=False
    )
    telemetry = _telemetry(flush_only_raiser, None, _FastProvider("fast", recorder))

    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)

    assert recorder.methods_for("flush_only") == ["force_flush", "shutdown"]
    assert recorder.violations == [], recorder.violations


def test_fast_providers_return_promptly_and_preserve_order() -> None:
    """AC 2 and AC 3: per provider, flush then shutdown, never overlapping."""

    recorder = _CallRecorder()
    providers = [_FastProvider(f"p{index}", recorder) for index in range(3)]
    telemetry = _telemetry(*providers)

    started = time.monotonic()
    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
    elapsed = time.monotonic() - started

    for provider in providers:
        # AC 2: ordering within the provider.
        assert recorder.methods_for(provider.name) == ["force_flush", "shutdown"]
        # AC 3: the flush must have fully exited before shutdown was entered.
        # The recorded-violation check below is the primary guard; pin the
        # interleaving in the event log too so the failure message is readable.
        own = recorder.events_for(provider.name)
        assert own == [
            ("force_flush", "enter"),
            ("force_flush", "exit"),
            ("shutdown", "enter"),
            ("shutdown", "exit"),
        ]

    assert recorder.violations == [], recorder.violations
    # No unconditional sleep-to-deadline: fast providers must not cost a budget.
    assert elapsed < _TIMEOUT_SECONDS / 2


def test_readerless_meter_provider_only() -> None:
    """A "no-provider" install is a readerless MeterProvider and two Nones.

    ``_meter_provider`` always returns a MeterProvider, so ``meter_provider`` is
    never None in production; a reader-free one is the real minimum shape.
    """

    telemetry = ServiceTelemetry(None, None, MeterProvider(metric_readers=[]))

    started = time.monotonic()
    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
    elapsed = time.monotonic() - started

    assert elapsed < _TIMEOUT_SECONDS


def test_second_shutdown_is_a_noop() -> None:
    """Idempotency: the second call touches no provider and returns at once."""

    recorder = _CallRecorder()
    providers = [_FastProvider(f"p{index}", recorder) for index in range(3)]
    telemetry = _telemetry(*providers)

    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
    started = time.monotonic()
    telemetry.shutdown(timeout_millis=_TIMEOUT_MILLIS)
    elapsed = time.monotonic() - started

    for provider in providers:
        assert recorder.methods_for(provider.name) == ["force_flush", "shutdown"]
    assert recorder.violations == [], recorder.violations
    assert elapsed < _TIMEOUT_SECONDS / 2


_CHILD_PROGRAM = textwrap.dedent(
    """
    import logging
    import time

    from curie_telemetry.bootstrap import bootstrap_service_telemetry
    from opentelemetry import trace

    logger = logging.getLogger("curie.test.2362")
    telemetry = bootstrap_service_telemetry(
        "curie-2362-child",
        service_version="0.0.0",
        logger=logger,
    )
    with trace.get_tracer("curie.test.2362").start_as_current_span("child-span"):
        # The log record matters as much as the span: measurement on
        # opentelemetry-sdk 1.44.0 shows the gRPC LoggerProvider is the only
        # provider that actually blocks against an unreachable endpoint —
        # force_flush burns the full 5s and shutdown() burns another 5s, while
        # the tracer and meter both return in ~0.000s.
        logger.info("child emitted a record")

    # The child times its own shutdown() and prints it: interpreter startup on a
    # loaded box is unbounded noise, so the parent cannot derive this number
    # from total wall clock tightly enough to discriminate.
    started = time.monotonic()
    telemetry.shutdown()
    print(f"SHUTDOWN_SECONDS={time.monotonic() - started:.3f}", flush=True)
    print("SHUTDOWN_RETURNED", flush=True)
    # Deliberately no os._exit: the interpreter must be able to fall off the end
    # of the program on its own. A non-daemon export thread left running would
    # keep it alive past the assertion below while every fake-provider test in
    # this file still passed.
    """
)


def test_subprocess_with_unreachable_exporter_exits_under_deadline(
    tmp_path: Path,
) -> None:
    """AC 5: the process itself exits, not just ``shutdown()``.

    The band below is empirical, not guessed. Measured against an unreachable
    gRPC endpoint: control (sequential, pre-fix) 10.004s in-process and 10.64s
    SIGTERM-to-pod-gone; candidate (one shared deadline) 5.000s and 5.45s.
    """

    script = tmp_path / "child.py"
    script.write_text(_CHILD_PROGRAM, encoding="utf-8")

    env = dict(os.environ)
    # A blackholed address rather than a refused connection: connection-refused
    # fails fast and would let this pass vacuously without ever exercising a
    # blocked exporter. gRPC specifically — the measured hang is the gRPC
    # LoggerProvider; http/protobuf does not reproduce it, so forcing HTTP here
    # would test a code path that never blocks.
    # Per-signal OTLP variables outrank the generic ones in the SDK's
    # precedence rules (see bootstrap.py's ``_exporter_endpoint``). If the
    # ambient environment happened to set one of these to a reachable
    # collector, the child would export there instead of to the blackholed
    # address below, and this test would pass without ever exercising a
    # blocked exporter. Clear them so the blackholed endpoint is unambiguous.
    for signal in ("TRACES", "LOGS", "METRICS"):
        env.pop(f"OTEL_EXPORTER_OTLP_{signal}_ENDPOINT", None)
        env.pop(f"OTEL_EXPORTER_OTLP_{signal}_PROTOCOL", None)
        env.pop(f"OTEL_EXPORTER_OTLP_{signal}_HEADERS", None)
    env.pop("OTEL_EXPORTER_OTLP_HEADERS", None)

    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://10.255.255.1:4317"
    env["OTEL_EXPORTER_OTLP_PROTOCOL"] = "grpc"
    env.pop("OTEL_SDK_DISABLED", None)

    started = time.monotonic()
    # The subprocess timeout sits far above every assertion on purpose, so a
    # regression surfaces as a readable assertion failure rather than a hang.
    completed = subprocess.run(
        [sys.executable, str(script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    elapsed = time.monotonic() - started

    # AC 5's other half: the interpreter falls off the end of the program with
    # no os._exit, so a non-daemon export thread left running fails here.
    assert completed.returncode == 0, completed.stderr
    assert "SHUTDOWN_RETURNED" in completed.stdout

    reported = [
        line for line in completed.stdout.splitlines() if line.startswith("SHUTDOWN_SECONDS=")
    ]
    assert len(reported) == 1, completed.stdout
    shutdown_seconds = float(reported[0].split("=", 1)[1])
    # 7.0 sits in the measured gap: post-fix is a hard 5s bound (5.000s), pre-fix
    # was 10.0s. ~2s of slack on both sides, and interpreter startup is excluded
    # because the child times only the shutdown() call itself.
    assert shutdown_seconds < 7.0, (
        f"shutdown() took {shutdown_seconds:.3f}s — the 10s sequential shape, "
        "not the 5s shared deadline"
    )
    # Secondary guard only, to catch an outright hang; deliberately loose,
    # because interpreter startup on a loaded box cannot be bounded tightly.
    assert elapsed < 25, f"child process took {elapsed:.2f}s to exit"
