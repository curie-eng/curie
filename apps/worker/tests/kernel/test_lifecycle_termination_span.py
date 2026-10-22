"""A confirmed runner pod termination is a closed reason on the turn span (#4940).

The ``curie.turn.process`` lifecycle span carries the termination reason as one
of ``OOMKilled``, ``Evicted``, ``Deleted`` or ``Unknown``, plus the exit code
when it is a real 0..255 int. Kubelet message text never reaches the span.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import QueuedTurn, TextDelta
from curie_worker import consumer as consumer_module
from curie_worker import runner_client as runner_client_module
from curie_worker import stream_consumer as stream_consumer_module
from curie_worker import threadlock as threadlock_module
from curie_worker.kernel import log as kernel_log
from curie_worker.sandbox import substrate as substrate_module
from curie_worker.sandbox.types import SandboxTermination

# importlib import mode does not add the tests directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

import otel_fixtures  # noqa: E402
from otel_fixtures import Probe, SpanCall, install  # noqa: E402
from queue_fixtures import qevent  # noqa: E402

_REASON_KEY = "curie.sandbox.termination.reason"
_EXIT_CODE_KEY = "curie.sandbox.termination.exit_code"


def _install(monkeypatch: pytest.MonkeyPatch) -> Probe:
    probe = install(
        monkeypatch,
        consumer_module,
        stream_consumer_module,
        kernel_log,
        threadlock_module,
        runner_client_module,
        substrate_module,
    )

    # The shared probe stringifies attribute values; keep the raw value so the
    # exit code's int type is observable.
    def set_attribute(self: otel_fixtures.ProbeSpan, name: str, value: Any) -> None:
        self._call.attributes[name] = value

    monkeypatch.setattr(otel_fixtures.ProbeSpan, "set_attribute", set_attribute)
    return probe


def _qevent(text: str, **overrides: Any) -> QueuedTurn:
    overrides.setdefault("thread", "thread-termination-span")
    return qevent(
        text,
        channel="C0EXAMPLE1",
        received_at=datetime.now(UTC).isoformat(),
        **overrides,
    )


def _spans(probe: Probe, name: str) -> list[SpanCall]:
    return [span for span in probe.spans if span.name == name]


async def _drop_stream(
    h, probe: Probe, termination: SandboxTermination | None, thread: str
) -> SpanCall:
    h.fake_k8s.termination = termination
    h.runner.default_script = [TextDelta(text="partial")]
    h.runner.abort_after_frames = True
    await h.kernel.process_event(_qevent("go", thread=thread))
    assert h.runner.opened == ["go"]
    assert h.fake_k8s.termination_queries
    [span] = _spans(probe, "curie.turn.process")
    return span


def test_oomkilled_termination_stamps_reason_and_int_exit_code(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=1) as h:
            span = await _drop_stream(
                h, probe, SandboxTermination("OOMKilled", "exit code 137", 137), "thread-oom"
            )

            assert "sandbox-terminated" in (h.sink.last_text or "")
            assert span.attributes.get("curie.sandbox.termination.reason") == "OOMKilled"
            exit_code = span.attributes.get("curie.sandbox.termination.exit_code")
            assert exit_code == 137
            assert type(exit_code) is int

    asyncio.run(go())


def test_unlisted_termination_reason_is_exported_as_unknown(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=1) as h:
            span = await _drop_stream(
                h, probe, SandboxTermination("NodeLost", "node went away"), "thread-nodelost"
            )

            assert "sandbox-terminated" in (h.sink.last_text or "")
            assert span.attributes.get("curie.sandbox.termination.reason") == "Unknown"
            assert "curie.sandbox.termination.exit_code" not in span.attributes

    asyncio.run(go())


def test_evicted_termination_keeps_kubelet_message_off_the_span(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    canary = "CANARY-4940-kubelet-detail-7f3a"
    message = 'Usage of EmptyDir volume "workspace" exceeds the limit "1Gi".'
    detail = f"{message} {canary}"

    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=1) as h:
            span = await _drop_stream(
                h, probe, SandboxTermination("Evicted", detail), "thread-evicted"
            )

            # The terminal notice still carries the detail; only the span is closed.
            assert canary in (h.sink.last_text or "")
            assert span.attributes.get("curie.sandbox.termination.reason") == "Evicted"
            assert "curie.sandbox.termination.exit_code" not in span.attributes

            for turn_span in _spans(probe, "curie.turn.process"):
                values = [str(value) for value in turn_span.attributes.values()]
                for _name, event_attributes in turn_span.events:
                    values.extend(str(value) for value in event_attributes.values())
                for value in values:
                    assert canary not in value
                    assert message not in value
                    assert "EmptyDir" not in value

    asyncio.run(go())


def test_stream_drop_without_pod_termination_sets_neither_key(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        probe = _install(monkeypatch)
        async with make_harness(max_attempts=1) as h:
            span = await _drop_stream(h, probe, None, "thread-plain-drop")

            assert "runner-error" in (h.sink.last_text or "")
            assert "sandbox-terminated" not in (h.sink.last_text or "")
            assert "curie.sandbox.termination.reason" not in span.attributes
            assert "curie.sandbox.termination.exit_code" not in span.attributes

    asyncio.run(go())
