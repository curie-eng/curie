"""The ACI conformance producer, backed by the real runner over a fake session.

``packages/aci-protocol``'s ``run_conformance`` takes a synchronous producer that
maps one inbound frame to the NDJSON lines a compliant runner emits. This builds
that producer over a real ``SessionRunner`` driven by ``FakeModelSession``, so the
conformance gate validates the actual translation/final plumbing, not a bespoke
canned stream. Each call runs an isolated session to completion via ``anyio.run``.
"""

from __future__ import annotations

from collections.abc import Iterable

import anyio
from aci_protocol import Event, Interrupt

from .fake import FakeModelSession
from .otel import RunTracer
from .session import SessionRunner
from .side_effects import SideEffectClassifier


def _build_runner() -> SessionRunner:
    return SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,  # unbounded: conformance validates protocol shape, not budgets
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="conformance",
        held_secrets=frozenset(),
    )


def conformance_producer(message: Event | Interrupt) -> Iterable[str]:
    """Return the NDJSON lines the runner emits for one inbound frame."""

    lines: list[str] = []

    async def _run() -> None:
        runner = _build_runner()
        await runner.start()
        try:
            async for line in runner.run_inbound(message):
                lines.append(line)
        finally:
            await runner.close()

    anyio.run(_run)
    return lines
