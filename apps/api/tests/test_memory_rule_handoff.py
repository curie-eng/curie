"""Learned rules, API to worker to runner, with no double in between (#5798).

A request is admitted with accepted rules seeded for its repository and started
through the functions the internal routes call. Then the production worker
`deliver` reads the envelope from the real internal API route, posts it to the
real runner route over HTTP (the runner's own `MemoryRulesContextGate`, bound to
the execution the API admitted), and relays the runner's acknowledgment back to
the real acknowledgment route. Nothing builds an envelope by hand, so a field the
producer drops (the named counterexample is ``tenant_id``) is refused by the
consumer exactly as it would be in a deployment.
"""

# ruff: noqa: F401, F811  (fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Any

import aiohttp
import personal_execution_support as support
import pytest
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.fake import FakeModelSession
from curie_runner.memory_rules_context import MemoryRulesContextGate
from curie_runner.requester_context import ExecutionBinding
from curie_runner.session import SessionRunner
from curie_worker.memory_rules_context import MemoryRulesContextRefused, deliver
from curie_worker.runner_client import RunnerClient
from curie_worker.workitem_dispatch import WorkItemDispatchClient
from fastapi.testclient import TestClient
from test_memory_rule_admission import TWO_TEXT
from test_memory_rule_context import _agent_of, _evidence, _running
from test_personal_execution_lifecycle import WORKER_HEADERS, api, world

TOKEN = "per-claim-bearer-placeholder"
API_BASE = "http://api.invalid"


class _SyncApiAsAsyncClient:
    """The worker's ``httpx.AsyncClient`` seat, answered by the real API app.

    The API test client is synchronous, so the call is made inline; the response
    is the real response the API produced.
    """

    def __init__(self, api: TestClient) -> None:
        self._api = api

    async def post(
        self, url: str, *, headers: dict[str, str], json: Any, follow_redirects: bool
    ) -> Any:
        assert url.startswith(API_BASE)
        return self._api.post(url.removeprefix(API_BASE), headers=headers, json=json)


class _StrippingRunnerClient(RunnerClient):
    """The real runner client, delivering an envelope a producer defect damaged."""

    def __init__(self, field: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._field = field

    async def deliver_memory_rules_context(
        self, base_url: str, envelope: bytes, *, token: str | None, timeout_s: float
    ) -> tuple[int, dict[str, object] | None]:
        damaged = json.loads(envelope)
        del damaged[self._field]
        return await super().deliver_memory_rules_context(
            base_url,
            json.dumps(damaged, separators=(",", ":"), sort_keys=True).encode(),
            token=token,
            timeout_s=timeout_s,
        )


@dataclass(frozen=True)
class _Run:
    """The two fields of a started run that `deliver` reads."""

    request_id: uuid.UUID
    runtime_epoch: int


def _runner_app(gate: MemoryRulesContextGate) -> Any:
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: FakeModelSession(),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    return create_app(runner, token=TOKEN, memory_rules_context=gate)


def _deliver(
    api: TestClient,
    world: support.World,
    admitted: Any,
    epoch: int,
    *,
    strip: str | None = None,
) -> tuple[str | None, MemoryRulesContextRefused | None, MemoryRulesContextGate]:
    """Run the production worker `deliver` against the real API and runner route."""

    gate = MemoryRulesContextGate()
    gate.bind(
        ExecutionBinding(
            tenant_id=str(world.tenant_id),
            work_item_id=str(admitted.work_item_id),
            execution_request_id=str(admitted.request_id),
            execution_fence=epoch,
        )
    )

    async def go() -> tuple[str | None, MemoryRulesContextRefused | None]:
        work_items = WorkItemDispatchClient(
            api_base_url=API_BASE,
            worker_token=WORKER_HEADERS["X-Curie-Worker-Token"],
            client=_SyncApiAsAsyncClient(api),  # type: ignore[arg-type]
        )
        async with aiohttp.ClientSession() as session:
            runner = (
                RunnerClient(session=session)
                if strip is None
                else _StrippingRunnerClient(strip, session=session)
            )
            async with TestServer(_runner_app(gate)) as server:
                try:
                    evidence = await deliver(
                        work_items=work_items,
                        runner=runner,
                        run=_Run(admitted.request_id, epoch),  # type: ignore[arg-type]
                        base_url=str(server.make_url("")).rstrip("/"),
                        token=TOKEN,
                        remaining_s=30.0,
                    )
                except MemoryRulesContextRefused as refused:
                    return None, refused
                return evidence, None

    evidence, refused = asyncio.run(go())
    return evidence, refused, gate


def test_the_apis_snapshot_reaches_the_runner_gate_and_is_acknowledged_applied(
    api: TestClient, world: support.World
) -> None:
    admitted, epoch = _running(api, world, 579831)

    evidence, refused, gate = _deliver(api, world, admitted, epoch)

    assert refused is None
    assert evidence == "applied"
    assert gate.applied is not None
    assert gate.applied.text == TWO_TEXT
    assert gate.applied.rule_numbers == (1, 4)
    assert _evidence(admitted.request_id) == "applied"


def test_an_absent_pin_travels_the_same_chain_and_stays_none(
    api: TestClient, world: support.World
) -> None:
    admitted, epoch = _running(api, world, 579832, rules=False)

    evidence, refused, gate = _deliver(api, world, admitted, epoch)

    assert refused is None
    assert evidence == "none"
    assert gate.applied is None
    assert _evidence(admitted.request_id) == "none"


def test_an_envelope_without_tenant_id_is_refused_by_the_real_runner_gate(
    api: TestClient, world: support.World
) -> None:
    # The counterexample: the producer's tenant_id is lost on the way. The real
    # runner refuses it as another execution, the worker fails the request with
    # that reason, and the evidence is failed, never applied.
    admitted, epoch = _running(api, world, 579833)

    evidence, refused, gate = _deliver(api, world, admitted, epoch, strip="tenant_id")

    assert evidence is None
    assert refused is not None
    assert refused.reason == "wrong_execution"
    assert gate.applied is None
    assert _evidence(admitted.request_id) == "failed"
    # Liveness: the same chain with the field intact is applied.
    live, live_epoch = _running(api, world, 579834, agent=_agent_of(admitted.work_item_id))
    control, control_refused, _ = _deliver(api, world, live, live_epoch)
    assert (control, control_refused) == ("applied", None)
