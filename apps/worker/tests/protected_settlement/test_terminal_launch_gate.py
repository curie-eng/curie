"""The real HTTPS mediator independently fences permanent terminal evidence."""

from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_worker.kernel.core import Kernel
from curie_worker.protected_launch_records import canonical_json

from ..test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


async def ready_launch(client, request):
    status, raw = await client._transport.control(
        "POST", "/v1/launches", canonical_json(request), {}, 5
    )
    assert status == 202
    pending = json.loads(raw)
    path = "/v1/launches/" + pending["launch_id"]
    headers = {"X-Curie-Launch-Capability": pending["control_capability"]}
    for _ in range(20):
        _, raw = await client._transport.control("GET", path, None, headers, 5)
        observation = json.loads(raw)
        if observation["state"] == "idle":
            return path, headers, observation
        await asyncio.sleep(0.05)
    pytest.fail("The actual launch never became idle")


@pytest.mark.parametrize("marker", ["malformed", "wrong-type"])
@pytest.mark.parametrize("phase", ["create", "start", "admit", "start-status", "admit-status"])
def test_terminal_evidence_blocks_positive_http_without_disabling_retirement(
    owned_worker, tmp_path, phase, marker
):
    async def go():
        async with actual_pair(owned_worker, tmp_path) as (
            server, client, external, delivery, _, request
        ):
            key = (
                derive_lane_namespace(client._manifest).prefix
                + ":settlement:"
                + delivery.turn.event_id
            )

            def settle():
                if marker == "malformed":
                    owned_worker.broker.command("SET", key, "invalid-terminal-evidence")
                else:
                    owned_worker.broker.command("LPUSH", key, "invalid-terminal-evidence")

            if phase == "create":
                settle()
                response = await client._transport.request(
                    "POST", "/v1/launches", canonical_json(request), {}, 5
                )
                try:
                    assert response.status == 503
                    assert not external.objects
                    assert not any(method == "RUNNER_POST" for method, _ in external.events)
                finally:
                    response.close()
                return

            path, headers, observation = await ready_launch(client, request)
            start = {
                "schema_version": 1,
                "operation_id": str(uuid4()),
                "expected_receipt_revision": observation["receipt_revision"],
                "capacity_admission": True,
                "event": Kernel._to_event(delivery.turn).model_dump(mode="json"),
            }
            streamed = None
            try:
                if phase.startswith("admit"):
                    streamed = await client._transport.request(
                        "POST", path + "/start", canonical_json(start), headers, 5,
                        stream_budget=10,
                    )
                    assert streamed.status == 200
                    epoch = streamed.headers["X-Curie-Turn-Epoch"]
                    _, raw = await client._transport.control("GET", path, None, headers, 5)
                    observation = json.loads(raw)
                    body = {
                        "schema_version": 1,
                        "operation_id": str(uuid4()),
                        "expected_receipt_revision": observation["receipt_revision"],
                        "epoch": epoch,
                        "allow": True,
                    }
                    operation = "/admit"
                else:
                    body, operation = start, "/start"
                if phase.endswith("status"):
                    # Only the external runner is simulated. Its status callback
                    # writes real broker evidence after the first positive check.
                    external.status_hook = settle
                else:
                    settle()
                response = await client._transport.request(
                    "POST", path + operation, canonical_json(body), headers, 5
                )
                try:
                    assert response.status == 503
                finally:
                    response.close()
                if phase.startswith("admit"):
                    assert ("RUNNER_POST", "/v1/turn-admit") not in external.events
                    # Negative admission and owned stop remain available.
                    status, _ = await client._transport.control(
                        "POST", path + "/admit", canonical_json(dict(body, allow=False)), headers, 5
                    )
                    assert status == 200 and external.decision == "denied"
                else:
                    assert not any(method == "RUNNER_POST" for method, _ in external.events)
                status, raw = await client._transport.control(
                    "POST", path + "/retire",
                    canonical_json({"schema_version": 1, "operation_id": str(uuid4())}), headers, 5
                )
                assert status == 200 and json.loads(raw)["state"] == "retired"
                assert not external.objects
            finally:
                if streamed is not None:
                    streamed.close()

    asyncio.run(go())


def test_unavailable_terminal_check_refuses_start_but_keeps_owned_stop(owned_worker, tmp_path):
    async def go():
        async with actual_pair(owned_worker, tmp_path) as (
            server, client, external, delivery, _, request
        ):
            path, headers, observation = await ready_launch(client, request)
            start = {
                "schema_version": 1,
                "operation_id": str(uuid4()),
                "expected_receipt_revision": observation["receipt_revision"],
                "capacity_admission": True,
                "event": Kernel._to_event(delivery.turn).model_dump(mode="json"),
            }
            # Pause only the broker container recorded by this module's fixture,
            # after retained admission and workload observations have succeeded.
            external.status_hook = lambda: owned_worker.broker.docker(
                "pause", owned_worker.broker.cid
            )
            try:
                response = await client._transport.request(
                    "POST", path + "/start", canonical_json(start), headers, 5
                )
                try:
                    assert response.status == 503
                    assert not any(method == "RUNNER_POST" for method, _ in external.events)
                finally:
                    response.close()
                status, raw = await client._transport.control(
                    "POST", path + "/retire",
                    canonical_json({"schema_version": 1, "operation_id": str(uuid4())}), headers, 5
                )
                assert status == 200 and json.loads(raw)["state"] == "retired"
                assert not external.objects
            finally:
                owned_worker.broker.docker("unpause", owned_worker.broker.cid)

    asyncio.run(go())
