"""Actual capture-to-API plumbing; not primary guarded-runner qualification.

@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7.
Only the external runner stream is synthetic. API HTTP, submission transport,
protected hook admission/binding verification and both stores are real.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import socket
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn
from _protected_ingress_harness import private_entries
from aci_protocol import Event, Final, TextDelta
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_api.config import get_settings
from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient, WorkerCredential
from curie_protected_hooks.worker_verification import (
    Dispatchable,
    verification_acl_rules,
    verify_delivery,
)
from curie_worker.protected_binding import protected_delivery_scope
from curie_worker.protected_nomination import (
    HttpNominationSubmitter,
    NominationCapturingRunner,
    NominationSubmissionError,
)
from curie_worker.runner_client import RunnerClient
from test_remediation_nomination_routes import (
    _broker,
    block,
    entry,
    ingress_broker_fixture,  # noqa: F401
    remediation,
    rows,
    side_effect_counts,
    staged,
    support_db,  # noqa: F401
)

admission_service = _broker.admission_service
pytestmark = pytest.mark.usefixtures("support_db")


@asynccontextmanager
async def listening_api(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    origin = "http://127.0.0.1:" + str(listener.getsockname()[1])
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_config=None, access_log=False))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    raise AssertionError("actual API listener did not start")
                await asyncio.sleep(0.01)
        yield origin
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        listener.close()


async def verified_delivery(broker):
    raw = private_entries(broker)[-1][1]
    fields = {raw[i].decode(): raw[i + 1].decode() for i in range(0, len(raw), 2)}
    digest = json.loads(fields["protected_envelope"])["manifest_digest"]
    manifest = Manifest(broker.command("GET", "protected:control:manifest:" + digest))
    username, password = "nomination-worker-" + secrets.token_hex(6), secrets.token_hex(24)
    broker.command(
        "ACL", "SETUSER", username, "reset", "on", ">" + password, *verification_acl_rules("worker")
    )
    client = None
    try:
        client = await AuthenticatedWorkerClient.connect(
            manifest, WorkerCredential(username, password), broker.ca_pem
        )
        delivery = await verify_delivery(client, fields, trusted_manifest=manifest)
        assert type(delivery) is Dispatchable
        return delivery
    finally:
        if client is not None:
            await client.close()
        broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
        broker.command("ACL", "DELUSER", username)


@pytest.mark.parametrize("case", ["replay", "conflict", "stale", "missing-binding", "malformed"])
def test_actual_capture_submitter_api_and_persistence(ingress_broker, tmp_path, monkeypatch, case):
    remediation(monkeypatch, enabled=True)

    async def scenario():
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            admitted = await stage.bind()
            event_id = await stage.protected_event()
            delivery = await verified_delivery(ingress_broker)
            assert delivery.turn.event_id == event_id
            if case == "stale":
                assert await stage.bind() > admitted
            if case == "missing-binding":
                ingress_broker.command("DEL", "protected:admission:binding:" + event_id)
            captured = (
                block(entry()) if case != "malformed" else "```curie-remediation\n{bad json}\n```\n"
            )
            output = "Public introduction\n" + captured + "Public tail"

            async def runner_event(request):
                assert request.headers.get("Authorization") == "Bearer synthetic-runner-token"
                assert "X-Curie-Worker-Token" not in request.headers
                frames = [
                    TextDelta(text=output[:25]),
                    TextDelta(text=output[25:]),
                    Final(text=output),
                ]
                return web.Response(
                    text="".join(frame.model_dump_json() + "\n" for frame in frames),
                    headers={"X-Curie-Turn-Epoch": "e" * 32},
                )

            runner_app = web.Application()
            runner_app.add_routes([web.post("/v1/event", runner_event)])
            app = stage.client._transport.app
            before = await side_effect_counts()
            async with listening_api(app) as api_origin, TestServer(runner_app) as external:
                submitter = HttpNominationSubmitter(
                    api_origin, get_settings().internal_worker_token
                )
                async with RunnerClient() as raw_runner:
                    runner = NominationCapturingRunner(raw_runner, submitter)
                    async with protected_delivery_scope(delivery):
                        turn = await runner.start_turn(
                            str(external.make_url("")).rstrip("/"),
                            Event(
                                type="message", text="Synthetic alert", user="hook/example", ts="1"
                            ),
                            "synthetic-runner-token",
                        )
                    if case == "missing-binding":
                        with pytest.raises(NominationSubmissionError):
                            async with turn:
                                _ = [frame async for frame in turn]
                        assert await rows(event_id) == []
                        assert await side_effect_counts() == before
                        return
                    async with turn:
                        frames = [frame async for frame in turn]
                    assert frames[-1].text == "Public introduction\nPublic tail"
                    assert "curie-remediation" not in "".join(
                        frame.text for frame in frames if isinstance(frame, TextDelta)
                    )
                stored = await rows(event_id)
                assert len(stored) == 1 and str(stored[0]["agent_id"]) == stage.agent
                assert int(stored[0]["admitted_generation"]) == admitted
                if case == "malformed":
                    assert stored[0]["state"] == "refused"
                    assert stored[0]["refusal_code"] == "nomination_malformed"
                    assert await side_effect_counts() == before
                else:
                    assert stored[0]["state"] == "approval_requested"
                    assert stored[0]["execution_id"] is None
                if case == "stale":
                    assert int(stored[0]["current_generation"]) > admitted
                    from _migration_support import sql_dicts

                    reason = await asyncio.to_thread(
                        sql_dicts,
                        "SELECT approval_reason FROM curie.remediation_nominations "
                        "WHERE event_id=:event",
                        {"event": event_id},
                    )
                    assert reason == [{"approval_reason": "generation_not_current"}]
                if case in {"replay", "conflict"}:
                    counts = await side_effect_counts()
                    await submitter.submit(event_id=event_id, block=captured)
                    assert await rows(event_id) == stored
                    assert await side_effect_counts() == counts
                    async with httpx.AsyncClient(
                        base_url=api_origin, trust_env=False
                    ) as actual_http:
                        first = await actual_http.post(
                            "/v1/internal/remediation/nominations",
                            json={"event_id": event_id, "block": captured},
                            headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
                        )
                        assert first.status_code == 200
                        assert first.json()["nomination_ids"] == [str(stored[0]["id"])]
                    if case == "conflict":
                        with pytest.raises(NominationSubmissionError):
                            await submitter.submit(event_id=event_id, block=captured + " ")
                        assert await rows(event_id) == stored
                        assert await side_effect_counts() == counts

    asyncio.run(asyncio.wait_for(scenario(), 60))
