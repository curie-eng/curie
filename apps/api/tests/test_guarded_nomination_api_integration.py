"""Actual hook intake and guarded runtime to nomination API; source mechanics only."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
from uuid import UUID, uuid4

import pytest
from _guarded_nomination_harness import guarded_pair, inputs, wb
from _protected_ingress_harness import (
    _broker,
    deliver,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401
    install,
    private_entries,
    provision,
    seed_row,
    signed,
)
from curie_api.config import get_settings
from curie_api.models import AgentChannel, AgentVersion
from curie_worker.protected_nomination import NominationCapturingRunner, NominationSubmissionError
from curie_worker.protected_runner import ProtectedRunnerClient
from sqlalchemy import insert, update
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine
from test_hook_source_support import (
    scoped_secret,
    support_db,  # noqa: F401
)
from test_protected_nomination_capture_integration import listening_api
from test_remediation_nomination_routes import (
    CHANNEL,
    OPERATOR,
    ROUTE_NAME,
    Stage,
    block,
    entry,
    remediation,
    rows,
    side_effect_counts,
)

from apps.worker.tests.protected_configuration_vectors import execution_profile
from apps.worker.tests.test_protected_primary_runtime import external_reply

admission_service = _broker.admission_service
owned_worker = wb.owned_worker


@pytest.fixture(scope="module")
def pworker_service(admission_service):
    return admission_service


def test_shared_fixture_can_provision_without_admitting_an_example_event():
    parameters = inspect.signature(inputs).parameters
    assert "profile_override" in parameters and "admit" in parameters


@pytest.mark.usefixtures("support_db")
@pytest.mark.parametrize("mode", ["current", "stale"])
def test_hook_guarded_runtime_capture_reaches_actual_nomination_api(
    ingress_broker, owned_worker, tmp_path, monkeypatch, mode
):
    assert owned_worker.broker is ingress_broker
    remediation(monkeypatch, enabled=True)

    async def scenario():
        async with ingress_app(tmp_path, monkeypatch) as (app, api, agent, directory):
            engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
            consumer_task = None
            runtime = None
            try:
                async with external_reply() as (router, replies), listening_api(app) as api_origin:
                    text = block(entry())
                    output = "Public introduction\n" + text + "Public tail"
                    profile = execution_profile(agent_id=agent, version_id=str(uuid4()))
                    root = tmp_path / "worker"
                    root.mkdir()
                    async with guarded_pair(
                        owned_worker,
                        root,
                        profile,
                        engine,
                        router,
                        api_origin,
                        get_settings().internal_worker_token,
                        output,
                    ) as (server, client, external, runtime, start_consumer):
                        manifest = client._manifest.as_dict()
                        rt = provision(ingress_broker, agent, directory)
                        rt.manifest = manifest
                        rt.row.update(
                            runtime_id=manifest["runtime_id"],
                            qualification_id=manifest["qualification_id"],
                            bundle_digest=manifest["bundle_digest"]["sha256"],
                        )
                        rt.rebuild()
                        rt.qualification = client._qualification.as_dict()
                        rt.readiness = json.loads(
                            ingress_broker.command(
                                "GET",
                                f"protected:control:readiness:{manifest['runtime_id']}:{manifest['runtime_generation']}",
                            )
                        )
                        await asyncio.to_thread(seed_row, agent, rt)
                        install(ingress_broker, rt)
                        async with engine.begin() as connection:
                            await connection.execute(
                                update(AgentChannel)
                                .where(AgentChannel.agent_id == UUID(agent))
                                .values(
                                    kind="slack",
                                    address="channel/example",
                                    adapter="adapter/example",
                                    endpoint=None,
                                )
                            )
                            await connection.execute(
                                insert(AgentVersion).values(
                                    id=UUID(profile["version_id"]),
                                    agent_id=UUID(agent),
                                    version_label="source-v1",
                                    created_by="source-test",
                                    bundle_ref=manifest["bundle_digest"]["object_identity"],
                                    bundle_sha256=manifest["bundle_digest"]["sha256"],
                                )
                            )
                        patched = await api.patch(
                            f"/agents/{agent}",
                            json={
                                "approval_routes": {
                                    ROUTE_NAME: {
                                        "resolution": {"kind": "slack", "address": CHANNEL},
                                        "approvers": {"users": [OPERATOR]},
                                    }
                                }
                            },
                            headers={"X-API-Key": get_settings().api_key},
                        )
                        assert patched.status_code == 200, patched.text
                        stage = Stage(api, agent, ingress_broker)
                        generation = await stage.bind()
                        assert private_entries(ingress_broker) == []
                        accepted = await deliver(
                            api, agent, signed(scoped_secret(agent), delivery="guarded-nomination")
                        )
                        assert (
                            accepted.status_code == 200
                            and accepted.json()["acceptance_status"] == "accepted"
                        )
                        event = accepted.json()["event_id"]
                        assert event.startswith("hook-" + agent + "-")
                        assert len(private_entries(ingress_broker)) == 1
                        if mode == "stale":
                            assert await stage.bind() > generation
                        assert type(runtime.runner) is NominationCapturingRunner
                        assert type(runtime.runner._runner) is ProtectedRunnerClient
                        before_effects = await side_effect_counts()
                        external.finish.set()
                        consumer_task = start_consumer()
                        async with asyncio.timeout(20):
                            while True:
                                stored = await rows(event)
                                owed = await runtime.markers.restore_owed(event)
                                if stored and owed is not None and owed.resolution is not None:
                                    pending = await client._broker.lane().runs.xpending(
                                        runtime.consumer._config.stream,
                                        runtime.consumer._config.consumer_group,
                                    )
                                    if pending["pending"] == 0:
                                        break
                                if consumer_task.done():
                                    await consumer_task
                                    raise AssertionError("actual consumer ended before completion")
                                await asyncio.sleep(0.05)
                        assert len(stored) == 1 and str(stored[0]["agent_id"]) == agent
                        assert int(stored[0]["admitted_generation"]) == generation
                        assert (
                            stored[0]["state"] == "approval_requested"
                            and stored[0]["execution_id"] is None
                        )
                        if mode == "stale":
                            assert int(stored[0]["current_generation"]) > generation
                            async with engine.connect() as connection:
                                reason = await connection.scalar(
                                    sql_text(
                                        "SELECT approval_reason FROM curie.remediation_nominations "
                                        "WHERE event_id=:event"
                                    ),
                                    {"event": event},
                                )
                            assert reason == "generation_not_current"
                        actual_effects = await side_effect_counts()
                        assert actual_effects["executions"] == before_effects["executions"]
                        assert actual_effects["approvals"] == before_effects["approvals"] + 1
                        assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                        assert replies and all(
                            "curie-remediation" not in str(body) for _, body in replies
                        )
                        assert any("Public introduction" in str(body) for _, body in replies)
                        snapshots = tuple(client._owned.values())
                        assert len(snapshots) == 1
                        launch = server._registry.get(
                            "worker/example", snapshots[0].capability.launch_id
                        )
                        assert launch.state == "retired" and launch.termination is not None
                        assert owed.resolution.as_dict()["resolution"] == "emitted"
                        counts = await side_effect_counts()
                        before = copy.deepcopy(stored)
                        submitter = runtime.runner._submitter
                        await submitter.submit(event_id=event, block=text)
                        assert await rows(event) == before and await side_effect_counts() == counts
                        with pytest.raises(NominationSubmissionError):
                            await submitter.submit(event_id=event, block=text + " ")
                        assert await rows(event) == before and await side_effect_counts() == counts
                        runtime.request_stop()
                        await asyncio.wait_for(consumer_task, 5)
            finally:
                await engine.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 60))
