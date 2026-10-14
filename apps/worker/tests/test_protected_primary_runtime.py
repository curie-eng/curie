"""Real SQL and protected consumer composition; external endpoint fakes only."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from aiohttp import web
from curie_api import models
from curie_api.db import SCHEMA, Base
from curie_protected_hooks.admission_records import DeliveryIdentity
from curie_protected_hooks.atomic_admission import AtomicAdmission
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_worker.protected_nomination import HttpNominationSubmitter, NominationCapturingRunner
from curie_worker.protected_run import ProtectedRuntime
from curie_worker.reply_sink import HttpReplyAdapter, ReplySinkRouter
from curie_worker.slack_sink import SlackReplyAdapter
from redis.exceptions import ResponseError
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import create_async_engine

from .test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


@pytest.fixture(scope="module")
def primary_postgres(tmp_path_factory):
    """One exact-owned capped Postgres; no compose or production access."""
    if os.environ.get("CURIE_PRIMARY_SOURCE_RESOURCES") != "1":
        pytest.fail("explicit primary resource campaign authorization required")
    owner = "curie-check-primary-pg-" + uuid4().hex[:12]
    private = tmp_path_factory.mktemp("primary-pg")
    cidfile = private / "container.cid"
    image = next(
        line.split("image:", 1)[1].strip()
        for line in (Path(__file__).resolve().parents[3] / "compose.dev.yaml")
        .read_text()
        .splitlines()
        if "image: postgres:" in line
    )

    def docker(*args, check=True):
        result = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=30
        )
        if check and result.returncode:
            pytest.fail("owned primary Postgres Docker operation failed", pytrace=False)
        return result

    cid = None
    try:
        docker(
            "run",
            "-d",
            "--memory",
            "256m",
            "--cpus",
            ".25",
            "--cidfile",
            str(cidfile),
            "--name",
            owner,
            "--label",
            "curie.test.owner=" + owner,
            "-p",
            "127.0.0.1::5432",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            image,
            "-c",
            "shared_buffers=32MB",
            "-c",
            "max_connections=20",
        )
        cid = cidfile.read_text().strip()
        info = json.loads(docker("inspect", cid).stdout)[0]
        assert info["HostConfig"]["Memory"] == 268435456
        assert info["HostConfig"]["NanoCpus"] == 250000000
        record_path = os.environ.get("CURIE_PRIMARY_RESOURCE_RECORD")
        if record_path:
            with Path(record_path).open("a") as record:
                record.write(
                    json.dumps(
                        {
                            "cid": cid,
                            "owner": owner,
                            "memory": info["HostConfig"]["Memory"],
                            "cpus": info["HostConfig"]["NanoCpus"],
                        }
                    )
                    + "\n"
                )
        mapping = docker("port", cid, "5432/tcp").stdout.strip()
        assert mapping.startswith("127.0.0.1:")
        url = "postgresql+asyncpg://postgres@" + mapping + "/postgres"
        ready = False
        for _ in range(80):
            # The pinned image initializes through a socket-only temporary server.
            # Wait for TCP, which is the transport used by the actual host consumer.
            if docker(
                "exec", cid, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False
            ).returncode == 0:
                ready = True
                break
            time.sleep(0.1)
        assert ready, "owned primary Postgres failed readiness"
        yield url
    finally:
        if cidfile.exists():
            cid = cidfile.read_text().strip()
            info = json.loads(docker("inspect", cid).stdout)[0]
            assert info["Id"] == cid and info["Config"]["Labels"]["curie.test.owner"] == owner
            docker("rm", "-f", cid)
            assert cid not in docker("ps", "-aq", "--no-trunc").stdout.splitlines()
            cidfile.unlink()


@contextlib.asynccontextmanager
async def external_reply():
    calls = []

    async def reply(request):
        assert request.headers.get("Authorization") == "Bearer synthetic-slack-token"
        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            body = dict(await request.post())
        calls.append((request.path, body))
        return web.json_response(
            {"ok": True, "ts": "message/example", "channel": "channel/example"}
        )

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", reply)
    owner = web.AppRunner(app, access_log=None)
    await owner.setup()
    try:
        site = web.TCPSite(owner, "127.0.0.1", 0)
        await site.start()
        base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1]) + "/api/"
        slack = SlackReplyAdapter(
            "synthetic-slack-token",
            identity_tokens={"adapter/example": "synthetic-slack-token"},
            base_url=base,
        )
        router = ReplySinkRouter(adapters={"slack": slack}, default=HttpReplyAdapter({}))
        yield router, calls
    finally:
        await owner.cleanup()


async def database(url, client):
    schema = "primary_" + uuid4().hex
    engine = create_async_engine(url, execution_options={"schema_translate_map": {SCHEMA: schema}})
    needed = set()

    def include(table):
        if table in needed:
            return
        needed.add(table)
        for fk in table.foreign_keys:
            include(fk.column.table)

    for cls in (models.Agent, models.AgentChannel, models.AgentVersion):
        include(cls.__table__)
    profile = client._execution.as_dict()
    async with engine.begin() as connection:
        await connection.execute(text("CREATE SCHEMA " + schema))
        await connection.run_sync(lambda conn: Base.metadata.create_all(conn, tables=list(needed)))
        await connection.execute(
            insert(models.Tenant).values(id=models.DEFAULT_TENANT_ID, deployment_id="acme-source")
        )
        await connection.execute(
            insert(models.Agent).values(id=UUID(profile["agent_id"]), name="acme-bot")
        )
        await connection.execute(
            insert(models.AgentVersion).values(
                id=UUID(profile["version_id"]),
                agent_id=UUID(profile["agent_id"]),
                version_label="source-v1",
                created_by="source-test",
                bundle_ref=client._manifest.as_dict()["bundle_digest"]["object_identity"],
                bundle_sha256=client._manifest.as_dict()["bundle_digest"]["sha256"],
            )
        )
        await connection.execute(
            insert(models.AgentChannel).values(
                id=uuid4(),
                agent_id=UUID(profile["agent_id"]),
                kind="slack",
                address="channel/example",
                adapter="adapter/example",
            )
        )
    return engine, schema


@pytest.mark.parametrize("mode", ["primary", "extra-fields", "missing-proof"])
def test_actual_primary_consumer_kernel_sql_binding_and_ownership(
    owned_worker, tmp_path, primary_postgres, mode
):
    async def go():
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            server,
            client,
            external,
            delivery,
            lease,
            request,
        ):
            assert lease is None
            engine, schema = await database(primary_postgres, client)
            runtime = None
            task = None
            try:
                async with external_reply() as (router, replies):
                    ns = derive_lane_namespace(client._manifest)
                    cfg = wb.lane_config(
                        owned_worker.broker,
                        key_prefix=ns.prefix,
                        consumer_group=ns.group,
                        dead_letter_stream=ns.dead,
                        max_attempts=1,
                        delivery_lease_ttl_s=15,
                        delivery_lease_heartbeat_s=5,
                    )
                    runtime = ProtectedRuntime(
                        broker=client._broker,
                        manifest=client._manifest,
                        engine=engine,
                        db_schema=schema,
                        execution=client._execution,
                        launch_client=client,
                        config=cfg,
                        reply_sink=router,
                        nomination_submitter=HttpNominationSubmitter(
                            "http://127.0.0.1:1", "synthetic-worker-token"
                        ),
                    )
                    assert type(runtime.runner) is NominationCapturingRunner
                    runs = client._broker.lane().runs
                    if mode == "extra-fields":
                        # Fresh source row is a closed-adapter unsupported recovery form.
                        owned_worker.broker.command("XTRIM", cfg.stream, "MAXLEN", "=", 0)
                        fields = {**request["source_fields"], "unexpected": "derived-unknown"}
                        await runs.xadd(cfg.stream, fields)
                    elif mode == "missing-proof":
                        owned_worker.broker.command(
                            "DEL", "protected:admission:binding:" + delivery.turn.event_id
                        )
                    external.finish.set()
                    ack_before = owned_worker.broker.command("INFO", "commandstats").get(
                        "cmdstat_xack", {}
                    ).get("calls", 0)
                    task = asyncio.create_task(runtime.run())
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        if (
                            mode == "primary"
                            and await runtime.markers.restore_owed(delivery.turn.event_id)
                            is not None
                        ):
                            pending = await runs.xpending(cfg.stream, cfg.consumer_group)
                            if pending["pending"] == 0:
                                break
                        elif mode != "primary":
                            try:
                                pending = await runs.xpending(cfg.stream, cfg.consumer_group)
                            except ResponseError:
                                pending = {"pending": 0}
                            if pending["pending"] > 0:
                                await asyncio.sleep(0.15)
                                break
                        if task.done():
                            await task
                        await asyncio.sleep(0.05)
                    assert time.monotonic() < deadline, "primary source did not converge"
                    if mode == "primary":
                        assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                        ack_after = owned_worker.broker.command("INFO", "commandstats")[
                            "cmdstat_xack"
                        ]["calls"]
                        assert ack_after - ack_before == 1
                        assert any(
                            "synthetic reply" in str(body.get("text", "")) for _, body in replies
                        )
                        owed = await runtime.markers.restore_owed(delivery.turn.event_id)
                        assert owed is not None and owed.resolution is not None
                        assert owed.resolution.as_dict()["resolution"] == "emitted"
                        assert not await runs.exists(cfg.completion_key(delivery.turn.event_id))
                        assert runtime.affinity is client._broker.lane().affinity
                        assert runtime.lock._redis is runs and runtime.pressure_lock._redis is runs
                        assert runtime.consumer._drain._redis is runs
                        owner = AtomicAdmission(
                            owned_worker.broker.admin,
                            trusted_manifest=client._manifest,
                            trusted_max_readiness_ms=60000,
                            backlog_limit=64,
                        )
                        identity = DeliveryIdentity(**delivery.intent.as_dict()["identity"])
                        assert owner.release_accepted(identity) == "released"
                        record = os.environ.get("CURIE_PRIMARY_RESOURCE_RECORD")
                        if record:
                            counts = {}
                            for path, _ in replies:
                                counts[path] = counts.get(path, 0) + 1
                            with Path(record).open("a") as output:
                                output.write(json.dumps({
                                    "primary_runner_posts": 1,
                                    "primary_xack_commands": ack_after - ack_before,
                                    "external_reply_requests": counts,
                                }) + "\n")
                    else:
                        assert not external.events and not replies
                        assert (
                            await runtime.markers._raw(
                                runtime.markers.settlement_key(delivery.turn.event_id)
                            )
                            is None
                        )
                        assert pending["pending"] == 1
            finally:
                if runtime is not None:
                    runtime.request_stop()
                if task is not None:
                    await asyncio.wait_for(task, 10)
                if runtime is not None:
                    await runtime.close()
                async with engine.begin() as connection:
                    await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                await engine.dispose()

    asyncio.run(go())
