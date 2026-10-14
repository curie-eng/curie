"""File-driven source boot against owned backends, not installation qualification."""

import asyncio
import dataclasses
import time

import pytest
from sqlalchemy import text

from .protected_configuration_vectors import canonical
from .test_protected_primary_runtime import database, external_reply
from .test_protected_primary_runtime import primary_postgres as primary_postgres
from .test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


@pytest.mark.parametrize("mode", ["scope", "cancel-serve"])
def test_actual_file_driven_boot_and_owned_runtime_shutdown(
    owned_worker, tmp_path, primary_postgres, monkeypatch, mode,
):
    async def go():
        from curie_worker import protected_entry

        assert hasattr(protected_entry, "open_runtime"), "file-driven runtime boot absent"
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            server, client, external, delivery, unused, request,
        ):
            engine, schema = await database(primary_postgres, client)
            try:
                async with external_reply() as (reply, calls):
                    credential = owned_worker.credential()
                    for name, raw in {
                        "manifest.json": client._manifest.canonical_bytes,
                        "execution_config.json": client._execution.canonical_bytes,
                        "worker.json": canonical({
                            "schema_version": 1,
                            "credential_ref": (
                                client._manifest.as_dict()["credential_refs"]["worker"]
                            ),
                            "worker": {
                                "username": credential.username, "password": credential.password,
                            },
                        }),
                        "ca.pem": owned_worker.broker.ca_pem.encode("ascii"),
                        "process.json": canonical({
                            "schema_version": 1, "database_url": primary_postgres,
                            "db_schema": schema, "api_base_url": "https://api.example.com",
                            "internal_worker_token": "synthetic-worker-token",
                            "adapter_credentials": {},
                            "slack": {
                                "token": "synthetic-slack-token",
                                "base_url": reply._adapters["slack"]._default_base_url,
                                "identity_tokens": {"adapter/example": "synthetic-slack-token"},
                                "trusted_origins": [],
                            },
                            "launch_tls": dataclasses.asdict(client._tls),
                        }),
                    }.items():
                        (tmp_path / name).write_bytes(raw)
                    if mode == "cancel-serve":
                        from curie_worker.protected_run import ProtectedRuntime

                        observed = []
                        real_run = ProtectedRuntime.run

                        async def observe_run(runtime):
                            observed.append(runtime)
                            return await real_run(runtime)

                        monkeypatch.setattr(ProtectedRuntime, "run", observe_run)
                        prior_tasks = set(asyncio.all_tasks())
                        serve = asyncio.create_task(protected_entry._serve(str(tmp_path)))
                        owned_tasks = set()
                        try:
                            async with asyncio.timeout(5):
                                while True:
                                    owned_tasks = {
                                        task for task in asyncio.all_tasks()
                                        if task not in prior_tasks
                                        and task.get_name().startswith("consumer:")
                                    }
                                    if any(
                                        task.get_name() == "consumer:read" for task in owned_tasks
                                    ):
                                        break
                                    if serve.done():
                                        await serve
                                    await asyncio.sleep(.01)
                            assert len(observed) == 1
                            external.finish.set()
                            serve.cancel()
                            with pytest.raises(asyncio.CancelledError):
                                await asyncio.wait_for(serve, 12)
                            assert all(task.done() for task in owned_tasks), (
                                "entry closed its resources before joining owned consumer tasks"
                            )
                        finally:
                            # Failed source assertions must still join only this fixture's tasks.
                            if observed:
                                owned_tasks.update(
                                    observed[0].consumer._generation_inflight_tasks()
                                )
                            for task in owned_tasks | {serve}:
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(*owned_tasks, serve, return_exceptions=True)
                        return
                    async with protected_entry.open_runtime(str(tmp_path)) as runtime:
                        assert runtime.consumer._client is not client._broker
                        external.finish.set()
                        task = asyncio.create_task(runtime.run())
                        try:
                            deadline = time.monotonic() + 12
                            while time.monotonic() < deadline:
                                if task.done():
                                    await task
                                owed = await runtime.markers.restore_owed(delivery.turn.event_id)
                                if (
                                    owed is not None and owed.resolution is not None
                                    and owed.resolution.as_dict()["resolution"] == "emitted"
                                ):
                                    if (await runtime.consumer._client.lane().runs.xpending(
                                        runtime.consumer._config.stream,
                                        runtime.consumer._config.consumer_group,
                                    ))["pending"] == 0:
                                        break
                                await asyncio.sleep(.05)
                            else:
                                raise AssertionError("file-driven runtime did not converge")
                            assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                            assert calls
                        finally:
                            runtime.request_stop()
                            await asyncio.wait_for(task, 5)
            finally:
                async with engine.begin() as connection:
                    await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                await engine.dispose()

    asyncio.run(go())
