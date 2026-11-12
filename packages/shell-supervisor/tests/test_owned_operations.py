"""A control client that disconnects never abandons a create or a retire.

The supervisor runs in process behind a real aiohttp listener with handler
cancellation on, as ``__main__`` serves it, so a dropped connection cancels
the request handler exactly as it would in a shell. A barrier holds staging
open while the client goes away.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from curie_shell_supervisor import stage
from curie_shell_supervisor.child import RunnerChild
from curie_shell_supervisor.config import SupervisorConfig
from curie_shell_supervisor.protocol import STATE_IDLE, STATE_QUARANTINED, claim_path
from curie_shell_supervisor.server import Supervisor
from curie_test_support.static_shell import CONTROL_TOKEN, claim_archive, free_port, wait_gone

BEARER = "bearer-" + "d" * 40


def _config(tmp_path: Path) -> SupervisorConfig:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    return SupervisorConfig.from_env(
        {
            "CURIE_SUPERVISOR_SLOT": "slot-a",
            "CURIE_SUPERVISOR_UNISOLATED_FOR_TESTS": "1",
            "CURIE_SUPERVISOR_CONTROL_TOKEN": CONTROL_TOKEN,
            "CURIE_SUPERVISOR_TLS_CERT_FILE": "unused.pem",
            "CURIE_SUPERVISOR_TLS_KEY_FILE": "unused.key",
            "CURIE_SUPERVISOR_STATE_ROOT": str(tmp_path / "state"),
            "CURIE_SUPERVISOR_CHILD_PORT": str(free_port()),
            "CURIE_SUPERVISOR_WORKSPACE_PATH": str(tmp_path / "links" / "workspace"),
            "CURIE_SUPERVISOR_ATTACHMENTS_PATH": str(tmp_path / "links" / "attachments"),
            "CURIE_SUPERVISOR_SCRUB_PATHS": str(scratch),
            "CURIE_SUPERVISOR_RUNNER_COMMAND": (
                f"{sys.executable} -m curie_test_support.fake_shell_runner"
            ),
            "CURIE_SUPERVISOR_SETTLE_TIMEOUT_SECONDS": "3",
            "CURIE_SUPERVISOR_STOP_GRACE_SECONDS": "3",
        }
    )


async def _drop_after_upload(port: int, path: str, body: bytes, entered: threading.Event) -> None:
    """Send a whole create, then hang up while the supervisor is staging it."""

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    head = (
        f"PUT {path} HTTP/1.1\r\nHost: shell\r\nAuthorization: Bearer {CONTROL_TOKEN}\r\n"
        f"Content-Type: application/x-tar\r\nContent-Length: {len(body)}\r\n\r\n"
    )
    writer.write(head.encode() + body)
    await writer.drain()
    assert await asyncio.to_thread(entered.wait, 10), "staging never started"
    writer.close()
    await writer.wait_closed()
    del reader


def test_a_create_whose_client_disconnects_is_ended_and_quarantines_the_shell(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = tmp_path / "records"
    records.mkdir()
    config = _config(tmp_path)
    entered, release = threading.Event(), threading.Event()
    real_prepare = stage.prepare

    def held_prepare(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        assert release.wait(10), "the test never released staging"
        return real_prepare(*args, **kwargs)

    monkeypatch.setattr(stage, "prepare", held_prepare)
    spawned: list[RunnerChild] = []
    real_spawn = RunnerChild.spawn

    async def recorded_spawn(*args: Any, **kwargs: Any) -> RunnerChild:
        child = await real_spawn(*args, **kwargs)
        spawned.append(child)
        return child

    monkeypatch.setattr(RunnerChild, "spawn", recorded_spawn)
    body = claim_archive(
        "claim-a",
        1,
        {"CURIE_RUNNER_TOKEN": BEARER, "FAKE_RECORD_DIR": str(records)},
    )

    async def scenario() -> None:
        supervisor = Supervisor(config)
        runner = web.AppRunner(supervisor.app(), handler_cancellation=True)
        await runner.setup()
        port = free_port()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        try:
            await _drop_after_upload(port, claim_path("claim-a", 1), body, entered)
            # The hang-up is seen while staging is still held: the create and
            # its follow-up are both supervisor tasks now.
            deadline = asyncio.get_running_loop().time() + 10
            while len(supervisor._operations) < 2:
                assert asyncio.get_running_loop().time() < deadline, "the hang-up was ignored"
                await asyncio.sleep(0)
            release.set()
            while supervisor._operations:
                await asyncio.gather(*list(supervisor._operations), return_exceptions=True)
            # The create finished, then its unacknowledged claim was ended.
            status = supervisor.status()
            assert status["state"] == STATE_QUARANTINED
            assert status["quarantine_reason"] == "create-unacknowledged"
            assert status["claim"] is None
            assert status["last"]["outcome"] == "quarantined"
            # The runner child was started and is gone, process group and all.
            assert [child.returncode is not None for child in spawned] == [True]
            assert wait_gone(spawned[0].pid, 10)
            assert list((config.state_root / "claims").iterdir()) == []
            # A retry of the same body is never told the claim runs.
            retry = tmp_path / "retry.tar"
            retry.write_bytes(body)
            digest = hashlib.sha256(body).hexdigest()
            assert (await supervisor.create("claim-a", 1, retry, digest))[0] == 409
            code, answer = await supervisor.retire("claim-a", 1, "retire")
            assert (code, answer["outcome"]) == (200, "quarantined")
            assert supervisor.status()["state"] == STATE_QUARANTINED
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_a_client_that_disconnects_during_retire_still_gets_a_clean_shell(
    tmp_path: Path,
) -> None:
    records = tmp_path / "records"
    records.mkdir()
    config = _config(tmp_path)
    archive = tmp_path / "claim.tar"
    archive.write_bytes(
        claim_archive(
            "claim-a",
            1,
            {"CURIE_RUNNER_TOKEN": BEARER, "FAKE_RECORD_DIR": str(records)},
        )
    )

    async def scenario() -> None:
        supervisor = Supervisor(config)
        runner = web.AppRunner(supervisor.app(), handler_cancellation=True)
        await runner.setup()
        port = free_port()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        try:
            assert (await supervisor.create("claim-a", 1, archive, "digest"))[0] == 201
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(
                (
                    f"DELETE {claim_path('claim-a', 1)}?mode=retire HTTP/1.1\r\nHost: shell\r\n"
                    f"Authorization: Bearer {CONTROL_TOKEN}\r\n\r\n"
                ).encode()
            )
            await writer.drain()
            while not supervisor._operations:
                await asyncio.sleep(0)
            writer.close()
            await writer.wait_closed()
            del reader
            await asyncio.gather(*supervisor._operations)
            assert supervisor.status()["state"] == STATE_IDLE
            assert supervisor.status()["last"]["outcome"] == "cleaned"
            assert list((config.state_root / "claims").iterdir()) == []
        finally:
            await runner.cleanup()

    asyncio.run(scenario())
