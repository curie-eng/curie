"""Static-pool qualification: real shell supervisors running the real runner.

Each shell is ``python -m curie_shell_supervisor`` in its own process, serving
TLS with a throwaway CA, and its child is the real ``python -m curie_runner``
on the fake model. The worker side is the real ``SandboxSubstrate``,
``StaticPoolSandboxClient`` and ``RunnerClient`` against real Valkey. Only the
state API is a stand-in: one aiohttp app holding transcript keys with the
real API's GET, append and compare-and-set shapes, so history survives the
recycling of a shell exactly as it would against the platform.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
import io
import json
import sys
import tarfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from aci_protocol import Event, SessionStatus
from aiohttp import web
from curie_test_support.static_shell import (
    CONTROL_TOKEN,
    ShellTls,
    SupervisorProcess,
    free_port,
    make_shell_tls,
    wait_until,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from curie_worker.runner_client import RunnerClient, RunnerError
from curie_worker.sandbox import (
    AffinityStore,
    SandboxSubstrate,
    StaticPoolSandboxClient,
    StaticShellSlot,
    SubstrateConfig,
    shell_ssl_context,
)
from curie_worker.sandbox.quota import capacity_rejection_is_valid
from curie_worker.sandbox.types import CapacityExhaustedError, SandboxHandle
from redis.asyncio import Redis as AsyncRedis

POOL = "curie-runner-pool"
BUNDLE_REF = "bundles/qualification.tar.gz"
RUNNER_COMMAND = f"{sys.executable} -m curie_runner"
_CAP = 65_536


def _bundle() -> bytes:
    manifest = {"name": "qualification", "description": "static pool", "version": "0.1.0"}
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        data = json.dumps(manifest).encode()
        info = tarfile.TarInfo(".claude-plugin/plugin.json")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return gzip.compress(raw.getvalue())


class _Bundles:
    def get(self, key: str) -> bytes:
        assert key == BUNDLE_REF
        return _bundle()


class _StateApi:
    """Transcript keys on a fake state API, served from a background thread."""

    def __init__(self) -> None:
        self.port = free_port()
        self.values: dict[str, list[dict[str, Any]]] = {}
        self.reads: dict[str, int] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._runner: web.AppRunner | None = None

    def key_url(self, thread: str) -> str:
        return f"http://127.0.0.1:{self.port}/agents/A/state/transcript/{thread}"

    def _entry(self, key: str) -> dict[str, Any]:
        value = self.values[key]
        return {"namespace": "transcript", "key": key, "value": list(value), "version": len(value)}

    def _app(self) -> web.Application:
        app = web.Application()
        cap = {"X-Curie-Transcript-Max-Bytes": str(_CAP)}

        async def get_key(request: web.Request) -> web.Response:
            key = request.match_info["key"]
            self.reads[key] = self.reads.get(key, 0) + 1
            if key not in self.values:
                return web.json_response({"detail": "not found"}, status=404, headers=cap)
            return web.json_response(self._entry(key), headers=cap)

        async def append_key(request: web.Request) -> web.Response:
            key = request.match_info["key"]
            body = await request.json()
            self.values.setdefault(key, []).append(body["item"])
            return web.json_response(self._entry(key))

        async def put_key(request: web.Request) -> web.Response:
            key = request.match_info["key"]
            body = await request.json()
            expected = body.get("expected_version")
            if expected is not None and expected != len(self.values.get(key, [])):
                return web.json_response({"detail": "version mismatch"}, status=409)
            self.values[key] = list(body["value"])
            return web.json_response(self._entry(key))

        path = "/agents/A/state/transcript/{key}"
        app.router.add_get(path, get_key)
        app.router.add_post(path + "/append", append_key)
        app.router.add_put(path, put_key)
        return app

    def start(self) -> _StateApi:
        self._thread.start()

        async def serve() -> None:
            self._runner = web.AppRunner(self._app())
            await self._runner.setup()
            await web.TCPSite(self._runner, "127.0.0.1", self.port).start()

        asyncio.run_coroutine_threadsafe(serve(), self._loop).result(10)
        return self

    def stop(self) -> None:
        if self._runner is not None:
            asyncio.run_coroutine_threadsafe(self._runner.cleanup(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)


@pytest.fixture(scope="module")
def tls(tmp_path_factory: pytest.TempPathFactory) -> ShellTls:
    return make_shell_tls(tmp_path_factory.mktemp("tls"))


@pytest.fixture(scope="module")
def state() -> Iterator[_StateApi]:
    api = _StateApi().start()
    yield api
    api.stop()


@pytest.fixture
def valkey() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=False)
    yield client
    client.close()


@pytest.fixture
def prefix(valkey: redis.Redis) -> Iterator[str]:
    key_prefix = f"test-static-q-{uuid.uuid4().hex[:8]}"
    yield key_prefix
    for key in valkey.scan_iter(match=f"{key_prefix}:*"):
        valkey.delete(key)


ShellFactory = Callable[[str], SupervisorProcess]


@pytest.fixture
def shells(tls: ShellTls, tmp_path: Path) -> Iterator[ShellFactory]:
    started: list[SupervisorProcess] = []

    def start(slot: str) -> SupervisorProcess:
        shell = SupervisorProcess(
            slot=slot, tls=tls, root=tmp_path / slot, runner_command=RUNNER_COMMAND
        )
        started.append(shell)
        return shell.start()

    yield start
    for shell in started:
        shell.stop()


class _Worker:
    """One worker process's view: its own clients over the shared Valkey and shells."""

    def __init__(
        self, valkey: redis.Redis, prefix: str, tls: ShellTls, shells: list[SupervisorProcess]
    ) -> None:
        self.tls = tls
        self.client = StaticPoolSandboxClient(
            slots=[StaticShellSlot(s.slot, "localhost", s.port, POOL) for s in shells],
            redis_client=valkey,
            control_token=CONTROL_TOKEN,
            ssl_context=shell_ssl_context(str(tls.ca)),
            bundle_store=_Bundles(),
            key_prefix=prefix,
            retire_timeout_seconds=30.0,
            allow_unisolated_shells=True,
        )
        self._pressure = AsyncRedis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW)
        self.substrate = SandboxSubstrate(
            self.client,
            AffinityStore(valkey, pressure_client=self._pressure, key_prefix=prefix),
            SubstrateConfig(
                namespace="default", warm_pool=POOL, key_prefix=prefix, claim_timeout_seconds=60
            ),
        )

    def close(self) -> None:
        asyncio.run(self._pressure.aclose())

    def runner(self) -> RunnerClient:
        return RunnerClient(ssl_context=shell_ssl_context(str(self.tls.ca)))

    def status(self, handle: SandboxHandle) -> dict[str, object]:
        async def read() -> dict[str, object]:
            async with self.runner() as runner:
                return await runner.status(handle.base_url, token=handle.token)

        return asyncio.run(read())

    def turn(self, handle: SandboxHandle, text: str) -> Any:
        """Deliver one event and return the turn's final frame."""

        async def deliver() -> Any:
            event = Event(
                type="message",
                text=text,
                user="U0QUALIFY",
                ts=f"{time.time():.6f}",
                session_id=handle.session_id,
            )
            async with self.runner() as runner:
                stream = await runner.start_turn(handle.base_url, event, handle.token)
                async with stream:
                    frames = [frame async for frame in stream]
            finals = [frame for frame in frames if getattr(frame, "type", None) == "final"]
            assert len(finals) == 1, frames
            return finals[0]

        return asyncio.run(deliver())


def _boot_env(state: _StateApi, thread: str, *, token: bool = True) -> dict[str, str]:
    env = {
        "CURIE_SESSION_ID": f"session-{thread}",
        "CURIE_BUNDLE_REF": BUNDLE_REF,
        "CURIE_BUDGET": json.dumps({"max_output_tokens_per_run": 1000, "max_usd_per_day": 5.0}),
        "CURIE_FAKE_MODEL": "1",
        "CURIE_HISTORY_REF": state.key_url(thread),
        "OTEL_SDK_DISABLED": "true",
    }
    if token:
        env["CURIE_RUNNER_TOKEN"] = "runner-" + uuid.uuid4().hex + uuid.uuid4().hex
    return env


def _slot(valkey: redis.Redis, prefix: str, slot: str) -> dict[str, str]:
    raw = valkey.hgetall(f"{prefix}:static:slot:{slot}")
    return {key.decode(): value.decode() for key, value in raw.items()}


def _claim(worker: _Worker, thread: str, env: dict[str, str]) -> SandboxHandle:
    handle = worker.substrate.claim(thread, env=env)
    wait_until(lambda: worker.status(handle).get("turn_active") is False, 30, "runner idle")
    return handle


@pytest.fixture
def worker_factory(
    valkey: redis.Redis, prefix: str, tls: ShellTls
) -> Iterator[Callable[[list[SupervisorProcess]], _Worker]]:
    made: list[_Worker] = []

    def make(shells: list[SupervisorProcess]) -> _Worker:
        worker = _Worker(valkey, prefix, tls, shells)
        made.append(worker)
        return worker

    yield make
    for worker in made:
        worker.close()


def test_turns_follow_up_and_approval_resume_cold_on_a_recycled_shell(
    valkey: redis.Redis,
    prefix: str,
    state: _StateApi,
    shells: ShellFactory,
    worker_factory: Callable[[list[SupervisorProcess]], _Worker],
) -> None:
    shell = shells("q0")
    worker = worker_factory([shell])
    thread = f"t-{uuid.uuid4().hex[:8]}"
    key = state.key_url(thread).rsplit("/", 1)[1]

    # First turn, then a follow-up on the retained route: one claim, one runner.
    first = _claim(worker, thread, _boot_env(state, thread))
    assert first.base_url.startswith(f"https://localhost:{shell.port}/c/")
    assert worker.turn(first, "hello").status == SessionStatus.DONE
    after_first = len(state.values[key])
    retained = worker.substrate.claim(thread, env=_boot_env(state, thread))
    assert retained == first
    assert worker.turn(first, "and again").status == SessionStatus.DONE
    after_follow_up = len(state.values[key])
    assert after_follow_up > after_first
    generation = int(_slot(valkey, prefix, "q0")["generation"])

    # The approval pause suspends: the slot returns to the pool once the shell
    # confirms its cleanup, and the history stays with the state API.
    paused = worker.turn(first, "ship it [fake:request-approval] deploy to prod")
    assert paused.status == SessionStatus.AWAITING_APPROVAL
    history = list(state.values[key])
    worker.substrate.suspend(thread, history_ref=state.key_url(thread))
    assert _slot(valkey, prefix, "q0")["state"] == "free"
    assert shell.shell()["claim"] is None
    assert list((shell.state_root / "claims").iterdir()) == []

    # Another thread uses the recycled shell meanwhile, and gives it back.
    other = f"t-{uuid.uuid4().hex[:8]}"
    borrowed = _claim(worker, other, _boot_env(state, other))
    assert worker.turn(borrowed, "unrelated").status == SessionStatus.DONE
    assert worker.substrate.release(other)

    # Cold resume: a fresh claim on the same shell under a new generation and a
    # newly minted bearer, booted from the stored history.
    reads_before = state.reads.get(key, 0)
    resumed = worker.substrate.resume(thread, env=_boot_env(state, thread, token=False))
    wait_until(lambda: worker.status(resumed).get("turn_active") is False, 30, "resumed idle")
    assert resumed.claim_name != first.claim_name
    assert resumed.token and resumed.token != first.token
    assert resumed.session_id == first.session_id
    assert int(_slot(valkey, prefix, "q0")["generation"]) > generation
    assert worker.turn(resumed, "approved, carry on").status == SessionStatus.DONE
    assert state.reads.get(key, 0) > reads_before
    assert state.values[key][: len(history)] == history
    assert len(state.values[key]) > len(history)

    # The paused claim's route and bearer reach nothing on the recycled shell.
    with pytest.raises(RunnerError):
        worker.status(first)
    stale_bearer = dataclasses.replace(resumed, token=first.token)
    with pytest.raises(RunnerError):
        worker.status(stale_bearer)

    # Cleanup leaves the shell idle and empty, and the slot free.
    assert worker.substrate.release(thread)
    assert _slot(valkey, prefix, "q0")["state"] == "free"
    assert shell.shell()["state"] == "idle"
    assert list((shell.state_root / "claims").iterdir()) == []


def test_a_worker_restart_mid_turn_keeps_the_shell_and_the_turn(
    valkey: redis.Redis,
    prefix: str,
    state: _StateApi,
    shells: ShellFactory,
    worker_factory: Callable[[list[SupervisorProcess]], _Worker],
) -> None:
    shell = shells("q1")
    before = worker_factory([shell])
    thread = f"t-{uuid.uuid4().hex[:8]}"
    handle = _claim(before, thread, _boot_env(state, thread))
    generation = _slot(valkey, prefix, "q1")["generation"]

    async def abandon() -> None:
        # The worker dies after the turn starts streaming: its connection drops.
        event = Event(
            type="message",
            text="[fake:progress-demo] long job",
            user="U0QUALIFY",
            ts=f"{time.time():.6f}",
            session_id=handle.session_id,
        )
        async with before.runner() as runner:
            stream = await runner.start_turn(handle.base_url, event, handle.token)
            async with stream:
                async for _frame in stream:
                    break

    asyncio.run(abandon())

    # A new worker process with fresh clients finds the same route and shell.
    after = worker_factory([shell])
    adopted = after.substrate.claim(thread, env=_boot_env(state, thread))
    assert adopted == handle
    slot = _slot(valkey, prefix, "q1")
    assert (slot["state"], slot["generation"]) == ("leased", generation)
    # The redelivered turn runs to its answer on the retained runner.
    wait_until(lambda: after.status(adopted).get("turn_active") is False, 30, "turn settled")
    final = after.turn(adopted, "[fake:progress-demo] long job")
    assert final.status == SessionStatus.DONE
    assert after.status(adopted).get("history_durable") is True
    assert after.substrate.release(thread)
    assert _slot(valkey, prefix, "q1")["state"] == "free"


def test_an_exhausted_pool_refuses_truthfully_and_admits_after_a_release(
    valkey: redis.Redis,
    prefix: str,
    state: _StateApi,
    shells: ShellFactory,
    worker_factory: Callable[[list[SupervisorProcess]], _Worker],
) -> None:
    shell = shells("q2")
    worker = worker_factory([shell])
    holder, waiting = f"t-{uuid.uuid4().hex[:8]}", f"t-{uuid.uuid4().hex[:8]}"
    held = _claim(worker, holder, _boot_env(state, holder))

    with pytest.raises(CapacityExhaustedError) as refused:
        worker.substrate.claim(waiting, env=_boot_env(state, waiting))
    rejection = refused.value.rejection
    assert capacity_rejection_is_valid(rejection)
    assert (rejection.quota_name, rejection.used, rejection.hard) == (
        POOL,
        {"pods": "1"},
        {"pods": "1"},
    )
    # The live route was not stolen: the holder still answers on its shell.
    assert worker.turn(held, "still mine").status == SessionStatus.DONE
    assert worker.substrate.lookup(waiting) is None

    assert worker.substrate.release(holder)
    admitted = _claim(worker, waiting, _boot_env(state, waiting))
    assert worker.turn(admitted, "my turn now").status == SessionStatus.DONE
    assert worker.substrate.release(waiting)
