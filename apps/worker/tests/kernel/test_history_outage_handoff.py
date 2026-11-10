"""#5354: a state API outage must not lock a thread against every later turn.

When the state API is unreachable, the runner's terminal transcript append fails
with a connect error and the turn still ends DONE. Since #3823 a follow-up turn
mints a boot state token that outlives the one the live runner booted with, so
the kernel replaces that runner at the durable handoff boundary (the #3071 turn
budget fence), which needs ``history_durable``. If the runner treats the connect
failure as a permanent loss, ``history_durable`` never comes back, every later
delivery raises ``ThreadBusyError``, and the stream dead-letters the follow-up.

A connect failure never reached the API, so the runner keeps the record and
retries it once the API is back. These tests drive the real ``Kernel`` against
real Valkey and the real runner app (only its model is faked), whose real
``StateApiTranscriptStore`` talks to a fake state API on a loopback port. The
outage is a stopped server on that port; recovery starts it on the same port.
The binding double delegates ``boot_env`` to the real resolver, so the boot
tokens and their expiries are the ones the worker really mints.
"""

from __future__ import annotations

import asyncio
import functools
import socket
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from curie_runner.fake import FakeModelSession
from curie_runner.history import StateApiTranscriptStore
from curie_runner.otel import RunTracer
from curie_runner.server import create_app
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_worker import binding as binding_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.binding import BindingResolver, HttpIdentityBinding, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.kernel.failures import ThreadBusyError

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent  # noqa: E402

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
_qevent = functools.partial(qevent, channel=CHANNEL)
_KEY_PATH = "/agents/A/state/transcript/th-outage"
_CAP_HEADERS = {"X-Curie-Transcript-Max-Bytes": "65536"}
# The bound on waiting for the runner's append retry before the follow-up's
# second and last delivery. The retry runs 0.5 s, then 1.5 s, after the failure.
_REDELIVERY_WAIT_S = 5.0


class _RealBootEnvBinding:
    """Resolves one agent; ``boot_env`` is the real resolver's minting."""

    def __init__(self) -> None:
        self._real = BindingResolver.__new__(BindingResolver)
        self._real._config = WorkerConfig()  # type: ignore[attr-defined]

    async def identity_binding(
        self,
        kind: str,
        adapter: str | None,
        address: str,
    ) -> HttpIdentityBinding | None:
        """@spec HTTP-INVOCATION-EXECUTION-18: explicit local fixture classification."""
        return HttpIdentityBinding(
            tenant_id=uuid.UUID("00000000-0000-4000-8000-000000000001"),
            agent_id=AGENT_ID,
            binding_id=uuid.uuid5(uuid.NAMESPACE_URL, repr((kind, adapter, address))),
            binding_generation=1,
            http_identity_required=False,
        )

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        return ResolvedDeployment(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            version_id=uuid.UUID("33333333-3333-4333-8333-333333333333"),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    def boot_env(self, resolved: Any, thread_key: str, **kwargs: Any) -> dict[str, str]:
        return self._real.boot_env(resolved, thread_key, **kwargs)

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()

    def release_boot_credential_sync(self, agent_id: str, credential: str) -> bool:
        return True

    def fresh_boot_credential(self, env: Any) -> dict[str, str]:
        return self._real.fresh_boot_credential(env)


class _LaterClock:
    """``time`` for the binding module only, read some minutes after the real one.

    A person's follow-up arrives after the first turn, so its boot token
    expires later than the live runner's. Shifting only the minting clock
    reproduces that without sleeping and without moving Valkey's clock.
    """

    def __init__(self, offset_s: float) -> None:
        self._offset_s = offset_s

    def time(self) -> float:
        return time.time() + self._offset_s

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _StateApi:
    """One transcript key on a fake state API, on a port that can go down.

    Replays the shapes of ``get_state`` and ``append_state`` in
    apps/api/src/curie_api/routers/state.py: every read advertises the
    transcript cap header, an unwritten key answers 404 ``{"detail": "not
    found"}``, and a stored key or a successful ``POST <key>/append`` answers
    the ``StateEntryOut`` body.
    """

    def __init__(self) -> None:
        self.port = _free_port()
        self.items: list[dict[str, Any]] = []
        self._runner: web.AppRunner | None = None

    @property
    def key_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{_KEY_PATH}"

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": "transcript",
            "key": "th-outage",
            "value": list(self.items),
            "version": len(self.items),
            "updated_at": "2026-01-01T00:00:00Z",
        }

    def _app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            if not self.items:
                return web.json_response({"detail": "not found"}, status=404, headers=_CAP_HEADERS)
            return web.json_response(self._entry(), headers=_CAP_HEADERS)

        async def append_key(request: web.Request) -> web.Response:
            body = await request.json()
            assert set(body) == {"item", "reserve_bytes"}
            self.items.append(body["item"])
            return web.json_response(self._entry())

        app.router.add_get(_KEY_PATH, get_key)
        app.router.add_post(f"{_KEY_PATH}/append", append_key)
        return app

    async def start(self) -> None:
        assert self._runner is None
        runner = web.AppRunner(self._app())
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", self.port, reuse_address=True).start()
        self._runner = runner

    async def stop(self) -> None:
        if self._runner is not None:
            runner, self._runner = self._runner, None
            await runner.cleanup()


def _real_runner(store: StateApiTranscriptStore) -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        session_id="session-outage",
        history_store=store,
    )


def _replies(h: Any, placeholder: str) -> list[str]:
    return [text for _channel, ref, text in h.sink.updates if ref == placeholder]


async def _retry_settled(runner: SessionRunner) -> None:
    """Wait, bounded, for the runner's background append retry to finish.

    Read with getattr so a runner without the retry goes straight to the
    second delivery and fails on its ``ThreadBusyError``, the product failure.
    """
    task = getattr(runner, "_history_retry_task", None)
    if task is not None:
        await asyncio.wait_for(asyncio.shield(task), timeout=_REDELIVERY_WAIT_S)


def test_a_follow_up_after_a_history_append_outage_replaces_the_runner(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        api = _StateApi()
        store = StateApiTranscriptStore(api.key_url, token=None)
        # Boot reads the empty transcript while the API is up, then it goes away.
        await api.start()
        assert await store.load() == []
        await api.stop()
        runner = _real_runner(store)
        await runner.start()
        try:
            async with make_harness(
                binding=_RealBootEnvBinding(), runner_app=create_app(runner)
            ) as h:
                thread = "th-history-outage"

                await h.kernel.process_event(
                    _qevent("run the probes", thread=thread, placeholder="p-first")
                )
                assert _replies(h, "p-first")[-1].endswith("all done")
                assert runner.history_durable is False

                await api.start()
                monkeypatch.setattr(binding_module, "time", _LaterClock(300))

                # A bounded delivery budget of two, as the stream would retry.
                follow_up = _qevent("continue", thread=thread, placeholder="p-second")
                try:
                    await h.kernel.process_event(follow_up)
                except ThreadBusyError:
                    await _retry_settled(runner)
                    await h.kernel.process_event(follow_up)

                assert _replies(h, "p-second")[-1].endswith("all done")
                # The follow-up replaced the runner instead of adopting it.
                assert len(h.fake_k8s.claim_envs) == 2
                users = [item["user"] for item in api.items]
                assert len(users) == 2
                assert "run the probes" in users[0]
                assert "continue" in users[1]
                assert runner.history_durable is True
        finally:
            await api.stop()

    asyncio.run(go())


def test_a_follow_up_while_the_state_api_stays_down_is_still_refused(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fence still fails closed while the first turn's record is not durable."""

    async def go() -> None:
        api = _StateApi()  # never started: the port refuses every connection
        runner = _real_runner(StateApiTranscriptStore(api.key_url, token=None))
        await runner.start()
        async with make_harness(binding=_RealBootEnvBinding(), runner_app=create_app(runner)) as h:
            thread = "th-history-outage-down"

            await h.kernel.process_event(
                _qevent("run the probes", thread=thread, placeholder="p-first")
            )
            assert _replies(h, "p-first")[-1].endswith("all done")
            assert runner.history_durable is False
            monkeypatch.setattr(binding_module, "time", _LaterClock(300))

            with pytest.raises(ThreadBusyError, match="turn budget handoff boundary"):
                await h.kernel.process_event(
                    _qevent("continue", thread=thread, placeholder="p-second")
                )

            assert not any(text.endswith("all done") for text in _replies(h, "p-second"))
            assert len(h.fake_k8s.claim_envs) == 1
            assert runner.history_durable is False
            assert api.items == []

    asyncio.run(go())
