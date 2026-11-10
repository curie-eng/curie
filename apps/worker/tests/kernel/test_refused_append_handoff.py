"""#5366: a refused or cut-off transcript append must not wedge the thread.

The release QA deleted Postgres mid-turn. The state API answered the runner's
terminal transcript append with HTTP 500, the turn still ended DONE, and the
runner counted the record as a permanent loss. #5354 retried only an append
that never connected; an append the API answered, or one whose connection was
cut after the request was sent, may already have been written, and without an
idempotency key a retry could store the turn twice. So ``history_durable``
stayed false for the life of the runner. Since #3823 a follow-up mints a boot
token that outlives the live runner's, so the kernel must replace that runner at
the durable handoff boundary (the #3071 turn budget fence), which needs
``history_durable``. Every delivery of the follow-up raised ``ThreadBusyError``
until the stream dead-lettered it.

Each turn record now carries a ``record_id`` minted once per turn, and the API
returns the stored entry unchanged when that id is already stored. A retry is
then safe whatever happened to the first attempt, so the runner defers a record
whose append outcome is unknown and retries it once the API is back.

These tests drive the real ``Kernel`` against real Valkey and the real runner
app (only its model is faked), whose real ``StateApiTranscriptStore`` talks to a
fake state API on a loopback port. The binding double delegates ``boot_env`` to
the real resolver, so the boot tokens and their expiries are the ones the worker
really mints. The failure modes are:

1. ``connect``: the server is stopped, so the port refuses the connection.
2. ``refused``: the append answers 500 and writes nothing, the QA shape.
3. ``cut``: the append stores the item, then the connection closes before any
   response byte is sent.
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
_KEY = "th-refused"
_KEY_PATH = f"/agents/A/state/transcript/{_KEY}"
_CAP_HEADERS = {"X-Curie-Transcript-Max-Bytes": "65536"}
# The bound on waiting for the runner's append retry before the follow-up's
# second and last delivery. The retry runs 0.5 s, then 1.5 s, 3.5 s after the
# failure, so a recovery right after the first turn lands well inside it.
_REDELIVERY_WAIT_S = 5.0

_HEALTHY = "healthy"
_CONNECT = "connect"
_REFUSED = "refused"
_CUT = "cut"


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
    """One transcript key on a fake state API whose failure mode a test sets.

    Replays the shapes of ``get_state`` and ``append_state`` in
    apps/api/src/curie_api/routers/state.py and ``transcripts.append`` in
    apps/api/src/curie_api/transcripts.py: every read advertises the transcript
    cap header, an unwritten key answers 404 ``{"detail": "not found"}``, and a
    stored key or a successful ``POST <key>/append`` answers the
    ``StateEntryOut`` body. The #5366 replay rule: an item whose nonempty
    ``record_id`` is already stored is not appended again, and the stored entry
    is returned with 200 and an unchanged version.

    The failure mode applies to every request until ``recover``: ``refused``
    answers 500 ``{"detail": "Internal Server Error"}`` to reads and appends
    and writes nothing (the API's shape while Postgres is gone); ``cut``
    commits the append, then closes the connection without a response, and
    reads normally; ``connect`` stops the server so the port refuses.
    """

    def __init__(self) -> None:
        self.port = _free_port()
        self.items: list[dict[str, Any]] = []
        self.mode = _HEALTHY
        self._runner: web.AppRunner | None = None

    @property
    def key_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{_KEY_PATH}"

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": "transcript",
            "key": _KEY,
            "value": list(self.items),
            "version": len(self.items),
            "updated_at": "2026-01-01T00:00:00Z",
        }

    def _store(self, item: dict[str, Any]) -> None:
        record_id = item.get("record_id")
        if isinstance(record_id, str) and record_id:
            if any(stored.get("record_id") == record_id for stored in self.items):
                return
        self.items.append(item)

    def _app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            if self.mode == _REFUSED:
                return web.json_response({"detail": "Internal Server Error"}, status=500)
            if not self.items:
                return web.json_response({"detail": "not found"}, status=404, headers=_CAP_HEADERS)
            return web.json_response(self._entry(), headers=_CAP_HEADERS)

        async def append_key(request: web.Request) -> web.StreamResponse:
            body = await request.json()
            assert set(body) == {"item", "reserve_bytes"}
            if self.mode == _REFUSED:
                return web.json_response({"detail": "Internal Server Error"}, status=500)
            self._store(body["item"])
            if self.mode == _CUT:
                # The row is committed; now the connection drops before a
                # single response byte. Closing the transport sends FIN with
                # nothing buffered, so the client's read sees end of stream
                # and raises ServerDisconnectedError. The response returned
                # below is never written: aiohttp refuses or drops writes to a
                # closing transport and treats it as a client disconnect.
                transport = request.transport
                assert transport is not None
                transport.close()
                return web.Response(status=200)
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

    async def arm(self, mode: str) -> None:
        if mode == _CONNECT:
            await self.stop()
        else:
            self.mode = mode

    async def recover(self) -> None:
        self.mode = _HEALTHY
        if self._runner is None:
            await self.start()


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
        session_id="session-refused",
        history_store=store,
    )


def _replies(h: Any, placeholder: str) -> list[str]:
    return [text for _channel, ref, text in h.sink.updates if ref == placeholder]


async def _retry_settled(runner: SessionRunner) -> None:
    """Wait, bounded, for the runner's background append retry to finish.

    Read with getattr so a runner without the retry goes straight to the
    second delivery and fails on its ``ThreadBusyError``, the product failure.
    A retry that has not landed within the bound does the same.
    """
    task = getattr(runner, "_history_retry_task", None)
    if task is None:
        return
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=_REDELIVERY_WAIT_S)
    except TimeoutError:
        return


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param(_CONNECT, id="connect-refused"),
        pytest.param(_REFUSED, id="http-500-not-written"),
        pytest.param(_CUT, id="cut-after-commit"),
    ],
)
def test_a_follow_up_after_a_failed_history_append_is_answered(
    make_harness, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    async def go() -> None:
        api = _StateApi()
        store = StateApiTranscriptStore(api.key_url, token=None)
        # Boot reads the empty transcript while the API is up, then it fails.
        await api.start()
        assert await store.load() == []
        runner = _real_runner(store)
        await runner.start()
        try:
            async with make_harness(
                binding=_RealBootEnvBinding(), runner_app=create_app(runner)
            ) as h:
                thread = f"th-refused-{mode}"
                await api.arm(mode)

                await h.kernel.process_event(
                    _qevent("run the probes", thread=thread, placeholder="p-first")
                )
                assert _replies(h, "p-first")[-1].endswith("all done")
                assert runner.history_durable is False

                await api.recover()
                monkeypatch.setattr(binding_module, "time", _LaterClock(300))

                # A bounded delivery budget of two, as the stream would retry.
                follow_up = _qevent("continue", thread=thread, placeholder="p-second")
                try:
                    await h.kernel.process_event(follow_up)
                except ThreadBusyError:
                    await _retry_settled(runner)
                    try:
                        await h.kernel.process_event(follow_up)
                    except ThreadBusyError as exc:
                        pytest.fail(
                            f"follow-up still fenced on its second delivery after a "
                            f"{mode} append failure recovered: {exc}"
                        )

                assert _replies(h, "p-second")[-1].endswith("all done")
                # The follow-up replaced the runner instead of adopting it.
                assert len(h.fake_k8s.claim_envs) == 2
                # Each turn is stored exactly once, in order.
                users = [item["user"] for item in api.items]
                assert len(users) == 2, users
                assert "run the probes" in users[0]
                assert "continue" in users[1]
                record_ids = [item.get("record_id") for item in api.items]
                assert all(isinstance(rid, str) and rid for rid in record_ids), record_ids
                assert record_ids[0] != record_ids[1]
                assert runner.history_durable is True
        finally:
            # The runner app's cleanup closed the runner (and its retry task).
            await api.stop()

    asyncio.run(go())


def test_a_follow_up_while_appends_are_still_refused_is_still_fenced(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fence still fails closed while the first turn's record is not durable."""

    async def go() -> None:
        api = _StateApi()
        store = StateApiTranscriptStore(api.key_url, token=None)
        await api.start()
        assert await store.load() == []
        runner = _real_runner(store)
        await runner.start()
        try:
            async with make_harness(
                binding=_RealBootEnvBinding(), runner_app=create_app(runner)
            ) as h:
                thread = "th-refused-stays-down"
                await api.arm(_REFUSED)

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
        finally:
            # The runner app's cleanup closed the runner (and its retry task).
            await api.stop()

    asyncio.run(go())
