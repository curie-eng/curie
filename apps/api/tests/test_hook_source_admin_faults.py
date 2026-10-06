"""Administrative source route faults on owned stores, @spec PROTECTED-HOOK-SOURCE-3/6/7/10.

Real HTTP routes, isolated migrated Postgres and the owned disposable TLS
Valkey with distinct writer and reader principals, as in
``test_hook_source_admin_routes.py`` whose fixtures this module reuses. Faults
are real operations on owned resources only: ``CLIENT PAUSE WRITE`` holds a
writer EVAL in flight while reads proceed, an owned container restart or
freeze, an owned transparent TCP relay that holds one connection's server
bytes (TLS stays end to end, the relay sees ciphertext only), and an owned
advisory gate hold. Product code gains no failure injection parameter.
"""

from __future__ import annotations

import asyncio
import secrets
import socket
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import test_hook_source_admin_routes as _routes
from curie_api.config import get_settings
from curie_protected_hooks.source_policy_sql import SourceGate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_hook_source_admin_routes import (
    BODIES,
    PROTECTED_TARGET,
    _intent,
    admin_app,
    assert_refusal,
    connections,
    counter,
    delete_policy,
    get_policy,
    get_secret,
    ledger,
    policy_row,
    post_support,
    put_policy,
    second_agent,
    seed_row,
    source_record,
    state,
    support_headers,
)
from test_hook_source_support import legacy_secret

pytestmark = pytest.mark.usefixtures("support_db")
# Fixtures shared with the route suite, registered in this module by name.
globals().update(
    {name: getattr(_routes, name) for name in ("admission_service", "principals", "support_db")}
)

TIMEOUT = 60


def run(scenario: Callable[[], Any]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    asyncio.run(asyncio.wait_for(scenario(), TIMEOUT))


# -- owned observation helpers -------------------------------------------------------------


async def advisory(observer: Any, *, granted: bool) -> int:
    """Advisory locks granted (the held agent gates) or awaited, @spec PROTECTED-HOOK-SOURCE-2."""
    async with observer.connect() as connection:
        value = await connection.scalar(
            text(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid "
                "WHERE a.datname=current_database() AND l.locktype='advisory' "
                "AND l.granted=:granted"
            ),
            {"granted": granted},
        )
    return int(value or 0)


async def until(predicate: Callable[[], Any], seconds: float, message: str) -> None:
    """Poll an awaitable predicate, @spec PROTECTED-HOOK-SOURCE-10."""
    deadline = time.monotonic() + seconds
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        if time.monotonic() > deadline:
            pytest.fail(message, pytrace=False)
        await asyncio.sleep(0.02)


async def pending_registered(agent: str) -> bool:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    rows = await asyncio.to_thread(ledger, agent)
    return any(status == "pending" for _op, _generation, status, _intent in rows)


def pause_writes(broker: Any, milliseconds: int) -> None:
    """Writes (EVAL included) wait while reads, AUTH and HELLO proceed.

    @spec PROTECTED-HOOK-SOURCE-6.
    """
    broker.command("CLIENT", "PAUSE", str(milliseconds), "WRITE")


def unpause(broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    broker.command("CLIENT", "UNPAUSE")


def reinstall(broker: Any) -> None:
    """Owned principals after an owned restart, @spec PROTECTED-HOOK-LANE-3."""
    broker.reader.install(broker)
    broker.writer.install(broker)


class HoldingRelay:
    """Owned transparent loopback TCP relay in front of the broker.

    Bytes pass unchanged both ways (TLS, SPKI pin and hostname stay end to
    end). ``hold(n)`` buffers the server to client bytes of the n-th accepted
    connection; when that connection's upstream closes, the buffered bytes are
    delivered and the client side is closed. @spec PROTECTED-HOOK-SOURCE-6/7.
    """

    def __init__(self, port: int) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        self.upstream_port = port
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.accepted = 0
        self.held: int | None = None
        self.buffers: dict[int, bytearray] = {}
        self.sockets: list[socket.socket] = []
        self.threads: list[threading.Thread] = []
        self.stopping = threading.Event()

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<owned-relay>"

    def hold(self, index: int) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        with self.lock:
            self.held = index
            self.buffers.setdefault(index, bytearray())

    def _accept(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", self.upstream_port), timeout=2)
            except OSError:
                client.close()
                continue
            upstream.settimeout(None)
            with self.lock:
                self.accepted += 1
                index = self.accepted
                self.sockets += [client, upstream]
            for target, args in (
                (self._up, (client, upstream)),
                (self._down, (index, upstream, client)),
            ):
                thread = threading.Thread(target=target, args=args, daemon=True)
                self.threads.append(thread)
                thread.start()

    def _up(self, client: socket.socket, upstream: socket.socket) -> None:
        """Client to server, unchanged, @spec PROTECTED-HOOK-SOURCE-6."""
        try:
            while chunk := client.recv(65536):
                upstream.sendall(chunk)
        except OSError:
            pass
        try:
            upstream.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def _down(self, index: int, upstream: socket.socket, client: socket.socket) -> None:
        """Server to client, buffered while held, @spec PROTECTED-HOOK-SOURCE-6/7."""
        try:
            while chunk := upstream.recv(65536):
                with self.lock:
                    if self.held == index:
                        self.buffers[index] += chunk
                        continue
                client.sendall(chunk)
        except OSError:
            pass
        with self.lock:
            pending = bytes(self.buffers.pop(index, b""))
        try:
            if pending:
                client.sendall(pending)
            client.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def __enter__(self) -> HoldingRelay:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        thread = threading.Thread(target=self._accept, daemon=True)
        self.threads.append(thread)
        thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Only owned sockets and threads, @spec PROTECTED-HOOK-SOURCE-6."""
        self.stopping.set()
        self.listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass
        for thread in self.threads:
            thread.join(timeout=2)


# -- cancellation during a writer call -------------------------------------------------------


def test_cancellation_during_reserve_keeps_the_gate_until_the_writer_call_ends(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancelled request never releases the agent gate while its reserve is in flight.

    The reserve EVAL is held by an owned write pause; the request is
    cancelled; the gate stays granted until the pause ends and the call
    returns, then it is released with no SQL commit.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        observer = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        try:
            async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
                op = str(uuid.uuid4())
                await asyncio.to_thread(pause_writes, principals, 4000)
                try:
                    request = asyncio.create_task(put_policy(client, agent, "0", op))
                    await until(lambda: pending_registered(agent), 4, "no registration")
                    await asyncio.sleep(0.4)
                    assert not request.done(), "reserve did not wait on the paused broker"
                    assert source_record(principals, agent) is None
                    request.cancel()
                    await asyncio.sleep(0.6)
                    assert await advisory(observer, granted=True) == 1, (
                        "gate released while the writer call was in flight"
                    )
                finally:
                    await asyncio.to_thread(unpause, principals)
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(request, 8)
                deadline = time.monotonic() + 3
                while await advisory(observer, granted=True):
                    assert time.monotonic() < deadline, "gate never released"
                    await asyncio.sleep(0.02)
                assert await asyncio.to_thread(policy_row, agent) is None
                assert await asyncio.to_thread(ledger, agent) == [
                    (op, 1, "pending", _intent(PROTECTED_TARGET))
                ]
                assert await asyncio.to_thread(counter, agent) == 0
        finally:
            await observer.dispose()

    run(scenario)


# -- broker restart between a writer effect and its confirmation -----------------------------


def test_restart_between_reserve_and_confirmation_is_broker_unavailable(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reservation lands, its reply is held, the owned broker restarts: no commit.

    The writer cannot see run_id; the reader bracket after the effect cannot
    confirm on the restarted epoch, so the answer is 503 ``broker_unavailable``
    with a null committed generation and only pending history.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/7."""
        with HoldingRelay(principals.port) as relay:
            async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
                assert rt is not None
                rt.manifest["broker_identity"]["endpoint"]["port"] = relay.port
                rt.write()
                op = str(uuid.uuid4())
                await asyncio.to_thread(pause_writes, principals, 4000)
                restarted = False
                try:
                    request = asyncio.create_task(put_policy(client, agent, "0", op))
                    await until(lambda: pending_registered(agent), 4, "no registration")
                    await asyncio.sleep(0.4)
                    assert relay.accepted == 2, "expected one reader then one writer connection"
                    # Hold the writer connection's replies; the EVAL then executes.
                    relay.hold(2)
                    await asyncio.to_thread(unpause, principals)
                    await until(
                        lambda: (source_record(principals, agent) or {}).get("operation_id") == op,
                        3,
                        "reservation effect never landed",
                    )
                    await asyncio.to_thread(principals.restart)
                    restarted = True
                    response = await asyncio.wait_for(request, 15)
                finally:
                    await asyncio.to_thread(unpause, principals)
                    if not restarted:
                        await asyncio.to_thread(principals.restart)
                    await asyncio.to_thread(reinstall, principals)
                assert_refusal(response, 503, "broker_unavailable")
                assert await asyncio.to_thread(policy_row, agent) is None
                assert await asyncio.to_thread(ledger, agent) == [
                    (op, 1, "pending", _intent(PROTECTED_TARGET))
                ]
                assert await asyncio.to_thread(counter, agent) == 0

    run(scenario)


# -- concurrency on one agent ----------------------------------------------------------------


@pytest.mark.parametrize("second", ["put", "delete"])
def test_concurrent_mutations_on_one_agent_serialize_on_the_gate(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, second: str
) -> None:
    """Two mutations at expected generation 0 for one agent: exactly one commits.

    The loser waits on the gate, then refuses on stale CAS (or, for DELETE of
    the now configured row, stale CAS too) with no new history.
    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(pause_writes, principals, 1500)
            first_op, second_op = str(uuid.uuid4()), str(uuid.uuid4())
            try:
                first = asyncio.create_task(put_policy(client, agent, "0", first_op))
                await until(lambda: pending_registered(agent), 4, "no registration")
                other = asyncio.create_task(
                    put_policy(client, agent, "0", second_op)
                    if second == "put"
                    else delete_policy(client, agent, "0", second_op)
                )
                await asyncio.sleep(0.3)
                assert not other.done(), "second mutation did not wait on the agent gate"
            finally:
                await asyncio.to_thread(unpause, principals)
            winner, loser = await asyncio.wait_for(asyncio.gather(first, other), 15)
            assert_refusal(winner, 503, "source_publication_deferred", "1")
            assert_refusal(loser, 409, "stale_source_generation")
            assert await asyncio.to_thread(ledger, agent) == [
                (first_op, 1, "committed", _intent(PROTECTED_TARGET))
            ]
            assert source_record(principals, agent) == {
                "floor": "1",
                "operation_id": first_op,
                "active": None,
            }

    run(scenario)


# -- gate waits and the gate pool bound --------------------------------------------------------


class OwnedGateHold:
    """An owned holder of one agent's source gate on its own engine.

    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-10.
    """

    def __init__(self, agent: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.agent = uuid.UUID(agent)
        self.engine = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        self.held = asyncio.Event()
        self.release = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    async def _hold(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with SourceGate(self.engine).hold(self.agent):
            self.held.set()
            await self.release.wait()

    async def __aenter__(self) -> OwnedGateHold:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.task = asyncio.create_task(self._hold())
        await asyncio.wait_for(self.held.wait(), 5)
        return self

    async def __aexit__(self, *_: object) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.release.set()
        if self.task is not None:
            await asyncio.gather(self.task, return_exceptions=True)
        await self.engine.dispose()


async def timed(call: Any) -> tuple[httpx.Response, float]:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    started = time.monotonic()
    response = await call
    return response, time.monotonic() - started


def test_admin_requests_give_up_on_the_gate_after_five_seconds_without_effects(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutation, GET and secret that cannot acquire the agent gate: 503 source_state_unavailable.

    No registration, reservation or write, no broker connection, and each
    mutation releases its administrative slot when it gives up: afterwards two
    mutations for other agents both obtain a slot.
    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            others = [await second_agent(client), await second_agent(client)]
            before = await state(agent)
            opened = connections(principals)
            async with OwnedGateHold(agent):
                tasks = [
                    asyncio.create_task(timed(call))
                    for call in (
                        put_policy(client, agent, "9", str(uuid.uuid4())),
                        delete_policy(client, agent, "9", str(uuid.uuid4())),
                        get_policy(client, agent),
                        get_secret(client, agent),
                    )
                ]
                _done, waiting = await asyncio.wait(tasks, timeout=10)
            # The owned hold is released here, so any request still waiting now finishes.
            results = await asyncio.wait_for(asyncio.gather(*tasks), 20)
            assert not waiting, (
                f"{len(waiting)} administrative requests still waited on the gate after 10 s"
            )
            for response, elapsed in results:
                assert_refusal(response, 503, "source_state_unavailable")
                assert 4.5 <= elapsed <= 8, f"gate wait not bounded at five seconds: {elapsed:.2f}"
            assert connections(principals) == opened
            assert await state(agent) == before
            assert source_record(principals, agent) is None
            freed = await asyncio.gather(
                *(put_policy(client, other, "0", str(uuid.uuid4())) for other in others)
            )
            for response in freed:
                assert_refusal(response, 503, "source_publication_deferred", "1")

    run(scenario)


def test_gate_pool_exhaustion_by_slow_mutations_is_bounded_for_other_agents(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two mutations on a paused broker plus two gate waiters exhaust the four gate connections.

    Gated ingress for another agent (its support probe) waits for a gate
    connection, and no longer than the documented bound: the gate phase
    deadline plus SQL time, measured here as the pause plus slack.
    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9
    @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        observer = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        try:
            async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
                other = await second_agent(client)
                body = BODIES[None]
                pause_ms = 3000
                await asyncio.to_thread(pause_writes, principals, pause_ms)
                paused_at = time.monotonic()
                try:
                    first = asyncio.create_task(put_policy(client, agent, "0", str(uuid.uuid4())))
                    await until(lambda: pending_registered(agent), 2, "no registration")
                    waiters = [
                        asyncio.create_task(put_policy(client, agent, "0", str(uuid.uuid4()))),
                        asyncio.create_task(get_policy(client, agent)),
                        asyncio.create_task(get_secret(client, agent)),
                    ]
                    await until(
                        lambda: advisory(observer, granted=False),
                        2,
                        "gate waiters never queued",
                    )
                    await asyncio.sleep(0.3)
                    probe_started = time.monotonic()
                    probe = asyncio.create_task(
                        post_support(
                            client,
                            other,
                            body,
                            support_headers(
                                legacy_secret(other),
                                requested=None,
                                body=body,
                                delivery="bound-" + secrets.token_hex(4),
                            ),
                        )
                    )
                    await asyncio.sleep(0.5)
                    assert not probe.done(), "another agent's ingress did not wait on the pool"
                finally:
                    remaining = pause_ms / 1000 - (time.monotonic() - paused_at)
                    if remaining > 0:
                        await asyncio.sleep(remaining)
                    await asyncio.to_thread(unpause, principals)
                response = await asyncio.wait_for(probe, 15)
                waited = time.monotonic() - probe_started
                assert response.status_code == 503, response.text
                assert response.json()["reason"] == "source_unconfigured"
                # Bound: gate phase deadline (5 s) plus SQL time; the pause ends first here.
                assert waited <= 5 + 3, f"other agent's ingress stalled {waited:.2f}s"
                # The paused reserve outlives the writer's two second socket timeout or not.
                slow = await first
                assert slow.status_code == 503 and slow.json()["detail"]["code"] in (
                    "source_publication_deferred",
                    "broker_unavailable",
                ), slow.text
                await asyncio.gather(*waiters, return_exceptions=True)
        finally:
            await observer.dispose()

    run(scenario)


# -- exact protected replay from SQL alone ---------------------------------------------------


def test_exact_protected_replay_answers_deferred_with_the_broker_frozen(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With the owned broker frozen, an exact protected replay still answers from SQL alone.

    503 ``source_publication_deferred`` with the committed generation, fast,
    with no broker connection and no new durable state.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-7.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            op = str(uuid.uuid4())
            assert_refusal(
                await put_policy(client, agent, "0", op), 503, "source_publication_deferred", "1"
            )
            before, opened = await state(agent), connections(principals)
            await asyncio.to_thread(principals.docker, "pause", principals.cid)
            try:
                replay, elapsed = await timed(put_policy(client, agent, "4", op))
            finally:
                await asyncio.to_thread(principals.docker, "unpause", principals.cid)
            assert_refusal(replay, 503, "source_publication_deferred", "1")
            assert elapsed < 1.5, f"replay waited on the broker: {elapsed:.2f}s"
            assert connections(principals) == opened, "replay opened a broker connection"
            assert await state(agent) == before

    run(scenario)
