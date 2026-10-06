"""The API admission reconciliation owner, @spec PROTECTED-HOOK-LANE-4 PROTECTED-HOOK-ADMISSION-5.

One reconciler task per API process, started and stopped by the lifespan,
owns preparing intents that hold capacity: with no caller retry it commits an
interrupted intent once authority opens, consumes one attempt per tick while
authority is closed and fails with a refund at the tenth, fails and refunds
past the 300 second deadline, finds a preparing intent behind committed
members, skips a malformed intent, survives an unexpected exception, leaves an
intent without a quota member to a caller retry, performs no broker I/O with
the setting unset or a file invalid, never double appends or refunds when two
processes race, joins within ten seconds on shutdown with a paused broker, and
logs only counts.

Preparing intents are made by real partial admission on the owned broker:
the fixture denies the enqueue principal one inner write (an owned ACL
change), so the script's earlier writes persist. Seeded attempt counts and
deadlines are owned record edits the facade's snapshot accepts. See
``_protected_ingress_harness`` for the stores and principals.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from _protected_ingress_harness import (
    BODY,
    QUOTA,
    _broker,
    admission_key,
    assert_detail,
    broker_now_ms,
    broker_snapshot,
    canonical,
    conversation,
    deliver,
    event_id,
    forbidden,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    private_entries,
    provision,
    record,
    seed_row,
    set_admission_open,
    signed,
    until,
)
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_protected_hooks import admission_records as records
from curie_protected_hooks.atomic_admission import AtomicAdmission
from curie_protected_hooks.authority_records import parse_manifest
from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from test_hook_source_admin_routes import captured_logs, connections
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    GENERATION,
    HOOK,
    scoped_secret,
    support_db,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
TICK = 5.0


def run(scenario: Any, timeout: float = 90) -> None:
    """@spec PROTECTED-HOOK-LANE-4."""
    asyncio.run(asyncio.wait_for(scenario(), timeout))


def enqueue_client(broker: Any) -> Redis:
    """Fixture-side connection as the enqueue principal, @spec PROTECTED-HOOK-LANE-3."""
    return Redis(
        host="127.0.0.1",
        port=broker.port,
        username=broker.enqueue.username,
        password=broker.enqueue.password,
        ssl=True,
        ssl_ca_certs=str(broker.private / "ca.crt"),
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        protocol=3,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry=Retry(NoBackoff(), 0),
    )


def admission_request(agent: str, rt: Any, delivery: str) -> Any:
    """The committed row's tuple for one delivery, @spec PROTECTED-HOOK-ADMISSION-2."""
    return records.AdmissionRequest(
        identity=records.DeliveryIdentity(agent_id=agent, hook=HOOK, delivery_id=delivery),
        source_policy=dict(
            agent_id=agent,
            hook=HOOK,
            generation=str(GENERATION),
            operation_id=rt.operation,
            legacy_generation="0",
            mode="protected",
            tool_access="read-only",
            **rt.row,
        ),
        requested_tool_access=None,
        request_body_sha256=hashlib.sha256(BODY).hexdigest(),
        queued_payload=_broker.queued_payload(
            event_id=event_id(agent, delivery),
            conversation_id=conversation(agent),
            author=f"hook:{HOOK}",
        ),
    )


@contextlib.contextmanager
def facade(broker: Any, rt: Any) -> Any:
    """A fixture-side facade on the trusted manifest, @spec PROTECTED-HOOK-ADMISSION-1."""
    client = enqueue_client(broker)
    try:
        yield AtomicAdmission(
            client,
            trusted_manifest=parse_manifest(canonical(rt.manifest)),
            trusted_max_readiness_ms=60000,
            backlog_limit=64,
        )
    finally:
        client.close()


def interrupt(broker: Any, rt: Any, agent: str, delivery: str, command: str = "xadd") -> dict:
    """Real partial admission: deny one inner write, keep the earlier ones.

    @spec PROTECTED-HOOK-ADMISSION-4/5.
    """
    broker.enqueue.deny(broker, command)
    try:
        with facade(broker, rt) as f, pytest.raises(records.AdmissionUnavailable):
            f.admit(admission_request(agent, rt, delivery))
    finally:
        broker.enqueue.restore(broker)
    intent = record(broker, admission_key("intent", agent, delivery))
    assert intent is not None, "the owned fault left no intent"
    return intent


def admit(broker: Any, rt: Any, agent: str, delivery: str) -> None:
    """One committed delivery through the facade, @spec PROTECTED-HOOK-ADMISSION-4."""
    with facade(broker, rt) as f:
        assert f.admit(admission_request(agent, rt, delivery)).status == "accepted"


def state(broker: Any, agent: str, delivery: str) -> dict[str, Any] | None:
    """@spec PROTECTED-HOOK-ADMISSION-5."""
    return record(broker, admission_key("state", agent, delivery))


def committed(broker: Any, agent: str, delivery: str) -> bool:
    """@spec PROTECTED-HOOK-ADMISSION-5."""
    return bool(broker.command("EXISTS", admission_key("commit", agent, delivery)))


def set_attempts(broker: Any, agent: str, delivery: str, attempts: int) -> None:
    """Owned state edit the facade snapshot accepts, @spec PROTECTED-HOOK-ADMISSION-5."""
    broker.command(
        "SET",
        admission_key("state", agent, delivery),
        canonical(
            dict(
                schema_version=1,
                status="preparing",
                recovery_attempts=attempts,
                reason=None,
                receipt=None,
            )
        ),
    )


def test_interrupted_intent_commits_without_a_caller_retry_once_authority_opens(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Closed authority consumes attempts; opening it lets the reconciler commit once.

    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            intent = interrupt(broker, rt, agent, "delivery-1")
            set_admission_open(broker, rt, False)
            await until(
                lambda: (state(broker, agent, "delivery-1") or {}).get("recovery_attempts", 0) >= 1,
                3 * TICK,
                "no reconciler tick consumed an attempt of the preparing intent",
            )
            assert not committed(broker, agent, "delivery-1")
            assert private_entries(broker) == []
            set_admission_open(broker, rt, True)
            await until(
                lambda: committed(broker, agent, "delivery-1"),
                3 * TICK,
                "the reconciler did not commit the intent once authority opened",
            )
            entries = private_entries(broker)
            assert len(entries) == 1 and entries[0][0].decode() == intent["reserved_stream_id"]
            assert broker.command("ZCARD", QUOTA) == 1, "a committed member was released"
            assert not broker.command("EXISTS", admission_key("recovery", agent, "delivery-1"))
            retry = await deliver(client, agent, signed(scoped_secret(agent)))
            assert retry.status_code == 200, retry.text
            assert retry.json()["stream_id"] == intent["reserved_stream_id"]
            assert retry.json()["duplicate"] is True

    run(scenario)


def test_closed_authority_fails_with_refund_at_the_tenth_attempt(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One attempt per tick; the tenth fails attempts_exhausted, refunds and never appends.

    A caller retry then answers 409 protected_delivery_failed.
    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            interrupt(broker, rt, agent, "delivery-1")
            set_admission_open(broker, rt, False)
            set_attempts(broker, agent, "delivery-1", 8)
            await until(
                lambda: (state(broker, agent, "delivery-1") or {}).get("recovery_attempts") == 9,
                3 * TICK,
                "no reconciler tick consumed the ninth attempt",
            )
            assert (state(broker, agent, "delivery-1") or {})["status"] == "preparing"
            await until(
                lambda: (state(broker, agent, "delivery-1") or {}).get("status") == "failed",
                3 * TICK,
                "the tenth attempt did not fail the intent",
            )
            final = state(broker, agent, "delivery-1")
            assert final is not None and final["reason"] == "attempts_exhausted"
            assert broker.command("ZCARD", QUOTA) == 0, "the failed intent was not refunded"
            assert not broker.command("EXISTS", admission_key("recovery", agent, "delivery-1"))
            assert private_entries(broker) == []
            set_admission_open(broker, rt, True)
            retry = await deliver(client, agent, signed(scoped_secret(agent)))
            assert_detail(retry, 409, "protected_delivery_failed")

    run(scenario)


def test_deadline_fails_and_refunds_without_a_caller(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            intent = interrupt(broker, rt, agent, "delivery-1")
            now = broker_now_ms(broker)
            intent.update(created_at_ms=str(now - 300001), deadline_ms=str(now - 1))
            broker.command("SET", admission_key("intent", agent, "delivery-1"), canonical(intent))
            await until(
                lambda: (state(broker, agent, "delivery-1") or {}).get("status") == "failed",
                3 * TICK,
                "the reconciler did not fail an intent past its deadline",
            )
            assert (state(broker, agent, "delivery-1") or {})["reason"] == "deadline"
            assert broker.command("ZCARD", QUOTA) == 0
            assert private_entries(broker) == []

    run(scenario)


def test_preparing_intent_behind_63_committed_members_is_found(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            for index in range(63):
                await asyncio.to_thread(admit, broker, rt, agent, f"committed-{index}")
            interrupt(broker, rt, agent, "newest")
            await until(
                lambda: committed(broker, agent, "newest"),
                3 * TICK,
                "a preparing intent behind committed members was not found",
            )
            assert broker.command("ZCARD", QUOTA) == 64
            assert broker.command("XLEN", "curie:runs") == 64

    run(scenario)


def test_malformed_intent_is_skipped_and_the_tick_continues(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            interrupt(broker, rt, agent, "malformed")
            interrupt(broker, rt, agent, "sound")
            broker.command("SET", admission_key("intent", agent, "malformed"), b"{not json")
            await until(
                lambda: committed(broker, agent, "sound"),
                3 * TICK,
                "a malformed intent stopped the tick",
            )
            assert broker.command("GET", admission_key("intent", agent, "malformed")) == (
                b"{not json"
            )
            assert not committed(broker, agent, "malformed")

    run(scenario)


def test_an_unexpected_exception_in_the_loop_is_survived(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first listing raises; the supervised loop resumes on the next tick.

    The raise is test side only (the package facade's listing), never a
    product parameter. @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker
    original = AtomicAdmission.preparing
    calls: list[int] = []

    def flaky(self: Any, limit: int) -> Any:
        """@spec PROTECTED-HOOK-LANE-4."""
        calls.append(limit)
        if len(calls) == 1:
            raise RuntimeError("injected test failure")
        return original(self, limit)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            interrupt(broker, rt, agent, "delivery-1")
            monkeypatch.setattr(AtomicAdmission, "preparing", flaky)
            await until(
                lambda: committed(broker, agent, "delivery-1"),
                4 * TICK,
                "the reconciler did not resume after an unexpected exception",
            )
            assert len(calls) >= 2

    run(scenario)


def test_intent_without_a_quota_member_is_left_to_a_caller_retry(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupted before its quota member, the intent holds no capacity; the reconciler
    leaves it, and resending the signed request recovers it.

    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            headers = signed(scoped_secret(agent))
            broker.enqueue.deny(broker, "zadd")
            try:
                interrupted = await deliver(client, agent, headers)
            finally:
                broker.enqueue.restore(broker)
            assert_detail(interrupted, 503, "broker_unavailable")
            intent = record(broker, admission_key("intent", agent, "delivery-1"))
            assert intent is not None and broker.command("ZCARD", QUOTA) == 0
            before = broker_snapshot(broker)
            await asyncio.sleep(1.5 * TICK)
            assert broker_snapshot(broker) == before, (
                "the reconciler touched a capacity-free intent"
            )
            retry = await deliver(client, agent, headers)
            assert retry.status_code == 200, retry.text
            assert retry.json()["stream_id"] == intent["reserved_stream_id"]

    run(scenario)


@pytest.mark.parametrize("fault", ["unset", "enqueue_missing", "manifest_invalid"])
def test_unset_setting_or_invalid_file_performs_no_broker_io(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-SOURCE-6."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch, configure=fault != "unset") as (
            _app,
            _client,
            agent,
            directory,
        ):
            rt = provision(broker, agent, directory)
            if fault == "enqueue_missing":
                rt.files["enqueue.json"] = None
            elif fault == "manifest_invalid":
                rt.files["manifest.json"] = b"{}"
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            interrupt(broker, rt, agent, "delivery-1")
            before, opened = broker_snapshot(broker), connections(broker)
            await asyncio.sleep(2.5 * TICK)
            assert connections(broker) == opened, "an idle reconciler opened the broker"
            assert broker_snapshot(broker) == before

    run(scenario)


@asynccontextmanager
async def second_process() -> AsyncIterator[Any]:
    """Another API process on the same stores and runtime directory, @spec PROTECTED-HOOK-LANE-4."""
    app = create_app()
    async with app.router.lifespan_context(app):
        yield app


def test_two_reconcilers_never_double_append_or_refund(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            async with second_process():
                intents = [interrupt(broker, rt, agent, f"race-{i}") for i in range(4)]
                expired = interrupt(broker, rt, agent, "expired")
                now = broker_now_ms(broker)
                expired.update(created_at_ms=str(now - 300001), deadline_ms=str(now - 1))
                broker.command("SET", admission_key("intent", agent, "expired"), canonical(expired))
                await until(
                    lambda: (
                        all(committed(broker, agent, f"race-{i}") for i in range(4))
                        and (state(broker, agent, "expired") or {}).get("status") == "failed"
                    ),
                    4 * TICK,
                    "two reconcilers did not finish every preparing intent",
                )
                await asyncio.sleep(TICK)
            ids = [entry[0].decode() for entry in private_entries(broker)]
            assert sorted(ids) == sorted(i["reserved_stream_id"] for i in intents)
            assert broker.command("ZCARD", QUOTA) == 4
            assert (state(broker, agent, "expired") or {})["reason"] == "deadline"

    run(scenario)


def test_lifespan_joins_the_reconciler_within_ten_seconds_with_a_paused_broker(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reconciler runs under the lifespan and stops within ten seconds on shutdown.

    @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker
    pause_ms = 14000

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        app = None
        directory = tmp_path / "runtime-shutdown"
        from test_hook_source_support_broker import use_runtime_dir

        use_runtime_dir(monkeypatch, directory)
        app = create_app()
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        paused_at = None
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                created = await client.post(
                    "/agents",
                    headers={"X-API-Key": get_settings().api_key},
                    json={
                        "name": "acme-shutdown-" + uuid.uuid4().hex,
                        "channel": {
                            "kind": "email",
                            "address": "support@example.test",
                            "endpoint": "http://adapter.example.test",
                            "adapter": "mail",
                        },
                    },
                )
                assert created.status_code == 201, created.text
                agent = created.json()["id"]
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            interrupt(broker, rt, agent, "first")
            await until(
                lambda: committed(broker, agent, "first"),
                3 * TICK,
                "no reconciler ran under the API lifespan",
            )
            interrupt(broker, rt, agent, "second")
            broker.command("CLIENT", "PAUSE", str(pause_ms), "ALL")
            paused_at = time.monotonic()
            await asyncio.sleep(TICK + 1)
        finally:
            started = time.monotonic()
            await lifespan.__aexit__(None, None, None)
            elapsed = time.monotonic() - started
            if paused_at is not None:
                await asyncio.sleep(max(0.0, pause_ms / 1000 - (time.monotonic() - paused_at)) + 1)
        assert elapsed < 11, f"lifespan shutdown took {elapsed:.1f}s with a paused broker"

    run(scenario)


def test_tick_log_carries_counts_and_no_identity_payload_or_credential(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-LANE-4."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with captured_logs() as logs:
            async with ingress_app(tmp_path, monkeypatch) as (_app, _client, agent, directory):
                rt = provision(broker, agent, directory)
                await asyncio.to_thread(seed_row, agent, rt)
                install(broker, rt)
                await asyncio.to_thread(admit, broker, rt, agent, "parked")
                interrupt(broker, rt, agent, "delivery-1")
                await until(
                    lambda: committed(broker, agent, "delivery-1"),
                    3 * TICK,
                    "the reconciler did not recover the intent",
                )
                await asyncio.sleep(TICK + 1)
        ticks = [line for line in logs.lines if "quota" in line and "parked" in line]
        assert ticks, "no reconciler tick log reported quota occupancy and parked counts"
        secrets_ = [*forbidden(broker, rt), agent, "delivery-1", "anonymous prompt"]
        for line in ticks:
            for value in secrets_:
                assert value not in line, "a tick log carries an identity, payload or credential"

    run(scenario)


# -- review round: tick ending and shutdown cancellation -----------------------------------


def test_a_connection_lost_during_recover_ends_the_tick(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A connection loss inside ``recover`` ends the tick: no further recover on that tick,
    and the tick is logged as broker_unavailable, never ``outcome=ok``.

    Real fault: the owned broker pauses writes, so the first recover's script
    waits, and the fixture then kills the enqueue principal's connection. The
    recover count is a test side pass-through spy. No app runs, so no other
    reconciler races this one. @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5.
    """
    import threading

    from curie_api.protected_reconciler import ProtectedAdmissionReconciler

    broker = ingress_broker
    agent = str(uuid.uuid4())
    directory = tmp_path / "runtime-tick"
    rt = provision(broker, agent, directory)
    install(broker, rt)
    for index in range(3):
        interrupt(broker, rt, agent, f"lost-{index}")
    calls: list[int] = []
    entered = threading.Event()
    original = AtomicAdmission.recover

    def counted(self: Any, identity: Any) -> Any:
        """@spec PROTECTED-HOOK-LANE-4."""
        calls.append(1)
        entered.set()
        return original(self, identity)

    monkeypatch.setattr(AtomicAdmission, "recover", counted)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        reconciler = ProtectedAdmissionReconciler(lambda: str(directory))
        broker.command("CLIENT", "PAUSE", "4000", "WRITE")
        try:
            async with captured_logs() as logs:
                tick = asyncio.create_task(reconciler.tick())
                await asyncio.to_thread(entered.wait, 3)
                await asyncio.sleep(0.5)
                broker.command("CLIENT", "KILL", "USER", broker.enqueue.username)
                result = await asyncio.wait_for(tick, 10)
        finally:
            broker.command("CLIENT", "UNPAUSE")
            await reconciler.stop()
        assert len(calls) == 1, f"the tick kept calling recover on a lost connection: {len(calls)}"
        outcomes = [line for line in logs.lines if "reconciler tick outcome=" in line]
        assert not any("outcome=ok" in line for line in outcomes), outcomes
        assert any("outcome=broker_unavailable" in line for line in outcomes), outcomes
        assert result is None

    run(scenario)


def test_cancelling_shutdown_while_the_reconciler_stops_propagates(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancellation of the lifespan's own shutdown is not swallowed by ``stop``.

    @spec PROTECTED-HOOK-LANE-4.
    """
    from curie_api.protected_reconciler import ProtectedAdmissionReconciler

    broker = ingress_broker
    agent = str(uuid.uuid4())
    directory = tmp_path / "runtime-stop"
    install(broker, provision(broker, agent, directory))

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        reconciler = ProtectedAdmissionReconciler(lambda: str(directory))
        reconciler.start()
        await asyncio.sleep(0)
        stopping = asyncio.create_task(reconciler.stop())
        # Let stop() reach its join of the reconciler task, then cancel the shutdown.
        for _ in range(3):
            await asyncio.sleep(0)
        stopping.cancel()
        try:
            await asyncio.wait_for(stopping, 15)
        except asyncio.CancelledError:
            return
        pytest.fail("stop() swallowed the cancellation of the shutdown", pytrace=False)

    run(scenario)
