"""Protected ingress admission over real HTTP.

@spec PROTECTED-HOOK-SOURCE-2/8 @spec PROTECTED-HOOK-LANE-4.

A signed delivery to a published protected source is admitted onto the owned
disposable TLS broker through the real signed hook route: one intent, binding,
quota member and private stream entry carrying the exact turn, an immutable
receipt with the requested and effective policy, the source generation and the
body digest, and nothing in the ordinary Valkey or the database. Duplicates,
conflicts and refusals answer as the SOURCE-8 result table states. A published
tombstone restores ordinary delivery only while its ordinary publication is
active. See ``_protected_ingress_harness`` for the stores and principals.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts
from _protected_ingress_harness import (
    BODY,
    QUOTA,
    QUOTA_LIMIT,
    TURN_LIMIT,
    _broker,
    admission_key,
    assert_detail,
    assert_turn,
    broker_snapshot,
    canonical,
    conversation,
    deliver,
    event_id,
    expire_readiness,
    forbidden,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    ordinary_claim_key,
    private_entries,
    protected_receipt,
    provision,
    record,
    seed_row,
    set_admission_open,
    signed,
    stamp_now,
)
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from test_hook_source_admin_routes import (
    captured_logs,
    connections,
    publish_tombstone,
    source_key,
)
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    BODIES,
    GENERATION,
    HOOK,
    effects,
    expected,
    legacy_secret,
    post_support,
    rotate_protected,
    scoped_secret,
    seed,
    support_db,
    support_headers,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
TIMEOUT = 60


def run(scenario: Any, timeout: float = TIMEOUT) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    asyncio.run(asyncio.wait_for(scenario(), timeout))


async def ordinary_state(app: Any, agent: str) -> Any:
    """Ordinary Valkey keys, stream length and SQL records, @spec PROTECTED-HOOK-SOURCE-8."""
    return await effects(app, agent)


# -- accepted delivery and receipt -------------------------------------------------------


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_signed_protected_delivery_is_admitted_once_with_its_receipt(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: str | None
) -> None:
    """One intent, binding, quota member and private entry; nothing ordinary or in SQL.

    The receipt carries the requested and effective policy, the source
    generation and the stream ID; the commit record carries the body digest.
    The turn is the exact protected turn with the signed timestamp.
    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            before = await ordinary_state(app, agent)
            stamp = stamp_now()
            headers = signed(scoped_secret(agent), requested=requested, stamp=stamp)
            async with captured_logs() as logs:
                response = await deliver(client, agent, headers, requested=requested)
            assert response.status_code == 200, response.text
            entries = private_entries(broker)
            assert len(entries) == 1, "not exactly one private stream entry"
            stream_id = entries[0][0].decode()
            assert response.json() == protected_receipt(
                agent, "delivery-1", requested=requested, stream_id=stream_id
            )
            fields = entries[0][1]
            assert fields[0] == b"payload" and fields[2] == b"protected_envelope"
            assert_turn(fields[1], agent, "delivery-1", stamp)
            envelope = json.loads(fields[3])
            assert envelope["event_id"] == event_id(agent, "delivery-1")
            assert envelope["source_revision"] == str(GENERATION)
            assert envelope["logical_conversation_key"] == conversation(agent)
            assert envelope["payload_sha256"] == hashlib.sha256(fields[1]).hexdigest()
            commit = record(broker, admission_key("commit", agent, "delivery-1"))
            assert commit is not None and commit["status"] == "committed"
            receipt = commit["receipt"]
            assert receipt["request_body_sha256"] == hashlib.sha256(BODY).hexdigest()
            assert receipt["requested_tool_access"] == requested
            assert receipt["effective_tool_access"] == "read-only"
            assert receipt["source_generation"] == str(GENERATION)
            assert receipt["stream_id"] == stream_id
            assert broker.command("ZCARD", QUOTA) == 1
            assert broker.command("EXISTS", "protected:admission:binding:" + receipt["event_id"])
            assert await ordinary_state(app, agent) == before, (
                "a protected delivery wrote an ordinary claim, slot, entry or SQL row"
            )
            for text in (response.text, *logs.lines):
                for value in forbidden(broker, rt):
                    assert value not in text, "a response or log carries credential material"
            for text in logs.lines:
                assert "protected example" not in text, "a log carries the payload"

    run(scenario)


@pytest.mark.parametrize("retry", ["exact", "freshly_signed", "after_closure"])
def test_retries_return_the_original_receipt_without_a_second_entry(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, retry: str
) -> None:
    """Exact, freshly signed and post closure retries answer the original receipt.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            stamp = str(int(time.time()) - 2)
            headers = signed(scoped_secret(agent), stamp=stamp)
            first = await deliver(client, agent, headers)
            assert first.status_code == 200, first.text
            if retry == "exact":
                again = headers
            else:
                again = signed(scoped_secret(agent), stamp=stamp_now())
                assert again != headers
            if retry == "after_closure":
                set_admission_open(broker, rt, False)
                expire_readiness(broker, rt)
            before = broker_snapshot(broker)
            response = await deliver(client, agent, again)
            assert response.status_code == 200, response.text
            assert response.json() == {**first.json(), "duplicate": True}
            assert broker_snapshot(broker) == before
            assert len(private_entries(broker)) == 1

    run(scenario)


@pytest.mark.parametrize("change", ["whitespace_body", "requested_policy", "generation"])
def test_changed_body_policy_or_generation_conflicts(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """A validly signed retry of one delivery ID with another tuple is 409 delivery_conflict.

    @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            target = dict(mode="protected", tool_access="read-only", **rt.row)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            first = await deliver(client, agent, signed(scoped_secret(agent)))
            assert first.status_code == 200, first.text
            body, requested, key = BODY, None, scoped_secret(agent)
            if change == "whitespace_body":
                body = BODY.replace(b":", b": ")
            elif change == "requested_policy":
                requested = "read-only"
            else:
                await asyncio.to_thread(rotate_protected, agent, target, GENERATION + 1)
                operation = sql_dicts(
                    "SELECT operation_id FROM curie.hook_source_policies WHERE agent_id=:a",
                    {"a": agent},
                )[0]["operation_id"]
                rt.operation = str(operation)
                fresh = rt.source_record(floor=str(GENERATION + 1))
                # The rotated row's fingerprint, published as the writer would.
                from curie_protected_hooks.source_policy_records import policy_fingerprint

                fresh["active"]["policy_fingerprint"] = policy_fingerprint(
                    dict(
                        agent_id=agent,
                        hook=HOOK,
                        generation=str(GENERATION + 1),
                        operation_id=rt.operation,
                        legacy_generation="0",
                        **target,
                    )
                )
                broker.command("SET", source_key(agent), canonical(fresh))
                key = scoped_secret(agent, GENERATION + 1)
            before = broker_snapshot(broker)
            response = await deliver(
                client,
                agent,
                signed(key, requested=requested, body=body, stamp=stamp_now()),
                requested=requested,
                body=body,
            )
            assert_detail(response, 409, "delivery_conflict")
            assert broker_snapshot(broker) == before

    run(scenario)


def test_resending_one_signed_request_restores_deleted_recovery_bytes(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupted admission whose recovery bytes were deleted commits on an exact resend.

    The owned fault denies the enqueue principal's XADD so the script stops
    after its intent, state, quota, binding and recovery writes; the fixture then
    deletes the recovery bytes. Resending the identical signed request is
    byte identical (the turn's received_at is the signed timestamp) and commits
    the original receipt. @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-LANE-4
    @spec PROTECTED-HOOK-ADMISSION-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            headers = signed(scoped_secret(agent))
            broker.enqueue.deny(broker, "xadd")
            interrupted = await deliver(client, agent, headers)
            assert_detail(interrupted, 503, "broker_unavailable")
            intent = record(broker, admission_key("intent", agent, "delivery-1"))
            assert intent is not None, "the interrupted admission left no intent"
            broker.command("DEL", admission_key("recovery", agent, "delivery-1"))
            broker.enqueue.restore(broker)
            response = await deliver(client, agent, headers)
            assert response.status_code == 200, response.text
            assert response.json() == protected_receipt(
                agent, "delivery-1", requested=None, stream_id=intent["reserved_stream_id"]
            )
            entries = private_entries(broker)
            assert len(entries) == 1 and entries[0][0].decode() == intent["reserved_stream_id"]
            assert not broker.command("EXISTS", admission_key("recovery", agent, "delivery-1"))

    run(scenario)


# -- ordinary claims across both stores --------------------------------------------------


@pytest.mark.parametrize("held", ["enqueued", "pending"])
def test_prior_ordinary_claim_refuses_without_broker_write(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: str
) -> None:
    """An enqueued ordinary claim is 409; a pending one 503 with the ordinary lease.

    @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            value = "1-0" if held == "enqueued" else "pending:" + "0" * 32
            await app.state.valkey.set(ordinary_claim_key(agent, "delivery-1"), value)
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(client, agent, signed(scoped_secret(agent)))
            if held == "enqueued":
                assert_detail(response, 409, "delivery_conflict")
            else:
                assert_detail(response, 503, "ordinary_delivery_pending")
                assert response.headers.get("retry-after") == str(
                    get_settings().channel_delivery_lease_s
                )
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


# -- result table refusals ---------------------------------------------------------------


def _readiness_expired(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    expire_readiness(broker, rt)


def _closed(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    set_admission_open(broker, rt, False)


def _source_absent(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    broker.command("DEL", source_key(rt.agent))


def _selection_absent(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-8."""
    from _protected_ingress_harness import control_key

    broker.command("DEL", control_key(rt, "selection"))


def _quota_full(broker: Any, rt: Any) -> None:
    """Capacity held by other deliveries, @spec PROTECTED-HOOK-LANE-4."""
    for index in range(QUOTA_LIMIT):
        broker.command("ZADD", QUOTA, index + 1, hashlib.sha256(str(index).encode()).hexdigest())


def _enqueue_disabled(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    broker.command("ACL", "SETUSER", broker.enqueue.username, "off")


REFUSALS = [
    ("evidence_expired", _readiness_expired, 503, "evidence_unavailable"),
    ("admission_closed", _closed, 503, "admission_closed"),
    ("source_absent", _source_absent, 503, "source_unavailable"),
    ("selection_absent", _selection_absent, 503, "runtime_unavailable"),
    ("quota_full", _quota_full, 429, "protected_backlog_full"),
    ("enqueue_disabled", _enqueue_disabled, 503, "broker_unavailable"),
]


@pytest.mark.parametrize(
    "fault,status,detail", [c[1:] for c in REFUSALS], ids=[c[0] for c in REFUSALS]
)
def test_each_refusal_row_answers_as_tabled_with_an_unchanged_broker(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: Any,
    status: int,
    detail: str,
) -> None:
    """Refused and unavailable admission results map to the SOURCE-8 table.

    ``quota_full`` is 429 ``protected_backlog_full`` with no ``Retry-After``.
    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            fault(broker, rt)
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(client, agent, signed(scoped_secret(agent)))
            assert_detail(response, status, detail)
            if status == 429:
                assert "retry-after" not in response.headers
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


@pytest.mark.parametrize("target", ["both", "conversation_only"])
def test_explicit_reply_target_is_422_before_broker_io(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    """@spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-SOURCE-8."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            params = {"conversation_id": "conversation/example"}
            if target == "both":
                params["placeholder"] = "placeholder/example"
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(client, agent, signed(scoped_secret(agent)), params=params)
            assert_detail(response, 422, "protected_reply_target_unsupported")
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


@pytest.mark.parametrize("bound_hook", [HOOK, "other-hook"])
def test_declared_source_bindings_refuse_configuration_unsupported(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound_hook: str
) -> None:
    """Source bindings on any hook of the agent exclude protected delivery.

    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            await bind_sources(client, agent, bound_hook)
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(client, agent, signed(scoped_secret(agent)))
            assert_detail(response, 503, "configuration_unsupported")
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


async def bind_sources(client: Any, agent: str, hook: str) -> None:
    """Declare one source binding through the real agent route, @spec PROTECTED-HOOK-LANE-4."""
    response = await client.patch(
        f"/agents/{agent}",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "source_bindings": {
                hook: {
                    "workload_pointer": "/message",
                    "map": {
                        "example": {
                            "repository": "example-org/example-repo",
                            "revision": "0123456789abcdef0123456789abcdef01234567",
                        }
                    },
                }
            }
        },
    )
    assert response.status_code == 200, response.text


def test_oversize_turn_is_413_with_no_broker_io(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A body within the hook bound whose turn exceeds 262144 bytes refuses before broker I/O.

    @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-2.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            body = json.dumps({"message": "<" * (TURN_LIMIT // 3)}).encode()
            assert len(body) < get_settings().hook_max_body_bytes
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(
                client, agent, signed(scoped_secret(agent), body=body), body=body
            )
            assert response.status_code == 413, response.text
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


@pytest.mark.parametrize("fault", ["unset", "enqueue_missing", "enqueue_unbound"])
def test_unset_or_invalid_runtime_files_are_runtime_unavailable_without_broker_io(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-8."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch, configure=fault != "unset") as (
            app,
            client,
            agent,
            directory,
        ):
            rt = provision(broker, agent, directory)
            if fault == "enqueue_missing":
                rt.files["enqueue.json"] = None
            elif fault == "enqueue_unbound":
                from _protected_ingress_harness import enqueue_file

                rt.files["enqueue.json"] = enqueue_file(
                    broker, {"id": "credential/example-enqueue", "generation": "2"}
                )
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            opened = connections(broker)
            response = await deliver(client, agent, signed(scoped_secret(agent)))
            assert_detail(response, 503, "runtime_unavailable")
            assert connections(broker) == opened, "an invalid runtime still opened the broker"
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


def test_stale_signature_after_rotation_stays_401(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-4."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            target = dict(mode="protected", tool_access="read-only", **rt.row)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            await asyncio.to_thread(rotate_protected, agent, target, GENERATION + 1)
            before = broker_snapshot(broker)
            response = await deliver(client, agent, signed(scoped_secret(agent)))
            assert response.status_code == 401, response.text
            assert broker_snapshot(broker) == before

    run(scenario)


# -- concurrency -------------------------------------------------------------------------


def test_concurrent_deliveries_of_one_id_produce_one_entry(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """24 concurrent deliveries of one ID: one entry, only tabled 200, 202 and 503 answers.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            headers = signed(scoped_secret(agent))
            responses = await asyncio.gather(*(deliver(client, agent, headers) for _ in range(24)))
            statuses = sorted({response.status_code for response in responses})
            assert set(statuses) <= {200, 202, 503}, [r.text for r in responses]
            accepted = [r.json() for r in responses if r.status_code == 200]
            assert accepted, "no concurrent delivery was accepted"
            assert sum(not body["duplicate"] for body in accepted) <= 1
            assert len({body["stream_id"] for body in accepted}) == 1
            for response in responses:
                if response.status_code == 503:
                    assert response.json() in (
                        {"detail": "broker_unavailable"},
                        {"detail": "authority_unavailable"},
                    ), response.text
            assert len(private_entries(broker)) == 1

    run(scenario)


def test_reconciler_tick_racing_a_signed_retry_leaves_one_entry_and_the_original_receipt(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A preparing delivery recovered by the reconciler while signed retries arrive.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-LANE-4 @spec PROTECTED-HOOK-ADMISSION-5.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-LANE-4."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            headers = signed(scoped_secret(agent))
            broker.enqueue.deny(broker, "xadd")
            assert_detail(await deliver(client, agent, headers), 503, "broker_unavailable")
            intent = record(broker, admission_key("intent", agent, "delivery-1"))
            assert intent is not None
            broker.enqueue.restore(broker)
            answers = []
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                fresh = signed(scoped_secret(agent), stamp=stamp_now())
                answers.append(await deliver(client, agent, fresh))
                await asyncio.sleep(0.25)
            for response in answers:
                assert response.status_code in (200, 202), response.text
                if response.status_code == 200:
                    assert response.json()["stream_id"] == intent["reserved_stream_id"]
            assert answers[-1].status_code == 200
            entries = private_entries(broker)
            assert len(entries) == 1 and entries[0][0].decode() == intent["reserved_stream_id"]

    run(scenario)


# -- gate waits --------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["protected", "never_configured"])
def test_protected_gate_wait_is_bounded_and_ordinary_waits_unbounded(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """A protected waiter answers 503 authority_unavailable within the five second bound;
    a never configured waiter keeps waiting past it and then enqueues.

    @spec PROTECTED-HOOK-SOURCE-2.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory)
            if kind == "protected":
                await asyncio.to_thread(seed_row, agent, rt)
                key = scoped_secret(agent)
            else:
                key = legacy_secret(agent)
            install(broker, rt)
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    started = time.monotonic()
                    task = asyncio.create_task(deliver(client, agent, signed(key)))
                    if kind == "protected":
                        done, _ = await asyncio.wait({task}, timeout=9)
                        if not done:
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                            pytest.fail("a protected gate waiter was not bounded", pytrace=False)
                        assert 4.0 <= time.monotonic() - started, "the bound was not a wait"
                        assert_detail(task.result(), 503, "authority_unavailable")
                        return
                    await asyncio.sleep(6.5)
                    assert not task.done(), "an ordinary gate waiter was bounded"
                response = await asyncio.wait_for(task, 10)
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["source_generation"] is None
                assert body["acceptance_status"] == "accepted"
            finally:
                await observer.dispose()

    run(scenario)


# -- never configured and tombstone ----------------------------------------------------


def test_never_configured_hook_keeps_its_answers_and_opens_no_broker(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ordinary receipts gain requested, effective, null generation and acceptance status.

    The enqueue file is absent so the reconciler idles without broker I/O.
    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-2.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            rt.files["enqueue.json"] = None
            install(broker, rt)
            opened = connections(broker)
            for requested in (None, "read-only"):
                delivery = f"never-{requested}"
                headers = signed(legacy_secret(agent), requested=requested, delivery=delivery)
                first = await deliver(client, agent, headers, requested=requested)
                assert first.status_code == 200, first.text
                body = first.json()
                assert body["duplicate"] is False and body["stream_id"]
                assert body["requested_tool_access"] == requested
                assert body["effective_tool_access"] == requested
                assert body["tool_access"] == requested
                assert body["source_generation"] is None
                assert body["acceptance_status"] == "accepted"
                again = await deliver(client, agent, headers, requested=requested)
                assert again.status_code == 200, again.text
                assert again.json()["duplicate"] is True
                assert again.json()["stream_id"] == body["stream_id"]
                assert again.json()["source_generation"] is None
            assert connections(broker) == opened, "a never configured hook opened the broker"
            assert private_entries(broker) == []

    run(scenario)


def _seed_tombstone(broker: Any, agent: str, published: bool) -> None:
    """Committed ordinary row at GENERATION, optionally published, @spec PROTECTED-HOOK-SOURCE-6."""
    seed(agent, "ordinary")
    operation = str(
        sql_dicts(
            "SELECT operation_id FROM curie.hook_source_policies WHERE agent_id=:a", {"a": agent}
        )[0]["operation_id"]
    )
    if published:
        publish_tombstone(broker, agent, GENERATION, operation)


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_published_tombstone_restores_ordinary_delivery(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: str | None
) -> None:
    """A tombstone whose ordinary publication is active enqueues ordinarily with its generation.

    The probe still answers source_closed for it. @spec PROTECTED-HOOK-SOURCE-8
    @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-9.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            await asyncio.to_thread(_seed_tombstone, broker, agent, True)
            before = broker_snapshot(broker)
            headers = signed(legacy_secret(agent), requested=requested)
            response = await deliver(client, agent, headers, requested=requested)
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["duplicate"] is False and body["stream_id"]
            assert body["requested_tool_access"] == requested
            assert body["effective_tool_access"] == requested
            assert body["source_generation"] == str(GENERATION)
            assert body["acceptance_status"] == "accepted"
            assert await app.state.valkey.xlen(get_settings().runs_stream) == 1
            assert broker_snapshot(broker) == before, "tombstone ingress wrote to the broker"
            duplicate = await deliver(client, agent, headers, requested=requested)
            assert duplicate.status_code == 200, duplicate.text
            assert duplicate.json()["duplicate"] is True
            assert duplicate.json()["source_generation"] is None
            opened = connections(broker)
            probe = await post_support(
                client,
                agent,
                BODIES[requested],
                support_headers(legacy_secret(agent), requested=requested, body=BODIES[requested]),
            )
            assert probe.status_code == 503, probe.text
            assert probe.json() == expected(requested, requested, str(GENERATION), "source_closed")
            assert connections(broker) == opened, "the probe read the broker for a tombstone"

    run(scenario)


@pytest.mark.parametrize("fault", ["unpublished", "private_intent", "enqueue_disabled", "unset"])
def test_tombstone_ingress_refuses_without_ordinary_effects(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Unpublished is 503 source_closed, a private intent 409, a broker failure 503
    broker_unavailable, an unset setting 503 runtime_unavailable; no ordinary effect.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-2.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch, configure=fault != "unset") as (
            app,
            client,
            agent,
            directory,
        ):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            await asyncio.to_thread(_seed_tombstone, broker, agent, fault != "unpublished")
            if fault == "private_intent":
                broker.command(
                    "SET", admission_key("intent", agent, "delivery-1"), canonical({"held": True})
                )
            elif fault == "enqueue_disabled":
                broker.command("ACL", "SETUSER", broker.enqueue.username, "off")
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            response = await deliver(client, agent, signed(legacy_secret(agent)))
            want = {
                "unpublished": (503, "source_closed"),
                "private_intent": (409, "delivery_conflict"),
                "enqueue_disabled": (503, "broker_unavailable"),
                "unset": (503, "runtime_unavailable"),
            }[fault]
            assert_detail(response, *want)
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)


@pytest.mark.parametrize("kind", ["pending", "historical"])
def test_history_without_a_row_stays_closed(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Pending history keeps 503 pending_history and opens no broker connection.

    @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-SOURCE-10.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-8."""
        async with ingress_app(tmp_path, monkeypatch) as (app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            rt.files["enqueue.json"] = None
            install(broker, rt)
            await asyncio.to_thread(seed, agent, kind)
            before = broker_snapshot(broker), await ordinary_state(app, agent)
            opened = connections(broker)
            response = await deliver(client, agent, signed(legacy_secret(agent)))
            assert response.status_code == 503, response.text
            if kind == "pending":
                assert response.json() == {"detail": "pending_history"}
            assert connections(broker) == opened
            assert (broker_snapshot(broker), await ordinary_state(app, agent)) == before

    run(scenario)
