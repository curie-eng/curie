"""Protected publication, GET activation and the secret's active path over HTTP.

@spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.

PUT and rotate commit the protected row, then publish its active protected
record once a reader bracketed evaluation in its publication phase accepts
(``accept`` or ``admission_closed``); otherwise they answer 503 with the
committed generation and the outcome code, leaving only the reservation. GET
reports a published protected row ``active`` and the secret route serves the
scoped key only for it, with ``no-store``. Real Postgres, the app Valkey and
the owned disposable TLS broker with distinct reader, writer and enqueue
principals; see ``_protected_ingress_harness``.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import pytest
from _protected_ingress_harness import (
    _broker,
    control_key,
    deliver,
    expire_readiness,
    forbidden,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    provision,
    refresh_readiness,
    set_admission_open,
    signed,
)
from curie_api.config import get_settings
from curie_protected_hooks.source_policy_records import policy_fingerprint
from test_hook_source_admin_routes import (
    ORDINARY_TARGET,
    PROTECTED_TARGET,
    assert_dto,
    assert_refusal,
    captured_logs,
    counter,
    dto,
    get_policy,
    get_secret,
    ledger,
    publish_tombstone,
    put_policy,
    rotate_policy,
    seed_attempt,
    seed_row,
    set_counter,
    source_key,
    source_record,
)
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    HOOK,
    scoped_secret,
    support_db,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service


def run(scenario: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    asyncio.run(asyncio.wait_for(scenario(), 60))


def active_record(agent: str, generation: int, operation: str, legacy: int) -> dict[str, Any]:
    """The published active protected record for a committed row, @spec PROTECTED-HOOK-SOURCE-6."""
    fingerprint = policy_fingerprint(
        dict(
            agent_id=agent,
            hook=HOOK,
            generation=str(generation),
            operation_id=operation,
            legacy_generation=str(legacy),
            **PROTECTED_TARGET,
        )
    )
    return {
        "floor": str(generation),
        "operation_id": operation,
        "active": {
            "generation": str(generation),
            "operation_id": operation,
            "mode": "protected",
            "policy_fingerprint": fingerprint,
        },
    }


def secret_body(agent: str, generation: int) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return {
        "agent_id": agent,
        "hook": HOOK,
        "generation": str(generation),
        "secret": scoped_secret(agent, generation),
    }


def test_put_publishes_active_when_evidence_is_current(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PUT commits, publishes the active protected record and answers 200 active.

    GET then reports active and the secret route serves the scoped key with
    ``no-store``; a delivery signed with it is accepted. No key reaches a log.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            await asyncio.to_thread(set_counter, agent, 3)
            op = str(uuid.uuid4())
            async with captured_logs() as logs:
                response = await put_policy(client, agent, "0", op)
                want = dto(agent, generation="1", legacy="4", activation="active", protected=True)
                assert_dto(response, want)
                assert source_record(broker, agent) == active_record(agent, 1, op, 4)
                assert_dto(await get_policy(client, agent), want)
                secret = await get_secret(client, agent)
                assert secret.status_code == 200, secret.text
                assert secret.headers.get("cache-control") == "no-store"
                assert secret.json() == secret_body(agent, 1)
                delivered = await deliver(client, agent, signed(secret.json()["secret"]))
                assert delivered.status_code == 200, delivered.text
                assert delivered.json()["source_generation"] == "1"
            key = scoped_secret(agent, 1)
            for line in logs.lines:
                assert key not in line, "a log carries the scoped source key"
                for value in forbidden(broker, rt):
                    assert value not in line

    run(scenario)


def test_rotate_publishes_the_fresh_generation_and_revokes_the_old_key(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            first = str(uuid.uuid4())
            assert (await put_policy(client, agent, "0", first)).status_code == 200
            legacy = await asyncio.to_thread(counter, agent)
            op = str(uuid.uuid4())
            response = await rotate_policy(client, agent, "1", op)
            want = dto(
                agent, generation="2", legacy=str(legacy), activation="active", protected=True
            )
            assert_dto(response, want)
            assert source_record(broker, agent) == active_record(agent, 2, op, legacy)
            secret = await get_secret(client, agent)
            assert secret.status_code == 200, secret.text
            assert secret.json() == secret_body(agent, 2)
            stale = await deliver(client, agent, signed(scoped_secret(agent, 1)))
            assert stale.status_code == 401, stale.text

    run(scenario)


def _selection_absent(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    broker.command("DEL", control_key(rt, "selection"))


def _run_id_differs(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    rt.selection["broker_run_id"] = "0" * 40
    broker.command("SET", control_key(rt, "selection"), _broker.canonical(rt.selection))


def _qualification_absent(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    broker.command("DEL", control_key(rt, "qualification"))


def _readiness_absent(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    broker.command("DEL", control_key(rt, "readiness"))


def _readiness_expired(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    expire_readiness(broker, rt)


def _manifest_control_differs(broker: Any, rt: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    other = dict(rt.manifest, worker_image_digest="sha256:" + "9" * 64)
    broker.command("SET", control_key(rt, "manifest"), _broker.canonical(other))


OUTCOMES = [
    ("selection_absent", _selection_absent, "runtime_unavailable"),
    ("manifest_control_differs", _manifest_control_differs, "runtime_unavailable"),
    ("selection_run_id", _run_id_differs, "broker_identity_mismatch"),
    ("qualification_absent", _qualification_absent, "qualification_unavailable"),
    ("readiness_absent", _readiness_absent, "evidence_missing"),
    ("readiness_expired", _readiness_expired, "evidence_expired"),
]


@pytest.mark.parametrize("operation", ["put", "rotate"])
@pytest.mark.parametrize("fault,code", [c[1:] for c in OUTCOMES], ids=[c[0] for c in OUTCOMES])
def test_each_evaluation_outcome_refuses_with_the_committed_generation(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    fault: Any,
    code: str,
) -> None:
    """Commit happens, the reservation stays, no active record; GET reports source_closed.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-3.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            expected = "0"
            if operation == "rotate":
                assert (await put_policy(client, agent, "0", str(uuid.uuid4()))).status_code == 200
                expected = "1"
            fault(broker, rt)
            op = str(uuid.uuid4())
            committed = str(int(expected) + 1)
            if operation == "put":
                response = await put_policy(client, agent, expected, op)
            else:
                response = await rotate_policy(client, agent, expected, op)
            assert_refusal(response, 503, code, committed)
            assert source_record(broker, agent) == {
                "floor": committed,
                "operation_id": op,
                "active": None,
            }
            assert (await asyncio.to_thread(ledger, agent))[-1][:3] == (
                op,
                int(committed),
                "committed",
            )
            legacy = await asyncio.to_thread(counter, agent)
            assert_dto(
                await get_policy(client, agent),
                dto(
                    agent,
                    generation=committed,
                    legacy=str(legacy),
                    reason="source_closed",
                    protected=True,
                ),
            )
            secret = await get_secret(client, agent)
            assert_refusal(secret, 503, "source_closed")
            assert "secret" not in secret.text

    run(scenario)


def test_closed_admission_still_publishes(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The publication phase accepts ``admission_closed``; deliveries then refuse.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            set_admission_open(broker, rt, False)
            op = str(uuid.uuid4())
            response = await put_policy(client, agent, "0", op)
            assert_dto(
                response,
                dto(agent, generation="1", legacy="1", activation="active", protected=True),
            )
            assert source_record(broker, agent) == active_record(agent, 1, op, 1)
            refused = await deliver(client, agent, signed(scoped_secret(agent, 1)))
            assert refused.status_code == 503, refused.text
            assert refused.json() == {"detail": "admission_closed"}

    run(scenario)


def test_exact_replay_publishes_a_committed_unpublished_operation(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once evidence is current, replaying the committed operation publishes it.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            _readiness_absent(broker, rt)
            op = str(uuid.uuid4())
            assert_refusal(await put_policy(client, agent, "0", op), 503, "evidence_missing", "1")
            history = await asyncio.to_thread(ledger, agent)
            refresh_readiness(broker, rt)
            replay = await put_policy(client, agent, "0", op)
            assert_dto(
                replay, dto(agent, generation="1", legacy="1", activation="active", protected=True)
            )
            assert await asyncio.to_thread(ledger, agent) == history
            assert source_record(broker, agent) == active_record(agent, 1, op, 1)

    run(scenario)


def test_lost_reservation_answers_source_reservation_lost(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            _readiness_absent(broker, rt)
            op = str(uuid.uuid4())
            assert_refusal(await put_policy(client, agent, "0", op), 503, "evidence_missing", "1")
            broker.command("DEL", source_key(agent))
            refresh_readiness(broker, rt)
            replay = await put_policy(client, agent, "0", op)
            assert_refusal(replay, 503, "source_reservation_lost", "1")
            assert source_record(broker, agent) is None

    run(scenario)


def test_published_source_stays_active_after_readiness_expires_while_delivery_refuses(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GET active and the secret attest publication only; delivery repeats the evaluation.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-8.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            op = str(uuid.uuid4())
            want = dto(agent, generation="1", legacy="1", activation="active", protected=True)
            assert_dto(await put_policy(client, agent, "0", op), want)
            expire_readiness(broker, rt)
            assert_dto(await get_policy(client, agent), want)
            secret = await get_secret(client, agent)
            assert secret.status_code == 200, secret.text
            assert secret.json() == secret_body(agent, 1)
            refused = await deliver(client, agent, signed(scoped_secret(agent, 1)))
            assert refused.status_code == 503, refused.text
            assert refused.json() == {"detail": "evidence_unavailable"}

    run(scenario)


def _record_removed(broker: Any, agent: str, rt: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    broker.command("DEL", source_key(agent))


def _reader_disabled(broker: Any, agent: str, rt: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    broker.command("ACL", "SETUSER", broker.reader.username, "off")


def _runtime_unset(broker: Any, agent: str, rt: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    monkeypatch.delenv("CURIE_PROTECTED_RUNTIME_DIR", raising=False)
    get_settings.cache_clear()


CLOSED = [
    ("record_removed", _record_removed, "source_closed"),
    ("reader_disabled", _reader_disabled, "broker_unavailable"),
    ("runtime_unset", _runtime_unset, "runtime_unavailable"),
]


@pytest.mark.parametrize("fault,code", [c[1:] for c in CLOSED], ids=[c[0] for c in CLOSED])
def test_get_and_secret_report_each_closed_reason_for_a_protected_row(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: Any, code: str
) -> None:
    """GET closed with the tombstone's reasons; the secret 503 with a null generation.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            assert (await put_policy(client, agent, "0", str(uuid.uuid4()))).status_code == 200
            fault(broker, agent, rt, monkeypatch)
            async with captured_logs() as logs:
                read = await get_policy(client, agent)
                secret = await get_secret(client, agent)
            assert_dto(read, dto(agent, generation="1", legacy="1", reason=code, protected=True))
            assert_refusal(secret, 503, code)
            for text in (read.text, secret.text, *logs.lines):
                assert scoped_secret(agent, 1) not in text

    run(scenario)


@pytest.mark.parametrize("kind", ["absent", "pending", "tombstone"])
def test_secret_for_non_protected_rows_stays_source_not_protected(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory, source=False)
            install(broker, rt)
            if kind == "pending":
                await asyncio.to_thread(seed_attempt, agent, 9, PROTECTED_TARGET, "pending")
            elif kind == "tombstone":
                op = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
                await asyncio.to_thread(publish_tombstone, broker, agent, 9, op)
            response = await get_secret(client, agent)
            assert_refusal(response, 409, "source_not_protected")
            assert "secret" not in response.json()

    run(scenario)
