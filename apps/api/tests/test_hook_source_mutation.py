"""Unwired mechanics only, @spec PROTECTED-HOOK-SOURCE-2/3/5/6/7/10.

FakeExternalAuthorityForMechanics fakes external trust, never broker writes.
No fixture qualifies a broker epoch, protected admission, or production resolver.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import get_settings
from curie_api.hook_source_admin import SourceAdminError
from curie_protected_hooks.source_fence import SourceFence
from curie_protected_hooks.source_policy_sql import SourceGate, SourcePolicySnapshot
from curie_test_support.valkey import NO_RETRY, connect_or_skip
from redis import Redis
from redis.exceptions import ResponseError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

HOOK = "mechanics"
ORDINARY = dict(
    mode="ordinary", tool_access=None, runtime_id=None, qualification_id=None, bundle_digest=None
)
PROTECTED = dict(
    mode="protected",
    tool_access="read-only",
    runtime_id=str(uuid.UUID(int=10)),
    qualification_id=str(uuid.UUID(int=11)),
    bundle_digest="a" * 64,
)


def product() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    assert importlib.util.find_spec("curie_api.hook_source_mutation") is not None, (
        "SOURCE-3/10 internal mutation coordinator is missing"
    )
    return importlib.import_module("curie_api.hook_source_mutation")


def sql_state(agent: str) -> tuple[Any, Any, Any]:
    """@spec PROTECTED-HOOK-SOURCE-5/10."""
    return (
        sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:a", {"a": uuid.UUID(agent)}),
        sql_dicts(
            "SELECT * FROM curie.hook_source_policies WHERE agent_id=:a", {"a": uuid.UUID(agent)}
        ),
        sql_dicts(
            "SELECT * FROM curie.hook_source_operations WHERE agent_id=:a ORDER BY generation",
            {"a": uuid.UUID(agent)},
        ),
    )


@pytest.fixture
def mechanics(isolated_migration_db: IsolatedMigrationDb) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    isolated_migration_db.at("head")
    agent = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.agents(id,name,hook_generation) VALUES (:a,'mechanics-test',3)",
        {"a": uuid.UUID(agent)},
    )
    admin = connect_or_skip(decode_responses=True)
    key = f"protected:source:{agent}:{HOOK}"
    users = ["mechanics_reader_" + uuid.uuid4().hex, "mechanics_writer_" + uuid.uuid4().hex]
    passwords = [uuid.uuid4().hex, uuid.uuid4().hex]
    clients = []
    try:
        for index, user in enumerate(users):
            commands = ["+ping", "+auth", "+client|setinfo", "+get"]
            if index:
                commands += ["+set", "+eval"]
            admin.execute_command(
                "ACL",
                "SETUSER",
                user,
                "reset",
                "on",
                ">" + passwords[index],
                "~" + key,
                "-@all",
                *commands,
            )
            options = {
                name: admin.connection_pool.connection_kwargs[name]
                for name in ("host", "port", "db")
            }
            options["decode_responses"] = True
            options.update(
                username=user,
                password=passwords[index],
                retry=NO_RETRY,
                socket_timeout=2,
                socket_connect_timeout=2,
            )
            client = Redis(**options)
            client.ping()
            clients.append(client)
        yield dict(agent=agent, admin=admin, key=key, users=users, clients=clients)
    finally:
        for client in clients:
            client.close()
        admin.delete(key)
        for user in users:
            admin.execute_command("ACL", "DELUSER", user)
        assert admin.get(key) is None
        admin.close()


class FakeExternalAuthorityForMechanics:
    """TEST ONLY external trust substitution, @spec PROTECTED-HOOK-SOURCE-6/10."""

    def __init__(self, module: Any, fixture: Any, *, fault: str | None = None) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        self.module, self.fixture, self.fault = module, fixture, fault
        self.writer = SourceFence(fixture["clients"][1])
        self.reader = SourceFence(fixture["clients"][0])
        self.opened = asyncio.Event()
        self.release = asyncio.Event()
        self.publishing = asyncio.Event()
        self.publish_release = asyncio.Event()
        self.closed = 0
        self.publish_calls = 0
        self.resolve_calls = 0

    @asynccontextmanager
    async def resolve(
        self, source: Any, target: Any, *, durable_generation_highwater: int
    ) -> AsyncIterator[Any]:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        self.resolve_calls += 1
        assert source.agent_id == self.fixture["agent"] and source.hook == HOOK
        assert target.as_dict() in (ORDINARY, PROTECTED)
        assert type(durable_generation_highwater) is int
        if self.fault == "reference":
            raise SourceAdminError("invalid_source_reference", 422)
        try:
            self.opened.set()
            if self.fault == "wait":
                await self.release.wait()
            yield self.module.SourceControlSession(
                source=source, target=target, reader=self, writer=self
            )
        finally:
            self.closed += 1

    async def read_reconciled_floor(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        state = await asyncio.to_thread(self.reader.read, self.fixture["agent"], HOOK)
        if self.fault == "broker_denied":
            await asyncio.to_thread(
                self.fixture["admin"].execute_command,
                "ACL",
                "SETUSER",
                self.fixture["users"][1],
                "-eval",
            )
        return state["floor"]

    async def reserve(self, *, expected_floor: int, operation_id: str, min_generation: int) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        if self.fault == "reserve_conflict":
            await asyncio.to_thread(
                self.writer.reserve_and_revoke,
                self.fixture["agent"],
                HOOK,
                expected_floor,
                str(uuid.uuid4()),
                min_generation,
            )
        return await asyncio.to_thread(
            self.writer.reserve_and_revoke,
            self.fixture["agent"],
            HOOK,
            expected_floor,
            operation_id,
            min_generation,
        )

    async def publish_ordinary(
        self, *, generation: int, operation_id: str, fingerprint: str
    ) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-6/7."""
        self.publish_calls += 1
        if self.fault == "publish_wait":
            self.publishing.set()
            await self.publish_release.wait()
        return await asyncio.to_thread(
            self.writer.publish_ordinary,
            self.fixture["agent"],
            HOOK,
            generation,
            operation_id,
            fingerprint,
        )


@asynccontextmanager
async def coordinator(
    module: Any, fixture: Any, *, fault: str | None = None, unavailable: bool = False
) -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    gate = create_async_engine(
        get_settings().database_url, pool_size=2, max_overflow=0, pool_timeout=2
    )
    work = create_async_engine(
        get_settings().database_url, pool_size=1, max_overflow=0, pool_timeout=2
    )
    authority = FakeExternalAuthorityForMechanics(module, fixture, fault=fault)
    try:
        service = module.SourceMutationCoordinator(
            SourceGate(gate), work, authority_resolver=None if unavailable else authority
        )
        yield service, authority, gate, work
    finally:
        await gate.dispose()
        await work.dispose()


async def observe(fixture: Any) -> tuple[Any, Any]:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    return (
        await asyncio.to_thread(sql_state, fixture["agent"]),
        await asyncio.to_thread(fixture["admin"].get, fixture["key"]),
    )


async def rejected(call: Any, status: int, generation: str | None = None) -> SourceAdminError:
    """@spec PROTECTED-HOOK-SOURCE-3/6."""
    with pytest.raises(SourceAdminError) as caught:
        await call
    error = caught.value
    assert error.status_code == status
    assert error.committed_generation == generation
    assert str(error) == error.code and "postgres" not in str(error).lower()
    return error


def run(coroutine: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    asyncio.run(asyncio.wait_for(coroutine, 15))


def seed(
    fixture: Any,
    *,
    generation: int,
    pending: bool = False,
    protected: bool = False,
    policy: bool = True,
) -> str:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    operation = str(uuid.uuid4())
    target = PROTECTED if protected else ORDINARY
    intent = hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    params = dict(
        a=uuid.UUID(fixture["agent"]),
        h=HOOK,
        o=uuid.UUID(operation),
        g=generation,
        intent=intent,
        status="pending" if pending else "committed",
        **target,
    )
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,generation,intent_sha256,status) "
        "VALUES (:a,:h,:o,:g,:intent,:status)",
        params,
    )
    if policy:
        sql_dicts(
            "INSERT INTO curie.hook_source_policies "
            "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
            "qualification_id,bundle_digest,legacy_generation) VALUES "
            "(:a,:h,:o,:g,:mode,:tool_access,:runtime_id,:qualification_id,:bundle_digest,3)",
            params,
        )
    return operation


def trigger_abort(table: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    assert table in ("hook_source_operations", "hook_source_policies")
    sql_dicts(
        "CREATE FUNCTION curie.mechanics_abort() RETURNS trigger LANGUAGE plpgsql AS "
        "$$ BEGIN RAISE EXCEPTION 'mechanics abort' USING ERRCODE='23514'; END $$"
    )
    sql_dicts(
        f"CREATE TRIGGER mechanics_abort BEFORE INSERT OR UPDATE ON curie.{table} "
        "FOR EACH ROW EXECUTE FUNCTION curie.mechanics_abort()"
    )


def test_mechanical_clients_have_actual_separate_role_permissions(mechanics: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    product()
    reader, writer = mechanics["clients"]
    with pytest.raises(ResponseError):
        reader.set(mechanics["key"], "forbidden")
    for key in ("protected:proof:" + uuid.uuid4().hex, "protected:delivery:" + uuid.uuid4().hex):
        with pytest.raises(ResponseError):
            writer.get(key)
    assert reader.get(mechanics["key"]) is None


def test_default_unavailable_leaves_all_stores_unchanged(mechanics: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
        before = await observe(mechanics)
        async with coordinator(module, mechanics, unavailable=True) as (service, authority, _, _):
            await rejected(
                service.mutate(mechanics["agent"], HOOK, "0", str(uuid.uuid4()), PROTECTED), 503
            )
            assert authority.resolve_calls == 0
        assert await observe(mechanics) == before

    run(scenario())


@pytest.mark.parametrize("kind", ["reference", "malformed", "unknown", "stale"])
def test_refusal_precedes_pending_registration(mechanics: Any, kind: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        before = await observe(mechanics)
        async with coordinator(
            module, mechanics, fault="reference" if kind == "reference" else None
        ) as (service, authority, _, _):
            target = dict(PROTECTED)
            if kind == "malformed":
                target["runtime_id"] = "not-a-reference"
            await rejected(
                service.mutate(
                    str(uuid.uuid4()) if kind == "unknown" else mechanics["agent"],
                    HOOK,
                    "1" if kind == "stale" else "0",
                    str(uuid.uuid4()),
                    target,
                ),
                dict(reference=422, malformed=422, unknown=404, stale=409)[kind],
            )
            assert authority.resolve_calls == (1 if kind == "reference" else 0)
        assert await observe(mechanics) == before

    run(scenario())


@pytest.mark.parametrize("kind", ["pending", "historical", "changed"])
def test_operation_uuid_conflicts_never_resolve_or_write(mechanics: Any, kind: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    module = product()
    operation = seed(mechanics, generation=4, pending=kind == "pending", policy=kind != "pending")
    if kind == "historical":
        sql_dicts(
            "DELETE FROM curie.hook_source_policies WHERE agent_id=:a",
            {"a": uuid.UUID(mechanics["agent"])},
        )
        seed(mechanics, generation=5)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        before = await observe(mechanics)
        async with coordinator(module, mechanics) as (service, authority, _, _):
            await rejected(
                service.mutate(
                    mechanics["agent"],
                    HOOK,
                    "5" if kind == "historical" else "0" if kind == "pending" else "4",
                    operation,
                    PROTECTED if kind == "changed" else ORDINARY,
                ),
                409,
            )
            assert authority.resolve_calls == 0
        assert await observe(mechanics) == before

    run(scenario())


def test_enable_rotate_remove_reenable_preserves_counter_and_closed_authority(
    mechanics: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        async with coordinator(module, mechanics) as (service, authority, _, _):
            await rejected(
                service.mutate(mechanics["agent"], HOOK, "0", str(uuid.uuid4()), PROTECTED),
                503,
                "1",
            )
            state, raw = await observe(mechanics)
            assert state[0][0]["hook_generation"] == 4
            assert state[2][0]["status"] == "committed" and state[2][0]["generation"] == 1
            assert json.loads(raw)["active"] is None and authority.publish_calls == 0
            await rejected(
                service.rotate(mechanics["agent"], HOOK, "1", str(uuid.uuid4())), 503, "2"
            )
            result = await service.remove(mechanics["agent"], HOOK, "2", str(uuid.uuid4()))
            assert isinstance(result, SourcePolicySnapshot) and result.generation == 3
            state, raw = await observe(mechanics)
            assert state[0][0]["hook_generation"] == 4
            assert json.loads(raw)["active"]["mode"] == "ordinary"
            await rejected(
                service.mutate(mechanics["agent"], HOOK, "3", str(uuid.uuid4()), PROTECTED),
                503,
                "4",
            )
            state, raw = await observe(mechanics)
            assert state[0][0]["hook_generation"] == 5
            assert [row["generation"] for row in state[2]] == [1, 2, 3, 4]
            assert all(row["status"] == "committed" for row in state[2])
            assert json.loads(raw)["floor"] == "4" and json.loads(raw)["active"] is None
            assert authority.publish_calls == 1

    run(scenario())


@pytest.mark.parametrize("method", ["mutate", "remove"])
def test_current_ordinary_replay_bypasses_stale_cas_without_new_sql(
    mechanics: Any, method: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
        async with coordinator(module, mechanics) as (service, authority, _, _):
            operation = str(uuid.uuid4())
            first = await service.remove(mechanics["agent"], HOOK, "0", operation)
            before = await observe(mechanics)
            args = (mechanics["agent"], HOOK, "0", operation)
            second = await (
                service.mutate(*args, ORDINARY) if method == "mutate" else service.remove(*args)
            )
            assert second == first and authority.publish_calls == 2
            assert await observe(mechanics) == before

    run(scenario())


@pytest.mark.parametrize("kind", ["legacy", "source", "replay"])
def test_exhaustion_precedes_writes_but_not_current_exact_replay(mechanics: Any, kind: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
    module = product()
    operation = str(uuid.uuid4())
    if kind == "legacy":
        sql_dicts(
            "UPDATE curie.agents SET hook_generation=2147483647 WHERE id=:a",
            {"a": uuid.UUID(mechanics["agent"])},
        )
    else:
        operation = seed(
            mechanics, generation=2**63 - 1, pending=kind == "source", policy=kind == "replay"
        )
        if kind == "replay":
            SourceFence(mechanics["clients"][1]).reserve_and_revoke(
                mechanics["agent"], HOOK, 0, operation, 2**63 - 2
            )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
        before = await observe(mechanics)
        async with coordinator(module, mechanics) as (service, authority, _, _):
            if kind == "replay":
                result = await service.remove(mechanics["agent"], HOOK, "0", operation)
                assert result.generation == 2**63 - 1
                after, _ = await observe(mechanics)
                assert after == before[0] and authority.publish_calls == 1
            else:
                await rejected(
                    service.mutate(mechanics["agent"], HOOK, "0", str(uuid.uuid4()), PROTECTED), 409
                )
                assert await observe(mechanics) == before

    run(scenario())


@pytest.mark.parametrize("reset", [False, True])
def test_all_pending_attempts_bound_fresh_generation_after_fake_external_recovery(
    mechanics: Any, reset: bool
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1/7/10."""
    module = product()
    pending = seed(mechanics, generation=19, pending=True, policy=False)
    if reset:
        mechanics["admin"].set(mechanics["key"], "discarded-owned-epoch")
        mechanics["admin"].delete(mechanics["key"])
    # @spec PROTECTED-HOOK-SOURCE-7/10: fake EXTERNAL restoration, not runtime epoch proof.
    mechanics["admin"].set(
        mechanics["key"], json.dumps(dict(floor="19", operation_id=pending, active=None))
    )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-1/7/10."""
        async with coordinator(module, mechanics) as (service, _, _, _):
            result = await service.remove(mechanics["agent"], HOOK, "0", str(uuid.uuid4()))
            assert result.generation == 20
            state, raw = await observe(mechanics)
            assert [row["generation"] for row in state[2]] == [19, 20]
            assert state[2][0]["status"] == "pending"
            assert json.loads(raw)["floor"] == "20"

    run(scenario())


@pytest.mark.parametrize("fault", ["registration", "reserve_conflict", "broker_denied", "policy"])
def test_actual_phase_failures_preserve_exact_durable_boundary(mechanics: Any, fault: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    module = product()
    if fault in ("registration", "policy"):
        trigger_abort(
            "hook_source_operations" if fault == "registration" else "hook_source_policies"
        )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        async with coordinator(module, mechanics, fault=fault) as (service, authority, _, _):
            operation = str(uuid.uuid4())
            await rejected(service.mutate(mechanics["agent"], HOOK, "0", operation, PROTECTED), 503)
            state, raw = await observe(mechanics)
            assert not state[1] and state[0][0]["hook_generation"] == 3
            assert authority.publish_calls == 0
            if fault == "registration":
                assert not state[2] and raw is None
            else:
                assert len(state[2]) == 1 and state[2][0]["status"] == "pending"
                assert state[2][0]["generation"] == 1
                assert str(state[2][0]["operation_id"]) == operation
                if fault == "broker_denied":
                    assert raw is None
                else:
                    assert json.loads(raw)["active"] is None

    run(scenario())


def test_delayed_ordinary_cas_loses_after_new_operation_and_gate_is_released(
    mechanics: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/6/7/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/6/7/10."""
        async with coordinator(module, mechanics, fault="publish_wait") as (
            old,
            authority,
            gate,
            _,
        ):
            task = asyncio.create_task(old.remove(mechanics["agent"], HOOK, "0", str(uuid.uuid4())))
            try:
                await asyncio.wait_for(authority.publishing.wait(), 5)
                async with asyncio.timeout(2):
                    async with SourceGate(gate).hold(uuid.UUID(mechanics["agent"])):
                        pass
                async with coordinator(module, mechanics) as (new, _, _, _):
                    await rejected(
                        new.mutate(mechanics["agent"], HOOK, "1", str(uuid.uuid4()), PROTECTED),
                        503,
                        "2",
                    )
                before = await observe(mechanics)
                authority.publish_release.set()
                await rejected(task, 503, "1")
                assert await observe(mechanics) == before
                assert json.loads(before[1])["floor"] == "2"
                assert json.loads(before[1])["active"] is None
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("fault", ["cancel", "gate_loss"])
def test_boundary_wait_cancellation_or_actual_gate_loss_precedes_registration(
    mechanics: Any, fault: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        before = await observe(mechanics)
        async with coordinator(module, mechanics, fault="wait") as (service, authority, gate, work):
            task = asyncio.create_task(
                service.remove(mechanics["agent"], HOOK, "0", str(uuid.uuid4()))
            )
            try:
                await asyncio.wait_for(authority.opened.wait(), 5)
                assert work.pool.checkedout() == 0
                if fault == "cancel":
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    async with work.connect() as connection:
                        pid = await connection.scalar(
                            text(
                                "SELECT l.pid FROM pg_locks l "
                                "JOIN pg_stat_activity a ON a.pid=l.pid "
                                "WHERE a.datname=current_database() AND l.locktype='advisory' "
                                "AND l.granted"
                            )
                        )
                        assert pid is not None
                        assert await connection.scalar(
                            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                        )
                    authority.release.set()
                    await rejected(task, 503)
                assert authority.closed == 1
                assert await observe(mechanics) == before
                async with asyncio.timeout(2):
                    async with SourceGate(gate).hold(uuid.UUID(mechanics["agent"])):
                        pass
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    run(scenario())


def test_constructor_rejects_same_underlying_pool_alias_before_checkout(mechanics: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        engine = create_async_engine(get_settings().database_url, pool_size=1, max_overflow=0)
        try:
            with pytest.raises(SourceAdminError) as caught:
                module.SourceMutationCoordinator(SourceGate(engine), engine.execution_options())
            assert caught.value.status_code == 503
            assert caught.value.committed_generation is None
            assert engine.pool.checkedout() == 0
        finally:
            await engine.dispose()

    run(scenario())
