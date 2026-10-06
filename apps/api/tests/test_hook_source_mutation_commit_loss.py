"""Real COMMIT response loss, @spec PROTECTED-HOOK-SOURCE-2/6/10.

External trust is fake. PostgreSQL sessions, wire loss and Valkey scripts are real.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from _source_commit_relay import CommitResponseLossRelay
from curie_api.config import get_settings
from curie_api.hook_source_admin import SourceAdminError
from curie_protected_hooks.source_fence import SourceFence
from curie_protected_hooks.source_policy_records import policy_fingerprint
from curie_protected_hooks.source_policy_sql import SourceGate, SourcePolicySnapshot
from curie_test_support.valkey import NO_RETRY, connect_or_skip
from redis import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

HOOK = "commit-loss"
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
        "SOURCE-10 mutation coordinator COMMIT-loss handling is missing"
    )
    return importlib.import_module("curie_api.hook_source_mutation")


@pytest.fixture
def wire_source(isolated_migration_db: IsolatedMigrationDb) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    isolated_migration_db.at("head")
    agent = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.agents(id,name,hook_generation) VALUES (:a,'wire-test',3)",
        {"a": uuid.UUID(agent)},
    )
    admin = connect_or_skip(decode_responses=True)
    key = f"protected:source:{agent}:{HOOK}"
    users = ["wire_reader_" + uuid.uuid4().hex, "wire_writer_" + uuid.uuid4().hex]
    clients = []
    try:
        for index, user in enumerate(users):
            password = uuid.uuid4().hex
            commands = ["+ping", "+auth", "+client|setinfo", "+get"]
            if index:
                commands += ["+set", "+eval"]
            admin.execute_command(
                "ACL", "SETUSER", user, "reset", "on", ">" + password, "~" + key, "-@all", *commands
            )
            options = {
                name: admin.connection_pool.connection_kwargs[name]
                for name in ("host", "port", "db")
            }
            client = Redis(
                **options,
                username=user,
                password=password,
                decode_responses=True,
                retry=NO_RETRY,
                socket_timeout=2,
                socket_connect_timeout=2,
            )
            client.ping()
            clients.append(client)
        yield dict(agent=agent, admin=admin, key=key, clients=clients)
    finally:
        for client in clients:
            client.close()
        admin.delete(key)
        for user in users:
            admin.execute_command("ACL", "DELUSER", user)
        assert admin.get(key) is None
        admin.close()


class FakeExternalAuthorityForCommitLoss:
    """TEST ONLY external trust boundary, @spec PROTECTED-HOOK-SOURCE-6/10."""

    def __init__(self, module: Any, fixture: Any, relay: Any, phase: str | None) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        self.module, self.fixture, self.relay, self.phase = module, fixture, relay, phase
        self.reader = SourceFence(fixture["clients"][0])
        self.writer = SourceFence(fixture["clients"][1])
        self.reserve_calls = 0
        self.publish_calls = 0
        self.closed = 0

    @asynccontextmanager
    async def resolve(
        self, source: Any, target: Any, *, durable_generation_highwater: int
    ) -> AsyncIterator[Any]:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        assert source.agent_id == self.fixture["agent"] and source.hook == HOOK
        self.highwater = durable_generation_highwater
        try:
            yield self.module.SourceControlSession(
                source=source, target=target, reader=self, writer=self
            )
        finally:
            self.closed += 1

    async def read_reconciled_floor(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        state = await asyncio.to_thread(self.reader.read, self.fixture["agent"], HOOK)
        assert state["floor"] >= self.highwater
        if self.phase == "registration":
            self.relay.arm()
            self.phase = None
        return state["floor"]

    async def reserve(self, *, expected_floor: int, operation_id: str, min_generation: int) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        self.reserve_calls += 1
        actual = await asyncio.to_thread(
            self.writer.reserve_and_revoke,
            self.fixture["agent"],
            HOOK,
            expected_floor,
            operation_id,
            min_generation,
        )
        if self.phase == "authoritative":
            self.relay.arm()
            self.phase = None
        return actual

    async def publish_ordinary(
        self, *, generation: int, operation_id: str, fingerprint: str
    ) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-6/7/10."""
        self.publish_calls += 1
        return await asyncio.to_thread(
            self.writer.publish_ordinary,
            self.fixture["agent"],
            HOOK,
            generation,
            operation_id,
            fingerprint,
        )


@asynccontextmanager
async def wire_engines() -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    original = make_url(get_settings().database_url)
    # @spec PROTECTED-HOOK-SOURCE-10: exact owned clone from fixture, not a request host.
    relay = CommitResponseLossRelay(original.host, original.port or 5432)
    port = await relay.start()
    gate = create_async_engine(original, pool_size=2, max_overflow=0, pool_timeout=2)
    observer = create_async_engine(original, pool_size=1, max_overflow=0, pool_timeout=2)
    work = create_async_engine(
        original.set(host="127.0.0.1", port=port),
        pool_size=1,
        max_overflow=0,
        pool_timeout=2,
        connect_args={"ssl": "prefer", "timeout": 3},
    )
    try:
        yield relay, gate, work, observer
    finally:
        await work.dispose()
        await gate.dispose()
        await observer.dispose()
        await relay.close()
        assert not relay.tasks and not relay.writers and not relay.errors


async def state(observer: Any, fixture: Any) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-5/10."""
    async with observer.connect() as connection:
        params = {"a": uuid.UUID(fixture["agent"])}
        counter = await connection.scalar(
            text("SELECT hook_generation FROM curie.agents WHERE id=:a"), params
        )
        policy = (
            (
                await connection.execute(
                    text("SELECT * FROM curie.hook_source_policies WHERE agent_id=:a"), params
                )
            )
            .mappings()
            .all()
        )
        ledger = (
            (
                await connection.execute(
                    text(
                        "SELECT * FROM curie.hook_source_operations "
                        "WHERE agent_id=:a ORDER BY generation"
                    ),
                    params,
                )
            )
            .mappings()
            .all()
        )
        return counter, policy, ledger


async def error(call: Any, status: int, committed: str | None = None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    with pytest.raises(SourceAdminError) as caught:
        await call
    assert caught.value.status_code == status
    assert caught.value.committed_generation == committed
    assert str(caught.value) == caught.value.code


def run(call: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    asyncio.run(asyncio.wait_for(call, 20))


def test_transparent_relay_real_commit_error_can_leave_durable_sql(wire_source: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        async with wire_engines() as (relay, _, work, observer):
            async with observer.begin() as connection:
                await connection.execute(
                    text("CREATE TABLE curie.wire_control(marker int PRIMARY KEY)")
                )
            async with work.begin() as connection:
                await connection.execute(text("INSERT INTO curie.wire_control VALUES (1)"))
            async with work.connect() as connection:
                transaction = await connection.begin()
                await connection.execute(text("INSERT INTO curie.wire_control VALUES (2)"))
                relay.arm()
                with pytest.raises(DBAPIError) as caught:
                    async with asyncio.timeout(5):
                        await transaction.commit()
                assert caught.value.connection_invalidated
            await asyncio.wait_for(relay.dropped.wait(), 2)
            async with observer.connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM curie.wire_control")) == 2
            assert relay.ssl_rejected >= 1 and not relay.errors

    run(scenario())


def test_registration_commit_response_loss_consumes_pending_without_broker_continuation(
    wire_source: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        async with wire_engines() as (relay, gate, work, observer):
            boundary = FakeExternalAuthorityForCommitLoss(
                module, wire_source, relay, "registration"
            )
            service = module.SourceMutationCoordinator(
                SourceGate(gate), work, authority_resolver=boundary
            )
            operation = str(uuid.uuid4())
            await error(service.mutate(wire_source["agent"], HOOK, "0", operation, PROTECTED), 503)
            await asyncio.wait_for(relay.dropped.wait(), 2)
            counter, policy, ledger = await state(observer, wire_source)
            assert counter == 3 and not policy and len(ledger) == 1
            assert ledger[0]["generation"] == 1 and ledger[0]["status"] == "pending"
            assert str(ledger[0]["operation_id"]) == operation
            assert boundary.reserve_calls == boundary.publish_calls == 0 and boundary.closed == 1
            assert await asyncio.to_thread(wire_source["admin"].get, wire_source["key"]) is None
            await error(service.mutate(wire_source["agent"], HOOK, "0", operation, PROTECTED), 409)
            # @spec PROTECTED-HOOK-SOURCE-7/10: explicitly fake EXTERNAL recovery setup.
            await asyncio.to_thread(
                wire_source["admin"].set,
                wire_source["key"],
                json.dumps(dict(floor="1", operation_id=operation, active=None)),
            )
            await error(
                service.mutate(wire_source["agent"], HOOK, "0", str(uuid.uuid4()), PROTECTED),
                503,
                "2",
            )
            counter, policy, ledger = await state(observer, wire_source)
            assert counter == 4 and policy[0]["generation"] == 2
            assert [(row["generation"], row["status"]) for row in ledger] == [
                (1, "pending"),
                (2, "committed"),
            ]
            assert boundary.reserve_calls == 1 and boundary.publish_calls == 0

    run(scenario())


def test_authoritative_ordinary_commit_response_loss_never_publishes_until_exact_replay(
    wire_source: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
    module = product()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/10."""
        async with wire_engines() as (relay, gate, work, observer):
            boundary = FakeExternalAuthorityForCommitLoss(
                module, wire_source, relay, "authoritative"
            )
            service = module.SourceMutationCoordinator(
                SourceGate(gate), work, authority_resolver=boundary
            )
            operation = str(uuid.uuid4())
            await error(service.remove(wire_source["agent"], HOOK, "0", operation), 503)
            await asyncio.wait_for(relay.dropped.wait(), 2)
            before = await state(observer, wire_source)
            counter, policy, ledger = before
            assert counter == 3 and policy[0]["generation"] == 1 and policy[0]["mode"] == "ordinary"
            assert ledger[0]["status"] == "committed" and len(ledger) == 1
            broker = await asyncio.to_thread(boundary.reader.read, wire_source["agent"], HOOK)
            assert broker == dict(floor=1, operation_id=operation, active=None)
            assert (
                boundary.reserve_calls == 1 and boundary.publish_calls == 0 and boundary.closed == 1
            )
            replay = await service.remove(wire_source["agent"], HOOK, "0", operation)
            assert isinstance(replay, SourcePolicySnapshot) and replay.generation == 1
            assert await state(observer, wire_source) == before
            assert boundary.reserve_calls == 1 and boundary.publish_calls == 1
            broker = await asyncio.to_thread(boundary.reader.read, wire_source["agent"], HOOK)
            record = dict(policy[0])
            for field in ("agent_id", "operation_id", "generation", "legacy_generation"):
                record[field] = str(record[field])
            assert broker["active"] == dict(
                generation=1,
                operation_id=operation,
                mode="ordinary",
                policy_fingerprint=policy_fingerprint(record),
            )

    run(scenario())
