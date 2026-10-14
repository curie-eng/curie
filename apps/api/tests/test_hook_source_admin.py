"""Source administration service, @spec PROTECTED-HOOK-SOURCE-3/5/6/10."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import get_settings
from curie_api.hook_source_policy_schemas import HookSourcePolicyOut
from curie_protected_hooks.source_policy_sql import SourceGate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

HOOK = "daily-summary"
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


@pytest.fixture
def admin_agent(isolated_migration_db: IsolatedMigrationDb) -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    isolated_migration_db.at("head")
    agent = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents(id,name,hook_generation) VALUES (:id,'admin-test',7)",
        {"id": agent},
    )
    return agent


def admin_module() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-3/6."""
    assert importlib.util.find_spec("curie_api.hook_source_admin") is not None, (
        "PROTECTED-HOOK-SOURCE-3/6: unavailable administration service is not implemented"
    )
    return importlib.import_module("curie_api.hook_source_admin")


@asynccontextmanager
async def service(module: Any) -> AsyncIterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/3."""
    gate = create_async_engine(
        get_settings().database_url, pool_size=2, max_overflow=0, pool_timeout=2
    )
    work = create_async_engine(
        get_settings().database_url, pool_size=1, max_overflow=0, pool_timeout=2
    )
    try:
        yield module.SourceAdminService(SourceGate(gate), work), gate, work
    finally:
        await gate.dispose()
        await work.dispose()


def state(agent: uuid.UUID) -> tuple[Any, Any, Any]:
    """@spec PROTECTED-HOOK-SOURCE-5/10."""
    return (
        sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:id", {"id": agent}),
        sql_dicts(
            "SELECT * FROM curie.hook_source_policies WHERE agent_id=:id ORDER BY hook",
            {"id": agent},
        ),
        sql_dicts(
            "SELECT * FROM curie.hook_source_operations "
            "WHERE agent_id=:id ORDER BY hook,generation",
            {"id": agent},
        ),
    )


def seed(
    agent: uuid.UUID,
    *,
    protected: bool = False,
    pending: bool = False,
    policy: bool = True,
    generation: int = 9,
) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    target = PROTECTED if protected else ORDINARY
    operation = str(uuid.uuid4())
    intent = hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:op,:intent,:status,:gen)",
        dict(
            agent=agent,
            hook=HOOK,
            op=operation,
            intent=intent,
            status="pending" if pending else "committed",
            gen=generation,
        ),
    )
    if policy:
        sql_dicts(
            "INSERT INTO curie.hook_source_policies "
            "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
            "qualification_id,bundle_digest,legacy_generation) VALUES "
            "(:agent,:hook,:op,:gen,:mode,:tool_access,:runtime_id,:qualification_id,:bundle_digest,7)",
            dict(agent=agent, hook=HOOK, op=operation, gen=generation, **target),
        )
    return operation


def assert_error(error: Any, status: int) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    assert error.status_code == status
    assert re.fullmatch(r"[a-z][a-z0-9_]{0,62}", error.code)
    assert error.committed_generation is None
    assert "postgres" not in str(error).lower()
    assert "secret-input" not in str(error)


# -- one service path: the route operations only -----------------------------------------------

ROUTE_OPERATIONS = {"read_policy", "refuse_secret"}
RETIRED_ANSWERS = {"get_policy", "mutate", "remove", "rotate", "read_secret"}


def test_service_exposes_only_the_route_operations() -> None:
    """One service path serves every caller: no retired internal answer remains.

    The amended SOURCE-3 removes the earlier unrestricted read, the always
    unavailable mutations and the always unavailable secret read rather than
    keeping them beside the route operations, so a later CLI or console caller
    cannot inherit a retired answer. Mutations go through the coordinator.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10.
    """
    module = admin_module()
    public = {
        name
        for name in dir(module.SourceAdminService)
        if not name.startswith("_") and callable(getattr(module.SourceAdminService, name))
    }
    assert public & RETIRED_ANSWERS == set(), sorted(public & RETIRED_ANSWERS)
    assert public == ROUTE_OPERATIONS


def mutation_module() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    return importlib.import_module("curie_api.hook_source_mutation")


@asynccontextmanager
async def coordinator() -> AsyncIterator[Any]:
    """The route mutation path with no resolver composed, @spec PROTECTED-HOOK-SOURCE-3/10."""
    gate = create_async_engine(
        get_settings().database_url, pool_size=2, max_overflow=0, pool_timeout=2
    )
    work = create_async_engine(
        get_settings().database_url, pool_size=1, max_overflow=0, pool_timeout=2
    )
    try:
        yield mutation_module().SourceMutationCoordinator(SourceGate(gate), work)
    finally:
        await gate.dispose()
        await work.dispose()


class Evaluation:
    """Caller supplied tombstone evaluation that proves the gate and transaction ended.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """

    def __init__(self, gate: Any, work: Any, answer: tuple[str, str | None]) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        self.gate, self.work, self.answer = gate, work, answer
        self.calls: list[Any] = []

    async def __call__(self, policy: Any) -> tuple[str, str | None]:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        self.calls.append(policy)
        assert self.work.pool.checkedout() == 0, "evaluation ran inside the request transaction"
        async with asyncio.timeout(2):
            async with SourceGate(self.gate).hold(uuid.UUID(str(policy.agent_id))):
                pass
        return self.answer


@pytest.mark.parametrize("kind", ["absent", "pending", "ordinary", "protected"])
@pytest.mark.parametrize("answer", [("active", None), ("closed", "source_closed")])
def test_read_reports_the_route_dto_with_locked_counter(
    admin_agent: uuid.UUID, kind: str, answer: tuple[str, str | None]
) -> None:
    """GET's DTO: no row closed (null or pending_history); tombstone and protected evaluated.

    A tombstone and a protected row are evaluated, once, after the gate and the
    transaction ended, and the activation is the evaluation's (A3: a protected
    row reports ``active`` on the tombstone's rule, ``publication_deferred`` is
    retired). legacy_generation is the
    locked agent counter. @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10.
    """
    module = admin_module()
    if kind != "absent":
        seed(
            admin_agent,
            protected=kind == "protected",
            pending=kind == "pending",
            policy=kind != "pending",
        )
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with service(module) as (admin, gate, work):
            evaluation = Evaluation(gate, work, answer)
            result = await admin.read_policy(str(admin_agent), HOOK, evaluation)
            assert isinstance(result, HookSourcePolicyOut)
            assert result.agent_id == str(admin_agent) and result.hook == HOOK
            assert result.legacy_generation == "7"
            assert result.generation == ("0" if kind in ("absent", "pending") else "9")
            assert result.mode == ("protected" if kind == "protected" else "ordinary")
            assert len(evaluation.calls) == (1 if kind in ("ordinary", "protected") else 0)
            if kind in ("ordinary", "protected"):
                assert (result.activation, result.refusal_reason) == answer
            else:
                assert result.activation == "closed"
                assert (
                    result.refusal_reason
                    == {
                        "absent": None,
                        "pending": "pending_history",
                    }[kind]
                )
            assert (result.updated_at is None) == (kind in ("absent", "pending"))
            assert "secret" not in result.model_dump()

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


@pytest.mark.parametrize("kind", ["absent", "pending", "ordinary", "protected"])
def test_secret_refusals_are_the_route_answers(admin_agent: uuid.UUID, kind: str) -> None:
    """Absent, history only and tombstone 409 source_not_protected; an unpublished protected
    row 503 with a closed reason (``source_closed``, ``runtime_unavailable`` or
    ``broker_unavailable``), never the retired ``source_publication_deferred``.

    Always a refusal with a null committed generation, never a key, no write.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    module = admin_module()
    if kind != "absent":
        seed(
            admin_agent,
            protected=kind == "protected",
            pending=kind == "pending",
            policy=kind != "pending",
        )
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with service(module) as (admin, _gate, _work):
            with pytest.raises(module.SourceAdminError) as caught:
                await admin.refuse_secret(str(admin_agent), HOOK)
            assert_error(caught.value, 503 if kind == "protected" else 409)
            if kind == "protected":
                assert caught.value.code in (
                    "source_closed",
                    "runtime_unavailable",
                    "broker_unavailable",
                ), caught.value.code
            else:
                assert caught.value.code == "source_not_protected"

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


@pytest.mark.parametrize("case", ["agent", "hook", "expected", "operation", "target"])
def test_malformed_mutation_is_422_before_any_change(admin_agent: uuid.UUID, case: str) -> None:
    """The route mutation path validates before the gate, @spec PROTECTED-HOOK-SOURCE-3/10."""
    args: list[Any] = [str(admin_agent), HOOK, "0", str(uuid.uuid4()), dict(PROTECTED)]
    args[["agent", "hook", "expected", "operation", "target"].index(case)] = (
        dict(PROTECTED, extra="secret-input")
        if case == "target"
        else "secret-input/"
        if case == "hook"
        else "secret-input"
    )
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with coordinator() as mutations:
            with pytest.raises(admin_module().SourceAdminError) as caught:
                await mutations.mutate(*args)
            assert_error(caught.value, 422)

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


@pytest.mark.parametrize(
    "case", ["stale", "pending", "historical", "different_intent", "current_retry"]
)
def test_cas_and_operation_history_refuse_without_allocating(
    admin_agent: uuid.UUID, case: str
) -> None:
    """Stale CAS and operation history are 409; an exact retry names its committed generation.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """
    module = admin_module()
    old = seed(
        admin_agent,
        pending=case == "pending",
        policy=case not in ("pending", "historical"),
        generation=8,
    )
    if case == "historical":
        seed(admin_agent, generation=9)
    expected = (
        "9"
        if case == "historical"
        else "0"
        if case in ("pending", "current_retry", "stale")
        else "8"
    )
    operation = str(uuid.uuid4()) if case == "stale" else old
    target = PROTECTED if case == "different_intent" else ORDINARY
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with coordinator() as mutations:
            with pytest.raises(module.SourceAdminError) as caught:
                await mutations.mutate(str(admin_agent), HOOK, expected, operation, target)
            error = caught.value
            if case == "current_retry":
                # The retried operation committed: its 503 names that generation, never a key.
                assert error.status_code == 503 and error.committed_generation == "8"
            else:
                assert_error(error, 409)

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


async def _never(_policy: Any) -> tuple[str, str | None]:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    raise AssertionError("evaluation reached without a readable tombstone")


@pytest.mark.parametrize("case", ["missing_agent", "invalid_counter", "inconsistent_policy"])
def test_unknown_or_corrupt_authoritative_state_refuses(admin_agent: uuid.UUID, case: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
    module = admin_module()
    agent = str(uuid.uuid4()) if case == "missing_agent" else str(admin_agent)
    if case == "invalid_counter":
        sql_dicts("UPDATE curie.agents SET hook_generation=-1 WHERE id=:id", {"id": admin_agent})
    elif case == "inconsistent_policy":
        seed(admin_agent, pending=True)
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
        async with service(module) as (admin, _gate, _work):
            for call in (
                admin.read_policy(agent, HOOK, _never),
                admin.refuse_secret(agent, HOOK),
            ):
                with pytest.raises(module.SourceAdminError) as caught:
                    await call
                assert_error(caught.value, 404 if case == "missing_agent" else 503)
                assert caught.value.code == (
                    "source_agent_not_found"
                    if case == "missing_agent"
                    else "source_state_unavailable"
                )

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


def test_read_uses_current_agent_counter_without_rewriting_policy(admin_agent: uuid.UUID) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/5/6."""
    module = admin_module()
    seed(admin_agent, protected=True)
    sql_dicts(
        "UPDATE curie.hook_source_policies SET legacy_generation=3 WHERE agent_id=:id",
        {"id": admin_agent},
    )
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6."""
        async with service(module) as (admin, _gate, _work):
            evaluation = Evaluation(_gate, _work, ("active", None))
            result = await admin.read_policy(str(admin_agent), HOOK, evaluation)
            assert result.legacy_generation == "7"
            assert len(evaluation.calls) == 1
            assert (result.activation, result.refusal_reason) == ("active", None)

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before
    assert before[1][0]["legacy_generation"] == 3


@pytest.mark.parametrize("existing", [False, True])
def test_rotate_requires_existing_protected_policy(admin_agent: uuid.UUID, existing: bool) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/10."""
    module = admin_module()
    if existing:
        seed(admin_agent)
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with coordinator() as mutations:
            with pytest.raises(module.SourceAdminError) as caught:
                await mutations.rotate(
                    str(admin_agent), HOOK, "9" if existing else "0", str(uuid.uuid4())
                )
            assert_error(caught.value, 409)
            assert caught.value.code == "source_rotation_conflict"

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


def test_admin_waits_on_gate_without_checking_out_work_connection(admin_agent: uuid.UUID) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/3."""
    module = admin_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/3."""
        async with service(module) as (admin, gate, work):
            observer = create_async_engine(get_settings().database_url)
            try:
                async with SourceGate(gate).hold(admin_agent):
                    task = asyncio.create_task(admin.read_policy(str(admin_agent), HOOK, _never))
                    try:
                        async with asyncio.timeout(5):
                            while True:
                                async with observer.connect() as conn:
                                    waiting = await conn.scalar(
                                        text(
                                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                            "WHERE datname=current_database() "
                                            "AND wait_event='advisory')"
                                        )
                                    )
                                if waiting:
                                    break
                                await asyncio.sleep(0.01)
                        assert not task.done()
                        async with work.connect() as conn:
                            assert await conn.scalar(text("SELECT 1")) == 1
                    except BaseException:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        raise
                result = await asyncio.wait_for(task, 5)
                assert result.generation == "0" and result.activation == "closed"
            finally:
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 12))


def test_constructor_rejects_untyped_resolver_input(admin_agent: uuid.UUID) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/6."""
    module = admin_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with service(module) as (_admin, gate, work):
            with pytest.raises(TypeError):
                module.SourceAdminService(SourceGate(gate), work, resolver=object())

    asyncio.run(asyncio.wait_for(scenario(), 10))


@pytest.mark.parametrize("case", ["legacy_exhausted", "ledger_exhausted", "replay_exhausted"])
def test_exhaustion_refuses_fresh_allocation_but_not_current_replay(
    admin_agent: uuid.UUID, case: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
    module = admin_module()
    operation = str(uuid.uuid4())
    target = PROTECTED
    if case == "legacy_exhausted":
        sql_dicts(
            "UPDATE curie.agents SET hook_generation=2147483647 WHERE id=:id", {"id": admin_agent}
        )
    else:
        old = seed(
            admin_agent,
            pending=case == "ledger_exhausted",
            policy=case == "replay_exhausted",
            generation=2**63 - 1,
        )
        if case == "replay_exhausted":
            operation = old
            target = ORDINARY
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/10."""
        async with coordinator() as mutations:
            with pytest.raises(module.SourceAdminError) as caught:
                await mutations.mutate(str(admin_agent), HOOK, "0", operation, target)
            if case == "replay_exhausted":
                assert caught.value.status_code == 503
                assert caught.value.committed_generation == str(2**63 - 1)
            else:
                assert_error(caught.value, 409)

    asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state(admin_agent) == before


@pytest.mark.parametrize("change", ["replace", "delete"])
def test_mutation_captures_target_before_waiting_on_gate(
    admin_agent: uuid.UUID, change: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/3/10."""
    module = admin_module()
    before = state(admin_agent)

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/3/10."""
        gate = create_async_engine(
            get_settings().database_url, pool_size=2, max_overflow=0, pool_timeout=2
        )
        work = create_async_engine(
            get_settings().database_url, pool_size=1, max_overflow=0, pool_timeout=2
        )
        mutations = mutation_module().SourceMutationCoordinator(SourceGate(gate), work)
        target = dict(PROTECTED)
        observer = create_async_engine(get_settings().database_url)
        task = None
        try:
            async with SourceGate(gate).hold(admin_agent):
                task = asyncio.create_task(
                    mutations.mutate(str(admin_agent), HOOK, "0", str(uuid.uuid4()), target)
                )
                async with asyncio.timeout(5):
                    while True:
                        async with observer.connect() as conn:
                            waiting = await conn.scalar(
                                text(
                                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                    "WHERE datname=current_database() "
                                    "AND wait_event='advisory')"
                                )
                            )
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
                assert not task.done()
                if change == "replace":
                    target["mode"] = "secret-input"
                    target["runtime_id"] = "secret-input"
                else:
                    del target["runtime_id"]
            with pytest.raises(module.SourceAdminError) as caught:
                await asyncio.wait_for(task, 5)
            # The captured protected target reached the (absent) resolver, not the edit.
            assert_error(caught.value, 503)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await observer.dispose()
            await gate.dispose()
            await work.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 12))
    assert state(admin_agent) == before


def test_constructor_refuses_gate_pool_and_alias_before_checkout(admin_agent: uuid.UUID) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/3."""
    module = admin_module()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/3."""
        async with service(module) as (_admin, gate, _work):
            alias = gate.execution_options(isolation_level="READ COMMITTED")
            assert alias.pool is gate.pool
            for work_engine in (gate, alias):
                assert gate.pool.checkedout() == 0
                with pytest.raises(module.SourceAdminError) as caught:
                    module.SourceAdminService(SourceGate(gate), work_engine)
                assert_error(caught.value, 503)
                assert gate.pool.checkedout() == 0

    asyncio.run(asyncio.wait_for(scenario(), 10))
