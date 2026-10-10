"""#1461, #3621: the worker hands the runner a channel memory ref and a writes flag.

``CURIE_CHANNEL_MEMORY_REF`` is the binding-scoped memory namespace
(``.../agents/<id>/state/bindings/<kind>/<address>/memory``). The runner reads
channel facts from it, so it is set whenever the turn names a binding (kind and
address) and is not memory-isolated (eval), whether memory writes are on or
off. Reading memory needs no switch (#3621).

``CURIE_MEMORY_WRITES`` (#3659) is the switch for the remember, update and
forget tools. The worker sends it explicitly, ``1`` or ``0`` from the agent's
``memory_writes`` setting, whenever it sends a channel ref, and never without
one. DB-free: ``boot_env`` is exercised on a bare resolver, like
``test_eval_memory_isolation.py``.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid

import pytest
from curie_worker import binding
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_KEY = "CURIE_CHANNEL_MEMORY_REF"
_WRITES = "CURIE_MEMORY_WRITES"


def _resolved(**overrides: object) -> ResolvedDeployment:
    fields: dict[str, object] = {
        "agent_name": "test-agent",
        "agent_id": _AGENT,
        "version_id": uuid.UUID("22222222-2222-4222-8222-222222222222"),
        "version_label": "v1",
        "bundle_ref": "bundles/x.zip",
        "max_usd_per_day": None,
        "max_output_tokens_per_run": None,
    }
    fields.update(overrides)
    return ResolvedDeployment(**fields)  # type: ignore[arg-type]


def _boot_env(resolved: ResolvedDeployment, thread_key: str = "thread-1", **kw: object):
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()
    return resolver.boot_env(resolved, thread_key, **kw)  # type: ignore[arg-type]


def _base() -> str:
    return WorkerConfig().runner_facing_api_base_url.rstrip("/")


def test_memory_writes_defaults_to_off() -> None:
    assert _resolved().memory_writes is False


def test_writes_on_with_a_binding_passes_the_channel_memory_ref() -> None:
    env = _boot_env(_resolved(memory_writes=True), kind="slack", address="C0123")
    assert env[_KEY] == f"{_base()}/agents/{_AGENT}/state/bindings/slack/C0123/memory"
    # The agent-wide memory ref still rides beside it.
    assert env["CURIE_MEMORY_REF"] == f"{_base()}/agents/{_AGENT}/state/memory"


def test_kind_and_address_are_url_quoted() -> None:
    env = _boot_env(_resolved(memory_writes=True), kind="mail", address="ops/team@example.com")
    assert env[_KEY] == (
        f"{_base()}/agents/{_AGENT}/state/bindings/mail/ops%2Fteam%40example.com/memory"
    )


def test_writes_off_still_passes_the_channel_memory_ref() -> None:
    # #3621: turning writes off must not hide the channel's stored facts.
    env = _boot_env(_resolved(memory_writes=False), kind="slack", address="C0123")
    assert env[_KEY] == f"{_base()}/agents/{_AGENT}/state/bindings/slack/C0123/memory"
    # The same ref a writes-on turn gets.
    on = _boot_env(_resolved(memory_writes=True), kind="slack", address="C0123")
    assert env[_KEY] == on[_KEY]


@pytest.mark.parametrize(("writes", "flag"), [(True, "1"), (False, "0")], ids=["on", "off"])
def test_memory_writes_is_sent_explicitly_with_the_channel_ref(writes: bool, flag: str) -> None:
    env = _boot_env(_resolved(memory_writes=writes), kind="slack", address="C0123")
    assert _KEY in env
    assert env[_WRITES] == flag


def test_the_default_agent_sends_the_ref_with_writes_off() -> None:
    # memory_writes defaults to off; the default agent still reads channel memory.
    env = _boot_env(_resolved(), kind="slack", address="C0123")
    assert _KEY in env
    assert env[_WRITES] == "0"


@pytest.mark.parametrize("writes", [True, False], ids=["writes-on", "writes-off"])
@pytest.mark.parametrize(
    "kw",
    [{}, {"kind": "slack"}, {"address": "C0123"}],
    ids=["no-binding", "kind-only", "address-only"],
)
def test_no_binding_omits_the_channel_memory_ref(kw: dict[str, str], writes: bool) -> None:
    env = _boot_env(_resolved(memory_writes=writes), **kw)
    assert _KEY not in env
    assert _WRITES not in env
    # Guard against a vacuous pass: the same agent with a binding gets both.
    bound = _boot_env(_resolved(memory_writes=writes), kind="slack", address="C0123")
    assert _KEY in bound
    assert _WRITES in bound


@pytest.mark.parametrize("writes", [True, False], ids=["writes-on", "writes-off"])
def test_an_isolated_turn_omits_the_channel_memory_ref(writes: bool) -> None:
    agent = _resolved(memory_writes=writes)
    bound = _boot_env(agent, kind="slack", address="C0123")
    assert _KEY in bound
    assert _WRITES in bound
    for env in (
        _boot_env(agent, kind="slack", address="C0123", isolate_memory=True),
        # The legacy eval thread prefix isolates the same way.
        _boot_env(agent, "eval:1720000000.000100", kind="slack", address="C0123"),
    ):
        assert _KEY not in env
        assert _WRITES not in env


def test_the_memory_scoped_agent_shape_does_not_change_the_ref() -> None:
    # `memory=True` widens CURIE_STATE_URL to agent-wide; channel memory is a
    # separate decision and stays binding-scoped.
    env = _boot_env(_resolved(memory_writes=True, memory=True), kind="slack", address="C0123")
    assert env[_KEY] == f"{_base()}/agents/{_AGENT}/state/bindings/slack/C0123/memory"


# --- F1: memory_writes is read apart from deployment resolution --------------
#
# Resolution runs in migration tests against schemas that predate the column
# (migration 0068), so, as runner_resources did in 2f76e6283, the value comes
# from its own read. DB-free: a fake engine answers that read.


def test_resolver_statements_do_not_select_memory_writes() -> None:
    for sql in (binding._RESOLVE_SQL, binding._RESOLVE_AGENT_SQL):
        assert "memory_writes" not in sql


class _Result:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def first(self) -> tuple[object, ...] | None:
        return self._row


class _Conn:
    def __init__(self, engine: _Engine) -> None:
        self._engine = engine

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, sql: object, params: dict[str, object] | None = None) -> _Result:
        self._engine.statements.append((str(sql), dict(params or {})))
        return _Result(self._engine.row)


class _Engine:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row
        self.statements: list[tuple[str, dict[str, object]]] = []

    def connect(self) -> _Conn:
        return _Conn(self)


def _reader(row: tuple[object, ...] | None) -> tuple[BindingResolver, _Engine]:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()
    resolver._tenant_id = binding.DEFAULT_TENANT_ID
    engine = _Engine(row)
    resolver._engine = engine  # type: ignore[assignment]
    return resolver, engine


@pytest.mark.parametrize(
    ("row", "expected"),
    [((True,), True), ((False,), False), ((None,), False), (None, False)],
    ids=["on", "off", "null", "no-agent-row"],
)
def test_memory_writes_for_reads_the_agent_setting(
    row: tuple[object, ...] | None, expected: bool
) -> None:
    resolver, engine = _reader(row)
    assert asyncio.run(resolver.memory_writes_for(_AGENT)) is expected
    [(sql, params)] = engine.statements
    assert "memory_writes" in sql
    assert f"{WorkerConfig().db_schema}.agents" in sql
    assert _AGENT in params.values()
    assert "tenant_id = :tenant_id" in sql
    assert params["tenant_id"] == binding.DEFAULT_TENANT_ID


def test_memory_writes_for_is_called_on_the_turn_path() -> None:
    # The separate read is only useful if a turn uses it: the definition plus at
    # least one call site across the resolver and the kernel.
    from curie_worker.kernel import lifecycle as kernel

    sources = inspect.getsource(binding) + inspect.getsource(kernel)
    assert sources.count("memory_writes_for(") >= 2
