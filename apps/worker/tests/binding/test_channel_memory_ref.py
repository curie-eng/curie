"""#1461: the worker hands the runner a channel memory ref only when writes are on.

``CURIE_CHANNEL_MEMORY_REF`` is the binding-scoped memory namespace
(``.../agents/<id>/state/bindings/<kind>/<address>/memory``). The runner mounts
the remember/update/forget tools only when it is set, so this ref IS the on/off
switch the sandbox sees. It is set iff the agent's ``memory_writes`` setting is
on, the turn names a binding (kind and address), and the turn is not
memory-isolated (eval). DB-free: ``boot_env`` is exercised on a bare resolver,
like ``test_eval_memory_isolation.py``.
"""

from __future__ import annotations

import uuid

import pytest
from curie_worker import binding
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_KEY = "CURIE_CHANNEL_MEMORY_REF"


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


def test_writes_off_omits_the_channel_memory_ref() -> None:
    env = _boot_env(_resolved(), kind="slack", address="C0123")
    assert _KEY not in env
    # Guard against a vacuous pass: the same call with writes on does set it.
    assert _KEY in _boot_env(_resolved(memory_writes=True), kind="slack", address="C0123")


@pytest.mark.parametrize(
    "kw",
    [{}, {"kind": "slack"}, {"address": "C0123"}],
    ids=["no-binding", "kind-only", "address-only"],
)
def test_no_binding_omits_the_channel_memory_ref(kw: dict[str, str]) -> None:
    assert _KEY not in _boot_env(_resolved(memory_writes=True), **kw)
    assert _KEY in _boot_env(_resolved(memory_writes=True), kind="slack", address="C0123")


def test_an_isolated_turn_omits_the_channel_memory_ref() -> None:
    on = _resolved(memory_writes=True)
    assert _KEY in _boot_env(on, kind="slack", address="C0123")
    assert _KEY not in _boot_env(on, kind="slack", address="C0123", isolate_memory=True)
    # The legacy eval thread prefix isolates the same way.
    assert _KEY not in _boot_env(on, "eval:1720000000.000100", kind="slack", address="C0123")


def test_the_memory_scoped_agent_shape_does_not_change_the_ref() -> None:
    # `memory=True` widens CURIE_STATE_URL to agent-wide; channel memory is a
    # separate decision and stays binding-scoped.
    env = _boot_env(_resolved(memory_writes=True, memory=True), kind="slack", address="C0123")
    assert env[_KEY] == f"{_base()}/agents/{_AGENT}/state/bindings/slack/C0123/memory"


def test_both_resolver_statements_select_memory_writes() -> None:
    for sql in (binding._RESOLVE_SQL, binding._RESOLVE_AGENT_SQL):
        assert "memory_writes" in sql
