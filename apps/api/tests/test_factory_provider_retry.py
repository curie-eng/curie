"""A blocked provider gate releases only on a verified repair or allowance (#5426).

Every predecessor fails through the real paths of ``test_factory_retry.py``:
the internal admissions route, a real reconciler pass that wakes the request,
the worker's real dispatch client to acquire and start it, and the real finish
route carrying the worker's ``retry_safety`` and ``retry_condition``. The
settlement admits the blocked successor. Probe observations are written with
the real shared ledger (``register_current``, ``claim_probe``,
``record_probe``) under this test's own worker key prefix, stamped relative to
database time, so the clock is the database's. Each release pass is a newly
constructed ``WorkItemReconciler``, as after a restart. Postgres and Valkey
are real; nothing inside the API is patched.

Scope keys are low-entropy placeholders, never a digest of a real credential.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import pytest
import redis
import redis.asyncio as aredis
from curie_api.config import get_settings
from curie_api.factory_provider_gate import (
    PROBE_FRESH_S,
    GateFacts,
    GateObservation,
    clearance,
)
from curie_api.main import create_app
from curie_api.workitems import lifecycle
from curie_internal.provider_gate_ledger import (
    ScopeObservation,
    claim_probe,
    lease_key,
    read_scope,
    record_probe,
    register_current,
)
from curie_internal.provider_scope import AllowanceReading, ProviderScope
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from test_factory_retry import (
    OBJECTIVE,
    REQUESTER,
    Ref,
    _admit,
    _agent,
    _async_valkey,
    _configure,
    _db,
    _dispatch,
    _execute,
    _expire_wait,
    _finish,
    _now,
    _published_wake,
    _reconcile,
    _request,
    _requests,
    _successor,
    _used,
    _versions,
    _wake_ids,
)

# The pure clearance table below uses these fixed values. Every test that runs
# against the shared database gets its own through ``gate_api``, so a blocked gate
# another test left on the database can never match this test's ledger.
FAILED = "sha256:" + "0" * 64
REPLACED = "sha256:" + "1" * 64
OTHER = "sha256:" + "2" * 64
_OWN = "own"
OPENROUTER = "openrouter"
ANTHROPIC = "anthropic"
HOSTS = {OPENROUTER: "openrouter.ai", ANTHROPIC: "api.anthropic.com"}

GateKind = Literal["credential", "credit", "usage"]
# The finish cause, the condition reason and the block the policy derives.
KINDS: dict[GateKind, tuple[str, str, str]] = {
    "credential": ("model_credential_rejected", "credential_rejected", "provider_credential"),
    "credit": ("model_credit_exhausted", "credit_exhausted", "provider_credit"),
    "usage": ("model_usage_limited", "usage_reset", "provider_usage_reset"),
}
BASELINE_LIMIT = Decimal("10")
RAISED_LIMIT = Decimal("20")


# -- harness -----------------------------------------------------------------


@pytest.fixture
def gate_api(
    clean_db: None,
    runs_stream: str,
    valkey: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[TestClient]:
    """The real app, with a worker key prefix (the ledger's) owned by this test."""

    module = sys.modules[__name__]
    for name in ("FAILED", "REPLACED", "OTHER"):
        digest = int(hashlib.sha256(f"{request.node.nodeid}:{name}".encode()).hexdigest()[:30], 16)
        monkeypatch.setattr(module, name, f"sha256:{digest:064d}")
    _configure(monkeypatch)
    prefix = f"test:curie:provider-gate:{uuid.uuid4().hex}"
    monkeypatch.setenv("KEY_PREFIX", prefix)
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    for key in valkey.scan_iter(match=f"{prefix}:*"):
        valkey.delete(key)
    get_settings.cache_clear()


def _ledger[T](body: Callable[[aredis.Redis, str], Awaitable[T]]) -> T:
    async def go() -> T:
        client = _async_valkey()
        try:
            return await body(client, get_settings().worker_key_prefix)
        finally:
            await client.aclose()

    return asyncio.run(go())


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def _register(scope_key: str, at: datetime, provider: str = OPENROUTER) -> None:
    """A worker tick: ``scope_key`` is the provider's current scope at ``at``."""

    scope = ProviderScope(provider=provider, host=HOSTS[provider], scope_key=scope_key)
    _ledger(lambda client, prefix: register_current(client, prefix, scope, _ms(at)))


def _reading(
    outcome: Literal["authenticated", "rejected", "unreadable"] = "authenticated",
    *,
    limit: Decimal | None = None,
    remaining: Decimal | None = None,
) -> AllowanceReading:
    return AllowanceReading(outcome=outcome, limit_remaining=remaining, limit=limit)


def _probe(
    scope_key: str, at: datetime, reading: AllowanceReading, provider: str = OPENROUTER
) -> None:
    """One leased probe of ``scope_key`` recorded at ``at``."""

    async def body(client: aredis.Redis, prefix: str) -> bool:
        token = await claim_probe(client, prefix, provider, scope_key, "probe-test")
        assert token is not None
        return await record_probe(client, prefix, provider, scope_key, token, _ms(at), reading)

    assert _ledger(body)


def _observation(scope_key: str, provider: str = OPENROUTER) -> ScopeObservation | None:
    return _ledger(lambda client, prefix: read_scope(client, prefix, provider, scope_key))


def _raise_limit(
    at: datetime, *, limit: Decimal = RAISED_LIMIT, remaining: Decimal = Decimal("5")
) -> None:
    """The failed credential, still current, read with a strictly greater limit."""

    _register(FAILED, at)
    _probe(FAILED, at, _reading(limit=limit, remaining=remaining))


def _replace_credential(at: datetime) -> None:
    """A new current credential scope that authenticated at ``at``."""

    _register(REPLACED, at)
    _probe(REPLACED, at, _reading())


@dataclass(frozen=True)
class Blocked:
    ref: Ref
    agent_id: uuid.UUID
    successor_id: uuid.UUID


def _condition(reason: str, scope_key: str | None, retry_at: datetime | None) -> dict[str, Any]:
    condition: dict[str, Any] = {"reason": reason}
    if scope_key is not None:
        condition["scope_key"] = scope_key
    if retry_at is not None:
        condition["retry_at"] = retry_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return condition


def _fail_with(
    api: TestClient,
    auth_headers: dict[str, str],
    issue: int,
    cause: str,
    condition: dict[str, Any] | None,
    *,
    safety: str = "no_effects",
    provider: str | None = OPENROUTER,
    scope_key: str | None = None,
    agent_id: uuid.UUID | None = None,
) -> tuple[Ref, uuid.UUID]:
    """Run one admitted request and settle it failed through the finish route.

    The worker ran with ``scope_key`` as its provider's current scope, and an
    earlier probe read the baseline limit; ``provider`` None leaves the ledger
    empty, as for a provider no worker registered.
    """

    agent = agent_id or _agent(api, auth_headers)
    scope_key = scope_key or FAILED
    ref = _admit(api, agent, issue)
    epoch = _dispatch(api, ref.request_id)
    if provider is not None:
        now = _now()
        _register(scope_key, now, provider)
        _probe(
            scope_key,
            now,
            _reading(limit=BASELINE_LIMIT, remaining=Decimal("0")),
            provider,
        )
    kwargs: dict[str, Any] = {} if condition is None else {"condition": condition}
    finished = _finish(api, ref.request_id, epoch, cause, safety, **kwargs)
    assert finished.status_code == 200, finished.text
    return ref, agent


def _blocked(
    api: TestClient,
    auth_headers: dict[str, str],
    issue: int,
    kind: GateKind,
    *,
    scope_key: str | None = _OWN,
    provider: str | None = OPENROUTER,
    agent_id: uuid.UUID | None = None,
) -> Blocked:
    if scope_key == _OWN:
        scope_key = FAILED
    cause, reason, blocked_on = KINDS[kind]
    ref, agent = _fail_with(
        api,
        auth_headers,
        issue,
        cause,
        _condition(reason, scope_key, None),
        provider=provider,
        scope_key=scope_key or FAILED,
        agent_id=agent_id,
    )
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["blocked_on"] == blocked_on, successor
    assert successor["dispatch_not_before"] == successor["wait_deadline"]
    return Blocked(ref=ref, agent_id=agent, successor_id=successor["id"])


def _no_wake(request_id: uuid.UUID) -> bool:
    return not any(event.startswith(f"work-item-{request_id}") for event in _wake_ids())


def _wake_count(request_id: uuid.UUID) -> int:
    return sum(event.startswith(f"work-item-{request_id}") for event in _wake_ids())


def _assert_still_blocked(item: Blocked, blocked_on: str, version: int) -> None:
    row = _request(item.successor_id)
    assert (row["status"], row["blocked_on"], row["version"]) == ("waiting", blocked_on, version)
    assert row["dispatch_not_before"] == row["wait_deadline"]
    assert row["published_generation"] is None
    assert _no_wake(item.successor_id)


def _shift_terminal(request_id: uuid.UUID, seconds: int) -> None:
    """Move a settled request's terminal time back, as if it failed earlier."""

    _execute(
        "UPDATE curie.execution_requests "
        "SET terminal_at = terminal_at - make_interval(secs => :s) WHERE id = :id",
        {"id": request_id, "s": seconds},
        guarded=True,
    )


async def _always() -> bool:
    return True


def _release(
    request_id: uuid.UUID,
    work_item_id: uuid.UUID,
    *,
    work_version: int,
    request_version: int,
    recheck: Callable[[], Awaitable[bool]] = _always,
) -> lifecycle.ConditionRelease:
    async def body(session: AsyncSession) -> lifecycle.ConditionRelease:
        return await lifecycle.release_retry_condition(
            session,
            work_item_id=work_item_id,
            request_id=request_id,
            expected_work_item_version=work_version,
            expected_request_version=request_version,
            recheck=recheck,
        )

    return _db(body)


# -- AC1: gates hold until a verified repair, then release on the same pass ------


@pytest.mark.parametrize("kind", ["credential", "credit", "usage"])
def test_gate_holds_until_its_verified_control_then_dispatches_on_the_same_pass(
    gate_api: TestClient, auth_headers: dict[str, str], kind: GateKind
) -> None:
    _cause, _reason, blocked_on = KINDS[kind]
    item = _blocked(gate_api, auth_headers, 54260, kind)
    blocked = _request(item.successor_id)
    for _ in range(2):
        _reconcile()
        _assert_still_blocked(item, blocked_on, blocked["version"])

    predecessor = _request(item.ref.request_id)
    used = _used(item.ref.work_item_id)
    count = len(_requests(item.ref.work_item_id))
    if kind == "credential":
        _replace_credential(_now())
    else:
        _raise_limit(_now())
    _reconcile()

    released = _request(item.successor_id)
    assert released["blocked_on"] is None
    assert released["version"] == blocked["version"] + 1
    assert released["wait_deadline"] == blocked["wait_deadline"]
    assert released["dispatch_not_before"] < blocked["wait_deadline"]
    assert released["retry_of_request_id"] == item.ref.request_id
    # Dispatched by the existing publish path in the same pass.
    _published_wake(item.successor_id)
    assert _request(item.ref.request_id) == predecessor
    assert _used(item.ref.work_item_id) == used == 1
    assert len(_requests(item.ref.work_item_id)) == count


def test_daily_reset_with_a_time_is_scheduled_and_never_touched_by_the_gate_step(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    reset = (_now() + timedelta(minutes=10)).replace(microsecond=0)
    ref, _agent_id = _fail_with(
        gate_api,
        auth_headers,
        54261,
        "model_credit_exhausted",
        _condition("usage_reset", FAILED, reset),
    )
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["blocked_on"] is None
    assert successor["dispatch_not_before"] == reset + timedelta(seconds=60)
    _raise_limit(_now())
    _reconcile()
    after = _request(successor["id"])
    assert after["version"] == successor["version"]
    assert after["dispatch_not_before"] == successor["dispatch_not_before"]
    assert after["published_generation"] is None
    assert _no_wake(successor["id"])


def test_rate_limit_with_a_time_is_a_backoff_not_a_blocked_gate(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    at = (_now() + timedelta(minutes=10)).replace(microsecond=0)
    ref, _agent_id = _fail_with(
        gate_api,
        auth_headers,
        54262,
        "model_rate_limited",
        _condition("rate_limited", FAILED, at),
    )
    successor = _successor(ref.request_id)
    assert successor is not None
    assert successor["blocked_on"] is None
    assert successor["dispatch_not_before"] == at + timedelta(seconds=60)
    _raise_limit(_now())
    _reconcile()
    after = _request(successor["id"])
    assert after["version"] == successor["version"]
    assert after["dispatch_not_before"] == at + timedelta(seconds=60)
    assert _no_wake(successor["id"])


# -- AC1/AC2: everything else fails closed --------------------------------------


FailClosed = Callable[[Blocked], None]


def _credential_unchanged(_item: Blocked) -> None:
    now = _now()
    _register(FAILED, now)
    _probe(FAILED, now, _reading())


def _credential_rejected_probe(_item: Blocked) -> None:
    now = _now()
    _register(REPLACED, now)
    _probe(REPLACED, now, _reading("rejected"))


def _credential_unreadable_probe(_item: Blocked) -> None:
    now = _now()
    _register(REPLACED, now)
    _probe(REPLACED, now, _reading("unreadable"))


def _credential_probe_before_failure(item: Blocked) -> None:
    terminal_at = _request(item.ref.request_id)["terminal_at"]
    _register(REPLACED, _now())
    _probe(REPLACED, terminal_at - timedelta(seconds=1), _reading())


def _credit_rejected_probe(_item: Blocked) -> None:
    now = _now()
    _register(FAILED, now)
    _probe(FAILED, now, _reading("rejected", limit=RAISED_LIMIT, remaining=Decimal("5")))


def _credit_unreadable_probe(_item: Blocked) -> None:
    now = _now()
    _register(FAILED, now)
    _probe(FAILED, now, _reading("unreadable", limit=RAISED_LIMIT, remaining=Decimal("5")))


def _credit_stale_probe(item: Blocked) -> None:
    # The failure was ten minutes ago; the raise was read after it, but longer
    # ago than a probe stays fresh.
    _shift_terminal(item.ref.request_id, 600)
    _raise_limit(_now() - timedelta(seconds=PROBE_FRESH_S + 1))


def _credit_probe_before_failure(item: Blocked) -> None:
    terminal_at = _request(item.ref.request_id)["terminal_at"]
    _raise_limit(terminal_at - timedelta(seconds=1))


def _remaining_rose_under_an_equal_limit(_item: Blocked) -> None:
    _raise_limit(_now(), limit=BASELINE_LIMIT, remaining=Decimal("9"))


def _null_limit(_item: Blocked) -> None:
    now = _now()
    _register(FAILED, now)
    _probe(FAILED, now, _reading(limit=None, remaining=Decimal("9")))


def _nothing_remaining_after_a_raise(_item: Blocked) -> None:
    _raise_limit(_now(), remaining=Decimal("0"))


def _credit_scope_no_longer_current(_item: Blocked) -> None:
    _raise_limit(_now())
    _register(REPLACED, _now())


FAIL_CLOSED: dict[str, tuple[GateKind, FailClosed]] = {
    "credential_unchanged_scope_authenticated": ("credential", _credential_unchanged),
    "credential_rejected_probe": ("credential", _credential_rejected_probe),
    "credential_unreadable_probe": ("credential", _credential_unreadable_probe),
    "credential_probe_before_failure": ("credential", _credential_probe_before_failure),
    "credit_rejected_probe": ("credit", _credit_rejected_probe),
    "credit_unreadable_probe": ("credit", _credit_unreadable_probe),
    "credit_stale_probe": ("credit", _credit_stale_probe),
    "credit_probe_before_failure": ("credit", _credit_probe_before_failure),
    "remaining_rose_under_an_equal_limit": ("credit", _remaining_rose_under_an_equal_limit),
    "usage_remaining_rose_under_an_equal_limit": ("usage", _remaining_rose_under_an_equal_limit),
    "null_limit": ("credit", _null_limit),
    "nothing_remaining_after_a_raise": ("credit", _nothing_remaining_after_a_raise),
    "credit_scope_no_longer_current": ("credit", _credit_scope_no_longer_current),
}


@pytest.mark.parametrize("case", sorted(FAIL_CLOSED))
def test_unverified_evidence_keeps_the_gate_blocked(
    gate_api: TestClient, auth_headers: dict[str, str], case: str
) -> None:
    kind, arrange = FAIL_CLOSED[case]
    item = _blocked(gate_api, auth_headers, 54270, kind)
    version = _request(item.successor_id)["version"]
    arrange(item)
    _reconcile()
    _assert_still_blocked(item, KINDS[kind][2], version)


def test_unknown_provider_scope_never_releases(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    # No worker ever registered the failed scope, so no provider holds it.
    item = _blocked(gate_api, auth_headers, 54271, "credit", provider=None)
    version = _request(item.successor_id)["version"]
    _register(OTHER, _now())
    _probe(OTHER, _now(), _reading(limit=BASELINE_LIMIT, remaining=Decimal("0")))
    _probe(OTHER, _now(), _reading(limit=RAISED_LIMIT, remaining=Decimal("5")))
    _reconcile()
    _assert_still_blocked(item, "provider_credit", version)


@pytest.mark.parametrize("kind", ["credit", "credential", "usage"])
def test_a_wait_with_no_scope_key_is_terminal_and_spends_no_retry(
    gate_api: TestClient, auth_headers: dict[str, str], kind: GateKind
) -> None:
    # An account-level credit refusal carries no key scope; neither does a
    # credential failure the runner could not scope. No probe could ever
    # release such a wait, so none is admitted (#5424).
    cause, reason, _ = KINDS[kind]
    ref, agent_id = _fail_with(gate_api, auth_headers, 54272, cause, _condition(reason, None, None))
    assert _request(ref.request_id)["retry_condition_scope_key"] is None
    assert _successor(ref.request_id) is None
    assert _used(ref.work_item_id) == 0

    # Liveness: the same refusal on a probed scope waits blocked.
    item = _blocked(gate_api, auth_headers, 54279, kind, agent_id=agent_id)
    assert _used(item.ref.work_item_id) == 1


def test_an_unprobed_provider_scope_never_releases(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54273, "credit", provider=ANTHROPIC)
    version = _request(item.successor_id)["version"]
    now = _now()
    _register(FAILED, now, ANTHROPIC)
    _probe(FAILED, now, _reading(limit=RAISED_LIMIT, remaining=Decimal("5")), ANTHROPIC)
    observed = _observation(FAILED, ANTHROPIC)
    assert observed is not None and observed.allowance_increased_at_ms is not None
    _reconcile()
    _assert_still_blocked(item, "provider_credit", version)


def test_rate_limit_past_the_deadline_and_diagnosis_waits_are_never_admitted(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    # Nothing releases a block on a provider time past the waiting deadline or
    # on an undiagnosed runner fault, so neither admits a successor (#5424).
    agent_id = _agent(gate_api, auth_headers)
    beyond = _now() + timedelta(seconds=get_settings().work_item_wait_budget_seconds + 3600)
    limited, _ = _fail_with(
        gate_api,
        auth_headers,
        54274,
        "model_rate_limited",
        _condition("rate_limited", FAILED, beyond.replace(microsecond=0)),
        agent_id=agent_id,
    )
    diagnosed, _ = _fail_with(
        gate_api,
        auth_headers,
        54275,
        "runner_error",
        _condition("rate_limited", FAILED, None),
        agent_id=agent_id,
    )
    for ref in (limited, diagnosed):
        assert _request(ref.request_id)["status"] == "failed"
        assert _successor(ref.request_id) is None
        assert _used(ref.work_item_id) == 0


def test_a_block_the_policy_does_not_derive_is_refused(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54276, "usage")
    # The stored block no longer matches the predecessor's facts.
    _execute(
        "UPDATE curie.execution_requests SET blocked_on = 'provider_credit' WHERE id = :id",
        {"id": item.successor_id},
        guarded=True,
    )
    version = _request(item.successor_id)["version"]
    _raise_limit(_now())
    _reconcile()
    _assert_still_blocked(item, "provider_credit", version)
    work_version, request_version = _versions(item.successor_id)
    refused = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
    )
    assert (refused.refusal, refused.condition) == ("condition_mismatch", "provider_credit")


# -- AC2: concurrency, duplicates and precedence ----------------------------------


def test_two_same_scope_items_release_once_each_from_one_observation(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(gate_api, auth_headers)
    first = _blocked(gate_api, auth_headers, 54280, "credit", agent_id=agent_id)
    second = _blocked(gate_api, auth_headers, 54281, "credit", agent_id=agent_id)
    versions = {
        item.successor_id: _request(item.successor_id)["version"] for item in (first, second)
    }
    _raise_limit(_now())
    _reconcile()
    for item in (first, second):
        row = _request(item.successor_id)
        assert (row["blocked_on"], row["version"]) == (None, versions[item.successor_id] + 1)
        _published_wake(item.successor_id)
        assert _wake_count(item.successor_id) == 1
        assert _used(item.ref.work_item_id) == 1
    # Duplicate wakes: further passes change nothing and publish nothing more.
    _reconcile()
    _reconcile()
    for item in (first, second):
        row = _request(item.successor_id)
        assert row["version"] == versions[item.successor_id] + 1
        assert _wake_count(item.successor_id) == 1


def test_a_second_release_call_changes_nothing(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54282, "credential")
    work_version, request_version = _versions(item.successor_id)
    released = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
    )
    assert (released.released, released.condition) == (True, "provider_credential")
    after = _request(item.successor_id)
    replayed = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
    )
    assert replayed.refusal == "stale_version"
    current = _versions(item.successor_id)
    again = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=current[0],
        request_version=current[1],
    )
    assert again.refusal == "not_blocked"
    assert _request(item.successor_id) == after
    assert _used(item.ref.work_item_id) == 1


def test_a_crashed_probe_owners_late_write_cannot_release(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54283, "credit")
    version = _request(item.successor_id)["version"]
    now_ms = _ms(_now())

    async def crashed_then_peer(client: aredis.Redis, prefix: str) -> tuple[bool, bool]:
        lapsed = await claim_probe(client, prefix, OPENROUTER, FAILED, "worker-a")
        assert lapsed is not None
        # The owner crashed; its lease lapses.
        await client.delete(lease_key(prefix, OPENROUTER, FAILED))
        peer = await claim_probe(client, prefix, OPENROUTER, FAILED, "worker-b")
        assert peer is not None
        late = await record_probe(
            client,
            prefix,
            OPENROUTER,
            FAILED,
            lapsed,
            now_ms,
            _reading(limit=RAISED_LIMIT, remaining=Decimal("5")),
        )
        recorded = await record_probe(
            client,
            prefix,
            OPENROUTER,
            FAILED,
            peer,
            now_ms,
            _reading(limit=BASELINE_LIMIT, remaining=Decimal("0")),
        )
        return late, recorded

    _register(FAILED, _now())
    assert _ledger(crashed_then_peer) == (False, True)
    observed = _observation(FAILED)
    assert observed is not None and observed.allowance_increased_at_ms is None
    _reconcile()
    _assert_still_blocked(item, "provider_credit", version)


def test_unrelated_configuration_changes_do_not_release(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54284, "credit")
    version = _request(item.successor_id)["version"]
    patched = gate_api.patch(
        f"/agents/{item.agent_id}", json={"publication_policy": "auto"}, headers=auth_headers
    )
    assert patched.status_code == 200, patched.text
    _register(OTHER, _now(), ANTHROPIC)
    now = _now()
    _register(FAILED, now)
    _probe(FAILED, now, _reading(limit=BASELINE_LIMIT, remaining=Decimal("0")))
    _reconcile()
    _assert_still_blocked(item, "provider_credit", version)


def test_cancellation_wins_over_a_verified_repair(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54285, "credential")
    work_version, _ = _versions(item.successor_id)

    async def cancel(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.request_cancellation(
            session,
            work_item_id=item.ref.work_item_id,
            expected_work_item_version=work_version,
            cause="issue_cancelled",
        )

    assert isinstance(_db(cancel), lifecycle.WorkItemOutcome)
    _replace_credential(_now())
    _reconcile()
    row = _request(item.successor_id)
    assert (row["status"], row["blocked_on"]) == ("cancelled", "provider_credential")
    assert _no_wake(item.successor_id)
    assert _used(item.ref.work_item_id) == 1


def test_a_passed_wait_deadline_expires_before_any_release(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54286, "credit")
    _raise_limit(_now())
    _expire_wait(item.successor_id)
    work_version, request_version = _versions(item.successor_id)
    refused = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
    )
    assert refused.refusal == "waiting_deadline_elapsed"
    _reconcile()
    row = _request(item.successor_id)
    assert (row["status"], row["terminal_cause"], row["blocked_on"]) == (
        "expired",
        "retry_condition_expired",
        "provider_credit",
    )
    assert _no_wake(item.successor_id)
    assert _used(item.ref.work_item_id) == 1


def test_a_relabel_supersedes_the_blocked_successor_before_release(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54287, "credit")

    async def relabel(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.readmit(
            session,
            work_item_id=item.ref.work_item_id,
            request_id=uuid.uuid4(),
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            objective=OBJECTIVE,
            requester=REQUESTER,
            trigger_label=None,
        )

    assert isinstance(_db(relabel), lifecycle.WorkItemOutcome)
    _raise_limit(_now())
    _reconcile()
    row = _request(item.successor_id)
    assert (row["status"], row["blocked_on"]) == ("cancelled", "provider_credit")
    assert _no_wake(item.successor_id)
    assert _used(item.ref.work_item_id) == 1


def test_a_pending_readmit_and_stale_versions_are_refused(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54288, "credential")
    work_version, request_version = _versions(item.successor_id)
    for stale_work, stale_request in (
        (work_version - 1, request_version),
        (work_version, request_version - 1),
    ):
        refused = _release(
            item.successor_id,
            item.ref.work_item_id,
            work_version=stale_work,
            request_version=stale_request,
        )
        assert refused.refusal == "stale_version"
    _execute(
        "UPDATE curie.work_items SET readmit_request_id = :rid, readmit_requester = :who, "
        "readmit_objective = :what WHERE id = :id",
        {"rid": uuid.uuid4(), "who": REQUESTER, "what": OBJECTIVE, "id": item.ref.work_item_id},
    )
    refused = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
    )
    assert refused.refusal == "readmit_pending"
    row = _request(item.successor_id)
    assert (row["blocked_on"], row["version"]) == ("provider_credential", request_version)


def test_a_false_recheck_leaves_the_block(
    gate_api: TestClient, auth_headers: dict[str, str]
) -> None:
    item = _blocked(gate_api, auth_headers, 54289, "credit")
    work_version, request_version = _versions(item.successor_id)

    async def never() -> bool:
        return False

    refused = _release(
        item.successor_id,
        item.ref.work_item_id,
        work_version=work_version,
        request_version=request_version,
        recheck=never,
    )
    assert (refused.refusal, refused.condition) == ("not_cleared", "provider_credit")
    assert _request(item.successor_id)["blocked_on"] == "provider_credit"


@pytest.mark.parametrize("safety", ["flagged", "uncertain"])
def test_a_flagged_or_uncertain_predecessor_has_nothing_to_release(
    gate_api: TestClient, auth_headers: dict[str, str], safety: str
) -> None:
    ref, _agent_id = _fail_with(
        gate_api,
        auth_headers,
        54290,
        "model_credential_rejected",
        _condition("credential_rejected", FAILED, None),
        safety=safety,
    )
    assert _successor(ref.request_id) is None
    _replace_credential(_now())
    _reconcile()
    assert _successor(ref.request_id) is None
    assert _used(ref.work_item_id) == 0
    assert [row["id"] for row in _requests(ref.work_item_id)] == [ref.request_id]


# -- the clearance table, pure ----------------------------------------------------

_TERMINAL = datetime(2030, 1, 1, tzinfo=UTC)
_NOW = _TERMINAL + timedelta(seconds=30)


def _seen(
    *,
    provider: str | None = OPENROUTER,
    current: str | None = FAILED,
    outcome: str | None = "authenticated",
    probed: datetime | None = _NOW,
    limit: Decimal | None = RAISED_LIMIT,
    remaining: Decimal | None = Decimal("5"),
    increased: datetime | None = _NOW,
) -> GateObservation:
    return GateObservation(
        provider=provider,
        current_scope=current,
        observation=ScopeObservation(
            provider=provider or OPENROUTER,
            current_at_ms=None,
            probed_at_ms=None if probed is None else _ms(probed),
            outcome=outcome,
            limit=limit,
            limit_remaining=remaining,
            allowance_increased_at_ms=None if increased is None else _ms(increased),
        ),
    )


def _facts(blocked_on: str = "provider_credit", reason: str = "credit_exhausted") -> GateFacts:
    return GateFacts(
        blocked_on=blocked_on,
        reason=reason,
        retry_at=None,
        scope_key=FAILED,
        terminal_at=_TERMINAL,
    )


@pytest.mark.parametrize(
    ("facts", "seen", "expected"),
    [
        (_facts(), _seen(), True),
        (_facts("provider_usage_reset", "usage_reset"), _seen(), True),
        (_facts("provider_credit", "usage_reset"), _seen(), True),
        (_facts("provider_credential", "credential_rejected"), _seen(current=REPLACED), True),
        (_facts("provider_usage_reset", "credit_exhausted"), _seen(), False),
        (_facts("provider_credential", "credit_exhausted"), _seen(current=REPLACED), False),
        (_facts("provider_credential", "credential_rejected"), _seen(current=None), False),
        (_facts("provider_rate_limit", "rate_limited"), _seen(), False),
        (_facts(), _seen(provider=ANTHROPIC), False),
        (_facts(), _seen(provider=None), False),
        (_facts(), _seen(current=None), False),
        (_facts(), _seen(outcome="rejected"), False),
        (_facts(), _seen(probed=None), False),
        (_facts(), _seen(probed=_NOW + timedelta(seconds=PROBE_FRESH_S + 31)), False),
        (_facts(), _seen(probed=_TERMINAL), False),
        (_facts(), _seen(limit=None), False),
        (_facts(), _seen(remaining=Decimal("0")), False),
        (_facts(), _seen(remaining=None), False),
        (_facts(), _seen(increased=None), False),
        (_facts(), _seen(increased=_TERMINAL), False),
        (
            GateFacts("provider_credit", "credit_exhausted", _NOW, FAILED, _TERMINAL),
            _seen(),
            False,
        ),
        (GateFacts("provider_credit", "credit_exhausted", None, None, _TERMINAL), _seen(), False),
        (GateFacts("provider_credit", "credit_exhausted", None, FAILED, None), _seen(), False),
        (_facts(), GateObservation(OPENROUTER, FAILED, None), False),
    ],
)
def test_clearance_table(facts: GateFacts, seen: GateObservation, expected: bool) -> None:
    assert clearance(facts, seen, _NOW) is expected
