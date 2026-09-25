"""Durable KV state store (#23): real Postgres round-trip + compare-and-set.

Nothing mocked -- exercises the API against the compose Postgres (the
disposable-DB conftest provisions and migrates a throwaway database per run).
"""

import asyncio
import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import asyncpg
import pytest
from curie_api.config import get_settings
from curie_api.routers.state import (
    _NAMESPACE_LOCK_CLASS,
    _json_size,
    _namespace_lock_key,
)
from curie_api.sandbox_token import mint
from curie_telemetry import build_resource, configure_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from sqlalchemy import make_url
from sqlalchemy.exc import IntegrityError

# Scoped-sandbox-token auth matrix constants (#410).
_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time
_PAST = 1000000000  # 2001, expired at test time


def _agent(
    client: Any,
    headers: dict[str, str],
    *,
    address: str = "C000000S01",
    name: str = "state-agent",
) -> str:
    resp = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": address}},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    agent_id: str = resp.json()["id"]
    return agent_id


@pytest.fixture
def history_failure_metrics(
    client: Any,
) -> Iterator[tuple[MeterProvider, InMemoryMetricReader]]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-api",
            service_version="0.0.0-test",
            service_instance_id="acme-api-capacity-test",
            deployment_environment="test",
        ),
    )
    original = client.app.state.telemetry.meter_provider
    configure_meter_provider(provider)
    try:
        yield provider, reader
    finally:
        configure_meter_provider(original)
        provider.shutdown()


def _history_failure_points(
    metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> list[tuple[int, dict[str, str]]]:
    provider, reader = metrics
    assert provider.force_flush(timeout_millis=5000)
    data = reader.get_metrics_data()
    assert data is not None
    return [
        (int(point.value), dict(point.attributes))
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == "curie.history.persistence.failure"
        for point in metric.data.data_points
    ]


def test_state_router_accepts_a_scoped_token_for_the_path_agent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A scoped "state" token whose agent claim matches the path agent is a
    # first-class credential on the state router: no regression for the
    # platform key, and the sandboxed agent reaches its own namespace with a
    # least-privilege token instead of the raw shared key (#410).
    aid = _agent(client, auth_headers)
    api_key = get_settings().api_key
    token = mint(api_key, agent=aid, scope="state", exp=_FAR_FUTURE)
    headers = {"X-API-Key": token}
    url = f"/agents/{aid}/state/scoped/k"

    put = client.put(url, json={"value": {"n": 1}}, headers=headers)
    assert put.status_code == 200, put.text
    assert put.json()["value"] == {"n": 1}

    got = client.get(url, headers=headers)
    assert got.status_code == 200
    assert got.json()["value"] == {"n": 1}

    # The platform key still works on the same endpoint (no regression).
    assert client.get(url, headers=auth_headers).status_code == 200


def test_state_router_rejects_scoped_token_for_a_different_agent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    other = str(uuid.uuid4())
    token = mint(get_settings().api_key, agent=other, scope="state", exp=_FAR_FUTURE)
    r = client.put(
        f"/agents/{aid}/state/scoped/k",
        json={"value": {"n": 1}},
        headers={"X-API-Key": token},
    )
    assert r.status_code == 401, r.text


def test_state_router_rejects_expired_scoped_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_PAST)
    r = client.put(
        f"/agents/{aid}/state/scoped/k",
        json={"value": {"n": 1}},
        headers={"X-API-Key": token},
    )
    assert r.status_code == 401, r.text


def test_state_router_rejects_wrong_scope_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    token = mint(get_settings().api_key, agent=aid, scope="admin", exp=_FAR_FUTURE)
    r = client.put(
        f"/agents/{aid}/state/scoped/k",
        json={"value": {"n": 1}},
        headers={"X-API-Key": token},
    )
    assert r.status_code == 401, r.text


def test_state_router_rejects_wrong_signing_key_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    # Correct agent + scope, but signed with a key that is not the platform key.
    token = mint("not-the-platform-key", agent=aid, scope="state", exp=_FAR_FUTURE)
    r = client.put(
        f"/agents/{aid}/state/scoped/k",
        json={"value": {"n": 1}},
        headers={"X-API-Key": token},
    )
    assert r.status_code == 401, r.text


def test_state_router_rejects_garbage_token_without_echoing_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    garbage = "sbx.xxx.yyy"
    r = client.put(
        f"/agents/{aid}/state/scoped/k",
        json={"value": {"n": 1}},
        headers={"X-API-Key": garbage},
    )
    assert r.status_code == 401, r.text
    # A rejected credential is never echoed back in the error body.
    assert garbage not in r.text


def test_state_router_rejects_missing_api_key_header(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    r = client.put(f"/agents/{aid}/state/scoped/k", json={"value": {"n": 1}})
    assert r.status_code == 401, r.text


def test_app_scoped_token_is_refused_on_reserved_namespaces(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #249 security backstop: the bundle-facing state token is the NARROW
    # ``state.app`` scope, and the server refuses it on the memory/transcript
    # namespaces owned by the memory (#264) and history (#20) ports -- so a skill
    # cannot corrupt them by composing CURIE_STATE_URL directly, bypassing the
    # runner tool's own client-side refusal.
    aid = _agent(client, auth_headers)
    app = mint(get_settings().api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
    headers = {"X-API-Key": app}

    for ns in ("memory", "transcript"):
        put = client.put(
            f"/agents/{aid}/state/{ns}/k", json={"value": {"n": 1}}, headers=headers
        )
        assert put.status_code == 403, f"{ns}: {put.text}"
        assert "reserved" in put.text
        # Every verb over a reserved namespace is refused, not just writes.
        assert client.get(f"/agents/{aid}/state/{ns}/k", headers=headers).status_code == 403
        assert client.get(f"/agents/{aid}/state/{ns}", headers=headers).status_code == 403
        assert (
            client.post(
                f"/agents/{aid}/state/{ns}/log/append", json={"item": 1}, headers=headers
            ).status_code
            == 403
        )
        assert client.delete(f"/agents/{aid}/state/{ns}/k", headers=headers).status_code == 403


def test_namespace_enumeration_hides_reserved_from_the_app_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #856: the enumeration route (GET .../state) has no namespace path param, so
    # forbid_reserved_namespace cannot gate it. The narrow state.app token must
    # still not learn the reserved namespaces exist -- their key counts and write
    # times are exactly what that scope fences off. The platform key (the UI
    # inspector) keeps full reach.
    aid = _agent(client, auth_headers)
    # Seed both reserved namespaces and a normal one via the unrestricted key.
    for ns, key in (("memory", "m1"), ("transcript", "t1"), ("workflow", "w1")):
        put = client.put(
            f"/agents/{aid}/state/{ns}/{key}", json={"value": {"n": 1}}, headers=auth_headers
        )
        assert put.status_code == 200, f"{ns}: {put.text}"

    app = mint(get_settings().api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
    app_names = {
        row["namespace"]
        for row in client.get(f"/agents/{aid}/state", headers={"X-API-Key": app}).json()
    }
    assert app_names == {"workflow"}, app_names

    platform_names = {
        row["namespace"]
        for row in client.get(f"/agents/{aid}/state", headers=auth_headers).json()
    }
    assert platform_names == {"memory", "transcript", "workflow"}, platform_names


def test_app_scoped_token_works_on_a_non_reserved_namespace(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The narrow token is refused ONLY on the reserved set; everywhere else it is
    # a first-class credential -- the bundle "gets the rest".
    aid = _agent(client, auth_headers)
    app = mint(get_settings().api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
    headers = {"X-API-Key": app}
    url = f"/agents/{aid}/state/workflow/step"

    assert client.put(url, json={"value": {"n": 1}}, headers=headers).status_code == 200
    assert client.get(url, headers=headers).json()["value"] == {"n": 1}


def test_broad_state_token_and_platform_key_reach_reserved_namespaces(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The loaders MUST reach memory/transcript to rehydrate: the broad ``state``
    # token (their credential) and the platform key are both unrestricted. If this
    # regressed, memory/history rehydration would break -- the reason the fix
    # gates on scope, not on the namespace alone.
    aid = _agent(client, auth_headers)
    broad = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE)

    for headers in ({"X-API-Key": broad}, auth_headers):
        for ns in ("memory", "transcript"):
            r = client.put(
                f"/agents/{aid}/state/{ns}/k", json={"value": {"n": 1}}, headers=headers
            )
            assert r.status_code == 200, f"{ns}: {r.text}"


def test_put_get_list_delete_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/approvals"

    # put (create) -> version 1
    r = client.put(
        f"{base}/thread-1", json={"value": {"status": "pending"}}, headers=auth_headers
    )
    assert r.status_code == 200, r.text
    assert r.json()["value"] == {"status": "pending"}
    assert r.json()["version"] == 1

    # get returns what was written
    got = client.get(f"{base}/thread-1", headers=auth_headers)
    assert got.status_code == 200
    assert got.json()["value"] == {"status": "pending"}

    # put (update) -> version bumps to 2
    r2 = client.put(
        f"{base}/thread-1", json={"value": {"status": "approved"}}, headers=auth_headers
    )
    assert r2.json()["version"] == 2

    # list by namespace returns both keys
    client.put(
        f"{base}/thread-2", json={"value": {"status": "pending"}}, headers=auth_headers
    )
    listed = client.get(base, headers=auth_headers).json()
    assert {e["key"] for e in listed} == {"thread-1", "thread-2"}

    # delete -> 204, then gone
    d = client.delete(f"{base}/thread-1", headers=auth_headers)
    assert d.status_code == 204
    assert client.get(f"{base}/thread-1", headers=auth_headers).status_code == 404


def test_compare_and_set_rejects_a_stale_version(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/dedupe/seen"

    v1 = client.put(url, json={"value": {"n": 1}}, headers=auth_headers).json()
    assert v1["version"] == 1

    # CAS with the current version succeeds and bumps to 2.
    ok = client.put(
        url, json={"value": {"n": 2}, "expected_version": 1}, headers=auth_headers
    )
    assert ok.status_code == 200
    assert ok.json()["version"] == 2

    # CAS with the now-stale version 1 is rejected, and the value is unchanged.
    stale = client.put(
        url, json={"value": {"n": 3}, "expected_version": 1}, headers=auth_headers
    )
    assert stale.status_code == 409, stale.text
    assert client.get(url, headers=auth_headers).json()["value"] == {"n": 2}


def test_put_unknown_agent_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    r = client.put(
        f"/agents/{missing}/state/ns/k", json={"value": {}}, headers=auth_headers
    )
    assert r.status_code == 404


def test_get_missing_entry_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    r = client.get(f"/agents/{aid}/state/ns/nope", headers=auth_headers)
    assert r.status_code == 404


def test_concurrent_cas_one_writer_wins_loser_sees_compare_failure(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The AC's core scenario: two writers both read the same version, both try a
    # compare-and-set write; exactly one wins and the loser gets the 409.
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/counter/n"

    seed = client.put(url, json={"value": {"n": 0}}, headers=auth_headers).json()
    read_version = seed["version"]  # both writers observe this version

    winner = client.put(
        url,
        json={"value": {"n": 1}, "expected_version": read_version},
        headers=auth_headers,
    )
    loser = client.put(
        url,
        json={"value": {"n": 2}, "expected_version": read_version},
        headers=auth_headers,
    )

    assert winner.status_code == 200, winner.text
    assert loser.status_code == 409, loser.text
    # The winner's write stands; the loser's did not clobber it.
    assert client.get(url, headers=auth_headers).json()["value"] == {"n": 1}


def test_append_grows_a_log_shaped_entry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/audit/log/append"

    # First append creates the entry as a single-element array (version 1).
    r1 = client.post(url, json={"item": {"event": "created"}}, headers=auth_headers)
    assert r1.status_code == 200, r1.text
    assert r1.json()["value"] == [{"event": "created"}]
    assert r1.json()["version"] == 1

    # Second append extends the array and bumps the version.
    r2 = client.post(url, json={"item": {"event": "approved"}}, headers=auth_headers)
    assert r2.json()["value"] == [{"event": "created"}, {"event": "approved"}]
    assert r2.json()["version"] == 2


def test_append_onto_a_non_array_value_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/audit"
    client.put(f"{base}/obj", json={"value": {"not": "a list"}}, headers=auth_headers)

    r = client.post(f"{base}/obj/append", json={"item": 1}, headers=auth_headers)
    assert r.status_code == 409, r.text


def test_value_over_the_per_value_cap_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    # Shrink the per-value cap for this test. get_settings() is lru_cached, so it
    # returns a singleton we mutate and reset via cache_clear() in the finally.
    get_settings().state_max_value_bytes = 50
    try:
        oversized = {"blob": "x" * 200}
        r = client.put(
            f"/agents/{aid}/state/big/v", json={"value": oversized}, headers=auth_headers
        )
        assert r.status_code == 413, r.text
        # A small value under the cap still writes.
        ok = client.put(
            f"/agents/{aid}/state/big/v", json={"value": {"n": 1}}, headers=auth_headers
        )
        assert ok.status_code == 200
    finally:
        get_settings.cache_clear()


def test_namespace_over_the_per_namespace_cap_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/capped"
    get_settings().state_max_namespace_bytes = 100
    try:
        # First key fits (~58 bytes serialized).
        a = client.put(f"{base}/a", json={"value": {"s": "x" * 50}}, headers=auth_headers)
        assert a.status_code == 200, a.text
        # Second key pushes the namespace total (~116 bytes) over the 100 cap.
        b = client.put(f"{base}/b", json={"value": {"s": "x" * 50}}, headers=auth_headers)
        assert b.status_code == 413, b.text
    finally:
        get_settings.cache_clear()


def test_transcript_get_advertises_configured_cap_even_when_key_is_missing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/factory-3211"
    settings = get_settings()
    settings.transcript_max_thread_bytes = 128 * 1024
    try:
        missing = client.get(url, headers=auth_headers)
        assert missing.status_code == 404, missing.text
        assert missing.headers["X-Curie-Transcript-Max-Bytes"] == "131072"

        scoped_url = (
            f"/agents/{aid}/state/bindings/slack/C000000S01/transcript/factory-3211"
        )
        scoped_missing = client.get(scoped_url, headers=auth_headers)
        assert scoped_missing.status_code == 404, scoped_missing.text
        assert scoped_missing.headers["X-Curie-Transcript-Max-Bytes"] == "131072"

        created = client.post(
            f"{url}/append", json={"item": {"user": "issue", "assistant": "done"}},
            headers=auth_headers,
        )
        assert created.status_code == 200, created.text
        present = client.get(url, headers=auth_headers)
        assert present.status_code == 200, present.text
        assert present.headers["X-Curie-Transcript-Max-Bytes"] == "131072"
        assert present.json()["value"] == [{"user": "issue", "assistant": "done"}]

        scoped_created = client.post(
            f"{scoped_url}/append",
            json={"item": {"user": "scoped issue", "assistant": "scoped answer"}},
            headers=auth_headers,
        )
        assert scoped_created.status_code == 200, scoped_created.text
        scoped_present = client.get(scoped_url, headers=auth_headers)
        assert scoped_present.status_code == 200, scoped_present.text
        assert scoped_present.headers["X-Curie-Transcript-Max-Bytes"] == "131072"
        assert scoped_present.json()["value"] == [
            {"user": "scoped issue", "assistant": "scoped answer"}
        ]
    finally:
        get_settings.cache_clear()


def test_transcript_value_cap_records_once_without_mutating_the_log(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    history_failure_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-value/append"
    seed = {"kind": "message", "text": "seed"}
    settings = get_settings()
    settings.transcript_max_thread_bytes = _json_size([seed])
    try:
        initial = client.post(url, json={"item": seed}, headers=auth_headers)
        assert initial.status_code == 200, initial.text

        refused = client.post(
            url,
            json={"item": {"kind": "message", "text": "x" * 200}},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text

        stored = client.get(url.removesuffix("/append"), headers=auth_headers)
        assert stored.status_code == 200, stored.text
        assert stored.json()["value"] == [seed]
        assert stored.json()["version"] == initial.json()["version"]
        assert _history_failure_points(history_failure_metrics) == [
            (
                1,
                {
                    "service.name": "curie-api",
                    "source": "state-api",
                    "outcome": "capacity",
                    "limit": "value",
                },
            )
        ]
    finally:
        get_settings.cache_clear()


def test_many_threads_for_one_agent_all_persist_and_resume_past_the_old_namespace_cap(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    history_failure_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    """ADR-0170 (#3070): transcripts are capped per thread, never per agent.

    Before the move every thread shared the agent's 1 MiB transcript namespace,
    so a factory agent failed every new run once about 30 issues filled it. Here
    one agent writes threads whose total is well past that old cap, each close to
    its own per-thread cap, and every thread still appends and reads back.
    """
    aid = _agent(client, auth_headers)
    settings = get_settings()
    thread_cap = settings.transcript_max_thread_bytes
    old_namespace_cap = settings.state_max_namespace_bytes
    turn = {"role": "assistant", "content": "x" * (thread_cap // 4)}
    threads = [f"issue-{n}" for n in range(24)]

    written: dict[str, Any] = {}
    for thread in threads:
        url = f"/agents/{aid}/state/transcript/{thread}"
        for _ in range(3):
            appended = client.post(f"{url}/append", json={"item": turn}, headers=auth_headers)
            assert appended.status_code == 200, appended.text
        written[thread] = appended.json()["value"]

    total = sum(_json_size(value) for value in written.values())
    assert total > old_namespace_cap, (total, old_namespace_cap)
    for thread, value in written.items():
        url = f"/agents/{aid}/state/transcript/{thread}"
        resumed = client.get(url, headers=auth_headers)
        assert resumed.status_code == 200, resumed.text
        assert resumed.json()["value"] == value
        assert resumed.json()["version"] == 3
    # No thread lives in the capped state store any more.
    listed = client.get(f"/agents/{aid}/state/transcript", headers=auth_headers).json()
    assert {row["key"] for row in listed} == set(threads)
    assert _history_failure_points(history_failure_metrics) == []


def test_one_thread_over_its_cap_is_refused_without_touching_its_siblings(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    history_failure_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    aid = _agent(client, auth_headers)
    settings = get_settings()
    settings.transcript_max_thread_bytes = 200
    base = f"/agents/{aid}/state/transcript"
    try:
        sibling = client.post(
            f"{base}/thread-sibling/append",
            json={"item": {"text": "s" * 100}},
            headers=auth_headers,
        )
        assert sibling.status_code == 200, sibling.text
        runaway = client.post(
            f"{base}/thread-runaway/append",
            json={"item": {"text": "r" * 100}},
            headers=auth_headers,
        )
        assert runaway.status_code == 200, runaway.text
        refused = client.post(
            f"{base}/thread-runaway/append",
            json={"item": {"text": "r" * 100}},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text
        assert "per-thread" in refused.json()["detail"]
        grown = client.post(
            f"{base}/thread-new/append",
            json={"item": {"text": "n" * 100}},
            headers=auth_headers,
        )
        assert grown.status_code == 200, grown.text
        assert client.get(f"{base}/thread-sibling", headers=auth_headers).json()[
            "value"
        ] == sibling.json()["value"]
        assert _history_failure_points(history_failure_metrics) == [
            (
                1,
                {
                    "service.name": "curie-api",
                    "source": "state-api",
                    "outcome": "capacity",
                    "limit": "value",
                },
            )
        ]
    finally:
        get_settings.cache_clear()


def test_expired_idle_transcript_reads_as_absent_and_is_swept(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """ADR-0170: a thread with no WorkItem expires after its idle window."""
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/transcript"
    settings = get_settings()
    settings.transcript_idle_ttl_seconds = 0
    try:
        stale = client.post(
            f"{base}/thread-idle/append", json={"item": {"text": "old"}}, headers=auth_headers
        )
        assert stale.status_code == 200, stale.text
    finally:
        get_settings.cache_clear()
    assert client.get(f"{base}/thread-idle", headers=auth_headers).status_code == 404
    assert client.get(base, headers=auth_headers).json() == []

    fresh = client.post(
        f"{base}/thread-live/append", json={"item": {"text": "new"}}, headers=auth_headers
    )
    assert fresh.status_code == 200, fresh.text
    restarted = client.post(
        f"{base}/thread-idle/append", json={"item": {"text": "again"}}, headers=auth_headers
    )
    assert restarted.status_code == 200, restarted.text
    assert restarted.json()["value"] == [{"text": "again"}]
    assert restarted.json()["version"] == 1


def test_non_transcript_capacity_refusal_records_no_history_failure(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    history_failure_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    aid = _agent(client, auth_headers)
    settings = get_settings()
    settings.state_max_value_bytes = 10
    try:
        refused = client.post(
            f"/agents/{aid}/state/audit/log/append",
            json={"item": {"text": "x" * 100}},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text
        assert _history_failure_points(history_failure_metrics) == []
    finally:
        get_settings.cache_clear()


def test_namespace_exactly_at_the_byte_cap_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Cap check is strict greater-than, so a write that lands exactly on
    # state_max_namespace_bytes must still succeed. Two 50-byte JSON strings
    # fill a 100-byte namespace with no slack.
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/exact-cap"
    fifty = "x" * 48
    assert _json_size(fifty) == 50
    get_settings().state_max_namespace_bytes = 100
    try:
        a = client.put(f"{base}/a", json={"value": fifty}, headers=auth_headers)
        assert a.status_code == 200, a.text
        b = client.put(f"{base}/b", json={"value": fifty}, headers=auth_headers)
        assert b.status_code == 200, b.text
        assert b.json()["value"] == fifty
    finally:
        get_settings.cache_clear()


def test_namespace_one_byte_over_the_byte_cap_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # One extra serialized byte past the same 100-byte cap must 413, and the
    # message text is the load-bearing operator string (do not reword it).
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/over-cap"
    get_settings().state_max_namespace_bytes = 100
    try:
        a = client.put(f"{base}/a", json={"value": "x" * 48}, headers=auth_headers)
        assert a.status_code == 200, a.text
        b = client.put(f"{base}/b", json={"value": "x" * 49}, headers=auth_headers)
        assert b.status_code == 413, b.text
        assert "would be 101 bytes" in b.text
        assert "100-byte per-namespace cap" in b.text
    finally:
        get_settings.cache_clear()


def test_sql_text_bound_over_cap_falls_back_to_exact_size(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """JSONB ::text is an upper bound, not the cap unit.

    Postgres renders object whitespace (`{"k": 1}`) that compact json.dumps
    does not (`{"k":1}`). A namespace of those objects can have
    sum(octet_length(value::text)) over the cap while the exact Python size
    is still under it. That write must be accepted; using the SQL bound as
    the verdict (removing the exact fallback) 413s it.
    """
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    namespace = "sql-bound-slack"
    base = f"/agents/{aid}/state/{namespace}"
    obj = {"k": 1}
    exact_each = _json_size(obj)
    cap = 50
    incoming = obj
    get_settings().state_max_namespace_bytes = cap
    try:
        for i in range(6):
            r = client.put(f"{base}/s{i}", json={"value": obj}, headers=auth_headers)
            assert r.status_code == 200, r.text

        sibling_exact, sibling_sql = asyncio.run(
            _namespace_sibling_sizes(namespace, exclude_key="s6")
        )
        incoming_size = _json_size(incoming)
        assert sibling_exact + incoming_size <= cap, (
            f"exact total {sibling_exact + incoming_size} must be at or under the cap"
        )
        assert sibling_sql + incoming_size > cap, (
            f"SQL text bound {sibling_sql + incoming_size} must exceed the cap so "
            "this test fails if the exact fallback is removed; each="
            f"{exact_each} sql_siblings={sibling_sql}"
        )

        accepted = client.put(f"{base}/s6", json={"value": incoming}, headers=auth_headers)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["value"] == incoming
    finally:
        get_settings.cache_clear()


def test_multibyte_sibling_still_uses_exact_json_dumps_bytes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Postgres jsonb::text is UTF-8; compact json.dumps is ensure_ascii.

    A sibling of three 'é' characters is 8 bytes as jsonb::text but 20 bytes
    as json.dumps. Treating the SQL sum as the verdict would accept a follow-on
    write that is actually over the cap. Exact semantics must still 413.
    """
    from curie_api.config import get_settings

    aid = _agent(client, auth_headers)
    namespace = "unicode-bound"
    base = f"/agents/{aid}/state/{namespace}"
    sibling = "é" * 3
    incoming = "a"
    cap = 20
    assert _json_size(sibling) == 20
    assert _json_size(incoming) == 3
    get_settings().state_max_namespace_bytes = cap
    try:
        first = client.put(f"{base}/u0", json={"value": sibling}, headers=auth_headers)
        assert first.status_code == 200, first.text
        sibling_exact, sibling_sql = asyncio.run(
            _namespace_sibling_sizes(namespace, exclude_key="u1")
        )
        assert sibling_exact == 20, sibling_exact
        assert sibling_sql < cap, sibling_sql
        assert sibling_sql + _json_size(incoming) <= cap
        assert sibling_exact + _json_size(incoming) > cap
        refused = client.put(f"{base}/u1", json={"value": incoming}, headers=auth_headers)
        assert refused.status_code == 413, refused.text
        assert "100-byte per-namespace cap" not in refused.text
        assert "20-byte per-namespace cap" in refused.text
        assert "would be 23 bytes" in refused.text
    finally:
        get_settings.cache_clear()


def test_under_namespace_cap_does_not_reserialize_sibling_values(
    client: Any, auth_headers: dict[str, str], clean_db: None, monkeypatch: Any
) -> None:
    # Common-case O(1) wire: when the SQL upper bound of sibling jsonb::text
    # plus the incoming value is under the namespace cap, _enforce_caps must
    # not json.dumps the sibling values. Pre-change this is RED because every
    # write fetches and re-serializes every other key.
    import curie_api.routers.state as state_mod

    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/hot-path"
    for i in range(4):
        r = client.put(f"{base}/k{i}", json={"value": {"n": i}}, headers=auth_headers)
        assert r.status_code == 200, r.text

    calls: list[Any] = []
    original = state_mod._json_size

    def _spy(value: Any) -> int:
        calls.append(value)
        return original(value)

    monkeypatch.setattr(state_mod, "_json_size", _spy)
    incoming = {"n": 4}
    r = client.put(f"{base}/k4", json={"value": incoming}, headers=auth_headers)
    assert r.status_code == 200, r.text
    assert calls == [incoming], calls


async def _namespace_sibling_sizes(namespace: str, exclude_key: str) -> tuple[int, int]:
    """Exact compact-json bytes vs Postgres jsonb::text bytes for siblings."""
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        rows = await connection.fetch(
            """
            SELECT value
            FROM curie.workflow_state_entries
            WHERE namespace = $1 AND key <> $2
            """,
            namespace,
            exclude_key,
        )
        sql_bound = await connection.fetchval(
            """
            SELECT COALESCE(SUM(octet_length(value::text)), 0)
            FROM curie.workflow_state_entries
            WHERE namespace = $1 AND key <> $2
            """,
            namespace,
            exclude_key,
        )
    finally:
        await connection.close()
    exact = 0
    for row in rows:
        raw = row["value"]
        value = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        exact += _json_size(value)
    return exact, int(sql_bound)


@pytest.mark.skipif(
    not os.environ.get("CURIE_STATE_CAP_BENCH"),
    reason="opt-in namespace-cap microbenchmark; set CURIE_STATE_CAP_BENCH=1",
)
def test_namespace_cap_write_p50_and_payload_bytes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Record p50 PUT latency and cap-check payload bytes at 16 x 64 KiB.

    Not a CI gate: it prints one STATE_CAP_BENCH JSON line for the PR body.
    `legacy_sibling_json_bytes` is what the old path transferred (every sibling
    jsonb::text). `sql_bound_result_bytes` is what the SQL SUM path returns.
    """
    aid = _agent(client, auth_headers)
    namespace = "bench-ns"
    base = f"/agents/{aid}/state/{namespace}"
    value_bytes = 64 * 1024
    payload = "x" * (value_bytes - 2)
    assert _json_size(payload) == value_bytes
    n_keys = 16
    for i in range(n_keys):
        r = client.put(f"{base}/k{i:02d}", json={"value": payload}, headers=auth_headers)
        assert r.status_code == 200, r.text

    sibling_json_bytes, bound_result_bytes = asyncio.run(
        _cap_check_payload_bytes(namespace, exclude_key="k00")
    )
    samples: list[float] = []
    iterations = 30
    for _ in range(iterations):
        started = time.perf_counter()
        r = client.put(f"{base}/k00", json={"value": payload}, headers=auth_headers)
        elapsed_ms = (time.perf_counter() - started) * 1000
        assert r.status_code == 200, r.text
        samples.append(elapsed_ms)
    samples.sort()
    p50 = samples[len(samples) // 2]
    report = {
        "p50_ms": round(p50, 3),
        "samples_ms": [round(s, 3) for s in samples],
        "iterations": iterations,
        "keys": n_keys,
        "value_bytes": value_bytes,
        "legacy_sibling_json_bytes": sibling_json_bytes,
        "sql_bound_result_bytes": bound_result_bytes,
    }
    print(f"STATE_CAP_BENCH {json.dumps(report)}", flush=True)
    assert sibling_json_bytes >= (n_keys - 1) * value_bytes
    assert bound_result_bytes > 0
    assert bound_result_bytes < sibling_json_bytes


async def _cap_check_payload_bytes(namespace: str, exclude_key: str) -> tuple[int, int]:
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        sibling_json_bytes = await connection.fetchval(
            """
            SELECT COALESCE(SUM(octet_length(value::text)), 0)::bigint
            FROM curie.workflow_state_entries
            WHERE namespace = $1 AND key <> $2
            """,
            namespace,
            exclude_key,
        )
        bound_result_bytes = await connection.fetchval(
            """
            SELECT pg_column_size(
                COALESCE(SUM(octet_length(value::text)), 0)
            )
            FROM curie.workflow_state_entries
            WHERE namespace = $1 AND key <> $2
            """,
            namespace,
            exclude_key,
        )
    finally:
        await connection.close()
    return int(sibling_json_bytes), int(bound_result_bytes)


def test_namespace_count_over_the_per_agent_cap_is_rejected(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #852: a per-agent cap on the NUMBER of namespaces refuses a NEW namespace
    # once the agent is at its limit, so a sandbox cannot loop creating unbounded
    # namespaces (each under the byte caps). Writes to an existing namespace are
    # unaffected. Same lru_cache mutate-and-reset pattern as the byte-cap tests.
    aid = _agent(client, auth_headers)

    def put(ns: str, key: str = "k") -> Any:
        return client.put(
            f"/agents/{aid}/state/{ns}/{key}", json={"value": 1}, headers=auth_headers
        )

    get_settings().state_max_namespaces = 2
    try:
        assert put("ns1").status_code == 200
        assert put("ns2").status_code == 200
        # A third, NEW namespace is refused with a clear 4xx, not a 500.
        over = put("ns3")
        assert over.status_code == 403, over.text
        assert "cap" in over.text.lower() and "namespace" in over.text.lower()
        # More keys in an EXISTING namespace still succeed (not a new namespace).
        assert put("ns1", "k2").status_code == 200
    finally:
        get_settings.cache_clear()


def _asyncpg_dsn() -> str:
    url = make_url(get_settings().database_url).set(drivername="postgresql")
    return url.render_as_string(hide_password=False)


async def _install_state_insert_gate() -> None:
    """Hold every INSERT into the state store at an advisory lock (#933).

    The state variant of test_memory.py's BEFORE UPDATE gate: the write that can
    create a new namespace is an INSERT, not an UPDATE. Lock id 933933 is
    distinct from test_memory's 391391 so the two gates can never alias, and both
    live in the ONE-argument advisory space, which Postgres keeps entirely
    separate from the two-int4 space the production namespace lock uses.
    """
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        await connection.execute(
            """
            CREATE OR REPLACE FUNCTION curie.test_state_insert_gate()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(933933);
                RETURN NEW;
            END;
            $$
            """
        )
        await connection.execute(
            """
            CREATE TRIGGER test_state_insert_gate
            BEFORE INSERT ON curie.workflow_state_entries
            FOR EACH ROW
            EXECUTE FUNCTION curie.test_state_insert_gate()
            """
        )
    finally:
        await connection.close()


async def _remove_state_insert_gate() -> None:
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        await connection.execute(
            """
            DROP TRIGGER IF EXISTS test_state_insert_gate
            ON curie.workflow_state_entries
            """
        )
        await connection.execute("DROP FUNCTION IF EXISTS curie.test_state_insert_gate()")
    finally:
        await connection.close()


async def _wait_for_blocked_state_requests(minimum: int, requests: list[Future[Any]]) -> None:
    """Block until `minimum` sessions are waiting on a lock, or fail loudly.

    Deliberately NOT filtered by `query LIKE '%workflow_state_entries%'` the way
    test_memory's twin is: once the cap is atomic, the second writer waits on
    `SELECT pg_advisory_xact_lock($1, $2)` inside the cap check and never reaches
    an INSERT, so its query text names no table and that filter would never see
    it. Leaving the predicate on `wait_event_type` alone is safe because the
    disposable-DB conftest gives the run its own database (so `current_database()`
    already scopes the count to this test's traffic) and the gate holder *holds*
    its advisory lock rather than waiting on one.
    """
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                """
            )
            if int(count) >= minimum:
                return
            # A request that finished instead of blocking means the interleaving
            # under test never happened -- fail loudly rather than hang to the
            # deadline and report a timeout.
            completed = [request.result() for request in requests if request.done()]
            assert not completed, f"request completed before blocking: {completed}"
            await asyncio.sleep(0.01)
        raise AssertionError(f"only some of {minimum} state requests blocked")
    finally:
        await connection.close()


def _hold_advisory_lock(
    args: tuple[int, ...],
    acquired: threading.Event,
    release: threading.Event,
    errors: list[Exception],
) -> None:
    """Hold a Postgres session-level advisory lock from an outside session (#933).

    Shared by both #933 gates: the one-argument INSERT gate (`(933933,)`) and
    the two-argument PRODUCTION namespace lock (`_NAMESPACE_LOCK_CLASS`,
    `_namespace_lock_key(...)`). Postgres keeps the one-argument and
    two-argument advisory-lock spaces entirely separate, so 933933 as a
    single arg can never collide with the two-arg production key.

    Session-level (`pg_advisory_lock`), not transaction-level, because it has
    to outlive the statement that takes it and be released on a signal from
    the test.
    """
    placeholders = ", ".join(f"${i}" for i in range(1, len(args) + 1))

    async def hold() -> None:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            await connection.execute(f"SELECT pg_advisory_lock({placeholders})", *args)
            acquired.set()
            release.wait()
            await connection.execute(f"SELECT pg_advisory_unlock({placeholders})", *args)
        finally:
            # Closing the connection releases the session lock too, so no
            # failure path can leave the key held and wedge every later test.
            await connection.close()

    try:
        asyncio.run(hold())
    except Exception as error:
        errors.append(error)
        acquired.set()


def _request_result_or_exception(request: Future[Any]) -> Any:
    try:
        return request.result(timeout=10)
    except Exception as error:
        return error


def _run_ordered_state_request_outcomes(
    first: Callable[[], Any], second: Callable[[], Any]
) -> tuple[Any, Any]:
    """Return each real request's response or exception after the INSERT gate.

    Ordering is established by the database, not by a timer: `first` is submitted
    and confirmed blocked before `second` is submitted, and both are confirmed
    blocked before the gate is released. "Confirmed blocked" means observed --
    polling `pg_stat_activity` until the expected number of waiters is seen --
    not timed; the poll loop's sleep is pacing between observations, not a
    handoff window, so the interleaving is identical on a loaded box regardless
    of that sleep's duration.
    """
    asyncio.run(_install_state_insert_gate())
    acquired = threading.Event()
    release = threading.Event()
    lock_errors: list[Exception] = []
    lock_thread = threading.Thread(
        target=_hold_advisory_lock,
        args=((933933,), acquired, release, lock_errors),
        daemon=True,
    )
    lock_thread.start()
    try:
        assert acquired.wait(timeout=10), "database gate was not acquired"
        assert not lock_errors, lock_errors
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_request = executor.submit(first)
            try:
                asyncio.run(_wait_for_blocked_state_requests(1, [first_request]))
                second_request = executor.submit(second)
                asyncio.run(_wait_for_blocked_state_requests(2, [first_request, second_request]))
            finally:
                release.set()
            first_outcome = _request_result_or_exception(first_request)
            second_outcome = _request_result_or_exception(second_request)
    finally:
        # An INSERT gate left installed would block essentially every later test
        # in the session, so it comes down even if the orchestration failed.
        release.set()
        lock_thread.join(timeout=10)
        asyncio.run(_remove_state_insert_gate())

    assert not lock_thread.is_alive(), "database gate did not release"
    assert not lock_errors, lock_errors
    return first_outcome, second_outcome


def _run_ordered_state_requests(
    first: Callable[[], Any], second: Callable[[], Any]
) -> tuple[Any, Any]:
    """Success-only wrapper preserving the original #933 helper contract."""
    first_outcome, second_outcome = _run_ordered_state_request_outcomes(first, second)
    if isinstance(first_outcome, Exception):
        raise first_outcome
    if isinstance(second_outcome, Exception):
        raise second_outcome
    return first_outcome, second_outcome


def _state_writer(
    client: Any, headers: dict[str, str], aid: str, namespace: str, key: str
) -> Callable[[], Any]:
    def request() -> Any:
        return client.put(
            f"/agents/{aid}/state/{namespace}/{key}", json={"value": 1}, headers=headers
        )

    return request


def test_existing_namespace_write_does_not_take_the_namespace_lock(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#933 AC4: an existing-namespace write never touches the advisory lock.

    The mutation this catches is deleting the hot-path early `return` in
    `_enforce_caps` (routers/state.py) -- i.e. making the advisory lock
    unconditional. Every other test in this file still passes under that
    mutation, because they all create ABSENT namespaces and so take the lock
    either way. dadf93e2's stated contract is that "writes to an existing
    namespace are unaffected": no lock, no added latency. Without this test that
    sentence is pinned by nothing.

    Both directions are proved under ONE held lock, which is what makes a pass
    meaningful rather than vacuous:

    * the brand-NEW namespace write is confirmed blocked on the key (via
      `pg_stat_activity`, not a sleep), so the key really is the production one
      and the lock really is being contended; then
    * the EXISTING-namespace write must return 200 inside a bounded timeout.

    Failure is a `TimeoutError` from `.result()`, never a hang: the holder is
    released in a `finally` on every path.
    """
    aid = _agent(client, auth_headers)
    # Seed the namespace so the second write into it is an EXISTING-namespace
    # write. Seeded before the lock is taken; seeding afterwards would itself
    # block on the very key under test.
    seed = client.put(f"/agents/{aid}/state/existing/k1", json={"value": 1}, headers=auth_headers)
    assert seed.status_code == 200, seed.text

    acquired = threading.Event()
    release = threading.Event()
    lock_errors: list[Exception] = []
    lock_thread = threading.Thread(
        target=_hold_advisory_lock,
        args=(
            (_NAMESPACE_LOCK_CLASS, _namespace_lock_key(uuid.UUID(aid))),
            acquired,
            release,
            lock_errors,
        ),
        daemon=True,
    )
    lock_thread.start()
    try:
        assert acquired.wait(timeout=10), "production namespace lock was not acquired"
        assert not lock_errors, lock_errors
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Control arm: a NEW namespace must serialize on the held key.
            blocked = executor.submit(_state_writer(client, auth_headers, aid, "brand-new", "k1"))
            try:
                asyncio.run(_wait_for_blocked_state_requests(1, [blocked]))
                # The arm under test. Post-fix it returns before the lock is ever
                # requested; with the hot-path `return` deleted it joins the
                # queue behind the holder and this `.result` raises TimeoutError.
                unblocked = executor.submit(
                    _state_writer(client, auth_headers, aid, "existing", "k2")
                )
                existing_response = unblocked.result(timeout=5)
            finally:
                # Released INSIDE the executor context: exiting the `with` joins
                # its workers with no timeout, so a still-blocked request would
                # hang there instead of failing.
                release.set()
            blocked_response = blocked.result(timeout=10)
    finally:
        release.set()
        lock_thread.join(timeout=10)

    assert not lock_thread.is_alive(), "namespace lock holder did not release"
    assert not lock_errors, lock_errors
    assert existing_response.status_code == 200, existing_response.text
    # The control arm proves the block was real contention, not a dead key: it
    # only completes once the holder lets go.
    assert blocked_response.status_code == 200, blocked_response.text

    listing = client.get(f"/agents/{aid}/state", headers=auth_headers)
    assert listing.status_code == 200, listing.text
    by_ns = {row["namespace"]: row for row in listing.json()}
    assert by_ns["existing"]["key_count"] == 2, listing.text
    assert by_ns["brand-new"]["key_count"] == 1, listing.text


def test_concurrent_new_namespaces_cannot_exceed_the_cap(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#933: a concurrent burst must not walk past the per-agent namespace cap.

    Pre-fix this is RED. The cap is a check-then-write TOCTOU: writer 2's
    unlocked `count(distinct namespace)` sees only the seed, because writer 1's
    `race-a` row is still uncommitted behind the gate, so it reads 1 < 2 and
    passes its check too. Both insert -- two 200s, zero 403s, three namespaces
    for a cap of 2.
    """
    aid = _agent(client, auth_headers)
    # Seed BEFORE the gate is installed; a seed written afterwards would itself
    # block on the gate.
    seed = client.put(f"/agents/{aid}/state/seed/k", json={"value": 1}, headers=auth_headers)
    assert seed.status_code == 200, seed.text

    get_settings().state_max_namespaces = 2
    try:
        # The distinct-namespace count reaches 2 either way, so the same wait
        # predicate holds pre-fix and post-fix. Post-fix writer 1 blocks at the
        # gate while holding the namespace advisory lock and writer 2 blocks on
        # that lock inside the cap check; pre-fix writer 2 sails through its
        # check and blocks at the gate on its own INSERT. Two waiters either way.
        first, second = _run_ordered_state_requests(
            _state_writer(client, auth_headers, aid, "race-a", "k"),
            _state_writer(client, auth_headers, aid, "race-b", "k"),
        )

        # Which writer wins is not under test, so assert the multiset.
        statuses = sorted([first.status_code, second.status_code])
        assert statuses == [200, 403], (first.text, second.text)
        refused = first if first.status_code == 403 else second
        assert "cap" in refused.text.lower() and "namespace" in refused.text.lower()

        listing = client.get(f"/agents/{aid}/state", headers=auth_headers)
        assert listing.status_code == 200, listing.text
        assert len(listing.json()) == 2, listing.text
    finally:
        get_settings.cache_clear()


def test_concurrent_writes_to_the_same_new_namespace_both_succeed(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The false-positive guard for #933's own fix, not regression coverage.

    Stated honestly: this test also passes PRE-fix, because today both writers
    sail through the unlocked check and insert. Its value is in the other
    direction -- it goes RED against a fix that takes the namespace lock WITHOUT
    re-checking existence under it. Such a fix would make writer 2 run the count,
    see 2 (`seed` + `shared-new`) >= 2, and 403 a write that must be allowed. The
    cap and seed numbers are chosen so it discriminates; do not raise the cap.

    The two writers use DIFFERENT keys so the `uq_state_agent_ns_key` unique
    constraint is not involved: this is about the cap, not about insert
    conflicts.
    """
    aid = _agent(client, auth_headers)
    seed = client.put(f"/agents/{aid}/state/seed/k", json={"value": 1}, headers=auth_headers)
    assert seed.status_code == 200, seed.text

    get_settings().state_max_namespaces = 2
    try:
        first, second = _run_ordered_state_requests(
            _state_writer(client, auth_headers, aid, "shared-new", "k1"),
            _state_writer(client, auth_headers, aid, "shared-new", "k2"),
        )

        assert first.status_code == 200, first.text
        assert second.status_code == 200, second.text

        listing = client.get(f"/agents/{aid}/state", headers=auth_headers)
        assert listing.status_code == 200, listing.text
        body = listing.json()
        assert len(body) == 2, body
        by_ns = {row["namespace"]: row for row in body}
        assert by_ns["shared-new"]["key_count"] == 2, body
    finally:
        get_settings.cache_clear()


def test_concurrent_initial_shared_state_creation_is_database_unique(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#1901: PostgreSQL arbitrates concurrent creation of one NULL identity.

    Seeding a different key makes both PUTs take `_enforce_caps`' intentional
    existing-namespace hot path. They therefore race all the way to INSERT,
    where the shared NULL scope must be protected by the named constraint.
    """
    aid = _agent(client, auth_headers)
    namespace = "shared-race"
    target_key = "target"
    seed = client.put(
        f"/agents/{aid}/state/{namespace}/seed",
        json={"value": "existing namespace"},
        headers=auth_headers,
    )
    assert seed.status_code == 200, seed.text

    outcomes = _run_ordered_state_request_outcomes(
        _state_writer(client, auth_headers, aid, namespace, target_key),
        _state_writer(client, auth_headers, aid, namespace, target_key),
    )
    responses = [outcome for outcome in outcomes if not isinstance(outcome, Exception)]
    errors = [outcome for outcome in outcomes if isinstance(outcome, Exception)]

    assert len(responses) == 1, outcomes
    assert responses[0].status_code == 200, responses[0].text
    assert len(errors) == 1, outcomes
    assert isinstance(errors[0], IntegrityError), repr(errors[0])
    assert "uq_state_agent_scope_ns_key" in str(errors[0])

    listing = client.get(f"/agents/{aid}/state/{namespace}", headers=auth_headers)
    assert listing.status_code == 200, listing.text
    target_rows = [entry for entry in listing.json() if entry["key"] == target_key]
    assert len(target_rows) == 1, listing.text

    readback = client.get(
        f"/agents/{aid}/state/{namespace}/{target_key}", headers=auth_headers
    )
    assert readback.status_code == 200, readback.text
    assert readback.json()["value"] == 1


def test_list_namespaces_summarizes_the_store_recent_first(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The operator read/inspect surface (#250) enumerates an agent's namespaces
    # with key counts and the last write time, most-recently-written first.
    aid = _agent(client, auth_headers)
    assert client.get(f"/agents/{aid}/state", headers=auth_headers).json() == []

    client.put(f"/agents/{aid}/state/alpha/a", json={"value": 1}, headers=auth_headers)
    client.put(f"/agents/{aid}/state/alpha/b", json={"value": 2}, headers=auth_headers)
    # beta is written after alpha, so it sorts first (most recent).
    client.put(f"/agents/{aid}/state/beta/c", json={"value": 3}, headers=auth_headers)

    resp = client.get(f"/agents/{aid}/state", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    namespaces = [row["namespace"] for row in body]
    assert namespaces == ["beta", "alpha"]
    by_ns = {row["namespace"]: row for row in body}
    assert by_ns["alpha"]["key_count"] == 2
    assert by_ns["beta"]["key_count"] == 1
    assert by_ns["alpha"]["last_updated"] and by_ns["beta"]["last_updated"]


def test_binding_scoped_state_is_isolated_from_the_shared_and_sibling_scopes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#1525 follow-up: the `/state/bindings/{kind}/{address}/...` path names an
    independent partition of the same agent's general state, distinct from the
    plain (shared) path and from any other binding's own partition. This is
    what makes a memory=False agent's bindings behave as isolated instances
    that happen to share one bundle/budget/kill-state, and a memory=True
    agent's worth of state (the plain path) as one instance reachable from
    several doors -- the worker decides which URL to hand a runner (never
    exercised here, an API-level test), but the API must honor both shapes
    correctly regardless of which one is asked for.
    """
    aid = _agent(client, auth_headers)
    second = client.post(
        f"/agents/{aid}/channels",
        json={"kind": "slack", "address": "C000000S02"},
        headers=auth_headers,
    )
    assert second.status_code == 201, second.text

    shared_url = f"/agents/{aid}/state/ns/k"
    first_url = f"/agents/{aid}/state/bindings/slack/C000000S01/ns/k"
    second_url = f"/agents/{aid}/state/bindings/slack/C000000S02/ns/k"

    assert client.put(shared_url, json={"value": "shared"}, headers=auth_headers).status_code == 200
    assert client.put(first_url, json={"value": "first"}, headers=auth_headers).status_code == 200
    assert client.put(second_url, json={"value": "second"}, headers=auth_headers).status_code == 200

    assert client.get(shared_url, headers=auth_headers).json()["value"] == "shared"
    assert client.get(first_url, headers=auth_headers).json()["value"] == "first"
    assert client.get(second_url, headers=auth_headers).json()["value"] == "second"

    # Each scope's namespace listing sees only its own row.
    listed = client.get(f"/agents/{aid}/state/bindings/slack/C000000S01/ns", headers=auth_headers)
    assert [e["value"] for e in listed.json()] == ["first"]


def test_binding_scoped_route_404s_for_a_pair_that_is_not_this_agents(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    resp = client.get(
        f"/agents/{aid}/state/bindings/slack/C000000S03/ns/k", headers=auth_headers
    )
    assert resp.status_code == 404
    assert "binding" in resp.text.lower()


def test_binding_scoped_route_accepts_the_same_scoped_token_shape_as_the_plain_route(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The binding path adds no new credential shape (#1525 follow-up): the
    # SAME app-scoped sandbox token that already works on the plain path
    # authenticates here too, since the token still only ever names an agent,
    # never a binding -- the rejected #1525 alternative was widening it to.
    aid = _agent(client, auth_headers)
    api_key = get_settings().api_key
    token = mint(api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
    headers = {"X-API-Key": token}
    url = f"/agents/{aid}/state/bindings/slack/C000000S01/ns/k"

    put = client.put(url, json={"value": {"n": 1}}, headers=headers)
    assert put.status_code == 200, put.text
    assert client.get(url, headers=headers).json()["value"] == {"n": 1}


def test_value_cap_refusal_names_the_key_over_the_limit(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #2820: a transcript key is a thread key, so naming the key names the
    # thread an operator must recover.
    aid = _agent(client, auth_headers)
    settings = get_settings()
    settings.transcript_max_thread_bytes = 50
    try:
        refused = client.post(
            f"/agents/{aid}/state/transcript/thread-runaway/append",
            json={"item": {"kind": "message", "text": "x" * 200}},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text
        assert "'thread-runaway'" in refused.json()["detail"]
    finally:
        get_settings.cache_clear()


def test_namespace_cap_refusal_names_the_largest_thread_not_the_caller(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #2820: one runaway key fills a namespace and a small sibling's append is
    # refused. The refusal must name the runaway key, which is the one to export
    # and delete, not only the caller. Transcripts no longer share a namespace
    # (ADR-0170), so this now guards general state.
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/audit"
    settings = get_settings()
    settings.state_max_value_bytes = 10_000
    settings.state_max_namespace_bytes = 400
    try:
        runaway = client.post(
            f"{base}/thread-runaway/append",
            json={"item": {"kind": "message", "text": "r" * 300}},
            headers=auth_headers,
        )
        assert runaway.status_code == 200, runaway.text
        small = client.post(
            f"{base}/thread-small/append",
            json={"item": {"kind": "message", "text": "s" * 20}},
            headers=auth_headers,
        )
        assert small.status_code == 200, small.text

        refused = client.post(
            f"{base}/thread-small/append",
            json={"item": {"kind": "message", "text": "s" * 80}},
            headers=auth_headers,
        )
        assert refused.status_code == 413, refused.text
        detail = refused.json()["detail"]
        assert "'thread-runaway'" in detail
        assert "largest key" in detail

        # Negative control: when the caller's own key is the largest, the
        # refusal names the caller rather than an innocent sibling.
        refused_self = client.post(
            f"{base}/thread-runaway/append",
            json={"item": {"kind": "message", "text": "r" * 80}},
            headers=auth_headers,
        )
        assert refused_self.status_code == 413, refused_self.text
        assert "largest key 'thread-runaway'" in refused_self.json()["detail"]
        assert "'thread-small'" not in refused_self.json()["detail"]
    finally:
        get_settings.cache_clear()


def test_delete_with_expected_version_refuses_a_row_that_moved(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #2820: the documented capacity recovery exports a transcript, then
    # deletes it. A DELETE carrying the exported version must refuse when a
    # turn appended in between, so the recovery never destroys history it did
    # not export.
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-recover"
    first = client.post(
        f"{url}/append", json={"item": {"kind": "message", "text": "one"}},
        headers=auth_headers,
    )
    assert first.status_code == 200, first.text
    exported_version = client.get(url, headers=auth_headers).json()["version"]
    moved = client.post(
        f"{url}/append", json={"item": {"kind": "message", "text": "two"}},
        headers=auth_headers,
    )
    assert moved.status_code == 200, moved.text

    stale = client.delete(
        url, params={"expected_version": exported_version}, headers=auth_headers
    )
    assert stale.status_code == 409, stale.text
    kept = client.get(url, headers=auth_headers)
    assert kept.status_code == 200
    assert len(kept.json()["value"]) == 2

    current = kept.json()["version"]
    ok = client.delete(url, params={"expected_version": current}, headers=auth_headers)
    assert ok.status_code == 204, ok.text
    assert client.get(url, headers=auth_headers).status_code == 404

    # A versioned delete of a row that no longer exists is a conflict, not a
    # silent success: the exported version is not what is stored.
    gone = client.delete(url, params={"expected_version": current}, headers=auth_headers)
    assert gone.status_code == 409, gone.text
    # The unversioned delete keeps its idempotent 204.
    assert client.delete(url, headers=auth_headers).status_code == 204


def test_binding_scoped_delete_honors_expected_version(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #2820 sibling path: the binding-scoped DELETE shares the versioned
    # recovery, so a stale version never removes that partition's row.
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/bindings/slack/C000000S01/transcript/thread-b"
    first = client.put(url, json={"value": [1]}, headers=auth_headers)
    assert first.status_code == 200, first.text
    moved = client.put(url, json={"value": [1, 2]}, headers=auth_headers)
    assert moved.status_code == 200, moved.text

    stale = client.delete(
        url, params={"expected_version": first.json()["version"]}, headers=auth_headers
    )
    assert stale.status_code == 409, stale.text
    assert client.get(url, headers=auth_headers).json()["value"] == [1, 2]

    ok = client.delete(
        url, params={"expected_version": moved.json()["version"]}, headers=auth_headers
    )
    assert ok.status_code == 204, ok.text
    assert client.get(url, headers=auth_headers).status_code == 404


def _hold_row_lock_then_append(
    aid: str,
    key: str,
    item: Any,
    locked: threading.Event,
    release: threading.Event,
    errors: list[Exception],
) -> None:
    """A concurrent append from an outside session (#2927).

    Takes the transcript row lock exactly as an append does (SELECT ... FOR UPDATE),
    signals, waits for the test's go-ahead, then appends `item` and bumps the
    version before committing.
    """

    async def hold() -> None:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT id, value
                    FROM curie.thread_transcripts
                    WHERE agent_id = $1 AND binding_scope IS NULL
                      AND thread_key = $2
                    FOR UPDATE
                    """,
                    uuid.UUID(aid),
                    key,
                )
                assert row is not None
                locked.set()
                await asyncio.to_thread(release.wait, 10)
                await connection.execute(
                    """
                    UPDATE curie.thread_transcripts
                    SET value = $2::jsonb, version = version + 1
                    WHERE id = $1
                    """,
                    row["id"],
                    json.dumps([*json.loads(row["value"]), item]),
                )
        finally:
            await connection.close()

    try:
        asyncio.run(hold())
    except Exception as error:
        errors.append(error)
        locked.set()


def test_cas_put_behind_a_locked_concurrent_append_is_409_and_keeps_the_append(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#2927: capacity compaction rewrites a transcript with a CAS PUT.

    The PUT read version N; a concurrent append holds the row lock and commits
    N+1. The PUT must see N+1 under the same row lock and refuse with 409. Before
    the fix the PUT's plain read raced the lock, passed the version check on the
    stale N, then waited on the row and overwrote the committed append.
    """
    aid = _agent(client, auth_headers)
    key = "thread-cas-2927"
    url = f"/agents/{aid}/state/transcript/{key}"
    seeded = client.post(
        f"{url}/append", json={"item": {"text": "one"}}, headers=auth_headers
    )
    assert seeded.status_code == 200, seeded.text
    read_version = client.get(url, headers=auth_headers).json()["version"]

    locked = threading.Event()
    release = threading.Event()
    errors: list[Exception] = []
    holder = threading.Thread(
        target=_hold_row_lock_then_append,
        args=(aid, key, {"text": "two"}, locked, release, errors),
        daemon=True,
    )
    holder.start()
    try:
        assert locked.wait(timeout=10), "row lock was not taken"
        assert not errors, errors
        with ThreadPoolExecutor(max_workers=1) as executor:
            put = executor.submit(
                client.put,
                url,
                json={"value": [{"text": "compacted"}], "expected_version": read_version},
                headers=auth_headers,
            )
            try:
                # The PUT is observed waiting on the append's row lock before the
                # append is allowed to commit: the interleaving is not timed.
                asyncio.run(_wait_for_blocked_state_requests(1, [put]))
            finally:
                release.set()
            outcome = _request_result_or_exception(put)
    finally:
        release.set()
        holder.join(timeout=10)

    assert not holder.is_alive(), "concurrent append did not finish"
    assert not errors, errors
    assert not isinstance(outcome, Exception), repr(outcome)
    assert outcome.status_code == 409, outcome.text
    stored = client.get(url, headers=auth_headers).json()
    assert stored["value"] == [{"text": "one"}, {"text": "two"}]
    assert stored["version"] == read_version + 1


def test_append_reserve_refusal_is_413_unchanged_and_not_a_persistence_failure(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    history_failure_metrics: tuple[MeterProvider, InMemoryMetricReader],
) -> None:
    """#2927: `reserve_bytes` keeps headroom for the worker's publication append.

    An append that would leave fewer than `reserve_bytes` free under the value
    cap is refused atomically with 413 and leaves the log as it was. That refusal
    is the runner's compaction trigger, not a persistence failure, so it does not
    count toward curie.history.persistence.failure. The same append without a
    reserve still succeeds, and a real over-the-cap append still counts.
    """
    aid = _agent(client, auth_headers)
    url = f"/agents/{aid}/state/transcript/thread-reserve/append"
    seed = {"kind": "message", "text": "seed"}
    item = {"kind": "message", "text": "x" * 100}
    settings = get_settings()
    # After appending `item` exactly 50 bytes remain free under the cap.
    settings.transcript_max_thread_bytes = _json_size([seed, item]) + 50
    try:
        initial = client.post(url, json={"item": seed}, headers=auth_headers)
        assert initial.status_code == 200, initial.text

        reserved = client.post(
            url, json={"item": item, "reserve_bytes": 51}, headers=auth_headers
        )
        assert reserved.status_code == 413, reserved.text
        assert "reserve" in reserved.json()["detail"]
        stored = client.get(url.removesuffix("/append"), headers=auth_headers).json()
        assert stored["value"] == [seed]
        assert stored["version"] == initial.json()["version"]
        assert _history_failure_points(history_failure_metrics) == []

        plain = client.post(url, json={"item": item}, headers=auth_headers)
        assert plain.status_code == 200, plain.text
        assert plain.json()["value"] == [seed, item]
        assert _history_failure_points(history_failure_metrics) == []

        over_cap = client.post(
            url,
            json={"item": {"kind": "message", "text": "y" * 200}, "reserve_bytes": 51},
            headers=auth_headers,
        )
        assert over_cap.status_code == 413, over_cap.text
        assert client.get(
            url.removesuffix("/append"), headers=auth_headers
        ).json()["value"] == [seed, item]
        assert _history_failure_points(history_failure_metrics) == [
            (
                1,
                {
                    "service.name": "curie-api",
                    "source": "state-api",
                    "outcome": "capacity",
                    "limit": "value",
                },
            )
        ]
    finally:
        get_settings.cache_clear()


def _legacy_transcript(aid: str, key: str, value: Any, *, ago_seconds: int = 0) -> None:
    """A transcript row an older API instance wrote to the state store (pre-0053)."""

    async def write() -> None:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            await connection.execute(
                """
                INSERT INTO curie.workflow_state_entries
                    (id, agent_id, namespace, key, value, version, updated_at)
                VALUES ($1, $2, 'transcript', $3, $4::jsonb, 7,
                        now() - make_interval(secs => $5))
                """,
                uuid.uuid4(),
                uuid.UUID(aid),
                key,
                json.dumps(value),
                ago_seconds,
            )
        finally:
            await connection.close()

    asyncio.run(write())


def _legacy_keys(aid: str) -> list[str]:
    async def read() -> list[str]:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            rows = await connection.fetch(
                "SELECT key FROM curie.workflow_state_entries "
                "WHERE agent_id = $1 AND namespace = 'transcript' ORDER BY key",
                uuid.UUID(aid),
            )
            return [row["key"] for row in rows]
        finally:
            await connection.close()

    return asyncio.run(read())


def test_a_legacy_transcript_row_is_adopted_on_first_access(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """ADR-0170 upgrade: rows an older API wrote during a rollout are not lost."""
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/transcript"
    _legacy_transcript(aid, "thread-read", [{"text": "old"}])
    _legacy_transcript(aid, "thread-append", [{"text": "old"}])
    _legacy_transcript(aid, "thread-listed", [{"text": "listed"}])

    read = client.get(f"{base}/thread-read", headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["value"] == [{"text": "old"}]
    assert read.json()["version"] == 7

    appended = client.post(
        f"{base}/thread-append/append", json={"item": {"text": "new"}}, headers=auth_headers
    )
    assert appended.status_code == 200, appended.text
    assert appended.json()["value"] == [{"text": "old"}, {"text": "new"}]
    assert appended.json()["version"] == 8

    listed = client.get(base, headers=auth_headers).json()
    assert {row["key"] for row in listed} == {"thread-read", "thread-append", "thread-listed"}
    # An older API instance still serving mid-rollout keeps every legacy row.
    assert _legacy_keys(aid) == ["thread-append", "thread-listed", "thread-read"]
    # The new API's own append is not undone by re-reading the older legacy row.
    again = client.get(f"{base}/thread-append", headers=auth_headers).json()
    assert again["value"] == [{"text": "old"}, {"text": "new"}]


def test_a_legacy_row_newer_than_the_copy_wins_and_an_older_one_does_not(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/transcript"
    for key in ("thread-newer", "thread-older"):
        seeded = client.post(
            f"{base}/{key}/append", json={"item": {"text": "copy"}}, headers=auth_headers
        )
        assert seeded.status_code == 200, seeded.text
    # An older API instance appended after the copy (newer), or wrote a stale row.
    _legacy_transcript(aid, "thread-newer", [{"text": "copy"}, {"text": "later"}])
    _legacy_transcript(aid, "thread-older", [{"text": "stale"}], ago_seconds=3600)

    newer = client.get(f"{base}/thread-newer", headers=auth_headers).json()
    assert newer["value"] == [{"text": "copy"}, {"text": "later"}]
    assert newer["version"] == 8
    older = client.get(f"{base}/thread-older", headers=auth_headers).json()
    assert older["value"] == [{"text": "copy"}]


def test_ending_a_thread_deletes_its_legacy_row_so_it_is_not_adopted_back(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/transcript"
    _legacy_transcript(aid, "thread-deleted", [{"text": "old"}])
    _legacy_transcript(aid, "thread-versioned", [{"text": "old"}])
    _legacy_transcript(aid, "thread-idle", [{"text": "old"}])
    _legacy_transcript(aid, "thread-kept", [{"text": "old"}])

    assert client.delete(f"{base}/thread-deleted", headers=auth_headers).status_code == 204
    assert client.get(f"{base}/thread-deleted", headers=auth_headers).status_code == 404

    stored = client.get(f"{base}/thread-versioned", headers=auth_headers).json()
    versioned = client.delete(
        f"{base}/thread-versioned",
        params={"expected_version": stored["version"]},
        headers=auth_headers,
    )
    assert versioned.status_code == 204, versioned.text
    assert client.get(f"{base}/thread-versioned", headers=auth_headers).status_code == 404

    settings = get_settings()
    settings.transcript_idle_ttl_seconds = 0
    try:
        # Adopted with a zero idle window, the thread is already expired.
        idle = client.get(f"{base}/thread-idle", headers=auth_headers)
        assert idle.status_code == 404, idle.text
    finally:
        get_settings.cache_clear()
    # Any later write for the agent sweeps the expired thread and its legacy row.
    swept = client.post(
        f"{base}/thread-other/append", json={"item": {"text": "x"}}, headers=auth_headers
    )
    assert swept.status_code == 200, swept.text
    assert client.get(f"{base}/thread-idle", headers=auth_headers).status_code == 404
    assert _legacy_keys(aid) == ["thread-kept"]
