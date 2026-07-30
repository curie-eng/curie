"""Durable KV state store (#23): real Postgres round-trip + compare-and-set.

Nothing mocked -- exercises the API against the compose Postgres (the
disposable-DB conftest provisions and migrates a throwaway database per run).
"""

import uuid
from typing import Any

from curie_api.config import get_settings
from curie_api.sandbox_token import mint

# Scoped-sandbox-token auth matrix constants (#410).
_FAR_FUTURE = 4102444800  # 2100-01-01, valid at test time
_PAST = 1000000000  # 2001, expired at test time


def _agent(client: Any, headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={"name": "state-agent", "slack_channel": "C000000S01"},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    agent_id: str = resp.json()["id"]
    return agent_id


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
        put = client.put(f"/agents/{aid}/state/{ns}/k", json={"value": {"n": 1}}, headers=headers)
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
        row["namespace"] for row in client.get(f"/agents/{aid}/state", headers=auth_headers).json()
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
            r = client.put(f"/agents/{aid}/state/{ns}/k", json={"value": {"n": 1}}, headers=headers)
            assert r.status_code == 200, f"{ns}: {r.text}"


def test_put_get_list_delete_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    base = f"/agents/{aid}/state/approvals"

    # put (create) -> version 1
    r = client.put(f"{base}/thread-1", json={"value": {"status": "pending"}}, headers=auth_headers)
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
    client.put(f"{base}/thread-2", json={"value": {"status": "pending"}}, headers=auth_headers)
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
    ok = client.put(url, json={"value": {"n": 2}, "expected_version": 1}, headers=auth_headers)
    assert ok.status_code == 200
    assert ok.json()["version"] == 2

    # CAS with the now-stale version 1 is rejected, and the value is unchanged.
    stale = client.put(url, json={"value": {"n": 3}, "expected_version": 1}, headers=auth_headers)
    assert stale.status_code == 409, stale.text
    assert client.get(url, headers=auth_headers).json()["value"] == {"n": 2}


def test_put_unknown_agent_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    r = client.put(f"/agents/{missing}/state/ns/k", json={"value": {}}, headers=auth_headers)
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
