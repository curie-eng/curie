"""#3776: a turn's memory write credential is refused once the turn has ended.

ADR 0188's per-turn credential (``{binding, memory: "write", sender, turn}``)
is a bearer token valid until ``exp``. Code in the sandbox that copies it could
keep writing facts attributed to that turn's sender after the turn ends. The
worker now tells the API when a turn ends, on the internal route
``POST /v1/internal/memory/closed-turns`` (worker-token auth, like the other
``/v1/internal`` routes), and the API refuses a write made with a closed turn's
credential even before it expires. Reads are unaffected.

Runs against the real app, Postgres and Valkey the conftest provisions.
"""

import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from curie_test_support.valkey import connect_or_skip

_FAR_FUTURE = 4102444800  # 2100-01-01: the credential is nowhere near expiry
CHANNEL = "C0EXAMPLE1"
OTHER_CHANNEL = "C0EXAMPLE2"
SENDER = "U0ALICE001"
FACT = "fact-" + "0123456789abcdef" * 2
OTHER_FACT = "fact-" + "fedcba9876543210" * 2
CLOSE_URL = "/v1/internal/memory/closed-turns"
TURN_ENDED = "this conversation's turn has ended"
# The longest a per-turn credential lives: the worker's SANDBOX_TOKEN_TTL_SECONDS.
LONGEST_TURN_TOKEN_S = 24 * 60 * 60


def _worker_headers() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def _agent(client: Any, auth_headers: dict[str, str], channel: str = CHANNEL) -> str:
    resp = client.post(
        "/agents",
        json={
            "name": f"turn-closed-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    return aid


def _turn() -> str:
    return f"evt-{uuid.uuid4().hex}#1"


def _writer(aid: str, turn: str, channel: str = CHANNEL) -> dict[str, str]:
    token = mint(
        get_settings().api_key,
        agent=aid,
        scope="state",
        exp=_FAR_FUTURE,
        claims={"binding": f"slack:{channel}", "memory": "write", "sender": SENDER, "turn": turn},
    )
    return {"X-API-Key": token}


def _channel_memory(aid: str, channel: str = CHANNEL) -> str:
    return f"/agents/{aid}/state/bindings/slack/{channel}/memory"


def _agent_memory(aid: str) -> str:
    return f"/agents/{aid}/state/memory"


def _fact(statement: str = "the deploy window is Tuesday") -> dict[str, Any]:
    return {
        "value": {
            "statement": statement,
            "author": SENDER,
            "stated_at": "2026-10-01T00:00:00+00:00",
            "session_id": "sess-1",
        }
    }


def _close(client: Any, aid: str, turn: str) -> Any:
    return client.post(CLOSE_URL, json={"agent_id": aid, "turn": turn}, headers=_worker_headers())


def _detail(resp: Any) -> str:
    return str(resp.json().get("detail", ""))


def test_close_requires_the_internal_worker_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    body = {"agent_id": aid, "turn": turn}
    for headers in (
        {},
        {"X-Curie-Worker-Token": "not-the-worker-token"},
        # Neither the platform key nor a sandbox credential may close a turn.
        auth_headers,
        _writer(aid, turn),
        {"X-Curie-Worker-Token": _writer(aid, turn)["X-API-Key"]},
    ):
        resp = client.post(CLOSE_URL, json=body, headers=headers)
        assert resp.status_code in (401, 403), (headers.keys(), resp.status_code, resp.text)
    # None of those closed it: the turn's credential still writes.
    put = client.put(f"{_channel_memory(aid)}/{FACT}", json=_fact(), headers=_writer(aid, turn))
    assert put.status_code == 200, put.text

    closed = _close(client, aid, turn)
    assert closed.status_code == 204, closed.text


def test_closed_turn_credential_cannot_write_before_expiry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    writer = _writer(aid, turn)
    for memory in (_channel_memory(aid), _agent_memory(aid)):
        before = client.put(f"{memory}/{FACT}", json=_fact(), headers=writer)
        assert before.status_code == 200, before.text

    assert _close(client, aid, turn).status_code == 204

    for memory in (_channel_memory(aid), _agent_memory(aid)):
        put = client.put(f"{memory}/{OTHER_FACT}", json=_fact("planted"), headers=writer)
        assert put.status_code == 403, put.text
        assert TURN_ENDED in _detail(put)
        overwrite = client.put(f"{memory}/{FACT}", json=_fact("rewritten"), headers=writer)
        assert overwrite.status_code == 403, overwrite.text
        assert TURN_ENDED in _detail(overwrite)
        deleted = client.delete(f"{memory}/{FACT}", headers=writer)
        assert deleted.status_code == 403, deleted.text
        assert TURN_ENDED in _detail(deleted)

        # Nothing changed underneath.
        stored = client.get(f"{memory}/{FACT}", headers=auth_headers)
        assert stored.status_code == 200, stored.text
        assert stored.json()["value"]["statement"] == "the deploy window is Tuesday"
        assert client.get(f"{memory}/{OTHER_FACT}", headers=auth_headers).status_code == 404


def test_closed_turn_credential_still_reads(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    writer = _writer(aid, turn)
    put = client.put(f"{_channel_memory(aid)}/{FACT}", json=_fact(), headers=writer)
    assert put.status_code == 200, put.text

    assert _close(client, aid, turn).status_code == 204

    got = client.get(f"{_channel_memory(aid)}/{FACT}", headers=writer)
    assert got.status_code == 200, got.text
    assert got.json()["value"]["statement"] == "the deploy window is Tuesday"
    listed = client.get(_channel_memory(aid), headers=writer)
    assert listed.status_code == 200, listed.text
    assert [e["key"] for e in listed.json()] == [FACT]
    assert client.get(_agent_memory(aid), headers=writer).status_code == 200


def test_open_turn_credential_still_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    closed_turn, open_turn = _turn(), _turn()
    assert _close(client, aid, closed_turn).status_code == 204

    put = client.put(
        f"{_channel_memory(aid)}/{FACT}", json=_fact(), headers=_writer(aid, open_turn)
    )
    assert put.status_code == 200, put.text
    # The platform key has no turn and is never refused by a close.
    platform = client.put(
        f"{_channel_memory(aid)}/{OTHER_FACT}", json=_fact(), headers=auth_headers
    )
    assert platform.status_code == 200, platform.text


def test_close_is_scoped_to_its_agent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The same turn id closed for one agent does not refuse another agent's
    # credential that happens to carry it.
    first = _agent(client, auth_headers)
    second = _agent(client, auth_headers, OTHER_CHANNEL)
    turn = _turn()
    assert _close(client, first, turn).status_code == 204

    refused = client.put(
        f"{_channel_memory(first)}/{FACT}", json=_fact(), headers=_writer(first, turn)
    )
    assert refused.status_code == 403, refused.text
    allowed = client.put(
        f"{_channel_memory(second, OTHER_CHANNEL)}/{FACT}",
        json=_fact(),
        headers=_writer(second, turn, OTHER_CHANNEL),
    )
    assert allowed.status_code == 200, allowed.text


def test_close_is_idempotent(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    aid = _agent(client, auth_headers)
    turn = _turn()
    assert _close(client, aid, turn).status_code == 204
    assert _close(client, aid, turn).status_code == 204
    put = client.put(f"{_channel_memory(aid)}/{FACT}", json=_fact(), headers=_writer(aid, turn))
    assert put.status_code == 403, put.text


@pytest.mark.parametrize("body", [{"turn": ""}, {}, {"turn": "evt-1", "agent_id": "not-a-uuid"}])
def test_close_rejects_a_malformed_body(
    client: Any, auth_headers: dict[str, str], clean_db: None, body: dict[str, str]
) -> None:
    aid = _agent(client, auth_headers)
    payload = {"agent_id": aid, **body}
    resp = client.post(CLOSE_URL, json=payload, headers=_worker_headers())
    assert resp.status_code == 422, resp.text


def test_closed_turn_record_expires_after_the_longest_credential(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The record only has to outlive the credential: a TTL'd Valkey key, no
    # unbounded growth.
    aid = _agent(client, auth_headers)
    turn = _turn()
    assert _close(client, aid, turn).status_code == 204

    valkey = connect_or_skip(decode_responses=True)
    try:
        keys = [k for k in valkey.scan_iter(match=f"*{turn}*") if aid in k]
        assert len(keys) == 1, keys
        ttl = valkey.ttl(keys[0])
        assert 0 < ttl <= LONGEST_TURN_TOKEN_S, ttl
        assert ttl > LONGEST_TURN_TOKEN_S - 60, ttl
    finally:
        valkey.close()
