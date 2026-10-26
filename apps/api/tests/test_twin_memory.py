"""ADR 0249 twin memory: the API half of Contract r4.

Every case drives the real state and agent routers over HTTP against the
disposable Postgres and the compose Valkey the conftest provisions. Nothing is
mocked. Sandbox credentials are minted with the shared ``sandbox_token`` module
the worker uses, carrying the same claims the worker signs: a per-turn write
credential is ``{binding, memory: "write", sender, turn, conversation}`` and the
boot credential is ``{binding, memory: "read"}``. Where the API has no read path
(the immutable revision rows) the test reads ``curie.memory_revisions`` with SQL.

Every refusal has a liveness twin that shows the same caller succeeding once
the refused condition is removed, so a refusal can never pass because the
route is simply broken.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from _migration_support import sql_dicts
from curie_api import memory_records
from curie_api.config import get_settings
from curie_api.memory_guidance import DEFAULT_MEMORY_GUIDANCE
from curie_internal.sandbox_token import mint
from sqlalchemy import make_url

ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests" / "vectors" / "twin-memory.json"
OPENAPI = ROOT / "apps" / "api" / "openapi.json"

_FAR_FUTURE = 4102444800  # 2100-01-01
CHANNEL_A = "C0EXAMPLE1"
CHANNEL_B = "C0EXAMPLE2"
BINDING_A = f"slack:{CHANNEL_A}"
CONVERSATION_A = f"slack:{CHANNEL_A}:1700000000.000001"
CONVERSATION_A2 = f"slack:{CHANNEL_A}:1700000000.000002"
SENDER = "U0ALICE001"
SECOND_SENDER = "U0BOB00001"
THIRD_SENDER = "U0CAROL001"
CLOSE_URL = "/v1/internal/memory/closed-turns"
FACT_KEY = re.compile(r"^fact-[0-9a-f]{32}$")

ORIGINAL_DEFAULT_SHA256 = "ac65ffde36f1252cd369d9d5c6226ca006782cf045c3cf758a5704fbf9338e2f"
ORIGINAL_DEFAULT_BYTES = 1412


# --- fixtures and helpers -----------------------------------------------------


@pytest.fixture
def entry_cap() -> Iterator[Callable[[int], None]]:
    """Set ``Settings.memory_max_entries_per_scope`` for one test.

    ``get_settings`` is lru-cached and read per request, so the cap is set
    through the environment and the cache cleared, then both restored."""

    name = "MEMORY_MAX_ENTRIES_PER_SCOPE"
    saved = os.environ.get(name)

    def set_cap(value: int) -> None:
        os.environ[name] = str(value)
        get_settings.cache_clear()

    try:
        yield set_cap
    finally:
        if saved is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = saved
        get_settings.cache_clear()


def _agent(
    client: Any,
    auth_headers: dict[str, str],
    *,
    writes: bool = True,
    learning: bool = False,
    channel_a: str = CHANNEL_A,
    channel_b: str = CHANNEL_B,
) -> str:
    # One agent per route: a second agent in the same test passes its own
    # channel addresses, or the API refuses it with 409.
    resp = client.post(
        "/agents",
        json={
            "name": f"twin-memory-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel_a},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    aid: str = resp.json()["id"]
    second = client.post(
        f"/agents/{aid}/channels",
        json={"kind": "slack", "address": channel_b},
        headers=auth_headers,
    )
    assert second.status_code == 201, second.text
    patched = _patch(
        client, auth_headers, aid, {"memory_writes": writes, "agent_wide_learning": learning}
    )
    assert patched.status_code == 200, patched.text
    return aid


def _patch(client: Any, auth_headers: dict[str, str], aid: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{aid}", json=body, headers=auth_headers)


def _read_agent(client: Any, auth_headers: dict[str, str], aid: str) -> dict[str, Any]:
    resp = client.get(f"/agents/{aid}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body: dict[str, Any] = resp.json()
    return body


def _mint(aid: str, claims: dict[str, Any] | None) -> dict[str, str]:
    token = mint(get_settings().api_key, agent=aid, scope="state", exp=_FAR_FUTURE, claims=claims)
    return {"X-API-Key": token}


def _reader(aid: str, binding: str | None = BINDING_A) -> dict[str, str]:
    return _mint(aid, {"binding": binding, "memory": "read"})


def _writer(
    aid: str,
    *,
    sender: str | None = SENDER,
    turn: str | None = None,
    conversation: str | None = CONVERSATION_A,
    binding: str | None = BINDING_A,
) -> dict[str, str]:
    claims: dict[str, Any] = {
        "binding": binding,
        "memory": "write",
        "turn": turn or f"evt-{uuid.uuid4().hex}#1",
    }
    if sender is not None:
        claims["sender"] = sender
    if conversation is not None:
        claims["conversation"] = conversation
    return _mint(aid, claims)


def _fid() -> str:
    return "fact-" + uuid.uuid4().hex


def _value(
    kind: str = "fact",
    title: str = "Deploy window",
    statement: str = "The deploy window is Tuesday.",
    **overrides: Any,
) -> dict[str, Any]:
    # The provenance fields carry forged values on purpose: a sandbox caller's
    # author, stated_at, conversation and archived are the server's to set.
    value: dict[str, Any] = {
        "kind": kind,
        "title": title,
        "statement": statement,
        "author": "U0FORGED01",
        "stated_at": "1999-01-01T00:00:00Z",
        "session_id": "sess-body",
        "conversation": "forged-conversation",
        "archived": False,
    }
    value.update(overrides)
    return value


def _agent_url(aid: str, fid: str) -> str:
    return f"/agents/{aid}/state/memory/{fid}"


def _chan_url(aid: str, fid: str, address: str = CHANNEL_A) -> str:
    return f"/agents/{aid}/state/bindings/slack/{address}/memory/{fid}"


def _typed(aid: str, path: str) -> str:
    return f"/agents/{aid}/state/_memory/{path}"


def _put(
    client: Any,
    url: str,
    value: Any,
    headers: dict[str, str],
    expected_version: int | None = None,
) -> Any:
    body: dict[str, Any] = {"value": value}
    if expected_version is not None:
        body["expected_version"] = expected_version
    return client.put(url, json=body, headers=headers)


def _create(client: Any, url: str, headers: dict[str, str], **value: Any) -> dict[str, Any]:
    resp = _put(client, url, _value(**value), headers)
    assert resp.status_code == 200, resp.text
    body: dict[str, Any] = resp.json()
    assert body["version"] == 1, body
    return body


def _head(client: Any, auth_headers: dict[str, str], url: str) -> dict[str, Any] | None:
    resp = client.get(url, headers=auth_headers)
    if resp.status_code == 404:
        return None
    assert resp.status_code == 200, resp.text
    body: dict[str, Any] = resp.json()
    return body


def _revisions(fid: str) -> list[dict[str, Any]]:
    rows = sql_dicts(
        "SELECT version, operation, actor, source, conversation, reverted_to_version, "
        "snapshot::text AS snapshot FROM curie.memory_revisions "
        "WHERE fact_id = :fid ORDER BY version",
        {"fid": fid},
    )
    for row in rows:
        row["snapshot"] = None if row["snapshot"] is None else json.loads(row["snapshot"])
    return rows


def _agent_revision_count(aid: str) -> int:
    rows = sql_dicts(
        "SELECT count(*) AS n FROM curie.memory_revisions WHERE agent_id = :aid",
        {"aid": uuid.UUID(aid)},
    )
    return int(rows[0]["n"])


def _canonical_keys(
    client: Any, auth_headers: dict[str, str], aid: str, address: str | None = None
) -> list[str]:
    url = (
        f"/agents/{aid}/state/memory"
        if address is None
        else f"/agents/{aid}/state/bindings/slack/{address}/memory"
    )
    resp = client.get(url, headers=auth_headers)
    if resp.status_code == 404:
        return []
    assert resp.status_code == 200, resp.text
    return [row["key"] for row in resp.json() if FACT_KEY.fullmatch(row["key"])]


def _detail(resp: Any) -> str:
    try:
        return str(resp.json().get("detail", ""))
    except ValueError:
        return resp.text


def _close_turn(client: Any, aid: str, turn: str) -> None:
    resp = client.post(
        CLOSE_URL,
        json={"agent_id": aid, "turn": turn},
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
    )
    assert resp.status_code == 204, resp.text


def _lifecycle(
    client: Any,
    aid: str,
    op: str,
    headers: dict[str, str],
    fid: str,
    expected_version: int,
    scope: str = "agent",
    **extra: Any,
) -> Any:
    body: dict[str, Any] = {"scope": scope, "id": fid, "expected_version": expected_version}
    body.update(extra)
    return client.post(_typed(aid, op), json=body, headers=headers)


def _entry_detail(
    client: Any, aid: str, fid: str, headers: dict[str, str], scope: str = "agent", **params: Any
) -> Any:
    return client.get(
        _typed(aid, f"entries/{fid}"), params={"scope": scope, **params}, headers=headers
    )


def _context(client: Any, aid: str, headers: dict[str, str], **params: Any) -> Any:
    return client.get(_typed(aid, "context"), params=params, headers=headers)


def _search(client: Any, aid: str, headers: dict[str, str], **params: Any) -> Any:
    return client.get(_typed(aid, "search"), params=params, headers=headers)


def _parse_time(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# --- the operator toggle --------------------------------------------------------


def test_learning_toggle_defaults_off_and_is_independent_of_memory_writes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = client.post(
        "/agents",
        json={"name": "learning-toggle", "channel": {"kind": "slack", "address": CHANNEL_A}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    aid = created.json()["id"]
    assert created.json()["agent_wide_learning"] is False
    assert _read_agent(client, auth_headers, aid)["agent_wide_learning"] is False
    listed = client.get("/agents", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert next(a for a in listed.json() if a["id"] == aid)["agent_wide_learning"] is False

    # Turning learning on does not turn memory writes on.
    on = _patch(client, auth_headers, aid, {"agent_wide_learning": True})
    assert on.status_code == 200, on.text
    assert on.json()["agent_wide_learning"] is True
    assert on.json()["memory_writes"] is False
    stored = _read_agent(client, auth_headers, aid)
    assert (stored["agent_wide_learning"], stored["memory_writes"]) == (True, False)

    # Turning writes on does not touch learning.
    writes = _patch(client, auth_headers, aid, {"memory_writes": True})
    assert writes.status_code == 200, writes.text
    assert (writes.json()["agent_wide_learning"], writes.json()["memory_writes"]) == (True, True)

    # Omitted and explicit null leave it unchanged.
    for body in ({}, {"thinking": "adaptive"}, {"agent_wide_learning": None}):
        resp = _patch(client, auth_headers, aid, body)
        assert resp.status_code == 200, (body, resp.text)
        assert _read_agent(client, auth_headers, aid)["agent_wide_learning"] is True, body

    # Explicit false turns it off and leaves writes on.
    off = _patch(client, auth_headers, aid, {"agent_wide_learning": False})
    assert off.status_code == 200, off.text
    stored = _read_agent(client, auth_headers, aid)
    assert (stored["agent_wide_learning"], stored["memory_writes"]) == (False, True)


@pytest.mark.parametrize("value", ["sometimes", 2, [], {"on": True}])
def test_learning_toggle_refuses_a_non_boolean_and_keeps_the_stored_value(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: Any
) -> None:
    aid = _agent(client, auth_headers, learning=True)
    resp = _patch(client, auth_headers, aid, {"agent_wide_learning": value})
    assert resp.status_code == 422, resp.text
    assert _read_agent(client, auth_headers, aid)["agent_wide_learning"] is True


# --- raw PUT and DELETE on canonical keys ----------------------------------------


@pytest.mark.parametrize("scope", ["agent", "channel"])
def test_sandbox_create_stamps_server_provenance_and_records_a_create_revision(
    client: Any, auth_headers: dict[str, str], clean_db: None, scope: str
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid) if scope == "agent" else _chan_url(aid, fid)
    skew = timedelta(seconds=5)
    before = datetime.now(UTC)
    resp = _put(
        client,
        url,
        _value(title="Deploy window", statement="Tuesday", archived=True),
        _writer(aid),
    )
    after = datetime.now(UTC)
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 1

    head = _head(client, auth_headers, url)
    assert head is not None
    value = head["value"]
    assert value["kind"] == "fact"
    assert value["title"] == "Deploy window"
    assert value["statement"] == "Tuesday"
    # Forged provenance is replaced by the signed claims and the server clock.
    assert value["author"] == SENDER
    assert value["conversation"] == CONVERSATION_A
    assert before - skew <= _parse_time(value["stated_at"]) <= after + skew
    # A create is never archived, whatever the body says.
    assert value["archived"] is False
    # session_id stays a diagnostic field, as sent.
    assert value["session_id"] == "sess-body"

    [revision] = _revisions(fid)
    assert revision["version"] == 1
    assert revision["operation"] == "create"
    assert revision["source"] == "sandbox"
    assert revision["actor"] == SENDER
    assert revision["conversation"] == CONVERSATION_A
    assert revision["reverted_to_version"] is None
    assert revision["snapshot"] == value


def test_update_requires_the_head_version_keeps_kind_and_archived(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), title="Deploy window", statement="Tuesday")

    # Missing expected_version: 428. Stale: 409. Neither changes anything.
    missing = _put(client, url, _value(statement="Wednesday"), _writer(aid))
    assert missing.status_code == 428, missing.text
    stale = _put(client, url, _value(statement="Wednesday"), _writer(aid), expected_version=7)
    assert stale.status_code == 409, stale.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["statement"]) == (1, "Tuesday")
    assert len(_revisions(fid)) == 1

    # Liveness: the current version updates, stamped from the second turn.
    second = _writer(aid, sender=SECOND_SENDER, conversation=CONVERSATION_A2)
    updated = _put(
        client,
        url,
        _value(title="Deploy day", statement="Wednesday"),
        second,
        expected_version=1,
    )
    assert updated.status_code == 200, updated.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert head["version"] == 2
    assert head["value"]["title"] == "Deploy day"
    assert head["value"]["statement"] == "Wednesday"
    assert head["value"]["author"] == SECOND_SENDER
    assert head["value"]["conversation"] == CONVERSATION_A2
    revisions = _revisions(fid)
    assert [(r["version"], r["operation"]) for r in revisions] == [(1, "create"), (2, "update")]
    assert revisions[1]["actor"] == SECOND_SENDER
    assert revisions[1]["snapshot"] == head["value"]

    # Kind is immutable: changing it is refused and changes nothing.
    flipped = _put(client, url, _value(kind="instruction"), _writer(aid), expected_version=2)
    assert flipped.status_code == 422, flipped.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["kind"]) == (2, "fact")
    assert len(_revisions(fid)) == 2

    # Archived is the server's: an update cannot unarchive through the body.
    archived = _lifecycle(client, aid, "archive", _writer(aid), fid, 2)
    assert archived.status_code == 200, archived.text
    kept = _put(
        client, url, _value(statement="Thursday", archived=False), _writer(aid), expected_version=3
    )
    assert kept.status_code == 200, kept.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["archived"]) == (4, True)


INVALID_CANONICAL = {
    "title_81_chars": _value(title="é" * 81),
    "statement_501_chars": _value(statement="s" * 501),
    "unknown_kind": _value(kind="note"),
    "missing_kind": {k: v for k, v in _value().items() if k != "kind"},
    "missing_title": {k: v for k, v in _value().items() if k != "title"},
    "blank_title": _value(title="   "),
    "multiline_title": _value(title="first line\nsecond line"),
    "blank_statement": _value(statement=""),
    "non_string_statement": _value(statement=42),
    "not_an_object": "a bare string",
}


@pytest.mark.parametrize("case", sorted(INVALID_CANONICAL))
@pytest.mark.parametrize("caller", ["sandbox", "platform"])
def test_invalid_canonical_create_is_422_with_no_head_and_no_revision(
    client: Any, auth_headers: dict[str, str], clean_db: None, case: str, caller: str
) -> None:
    aid = _agent(client, auth_headers)
    headers = _writer(aid) if caller == "sandbox" else auth_headers
    fid = _fid()
    url = _agent_url(aid, fid)
    resp = _put(client, url, INVALID_CANONICAL[case], headers)
    assert resp.status_code == 422, resp.text
    assert _head(client, auth_headers, url) is None
    assert _revisions(fid) == []


@pytest.mark.parametrize("caller", ["sandbox", "platform"])
def test_boundary_sized_canonical_create_is_accepted(
    client: Any, auth_headers: dict[str, str], clean_db: None, caller: str
) -> None:
    # Liveness twin of the 422 cases: exactly 80 characters of title (counted
    # as Unicode characters, not bytes) and 500 of statement are allowed.
    aid = _agent(client, auth_headers)
    headers = _writer(aid) if caller == "sandbox" else auth_headers
    fid = _fid()
    url = _agent_url(aid, fid)
    resp = _put(client, url, _value(title="é" * 80, statement="s" * 500), headers)
    assert resp.status_code == 200, resp.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert head["value"]["title"] == "é" * 80
    assert len(_revisions(fid)) == 1


def test_create_on_an_existing_identity_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="original")
    for headers in (_writer(aid), auth_headers):
        # expected_version 0 asks for a create, and the identity has a head.
        created = _put(client, url, _value(statement="replacement"), headers, 0)
        assert created.status_code == 409, created.text
        # No expected_version on an existing head is an unversioned update.
        blind = _put(client, url, _value(statement="replacement"), headers)
        assert blind.status_code == 428, blind.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["statement"]) == (1, "original")
    assert len(_revisions(fid)) == 1


def test_create_on_a_deleted_identity_is_409(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Resurrection is the explicit revert path; a raw create may not reuse an
    # identity with deletion history (ABA safety for stale versions).
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="original")
    deleted = client.delete(url, params={"expected_version": 1}, headers=_writer(aid))
    assert deleted.status_code == 204, deleted.text
    for headers in (_writer(aid), auth_headers):
        for expected in (None, 0):
            resp = _put(client, url, _value(statement="reborn"), headers, expected)
            assert resp.status_code == 409, (expected, resp.text)
    assert _head(client, auth_headers, url) is None
    assert [(r["version"], r["operation"]) for r in _revisions(fid)] == [
        (1, "create"),
        (2, "delete"),
    ]
    # Liveness: a fresh identity still creates.
    _create(client, _agent_url(aid, _fid()), _writer(aid))


@pytest.mark.parametrize("scope", ["agent", "channel"])
@pytest.mark.parametrize("caller", ["sandbox", "platform"])
def test_stale_forget_is_refused_and_leaves_head_and_history(
    client: Any, auth_headers: dict[str, str], clean_db: None, scope: str, caller: str
) -> None:
    # Done-when item 5: read version N, update to N+1, forget with N -> 409,
    # same newer head, same history length.
    aid = _agent(client, auth_headers)
    headers = _writer(aid) if caller == "sandbox" else auth_headers
    fid = _fid()
    url = _agent_url(aid, fid) if scope == "agent" else _chan_url(aid, fid)
    _create(client, url, headers, statement="v1")
    updated = _put(client, url, _value(statement="v2"), headers, expected_version=1)
    assert updated.status_code == 200, updated.text

    missing = client.delete(url, headers=headers)
    assert missing.status_code == 428, missing.text
    stale = client.delete(url, params={"expected_version": 1}, headers=headers)
    assert stale.status_code == 409, stale.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["statement"]) == (2, "v2")
    assert len(_revisions(fid)) == 2

    # Liveness: the current version forgets, and history records the delete.
    current = client.delete(url, params={"expected_version": 2}, headers=headers)
    assert current.status_code == 204, current.text
    assert _head(client, auth_headers, url) is None
    revisions = _revisions(fid)
    assert [(r["version"], r["operation"]) for r in revisions] == [
        (1, "create"),
        (2, "update"),
        (3, "delete"),
    ]
    assert revisions[-1]["snapshot"] is None


def test_append_on_a_canonical_key_is_refused_for_every_principal(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    for base in (
        f"/agents/{aid}/state/memory",
        f"/agents/{aid}/state/bindings/slack/{CHANNEL_A}/memory",
    ):
        for headers in (auth_headers, _writer(aid)):
            resp = client.post(f"{base}/{fid}/append", json={"item": {"n": 1}}, headers=headers)
            assert resp.status_code == 422, f"{base}: {resp.status_code} {resp.text}"
        assert _head(client, auth_headers, f"{base}/{fid}") is None
    assert _revisions(fid) == []

    # Liveness: the platform key's legacy log append still works.
    log = client.post(
        f"/agents/{aid}/state/memory/log/append", json={"item": {"n": 1}}, headers=auth_headers
    )
    assert log.status_code == 200, log.text
    again = client.post(
        f"/agents/{aid}/state/memory/log/append", json={"item": {"n": 2}}, headers=auth_headers
    )
    assert again.status_code == 200, again.text
    assert again.json()["value"] == [{"n": 1}, {"n": 2}]


# --- sandbox write conditions -----------------------------------------------------


def test_instruction_writes_need_agent_wide_learning(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, learning=False)
    fid = _fid()
    url = _agent_url(aid, fid)
    refused = _put(client, url, _value(kind="instruction", title="Sign replies"), _writer(aid))
    assert refused.status_code == 403, refused.text
    assert _head(client, auth_headers, url) is None
    assert _revisions(fid) == []
    # Facts still write with learning off.
    _create(client, _agent_url(aid, _fid()), _writer(aid))

    # Liveness: with learning on the same instruction writes.
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": True}).status_code == 200
    created = _put(client, url, _value(kind="instruction", title="Sign replies"), _writer(aid))
    assert created.status_code == 200, created.text

    # Turning learning off again refuses an instruction update, using current
    # server state, not the toggle at mint time.
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": False}).status_code == 200
    update = _put(
        client,
        url,
        _value(kind="instruction", title="Sign replies", statement="changed"),
        _writer(aid),
        expected_version=1,
    )
    assert update.status_code == 403, update.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert head["version"] == 1
    assert len(_revisions(fid)) == 1


def test_retained_write_token_is_refused_after_memory_writes_turned_off(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, writes=True)
    writer = _writer(aid)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, writer, statement="kept")

    assert _patch(client, auth_headers, aid, {"memory_writes": False}).status_code == 200
    other = _fid()
    planted = _put(client, _agent_url(aid, other), _value(), writer)
    assert planted.status_code == 403, planted.text
    overwrite = _put(client, url, _value(statement="changed"), writer, expected_version=1)
    assert overwrite.status_code == 403, overwrite.text
    removed = client.delete(url, params={"expected_version": 1}, headers=writer)
    assert removed.status_code == 403, removed.text
    archived = _lifecycle(client, aid, "archive", writer, fid, 1)
    assert archived.status_code == 403, archived.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["statement"]) == (1, "kept")
    assert _head(client, auth_headers, _agent_url(aid, other)) is None
    assert len(_revisions(fid)) == 1
    # Reads stay available with writes off.
    assert _context(client, aid, _reader(aid)).status_code == 200

    # Liveness: the same token writes again once the operator turns writes on.
    assert _patch(client, auth_headers, aid, {"memory_writes": True}).status_code == 200
    again = _put(client, url, _value(statement="changed"), writer, expected_version=1)
    assert again.status_code == 200, again.text


def test_write_token_without_signed_conversation_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    unsigned = _writer(aid, conversation=None)
    fid = _fid()
    url = _agent_url(aid, fid)
    # The body's conversation is never a fallback for the missing claim.
    refused = _put(client, url, _value(conversation=CONVERSATION_A), unsigned)
    assert refused.status_code == 403, refused.text
    assert _head(client, auth_headers, url) is None
    assert _revisions(fid) == []
    no_sender = _put(client, url, _value(), _writer(aid, sender=None))
    assert no_sender.status_code == 403, no_sender.text
    assert _head(client, auth_headers, url) is None

    # Liveness twin: the same write with the signed claim succeeds.
    _create(client, url, _writer(aid))
    archive = _lifecycle(client, aid, "archive", unsigned, fid, 1)
    assert archive.status_code == 403, archive.text
    assert len(_revisions(fid)) == 1
    signed = _lifecycle(client, aid, "archive", _writer(aid), fid, 1)
    assert signed.status_code == 200, signed.text


def test_boot_read_token_is_refused_on_mutating_posts(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    _create(client, _agent_url(aid, fid), _writer(aid))
    reader = _reader(aid)
    for op, extra in (("archive", {}), ("restore", {}), ("revert", {"to_version": 0})):
        resp = _lifecycle(client, aid, op, reader, fid, 1, **extra)
        assert resp.status_code == 403, (op, resp.text)
    head = _head(client, auth_headers, _agent_url(aid, fid))
    assert head is not None and head["version"] == 1
    assert len(_revisions(fid)) == 1
    # Liveness: the read token reads, and the write token archives.
    assert _context(client, aid, reader).status_code == 200
    assert _entry_detail(client, aid, fid, reader).status_code == 200
    assert _lifecycle(client, aid, "archive", _writer(aid), fid, 1).status_code == 200


def test_closed_turn_and_other_agent_tokens_are_refused_on_typed_routes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    other = _agent(client, auth_headers, channel_a="C0EXAMPLE3", channel_b="C0EXAMPLE4")
    fid = _fid()
    _create(client, _agent_url(aid, fid), _writer(aid))

    turn = f"evt-{uuid.uuid4().hex}#1"
    closed = _writer(aid, turn=turn)
    _close_turn(client, aid, turn)
    resp = _lifecycle(client, aid, "archive", closed, fid, 1)
    assert resp.status_code == 403, resp.text
    assert "turn has ended" in _detail(resp)

    foreign = _writer(other)
    assert _context(client, aid, foreign).status_code == 401
    assert _lifecycle(client, aid, "archive", foreign, fid, 1).status_code == 401
    assert _entry_detail(client, aid, fid, foreign).status_code == 401
    assert len(_revisions(fid)) == 1

    # Liveness: an open turn of this agent archives.
    assert _lifecycle(client, aid, "archive", _writer(aid), fid, 1).status_code == 200


TYPED_ROUTES = ("context", "entries", "search", "archive", "restore", "revert")
MUTATING_ROUTES = frozenset({"archive", "restore", "revert"})
MATRIX_STATEMENT = "matrix statement only channel A may see"
# Why a caller may not reach channel A's entry. "refused" outcomes must be one
# of the listed statuses; "hidden" outcomes are a 200 that carries nothing of
# channel A (the read routes derive the channel from the claims, so a caller
# with no claim on channel A sees an empty or agent-only view).
REFUSAL_CASES = (
    "closed_turn",
    "released_credential",
    "state_app",
    "legacy",
    "other_agent",
    "other_binding",
    "unbound",
)


def _matrix_request(client: Any, aid: str, route: str, headers: dict[str, str], fid: str) -> Any:
    if route == "context":
        return _context(client, aid, headers)
    if route == "entries":
        return _entry_detail(client, aid, fid, headers, scope="channel")
    if route == "search":
        return _search(client, aid, headers, scope="channel", q="matrix statement")
    extra: dict[str, Any] = {"to_version": 0} if route == "revert" else {}
    return _lifecycle(client, aid, route, headers, fid, 1, scope="channel", **extra)


def _matrix_caller(
    client: Any, auth_headers: dict[str, str], aid: str, case: str
) -> tuple[dict[str, str], dict[str, tuple[int, ...] | str]]:
    """The refused caller for ``case`` and its expected outcome per route."""

    api_key = get_settings().api_key
    refused_all: dict[str, tuple[int, ...] | str] = {r: (403,) for r in TYPED_ROUTES}
    if case == "closed_turn":
        turn = f"evt-{uuid.uuid4().hex}#1"
        headers = _writer(aid, turn=turn)
        _close_turn(client, aid, turn)
        return headers, {r: (403,) for r in MUTATING_ROUTES}
    if case == "released_credential":
        cred = uuid.uuid4().hex
        token = mint(
            api_key,
            agent=aid,
            scope="state",
            exp=_FAR_FUTURE,
            claims={
                "binding": BINDING_A,
                "memory": "write",
                "sender": SENDER,
                "turn": f"evt-{uuid.uuid4().hex}#1",
                "conversation": CONVERSATION_A,
                "cred": cred,
            },
        )
        released = client.post(
            "/v1/internal/state/released-credentials",
            json={"agent_id": aid, "credential": cred},
            headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
        )
        assert released.status_code == 204, released.text
        return {"X-API-Key": token}, refused_all
    if case == "state_app":
        token = mint(api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
        return {"X-API-Key": token}, refused_all
    if case == "legacy":
        token = mint(api_key, agent=aid, scope="state", exp=_FAR_FUTURE)
        outcome: dict[str, tuple[int, ...] | str] = dict(refused_all)
        outcome["context"] = "hidden"
        return {"X-API-Key": token}, outcome
    if case == "other_agent":
        other = _agent(client, auth_headers, channel_a="C0EXAMPLE5", channel_b="C0EXAMPLE6")
        return _writer(other), {r: (401,) for r in TYPED_ROUTES}
    if case == "other_binding":
        headers = _writer(aid, binding=f"slack:{CHANNEL_B}")
        outcome = {r: (403, 404) for r in TYPED_ROUTES}
        outcome["context"] = "hidden"
        outcome["search"] = "hidden"
        return headers, outcome
    if case == "unbound":
        headers = _writer(aid, binding=None)
        outcome = {r: (403, 404) for r in TYPED_ROUTES}
        outcome["context"] = "hidden"
        return headers, outcome
    raise AssertionError(case)


@pytest.mark.parametrize(
    ("route", "case"),
    [
        (route, case)
        for case in REFUSAL_CASES
        for route in TYPED_ROUTES
        # A closed turn only refuses writes; its reads are covered elsewhere.
        if not (case == "closed_turn" and route not in MUTATING_ROUTES)
    ],
)
def test_typed_route_authorization_matrix(
    client: Any, auth_headers: dict[str, str], clean_db: None, route: str, case: str
) -> None:
    aid = _agent(client, auth_headers, learning=True)
    fid = _fid()
    url = _chan_url(aid, fid)
    _create(client, url, _writer(aid), title="matrix entry", statement=MATRIX_STATEMENT)

    headers, outcomes = _matrix_caller(client, auth_headers, aid, case)
    expected = outcomes[route]

    resp = _matrix_request(client, aid, route, headers, fid)
    if expected == "hidden":
        assert resp.status_code == 200, resp.text
        assert MATRIX_STATEMENT not in resp.text
        assert fid not in resp.text
    else:
        assert resp.status_code in expected, (case, route, resp.status_code, resp.text)
        assert MATRIX_STATEMENT not in resp.text

    # Nothing changed underneath.
    head = _head(client, auth_headers, url)
    assert head is not None
    assert head["version"] == 1
    assert head["value"]["archived"] is False
    assert head["value"]["statement"] == MATRIX_STATEMENT
    assert [r["operation"] for r in _revisions(fid)] == ["create"]


@pytest.mark.parametrize("route", TYPED_ROUTES)
def test_typed_route_valid_caller_twin(
    client: Any, auth_headers: dict[str, str], clean_db: None, route: str
) -> None:
    # The liveness twin of the matrix: the open-turn write credential bound to
    # channel A reaches every typed route through the same request builder.
    aid = _agent(client, auth_headers, learning=True)
    fid = _fid()
    url = _chan_url(aid, fid)
    _create(client, url, _writer(aid), title="matrix entry", statement=MATRIX_STATEMENT)

    resp = _matrix_request(client, aid, route, _writer(aid), fid)
    assert resp.status_code == 200, resp.text
    if route == "context":
        assert fid in {item["id"] for item in resp.json()["index"]}
        assert MATRIX_STATEMENT not in resp.text
    elif route == "entries":
        assert resp.json()["entry"]["statement"] == MATRIX_STATEMENT
    elif route == "search":
        assert [rec["id"] for rec in resp.json()["entries"]] == [fid]
    elif route == "archive":
        assert (resp.json()["archived"], resp.json()["version"]) == (True, 2)
    elif route == "restore":
        # Already active at the current version: an unchanged success.
        assert (resp.json()["archived"], resp.json()["version"]) == (False, 1)
    else:
        assert resp.json()["deleted"] is True
        assert _head(client, auth_headers, url) is None


def test_sandbox_may_not_name_a_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    _create(client, _chan_url(aid, fid), _writer(aid))
    params = {"channel_kind": "slack", "channel_address": CHANNEL_A}
    for headers in (_reader(aid), _writer(aid)):
        assert _context(client, aid, headers, **params).status_code == 403
        assert (
            _entry_detail(client, aid, fid, headers, scope="channel", **params).status_code == 403
        )
        assert (
            _search(client, aid, headers, scope="channel", q="Deploy", **params).status_code == 403
        )
    archive = _lifecycle(client, aid, "archive", _writer(aid), fid, 1, scope="channel", **params)
    assert archive.status_code == 403, archive.text
    assert len(_revisions(fid)) == 1
    # Liveness: without naming it, "channel" is the signed binding.
    assert _context(client, aid, _writer(aid)).status_code == 200
    ok = _lifecycle(client, aid, "archive", _writer(aid), fid, 1, scope="channel")
    assert ok.status_code == 200, ok.text
    assert ok.json()["scope"] == "channel"


# --- toggles are read after the scope lock -------------------------------------------


def _asyncpg_dsn() -> str:
    url = make_url(get_settings().database_url).set(drivername="postgresql")
    return url.render_as_string(hide_password=False)


def _hold_scope_lock(
    key: int,
    acquired: threading.Event,
    release: threading.Event,
    errors: list[Exception],
) -> None:
    """Hold one scope's memory advisory lock in its own transaction until told
    to release it, the way a concurrent writer of that scope would."""

    async def hold() -> None:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock($1, $2)",
                    memory_records._MEMORY_LOCK_CLASS,
                    key,
                )
                acquired.set()
                await asyncio.to_thread(release.wait)
        finally:
            await connection.close()

    try:
        asyncio.run(hold())
    except Exception as error:  # noqa: BLE001 - surfaced by the caller
        errors.append(error)
        acquired.set()


def _scope_lock_waiters(key: int) -> int:
    """Count backends queued on one scope's memory advisory lock. A two-int4
    advisory key shows as classid, objid (as an unsigned oid) and objsubid 2."""

    async def count() -> int:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            value = await connection.fetchval(
                """
                SELECT count(*)
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND NOT granted
                  AND objsubid = 2
                  AND classid::bigint = $1
                  AND objid::bigint = $2
                """,
                memory_records._MEMORY_LOCK_CLASS,
                key & 0xFFFFFFFF,
            )
            return int(value)
        finally:
            await connection.close()

    return asyncio.run(count())


def _wait_until_queued(key: int, request: Future[Any]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        assert not request.done(), f"write did not wait on the scope lock: {request.result()}"
        if _scope_lock_waiters(key) >= 1:
            return
        time.sleep(0.05)
    raise AssertionError("the write never queued on the scope's memory lock")


def _write_queued_behind_a_toggle_change(
    aid: str,
    write: Callable[[], Any],
    meanwhile: Callable[[], Any] | None,
) -> Any:
    """Queue ``write`` behind another holder of the agent scope's memory lock,
    run ``meanwhile`` (the operator PATCH) to completion while the write waits,
    then release the lock and return the write's response."""

    key = memory_records._lock_key(uuid.UUID(aid), None)
    acquired = threading.Event()
    release = threading.Event()
    lock_errors: list[Exception] = []
    holder = threading.Thread(
        target=_hold_scope_lock, args=(key, acquired, release, lock_errors), daemon=True
    )
    holder.start()
    try:
        assert acquired.wait(timeout=10), "the scope lock was not acquired"
        assert not lock_errors, lock_errors
        with ThreadPoolExecutor(max_workers=2) as pool:
            queued = pool.submit(write)
            try:
                _wait_until_queued(key, queued)
                if meanwhile is not None:
                    # The PATCH takes no memory lock, so it completes while the
                    # write is still queued.
                    patched = pool.submit(meanwhile).result(timeout=10)
                    assert patched.status_code == 200, patched.text
                    assert not queued.done(), "the write finished before the lock was released"
                    assert _scope_lock_waiters(key) >= 1
            finally:
                release.set()
            response = queued.result(timeout=10)
    finally:
        release.set()
        holder.join(timeout=10)
    assert not holder.is_alive(), "the scope lock was not released"
    assert not lock_errors, lock_errors
    return response


@pytest.mark.parametrize("turned_off", [True, False])
def test_queued_raw_create_rereads_memory_writes_after_the_scope_lock(
    client: Any, auth_headers: dict[str, str], clean_db: None, turned_off: bool
) -> None:
    """CR2-1 (a): a raw canonical PUT that waited on the scope lock while the
    operator turned memory_writes off is refused with the current toggle, not
    the one loaded before the wait. The twin with no PATCH proves the queued
    write itself succeeds."""

    aid = _agent(client, auth_headers, writes=True)
    writer = _writer(aid)
    fid = _fid()
    url = _agent_url(aid, fid)
    before = _agent_revision_count(aid)

    response = _write_queued_behind_a_toggle_change(
        aid,
        lambda: _put(client, url, _value(statement="queued"), writer),
        (lambda: _patch(client, auth_headers, aid, {"memory_writes": False}))
        if turned_off
        else None,
    )

    if turned_off:
        assert response.status_code == 403, response.text
        assert _head(client, auth_headers, url) is None
        assert _revisions(fid) == []
        assert _agent_revision_count(aid) == before
        assert _read_agent(client, auth_headers, aid)["memory_writes"] is False
    else:
        assert response.status_code == 200, response.text
        assert [r["operation"] for r in _revisions(fid)] == ["create"]


@pytest.mark.parametrize("turned_off", [True, False])
def test_queued_typed_archive_rereads_memory_writes_after_the_scope_lock(
    client: Any, auth_headers: dict[str, str], clean_db: None, turned_off: bool
) -> None:
    """CR2-1 (b): a typed lifecycle write (archive) that waited on the scope
    lock while memory_writes was turned off is refused, and the head and its
    history are unchanged. The twin with no PATCH archives."""

    aid = _agent(client, auth_headers, writes=True)
    writer = _writer(aid)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, writer, statement="kept")

    response = _write_queued_behind_a_toggle_change(
        aid,
        lambda: _lifecycle(client, aid, "archive", writer, fid, 1),
        (lambda: _patch(client, auth_headers, aid, {"memory_writes": False}))
        if turned_off
        else None,
    )

    head = _head(client, auth_headers, url)
    assert head is not None
    if turned_off:
        assert response.status_code == 403, response.text
        assert (head["version"], head["value"]["archived"]) == (1, False)
        assert head["value"]["statement"] == "kept"
        assert [r["operation"] for r in _revisions(fid)] == ["create"]
    else:
        assert response.status_code == 200, response.text
        assert (head["version"], head["value"]["archived"]) == (2, True)
        assert [r["operation"] for r in _revisions(fid)] == ["create", "archive"]


@pytest.mark.parametrize("turned_off", [True, False])
def test_queued_instruction_create_rereads_agent_wide_learning_after_the_scope_lock(
    client: Any, auth_headers: dict[str, str], clean_db: None, turned_off: bool
) -> None:
    """CR2-1 (c): a standing instruction create that waited on the scope lock
    while the operator turned agent_wide_learning off (memory_writes stays on)
    is refused. The twin with no PATCH creates the instruction."""

    aid = _agent(client, auth_headers, writes=True, learning=True)
    writer = _writer(aid)
    fid = _fid()
    url = _agent_url(aid, fid)
    before = _agent_revision_count(aid)

    response = _write_queued_behind_a_toggle_change(
        aid,
        lambda: _put(client, url, _value(kind="instruction", title="Sign replies"), writer),
        (lambda: _patch(client, auth_headers, aid, {"agent_wide_learning": False}))
        if turned_off
        else None,
    )

    if turned_off:
        assert response.status_code == 403, response.text
        assert _head(client, auth_headers, url) is None
        assert _revisions(fid) == []
        assert _agent_revision_count(aid) == before
        stored = _read_agent(client, auth_headers, aid)
        assert (stored["agent_wide_learning"], stored["memory_writes"]) == (False, True)
    else:
        assert response.status_code == 200, response.text
        assert [r["operation"] for r in _revisions(fid)] == ["create"]


# --- the count cap ------------------------------------------------------------------


def test_concurrent_creates_at_the_cap_admit_exactly_the_cap(
    client: Any, auth_headers: dict[str, str], clean_db: None, entry_cap: Callable[[int], None]
) -> None:
    aid = _agent(client, auth_headers)
    entry_cap(3)
    writer = _writer(aid)
    ids = [_fid() for _ in range(10)]

    def create(fid: str) -> Any:
        return _put(client, _agent_url(aid, fid), _value(title=fid[-8:]), writer)

    with ThreadPoolExecutor(max_workers=len(ids)) as pool:
        results = list(pool.map(create, ids))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200] * 3 + [413] * 7, [(r.status_code, r.text) for r in results]
    for refused in (r for r in results if r.status_code == 413):
        assert "entries" in _detail(refused), refused.text
    assert len(_canonical_keys(client, auth_headers, aid)) == 3
    assert _agent_revision_count(aid) == 3

    # Another scope keeps its own room.
    _create(client, _chan_url(aid, _fid()), writer)


def test_archive_keeps_capacity_and_delete_frees_it(
    client: Any, auth_headers: dict[str, str], clean_db: None, entry_cap: Callable[[int], None]
) -> None:
    aid = _agent(client, auth_headers)
    entry_cap(2)
    first, second, third = _fid(), _fid(), _fid()
    _create(client, _agent_url(aid, first), _writer(aid))
    _create(client, _agent_url(aid, second), _writer(aid))
    assert _lifecycle(client, aid, "archive", _writer(aid), first, 1).status_code == 200

    full = _put(client, _agent_url(aid, third), _value(), _writer(aid))
    assert full.status_code == 413, full.text
    assert "entries" in _detail(full)
    assert _revisions(third) == []

    deleted = client.delete(
        _agent_url(aid, first), params={"expected_version": 2}, headers=_writer(aid)
    )
    assert deleted.status_code == 204, deleted.text
    _create(client, _agent_url(aid, third), _writer(aid))
    assert sorted(_canonical_keys(client, auth_headers, aid)) == sorted([second, third])


def test_concurrent_create_and_revert_at_the_cap_admit_one(
    client: Any, auth_headers: dict[str, str], clean_db: None, entry_cap: Callable[[int], None]
) -> None:
    aid = _agent(client, auth_headers)
    entry_cap(2)
    gone, kept = _fid(), _fid()
    _create(client, _agent_url(aid, gone), _writer(aid))
    _create(client, _agent_url(aid, kept), _writer(aid))
    assert (
        client.delete(
            _agent_url(aid, gone), params={"expected_version": 1}, headers=_writer(aid)
        ).status_code
        == 204
    )
    # One slot left; two creates and one revert-of-deletion race for it.
    fresh = [_fid(), _fid()]

    def run(job: str) -> Any:
        if job == "revert":
            return _lifecycle(client, aid, "revert", _writer(aid), gone, 2, to_version=1)
        return _put(client, _agent_url(aid, job), _value(), _writer(aid))

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, ["revert", *fresh]))
    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 413, 413], [(r.status_code, r.text) for r in results]
    assert len(_canonical_keys(client, auth_headers, aid)) == 2
    # gone: create + delete; kept: create; plus exactly one winner.
    assert _agent_revision_count(aid) == 4


# --- the bounded boot context ----------------------------------------------------------


def test_context_is_bounded_newest_first_and_carries_no_fact_statement(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, learning=True)

    def seed(url: str, **value: Any) -> None:
        resp = _put(client, url, _value(**value), auth_headers)
        assert resp.status_code == 200, resp.text

    # Instructions: 15 agent-wide, then 10 in channel A, then 3 in channel B.
    agent_instructions = [_fid() for _ in range(15)]
    channel_instructions = [_fid() for _ in range(10)]
    for n, fid in enumerate(agent_instructions):
        seed(
            _agent_url(aid, fid), kind="instruction", title=f"agent rule {n}", statement=f"rule {n}"
        )
    for n, fid in enumerate(channel_instructions):
        seed(
            _chan_url(aid, fid), kind="instruction", title=f"chan rule {n}", statement=f"crule {n}"
        )
    for n in range(3):
        seed(
            _chan_url(aid, _fid(), CHANNEL_B),
            kind="instruction",
            title=f"other rule {n}",
            statement=f"other-channel-instruction {n}",
        )

    # Facts: 240 agent-wide, then 10 in channel A, then 5 in channel B. Every
    # statement carries a unique nonce that must never reach the response.
    nonces: list[str] = []
    statements: list[str] = []

    def fact_statement(n: int) -> str:
        nonce = f"nonce-{uuid.uuid4().hex}"
        nonces.append(nonce)
        statement = f"{nonce} fact body {n}"
        statements.append(statement)
        return statement

    agent_facts = [_fid() for _ in range(240)]
    channel_facts = [_fid() for _ in range(10)]
    for n, fid in enumerate(agent_facts):
        seed(_agent_url(aid, fid), title=f"agent fact {n}", statement=fact_statement(n))
    for n, fid in enumerate(channel_facts):
        seed(_chan_url(aid, fid), title=f"chan fact {n}", statement=fact_statement(n))
    other_channel_facts = [_fid() for _ in range(5)]
    for n, fid in enumerate(other_channel_facts):
        seed(_chan_url(aid, fid, CHANNEL_B), title=f"other fact {n}", statement=fact_statement(n))

    # The newest agent fact is archived: it leaves the index.
    archived = _lifecycle(client, aid, "archive", auth_headers, agent_facts[-1], 1)
    assert archived.status_code == 200, archived.text

    resp = _context(client, aid, _reader(aid))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    raw = resp.text

    assert body["agent_wide_learning"] is True
    assert body["memory_writes"] is True
    assert body["guidance"]["source"] == "learning_default"

    expected_index = list(reversed(channel_facts)) + list(reversed(agent_facts[199:239]))
    assert [item["id"] for item in body["index"]] == expected_index
    assert len(body["index"]) == 50
    for item in body["index"]:
        assert set(item) == {"id", "scope", "title", "version"}, item
        assert item["scope"] in ("agent", "channel")
    assert {item["scope"] for item in body["index"][:10]} == {"channel"}

    expected_instructions = list(reversed(channel_instructions)) + list(
        reversed(agent_instructions[5:])
    )
    assert [rec["id"] for rec in body["instructions"]] == expected_instructions
    assert len(body["instructions"]) == 20
    assert all(rec["kind"] == "instruction" for rec in body["instructions"])
    assert all(len(rec["statement"]) <= 500 for rec in body["instructions"])

    assert body["totals"] == {"instructions": 25, "facts": 249, "archived": 1}
    assert body["omitted"] == {"instructions": 5, "facts": 199}

    # No fact statement anywhere in the serialized response.
    for nonce in nonces:
        assert nonce not in raw
    for statement in statements:
        assert statement not in raw
    # Another channel's entries are not visible at all.
    assert "other-channel-instruction" not in raw
    for fid in other_channel_facts:
        assert fid not in raw
    assert agent_facts[-1] not in raw


def test_context_has_no_instructions_when_learning_is_off(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, learning=False)
    instruction = _fid()
    resp = _put(
        client,
        _agent_url(aid, instruction),
        _value(kind="instruction", title="Sign replies", statement="Sign every reply."),
        auth_headers,
    )
    assert resp.status_code == 200, resp.text
    fact = _fid()
    _create(client, _agent_url(aid, fact), auth_headers, title="Deploy window")

    off = _context(client, aid, _reader(aid))
    assert off.status_code == 200, off.text
    assert off.json()["agent_wide_learning"] is False
    assert off.json()["instructions"] == []
    assert off.json()["guidance"] == {"text": DEFAULT_MEMORY_GUIDANCE, "source": "default"}
    assert [item["id"] for item in off.json()["index"]] == [fact]
    assert "Sign every reply." not in off.text

    # Liveness: with learning on the stored instruction is in the context.
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": True}).status_code == 200
    on = _context(client, aid, _reader(aid))
    assert on.status_code == 200, on.text
    assert [rec["id"] for rec in on.json()["instructions"]] == [instruction]
    assert on.json()["instructions"][0]["statement"] == "Sign every reply."
    assert on.json()["guidance"]["source"] == "learning_default"


# --- detail ---------------------------------------------------------------------------


def test_detail_paginates_history_with_before_version(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="v1")
    for version in range(2, 13):
        resp = _put(
            client, url, _value(statement=f"v{version}"), _writer(aid), expected_version=version - 1
        )
        assert resp.status_code == 200, resp.text

    first = _entry_detail(client, aid, fid, _reader(aid))
    assert first.status_code == 200, first.text
    page = first.json()
    assert page["deleted"] is False
    assert page["version"] == 12
    assert page["entry"]["statement"] == "v12"
    assert page["entry"]["version"] == 12
    assert [rev["version"] for rev in page["history"]] == list(range(12, 2, -1))
    assert page["history"][0]["operation"] == "update"
    assert page["history"][0]["snapshot"]["statement"] == "v12"
    assert page["history_before"] is not None

    second = _entry_detail(client, aid, fid, _reader(aid), before_version=page["history_before"])
    assert second.status_code == 200, second.text
    older = second.json()
    assert [rev["version"] for rev in older["history"]] == [2, 1]
    assert older["history"][-1]["operation"] == "create"
    assert older["history"][-1]["snapshot"]["statement"] == "v1"
    assert older["history_before"] is None

    # A revert can target a version beyond the first page.
    reverted = _lifecycle(client, aid, "revert", _writer(aid), fid, 12, to_version=1)
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["entry"]["statement"] == "v1"
    assert reverted.json()["version"] == 13


def test_detail_of_a_deleted_identity_and_of_a_missing_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="soon gone")
    assert (
        client.delete(url, params={"expected_version": 1}, headers=_writer(aid)).status_code == 204
    )

    gone = _entry_detail(client, aid, fid, _reader(aid))
    assert gone.status_code == 200, gone.text
    body = gone.json()
    assert body["entry"] is None
    assert body["deleted"] is True
    assert body["version"] == 2
    assert [rev["operation"] for rev in body["history"]] == ["delete", "create"]
    assert body["history"][0]["snapshot"] is None

    # Never created: 404.
    assert _entry_detail(client, aid, _fid(), _reader(aid)).status_code == 404


def test_detail_does_not_reach_another_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    secret = _fid()
    _create(
        client,
        _chan_url(aid, secret, CHANNEL_B),
        auth_headers,
        title="dm secret",
        statement="a DM secret statement",
    )
    for headers in (_reader(aid), _writer(aid)):
        for scope in ("channel", "agent"):
            resp = _entry_detail(client, aid, secret, headers, scope=scope)
            assert resp.status_code in (403, 404), resp.text
            assert "a DM secret statement" not in resp.text
    # Liveness: the platform key reads it with the channel named.
    operator = _entry_detail(
        client,
        aid,
        secret,
        auth_headers,
        scope="channel",
        channel_kind="slack",
        channel_address=CHANNEL_B,
    )
    assert operator.status_code == 200, operator.text
    assert operator.json()["entry"]["statement"] == "a DM secret statement"


# --- search ---------------------------------------------------------------------------


def test_search_is_literal_paginated_and_reaches_archived_and_unindexed_entries(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)

    def seed(url: str, **value: Any) -> str:
        resp = _put(client, url, _value(**value), auth_headers)
        assert resp.status_code == 200, resp.text
        return url.rsplit("/", 1)[1]

    needles = [
        seed(_agent_url(aid, _fid()), title=f"needle report {n}", statement=f"body {n}")
        for n in range(25)
    ]
    in_statement = seed(
        _agent_url(aid, _fid()), title="unrelated", statement="the NEEDLE is in the statement"
    )
    literal = seed(_agent_url(aid, _fid()), title="discount 50%_off", statement="x")
    seed(_agent_url(aid, _fid()), title="discount 50 percent off", statement="x")
    seed(_agent_url(aid, _fid()), title="discount 50xxoff", statement="x")
    channel_needle = seed(_chan_url(aid, _fid()), title="needle in channel A", statement="x")
    other_needle = seed(
        _chan_url(aid, _fid(), CHANNEL_B), title="needle in channel B", statement="other secret"
    )
    # 55 newer fillers push every needle out of the 50-title index.
    for n in range(55):
        seed(_agent_url(aid, _fid()), title=f"filler {n}", statement=f"filler body {n}")
    archived = _lifecycle(client, aid, "archive", auth_headers, needles[0], 1)
    assert archived.status_code == 200, archived.text

    reader = _reader(aid)
    context = _context(client, aid, reader).json()
    assert needles[1] not in {item["id"] for item in context["index"]}

    # Pages of 10 over the 26 agent-wide matches (case-insensitive, title or statement).
    seen: list[str] = []
    sizes: list[int] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"scope": "agent", "q": "needle", "limit": 10}
        if cursor is not None:
            params["cursor"] = cursor
        resp = _search(client, aid, reader, **params)
        assert resp.status_code == 200, resp.text
        page = resp.json()
        sizes.append(len(page["entries"]))
        seen.extend(rec["id"] for rec in page["entries"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
        assert len(sizes) < 10, "search cursor never ended"
    assert sizes == [10, 10, 6]
    assert len(seen) == len(set(seen))
    assert set(seen) == {*needles, in_statement}
    assert channel_needle not in seen and other_needle not in seen

    # Full records, archived included.
    full = _search(client, aid, reader, scope="agent", q="needle report 0", limit=50).json()
    by_id = {rec["id"]: rec for rec in full["entries"]}
    assert by_id[needles[0]]["archived"] is True
    assert by_id[needles[0]]["statement"] == "body 0"

    # Literal match: % and _ are not wildcards.
    literal_hits = _search(client, aid, reader, scope="agent", q="50%_").json()
    assert [rec["id"] for rec in literal_hits["entries"]] == [literal]

    # The channel scope is the signed binding only.
    channel_hits = _search(client, aid, reader, scope="channel", q="needle").json()
    assert [rec["id"] for rec in channel_hits["entries"]] == [channel_needle]
    assert "other secret" not in json.dumps(channel_hits)

    # Default limit 20, maximum 50.
    default = _search(client, aid, reader, scope="agent", q="filler").json()
    assert len(default["entries"]) == 20
    assert default["next_cursor"] is not None
    over = _search(client, aid, reader, scope="agent", q="filler", limit=51)
    assert over.status_code == 422 or len(over.json()["entries"]) <= 50, over.text

    # A miss is an empty success.
    miss = _search(client, aid, reader, scope="agent", q="no-such-text-anywhere")
    assert miss.status_code == 200, miss.text
    assert miss.json() == {"entries": [], "next_cursor": None}


# --- archive and restore -----------------------------------------------------------------


def test_archive_and_restore_use_cas_and_are_no_ops_in_the_requested_state(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), title="Deploy window")

    stale = _lifecycle(client, aid, "archive", _writer(aid), fid, 0)
    assert stale.status_code == 409, stale.text
    head = _head(client, auth_headers, url)
    assert head is not None and head["value"]["archived"] is False
    assert len(_revisions(fid)) == 1

    archived = _lifecycle(client, aid, "archive", _writer(aid), fid, 1)
    assert archived.status_code == 200, archived.text
    assert archived.json()["archived"] is True
    assert archived.json()["version"] == 2
    assert fid not in {item["id"] for item in _context(client, aid, _reader(aid)).json()["index"]}

    # Already archived at the current version: unchanged, no revision.
    again = _lifecycle(client, aid, "archive", _writer(aid), fid, 2)
    assert again.status_code == 200, again.text
    assert (again.json()["version"], again.json()["archived"]) == (2, True)
    assert [r["operation"] for r in _revisions(fid)] == ["create", "archive"]

    # Stale restore conflicts even though the desired state differs.
    stale_restore = _lifecycle(client, aid, "restore", _writer(aid), fid, 1)
    assert stale_restore.status_code == 409, stale_restore.text
    restored = _lifecycle(client, aid, "restore", _writer(aid), fid, 2)
    assert restored.status_code == 200, restored.text
    assert (restored.json()["version"], restored.json()["archived"]) == (3, False)
    noop = _lifecycle(client, aid, "restore", _writer(aid), fid, 3)
    assert noop.status_code == 200, noop.text
    assert noop.json()["version"] == 3
    # A stale no-op still conflicts.
    assert _lifecycle(client, aid, "restore", _writer(aid), fid, 2).status_code == 409
    assert [r["operation"] for r in _revisions(fid)] == ["create", "archive", "restore"]
    head = _head(client, auth_headers, url)
    assert head is not None and head["value"]["archived"] is False and head["version"] == 3
    assert fid in {item["id"] for item in _context(client, aid, _reader(aid)).json()["index"]}


# --- revert ---------------------------------------------------------------------------------


def test_revert_to_an_earlier_version_to_absence_and_back(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="first")
    assert _put(client, url, _value(statement="second"), _writer(aid), 1).status_code == 200

    back = _lifecycle(client, aid, "revert", _writer(aid), fid, 2, to_version=1)
    assert back.status_code == 200, back.text
    assert back.json()["entry"]["statement"] == "first"
    assert (back.json()["version"], back.json()["deleted"]) == (3, False)

    absent = _lifecycle(client, aid, "revert", _writer(aid), fid, 3, to_version=0)
    assert absent.status_code == 200, absent.text
    assert absent.json()["entry"] is None
    assert (absent.json()["deleted"], absent.json()["version"]) == (True, 4)
    assert _head(client, auth_headers, url) is None

    # Reverting the deletion restores the payload under the same id, new version.
    revived = _lifecycle(client, aid, "revert", _writer(aid), fid, 4, to_version=3)
    assert revived.status_code == 200, revived.text
    assert revived.json()["entry"]["id"] == fid
    assert revived.json()["entry"]["statement"] == "first"
    assert revived.json()["version"] == 5
    head = _head(client, auth_headers, url)
    assert head is not None and head["version"] == 5

    # The old expected version never deletes the resurrected head.
    old = client.delete(url, params={"expected_version": 3}, headers=_writer(aid))
    assert old.status_code == 409, old.text
    revisions = _revisions(fid)
    assert [(r["version"], r["operation"], r["reverted_to_version"]) for r in revisions] == [
        (1, "create", None),
        (2, "update", None),
        (3, "revert", 1),
        (4, "revert", 0),
        (5, "revert", 3),
    ]
    assert revisions[3]["snapshot"] is None

    # A target that is not an earlier version is refused with no change.
    for target in (5, 9):
        bad = _lifecycle(client, aid, "revert", _writer(aid), fid, 5, to_version=target)
        assert bad.status_code == 422, (target, bad.text)
    assert len(_revisions(fid)) == 5


def test_stale_revert_is_409_and_changes_nothing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(client, url, _writer(aid), statement="first")
    assert _put(client, url, _value(statement="second"), _writer(aid), 1).status_code == 200
    stale = _lifecycle(client, aid, "revert", _writer(aid), fid, 1, to_version=0)
    assert stale.status_code == 409, stale.text
    head = _head(client, auth_headers, url)
    assert head is not None
    assert (head["version"], head["value"]["statement"]) == (2, "second")
    assert len(_revisions(fid)) == 2
    # Liveness: the current version reverts.
    assert _lifecycle(client, aid, "revert", _writer(aid), fid, 2, to_version=0).status_code == 200


def test_a_second_sender_reverts_the_first_senders_instruction(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers, learning=True)
    fid = _fid()
    url = _agent_url(aid, fid)
    _create(
        client,
        url,
        _writer(aid, sender=SENDER),
        kind="instruction",
        title="Sign replies",
        statement="End every reply with: Example Team.",
    )

    bob = _writer(aid, sender=SECOND_SENDER, conversation=CONVERSATION_A2)
    removed = _lifecycle(client, aid, "revert", bob, fid, 1, to_version=0)
    assert removed.status_code == 200, removed.text
    assert removed.json()["deleted"] is True
    assert _head(client, auth_headers, url) is None
    revision = _revisions(fid)[-1]
    assert (revision["operation"], revision["reverted_to_version"]) == ("revert", 0)
    assert revision["actor"] == SECOND_SENDER
    assert revision["source"] == "sandbox"
    assert revision["conversation"] == CONVERSATION_A2

    # A third sender in a fresh session finds the deletion and reverts it.
    carol = _writer(aid, sender=THIRD_SENDER, conversation=f"slack:{CHANNEL_A}:1700000000.000003")
    found = _entry_detail(client, aid, fid, carol)
    assert found.status_code == 200, found.text
    assert (found.json()["deleted"], found.json()["version"]) == (True, 2)
    restored = _lifecycle(client, aid, "revert", carol, fid, 2, to_version=1)
    assert restored.status_code == 200, restored.text
    assert restored.json()["entry"]["kind"] == "instruction"
    assert restored.json()["entry"]["statement"] == "End every reply with: Example Team."
    assert restored.json()["version"] == 3

    # Stale: 409, nothing changes.
    stale = _lifecycle(client, aid, "revert", bob, fid, 2, to_version=0)
    assert stale.status_code == 409, stale.text
    assert len(_revisions(fid)) == 3

    # Instruction targets need learning on.
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": False}).status_code == 200
    refused = _lifecycle(client, aid, "revert", bob, fid, 3, to_version=0)
    assert refused.status_code == 403, refused.text
    head = _head(client, auth_headers, url)
    assert head is not None and head["version"] == 3


def test_cross_channel_revert_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    secret = _fid()
    b_url = _chan_url(aid, secret, CHANNEL_B)
    _create(client, b_url, auth_headers, statement="channel B only")
    for scope in ("channel", "agent"):
        resp = _lifecycle(client, aid, "revert", _writer(aid), secret, 1, scope=scope, to_version=0)
        assert resp.status_code in (403, 404), resp.text
    named = _lifecycle(
        client,
        aid,
        "revert",
        _writer(aid),
        secret,
        1,
        scope="channel",
        to_version=0,
        channel_kind="slack",
        channel_address=CHANNEL_B,
    )
    assert named.status_code == 403, named.text
    head = _head(client, auth_headers, b_url)
    assert head is not None and head["version"] == 1
    assert len(_revisions(secret)) == 1
    # Liveness: channel A's own entry reverts with the same token.
    own = _fid()
    _create(client, _chan_url(aid, own), _writer(aid))
    ok = _lifecycle(client, aid, "revert", _writer(aid), own, 1, scope="channel", to_version=0)
    assert ok.status_code == 200, ok.text


# --- operator lifecycle -------------------------------------------------------------------


def test_operator_archive_restore_and_revert_name_the_channel(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    url = _chan_url(aid, fid)
    _create(client, url, _writer(aid), statement="channel fact")
    channel = {"channel_kind": "slack", "channel_address": CHANNEL_A}

    unnamed = _lifecycle(client, aid, "archive", auth_headers, fid, 1, scope="channel")
    assert unnamed.status_code in (400, 422), unnamed.text
    assert len(_revisions(fid)) == 1

    archived = _lifecycle(client, aid, "archive", auth_headers, fid, 1, scope="channel", **channel)
    assert archived.status_code == 200, archived.text
    assert (archived.json()["archived"], archived.json()["version"]) == (True, 2)
    assert archived.json()["scope"] == "channel"
    restored = _lifecycle(client, aid, "restore", auth_headers, fid, 2, scope="channel", **channel)
    assert restored.status_code == 200, restored.text
    assert (restored.json()["archived"], restored.json()["version"]) == (False, 3)
    reverted = _lifecycle(
        client, aid, "revert", auth_headers, fid, 3, scope="channel", to_version=0, **channel
    )
    assert reverted.status_code == 200, reverted.text
    assert reverted.json()["deleted"] is True
    assert _head(client, auth_headers, url) is None

    revisions = _revisions(fid)
    assert [r["operation"] for r in revisions] == ["create", "archive", "restore", "revert"]
    for revision in revisions[1:]:
        assert revision["actor"] == "operator"
        assert revision["source"] == "operator"
        assert revision["conversation"] is None
    # A stale operator revert conflicts with no change.
    stale = _lifecycle(
        client, aid, "revert", auth_headers, fid, 3, scope="channel", to_version=1, **channel
    )
    assert stale.status_code == 409, stale.text
    assert len(_revisions(fid)) == 4


def test_operator_revert_to_zero_on_a_never_created_identity_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    fid = _fid()
    resp = _lifecycle(client, aid, "revert", auth_headers, fid, 0, to_version=0)
    assert resp.status_code in (404, 422), resp.text
    assert _revisions(fid) == []


# --- guidance -----------------------------------------------------------------------------


def test_default_guidance_bytes_are_unchanged() -> None:
    encoded = DEFAULT_MEMORY_GUIDANCE.encode("utf-8")
    assert len(encoded) == ORIGINAL_DEFAULT_BYTES
    assert hashlib.sha256(encoded).hexdigest() == ORIGINAL_DEFAULT_SHA256


def test_guidance_source_follows_operator_text_then_learning_then_default(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    import curie_api.memory_guidance as guidance_module

    learning_text = getattr(guidance_module, "LEARNING_MEMORY_GUIDANCE", None)
    aid = _agent(client, auth_headers, learning=False)
    url = f"/agents/{aid}/memory/guidance"

    def effective() -> dict[str, Any]:
        resp = client.get(url, headers=auth_headers)
        assert resp.status_code == 200, resp.text
        body: dict[str, Any] = resp.json()
        context = _context(client, aid, _reader(aid))
        assert context.status_code == 200, context.text
        # The boot context reports the same selection as the operator route.
        assert context.json()["guidance"] == body
        return body

    assert effective() == {"text": DEFAULT_MEMORY_GUIDANCE, "source": "default"}

    assert _patch(client, auth_headers, aid, {"agent_wide_learning": True}).status_code == 200
    assert isinstance(learning_text, str) and learning_text.strip()
    assert learning_text != DEFAULT_MEMORY_GUIDANCE
    assert effective() == {"text": learning_text, "source": "learning_default"}

    operator = "Remember customer preferences only."
    assert client.put(url, json={"text": operator}, headers=auth_headers).status_code == 200
    assert effective() == {"text": operator, "source": "operator"}
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": False}).status_code == 200
    assert effective() == {"text": operator, "source": "operator"}

    assert client.delete(url, headers=auth_headers).status_code == 204
    assert effective() == {"text": DEFAULT_MEMORY_GUIDANCE, "source": "default"}
    assert _patch(client, auth_headers, aid, {"agent_wide_learning": True}).status_code == 200
    assert effective() == {"text": learning_text, "source": "learning_default"}


# --- the shared vectors are pinned to the real producer -----------------------------------


def _assert_same_keys(real: Any, vector: Any, path: str = "$") -> None:
    """Recursively compare key sets; ``None`` on either side is a wildcard."""

    if real is None or vector is None:
        return
    if isinstance(vector, dict):
        assert isinstance(real, dict), f"{path}: expected an object, got {real!r}"
        assert set(real) == set(vector), f"{path}: {sorted(real)} != {sorted(vector)}"
        for key in vector:
            _assert_same_keys(real[key], vector[key], f"{path}.{key}")
    elif isinstance(vector, list):
        assert isinstance(real, list), f"{path}: expected a list, got {real!r}"
        if vector:
            assert real, f"{path}: the real list is empty, nothing to compare"
            for index, item in enumerate(real):
                _assert_same_keys(item, vector[0], f"{path}[{index}]")


def test_shared_vectors_match_the_real_api_shapes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    vectors = json.loads(VECTORS.read_text("utf-8"))
    aid = _agent(client, auth_headers, learning=True)
    writer = _writer(aid)

    instruction = _fid()
    _create(
        client,
        _agent_url(aid, instruction),
        writer,
        kind="instruction",
        title="Sign replies",
        statement="End every reply with: Example Team.",
    )
    fact = _fid()
    fact_url = _agent_url(aid, fact)
    _create(client, fact_url, writer, title="Vendor contract approver", statement="Alex approves.")
    assert (
        _put(
            client,
            fact_url,
            _value(title="Vendor contract approver", statement="Sam approves."),
            writer,
            1,
        ).status_code
        == 200
    )
    archived_fact = _fid()
    _create(client, _chan_url(aid, archived_fact), writer, title="Deploy window")

    record = _lifecycle(client, aid, "archive", writer, archived_fact, 1, scope="channel")
    assert record.status_code == 200, record.text
    _assert_same_keys(record.json(), vectors["memory_record_out"])

    context = _context(client, aid, _reader(aid))
    assert context.status_code == 200, context.text
    _assert_same_keys(context.json(), vectors["memory_context_out"])

    detail = _entry_detail(client, aid, fact, _reader(aid))
    assert detail.status_code == 200, detail.text
    _assert_same_keys(detail.json(), vectors["memory_detail_out"])

    search = _search(client, aid, _reader(aid), scope="agent", q="Vendor contract")
    assert search.status_code == 200, search.text
    _assert_same_keys(search.json(), vectors["memory_search_out"])

    reverted = _lifecycle(client, aid, "revert", writer, fact, 2, to_version=1)
    assert reverted.status_code == 200, reverted.text
    _assert_same_keys(reverted.json(), vectors["memory_detail_out"])
