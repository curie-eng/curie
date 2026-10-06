"""ADR 0205 decisions 1 and 2: the thread attachment ledger in the API (#4079).

The worker records one reference per file the agent was given, keyed exactly
like the transcript (agent, binding scope, thread key), and reads the thread's
references back on every boot. The ledger lives in ``thread_attachment_refs``
(``curie_api.thread_attachments``: ``list_refs`` / ``append_refs`` /
``delete_for``) and has the transcript's lifetime: every path that removes or
expires the transcript removes the references, and a read for a thread whose
transcript has expired returns nothing.

Wire contract pinned here (the implementer follows it):

Both routes are ``POST`` under ``/v1/internal/thread-attachments``, accept ONLY
the internal worker credential in ``X-Curie-Worker-Token``
(``auth.require_internal_worker_token``), and answer 401 to anything else: the
platform ``X-API-Key``, a sandbox ``state`` token, a console session, a wrong
worker token, or nothing. The worker always writes binding scope NULL, so the
body names no scope.

``POST /v1/internal/thread-attachments/query``
    request  ``{"agent_id": "<uuid>", "thread_key": "<key>"}``
    200      ``{"refs": [<row>, ...]}`` in arrival order (``seq``), ``[]`` for a
             thread with no live references. A ``<row>`` is exactly a
             ``<ref>`` plus ``"event_id": str``, the event it was appended
             under, so a redelivered turn's worker recognises its own rows
             by (event_id, file_id).

``POST /v1/internal/thread-attachments/append``
    request  ``{"agent_id": "<uuid>", "thread_key": "<key>",
               "event_id": "<turn event id>", "refs": [<ref>, ...]}``
    200      ``{"appended": <rows newly inserted>}``. Idempotent per
             (event_id, file_id): a redelivered append inserts nothing and
             does not move an existing reference's place in the order.
    409      ``{"detail": {"code": "thread_attachment.name_conflict", ...}}``
             when a ref's ``disk_name`` is already held in the thread by a
             different (event_id, file_id), or repeated within the request.
             Nothing from the request is stored.
    413      ``{"detail": {"code": "thread_attachment.thread_full", ...}}``
             when the append would take the thread past
             ``thread_attachment_max_refs`` (setting, env
             ``THREAD_ATTACHMENT_MAX_REFS``, default 200). Nothing is stored.
    422      a ref carrying any field not listed below (``extra="forbid"``):
             the ledger never records an endpoint, a URL or bytes.

``<ref>`` is exactly these fields (an append's refs; a query row adds only
``event_id``)::

    {"file_id": str, "ordinal": int, "name": str, "disk_name": str,
     "mime_type": str | None, "size_bytes": int | None, "sha256": str,
     "route_kind": str, "route_adapter": str | None, "route_identity": str}

Lifetime: an append sets the reference's ``expires_at`` to now +
``transcript_idle_ttl_seconds``. A read applies the transcript's state:
expired transcript -> ``[]``; live transcript -> the rows, with their
``expires_at`` moved to the transcript's; no transcript yet -> the rows whose
own ``expires_at`` is still in the future. Removal follows
``transcripts.remove``, ``transcripts.expire_for_work_item``, the idle sweep
(``_sweep_expired``: references of swept keys, and orphan references past
their own expiry with no live transcript), the agent FK cascade, and the
ADR-0168 pre-identity copy-forward and deletion.

The next-only migration is revision ``0086`` after action executions at ``0085``,
following the immutable published stable chain through ``0081``.

Real router, real Postgres. Nothing mocked.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
)
from alembic import command
from alembic.script import ScriptDirectory
from channel_protocol import scoped_conversation_id
from curie_api import transcripts
from curie_api.config import Settings, get_settings
from curie_api.routers.console import SESSION_COOKIE
from curie_internal.sandbox_token import mint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

QUERY = "/v1/internal/thread-attachments/query"
APPEND = "/v1/internal/thread-attachments/append"

REF_FIELDS = {
    "file_id",
    "ordinal",
    "name",
    "disk_name",
    "mime_type",
    "size_bytes",
    "sha256",
    "route_kind",
    "route_adapter",
    "route_identity",
}
# A query row: the appended ref plus the event it was recorded under, nothing else.
ROW_FIELDS = REF_FIELDS | {"event_id"}

CHANNEL = "C0EXAMPLE1"
THREAD = scoped_conversation_id("slack", CHANNEL, "1700000000.000100")
OTHER_THREAD = scoped_conversation_id("slack", CHANNEL, "1700000000.000200")
HISTORY = [{"role": "user", "content": "here is the report"}]

MAIL_ADDRESS = "agent@example.test"
MAIL_ADAPTER = "agentmail-sandbox"
MAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
MAIL_OLD_KEY = scoped_conversation_id("email", MAIL_ADDRESS, "thread/9")
MAIL_NEW_KEY = scoped_conversation_id("email", MAIL_ADDRESS, "thread/9", identity=MAIL_ADAPTER)

REVISION = "0086"
BELOW = "0085"

_FAR_FUTURE = 4102444800  # 2100-01-01


# --- helpers -----------------------------------------------------------------


def _worker() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def _ref(
    file_id: str, ordinal: int = 0, *, disk_name: str | None = None, **extra: Any
) -> dict[str, Any]:
    name = f"{file_id}.pdf"
    ref: dict[str, Any] = {
        "file_id": file_id,
        "ordinal": ordinal,
        "name": name,
        "disk_name": disk_name or name,
        "mime_type": "application/pdf",
        "size_bytes": 1024,
        "sha256": hashlib.sha256(file_id.encode()).hexdigest(),
        "route_kind": "slack",
        "route_adapter": None,
        "route_identity": "default",
    }
    ref.update(extra)
    return ref


def _agent(client: Any, auth_headers: dict[str, str], channel: dict[str, Any] | None = None) -> str:
    resp = client.post(
        "/agents",
        json={
            "name": f"attach-{uuid.uuid4().hex[:8]}",
            "channel": channel or {"kind": "slack", "address": CHANNEL},
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


def _append(
    client: Any, aid: str, event_id: str, refs: list[dict[str, Any]], key: str = THREAD
) -> Any:
    return client.post(
        APPEND,
        json={"agent_id": aid, "thread_key": key, "event_id": event_id, "refs": refs},
        headers=_worker(),
    )


def _appended(
    client: Any, aid: str, event_id: str, refs: list[dict[str, Any]], key: str = THREAD
) -> int:
    resp = _append(client, aid, event_id, refs, key)
    assert resp.status_code == 200, resp.text
    return int(resp.json()["appended"])


def _query(client: Any, aid: str, key: str = THREAD) -> list[dict[str, Any]]:
    resp = client.post(QUERY, json={"agent_id": aid, "thread_key": key}, headers=_worker())
    assert resp.status_code == 200, resp.text
    refs: list[dict[str, Any]] = resp.json()["refs"]
    assert isinstance(refs, list)
    return refs


def _file_ids(refs: list[dict[str, Any]]) -> list[str]:
    return [ref["file_id"] for ref in refs]


def _transcript_url(aid: str, key: str) -> str:
    # Starlette's TestClient unquotes twice; see test_transcript_identity._url.
    return f"/agents/{aid}/state/transcript/{quote(quote(key, safe=''), safe='')}"


def _seed_transcript(client: Any, auth_headers: dict[str, str], aid: str, key: str) -> None:
    put = client.put(_transcript_url(aid, key), json={"value": HISTORY}, headers=auth_headers)
    assert put.status_code == 200, put.text


def _stored(aid: str, key: str | None = None) -> list[dict[str, Any]]:
    clause = "" if key is None else " AND thread_key = :k"
    return sql_dicts(
        "SELECT thread_key, file_id, disk_name, expires_at FROM curie.thread_attachment_refs "
        f"WHERE agent_id = :a{clause} ORDER BY seq",
        {"a": uuid.UUID(aid), **({} if key is None else {"k": key})},
    )


def _expire_transcript(aid: str, key: str) -> None:
    sql_dicts(
        "UPDATE curie.thread_transcripts SET expires_at = now() - interval '1 hour' "
        "WHERE agent_id = :a AND thread_key = :k",
        {"a": uuid.UUID(aid), "k": key},
    )


def _expire_refs(aid: str, key: str) -> None:
    sql_dicts(
        "UPDATE curie.thread_attachment_refs SET expires_at = now() - interval '1 hour' "
        "WHERE agent_id = :a AND thread_key = :k",
        {"a": uuid.UUID(aid), "k": key},
    )


def _refused(resp: Any, status: int, code: str) -> None:
    assert resp.status_code == status, resp.text
    assert resp.json()["detail"]["code"] == code


@pytest.fixture
def small_cap(monkeypatch: pytest.MonkeyPatch) -> Iterator[int]:
    monkeypatch.setenv("THREAD_ATTACHMENT_MAX_REFS", "3")
    get_settings.cache_clear()
    yield 3
    monkeypatch.delenv("THREAD_ATTACHMENT_MAX_REFS", raising=False)
    get_settings.cache_clear()


# --- append and read ---------------------------------------------------------


def test_refs_come_back_in_arrival_order_with_exactly_the_recorded_fields(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    first = [_ref("F0A", 0), _ref("F0B", 1)]
    second = [_ref("F0C", 0, route_kind="email", route_adapter=MAIL_ADAPTER)]

    assert _appended(client, aid, "evt-1", first) == 2
    assert _appended(client, aid, "evt-2", second) == 1

    refs = _query(client, aid)
    assert _file_ids(refs) == ["F0A", "F0B", "F0C"]
    # Exactly the recorded fields and their event: no endpoint, URL, bytes,
    # or row internals.
    for ref in refs:
        assert set(ref) == ROW_FIELDS, ref
    assert refs == [
        *({**ref, "event_id": "evt-1"} for ref in first),
        *({**ref, "event_id": "evt-2"} for ref in second),
    ]


def test_a_redelivered_append_records_nothing_twice_and_keeps_the_order(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 2
    assert _appended(client, aid, "evt-2", [_ref("F0C")]) == 1

    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 0

    assert _file_ids(_query(client, aid)) == ["F0A", "F0B", "F0C"]
    assert len(_stored(aid)) == 3


def test_a_redelivery_that_renames_a_recorded_file_is_refused_whole(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A disk name is fixed when it is recorded (ADR 0205 decision 4). A
    redelivery naming a recorded (event, file) differently must not be
    swallowed as a no-op: the worker would then tell the agent a path the
    ledger never holds."""
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 2

    renamed = _append(
        client,
        aid,
        "evt-1",
        [_ref("F0A", 0), _ref("F0B", 1, disk_name="renamed.pdf"), _ref("F0C", 2)],
    )
    _refused(renamed, 409, "thread_attachment.name_mismatch")
    body = renamed.text.lower()
    for leaked in ("http://", "https://", "endpoint", "url"):
        assert leaked not in body, renamed.text

    # Nothing written: no F0C, and F0B keeps its recorded name.
    refs = _query(client, aid)
    assert _file_ids(refs) == ["F0A", "F0B"]
    assert [ref["disk_name"] for ref in refs] == ["F0A.pdf", "F0B.pdf"]
    assert len(_stored(aid)) == 2

    # An identical redelivery is still an idempotent no-op.
    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 0


def test_the_same_file_on_a_later_event_is_a_new_reference(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Idempotence is per (event, file): a file re-shared on a later message is
    recorded again, under its own disambiguated disk name."""
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    assert _appended(client, aid, "evt-2", [_ref("F0A", disk_name="F0A (1).pdf")]) == 1

    refs = _query(client, aid)
    assert [ref["disk_name"] for ref in refs] == ["F0A.pdf", "F0A (1).pdf"]
    assert [(ref["event_id"], ref["file_id"]) for ref in refs] == [
        ("evt-1", "F0A"),
        ("evt-2", "F0A"),
    ]


def test_threads_and_agents_do_not_share_a_ledger(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    stranger = _agent(client, auth_headers, {"kind": "slack", "address": "C0EXAMPLE2"})
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1

    assert _query(client, aid, OTHER_THREAD) == []
    assert _query(client, stranger) == []
    # The same disk name is free in another thread.
    assert _appended(client, aid, "evt-9", [_ref("F0Z", disk_name="F0A.pdf")], OTHER_THREAD) == 1


def test_a_disk_name_held_by_another_file_is_a_conflict_and_stores_nothing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A", disk_name="report.pdf")]) == 1

    clash = _append(client, aid, "evt-2", [_ref("F0B", 0), _ref("F0C", 1, disk_name="report.pdf")])
    _refused(clash, 409, "thread_attachment.name_conflict")
    # One transaction: the non-clashing F0B was not stored either.
    assert _file_ids(_query(client, aid)) == ["F0A"]


def test_a_disk_name_repeated_within_one_append_is_a_conflict(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    clash = _append(
        client, aid, "evt-1", [_ref("F0A", 0, disk_name="a.pdf"), _ref("F0B", 1, disk_name="a.pdf")]
    )
    _refused(clash, 409, "thread_attachment.name_conflict")
    assert _query(client, aid) == []


def test_a_thread_past_its_reference_cap_is_refused_whole(
    client: Any, auth_headers: dict[str, str], clean_db: None, small_cap: int
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 2

    over = _append(client, aid, "evt-2", [_ref("F0C", 0), _ref("F0D", 1)])
    _refused(over, 413, "thread_attachment.thread_full")
    assert _file_ids(_query(client, aid)) == ["F0A", "F0B"]

    # Up to the cap is fine, and a redelivery at the cap inserts nothing, so
    # it is not refused.
    assert _appended(client, aid, "evt-3", [_ref("F0C")]) == 1
    assert _appended(client, aid, "evt-1", [_ref("F0A", 0), _ref("F0B", 1)]) == 0


def test_the_reference_cap_defaults_to_200(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THREAD_ATTACHMENT_MAX_REFS", raising=False)
    assert Settings().thread_attachment_max_refs == 200


@pytest.mark.parametrize("field", ["url", "endpoint", "content"])
def test_a_ref_carrying_an_endpoint_url_or_bytes_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str
) -> None:
    aid = _agent(client, auth_headers)
    ref = _ref("F0A")
    ref[field] = "https://files.example.test/F0A"
    resp = _append(client, aid, "evt-1", [ref])
    assert resp.status_code == 422, resp.text
    assert _stored(aid) == []


# --- credentials ---------------------------------------------------------------


def _console_cookie(client: Any, auth_headers: dict[str, str]) -> dict[str, str]:
    minted = client.post(
        "/console/login-codes", json={"subject": "U0EXAMPLE1"}, headers=auth_headers
    )
    assert minted.status_code == 201, minted.text
    exchanged = client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = client.cookies.get(SESSION_COOKIE)
    assert isinstance(token, str) and token
    client.cookies.clear()
    return {"Cookie": f"{SESSION_COOKIE}={token}"}


@pytest.mark.parametrize("route", [QUERY, APPEND])
def test_only_the_worker_credential_reaches_the_ledger(
    client: Any, auth_headers: dict[str, str], clean_db: None, route: str
) -> None:
    aid = _agent(client, auth_headers)
    state_token = mint(
        get_settings().api_key,
        agent=aid,
        scope="state",
        exp=_FAR_FUTURE,
        claims={"binding": f"slack:{CHANNEL}", "memory": "write", "sender": "U0S", "turn": "e"},
    )
    body: dict[str, Any] = {"agent_id": aid, "thread_key": THREAD}
    if route == APPEND:
        body |= {"event_id": "evt-1", "refs": [_ref("F0A")]}

    refused = {
        "platform key": auth_headers,
        "sandbox state token": {"X-API-Key": state_token},
        "state token as worker token": {"X-Curie-Worker-Token": state_token},
        "platform key as worker token": {"X-Curie-Worker-Token": get_settings().api_key},
        "console session": _console_cookie(client, auth_headers),
        "wrong worker token": {"X-Curie-Worker-Token": "not-the-token"},
        "nothing": {},
    }
    for label, headers in refused.items():
        resp = client.post(route, json=body, headers=headers)
        assert resp.status_code == 401, (label, resp.status_code, resp.text)
    assert _stored(aid) == []

    assert client.post(route, json=body, headers=_worker()).status_code == 200


# --- lazy expiry ---------------------------------------------------------------


def test_an_append_lives_for_the_transcript_idle_window(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    ttl = get_settings().transcript_idle_ttl_seconds
    rows = sql_dicts(
        "SELECT expires_at > now() + make_interval(secs => :lo) AS after_lo, "
        "expires_at < now() + make_interval(secs => :hi) AS before_hi "
        "FROM curie.thread_attachment_refs WHERE agent_id = :a",
        {"a": uuid.UUID(aid), "lo": ttl - 600, "hi": ttl + 600},
    )
    assert rows == [{"after_lo": True, "before_hi": True}]


def test_a_thread_whose_transcript_expired_reads_no_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, THREAD)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1

    _expire_transcript(aid, THREAD)

    assert _query(client, aid) == []


def test_a_live_transcript_carries_its_files_and_their_expiry_with_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, THREAD)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    # The transcript was written again later than the refs, so it lives longer.
    sql_dicts(
        "UPDATE curie.thread_transcripts SET expires_at = now() + interval '45 days' "
        "WHERE agent_id = :a AND thread_key = :k",
        {"a": uuid.UUID(aid), "k": THREAD},
    )

    assert _file_ids(_query(client, aid)) == ["F0A"]

    rows = sql_dicts(
        "SELECT r.expires_at = t.expires_at AS same FROM curie.thread_attachment_refs r "
        "JOIN curie.thread_transcripts t ON t.agent_id = r.agent_id "
        "AND t.binding_scope IS NOT DISTINCT FROM r.binding_scope "
        "AND t.thread_key = r.thread_key WHERE r.agent_id = :a",
        {"a": uuid.UUID(aid)},
    )
    assert rows == [{"same": True}]


def test_a_thread_with_no_transcript_yet_reads_its_live_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The append lands right after install, before the runner's first
    transcript write, so a boot in between must still see the files."""
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1

    assert _file_ids(_query(client, aid)) == ["F0A"]


def test_a_thread_with_no_transcript_drops_files_past_their_own_expiry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    _expire_refs(aid, THREAD)

    assert _query(client, aid) == []


# --- removal with the transcript -----------------------------------------------


def test_deleting_the_transcript_deletes_the_threads_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, THREAD)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    assert _appended(client, aid, "evt-2", [_ref("F0B")], OTHER_THREAD) == 1

    deleted = client.delete(_transcript_url(aid, THREAD), headers=auth_headers)
    assert deleted.status_code == 204, deleted.text

    assert _stored(aid, THREAD) == []
    assert _file_ids(_query(client, aid)) == []
    # Another thread is untouched.
    assert _file_ids(_query(client, aid, OTHER_THREAD)) == ["F0B"]


def test_deleting_a_thread_with_no_transcript_still_deletes_its_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """``remove`` ends the thread even when only references exist yet."""
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1

    client.delete(_transcript_url(aid, THREAD), headers=auth_headers)

    assert _stored(aid, THREAD) == []


def _run(body: Any) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                await body(session)
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_a_terminal_work_item_removes_its_threads_files_in_the_same_transaction(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, THREAD)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    assert _appended(client, aid, "evt-2", [_ref("F0B")], OTHER_THREAD) == 1
    work_item = SimpleNamespace(agent_id=uuid.UUID(aid), conversation_id=THREAD)

    async def rolled_back(session: AsyncSession) -> None:
        await transcripts.expire_for_work_item(session, work_item)  # type: ignore[arg-type]
        await session.rollback()

    _run(rolled_back)
    # The caller's rollback keeps them: the delete is in its transaction.
    assert _file_ids(_query(client, aid)) == ["F0A"]

    async def committed(session: AsyncSession) -> None:
        await transcripts.expire_for_work_item(session, work_item)  # type: ignore[arg-type]
        await session.commit()

    _run(committed)
    assert _stored(aid, THREAD) == []
    assert _file_ids(_query(client, aid, OTHER_THREAD)) == ["F0B"]


def test_the_idle_sweep_removes_the_files_of_a_swept_transcript(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, THREAD)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    _expire_transcript(aid, THREAD)

    # Any transcript write for the agent sweeps its expired threads.
    _seed_transcript(client, auth_headers, aid, OTHER_THREAD)

    assert _stored(aid, THREAD) == []


def test_the_idle_sweep_removes_orphan_files_past_their_own_expiry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A thread whose runner never wrote a transcript has nothing for the
    transcript sweep to find; its references expire on their own clock."""
    aid = _agent(client, auth_headers)
    third = scoped_conversation_id("slack", CHANNEL, "1700000000.000300")
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1
    assert _appended(client, aid, "evt-2", [_ref("F0B")], third) == 1
    _expire_refs(aid, THREAD)

    _seed_transcript(client, auth_headers, aid, OTHER_THREAD)

    assert _stored(aid, THREAD) == []
    # An orphan still inside its own window is kept.
    assert [row["file_id"] for row in _stored(aid, third)] == ["F0B"]


def test_deleting_the_agent_deletes_its_ledger(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_ref("F0A")]) == 1

    assert client.delete(f"/agents/{aid}", headers=auth_headers).status_code == 204

    assert _stored(aid) == []


# --- ADR-0168 pre-identity key -------------------------------------------------


def _mail_agent(client: Any, auth_headers: dict[str, str]) -> str:
    return _agent(
        client,
        auth_headers,
        {
            "kind": "email",
            "address": MAIL_ADDRESS,
            "endpoint": MAIL_ENDPOINT,
            "adapter": MAIL_ADAPTER,
        },
    )


def _mail_ref(file_id: str) -> dict[str, Any]:
    return _ref(
        file_id, route_kind="email", route_adapter=MAIL_ADAPTER, route_identity=MAIL_ADAPTER
    )


def test_a_named_route_reads_the_files_recorded_under_its_pre_identity_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A worker that has not rolled still records under the old key; the
    ledger follows the transcript's copy-forward and leaves the old rows."""
    aid = _mail_agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, MAIL_OLD_KEY)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1

    assert _file_ids(_query(client, aid, MAIL_NEW_KEY)) == ["F0A"]
    assert _file_ids(_query(client, aid, MAIL_OLD_KEY)) == ["F0A"]


def test_a_later_append_under_the_new_key_follows_the_adopted_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, MAIL_OLD_KEY)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1
    assert _file_ids(_query(client, aid, MAIL_NEW_KEY)) == ["F0A"]

    assert _appended(client, aid, "evt-2", [_mail_ref("F0B")], MAIL_NEW_KEY) == 1

    assert _file_ids(_query(client, aid, MAIL_NEW_KEY)) == ["F0A", "F0B"]


def test_an_adopted_disk_name_is_not_reused_under_the_new_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, MAIL_OLD_KEY)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1
    assert _file_ids(_query(client, aid, MAIL_NEW_KEY)) == ["F0A"]

    clash = _append(client, aid, "evt-2", [_ref("F0B", disk_name="F0A.pdf")], MAIL_NEW_KEY)
    _refused(clash, 409, "thread_attachment.name_conflict")


def test_deleting_the_new_key_removes_the_pre_identity_files_too(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Otherwise the old rows outlive the delete and the next read copies them
    straight back (test_transcript_identity's resurrection, for files)."""
    aid = _mail_agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, MAIL_OLD_KEY)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1
    assert _file_ids(_query(client, aid, MAIL_NEW_KEY)) == ["F0A"]

    deleted = client.delete(_transcript_url(aid, MAIL_NEW_KEY), headers=auth_headers)
    assert deleted.status_code == 204, deleted.text

    assert _stored(aid) == []
    assert _query(client, aid, MAIL_NEW_KEY) == []


def test_a_terminal_work_item_on_the_new_key_removes_the_pre_identity_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1
    work_item = SimpleNamespace(agent_id=uuid.UUID(aid), conversation_id=MAIL_NEW_KEY)

    async def committed(session: AsyncSession) -> None:
        await transcripts.expire_for_work_item(session, work_item)  # type: ignore[arg-type]
        await session.commit()

    _run(committed)

    assert _stored(aid) == []


def test_an_expired_pre_identity_transcript_is_not_adopted_with_its_files(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _mail_agent(client, auth_headers)
    _seed_transcript(client, auth_headers, aid, MAIL_OLD_KEY)
    assert _appended(client, aid, "evt-1", [_mail_ref("F0A")], MAIL_OLD_KEY) == 1
    _expire_transcript(aid, MAIL_OLD_KEY)

    assert _query(client, aid, MAIL_NEW_KEY) == []


# --- migration -----------------------------------------------------------------


def _require_revision() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    known = {rev.revision for rev in script.walk_revisions()}
    assert REVISION in known, f"no alembic revision {REVISION} adds thread_attachment_refs yet"
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def _table_exists() -> bool:
    rows = sql_dicts("SELECT to_regclass('curie.thread_attachment_refs') AS name")
    return rows[0]["name"] is not None


def _insert_ref(agent_id: uuid.UUID, event_id: str, file_id: str, disk_name: str) -> None:
    sql_dicts(
        "INSERT INTO curie.thread_attachment_refs (id, agent_id, binding_scope, thread_key, "
        "event_id, file_id, ordinal, name, disk_name, mime_type, size_bytes, sha256, "
        "route_kind, route_adapter, route_identity, expires_at) VALUES (:id, :a, NULL, :k, "
        ":e, :f, 0, :n, :n, NULL, NULL, :sha, 'slack', NULL, 'default', "
        "now() + interval '1 day')",
        {
            "id": uuid.uuid4(),
            "a": agent_id,
            "k": THREAD,
            "e": event_id,
            "f": file_id,
            "n": disk_name,
            "sha": "0" * 64,
        },
    )


def test_the_ledger_revision_sits_on_the_current_head() -> None:
    _require_revision()


def test_the_migration_creates_the_ledger_and_the_downgrade_drops_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    _require_revision()
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent_id = uuid.uuid4()
    try:
        assert not _table_exists()
        sql_dicts(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": agent_id, "name": f"attach-{agent_id.hex[:8]}"},
        )

        command.upgrade(config, REVISION)
        assert _table_exists()
        assert {
            "id",
            "seq",
            "agent_id",
            "binding_scope",
            "thread_key",
            "event_id",
            "expires_at",
            "created_at",
            *REF_FIELDS,
        } <= column_names("thread_attachment_refs")

        _insert_ref(agent_id, "evt-1", "F0A", "a.pdf")
        # (agent, scope, thread, event, file) is unique with NULL scope equal.
        with pytest.raises(IntegrityError):
            _insert_ref(agent_id, "evt-1", "F0A", "b.pdf")
        # (agent, scope, thread, disk_name) likewise.
        with pytest.raises(IntegrityError):
            _insert_ref(agent_id, "evt-2", "F0B", "a.pdf")
        _insert_ref(agent_id, "evt-2", "F0B", "b.pdf")
        seqs = sql_dicts(
            "SELECT file_id FROM curie.thread_attachment_refs WHERE agent_id = :a ORDER BY seq",
            {"a": agent_id},
        )
        assert [row["file_id"] for row in seqs] == ["F0A", "F0B"]

        # The FK cascades with the agent.
        sql_dicts("DELETE FROM curie.agents WHERE id = :a", {"a": agent_id})
        assert (
            sql_dicts(
                "SELECT 1 FROM curie.thread_attachment_refs WHERE agent_id = :a", {"a": agent_id}
            )
            == []
        )

        command.downgrade(config, BELOW)
        assert not _table_exists()
    finally:
        command.upgrade(config, "head")
