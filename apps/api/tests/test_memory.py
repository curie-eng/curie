"""Agent memory read + learned-from trace-back (#266).

Seeds the runner-written memory log via the state append endpoint (the same wire
the runner uses), then exercises the read/trace-back surface against the real
compose Postgres.
"""

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import asyncpg
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from sqlalchemy import make_url


def _agent(client: Any, headers: dict[str, str]) -> str:
    resp = client.post(
        "/agents",
        json={"name": "memory-agent", "channel": {"kind": "slack", "address": "C000000M01"}},
        headers=headers,
    )
    assert resp.status_code == 201, resp.text
    agent_id: str = resp.json()["id"]
    return agent_id


def _remember(
    client: Any, headers: dict[str, str], aid: str, record: dict[str, Any]
) -> dict[str, Any]:
    """Append one {content, provenance} record to the memory log key."""
    url = f"/agents/{aid}/state/memory/log/append"
    r = client.post(url, json={"item": record}, headers=headers)
    assert r.status_code == 200, r.text
    body: dict[str, Any] = r.json()
    return body


def _memory(client: Any, headers: dict[str, str], aid: str) -> list[dict[str, Any]]:
    response = client.get(f"/agents/{aid}/memory", headers=headers)
    assert response.status_code == 200, response.text
    entries: list[dict[str, Any]] = response.json()
    return entries


def _asyncpg_dsn() -> str:
    url = make_url(get_settings().database_url).set(drivername="postgresql")
    return url.render_as_string(hide_password=False)


async def _install_memory_update_gate() -> None:
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        await connection.execute(
            """
            CREATE OR REPLACE FUNCTION curie.test_memory_update_gate()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(391391);
                RETURN NEW;
            END;
            $$
            """
        )
        await connection.execute(
            """
            CREATE TRIGGER test_memory_update_gate
            BEFORE UPDATE ON curie.workflow_state_entries
            FOR EACH ROW
            EXECUTE FUNCTION curie.test_memory_update_gate()
            """
        )
    finally:
        await connection.close()


async def _remove_memory_update_gate() -> None:
    connection = await asyncpg.connect(_asyncpg_dsn())
    try:
        await connection.execute(
            """
            DROP TRIGGER IF EXISTS test_memory_update_gate
            ON curie.workflow_state_entries
            """
        )
        await connection.execute(
            "DROP FUNCTION IF EXISTS curie.test_memory_update_gate()"
        )
    finally:
        await connection.close()


async def _wait_for_blocked_memory_requests(
    minimum: int, requests: list[Future[Any]]
) -> None:
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
                  AND query LIKE '%workflow_state_entries%'
                """
            )
            if int(count) >= minimum:
                return
            completed = [request.result() for request in requests if request.done()]
            assert not completed, f"request completed before blocking: {completed}"
        raise AssertionError(f"only some of {minimum} memory requests blocked")
    finally:
        await connection.close()


def _hold_memory_update_gate(
    acquired: threading.Event,
    release: threading.Event,
    errors: list[Exception],
) -> None:
    async def hold() -> None:
        connection = await asyncpg.connect(_asyncpg_dsn())
        try:
            await connection.execute("SELECT pg_advisory_lock(391391)")
            acquired.set()
            release.wait()
            await connection.execute("SELECT pg_advisory_unlock(391391)")
        finally:
            await connection.close()

    try:
        asyncio.run(hold())
    except Exception as error:  # noqa: BLE001 - existing broad catch retained
        errors.append(error)
        acquired.set()


def _run_ordered_memory_requests(
    first: Callable[[], Any], second: Callable[[], Any]
) -> tuple[Any, Any]:
    asyncio.run(_install_memory_update_gate())
    acquired = threading.Event()
    release = threading.Event()
    lock_errors: list[Exception] = []
    lock_thread = threading.Thread(
        target=_hold_memory_update_gate,
        args=(acquired, release, lock_errors),
        daemon=True,
    )
    lock_thread.start()
    try:
        assert acquired.wait(timeout=10), "database gate was not acquired"
        assert not lock_errors, lock_errors
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_request = executor.submit(first)
            try:
                asyncio.run(_wait_for_blocked_memory_requests(1, [first_request]))
                second_request = executor.submit(second)
                asyncio.run(
                    _wait_for_blocked_memory_requests(
                        2, [first_request, second_request]
                    )
                )
            finally:
                release.set()
            first_response = first_request.result(timeout=10)
            second_response = second_request.result(timeout=10)
    finally:
        release.set()
        lock_thread.join(timeout=10)
        asyncio.run(_remove_memory_update_gate())

    assert not lock_thread.is_alive(), "database gate did not release"
    assert not lock_errors, lock_errors
    return first_response, second_response


def test_list_memory_empty_for_fresh_agent(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    r = client.get(f"/agents/{aid}/memory", headers=auth_headers)
    assert r.status_code == 200, r.text
    assert r.json() == []


def test_list_memory_returns_entries_with_provenance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(
        client,
        auth_headers,
        aid,
        {
            "content": "deploy is a git push",
            "provenance": {
                "learned_from_session_id": "sess-1",
                "source_trace_ids": ["trace-a", "trace-b"],
                "recorded_at": "2026-07-13T00:00:00+00:00",
            },
        },
    )
    appended = _remember(
        client, auth_headers, aid, {"content": "no-provenance lesson"}
    )

    entries = _memory(client, auth_headers, aid)
    assert len(entries) == 2
    assert entries[0]["index"] == 0
    assert entries[0]["content"] == "deploy is a git push"
    assert entries[0]["provenance"]["source_trace_ids"] == ["trace-a", "trace-b"]
    assert entries[0]["version"] == appended["version"]
    # A record with no recorded provenance degrades to empty provenance, not 500.
    assert entries[1]["provenance"]["source_trace_ids"] == []
    assert entries[1]["version"] == appended["version"]


def test_trace_back_resolves_sessions_and_traces(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(
        client,
        auth_headers,
        aid,
        {
            "content": "prod push reuses the dev bundle",
            "provenance": {
                "learned_from_session_id": "sess-9",
                "source_trace_ids": ["trace-x", "trace-y"],
                "recorded_at": "2026-07-13T01:00:00+00:00",
            },
        },
    )

    r = client.get(f"/agents/{aid}/memory/0/provenance", headers=auth_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["learned_from_session_id"] == "sess-9"
    assert body["recorded_at"] == "2026-07-13T01:00:00+00:00"
    trace_ids = [t["trace_id"] for t in body["source_traces"]]
    assert trace_ids == ["trace-x", "trace-y"]
    # Each source trace resolves to a viewable link.
    for t in body["source_traces"]:
        assert t["trace_url"].endswith(f"/trace/{t['trace_id']}")


def test_trace_back_out_of_range_index_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "only one"})
    r = client.get(f"/agents/{aid}/memory/5/provenance", headers=auth_headers)
    assert r.status_code == 404, r.text


def test_memory_unknown_agent_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    r = client.get(f"/agents/{missing}/memory", headers=auth_headers)
    assert r.status_code == 404, r.text


def _prov(session_id: str, *traces: str) -> dict:
    return {
        "learned_from_session_id": session_id,
        "source_trace_ids": list(traces),
        "recorded_at": "2026-07-13T00:00:00+00:00",
    }


def test_edit_entry_preserves_provenance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(
        client,
        auth_headers,
        aid,
        {"content": "old lesson", "provenance": _prov("sess-9", "t-x", "t-y")},
    )

    listed = _memory(client, auth_headers, aid)
    r = client.put(
        f"/agents/{aid}/memory/0",
        json={
            "content": "corrected lesson",
            "expected_version": listed[0]["version"],
        },
        headers=auth_headers,
    )
    assert r.status_code == 200, r.text
    assert r.json()["content"] == "corrected lesson"
    assert r.json()["version"] == listed[0]["version"] + 1
    # Editing the text must not erase where it was learned from.
    assert r.json()["provenance"]["source_trace_ids"] == ["t-x", "t-y"]

    # The change is durable -- a fresh read (what the next boot sees) reflects it.
    entries = _memory(client, auth_headers, aid)
    assert entries[0]["content"] == "corrected lesson"
    assert entries[0]["provenance"]["learned_from_session_id"] == "sess-9"


def test_delete_entry_removes_one_and_reindexes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    for c in ("a", "b", "c"):
        _remember(client, auth_headers, aid, {"content": c})

    listed = _memory(client, auth_headers, aid)
    d = client.delete(
        f"/agents/{aid}/memory/1",
        params={"expected_version": listed[0]["version"]},
        headers=auth_headers,
    )
    assert d.status_code == 204, d.text

    entries = _memory(client, auth_headers, aid)
    assert [e["content"] for e in entries] == ["a", "c"]
    assert [e["index"] for e in entries] == [0, 1]
    assert {e["version"] for e in entries} == {listed[0]["version"] + 1}


def test_edit_out_of_range_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "only one"})
    version = _memory(client, auth_headers, aid)[0]["version"]
    r = client.put(
        f"/agents/{aid}/memory/5",
        json={"content": "x", "expected_version": version},
        headers=auth_headers,
    )
    assert r.status_code == 404, r.text


def test_delete_out_of_range_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "only one"})
    version = _memory(client, auth_headers, aid)[0]["version"]
    r = client.delete(
        f"/agents/{aid}/memory/9",
        params={"expected_version": version},
        headers=auth_headers,
    )
    assert r.status_code == 404, r.text


def test_edit_requires_expected_version(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "keep this lesson"})

    response = client.put(
        f"/agents/{aid}/memory/0",
        json={"content": "unversioned correction"},
        headers=auth_headers,
    )

    assert response.status_code == 422, response.text
    assert _memory(client, auth_headers, aid)[0]["content"] == "keep this lesson"


def test_delete_requires_expected_version_query_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "keep this lesson"})

    response = client.delete(f"/agents/{aid}/memory/0", headers=auth_headers)

    assert response.status_code == 422, response.text
    assert [entry["content"] for entry in _memory(client, auth_headers, aid)] == [
        "keep this lesson"
    ]


def test_runner_append_makes_operator_edit_token_stale_without_losing_append(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "original lesson"})
    listed = _memory(client, auth_headers, aid)

    appended = _remember(
        client, auth_headers, aid, {"content": "runner learned another lesson"}
    )
    response = client.put(
        f"/agents/{aid}/memory/0",
        json={
            "content": "operator correction",
            "expected_version": listed[0]["version"],
        },
        headers=auth_headers,
    )

    assert response.status_code == 409, response.text
    assert appended["version"] == listed[0]["version"] + 1
    assert [entry["content"] for entry in _memory(client, auth_headers, aid)] == [
        "original lesson",
        "runner learned another lesson",
    ]


def test_stale_delete_does_not_remove_a_repositioned_entry(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "first lesson"})
    _remember(client, auth_headers, aid, {"content": "second lesson"})
    _remember(client, auth_headers, aid, {"content": "third lesson"})
    listed = _memory(client, auth_headers, aid)
    current_delete = client.delete(
        f"/agents/{aid}/memory/0",
        params={"expected_version": listed[0]["version"]},
        headers=auth_headers,
    )
    assert current_delete.status_code == 204, current_delete.text

    response = client.delete(
        f"/agents/{aid}/memory/1",
        params={"expected_version": listed[0]["version"]},
        headers=auth_headers,
    )

    assert response.status_code == 409, response.text
    assert [entry["content"] for entry in _memory(client, auth_headers, aid)] == [
        "second lesson",
        "third lesson",
    ]


def test_oversized_edit_is_rejected_without_changing_memory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    seeded = _remember(
        client,
        auth_headers,
        aid,
        {"content": "bounded lesson", "provenance": _prov("sess_cap", "trace_cap")},
    )
    listed = _memory(client, auth_headers, aid)
    get_settings().state_max_value_bytes = 100
    try:
        response = client.put(
            f"/agents/{aid}/memory/0",
            json={
                "content": "x" * 200,
                "expected_version": listed[0]["version"],
            },
            headers=auth_headers,
        )

        assert response.status_code == 413, response.text
        stored = client.get(
            f"/agents/{aid}/state/memory/log", headers=auth_headers
        ).json()
        assert stored["version"] == seeded["version"]
        assert stored["value"] == seeded["value"]
    finally:
        get_settings.cache_clear()


def test_overlapping_edit_then_append_preserves_both_changes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    seeded = _remember(client, auth_headers, aid, {"content": "draft lesson"})
    listed = _memory(client, auth_headers, aid)
    edited, appended = _run_ordered_memory_requests(
        lambda: client.put(
            f"/agents/{aid}/memory/0",
            json={
                "content": "reviewed lesson",
                "expected_version": listed[0]["version"],
            },
            headers=auth_headers,
        ),
        lambda: client.post(
            f"/agents/{aid}/state/memory/log/append",
            json={"item": {"content": "runner follow up"}},
            headers=auth_headers,
        ),
    )

    assert edited.status_code == 200, edited.text
    assert appended.status_code == 200, appended.text
    assert edited.json()["version"] == seeded["version"] + 1
    assert appended.json()["version"] == seeded["version"] + 2
    stored = client.get(f"/agents/{aid}/state/memory/log", headers=auth_headers).json()
    assert stored["version"] == seeded["version"] + 2
    assert [record["content"] for record in stored["value"]] == [
        "reviewed lesson",
        "runner follow up",
    ]


def test_overlapping_append_then_edit_rejects_stale_edit(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    seeded = _remember(client, auth_headers, aid, {"content": "draft lesson"})
    listed = _memory(client, auth_headers, aid)
    appended, edited = _run_ordered_memory_requests(
        lambda: client.post(
            f"/agents/{aid}/state/memory/log/append",
            json={"item": {"content": "runner follow up"}},
            headers=auth_headers,
        ),
        lambda: client.put(
            f"/agents/{aid}/memory/0",
            json={
                "content": "stale correction",
                "expected_version": listed[0]["version"],
            },
            headers=auth_headers,
        ),
    )

    assert appended.status_code == 200, appended.text
    assert edited.status_code == 409, edited.text
    assert appended.json()["version"] == seeded["version"] + 1
    stored = client.get(f"/agents/{aid}/state/memory/log", headers=auth_headers).json()
    assert stored["version"] == seeded["version"] + 1
    assert [record["content"] for record in stored["value"]] == [
        "draft lesson",
        "runner follow up",
    ]


def test_overlapping_delete_then_append_preserves_both_changes(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "remove this lesson"})
    seeded = _remember(client, auth_headers, aid, {"content": "keep this lesson"})
    listed = _memory(client, auth_headers, aid)
    deleted, appended = _run_ordered_memory_requests(
        lambda: client.delete(
            f"/agents/{aid}/memory/0",
            params={"expected_version": listed[0]["version"]},
            headers=auth_headers,
        ),
        lambda: client.post(
            f"/agents/{aid}/state/memory/log/append",
            json={"item": {"content": "runner follow up"}},
            headers=auth_headers,
        ),
    )

    assert deleted.status_code == 204, deleted.text
    assert appended.status_code == 200, appended.text
    assert appended.json()["version"] == seeded["version"] + 2
    stored = client.get(f"/agents/{aid}/state/memory/log", headers=auth_headers).json()
    assert stored["version"] == seeded["version"] + 2
    assert [record["content"] for record in stored["value"]] == [
        "keep this lesson",
        "runner follow up",
    ]


def test_overlapping_append_then_delete_rejects_stale_delete(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    seeded = _remember(client, auth_headers, aid, {"content": "keep this lesson"})
    listed = _memory(client, auth_headers, aid)
    appended, deleted = _run_ordered_memory_requests(
        lambda: client.post(
            f"/agents/{aid}/state/memory/log/append",
            json={"item": {"content": "runner follow up"}},
            headers=auth_headers,
        ),
        lambda: client.delete(
            f"/agents/{aid}/memory/0",
            params={"expected_version": listed[0]["version"]},
            headers=auth_headers,
        ),
    )

    assert appended.status_code == 200, appended.text
    assert deleted.status_code == 409, deleted.text
    assert appended.json()["version"] == seeded["version"] + 1
    stored = client.get(f"/agents/{aid}/state/memory/log", headers=auth_headers).json()
    assert stored["version"] == seeded["version"] + 1
    assert [record["content"] for record in stored["value"]] == [
        "keep this lesson",
        "runner follow up",
    ]


def _create(
    client: Any, headers: dict[str, str], aid: str, content: str
) -> Any:
    """POST one operator-authored memory record through the memory surface."""
    return client.post(
        f"/agents/{aid}/memory",
        json={"content": content},
        headers=headers,
    )


def test_create_memory_seeds_an_empty_log_with_operator_provenance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    lesson = (
        "Before translating into any target language other than Spanish, "
        "ask the user for explicit confirmation"
    )
    created = _create(client, auth_headers, aid, lesson)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["index"] == 0
    assert body["content"] == lesson
    assert body["provenance"]["source"] == "operator"
    assert body["provenance"]["learned_from_session_id"] is None
    assert body["provenance"]["source_trace_ids"] == []
    assert body["provenance"]["recorded_at"]
    assert body["version"] == 1

    entries = _memory(client, auth_headers, aid)
    assert len(entries) == 1
    assert entries[0]["content"] == body["content"]
    assert entries[0]["provenance"]["source"] == "operator"
    assert entries[0]["version"] == body["version"]


def test_create_memory_appends_to_an_existing_log(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    seeded = _remember(
        client,
        auth_headers,
        aid,
        {"content": "deploy is a git push", "provenance": _prov("sess-1", "t-a")},
    )
    created = _create(client, auth_headers, aid, "always confirm before French")
    assert created.status_code == 201, created.text
    assert created.json()["index"] == 1
    assert created.json()["version"] == seeded["version"] + 1
    assert created.json()["provenance"]["source"] == "operator"

    entries = _memory(client, auth_headers, aid)
    assert [e["content"] for e in entries] == [
        "deploy is a git push",
        "always confirm before French",
    ]
    assert entries[0]["provenance"]["source_trace_ids"] == ["t-a"]
    assert entries[1]["provenance"]["source"] == "operator"


def test_create_memory_rejects_empty_and_whitespace_content(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    empty = _create(client, auth_headers, aid, "")
    assert empty.status_code == 422, empty.text
    blank = _create(client, auth_headers, aid, "   \n")
    assert blank.status_code == 422, blank.text
    assert _memory(client, auth_headers, aid) == []


def test_create_memory_unknown_agent_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    response = _create(client, auth_headers, missing, "a lesson")
    assert response.status_code == 404, response.text


def test_create_memory_requires_the_platform_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    assert (
        client.post(
            f"/agents/{aid}/memory", json={"content": "no key"}
        ).status_code
        == 401
    )
    token = mint(
        get_settings().api_key,
        agent=aid,
        scope="state",
        exp=4102444800,
    )
    denied = client.post(
        f"/agents/{aid}/memory",
        json={"content": "scoped token"},
        headers={"X-API-Key": token},
    )
    assert denied.status_code == 401, denied.text
    assert _memory(client, auth_headers, aid) == []


def test_create_memory_rejects_caller_supplied_provenance(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    response = client.post(
        f"/agents/{aid}/memory",
        json={
            "content": "operator lesson",
            "provenance": {
                "learned_from_session_id": "sess-forged",
                "source_trace_ids": ["trace-forged"],
                "source": "learned",
            },
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["provenance"]["source"] == "operator"
    assert body["provenance"]["learned_from_session_id"] is None
    assert body["provenance"]["source_trace_ids"] == []


def test_create_memory_oversized_content_is_rejected_without_writing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    get_settings().state_max_value_bytes = 100
    try:
        response = _create(client, auth_headers, aid, "x" * 200)
        assert response.status_code == 413, response.text
        assert _memory(client, auth_headers, aid) == []
        stored = client.get(
            f"/agents/{aid}/state/memory/log", headers=auth_headers
        )
        assert stored.status_code == 404, stored.text
    finally:
        get_settings.cache_clear()


def test_create_then_list_is_the_shape_the_runner_loader_accepts(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The operator POST writes the same {content, provenance} item the runner loads."""
    aid = _agent(client, auth_headers)
    created = _create(client, auth_headers, aid, "prefer celsius")
    assert created.status_code == 201, created.text
    stored = client.get(
        f"/agents/{aid}/state/memory/log", headers=auth_headers
    )
    assert stored.status_code == 200, stored.text
    items = stored.json()["value"]
    assert items == [
        {
            "content": "prefer celsius",
            "provenance": items[0]["provenance"],
        }
    ]
    provenance = items[0]["provenance"]
    assert provenance["source"] == "operator"
    assert provenance["learned_from_session_id"] is None
    assert provenance["source_trace_ids"] == []
    assert provenance["recorded_at"]


def test_overlapping_operator_creates_both_persist(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "seeded lesson"})
    first, second = _run_ordered_memory_requests(
        lambda: _create(client, auth_headers, aid, "first operator lesson"),
        lambda: _create(client, auth_headers, aid, "second operator lesson"),
    )
    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text
    entries = _memory(client, auth_headers, aid)
    assert [entry["content"] for entry in entries] == [
        "seeded lesson",
        "first operator lesson",
        "second operator lesson",
    ]
    assert [entry["provenance"].get("source") for entry in entries[1:]] == [
        "operator",
        "operator",
    ]


# --------------------------------------------------------------------------- #
# Who removed what (#3673)
# --------------------------------------------------------------------------- #
_STATE_MUTATION_LOGGER = "curie_api.state_mutation"


def _mutation_records(caplog: Any) -> list[Any]:
    return [r for r in caplog.records if r.name == _STATE_MUTATION_LOGGER]


def test_deleting_a_memory_entry_records_who_removed_which(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: Any
) -> None:
    aid = _agent(client, auth_headers)
    for c in ("lesson-a", "lesson-b-secret", "lesson-c"):
        _remember(client, auth_headers, aid, {"content": c})
    listed = _memory(client, auth_headers, aid)

    with caplog.at_level("INFO", logger=_STATE_MUTATION_LOGGER):
        d = client.delete(
            f"/agents/{aid}/memory/1",
            params={"expected_version": listed[0]["version"]},
            headers=auth_headers,
        )
    assert d.status_code == 204, d.text

    records = _mutation_records(caplog)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].state_mutation == {
        "op": "delete",
        "agent_id": aid,
        "scope": "shared",
        "namespace": "memory",
        "key": "log",
        "index": 1,
        "removed": True,
        "principal": "platform",
    }
    message = records[0].getMessage()
    for part in ("op=delete", f"agent_id={aid}", "namespace=memory", "index=1", "removed=true"):
        assert part in message, message
    assert "lesson-b-secret" not in message


def test_editing_a_memory_entry_records_which_entry_changed(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: Any
) -> None:
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "old-secret-lesson"})
    listed = _memory(client, auth_headers, aid)

    with caplog.at_level("INFO", logger=_STATE_MUTATION_LOGGER):
        r = client.put(
            f"/agents/{aid}/memory/0",
            json={"content": "new-secret-lesson", "expected_version": listed[0]["version"]},
            headers=auth_headers,
        )
    assert r.status_code == 200, r.text

    records = _mutation_records(caplog)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].state_mutation == {
        "op": "edit",
        "agent_id": aid,
        "scope": "shared",
        "namespace": "memory",
        "key": "log",
        "index": 0,
        "principal": "platform",
    }
    message = records[0].getMessage()
    assert "op=edit" in message and "index=0" in message
    assert "old-secret-lesson" not in message
    assert "new-secret-lesson" not in message


def test_a_refused_memory_delete_records_nothing(
    client: Any, auth_headers: dict[str, str], clean_db: None, caplog: Any
) -> None:
    # A stale version or an out-of-range index is refused before anything is
    # touched, so there is no mutation to attribute.
    aid = _agent(client, auth_headers)
    _remember(client, auth_headers, aid, {"content": "a"})
    listed = _memory(client, auth_headers, aid)

    with caplog.at_level("INFO", logger=_STATE_MUTATION_LOGGER):
        missing = client.delete(
            f"/agents/{aid}/memory/9",
            params={"expected_version": listed[0]["version"]},
            headers=auth_headers,
        )
        stale = client.delete(
            f"/agents/{aid}/memory/0",
            params={"expected_version": listed[0]["version"] + 5},
            headers=auth_headers,
        )
    assert missing.status_code == 404
    assert stale.status_code == 409
    assert _mutation_records(caplog) == []
