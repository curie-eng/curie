"""Publication candidate retention through authenticated HTTP and real PostgreSQL (#5428)."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier
from typing import Any

import pytest
from _migration_support import sql_dicts
from curie_api import publication_candidates as ledger
from curie_api.config import Settings, get_settings
from curie_api.models import PublicationCandidate
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ROOT = "/v1/internal/publication-candidates"
REPOSITORY = {
    "provider": "github",
    "host": "github.com",
    "project_id": "101",
    "path": "acme-corp/acme-bot",
}
BASE = "a" * 40
ARCHIVE_LIMIT = 268435456
RETENTION = timedelta(seconds=86400)
OBJECT_FIELDS = {"kind", "key", "size_bytes", "sha256"}
TITLE = "Fixture title marker-7f3a"
BODY = "Fixture body marker-91bc explains the change."
PATHS = ["src/marker-path-a.py", "docs/marker-path-b.md"]
OPERATIONS = (
    "context",
    "outcome",
    "reserve",
    "complete",
    "fail",
    "acquire-reader",
    "release-reader",
    "claim-collection",
    "finalize-collection",
)
CANDIDATE_TABLES = (
    "publication_candidate_groups",
    "publication_candidates",
    "publication_candidate_objects",
    "publication_candidate_readers",
)


@pytest.fixture
def candidates(monkeypatch: pytest.MonkeyPatch, clean_db: None) -> Iterator[None]:
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", raising=False)
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", str(ARCHIVE_LIMIT))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _worker() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def _post(client: Any, operation: str, body: dict[str, Any]) -> Any:
    return client.post(f"{ROOT}/{operation}", json=body, headers=_worker())


def _ok(response: Any) -> dict[str, Any]:
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _refused(response: Any, status: int, code: str) -> None:
    assert response.status_code == status, response.text
    assert response.json()["detail"] == {"code": f"publication_candidates.{code}"}


def _sql(statement: str, **params: Any) -> list[dict[str, Any]]:
    return sql_dicts(statement, params)


def _date(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _set_env(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    get_settings.cache_clear()


def _work_item(agent_id: str | None = None) -> dict[str, str]:
    agent_id = agent_id or str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    if not _sql("SELECT id FROM curie.agents WHERE id = :id", id=uuid.UUID(agent_id)):
        _sql(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            id=uuid.UUID(agent_id),
            name=f"acme-candidate-{uuid.uuid4().hex[:12]}",
        )
    _sql(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, agent_id, conversation_id, version, next_sequence, "
        "base_branch, base_source, base_commit) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', '101', "
        ":path, 202, :agent, :conversation, 7, 1, 'main', 'default', :base)",
        id=uuid.UUID(item_id),
        agent=uuid.UUID(agent_id),
        issue=str(uuid.uuid4().int),
        path=REPOSITORY["path"],
        conversation=f"slack:C0EXAMPLE1:{uuid.uuid4().int}.000100",
        base=BASE,
    )
    return {"agent_id": agent_id, "work_item_id": item_id}


def _request(item: dict[str, str], status: str = "running") -> dict[str, Any]:
    """Insert the WorkItem's next request: running, waiting (pending) or queued."""

    request_id = str(uuid.uuid4())
    sequence = _sql(
        "SELECT COALESCE(max(sequence), 0) + 1 AS value "
        "FROM curie.execution_requests WHERE work_item_id = :id",
        id=uuid.UUID(item["work_item_id"]),
    )[0]["value"]
    running = status == "running"
    _sql(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, version, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at, dispatch_generation, execution_attempts) VALUES "
        "(:id, :item, :sequence, :status, "
        "CASE WHEN :queued THEN NULL ELSE clock_timestamp() + interval '1 hour' END, "
        "CASE WHEN :running THEN clock_timestamp() - interval '1 minute' END, "
        "CASE WHEN :running THEN clock_timestamp() + interval '1 hour' END, "
        "4, 'fixture-worker', 3, clock_timestamp() + interval '1 hour', 2, "
        "CASE WHEN :running THEN 1 ELSE 0 END)",
        id=uuid.UUID(request_id),
        item=uuid.UUID(item["work_item_id"]),
        sequence=sequence,
        status=status,
        queued=status == "queued",
        running=running,
    )
    _sql(
        "UPDATE curie.work_items SET next_sequence = :sequence WHERE id = :id",
        id=uuid.UUID(item["work_item_id"]),
        sequence=sequence + 1,
    )
    return {
        **item,
        "request_id": request_id,
        "sequence": sequence,
        "runtime_epoch": 3,
        "dispatch_generation": 2,
    }


def _seed(agent_id: str | None = None) -> dict[str, Any]:
    return _request(_work_item(agent_id))


TERMINATE = (
    "UPDATE curie.execution_requests SET status = 'completed', terminal_cause = 'completed', "
    "terminal_at = clock_timestamp() - CAST(:ago AS integer) * interval '1 second' "
    "WHERE id = :id AND terminal_at IS NULL RETURNING terminal_at"
)


def _terminate(source: dict[str, Any], seconds_ago: int = 0) -> datetime:
    """Give a pending request its final disposition ``seconds_ago`` on the database clock.

    Set once, backdated, before any read could fix an expiry from it: a
    request's ``terminal_at`` is never rewritten afterwards.
    """

    rows = _sql(TERMINATE, id=uuid.UUID(source["request_id"]), ago=seconds_ago)
    assert len(rows) == 1, "a request's terminal_at is set exactly once"
    return _terminal_at(source)


def _terminal_at(source: dict[str, Any]) -> datetime:
    value = _sql(
        "SELECT terminal_at FROM curie.execution_requests WHERE id = :id",
        id=uuid.UUID(source["request_id"]),
    )[0]["terminal_at"]
    assert isinstance(value, datetime)
    return value


def _expiry(reservation: dict[str, Any]) -> dict[str, Any]:
    """The candidate's expiry, checked against its owner on the database clock."""

    [row] = _sql(
        "SELECT c.expires_at, "
        "c.expires_at = r.terminal_at + interval '86400 seconds' AS exact, "
        "c.expires_at <= clock_timestamp() AS past "
        "FROM curie.publication_candidates c "
        "JOIN curie.execution_requests r ON r.id = c.owner_request_id WHERE c.id = :id",
        id=uuid.UUID(reservation["candidate_id"]),
    )
    return row


def _authority(source: dict[str, Any]) -> dict[str, Any]:
    return {
        key: source[key]
        for key in ("work_item_id", "request_id", "runtime_epoch", "dispatch_generation")
    }


def _descriptor(kind: str, size: int, salt: str = "fixture") -> dict[str, Any]:
    return {
        "kind": kind,
        "size_bytes": size,
        "sha256": hashlib.sha256(f"{kind}:{salt}".encode()).hexdigest(),
    }


def _reserve_body(
    source: dict[str, Any],
    *,
    key: str | None = None,
    base: bool = True,
    patch_size: int = 30,
    base_size: int = 100,
    patch_limit: int = 900000,
    base_salt: str = "fixture",
) -> dict[str, Any]:
    key = key or uuid.uuid4().hex
    return {
        **_authority(source),
        "dedupe_key": key,
        "repository": dict(REPOSITORY),
        "base_commit": BASE,
        "target_branch": "main",
        "base_archive_limit_bytes": ARCHIVE_LIMIT,
        "patch_limit_bytes": patch_limit,
        "changed_paths": list(PATHS),
        "contains_workflow_files": False,
        "title": TITLE,
        "body": BODY,
        "objects": [
            *([_descriptor("base", base_size, base_salt)] if base else []),
            _descriptor("patch", patch_size, key),
        ],
        "capture_budget_seconds": 60,
    }


def _reserved(client: Any, source: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    body = _reserve_body(source, **kwargs)
    result = _ok(_post(client, "reserve", body))
    assert set(result) == {
        "candidate_id",
        "objects",
        "capture_deadline",
        "staging_expires_at",
        "replayed",
    }
    assert result["replayed"] is False
    expected = {obj["kind"]: obj for obj in body["objects"]}
    assert {obj["kind"] for obj in result["objects"]} == set(expected)
    for obj in result["objects"]:
        assert set(obj) == OBJECT_FIELDS
        assert {k: v for k, v in obj.items() if k != "key"} == expected[obj["kind"]]
        assert obj["key"].startswith("publication-candidates/")
        assert "://" not in obj["key"] and ".." not in obj["key"]
    result["body"] = body
    return result


def _completion(source: dict[str, Any], reservation: dict[str, Any]) -> dict[str, Any]:
    return {
        **_authority(source),
        "candidate_id": reservation["candidate_id"],
        "receipts": copy.deepcopy(reservation["objects"]),
    }


def _completed(client: Any, source: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    reservation = _reserved(client, source, **kwargs)
    result = _ok(_post(client, "complete", _completion(source, reservation)))
    assert set(result) == {"candidate_id", "status", "completed_at"}
    assert result["candidate_id"] == reservation["candidate_id"]
    assert result["status"] == "complete"
    return reservation


def _reader_body(
    source: dict[str, Any], reservation: dict[str, Any], holder: str = "fixture-reader"
) -> dict[str, Any]:
    return {
        "work_item_id": source["work_item_id"],
        "candidate_id": reservation["candidate_id"],
        "holder_id": holder,
    }


def _reader(
    client: Any, source: dict[str, Any], reservation: dict[str, Any], holder: str | None = None
) -> dict[str, Any]:
    holder = holder or f"reader-{uuid.uuid4().hex[:8]}"
    result = _ok(_post(client, "acquire-reader", _reader_body(source, reservation, holder)))
    assert set(result) == {"lease_id", "candidate_id", "holder_id", "expires_at", "objects"}
    assert result["candidate_id"] == reservation["candidate_id"]
    assert sorted(obj["kind"] for obj in result["objects"]) == ["base", "patch"]
    patch = next(obj for obj in reservation["objects"] if obj["kind"] == "patch")
    assert next(obj for obj in result["objects"] if obj["kind"] == "patch") == patch
    return result


def _release(client: Any, reader: dict[str, Any]) -> None:
    _ok(
        _post(
            client,
            "release-reader",
            {"lease_id": reader["lease_id"], "holder_id": reader["holder_id"]},
        )
    )


def _readable(client: Any, source: dict[str, Any], reservation: dict[str, Any]) -> None:
    _release(client, _reader(client, source, reservation))


def _claims(client: Any, collector: str = "fixture-collector") -> list[dict[str, Any]]:
    result = _ok(_post(client, "claim-collection", {"collector_id": collector, "limit": 100}))
    assert set(result) == {"claims"}
    for claim in result["claims"]:
        assert set(claim) == {"object_id", "key", "claim_id", "version", "claimed_until"}
    return list(result["claims"])


def _finalize(client: Any, claim: dict[str, Any], collector: str = "fixture-collector") -> Any:
    return _post(
        client,
        "finalize-collection",
        {
            "collector_id": collector,
            "object_id": claim["object_id"],
            "claim_id": claim["claim_id"],
            "version": claim["version"],
        },
    )


def _drain(client: Any) -> None:
    for _ in range(4):
        for claim in _claims(client):
            _ok(_finalize(client, claim))


def _snapshot() -> dict[str, Any]:
    result = {}
    for table in CANDIDATE_TABLES:
        result[table] = _sql(
            f"SELECT COALESCE(jsonb_agg(value ORDER BY value::text), '[]'::jsonb) AS rows "
            f"FROM (SELECT to_jsonb(t) AS value FROM curie.{table} t) source"
        )[0]["rows"]
    return result


def _outcome(source: dict[str, Any]) -> str | None:
    rows = _sql(
        "SELECT code FROM curie.publication_candidate_outcomes WHERE work_item_id = :id",
        id=uuid.UUID(source["work_item_id"]),
    )
    return rows[0]["code"] if rows else None


def _candidate(reservation: dict[str, Any]) -> dict[str, Any]:
    return _sql(
        "SELECT * FROM curie.publication_candidates WHERE id = :id",
        id=uuid.UUID(reservation["candidate_id"]),
    )[0]


def _count(table: str) -> int:
    value = _sql(f"SELECT count(*) AS value FROM curie.{table}")[0]["value"]
    assert isinstance(value, int)
    return value


def _listing(client: Any, auth_headers: Any, source: dict[str, Any]) -> dict[str, Any]:
    response = client.get(
        f"/work-items/{source['work_item_id']}/publication-candidates", headers=auth_headers
    )
    return _ok(response)


def _transfer(source: dict[str, Any], reservation: dict[str, Any], target: str) -> str | None:
    """Call the domain transfer in its own transaction; the refusal code, or None."""

    async def run() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                try:
                    row = await ledger.transfer_ownership(
                        session,
                        work_item_id=uuid.UUID(source["work_item_id"]),
                        candidate_id=uuid.UUID(reservation["candidate_id"]),
                        to_request_id=uuid.UUID(target),
                    )
                except ledger.CandidateRefusal as refusal:
                    await session.rollback()
                    return refusal.code
                assert row.owner_request_id == uuid.UUID(target)
                await session.commit()
                return None
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _race(client: Any, requests: list[tuple[str, dict[str, Any]]]) -> list[Any]:
    barrier = Barrier(len(requests))

    def call(request: tuple[str, dict[str, Any]]) -> Any:
        barrier.wait(timeout=10)
        return _post(client, *request)

    with ThreadPoolExecutor(max_workers=len(requests)) as executor:
        futures = [executor.submit(call, request) for request in requests]
        return [future.result(timeout=30) for future in futures]


def test_candidate_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", raising=False)
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", raising=False)
    defaults = Settings(_env_file=None)
    assert defaults.publication_candidates_enabled is False
    assert defaults.publication_candidate_max_bytes == 1073741824
    for value in ("0", "-1", "not-a-number"):
        monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", value)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", "4321")
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    configured = Settings(_env_file=None)
    assert configured.publication_candidates_enabled is True
    assert configured.publication_candidate_max_bytes == 4321


def test_candidate_routes_require_the_worker_token(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    source = _seed()
    body = _reserve_body(source)
    before = _snapshot()
    for operation in OPERATIONS:
        for headers in (
            {},
            {"X-Curie-Worker-Token": "wrong-token"},
            auth_headers,
            {"X-Curie-Worker-Token": get_settings().api_key},
        ):
            response = client.post(f"{ROOT}/{operation}", json=body, headers=headers)
            assert response.status_code == 401, response.text
    assert _snapshot() == before
    assert _outcome(source) is None
    # Liveness: the worker token opens the capture context.
    assert _ok(_post(client, "context", _authority(source))) == {
        "repository": REPOSITORY,
        "base_commit": BASE,
        "target_branch": "main",
    }


def test_reserve_complete_and_replay_answer_one_candidate(candidates: None, client: Any) -> None:
    source = _seed()
    body = _reserve_body(source, key="same-capture")
    first = _ok(_post(client, "reserve", body))
    assert first["replayed"] is False
    assert _outcome(source) == "reserved"
    replay = _ok(_post(client, "reserve", {**body, "capture_budget_seconds": 30}))
    assert replay == {**first, "replayed": True}
    assert _count("publication_candidates") == 1

    for change in (
        {"title": "A different title"},
        {"changed_paths": ["src/other.py"]},
        {"objects": [body["objects"][0], _descriptor("patch", 31, "same-capture")]},
    ):
        before = _snapshot()
        _refused(_post(client, "reserve", {**body, **change}), 409, "idempotency_conflict")
        assert _snapshot() == before
    assert _count("publication_candidates") == 1

    completion = _completion(source, first)
    for invalid in (
        {**completion, "request_id": str(uuid.uuid4())},
        {**completion, "runtime_epoch": 99},
        {**completion, "receipts": completion["receipts"][:-1]},
    ):
        before = _snapshot()
        response = _post(client, "complete", invalid)
        assert response.status_code == 409, response.text
        assert _snapshot() == before
    done = _ok(_post(client, "complete", completion))
    assert done["status"] == "complete"
    assert _ok(_post(client, "complete", completion)) == done
    assert _outcome(source) == "complete"
    after = _ok(_post(client, "reserve", body))
    assert after == {**first, "replayed": True}
    _refused(
        _post(client, "reserve", {**body, "title": "A different title"}),
        409,
        "idempotency_conflict",
    )
    assert _count("publication_candidates") == 1
    row = _candidate(first)
    assert row["status"] == "complete"
    assert row["expires_at"] is None, "a pending owner fixes no expiry"
    assert str(row["origin_request_id"]) == str(row["owner_request_id"]) == source["request_id"]

    # A writer never withdraws an acknowledged candidate.
    before = _snapshot()
    _refused(
        _post(
            client,
            "fail",
            {
                **_authority(source),
                "candidate_id": first["candidate_id"],
                "reason": "upload_failed",
            },
        ),
        409,
        "candidate_state",
    )
    assert _snapshot() == before
    _readable(client, source, first)


def test_fail_withdraws_only_a_staging_candidate(candidates: None, client: Any) -> None:
    source = _seed()
    staging = _reserved(client, source)
    failure = {
        **_authority(source),
        "candidate_id": staging["candidate_id"],
        "reason": "upload_failed",
    }
    assert _ok(_post(client, "fail", failure)) == {
        "candidate_id": staging["candidate_id"],
        "status": "tombstoned",
    }
    assert _ok(_post(client, "fail", failure))["status"] == "tombstoned"
    assert _outcome(source) == "upload_failed"
    _refused(
        _post(client, "complete", _completion(source, staging)), 409, "capture_deadline_or_state"
    )
    _drain(client)
    assert _count("publication_candidate_groups") == 0
    # Liveness: a fresh capture of the same WorkItem starts a new group.
    _completed(client, source)


def test_replay_without_the_base_declaration_answers_the_same_candidate(
    candidates: None, client: Any
) -> None:
    source = _seed()
    without_base = _reserve_body(source, key="base-later", base=False)
    before = _snapshot()
    _refused(_post(client, "reserve", without_base), 409, "base_required")
    assert _snapshot() == before
    assert _outcome(source) is None
    with_base = _reserve_body(source, key="base-later")
    first = _ok(_post(client, "reserve", with_base))
    assert first["replayed"] is False
    assert sorted(obj["kind"] for obj in first["objects"]) == ["base", "patch"]
    _ok(_post(client, "complete", _completion(source, first)))
    for body in (without_base, with_base):
        replay = _ok(_post(client, "reserve", body))
        assert replay == {**first, "replayed": True}
    assert _count("publication_candidates") == 1
    # The rest of the content still binds the identity.
    changed = {**without_base, "objects": [_descriptor("patch", 31, "base-later")]}
    before = _snapshot()
    _refused(_post(client, "reserve", changed), 409, "idempotency_conflict")
    assert _snapshot() == before


def test_outcome_needs_only_the_capture_identity(candidates: None, client: Any) -> None:
    source = _seed()
    _sql(
        "UPDATE curie.execution_requests SET "
        "runtime_heartbeat_expires_at = clock_timestamp() - interval '1 minute' WHERE id = :id",
        id=uuid.UUID(source["request_id"]),
    )
    # The capture is no longer live, so it cannot start ...
    _refused(_post(client, "context", _authority(source)), 409, "expired_source")
    # ... but it still records why it ended.
    outcome = {**_authority(source), "reason": "capture_timeout"}
    assert _ok(_post(client, "outcome", outcome)) == {"recorded": True}
    assert _outcome(source) == "capture_timeout"
    _terminate(source)
    assert _ok(_post(client, "outcome", {**outcome, "reason": "capture_failed"})) == {
        "recorded": True
    }
    assert _outcome(source) == "capture_failed"
    # A later admitted request of the same WorkItem does not take the record away.
    _request(source, "waiting")
    assert _ok(_post(client, "outcome", outcome)) == {"recorded": True}
    assert _outcome(source) == "capture_timeout"
    # Identity still binds: another epoch, generation or request records nothing.
    for invalid in (
        {**outcome, "runtime_epoch": 2},
        {**outcome, "dispatch_generation": 1},
        {**outcome, "request_id": str(uuid.uuid4())},
    ):
        _refused(_post(client, "outcome", invalid), 409, "source_identity")
        assert _outcome(source) == "capture_timeout"


def test_expiry_is_fixed_exactly_from_the_owner_final_disposition(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    source = _seed()
    reservation = _completed(client, source)
    # Move every candidate timestamp a month back: the database clock is far
    # past them, yet a pending owner keeps the candidate.
    _sql(
        "UPDATE curie.publication_candidates SET created_at = created_at - interval '30 days', "
        "completed_at = completed_at - interval '30 days', "
        "capture_deadline = capture_deadline - interval '30 days', "
        "staging_expires_at = staging_expires_at - interval '30 days' WHERE id = :id",
        id=uuid.UUID(reservation["candidate_id"]),
    )
    _readable(client, source, reservation)
    assert _claims(client) == []
    assert _candidate(reservation)["expires_at"] is None
    listed = _listing(client, auth_headers, source)["candidates"][0]
    assert listed["expires_at"] is None and listed["final_disposition_at"] is None
    assert listed["available"] is True

    terminal_at = _terminate(source, seconds_ago=3600)
    reader = _reader(client, source, reservation)
    fixed = _candidate(reservation)["expires_at"]
    assert fixed == terminal_at + RETENTION
    check = _expiry(reservation)
    assert check["exact"] is True and check["past"] is False
    listed = _listing(client, auth_headers, source)["candidates"][0]
    assert _date(listed["expires_at"]) == terminal_at + RETENTION
    assert _date(listed["final_disposition_at"]) == terminal_at
    assert listed["available"] is True
    for _ in range(2):
        _release(client, _reader(client, source, reservation))
        assert _candidate(reservation)["expires_at"] == fixed, "a read never moves expiry"
    _release(client, reader)

    # An owner whose disposition is already more than 86400 s old.
    expired_source = _seed()
    expired = _completed(client, expired_source)
    terminal_at = _terminate(expired_source, seconds_ago=86401)
    before_version = _candidate(expired)["version"]
    _refused(
        _post(client, "acquire-reader", _reader_body(expired_source, expired)),
        409,
        "candidate_unavailable",
    )
    row = _candidate(expired)
    assert row["version"] == before_version and row["expires_at"] is None, (
        "a refused read writes nothing"
    )
    claims = _claims(client)
    row = _candidate(expired)
    assert row["expires_at"] == terminal_at + RETENTION
    assert row["status"] == "tombstoned"
    check = _expiry(expired)
    assert check["exact"] is True and check["past"] is True
    # A second read of either kind leaves the fixed expiry where it is.
    _refused(
        _post(client, "acquire-reader", _reader_body(expired_source, expired)),
        409,
        "candidate_unavailable",
    )
    assert _claims(client) == claims
    assert _expiry(expired) == check
    patch_key = next(obj["key"] for obj in expired["objects"] if obj["kind"] == "patch")
    assert [claim["key"] for claim in claims] == [patch_key]
    listed = _listing(client, auth_headers, expired_source)["candidates"][0]
    assert listed["available"] is False and listed["status"] == "tombstoned"
    # The neighbour whose expiry is still ahead is untouched and readable.
    assert _candidate(reservation)["status"] == "complete"
    _readable(client, source, reservation)


def test_transfer_clears_expiry_and_fixes_it_from_the_successor(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    origin = _seed()
    reservation = _completed(client, origin)
    _terminate(origin, seconds_ago=60)
    successor = _request(origin, "running")
    assert _transfer(origin, reservation, successor["request_id"]) is None
    row = _candidate(reservation)
    assert str(row["origin_request_id"]) == origin["request_id"]
    assert str(row["owner_request_id"]) == successor["request_id"]
    assert str(row["previous_owner_request_id"]) == origin["request_id"]
    assert row["owner_transferred_at"] is not None
    assert row["expires_at"] is None

    _readable(client, origin, reservation)
    assert _claims(client) == []
    row = _candidate(reservation)
    assert row["status"] == "complete" and row["expires_at"] is None
    assert str(row["origin_request_id"]) == origin["request_id"]
    listed = _listing(client, auth_headers, origin)["candidates"][0]
    assert listed["origin_request_id"] == origin["request_id"]
    assert listed["owner_request_id"] == successor["request_id"]
    assert listed["previous_owner_request_id"] == origin["request_id"]
    assert listed["owner_transferred_at"] is not None
    assert listed["final_disposition_at"] is None and listed["available"] is True

    terminal_at = _terminate(successor, seconds_ago=10)
    _readable(client, origin, reservation)
    assert _candidate(reservation)["expires_at"] == terminal_at + RETENTION
    check = _expiry(reservation)
    assert check["exact"] is True and check["past"] is False
    _readable(client, origin, reservation)
    assert _expiry(reservation) == check


def test_transfer_keeps_a_candidate_past_its_origin_expiry(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    """A stale owner observation never fixes an expiry from the previous owner."""

    origin = _seed()
    reservation = _completed(client, origin)
    successor = _request(origin, "queued")
    candidate_id = uuid.UUID(reservation["candidate_id"])
    origin_id = uuid.UUID(origin["request_id"])
    age = 3 * 86400

    async def stale_fix() -> tuple[Any, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with (
                AsyncSession(engine, expire_on_commit=False) as stale,
                AsyncSession(engine, expire_on_commit=False) as other,
            ):
                # The stale reader observes the origin as owner, then the
                # candidate moves to the pending successor and the origin turns
                # terminal far past its retention window, before it acts.
                row = await stale.get(PublicationCandidate, candidate_id)
                assert row is not None and row.owner_request_id == origin_id
                moved = await ledger.transfer_ownership(
                    other,
                    work_item_id=uuid.UUID(origin["work_item_id"]),
                    candidate_id=candidate_id,
                    to_request_id=uuid.UUID(successor["request_id"]),
                )
                assert moved.expires_at is None
                assert (
                    await other.execute(text(TERMINATE), {"id": origin_id, "ago": age})
                ).one_or_none() is not None
                await other.commit()
                await ledger._fix_expiry(stale, row)
                await stale.commit()
                return row.owner_request_id, row.expires_at
        finally:
            await engine.dispose()

    owner, expires_at = asyncio.run(stale_fix())
    assert owner == uuid.UUID(successor["request_id"]) and expires_at is None
    assert _sql(
        "SELECT terminal_at + interval '86400 seconds' <= clock_timestamp() AS past "
        "FROM curie.execution_requests WHERE id = :id",
        id=origin_id,
    ) == [{"past": True}]

    assert _claims(client) == []
    row = _candidate(reservation)
    assert row["status"] == "complete" and row["expires_at"] is None
    assert str(row["origin_request_id"]) == origin["request_id"]
    assert str(row["owner_request_id"]) == successor["request_id"]
    _readable(client, origin, reservation)
    listed = _listing(client, auth_headers, origin)["candidates"][0]
    assert listed["origin_request_id"] == origin["request_id"]
    assert listed["owner_request_id"] == successor["request_id"]
    assert listed["previous_owner_request_id"] == origin["request_id"]
    assert listed["final_disposition_at"] is None and listed["available"] is True
    assert listed["status"] == "complete"
    assert _candidate(reservation)["expires_at"] is None


def test_transfer_refusals_leave_ownership_unchanged(candidates: None, client: Any) -> None:
    # Lower and equal sequence: an older queued request and the owner itself.
    item = _work_item()
    older = _request(item, "queued")
    owner = _request(item, "running")
    reservation = _completed(client, owner)
    before = _snapshot()
    assert _transfer(owner, reservation, older["request_id"]) == (
        "publication_candidates.transfer_target_sequence"
    )
    assert _transfer(owner, reservation, owner["request_id"]) == (
        "publication_candidates.transfer_target_sequence"
    )
    # Another WorkItem's request.
    other = _seed()
    assert _transfer(owner, reservation, other["request_id"]) == (
        "publication_candidates.transfer_target_identity"
    )
    # A terminal target with a greater sequence.
    _terminate(owner, seconds_ago=60)
    finished = _request(item, "running")
    _terminate(finished, seconds_ago=30)
    assert _transfer(owner, reservation, finished["request_id"]) == (
        "publication_candidates.transfer_target_terminal"
    )
    assert _snapshot() == before
    # Liveness: a pending successor with a greater sequence is accepted.
    pending = _request(item, "queued")
    assert _transfer(owner, reservation, pending["request_id"]) is None
    assert str(_candidate(reservation)["owner_request_id"]) == pending["request_id"]

    # An expired candidate cannot be transferred.
    expired_owner = _seed()
    expired = _completed(client, expired_owner)
    _terminate(expired_owner, seconds_ago=86401)
    successor = _request(expired_owner, "queued")
    assert _transfer(expired_owner, expired, successor["request_id"]) == (
        "publication_candidates.candidate_unavailable"
    )
    assert str(_candidate(expired)["owner_request_id"]) == expired_owner["request_id"]


def test_refusals_are_recorded_and_preserve_existing_candidates(
    candidates: None, client: Any, auth_headers: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _seed()
    first = _completed(client, source)

    def refused(body: dict[str, Any], status: int, code: str, *, recorded: bool) -> None:
        before = _snapshot()
        outcome = _outcome(source)
        _refused(_post(client, "reserve", body), status, code)
        assert _snapshot() == before
        if recorded:
            assert _outcome(source) == code
            assert _listing(client, auth_headers, source)["capture_outcome"] == code
        else:
            assert _outcome(source) == outcome
        _readable(client, source, first)

    # Oversized patch: above the 900000 ceiling, and above the declared limit.
    refused(
        _reserve_body(source, base=False, patch_size=900001), 413, "patch_too_large", recorded=True
    )
    refused(
        _reserve_body(source, base=False, patch_size=1001, patch_limit=1000),
        413,
        "patch_too_large",
        recorded=True,
    )
    refused(
        _reserve_body(source, base_size=ARCHIVE_LIMIT + 1), 413, "base_too_large", recorded=True
    )
    # A retained base with another digest conflicts; the same digest is a redundant
    # declaration.
    refused(
        _reserve_body(source, base_salt="other-base"), 409, "retained_base_conflict", recorded=True
    )
    refused(_reserve_body(source), 409, "retained_base_declaration", recorded=False)
    # Wrong identity: another request, a stale epoch, another WorkItem's repository.
    refused(
        {**_reserve_body(source, base=False), "request_id": str(uuid.uuid4())},
        409,
        "source_identity",
        recorded=False,
    )
    refused(
        {**_reserve_body(source, base=False), "runtime_epoch": 2},
        409,
        "source_identity",
        recorded=False,
    )
    refused(
        {
            **_reserve_body(source, base=False),
            "repository": {**REPOSITORY, "path": "acme-corp/other-repo"},
        },
        409,
        "workspace_identity",
        recorded=False,
    )
    # Liveness: the neighbouring valid inputs are accepted (exactly at the limits).
    second = _completed(client, source, base=False, patch_size=1000, patch_limit=1000)
    third = _completed(client, source, base=False, patch_size=900000)
    assert _count("publication_candidates") == 3
    # A fourth candidate.
    refused(_reserve_body(source, base=False), 429, "work_item_capacity", recorded=True)
    for kept in (second, third):
        _readable(client, source, kept)
    listing = _listing(client, auth_headers, source)
    assert [entry["candidate_id"] for entry in listing["candidates"]] == [
        first["candidate_id"],
        second["candidate_id"],
        third["candidate_id"],
    ]
    assert all(entry["available"] for entry in listing["candidates"])

    # Installation capacity: an exact fit is accepted, one byte more is refused.
    used = _sql("SELECT sum(size_bytes) AS value FROM curie.publication_candidate_objects")[0][
        "value"
    ]
    _set_env(monkeypatch, "CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", str(used + 130))
    full = _seed()
    _refused(
        _post(client, "reserve", _reserve_body(full, patch_size=31)), 429, "installation_capacity"
    )
    assert _outcome(full) == "installation_capacity"
    assert _listing(client, auth_headers, full)["capture_outcome"] == "installation_capacity"
    assert not _sql(
        "SELECT 1 FROM curie.publication_candidate_groups WHERE work_item_id = :id",
        id=uuid.UUID(full["work_item_id"]),
    )
    _readable(client, source, first)
    _completed(client, full, patch_size=30)


def test_retained_work_item_capacity_refuses_a_new_group(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    source = _seed()
    first = _completed(client, source)
    _sql(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, agent_id, conversation_id, base_branch, base_source, "
        "base_commit) SELECT gen_random_uuid(), 'github', 'github.com', '101', "
        "(800000 + n)::text, 'github', 'github.com', '101', :path, 202, :agent, "
        "'candidate-count-seed', 'main', 'default', :base FROM generate_series(1, 9999) n",
        path=REPOSITORY["path"],
        agent=uuid.UUID(source["agent_id"]),
        base=BASE,
    )
    _sql(
        "INSERT INTO curie.publication_candidate_groups "
        "(work_item_id, repository_provider, repository_host, repository_project_id, "
        "repository_path, base_commit, target_branch, base_sha256) SELECT id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "base_commit, base_branch, :digest FROM curie.work_items "
        "WHERE conversation_id = 'candidate-count-seed'",
        digest="b" * 64,
    )
    assert _count("publication_candidate_groups") == 10000
    newcomer = _seed()
    before = _snapshot()
    _refused(_post(client, "reserve", _reserve_body(newcomer)), 429, "retained_work_item_capacity")
    assert _snapshot() == before
    assert _outcome(newcomer) == "retained_work_item_capacity"
    assert _listing(client, auth_headers, newcomer)["capture_outcome"] == (
        "retained_work_item_capacity"
    )
    _readable(client, source, first)
    # Liveness: a WorkItem that already holds a group still captures.
    _completed(client, source, base=False)


def test_disabled_ledger_refuses_capture_and_readers_but_drains(
    candidates: None, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _seed()
    first = _completed(client, source)
    staging = _reserved(client, source, base=False)
    _set_env(monkeypatch, "CURIE_PUBLICATION_CANDIDATES_ENABLED", "false")
    before = _snapshot()
    for operation, body in (
        ("context", _authority(source)),
        ("outcome", {**_authority(source), "reason": "capture_failed"}),
        ("reserve", _reserve_body(source, base=False)),
        ("acquire-reader", _reader_body(source, first)),
    ):
        _refused(_post(client, operation, body), 503, "disabled")
    assert _snapshot() == before
    # A capture already reserved still finishes, and collection still drains.
    _ok(_post(client, "complete", _completion(source, staging)))
    _sql(
        "UPDATE curie.publication_candidates SET expires_at = clock_timestamp() - "
        "interval '1 second' WHERE work_item_id = :id",
        id=uuid.UUID(source["work_item_id"]),
    )
    _drain(client)
    assert _count("publication_candidate_groups") == 0
    assert _count("publication_candidate_objects") == 0
    _set_env(monkeypatch, "CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    _completed(client, source)


def test_concurrent_reserves_never_over_admit_the_installation(
    candidates: None, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_env(monkeypatch, "CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", "200")
    sources = [_seed(), _seed()]
    bodies = [_reserve_body(source) for source in sources]
    answers = _race(client, [("reserve", body) for body in bodies])
    assert sorted(answer.status_code for answer in answers) == [200, 429]
    loser = next(i for i, answer in enumerate(answers) if answer.status_code == 429)
    _refused(answers[loser], 429, "installation_capacity")
    assert (
        _sql("SELECT sum(size_bytes) AS value FROM curie.publication_candidate_objects")[0]["value"]
        == 130
    )
    assert _outcome(sources[loser]) == "installation_capacity"
    assert _count("publication_candidate_groups") == 1


@pytest.mark.parametrize("unpin", ("release", "expire"))
def test_reader_lease_pins_a_tombstoned_patch_against_collection(
    candidates: None, client: Any, unpin: str
) -> None:
    source = _seed()
    reservation = _completed(client, source)
    reader = _reader(client, source, reservation)
    _terminate(source, seconds_ago=86401)
    assert _claims(client) == [], "a leased candidate's patch is never claimed"
    assert _candidate(reservation)["status"] == "tombstoned"
    assert _count("publication_candidate_readers") == 1
    _refused(
        _post(client, "acquire-reader", _reader_body(source, reservation, "late")),
        409,
        "candidate_unavailable",
    )
    if unpin == "release":
        _release(client, reader)
    else:
        _sql(
            "UPDATE curie.publication_candidate_readers SET expires_at = clock_timestamp() - "
            "interval '1 second' WHERE id = :id",
            id=uuid.UUID(reader["lease_id"]),
        )
    claims = _claims(client)
    patch_key = next(obj["key"] for obj in reservation["objects"] if obj["kind"] == "patch")
    assert [claim["key"] for claim in claims] == [patch_key]


def test_collector_killed_after_delete_is_finished_by_the_next_claim(
    candidates: None, client: Any
) -> None:
    source = _seed()
    reservation = _completed(client, source)
    _terminate(source, seconds_ago=86401)
    crashed = _claims(client, "crashed-collector")
    assert len(crashed) == 1
    # The crashed collector deleted the object but never finalized; its claim lapses.
    assert _claims(client, "replacement-collector") == []
    _sql(
        "UPDATE curie.publication_candidate_objects SET claimed_until = clock_timestamp() - "
        "interval '1 second' WHERE claim_id IS NOT NULL"
    )
    resumed = _claims(client, "replacement-collector")
    assert [claim["object_id"] for claim in resumed] == [crashed[0]["object_id"]]
    assert resumed[0]["claim_id"] != crashed[0]["claim_id"]
    assert resumed[0]["version"] > crashed[0]["version"]
    before = _snapshot()
    _refused(_finalize(client, crashed[0], "crashed-collector"), 409, "collection_claim_conflict")
    assert _snapshot() == before
    _ok(_finalize(client, resumed[0], "replacement-collector"))
    assert not _sql(
        "SELECT 1 FROM curie.publication_candidates WHERE id = :id",
        id=uuid.UUID(reservation["candidate_id"]),
    )
    assert _count("publication_candidate_groups") == 1, "the base outlives its last patch"
    _ok(_finalize(client, resumed[0], "replacement-collector"))
    base_claims = _claims(client, "replacement-collector")
    assert [claim["key"] for claim in base_claims] == [
        next(obj["key"] for obj in reservation["objects"] if obj["kind"] == "base")
    ]
    _ok(_finalize(client, base_claims[0], "replacement-collector"))
    assert _count("publication_candidate_groups") == 0
    assert _count("publication_candidate_objects") == 0


def test_metadata_route_reveals_no_private_content(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    source = _seed()
    reservation = _completed(client, source)
    path = f"/work-items/{source['work_item_id']}/publication-candidates"
    for headers in ({}, {"X-API-Key": "invalid"}, _worker()):
        assert client.get(path, headers=headers).status_code == 401
    unknown = client.get(f"/work-items/{uuid.uuid4()}/publication-candidates", headers=auth_headers)
    _refused(unknown, 404, "work_item_not_found")
    response = client.get(path, headers=auth_headers)
    listing = _ok(response)
    assert set(listing) == {"work_item_id", "capture_outcome", "capture_outcome_at", "candidates"}
    assert listing["capture_outcome"] == "complete"
    [entry] = listing["candidates"]
    assert set(entry) == {
        "candidate_id",
        "origin_request_id",
        "owner_request_id",
        "previous_owner_request_id",
        "owner_transferred_at",
        "runtime_epoch",
        "status",
        "created_at",
        "completed_at",
        "final_disposition_at",
        "expires_at",
        "available",
        "changed_path_count",
        "patch",
        "base",
    }
    assert entry["changed_path_count"] == len(PATHS)
    patch = next(obj for obj in reservation["objects"] if obj["kind"] == "patch")
    base = next(obj for obj in reservation["objects"] if obj["kind"] == "base")
    assert entry["patch"] == {"size_bytes": patch["size_bytes"], "sha256": patch["sha256"]}
    assert entry["base"] == {
        "size_bytes": base["size_bytes"],
        "sha256": base["sha256"],
        "base_commit": BASE,
    }
    text = response.text
    for secret in (
        TITLE,
        BODY,
        "marker",
        *PATHS,
        patch["key"],
        base["key"],
        "publication-candidates/",
        "http",
    ):
        assert secret not in text


def test_agent_deletion_conflicts_until_collection_removes_the_group(
    candidates: None, client: Any, auth_headers: Any
) -> None:
    source = _seed()
    _completed(client, source)
    before = _snapshot()
    response = client.delete(f"/agents/{source['agent_id']}", headers=auth_headers)
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == {"code": "publication_candidates.retained_objects"}
    assert _snapshot() == before
    _terminate(source, seconds_ago=86401)
    _drain(client)
    assert _count("publication_candidate_groups") == 0
    assert _outcome(source) == "complete", "the outcome row is metadata, not a retained object"
    response = client.delete(f"/agents/{source['agent_id']}", headers=auth_headers)
    assert response.status_code == 204, response.text
    assert not _sql("SELECT 1 FROM curie.agents WHERE id = :id", id=uuid.UUID(source["agent_id"]))
    assert _outcome(source) is None
