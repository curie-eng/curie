"""Recovery metadata is exercised through authenticated HTTP and real PostgreSQL."""

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
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api.config import Settings, get_settings
from curie_api.main import create_app
from curie_internal.sandbox_token import mint
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = "/v1/internal/workspace-recovery"
REPOSITORY = {
    "provider": "github",
    "host": "github.com",
    "project_id": "101",
    "path": "acme-corp/acme-bot",
}
BASE = "a" * 40
ARCHIVE_LIMIT = 268435456
OBJECT_FIELDS = {"kind", "key", "size_bytes", "sha256"}
OPERATIONS = (
    "reserve",
    "complete",
    "fail",
    "acquire-reader",
    "release-reader",
    "claim-collection",
    "finalize-collection",
)


@pytest.fixture
def recovery(monkeypatch: pytest.MonkeyPatch, clean_db: None) -> Iterator[None]:
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", "10000000")
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


def _refused(response: Any, status: int = 409) -> None:
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"].startswith("workspace_recovery.")


def _routes_exist(client: Any) -> None:
    response = client.post(f"{ROOT}/reserve", json={})
    assert response.status_code == 401, response.text


def _sql(statement: str, **params: Any) -> list[dict[str, Any]]:
    return sql_dicts(statement, params)


def _seed(*, agent_id: str | None = None, item_id: str | None = None) -> dict[str, Any]:
    agent_id = agent_id or str(uuid.uuid4())
    item_id = item_id or str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    if not _sql("SELECT id FROM curie.agents WHERE id = :id", id=uuid.UUID(agent_id)):
        _sql(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            id=uuid.UUID(agent_id),
            name=f"acme-recovery-{uuid.uuid4().hex[:12]}",
        )
    existing = _sql("SELECT id FROM curie.work_items WHERE id = :id", id=uuid.UUID(item_id))
    if not existing:
        _sql(
            "INSERT INTO curie.work_items "
            "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
            "code_host_kind, code_host_host, repository_project_id, repository_path, "
            "code_host_installation_id, agent_id, conversation_id, version, next_sequence, "
            "base_branch, base_source, base_commit) VALUES "
            "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', '101', "
            ":path, 202, :agent, :conversation, 7, 2, 'main', 'default', :base)",
            id=uuid.UUID(item_id),
            agent=uuid.UUID(agent_id),
            issue=str(uuid.uuid4().int),
            path=REPOSITORY["path"],
            conversation=f"slack:C0EXAMPLE1:{uuid.uuid4().int}.000100",
            base=BASE,
        )
    sequence = _sql(
        "SELECT COALESCE(max(sequence), 0) + 1 AS value "
        "FROM curie.execution_requests WHERE work_item_id = :id",
        id=uuid.UUID(item_id),
    )[0]["value"]
    _sql(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, terminal_at, terminal_cause, version, runtime_owner, "
        "runtime_epoch, runtime_heartbeat_expires_at, dispatch_generation, "
        "execution_attempts) VALUES "
        "(:id, :item, :sequence, 'failed', clock_timestamp() + interval '1 hour', "
        "clock_timestamp() - interval '1 minute', clock_timestamp() + interval '1 hour', "
        "clock_timestamp() - interval '1 second', 'runner_failed', 4, 'fixture-worker', "
        "3, clock_timestamp() + interval '1 hour', 2, 1)",
        id=uuid.UUID(request_id),
        item=uuid.UUID(item_id),
        sequence=sequence,
    )
    _sql(
        "UPDATE curie.work_items SET next_sequence = :sequence WHERE id = :id",
        id=uuid.UUID(item_id),
        sequence=sequence + 1,
    )
    return {
        "agent_id": agent_id,
        "work_item_id": item_id,
        "source_request_id": request_id,
        "runtime_epoch": 3,
        "dispatch_generation": 2,
    }


def _authority(source: dict[str, Any]) -> dict[str, Any]:
    return {key: source[key] for key in (
        "work_item_id", "source_request_id", "runtime_epoch", "dispatch_generation"
    )}


def _descriptor(kind: str, size: int, salt: str = "fixture") -> dict[str, Any]:
    return {
        "kind": kind,
        "size_bytes": size,
        "sha256": hashlib.sha256(f"{kind}:{salt}".encode()).hexdigest(),
    }


def _reserve_body(
    source: dict[str, Any],
    *,
    reuse: bool = False,
    key: str | None = None,
    sizes: tuple[int, int, int] = (100, 30, 20),
) -> dict[str, Any]:
    key = key or uuid.uuid4().hex
    return {
        **_authority(source),
        "idempotency_key": key,
        "repository": dict(REPOSITORY),
        "base_commit": BASE,
        "target_branch": "main",
        "base_archive_limit_bytes": ARCHIVE_LIMIT,
        "objects": [
            *([] if reuse else [_descriptor("base", sizes[0])]),
            _descriptor("patch", sizes[1], key),
            _descriptor("manifest", sizes[2], key),
        ],
    }


def _reserved(client: Any, source: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    body = _reserve_body(source, **kwargs)
    result = _ok(_post(client, "reserve", body))
    assert set(result) == {
        "checkpoint_id", "objects", "capture_deadline", "staging_expires_at"
    }
    uuid.UUID(result["checkpoint_id"])
    assert len(result["objects"]) == len(body["objects"])
    expected = {obj["kind"]: obj for obj in body["objects"]}
    assert {obj["kind"] for obj in result["objects"]} == set(expected)
    keys = []
    for obj in result["objects"]:
        assert set(obj) == OBJECT_FIELDS
        assert {k: v for k, v in obj.items() if k != "key"} == expected[obj["kind"]]
        key = obj["key"]
        assert isinstance(key, str) and 0 < len(key) <= 512
        assert "://" not in key and ".." not in key and not key.startswith("/")
        if obj["kind"] != "base":
            assert result["checkpoint_id"] in key
        keys.append(key)
    assert len(set(keys)) == len(keys)
    return result


def _completion(source: dict[str, Any], reservation: dict[str, Any]) -> dict[str, Any]:
    return {
        **_authority(source),
        "checkpoint_id": reservation["checkpoint_id"],
        "receipts": copy.deepcopy(reservation["objects"]),
    }


def _completed(
    client: Any, source: dict[str, Any], reservation: dict[str, Any]
) -> dict[str, Any]:
    result = _ok(_post(client, "complete", _completion(source, reservation)))
    assert set(result) == {"checkpoint_id", "status", "completed_at", "expires_at"}
    assert result["checkpoint_id"] == reservation["checkpoint_id"]
    assert result["status"] == "complete"
    assert _date(result["expires_at"]) - _date(result["completed_at"]) == timedelta(hours=24)
    return result


def _date(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _reader_body(
    source: dict[str, Any], reservation: dict[str, Any], holder: str = "fixture-reader"
) -> dict[str, Any]:
    return {
        "work_item_id": source["work_item_id"],
        "checkpoint_id": reservation["checkpoint_id"],
        "holder_id": holder,
    }


def _reader(
    client: Any,
    source: dict[str, Any],
    reservation: dict[str, Any],
    holder: str = "fixture-reader",
) -> dict[str, Any]:
    before = _sql("SELECT clock_timestamp() AS value")[0]["value"]
    result = _ok(_post(client, "acquire-reader", _reader_body(source, reservation, holder)))
    after = _sql("SELECT clock_timestamp() AS value")[0]["value"]
    assert set(result) == {
        "lease_id", "checkpoint_id", "holder_id", "expires_at", "objects"
    }
    assert result["checkpoint_id"] == reservation["checkpoint_id"]
    assert result["holder_id"] == holder
    assert before + timedelta(seconds=300) <= _date(result["expires_at"])
    assert _date(result["expires_at"]) <= after + timedelta(seconds=300)
    assert len(result["objects"]) == 3
    assert {obj["kind"] for obj in result["objects"]} == {"base", "patch", "manifest"}
    assert all(set(obj) == OBJECT_FIELDS for obj in result["objects"])
    return result


def _release(client: Any, reader: dict[str, Any]) -> None:
    _ok(_post(client, "release-reader", {
        "lease_id": reader["lease_id"], "holder_id": reader["holder_id"]
    }))


def _claims(client: Any, collector: str = "fixture-collector", limit: int = 100) -> list[Any]:
    result = _ok(_post(client, "claim-collection", {"collector_id": collector, "limit": limit}))
    assert set(result) == {"claims"}
    claims = result["claims"]
    assert len(claims) <= limit
    for claim in claims:
        assert set(claim) == {"object_id", "key", "claim_id", "version", "claimed_until"}
        assert "://" not in claim["key"]
    return list(claims)


def _finalize(client: Any, claim: dict[str, Any], collector: str = "fixture-collector") -> Any:
    return _post(client, "finalize-collection", {
        "collector_id": collector,
        "object_id": claim["object_id"],
        "claim_id": claim["claim_id"],
        "version": claim["version"],
    })


def _fail(client: Any, source: dict[str, Any], reservation: dict[str, Any]) -> None:
    _ok(_post(client, "fail", {
        **_authority(source),
        "checkpoint_id": reservation["checkpoint_id"],
        "reason": "upload_failed",
    }))


def _snapshot(*, omit_outcomes: bool = False) -> dict[str, Any]:
    result = {}
    for table in (
        "work_items", "execution_requests", "approvals", "publications",
        "workspace_recovery_installation", "workspace_recovery_groups",
        "workspace_recovery_checkpoints", "workspace_recovery_objects",
        "workspace_recovery_readers",
    ):
        row = "to_jsonb(t)"
        if table == "work_items" and omit_outcomes:
            row += (
                " - 'workspace_recovery_outcome' - 'workspace_recovery_outcome_at' - 'updated_at'"
            )
        result[table] = _sql(
            f"SELECT COALESCE(jsonb_agg(value ORDER BY value::text), '[]'::jsonb) AS rows "
            f"FROM (SELECT {row} AS value FROM curie.{table} t) source"
        )[0]["rows"]
    return result


def _outcome(source: dict[str, Any]) -> dict[str, Any]:
    return _sql(
        "SELECT workspace_recovery_outcome, workspace_recovery_outcome_at, version "
        "FROM curie.work_items WHERE id = :id",
        id=uuid.UUID(source["work_item_id"]),
    )[0]


def _cap(monkeypatch: pytest.MonkeyPatch, value: int) -> None:
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", str(value))
    get_settings.cache_clear()


def _race(client: Any, requests: list[tuple[str, dict[str, Any]]]) -> list[Any]:
    barrier = Barrier(len(requests))

    def call(request: tuple[str, dict[str, Any]]) -> Any:
        barrier.wait(timeout=10)
        return _post(client, *request)

    with ThreadPoolExecutor(max_workers=len(requests)) as executor:
        futures = [executor.submit(call, request) for request in requests]
        return [future.result(timeout=20) for future in futures]


def test_recovery_http_authority(recovery: None, client: Any, auth_headers: Any) -> None:
    _routes_exist(client)
    source = _seed()
    body = _reserve_body(source, key="same-capture")
    before = _snapshot()
    state_token = mint(
        get_settings().api_key, agent=source["agent_id"], scope="state", exp=4102444800,
        claims={"binding": "slack:C0EXAMPLE1", "memory": "write", "sender": "U0EXAMPLE1",
                "turn": "fixture"},
    )
    for operation in OPERATIONS:
        for headers in (
            {}, {"X-Curie-Worker-Token": "wrong-token"}, auth_headers,
            {"X-Curie-Worker-Token": get_settings().api_key},
            {"X-API-Key": state_token}, {"X-Curie-Worker-Token": state_token},
        ):
            response = client.post(f"{ROOT}/{operation}", json=body, headers=headers)
            assert response.status_code == 401, response.text
            assert _snapshot() == before

    for change in (
        {"work_item_id": str(uuid.uuid4())},
        {"source_request_id": str(uuid.uuid4())},
        {"runtime_epoch": 4},
        {"dispatch_generation": 3},
        {"base_commit": "b" * 40},
        {"target_branch": "other"},
        {"base_archive_limit_bytes": ARCHIVE_LIMIT - 1},
        {"repository": {**REPOSITORY, "provider": "gitlab"}},
        {"repository": {**REPOSITORY, "host": "forge.example.com"}},
        {"repository": {**REPOSITORY, "project_id": "102"}},
        {"repository": {**REPOSITORY, "path": "acme-corp/other"}},
    ):
        _refused(_post(client, "reserve", {**body, **change}))
        assert _snapshot() == before

    malformed = [
        {**body, "key": "caller-selected-key"},
        {**body, "source_bytes": "unaccepted-source"},
        {**body, "repository": {**REPOSITORY, "url": "https://example.com/repo"}},
        {**body, "idempotency_key": "x" * 257},
        {**body, "objects": [{**body["objects"][0], "key": "foreign/key"}]},
        {**body, "objects": [_descriptor("patch", -1), _descriptor("manifest", 1)]},
        {**body, "objects": [_descriptor("patch", 1), _descriptor("patch", 2)]},
        {**body, "objects": [{**_descriptor("patch", 1), "sha256": "bad"}]},
    ]
    for invalid in malformed:
        response = _post(client, "reserve", invalid)
        assert response.status_code == 422, response.text
        assert _snapshot() == before

    reservation = _ok(_post(client, "reserve", body))
    reserved_state = _snapshot()
    assert _ok(_post(client, "reserve", body)) == reservation
    assert _snapshot() == reserved_state
    conflict = copy.deepcopy(body)
    conflict["objects"][1]["sha256"] = "f" * 64
    _refused(_post(client, "reserve", conflict))
    assert _snapshot() == reserved_state
    assert _outcome(source)["version"] == 7
    _refused(_post(client, "acquire-reader", _reader_body(source, reservation)))
    assert _snapshot() == reserved_state

    completion = _completion(source, reservation)
    for invalid in (
        {**completion, "source_request_id": str(uuid.uuid4())},
        {**completion, "runtime_epoch": 99},
        {**completion, "dispatch_generation": 99},
        {**completion, "receipts": completion["receipts"][:-1]},
    ):
        _refused(_post(client, "complete", invalid))
        assert _snapshot() == reserved_state
    for field, value in (("size_bytes", 999), ("sha256", "f" * 64), ("key", "foreign/key")):
        invalid = copy.deepcopy(completion)
        invalid["receipts"][0][field] = value
        _refused(_post(client, "complete", invalid))
        assert _snapshot() == reserved_state

    complete = _completed(client, source, reservation)
    completed_state = _snapshot()
    assert _ok(_post(client, "complete", completion)) == complete
    assert _snapshot() == completed_state
    reader = _reader(client, source, reservation)
    assert {obj["kind"]: obj for obj in reader["objects"]} == {
        obj["kind"]: obj for obj in reservation["objects"]
    }
    assert _outcome(source)["version"] == 7
    assert _snapshot()["execution_requests"] == before["execution_requests"]
    assert _snapshot()["approvals"] == before["approvals"]
    assert _snapshot()["publications"] == before["publications"]
    _release(client, reader)


@pytest.mark.parametrize(
    "state",
    ("running", "completed", "owner_lost", "start_failed", "execution_deadline",
     "issue_cancelled", "lineage_closed", "cancelled_item", "successor", "null_base",
     "expired_lease"),
)
def test_capture_refuses_ineligible_source_without_writes(
    recovery: None, client: Any, state: str
) -> None:
    _routes_exist(client)
    source = _seed()
    params = {"id": uuid.UUID(source["source_request_id"])}
    if state in {"running", "completed"}:
        _sql(
            "UPDATE curie.execution_requests SET status = :status, "
            "terminal_at = CASE WHEN :status = 'running' THEN NULL ELSE terminal_at END, "
            "terminal_cause = CASE WHEN :status = 'running' THEN NULL ELSE 'completed' END "
            "WHERE id = :id", status=state, **params,
        )
    elif state == "start_failed":
        _sql(
            "UPDATE curie.execution_requests SET terminal_cause = 'start_failed', "
            "started_at = NULL, execution_deadline = NULL, execution_attempts = 0 "
            "WHERE id = :id", **params,
        )
    elif state in {"owner_lost", "execution_deadline", "issue_cancelled", "lineage_closed"}:
        status = {
            "owner_lost": "failed", "execution_deadline": "expired",
            "issue_cancelled": "cancelled", "lineage_closed": "cancelled",
        }[state]
        _sql(
            "UPDATE curie.execution_requests SET status = :status, terminal_cause = :cause, "
            "termination_observation = 'fixture observed absent runtime' WHERE id = :id",
            status=status, cause=state, **params,
        )
    elif state == "cancelled_item":
        _sql("UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
             id=uuid.UUID(source["work_item_id"]))
    elif state == "successor":
        _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    elif state == "null_base":
        _sql(
            "UPDATE curie.work_items SET base_commit = NULL, base_branch = NULL, "
            "base_source = NULL WHERE id = :id", id=uuid.UUID(source["work_item_id"]),
        )
    else:
        _sql(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id", **params,
        )
    before = _snapshot()
    _refused(_post(client, "reserve", _reserve_body(source)))
    assert _snapshot() == before


def test_recovery_budget_races(
    recovery: None, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _routes_exist(client)
    _cap(monkeypatch, 250)
    sources = [_seed(), _seed()]
    bodies = [_reserve_body(source) for source in sources]
    answers = _race(client, [("reserve", body) for body in bodies])
    assert sorted(answer.status_code for answer in answers) == [200, 429]
    winner = next(i for i, answer in enumerate(answers) if answer.status_code == 200)
    loser = 1 - winner
    reservation = answers[winner].json()
    assert _sql("SELECT sum(size_bytes) AS value FROM curie.workspace_recovery_objects")[0][
        "value"
    ] == 150
    assert _outcome(sources[loser])["workspace_recovery_outcome"]
    assert _outcome(sources[loser])["version"] == 7
    assert not _sql(
        "SELECT 1 FROM curie.workspace_recovery_groups WHERE work_item_id = :id",
        id=uuid.UUID(sources[loser]["work_item_id"]),
    )
    _fail(client, sources[winner], reservation)
    before = _snapshot(omit_outcomes=True)
    _refused(_post(client, "reserve", bodies[loser]), 429)
    assert _snapshot(omit_outcomes=True) == before
    assert _sql("SELECT sum(size_bytes) AS value FROM curie.workspace_recovery_objects")[0][
        "value"
    ] == 150, "tombstones consume the installation budget until finalization"
    for _ in range(3):
        for claim in _claims(client):
            _ok(_finalize(client, claim))
    assert not _sql("SELECT 1 FROM curie.workspace_recovery_groups")
    _reserved(client, sources[loser], key=bodies[loser]["idempotency_key"])


def test_checkpoint_and_object_limits_preserve_a_tombstoned_reader(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    reader = _reader(client, source, first)
    second = _reserved(client, source, reuse=True)
    _completed(client, source, second)
    third = _reserved(client, source, reuse=True)
    assert len(_sql("SELECT 1 FROM curie.workspace_recovery_checkpoints")) == 3
    assert len(_sql("SELECT 1 FROM curie.workspace_recovery_objects")) == 7
    before = _snapshot(omit_outcomes=True)
    _refused(_post(client, "reserve", _reserve_body(source, reuse=True)), 429)
    assert _snapshot(omit_outcomes=True) == before
    _completed(client, source, third)
    assert _sql(
        "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
        id=uuid.UUID(first["checkpoint_id"]),
    )[0]["status"] == "tombstoned"
    _refused(_post(client, "acquire-reader", _reader_body(source, first, "late-reader")))
    assert _claims(client) == []
    assert _sql("SELECT count(*) AS value FROM curie.workspace_recovery_readers")[0]["value"] == 1
    assert _sql("SELECT sum(size_bytes) AS value FROM curie.workspace_recovery_objects")[0][
        "value"
    ] == 250
    for checkpoint in (second, third):
        selected = _reader(client, source, checkpoint, checkpoint["checkpoint_id"])
        _release(client, selected)
    _release(client, reader)
    claims = _claims(client)
    assert {claim["key"] for claim in claims} == {
        obj["key"] for obj in first["objects"] if obj["kind"] != "base"
    }
    for claim in claims:
        _ok(_finalize(client, claim))
    assert len(_sql("SELECT 1 FROM curie.workspace_recovery_objects")) == 5
    next_capture = _reserved(client, source, reuse=True)
    assert {obj["kind"] for obj in next_capture["objects"]} == {"patch", "manifest"}


@pytest.mark.parametrize("kind,limit", (("patch", 900000), ("manifest", 1048576),
                                        ("base", ARCHIVE_LIMIT)))
def test_oversize_capture_records_only_latest_outcome(
    recovery: None, client: Any, kind: str, limit: int
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot(omit_outcomes=True)
    body = _reserve_body(source)
    next(obj for obj in body["objects"] if obj["kind"] == kind)["size_bytes"] = limit + 1
    _refused(_post(client, "reserve", body), 413)
    assert _snapshot(omit_outcomes=True) == before
    outcome = _outcome(source)
    assert outcome["workspace_recovery_outcome"] and outcome["workspace_recovery_outcome_at"]
    assert outcome["version"] == 7
    accepted = _reserved(client, source)
    _completed(client, source, accepted)
    assert _outcome(source)["workspace_recovery_outcome"] != outcome["workspace_recovery_outcome"]
    assert _outcome(source)["version"] == 7


def test_recovery_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_WORKSPACE_RECOVERY_ENABLED", raising=False)
    monkeypatch.delenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", raising=False)
    monkeypatch.delenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", raising=False)
    defaults = Settings(_env_file=None)
    assert getattr(defaults, "workspace_recovery_enabled", None) is False
    assert getattr(defaults, "workspace_recovery_max_bytes", "missing") is None
    assert getattr(defaults, "workspace_max_archive_bytes", None) == ARCHIVE_LIMIT
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "true")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
    for cap in ("0", "-1", "not-a-number"):
        monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", cap)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", "1234")
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", "4321")
    configured = Settings(_env_file=None)
    assert configured.workspace_recovery_enabled
    assert configured.workspace_recovery_max_bytes == 1234
    assert configured.workspace_max_archive_bytes == 4321
    for value in ("0", "-1"):
        monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", value)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)


def test_disabling_recovery_refuses_new_work_and_still_drains(
    recovery: None, client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    reader = _reader(client, source, first)
    staging = _reserved(client, source, reuse=True)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "false")
    get_settings.cache_clear()
    before = _snapshot()
    _refused(_post(client, "reserve", _reserve_body(source, reuse=True)), 503)
    _refused(_post(client, "acquire-reader", _reader_body(source, first, "new")), 503)
    assert _snapshot() == before
    _completed(client, source, staging)
    _release(client, reader)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "true")
    get_settings.cache_clear()
    failed = _reserved(client, source, reuse=True)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "false")
    get_settings.cache_clear()
    _fail(client, source, failed)
    claims = _claims(client)
    assert len(claims) == 2
    for claim in claims:
        _ok(_finalize(client, claim))


def test_retained_work_item_count_is_serialized_with_new_reservations(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    sources = [_seed(), _seed()]
    _sql(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, agent_id, conversation_id, base_branch, base_source, "
        "base_commit) SELECT gen_random_uuid(), 'github', 'github.com', '101', "
        "(700000 + n)::text, 'github', 'github.com', '101', :path, 202, :agent, "
        "'recovery-count-seed', 'main', 'default', :base FROM generate_series(1, 9999) n",
        path=REPOSITORY["path"], agent=uuid.UUID(sources[0]["agent_id"]), base=BASE,
    )
    _sql(
        "INSERT INTO curie.workspace_recovery_groups "
        "(work_item_id, repository_provider, repository_host, repository_project_id, "
        "repository_path, base_commit, target_branch) SELECT id, code_host_kind, "
        "code_host_host, repository_project_id, repository_path, base_commit, base_branch "
        "FROM curie.work_items WHERE conversation_id = 'recovery-count-seed'"
    )
    _sql(
        "INSERT INTO curie.workspace_recovery_objects "
        "(id,work_item_id,kind,key,size_bytes,sha256,status) "
        "SELECT gen_random_uuid(), work_item_id, 'base', "
        "'workspace-recovery/retained/' || work_item_id::text || '/base', 1, :digest, "
        "'complete' FROM curie.workspace_recovery_groups", digest="a" * 64,
    )
    answers = _race(client, [("reserve", _reserve_body(source)) for source in sources])
    assert sorted(answer.status_code for answer in answers) == [200, 429]
    assert _sql("SELECT count(*) AS value FROM curie.workspace_recovery_groups")[0][
        "value"
    ] == 10000
    refused = sources[next(i for i, answer in enumerate(answers) if answer.status_code == 429)]
    assert _outcome(refused)["workspace_recovery_outcome"]
    assert not _sql(
        "SELECT 1 FROM curie.workspace_recovery_groups WHERE work_item_id = :id",
        id=uuid.UUID(refused["work_item_id"]),
    )


def _blocked_http(
    client: Any,
    source: dict[str, Any],
    operation: str,
    body: dict[str, Any],
    *,
    table: str = "workspace_recovery_groups",
    mutate: str | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        task: asyncio.Task[Any] | None = None
        try:
            async with engine.connect() as holder, engine.connect() as observer:
                pid = (await holder.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                column = "id" if table == "work_items" else "work_item_id"
                await holder.execute(
                    text(f"SELECT 1 FROM curie.{table} WHERE {column} = :id FOR UPDATE"),
                    {"id": uuid.UUID(source["work_item_id"])},
                )
                task = asyncio.create_task(asyncio.to_thread(_post, client, operation, body))

                async def blocked() -> None:
                    while True:
                        await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                        waiting = await observer.scalar(text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity a "
                            "WHERE a.datname = current_database() AND a.wait_event_type = 'Lock' "
                            "AND CAST(:pid AS integer) = ANY(pg_blocking_pids(a.pid)))"
                        ), {"pid": pid})
                        if waiting:
                            return
                        if task is not None and task.done():
                            answer = task.result()
                            raise AssertionError(
                                f"HTTP {operation} finished before its row lock: {answer.text}"
                            )
                        await asyncio.sleep(0.01)

                try:
                    await asyncio.wait_for(blocked(), timeout=10)
                    if mutate is not None:
                        await holder.execute(text(mutate), params or {})
                    await holder.commit()
                    return await asyncio.wait_for(task, timeout=10)
                finally:
                    await holder.rollback()
                    if task is not None and not task.done():
                        await asyncio.wait_for(task, timeout=10)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_recovery_selection_gc_race(recovery: None, client: Any) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    _completed(client, source, reservation)
    selected = _ok(_blocked_http(
        client, source, "acquire-reader", _reader_body(source, reservation)
    ))
    _sql(
        "UPDATE curie.workspace_recovery_checkpoints SET expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(reservation["checkpoint_id"]),
    )
    assert _claims(client) == []
    assert _sql(
        "SELECT id FROM curie.workspace_recovery_readers WHERE id = :id",
        id=uuid.UUID(selected["lease_id"]),
    )
    assert _sql(
        "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
        id=uuid.UUID(reservation["checkpoint_id"]),
    )[0]["status"] == "tombstoned"
    before = _snapshot()
    _refused(_post(client, "acquire-reader", _reader_body(source, reservation, "late")))
    assert _snapshot() == before
    _release(client, selected)
    claims = _claims(client)
    assert len(claims) == 2
    for claim in claims:
        _ok(_finalize(client, claim))

    other = _seed()
    live = _reserved(client, other)
    _completed(client, other, live)
    refused = _blocked_http(
        client, other, "acquire-reader", _reader_body(other, live),
        mutate=(
            "UPDATE curie.workspace_recovery_checkpoints SET expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id"
        ),
        params={"id": uuid.UUID(live["checkpoint_id"])},
    )
    _refused(refused)
    assert not _sql(
        "SELECT 1 FROM curie.workspace_recovery_readers WHERE work_item_id = :id",
        id=uuid.UUID(other["work_item_id"]),
    ), "selection must repeat the database-clock expiry check after its lock wait"
    collected = _claims(client)
    assert {obj["key"] for obj in live["objects"] if obj["kind"] != "base"} <= {
        claim["key"] for claim in collected
    }


def test_eight_readers_are_nonrenewable_and_expired_readers_are_reclaimed(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    _completed(client, source, reservation)
    first = _reader(client, source, reservation, "same-holder")
    _sql(
        "UPDATE curie.workspace_recovery_readers SET expires_at = "
        "clock_timestamp() + interval '1 minute' WHERE id = :id",
        id=uuid.UUID(first["lease_id"]),
    )
    shortened = _sql(
        "SELECT expires_at FROM curie.workspace_recovery_readers WHERE id = :id",
        id=uuid.UUID(first["lease_id"]),
    )[0]["expires_at"]
    repeated = _ok(_post(client, "acquire-reader", _reader_body(
        source, reservation, "same-holder"
    )))
    assert repeated["lease_id"] == first["lease_id"]
    assert _date(repeated["expires_at"]) == shortened
    answers = _race(client, [
        ("acquire-reader", _reader_body(source, reservation, f"holder-{i}")) for i in range(8)
    ])
    assert sorted(answer.status_code for answer in answers) == [200] * 7 + [429]
    assert _sql("SELECT count(*) AS value FROM curie.workspace_recovery_readers")[0]["value"] == 8
    before = _snapshot()
    _refused(_post(client, "release-reader", {
        "lease_id": first["lease_id"], "holder_id": "wrong-holder"
    }))
    assert _snapshot() == before
    response = _post(client, "release-reader", {
        "lease_id": first["lease_id"], "holder_id": "same-holder", "renew_seconds": 300
    })
    assert response.status_code == 422
    assert _snapshot() == before
    _sql(
        "UPDATE curie.workspace_recovery_readers SET expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(first["lease_id"]),
    )
    replacement = _reader(client, source, reservation, "replacement-holder")
    assert replacement["lease_id"] != first["lease_id"]
    assert _sql("SELECT count(*) AS value FROM curie.workspace_recovery_readers")[0]["value"] == 8
    assert not _sql(
        "SELECT 1 FROM curie.workspace_recovery_readers WHERE id = :id",
        id=uuid.UUID(first["lease_id"]),
    )


def test_recovery_rollback_and_expiry(recovery: None, client: Any) -> None:
    _routes_exist(client)
    source = _seed()
    before_reserve = _sql("SELECT clock_timestamp() AS value")[0]["value"]
    first = _reserved(client, source)
    after_reserve = _sql("SELECT clock_timestamp() AS value")[0]["value"]
    assert before_reserve + timedelta(seconds=60) <= _date(first["capture_deadline"])
    assert _date(first["capture_deadline"]) <= after_reserve + timedelta(seconds=60)
    assert before_reserve + timedelta(seconds=600) <= _date(first["staging_expires_at"])
    assert _date(first["staging_expires_at"]) <= after_reserve + timedelta(seconds=600)
    _completed(client, source, first)
    candidate = _reserved(client, source, reuse=True)
    _sql(
        "CREATE FUNCTION curie.recovery_test_reject_pointer() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "RAISE EXCEPTION 'injected recovery rollback'; END $$"
    )
    _sql(
        "CREATE TRIGGER recovery_test_reject_pointer BEFORE UPDATE OF "
        "last_good_checkpoint_id ON curie.workspace_recovery_groups FOR EACH ROW "
        "WHEN (OLD.last_good_checkpoint_id IS DISTINCT FROM NEW.last_good_checkpoint_id) "
        "EXECUTE FUNCTION curie.recovery_test_reject_pointer()"
    )
    initial = _snapshot()
    try:
        try:
            response = _post(client, "complete", _completion(source, candidate))
        except DBAPIError as error:
            assert "injected recovery rollback" in str(error.orig)
        else:
            assert response.status_code == 500, response.text
        assert _snapshot() == initial
    finally:
        _sql("DROP TRIGGER recovery_test_reject_pointer ON curie.workspace_recovery_groups")
        _sql("DROP FUNCTION curie.recovery_test_reject_pointer()")
    old_reader = _reader(client, source, first)
    _release(client, old_reader)
    _refused(_post(client, "acquire-reader", _reader_body(source, candidate)))
    _completed(client, source, candidate)
    assert str(_sql(
        "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
        "WHERE work_item_id = :id", id=uuid.UUID(source["work_item_id"]),
    )[0]["last_good_checkpoint_id"]) == candidate["checkpoint_id"]

    abandoned = _seed()
    staging = _reserved(client, abandoned)
    response = _blocked_http(
        client, abandoned, "complete", _completion(abandoned, staging), table="work_items",
        mutate=(
            "UPDATE curie.workspace_recovery_checkpoints SET capture_deadline = "
            "clock_timestamp() - interval '1 second' WHERE id = :id"
        ), params={"id": uuid.UUID(staging["checkpoint_id"])},
    )
    _refused(response)
    assert _sql(
        "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
        id=uuid.UUID(staging["checkpoint_id"]),
    )[0]["status"] == "staging"
    assert _claims(client) == []
    _sql(
        "UPDATE curie.workspace_recovery_checkpoints SET staging_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(staging["checkpoint_id"]),
    )
    claims = _claims(client, "crashed-collector")
    assert len(claims) == 2
    original = claims[0]
    assert _claims(client, "replacement-collector") == []
    wrong_version = {**original, "version": original["version"] + 1}
    initial = _snapshot()
    _refused(_finalize(client, wrong_version, "crashed-collector"))
    assert _snapshot() == initial
    _sql(
        "UPDATE curie.workspace_recovery_objects SET claimed_until = "
        "clock_timestamp() - interval '1 second' WHERE work_item_id = :id "
        "AND claim_id IS NOT NULL", id=uuid.UUID(abandoned["work_item_id"]),
    )
    resumed = _claims(client, "replacement-collector")
    assert {claim["object_id"] for claim in resumed} == {claim["object_id"] for claim in claims}
    for claim in resumed:
        predecessor = next(old for old in claims if old["object_id"] == claim["object_id"])
        assert claim["version"] > predecessor["version"]
        assert claim["claim_id"] != predecessor["claim_id"]
        initial = _snapshot()
        _refused(_finalize(client, predecessor, "crashed-collector"))
        assert _snapshot() == initial
        _ok(_finalize(client, claim, "replacement-collector"))
        _ok(_finalize(client, claim, "replacement-collector"))
    base_claims = _claims(client, "replacement-collector")
    assert len(base_claims) == 1
    assert base_claims[0]["key"] == next(
        obj["key"] for obj in staging["objects"] if obj["kind"] == "base"
    )
    _ok(_finalize(client, base_claims[0], "replacement-collector"))
    assert not _sql(
        "SELECT 1 FROM curie.workspace_recovery_groups WHERE work_item_id = :id",
        id=uuid.UUID(abandoned["work_item_id"]),
    )


def test_base_reuse_is_fenced_by_completion_and_collection(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    before = _snapshot(omit_outcomes=True)
    _refused(_post(client, "reserve", _reserve_body(source, reuse=True)))
    assert _snapshot(omit_outcomes=True) == before
    _completed(client, source, first)
    before = _snapshot()
    _refused(_post(client, "reserve", _reserve_body(source)))
    assert _snapshot() == before, "a retained complete base accepts no new base declaration"
    second = _reserved(client, source, reuse=True)
    assert {obj["kind"] for obj in second["objects"]} == {"patch", "manifest"}
    _fail(client, source, second)
    for claim in _claims(client):
        _ok(_finalize(client, claim))
    _sql(
        "UPDATE curie.workspace_recovery_checkpoints SET expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(first["checkpoint_id"]),
    )
    for claim in _claims(client):
        _ok(_finalize(client, claim))
    assert not _sql("SELECT 1 FROM curie.workspace_recovery_checkpoints")
    answers = _race(client, [
        ("reserve", _reserve_body(source, reuse=True)),
        ("claim-collection", {"collector_id": "base-collector", "limit": 100}),
    ])
    reservation_response, collection_response = answers
    collected = _ok(collection_response)["claims"]
    if reservation_response.status_code == 200:
        assert collected == []
        _fail(client, source, reservation_response.json())
        for claim in _claims(client):
            _ok(_finalize(client, claim))
        collected = _claims(client, "base-collector")
    else:
        _refused(reservation_response)
    assert len(collected) == 1
    assert collected[0]["key"] == next(
        obj["key"] for obj in first["objects"] if obj["kind"] == "base"
    )
    before = _snapshot(omit_outcomes=True)
    _refused(_post(client, "reserve", _reserve_body(source, reuse=True)))
    assert _snapshot(omit_outcomes=True) == before
    _ok(_finalize(client, collected[0], "base-collector"))
    fresh = _reserved(client, source)
    assert next(obj["key"] for obj in fresh["objects"] if obj["kind"] == "base") != next(
        obj["key"] for obj in first["objects"] if obj["kind"] == "base"
    )


def test_changed_base_is_refused_until_every_tombstone_is_finalized(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    _sql("UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
         id=uuid.UUID(source["work_item_id"]), base="b" * 40)
    body = _reserve_body(source)
    body["base_commit"] = "b" * 40
    for _ in range(2):
        before = _snapshot(omit_outcomes=True)
        _refused(_post(client, "reserve", body))
        assert _snapshot(omit_outcomes=True) == before
        if _ == 0:
            _sql(
                "UPDATE curie.workspace_recovery_checkpoints SET expires_at = "
                "clock_timestamp() - interval '1 second' WHERE id = :id",
                id=uuid.UUID(first["checkpoint_id"]),
            )
            assert len(_claims(client)) == 2
    for _ in range(3):
        for claim in _claims(client):
            _ok(_finalize(client, claim))
        _sql(
            "UPDATE curie.workspace_recovery_objects SET claimed_until = "
            "clock_timestamp() - interval '1 second' WHERE claim_id IS NOT NULL"
        )
    assert not _sql("SELECT 1 FROM curie.workspace_recovery_groups")
    new = _ok(_post(client, "reserve", body))
    assert new["checkpoint_id"] != first["checkpoint_id"]


def test_agent_deletion_conflicts_until_recovery_collection_is_finalized(
    recovery: None, client: Any, auth_headers: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    _completed(client, source, reservation)
    before = _snapshot()
    response = client.delete(f"/agents/{source['agent_id']}", headers=auth_headers)
    assert response.status_code == 409, response.text
    assert _snapshot() == before
    _sql(
        "UPDATE curie.workspace_recovery_checkpoints SET expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(reservation["checkpoint_id"]),
    )
    for _ in range(3):
        for claim in _claims(client):
            _ok(_finalize(client, claim))
    assert not _sql("SELECT 1 FROM curie.workspace_recovery_groups")
    response = client.delete(f"/agents/{source['agent_id']}", headers=auth_headers)
    assert response.status_code == 204, response.text
    assert not _sql("SELECT 1 FROM curie.agents WHERE id = :id", id=uuid.UUID(source["agent_id"]))


def test_recovery_candidate_cannot_serve_schema_before_0140(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0139")
    with pytest.raises(RuntimeError, match="schema"):
        with TestClient(create_app()):
            pytest.fail("the API started against a schema without the recovery ledger")
