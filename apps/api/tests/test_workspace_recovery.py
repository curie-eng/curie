"""Recovery metadata is exercised through authenticated HTTP and real PostgreSQL."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import time
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


def _seed(
    *, agent_id: str | None = None, item_id: str | None = None, start_failed: bool = False,
    lineage_closed: bool = False, deadline_passed: bool = False
) -> dict[str, Any]:
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
        "(:id, :item, :sequence, CASE WHEN :lineage_closed THEN 'cancelled' ELSE 'failed' END, "
        "CASE WHEN :lineage_closed THEN NULL ELSE clock_timestamp() + interval '1 hour' END, "
        "CASE WHEN :start_failed OR :lineage_closed THEN NULL "
        "ELSE clock_timestamp() - interval '1 minute' END, "
        "CASE WHEN :start_failed OR :lineage_closed THEN NULL "
        "WHEN :deadline_passed THEN clock_timestamp() - interval '1 second' "
        "ELSE clock_timestamp() + interval '1 hour' END, "
        "clock_timestamp() - interval '1 second', "
        "CASE WHEN :lineage_closed THEN 'lineage_closed' "
        "WHEN :start_failed THEN 'start_failed' ELSE 'runner_failed' END, "
        "4, 'fixture-worker', "
        "3, clock_timestamp() + interval '1 hour', 2, "
        "CASE WHEN :start_failed OR :lineage_closed THEN 0 ELSE 1 END)",
        id=uuid.UUID(request_id),
        item=uuid.UUID(item_id),
        sequence=sequence,
        start_failed=start_failed,
        lineage_closed=lineage_closed,
        deadline_passed=deadline_passed,
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
        "checkpoint_id", "objects", "capture_deadline", "staging_expires_at", "replayed"
    }
    assert isinstance(result["replayed"], bool)
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
    source = _seed(start_failed=state == "start_failed", lineage_closed=state == "lineage_closed")
    params = {"id": uuid.UUID(source["source_request_id"])}
    if state in {"running", "completed"}:
        _sql(
            "UPDATE curie.execution_requests SET status = :status, "
            "terminal_at = CASE WHEN CAST(:status AS varchar) = 'running' THEN NULL "
            "ELSE terminal_at END, "
            "terminal_cause = CASE WHEN CAST(:status AS varchar) = 'running' THEN NULL "
            "ELSE 'completed' END "
            "WHERE id = :id", status=state, **params,
        )
    elif state in {"start_failed", "lineage_closed"}:
        pass
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


def test_reserved_capture_completes_after_source_heartbeat_expires(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(source["source_request_id"]),
    )
    assert _sql(
        "SELECT r.runtime_heartbeat_expires_at < clock_timestamp() AS heartbeat_expired, "
        "c.capture_deadline > clock_timestamp() AS capture_valid "
        "FROM curie.execution_requests r JOIN curie.workspace_recovery_checkpoints c "
        "ON c.source_request_id = r.id WHERE c.id = :id",
        id=uuid.UUID(reservation["checkpoint_id"]),
    ) == [{"heartbeat_expired": True, "capture_valid": True}]
    execution_before = _snapshot()["execution_requests"]

    _completed(client, source, reservation)

    assert str(_sql(
        "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
        "WHERE work_item_id = :id", id=uuid.UUID(source["work_item_id"]),
    )[0]["last_good_checkpoint_id"]) == reservation["checkpoint_id"]
    reader = _reader(client, source, reservation)
    assert {obj["kind"]: obj for obj in reader["objects"]} == {
        obj["kind"]: obj for obj in reservation["objects"]
    }
    assert _snapshot()["execution_requests"] == execution_before
    _release(client, reader)


def test_reserved_reused_base_capture_fails_after_source_heartbeat_expires(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    reservation = _reserved(client, source, reuse=True)
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(source["source_request_id"]),
    )
    execution_before = _snapshot()["execution_requests"]

    _fail(client, source, reservation)

    assert _sql(
        "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
        id=uuid.UUID(reservation["checkpoint_id"]),
    ) == [{"status": "tombstoned"}]
    assert _sql(
        "SELECT kind, status FROM curie.workspace_recovery_objects "
        "WHERE checkpoint_id = :id ORDER BY kind",
        id=uuid.UUID(reservation["checkpoint_id"]),
    ) == [
        {"kind": "manifest", "status": "tombstoned"},
        {"kind": "patch", "status": "tombstoned"},
    ]
    assert str(_sql(
        "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
        "WHERE work_item_id = :id", id=uuid.UUID(source["work_item_id"]),
    )[0]["last_good_checkpoint_id"]) == first["checkpoint_id"]
    claims = _claims(client)
    assert {claim["key"] for claim in claims} == {
        obj["key"] for obj in reservation["objects"]
    }
    for claim in claims:
        _ok(_finalize(client, claim))
    reader = _reader(client, source, first)
    _release(client, reader)
    assert _snapshot()["execution_requests"] == execution_before


def test_recovery_candidate_cannot_serve_schema_before_0141(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0139")
    with pytest.raises(RuntimeError, match="schema"):
        with TestClient(create_app()):
            pytest.fail("the API started against a schema without the recovery ledger")


# --- terminal capture: context, outcome, budget and lease-loss cleanup (#5143) ------

CAPTURE_REASONS = (
    "torn_capture",
    "capture_timeout",
    "capture_failed",
    "runner_unavailable",
    "patch_too_large",
    "manifest_too_large",
    "base_mismatch",
)


def _no_capability(text: str) -> None:
    """A metadata answer names no URL, signature or object bytes."""

    assert "://" not in text
    assert "X-Amz" not in text and "Signature" not in text
    assert "base64" not in text


def _outcome_body(source: dict[str, Any], reason: str) -> dict[str, Any]:
    return {**_authority(source), "reason": reason}


def _last_good(source: dict[str, Any]) -> str | None:
    rows = _sql(
        "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
        "WHERE work_item_id = :id",
        id=uuid.UUID(source["work_item_id"]),
    )
    value = rows[0]["last_good_checkpoint_id"] if rows else None
    return str(value) if value is not None else None


def _checkpoint_status(reservation: dict[str, Any]) -> str:
    return str(
        _sql(
            "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
            id=uuid.UUID(reservation["checkpoint_id"]),
        )[0]["status"]
    )


def _lease_lost(
    client: Any, source: dict[str, Any], reservation: dict[str, Any], *, retract: bool = False
) -> Any:
    """``retract`` is sent only by a writer whose own completion returned this invocation."""

    body: dict[str, Any] = {
        **_authority(source),
        "checkpoint_id": reservation["checkpoint_id"],
        "reason": "lease_lost",
    }
    if retract:
        body["retract"] = True
    return _post(client, "fail", body)


def _terminal_now(source: dict[str, Any]) -> None:
    """Move the source's terminus to now, after any outcome already written."""

    _sql(
        "UPDATE curie.execution_requests SET terminal_at = clock_timestamp() WHERE id = :id",
        id=uuid.UUID(source["source_request_id"]),
    )


def test_capture_context_and_outcome_require_the_worker_token(
    recovery: None, client: Any, auth_headers: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot()
    for operation, body in (
        ("context", _authority(source)),
        ("outcome", _outcome_body(source, "capture_failed")),
    ):
        for headers in (
            {}, {"X-Curie-Worker-Token": "wrong-token"}, auth_headers,
            {"X-Curie-Worker-Token": get_settings().api_key},
        ):
            response = client.post(f"{ROOT}/{operation}", json=body, headers=headers)
            assert response.status_code == 401, response.text
            assert _snapshot() == before
    # Liveness: the worker token opens both.
    _ok(_post(client, "context", _authority(source)))
    _ok(_post(client, "outcome", _outcome_body(source, "capture_failed")))


def test_capture_context_names_the_work_item_repository_and_writes_nothing(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot()
    response = _post(client, "context", _authority(source))
    body = _ok(response)
    assert body == {
        "repository": dict(REPOSITORY),
        "base_commit": BASE,
        "target_branch": "main",
    }
    _no_capability(response.text)
    assert _snapshot() == before


@pytest.mark.parametrize(
    "state",
    ("disabled", "unknown_item", "unknown_source", "other_epoch", "other_generation",
     "successor", "running", "start_failed", "cancelled_item", "expired_heartbeat",
     "expired_deadline", "null_base"),
)
def test_capture_context_refuses_a_source_that_cannot_start_a_capture(
    recovery: None, client: Any, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    _routes_exist(client)
    # Execution deadlines are write once, so an expired one is seeded expired.
    source = _seed(
        start_failed=state == "start_failed", deadline_passed=state == "expired_deadline"
    )
    body = _authority(source)
    request = {"id": uuid.UUID(source["source_request_id"])}
    status, code = 409, None
    if state == "disabled":
        monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "false")
        get_settings.cache_clear()
        status, code = 503, "workspace_recovery.disabled"
    elif state == "unknown_item":
        body["work_item_id"] = str(uuid.uuid4())
        code = "workspace_recovery.source_identity"
    elif state == "unknown_source":
        body["source_request_id"] = str(uuid.uuid4())
        code = "workspace_recovery.source_identity"
    elif state == "other_epoch":
        body["runtime_epoch"] = 4
        code = "workspace_recovery.source_identity"
    elif state == "other_generation":
        body["dispatch_generation"] = 3
        code = "workspace_recovery.source_identity"
    elif state == "successor":
        _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
        code = "workspace_recovery.source_identity"
    elif state == "running":
        _sql(
            "UPDATE curie.execution_requests SET status = 'running', terminal_at = NULL, "
            "terminal_cause = NULL WHERE id = :id", **request,
        )
        code = "workspace_recovery.ineligible_source"
    elif state == "start_failed":
        code = "workspace_recovery.ineligible_source"
    elif state == "cancelled_item":
        _sql("UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
             id=uuid.UUID(source["work_item_id"]))
        code = "workspace_recovery.ineligible_source"
    elif state == "expired_heartbeat":
        _sql(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id", **request,
        )
        code = "workspace_recovery.expired_source"
    elif state == "expired_deadline":
        code = "workspace_recovery.expired_source"
    else:
        _sql(
            "UPDATE curie.work_items SET base_commit = NULL, base_branch = NULL, "
            "base_source = NULL WHERE id = :id", id=uuid.UUID(source["work_item_id"]),
        )
        code = "workspace_recovery.source_identity"
    before = _snapshot()
    response = _post(client, "context", body)
    _refused(response, status)
    assert response.json()["detail"]["code"] == code
    _no_capability(response.text)
    assert _snapshot() == before


@pytest.mark.parametrize("reason", CAPTURE_REASONS)
def test_capture_outcome_records_each_pre_reservation_reason_and_nothing_else(
    recovery: None, client: Any, reason: str
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot(omit_outcomes=True)
    response = _post(client, "outcome", _outcome_body(source, reason))
    _ok(response)
    _no_capability(response.text)
    recorded = _outcome(source)
    assert recorded["workspace_recovery_outcome"] == reason
    assert recorded["workspace_recovery_outcome_at"] is not None
    # Only the WorkItem outcome moved: no checkpoint, object, approval or publication.
    assert _snapshot(omit_outcomes=True) == before
    assert recorded["version"] == 7


@pytest.mark.parametrize(
    "reason", ("reserved", "complete", "lease_lost", "upload_failed", "anything"),
)
def test_capture_outcome_refuses_a_reason_outside_its_closed_set(
    recovery: None, client: Any, reason: str
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot()
    response = _post(client, "outcome", _outcome_body(source, reason))
    assert response.status_code == 422, response.text
    assert _snapshot() == before


def test_capture_outcome_survives_an_expired_heartbeat_and_deadline(
    recovery: None, client: Any
) -> None:
    """A capture that spent its whole budget still records why, after the lease lapsed."""

    _routes_exist(client)
    source = _seed(deadline_passed=True)
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(source["source_request_id"]),
    )
    # The same expiry refuses a new capture: the asymmetry is the point.
    _refused(_post(client, "context", _authority(source)))
    _ok(_post(client, "outcome", _outcome_body(source, "capture_timeout")))
    assert _outcome(source)["workspace_recovery_outcome"] == "capture_timeout"


@pytest.mark.parametrize(
    "state", ("successor", "running", "start_failed", "other_epoch", "cancelled_item"),
)
def test_capture_outcome_refuses_a_source_that_is_not_the_latest_failed_run(
    recovery: None, client: Any, state: str
) -> None:
    _routes_exist(client)
    source = _seed(start_failed=state == "start_failed")
    body = _outcome_body(source, "capture_failed")
    if state == "successor":
        _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    elif state == "running":
        _sql(
            "UPDATE curie.execution_requests SET status = 'running', terminal_at = NULL, "
            "terminal_cause = NULL WHERE id = :id",
            id=uuid.UUID(source["source_request_id"]),
        )
    elif state == "other_epoch":
        body["runtime_epoch"] = 4
    elif state == "cancelled_item":
        _sql("UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
             id=uuid.UUID(source["work_item_id"]))
    before = _snapshot()
    _refused(_post(client, "outcome", body))
    assert _snapshot() == before


def test_capture_outcome_overwrites_only_an_outcome_older_than_its_source_terminus(
    recovery: None, client: Any
) -> None:
    """Structural rule on real ``clock_timestamp()`` ordering, no clock arithmetic."""

    _routes_exist(client)
    previous = _seed()
    _ok(_post(client, "outcome", _outcome_body(previous, "runner_unavailable")))
    older = _outcome(previous)
    source = _seed(agent_id=previous["agent_id"], item_id=previous["work_item_id"])
    _terminal_now(source)
    # The previous run's outcome predates this source's terminus: overwritten.
    _ok(_post(client, "outcome", _outcome_body(source, "torn_capture")))
    written = _outcome(source)
    assert written["workspace_recovery_outcome"] == "torn_capture"
    assert written["workspace_recovery_outcome_at"] > older["workspace_recovery_outcome_at"]

    # A reservation by this same source writes a later outcome; a pre-reservation
    # reason arriving after it does not displace it.
    reservation = _reserved(client, source)
    assert _outcome(source)["workspace_recovery_outcome"] == "reserved"
    _ok(_post(client, "outcome", _outcome_body(source, "capture_failed")))
    assert _outcome(source)["workspace_recovery_outcome"] == "reserved"
    _fail(client, source, reservation)


@pytest.mark.parametrize("budget", (0, -1, 0.0, 60.001, 61))
def test_reserve_refuses_a_capture_budget_outside_zero_to_sixty(
    recovery: None, client: Any, budget: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    before = _snapshot()
    body = {**_reserve_body(source), "capture_budget_seconds": budget}
    response = _post(client, "reserve", body)
    assert response.status_code == 422, response.text
    assert _snapshot() == before


@pytest.mark.parametrize("budget", (None, 60, 0.5, 17.25))
def test_reserve_sets_the_capture_deadline_from_the_remaining_budget(
    recovery: None, client: Any, budget: float | None
) -> None:
    _routes_exist(client)
    source = _seed()
    body = _reserve_body(source)
    if budget is not None:
        body["capture_budget_seconds"] = budget
    reservation = _ok(_post(client, "reserve", body))
    expected = timedelta(seconds=60 if budget is None else budget)
    # reserve stamps one database ``now`` for both deadlines.
    reserved_at = _date(reservation["staging_expires_at"]) - timedelta(seconds=600)
    assert _date(reservation["capture_deadline"]) - reserved_at == expected
    row = _sql(
        "SELECT capture_deadline, staging_expires_at FROM curie.workspace_recovery_checkpoints "
        "WHERE id = :id", id=uuid.UUID(reservation["checkpoint_id"]),
    )[0]
    assert row["capture_deadline"] - (row["staging_expires_at"] - timedelta(seconds=600)) == (
        expected
    )
    assert row["capture_deadline"] == _date(reservation["capture_deadline"])


def test_reserve_replays_with_a_smaller_remaining_budget_and_keeps_its_deadline(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    body = {**_reserve_body(source, key="replayed-capture"), "capture_budget_seconds": 40}
    first = _ok(_post(client, "reserve", body))
    state = _snapshot()
    replay = _ok(_post(client, "reserve", {**body, "capture_budget_seconds": 12.5}))
    assert {**replay, "replayed": False} == first
    assert _snapshot() == state
    # The budget is excluded from the content digest; the content is not.
    changed = copy.deepcopy(body)
    changed["objects"][-1]["sha256"] = "f" * 64
    _refused(_post(client, "reserve", changed))
    assert _snapshot() == state


def test_reserve_answers_replayed_only_when_the_idempotency_key_matched(
    recovery: None, client: Any
) -> None:
    """``replayed`` is the writer's proof that this invocation did not create the checkpoint."""

    _routes_exist(client)
    source = _seed()
    body = _reserve_body(source, key="replay-proof")
    first = _ok(_post(client, "reserve", body))
    assert first["replayed"] is False
    replay = _ok(_post(client, "reserve", body))
    assert replay["replayed"] is True
    assert replay["checkpoint_id"] == first["checkpoint_id"]
    _completed(client, source, first)
    # A replay of a complete checkpoint still says so.
    after = _ok(_post(client, "reserve", body))
    assert after["replayed"] is True
    assert after["checkpoint_id"] == first["checkpoint_id"]
    fresh = _ok(_post(client, "reserve", _reserve_body(source, reuse=True)))
    assert fresh["replayed"] is False
    assert fresh["checkpoint_id"] != first["checkpoint_id"]


def test_a_completion_after_the_budgeted_deadline_is_refused_and_publishes_nothing(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _ok(_post(client, "reserve", {
        **_reserve_body(source), "capture_budget_seconds": 0.5,
    }))
    # Wait out the budget on the database clock, not by arithmetic.
    for _ in range(100):
        if _sql(
            "SELECT capture_deadline <= clock_timestamp() AS passed "
            "FROM curie.workspace_recovery_checkpoints WHERE id = :id",
            id=uuid.UUID(reservation["checkpoint_id"]),
        )[0]["passed"]:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("capture deadline never passed")
    _refused(_post(client, "complete", _completion(source, reservation)))
    assert _last_good(source) is None
    assert _checkpoint_status(reservation) == "staging"
    _fail(client, source, reservation)
    # Liveness: the same shape with the default budget completes.
    fresh = _seed()
    accepted = _reserved(client, fresh)
    _completed(client, fresh, accepted)
    assert _last_good(fresh) == accepted["checkpoint_id"]


def test_lease_lost_cleanup_tombstones_a_superseded_source_staging(
    recovery: None, client: Any
) -> None:
    """A predecessor whose lease was lost cleans up even after a successor was admitted."""

    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    assert _outcome(source)["workspace_recovery_outcome"] == "reserved"
    successor = _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    # The successor's admission alone bars the predecessor from new captures.
    _refused(_post(client, "context", _authority(source)))
    _refused(_post(client, "reserve", _reserve_body(source, reuse=True)))
    _refused(_post(client, "complete", _completion(source, reservation)))

    response = _lease_lost(client, source, reservation)
    body = _ok(response)
    assert body == {"checkpoint_id": reservation["checkpoint_id"], "status": "tombstoned"}
    _no_capability(response.text)
    assert _checkpoint_status(reservation) == "tombstoned"
    assert _last_good(source) is None
    # The successor never captured, so the predecessor's own reservation outcome
    # is replaced by the cleanup reason.
    assert _outcome(source)["workspace_recovery_outcome"] == "lease_lost"
    # The successor's own capture is unaffected.
    _terminal_now(successor)
    assert _ok(_post(client, "context", _authority(successor)))["base_commit"] == BASE


@pytest.mark.parametrize("stage", ("staging", "complete"))
def test_predecessor_cleanup_survives_a_successor_readmitted_on_a_new_base(
    recovery: None, client: Any, stage: str
) -> None:
    """Cleanup is fenced by the retained checkpoint's identity, not the WorkItem's base.

    Ordinary readmission moves the WorkItem to a new base commit; the predecessor
    whose lease was lost still withdraws its own staging or completion.
    """

    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    if stage == "complete":
        _completed(client, source, reservation)
    _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    _sql(
        "UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
        id=uuid.UUID(source["work_item_id"]),
        base="b" * 40,
    )
    response = _lease_lost(client, source, reservation, retract=stage == "complete")
    expected = "retracted" if stage == "complete" else "tombstoned"
    assert _ok(response) == {"checkpoint_id": reservation["checkpoint_id"], "status": expected}
    _no_capability(response.text)
    assert _checkpoint_status(reservation) == "tombstoned"
    assert _last_good(source) is None
    assert _outcome(source)["workspace_recovery_outcome"] == "lease_lost"


@pytest.mark.parametrize("change", ("other_epoch", "other_generation", "running_source"))
def test_superseded_cleanup_still_requires_the_exact_failed_source(
    recovery: None, client: Any, change: str
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    body = {
        **_authority(source),
        "checkpoint_id": reservation["checkpoint_id"],
        "reason": "lease_lost",
    }
    if change == "other_epoch":
        body["runtime_epoch"] = 4
    elif change == "other_generation":
        body["dispatch_generation"] = 3
    else:
        _sql(
            "UPDATE curie.execution_requests SET status = 'running', terminal_at = NULL, "
            "terminal_cause = NULL WHERE id = :id",
            id=uuid.UUID(source["source_request_id"]),
        )
    before = _snapshot()
    _refused(_post(client, "fail", body))
    assert _snapshot() == before
    assert _checkpoint_status(reservation) == "staging"


def test_lease_lost_cleanup_keeps_a_successors_newer_outcome(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    reservation = _reserved(client, source)
    successor = _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    _terminal_now(successor)
    _ok(_post(client, "outcome", _outcome_body(successor, "runner_unavailable")))
    newer = _outcome(successor)
    assert newer["workspace_recovery_outcome"] == "runner_unavailable"

    _ok(_lease_lost(client, source, reservation))

    assert _checkpoint_status(reservation) == "tombstoned"
    assert _outcome(source)["workspace_recovery_outcome"] == "runner_unavailable"
    assert (
        _outcome(source)["workspace_recovery_outcome_at"]
        == newer["workspace_recovery_outcome_at"]
    )


def test_lease_lost_retracts_the_just_completed_checkpoint_to_the_previous_one(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    second = _reserved(client, source, reuse=True)
    _completed(client, source, second)
    assert _last_good(source) == second["checkpoint_id"]

    response = _lease_lost(client, source, second, retract=True)
    body = _ok(response)
    assert body == {"checkpoint_id": second["checkpoint_id"], "status": "retracted"}
    _no_capability(response.text)
    assert _checkpoint_status(second) == "tombstoned"
    assert _checkpoint_status(first) == "complete"
    assert _last_good(source) == first["checkpoint_id"]
    assert _outcome(source)["workspace_recovery_outcome"] == "lease_lost"
    # New selection of the retracted checkpoint is refused; the previous one reads.
    _refused(_post(client, "acquire-reader", _reader_body(source, second)))
    _release(client, _reader(client, source, first))
    # A repeated retraction is not a second state change.
    state = _snapshot()
    repeated = _post(client, "fail", {
        **_authority(source), "checkpoint_id": second["checkpoint_id"], "reason": "lease_lost",
        "retract": True,
    })
    assert repeated.status_code in {200, 409}, repeated.text
    assert _snapshot() == state


def test_lease_lost_retraction_of_the_only_checkpoint_clears_the_pointer(
    recovery: None, client: Any
) -> None:
    _routes_exist(client)
    source = _seed()
    only = _reserved(client, source)
    _completed(client, source, only)
    # Superseded while the completion committed: the retraction still applies.
    _seed(agent_id=source["agent_id"], item_id=source["work_item_id"])
    assert _ok(_lease_lost(client, source, only, retract=True))["status"] == "retracted"
    assert _last_good(source) is None
    assert _checkpoint_status(only) == "tombstoned"


@pytest.mark.parametrize("case", ("other_reason", "not_last_good", "other_source"))
def test_a_complete_checkpoint_is_retracted_only_by_its_own_lease_loss(
    recovery: None, client: Any, case: str
) -> None:
    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    second = _reserved(client, source, reuse=True)
    _completed(client, source, second)
    target, authority, reason = second, _authority(source), "lease_lost"
    if case == "other_reason":
        reason = "upload_failed"
    elif case == "not_last_good":
        target = first
    else:
        authority = {**authority, "runtime_epoch": 4}
    before = _snapshot()
    _refused(_post(client, "fail", {
        **authority, "checkpoint_id": target["checkpoint_id"], "reason": reason,
        "retract": True,
    }))
    assert _snapshot() == before
    assert _last_good(source) == second["checkpoint_id"]


def test_lease_lost_without_retract_never_withdraws_a_complete_checkpoint(
    recovery: None, client: Any
) -> None:
    """A replay that lost its lease before its own completion cannot retract.

    Its reserve answered an already complete checkpoint, so only ``retract: true``
    (sent after the writer's own ``complete`` returned) may withdraw it.
    """

    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    second = _reserved(client, source, reuse=True)
    _completed(client, source, second)
    before = _snapshot()
    response = _lease_lost(client, source, second)
    _refused(response)
    assert response.json()["detail"]["code"] == "workspace_recovery.checkpoint_state"
    assert _snapshot() == before
    assert _checkpoint_status(second) == "complete"
    assert _last_good(source) == second["checkpoint_id"]
    _release(client, _reader(client, source, second))


@pytest.mark.parametrize("repeat", ("retract", "lease_lost", "upload_failed"))
def test_a_repeated_fail_after_retraction_answers_retracted_never_tombstoned(
    recovery: None, client: Any, repeat: str
) -> None:
    """``tombstoned`` tells the writer to discard; a once-complete checkpoint never is."""

    _routes_exist(client)
    source = _seed()
    first = _reserved(client, source)
    _completed(client, source, first)
    second = _reserved(client, source, reuse=True)
    _completed(client, source, second)
    reader = _reader(client, source, second)
    assert _ok(_lease_lost(client, source, second, retract=True))["status"] == "retracted"
    state = _snapshot()
    body: dict[str, Any] = {
        **_authority(source),
        "checkpoint_id": second["checkpoint_id"],
        "reason": "upload_failed" if repeat == "upload_failed" else "lease_lost",
    }
    if repeat == "retract":
        body["retract"] = True
    response = _post(client, "fail", body)
    assert _ok(response) == {"checkpoint_id": second["checkpoint_id"], "status": "retracted"}
    _no_capability(response.text)
    assert _snapshot() == state
    assert _last_good(source) == first["checkpoint_id"]
    _release(client, reader)
