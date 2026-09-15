"""Alertmanager source binding on the existing signed hook ingress (#2572).

One operator-controlled map, not a model guess: a signed Alertmanager-shaped
delivery either selects the mapped allowlisted repository and recorded revision
or visibly stops coding. Invalid signatures, replays, and unauthorized mappings
must not multiply work.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn, TurnSource
from curie_api.config import get_settings
from curie_api.hook_signing import derive
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine

EMAIL_ENDPOINT = "http://curie-mail-adapter:8080/"
EMAIL_ADAPTER = "agentmail-sandbox"
HOOK = "alertmanager"
REPO = "acme-corp/acme-bot"
REVISION = "0123456789abcdef0123456789abcdef01234567"
WORKLOAD = "curie-api"
STOP_PHRASE = "Coding is stopped"
MAPPING_PHRASE = "authorized source mapping"


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _secret_for(agent_id: str, generation: int = 0) -> str:
    return derive(get_settings().api_key, agent_id=agent_id, generation=generation)


def _alert_body(
    *,
    workload: str | list[str] | None = WORKLOAD,
    github_url: str | None = None,
    partition: str = "a" * 32,
    status: str = "firing",
    fingerprints: list[str] | None = None,
) -> bytes:
    """An Alertmanager-shaped notification with signer-injected partition.

    Alertmanager's groupKey is not a legal partition value; the signer injects
    ``curie_partition``. Tests send the body the API actually verifies.
    """

    if isinstance(workload, list):
        alerts = [
            {
                "status": status,
                "labels": {"curie_workload": item, "alertname": "Example"},
                "fingerprint": (fingerprints or [f"fp{i}" for i in range(len(workload))])[i],
                "startsAt": "2026-09-15T00:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "annotations": {},
            }
            for i, item in enumerate(workload)
        ]
        common: dict[str, str] = {"alertname": "Example"}
    elif workload is None:
        alerts = [
            {
                "status": status,
                "labels": {"alertname": "Example"},
                "fingerprint": (fingerprints or ["fp0"])[0],
                "startsAt": "2026-09-15T00:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "annotations": {},
            }
        ]
        common = {"alertname": "Example"}
    else:
        alerts = [
            {
                "status": status,
                "labels": {"curie_workload": workload, "alertname": "Example"},
                "fingerprint": (fingerprints or ["fp0"])[0],
                "startsAt": "2026-09-15T00:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "annotations": {"summary": "api error rate"},
            }
        ]
        common = {"curie_workload": workload, "alertname": "Example"}
    if github_url is not None:
        alerts[0]["annotations"]["runbook"] = github_url
    payload = {
        "version": "4",
        "groupKey": '{}:{alertname="Example"}',
        "status": status,
        "receiver": "curie",
        "groupLabels": {"alertname": "Example"},
        "commonLabels": common,
        "commonAnnotations": {},
        "externalURL": "http://alertmanager.example.com",
        "alerts": alerts,
        "curie_partition": partition,
    }
    return json.dumps(payload).encode()


def _bindings(
    *,
    repository: str = REPO,
    revision: str = REVISION,
    extra: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    mapping = {
        WORKLOAD: {"repository": repository, "revision": revision},
        **(extra or {}),
    }
    return {
        HOOK: {
            "workload_pointer": "/commonLabels/curie_workload",
            "map": mapping,
        }
    }


def _bind(
    client: TestClient,
    headers: dict[str, str],
    *,
    name: str,
    source_bindings: dict[str, Any] | None = None,
    hook_partitions: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "name": name,
        "channel": {
            "kind": "email",
            "address": f"{name}@example.test",
            "endpoint": EMAIL_ENDPOINT,
            "adapter": EMAIL_ADAPTER,
        },
        "hook_partitions": hook_partitions
        or {HOOK: {"pointer": "/curie_partition"}},
    }
    if source_bindings is not None:
        payload["source_bindings"] = source_bindings
    created = client.post("/agents", json=payload, headers=headers)
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _post(
    client: TestClient,
    agent_id: str,
    body: bytes,
    *,
    secret: str | None = None,
    signature: str | None = None,
    delivery_id: str | None = "dlv-1",
    hook: str = HOOK,
) -> Any:
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Curie-Signature-256"] = signature
    elif secret is not None:
        headers["X-Curie-Signature-256"] = _sign(secret, body)
    if delivery_id is not None:
        headers["X-Curie-Delivery-Id"] = delivery_id
    return client.post(f"/hooks/{agent_id}/{hook}", content=body, headers=headers)


def _queued(valkey: redis.Redis, stream: str) -> list[QueuedTurn]:
    entries = valkey.xrange(stream)
    return [QueuedTurn.model_validate_json(fields["payload"]) for _, fields in entries]


def _workspace_rows() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    sql_text(
                        "SELECT agent_id, conversation_id, repo_full_name, "
                        "revision, selected_by "
                        "FROM curie.thread_workspaces ORDER BY created_at, id"
                    )
                )
                return [dict(row._mapping) for row in result]
        finally:
            await engine.dispose()

    return asyncio.run(run())


@pytest.fixture
def allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_source_bindings_round_trip_and_empty_map_clears(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="maproundtrip",
        source_bindings=_bindings(),
    )
    fetched = hooks_client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["source_bindings"][HOOK]["map"][WORKLOAD]["repository"] == REPO
    assert fetched.json()["source_bindings"][HOOK]["map"][WORKLOAD]["revision"] == REVISION

    cleared = hooks_client.patch(
        f"/agents/{agent_id}",
        json={"source_bindings": {}},
        headers=auth_headers,
    )
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["source_bindings"] is None


def test_invalid_signature_creates_no_turn(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client, auth_headers, name="badsig", source_bindings=_bindings()
    )
    refused = _post(
        hooks_client,
        agent_id,
        _alert_body(),
        signature="sha256=" + ("ab" * 32),
    )
    assert refused.status_code == 401, refused.text
    assert valkey.xrange(runs_stream) == []
    assert _workspace_rows() == []


def test_duplicate_delivery_does_not_multiply_work(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client, auth_headers, name="replay", source_bindings=_bindings()
    )
    secret = _secret_for(agent_id)
    body = _alert_body()
    first = _post(hooks_client, agent_id, body, secret=secret, delivery_id="same")
    second = _post(hooks_client, agent_id, body, secret=secret, delivery_id="same")
    assert first.status_code == 200, first.text
    assert first.json()["duplicate"] is False
    assert second.status_code == 200, second.text
    assert second.json()["duplicate"] is True
    turns = _queued(valkey, runs_stream)
    assert len(turns) == 1
    assert turns[0].conversation_id == first.json()["conversation_id"]
    assert len(_workspace_rows()) == 1


def test_unique_allowlisted_mapping_selects_repo_and_revision(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client, auth_headers, name="mapped", source_bindings=_bindings()
    )
    body = _alert_body(partition="b" * 32)
    answer = _post(hooks_client, agent_id, body, secret=_secret_for(agent_id))
    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert turn.source is TurnSource.WEBHOOK
    assert turn.conversation_id.endswith(":" + "b" * 32)
    trusted, _, untrusted = turn.text.partition("<untrusted-hook-payload>")
    assert MAPPING_PHRASE in trusted
    assert REPO in trusted
    assert REVISION in trusted
    assert "https://github.com/evil-corp/evil" not in trusted
    rows = _workspace_rows()
    assert len(rows) == 1
    assert rows[0]["repo_full_name"] == REPO
    assert rows[0]["revision"] == REVISION
    assert rows[0]["selected_by"] == f"hook:{HOOK}"
    assert str(rows[0]["conversation_id"]) == turn.conversation_id


def test_payload_github_url_cannot_select_a_repository(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="guessed",
        source_bindings=_bindings(),
    )
    body = _alert_body(
        workload=None,
        github_url="https://github.com/evil-corp/evil",
    )
    answer = _post(hooks_client, agent_id, body, secret=_secret_for(agent_id))
    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    trusted, _, untrusted = turn.text.partition("<untrusted-hook-payload>")
    assert STOP_PHRASE in trusted
    assert "https://github.com/evil-corp/evil" not in trusted
    assert _workspace_rows() == []
    assert "https://github.com/evil-corp/evil" in untrusted or "evil-corp" in untrusted


def test_missing_workload_stops_coding_and_still_enqueues_one_investigation(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client, auth_headers, name="missing", source_bindings=_bindings()
    )
    answer = _post(
        hooks_client,
        agent_id,
        _alert_body(workload=None),
        secret=_secret_for(agent_id),
    )
    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert STOP_PHRASE in turn.text
    assert "authorized" in turn.text.lower() or "mapping" in turn.text.lower()
    assert _workspace_rows() == []


def test_ambiguous_workloads_stop_coding(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    extra = {
        "curie-worker": {
            "repository": "acme-corp/acme-worker",
            "revision": REVISION,
        }
    }
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="ambiguous",
        source_bindings=_bindings(extra=extra),
    )
    body = json.dumps(
        {
            "version": "4",
            "status": "firing",
            "groupKey": "group",
            "commonLabels": {},
            "alerts": [
                {"labels": {"curie_workload": WORKLOAD}, "fingerprint": "fp1"},
                {"labels": {"curie_workload": "curie-worker"}, "fingerprint": "fp2"},
            ],
            "curie_partition": "c" * 32,
        }
    ).encode()
    answer = _post(hooks_client, agent_id, body, secret=_secret_for(agent_id))
    assert answer.status_code == 200, answer.text
    (turn,) = _queued(valkey, runs_stream)
    assert "more than one mapped" in turn.text.lower()
    assert STOP_PHRASE in turn.text
    assert _workspace_rows() == []


def test_unauthorized_mapping_creates_no_workspace(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client,
        auth_headers,
        name="unauthorized",
        source_bindings=_bindings(repository="evil-corp/evil"),
    )
    answer = _post(
        hooks_client, agent_id, _alert_body(), secret=_secret_for(agent_id)
    )
    assert answer.status_code == 403, answer.text
    assert valkey.xrange(runs_stream) == []
    assert _workspace_rows() == []


def test_wrong_hook_binding_does_not_use_the_map(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    valkey: redis.Redis,
    runs_stream: str,
    clean_db: None,
    allowlist: None,
) -> None:
    agent_id = _bind(
        hooks_client, auth_headers, name="wronghook", source_bindings=_bindings()
    )
    answer = _post(
        hooks_client,
        agent_id,
        _alert_body(),
        secret=_secret_for(agent_id),
        hook="otherhook",
        delivery_id="other-1",
    )
    assert answer.status_code == 403, answer.text
    assert valkey.xrange(runs_stream) == []
    assert _workspace_rows() == []


def test_create_rejects_a_map_entry_outside_the_allowlist_shape(
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    created = hooks_client.post(
        "/agents",
        json={
            "name": "badrepo",
            "channel": {
                "kind": "email",
                "address": "badrepo@example.test",
                "endpoint": EMAIL_ENDPOINT,
                "adapter": EMAIL_ADAPTER,
            },
            "source_bindings": _bindings(repository="not a repo"),
        },
        headers=auth_headers,
    )
    assert created.status_code == 422, created.text
