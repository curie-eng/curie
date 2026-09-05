"""Durable, approval-gated publication control-plane contracts.

The API owns only durable state and operator-credential redemption.  Resolving a
publication approval never enqueues a model turn and never performs Kubernetes
or GitHub side effects; the worker reconciles the durable publication row.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import subprocess
import tarfile
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote, urlsplit

import channel_protocol
import httpx
import pytest
import redis
import redis.asyncio as aioredis
from curie_api import approval_principal, crud
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_api.resumequeue import ResumeQueue
from curie_api.schemas import ChannelBindingWrite, PublicationCreate
from curie_api.sweeper import sweep_expired_approvals
from curie_telemetry import record_metric
from curie_test_support.valkey import connect_or_skip
from fastapi import Response
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
WORKER_TOKEN = "remote-dev-publication-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
PATCH_LIMIT = 900_000
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
FIRST_REVISION_SHA = "1123456789abcdef0123456789abcdef01234567"
SECOND_REVISION_SHA = "2123456789abcdef0123456789abcdef01234567"
EXTERNAL_REVISION_SHA = "3123456789abcdef0123456789abcdef01234567"
PR_NUMBER = 123
PR_URL = f"https://github.com/{REPO}/pull/{PR_NUMBER}"
CLUSTER_MESSAGE_ADAPTER = "curie-cluster-message"
_PUBLICATION_TRACEPARENT = "00-7123456789abcdef0123456789abcdef-7123456789abcdef-01"
_REPLAY_TRACEPARENT = "00-8123456789abcdef0123456789abcdef-8123456789abcdef-01"


@pytest.fixture
def publication_stack(
    _disposable_db: Any, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, str]]:
    runs_stream = f"test:curie:publication-runs:{uuid.uuid4().hex}"
    monkeypatch.setenv("RUNS_STREAM", runs_stream)
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_publication_operator")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    _RESOLVERS.clear()
    with TestClient(create_app()) as test_client:
        yield test_client, runs_stream
    valkey = connect_or_skip(decode_responses=True)
    valkey.delete(runs_stream, f"{runs_stream}:dead")
    valkey.close()
    _RESOLVERS.clear()
    get_settings.cache_clear()


def _create_deployment(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    name: str | None = None,
    channel: str = "C0EXAMPLE1",
    workspace_enabled: bool = True,
) -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:8]
    agent_response = client.post(
        "/agents",
        json={
            "name": name or f"publisher-{suffix}",
            "channel": {"kind": "slack", "address": channel},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert agent_response.status_code == 201, agent_response.text
    agent_id = agent_response.json()["id"]
    version_response = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version_response.status_code == 201, version_response.text
    deployment_response = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_response.json()["id"],
            "environment": "dev",
            "workspace_enabled": workspace_enabled,
        },
        headers=auth_headers,
    )
    assert deployment_response.status_code == 201, deployment_response.text
    return deployment_response.json()


def _publication_payload(
    deployment_id: str,
    *,
    patch: bytes = b"diff --git a/README.md b/README.md\n",
    dedupe_key: str | None = None,
    author: str = "U0REQUEST1",
    expires_in_seconds: int | None = 600,
    conversation_id: str | None = None,
    base_sha: str = BASE_SHA,
) -> dict[str, Any]:
    return {
        "deployment_id": deployment_id,
        "conversation_id": conversation_id or f"thread-{uuid.uuid4().hex[:8]}",
        "repo_full_name": REPO,
        "author": author,
        "summary": "Publish the repository changes",
        "reply_kind": "slack",
        "reply_channel": "C0EXAMPLE1",
        "reply_placeholder": "1700000000.000001",
        "dedupe_key": dedupe_key or f"publish-{uuid.uuid4().hex}",
        "base_sha": base_sha,
        "patch_b64": base64.b64encode(patch).decode(),
        "changed_paths": ["README.md"],
        "expires_in_seconds": expires_in_seconds,
    }


def _workspace_identity(payload: Mapping[str, Any]) -> str:
    """Return the canonical authorization key for either worker request shape."""

    if payload.get("reply_conversation_id") is not None:
        return str(payload["conversation_id"])

    return channel_protocol.scoped_conversation_id(
        str(payload["reply_kind"]),
        str(payload["reply_channel"]),
        str(payload["conversation_id"]),
    )


def test_publication_schema_refuses_github_workflow_changes() -> None:
    payload = _publication_payload(str(uuid.uuid4()))
    payload["changed_paths"] = [".github/workflows/publish.yml"]

    with pytest.raises(ValidationError, match="workflow changes cannot be published"):
        PublicationCreate.model_validate(payload)


def test_publication_schema_accepts_the_builtin_reply_adapter_without_an_endpoint() -> None:
    reply_ref = str(uuid.uuid4())
    payload = _publication_payload(str(uuid.uuid4()))
    payload.update(
        reply_placeholder=reply_ref,
        reply_endpoint=None,
        reply_adapter=CLUSTER_MESSAGE_ADAPTER,
    )

    publication = PublicationCreate.model_validate(payload)

    assert publication.reply_placeholder == reply_ref
    assert publication.reply_endpoint is None
    assert publication.reply_adapter == CLUSTER_MESSAGE_ADAPTER

    # The exception is purpose-scoped. Operators still cannot configure the
    # built-in worker adapter on a durable channel binding and shadow its trust.
    with pytest.raises(ValidationError, match="reserved"):
        ChannelBindingWrite.model_validate(
            {
                "kind": "email",
                "address": "ops@example.test",
                "endpoint": "https://adapter.example.test/replies",
                "adapter": CLUSTER_MESSAGE_ADAPTER,
            }
        )


@pytest.mark.parametrize(
    "route",
    [
        {"reply_endpoint": None, "reply_adapter": "agentmail-sandbox"},
        {
            "reply_endpoint": "https://adapter.example.test/replies",
            "reply_adapter": None,
        },
    ],
)
def test_ordinary_publication_adapters_still_require_both_route_halves(
    route: dict[str, str | None],
) -> None:
    payload = _publication_payload(str(uuid.uuid4()))
    payload.update(route)

    with pytest.raises(ValidationError, match="endpoint and adapter together"):
        PublicationCreate.model_validate(payload)

    payload.update(
        reply_endpoint="https://adapter.example.test/replies",
        reply_adapter="agentmail-sandbox",
    )
    ordinary = PublicationCreate.model_validate(payload)
    assert ordinary.reply_endpoint == "https://adapter.example.test/replies"
    assert ordinary.reply_adapter == "agentmail-sandbox"


def test_builtin_reply_adapter_and_ref_persist_on_both_publication_rows(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    reply_ref = str(uuid.uuid4())
    payload = _publication_payload(deployment["id"], dedupe_key="builtin-cluster-message-reply")
    payload.update(
        reply_placeholder=reply_ref,
        reply_endpoint=None,
        reply_adapter=CLUSTER_MESSAGE_ADAPTER,
    )

    status_code, publication = _create_publication(client, payload)

    assert status_code == 201
    stored = _rows(
        "SELECT p.reply_placeholder AS publication_ref, "
        "p.reply_endpoint AS publication_endpoint, "
        "p.reply_adapter AS publication_adapter, "
        "a.reply_placeholder AS approval_ref, "
        "a.reply_endpoint AS approval_endpoint, "
        "a.reply_adapter AS approval_adapter "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id = :id",
        {"id": publication["id"]},
    )[0]
    assert stored == {
        "publication_ref": reply_ref,
        "publication_endpoint": None,
        "publication_adapter": CLUSTER_MESSAGE_ADAPTER,
        "approval_ref": reply_ref,
        "approval_endpoint": None,
        "approval_adapter": CLUSTER_MESSAGE_ADAPTER,
    }


def _create_publication(
    client: TestClient,
    payload: dict[str, Any],
    *,
    request_headers: Mapping[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    selected = client.post(
        f"/v1/internal/workspaces/{payload['deployment_id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    response = client.post(
        "/v1/internal/publications",
        json=payload,
        headers={**WORKER_HEADERS, **dict(request_headers or {})},
    )
    assert response.status_code in (200, 201), response.text
    return response.status_code, response.json()


def _get_lineage(
    client: TestClient,
    *,
    deployment_id: str,
    conversation_id: str,
    repo_full_name: str = REPO,
) -> Any:
    """Call the current worker endpoint with its canonical scoped identity."""

    return client.get(
        "/v1/internal/publications/lineage",
        params={
            "deployment_id": deployment_id,
            "conversation_id": channel_protocol.scoped_conversation_id(
                "slack", "C0EXAMPLE1", conversation_id
            ),
            "repo_full_name": repo_full_name,
        },
        headers=WORKER_HEADERS,
    )


def _advance_lineage(
    client: TestClient,
    publication_id: str,
    *,
    expected_version: int,
    expected_head_sha: str | None,
    head_sha: str,
    state: str = "open",
    pr_number: int = PR_NUMBER,
    pr_url: str = PR_URL,
) -> Any:
    return client.patch(
        f"/v1/internal/publications/{publication_id}/lineage",
        json={
            "expected_version": expected_version,
            "expected_head_sha": expected_head_sha,
            "state": state,
            "pr_number": pr_number,
            "pr_url": pr_url,
            "head_sha": head_sha,
        },
        headers=WORKER_HEADERS,
    )


def _mark_outcome_history_ready(publication_id: str) -> None:
    """Stand in for the worker transcript outbox in API-only lineage tests."""

    _execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() "
        "WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )


def _open_lineage(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    conversation_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key=f"{conversation_id}-revision-one",
        ),
    )
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    advanced = _advance_lineage(
        client,
        publication["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    _mark_outcome_history_ready(publication["id"])
    return deployment, publication


def _get_lineage_with_http(
    client: TestClient,
    *,
    deployment_id: str,
    conversation_id: str,
    handler: Any,
) -> Any:
    injected = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        return _get_lineage(
            client,
            deployment_id=deployment_id,
            conversation_id=conversation_id,
        )
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())


def _healthy_github_pr(*, branch: str, head_sha: str) -> httpx.Response:
    """A verified-open GitHub response for lineage/CAS control reads."""

    return httpx.Response(
        200,
        json={
            "number": PR_NUMBER,
            "html_url": PR_URL,
            "state": "open",
            "merged": False,
            "base": {"ref": "main", "sha": BASE_SHA},
            "head": {"ref": branch, "sha": head_sha},
        },
    )


def _rows(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _execute(query: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(query), params or {})
        finally:
            await engine.dispose()

    asyncio.run(run())


def _make_publication_legacy(
    publication_id: str,
    deployment_id: str,
    payload: Mapping[str, Any],
) -> None:
    """Model an exact pre-scoping row without inventing a canonical identity."""

    _execute(
        "UPDATE curie.publications SET workspace_conversation_id = NULL "
        "WHERE id = :publication_id",
        {"publication_id": publication_id},
    )
    _execute(
        "UPDATE curie.thread_workspaces SET conversation_id = :bare "
        "WHERE selected_by_deployment_id = :deployment_id "
        "AND conversation_id = :scoped",
        {
            "bare": payload["conversation_id"],
            "deployment_id": deployment_id,
            "scoped": _workspace_identity(payload),
        },
    )


def _counts() -> tuple[int, int]:
    row = _rows(
        "SELECT (SELECT count(*) FROM curie.approvals) AS approvals, "
        "(SELECT count(*) FROM curie.publications) AS publications"
    )[0]
    return int(row["approvals"]), int(row["publications"])


def test_publication_approval_privately_preserves_the_first_inbound_carrier(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Publication creation is the approval sibling, including replay privacy."""

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(
        deployment["id"], dedupe_key="publication-traceparent-example"
    )
    payload["traceparent"] = _REPLAY_TRACEPARENT

    first_status, first = _create_publication(
        client,
        payload,
        request_headers={"traceparent": _PUBLICATION_TRACEPARENT},
    )
    replay_status, replay = _create_publication(
        client,
        payload,
        request_headers={"traceparent": _REPLAY_TRACEPARENT},
    )

    assert first_status == 201
    assert replay_status == 200
    assert replay["id"] == first["id"]
    assert "traceparent" not in first
    assert "traceparent" not in replay
    stored = _rows(
        "SELECT a.traceparent FROM curie.approvals a "
        "JOIN curie.publications p ON p.approval_id = a.id "
        "WHERE p.id = :id",
        {"id": uuid.UUID(first["id"])},
    )
    assert stored == [{"traceparent": _PUBLICATION_TRACEPARENT}]


def test_publication_with_malformed_carrier_persists_null(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(deployment["id"]),
        request_headers={"traceparent": "not-w3c"},
    )

    stored = _rows(
        "SELECT a.traceparent FROM curie.approvals a "
        "JOIN curie.publications p ON p.approval_id = a.id "
        "WHERE p.id = :id",
        {"id": uuid.UUID(publication["id"])},
    )
    assert stored == [{"traceparent": None}]


def test_publication_persistence_refuses_casefolded_git_metadata_path(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"])
    payload["changed_paths"] = [".GIT/config"]

    refused = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)

    assert refused.status_code == 422
    assert "safe repository-relative paths" in refused.text
    assert _counts() == (0, 0)


def _stream_entries(stream: str) -> list[Any]:
    valkey: redis.Redis = connect_or_skip(decode_responses=True)
    try:
        return list(valkey.xrange(stream))
    finally:
        valkey.close()


def _resolve(
    client: TestClient,
    auth_headers: dict[str, str],
    approval_id: str,
    *,
    decision: str = "approved",
    actor: str = "U0REQUEST1",
    channel: str = "C0EXAMPLE1",
    note: str | None = None,
) -> Any:
    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=actor,
        kind="chat",
        actor_channel=channel,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(datetime.now(UTC).timestamp()) + 60,
    )
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision, "note": note},
        headers={
            **auth_headers,
            "X-Curie-Approval-Principal": token,
        },
    )


def _assert_patch_private(value: Any, private_fragment: str) -> None:
    def keys(node: Any) -> Iterator[str]:
        if isinstance(node, dict):
            for key, child in node.items():
                yield str(key)
                yield from keys(child)
        elif isinstance(node, list):
            for child in node:
                yield from keys(child)

    rendered = json.dumps(value, sort_keys=True)
    assert {"patch", "patch_b64", "patch_bytes"}.isdisjoint(keys(value))
    assert private_fragment not in rendered


def test_publication_create_is_atomic_private_and_idempotent(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    private_patch = b"private-diff-material-that-must-not-leave-the-api"
    payload = _publication_payload(
        deployment["id"], patch=private_patch, dedupe_key="event-publish-once"
    )

    first_status, first = _create_publication(client, payload)
    second_status, second = _create_publication(client, payload)

    assert first_status == 201
    assert second_status == 200
    assert second["id"] == first["id"]
    assert second["approval_id"] == first["approval_id"]
    assert first["repo_full_name"] == REPO
    assert first["status"] == "pending"
    assert first["version"] == 1
    assert _counts() == (1, 1)
    _assert_patch_private(first, base64.b64encode(private_patch).decode())

    got = client.get(f"/publications/{first['id']}", headers=auth_headers)
    listed = client.get("/publications", headers=auth_headers)
    approval = client.get(f"/approvals/{first['approval_id']}", headers=auth_headers)
    assert got.status_code == listed.status_code == approval.status_code == 200
    _assert_patch_private(got.json(), base64.b64encode(private_patch).decode())
    _assert_patch_private(listed.json(), base64.b64encode(private_patch).decode())
    _assert_patch_private(approval.json(), base64.b64encode(private_patch).decode())

    stored = _rows(
        "SELECT octet_length(patch_bytes) AS size, repo_full_name, approval_id "
        "FROM curie.publications WHERE id = :id",
        {"id": first["id"]},
    )[0]
    assert stored["size"] == len(private_patch)
    assert stored["repo_full_name"] == REPO
    assert str(stored["approval_id"]) == first["approval_id"]

    changed_replay = dict(payload)
    changed_replay["patch_b64"] = base64.b64encode(b"different patch").decode()
    conflict = client.post("/v1/internal/publications", json=changed_replay, headers=WORKER_HEADERS)
    assert conflict.status_code == 409
    assert _counts() == (1, 1)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("repo_full_name", REPO + "\n"),
        ("title", "t" * 257),
        ("body", "b" * 65_537),
    ],
)
def test_publication_create_rejects_noncanonical_or_oversized_text(field: str, value: str) -> None:
    from curie_api.schemas import PublicationCreate

    payload = _publication_payload(str(uuid.uuid4()))
    payload[field] = value
    with pytest.raises(ValidationError):
        PublicationCreate.model_validate(payload)


def test_publication_replay_refuses_a_changed_conversation_or_agent_identity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="identity-replay")
    _create_publication(client, payload)

    wrong_thread = dict(payload, conversation_id="thread-other")
    rejected_thread = client.post(
        "/v1/internal/publications", json=wrong_thread, headers=WORKER_HEADERS
    )
    assert rejected_thread.status_code == 409

    wrong_channel = dict(payload, reply_channel="C0EXAMPLE2")
    rejected_channel = client.post(
        "/v1/internal/publications", json=wrong_channel, headers=WORKER_HEADERS
    )
    assert rejected_channel.status_code == 409

    wrong_kind = dict(payload, reply_kind="email")
    rejected_kind = client.post(
        "/v1/internal/publications", json=wrong_kind, headers=WORKER_HEADERS
    )
    assert rejected_kind.status_code == 409

    other_deployment = _create_deployment(client, auth_headers, channel="C0EXAMPLE2")
    wrong_agent = dict(payload, deployment_id=other_deployment["id"])
    rejected_agent = client.post(
        "/v1/internal/publications", json=wrong_agent, headers=WORKER_HEADERS
    )
    assert rejected_agent.status_code == 409
    assert _counts() == (1, 1)


def test_null_workspace_publication_replay_refuses_bare_authorization_fallback(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="legacy-exact-replay")
    _, publication = _create_publication(client, payload)
    _make_publication_legacy(publication["id"], deployment["id"], payload)

    exact = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )
    assert exact.status_code == 409
    assert _rows(
        "SELECT workspace_conversation_id FROM curie.publications WHERE id = :id",
        {"id": publication["id"]},
    ) == [{"workspace_conversation_id": None}], "legacy replay must not fabricate history"

    crossed = client.post(
        "/v1/internal/publications",
        json={**payload, "reply_channel": "C0EXAMPLE2"},
        headers=WORKER_HEADERS,
    )
    assert crossed.status_code == 409

    assert _counts() == (1, 1)


def test_legacy_publication_replay_rechecks_the_current_allowlist(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="legacy-allowlist-replay")
    _, publication = _create_publication(client, payload)
    _make_publication_legacy(publication["id"], deployment["id"], payload)
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", "[]")
    get_settings.cache_clear()

    replay = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )
    assert replay.status_code == 409
    assert _counts() == (1, 1)


def test_publication_splits_scoped_thread_identity_from_slack_reply_identity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore
    from curie_worker.slack_sink import _thread_ts

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    scoped_thread = "slack:C0EXAMPLE1:1700000000.000100"
    reply_thread = "1700000000.000100"
    payload = _publication_payload(
        deployment["id"],
        conversation_id=scoped_thread,
        dedupe_key="scoped-thread-bare-slack-reply",
    )
    payload["reply_conversation_id"] = reply_thread

    status_code, publication = _create_publication(client, payload)

    assert status_code == 201
    assert _rows(
        "SELECT a.conversation_id AS reply_conversation_id, "
        "l.conversation_id AS lineage_conversation_id "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "WHERE p.id = :id",
        {"id": publication["id"]},
    ) == [
        {
            "reply_conversation_id": reply_thread,
            "lineage_conversation_id": scoped_thread,
        }
    ]
    assert _thread_ts(reply_thread) == reply_thread
    assert _thread_ts(scoped_thread) is None

    replay = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )
    assert replay.status_code == 200, replay.text
    changed_reply = dict(payload, reply_conversation_id="1700000000.000200")
    conflict = client.post(
        "/v1/internal/publications", json=changed_reply, headers=WORKER_HEADERS
    )
    assert conflict.status_code == 409

    async def claim_outboxes() -> tuple[Any, Any]:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="split-identity-outbox"
        )
        try:
            card = await store.claim_pending_card()
            assert card is not None
            await store.mark_card_delivered(card.publication_id)
            approved = _resolve(client, auth_headers, publication["approval_id"])
            assert approved.status_code == 200, approved.text
            work = await store.claim_next()
            assert work is not None
            await store.persist_result(
                work.publication_id,
                outcome="failed",
                pr_url=None,
                error="safe terminal fixture",
            )
            cleanup = await store.claim_pending_cleanup()
            assert cleanup is not None
            await store.mark_cleanup_completed(cleanup.publication_id)
            result = await store.pending_result(work.publication_id)
            assert result is not None
            return card, result
        finally:
            await engine.dispose()

    card, result = asyncio.run(claim_outboxes())
    assert card.target.conversation_id == reply_thread
    assert result.target.conversation_id == reply_thread
    assert result.workspace_conversation_id == scoped_thread


def test_old_worker_publication_falls_back_to_one_conversation_identity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(
        deployment["id"],
        conversation_id="legacy-worker-conversation",
        dedupe_key="legacy-worker-without-reply-conversation",
    )

    _, publication = _create_publication(client, payload)

    stored = _rows(
        "SELECT a.conversation_id AS reply_conversation_id, "
        "l.conversation_id AS lineage_conversation_id "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "WHERE p.id = :id",
        {"id": publication["id"]},
    )
    assert stored == [
        {
            "reply_conversation_id": payload["conversation_id"],
            "lineage_conversation_id": _workspace_identity(payload),
        }
    ]

    _execute(
        "UPDATE curie.publications "
        "SET lineage_id = NULL, status = 'denied', patch_bytes = NULL, "
        "approval_card_delivery_dead_lettered_at = now(), terminal_at = now() "
        "WHERE id = :id",
        {"id": publication["id"]},
    )

    async def claim_legacy_result() -> Any:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="legacy-lineageless-result"
        )
        try:
            return await store.pending_result(uuid.UUID(publication["id"]))
        finally:
            await engine.dispose()

    result = asyncio.run(claim_legacy_result())
    assert result is not None
    assert result.target.conversation_id == payload["conversation_id"]
    assert result.workspace_conversation_id == _workspace_identity(payload)


def test_exact_publication_replay_rechecks_the_current_thread_selection(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="selection-revocation-replay")
    _create_publication(client, payload)

    async def revoke_then_reselect() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "DELETE FROM curie.thread_workspaces "
                        "WHERE selected_by_deployment_id = :deployment_id "
                        "AND conversation_id = :conversation_id"
                    ),
                    {
                        "deployment_id": deployment["id"],
                        "conversation_id": _workspace_identity(payload),
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(revoke_then_reselect())
    reselected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": "acme-corp/acme-api",
        },
        headers=WORKER_HEADERS,
    )
    assert reselected.status_code == 200, reselected.text
    replay = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)
    assert replay.status_code == 409


def test_exact_publication_replay_rechecks_the_current_allowlist(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="allowlist-revocation-replay")
    _create_publication(client, payload)
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", "[]")
    get_settings.cache_clear()

    replay = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)
    assert replay.status_code == 409


def test_publication_credential_resolution_does_not_block_the_event_loop(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synchronous installation-token mint must run outside FastAPI's loop."""

    from curie_api.routers.publications import redeem_publication_credential

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text

    resolver_started = threading.Event()
    loop_progressed = threading.Event()

    def blocking_resolver(_: str, __: Any) -> tuple[str, str]:
        resolver_started.set()
        if not loop_progressed.wait(timeout=0.5):
            raise AssertionError("credential resolver blocked the event loop")
        return "https://github.com/acme-corp/acme-bot.git", "Basic test"

    monkeypatch.setattr(
        "curie_api.routers.publications.resolve_repository_credential", blocking_resolver
    )

    async def exercise() -> str:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                call = asyncio.create_task(
                    redeem_publication_credential(uuid.UUID(publication["id"]), session, Response())
                )

                async def prove_progress() -> None:
                    await asyncio.to_thread(resolver_started.wait)
                    await asyncio.sleep(0)
                    loop_progressed.set()

                progress = asyncio.create_task(prove_progress())
                result = await asyncio.wait_for(call, timeout=2)
                await progress
                return result.authorization_header
        finally:
            await engine.dispose()

    assert asyncio.run(exercise()) == "Basic test"


def test_publication_repo_must_match_the_thread_selection(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"])
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    payload["repo_full_name"] = "acme-corp/acme-api"

    refused = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)

    assert refused.status_code == 409
    assert "differs" in refused.json()["detail"]
    assert _counts() == (0, 0)


def test_publication_without_a_selected_workspace_is_inert(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="generic-no-selection")

    refused = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )

    assert refused.status_code == 409
    assert _counts() == (0, 0)


@pytest.mark.parametrize(
    "changed_route",
    [
        {"reply_kind": "email"},
        {"reply_channel": "C0EXAMPLE2"},
        {"conversation_id": "1700000000.000200"},
    ],
)
def test_publication_route_substitution_derives_an_unselected_identity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    changed_route: dict[str, str],
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], dedupe_key="route-substitution")
    payload["conversation_id"] = "1700000000.000100"
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    refused = client.post(
        "/v1/internal/publications",
        json={**payload, **changed_route},
        headers=WORKER_HEADERS,
    )

    assert refused.status_code == 409
    assert _counts() == (0, 0)


def test_publication_card_outbox_acks_original_turn_before_job_claim(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Real Postgres owns card retry; the source turn never reruns its model."""

    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(
        deployment["id"], dedupe_key="event-card-outbox-original-snapshot"
    )
    _, publication = _create_publication(client, payload)

    # A changed replay is still rejected by the atomic API facts. Card delivery
    # cannot depend on reclaiming this event, because a second model snapshot
    # would hit this conflict and strand the first approval.
    changed = dict(payload)
    changed["patch_b64"] = base64.b64encode(b"changed second-run snapshot").decode()
    replay = client.post("/v1/internal/publications", json=changed, headers=WORKER_HEADERS)
    assert replay.status_code == 409

    async def exercise() -> tuple[Any, Any, Any]:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="card-outbox-test",
            lease_seconds=10,
            result_max_attempts=2,
        )
        try:
            card = await store.claim_pending_card()
            assert card is not None
            approved = _resolve(client, auth_headers, publication["approval_id"])
            assert approved.status_code == 200, approved.text
            before_card = await store.claim_next()
            await store.mark_card_delivered(card.publication_id)
            after_card = await store.claim_next()
            return card, before_card, after_card
        finally:
            await engine.dispose()

    card, before_card, after_card = asyncio.run(exercise())
    assert str(card.publication_id) == publication["id"]
    assert str(card.approval_id) == publication["approval_id"]
    assert before_card is None, "Job mutation is gated on durable card delivery"
    assert after_card is not None
    assert after_card.patch == base64.b64decode(payload["patch_b64"])


def test_publication_card_outbox_dead_letters_to_terminal_result(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(deployment["id"], dedupe_key="event-card-outbox-dead-letter"),
    )

    async def exhaust() -> Any:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="card-dead-letter-test",
            result_max_attempts=2,
        )
        try:
            first = await store.claim_pending_card()
            assert first is not None
            await store.retry_card_delivery(first.publication_id, error="Slack unavailable")
            second = await store.claim_pending_card()
            assert second is not None
            await store.retry_card_delivery(second.publication_id, error="Slack unavailable")
            cleanup = await store.claim_pending_cleanup()
            assert cleanup is not None
            await store.mark_cleanup_completed(cleanup.publication_id)
            return await store.pending_result(second.publication_id)
        finally:
            await engine.dispose()

    result = asyncio.run(exhaust())
    assert result is not None and result.outcome == "failed"
    stored = _rows(
        "SELECT p.status, p.patch_bytes IS NULL AS patch_cleared, "
        "p.approval_card_delivery_attempts, "
        "p.approval_card_delivery_dead_lettered_at IS NOT NULL AS dead_lettered, "
        "a.status AS approval_status "
        "FROM curie.publications p JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id = :id",
        {"id": publication["id"]},
    )[0]
    assert stored == {
        "status": "failed",
        "patch_cleared": True,
        "approval_card_delivery_attempts": 2,
        "dead_lettered": True,
        "approval_status": "expired",
    }


@pytest.mark.parametrize("legacy", [False, True])
def test_publication_result_store_separates_history_identity_from_bare_reply_route(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    legacy: bool,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(
        deployment["id"], dedupe_key=f"history-identity-{'legacy' if legacy else 'scoped'}"
    )
    payload["conversation_id"] = "1700000000.000100"
    _, publication = _create_publication(client, payload)
    if legacy:
        _make_publication_legacy(publication["id"], deployment["id"], payload)
    _execute(
        "UPDATE curie.publications SET status = 'failed', patch_bytes = NULL, "
        "terminal_at = now(), approval_card_delivery_dead_lettered_at = now(), "
        "resource_cleanup_completed_at = now() WHERE id = :id",
        {"id": publication["id"]},
    )

    async def claim() -> Any:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner=f"history-identity-{'legacy' if legacy else 'scoped'}",
        )
        try:
            return await store.pending_result(uuid.UUID(publication["id"]))
        finally:
            await engine.dispose()

    result = asyncio.run(claim())
    assert result is not None
    assert result.workspace_conversation_id == _workspace_identity(payload)
    assert result.target.kind == payload["reply_kind"]
    assert result.target.address == payload["reply_channel"]
    assert result.target.conversation_id == payload["conversation_id"]


def test_publication_card_and_result_claims_survive_process_replacement(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Expired leases reclaim, while failed result delivery observes backoff."""

    from curie_worker.publication_store import (
        PostgresPublicationStore,
        PublicationStoreError,
    )

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(deployment["id"], dedupe_key="process-replacement"),
    )

    async def exercise() -> tuple[int, int, int, int, int]:
        engine = create_async_engine(get_settings().database_url)
        first = PostgresPublicationStore(
            engine, schema="curie", lease_owner="replaced-worker", lease_seconds=60
        )
        replacement = PostgresPublicationStore(
            engine, schema="curie", lease_owner="replacement-worker", lease_seconds=60
        )
        try:
            abandoned_card = await first.claim_pending_card()
            assert abandoned_card is not None
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.publications "
                        "SET approval_card_lease_expires_at = now() - interval '1 second' "
                        "WHERE id = :id"
                    ),
                    {"id": abandoned_card.publication_id},
                )
            reclaimed_card = await replacement.claim_pending_card()
            assert reclaimed_card is not None
            await replacement.mark_card_delivered(reclaimed_card.publication_id)

            approved = _resolve(
                client,
                auth_headers,
                publication["approval_id"],
                note="Approved for the release fixture",
            )
            assert approved.status_code == 200, approved.text
            job = await replacement.claim_next()
            assert job is not None
            await replacement.persist_result(
                job.publication_id,
                outcome="failed",
                pr_url=None,
                error="safe terminal fixture",
            )
            cleanup = await replacement.claim_pending_cleanup()
            assert cleanup is not None
            await replacement.mark_cleanup_completed(cleanup.publication_id)

            abandoned_result = await first.pending_result(job.publication_id)
            assert abandoned_result is not None
            assert abandoned_result.resolved_by == "U0REQUEST1"
            assert abandoned_result.resolution_note == "Approved for the release fixture"
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.publications "
                        "SET lease_expires_at = now() - interval '1 second' "
                        "WHERE id = :id"
                    ),
                    {"id": job.publication_id},
                )
            reclaimed_result = await replacement.pending_result(job.publication_id)
            assert reclaimed_result is not None
            await replacement.retry_result_delivery(
                reclaimed_result.publication_id, error="reply unavailable"
            )
            assert await first.pending_result(job.publication_id) is None
            async with engine.begin() as connection:
                backoff_active = (
                    await connection.execute(
                        text(
                            "SELECT lease_owner IS NULL "
                            "AND lease_expires_at > now() "
                            "FROM curie.publications WHERE id = :id"
                        ),
                        {"id": job.publication_id},
                    )
                ).scalar_one()
                assert backoff_active is True
                await connection.execute(
                    text(
                        "UPDATE curie.publications "
                        "SET lease_expires_at = now() - interval '1 second' "
                        "WHERE id = :id"
                    ),
                    {"id": job.publication_id},
                )
            retried_result = await first.pending_result(job.publication_id)
            assert retried_result is not None
            with pytest.raises(
                PublicationStoreError, match="publication result delivery CAS was lost"
            ):
                await first.mark_result_delivered(retried_result.publication_id)
            await first.mark_outcome_history_ready(retried_result.publication_id)
            await first.mark_result_delivered(retried_result.publication_id)
            return (
                abandoned_card.attempt,
                reclaimed_card.attempt,
                abandoned_result.attempt,
                reclaimed_result.attempt,
                retried_result.attempt,
            )
        finally:
            await engine.dispose()

    assert asyncio.run(exercise()) == (0, 0, 0, 0, 1)
    attempts = _rows(
        "SELECT approval_card_delivery_attempts, result_delivery_attempts "
        "FROM curie.publications WHERE id = :id",
        {"id": publication["id"]},
    )[0]
    assert attempts == {
        "approval_card_delivery_attempts": 0,
        "result_delivery_attempts": 1,
    }


def test_publication_cleanup_outbox_retries_beyond_result_delivery_cap(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"], dedupe_key="cleanup-unbounded")
    )

    async def exercise() -> tuple[bool, str | None]:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="cleanup-unbounded",
            result_max_attempts=2,
        )
        try:
            card = await store.claim_pending_card()
            assert card is not None
            await store.mark_card_delivered(card.publication_id)
            approved = _resolve(client, auth_headers, publication["approval_id"])
            assert approved.status_code == 200, approved.text
            job = await store.claim_next()
            assert job is not None
            await store.persist_result(
                job.publication_id,
                outcome="failed",
                pr_url=None,
                error="terminal fixture",
            )
            for attempt in range(6):
                cleanup = await store.claim_pending_cleanup()
                assert cleanup is not None
                await store.retry_cleanup(
                    cleanup.publication_id, error=f"apiserver unavailable {attempt}"
                )
                assert await store.pending_result(job.publication_id) is None
            recovered = await store.claim_pending_cleanup()
            assert recovered is not None
            await store.mark_cleanup_completed(recovered.publication_id)
            result = await store.pending_result(job.publication_id)
            assert result is not None
            async with engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            text(
                                "SELECT resource_cleanup_completed_at IS NOT NULL AS completed, "
                                "resource_cleanup_error FROM curie.publications WHERE id = :id"
                            ),
                            {"id": publication["id"]},
                        )
                    )
                    .mappings()
                    .one()
                )
            return bool(row["completed"]), row["resource_cleanup_error"]
        finally:
            await engine.dispose()

    assert asyncio.run(exercise()) == (True, None)


def test_claimed_card_then_denied_waits_for_adoption_without_status_overwrite(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_worker.publication_store import PostgresPublicationStore

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"], dedupe_key="claim-then-deny")
    )

    async def exercise() -> str:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="card-deny-race",
            lease_seconds=60,
            result_max_attempts=1,
        )
        try:
            claimed = await store.claim_pending_card()
            assert claimed is not None
            denied = _resolve(
                client,
                auth_headers,
                publication["approval_id"],
                decision="rejected",
            )
            assert denied.status_code == 200, denied.text
            assert await store.pending_result(claimed.publication_id) is None
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.publications "
                        "SET approval_card_lease_expires_at = now() - interval '1 second' "
                        "WHERE id = :id"
                    ),
                    {"id": claimed.publication_id},
                )
            adopted = await store.claim_pending_card()
            assert adopted is not None
            await store.retry_card_delivery(adopted.publication_id, error="post uncertain")
            result = await store.pending_result(adopted.publication_id)
            assert result is not None
            return result.outcome
        finally:
            await engine.dispose()

    assert asyncio.run(exercise()) == "denied"
    assert _rows(
        "SELECT status FROM curie.publications WHERE id = :id",
        {"id": publication["id"]},
    ) == [{"status": "denied"}]


def test_claimed_card_then_expired_waits_for_adoption_without_status_overwrite(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    from curie_api.models import Approval
    from curie_worker.publication_store import PostgresPublicationStore

    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"], dedupe_key="claim-then-expire")
    )

    async def exercise() -> str:
        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="card-expire-race",
            lease_seconds=60,
            result_max_attempts=1,
        )
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(get_settings().valkey_dsn())
        try:
            claimed = await store.claim_pending_card()
            assert claimed is not None
            now = datetime.now(UTC).replace(tzinfo=None)
            async with sessionmaker() as session:
                await session.execute(
                    update(Approval)
                    .where(Approval.id == uuid.UUID(publication["approval_id"]))
                    .values(expires_at=now - timedelta(seconds=1))
                )
                await session.commit()
                queue = ResumeQueue(valkey, stream=runs_stream)
                assert await sweep_expired_approvals(session, queue, now=now) == 1
            assert await store.pending_result(claimed.publication_id) is None
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.publications "
                        "SET approval_card_lease_expires_at = now() - interval '1 second' "
                        "WHERE id = :id"
                    ),
                    {"id": claimed.publication_id},
                )
            adopted = await store.claim_pending_card()
            assert adopted is not None
            await store.retry_card_delivery(adopted.publication_id, error="post uncertain")
            result = await store.pending_result(adopted.publication_id)
            assert result is not None
            return result.outcome
        finally:
            await valkey.aclose()
            await engine.dispose()

    assert asyncio.run(exercise()) == "expired"
    assert _rows(
        "SELECT status FROM curie.publications WHERE id = :id",
        {"id": publication["id"]},
    ) == [{"status": "expired"}]


def test_publication_turn_is_done_before_card_delivery_and_never_replays_model(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The real publication insert is the card outbox; Slack never owns the turn ACK."""

    from aci_protocol import QueuedTurn, ReplyHandle, SessionStatus
    from curie_api.schemas import PublicationCreate
    from curie_worker.approvals import CreatedPublication, PublicationCreateRequest
    from curie_worker.behaviorpacks import BehaviorPacks
    from curie_worker.binding import ResolvedDeployment
    from curie_worker.kernel import TurnOutcome
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    from apps.worker.tests.kernel.conftest import kernel_harness

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    token = uuid.uuid4().hex
    names = {
        "stream": f"test:curie:publication-kernel:{token}",
        "group": f"g-{token}",
        "prefix": f"test:curie:publication-kernel:{token}:",
        "sandbox_prefix": f"test:curie:publication-sandbox:{token}:",
    }
    sync_redis = connect_or_skip(decode_responses=True)
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": channel_protocol.scoped_conversation_id(
                "slack", "C0EXAMPLE1", "1700000000.000100"
            ),
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    class DatabasePublicationCreator:
        def __init__(self, engine: Any) -> None:
            self.engine = engine

        async def create_publication(self, request: PublicationCreateRequest) -> CreatedPublication:
            sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)
            async with sessionmaker() as session:
                data = PublicationCreate.model_validate(request.to_json())
                publication, _ = await crud.create_publication(
                    session, data, patch=data.decoded_patch()
                )
                return CreatedPublication(
                    id=str(publication.id),
                    approval_id=str(publication.approval_id),
                    status=publication.status,
                )

    class WorkspaceBinding:
        async def resolve(self, kind: str, channel: str) -> ResolvedDeployment:
            return ResolvedDeployment(
                agent_id=uuid.UUID(deployment["agent_id"]),
                agent_name="acme-bot",
                deployment_id=uuid.UUID(deployment["id"]),
                workspace_enabled=True,
                version_id=uuid.UUID(deployment["version_id"]),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

        def boot_env(
            self,
            resolved: Any,
            thread: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            return {"CURIE_SESSION_ID": f"session-{thread}"}

        def packs_for(self, resolved: Any) -> BehaviorPacks:
            return BehaviorPacks.from_config(None)

    event = QueuedTurn(
        event_id="event-publication-card-outbox-no-model-replay",
        conversation_id="1700000000.000100",
        author="U0REQUEST1",
        text="publish these changes",
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C0EXAMPLE1",
            placeholder="1700000000.000001",
        ),
        received_at="2026-08-23T00:00:00+00:00",
    )

    async def exercise() -> tuple[int, int]:
        engine = create_async_engine(get_settings().database_url)
        attempts = 0
        patch = b"diff --git a/README.md b/README.md\n"
        try:
            async with kernel_harness(
                names,
                sync_redis,
                binding=WorkspaceBinding(),
                publication_creator=DatabasePublicationCreator(engine),
            ) as harness:
                harness.sink.fail_events.add("reply.post")

                async def publication_attempt(*args: Any, **kwargs: Any) -> TurnOutcome:
                    nonlocal attempts
                    attempts += 1
                    return TurnOutcome(
                        terminal_ok=False,
                        text="Prepared repository changes",
                        status=SessionStatus.AWAITING_APPROVAL,
                        approval_gate_kind="permission",
                        approval_granted_tool="mcp__curie__publish_changes",
                        publication_snapshot=RunnerWorkspaceSnapshot(
                            repo_full_name=REPO,
                            base_sha=BASE_SHA,
                            patch=patch,
                            changed_paths=("README.md",),
                            contains_workflow_files=False,
                            publication_title="Update repository",
                            publication_body="Approved platform publication.",
                        ),
                    )

                harness.kernel._attempt = publication_attempt  # type: ignore[method-assign]
                await harness.kernel.process_event(event)
                # A reclaim would produce changed bytes if the model ran again.
                # The durable done marker must short-circuit before _attempt.
                patch = b"changed second-run snapshot"
                await harness.kernel.process_event(event)
                return attempts, len(harness.sink.posts)
        finally:
            await engine.dispose()

    try:
        attempts, card_posts = asyncio.run(exercise())
    finally:
        keys = list(sync_redis.scan_iter(match=f"*{token}*"))
        if keys:
            sync_redis.delete(*keys)
        sync_redis.close()

    assert attempts == 1
    assert card_posts == 0, "the persisted outbox, not process_event, posts the card"
    rows = _rows("SELECT patch_bytes, approval_card_reported_at FROM curie.publications")
    assert rows == [
        {
            "patch_bytes": b"diff --git a/README.md b/README.md\n",
            "approval_card_reported_at": None,
        }
    ]


class _MemoryWorkspaceObjects:
    """Small private-object-store port around real workspace coordination."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        self.objects[key] = b"".join(chunks)

    def get_stream(self, key: str) -> Iterator[bytes]:
        yield self.objects[key]

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def list_keys(self, prefix: str) -> Iterator[str]:
        needle = f"{prefix.strip('/')}/"
        yield from sorted(key for key in self.objects if key.startswith(needle))


class _LocalWorkspacePreparer:
    """Provide local archives while keeping API credential clients production-real."""

    def __init__(
        self,
        workspace_module: Any,
        credentials: Any,
        assets: Mapping[str, tuple[str, bytes]],
    ) -> None:
        self._workspace = workspace_module
        self.credentials = credentials
        self._assets = assets
        self.objects = _MemoryWorkspaceObjects()
        self.limits = SimpleNamespace(max_archive_bytes=1_000_000)

    def prepare(
        self,
        *,
        deployment_id: uuid.UUID,
        thread_key: str,
        generation: str,
    ) -> Any:
        credential = self.credentials.redeem(deployment_id, thread_key)
        base_sha, archive = self._assets[credential.repo_full_name]
        digest = hashlib.sha256(archive).hexdigest()
        object_key = f"workspaces/{hashlib.sha256(thread_key.encode()).hexdigest()}/{generation}"
        self.objects.put_stream(object_key, (archive,))
        return self._workspace.PreparedWorkspace(
            object_key=object_key,
            sha256=digest,
            clean_clone_url=credential.clone_url,
            repo_full_name=credential.repo_full_name,
            base_sha=base_sha,
            materialized_head=base_sha,
            checkout_mode=0o700,
            reference=self._workspace.WorkspaceRef(
                url=f"https://objects.example.test/{object_key}",
                sha256=digest,
                expires_at_epoch=int(time.time()) + 300,
            ),
        )

    def verify(self, prepared: Any) -> None:
        assert hashlib.sha256(self.objects.objects[prepared.object_key]).hexdigest() == (
            prepared.sha256
        )

    def delete(self, prepared: Any) -> None:
        self.objects.delete(prepared.object_key)


class _RecordingWorkspaceCoordinator:
    """Observe keys while delegating every workspace operation to production."""

    def __init__(self, coordinator: Any) -> None:
        self._coordinator = coordinator
        self.preparer = coordinator.preparer
        self.release_keys: list[str] = []
        self.claims: list[tuple[str, Any | None, Any]] = []
        self.validating_candidate: Any | None = None

    def select_repository(self, **kwargs: Any) -> str | None:
        return self._coordinator.select_repository(**kwargs)

    def claim_or_resume_with_handle(self, **kwargs: Any) -> Any:
        validate_candidate = kwargs.get("validate_candidate")
        if callable(validate_candidate):

            def observe_candidate(candidate: Any) -> None:
                self.validating_candidate = candidate
                try:
                    validate_candidate(candidate)
                finally:
                    self.validating_candidate = None

            kwargs = {**kwargs, "validate_candidate": observe_candidate}
        result = self._coordinator.claim_or_resume_with_handle(**kwargs)
        self.claims.append(
            (str(kwargs["thread_key"]), kwargs.get("replace_handle"), result.handle)
        )
        return result

    def current(self, thread_key: str) -> Any:
        return self._coordinator.current(thread_key)

    def stream_current_base(self, thread_key: str) -> Iterator[bytes]:
        yield from self._coordinator.stream_current_base(thread_key)

    def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
        return self._coordinator.touch(thread_key, ttl_seconds=ttl_seconds)

    def release(self, thread_key: str) -> None:
        self.release_keys.append(thread_key)
        self._coordinator.release(thread_key)


def _testclient_transport(client: TestClient) -> Any:
    def transport(**request: Any) -> Any:
        parsed = urlsplit(str(request["url"]))
        target = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        response = client.request(
            str(request["method"]),
            target,
            headers=dict(request["headers"]),
            content=request.get("body"),
            follow_redirects=bool(request.get("allow_redirects", False)),
        )
        return SimpleNamespace(
            status=response.status_code,
            headers=dict(response.headers),
            body=response.content,
        )

    return transport


def _local_publication_repository(
    root: Path, repo_full_name: str
) -> tuple[Path, str, bytes]:
    repo = root / repo_full_name.replace("/", "-")
    repo.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "coder@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Coder Test"], cwd=repo, check=True)
    subprocess.run(
        ["git", "remote", "add", "origin", f"https://github.com/{repo_full_name}.git"],
        cwd=repo,
        check=True,
    )
    (repo / "README.md").write_text(f"before {repo_full_name}\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "base"], cwd=repo, check=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    base_archive = BytesIO()
    with tarfile.open(fileobj=base_archive, mode="w:gz") as archive:
        archive.add(repo / "README.md", arcname="README.md")
    (repo / "README.md").write_text(f"after {repo_full_name}\n")
    return repo, base_sha, base_archive.getvalue()


@pytest.mark.parametrize("late_handoff", [False, True], ids=["initial", "late-handoff"])
def test_coder_path_reaches_the_publication_boundary_through_real_runner_and_api(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    tmp_path: Path,
    late_handoff: bool,
) -> None:
    """The coder's publish tool crosses runner -> kernel -> API, not a test-only _attempt.

    Kubernetes is deliberately outside this test: a pending publication row is
    the handoff consumed by ``publication_k8s`` after an approver resolves it.
    The model is the sole fake; the runner HTTP server, ACI client, kernel,
    snapshot validation, and internal publication route are production code.
    """
    from aci_protocol import QueuedTurn, ReplyHandle
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
    from claude_agent_sdk.types import PermissionResultDeny
    from curie_runner.approval import PUBLISH_TOOL_NAME, build_approval_gate, build_can_use_tool
    from curie_runner.fake import FakeModelSession
    from curie_runner.otel import RunTracer
    from curie_runner.server import create_app as create_runner_app
    from curie_runner.session import SessionRunner
    from curie_runner.side_effects import SideEffectClassifier
    from curie_runner.workspace_snapshot import capture_workspace_snapshot
    from curie_worker.approvals import ApprovalClient
    from curie_worker.behaviorpacks import BehaviorPacks
    from curie_worker.binding import ResolvedDeployment
    from curie_worker.workspace import WorkspaceClaimCoordinator, WorkspaceCredentialClient

    from apps.worker.tests.kernel.conftest import kernel_harness

    client, _ = publication_stack
    runner_token = "coder-publication-runner-token"
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "1700000000.000100"
    # A real, credential-free checkout supplies both the runner snapshot and
    # the worker's independently rehashed retained base.
    repo, base_sha, base_archive = _local_publication_repository(tmp_path, REPO)

    class WorkspaceBinding:
        async def resolve(self, kind: str, address: str) -> ResolvedDeployment:
            assert (kind, address) == ("slack", "C0EXAMPLE1")
            return ResolvedDeployment(
                agent_id=uuid.UUID(deployment["agent_id"]),
                agent_name="acme-coder",
                deployment_id=uuid.UUID(deployment["id"]),
                workspace_enabled=True,
                version_id=uuid.UUID(deployment["version_id"]),
                version_label="v1",
                bundle_ref=None,
                max_usd_per_day=None,
                max_output_tokens_per_run=None,
            )

        def boot_env(self, resolved: Any, thread: str, **_kwargs: Any) -> dict[str, str]:
            return {
                "CURIE_SESSION_ID": f"session-{thread}",
                "CURIE_RUNNER_TOKEN": runner_token,
            }

        def packs_for(self, resolved: Any) -> BehaviorPacks:
            return BehaviorPacks.from_config(None)

    gate = build_approval_gate(operator_tools=None, policy_routes={}, managed_workspace=True)
    assert gate is not None

    async def record_publish_gate(*args: Any, **kwargs: Any) -> object:
        decision = await build_can_use_tool(gate)(*args, **kwargs)
        # The SDK reports the denied platform-owned publish call, then returns
        # its terminal response.  Keep the fake model on that observable path
        # so SessionRunner emits the real awaiting-approval final frame.
        if isinstance(decision, PermissionResultDeny):
            return replace(decision, interrupt=False)
        return decision

    def scripted_turn() -> list[Any]:
        if "https://github.com/" not in model.queries[-1]:
            return [
                AssistantMessage(
                    content=[TextBlock(text="Ready for a repository when you are.")],
                    model="fake-model",
                ),
                ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="fake",
                    result="Ready for a repository when you are.",
                ),
            ]
        return [
            AssistantMessage(
                content=[
                    TextBlock(text="Prepared README."),
                    ToolUseBlock(
                        id="publish",
                        name=PUBLISH_TOOL_NAME,
                        input={"title": "Update README", "body": "Prepared by coder."},
                    ),
                ],
                model="fake-model",
            ),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="fake",
                result="Prepared README.",
            ),
        ]

    model = FakeModelSession(
        scripted_turn,
        can_use_tool=record_publish_gate,
        approval_gate=gate,
    )
    runner = SessionRunner(
        session_factory=lambda: model,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="coder-publication",
        approval_gate=gate,
    )

    async def exercise() -> tuple[
        list[int],
        list[dict[str, Any]],
        list[str],
        list[Any],
        list[tuple[str, Any | None, Any]],
        list[dict[str, object]],
    ]:
        await runner.start()
        suffix = uuid.uuid4().hex
        names = {
            "stream": f"test:coder-publication:{suffix}",
            "group": f"g-{suffix}",
            "prefix": f"test:coder-publication:{suffix}:",
            "sandbox_prefix": f"test:coder-sandbox:{suffix}:",
        }
        sync_redis = connect_or_skip(decode_responses=True)
        validation_scratch = tmp_path / "publication-validation"
        validation_scratch.mkdir()
        statuses: list[int] = []
        requests: list[dict[str, Any]] = []

        def approval_transport(request: httpx.Request) -> httpx.Response:
            target = request.url.path
            if request.url.query:
                target += f"?{request.url.query.decode()}"
            response = client.request(
                request.method,
                target,
                headers=dict(request.headers),
                content=request.content,
            )
            if request.url.path == "/v1/internal/publications":
                statuses.append(response.status_code)
                requests.append(json.loads(request.content))
            return httpx.Response(
                response.status_code,
                headers=dict(response.headers),
                content=response.content,
            )

        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(approval_transport),
                base_url="https://api.example.test",
            ) as api_client:
                creator = ApprovalClient(
                    api_base_url="https://api.example.test",
                    api_key=get_settings().api_key,
                    client=api_client,
                    read_timeout_s=2,
                    worker_token=WORKER_TOKEN,
                )
                credentials = WorkspaceCredentialClient(
                    api_url="https://api.example.test",
                    worker_token=WORKER_TOKEN,
                    transport=_testclient_transport(client),
                )
                async with kernel_harness(
                    names,
                    sync_redis,
                    binding=WorkspaceBinding(),
                    publication_creator=creator,
                    workspace_scratch_root=str(validation_scratch),
                    runner_app=create_runner_app(
                        runner,
                        token=runner_token,
                        snapshotter=lambda: capture_workspace_snapshot(
                            repo,
                            expected_repo=REPO,
                            publication_title="Update README",
                            publication_body="Prepared by coder.",
                        ),
                    ),
                ) as harness:
                    preparer = _LocalWorkspacePreparer(
                        __import__("curie_worker.workspace", fromlist=["workspace"]),
                        credentials,
                        {REPO: (base_sha, base_archive)},
                    )
                    workspace = _RecordingWorkspaceCoordinator(
                        WorkspaceClaimCoordinator(
                            preparer=preparer,
                            substrate=harness.substrate,
                        )
                    )
                    harness.kernel._workspace = workspace  # type: ignore[assignment]
                    real_status = harness.kernel._runner.status
                    candidate_status_attestations: list[dict[str, object]] = []

                    async def attested_status(
                        base_url: str,
                        *,
                        token: str | None = None,
                        remaining_s: float | None = None,
                    ) -> dict[str, object]:
                        status = await real_status(
                            base_url,
                            token=token,
                            remaining_s=remaining_s,
                        )
                        candidate = workspace.validating_candidate
                        if candidate is None:
                            return status
                        attested_status = {
                            **status,
                            "session_id": candidate.session_id,
                            "sandbox_id": candidate.sandbox_id,
                            "managed_workspace": True,
                            "cwd": "/workspace",
                            "ready": True,
                            "turn_active": False,
                            "history_durable": True,
                            "status": "idle-awaiting-input",
                        }
                        candidate_status_attestations.append(attested_status)
                        return attested_status

                    harness.kernel._runner.status = attested_status  # type: ignore[method-assign]
                    if late_handoff:
                        await harness.kernel.process_event(
                            QueuedTurn(
                                event_id="coder-publication-generic-first",
                                conversation_id=conversation_id,
                                author="U0REQUEST1",
                                text="Please get ready to edit a repository.",
                                reply_handle=ReplyHandle(
                                    kind="slack",
                                    channel="C0EXAMPLE1",
                                    placeholder="1700000000.000001",
                                ),
                                received_at="2026-09-01T00:00:00+00:00",
                            )
                        )
                    await harness.kernel.process_event(
                        QueuedTurn(
                            event_id="coder-publication-boundary",
                            conversation_id=conversation_id,
                            author="U0REQUEST1",
                            text=f"Publish the README change from https://github.com/{REPO}",
                            reply_handle=ReplyHandle(
                                kind="slack",
                                channel="C0EXAMPLE1",
                                placeholder="1700000000.000002",
                            ),
                            received_at="2026-09-01T00:00:00+00:00",
                        )
                    )
                    reply_targets = [
                        event.target
                        for event, _route, _best_effort in harness.sink.events
                        if hasattr(event, "target")
                    ]
                    return (
                        statuses,
                        requests,
                        workspace.release_keys,
                        reply_targets,
                        workspace.claims,
                        candidate_status_attestations,
                    )
        finally:
            await runner.close()
            keys = list(sync_redis.scan_iter(match=f"*{suffix}*"))
            if keys:
                sync_redis.delete(*keys)
            sync_redis.close()

    (
        statuses,
        requests,
        release_keys,
        reply_targets,
        claims,
        candidate_status_attestations,
    ) = asyncio.run(exercise())
    # Keep the expected identity independent of the production helper so the
    # fix-pin reversal reaches the publication boundary and fails on its 409.
    scoped = f"slack:C0EXAMPLE1:{conversation_id}"
    rows = _rows(
        "SELECT p.id, p.status, p.base_sha, p.patch_bytes, p.changed_paths, p.title, p.body, "
        "p.workspace_conversation_id, p.reply_kind, p.reply_channel, "
        "a.conversation_id FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id"
    )
    assert statuses == [201]
    assert len(requests) == 1
    assert "workspace_conversation_id" not in requests[0]
    assert requests[0]["conversation_id"] == scoped
    assert requests[0]["reply_conversation_id"] == conversation_id
    assert release_keys == [scoped]
    assert len(claims) == 1
    assert claims[0][0] == scoped
    assert (claims[0][1] is not None) is late_handoff
    assert candidate_status_attestations == (
        [
            {
                "session_id": claims[0][2].session_id,
                "sandbox_id": claims[0][2].sandbox_id,
                "managed_workspace": True,
                "cwd": "/workspace",
                "ready": True,
                "turn_active": False,
                "history_durable": True,
                "status": "idle-awaiting-input",
            }
        ]
        if late_handoff
        else []
    )
    assert reply_targets
    assert all(target.address == "C0EXAMPLE1" for target in reply_targets)
    assert all(target.conversation_id == conversation_id for target in reply_targets)
    assert len(rows) == 1
    boundary = rows[0]
    assert boundary["status"] == "pending"
    assert boundary["base_sha"] == base_sha
    assert boundary["changed_paths"] == ["README.md"]
    assert boundary["title"] == "Update README"
    assert boundary["body"] == "Prepared by coder."
    assert boundary["workspace_conversation_id"] == scoped
    assert boundary["conversation_id"] == conversation_id
    assert (boundary["reply_kind"], boundary["reply_channel"]) == (
        "slack",
        "C0EXAMPLE1",
    )
    assert boundary["patch_bytes"].endswith(
        f"@@ -1 +1 @@\n-before {REPO}\n+after {REPO}\n".encode()
    )
    public = client.get(f"/publications/{boundary['id']}", headers=auth_headers)
    assert public.status_code == 200, public.text
    assert "workspace_conversation_id" not in public.json()


def test_kernel_publications_isolate_same_timestamp_across_slack_channels(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    tmp_path: Path,
) -> None:
    """One bare Slack timestamp cannot join publication authority across channels.

    The full terminal publication loop has its own store/reconciler coverage.
    This kernel pin stops at the durable outbox boundary and proves the route
    that loop will consume: both the stored approval target and every kernel
    reply remain channel-local while workspace/history authority stays scoped.
    """
    from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
    from aiohttp import web
    from curie_worker.approvals import ApprovalClient
    from curie_worker.behaviorpacks import BehaviorPacks
    from curie_worker.binding import HISTORY_REF_ENV, BindingResolver
    from curie_worker.runner_client import RunnerWorkspaceSnapshot
    from curie_worker.workspace import WorkspaceClaimCoordinator, WorkspaceCredentialClient

    from apps.worker.tests.kernel.conftest import FakeRunner, kernel_harness, make_config

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers, channel="C0EXAMPLE1")
    second_binding = client.post(
        f"/agents/{deployment['agent_id']}/channels",
        json={"kind": "slack", "address": "C0EXAMPLE2"},
        headers=auth_headers,
    )
    assert second_binding.status_code == 201, second_binding.text

    shared_timestamp = "1700000000.000100"
    repos = {
        "C0EXAMPLE1": "acme-corp/acme-bot-a",
        "C0EXAMPLE2": "acme-corp/acme-bot-b",
    }
    placeholders = {
        "C0EXAMPLE1": "1700000000.000101",
        "C0EXAMPLE2": "1700000000.000102",
    }
    repository_assets: dict[str, tuple[str, bytes]] = {}
    snapshots: dict[str, RunnerWorkspaceSnapshot] = {}
    for channel, repo_full_name in repos.items():
        _repo, base_sha, base_archive = _local_publication_repository(
            tmp_path, repo_full_name
        )
        repository_assets[repo_full_name] = (base_sha, base_archive)
        snapshots[channel] = RunnerWorkspaceSnapshot(
            repo_full_name=repo_full_name,
            base_sha=base_sha,
            patch=(
                "diff --git a/README.md b/README.md\n"
                "--- a/README.md\n"
                "+++ b/README.md\n"
                "@@ -1 +1 @@\n"
                f"-before {repo_full_name}\n"
                f"+after {repo_full_name}\n"
            ).encode(),
            changed_paths=("README.md",),
            contains_workflow_files=False,
            publication_title=f"Update {channel} README",
            publication_body="Approved platform publication.",
        )

    token = uuid.uuid4().hex
    names = {
        "stream": f"test:curie:publication-collision:{token}",
        "group": f"g-{token}",
        "prefix": f"test:curie:publication-collision:{token}:",
        "sandbox_prefix": f"test:curie:publication-collision-sandbox:{token}:",
    }
    validation_scratch = tmp_path / "publication-collision-validation"
    validation_scratch.mkdir()
    worker_config = make_config(
        names,
        api_base_url="https://api.example.test",
        workspace_scratch_root=str(validation_scratch),
    )

    class RecordingBinding:
        """Observe the production resolver's canonical history identity."""

        def __init__(self, resolver: BindingResolver) -> None:
            self._resolver = resolver
            self.history_keys: list[tuple[str, str, str]] = []

        async def resolve(self, kind: str, address: str) -> Any:
            return await self._resolver.resolve(kind, address)

        def boot_env(
            self,
            resolved: Any,
            thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            env = self._resolver.boot_env(
                resolved,
                thread_key,
                kind=kind,
                address=address,
            )
            history_segment = unquote(
                urlsplit(env[HISTORY_REF_ENV]).path.rsplit("/", 1)[-1]
            )
            self.history_keys.append((address or "", thread_key, history_segment))
            return env

        def packs_for(self, resolved: Any) -> BehaviorPacks:
            return self._resolver.packs_for(resolved)

    events = [
        QueuedTurn(
            event_id=f"publication-collision-{channel}",
            conversation_id=shared_timestamp,
            author="U0REQUEST1",
            text=f"Publish the README change from https://github.com/{repo_full_name}",
            reply_handle=ReplyHandle(
                kind="slack",
                channel=channel,
                placeholder=placeholders[channel],
            ),
            received_at="2026-09-01T00:00:00+00:00",
        )
        for channel, repo_full_name in repos.items()
    ]

    async def exercise() -> tuple[
        list[int],
        list[dict[str, Any]],
        list[str],
        list[tuple[str, Any | None, Any]],
        list[tuple[str, str, str]],
        list[tuple[str, Any]],
    ]:
        engine = create_async_engine(get_settings().database_url)
        sync_redis = connect_or_skip(decode_responses=True)
        statuses: list[int] = []
        requests: list[dict[str, Any]] = []

        def approval_transport(request: httpx.Request) -> httpx.Response:
            target = request.url.path
            if request.url.query:
                target += f"?{request.url.query.decode()}"
            response = client.request(
                request.method,
                target,
                headers=dict(request.headers),
                content=request.content,
            )
            if request.url.path == "/v1/internal/publications":
                statuses.append(response.status_code)
                requests.append(json.loads(request.content))
            return httpx.Response(
                response.status_code,
                headers=dict(response.headers),
                content=response.content,
            )

        binding = RecordingBinding(BindingResolver(engine, worker_config))
        controlled_runner = FakeRunner()
        controlled_runner.default_script = [
            Final(
                text="Prepared repository changes",
                status=SessionStatus.AWAITING_APPROVAL,
                approval_summary="Publish the prepared changes",
                approval_gate_kind="permission",
                approval_granted_tool="mcp__curie__publish_changes",
            )
        ]

        async def snapshot(_request: web.Request) -> web.Response:
            assert controlled_runner.opened
            matching_channels = [
                channel
                for channel, repo_full_name in repos.items()
                if repo_full_name in controlled_runner.opened[-1]
            ]
            assert len(matching_channels) == 1
            captured = snapshots[matching_channels[0]]
            return web.json_response(
                {
                    "repo_full_name": captured.repo_full_name,
                    "base_sha": captured.base_sha,
                    "patch_base64": base64.b64encode(captured.patch).decode("ascii"),
                    "changed_paths": list(captured.changed_paths),
                    "contains_workflow_files": captured.contains_workflow_files,
                    "patch_size_bytes": len(captured.patch),
                    "publication_title": captured.publication_title,
                    "publication_body": captured.publication_body,
                }
            )

        controlled_runner.app.add_routes([web.post("/v1/snapshot", snapshot)])
        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(approval_transport),
                base_url="https://api.example.test",
            ) as api_client:
                creator = ApprovalClient(
                    api_base_url="https://api.example.test",
                    api_key=get_settings().api_key,
                    client=api_client,
                    read_timeout_s=2,
                    worker_token=WORKER_TOKEN,
                )
                credentials = WorkspaceCredentialClient(
                    api_url="https://api.example.test",
                    worker_token=WORKER_TOKEN,
                    transport=_testclient_transport(client),
                )
                async with kernel_harness(
                    names,
                    sync_redis,
                    binding=binding,
                    publication_creator=creator,
                    api_base_url="https://api.example.test",
                    workspace_scratch_root=str(validation_scratch),
                    runner_app=controlled_runner.app,
                ) as harness:
                    preparer = _LocalWorkspacePreparer(
                        __import__("curie_worker.workspace", fromlist=["workspace"]),
                        credentials,
                        repository_assets,
                    )
                    workspace = _RecordingWorkspaceCoordinator(
                        WorkspaceClaimCoordinator(
                            preparer=preparer,
                            substrate=harness.substrate,
                        )
                    )
                    harness.kernel._workspace = workspace  # type: ignore[assignment]
                    for event in events:
                        await harness.kernel.process_event(event)

                    routed_events = [
                        (event.event, event.target)
                        for event, _route, _best_effort in harness.sink.events
                        if hasattr(event, "target")
                    ]
                    return (
                        statuses,
                        requests,
                        workspace.release_keys,
                        workspace.claims,
                        binding.history_keys,
                        routed_events,
                    )
        finally:
            await engine.dispose()
            keys = list(sync_redis.scan_iter(match=f"*{token}*"))
            if keys:
                sync_redis.delete(*keys)
            sync_redis.close()

    statuses, requests, release_keys, claims, history_keys, routed_events = asyncio.run(
        exercise()
    )
    scoped_by_channel = {
        channel: channel_protocol.scoped_conversation_id(
            "slack", channel, shared_timestamp
        )
        for channel in repos
    }
    workspace_rows = _rows(
        "SELECT agent_id, selected_by_deployment_id, conversation_id, repo_full_name "
        "FROM curie.thread_workspaces ORDER BY conversation_id"
    )
    assert workspace_rows == [
        {
            "agent_id": uuid.UUID(deployment["agent_id"]),
            "selected_by_deployment_id": uuid.UUID(deployment["id"]),
            "conversation_id": scoped_by_channel[channel],
            "repo_full_name": repos[channel],
        }
        for channel in sorted(repos, key=scoped_by_channel.__getitem__)
    ]
    assert statuses == [201, 201]
    assert len(requests) == 2
    assert all("workspace_conversation_id" not in request for request in requests)

    scoped_identities = set(scoped_by_channel.values())
    assert len(scoped_identities) == 2
    assert len(claims) == 2
    assert {claim[0] for claim in claims} == scoped_identities
    assert all(claim[1] is None for claim in claims)
    assert len(release_keys) == 2
    assert set(release_keys) == scoped_identities
    assert len(history_keys) == 2
    assert {
        (address, thread_key, history_key)
        for address, thread_key, history_key in history_keys
    } == {
        (channel, scoped, scoped)
        for channel, scoped in scoped_by_channel.items()
    }

    durable_rows = _rows(
        "SELECT p.workspace_conversation_id, p.repo_full_name, "
        "a.conversation_id, a.reply_kind, a.reply_channel, a.reply_placeholder "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "ORDER BY a.reply_channel"
    )
    assert len(durable_rows) == 2
    for row in durable_rows:
        channel = row["reply_channel"]
        assert row["workspace_conversation_id"] == scoped_by_channel[channel]
        assert row["repo_full_name"] == repos[channel]
        assert row["conversation_id"] == shared_timestamp
        assert row["reply_kind"] == "slack"
        assert row["reply_placeholder"] == placeholders[channel]

    reply_targets = [target for _event, target in routed_events]
    for channel in repos:
        channel_targets = [target for target in reply_targets if target.address == channel]
        assert channel_targets, f"nothing was addressed to {channel}"
        assert all(target.kind == "slack" for target in channel_targets)
        assert all(target.conversation_id == shared_timestamp for target in channel_targets)
        assert all(target.reply_ref == placeholders[channel] for target in channel_targets)
    assert len(reply_targets) == sum(
        1 for target in reply_targets if target.address in repos
    )
    completion_targets = [
        target for event_name, target in routed_events if event_name == "turn.completed"
    ]
    assert len(completion_targets) == 2
    assert {
        (target.address, target.conversation_id, target.reply_ref)
        for target in completion_targets
    } == {
        (channel, shared_timestamp, placeholder)
        for channel, placeholder in placeholders.items()
    }

    request_by_channel = {request["reply_channel"]: request for request in requests}
    before_crossed = _counts()
    crossed_requests = [
        {
            **request_by_channel["C0EXAMPLE1"],
            "reply_channel": "C0EXAMPLE2",
            "dedupe_key": "publication-collision-crossed-route",
        },
        {
            **request_by_channel["C0EXAMPLE1"],
            "repo_full_name": repos["C0EXAMPLE2"],
            "dedupe_key": "publication-collision-crossed-repository",
        },
    ]
    for crossed in crossed_requests:
        refused = client.post(
            "/v1/internal/publications",
            json=crossed,
            headers=WORKER_HEADERS,
        )
        assert refused.status_code == 409, refused.text
        assert _counts() == before_crossed


def test_publication_insert_failure_rolls_back_the_approval(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A real Postgres trigger fails after the Approval insert, not validation."""

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)

    async def install_trigger() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "CREATE OR REPLACE FUNCTION curie.fail_publication_insert() "
                        "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        "RAISE EXCEPTION 'forced publication insert failure'; END $$"
                    )
                )
                await conn.execute(
                    text(
                        "CREATE TRIGGER test_fail_publication BEFORE INSERT ON "
                        "curie.publications FOR EACH ROW EXECUTE FUNCTION "
                        "curie.fail_publication_insert()"
                    )
                )
        finally:
            await engine.dispose()

    async def remove_trigger() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("DROP TRIGGER IF EXISTS test_fail_publication ON curie.publications")
                )
                await conn.execute(text("DROP FUNCTION IF EXISTS curie.fail_publication_insert()"))
        finally:
            await engine.dispose()

    asyncio.run(install_trigger())
    try:
        payload = _publication_payload(deployment["id"])
        selected = client.post(
            f"/v1/internal/workspaces/{deployment['id']}/selection",
            json={
                "conversation_id": _workspace_identity(payload),
                "author": payload["author"],
                "repo_full_name": REPO,
            },
            headers=WORKER_HEADERS,
        )
        assert selected.status_code == 200, selected.text
        with pytest.raises(Exception, match="forced publication insert failure"):
            client.post(
                "/v1/internal/publications",
                json=payload,
                headers=WORKER_HEADERS,
            )
    finally:
        asyncio.run(remove_trigger())
    assert _counts() == (0, 0)


@pytest.mark.parametrize(
    ("size", "expected_status"),
    [(PATCH_LIMIT - 1, 201), (PATCH_LIMIT, 201), (PATCH_LIMIT + 1, 413)],
)
def test_publication_patch_cap_counts_decoded_raw_bytes_exactly(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    size: int,
    expected_status: int,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"], patch=b"x" * size)
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    response = client.post(
        "/v1/internal/publications",
        json=payload,
        headers=WORKER_HEADERS,
    )
    assert response.status_code == expected_status, response.text
    if expected_status == 201:
        stored = _rows("SELECT octet_length(patch_bytes) AS size FROM curie.publications")
        assert stored == [{"size": size}]
        assert _counts() == (1, 1)
    else:
        assert "900000" in response.json()["detail"]
        assert _counts() == (0, 0)


def test_publication_requester_self_approval_requires_membership_and_is_audited(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"], author="U0REQUEST1")
    )

    outside = _resolve(
        client,
        auth_headers,
        publication["approval_id"],
        actor="U0REQUEST1",
        channel="C0ELSE001",
    )
    assert outside.status_code == 403
    assert "approver" in outside.json()["detail"]

    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "approved"
    assert _stream_entries(runs_stream) == []

    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.status_code == 200, stored.text
    assert stored.json()["status"] == "approved"
    assert stored.json()["version"] == 2

    audit = client.get(f"/approvals/{publication['approval_id']}/audit", headers=auth_headers)
    assert audit.status_code == 200, audit.text
    denied, resolved = audit.json()
    assert denied["authorized"] is False
    assert resolved["authorized"] is True
    assert resolved["actor"] == "U0REQUEST1"
    assert "publication_requester_exception" not in resolved["evidence"]
    assert resolved["evidence"]["approvers_channel"] == "C0EXAMPLE1"
    assert resolved["evidence"]["actor_channel"] == "C0EXAMPLE1"
    assert resolved["principal_kind"] == "chat"
    assert resolved["authenticated"] is True


def test_publication_resolution_cas_has_one_winner_and_never_enqueues_resume(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))

    def attempt(decision: str) -> Any:
        return _resolve(
            client,
            auth_headers,
            publication["approval_id"],
            decision=decision,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(attempt, ["approved", "rejected"]))

    assert sorted(response.status_code for response in responses) == [200, 409]
    assert _stream_entries(runs_stream) == []
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers).json()
    winner = next(
        response.json()["status"] for response in responses if response.status_code == 200
    )
    assert stored["status"] == ("approved" if winner == "approved" else "denied")
    assert stored["version"] == 2


def test_publication_approval_is_never_in_the_owed_wake_worklist(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Finder, row claim, and dead-letter reopen share one exclusion."""

    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))
    resolved = _resolve(client, auth_headers, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text

    async def inspect() -> tuple[list[uuid.UUID], bool, bool, datetime | None]:
        from curie_api.models import Approval

        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                approval_id = uuid.UUID(publication["approval_id"])
                ids = await crud.list_resolved_unresumed(
                    session,
                    resolved_before=datetime.now(UTC).replace(tzinfo=None) + timedelta(days=1),
                    limit=100,
                )
                claim = await crud.claim_resume_row(session, approval_id)
                await session.rollback()
                reopened = await crud.reopen_dead_lettered_resume(
                    session,
                    approval_id,
                    dead_lettered_after=datetime.now(UTC).replace(tzinfo=None) + timedelta(days=1),
                )
                approval = await session.get(Approval, approval_id)
                assert approval is not None
                return ids, claim is not None, reopened, approval.resumed_at
        finally:
            await engine.dispose()

    ids, claimed, reopened, resumed_at = asyncio.run(inspect())
    assert uuid.UUID(publication["approval_id"]) not in ids
    assert claimed is False
    assert reopened is False
    assert resumed_at is not None, "durable-only resolution must mark no wake owed"
    assert _stream_entries(runs_stream) == []


def test_expired_publication_settles_without_expiry_or_reconciler_wake(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(deployment["id"], expires_in_seconds=1),
    )
    past = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=1)

    async def expire_and_sweep() -> tuple[int, list[uuid.UUID]]:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(get_settings().valkey_dsn())
        queue = ResumeQueue(valkey, stream=runs_stream)
        try:
            async with sessionmaker() as session:
                from curie_api.models import Approval

                await session.execute(
                    update(Approval)
                    .where(Approval.id == uuid.UUID(publication["approval_id"]))
                    .values(expires_at=past)
                )
                await session.commit()
                count = await sweep_expired_approvals(
                    session, queue, now=past + timedelta(seconds=1)
                )
                ids = await crud.list_resolved_unresumed(
                    session,
                    resolved_before=past + timedelta(days=1),
                    limit=100,
                )
                return count, ids
        finally:
            await valkey.aclose()
            await engine.dispose()

    count, owed = asyncio.run(expire_and_sweep())
    assert count == 1
    assert uuid.UUID(publication["approval_id"]) not in owed
    assert _stream_entries(runs_stream) == []
    assert (
        client.get(f"/approvals/{publication['approval_id']}", headers=auth_headers).json()[
            "status"
        ]
        == "expired"
    )
    assert (
        client.get(f"/publications/{publication['id']}", headers=auth_headers).json()["status"]
        == "expired"
    )


def test_denied_publication_has_no_credential_or_launch_opportunity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, runs_stream = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))

    denied = _resolve(
        client,
        auth_headers,
        publication["approval_id"],
        decision="rejected",
    )
    assert denied.status_code == 200, denied.text
    stored = client.get(f"/publications/{publication['id']}", headers=auth_headers)
    assert stored.json()["status"] == "denied"
    assert stored.json()["version"] == 2
    credential = client.post(
        f"/v1/internal/publications/{publication['id']}/credential",
        headers=WORKER_HEADERS,
    )
    assert credential.status_code == 409
    assert credential.headers["cache-control"] == "no-store"
    assert "approved" in credential.json()["detail"].lower()
    assert _stream_entries(runs_stream) == []
    assert _rows(
        "SELECT count(*) AS count FROM curie.publications "
        "WHERE status IN ('approved', 'launching', 'running')"
    ) == [{"count": 0}]


def test_publication_credential_is_approved_only_server_derived_and_audited(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))
    url = f"/v1/internal/publications/{publication['id']}/credential"

    pending = client.post(url, headers=WORKER_HEADERS)
    assert pending.status_code == 409
    assert pending.headers["cache-control"] == "no-store"
    platform_key = client.post(url, headers=auth_headers)
    assert platform_key.status_code == 401
    assert platform_key.headers["cache-control"] == "no-store"
    assert "internal worker token" in platform_key.json()["detail"]
    # The authenticated pending-state refusal above is auditable. The
    # unauthenticated probe is bounded to its 401/access log and adds no row.
    audit_after_unauthenticated = _rows(
        "SELECT outcome FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": publication["id"]},
    )
    assert audit_after_unauthenticated == [{"outcome": "refused"}]

    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    issued = client.post(
        url,
        json={"repo_full_name": "attacker/other-bot"},
        headers=WORKER_HEADERS,
    )
    assert issued.status_code == 200, issued.text
    assert issued.headers["cache-control"] == "no-store"
    assert issued.json() == {
        "repo_full_name": REPO,
        "clone_url": "https://github.com/acme-corp/acme-bot.git",
        "authorization_header": "Basic "
        + base64.b64encode(b"x-access-token:ghp_publication_operator").decode(),
    }

    audit = _rows(
        "SELECT purpose, outcome, deployment_id, publication_id, repo_full_name, "
        "detail FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": publication["id"]},
    )
    assert [row["outcome"] for row in audit] == ["refused", "issued"]
    assert all(row["purpose"] == "publication_push" for row in audit)
    assert all(str(row["publication_id"]) == publication["id"] for row in audit)
    assert all(str(row["deployment_id"]) == deployment["id"] for row in audit)
    assert all(row["repo_full_name"] == REPO for row in audit)
    assert "ghp_publication_operator" not in json.dumps(audit, default=str)


@pytest.mark.parametrize("legacy", [False, True])
def test_publication_credential_rechecks_canonical_selection_and_refuses_bare_fallback(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    legacy: bool,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(
        deployment["id"], dedupe_key=f"credential-selection-{'legacy' if legacy else 'scoped'}"
    )
    payload["conversation_id"] = "1700000000.000100"
    _, publication = _create_publication(client, payload)
    if legacy:
        _make_publication_legacy(publication["id"], deployment["id"], payload)
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    url = f"/v1/internal/publications/{publication['id']}/credential"

    issued = client.post(url, headers=WORKER_HEADERS)
    if legacy:
        assert issued.status_code == 403
        audit = _rows(
            "SELECT outcome, detail FROM curie.credential_redemption_audit_entries "
            "WHERE publication_id = :id ORDER BY created_at, id",
            {"id": publication["id"]},
        )
        assert [row["outcome"] for row in audit] == ["refused"]
        assert "ghp_publication_operator" not in json.dumps(audit, default=str)
        return
    assert issued.status_code == 200, issued.text

    _execute(
        "DELETE FROM curie.thread_workspaces "
        "WHERE selected_by_deployment_id = :deployment_id "
        "AND conversation_id = :conversation_id",
        {
            "deployment_id": deployment["id"],
            "conversation_id": _workspace_identity(payload),
        },
    )
    refused = client.post(url, headers=WORKER_HEADERS)

    assert refused.status_code == 403
    assert refused.headers["cache-control"] == "no-store"
    audit = _rows(
        "SELECT outcome, detail FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": publication["id"]},
    )
    assert [row["outcome"] for row in audit] == ["issued", "refused"]
    assert "ghp_publication_operator" not in json.dumps(audit, default=str)


def test_publication_credential_authorization_uses_scoped_lineage_identity(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    scoped_thread = "slack:C0EXAMPLE1:1700000000.000100"
    reply_thread = "1700000000.000100"
    payload = _publication_payload(
        deployment["id"],
        conversation_id=scoped_thread,
        dedupe_key="scoped-workspace-credential-authorization",
    )
    payload["reply_conversation_id"] = reply_thread
    _, publication = _create_publication(client, payload)
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    url = f"/v1/internal/publications/{publication['id']}/credential"

    issued = client.post(url, headers=WORKER_HEADERS)
    assert issued.status_code == 200, issued.text

    _execute(
        "DELETE FROM curie.thread_workspaces "
        "WHERE selected_by_deployment_id = :deployment_id "
        "AND conversation_id = :conversation_id",
        {"deployment_id": deployment["id"], "conversation_id": scoped_thread},
    )
    bare_selection = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": reply_thread,
            "author": payload["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert bare_selection.status_code == 200, bare_selection.text

    refused = client.post(url, headers=WORKER_HEADERS)
    assert refused.status_code == 403
    assert "authorized" in refused.json()["detail"]


def test_disabled_deployment_flag_still_allows_authorized_publication(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Selection plus approval and allowlist, not the legacy flag, gate publish."""

    client, _ = publication_stack
    deployment = _create_deployment(
        client,
        auth_headers,
        workspace_enabled=False,
    )
    _, publication = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            dedupe_key="disabled-flag-publication",
        ),
    )
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text

    credential = client.post(
        f"/v1/internal/publications/{publication['id']}/credential",
        headers=WORKER_HEADERS,
    )

    assert credential.status_code == 200, credential.text
    assert credential.headers["cache-control"] == "no-store"
    assert credential.json()["repo_full_name"] == REPO
    audit = _rows(
        "SELECT purpose, outcome FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": publication["id"]},
    )
    assert audit == [{"purpose": "publication_push", "outcome": "issued"}]


def test_allowlist_revocation_stops_an_approved_publication_credential(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(client, _publication_payload(deployment["id"]))
    approved = _resolve(client, auth_headers, publication["approval_id"])
    assert approved.status_code == 200, approved.text
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", "[]")
    get_settings.cache_clear()

    refused = client.post(
        f"/v1/internal/publications/{publication['id']}/credential",
        headers=WORKER_HEADERS,
    )

    assert refused.status_code == 403
    assert refused.headers["cache-control"] == "no-store"
    assert "authorized" in refused.json()["detail"]
    audit = _rows(
        "SELECT outcome, detail FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": publication["id"]},
    )
    assert audit == [
        {
            "outcome": "refused",
            "detail": "publication repository is no longer authorized for this thread",
        }
    ]
    assert "ghp_publication_operator" not in json.dumps(audit, default=str)


def test_terminal_patch_retention_reaps_bytes_but_keeps_public_metadata(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    _, terminal = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            patch=b"terminal-private-patch",
            dedupe_key="terminal-publication",
        ),
    )
    _, pending = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            patch=b"pending-private-patch",
            dedupe_key="pending-publication",
        ),
    )
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=2)

    async def terminal_and_reap() -> int:
        from curie_api.models import Publication

        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                await session.execute(
                    update(Publication)
                    .where(Publication.id == uuid.UUID(terminal["id"]))
                    .values(
                        status="succeeded",
                        terminal_at=old,
                        result_url="https://github.com/acme-corp/acme-bot/pull/123",
                        version=Publication.version + 1,
                    )
                )
                await session.commit()
                return await crud.reap_terminal_publication_patches(
                    session,
                    terminal_before=old + timedelta(minutes=1),
                    limit=100,
                )
        finally:
            await engine.dispose()

    assert asyncio.run(terminal_and_reap()) == 1
    sizes = _rows(
        "SELECT id, octet_length(patch_bytes) AS size FROM curie.publications ORDER BY id"
    )
    by_id = {str(row["id"]): row["size"] for row in sizes}
    assert by_id[terminal["id"]] is None
    assert by_id[pending["id"]] == len(b"pending-private-patch")

    read = client.get(f"/publications/{terminal['id']}", headers=auth_headers)
    assert read.status_code == 200, read.text
    assert read.json()["status"] == "succeeded"
    assert read.json()["result_url"].endswith("/pull/123")
    _assert_patch_private(read.json(), "terminal-private-patch")


def test_two_publication_revisions_advance_one_durable_thread_lineage(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Each approved snapshot advances one branch and one PR by an exact head."""

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-two-revisions"
    first_payload = _publication_payload(
        deployment["id"],
        conversation_id=conversation_id,
        dedupe_key="lineage-revision-one",
    )

    first_status, first = _create_publication(client, first_payload)

    assert first_status == 201
    lineage_id = first["lineage_id"]
    uuid.UUID(lineage_id)
    assert first["revision_number"] == 1
    assert first["expected_prior_head"] == BASE_SHA
    assert first["lineage_base_sha"] == BASE_SHA
    assert first["lineage_head_sha"] is None
    assert first["lineage_state"] == "open"
    assert first["lineage_version"] == 1
    assert first["pr_number"] is None
    assert first["pr_url"] is None
    assert first["branch"].startswith("curie/publication-")

    approved = _resolve(client, auth_headers, first["approval_id"])
    assert approved.status_code == 200, approved.text
    advanced = _advance_lineage(
        client,
        first["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    assert {
        key: advanced.json()[key]
        for key in (
            "id",
            "deployment_id",
            "conversation_id",
            "repo_full_name",
            "base_sha",
            "branch",
            "pr_number",
            "pr_url",
            "head_sha",
            "state",
            "version",
            "latest_revision",
        )
    } == {
        "id": lineage_id,
        "deployment_id": deployment["id"],
        "conversation_id": _workspace_identity(first_payload),
        "repo_full_name": REPO,
        "base_sha": BASE_SHA,
        "branch": first["branch"],
        "pr_number": PR_NUMBER,
        "pr_url": PR_URL,
        "head_sha": FIRST_REVISION_SHA,
        "state": "open",
        "version": 2,
        "latest_revision": 1,
    }
    assert (
        client.get(f"/publications/{first['id']}", headers=auth_headers).json()[
            "status"
        ]
        == "succeeded"
    )
    assert advanced.json()["has_pending_outcome"] is True
    assert advanced.json()["visible_outcome_revision"] == 0
    _mark_outcome_history_ready(first["id"])

    second_payload = _publication_payload(
        deployment["id"],
        conversation_id=conversation_id,
        base_sha=FIRST_REVISION_SHA,
        dedupe_key="lineage-revision-two",
    )
    second_status, second = _create_publication(client, second_payload)

    assert second_status == 201
    assert second["lineage_id"] == lineage_id
    assert second["revision_number"] == 2
    assert second["expected_prior_head"] == FIRST_REVISION_SHA
    assert second["branch"] == first["branch"]
    assert second["pr_number"] == PR_NUMBER
    assert second["pr_url"] == PR_URL
    assert second["lineage_head_sha"] == FIRST_REVISION_SHA
    assert second["lineage_version"] == 2

    approved = _resolve(client, auth_headers, second["approval_id"])
    assert approved.status_code == 200, approved.text
    advanced = _advance_lineage(
        client,
        second["id"],
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    assert advanced.json()["id"] == lineage_id
    assert advanced.json()["head_sha"] == SECOND_REVISION_SHA
    assert advanced.json()["version"] == 3
    assert advanced.json()["latest_revision"] == 2
    assert advanced.json()["has_pending_outcome"] is True
    assert advanced.json()["visible_outcome_revision"] == 1

    current = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=lambda _request: _healthy_github_pr(
            branch=first["branch"], head_sha=SECOND_REVISION_SHA
        ),
    )
    assert current.status_code == 200, current.text
    assert current.json() == advanced.json()
    assert _counts() == (2, 2)
    assert _rows("SELECT count(*) AS count FROM curie.thread_publication_lineages") == [
        {"count": 1}
    ]


def test_concurrent_first_revisions_create_one_lineage_and_one_approval(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-concurrent-first-revision"
    selected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": channel_protocol.scoped_conversation_id(
                "slack", "C0EXAMPLE1", conversation_id
            ),
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    payloads = [
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key=f"concurrent-lineage-{index}",
        )
        for index in range(2)
    ]

    def attempt(payload: dict[str, Any]) -> Any:
        return client.post(
            "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(attempt, payloads))

    assert sorted(response.status_code for response in responses) == [201, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["detail"]["code"] == "publication.revision_conflict"
    winner = next(response.json() for response in responses if response.status_code == 201)
    assert winner["revision_number"] == 1
    assert winner["pr_number"] is None
    assert winner["lineage_head_sha"] is None
    assert _counts() == (1, 1)
    assert _rows("SELECT count(*) AS count FROM curie.thread_publication_lineages") == [
        {"count": 1}
    ]


def test_denied_first_revision_leaves_absent_pr_create_path_available(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-denied-first-revision"
    _, first = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="denied-lineage-one",
        ),
    )
    denied = _resolve(
        client,
        auth_headers,
        first["approval_id"],
        decision="rejected",
    )
    assert denied.status_code == 200, denied.text

    after_denial = _get_lineage(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
    )
    assert after_denial.status_code == 200, after_denial.text
    assert after_denial.json()["pr_number"] is None
    assert after_denial.json()["pr_url"] is None
    assert after_denial.json()["head_sha"] is None
    assert after_denial.json()["version"] == 1
    assert after_denial.json()["has_pending_outcome"] is True
    assert after_denial.json()["visible_outcome_revision"] == 0

    blocked = client.post(
        "/v1/internal/publications",
        json=_publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="denied-lineage-two",
        ),
        headers=WORKER_HEADERS,
    )
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "publication.outcome_pending"

    _mark_outcome_history_ready(first["id"])
    visible = _get_lineage(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
    )
    assert visible.status_code == 200, visible.text
    assert visible.json()["has_pending_outcome"] is False
    assert visible.json()["visible_outcome_revision"] == 1
    status_code, retry = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="denied-lineage-two-ready",
        ),
    )
    assert status_code == 201
    assert retry["lineage_id"] == first["lineage_id"]
    assert retry["revision_number"] == 2
    assert retry["branch"] == first["branch"]
    assert retry["expected_prior_head"] == BASE_SHA
    assert retry["pr_number"] is None
    assert retry["pr_url"] is None
    assert retry["lineage_head_sha"] is None
    assert _counts() == (2, 2)


def test_redeployment_reuses_agent_owned_thread_lineage(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    first_deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-same-agent-redeployment"
    _, first = _create_publication(
        client,
        _publication_payload(
            first_deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="same-agent-redeployment-one",
        ),
    )
    assert _resolve(client, auth_headers, first["approval_id"]).status_code == 200
    advanced = _advance_lineage(
        client,
        first["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    _mark_outcome_history_ready(first["id"])

    agent_id = first_deployment["agent_id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v2", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    redeployment = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=auth_headers,
    )
    assert redeployment.status_code == 201, redeployment.text

    _, second = _create_publication(
        client,
        _publication_payload(
            redeployment.json()["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key="same-agent-redeployment-two",
        ),
    )
    assert second["lineage_id"] == first["lineage_id"]
    assert second["branch"] == first["branch"]
    assert second["pr_number"] == PR_NUMBER
    assert second["revision_number"] == 2
    assert _rows("SELECT count(*) AS count FROM curie.thread_publication_lineages") == [
        {"count": 1}
    ]


def test_same_thread_and_repo_are_isolated_between_agents(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    first_deployment = _create_deployment(client, auth_headers, channel="C0EXAMPLE1")
    second_deployment = _create_deployment(client, auth_headers, channel="C0EXAMPLE2")
    conversation_id = "thread-agent-isolation"

    _, first = _create_publication(
        client,
        _publication_payload(
            first_deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="agent-isolation-one",
        ),
    )
    _, second = _create_publication(
        client,
        _publication_payload(
            second_deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="agent-isolation-two",
        ),
    )

    assert first["lineage_id"] != second["lineage_id"]
    assert first["branch"] != second["branch"]
    assert _rows("SELECT count(*) AS count FROM curie.thread_publication_lineages") == [
        {"count": 2}
    ]


@pytest.mark.parametrize("terminal_state", ["merged", "closed"])
def test_terminal_lineage_refuses_revision_and_credential_before_resolution(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    terminal_state: str,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = f"thread-{terminal_state}-lineage"
    _, first = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key=f"{terminal_state}-lineage-one",
        ),
    )
    assert _resolve(client, auth_headers, first["approval_id"]).status_code == 200
    opened = _advance_lineage(
        client,
        first["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert opened.status_code == 200, opened.text
    _mark_outcome_history_ready(first["id"])
    _, pending = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key=f"{terminal_state}-lineage-two",
        ),
    )
    assert _resolve(client, auth_headers, pending["approval_id"]).status_code == 200
    terminal = _advance_lineage(
        client,
        pending["id"],
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=FIRST_REVISION_SHA,
        state=terminal_state,
    )
    assert terminal.status_code == 200, terminal.text
    assert terminal.json()["state"] == terminal_state

    def forbidden_resolver(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("terminal lineage reached credential resolution")

    monkeypatch.setattr(
        "curie_api.routers.publications.resolve_repository_credential",
        forbidden_resolver,
    )
    credential = client.post(
        f"/v1/internal/publications/{pending['id']}/credential",
        headers=WORKER_HEADERS,
    )
    assert credential.status_code == 409
    assert credential.headers["cache-control"] == "no-store"
    assert credential.json()["detail"]["code"] == "publication.lineage_terminal"

    refused = client.post(
        "/v1/internal/publications",
        json=_publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key=f"{terminal_state}-lineage-three",
        ),
        headers=WORKER_HEADERS,
    )
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "publication.lineage_terminal"
    assert _counts() == (2, 2)
    assert _rows(
        "SELECT outcome FROM curie.credential_redemption_audit_entries "
        "WHERE publication_id = :id ORDER BY created_at, id",
        {"id": pending["id"]},
    ) == [{"outcome": "refused"}]


def test_publication_revision_refuses_a_checkout_not_at_the_lineage_head(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-stale-checkout-head"
    _, first = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="stale-checkout-one",
        ),
    )
    assert _resolve(client, auth_headers, first["approval_id"]).status_code == 200
    advanced = _advance_lineage(
        client,
        first["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    _mark_outcome_history_ready(first["id"])

    stale = client.post(
        "/v1/internal/publications",
        json=_publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=EXTERNAL_REVISION_SHA,
            dedupe_key="stale-checkout-two",
        ),
        headers=WORKER_HEADERS,
    )

    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "publication.lineage_stale"
    assert _counts() == (1, 1)
    current = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=lambda _request: _healthy_github_pr(
            branch=first["branch"], head_sha=FIRST_REVISION_SHA
        ),
    ).json()
    assert current["head_sha"] == FIRST_REVISION_SHA
    assert current["version"] == 2


def test_lineage_advance_cas_refuses_stale_version_and_expected_head(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-lineage-cas"
    _, first = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="lineage-cas-one",
        ),
    )
    assert _resolve(client, auth_headers, first["approval_id"]).status_code == 200
    assert (
        _advance_lineage(
            client,
            first["id"],
            expected_version=1,
            expected_head_sha=None,
            head_sha=FIRST_REVISION_SHA,
        ).status_code
        == 200
    )
    _mark_outcome_history_ready(first["id"])
    _, second = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key="lineage-cas-two",
        ),
    )
    assert _resolve(client, auth_headers, second["approval_id"]).status_code == 200

    stale_version = _advance_lineage(
        client,
        second["id"],
        expected_version=1,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert stale_version.status_code == 409
    assert stale_version.json()["detail"]["code"] == "publication.lineage_stale"
    stale_head = _advance_lineage(
        client,
        second["id"],
        expected_version=2,
        expected_head_sha=EXTERNAL_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert stale_head.status_code == 409
    assert stale_head.json()["detail"]["code"] == "publication.lineage_stale"

    current = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=lambda _request: _healthy_github_pr(
            branch=first["branch"], head_sha=FIRST_REVISION_SHA
        ),
    ).json()
    assert current["head_sha"] == FIRST_REVISION_SHA
    assert current["version"] == 2
    assert (
        client.get(f"/publications/{second['id']}", headers=auth_headers).json()[
            "status"
        ]
        == "approved"
    )


def test_concurrent_lineage_advances_have_one_exact_cas_winner(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    conversation_id = "thread-concurrent-lineage-cas"
    _, first = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            dedupe_key="concurrent-cas-one",
        ),
    )
    assert _resolve(client, auth_headers, first["approval_id"]).status_code == 200
    assert (
        _advance_lineage(
            client,
            first["id"],
            expected_version=1,
            expected_head_sha=None,
            head_sha=FIRST_REVISION_SHA,
        ).status_code
        == 200
    )
    _mark_outcome_history_ready(first["id"])
    _, second = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key="concurrent-cas-two",
        ),
    )
    assert _resolve(client, auth_headers, second["approval_id"]).status_code == 200

    def attempt(head_sha: str) -> Any:
        return _advance_lineage(
            client,
            second["id"],
            expected_version=2,
            expected_head_sha=FIRST_REVISION_SHA,
            head_sha=head_sha,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(attempt, [SECOND_REVISION_SHA, EXTERNAL_REVISION_SHA]))

    assert sorted(response.status_code for response in responses) == [200, 409]
    loser = next(response for response in responses if response.status_code == 409)
    assert loser.json()["detail"]["code"] == "publication.lineage_stale"
    winner = next(response.json() for response in responses if response.status_code == 200)
    current = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=lambda _request: _healthy_github_pr(
            branch=first["branch"], head_sha=winner["head_sha"]
        ),
    ).json()
    assert current["head_sha"] == winner["head_sha"]
    assert current["version"] == 3
    assert current["latest_revision"] == 2


def test_lineage_get_refreshes_stored_pr_truth_and_reports_pending_revision_safely(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    conversation_id = "thread-refresh-open-lineage"
    deployment, first = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )
    _, pending = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key="refresh-open-lineage-revision-two",
        ),
    )
    calls: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        # GitHub's Get a pull request response owns ``state``, ``merged``,
        # ``html_url``, and base/head refs and SHAs:
        # https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#get-a-pull-request
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "state": "open",
                "merged": False,
                "base": {"ref": "main", "sha": BASE_SHA},
                "head": {
                    "ref": first["branch"],
                    "sha": FIRST_REVISION_SHA,
                },
            },
        )

    refreshed = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=github,
    )

    assert refreshed.status_code == 200, refreshed.text
    body = refreshed.json()
    assert set(body) == {
        "id",
        "deployment_id",
        "conversation_id",
        "repo_full_name",
        "base_sha",
        "branch",
        "pr_number",
        "pr_url",
        "head_sha",
        "state",
        "version",
        "latest_revision",
        "has_pending_revision",
        "has_pending_outcome",
        "visible_outcome_revision",
    }
    assert body["id"] == first["lineage_id"]
    assert body["pr_number"] == PR_NUMBER
    assert body["pr_url"] == PR_URL
    assert body["head_sha"] == FIRST_REVISION_SHA
    assert body["state"] == "open"
    assert body["latest_revision"] == 2
    assert body["has_pending_revision"] is True
    assert body["has_pending_outcome"] is False
    assert body["visible_outcome_revision"] == 1
    assert len(calls) == 1
    assert calls[0].method == "GET"
    assert calls[0].url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}"
    assert calls[0].headers["accept"] == "application/vnd.github+json"
    assert calls[0].headers["x-github-api-version"] == "2022-11-28"
    assert calls[0].headers["authorization"].startswith("Basic ")
    rendered = json.dumps(body, sort_keys=True)
    assert pending["id"] not in rendered
    assert "authorization" not in rendered.casefold()
    assert "credential" not in rendered.casefold()
    assert "token" not in rendered.casefold()
    assert _rows("SELECT * FROM curie.credential_redemption_audit_entries") == []


def test_lineage_get_refuses_redirect_without_forwarding_authorization(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """An authorization-bearing GitHub read stops at the first origin."""

    client, _ = publication_stack
    conversation_id = "thread-refresh-redirect-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )
    calls: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                302,
                headers={"Location": "https://capture.example.test/pull"},
            )
        raise AssertionError("the redirect target received the GitHub request")

    # The app-wide client may follow redirects for unrelated consumers. The
    # credentialed lineage operation must override that behavior at its callsite.
    injected = httpx.AsyncClient(
        transport=httpx.MockTransport(github),
        follow_redirects=True,
    )
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        refused = _get_lineage(
            client,
            deployment_id=deployment["id"],
            conversation_id=conversation_id,
        )
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())

    assert refused.status_code == 503
    assert refused.json()["detail"]["code"] == "publication.github_unavailable"
    assert len(calls) == 1
    assert calls[0].url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}"
    assert "Authorization" in calls[0].headers
    assert _rows(
        "SELECT status, head_sha, version FROM curie.thread_publication_lineages "
        "WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    ) == [{"status": "open", "head_sha": FIRST_REVISION_SHA, "version": 2}]


@pytest.mark.parametrize("upstream_status", [403, 404, 429, 500, 502])
def test_lineage_get_refuses_non_200_github_truth_without_mutating_lineage(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    upstream_status: int,
) -> None:
    client, _ = publication_stack
    conversation_id = f"thread-refresh-http-{upstream_status}-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )

    refused = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=lambda _request: httpx.Response(upstream_status),
    )

    assert refused.status_code == 503
    detail = refused.json()["detail"]
    assert detail["code"] == "publication.github_unavailable"
    assert "Try again later" in detail["message"]
    assert _rows(
        "SELECT status, head_sha, version FROM curie.thread_publication_lineages "
        "WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    ) == [{"status": "open", "head_sha": FIRST_REVISION_SHA, "version": 2}]
    assert _rows("SELECT * FROM curie.credential_redemption_audit_entries") == []


def test_lineage_get_refuses_github_transport_failure_without_mutating_lineage(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    conversation_id = "thread-refresh-transport-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )

    def unavailable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("upstream unavailable", request=request)

    refused = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=unavailable,
    )

    assert refused.status_code == 503
    assert refused.json()["detail"]["code"] == "publication.github_unavailable"
    assert _rows(
        "SELECT status, head_sha, version FROM curie.thread_publication_lineages "
        "WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    ) == [{"status": "open", "head_sha": FIRST_REVISION_SHA, "version": 2}]
    assert _rows("SELECT * FROM curie.credential_redemption_audit_entries") == []


def test_lineage_get_adopts_head_for_migrated_succeeded_publication(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """A legacy URL-only lineage learns its existing PR head exactly once."""

    client, _ = publication_stack
    conversation_id = "thread-migrated-succeeded-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )

    async def clear_migration_unknown_head() -> None:
        from curie_api.models import ThreadPublicationLineage

        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                await session.execute(
                    update(ThreadPublicationLineage)
                    .where(
                        ThreadPublicationLineage.id
                        == uuid.UUID(publication["lineage_id"])
                    )
                    .values(head_sha=None, version=1)
                )
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(clear_migration_unknown_head())

    def github(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "state": "open",
                "merged": False,
                "base": {"ref": "main", "sha": BASE_SHA},
                "head": {
                    "ref": publication["branch"],
                    "sha": FIRST_REVISION_SHA,
                },
            },
        )

    refreshed = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=github,
    )

    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["head_sha"] == FIRST_REVISION_SHA
    assert refreshed.json()["version"] == 2
    assert _rows(
        "SELECT head_sha, version FROM curie.thread_publication_lineages "
        "WHERE id = :id",
        {"id": uuid.UUID(publication["lineage_id"])},
    ) == [{"head_sha": FIRST_REVISION_SHA, "version": 2}]


@pytest.mark.parametrize(
    ("merged", "expected_state"),
    [(True, "merged"), (False, "closed")],
)
def test_lineage_get_persists_terminal_github_truth_without_credential_redemption(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    merged: bool,
    expected_state: str,
) -> None:
    client, _ = publication_stack
    conversation_id = f"thread-refresh-{expected_state}-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )
    calls: list[httpx.Request] = []

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "state": "closed",
                "merged": merged,
                "base": {"ref": "main", "sha": BASE_SHA},
                "head": {
                    "ref": publication["branch"],
                    "sha": FIRST_REVISION_SHA,
                },
            },
        )

    refreshed = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=github,
    )

    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["state"] == expected_state
    assert refreshed.json()["head_sha"] == FIRST_REVISION_SHA
    assert refreshed.json()["has_pending_revision"] is False
    assert refreshed.json()["version"] == 3
    assert len(calls) == 1
    stored = client.get(
        f"/publications/{publication['id']}", headers=auth_headers
    )
    assert stored.status_code == 200, stored.text
    assert stored.json()["lineage_state"] == expected_state
    assert _rows("SELECT * FROM curie.credential_redemption_audit_entries") == []


@pytest.mark.parametrize("revision_status", ["approved", "launching", "running"])
def test_lineage_get_reports_pending_revision_while_its_push_is_in_flight(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
    revision_status: str,
) -> None:
    client, _ = publication_stack
    conversation_id = f"thread-refresh-in-flight-{revision_status}"
    deployment, first = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )
    _, revision = _create_publication(
        client,
        _publication_payload(
            deployment["id"],
            conversation_id=conversation_id,
            base_sha=FIRST_REVISION_SHA,
            dedupe_key=f"refresh-in-flight-{revision_status}",
        ),
    )
    approved = _resolve(client, auth_headers, revision["approval_id"])
    assert approved.status_code == 200, approved.text
    if revision_status != "approved":
        _execute(
            "UPDATE curie.publications SET status = :status WHERE id = :id",
            {"status": revision_status, "id": uuid.UUID(revision["id"])},
        )

    def github(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "state": "open",
                "merged": False,
                "base": {"ref": "main", "sha": BASE_SHA},
                "head": {
                    "ref": first["branch"],
                    "sha": SECOND_REVISION_SHA,
                },
            },
        )

    refreshed = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=github,
    )

    assert refreshed.status_code == 200, refreshed.text
    body = refreshed.json()
    assert body["head_sha"] == FIRST_REVISION_SHA
    assert body["version"] == 2
    assert body["latest_revision"] == 2
    assert body["has_pending_revision"] is True
    assert _rows(
        "SELECT head_sha, version FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": uuid.UUID(first["lineage_id"])},
    ) == [{"head_sha": FIRST_REVISION_SHA, "version": 2}]


def test_lineage_get_reports_external_head_without_pending_revision_or_overwrite(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    conversation_id = "thread-refresh-stale-lineage"
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )

    def github(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "state": "open",
                "merged": False,
                "base": {"ref": "main", "sha": BASE_SHA},
                "head": {
                    "ref": publication["branch"],
                    "sha": EXTERNAL_REVISION_SHA,
                },
            },
        )

    stale = _get_lineage_with_http(
        client,
        deployment_id=deployment["id"],
        conversation_id=conversation_id,
        handler=github,
    )

    assert stale.status_code == 409
    detail = stale.json()["detail"]
    assert detail["code"] == "publication.lineage_stale"
    assert detail["expected_head_sha"] == FIRST_REVISION_SHA
    assert detail["actual_head_sha"] == EXTERNAL_REVISION_SHA
    assert "credential" not in json.dumps(detail, sort_keys=True).casefold()
    stored = client.get(
        f"/publications/{publication['id']}", headers=auth_headers
    )
    assert stored.status_code == 200, stored.text
    assert stored.json()["lineage_head_sha"] == FIRST_REVISION_SHA
    assert stored.json()["lineage_state"] == "open"
    assert _rows("SELECT * FROM curie.credential_redemption_audit_entries") == []


@pytest.mark.parametrize(
    ("operation", "method"),
    [
        ("/v1/internal/publications/lineage", "GET"),
        ("/v1/internal/publications/{publication_id}/lineage", "PATCH"),
    ],
)
def test_publication_lineage_route_operations_are_in_every_http_metric_domain(
    operation: str,
    method: str,
) -> None:
    active = {
        "service.name": "curie-api",
        "operation": operation,
        "role": "server",
        "source": method,
    }
    record_metric("curie.http.server.active", 1, attributes=active)
    complete = {**active, "outcome": "2xx"}
    record_metric("curie.http.server.request", attributes=complete)
    record_metric("curie.http.server.request.duration", 0.001, attributes=complete)


# These API integration tests use the actual disposable Postgres/Valkey stack;
# only GitHub HTTP is fixture-controlled. Provider identity fields are documented
# at https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request and
# https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app.


def _reserve_review(
    client: TestClient, lineage: dict[str, Any], origin: str, *, version: int | None = None
) -> Any:
    return client.post(
        "/v1/internal/publications/review-reservations",
        headers=WORKER_HEADERS,
        json={
            "repository_id": 9001,
            "pr_number": PR_NUMBER,
            "expected_lineage_version": version or lineage["version"],
            "origin_key": origin,
        },
    )


def test_review_reservation_refuses_legacy_unproved_lineage(
    clean_db: None, publication_stack: tuple[TestClient, str], auth_headers: dict[str, str]
) -> None:
    client, stream = publication_stack
    _, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id='test-1',
    )
    response = _reserve_review(client, {"version": 2}, "review:legacy")
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "publication.review_ineligible"
    assert _rows("SELECT count(*) AS n FROM curie.publication_review_reservations")[0]["n"] == 0
    assert (
        _rows(
            "SELECT github_repository_id FROM curie.thread_publication_lineages WHERE id=:id",
            {"id": publication["lineage_id"]},
        )[0]["github_repository_id"]
        is None
    )
    valkey = connect_or_skip(decode_responses=True)
    try:
        assert valkey.xlen(stream) == 0
    finally:
        valkey.close()


def test_review_reservation_requires_worker_identity(
    clean_db: None, publication_stack: tuple[TestClient, str], auth_headers: dict[str, str]
) -> None:
    client, _ = publication_stack
    response = client.post(
        "/v1/internal/publications/review-reservations",
        headers=auth_headers,
        json={
            "repository_id": 9001,
            "pr_number": PR_NUMBER,
            "expected_lineage_version": 1,
            "origin_key": "review:untrusted",
        },
    )
    assert response.status_code == 401, response.text
    assert _rows("SELECT count(*) AS n FROM curie.publication_review_reservations")[0]["n"] == 0


@pytest.fixture
def review_lineage_app(
    clean_db: None, publication_stack: tuple[TestClient, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, dict[str, Any], str]]:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    client, stream = publication_stack
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setenv("GITHUB_APP_ID", "51")
    monkeypatch.setenv(
        "GITHUB_APP_PRIVATE_KEY",
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
    )
    get_settings.cache_clear()
    truth: dict[str, Any] = {
        "repository_id": 9001,
        "installation_id": 41,
        "node_id": "PR_example_123",
        "branch": "pending",
        "head_sha": FIRST_REVISION_SHA,
        "status": 200,
        "calls": [],
    }
    real_client = httpx.Client

    def handle(request: httpx.Request) -> httpx.Response:
        truth["calls"].append((request.method, request.url.path))
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": truth["installation_id"]})
        if request.url.path.endswith("/access_tokens"):
            return httpx.Response(
                201,
                json={
                    "token": "fixture-publication-app-token",
                    "expires_at": "2999-01-01T00:00:00Z",
                },
            )
        assert request.headers["authorization"] == "Bearer fixture-publication-app-token"
        if truth["status"] != 200:
            return httpx.Response(truth["status"], json={"message": "fixture-unavailable"})
        repo = {"id": truth["repository_id"], "full_name": REPO}
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=repo)
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": PR_URL,
                "node_id": truth["node_id"],
                "state": "open",
                "merged": False,
                "head": {"sha": truth["head_sha"], "ref": truth["branch"], "repo": repo},
                "base": {"repo": repo, "ref": "main"},
            },
        )

    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handle)),
    )
    injected = httpx.AsyncClient(transport=httpx.MockTransport(handle), follow_redirects=True)
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        yield client, truth, stream
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())
        _RESOLVERS.clear()
        get_settings.cache_clear()


def _verified_lineage(
    client: TestClient,
    truth: dict[str, Any],
    auth_headers: dict[str, str],
    *,
    conversation: str = "review-original",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    deployment = _create_deployment(client, auth_headers)
    _, publication = _create_publication(
        client, _publication_payload(deployment["id"], conversation_id=conversation)
    )
    truth["branch"] = publication["branch"]
    resolved = _resolve(client, auth_headers, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text
    advanced = _advance_lineage(
        client,
        publication["id"],
        expected_version=1,
        expected_head_sha=None,
        head_sha=FIRST_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    _mark_outcome_history_ready(publication["id"])
    return deployment, publication, advanced.json()


def test_review_reservation_captures_verified_identity_and_exact_replay(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, stream = review_lineage_app
    deployment, publication, lineage = _verified_lineage(client, truth, auth_headers)
    before_approvals = _rows("SELECT count(*) AS n FROM curie.approvals")[0]["n"]
    first = _reserve_review(client, lineage, "review:123:71")
    assert first.status_code == 201, first.text
    result = first.json()
    assert first.headers["cache-control"] == "no-store"
    assert (result["repository_id"], result["installation_id"], result["pr_node_id"]) == (
        9001,
        41,
        "PR_example_123",
    )
    assert result["agent_id"] == deployment["agent_id"]
    assert result["reply_conversation_id"] == "review-original"
    assert result["conversation_id"] == channel_protocol.scoped_conversation_id(
        "slack", "C0EXAMPLE1", "review-original"
    )
    assert result["revision_number"] == 2 and result["expected_head_sha"] == FIRST_REVISION_SHA
    replay = _reserve_review(client, lineage, "review:123:71")
    assert replay.status_code == 200 and replay.json() == result
    conflict = _reserve_review(client, lineage, "review:123:72")
    assert (
        conflict.status_code == 409
        and conflict.json()["detail"]["code"] == "publication.revision_conflict"
    )
    assert _rows("SELECT count(*) AS n FROM curie.publication_review_reservations")[0]["n"] == 1
    assert _rows("SELECT count(*) AS n FROM curie.approvals")[0]["n"] == before_approvals
    valkey = connect_or_skip(decode_responses=True)
    try:
        assert valkey.xlen(stream) == 0
    finally:
        valkey.close()


def test_review_reservation_stale_version_and_binding_reassert_are_refused(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    stale = _reserve_review(client, lineage, "review:stale", version=1)
    assert (
        stale.status_code == 409 and stale.json()["detail"]["code"] == "publication.lineage_stale"
    )
    rebind = client.patch(
        f"/agents/{deployment['agent_id']}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1", "expected_generation": 0},
        json={"kind": "slack", "address": "C0EXAMPLE1"},
        headers=auth_headers,
    )
    assert rebind.status_code == 200, rebind.text
    denied = _reserve_review(client, lineage, "review:rebound")
    assert (
        denied.status_code == 409
        and denied.json()["detail"]["code"] == "publication.review_ineligible"
    )
    assert _rows("SELECT count(*) AS n FROM curie.publication_review_reservations")[0]["n"] == 0


def test_review_reservation_concurrency_has_one_winner(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    _, _, lineage = _verified_lineage(client, truth, auth_headers)
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(lambda n: _reserve_review(client, lineage, f"review:concurrent:{n}"), range(4))
        )
    assert sorted(response.status_code for response in responses) == [201, 409, 409, 409]
    assert (
        _rows(
            "SELECT count(*) AS n FROM curie.publication_review_reservations "
            "WHERE status='reserved'"
        )[0]["n"]
        == 1
    )


def test_review_reservation_only_explicit_origin_can_create_fresh_approval(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    reservation = _reserve_review(client, lineage, "review:accepted-origin")
    assert reservation.status_code == 201, reservation.text
    payload = _publication_payload(
        deployment["id"], conversation_id="review-original", base_sha=FIRST_REVISION_SHA
    )
    for origin in (None, "review:other-origin"):
        attempted = dict(payload)
        if origin is not None:
            attempted["review_origin_key"] = origin
        refused = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=attempted)
        assert refused.status_code == 409, refused.text
    payload["review_origin_key"] = "review:accepted-origin"
    created = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=payload)
    assert created.status_code == 201, created.text
    assert created.json()["id"] == reservation.json()["revision_id"]
    assert created.json()["status"] == "pending" and created.json()["revision_number"] == 2
    assert created.json()["pr_number"] == PR_NUMBER
    assert (
        _rows("SELECT status FROM curie.publication_review_reservations")[0]["status"] == "consumed"
    )
    # Replaying an ordinary snapshot cannot strip the review origin from an
    # already-created approval, even if all patch and dedupe bytes match.
    payload.pop("review_origin_key")
    changed = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=payload)
    assert changed.status_code == 409, changed.text


def test_review_reservation_cancel_is_fenced_and_frees_only_its_own_slot(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    _, _, lineage = _verified_lineage(client, truth, auth_headers)
    reservation = _reserve_review(client, lineage, "review:cancel-me").json()
    path = f"/v1/internal/publications/review-reservations/{reservation['revision_id']}/cancel"
    for origin, version in (("review:other", 1), ("review:cancel-me", 2)):
        refused = client.post(
            path, headers=WORKER_HEADERS, json={"origin_key": origin, "expected_version": version}
        )
        assert refused.status_code == 409, refused.text
    cancelled = client.post(
        path, headers=WORKER_HEADERS, json={"origin_key": "review:cancel-me", "expected_version": 1}
    )
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    repeated = client.post(
        path, headers=WORKER_HEADERS, json={"origin_key": "review:cancel-me", "expected_version": 1}
    )
    assert repeated.status_code == 409
    next_origin = _reserve_review(client, lineage, "review:after-cancel")
    assert next_origin.status_code == 201 and next_origin.json()["revision_number"] == 2


@pytest.mark.parametrize(
    "mutation", ["repository_id", "installation_id", "node_id", "head_sha", "status"]
)
def test_review_identity_cannot_change_on_existing_pr_advance(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    mutation: str,
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    _, publication = _create_publication(
        client,
        _publication_payload(
            deployment["id"], conversation_id="review-original", base_sha=FIRST_REVISION_SHA
        ),
    )
    assert _resolve(client, auth_headers, publication["approval_id"]).status_code == 200
    truth["head_sha"] = SECOND_REVISION_SHA
    truth[mutation] = {
        "repository_id": 9002,
        "installation_id": 42,
        "node_id": "PR_other_123",
        "head_sha": EXTERNAL_REVISION_SHA,
        "status": 503,
    }[mutation]
    refused = _advance_lineage(
        client,
        publication["id"],
        expected_version=lineage["version"],
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert refused.status_code == (503 if mutation == "status" else 409), refused.text
    stored = _rows(
        "SELECT github_repository_id, github_installation_id, github_pr_node_id, "
        "head_sha, version FROM curie.thread_publication_lineages WHERE id=:id",
        {"id": lineage["id"]},
    )[0]
    assert stored == {
        "github_repository_id": 9001,
        "github_installation_id": 41,
        "github_pr_node_id": "PR_example_123",
        "head_sha": FIRST_REVISION_SHA,
        "version": lineage["version"],
    }


def test_review_identity_constraints_reject_partial_authority(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    _, _, lineage = _verified_lineage(client, truth, auth_headers)
    for assignment in (
        "github_repository_id=NULL",
        "github_installation_id=NULL",
        "github_pr_node_id=NULL",
        "base_ref=NULL",
    ):
        with pytest.raises(IntegrityError):
            _execute(
                "UPDATE curie.thread_publication_lineages SET " + assignment + " WHERE id=:id",
                {"id": lineage["id"]},
            )
    assert (
        _rows(
            "SELECT github_repository_id FROM curie.thread_publication_lineages WHERE id=:id",
            {"id": lineage["id"]},
        )[0]["github_repository_id"]
        == 9001
    )


def test_review_authority_downgrade_refuses_an_active_reservation(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    from alembic import command
    from alembic.config import Config

    client, truth, _ = review_lineage_app
    _, _, lineage = _verified_lineage(client, truth, auth_headers)
    api_dir = Path(__file__).resolve().parents[1]
    config = Config(str(api_dir / "alembic.ini"))
    config.set_main_option("script_location", str(api_dir / "alembic"))
    original_revision = _rows("SELECT version_num FROM curie.alembic_version")
    try:
        # #2300 commits each migration independently. Exercise the authority
        # guard at its own boundary, after higher revisions have safely retired.
        command.downgrade(config, "0042")
        before_revision = _rows("SELECT version_num FROM curie.alembic_version")
        assert before_revision == [{"version_num": "0042"}]
        reservation = _reserve_review(client, lineage, "review:retain-on-rollback")
        assert reservation.status_code == 201, reservation.text
        before_reservation = _rows("SELECT * FROM curie.publication_review_reservations")
        assert [row["status"] for row in before_reservation] == ["reserved"]
        with pytest.raises(RuntimeError, match="revisions are active"):
            command.downgrade(config, "0041")
        assert _rows("SELECT version_num FROM curie.alembic_version") == before_revision
        assert _rows("SELECT * FROM curie.publication_review_reservations") == before_reservation
    finally:
        command.upgrade(config, original_revision[0]["version_num"])
    assert _rows("SELECT version_num FROM curie.alembic_version") == original_revision


@pytest.mark.parametrize(
    "changed_route", [{"reply_channel": "C0EXAMPLE2"}, {"reply_conversation_id": "another-thread"}]
)
def test_scoped_review_consumption_cannot_redirect_the_fresh_approval(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    changed_route: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    reservation = _reserve_review(client, lineage, "review:scoped-route")
    assert reservation.status_code == 201, reservation.text
    payload = _publication_payload(
        deployment["id"], conversation_id=lineage["conversation_id"], base_sha=FIRST_REVISION_SHA
    )
    payload.update(reply_conversation_id="review-original", review_origin_key="review:scoped-route")
    payload.update(changed_route)
    refused = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=payload)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.review_ineligible"
    assert _rows("SELECT count(*) AS n FROM curie.publications WHERE status='pending'")[0]["n"] == 0
    assert (
        _rows("SELECT status FROM curie.publication_review_reservations")[0]["status"] == "reserved"
    )
    payload.update(reply_channel="C0EXAMPLE1", reply_conversation_id="review-original")
    accepted = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=payload)
    assert accepted.status_code == 201, accepted.text
    assert accepted.json()["id"] == reservation.json()["revision_id"]
    assert accepted.json()["reply_channel"] == "C0EXAMPLE1"


def test_cancelled_review_origin_remains_a_non_executing_tombstone(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    reserved = _reserve_review(client, lineage, "review:never-rearm").json()
    response = client.post(
        f"/v1/internal/publications/review-reservations/{reserved['revision_id']}/cancel",
        headers=WORKER_HEADERS,
        json={"origin_key": "review:never-rearm", "expected_version": 1},
    )
    assert response.status_code == 200
    replay = _reserve_review(client, lineage, "review:never-rearm")
    assert replay.status_code == 200 and replay.json()["status"] == "cancelled"
    payload = _publication_payload(
        deployment["id"], conversation_id="review-original", base_sha=FIRST_REVISION_SHA
    )
    payload["review_origin_key"] = "review:never-rearm"
    rejected = client.post("/v1/internal/publications", headers=WORKER_HEADERS, json=payload)
    assert rejected.status_code == 409
    assert _rows("SELECT count(*) AS n FROM curie.publications WHERE status='pending'")[0]["n"] == 0


def test_verified_github_pr_has_only_one_conversation_owner(
    review_lineage_app: tuple[TestClient, dict[str, Any], str], auth_headers: dict[str, str]
) -> None:
    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    _, other = _create_publication(
        client, _publication_payload(deployment["id"], conversation_id="other-conversation")
    )
    assert _resolve(client, auth_headers, other["approval_id"]).status_code == 200
    truth["branch"] = other["branch"]
    conflict = _advance_lineage(
        client, other["id"], expected_version=1, expected_head_sha=None, head_sha=FIRST_REVISION_SHA
    )
    assert (
        conflict.status_code == 409
        and conflict.json()["detail"]["code"] == "publication.revision_conflict"
    )
    owners = _rows(
        "SELECT id FROM curie.thread_publication_lineages "
        "WHERE github_repository_id=9001 AND pr_number=:number",
        {"number": PR_NUMBER},
    )
    assert owners == [{"id": uuid.UUID(lineage["id"])}]
    assert _rows(
        "SELECT github_repository_id,head_sha FROM curie.thread_publication_lineages WHERE id=:id",
        {"id": other["lineage_id"]},
    ) == [{"github_repository_id": None, "head_sha": None}]


def test_adding_app_after_pat_publication_does_not_backfill_legacy_identity(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, truth, _ = review_lineage_app
    conversation_id = "legacy-pat-thread"
    app_id = get_settings().github_app_id
    monkeypatch.setenv("GITHUB_APP_ID", "")
    get_settings.cache_clear()
    deployment, publication = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation_id,
    )
    monkeypatch.setenv("GITHUB_APP_ID", app_id)
    get_settings.cache_clear()
    _, later = _create_publication(
        client,
        _publication_payload(
            deployment["id"], conversation_id=conversation_id, base_sha=FIRST_REVISION_SHA
        ),
    )
    assert _resolve(client, auth_headers, later["approval_id"]).status_code == 200
    advanced = _advance_lineage(
        client,
        later["id"],
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    assert truth["calls"] == []
    assert _rows(
        "SELECT github_repository_id,head_sha FROM curie.thread_publication_lineages WHERE id=:id",
        {"id": publication["lineage_id"]},
    ) == [{"github_repository_id": None, "head_sha": SECOND_REVISION_SHA}]
    assert _reserve_review(client, advanced.json(), "review:legacy-replay").status_code == 409


@pytest.mark.parametrize("mutation", ["binding", "head", "deployment"])
def test_review_reservation_refreshes_authority_already_loaded_by_its_caller(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    mutation: str,
) -> None:
    """A row lock must refresh objects retained during earlier authority reads."""
    from curie_api.models import AgentChannel, Deployment, ThreadPublicationLineage
    from curie_api.schemas import ReviewRevisionReserve
    from sqlalchemy.ext.asyncio import AsyncSession

    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    later = None
    if mutation == "head":
        _, later = _create_publication(
            client,
            _publication_payload(
                deployment["id"], conversation_id="review-original", base_sha=FIRST_REVISION_SHA
            ),
        )
        assert _resolve(client, auth_headers, later["approval_id"]).status_code == 200

    async def exercise() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                loaded = await session.get(ThreadPublicationLineage, uuid.UUID(lineage["id"]))
                assert loaded is not None
                binding = await session.get(AgentChannel, loaded.binding_id)
                assert binding is not None
                held_deployment = await session.get(Deployment, loaded.deployment_id)
                assert held_deployment is not None
                # Keep both strong references while another real API transaction
                # changes authority. FOR UPDATE alone does not refresh this map.
                if mutation == "binding":
                    response = await asyncio.to_thread(
                        client.patch,
                        f"/agents/{deployment['agent_id']}/channels",
                        params={
                            "kind": "slack",
                            "address": "C0EXAMPLE1",
                            "expected_generation": binding.generation,
                        },
                        json={"kind": "slack", "address": "C0EXAMPLE1"},
                        headers=auth_headers,
                    )
                elif mutation == "deployment":
                    response = await asyncio.to_thread(
                        client.delete, f"/deployments/{deployment['id']}", headers=auth_headers
                    )
                else:
                    assert later is not None
                    truth["head_sha"] = EXTERNAL_REVISION_SHA
                    response = await asyncio.to_thread(
                        _advance_lineage,
                        client,
                        later["id"],
                        expected_version=lineage["version"],
                        expected_head_sha=FIRST_REVISION_SHA,
                        head_sha=EXTERNAL_REVISION_SHA,
                    )
                    await asyncio.to_thread(_mark_outcome_history_ready, later["id"])
                assert response.status_code == (204 if mutation == "deployment" else 200), (
                    response.text
                )
                with pytest.raises(crud.PublicationLineageConflict) as conflict:
                    await crud.reserve_review_revision(
                        session,
                        ReviewRevisionReserve(
                            repository_id=9001,
                            pr_number=PR_NUMBER,
                            expected_lineage_version=lineage["version"],
                            origin_key=f"review:cached-{mutation}",
                        ),
                    )
                assert conflict.value.code == (
                    "publication.review_ineligible" if mutation != "head"
                    else "publication.lineage_stale"
                )
        finally:
            await engine.dispose()

    asyncio.run(exercise())
    assert _rows("SELECT count(*) AS n FROM curie.publication_review_reservations")[0]["n"] == 0
