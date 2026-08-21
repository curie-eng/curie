"""Durable, approval-gated publication control-plane contracts.

The API owns only durable state and operator-credential redemption.  Resolving a
publication approval never enqueues a model turn and never performs Kubernetes
or GitHub side effects; the worker reconciles the durable publication row.
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis
import redis.asyncio as aioredis
from curie_api import crud
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_api.resumequeue import ResumeQueue
from curie_api.schemas import PublicationCreate
from curie_api.sweeper import sweep_expired_approvals
from curie_test_support.valkey import connect_or_skip
from fastapi import Response
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
WORKER_TOKEN = "remote-dev-publication-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
PATCH_LIMIT = 900_000
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"


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
            "workspace_enabled": True,
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
) -> dict[str, Any]:
    return {
        "deployment_id": deployment_id,
        "conversation_id": f"thread-{uuid.uuid4().hex[:8]}",
        "repo_full_name": REPO,
        "author": author,
        "summary": "Publish the repository changes",
        "reply_kind": "slack",
        "reply_channel": "C0EXAMPLE1",
        "reply_placeholder": "1700000000.000001",
        "dedupe_key": dedupe_key or f"publish-{uuid.uuid4().hex}",
        "base_sha": BASE_SHA,
        "patch_b64": base64.b64encode(patch).decode(),
        "changed_paths": ["README.md"],
        "expires_in_seconds": expires_in_seconds,
    }


def test_publication_schema_refuses_github_workflow_changes() -> None:
    payload = _publication_payload(str(uuid.uuid4()))
    payload["changed_paths"] = [".github/workflows/publish.yml"]

    with pytest.raises(ValidationError, match="workflow changes cannot be published"):
        PublicationCreate.model_validate(payload)


def _create_publication(client: TestClient, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    selected = client.post(
        f"/v1/internal/workspaces/{payload['deployment_id']}/selection",
        json={
            "conversation_id": payload["conversation_id"],
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    response = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)
    assert response.status_code in (200, 201), response.text
    return response.status_code, response.json()


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


def _counts() -> tuple[int, int]:
    row = _rows(
        "SELECT (SELECT count(*) FROM curie.approvals) AS approvals, "
        "(SELECT count(*) FROM curie.publications) AS publications"
    )[0]
    return int(row["approvals"]), int(row["publications"])


def test_publication_persistence_refuses_casefolded_git_metadata_path(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"])
    payload["changed_paths"] = [".GIT/config"]

    refused = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )

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
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={
            "decision": decision,
            "resolved_by": actor,
            "note": note,
            "actor_channel": channel,
        },
        headers=auth_headers,
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
    conflict = client.post(
        "/v1/internal/publications", json=changed_replay, headers=WORKER_HEADERS
    )
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
def test_publication_create_rejects_noncanonical_or_oversized_text(
    field: str, value: str
) -> None:
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

    other_deployment = _create_deployment(
        client, auth_headers, channel="C0EXAMPLE2"
    )
    wrong_agent = dict(payload, deployment_id=other_deployment["id"])
    rejected_agent = client.post(
        "/v1/internal/publications", json=wrong_agent, headers=WORKER_HEADERS
    )
    assert rejected_agent.status_code == 409


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
                        "conversation_id": payload["conversation_id"],
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(revoke_then_reselect())
    reselected = client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": payload["conversation_id"],
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
                    redeem_publication_credential(
                        uuid.UUID(publication["id"]), session, Response()
                    )
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
            "conversation_id": payload["conversation_id"],
            "author": payload["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    payload["repo_full_name"] = "acme-corp/acme-api"

    refused = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )

    assert refused.status_code == 409
    assert "differs" in refused.json()["detail"]
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
    replay = client.post(
        "/v1/internal/publications", json=changed, headers=WORKER_HEADERS
    )
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
        _publication_payload(
            deployment["id"], dedupe_key="event-card-outbox-dead-letter"
        ),
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


def test_publication_card_and_result_claims_survive_process_replacement(
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Expired leases reclaim, while failed result delivery observes backoff."""

    from curie_worker.publication_store import PostgresPublicationStore

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
            assert (
                abandoned_result.resolution_note
                == "Approved for the release fixture"
            )
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
                    await connection.execute(
                        text(
                            "SELECT resource_cleanup_completed_at IS NOT NULL AS completed, "
                            "resource_cleanup_error FROM curie.publications WHERE id = :id"
                        ),
                        {"id": publication["id"]},
                    )
                ).mappings().one()
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
            "conversation_id": "1700000000.000100",
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    class DatabasePublicationCreator:
        def __init__(self, engine: Any) -> None:
            self.engine = engine

        async def create_publication(
            self, request: PublicationCreateRequest
        ) -> CreatedPublication:
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
    rows = _rows(
        "SELECT patch_bytes, approval_card_reported_at FROM curie.publications"
    )
    assert rows == [
        {
            "patch_bytes": b"diff --git a/README.md b/README.md\n",
            "approval_card_reported_at": None,
        }
    ]


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
                "conversation_id": payload["conversation_id"],
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
            "conversation_id": payload["conversation_id"],
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


def test_publication_requester_self_approval_still_requires_membership_and_is_audited(
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
    assert resolved["evidence"]["publication_requester_exception"] is True
    assert resolved["evidence"]["approvers_channel"] == "C0EXAMPLE1"
    assert resolved["evidence"]["actor_channel"] == "C0EXAMPLE1"


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
