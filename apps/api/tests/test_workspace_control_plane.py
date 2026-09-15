"""Deployment workspace and worker-only clone-credential contracts.

These tests exercise the public deployment API and the private worker redemption
surface against the disposable Postgres database.  The repository selector is
always a stored deployment id: callers never get to aim the operator credential
at an arbitrary origin.
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_api.workspace_policy import repository_is_allowed
from fastapi import Response
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
OTHER_REPO = "attacker/other-bot"
WORKER_TOKEN = "remote-dev-worker-test-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}


def _create_agent_version(
    client: TestClient, auth_headers: dict[str, str], *, name: str = "workspace-bot"
) -> tuple[str, str]:
    agent_response = client.post(
        "/agents",
        json={
            "name": name,
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
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
    return agent_id, version_response.json()["id"]


def _deploy(
    client: TestClient,
    auth_headers: dict[str, str],
    agent_id: str,
    version_id: str,
    *,
    workspace: object = ...,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "agent_id": agent_id,
        "version_id": version_id,
        "environment": "dev",
    }
    if workspace is not ...:
        body["workspace_enabled"] = workspace
    response = client.post("/deployments", json=body, headers=auth_headers)
    assert response.status_code == 201, response.text
    return response.json()


def _audit_rows() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT purpose, outcome, deployment_id, publication_id, "
                        "repo_full_name, detail FROM "
                        "curie.credential_redemption_audit_entries "
                        "ORDER BY created_at, id"
                    )
                )
                return [dict(row._mapping) for row in result]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _credential_audit_columns() -> set[str]:
    async def run() -> set[str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'curie' AND "
                        "table_name = 'credential_redemption_audit_entries'"
                    )
                )
                return set(result.scalars())
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _selection_rows() -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT agent_id, conversation_id, repo_full_name, selected_by "
                        "FROM curie.thread_workspaces ORDER BY created_at, id"
                    )
                )
                return [dict(row._mapping) for row in result]
        finally:
            await engine.dispose()

    return asyncio.run(run())


@pytest.fixture
def worker_client(_disposable_db: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_APP_ID", "")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_operator_workspace")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    _RESOLVERS.clear()
    with TestClient(create_app()) as test_client:
        yield test_client
    _RESOLVERS.clear()
    get_settings.cache_clear()


def test_legacy_deployment_workspace_field_preserves_explicit_values(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The deprecated field remains round-trip compatible for existing clients."""

    agent_id, version_id = _create_agent_version(client, auth_headers)

    enabled = _deploy(client, auth_headers, agent_id, version_id, workspace=True)
    carried = _deploy(client, auth_headers, agent_id, version_id)
    disabled = _deploy(client, auth_headers, agent_id, version_id, workspace=False)
    disabled_carried = _deploy(client, auth_headers, agent_id, version_id)

    assert enabled["workspace_enabled"] is True
    assert carried["workspace_enabled"] is True
    assert disabled["workspace_enabled"] is False
    assert disabled_carried["workspace_enabled"] is False

    listed = client.get("/deployments", params={"agent_id": agent_id}, headers=auth_headers)
    assert listed.status_code == 200, listed.text
    assert [row["workspace_enabled"] for row in listed.json()] == [
        True,
        True,
        False,
        False,
    ]


def test_first_repo_selection_is_sticky_allowlisted_and_conflict_safe(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    url = f"/v1/internal/workspaces/{deployment['id']}/selection"

    selected = worker_client.post(
        url,
        json={"conversation_id": "thread-1", "author": "U0REQUEST1", "repo_full_name": REPO},
        headers=WORKER_HEADERS,
    )
    reused = worker_client.post(
        url,
        json={"conversation_id": "thread-1", "author": "U0REQUEST1", "repo_full_name": None},
        headers=WORKER_HEADERS,
    )
    conflict = worker_client.post(
        url,
        json={"conversation_id": "thread-1", "author": "U0REQUEST1", "repo_full_name": OTHER_REPO},
        headers=WORKER_HEADERS,
    )

    assert selected.status_code == reused.status_code == 200
    assert selected.json() == reused.json() == {"repo_full_name": REPO, "revision": None}
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "workspace.selection_conflict"
    assert _selection_rows() == [
        {
            "agent_id": uuid.UUID(agent_id),
            "conversation_id": "thread-1",
            "repo_full_name": REPO,
            "selected_by": "U0REQUEST1",
        }
    ]


def test_workspace_capability_without_repo_remains_an_unselected_generic_thread(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """Capability alone must not force repository selection or persist a row."""

    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)

    response = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-generic-first",
            "author": "U0REQUEST1",
            "repo_full_name": None,
        },
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 200, response.text
    assert response.json() == {"repo_full_name": None, "revision": None}
    assert _selection_rows() == []


def test_no_url_and_no_durable_selection_returns_null_without_redemption(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """An ordinary non-coding turn creates neither selection nor credential audit state."""
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(
        worker_client,
        auth_headers,
        agent_id,
        version_id,
        workspace=False,
    )

    response = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-no-repository",
            "author": "U0REQUEST1",
            "repo_full_name": None,
        },
        headers=WORKER_HEADERS,
    )

    assert response.status_code == 200, response.text
    assert response.json() == {"repo_full_name": None, "revision": None}
    assert _selection_rows() == []
    assert _audit_rows() == []


def test_selection_survives_redeployment_for_the_same_agent_thread(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    first = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    selected = worker_client.post(
        f"/v1/internal/workspaces/{first['id']}/selection",
        json={
            "conversation_id": "thread-redeploy",
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    replacement = _deploy(worker_client, auth_headers, agent_id, version_id)

    reused = worker_client.post(
        f"/v1/internal/workspaces/{replacement['id']}/selection",
        json={
            "conversation_id": "thread-redeploy",
            "author": "U0REQUEST2",
            "repo_full_name": None,
        },
        headers=WORKER_HEADERS,
    )

    assert reused.status_code == 200, reused.text
    assert reused.json() == {"repo_full_name": REPO, "revision": None}
    assert len(_selection_rows()) == 1


def test_concurrent_different_repo_selection_has_one_database_winner(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    url = f"/v1/internal/workspaces/{deployment['id']}/selection"
    barrier = threading.Barrier(2)

    def choose(repo: str) -> int:
        barrier.wait()
        return worker_client.post(
            url,
            json={
                "conversation_id": "thread-race",
                "author": "U0REQUEST1",
                "repo_full_name": repo,
            },
            headers=WORKER_HEADERS,
        ).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(
            pool.map(choose, [REPO, "acme-corp/acme-api"])
        )

    assert sorted(statuses) == [200, 409]
    assert len(_selection_rows()) == 1


def test_repo_selection_denies_before_persistence_or_credential_mint(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/acme-bot"]')
    get_settings.cache_clear()
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)

    denied = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-denied",
            "author": "U0REQUEST1",
            "repo_full_name": OTHER_REPO,
        },
        headers=WORKER_HEADERS,
    )

    assert denied.status_code == 403
    assert _audit_rows() == []


def test_repository_allowlist_matches_casefolded_whole_repositories_or_owners_only() -> None:
    assert repository_is_allowed(REPO, ("ACME-CORP/ACME-BOT",))
    assert repository_is_allowed(REPO, ("AcMe-CoRp/*",))
    assert not repository_is_allowed(REPO, ("acme-corp/acme-bot-extra",))
    assert not repository_is_allowed(REPO, ("other-corp/*",))


def test_malformed_workspace_allowlist_refuses_settings_boot() -> None:
    from curie_api.config import Settings

    with pytest.raises(ValidationError, match="GITHUB_REPO_ALLOWLIST"):
        Settings(github_repo_allowlist=("acme-corp/*/extra",))


def test_workspace_credential_is_worker_only_server_derived_and_no_store(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    url = f"/v1/internal/workspaces/{deployment['id']}/credential"

    refused = worker_client.post(
        url, json={"conversation_id": "thread-credential"}, headers=auth_headers
    )
    assert refused.status_code == 401
    assert refused.headers["cache-control"] == "no-store"
    assert "internal worker token" in refused.json()["detail"]
    assert "ghp_operator_workspace" not in refused.text
    # Unauthenticated internet traffic is bounded to the 401 response and
    # access log; it cannot grow the durable credential audit table.
    assert _audit_rows() == []

    selected = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-credential",
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    issued = worker_client.post(
        url,
        json={"conversation_id": "thread-credential"},
        headers=WORKER_HEADERS,
    )
    assert issued.status_code == 200, issued.text
    assert issued.headers["cache-control"] == "no-store"
    assert issued.json() == {
        "repo_full_name": REPO,
        "clone_url": "https://github.com/acme-corp/acme-bot.git",
        "authorization_header": "Basic "
        + base64.b64encode(b"x-access-token:ghp_operator_workspace").decode(),
        "revision": None,
    }
    assert OTHER_REPO not in issued.text
    assert "evil.example" not in issued.text

    rows = _audit_rows()
    assert [(row["purpose"], row["outcome"]) for row in rows] == [
        ("workspace_clone", "issued"),
    ]
    assert all(str(row["deployment_id"]) == deployment["id"] for row in rows)
    assert all(row["repo_full_name"] == REPO for row in rows)
    serialized = json.dumps(rows, default=str)
    assert "ghp_operator_workspace" not in serialized
    assert "token" not in _credential_audit_columns()
    assert "authorization" not in _credential_audit_columns()


def test_workspace_credential_pat_fallback_is_redeemed_through_the_endpoint(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    selected = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={"conversation_id": "thread-pat", "author": "U0REQUEST1", "repo_full_name": REPO},
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    response = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/credential",
        json={"conversation_id": "thread-pat"},
        headers=WORKER_HEADERS,
    )
    assert response.status_code == 200, response.text
    assert (
        response.json()["authorization_header"]
        == "Basic " + base64.b64encode(b"x-access-token:ghp_operator_workspace").decode()
    )


def test_workspace_credential_resolution_does_not_block_the_event_loop(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synchronous installation-token mint must run outside FastAPI's loop."""

    from curie_api.routers.workspaces import redeem_workspace_credential
    from curie_api.schemas import WorkspaceCredentialRequest

    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=True)
    selected = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-nonblocking",
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    resolver_started = threading.Event()
    loop_progressed = threading.Event()

    def blocking_resolver(_: str, __: Any) -> tuple[str, str]:
        resolver_started.set()
        if not loop_progressed.wait(timeout=0.5):
            raise AssertionError("credential resolver blocked the event loop")
        return "https://github.com/acme-corp/acme-bot.git", "Basic test"

    monkeypatch.setattr(
        "curie_api.routers.workspaces.resolve_repository_credential", blocking_resolver
    )

    async def exercise() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session:
                call = asyncio.create_task(
                    redeem_workspace_credential(
                        uuid.UUID(deployment["id"]),
                        WorkspaceCredentialRequest(conversation_id="thread-nonblocking"),
                        session,
                        Response(),
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


def test_workspace_credential_prefers_app_installation_token_over_pat(
    _disposable_db: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", pem)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_must_not_win")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    _RESOLVERS.clear()

    calls: list[tuple[str, str, dict[str, Any] | None]] = []
    real_client = httpx.Client

    def handle(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, str(request.url), body))
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": 4242})
        if request.url.path.endswith("/access_tokens"):
            return httpx.Response(
                201,
                json={
                    "token": "ghs_repo_scoped",
                    "expires_at": "2999-01-01T00:00:00Z",
                },
            )
        return httpx.Response(404, json={"message": "Not Found"})

    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *args, **kwargs: real_client(transport=httpx.MockTransport(handle)),
    )
    with TestClient(create_app()) as app_client:
        agent_id, version_id = _create_agent_version(app_client, auth_headers)
        deployment = _deploy(app_client, auth_headers, agent_id, version_id, workspace=True)
        selected = app_client.post(
            f"/v1/internal/workspaces/{deployment['id']}/selection",
            json={"conversation_id": "thread-app", "author": "U0REQUEST1", "repo_full_name": REPO},
            headers=WORKER_HEADERS,
        )
        assert selected.status_code == 200, selected.text

        response = app_client.post(
            f"/v1/internal/workspaces/{deployment['id']}/credential",
            json={"conversation_id": "thread-app"},
            headers=WORKER_HEADERS,
        )
        assert response.status_code == 200, response.text
        authorization = response.json()["authorization_header"]
        assert (
            authorization == "Basic " + base64.b64encode(b"x-access-token:ghs_repo_scoped").decode()
        )
        assert base64.b64encode(b"x-access-token:ghp_must_not_win").decode() not in authorization
    assert any(url.endswith(f"/repos/{REPO}/installation") for _, url, _ in calls)
    mint = next(body for _, url, body in calls if url.endswith("/access_tokens"))
    assert mint == {"repositories": ["acme-bot"]}


def test_legacy_deployment_workspace_field_does_not_gate_selection_or_credential(
    worker_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    agent_id, version_id = _create_agent_version(worker_client, auth_headers)
    deployment = _deploy(worker_client, auth_headers, agent_id, version_id, workspace=False)

    selected = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/selection",
        json={
            "conversation_id": "thread-disabled",
            "author": "U0REQUEST1",
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    response = worker_client.post(
        f"/v1/internal/workspaces/{deployment['id']}/credential",
        json={"conversation_id": "thread-disabled"},
        headers=WORKER_HEADERS,
    )

    assert selected.status_code == 200, selected.text
    assert selected.json() == {"repo_full_name": REPO, "revision": None}
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["repo_full_name"] == REPO
    audit = _audit_rows()
    assert len(audit) == 1
    assert audit[0]["outcome"] == "issued"
    assert str(audit[0]["deployment_id"]) == deployment["id"]
