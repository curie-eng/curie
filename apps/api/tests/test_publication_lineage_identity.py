"""Contract for the worker lineage PATCH that verifies identity and settles one revision.

The helpers and the `publication_stack` / `review_lineage_app` fixtures are
reproduced from `apps/api/tests/test_publications.py` rather than imported:
the suite runs under ``--import-mode=importlib`` with no ``__init__.py``, so
one test module cannot import another by name (`apps/api/tests/conftest.py`
states this), and that module belongs to the other implementation stream.
"""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import channel_protocol
import httpx
import pytest
import redis
from curie_api import approval_principal
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

REPO = "acme-corp/acme-bot"
WORKER_TOKEN = "remote-dev-publication-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
FIRST_REVISION_SHA = "1123456789abcdef0123456789abcdef01234567"
SECOND_REVISION_SHA = "2123456789abcdef0123456789abcdef01234567"
PR_NUMBER = 123
PR_URL = f"https://github.com/{REPO}/pull/{PR_NUMBER}"
LINEAGE_OPERATION = "/v1/internal/publications/{publication_id}/lineage"
GITHUB_UNAVAILABLE_CODE = "publication.github_unavailable"

_LINEAGE_COLUMNS = (
    "id, version, status, pr_number, pr_url, head_sha, latest_revision, binding_id, "
    "github_repository_id, github_installation_id, github_pr_node_id, base_ref, updated_at"
)


# --- fixtures and helpers reproduced from test_publications.py ----------------


@pytest.fixture
def publication_stack(
    _disposable_db: Any, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, str]]:
    runs_stream = f"test:curie:publication-identity:{uuid.uuid4().hex}"
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
    valkey: redis.Redis = connect_or_skip(decode_responses=True)
    valkey.delete(runs_stream, f"{runs_stream}:dead")
    valkey.close()
    _RESOLVERS.clear()
    get_settings.cache_clear()


@pytest.fixture
def review_lineage_app(
    clean_db: None, publication_stack: tuple[TestClient, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, dict[str, Any], str]]:
    """The App-configured GitHub fixture from test_publications.py:4638.

    `publication_stack` alone sets GITHUB_TOKEN and configures no App, so
    `verify_publication_identity` returns None before any GitHub call and can
    only ever produce `eligible: false`. This fixture is the one that actually
    drives the verifier: a real RSA key, the installation-token mint served on
    the private `curie_api.github_app.httpx.Client` (which an app.state
    injection cannot reach), and the same transport injected into
    `app.state.http_client`.

    Two additions over the original, both needed by the cases this file pins
    and neither weakening it: `truth["state"]` / `truth["merged"]` drive the
    pull payload's state (T5c), `truth["html_url"]` lets the pull name another
    repository, and `truth["transport_error"]` raises a real httpx error
    instead of answering (the transport-failure half of T2).
    """

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
        "html_url": PR_URL,
        "state": "open",
        "merged": False,
        "transport_error": False,
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
        if truth["transport_error"]:
            raise httpx.ReadTimeout("fixture github timeout", request=request)
        if truth["status"] != 200:
            return httpx.Response(truth["status"], json={"message": "fixture-unavailable"})
        repo = {"id": truth["repository_id"], "full_name": REPO}
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=repo)
        return httpx.Response(
            200,
            json={
                "number": PR_NUMBER,
                "html_url": truth["html_url"],
                "node_id": truth["node_id"],
                "state": truth["state"],
                "merged": truth["merged"],
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


@contextmanager
def _recording_github(client: TestClient) -> Iterator[list[tuple[str, str]]]:
    """Injected transport that records, and refuses to serve, any GitHub read."""

    calls: list[tuple[str, str]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        return httpx.Response(200, json={})

    injected = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        yield calls
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())


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


def _create_deployment(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    channel: str = "C0EXAMPLE1",
) -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:8]
    agent_response = client.post(
        "/agents",
        json={
            "name": f"publisher-{suffix}",
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


def _publication_payload(deployment_id: str, *, conversation_id: str) -> dict[str, Any]:
    return {
        "deployment_id": deployment_id,
        "conversation_id": conversation_id,
        "repo_full_name": REPO,
        "author": "U0REQUEST1",
        "summary": "Publish the repository changes",
        "reply_kind": "slack",
        "reply_channel": "C0EXAMPLE1",
        "reply_placeholder": "1700000000.000001",
        "dedupe_key": f"publish-{uuid.uuid4().hex}",
        "base_sha": BASE_SHA,
        "patch_b64": base64.b64encode(b"diff --git a/README.md b/README.md\n").decode(),
        "changed_paths": ["README.md"],
        "expires_in_seconds": 600,
    }


def _workspace_identity(payload: Mapping[str, Any]) -> str:
    return channel_protocol.scoped_conversation_id(
        str(payload["reply_kind"]),
        str(payload["reply_channel"]),
        str(payload["conversation_id"]),
    )


def _create_publication(client: TestClient, payload: dict[str, Any]) -> dict[str, Any]:
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
    response = client.post("/v1/internal/publications", json=payload, headers=WORKER_HEADERS)
    assert response.status_code in (200, 201), response.text
    return response.json()


def _resolve(client: TestClient, auth_headers: dict[str, str], approval_id: str) -> Any:
    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject="U0REQUEST1",
        kind="chat",
        actor_channel="C0EXAMPLE1",
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(datetime.now(UTC).timestamp()) + 60,
    )
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": "approved", "note": None},
        headers={**auth_headers, "X-Curie-Approval-Principal": token},
    )


def _approved_publication(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    truth: dict[str, Any] | None = None,
    deployment: dict[str, Any] | None = None,
    conversation_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A deployment plus an approved publication whose lineage has no PR yet.

    `deployment` is reused when a test needs two lineages: a second agent on
    the same Slack channel is refused by the binding, so a second lineage has
    to come from a second conversation on the same deployment.
    """

    deployment = deployment or _create_deployment(client, auth_headers)
    conversation_id = conversation_id or f"identity-{uuid.uuid4().hex[:8]}"
    publication = _create_publication(
        client,
        _publication_payload(deployment["id"], conversation_id=conversation_id),
    )
    if truth is not None:
        truth["branch"] = publication["branch"]
    resolved = _resolve(client, auth_headers, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text
    return deployment, publication


# --- canonical worker lineage PATCH helpers -----------------------------------


def _lineage_row(lineage_id: str) -> dict[str, Any]:
    rows = _rows(
        f"SELECT {_LINEAGE_COLUMNS} FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": uuid.UUID(lineage_id)},
    )
    assert len(rows) == 1
    return rows[0]


def _identity_columns(row: Mapping[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (
        row["github_repository_id"],
        row["github_installation_id"],
        row["github_pr_node_id"],
        row["base_ref"],
    )


# --- Canonical PATCH verification and settlement -----------------------------


def _claim_publication_lease(publication_id: str, owner: str) -> tuple[int, str]:
    async def claim() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(
                    text(
                        "UPDATE curie.publications SET lease_owner = :owner, "
                        "lease_expires_at = now() + interval '1 minute', version = version + 1 "
                        "WHERE id = :id RETURNING version"
                    ),
                    {"id": uuid.UUID(publication_id), "owner": owner},
                )
                return int(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(claim()), owner


def _advance_lineage(
    client: TestClient,
    publication_id: str,
    *,
    expected_version: int = 1,
    expected_head_sha: str | None = None,
    expected_publication_version: int | None = None,
    lease_owner: str | None = None,
    head_sha: str = FIRST_REVISION_SHA,
    state: str = "open",
    pr_number: int = PR_NUMBER,
    pr_url: str = PR_URL,
    headers: Mapping[str, str] | None = WORKER_HEADERS,
) -> Any:
    if expected_publication_version is None or lease_owner is None:
        expected_publication_version, lease_owner = _claim_publication_lease(
            publication_id, "lineage-identity-test-worker"
        )
    return client.patch(
        f"/v1/internal/publications/{publication_id}/lineage",
        json={
            "expected_version": expected_version,
            "expected_head_sha": expected_head_sha,
            "expected_publication_version": expected_publication_version,
            "lease_owner": lease_owner,
            "state": state,
            "pr_number": pr_number,
            "pr_url": pr_url,
            "head_sha": head_sha,
            "metadata_updated_at": None,
        },
        headers=dict(headers or {}),
    )


def _mark_outcome_history_ready(publication_id: str) -> None:
    _execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )


def test_patch_captures_provider_identity_with_the_first_pr_facts(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)

    response = _advance_lineage(client, publication["id"])

    assert response.status_code == 200, response.text
    assert ("GET", f"/repos/{REPO}") in truth["calls"]
    assert ("GET", f"/repos/{REPO}/pulls/{PR_NUMBER}") in truth["calls"]
    row = _lineage_row(publication["lineage_id"])
    assert _identity_columns(row) == (9001, 41, "PR_example_123", "main")
    assert (row["pr_number"], row["pr_url"], row["head_sha"], row["version"]) == (
        PR_NUMBER,
        PR_URL,
        FIRST_REVISION_SHA,
        2,
    )


def test_patch_preserves_captured_identity_on_a_later_revision(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    conversation_id = f"identity-later-{uuid.uuid4().hex[:8]}"
    deployment, first = _approved_publication(
        client,
        auth_headers,
        truth=truth,
        conversation_id=conversation_id,
    )
    assert _advance_lineage(client, first["id"]).status_code == 200
    _mark_outcome_history_ready(first["id"])
    second_payload = _publication_payload(
        deployment["id"], conversation_id=conversation_id
    )
    second_payload["base_sha"] = FIRST_REVISION_SHA
    second = _create_publication(client, second_payload)
    truth["branch"] = second["branch"]
    assert _resolve(client, auth_headers, second["approval_id"]).status_code == 200
    truth["head_sha"] = SECOND_REVISION_SHA

    advanced = _advance_lineage(
        client,
        second["id"],
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )

    assert advanced.status_code == 200, advanced.text
    row = _lineage_row(first["lineage_id"])
    assert _identity_columns(row) == (9001, 41, "PR_example_123", "main")
    assert (row["pr_number"], row["head_sha"], row["version"]) == (
        PR_NUMBER,
        SECOND_REVISION_SHA,
        3,
    )


def test_case_only_pr_url_spelling_is_accepted_by_the_patch(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)

    response = _advance_lineage(
        client,
        publication["id"],
        pr_url=f"https://github.com/ACME-Corp/Acme-Bot/pull/{PR_NUMBER}",
    )

    assert response.status_code == 200, response.text
    assert _identity_columns(_lineage_row(publication["lineage_id"])) == (
        9001,
        41,
        "PR_example_123",
        "main",
    )


def test_token_mode_patch_settles_without_provider_identity(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
) -> None:
    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)

    with _recording_github(client) as calls:
        response = _advance_lineage(client, publication["id"])

    assert response.status_code == 200, response.text
    assert calls == []
    assert _identity_columns(_lineage_row(publication["lineage_id"])) == (
        None,
        None,
        None,
        None,
    )


@pytest.mark.parametrize(
    ("mutation", "expected_status", "expected_code"),
    [
        pytest.param({"installation_id": 0}, 409, "publication.lineage_stale", id="installation"),
        pytest.param({"status": 500}, 503, GITHUB_UNAVAILABLE_CODE, id="github_500"),
        pytest.param({"status": 401}, 503, GITHUB_UNAVAILABLE_CODE, id="github_401"),
        pytest.param({"status": 403}, 503, GITHUB_UNAVAILABLE_CODE, id="github_403"),
        pytest.param({"status": 404}, 503, GITHUB_UNAVAILABLE_CODE, id="github_404"),
        pytest.param({"status": 429}, 503, GITHUB_UNAVAILABLE_CODE, id="github_429"),
        pytest.param({"transport_error": True}, 503, GITHUB_UNAVAILABLE_CODE, id="transport"),
        pytest.param({"node_id": None}, 409, "publication.lineage_stale", id="node_id"),
        pytest.param(
            {"head_sha": SECOND_REVISION_SHA},
            409,
            "publication.lineage_stale",
            id="head",
        ),
        pytest.param(
            {"html_url": f"https://github.com/other-corp/other-bot/pull/{PR_NUMBER}"},
            409,
            "publication.lineage_stale",
            id="foreign_url",
        ),
    ],
)
def test_patch_keeps_provider_refusal_and_unavailable_meanings(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    mutation: dict[str, Any],
    expected_status: int,
    expected_code: str,
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth.update(mutation)
    before = _lineage_row(publication["lineage_id"])

    response = _advance_lineage(client, publication["id"])

    assert response.status_code == expected_status, response.text
    assert response.json()["detail"]["code"] == expected_code
    after = _lineage_row(publication["lineage_id"])
    assert _identity_columns(after) == _identity_columns(before)
    assert after["pr_number"] == before["pr_number"]


@pytest.mark.parametrize("revocation", ["deployment_inactive", "workspace_repointed", "allowlist"])
def test_patch_rechecks_workspace_authorization_before_identity_capture(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    revocation: str,
) -> None:
    client, truth, _ = review_lineage_app
    deployment, publication = _approved_publication(client, auth_headers, truth=truth)
    if revocation == "deployment_inactive":
        _execute(
            "UPDATE curie.deployments SET status = 'inactive' WHERE id = :id",
            {"id": uuid.UUID(deployment["id"])},
        )
    elif revocation == "workspace_repointed":
        _execute(
            "UPDATE curie.thread_workspaces SET repo_full_name = 'acme-corp/other-bot' "
            "WHERE selected_by_deployment_id = :id",
            {"id": uuid.UUID(deployment["id"])},
        )
    else:
        monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", "[]")
        get_settings.cache_clear()
    before = _lineage_row(publication["lineage_id"])

    response = _advance_lineage(client, publication["id"])

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "publication.lineage_stale"
    assert _identity_columns(_lineage_row(publication["lineage_id"])) == _identity_columns(before)


@pytest.mark.parametrize(
    ("pull_state", "merged", "observed"),
    [
        pytest.param("closed", True, "merged", id="merged"),
        pytest.param("closed", False, "closed", id="closed"),
    ],
)
def test_patch_exposes_provider_terminal_state_for_worker_terminal_cas(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    pull_state: str,
    merged: bool,
    observed: str,
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth["state"] = pull_state
    truth["merged"] = merged
    before = _lineage_row(publication["lineage_id"])

    response = _advance_lineage(client, publication["id"])

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == {
        "code": "publication.lineage_terminal",
        "message": "the pull request for this thread is merged or closed; start a new thread",
        "observed_state": observed,
    }
    assert _lineage_row(publication["lineage_id"]) == before


def test_malformed_merged_open_provider_payload_is_a_plain_stale_refusal(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth["state"] = "open"
    truth["merged"] = True

    response = _advance_lineage(client, publication["id"])

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "publication.lineage_stale"
    assert "observed_state" not in detail


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param(None, id="no_token"),
        pytest.param({"X-Curie-Worker-Token": "wrong"}, id="wrong"),
    ],
)
def test_patch_requires_the_internal_worker_token(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    headers: Mapping[str, str] | None,
) -> None:
    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)

    response = _advance_lineage(client, publication["id"], headers=headers)

    assert response.status_code in (401, 403), response.text


def test_patch_404s_for_unknown_publication(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
) -> None:
    client, _ = publication_stack
    lease = (1, "lineage-identity-test-worker")

    response = _advance_lineage(
        client,
        str(uuid.uuid4()),
        expected_publication_version=lease[0],
        lease_owner=lease[1],
    )

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "publication lineage not found"
