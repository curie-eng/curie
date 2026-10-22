"""Contract for the worker lineage PATCH that verifies identity and settles one revision.

The admission fixtures seed real Postgres and Valkey state. Shared external
GitHub responses supply the commit objects required for settlement; the API
verifies their ancestry and binds the resulting proof to the locked rows.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
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

from apps.api.tests.forge_fakes.github import (
    INSTALLATION_ID,
    LABEL,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubRepositoryFake,
)
from apps.api.tests.test_github_factory_base import (
    BaseGitHubAPI,
    _label,
    base_app,  # noqa: F401
)
from apps.api.tests.test_workitem_outcomes import with_session

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
    "id, version, status, base_sha, pr_number, pr_url, head_sha, latest_revision, binding_id, "
    "binding_generation, "
    "code_host_kind, code_host_host, repository_project_id, code_host_installation_id, "
    "code_host_pr_id, base_ref, updated_at"
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
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
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
        "default_branch": "main",
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
        "branches": {},
        "commits": {},
        "pull_exists": False,
        "opened": [],
        "updated": [],
        "title": "Old title",
        "body": "Old body",
        "updated_at": "2026-10-05T12:00:00Z",
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
        # verify_publication_identity uses Bearer; GitHubCodeHost._token uses Basic for REST.
        assert request.headers["authorization"] in {
            "Bearer fixture-publication-app-token",
            "Basic "
            + base64.b64encode(b"x-access-token:fixture-publication-app-token").decode("ascii"),
        }
        if truth["transport_error"]:
            raise httpx.ReadTimeout("fixture github timeout", request=request)
        if truth["status"] != 200:
            return httpx.Response(truth["status"], json={"message": "fixture-unavailable"})
        # https://docs.github.com/en/rest/repos/repos#get-a-repository
        repo = {
            "id": truth["repository_id"],
            "full_name": REPO,
            "default_branch": truth["default_branch"],
        }
        if request.url.path == f"/repos/{REPO}":
            return httpx.Response(200, json=repo)
        # https://docs.github.com/en/rest/git/refs#get-a-reference
        if request.url.path.startswith(f"/repos/{REPO}/git/ref/heads/"):
            branch = request.url.path.removeprefix(f"/repos/{REPO}/git/ref/heads/")
            sha = truth["branches"].get(branch)
            if sha is None:
                return httpx.Response(404)
            return httpx.Response(
                200, json={"ref": f"refs/heads/{branch}", "object": {"type": "commit", "sha": sha}}
            )
        # https://docs.github.com/en/rest/git/commits#get-a-commit-object
        if request.url.path.startswith(f"/repos/{REPO}/git/commits/"):
            commit = truth["commits"].get(request.url.path.rsplit("/", 1)[-1])
            return httpx.Response(200, json=commit) if commit else httpx.Response(404)
        # https://docs.github.com/en/rest/pulls/pulls
        if request.method == "POST" and request.url.path == f"/repos/{REPO}/pulls":
            fields = json.loads(request.content)
            truth["opened"].append(fields)
            truth.update(
                branch=fields["head"],
                base_ref=fields["base"],
                title=fields["title"],
                body=fields["body"],
                pull_exists=True,
            )
        if request.method == "PATCH" and request.url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}":
            fields = json.loads(request.content)
            truth["updated"].append(fields)
            truth.update(fields)
        pull = {
            "number": PR_NUMBER,
            "html_url": truth["html_url"],
            "node_id": truth["node_id"],
            "state": truth["state"],
            "merged": truth["merged"],
            "head": {"sha": truth["head_sha"], "ref": truth["branch"], "repo": repo},
            "base": {"repo": repo, "ref": truth.get("base_ref", "main")},
            "title": truth["title"],
            "body": truth["body"],
            "updated_at": truth["updated_at"],
        }
        if request.method == "GET" and request.url.path == f"/repos/{REPO}/pulls":
            return httpx.Response(200, json=[pull] if truth["pull_exists"] else [])
        return httpx.Response(201 if request.method == "POST" else 200, json=pull)

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
    return _deploy_agent(client, auth_headers, agent_id)


def _deploy_agent(
    client: TestClient, auth_headers: dict[str, str], agent_id: str
) -> dict[str, Any]:
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
    if payload.get("reply_conversation_id") is not None:
        return str(payload["conversation_id"])
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


ManualPublication = tuple[TestClient, dict[str, Any], dict[str, Any], dict[str, Any]]


def _start_publication_request(
    client: TestClient, request_id: str, wake: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    acquired = client.post(
        f"/v1/internal/work-items/requests/{request_id}/acquire",
        json={"owner": "publication-worker", "generation": 1, "context": None, "wake": wake},
        headers=WORKER_HEADERS,
    )
    assert acquired.status_code == 200, acquired.text
    started = client.post(
        f"/v1/internal/work-items/requests/{request_id}/start",
        json={
            "owner": "publication-worker",
            "generation": 1,
            "claim_name": "publication-claim",
            "sandbox_name": "publication-sandbox",
            "context": acquired.json()["context"],
        },
        headers=WORKER_HEADERS,
    )
    assert started.status_code == 200, started.text
    return acquired.json(), started.json()


@pytest.fixture
def manual_publication(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> ManualPublication:
    """Admit a request without base fields through the supported internal API (#4943)."""

    client, truth, _ = review_lineage_app
    truth.update(default_branch="release/stable", base_ref="release/stable")
    truth["branches"]["release/stable"] = BASE_SHA
    truth["commits"] = {
        BASE_SHA: {"sha": BASE_SHA, "parents": [], "message": "Original base"},
        MOVED_PARENT_SHA: {
            "sha": MOVED_PARENT_SHA,
            "parents": [{"sha": BASE_SHA}],
            "message": "Base moved before publication",
        },
        LATER_BASE_SHA: {
            "sha": LATER_BASE_SHA,
            "parents": [{"sha": MOVED_PARENT_SHA}],
            "message": "Base moved again after push",
        },
    }
    deployment = _create_deployment(client, auth_headers)
    conversation = f"manual-{uuid.uuid4().hex[:8]}"
    admitted = client.post(
        "/v1/internal/work-items/admissions",
        json={
            "agent_id": deployment["agent_id"],
            "kind": "slack",
            "address": "C0EXAMPLE1",
            "reply_conversation_id": conversation,
            "tracker": {
                "kind": "github",
                "host": "github.com",
                "scope_id": "9001",
                "issue_id": "4943",
            },
            "repository": {
                "code_host_kind": "github",
                "host": "github.com",
                "project_id": "9001",
                "path": REPO,
            },
            "code_host_installation_id": 41,
            "objective": "Implement the requested repository change",
            "requester": "U0REQUEST1",
            "request_id": str(uuid.uuid4()),
        },
        headers=WORKER_HEADERS,
    )
    assert admitted.status_code == 200, admitted.text
    request_id = admitted.json()["request"]["id"]
    acquired, started = _start_publication_request(
        client,
        request_id,
        {
            "kind": "slack",
            "address": "C0EXAMPLE1",
            "adapter": "default",
            "conversation_id": conversation,
        },
    )
    payload = _publication_payload(deployment["id"], conversation_id=acquired["conversation_id"])
    payload.update(
        reply_conversation_id=conversation,
        work_item_request_id=request_id,
        work_item_runtime_epoch=started["runtime_epoch"],
    )
    publication = _create_publication(client, payload)
    assert _rows(
        "SELECT execution_request_id FROM curie.publications WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )[0]["execution_request_id"] == uuid.UUID(request_id)
    truth.update(branch=publication["branch"], title=publication["title"], body=publication["body"])
    resolved = _resolve(client, auth_headers, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text
    return client, truth, payload, publication


def _manual_first_commit(truth: dict[str, Any], publication: dict[str, Any], parent: str) -> None:
    truth["branches"][publication["branch"]] = FIRST_REVISION_SHA
    truth["branches"]["release/stable"] = BASE_SHA if parent == BASE_SHA else LATER_BASE_SHA
    truth["commits"][FIRST_REVISION_SHA] = {
        "sha": FIRST_REVISION_SHA,
        "parents": [{"sha": parent}],
        "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
    }


# --- canonical worker lineage PATCH helpers -----------------------------------


def _lineage_row(lineage_id: str) -> dict[str, Any]:
    rows = _rows(
        f"SELECT {_LINEAGE_COLUMNS} FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": uuid.UUID(lineage_id)},
    )
    assert len(rows) == 1
    return rows[0]


def _identity_columns(row: Mapping[str, Any]) -> tuple[Any, Any, Any, Any, Any, Any]:
    return (
        row["code_host_kind"],
        row["code_host_host"],
        row["repository_project_id"],
        row["code_host_installation_id"],
        row["code_host_pr_id"],
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
    from test_publications import _publication_commit_http

    if expected_publication_version is None or lease_owner is None:
        expected_publication_version, lease_owner = _claim_publication_lease(
            publication_id, "lineage-identity-test-worker"
        )
    with _publication_commit_http(client, publication_id, head_sha):
        return _patch_lineage(
            client,
            publication_id,
            expected_version=expected_version,
            expected_head_sha=expected_head_sha,
            expected_publication_version=expected_publication_version,
            lease_owner=lease_owner,
            head_sha=head_sha,
            state=state,
            pr_number=pr_number,
            pr_url=pr_url,
            headers=headers,
        )


def _patch_lineage(
    client: TestClient,
    publication_id: str,
    *,
    expected_version: int,
    expected_head_sha: str | None,
    expected_publication_version: int,
    lease_owner: str,
    head_sha: str,
    state: str,
    pr_number: int,
    pr_url: str,
    headers: Mapping[str, str] | None,
) -> Any:
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
    assert _identity_columns(row) == (
        "github",
        "github.com",
        "9001",
        41,
        "PR_example_123",
        "main",
    )
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
    assert _identity_columns(row) == (
        "github",
        "github.com",
        "9001",
        41,
        "PR_example_123",
        "main",
    )
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
        "github",
        "github.com",
        "9001",
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


MOVED_PARENT_SHA = "3" * 40
LATER_BASE_SHA = "4" * 40


@contextmanager
def _moved_base_graph(
    client: TestClient,
    publication: dict[str, Any],
    *,
    race: tuple[str, dict[str, Any]] | None = None,
) -> Iterator[dict[str, Any]]:
    graph: dict[str, Any] = {
        "tip": MOVED_PARENT_SHA,
        "raced": False,
        "commits": {
            BASE_SHA: {"sha": BASE_SHA, "parents": [], "message": "Original base"},
            MOVED_PARENT_SHA: {
                "sha": MOVED_PARENT_SHA,
                "parents": [{"sha": BASE_SHA}],
                "message": "Moved base",
            },
            LATER_BASE_SHA: {
                "sha": LATER_BASE_SHA,
                "parents": [{"sha": MOVED_PARENT_SHA}],
                "message": "Base moved again after push",
            },
            FIRST_REVISION_SHA: {
                "sha": FIRST_REVISION_SHA,
                "parents": [{"sha": MOVED_PARENT_SHA}],
                "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
            },
        },
    }

    async def handle(request: httpx.Request) -> httpx.Response:
        # Observed GitHubCodeHost._token contract: REST uses the Git Basic header.
        assert request.headers["Authorization"] == (
            "Basic " + base64.b64encode(b"x-access-token:ghp_publication_operator").decode("ascii")
        )
        path = request.url.path
        # https://docs.github.com/en/rest/git/commits#get-a-commit-object
        if path.startswith(f"/repos/{REPO}/git/commits/"):
            sha = path.rsplit("/", 1)[-1]
            if race is not None and not graph["raced"]:
                graph["raced"] = True
                engine = create_async_engine(get_settings().database_url)
                try:
                    async with engine.begin() as connection:
                        await connection.execute(text(race[0]), race[1])
                finally:
                    await engine.dispose()
            payload = graph["commits"].get(sha)
            return httpx.Response(200, json=payload) if payload else httpx.Response(404)
        if path.startswith(f"/repos/{REPO}/git/ref/heads/"):
            return httpx.Response(
                200,
                json={
                    "ref": "refs/heads/main",
                    "object": {"type": "commit", "sha": graph["tip"]},
                },
            )
        assert path == f"/repos/{REPO}", path
        return httpx.Response(200, json={"id": 9001, "full_name": REPO, "default_branch": "main"})

    original = client.app.state.http_client
    injected = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client.app.state.http_client = injected
    try:
        yield graph
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())


def _publication_row(publication_id: str) -> dict[str, Any]:
    return _rows(
        "SELECT base_sha, expected_prior_head, status, version, patch_bytes, lease_owner, "
        "lease_expires_at, terminal_at FROM curie.publications WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )[0]


def _settle_moved(
    client: TestClient, publication: dict[str, Any], lease: tuple[int, str], **overrides: Any
) -> Any:
    data: dict[str, Any] = {
        "expected_version": 1,
        "expected_head_sha": None,
        "expected_publication_version": lease[0],
        "lease_owner": lease[1],
        "head_sha": FIRST_REVISION_SHA,
        "state": "open",
        "pr_number": PR_NUMBER,
        "pr_url": PR_URL,
        "headers": WORKER_HEADERS,
    }
    data.update(overrides)
    return _patch_lineage(client, publication["id"], **data)


def test_label_readmission_of_headless_all_null_base_uses_moved_repository_default_and_settles(
    base_app: tuple[TestClient, BaseGitHubAPI],  # noqa: F811
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api.workitems.lifecycle import WorkItemOutcome, link_publication_lineage

    client, ingress = base_app
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    get_settings.cache_clear()
    repository = GitHubRepositoryFake()
    repository.default_branch = "release/stable"
    repository.branches = {"release/stable": BASE_SHA}
    repository.commits = {
        BASE_SHA: {"sha": BASE_SHA, "parents": [], "message": "Original base"},
        MOVED_PARENT_SHA: {
            "sha": MOVED_PARENT_SHA,
            "parents": [{"sha": BASE_SHA}],
            "message": "Moved base",
        },
        LATER_BASE_SHA: {
            "sha": LATER_BASE_SHA,
            "parents": [{"sha": MOVED_PARENT_SHA}],
            "message": "Base moved after push",
        },
    }
    calls: list[tuple[str, str]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if "/issues/" in request.url.path or "/collaborators/" in request.url.path:
            return ingress.handle(request)
        response = repository.handle(request)
        if response.status_code not in {200, 201} or not (
            "/pulls/" in request.url.path
            or (response.status_code == 201 and request.url.path == f"/repos/{REPO}/pulls")
        ):
            return response
        payload = response.json()
        # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
        payload["node_id"] = f"PR_example_{payload['number']}"
        payload["merged"] = False
        for side in ("head", "base"):
            payload[side]["repo"]["id"] = REPO_ID
        return httpx.Response(response.status_code, json=payload)

    original = client.app.state.http_client
    injected = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client.app.state.http_client = injected
    try:
        [agent] = _rows("SELECT id FROM curie.agents WHERE repo_full_name = :repo", {"repo": REPO})
        agent_id = str(agent["id"])
        policy = client.patch(
            f"/agents/{agent_id}", json={"publication_policy": "auto"}, headers=auth_headers
        )
        assert policy.status_code == 200, policy.text
        deployment = _deploy_agent(client, auth_headers, agent_id)
        conversation = "issue-4943"
        admitted = client.post(
            "/v1/internal/work-items/admissions",
            json={
                "agent_id": agent_id,
                "kind": "github",
                "address": REPO,
                "reply_conversation_id": conversation,
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": str(REPO_ID),
                    "issue_id": "4943",
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                },
                "code_host_installation_id": INSTALLATION_ID,
                "objective": "Implement the requested change",
                "requester": f"github:{SENDER_ID}:{SENDER}",
                "request_id": str(uuid.uuid4()),
            },
            headers=WORKER_HEADERS,
        )
        assert admitted.status_code == 200, admitted.text
        request_id = admitted.json()["request"]["id"]
        item_id = uuid.UUID(admitted.json()["work_item"]["id"])
        acquired, started = _start_publication_request(
            client,
            request_id,
            {"kind": "github", "address": REPO, "adapter": None, "conversation_id": conversation},
        )
        payload = _publication_payload(
            deployment["id"], conversation_id=acquired["conversation_id"]
        )
        payload.update(
            reply_kind="github",
            reply_channel=REPO,
            reply_placeholder="4943",
            reply_conversation_id=conversation,
            work_item_request_id=request_id,
            work_item_runtime_epoch=started["runtime_epoch"],
        )
        publication = _create_publication(client, payload)
        initial_lineage = _lineage_row(publication["lineage_id"])
        assert initial_lineage["status"] == "open"
        assert initial_lineage["head_sha"] is initial_lineage["pr_number"] is None
        assert initial_lineage["binding_id"] is not None
        versions = _rows(
            "SELECT w.version AS item_version, r.version AS request_version "
            "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
            "WHERE r.id = :id",
            {"id": uuid.UUID(request_id)},
        )[0]

        async def link(session: Any) -> None:
            result = await link_publication_lineage(
                session,
                work_item_id=item_id,
                request_id=uuid.UUID(request_id),
                publication_lineage_id=uuid.UUID(publication["lineage_id"]),
                expected_work_item_version=versions["item_version"],
                expected_request_version=versions["request_version"],
            )
            assert isinstance(result, WorkItemOutcome), result

        with_session(link)
        finished = client.post(
            f"/v1/internal/work-items/requests/{request_id}/finish",
            json={
                "runtime_epoch": started["runtime_epoch"],
                "outcome": "failed",
                "cause": "provider_error",
            },
            headers=WORKER_HEADERS,
        )
        assert finished.status_code == 200, finished.text
        prior_item = _rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id})[0]
        assert prior_item["trigger_label"] is None
        assert all(
            prior_item[column] is None for column in ("base_branch", "base_source", "base_commit")
        )
        calls.clear()

        relabelled = _label(client, ingress, 4943, "base:next")

        assert relabelled.status_code == 200, relabelled.text
        assert relabelled.json()["status"] == "factory_admitted"
        item = _rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id})[0]
        assert item["trigger_label"] == LABEL
        assert all(item[column] is None for column in ("base_branch", "base_source", "base_commit"))
        for column in (
            "agent_id",
            "code_host_kind",
            "code_host_host",
            "repository_project_id",
            "code_host_installation_id",
            "publication_lineage_id",
            "conversation_id",
        ):
            assert item[column] == prior_item[column]
        requests = _rows(
            "SELECT id, status, sequence FROM curie.execution_requests "
            "WHERE work_item_id = :id ORDER BY sequence",
            {"id": item_id},
        )
        assert [(row["status"], row["sequence"]) for row in requests] == [
            ("failed", 1),
            ("waiting", 2),
        ]
        assert requests[1]["id"] != uuid.UUID(request_id)
        assert not any("/git/ref/heads/" in path for _, path in calls)
        assert _lineage_row(publication["lineage_id"]) == initial_lineage
        root = f"/v1/internal/publications/{publication['id']}"

        context = client.get(f"{root}/branch-head", headers=WORKER_HEADERS)

        assert context.status_code == 200, context.text
        assert context.json() == {"head_sha": None, "base_branch": "release/stable"}
        repository.branches[publication["branch"]] = FIRST_REVISION_SHA
        repository.branches["release/stable"] = LATER_BASE_SHA
        repository.commits[FIRST_REVISION_SHA] = {
            "sha": FIRST_REVISION_SHA,
            "parents": [{"sha": MOVED_PARENT_SHA}],
            "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
        }
        lease = _claim_publication_lease(publication["id"], "recovery-worker")
        before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))
        verified = client.post(
            f"{root}/revision-commit",
            json={
                "commit_sha": FIRST_REVISION_SHA,
                "revision_id": publication["id"],
                "expected_parent": BASE_SHA,
            },
            headers=WORKER_HEADERS,
        )
        assert verified.status_code == 200, verified.text
        assert verified.json() == {"commit_sha": FIRST_REVISION_SHA, "parent_sha": MOVED_PARENT_SHA}
        opened = client.post(
            f"{root}/pull-request",
            json={"expected_head_sha": FIRST_REVISION_SHA},
            headers=WORKER_HEADERS,
        )
        assert opened.status_code == 200, opened.text
        number, url = opened.json()["number"], opened.json()["url"]
        assert len(repository.pulls) == 1
        assert repository.pulls[number]["base"] == "release/stable"
        assert repository.pulls[number]["head"] == publication["branch"]
        assert (
            _publication_row(publication["id"]),
            _lineage_row(publication["lineage_id"]),
        ) == before
        assert _rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id}) == [item]
        for sha in (FIRST_REVISION_SHA, MOVED_PARENT_SHA, LATER_BASE_SHA):
            assert ("GET", f"/repos/{REPO}/git/commits/{sha}") in calls
        assert ("GET", f"/repos/{REPO}/git/ref/heads/release/stable") in calls
        assert ("GET", f"/repos/{REPO}/git/ref/heads/next") not in calls
        stale = _settle_moved(
            client,
            publication,
            lease,
            pr_number=number,
            pr_url=url,
            expected_publication_version=lease[0] - 1,
        )
        assert stale.status_code == 409, stale.text
        assert (
            _publication_row(publication["id"]),
            _lineage_row(publication["lineage_id"]),
        ) == before

        settled = _settle_moved(client, publication, lease, pr_number=number, pr_url=url)

        assert settled.status_code == 200, settled.text
        lineage = _lineage_row(publication["lineage_id"])
        assert (
            lineage["base_sha"],
            lineage["head_sha"],
            lineage["pr_number"],
            lineage["version"],
        ) == (
            MOVED_PARENT_SHA,
            FIRST_REVISION_SHA,
            number,
            initial_lineage["version"] + 1,
        )
        assert lineage["binding_id"] == initial_lineage["binding_id"]
        assert lineage["binding_generation"] == initial_lineage["binding_generation"]
        assert _identity_columns(lineage) == (
            "github",
            "github.com",
            str(REPO_ID),
            INSTALLATION_ID,
            f"PR_example_{number}",
            "release/stable",
        )
        stored = _publication_row(publication["id"])
        assert stored["base_sha"] == stored["expected_prior_head"] == BASE_SHA
        assert stored["status"] == "succeeded"
        assert stored["version"] == before[0]["version"] + 1
        assert stored["terminal_at"] is not None
        assert stored["lease_owner"] is stored["lease_expires_at"] is None
        assert _rows("SELECT * FROM curie.work_items WHERE id = :id", {"id": item_id}) == [item]
    finally:
        client.app.state.http_client = original
        client.portal.call(injected.aclose)


@pytest.mark.parametrize("moved", [False, True], ids=["unchanged", "moved"])
def test_manual_admission_without_base_can_open_first_publication(
    manual_publication: ManualPublication, moved: bool
) -> None:
    client, truth, _, publication = manual_publication
    root = f"/v1/internal/publications/{publication['id']}"
    context = client.get(f"{root}/branch-head", headers=WORKER_HEADERS)
    assert context.status_code == 200, context.text
    assert context.json() == {"head_sha": None, "base_branch": "release/stable"}
    parent = MOVED_PARENT_SHA if moved else BASE_SHA
    _manual_first_commit(truth, publication, parent)
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))

    verified = client.post(
        f"{root}/revision-commit",
        json={
            "commit_sha": FIRST_REVISION_SHA,
            "revision_id": publication["id"],
            "expected_parent": BASE_SHA,
        },
        headers=WORKER_HEADERS,
    )
    assert verified.status_code == 200, verified.text
    assert verified.json() == {"commit_sha": FIRST_REVISION_SHA, "parent_sha": parent}
    opened = client.post(
        f"{root}/pull-request",
        json={"expected_head_sha": FIRST_REVISION_SHA},
        headers=WORKER_HEADERS,
    )
    assert opened.status_code == 200, opened.text
    assert (opened.json()["number"], opened.json()["url"]) == (PR_NUMBER, PR_URL)
    [fields] = truth["opened"]
    assert fields["base"] == "release/stable"
    assert fields["head"] == publication["branch"]
    assert ("GET", f"/repos/{REPO}/git/ref/heads/release/stable") in truth["calls"]
    assert ("GET", f"/repos/{REPO}/git/ref/heads/main") not in truth["calls"]
    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before


@pytest.mark.parametrize("moved", [False, True], ids=["unchanged", "moved"])
def test_manual_admission_without_base_persists_parent_then_revises_same_pr_and_metadata(
    manual_publication: ManualPublication, auth_headers: dict[str, str], moved: bool
) -> None:
    client, truth, payload, publication = manual_publication
    parent = MOVED_PARENT_SHA if moved else BASE_SHA
    _manual_first_commit(truth, publication, parent)
    truth["pull_exists"] = True
    lease = _claim_publication_lease(publication["id"], "manual-publication-worker")

    settled = _settle_moved(client, publication, lease)

    assert settled.status_code == 200, settled.text
    row = _lineage_row(publication["lineage_id"])
    identity = _identity_columns(row)
    assert identity == ("github", "github.com", "9001", 41, "PR_example_123", "release/stable")
    assert (row["base_sha"], row["head_sha"], row["pr_number"], row["version"]) == (
        parent,
        FIRST_REVISION_SHA,
        PR_NUMBER,
        2,
    )
    stored = _publication_row(publication["id"])
    assert stored["base_sha"] == stored["expected_prior_head"] == BASE_SHA
    assert stored["status"] == "succeeded"
    assert _rows(
        "SELECT w.publication_lineage_id FROM curie.work_items w "
        "JOIN curie.execution_requests r ON r.work_item_id = w.id WHERE r.id = :id",
        {"id": uuid.UUID(payload["work_item_request_id"])},
    )[0]["publication_lineage_id"] == uuid.UUID(publication["lineage_id"])

    _mark_outcome_history_ready(publication["id"])
    second = _create_publication(
        client, {**payload, "base_sha": FIRST_REVISION_SHA, "dedupe_key": uuid.uuid4().hex}
    )
    assert second["lineage_id"] == publication["lineage_id"]
    resolved = _resolve(client, auth_headers, second["approval_id"])
    assert resolved.status_code == 200, resolved.text
    truth["default_branch"] = "different-default"
    truth["head_sha"] = SECOND_REVISION_SHA
    truth["branches"][publication["branch"]] = SECOND_REVISION_SHA
    truth["commits"][SECOND_REVISION_SHA] = {
        "sha": SECOND_REVISION_SHA,
        "parents": [{"sha": LATER_BASE_SHA}],
        "message": f"Revise\n\nCurie-Revision: {second['id']}\n",
    }
    root = f"/v1/internal/publications/{second['id']}"
    verify_body = {
        "commit_sha": SECOND_REVISION_SHA,
        "revision_id": second["id"],
        "expected_parent": FIRST_REVISION_SHA,
    }
    refused = client.post(f"{root}/revision-commit", json=verify_body, headers=WORKER_HEADERS)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.revision_mismatch"
    truth["commits"][SECOND_REVISION_SHA]["parents"] = [{"sha": FIRST_REVISION_SHA}]
    verified = client.post(f"{root}/revision-commit", json=verify_body, headers=WORKER_HEADERS)
    assert verified.status_code == 200, verified.text
    assert verified.json() == {"commit_sha": SECOND_REVISION_SHA, "parent_sha": FIRST_REVISION_SHA}
    advanced = _settle_moved(
        client,
        second,
        _claim_publication_lease(second["id"], "manual-revision-worker"),
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    row = _lineage_row(publication["lineage_id"])
    assert _identity_columns(row) == identity
    assert (row["base_sha"], row["head_sha"], row["pr_number"], row["version"]) == (
        parent,
        SECOND_REVISION_SHA,
        PR_NUMBER,
        3,
    )

    _mark_outcome_history_ready(second["id"])
    metadata = _create_publication(
        client,
        {
            **payload,
            "base_sha": SECOND_REVISION_SHA,
            "dedupe_key": uuid.uuid4().hex,
            "patch_b64": "",
            "changed_paths": [],
            "title": "Reviewed manual title",
            "body": "Reviewed manual body",
            "observed_title": truth["title"],
            "observed_body_sha256": hashlib.sha256(truth["body"].encode()).hexdigest(),
            "observed_lineage_id": publication["lineage_id"],
            "observed_lineage_version": 3,
        },
    )
    resolved = _resolve(client, auth_headers, metadata["approval_id"])
    assert resolved.status_code == 200, resolved.text
    before_metadata = _lineage_row(publication["lineage_id"])
    assert _identity_columns(before_metadata) == identity
    assert (
        before_metadata["base_sha"],
        before_metadata["head_sha"],
        before_metadata["pr_number"],
        before_metadata["pr_url"],
    ) == (parent, SECOND_REVISION_SHA, PR_NUMBER, PR_URL)
    assert (before_metadata["latest_revision"], before_metadata["version"]) == (3, 3)
    observed_body = truth["body"]
    truth["body"] = "An edit after approval"
    metadata_url = f"/v1/internal/publications/{metadata['id']}/pull-request/metadata"
    refused = client.post(metadata_url, headers=WORKER_HEADERS)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.metadata_changed"
    assert truth["updated"] == []
    assert _lineage_row(publication["lineage_id"]) == before_metadata
    truth["body"] = observed_body
    updated = client.post(metadata_url, headers=WORKER_HEADERS)
    assert updated.status_code == 200, updated.text
    assert (updated.json()["number"], updated.json()["head_sha"]) == (
        PR_NUMBER,
        SECOND_REVISION_SHA,
    )
    assert updated.json()["updated_at"] is not None
    assert truth["updated"] == [{"title": "Reviewed manual title", "body": "Reviewed manual body"}]
    assert truth["opened"] == []
    assert _lineage_row(publication["lineage_id"]) == before_metadata


@pytest.mark.parametrize("defect", ["off-branch", "unrelated", "rewound", "marker", "merge"])
def test_manual_admission_without_base_still_requires_ancestry_and_revision_identity(
    manual_publication: ManualPublication, defect: str
) -> None:
    client, truth, _, publication = manual_publication
    _manual_first_commit(truth, publication, MOVED_PARENT_SHA)
    if defect == "off-branch":
        truth["commits"][LATER_BASE_SHA]["parents"] = [{"sha": BASE_SHA}]
    elif defect == "unrelated":
        truth["commits"][MOVED_PARENT_SHA]["parents"] = []
    elif defect == "rewound":
        truth["branches"]["release/stable"] = BASE_SHA
    elif defect == "marker":
        truth["commits"][FIRST_REVISION_SHA]["message"] = f"Curie-Revision: {uuid.uuid4()}"
    else:
        truth["commits"][FIRST_REVISION_SHA]["parents"].append({"sha": BASE_SHA})
    lease = _claim_publication_lease(publication["id"], "manual-refusal-worker")
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))
    root = f"/v1/internal/publications/{publication['id']}"
    responses = [
        client.post(
            f"{root}/revision-commit",
            json={
                "commit_sha": FIRST_REVISION_SHA,
                "revision_id": publication["id"],
                "expected_parent": BASE_SHA,
            },
            headers=WORKER_HEADERS,
        ),
        client.post(
            f"{root}/pull-request",
            json={"expected_head_sha": FIRST_REVISION_SHA},
            headers=WORKER_HEADERS,
        ),
        _settle_moved(client, publication, lease),
    ]
    code = "revision_mismatch" if defect in {"marker", "merge"} else "base_ancestry_mismatch"
    for response in responses:
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["code"] == f"publication.{code}"
    assert truth["opened"] == []
    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before
    _manual_first_commit(truth, publication, BASE_SHA)
    healthy = _settle_moved(client, publication, lease)
    assert healthy.status_code == 200, healthy.text


def test_first_settlement_records_actual_parent_with_head_and_keeps_approval_base(
    clean_db: None, publication_stack: tuple[TestClient, str], auth_headers: dict[str, str]
) -> None:
    client, _ = publication_stack
    deployment, publication = _approved_publication(client, auth_headers)
    lease = _claim_publication_lease(publication["id"], "moved-base-worker")
    with _moved_base_graph(client, publication) as graph:
        graph["tip"] = LATER_BASE_SHA
        response = _settle_moved(client, publication, lease)
    assert response.status_code == 200, response.text
    assert response.json()["base_sha"] == MOVED_PARENT_SHA
    assert response.json()["head_sha"] == FIRST_REVISION_SHA
    row = _lineage_row(publication["lineage_id"])
    assert (row["base_sha"], row["head_sha"], row["version"]) == (
        MOVED_PARENT_SHA,
        FIRST_REVISION_SHA,
        2,
    )
    settled = _publication_row(publication["id"])
    assert settled["base_sha"] == settled["expected_prior_head"] == BASE_SHA
    assert settled["status"] == "succeeded"
    assert settled["lease_owner"] is None

    # A later approved revision retains the first publication's actual base.
    _mark_outcome_history_ready(publication["id"])
    conversation = _rows(
        "SELECT conversation_id FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(publication["approval_id"])},
    )[0]["conversation_id"]
    payload = _publication_payload(deployment["id"], conversation_id=conversation)
    payload["base_sha"] = FIRST_REVISION_SHA
    second = _create_publication(client, payload)
    assert _resolve(client, auth_headers, second["approval_id"]).status_code == 200
    advanced = _advance_lineage(
        client,
        second["id"],
        expected_version=2,
        expected_head_sha=FIRST_REVISION_SHA,
        head_sha=SECOND_REVISION_SHA,
    )
    assert advanced.status_code == 200, advanced.text
    assert _lineage_row(publication["lineage_id"])["base_sha"] == MOVED_PARENT_SHA

    _mark_outcome_history_ready(second["id"])
    third_payload = _publication_payload(deployment["id"], conversation_id=conversation)
    third_payload["base_sha"] = SECOND_REVISION_SHA
    third = _create_publication(client, third_payload)
    assert _resolve(client, auth_headers, third["approval_id"]).status_code == 200
    _execute(
        "UPDATE curie.publications SET approval_card_reported_at = now() WHERE id = :id",
        {"id": uuid.UUID(third["id"])},
    )

    async def close_established_lineage() -> None:
        from curie_worker.publication_store import PostgresPublicationStore

        engine = create_async_engine(get_settings().database_url)
        try:
            store = PostgresPublicationStore(engine, schema="curie", lease_owner="terminal-worker")
            work = await store.claim_next()
            assert work is not None and str(work.publication_id) == third["id"]
            await store.mark_lineage_terminal(
                work.lineage_id,
                publication_id=work.publication_id,
                expected_publication_version=work.version,
                lease_owner=work.lease_owner,
                expected_version=work.lineage_version,
                expected_stored_head=SECOND_REVISION_SHA,
                state="closed",
                pr_number=PR_NUMBER,
                pr_url=PR_URL,
                head_sha=SECOND_REVISION_SHA,
                first_parent_sha=None,
            )
        finally:
            await engine.dispose()

    client.portal.call(close_established_lineage)
    closed = _lineage_row(publication["lineage_id"])
    assert (closed["status"], closed["base_sha"], closed["head_sha"]) == (
        "closed",
        MOVED_PARENT_SHA,
        SECOND_REVISION_SHA,
    )


@pytest.mark.parametrize(
    "mutation", ["publication-version", "lineage-version", "lease", "revision", "expiry"]
)
def test_settlement_rechecks_stored_authority_after_remote_read_without_partial_mutation(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    mutation: str,
) -> None:
    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    lease = _claim_publication_lease(publication["id"], "moved-base-worker")
    statements = {
        "publication-version": "UPDATE curie.publications SET version = version + 1 WHERE id = :id",
        "lineage-version": (
            "UPDATE curie.thread_publication_lineages SET version = version + 1 WHERE id = :id"
        ),
        "lease": (
            "UPDATE curie.publications SET lease_owner = 'another-worker', "
            "version = version + 1 WHERE id = :id"
        ),
        "revision": (
            "UPDATE curie.thread_publication_lineages "
            "SET latest_revision = latest_revision + 1 WHERE id = :id"
        ),
        "expiry": (
            "UPDATE curie.publications SET status = 'expired', version = version + 1 WHERE id = :id"
        ),
    }
    lineage_mutation = mutation in {"lineage-version", "revision"}
    target = publication["lineage_id"] if lineage_mutation else publication["id"]
    race = (statements[mutation], {"id": uuid.UUID(target)})
    before_lineage = _lineage_row(publication["lineage_id"])
    before_publication = _publication_row(publication["id"])
    with _moved_base_graph(client, publication, race=race) as graph:
        response = _settle_moved(client, publication, lease)
    assert graph["raced"]
    assert response.status_code == 409, response.text
    after_lineage = _lineage_row(publication["lineage_id"])
    after_publication = _publication_row(publication["id"])
    if mutation == "lineage-version":
        before_lineage["version"] += 1
    elif mutation == "revision":
        before_lineage["latest_revision"] += 1
    else:
        before_publication["version"] += 1
        if mutation == "lease":
            before_publication["lease_owner"] = "another-worker"
        elif mutation == "expiry":
            before_publication["status"] = "expired"
    assert after_lineage == before_lineage
    assert after_publication == before_publication


def _real_proof(client: TestClient, publication_id: str, head_sha: str) -> Any:
    from curie_api.crud import publication_queries
    from curie_api.factory_rediscovery import publishing_code_host
    from curie_api.publication_verification import publication_base_ref, verify_publication_commit
    from curie_api.repository_access import lineage_repository

    async def prove() -> Any:
        async with client.app.state.sessionmaker() as session:
            publication = await publication_queries.get_publication(
                session, uuid.UUID(publication_id)
            )
            assert publication is not None and publication.lineage is not None
            repository = await lineage_repository(session, get_settings(), publication.lineage)
            code_host = publishing_code_host(
                get_settings(), client.app.state.http_client, repository
            )
            branch = await publication_base_ref(session, code_host, publication, repository)
            return await verify_publication_commit(
                code_host, repository, publication, commit_sha=head_sha, base_branch=branch
            )

    return client.portal.call(prove)


def _write_proof(client: TestClient, publication_id: str, proof: Any, **changes: Any) -> Any:
    from curie_api.crud import lineages
    from curie_api.schemas.publications import PublicationLineageAdvance

    row = _publication_row(publication_id)
    lineage = _rows(
        "SELECT l.version, l.head_sha FROM curie.thread_publication_lineages l "
        "JOIN curie.publications p ON p.lineage_id = l.id WHERE p.id = :id",
        {"id": uuid.UUID(publication_id)},
    )[0]
    values = {
        "expected_version": lineage["version"],
        "expected_head_sha": lineage["head_sha"],
        "expected_publication_version": row["version"],
        "lease_owner": row["lease_owner"],
        "state": "open",
        "pr_number": PR_NUMBER,
        "pr_url": PR_URL,
        "head_sha": FIRST_REVISION_SHA,
        "metadata_updated_at": None,
    }
    values.update(changes)

    async def write() -> Any:
        async with client.app.state.sessionmaker() as session:
            return await lineages.advance_publication_lineage(
                session,
                uuid.UUID(publication_id),
                PublicationLineageAdvance(**values),
                github_html_base="https://github.com",
                first_parent_proof=proof,
            )

    return client.portal.call(write)


@pytest.mark.parametrize("changed_row", ["publication", "lineage"])
def test_stale_verified_snapshot_cannot_be_rebound_to_fresh_outcome_versions(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    changed_row: str,
) -> None:
    from curie_api.crud.errors import PublicationLineageConflict

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    _claim_publication_lease(publication["id"], "moved-base-worker")
    with _moved_base_graph(client, publication):
        proof = _real_proof(client, publication["id"], FIRST_REVISION_SHA)
    table = "publications" if changed_row == "publication" else "thread_publication_lineages"
    target = publication["id"] if changed_row == "publication" else publication["lineage_id"]
    _execute(
        f"UPDATE curie.{table} SET version = version + 1 WHERE id = :id", {"id": uuid.UUID(target)}
    )
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))

    with pytest.raises(PublicationLineageConflict):
        _write_proof(client, publication["id"], proof)

    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before


@pytest.mark.parametrize("field", ["parent_sha", "first_parent_proof"])
def test_headless_settlement_requires_verified_proof_and_refuses_worker_supplied_parent(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    field: str,
) -> None:
    from curie_api.crud.errors import PublicationLineageConflict

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    lease = _claim_publication_lease(publication["id"], "moved-base-worker")
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))
    with pytest.raises(PublicationLineageConflict):
        _write_proof(client, publication["id"], None)
    forged = client.patch(
        LINEAGE_OPERATION.format(publication_id=publication["id"]),
        headers=WORKER_HEADERS,
        json={
            "expected_version": 1,
            "expected_head_sha": None,
            "expected_publication_version": lease[0],
            "lease_owner": lease[1],
            "state": "open",
            "pr_number": PR_NUMBER,
            "pr_url": PR_URL,
            "head_sha": FIRST_REVISION_SHA,
            "metadata_updated_at": None,
            field: MOVED_PARENT_SHA,
        },
    )
    assert forged.status_code == 422, forged.text
    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before


def test_losing_publication_cas_rolls_back_parent_head_and_identity_together(
    clean_db: None, publication_stack: tuple[TestClient, str], auth_headers: dict[str, str]
) -> None:
    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    lease = _claim_publication_lease(publication["id"], "moved-base-worker")
    name = f"test_publication_cas_{uuid.uuid4().hex}"
    _execute(
        f"CREATE FUNCTION curie.{name}() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN UPDATE curie.publications SET version = version + 1 "
        f"WHERE id = '{publication['id']}'; "
        "RETURN NEW; END; $$"
    )
    try:
        _execute(
            f"CREATE TRIGGER {name} BEFORE UPDATE ON curie.thread_publication_lineages "
            f"FOR EACH ROW WHEN (OLD.id = '{publication['lineage_id']}'::uuid) "
            f"EXECUTE FUNCTION curie.{name}()"
        )
        before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))
        with _moved_base_graph(client, publication):
            response = _settle_moved(client, publication, lease)
        assert response.status_code == 409, response.text
        assert (
            _publication_row(publication["id"]),
            _lineage_row(publication["lineage_id"]),
        ) == before
    finally:
        _execute(f"DROP TRIGGER IF EXISTS {name} ON curie.thread_publication_lineages")
        _execute(f"DROP FUNCTION curie.{name}()")

    with _moved_base_graph(client, publication):
        healthy = _settle_moved(client, publication, lease)
    assert healthy.status_code == 200, healthy.text


def test_two_verified_contenders_cannot_settle_different_first_parents(
    clean_db: None, publication_stack: tuple[TestClient, str], auth_headers: dict[str, str]
) -> None:
    from curie_api.crud import lineages
    from curie_api.crud.errors import PublicationLineageConflict
    from curie_api.schemas.publications import PublicationLineageAdvance

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    lease = _claim_publication_lease(publication["id"], "contending-worker")
    with _moved_base_graph(client, publication) as graph:
        first = _real_proof(client, publication["id"], FIRST_REVISION_SHA)
        graph["tip"] = LATER_BASE_SHA
        graph["commits"][SECOND_REVISION_SHA] = {
            "sha": SECOND_REVISION_SHA,
            "parents": [{"sha": LATER_BASE_SHA}],
            "message": f"Publish\n\nCurie-Revision: {publication['id']}\n",
        }
        second = _real_proof(client, publication["id"], SECOND_REVISION_SHA)

    async def settle(proof: Any) -> tuple[str, str]:
        async with client.app.state.sessionmaker() as session:
            try:
                await lineages.advance_publication_lineage(
                    session,
                    uuid.UUID(publication["id"]),
                    PublicationLineageAdvance(
                        expected_version=1,
                        expected_head_sha=None,
                        expected_publication_version=lease[0],
                        lease_owner=lease[1],
                        state="open",
                        pr_number=PR_NUMBER,
                        pr_url=PR_URL,
                        head_sha=proof.commit_sha,
                        metadata_updated_at=None,
                    ),
                    github_html_base="https://github.com",
                    first_parent_proof=proof,
                )
            except PublicationLineageConflict as exc:
                return "refused", exc.code
            return "accepted", proof.commit_sha

    async def race() -> list[tuple[str, str]]:
        return list(await asyncio.gather(settle(first), settle(second)))

    results = client.portal.call(race)
    assert sorted(result[0] for result in results) == ["accepted", "refused"]
    row = _lineage_row(publication["lineage_id"])
    assert (row["head_sha"], row["base_sha"]) in {
        (FIRST_REVISION_SHA, MOVED_PARENT_SHA),
        (SECOND_REVISION_SHA, LATER_BASE_SHA),
    }
    assert row["version"] == 2
    assert _publication_row(publication["id"])["base_sha"] == BASE_SHA


@pytest.mark.parametrize("mismatch", ["other-publication", "accepted-head", "lineage-base"])
def test_valid_proof_cannot_authorize_another_publication_head_or_initial_base(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    mismatch: str,
) -> None:
    from curie_api.crud.errors import PublicationLineageConflict

    client, _ = publication_stack
    deployment, publication = _approved_publication(client, auth_headers)
    _claim_publication_lease(publication["id"], "moved-base-worker")
    target = publication
    if mismatch == "other-publication":
        _, target = _approved_publication(client, auth_headers, deployment=deployment)
        _claim_publication_lease(target["id"], "moved-base-worker")
    with _moved_base_graph(client, target):
        proof = _real_proof(client, target["id"], FIRST_REVISION_SHA)
    if mismatch == "lineage-base":
        _execute(
            "UPDATE curie.thread_publication_lineages SET base_sha = :base WHERE id = :id",
            {"base": LATER_BASE_SHA, "id": uuid.UUID(publication["lineage_id"])},
        )
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))
    with pytest.raises(PublicationLineageConflict):
        _write_proof(
            client,
            publication["id"],
            proof,
            head_sha=SECOND_REVISION_SHA if mismatch == "accepted-head" else FIRST_REVISION_SHA,
        )
    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before


def test_verified_pr_base_must_agree_with_the_authoritative_proof_branch(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    lease = _claim_publication_lease(publication["id"], "branch-proof-worker")
    truth["base_ref"] = "release/other"
    before = (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"]))

    refused = _advance_lineage(
        client, publication["id"], expected_publication_version=lease[0], lease_owner=lease[1]
    )

    assert refused.status_code == 409, refused.text
    assert (_publication_row(publication["id"]), _lineage_row(publication["lineage_id"])) == before
    truth["base_ref"] = "main"
    healthy = _advance_lineage(
        client, publication["id"], expected_publication_version=lease[0], lease_owner=lease[1]
    )
    assert healthy.status_code == 200, healthy.text
