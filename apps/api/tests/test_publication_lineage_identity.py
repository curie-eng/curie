"""Contract for POST /v1/internal/publications/{id}/lineage/identity.

The worker asks the API for one verified GitHub identity and receives values,
never authority, and the API never writes the lineage row while answering.
Every eligible-path test therefore reads the row before and after and asserts
it is byte-for-byte unchanged: an endpoint that quietly wrote would be the
exact failure #2903 exists to remove (capture proved through a route nothing
calls).

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
from curie_telemetry import record_metric
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
IDENTITY_OPERATION = "/v1/internal/publications/{publication_id}/lineage/identity"
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
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A deployment plus an approved publication whose lineage has no PR yet.

    `deployment` is reused when a test needs two lineages: a second agent on
    the same Slack channel is refused by the binding, so a second lineage has
    to come from a second conversation on the same deployment.
    """

    deployment = deployment or _create_deployment(client, auth_headers)
    publication = _create_publication(
        client,
        _publication_payload(deployment["id"], conversation_id=f"identity-{uuid.uuid4().hex[:8]}"),
    )
    if truth is not None:
        truth["branch"] = publication["branch"]
    resolved = _resolve(client, auth_headers, publication["approval_id"])
    assert resolved.status_code == 200, resolved.text
    return deployment, publication


# --- the endpoint under test --------------------------------------------------


def _verify_identity(
    client: TestClient,
    publication_id: str,
    *,
    expected_version: int = 1,
    expected_head_sha: str | None = None,
    head_sha: str = FIRST_REVISION_SHA,
    state: str = "open",
    pr_number: int = PR_NUMBER,
    pr_url: str = PR_URL,
    headers: Mapping[str, str] | None = WORKER_HEADERS,
) -> Any:
    return client.post(
        f"/v1/internal/publications/{publication_id}/lineage/identity",
        json={
            "expected_version": expected_version,
            "expected_head_sha": expected_head_sha,
            "state": state,
            "pr_number": pr_number,
            "pr_url": pr_url,
            "head_sha": head_sha,
        },
        headers=dict(headers or {}),
    )


def _advance_lineage(
    client: TestClient,
    publication_id: str,
    *,
    expected_version: int = 1,
    expected_head_sha: str | None = None,
    head_sha: str = FIRST_REVISION_SHA,
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


# --- T1: the eligible answer --------------------------------------------------


def test_eligible_lineage_returns_verified_identity_and_writes_nothing(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    """T1. The four provider-owned values come back; the row does not move."""

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    before = _lineage_row(publication["lineage_id"])
    assert _identity_columns(before) == (None, None, None, None)
    assert before["pr_number"] is None
    assert truth["calls"] == []

    response = _verify_identity(client, publication["id"])

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["eligible"] is True
    assert body["lineage_id"] == publication["lineage_id"]
    assert (
        body["repository_id"],
        body["installation_id"],
        body["pr_node_id"],
        body["base_ref"],
    ) == (9001, 41, "PR_example_123", "main")
    # Provider truth was actually read, so `eligible` is not a constant.
    assert ("GET", f"/repos/{REPO}") in truth["calls"]
    assert ("GET", f"/repos/{REPO}/pulls/{PR_NUMBER}") in truth["calls"]
    # The endpoint returns values; only the worker writes them.
    assert _lineage_row(publication["lineage_id"]) == before


def test_identity_answer_echoes_the_lineage_id_on_both_answers(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    """T5b. The echo is what stops a reordered answer landing on another lineage."""

    client, truth, _ = review_lineage_app
    deployment, eligible_publication = _approved_publication(client, auth_headers, truth=truth)
    _, ineligible_publication = _approved_publication(client, auth_headers, deployment=deployment)
    _execute(
        "UPDATE curie.thread_publication_lineages SET binding_id = NULL WHERE id = :id",
        {"id": uuid.UUID(ineligible_publication["lineage_id"])},
    )

    eligible = _verify_identity(client, eligible_publication["id"])
    ineligible = _verify_identity(client, ineligible_publication["id"])

    assert eligible.status_code == 200, eligible.text
    assert ineligible.status_code == 200, ineligible.text
    assert eligible.json()["eligible"] is True
    assert ineligible.json()["eligible"] is False
    assert eligible.json()["lineage_id"] == eligible_publication["lineage_id"]
    assert ineligible.json()["lineage_id"] == ineligible_publication["lineage_id"]


def test_case_only_pr_url_spelling_is_still_eligible(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    """A3's deliberate casefold: GitHub's own spelling must not dead-letter."""

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(
        client,
        publication["id"],
        pr_url=f"https://github.com/ACME-Corp/Acme-Bot/pull/{PR_NUMBER}",
    )

    assert response.status_code == 200, response.text
    assert response.json()["eligible"] is True
    assert _lineage_row(publication["lineage_id"]) == before


# --- T2: every ineligible and refusing case -----------------------------------


def test_token_mode_install_is_ineligible_and_reads_no_provider_truth(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
) -> None:
    """T2, App never configured. Token-mode installs answer, they do not fail."""

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    before = _lineage_row(publication["lineage_id"])

    with _recording_github(client) as calls:
        response = _verify_identity(client, publication["id"])

    assert response.status_code == 200, response.text
    assert response.json()["eligible"] is False
    assert response.json()["lineage_id"] == publication["lineage_id"]
    assert calls == []
    assert _lineage_row(publication["lineage_id"]) == before


@pytest.mark.parametrize("case", ["binding_absent", "pre_app_pull_request"])
def test_ineligible_lineages_short_circuit_before_any_github_call(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    case: str,
) -> None:
    """T2. The guard at publication_authority.py:107 answers before a provider read.

    `pre_app_pull_request` is user constraint 1: a PR opened before the App
    existed keeps a non-NULL pr_number with NULL identity and must never be
    backfilled. `truth["calls"]` proves the refusal is decided locally, so a
    GitHub outage can never turn either case into a failure.
    """

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    lineage_id = uuid.UUID(publication["lineage_id"])
    expected_version = 1
    expected_head_sha: str | None = None
    if case == "binding_absent":
        _execute(
            "UPDATE curie.thread_publication_lineages SET binding_id = NULL WHERE id = :id",
            {"id": lineage_id},
        )
    else:
        _execute(
            "UPDATE curie.thread_publication_lineages SET pr_number = :n, pr_url = :u, "
            "head_sha = :sha, version = 2 WHERE id = :id",
            {"n": PR_NUMBER, "u": PR_URL, "sha": FIRST_REVISION_SHA, "id": lineage_id},
        )
        expected_version = 2
        expected_head_sha = FIRST_REVISION_SHA
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(
        client,
        publication["id"],
        expected_version=expected_version,
        expected_head_sha=expected_head_sha,
    )

    assert response.status_code == 200, response.text
    assert response.json()["eligible"] is False
    assert response.json()["repository_id"] is None
    assert truth["calls"] == []
    assert _lineage_row(publication["lineage_id"]) == before


@pytest.mark.parametrize(
    ("mutation", "expected_status", "expected_code"),
    [
        pytest.param(
            {"installation_id": 0}, 409, "publication.lineage_stale", id="installation_refused"
        ),
        pytest.param({"status": 500}, 503, GITHUB_UNAVAILABLE_CODE, id="github_500"),
        pytest.param({"status": 401}, 503, GITHUB_UNAVAILABLE_CODE, id="github_401"),
        pytest.param({"status": 403}, 503, GITHUB_UNAVAILABLE_CODE, id="github_403"),
        pytest.param({"status": 404}, 503, GITHUB_UNAVAILABLE_CODE, id="github_404"),
        pytest.param({"status": 429}, 503, GITHUB_UNAVAILABLE_CODE, id="github_429"),
        pytest.param({"transport_error": True}, 503, GITHUB_UNAVAILABLE_CODE, id="github_timeout"),
        pytest.param({"node_id": None}, 409, "publication.lineage_stale", id="node_id_absent"),
        pytest.param(
            {"head_sha": SECOND_REVISION_SHA},
            409,
            "publication.lineage_stale",
            id="head_sha_disagrees",
        ),
        pytest.param(
            {"html_url": f"https://github.com/other-corp/other-bot/pull/{PR_NUMBER}"},
            409,
            "publication.lineage_stale",
            id="pull_names_another_repository",
        ),
    ],
)
def test_provider_refusals_keep_their_existing_meanings(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    mutation: dict[str, Any],
    expected_status: int,
    expected_code: str,
) -> None:
    """T2. User constraint 2: unavailable stays 503, refused stays 409 stale."""

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth.update(mutation)
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(client, publication["id"])

    assert response.status_code == expected_status, response.text
    assert response.json()["detail"]["code"] == expected_code
    assert _lineage_row(publication["lineage_id"]) == before


# --- T3: the authorization recheck --------------------------------------------


@pytest.mark.parametrize("revocation", ["deployment_inactive", "workspace_repointed", "allowlist"])
def test_workspace_authorization_is_rechecked_before_identity_is_released(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    revocation: str,
) -> None:
    """T3. A2 step 5. In the split, only this endpoint can still perform it."""

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

    response = _verify_identity(client, publication["id"])

    assert response.status_code == 409, response.text
    body = response.json()
    assert body["detail"]["code"] == "publication.lineage_stale"
    # No identity leaves the API for a lineage that is no longer authorized.
    assert "repository_id" not in body
    assert "9001" not in response.text and "PR_example_123" not in response.text
    assert _lineage_row(publication["lineage_id"]) == before


# --- T4: preconditions agree with the PATCH endpoint --------------------------


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("stale_version", "publication.lineage_stale"),
        ("stale_expected_head", "publication.lineage_stale"),
        ("foreign_pr_url", "publication.lineage_stale"),
        ("terminal_lineage", "publication.lineage_terminal"),
        ("revision_not_current", "publication.lineage_stale"),
    ],
)
def test_precondition_conflicts_match_the_patch_endpoint_exactly(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    case: str,
    expected_code: str,
) -> None:
    """T4. The shared A3 extraction is only worth anything if the codes agree.

    Asserted against the PATCH sibling for the same body: a 409-only assertion
    would pass while the two endpoints disagreed about WHY, which is the
    divergence the extraction exists to prevent. Token mode keeps GitHub out
    of it, so this compares preconditions and nothing else.
    """

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    lineage_id = uuid.UUID(publication["lineage_id"])
    body: dict[str, Any] = {}
    if case == "stale_version":
        body = {"expected_version": 99}
    elif case == "stale_expected_head":
        body = {"expected_head_sha": SECOND_REVISION_SHA}
    elif case == "foreign_pr_url":
        body = {"pr_url": f"https://github.com/other-corp/other-bot/pull/{PR_NUMBER}"}
    elif case == "terminal_lineage":
        _execute(
            "UPDATE curie.thread_publication_lineages SET status = 'merged' WHERE id = :id",
            {"id": lineage_id},
        )
    else:
        _execute(
            "UPDATE curie.thread_publication_lineages SET latest_revision = 7 WHERE id = :id",
            {"id": lineage_id},
        )
    before = _lineage_row(publication["lineage_id"])

    identity = _verify_identity(client, publication["id"], **body)
    advance = _advance_lineage(client, publication["id"], **body)

    assert identity.status_code == 409, identity.text
    assert advance.status_code == 409, advance.text
    assert identity.json()["detail"]["code"] == expected_code
    assert identity.json()["detail"]["code"] == advance.json()["detail"]["code"]
    assert _lineage_row(publication["lineage_id"]) == before


# --- T4b: route hygiene -------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param(None, id="no_token"),
        pytest.param({"X-Curie-Worker-Token": "not-the-worker-token"}, id="wrong_token"),
    ],
)
def test_identity_route_requires_the_internal_worker_token(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    headers: dict[str, str] | None,
) -> None:
    """T4b. The route is worker-only, like every sibling on this router."""

    client, _ = publication_stack
    _, publication = _approved_publication(client, auth_headers)
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(client, publication["id"], headers=headers)

    assert response.status_code in (401, 403), response.text
    assert _lineage_row(publication["lineage_id"]) == before


def test_identity_route_operation_is_in_every_http_metric_domain() -> None:
    """T4b. A4/A5: an unregistered operation makes record_metric raise."""

    active = {
        "service.name": "curie-api",
        "operation": IDENTITY_OPERATION,
        "role": "server",
        "source": "POST",
    }
    record_metric("curie.http.server.active", 1, attributes=active)
    complete = {**active, "outcome": "2xx"}
    record_metric("curie.http.server.request", attributes=complete)
    record_metric("curie.http.server.request.duration", 0.001, attributes=complete)


def test_identity_route_404s_for_an_unknown_publication(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
) -> None:
    """A2 step 1. An absent publication or lineage is 404, never a 500."""

    client, _ = publication_stack

    response = _verify_identity(client, str(uuid.uuid4()))

    assert response.status_code == 404, response.text
    # The handler's own LookupError, not the router's "no such route" 404.
    assert response.json()["detail"] == "publication lineage not found"


# --- T5c: a provider-terminal PR is not a stale one ---------------------------


@pytest.mark.parametrize(
    ("pull_state", "merged", "observed"),
    [
        pytest.param("closed", True, "merged", id="merged"),
        pytest.param("closed", False, "closed", id="closed"),
    ],
)
def test_provider_terminal_pull_request_is_distinguishable_from_stale(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
    pull_state: str,
    merged: bool,
    observed: str,
) -> None:
    """T5c, API half (A7).

    The worker has to be able to terminalize the lineage instead of retrying
    forever, so this refusal must be distinguishable on the wire. The PATCH
    sibling is asserted on the same payload to prove A7 refined a refusal
    rather than changing an existing meaning.
    """

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth["state"] = pull_state
    truth["merged"] = merged
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(client, publication["id"])

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "publication.lineage_terminal"
    assert detail["observed_state"] == observed
    assert _lineage_row(publication["lineage_id"]) == before

    unchanged_patch = _advance_lineage(client, publication["id"])
    assert unchanged_patch.status_code == 409, unchanged_patch.text
    assert unchanged_patch.json()["detail"]["code"] == "publication.lineage_stale"
    assert _lineage_row(publication["lineage_id"]) == before


def test_malformed_merged_open_payload_is_a_plain_stale_refusal(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    """T5c companion. `merged: true` with `state: "open"` is a corrupt payload.

    GitHub never returns that pair, so it is not a terminal pull request and
    must keep the plain refusal. With the merged case above, these two pin the
    exception ladder's order: a reordering that reclassified a merged PR as
    stale, or this corrupt payload as terminal, fails exactly one of them.
    """

    client, truth, _ = review_lineage_app
    _, publication = _approved_publication(client, auth_headers, truth=truth)
    truth["state"] = "open"
    truth["merged"] = True
    before = _lineage_row(publication["lineage_id"])

    response = _verify_identity(client, publication["id"])

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "publication.lineage_stale"
    assert "observed_state" not in detail
    assert _lineage_row(publication["lineage_id"]) == before
