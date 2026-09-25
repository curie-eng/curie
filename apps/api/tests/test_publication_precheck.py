"""Execution scoped publication comparison authority from ADR 0174.

GitHub REST shapes come from
https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request and
https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app.
Only the external GitHub responses are replaced. Postgres and Valkey are real.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from fastapi.testclient import TestClient

from apps.api.tests.test_publications import (
    FIRST_REVISION_SHA,
    PR_NUMBER,
    PR_URL,
    REPO,
    WORKER_HEADERS,
    _execute,
    _open_lineage,
    _rows,
)
from apps.api.tests.test_publications import publication_stack as publication_stack

MINT_URL = "/v1/internal/publications/precheck/context"
COMPARE_URL = "/publications/precheck"
CAPABILITY_HEADER = "X-Curie-Publication-Precheck"
REPOSITORY_ID = 9001
INSTALLATION_ID = 41
PR_NODE_ID = "PR_example_123"
OBSERVED_TITLE = "Update the guide λ"
OBSERVED_BODY = "Keep two spaces  and a final newline.\n"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _signed_claim_change(token: str, **changes: Any) -> str:
    encoded = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    claims.update(changes)
    payload = base64.urlsafe_b64encode(
        json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
    ).rstrip(b"=").decode()
    signed = f"ppc.{payload}"
    signature = base64.urlsafe_b64encode(
        hmac.new(get_settings().api_key.encode(), signed.encode(), hashlib.sha256).digest()
    ).rstrip(b"=").decode()
    return f"{signed}.{signature}"


@pytest.fixture
def precheck_case(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[dict[str, Any]]:
    client, _ = publication_stack
    conversation = f"precheck-{uuid.uuid4().hex}"
    deployment, first = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation,
    )
    _execute(
        "UPDATE curie.publications SET status = 'succeeded' WHERE id = :id",
        {"id": uuid.UUID(first["id"])},
    )
    lineage_id = first["lineage_id"]
    lineage = _rows(
        "SELECT conversation_id, branch, version FROM curie.thread_publication_lineages "
        "WHERE id = :id",
        {"id": uuid.UUID(lineage_id)},
    )[0]
    _execute(
        "UPDATE curie.thread_publication_lineages SET "
        "github_repository_id = :repository, github_installation_id = :installation, "
        "github_pr_node_id = :node, base_ref = 'main' WHERE id = :lineage",
        {
            "repository": REPOSITORY_ID,
            "installation": INSTALLATION_ID,
            "node": PR_NODE_ID,
            "lineage": uuid.UUID(lineage_id),
        },
    )
    work_item_id, request_id = uuid.uuid4(), uuid.uuid4()
    now = datetime.now(UTC)
    _execute(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, publication_lineage_id) "
        "VALUES (:id, :repository, 3215, :installation, :agent, :repo, "
        ":conversation, :lineage)",
        {
            "id": work_item_id,
            "repository": REPOSITORY_ID,
            "installation": INSTALLATION_ID,
            "agent": uuid.UUID(deployment["agent_id"]),
            "repo": (
                "acme-corp/other"
                if getattr(request.node, "callspec", None)
                and request.node.callspec.params.get("mutation") == "repository"
                else REPO
            ),
            "conversation": (
                "other-conversation"
                if getattr(request.node, "callspec", None)
                and request.node.callspec.params.get("mutation") == "conversation"
                else lineage["conversation_id"]
            ),
            "lineage": uuid.UUID(lineage_id),
        },
    )
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, execution_attempts, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, "
        "1, 'fixture-runner', 7, :lease)",
        {
            "id": request_id,
            "item": work_item_id,
            "wait": now - timedelta(minutes=2),
            "started": now - timedelta(minutes=1),
            "deadline": (
                now - timedelta(seconds=1)
                if getattr(request.node, "callspec", None)
                and request.node.callspec.params.get("mutation") == "deadline"
                else now + timedelta(seconds=4)
                if getattr(request.node, "callspec", None)
                and request.node.callspec.params.get("mutation") == "deadline elapsed"
                else now + timedelta(hours=1)
            ),
            "lease": now + timedelta(minutes=5),
        },
    )

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
        "number": PR_NUMBER,
        "node_id": PR_NODE_ID,
        "html_url": PR_URL,
        "state": "open",
        "merged": False,
        "title": OBSERVED_TITLE,
        "body": OBSERVED_BODY,
        "head": {
            "sha": FIRST_REVISION_SHA,
            "ref": lineage["branch"],
            "repo": {"id": REPOSITORY_ID, "full_name": REPO},
        },
        "base": {
            "ref": "main",
            "repo": {"id": REPOSITORY_ID, "full_name": REPO},
        },
    }
    calls: list[httpx.Request] = []
    provider_status = {"value": 200}

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": INSTALLATION_ID})
        if request.url.path.endswith("/access_tokens"):
            return httpx.Response(
                201,
                json={"token": "fixture-precheck-app-token", "expires_at": "2999-01-01T00:00:00Z"},
            )
        assert request.headers["authorization"] == "Bearer fixture-precheck-app-token"
        assert request.method == "GET"
        assert request.url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}"
        if provider_status["value"] == 302:
            return httpx.Response(
                302, headers={"Location": "https://attacker.example.com/collect"}
            )
        return httpx.Response(provider_status["value"], json=copy.deepcopy(truth))

    real_client = httpx.Client
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *args, **kwargs: real_client(transport=httpx.MockTransport(github)),
    )
    injected = httpx.AsyncClient(transport=httpx.MockTransport(github), follow_redirects=True)
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        yield {
            "client": client,
            "deployment": deployment,
            "work_item_id": work_item_id,
            "request_id": request_id,
            "lineage_id": lineage_id,
            "lineage": lineage,
            "publication_id": first["id"],
            "truth": truth,
            "provider_status": provider_status,
            "calls": calls,
            "queued_event_id": str(uuid.uuid4()),
        }
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())
        _RESOLVERS.clear()
        get_settings.cache_clear()


def _mint_body(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "deployment_id": case["deployment"]["id"],
        "work_item_id": str(case["work_item_id"]),
        "execution_request_id": str(case["request_id"]),
        "runtime_epoch": 7,
        "queued_event_id": case["queued_event_id"],
    }


def _mint(case: dict[str, Any]) -> dict[str, Any]:
    response = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    return response.json()


def _compare(
    case: dict[str, Any], context: dict[str, Any], *, title: str, body: str
) -> Any:
    return case["client"].post(
        COMPARE_URL,
        headers={CAPABILITY_HEADER: context["capability"]},
        json={
            "observed_title": context["observed_title"],
            "observed_body_sha256": context["observed_body_sha256"],
            "observed_at": context["observed_at"],
            "proposed_title": title,
            "proposed_body": body,
        },
    )


def _pull_reads(case: dict[str, Any]) -> list[httpx.Request]:
    return [
        request
        for request in case["calls"]
        if request.url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}"
    ]


def _durable_snapshot() -> tuple[list[dict[str, Any]], ...]:
    return (
        _rows(
            "SELECT id, status, version, head_sha, pr_number FROM "
            "curie.thread_publication_lineages ORDER BY id"
        ),
        _rows(
            "SELECT (SELECT count(*) FROM curie.approvals) AS approvals, "
            "(SELECT count(*) FROM curie.publications) AS publications, "
            "(SELECT count(*) FROM curie.credential_redemption_audit_entries) AS redemptions"
        ),
        _rows(
            "SELECT id, publication_lineage_id, version, conversation_id, repo_full_name "
            "FROM curie.work_items ORDER BY id"
        ),
        _rows(
            "SELECT id, status, runtime_epoch, runtime_heartbeat_expires_at, "
            "execution_deadline FROM curie.execution_requests ORDER BY id"
        ),
    )


def test_mint_binds_running_request_and_comparison_reads_fresh_truth_without_writes(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    before = _durable_snapshot()
    context = _mint(case)
    expected = {
        "agent_id": case["deployment"]["agent_id"],
        "deployment_id": case["deployment"]["id"],
        "work_item_id": str(case["work_item_id"]),
        "execution_request_id": str(case["request_id"]),
        "lineage_id": case["lineage_id"],
        "runtime_epoch": 7,
        "lineage_version": case["lineage"]["version"],
        "conversation_id": case["lineage"]["conversation_id"],
        "queued_event_id": case["queued_event_id"],
        "expected_head": FIRST_REVISION_SHA,
    }
    assert set(context) == set(expected) | {
        "precheck_url",
        "capability",
        "observed_title",
        "observed_body_sha256",
        "observed_at",
    }
    assert {key: context[key] for key in expected} == expected
    assert context["observed_title"] == OBSERVED_TITLE
    assert context["observed_body_sha256"] == _digest(OBSERVED_BODY)
    assert datetime.fromisoformat(context["observed_at"]).tzinfo is not None
    assert context["precheck_url"].endswith(COMPARE_URL)
    assert context["capability"].startswith("ppc.")
    encoded = context["capability"].split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    assert set(claims) == set(expected) | {
        "scope",
        "observed_title_sha256",
        "observed_body_sha256",
        "observed_at",
        "iat",
        "exp",
    }
    for key, value in expected.items():
        assert claims[key] == value
    assert claims["scope"] == "publication.precheck"
    assert claims["observed_title_sha256"] == _digest(OBSERVED_TITLE)
    assert claims["observed_body_sha256"] == _digest(OBSERVED_BODY)
    assert claims["observed_at"] == context["observed_at"]
    assert claims["iat"] <= claims["exp"]
    assert claims["exp"] <= int(
        _rows(
            "SELECT extract(epoch FROM execution_deadline)::bigint AS deadline "
            "FROM curie.execution_requests WHERE id = :id",
            {"id": case["request_id"]},
        )[0]["deadline"]
    )
    assert "repo_full_name" not in claims
    assert "pr_number" not in claims
    assert "pr_url" not in claims
    assert OBSERVED_TITLE not in str(claims)
    assert OBSERVED_BODY not in str(claims)
    assert len(_pull_reads(case)) == 1
    assert _durable_snapshot() == before

    same = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)
    assert same.status_code == 200, same.text
    assert same.json() == {"result": "unchanged"}
    different = _compare(case, context, title="Better title", body="New body")
    assert different.status_code == 200, different.text
    assert different.json() == {"result": "metadata_changed"}
    assert len(_pull_reads(case)) == 3
    assert all(request.extensions.get("timeout") is not None for request in _pull_reads(case))
    assert _durable_snapshot() == before


def test_running_factory_request_without_existing_pr_gets_authenticated_absence(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    work_item_id, request_id = uuid.uuid4(), uuid.uuid4()
    conversation = f"first-pr-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    _execute(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id) "
        "VALUES (:id, :repository, 3216, :installation, :agent, :repo, :conversation)",
        {
            "id": work_item_id,
            "repository": REPOSITORY_ID,
            "installation": INSTALLATION_ID,
            "agent": uuid.UUID(case["deployment"]["agent_id"]),
            "repo": REPO,
            "conversation": conversation,
        },
    )
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, execution_attempts, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, "
        "1, 'fixture-runner', 7, :lease)",
        {
            "id": request_id,
            "item": work_item_id,
            "wait": now - timedelta(minutes=2),
            "started": now - timedelta(minutes=1),
            "deadline": now + timedelta(hours=1),
            "lease": now + timedelta(minutes=5),
        },
    )
    request = _mint_body(case)
    request["work_item_id"] = str(work_item_id)
    request["execution_request_id"] = str(request_id)
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=request, headers=WORKER_HEADERS)

    assert response.status_code == 204, response.text
    assert response.headers["cache-control"] == "no-store"
    assert _pull_reads(case) == []
    assert _durable_snapshot() == before


@pytest.mark.parametrize("change", ["title", "body"])
@pytest.mark.parametrize("proposed", ["old", "new"])
def test_remote_metadata_change_refuses_both_old_and_new_proposals(
    precheck_case: dict[str, Any], change: str, proposed: str
) -> None:
    case = precheck_case
    context = _mint(case)
    before = _durable_snapshot()
    case["truth"][change] = f"Remote {change} edit"
    title = OBSERVED_TITLE if proposed == "old" else case["truth"]["title"]
    body = OBSERVED_BODY if proposed == "old" else case["truth"]["body"]

    response = _compare(case, context, title=title, body=body)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "stale_context"
    assert len(_pull_reads(case)) == 2
    assert _durable_snapshot() == before


def test_null_github_body_is_observed_as_empty_bytes(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    case["truth"]["body"] = None
    context = _mint(case)
    assert context["observed_body_sha256"] == _digest("")

    response = _compare(case, context, title=OBSERVED_TITLE, body="A useful description")

    assert response.status_code == 200, response.text
    assert response.json() == {"result": "metadata_changed"}
    assert len(_pull_reads(case)) == 2


def test_comparison_cannot_select_a_different_repository_or_pull_request(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    before = _durable_snapshot()
    response = case["client"].post(
        COMPARE_URL,
        headers={CAPABILITY_HEADER: context["capability"]},
        json={
            "observed_title": context["observed_title"],
            "observed_body_sha256": context["observed_body_sha256"],
            "observed_at": context["observed_at"],
            "proposed_title": OBSERVED_TITLE,
            "proposed_body": OBSERVED_BODY,
            "repo_full_name": "acme-corp/other",
            "pr_number": 999,
            "lineage_id": str(uuid.uuid4()),
        },
    )

    assert response.status_code == 422, response.text
    assert len(_pull_reads(case)) == 1
    assert _durable_snapshot() == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("number", 124),
        ("node_id", "PR_other"),
        ("html_url", f"https://github.com/{REPO}/pull/124"),
        ("state", "closed"),
        ("merged", True),
        ("head.sha", "a" * 40),
        ("head.ref", "other"),
        ("head.repo.id", 9002),
        ("base.ref", "develop"),
        ("base.repo.id", 9002),
    ],
)
def test_changed_provider_identity_is_unavailable_and_does_not_change_lineage(
    precheck_case: dict[str, Any], field: str, value: Any
) -> None:
    case = precheck_case
    context = _mint(case)
    before = _durable_snapshot()
    target = case["truth"]
    path = field.split(".")
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code in {409, 502, 503}, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 2
    assert _durable_snapshot() == before


def test_provider_failure_is_unavailable_and_charges_an_authorized_attempt(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    before = _durable_snapshot()
    case["provider_status"]["value"] = 503

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code == 503, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 2
    assert _durable_snapshot() == before

    case["provider_status"]["value"] = 200
    second_context = _mint(case)
    for index in range(19):
        credential = context if index % 2 == 0 else second_context
        accepted = _compare(
            case, credential, title=OBSERVED_TITLE, body=OBSERVED_BODY
        )
        assert accepted.status_code == 200, accepted.text
        assert accepted.json() == {"result": "unchanged"}
    assert len(_pull_reads(case)) == 22
    exhausted = _compare(case, second_context, title=OBSERVED_TITLE, body=OBSERVED_BODY)
    assert exhausted.status_code == 429, exhausted.text
    assert len(_pull_reads(case)) == 22
    assert _durable_snapshot() == before


def test_provider_redirect_does_not_forward_the_github_credential(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    case["provider_status"]["value"] = 302

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code == 503, response.text
    assert len(_pull_reads(case)) == 2
    assert all(request.url.host != "attacker.example.com" for request in case["calls"])


def test_inflight_publication_makes_comparison_unavailable(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    _execute(
        "UPDATE curie.publications SET status = 'running' WHERE id = :id",
        {"id": uuid.UUID(case["publication_id"])},
    )
    before = _durable_snapshot()

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code == 503, response.text
    assert len(_pull_reads(case)) == 1
    assert _durable_snapshot() == before


@pytest.mark.parametrize(
    "mutation", ["epoch", "lease", "deadline", "status", "conversation", "repository"]
)
def test_mint_refuses_without_current_execution_and_lineage_authority(
    precheck_case: dict[str, Any], mutation: str
) -> None:
    case = precheck_case
    request = _mint_body(case)
    if mutation == "epoch":
        request["runtime_epoch"] += 1
    elif mutation == "lease":
        _execute(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
            {"id": case["request_id"]},
        )
    elif mutation == "deadline":
        pass
    elif mutation == "status":
        _execute(
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'owner_lost' WHERE id = :id",
            {"id": case["request_id"]},
        )
    elif mutation == "conversation":
        pass
    else:
        assert mutation == "repository"
    before = _durable_snapshot()
    response = case["client"].post(MINT_URL, json=request, headers=WORKER_HEADERS)

    assert 400 <= response.status_code < 500, response.text
    assert "capability" not in response.text
    assert _pull_reads(case) == []
    assert _durable_snapshot() == before


@pytest.mark.parametrize(
    "mutation,statement",
    [
        (
            "new epoch",
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
            "WHERE id = :id",
        ),
        (
            "lease lost",
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
        ),
        (
            "deadline elapsed",
            "",
        ),
        (
            "request terminated",
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'owner_lost' WHERE id = :id",
        ),
        (
            "lineage advanced",
            "UPDATE curie.thread_publication_lineages SET version = version + 1 "
            "WHERE id = :id",
        ),
        (
            "lineage head moved",
            "UPDATE curie.thread_publication_lineages SET head_sha = "
            "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE id = :id",
        ),
    ],
)
def test_existing_capability_refuses_changed_durable_authority_before_github(
    precheck_case: dict[str, Any], mutation: str, statement: str
) -> None:
    case = precheck_case
    context = _mint(case)
    target = case["lineage_id"] if mutation.startswith("lineage ") else case["request_id"]
    if mutation == "deadline elapsed":
        deadline = _rows(
            "SELECT extract(epoch FROM execution_deadline) AS deadline "
            "FROM curie.execution_requests WHERE id = :id",
            {"id": target},
        )[0]["deadline"]
        time.sleep(max(0.0, float(deadline) - time.time() + 0.1))
    else:
        _execute(statement, {"id": target})
    before = _durable_snapshot()

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert 400 <= response.status_code < 500, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 1
    assert _durable_snapshot() == before


def test_comparison_rechecks_authority_after_the_provider_read(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    context = _mint(case)
    original = case["client"].app.state.http_client

    async def change_epoch(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/repos/{REPO}/pulls/{PR_NUMBER}"
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
                        "WHERE id = :id"
                    ),
                    {"id": case["request_id"]},
                )
        finally:
            await engine.dispose()
        return httpx.Response(200, json=copy.deepcopy(case["truth"]))

    injected = httpx.AsyncClient(transport=httpx.MockTransport(change_epoch))
    case["client"].app.state.http_client = injected
    try:
        response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)
    finally:
        case["client"].app.state.http_client = original
        asyncio.run(injected.aclose())

    assert 400 <= response.status_code < 500, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert _rows(
        "SELECT runtime_epoch FROM curie.execution_requests WHERE id = :id",
        {"id": case["request_id"]},
    ) == [{"runtime_epoch": 8}]


def test_comparison_rejects_mismatched_event_observation_before_github(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    forged_event = dict(context)
    forged_event["observed_title"] = "A replacement model title"

    response = _compare(case, forged_event, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert 400 <= response.status_code < 500, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 1


def test_capability_is_accepted_only_at_the_comparison_route(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    token = context["capability"]
    client = case["client"]
    before = _durable_snapshot()
    # Neither the worker credential nor the platform key is a comparison token.
    for headers in ({}, WORKER_HEADERS, {"X-API-Key": get_settings().api_key}):
        response = client.post(
            COMPARE_URL,
            headers=headers,
            json={
                "observed_title": context["observed_title"],
                "observed_body_sha256": context["observed_body_sha256"],
                "observed_at": context["observed_at"],
                "proposed_title": OBSERVED_TITLE,
                "proposed_body": OBSERVED_BODY,
            },
        )
        assert response.status_code == 401, response.text
    assert client.get("/publications", headers={"X-API-Key": token}).status_code == 401
    assert client.get("/approvals", headers={"X-API-Key": token}).status_code == 401
    assert client.get(
        f"/agents/{case['deployment']['agent_id']}/state", headers={"X-API-Key": token}
    ).status_code == 401
    credential = client.post(
        f"/v1/internal/publications/{case['publication_id']}/credential",
        headers={"X-Curie-Worker-Token": token},
    )
    assert credential.status_code == 401
    mint_with_capability = client.post(
        MINT_URL, json=_mint_body(case), headers={"X-Curie-Worker-Token": token}
    )
    assert mint_with_capability.status_code == 401
    assert len(_pull_reads(case)) == 1
    assert _durable_snapshot() == before


def test_invalid_capability_never_reaches_github(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    context = _mint(case)
    original = context["capability"]
    prefix, encoded, signature = original.split(".")
    assert prefix == "ppc"
    altered = dict(context)
    replacement = "A" if signature[0] != "A" else "B"
    altered["capability"] = f"{prefix}.{encoded}.{replacement}{signature[1:]}"

    response = _compare(case, altered, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code == 401, response.text
    assert len(_pull_reads(case)) == 1


@pytest.mark.parametrize("change", ["expired", "scope", "agent", "lineage"])
def test_validly_signed_but_unbound_claims_do_not_reach_github(
    precheck_case: dict[str, Any], change: str
) -> None:
    case = precheck_case
    context = _mint(case)
    claims_change: dict[str, Any]
    if change == "expired":
        claims_change = {"exp": int(time.time()) - 1}
    elif change == "scope":
        claims_change = {"scope": "state"}
    elif change == "agent":
        claims_change = {"agent_id": str(uuid.uuid4())}
    else:
        claims_change = {"lineage_id": str(uuid.uuid4())}
    altered = dict(context)
    altered["capability"] = _signed_claim_change(context["capability"], **claims_change)

    response = _compare(case, altered, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert 400 <= response.status_code < 500, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 1


def test_worker_mint_requires_its_own_credential_and_matching_deployment(
    precheck_case: dict[str, Any]
) -> None:
    case = precheck_case
    client = case["client"]
    body = _mint_body(case)
    wrong_deployment = dict(body, deployment_id=str(uuid.uuid4()))
    for headers, request in (
        ({}, body),
        ({"X-API-Key": get_settings().api_key}, body),
        (WORKER_HEADERS, wrong_deployment),
    ):
        response = client.post(MINT_URL, json=request, headers=headers)
        assert 400 <= response.status_code < 500, response.text
        assert "capability" not in response.text
    assert _pull_reads(case) == []
