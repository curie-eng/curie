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
from concurrent.futures import ThreadPoolExecutor
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
    ENTERPRISE_API_URL,
    ENTERPRISE_HTML_BASE,
    FIRST_REVISION_SHA,
    PR_NUMBER,
    PR_URL,
    REPO,
    WORKER_HEADERS,
    _execute,
    _open_lineage,
    _publication_payload,
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
    payload = (
        base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )
    signed = f"ppc.{payload}"
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(get_settings().api_key.encode(), signed.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
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
    forge = getattr(request, "param", None)
    html_base = "https://github.com"
    if forge is not None and "api_url" in forge:
        html_base = forge["html_base"]
        monkeypatch.setenv("GITHUB_API_URL", forge["api_url"])
        monkeypatch.setenv("GITHUB_CLONE_BASE", html_base)
        get_settings.cache_clear()
    pr_url = f"{html_base}/{REPO}/pull/{PR_NUMBER}"
    conversation = f"precheck-{uuid.uuid4().hex}"
    deployment, first = _open_lineage(
        client,
        auth_headers,
        conversation_id=conversation,
        pr_url=pr_url,
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
    refusal = (
        request.node.callspec.params.get("refusal")
        if getattr(request.node, "callspec", None)
        else None
    )
    _execute(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, publication_lineage_id) "
        "VALUES (:id, :repository, 3215, :installation, :agent, :repo, "
        ":conversation, :lineage)",
        {
            "id": work_item_id,
            "repository": 9002 if refusal == "repository_id" else REPOSITORY_ID,
            "installation": 42 if refusal == "installation_id" else INSTALLATION_ID,
            "agent": uuid.UUID(deployment["agent_id"]),
            "repo": (
                "acme-corp/other"
                if getattr(request.node, "callspec", None)
                and (
                    request.node.callspec.params.get("mutation") == "repository"
                    or request.node.callspec.params.get("other_scope") == "repository"
                )
                else REPO
            ),
            "conversation": (
                "other-conversation"
                if getattr(request.node, "callspec", None)
                and (
                    request.node.callspec.params.get("mutation") == "conversation"
                    or request.node.callspec.params.get("other_scope") == "conversation"
                )
                else lineage["conversation_id"]
            ),
            "lineage": (
                None
                if forge is not None and forge.get("unlinked_work_item")
                else uuid.UUID(lineage_id)
            ),
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
            "started": now - timedelta(hours=2)
            if refusal == "deadline"
            else now - timedelta(minutes=1),
            "deadline": (
                now - timedelta(seconds=1)
                if getattr(request.node, "callspec", None)
                and (
                    request.node.callspec.params.get("mutation") == "deadline"
                    or refusal == "deadline"
                )
                # Leave fixture setup and the bounded mint enough time under
                # concurrent integration load before waiting for real expiry.
                else now + timedelta(seconds=30)
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
        "html_url": pr_url,
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
    pull_observer: dict[str, Any] = {}

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
        assert request.url.path.endswith(f"/repos/{REPO}/pulls/{PR_NUMBER}")
        if callback := pull_observer.get("callback"):
            callback()
        if provider_status["value"] == 302:
            return httpx.Response(302, headers={"Location": "https://attacker.example.com/collect"})
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
            "reply_conversation_id": conversation,
            "truth": truth,
            "provider_status": provider_status,
            "pull_observer": pull_observer,
            "calls": calls,
            "queued_event_id": str(uuid.uuid4()),
            "html_base": html_base,
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


def _compare(case: dict[str, Any], context: dict[str, Any], *, title: str, body: str) -> Any:
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
        if request.url.path.endswith(f"/repos/{REPO}/pulls/{PR_NUMBER}")
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


def _orphaned_precheck(case: dict[str, Any]) -> uuid.UUID:
    """Replay a cancelled publisher followed by an unlinked running successor."""
    previous_request_id = case["request_id"]
    current_request_id = uuid.uuid4()
    _execute(
        "UPDATE curie.execution_requests SET status = 'cancelled', "
        "terminal_at = clock_timestamp(), terminal_cause = 'issue_cancelled', "
        "termination_observation = 'fixture runtime termination observed' WHERE id = :id",
        {"id": previous_request_id},
    )
    _execute(
        "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status, "
        "wait_deadline, started_at, execution_deadline, execution_attempts, runtime_owner, "
        "runtime_epoch, runtime_heartbeat_expires_at) SELECT :id, work_item_id, 2, 'running', "
        "wait_deadline, started_at, execution_deadline, execution_attempts, runtime_owner, "
        "runtime_epoch, runtime_heartbeat_expires_at FROM curie.execution_requests WHERE id = :old",
        {"id": current_request_id, "old": previous_request_id},
    )
    _execute(
        "UPDATE curie.work_items SET next_sequence = 3 WHERE id = :id",
        {"id": case["work_item_id"]},
    )
    _execute(
        "UPDATE curie.publications SET execution_request_id = :request WHERE id = :id",
        {"id": uuid.UUID(case["publication_id"]), "request": previous_request_id},
    )
    case["request_id"] = current_request_id
    return previous_request_id


def _other_precheck_work_item(case: dict[str, Any]) -> uuid.UUID:
    item_id = uuid.uuid4()
    _execute(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id) "
        "SELECT :other, github_repository_id, github_issue_number + 1, "
        "github_installation_id, agent_id, repo_full_name, 'other-conversation' "
        "FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"], "other": item_id},
    )
    return item_id


@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True)
def test_mint_adopts_the_cancelled_predecessors_pr_before_reading_github(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    previous_request_id = _orphaned_precheck(case)
    old_request = _rows(
        "SELECT * FROM curie.execution_requests WHERE id = :id", {"id": previous_request_id}
    )
    old_publication = _rows(
        "SELECT * FROM curie.publications WHERE id = :id",
        {"id": uuid.UUID(case["publication_id"])},
    )
    version = _rows(
        "SELECT version FROM curie.work_items WHERE id = :id", {"id": case["work_item_id"]}
    )[0]["version"]
    expected = {
        "publication_lineage_id": uuid.UUID(case["lineage_id"]),
        "version": version + 1,
    }
    observed: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=1) as database_reader:

        def observe_committed_link() -> None:
            # A fresh database connection must see the committed link and be
            # able to lock the item while the external pull request read runs.
            rows = database_reader.submit(
                _rows,
                "SELECT publication_lineage_id, version FROM curie.work_items "
                "WHERE id = :id FOR UPDATE NOWAIT",
                {"id": case["work_item_id"]},
            ).result(timeout=5)
            assert rows == [expected]
            observed.extend(rows)

        case["pull_observer"]["callback"] = observe_committed_link
        context = _mint(case)
        replay = _mint(case)

    assert context["lineage_id"] == replay["lineage_id"] == case["lineage_id"]
    assert observed == [expected, expected]
    assert len(_pull_reads(case)) == 2
    assert _rows(
        "SELECT publication_lineage_id, version FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"]},
    ) == [expected]
    assert (
        _rows("SELECT * FROM curie.execution_requests WHERE id = :id", {"id": previous_request_id})
        == old_request
    )
    assert (
        _rows(
            "SELECT * FROM curie.publications WHERE id = :id",
            {"id": uuid.UUID(case["publication_id"])},
        )
        == old_publication
    )


@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True)
@pytest.mark.parametrize(
    "refusal", ["closed", "merged", "another_owner", "different_work_item", "no_publication"]
)
def test_mint_refuses_ineligible_orphan_lineages_without_durable_writes(
    precheck_case: dict[str, Any], refusal: str
) -> None:
    case = precheck_case
    previous_request_id = _orphaned_precheck(case)
    if refusal in {"closed", "merged"}:
        _execute(
            "UPDATE curie.thread_publication_lineages SET status = :status WHERE id = :id",
            {"id": uuid.UUID(case["lineage_id"]), "status": refusal},
        )
    elif refusal == "another_owner":
        _execute(
            "UPDATE curie.work_items SET publication_lineage_id = :lineage WHERE id = :id",
            {"id": _other_precheck_work_item(case), "lineage": uuid.UUID(case["lineage_id"])},
        )
    elif refusal == "different_work_item":
        foreign_request_id = uuid.uuid4()
        _execute(
            "INSERT INTO curie.execution_requests (id, work_item_id, sequence, status, "
            "wait_deadline, started_at, execution_deadline, terminal_at, terminal_cause, "
            "termination_observation, execution_attempts) SELECT :id, :item, 1, status, "
            "wait_deadline, started_at, execution_deadline, terminal_at, terminal_cause, "
            "termination_observation, execution_attempts FROM curie.execution_requests "
            "WHERE id = :old",
            {
                "id": foreign_request_id,
                "item": _other_precheck_work_item(case),
                "old": previous_request_id,
            },
        )
        _execute(
            "UPDATE curie.publications SET execution_request_id = :request WHERE id = :id",
            {"id": uuid.UUID(case["publication_id"]), "request": foreign_request_id},
        )
    else:
        _execute(
            "UPDATE curie.publications SET execution_request_id = NULL WHERE id = :id",
            {"id": uuid.UUID(case["publication_id"])},
        )
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)

    assert response.status_code == 409, response.text
    assert _durable_snapshot() == before
    assert _pull_reads(case) == []


@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True)
@pytest.mark.parametrize(
    "refusal",
    [
        "cancelled",
        "not_running",
        "repository_id",
        "installation_id",
        "epoch",
        "lease",
        "deadline",
        "not_earlier",
    ],
)
def test_mint_cannot_adopt_without_current_execution_and_matching_identity(
    precheck_case: dict[str, Any], refusal: str
) -> None:
    case = precheck_case
    _orphaned_precheck(case)
    body = _mint_body(case)
    if refusal == "epoch":
        body["runtime_epoch"] += 1
    elif refusal == "not_earlier":
        _execute(
            "UPDATE curie.publications SET execution_request_id = :request WHERE id = :id",
            {"id": uuid.UUID(case["publication_id"]), "request": case["request_id"]},
        )
    elif refusal == "cancelled":
        _execute(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
            {"id": case["work_item_id"]},
        )
    elif refusal in {"repository_id", "installation_id", "deadline"}:
        pass  # Immutable identity and deadlines were seeded by precheck_case.
    else:
        changes = {
            "not_running": "status = 'cancellation_requested', terminal_cause = 'issue_cancelled'",
            "lease": "runtime_heartbeat_expires_at = clock_timestamp() - interval '1 second'",
        }
        _execute(
            f"UPDATE curie.execution_requests SET {changes[refusal]} WHERE id = :id",
            {"id": case["request_id"]},
        )
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=body, headers=WORKER_HEADERS)

    assert response.status_code == 409, response.text
    assert _durable_snapshot() == before
    assert _pull_reads(case) == []


@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True)
def test_adoption_remains_committed_when_the_provider_read_is_unavailable(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    _orphaned_precheck(case)
    version = _rows(
        "SELECT version FROM curie.work_items WHERE id = :id", {"id": case["work_item_id"]}
    )[0]["version"]
    case["provider_status"]["value"] = 500

    response = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)

    assert response.status_code == 503, response.text
    assert _rows(
        "SELECT publication_lineage_id, version FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"]},
    ) == [{"publication_lineage_id": uuid.UUID(case["lineage_id"]), "version": version + 1}]
    before_retry = _durable_snapshot()
    case["provider_status"]["value"] = 200
    assert _mint(case)["lineage_id"] == case["lineage_id"]
    assert _durable_snapshot() == before_retry


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


@pytest.mark.parametrize(
    "precheck_case",
    [
        pytest.param(None, id="public"),
        pytest.param(
            {"api_url": ENTERPRISE_API_URL, "html_base": ENTERPRISE_HTML_BASE},
            id="enterprise",
        ),
    ],
    indirect=True,
)
@pytest.mark.parametrize("case_kind", ["changed", "unchanged", "external_edit"])
def test_metadata_only_admission_rechecks_current_pull_request(
    precheck_case: dict[str, Any], case_kind: str
) -> None:
    case = precheck_case
    context = _mint(case)
    _execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(case["publication_id"])},
    )
    payload = _publication_payload(
        case["deployment"]["id"],
        patch=b"",
        base_sha=FIRST_REVISION_SHA,
        conversation_id=context["conversation_id"],
    )
    payload.update(
        changed_paths=[],
        reply_conversation_id=case["reply_conversation_id"],
        title=OBSERVED_TITLE,
        body=(
            OBSERVED_BODY
            if case_kind == "unchanged"
            else "Corrected body for the pull request check.\n"
        ),
        work_item_request_id=str(case["request_id"]),
        work_item_runtime_epoch=7,
        observed_title=context["observed_title"],
        observed_body_sha256=context["observed_body_sha256"],
        observed_lineage_id=context["lineage_id"],
        observed_lineage_version=context["lineage_version"],
    )
    if case_kind == "external_edit":
        case["truth"]["body"] = "A human edited this body after the model saw it.\n"
    response = case["client"].post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )
    if case_kind == "changed":
        assert response.status_code == 201, response.text
        assert response.json()["changed_paths"] == []
    else:
        assert response.status_code == 409, response.text
        assert (
            _rows(
                "SELECT id FROM curie.publications WHERE id <> :id",
                {"id": uuid.UUID(case["publication_id"])},
            )
            == []
        )


@pytest.mark.parametrize(
    "precheck_case",
    [{"api_url": ENTERPRISE_API_URL, "html_base": ENTERPRISE_HTML_BASE}],
    indirect=True,
)
def test_enterprise_publication_precheck_preserves_forge_identity(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    before = _durable_snapshot()
    context = _mint(case)

    unchanged = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)
    changed = _compare(case, context, title="Corrected title", body="Corrected body.\n")

    assert unchanged.status_code == 200, unchanged.text
    assert unchanged.json() == {"result": "unchanged"}
    assert changed.status_code == 200, changed.text
    assert changed.json() == {"result": "metadata_changed"}
    assert len(_pull_reads(case)) == 3
    assert all(
        str(request.url) == f"{ENTERPRISE_API_URL}/repos/{REPO}/pulls/{PR_NUMBER}"
        for request in _pull_reads(case)
    )
    assert _rows(
        "SELECT pr_url FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": uuid.UUID(case["lineage_id"])},
    ) == [{"pr_url": f"{ENTERPRISE_HTML_BASE}/{REPO}/pull/{PR_NUMBER}"}]
    assert _durable_snapshot() == before


@pytest.mark.parametrize(
    "precheck_case",
    [{"api_url": ENTERPRISE_API_URL, "html_base": ENTERPRISE_HTML_BASE}],
    indirect=True,
)
@pytest.mark.parametrize("stage", ["stored_lineage", "provider", "comparison", "admission"])
def test_enterprise_publication_precheck_refuses_public_github_identity(
    precheck_case: dict[str, Any], stage: str
) -> None:
    case = precheck_case
    context = _mint(case) if stage in {"comparison", "admission"} else None
    if stage == "stored_lineage":
        _execute(
            "UPDATE curie.thread_publication_lineages SET pr_url = :url WHERE id = :id",
            {"id": uuid.UUID(case["lineage_id"]), "url": PR_URL},
        )
    else:
        case["truth"]["html_url"] = PR_URL
    before = _durable_snapshot()
    case["calls"].clear()

    if stage == "comparison":
        assert context is not None
        refused = _compare(case, context, title="Corrected title", body="Corrected body.\n")
    elif stage == "admission":
        assert context is not None
        payload = _publication_payload(
            case["deployment"]["id"],
            patch=b"",
            base_sha=FIRST_REVISION_SHA,
            conversation_id=context["conversation_id"],
        )
        payload.update(
            changed_paths=[],
            reply_conversation_id=case["reply_conversation_id"],
            title=OBSERVED_TITLE,
            body="Corrected body.\n",
            work_item_request_id=str(case["request_id"]),
            work_item_runtime_epoch=7,
            observed_title=context["observed_title"],
            observed_body_sha256=context["observed_body_sha256"],
            observed_lineage_id=context["lineage_id"],
            observed_lineage_version=context["lineage_version"],
        )
        refused = case["client"].post(
            "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
        )
    else:
        refused = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)

    assert refused.status_code == 503, refused.text
    assert _durable_snapshot() == before
    assert len(_pull_reads(case)) == (0 if stage == "stored_lineage" else 1)


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


@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True, ids=["unlinked"])
def test_mint_names_an_existing_conversation_pr_without_a_work_item_link(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    assert _rows(
        "SELECT publication_lineage_id FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"]},
    ) == [{"publication_lineage_id": None}]
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "pull_request_not_adopted"
    assert "capability" not in response.text
    assert _pull_reads(case) == []
    assert _durable_snapshot() == before


@pytest.mark.parametrize("authority_loss", ["epoch", "lease", "status"])
@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True, ids=["unlinked"])
def test_unlinked_existing_pr_does_not_hide_a_generic_execution_refusal(
    precheck_case: dict[str, Any], authority_loss: str
) -> None:
    case = precheck_case
    assert _rows(
        "SELECT publication_lineage_id FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"]},
    ) == [{"publication_lineage_id": None}]
    request = _mint_body(case)
    if authority_loss == "epoch":
        request["runtime_epoch"] += 1
    elif authority_loss == "lease":
        _execute(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
            {"id": case["request_id"]},
        )
    else:
        _execute(
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'owner_lost' WHERE id = :id",
            {"id": case["request_id"]},
        )
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=request, headers=WORKER_HEADERS)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "invalid_context"
    assert _pull_reads(case) == []
    assert _durable_snapshot() == before


@pytest.mark.parametrize("other_scope", ["conversation", "repository"])
@pytest.mark.parametrize("precheck_case", [{"unlinked_work_item": True}], indirect=True, ids=["unlinked"])
def test_an_unlinked_running_request_ignores_a_pr_outside_its_scope(
    precheck_case: dict[str, Any], other_scope: str
) -> None:
    case = precheck_case
    item = _rows(
        "SELECT publication_lineage_id, conversation_id, repo_full_name "
        "FROM curie.work_items WHERE id = :id",
        {"id": case["work_item_id"]},
    )[0]
    assert item["publication_lineage_id"] is None
    if other_scope == "conversation":
        assert item["conversation_id"] != case["lineage"]["conversation_id"]
    else:
        assert item["repo_full_name"] != REPO
    before = _durable_snapshot()

    response = case["client"].post(MINT_URL, json=_mint_body(case), headers=WORKER_HEADERS)

    assert response.status_code == 204, response.text
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


def test_null_github_body_is_observed_as_empty_bytes(precheck_case: dict[str, Any]) -> None:
    case = precheck_case
    case["truth"]["body"] = None
    context = _mint(case)
    assert context["observed_body_sha256"] == _digest("")

    response = _compare(case, context, title=OBSERVED_TITLE, body="A useful description")

    assert response.status_code == 200, response.text
    assert response.json() == {"result": "metadata_changed"}
    assert len(_pull_reads(case)) == 2


def test_comparison_cannot_select_a_different_repository_or_pull_request(
    precheck_case: dict[str, Any],
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
    precheck_case: dict[str, Any],
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
        accepted = _compare(case, credential, title=OBSERVED_TITLE, body=OBSERVED_BODY)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json() == {"result": "unchanged"}
    assert len(_pull_reads(case)) == 22
    exhausted = _compare(case, second_context, title=OBSERVED_TITLE, body=OBSERVED_BODY)
    assert exhausted.status_code == 429, exhausted.text
    assert len(_pull_reads(case)) == 22
    assert _durable_snapshot() == before


def test_provider_redirect_does_not_forward_the_github_credential(
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    context = _mint(case)
    case["provider_status"]["value"] = 302

    response = _compare(case, context, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert response.status_code == 503, response.text
    assert len(_pull_reads(case)) == 2
    assert all(request.url.host != "attacker.example.com" for request in case["calls"])


def test_inflight_publication_makes_comparison_unavailable(precheck_case: dict[str, Any]) -> None:
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
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 WHERE id = :id",
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
            "UPDATE curie.thread_publication_lineages SET version = version + 1 WHERE id = :id",
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
    precheck_case: dict[str, Any],
) -> None:
    case = precheck_case
    context = _mint(case)
    forged_event = dict(context)
    forged_event["observed_title"] = "A replacement model title"

    response = _compare(case, forged_event, title=OBSERVED_TITLE, body=OBSERVED_BODY)

    assert 400 <= response.status_code < 500, response.text
    assert response.json().get("result") not in {"unchanged", "metadata_changed"}
    assert len(_pull_reads(case)) == 1


def test_capability_is_accepted_only_at_the_comparison_route(precheck_case: dict[str, Any]) -> None:
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
    assert (
        client.get(
            f"/agents/{case['deployment']['agent_id']}/state", headers={"X-API-Key": token}
        ).status_code
        == 401
    )
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


def test_invalid_capability_never_reaches_github(precheck_case: dict[str, Any]) -> None:
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
    precheck_case: dict[str, Any],
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
