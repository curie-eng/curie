"""The factory issue read served to the sandbox (ADR 0187, #3748).

GitHub REST shapes come from
https://docs.github.com/en/rest/issues/issues#get-an-issue,
https://docs.github.com/en/rest/issues/comments#list-issue-comments and
https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app.
Only the external GitHub responses are replaced. Postgres is real.
"""

from __future__ import annotations

import asyncio
import base64
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
from curie_api.routers import work_item_issue
from fastapi.testclient import TestClient

from apps.api.tests.test_publications import REPO, WORKER_HEADERS, _execute, _rows
from apps.api.tests.test_publications import publication_stack as publication_stack

MINT_URL = "/v1/internal/work-items/issue-read/context"
READ_URL = "/work-items/issue-read"
CAPABILITY_HEADER = "X-Curie-Issue-Read"
REPOSITORY_ID = 9001
INSTALLATION_ID = 41
ISSUE_NUMBER = 3215
TITLE = "Add a --dry-run flag λ"
BODY = "Acceptance criteria:\n\n1. It prints the plan.  \n"
COMMENTS = [
    {"user": {"login": "maintainer"}, "created_at": "2026-10-01T00:00:00Z", "body": "Also docs."},
    {"user": {"login": "octocat"}, "created_at": "2026-10-01T01:00:00Z", "body": None},
]


def _resign(token: str, **changes: Any) -> str:
    """A validly signed capability with changed claims, as a forger with the key."""

    encoded = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    claims.update(changes)
    payload = (
        base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )
    signed = f"wir.{payload}"
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(get_settings().api_key.encode(), signed.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return f"{signed}.{signature}"


def _insert_execution(
    agent_id: uuid.UUID,
    *,
    repo: str = REPO,
    repository_id: int = REPOSITORY_ID,
    issue: int = ISSUE_NUMBER,
    status: str = "running",
    started_ago: timedelta = timedelta(minutes=1),
    deadline_in: timedelta = timedelta(hours=2),
) -> tuple[uuid.UUID, uuid.UUID]:
    work_item_id, request_id = uuid.uuid4(), uuid.uuid4()
    now = datetime.now(UTC)
    _execute(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, agent_id, conversation_id) "
        "VALUES (:id, 'github', 'github.com', :repository, :issue, 'github', "
        "'github.com', :repository, :repo, :installation, :agent, :conversation)",
        {
            "id": work_item_id,
            "repository": str(repository_id),
            "issue": str(issue),
            "installation": INSTALLATION_ID,
            "agent": agent_id,
            "repo": repo,
            "conversation": f"issue-read-{uuid.uuid4().hex}",
        },
    )
    started = status != "waiting"
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, execution_attempts, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at) "
        "VALUES (:id, :item, 1, :status, :wait, :started, :deadline, "
        ":attempts, 'fixture-runner', 7, :lease)",
        {
            "id": request_id,
            "item": work_item_id,
            "status": status,
            "wait": now + timedelta(minutes=5),
            "started": now - started_ago if started else None,
            "deadline": now + deadline_in if started else None,
            "attempts": 1 if started else 0,
            "lease": now + timedelta(minutes=5),
        },
    )
    return work_item_id, request_id


@pytest.fixture
def issue_case(
    clean_db: None,
    publication_stack: tuple[TestClient, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[dict[str, Any]]:
    client, _ = publication_stack
    agent_id = uuid.uuid4()
    _execute(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"factory-{agent_id.hex[:8]}"},
    )
    work_item_id, request_id = _insert_execution(agent_id)
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
    calls: list[httpx.Request] = []
    minted: list[str] = []
    issue_path = f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}"

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": INSTALLATION_ID})
        if request.url.path.endswith("/access_tokens"):
            # A distinct short lived token each mint, like GitHub's.
            token = f"fixture-issue-token-{len(minted)}"
            minted.append(token)
            expires = datetime.now(UTC) + timedelta(hours=1)
            return httpx.Response(
                201, json={"token": token, "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}
            )
        assert request.method == "GET"
        assert request.headers["authorization"] == f"Bearer {minted[-1]}"
        if request.url.path == issue_path:
            return httpx.Response(
                200,
                json={
                    "number": ISSUE_NUMBER,
                    "title": TITLE,
                    "body": BODY,
                    "state": "open",
                    "user": {"login": "reporter"},
                },
            )
        if request.url.path == f"{issue_path}/comments":
            page = int(request.url.params["page"])
            batch = [COMMENTS[page - 1]] if page <= len(COMMENTS) else []
            headers = {"link": f'<{request.url}>; rel="next"'} if page < len(COMMENTS) else {}
            return httpx.Response(200, json=batch, headers=headers)
        return httpx.Response(404, json={"message": "Not Found"})

    real_client = httpx.Client
    monkeypatch.setattr(
        "curie_api.github_app.httpx.Client",
        lambda *args, **kwargs: real_client(transport=httpx.MockTransport(github)),
    )
    injected = httpx.AsyncClient(transport=httpx.MockTransport(github))
    original = client.app.state.http_client
    client.app.state.http_client = injected
    try:
        yield {
            "client": client,
            "agent_id": agent_id,
            "work_item_id": work_item_id,
            "request_id": request_id,
            "calls": calls,
            "minted": minted,
        }
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())
        _RESOLVERS.clear()
        get_settings.cache_clear()


def _replace_execution(case: dict[str, Any], **shape: Any) -> None:
    """Rebuild the case's WorkItem and request with another durable shape.

    Execution deadlines are write once, so a different deadline is a new row.
    """

    _execute("DELETE FROM curie.work_items WHERE id = :id", {"id": case["work_item_id"]})
    case["work_item_id"], case["request_id"] = _insert_execution(case["agent_id"], **shape)


def _mint(case: dict[str, Any], request_id: uuid.UUID | None = None) -> dict[str, Any]:
    response = case["client"].post(
        MINT_URL,
        json={"execution_request_id": str(request_id or case["request_id"])},
        headers=WORKER_HEADERS,
    )
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    return response.json()


def _read(
    case: dict[str, Any], capability: str, *, repo: str = REPO, issue: int = ISSUE_NUMBER
) -> httpx.Response:
    return case["client"].post(
        READ_URL,
        headers={CAPABILITY_HEADER: capability},
        json={"repo_full_name": repo, "issue_number": issue},
    )


def _github_reads(case: dict[str, Any]) -> list[httpx.Request]:
    return [r for r in case["calls"] if r.url.path.startswith("/repositories/")]


def _durable_snapshot() -> tuple[list[dict[str, Any]], ...]:
    return (
        _rows("SELECT * FROM curie.work_items ORDER BY id"),
        _rows("SELECT * FROM curie.execution_requests ORDER BY id"),
    )


def test_valid_read_returns_the_issue_and_comments_verbatim_and_stores_nothing(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    context = _mint(case)
    assert context["work_item_id"] == str(case["work_item_id"])
    assert context["execution_request_id"] == str(case["request_id"])
    assert (context["repo_full_name"], context["issue_number"]) == (REPO, ISSUE_NUMBER)
    assert context["capability"].startswith("wir.")
    before = _durable_snapshot()

    response = _read(case, context["capability"], repo=REPO.upper())
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "repo_full_name": REPO,
        "issue_number": ISSUE_NUMBER,
        "title": TITLE,
        "body": BODY,
        "state": "open",
        "author": "reporter",
        "comments": [
            {"author": "maintainer", "created_at": "2026-10-01T00:00:00Z", "body": "Also docs."},
            {"author": "octocat", "created_at": "2026-10-01T01:00:00Z", "body": ""},
        ],
        "comments_truncated": False,
    }
    # Addressed by repository id, with the App installation token.
    assert [r.url.path for r in _github_reads(case)] == [
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}",
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}/comments",
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}/comments",
    ]
    assert _durable_snapshot() == before


@pytest.mark.parametrize(
    ("repo", "issue"),
    [(REPO, ISSUE_NUMBER + 1), ("acme-corp/other", ISSUE_NUMBER), ("evil/acme-bot", 1)],
)
def test_wrong_issue_or_repository_is_refused_before_github(
    issue_case: dict[str, Any], repo: str, issue: int
) -> None:
    case = issue_case
    context = _mint(case)
    response = _read(case, context["capability"], repo=repo, issue=issue)
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "issue_not_granted"
    assert _github_reads(case) == []


def test_capability_of_another_execution_cannot_read_this_issue(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    _, other_request = _insert_execution(
        case["agent_id"], repo="acme-corp/other", repository_id=REPOSITORY_ID + 1, issue=5
    )
    foreign = _mint(case, other_request)["capability"]
    # It names its own issue only.
    response = _read(case, foreign)
    assert response.status_code == 403
    assert _github_reads(case) == []


@pytest.mark.parametrize(
    "forge",
    ["expired", "foreign_key", "garbage", "absent", "other_scope_prefix", "forged_issue"],
)
def test_expired_forged_or_foreign_capability_never_reaches_github(
    issue_case: dict[str, Any], forge: str
) -> None:
    case = issue_case
    capability = _mint(case)["capability"]
    if forge == "expired":
        capability = _resign(capability, iat=int(time.time()) - 7200, exp=int(time.time()) - 1)
    elif forge == "foreign_key":
        prefix, payload, _ = capability.split(".")
        capability = f"{prefix}.{payload}.{base64.urlsafe_b64encode(b'x' * 32).decode()}"
    elif forge == "garbage":
        capability = "wir.not-base64.sig"
    elif forge == "other_scope_prefix":
        capability = "ppc." + capability.split(".", 1)[1]
    elif forge == "forged_issue":
        # Signed with the key, but the durable WorkItem names another issue.
        capability = _resign(capability, issue_number=ISSUE_NUMBER + 9)
    headers = {} if forge == "absent" else {CAPABILITY_HEADER: capability}
    issue = ISSUE_NUMBER + 9 if forge == "forged_issue" else ISSUE_NUMBER
    response = case["client"].post(
        READ_URL, headers=headers, json={"repo_full_name": REPO, "issue_number": issue}
    )
    expected = 409 if forge == "forged_issue" else 401
    assert response.status_code == expected, response.text
    assert _github_reads(case) == []


def test_platform_key_does_not_authorize_the_read(
    issue_case: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    case = issue_case
    response = case["client"].post(
        READ_URL, headers=auth_headers, json={"repo_full_name": REPO, "issue_number": ISSUE_NUMBER}
    )
    assert response.status_code == 401
    assert _github_reads(case) == []


@pytest.mark.parametrize("ended", ["cancelled_item", "deadline"])
def test_an_ended_execution_cannot_read(issue_case: dict[str, Any], ended: str) -> None:
    case = issue_case
    if ended == "deadline":
        _replace_execution(case, started_ago=timedelta(hours=3), deadline_in=timedelta(seconds=-1))
        capability = _resign(_mint_capability_ignoring_deadline(case), exp=int(time.time()) + 3600)
    else:
        capability = _mint(case)["capability"]
    if ended == "cancelled_item":
        _execute(
            "UPDATE curie.work_items SET cancelled_at = now() WHERE id = :id",
            {"id": case["work_item_id"]},
        )
    response = _read(case, capability)
    assert response.status_code == 409, response.text
    assert _github_reads(case) == []


def _mint_capability_ignoring_deadline(case: dict[str, Any]) -> str:
    """A capability for the case's request as the mint would have issued it earlier."""

    template = _mint_for_other_issue(case)
    return _resign(
        template,
        work_item_id=str(case["work_item_id"]),
        execution_request_id=str(case["request_id"]),
        repo_full_name=REPO,
        github_repository_id=REPOSITORY_ID,
        issue_number=ISSUE_NUMBER,
    )


def _mint_for_other_issue(case: dict[str, Any]) -> str:
    _, other = _insert_execution(case["agent_id"], repository_id=REPOSITORY_ID + 2, issue=11)
    return str(_mint(case, other)["capability"])


def test_a_run_longer_than_one_hour_still_reads_with_a_fresh_app_token(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    # The execution started two hours ago and runs to its three hour deadline.
    # Its sandbox booted then, and the worker minted the capability at boot.
    _replace_execution(case, started_ago=timedelta(hours=2), deadline_in=timedelta(minutes=50))
    boot = time.time() - 2 * 3600
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("curie_api.routers.work_item_issue.time.time", lambda: boot)
        capability = _mint(case)["capability"]
    assert case["minted"] == []
    # Two hours after boot, past the one hour lifetime of any App installation
    # token that existed then, the boot capability still reads, and the read
    # authenticates with an installation token minted for this read.
    response = _read(case, capability)
    assert response.status_code == 200, response.text
    assert response.json()["title"] == TITLE
    assert len(case["minted"]) == 1
    assert all(
        r.headers["authorization"] == f"Bearer {case['minted'][0]}" for r in _github_reads(case)
    )


def test_mint_requires_the_worker_credential_and_a_live_execution(
    issue_case: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    case = issue_case
    body = {"execution_request_id": str(case["request_id"])}
    assert case["client"].post(MINT_URL, json=body).status_code == 401
    assert case["client"].post(MINT_URL, json=body, headers=auth_headers).status_code == 401
    unknown = {"execution_request_id": str(uuid.uuid4())}
    assert case["client"].post(MINT_URL, json=unknown, headers=WORKER_HEADERS).status_code == 409
    # Minting at sandbox boot precedes the start grant.
    _, waiting = _insert_execution(
        case["agent_id"], repository_id=REPOSITORY_ID + 3, status="waiting"
    )
    assert _mint(case, waiting)["issue_number"] == ISSUE_NUMBER
    _execute(
        "UPDATE curie.work_items SET cancelled_at = now() WHERE id = :id",
        {"id": case["work_item_id"]},
    )
    assert case["client"].post(MINT_URL, json=body, headers=WORKER_HEADERS).status_code == 409


def test_a_runtime_whose_heartbeat_lease_expired_cannot_read(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    capability = _mint(case)["capability"]
    # The request stays running until the reconciler notices the lost owner.
    _execute(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = now() - interval '1 second' WHERE id = :id",
        {"id": case["request_id"]},
    )
    response = _read(case, capability)
    assert response.status_code == 409, response.text
    assert _github_reads(case) == []


def test_cancellation_during_the_github_read_returns_no_issue_content(
    issue_case: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    case = issue_case
    capability = _mint(case)["capability"]
    original = work_item_issue.read_issue

    async def read_then_cancel(*args: Any, **kwargs: Any) -> Any:
        content = await original(*args, **kwargs)
        await asyncio.to_thread(
            _execute,
            "UPDATE curie.work_items SET cancelled_at = now() WHERE id = :id",
            {"id": case["work_item_id"]},
        )
        return content

    monkeypatch.setattr(work_item_issue, "read_issue", read_then_cancel)
    response = _read(case, capability)
    assert response.status_code == 409, response.text
    assert TITLE not in response.text
    assert _github_reads(case) != []


def test_one_execution_has_a_bounded_read_budget(issue_case: dict[str, Any]) -> None:
    case = issue_case
    capability = _mint(case)["capability"]
    for _ in range(20):
        assert _read(case, capability).status_code == 200
    reads = len(_github_reads(case))
    refused = _read(case, capability)
    assert refused.status_code == 429, refused.text
    assert refused.json()["detail"]["code"] == "rate_limited"
    assert len(_github_reads(case)) == reads
