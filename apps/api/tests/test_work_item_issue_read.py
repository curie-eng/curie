"""The factory issue read served to the sandbox (ADR 0187, #3748, #4358 plan D8).

The capability names one execution and its WorkItem's tracker issue, for every
tracker kind; the read takes no body and answers the ticket as markdown through
`Tracker.read_ticket`, plus the execution's deadline. A GitHub WorkItem is read
with a token minted only for its recorded installation; a Jira WorkItem through
the Jira fake (`forge_fakes/jira.py`) over ASGI.

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
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.github_app import _RESOLVERS
from curie_api.routers import work_item_issue
from fastapi.testclient import TestClient
from forge_fakes import jira as jira_fake
from forge_fakes.github import forges_document, forges_env_json

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
# The ticket the GitHub tracker composes from the issue and its comments.
TICKET = (
    f"# {TITLE}\n\n{BODY}\n\n"
    "## Comment by maintainer at 2026-10-01T00:00:00Z\n\nAlso docs.\n\n"
    "## Comment by octocat at 2026-10-01T01:00:00Z\n\n\n"
)
ISSUE_URL = f"https://github.com/{REPO}/issues/{ISSUE_NUMBER}"
JIRA_SUMMARY = "Widget parser drops the last field"
JIRA_DESCRIPTION = "Parsing a three-field line yields two fields."


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
def factory_code_host(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The GitHub code host entry the issue read mints with (#4397 S14).

    Set before ``publication_stack`` boots the app, so the forge registry built
    at startup carries the App. The install's own settings keep only the
    publication PAT: a read that minted from settings would send that PAT and
    never reach the App token endpoints this case answers.
    """

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    document = forges_document(app_id="51", private_key=pem)
    # The Jira fixture site, bound to the same GitHub repository (#4358).
    jira = jira_fake.jira_document()
    document["trackers"].extend(jira["trackers"])
    document["bindings"].extend(jira["bindings"])
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path))
    get_settings.cache_clear()


@pytest.fixture
def issue_case(
    clean_db: None,
    factory_code_host: None,
    publication_stack: tuple[TestClient, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[dict[str, Any]]:
    client, _ = publication_stack
    assert get_settings().forges.code_hosts, "the forge config must exist before the app boots"
    agent_id = uuid.uuid4()
    _execute(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"factory-{agent_id.hex[:8]}"},
    )
    work_item_id, request_id = _insert_execution(agent_id)
    calls: list[httpx.Request] = []
    minted: list[str] = []
    issue_path = f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}"
    installation = {"id": INSTALLATION_ID}
    jira = jira_fake.JiraFake()

    def github(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": installation["id"]})
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
    injected = httpx.AsyncClient(
        transport=_ByHost(
            {jira_fake.SITE_HOST: httpx.ASGITransport(app=jira.app)}, httpx.MockTransport(github)
        )
    )
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
            "installation": installation,
            "jira": jira,
        }
    finally:
        client.app.state.http_client = original
        asyncio.run(injected.aclose())
        _RESOLVERS.clear()
        get_settings.cache_clear()


class _ByHost(httpx.AsyncBaseTransport):
    """Jira's site to the Jira fake, every other host to the GitHub stand-in."""

    def __init__(
        self, routes: dict[str, httpx.AsyncBaseTransport], default: httpx.AsyncBaseTransport
    ) -> None:
        self._routes = routes
        self._default = default

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        transport = self._routes.get(request.url.host, self._default)
        return await transport.handle_async_request(request)


def _insert_jira_execution(case: dict[str, Any]) -> tuple[uuid.UUID, uuid.UUID, str]:
    """A running execution whose WorkItem is a Jira issue on the fixture site."""

    fake: jira_fake.JiraFake = case["jira"]
    issue = fake.seed_issue(JIRA_SUMMARY, JIRA_DESCRIPTION)
    fake.add_comment(issue.id, fake.reporter, "Also handle a trailing comma.")
    work_item_id, request_id = uuid.uuid4(), uuid.uuid4()
    now = datetime.now(UTC)
    _execute(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, agent_id, conversation_id) "
        "VALUES (:id, 'jira_cloud', :site, :cloud, :issue, :key, 'github', 'github.com', "
        ":repository, :repo, :installation, :agent, :conversation)",
        {
            "id": work_item_id,
            "site": jira_fake.SITE_HOST,
            "cloud": jira_fake.CLOUD_ID,
            "issue": str(issue.id),
            "key": issue.key,
            "repository": jira_fake.GITHUB_REPO_ID,
            "repo": jira_fake.GITHUB_REPO,
            "installation": INSTALLATION_ID,
            "agent": case["agent_id"],
            "conversation": f"issue-read-{uuid.uuid4().hex}",
        },
    )
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, execution_attempts, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, 1, "
        "'fixture-runner', 7, :lease)",
        {
            "id": request_id,
            "item": work_item_id,
            "wait": now + timedelta(minutes=5),
            "started": now - timedelta(minutes=1),
            "deadline": now + timedelta(hours=2),
            "lease": now + timedelta(minutes=5),
        },
    )
    return work_item_id, request_id, issue.key


def _claims(capability: str) -> dict[str, Any]:
    encoded = capability.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))


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


def _read(case: dict[str, Any], capability: str) -> httpx.Response:
    # The capability names the issue; the route takes no body.
    return case["client"].post(READ_URL, headers={CAPABILITY_HEADER: capability})


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
    assert context["issue"] == f"{REPO}#{ISSUE_NUMBER}"
    assert "repo_full_name" not in context and "issue_number" not in context
    assert context["capability"].startswith("wir.")
    claims = _claims(context["capability"])
    assert {key: claims[key] for key in ("tracker_kind", "tracker_host")} == {
        "tracker_kind": "github",
        "tracker_host": "github.com",
    }
    assert (claims["tracker_scope_id"], claims["tracker_issue_id"]) == (
        str(REPOSITORY_ID),
        str(ISSUE_NUMBER),
    )
    assert not {"repo_full_name", "github_repository_id", "issue_number"} & set(claims)
    before = _durable_snapshot()
    execution = _rows(
        "SELECT started_at, execution_deadline FROM curie.execution_requests WHERE id = :id",
        {"id": case["request_id"]},
    )[0]

    response = _read(case, context["capability"])
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "issue": f"{REPO}#{ISSUE_NUMBER}",
        "url": ISSUE_URL,
        "ticket": TICKET,
        "execution_deadline": execution["execution_deadline"].isoformat().replace("+00:00", "Z"),
        "execution_seconds": int(
            (execution["execution_deadline"] - execution["started_at"]).total_seconds()
        ),
    }
    # Addressed by repository id, with the App installation token.
    assert [r.url.path for r in _github_reads(case)] == [
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}",
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}/comments",
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}/comments",
    ]
    assert _durable_snapshot() == before


@pytest.mark.parametrize("execution_seconds", [3600, 10800])
def test_issue_read_reports_the_stored_execution_deadline_and_configured_duration(
    issue_case: dict[str, Any], execution_seconds: int
) -> None:
    case = issue_case
    _execute(
        "UPDATE curie.agents SET execution_deadline_seconds = :seconds WHERE id = :id",
        {"id": case["agent_id"], "seconds": execution_seconds},
    )
    _replace_execution(case, status="waiting")
    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": case["request_id"]},
    )[0]["dispatch_generation"]
    request_url = f"/v1/internal/work-items/requests/{case['request_id']}"
    acquire = case["client"].post(
        f"{request_url}/acquire",
        headers=WORKER_HEADERS,
        json={"owner": "fixture-runner", "generation": generation},
    )
    assert acquire.status_code == 200, acquire.text
    started = case["client"].post(
        f"{request_url}/start",
        headers=WORKER_HEADERS,
        json={
            "owner": "fixture-runner",
            "generation": generation,
            "claim_name": "fixture-claim",
            "sandbox_name": "fixture-sandbox",
        },
    )
    assert started.status_code == 200, started.text
    execution = _rows(
        "SELECT started_at, execution_deadline FROM curie.execution_requests WHERE id = :id",
        {"id": case["request_id"]},
    )[0]
    response = _read(case, _mint(case)["capability"])
    assert response.status_code == 200, response.text
    result = response.json()
    assert datetime.fromisoformat(result["execution_deadline"]) == execution["execution_deadline"]
    assert (
        datetime.fromisoformat(started.json()["execution_deadline"])
        == execution["execution_deadline"]
    )
    assert result["execution_seconds"] == execution_seconds
    assert (execution["execution_deadline"] - execution["started_at"]).total_seconds() == (
        execution_seconds
    )


@pytest.mark.parametrize(
    "claim",
    [
        {"tracker_issue_id": str(ISSUE_NUMBER + 1)},
        {"tracker_scope_id": str(REPOSITORY_ID + 1)},
        {"tracker_kind": "jira_cloud"},
        {"tracker_host": "ghe.example"},
    ],
)
def test_a_capability_naming_another_issue_is_refused_and_the_real_one_still_reads(
    issue_case: dict[str, Any], claim: dict[str, str]
) -> None:
    case = issue_case
    capability = _mint(case)["capability"]
    # Signed with the key, but naming an issue the durable WorkItem does not hold.
    refused = _read(case, _resign(capability, **claim))
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "invalid_context"
    assert _github_reads(case) == []
    # Liveness: the execution's own capability still reads its own issue.
    allowed = _read(case, capability)
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["ticket"] == TICKET


def test_a_body_naming_another_issue_is_ignored(issue_case: dict[str, Any]) -> None:
    # An old tool still posting owner, repo and number cannot widen the grant.
    case = issue_case
    capability = _mint(case)["capability"]
    response = case["client"].post(
        READ_URL,
        headers={CAPABILITY_HEADER: capability},
        json={"repo_full_name": "acme-corp/other", "issue_number": ISSUE_NUMBER + 1},
    )
    assert response.status_code == 200, response.text
    assert response.json()["url"] == ISSUE_URL
    assert {r.url.path for r in _github_reads(case)} == {
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}",
        f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}/comments",
    }


def test_issue_read_refuses_a_work_item_whose_installation_no_longer_matches(
    issue_case: dict[str, Any],
) -> None:
    # The App was reinstalled: GitHub now names another installation on the
    # repository than the one the WorkItem recorded. No token is minted for it
    # and nothing is read (#4358 plan D14 item 2a).
    case = issue_case
    capability = _mint(case)["capability"]
    case["installation"]["id"] = INSTALLATION_ID + 1
    response = _read(case, capability)
    assert response.status_code == 503, response.text
    assert response.json()["detail"] == {
        "code": "issue_unavailable",
        "message": "The tracker could not answer the issue read; retry shortly",
    }
    assert _github_reads(case) == []
    assert case["minted"] == []
    assert str(INSTALLATION_ID + 1) not in response.text


def test_a_jira_work_item_reads_its_ticket_through_the_jira_tracker(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    work_item_id, request_id, key = _insert_jira_execution(case)
    context = _mint(case, request_id)
    assert context["work_item_id"] == str(work_item_id)
    assert context["issue"] == key
    claims = _claims(context["capability"])
    assert (claims["tracker_kind"], claims["tracker_host"], claims["tracker_scope_id"]) == (
        "jira_cloud",
        jira_fake.SITE_HOST,
        jira_fake.CLOUD_ID,
    )
    response = _read(case, context["capability"])
    assert response.status_code == 200, response.text
    result = response.json()
    assert set(result) == {"issue", "url", "ticket", "execution_deadline", "execution_seconds"}
    assert result["issue"] == key
    assert result["url"] == f"https://{jira_fake.SITE_HOST}/browse/{key}"
    assert result["ticket"].startswith(f"# {JIRA_SUMMARY}\n\n{JIRA_DESCRIPTION}")
    assert "Also handle a trailing comma." in result["ticket"]
    # Read with the Jira credential only: no GitHub request and no App mint.
    assert _github_reads(case) == [] and case["minted"] == []
    jira: jira_fake.JiraFake = case["jira"]
    assert jira.requests and all(
        r.authorization == jira_fake.basic(jira_fake.BOT_EMAIL, jira_fake.TOKEN)
        for r in jira.requests
        if r.kind != "tenant_info"
    )
    assert jira.writes == 0
    assert jira_fake.TOKEN not in response.text


def test_capability_of_another_execution_cannot_read_this_issue(
    issue_case: dict[str, Any],
) -> None:
    case = issue_case
    _, other_request = _insert_execution(
        case["agent_id"], repo="acme-corp/other", repository_id=REPOSITORY_ID + 1, issue=5
    )
    foreign = _mint(case, other_request)["capability"]
    # It names its own issue only: the read reaches that issue, never this one.
    _read(case, foreign)
    this_issue = f"/repositories/{REPOSITORY_ID}/issues/{ISSUE_NUMBER}"
    assert all(not r.url.path.startswith(this_issue) for r in _github_reads(case))
    assert {r.url.path for r in _github_reads(case)} <= {
        f"/repositories/{REPOSITORY_ID + 1}/issues/5",
        f"/repositories/{REPOSITORY_ID + 1}/issues/5/comments",
    }


@pytest.mark.parametrize(
    "forge",
    [
        "expired",
        "foreign_key",
        "garbage",
        "absent",
        "other_scope_prefix",
        "forged_issue",
        "old_claims",
    ],
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
        capability = _resign(capability, tracker_issue_id=str(ISSUE_NUMBER + 9))
    elif forge == "old_claims":
        # A capability in the removed GitHub-shaped claim set no longer verifies.
        capability = _resign(capability, repo_full_name=REPO, issue_number=ISSUE_NUMBER)
    headers = {} if forge == "absent" else {CAPABILITY_HEADER: capability}
    response = case["client"].post(READ_URL, headers=headers)
    expected = 409 if forge == "forged_issue" else 401
    assert response.status_code == expected, response.text
    assert _github_reads(case) == []


def test_platform_key_does_not_authorize_the_read(
    issue_case: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    case = issue_case
    response = case["client"].post(READ_URL, headers=auth_headers)
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
        tracker_kind="github",
        tracker_host="github.com",
        tracker_scope_id=str(REPOSITORY_ID),
        tracker_issue_id=str(ISSUE_NUMBER),
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
    assert response.json()["ticket"] == TICKET
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
    assert _mint(case, waiting)["issue"] == f"{REPO}#{ISSUE_NUMBER}"
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


def test_a_budget_change_during_the_github_read_keeps_the_granted_timing(
    issue_case: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    case = issue_case
    _execute(
        "UPDATE curie.agents SET execution_deadline_seconds = 7200 WHERE id = :id",
        {"id": case["agent_id"]},
    )
    _replace_execution(case, deadline_in=timedelta(minutes=59))
    capability = _mint(case)["capability"]
    execution = _rows(
        "SELECT started_at, execution_deadline FROM curie.execution_requests WHERE id = :id",
        {"id": case["request_id"]},
    )[0]
    original = work_item_issue.read_issue

    async def read_then_reconfigure(*args: Any, **kwargs: Any) -> Any:
        content = await original(*args, **kwargs)
        await asyncio.to_thread(
            _execute,
            "UPDATE curie.agents SET execution_deadline_seconds = 10800 WHERE id = :id",
            {"id": case["agent_id"]},
        )
        return content

    monkeypatch.setattr(work_item_issue, "read_issue", read_then_reconfigure)
    response = _read(case, capability)
    assert response.status_code == 200, response.text
    result = response.json()
    assert datetime.fromisoformat(result["execution_deadline"]) == execution["execution_deadline"]
    assert result["execution_seconds"] == 3600
    assert (
        _rows(
            "SELECT execution_deadline_seconds FROM curie.agents WHERE id = :id",
            {"id": case["agent_id"]},
        )[0]["execution_deadline_seconds"]
        == 10800
    )
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
