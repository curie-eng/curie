"""AC9 of #4358: the Jira Cloud tracker and the GitHub code host, end to end, offline.

Nothing inside the factory is faked. The Jira fake (`forge_fakes/jira.py`) is
served by uvicorn on 127.0.0.1 on an ephemeral port, and a GitHub REST fake
(`forge_fakes.github.GitHubRepositoryFake`, served the same way) stands in for
GitHub. The install's `ForgesConfig` names a ``jira_cloud`` tracker whose
``api_url`` is that port, the GitHub code host and tracker on the GitHub fake,
and one binding with an account allowlist or group; it is validated and built
by the production builder before the app boots on it.

Then the real passes run, through `WorkItemReconciler.run_once` (whose
``poll_trackers`` step is the Jira intake) and the status comment sync:

1. an authorized account creates a labelled Jira issue or labels it later;
   one WorkItem is admitted on the bound repository, replying on the ``jira`` surface;
2. one status comment is posted on the Jira issue and edited in place as the
   run's phases advance, and the state label moves through the tracker port;
3. the publication for the request is created through
   ``POST /v1/internal/publications`` and its body carries ``Jira: <KEY> <url>``;
   the pull request opens on the GitHub fake through the recover route;
4. the next status sync links the Jira issue to the pull request, once;
5. CI goes green on the GitHub fake and the request completes;
6. a review comment by a GitHub writer (signed webhook) admits a revision
   whose reply route is still the ``jira`` surface;
7. removing the label cancels the WorkItem;
8. an unauthorized initial creator or later label author admits nothing.

Two test-level shortcuts, both outside the factory core: the publication's
approval is recorded directly (the approval and publication Job path is not
offline, as in `test_offline_lifecycle`), and the publication Job's lineage
advance is made through the same CRUD service its route calls. The GitHub
App's installation token mint is answered by `fake_installation_tokens`.

The fake checks that ``/_edge/tenant_info`` is asked on the site host; served
on 127.0.0.1, the site host is that address, so the module constant is set to
it for the test. Placeholder identifiers only.
"""

# ruff: noqa: F811  (the shared ``comments`` fixture module is imported for its helpers)

from __future__ import annotations

import asyncio
import copy
import json
import re
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
import uvicorn
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_rediscovery import issue_marker
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.identity import notice_request_id
from curie_api.forges.registry import build_registry
from curie_api.main import create_app
from curie_api.publication_authority import VerifiedPublicationIdentity
from curie_api.schemas.publications import PublicationLineageAdvance
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from forge_fakes import jira as jira_fake
from forge_fakes.github import (
    APP_ID,
    INSTALLATION_ID,
    MENTION,
    PRIVATE_KEY,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubRepositoryFake,
    _post,
    _sender,
    fake_installation_tokens,
    forges_document,
    forges_env_json,
)
from forge_fakes.github_comments import _rows
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from test_factory_terminus import _reconcile

pytestmark = pytest.mark.usefixtures("clean_db")

LABEL = jira_fake.LABEL
BINDING = jira_fake.BINDING_NAME
TRACKER = jira_fake.TRACKER_NAME
STARTER_ID = "5b10a0000000000000000a11"
OUTSIDER_ID = "5b10a0000000000000000a22"
WORKER = {"X-Curie-Worker-Token": "jira-lifecycle-worker"}
REVIEW_BODY = f"@{MENTION} please rename the helper before merge"
FEEDBACK_ID = 88131
STATE_LABELS = {
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
}


# --- servers ------------------------------------------------------------------------


@dataclass
class _Served:
    url: str
    server: uvicorn.Server
    thread: threading.Thread
    sock: socket.socket

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)
        self.sock.close()


def _serve(app: Any) -> _Served:
    """``app`` on uvicorn at 127.0.0.1 on an ephemeral port, in a thread."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    server = uvicorn.Server(
        uvicorn.Config(app, lifespan="off", log_level="critical", access_log=False)
    )
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        assert thread.is_alive(), "the fake server exited during startup"
        assert time.monotonic() < deadline, "the fake server did not start"
        time.sleep(0.02)
    return _Served(f"http://127.0.0.1:{sock.getsockname()[1]}", server, thread, sock)


class _GitHub(GitHubRepositoryFake):
    """The GitHub repository fake, as GitHub renders it on the served origin.

    Pull requests carry the fields a real pull read has (node id, merge
    state, repository ids) and their web URL on the served origin; the
    repository is also readable by its immutable id; review comments the test
    adds carry GitHub's full review comment shape.
    """

    def __init__(self) -> None:
        super().__init__()
        self.html_base = "https://github.com"
        self.requests: list[tuple[str, str]] = []

    def _pull_payload(self, number: int) -> dict[str, Any]:
        payload = super()._pull_payload(number)
        side = {"id": REPO_ID, "full_name": REPO}
        payload.update(
            {
                "html_url": f"{self.html_base}/{REPO}/pull/{number}",
                "node_id": f"PR_node_{number}",
                "merged": payload["merged_at"] is not None,
                "mergeable": True,
                "mergeable_state": "clean",
            }
        )
        payload["head"]["repo"] = dict(side)
        payload["base"]["repo"] = dict(side)
        return payload

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.method, request.url.path))
        if request.url.path == f"/repositories/{REPO_ID}":
            request = httpx.Request(
                request.method,
                request.url.copy_with(path=f"/repos/{REPO}"),
                headers=request.headers,
            )
        return super().handle(request)

    def add_feedback(self, number: int, body: str) -> dict[str, Any]:
        """A writer's review comment on pull request ``number``, as GitHub shows it."""

        head = self.branches[self.pulls[number]["head"]]
        comment = {
            "id": FEEDBACK_ID,
            "body": body,
            "user": _sender(),
            "created_at": "2026-09-20T10:00:00Z",
            "updated_at": "2026-09-20T10:00:00Z",
            "performed_via_github_app": None,
            "author_association": "MEMBER",
            "html_url": f"{self.html_base}/{REPO}/pull/{number}#discussion_r{FEEDBACK_ID}",
            "commit_id": head,
            "path": "src/widget.txt",
            "line": 12,
            "pull_request_review_id": FEEDBACK_ID + 500000,
            "pull_request_url": f"{self.html_base}/repos/{REPO}/pulls/{number}",
            "_number": number,
        }
        self.review_comments.append(comment)
        return comment


def _asgi(handler: Any) -> Starlette:
    """An ASGI app answering every request with ``handler(httpx.Request)``."""

    async def endpoint(request: Request) -> Response:
        forwarded = httpx.Request(
            request.method,
            str(request.url),
            headers=[(k, v) for k, v in request.headers.items() if k.lower() != "host"],
            content=await request.body(),
        )
        answered = handler(forwarded)
        return Response(
            content=answered.content,
            status_code=answered.status_code,
            media_type=answered.headers.get("content-type", "application/json"),
            headers={k: v for k, v in answered.headers.items() if k.lower() == "link"},
        )

    methods = ["GET", "POST", "PATCH", "PUT", "DELETE"]
    return Starlette(routes=[Route("/{path:path}", endpoint, methods=methods)])


# --- the install ----------------------------------------------------------------------


@dataclass
class Install:
    client: TestClient
    jira: jira_fake.JiraFake
    jira_url: str
    jira_host: str
    github: _GitHub
    github_url: str
    starter: jira_fake.JiraUser
    outsider: jira_fake.JiraUser
    agent_id: uuid.UUID

    def issue_ref(self, issue: jira_fake.JiraIssue) -> types.TrackerIssueRef:
        return types.TrackerIssueRef(
            types.JIRA_CLOUD, self.jira_host, jira_fake.CLOUD_ID, str(issue.id), issue.key
        )

    def browse(self, key: str) -> str:
        return f"{self.jira_url}/browse/{key}"


def _document(jira_url: str, jira_host: str, github_url: str) -> dict[str, Any]:
    """The Jira site bound to the GitHub repository, beside today's GitHub entries."""

    document = forges_document(api_url=github_url, app_id=APP_ID, private_key=PRIVATE_KEY)
    document["trackers"].append(
        {
            "name": TRACKER,
            "kind": "jira_cloud",
            "host": jira_host,
            "api_url": jira_url,
            "credential": {
                "type": "static_token",
                "username": jira_fake.BOT_EMAIL,
                "token": jira_fake.TOKEN,
            },
            "label": LABEL,
            "mention": jira_fake.BOT_ACCOUNT_ID,
            "poll_interval_s": 45,
        }
    )
    document["bindings"].append(
        {
            "name": BINDING,
            "tracker": TRACKER,
            "tracker_scope_id": jira_fake.CLOUD_ID,
            "repos": [
                {
                    "alias": "bot",
                    "code_host": "github",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                }
            ],
            "default_repo": "bot",
            "start_authority": {"type": "allowlist", "accounts": [STARTER_ID]},
        }
    )
    return document


@pytest.fixture(params=["allowlist", "group"])
def authority_kind(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, authority_kind: str
) -> Iterator[Install]:
    jira = jira_fake.JiraFake()
    github = _GitHub()
    served_jira = _serve(jira.app)
    served_github = _serve(_asgi(github.handle))
    github.html_base = served_github.url
    jira_host = served_jira.url.removeprefix("http://")
    # The fake answers tenant_info only on its site host, which is this address.
    monkeypatch.setattr(jira_fake, "SITE_HOST", "127.0.0.1")
    document = _document(served_jira.url, jira_host, served_github.url)
    if authority_kind == "group":
        document["bindings"][-1]["start_authority"] = {
            "type": "group",
            "group_id": jira_fake.STARTERS,
        }
    # Validated and built by the production builder before the app boots on it.
    registry = build_registry(ForgesConfig.model_validate(copy.deepcopy(document)))
    assert registry.binding_tracker(TRACKER).config.api_url == served_jira.url
    env = {
        "GITHUB_API_URL": served_github.url,
        "GITHUB_APP_ID": APP_ID,
        "GITHUB_APP_PRIVATE_KEY": PRIVATE_KEY,
        "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_TOKEN": "",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "FACTORY_ENABLED": "true",
        # Review feedback arrives as a signed webhook (the GitHub half).
        "FACTORY_INTAKE": "webhook",
        "CURIE_FORGES": forges_env_json(document, tmp_path),
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": "30",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "INTERNAL_WORKER_TOKEN": WORKER["X-Curie-Worker-Token"],
        "RUNS_STREAM": f"test:curie:jira-lifecycle:{uuid.uuid4().hex}",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    fake_installation_tokens(monkeypatch)
    # The code host's git credential: the GitHub fake serves plain HTTP, which
    # the canonical HTTPS origin check would refuse (as `forge_fakes.github_comments`).
    monkeypatch.setattr(
        "curie_api.forges.github.code_host.resolve_repository_credential",
        lambda repo, *, html_base, credentials: (
            f"{html_base}/{repo}.git",
            "Basic eC1hY2Nlc3MtdG9rZW46Zml4dHVyZQ==",  # x-access-token:fixture
        ),
    )
    starter = jira.add_user(STARTER_ID, "Ada Starter")
    outsider = jira.add_user(OUTSIDER_ID, "Oscar Outsider")
    jira.set_group_member(jira_fake.STARTERS, starter.account_id, True)
    github.permissions[SENDER] = (SENDER_ID, "write")
    try:
        with TestClient(create_app()) as client:
            created = client.post(
                "/agents",
                headers={"X-API-Key": get_settings().api_key},
                json={
                    "name": f"acme-jira-factory-{uuid.uuid4().hex[:8]}",
                    "repo_full_name": REPO,
                    "channel": {"kind": "jira", "address": BINDING},
                },
            )
            assert created.status_code == 201, created.text
            yield Install(
                client=client,
                jira=jira,
                jira_url=served_jira.url,
                jira_host=jira_host,
                github=github,
                github_url=served_github.url,
                starter=starter,
                outsider=outsider,
                agent_id=uuid.UUID(created.json()["id"]),
            )
            _drop_stream()
    finally:
        served_jira.stop()
        served_github.stop()
        get_settings.cache_clear()


# --- persisted state --------------------------------------------------------------------


def _work_items(issue_id: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, base_branch, trigger_label, "
        "cancelled_at, publication_lineage_id, conversation_id, agent_id "
        "FROM curie.work_items WHERE tracker_kind = 'jira_cloud' AND tracker_issue_id = :id",
        {"id": str(issue_id)},
    )


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, sequence, status, terminal_cause, objective, requester, reply_kind, "
        "reply_address, reply_conversation_id, reply_target_kind, runtime_epoch "
        "FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def _lineage(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT l.id, l.branch, l.pr_number, l.pr_url, l.head_sha, l.status "
        "FROM curie.thread_publication_lineages l "
        "JOIN curie.work_items w ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def _turns() -> list[dict[str, Any]]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


def _drop_stream() -> None:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        valkey.delete(get_settings().runs_stream)
    finally:
        valkey.close()


def _start(request_id: uuid.UUID) -> int:
    """A worker acquires the published generation and starts the runtime."""

    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]

    async def go() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(
                    session,
                    request_id,
                    owner="jira-owner",
                    generation=generation,
                    wake=await factory_wake(session, request_id),
                    context=None,
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="jira-owner",
                    generation=generation,
                    claim_name="claim-jira",
                    sandbox_name="sbx-jira",
                    context=acquired.context,
                )
                assert not isinstance(started, DispatchConflict), started
                return int(started.runtime_epoch)
        finally:
            await engine.dispose()

    return asyncio.run(go())


# --- Jira observations ----------------------------------------------------------------


def _status_comments(install: Install, issue: jira_fake.JiraIssue) -> list[jira_fake.JiraComment]:
    return install.jira.own_comments(issue.id)


def _comment_text(comment: jira_fake.JiraComment) -> str:
    return jira_fake.flatten(comment.body)


def _state_labels(install: Install, issue: jira_fake.JiraIssue) -> set[str]:
    return set(install.jira.issue(issue.id).labels) & STATE_LABELS


# --- the publication ------------------------------------------------------------------


def _deployment(client: TestClient, agent_id: uuid.UUID) -> str:
    headers = {"X-API-Key": get_settings().api_key}
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        json={
            "agent_id": str(agent_id),
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text
    return str(deployment.json()["id"])


def _publish(install: Install, item: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """The worker's publication for the running request, as the bundle sends it."""

    client = install.client
    deployment_id = _deployment(client, item["agent_id"])
    author = request["requester"]
    selected = client.post(
        f"/v1/internal/workspaces/{deployment_id}/selection",
        json={
            "conversation_id": item["conversation_id"],
            "author": author,
            "repo_full_name": REPO,
        },
        headers=WORKER,
    )
    assert selected.status_code == 200, selected.text
    payload = {
        "deployment_id": deployment_id,
        "conversation_id": item["conversation_id"],
        "reply_conversation_id": request["reply_conversation_id"],
        "repo_full_name": REPO,
        "author": author,
        "summary": "Publish the widget parser fix",
        "reply_kind": request["reply_kind"],
        "reply_channel": request["reply_address"],
        "reply_placeholder": f"jira-{item['tracker_issue_id']}",
        "dedupe_key": f"jira-publication-{uuid.uuid4().hex}",
        "base_sha": install.github.branches["main"],
        "work_item_request_id": str(request["id"]),
        "work_item_runtime_epoch": request["runtime_epoch"],
        "patch_b64": "ZGlmZiAtLWdpdCBhL1JFQURNRS5tZCBiL1JFQURNRS5tZAo=",
        "changed_paths": ["README.md"],
        "expires_in_seconds": 600,
        "title": "Keep the last field of a line",
        "body": "Parsing a three-field line now yields three fields.",
    }
    created = client.post("/v1/internal/publications", json=payload, headers=WORKER)
    assert created.status_code == 201, created.text
    return dict(created.json())


def _approve(publication_id: str) -> None:
    """The approval the publication Job path records (not offline)."""

    from test_factory_ci_wait import _execute

    _execute(
        "UPDATE curie.publications SET status = 'approved' WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )


def _record_pull_request(
    install: Install, publication: dict[str, Any], pull: dict[str, Any]
) -> None:
    """The publication Job's lineage advance, through the CRUD service its route calls."""

    from curie_api.crud import lineages as crud_lineages
    from curie_api.crud import publication_queries
    from curie_api.factory_rediscovery import publishing_code_host
    from curie_api.publication_verification import publication_base_ref, verify_publication_commit
    from curie_api.repository_access import lineage_repository
    from test_factory_ci_wait import _execute

    owner = "jira-lifecycle-publisher"
    _execute(
        "UPDATE curie.publications SET lease_owner = :owner, "
        "lease_expires_at = now() + interval '1 minute', version = version + 1 WHERE id = :id",
        {"id": uuid.UUID(publication["id"]), "owner": owner},
    )
    version = _rows(
        "SELECT version FROM curie.publications WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )[0]["version"]

    async def advance() -> None:
        async with install.client.app.state.sessionmaker() as session:
            stored = await publication_queries.get_publication(
                session, uuid.UUID(publication["id"])
            )
            assert stored is not None and stored.lineage is not None
            settings = get_settings()
            repository = await lineage_repository(session, settings, stored.lineage)
            code_host = publishing_code_host(
                settings, install.client.app.state.http_client, repository
            )
            base_branch = await publication_base_ref(session, code_host, stored, repository)
            proof = await verify_publication_commit(
                code_host, repository, stored, commit_sha=pull["head_sha"], base_branch=base_branch
            )
            await crud_lineages.advance_publication_lineage(
                session,
                uuid.UUID(publication["id"]),
                PublicationLineageAdvance(
                    expected_version=publication["lineage_version"],
                    expected_head_sha=None,
                    expected_publication_version=version,
                    lease_owner=owner,
                    state="open",
                    pr_number=pull["number"],
                    pr_url=pull["url"],
                    head_sha=pull["head_sha"],
                    metadata_updated_at=None,
                ),
                identity=VerifiedPublicationIdentity(
                    host=install.github_url.removeprefix("http://"),
                    repository_id=REPO_ID,
                    installation_id=INSTALLATION_ID,
                    pr_node_id=f"PR_node_{pull['number']}",
                    base_ref="main",
                ),
                github_html_base=get_settings().github_html_base,
                first_parent_proof=proof,
            )

    install.client.portal.call(advance)


# --- the review ---------------------------------------------------------------------------


def _review(install: Install, pr: int) -> httpx.Response:
    github = install.github
    comment = github.add_feedback(pr, REVIEW_BODY)
    pull = github._pull_payload(pr)
    payload = {
        "action": "created",
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _sender(),
        "pull_request": pull,
        "comment": {k: v for k, v in comment.items() if not k.startswith("_")},
    }
    return _post(install.client, "pull_request_review_comment", payload)


# --- the test -------------------------------------------------------------------------------


@pytest.mark.parametrize("initial_label", [False, True], ids=["label_add", "initial_label"])
def test_jira_github_lifecycle(install: Install, initial_label: bool) -> None:
    jira, github = install.jira, install.github
    denied = jira.seed_issue(
        "Unauthorized initial creation",
        creator=install.outsider,
        reporter=install.starter,
        labels=[LABEL],
    )
    issue = jira.seed_issue(
        "Widget parser drops the last field",
        "Parsing a three-field line yields two fields.",
        creator=install.starter,
        reporter=install.outsider,
        labels=[LABEL] if initial_label else (),
    )
    key = issue.key

    # 1. An authorized account creates or labels the issue; the real poll pass admits one
    #    WorkItem on the bound repository, replying on the jira surface.
    label_event = (
        f"creation:{issue.id}:{issue.created_ms}"
        if initial_label
        else jira.add_label(issue.id, LABEL, install.starter)
    )
    if initial_label:
        assert issue.history == []
    _reconcile()
    [item] = _work_items(issue.id)
    assert (
        item["tracker_host"],
        item["tracker_scope_id"],
        item["tracker_display_key"],
        item["trigger_label"],
    ) == (install.jira_host, jira_fake.CLOUD_ID, key, LABEL)
    assert (item["code_host_kind"], item["repository_project_id"], item["repository_path"]) == (
        types.GITHUB,
        str(REPO_ID),
        REPO,
    )
    assert item["code_host_installation_id"] == INSTALLATION_ID
    assert item["base_branch"] == "main"
    assert item["agent_id"] == install.agent_id
    [first] = _requests(item["id"])
    assert first["objective"].splitlines()[0] == install.browse(key)
    assert (first["reply_kind"], first["reply_address"], first["reply_conversation_id"]) == (
        "jira",
        BINDING,
        f"issue-{issue.id}",
    )
    assert first["requester"] == f"{types.JIRA_CLOUD}:{STARTER_ID}:{install.starter.display_name}"
    assert first["status"] == "waiting"
    expected_notice = types.MarkedNotice(
        issue=install.issue_ref(issue),
        marker=LABEL,
        actor=install.starter.actor,
        event_id=label_event,
        disposition=types.Disposition.ADMIT,
        cursor="",
        occurred_at=datetime.fromtimestamp(issue.created_ms / 1000, UTC),
    )
    assert first["id"] == notice_request_id(expected_notice)
    assert _work_items(denied.id) == []
    assert _status_comments(install, denied) == []
    assert _state_labels(install, denied) == set()
    # The execute wake is published and the live status comment posted.
    assert [t for t in _turns() if str(first["id"]) in t["event_id"]]
    [comment] = _status_comments(install, issue)
    assert "curie.factory" in comment.properties
    assert marker_for(first["id"]) not in _comment_text(comment)  # never visible text
    assert _state_labels(install, issue) == {"curie-factory:queued"}
    posted_body = copy.deepcopy(comment.body)

    searches_before = len(jira.requests_of("search"))
    _reconcile()
    assert any(
        r.json["jql"] == 'labels = "factory" AND statusCategory != Done ORDER BY updated ASC'
        for r in jira.requests_of("search")[searches_before:]
    )
    assert [w["id"] for w in _work_items(issue.id)] == [item["id"]]
    assert [r["id"] for r in _requests(item["id"])] == [first["id"]]

    jira.advance(30)
    jira.edit_summary(issue.id, "Widget parser still drops the last field", install.outsider)
    jira.edit_reporter(issue.id, install.starter, install.outsider)
    _reconcile()
    assert [w["id"] for w in _work_items(issue.id)] == [item["id"]]
    assert [r["id"] for r in _requests(item["id"])] == [first["id"]]
    assert _requests(item["id"])[0]["requester"] == first["requester"]
    assert _work_items(denied.id) == []
    assert _status_comments(install, denied) == []
    assert _state_labels(install, denied) == set()

    # 2. The run starts: the same comment is edited in place.
    epoch = _start(first["id"])
    _reconcile()
    assert _requests(item["id"])[0]["status"] == "running"
    [same] = _status_comments(install, issue)
    assert same.id == comment.id and same.body != posted_body
    assert _state_labels(install, issue) == {"curie-factory:running"}
    edits = jira.requests_of("comment_update")
    assert edits and all(r.params.get("notifyUsers") == "false" for r in edits)

    # 3. The publication carries the Jira closing reference; the pull request
    #    opens on the GitHub fake through the recover route.
    request_row = {**_requests(item["id"])[0], "runtime_epoch": epoch}
    publication = _publish(install, item, request_row)
    reference = f"Jira: {key} {install.browse(key)}"
    # The reference, then the canonical marker naming the issue (#4629).
    marker = issue_marker(install.issue_ref(issue))
    assert publication["body"].endswith(f"\n\n{reference}\n\n{marker}")
    assert publication["body"].count(reference) == 1
    _approve(publication["id"])
    github.branches[publication["branch"]] = github.branches["main"]
    head = github.push_head(publication["branch"])
    github.commits[head]["message"] += f"\n\nCurie-Revision: {publication['id']}\n"
    recovered = install.client.post(
        f"/v1/internal/publications/{publication['id']}/pull-request",
        json={"expected_head_sha": head},
        headers=WORKER,
    )
    assert recovered.status_code == 200, recovered.text
    pull = recovered.json()
    [(pr, opened)] = list(github.pulls.items())
    assert pull["number"] == pr
    assert opened["body"].endswith(f"{reference}\n\n{marker}")
    assert jira.issue(issue.id).remote_links == []
    _record_pull_request(install, publication, {**pull, "head_sha": head})

    # 4. The next status sync links the Jira issue to the pull request, once.
    github.report_check(head, "unit-tests", "pending")
    _reconcile()
    links = jira.issue(issue.id).remote_links
    assert len(links) == 1
    assert links[0]["globalId"] == pull["url"]
    assert links[0]["object"]["url"] == pull["url"]
    _reconcile()
    assert len(jira.issue(issue.id).remote_links) == 1
    assert len(jira.requests_of("remote_link_create")) >= 1

    # 5. CI goes green: the request completes; still one status comment.
    github.report_check(head, "unit-tests", "success")
    _reconcile()
    assert _requests(item["id"])[0][("status")] == "completed"
    [final] = _status_comments(install, issue)
    assert final.id == comment.id
    assert pull["url"] in _comment_text(final)
    assert _state_labels(install, issue) == {"curie-factory:pr-open"}
    assert len(jira.issue(issue.id).remote_links) == 1

    # 6. A GitHub writer's review comment admits a revision on the same
    #    WorkItem, whose reply route is still the jira surface.
    reviewed = _review(install, pr)
    assert reviewed.status_code == 200, reviewed.text
    assert reviewed.json()["status"] == "factory_admitted", reviewed.text
    rows = _requests(item["id"])
    assert len(rows) == 2
    revision = rows[1]
    assert (revision["reply_kind"], revision["reply_address"]) == ("jira", BINDING)
    assert revision["reply_target_kind"] == "review_thread"
    assert revision["status"] == "waiting"
    # The sweep replays the admitted label event: no second WorkItem, no new run.
    _reconcile()
    assert len(_work_items(issue.id)) == 1
    assert len(_requests(item["id"])) == 2

    # 7. Removing the label cancels the WorkItem.
    jira.remove_label(issue.id, LABEL, install.starter)
    _reconcile()
    [cancelled] = _work_items(issue.id)
    assert cancelled["cancelled_at"] is not None
    assert all(r["status"] not in {"waiting", "running"} for r in _requests(item["id"]))

    # 8. A label applied by an account outside the allowlist admits nothing.
    other = jira.seed_issue("Widget parser drops a quoted field", "Quoted commas split.")
    jira.add_label(other.id, LABEL, install.outsider)
    _reconcile()
    assert _work_items(other.id) == []
    assert _status_comments(install, other) == []
    assert _state_labels(install, other) == set()

    # The Jira token never left the API: not in any GitHub request, comment or turn.
    assert jira_fake.TOKEN not in json.dumps(_turns())
    assert all(jira_fake.TOKEN not in _comment_text(c) for c in _status_comments(install, issue))
    assert not re.search(r"jira-fixture-token", json.dumps(github.requests))
