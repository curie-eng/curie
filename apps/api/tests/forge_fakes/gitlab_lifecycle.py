"""A gitlab+gitlab install on a GitLab backend, for the factory lifecycle (#4399 AC 8).

The backend is the GitLab one install talks to: `FakeBackend`, the stateful
`forge_fakes.gitlab.GitLabAPI` served by uvicorn on 127.0.0.1 on an ephemeral
port, or the live CE fixture (``tests/forges/gitlab_fixture``). Either way every
HTTP client the API makes (the app's shared client, the reconciler's own)
reaches it over a socket. ``CURIE_FORGES`` holds only the GitLab tracker, code
host and binding the backend's `Backend.forges_document` writes, validated and
built by the production builder before the app boots. Nothing GitHub is
configured: no App, no token, no ``GITHUB_REPO_ALLOWLIST``. No seam inside the
API is replaced; in particular the WorkItem's code host is resolved by the core
itself (`forges.hosts`, `forges.trackers.code_host_for_repository` are
untouched).

`gitlab_install` is a fixture body (a generator); the helpers below read and
drive the install the way the worker and the publication Job do, through the
internal HTTP routes, and the ``run_*`` functions are the lifecycle steps both
suites run. Two shortcuts sit outside the factory core, as in
``tests/forges/test_jira_github_lifecycle.py``: a publication's approval is
recorded by SQL (the approval Job path is not offline), and a publication's
lease is taken by SQL before its lineage advance, which itself goes through
``PATCH /v1/internal/publications/{id}/lineage``. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import httpx
import pytest
import redis
import uvicorn
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.registry import build_registry
from curie_api.forges.types import Actor, TrackerIssueRef
from curie_api.main import create_app
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from forge_fakes import factory_wake
from forge_fakes import gitlab as gitlab_fake
from forge_fakes.github import forges_env_json
from forge_fakes.github_comments import _clear_ci_keys, _rows

PROJECT_PATH = gitlab_fake.PROJECT_PATH
PROJECT_ID = gitlab_fake.PROJECT_ID
LABEL = gitlab_fake.LABEL
FAST = "factory-fast"
TOKEN = gitlab_fake.TOKEN
BOT = gitlab_fake.BOT
# The seeded people (ADR 0197 authority 1): Developer (30) may start, Reporter
# (20) can apply labels but may not.
DEVELOPER = Actor("31", "dev-dana")
REPORTER = Actor("32", "rep-ray")
WORKER = {"X-Curie-Worker-Token": "gitlab-lifecycle-worker"}
STATE_LABELS = frozenset(
    {
        "curie-factory:queued",
        "curie-factory:running",
        "curie-factory:pr-open",
        "curie-factory:needs-human",
    }
)
PATCH_B64 = "ZGlmZiAtLWdpdCBhL1JFQURNRS5tZCBiL1JFQURNRS5tZAo="


# --- the backend seam -----------------------------------------------------------------


@dataclass(frozen=True)
class Note:
    """One non-system note: its id, its author's user id and its raw body."""

    id: int
    author: str
    body: str


@dataclass(frozen=True)
class MergeRequest:
    iid: int
    source_branch: str
    target_branch: str
    description: str


class Backend(Protocol):
    """One GitLab the lifecycle drives: the served fake or the live CE fixture.

    The adapters are always the production ones, built by the registry from
    `forges_document` when the app boots; a backend only seeds and observes
    from outside, as a person or the publication Job would.
    """

    origin: str  # the web base, https://<host> or http://127.0.0.1:<port>
    host: str  # the configured host
    api_url: str
    project_id: int
    project_path: str
    label: str  # the tracker's own trigger label
    token: str  # the factory credential's secret, for the no-token checks
    bot: Actor
    developer: Actor  # Developer (30): may start
    reporter: Actor  # Reporter (20): may label, may not start
    ca_bundle: str  # CURIE_CODE_HOST_CA_BUNDLE ("" trusts the public roots)
    wait_budget_s: int

    def forges_document(self) -> dict[str, Any]:
        """The GitLab tracker, code host and binding; nothing else. Secrets inline."""
        ...

    def stop(self) -> None: ...

    def default_head(self) -> str: ...

    def add_issue(self, title: str, description: str = "", *, author: Actor | None = None) -> int:
        """A new open issue (by an administrator when ``author`` is None); its iid."""
        ...

    def label_issue(self, iid: int, label: str, actor: Actor, *, add: bool = True) -> None: ...

    def issue_note(self, iid: int, actor: Actor, body: str) -> int: ...

    def issue_labels(self, iid: int) -> set[str]: ...

    def issue_notes(self, iid: int) -> list[Note]:
        """Every non-system note on issue ``iid``, oldest first."""
        ...

    def push(self, branch: str, message: str, *, fix: bool = False) -> str:
        """The publication Job's push: one commit on ``branch`` (from the default
        branch when new), the widget fix when ``fix``. Returns its sha."""
        ...

    def merge_request(self, branch: str) -> MergeRequest:
        """The one merge request from ``branch``."""
        ...

    def ci_fails(self, head: str) -> str:
        """CI on ``head`` ends red; returns the failed job's id."""
        ...

    def rerun_fails(self, head: str, job: str) -> None:
        """The gate asked GitLab to rerun exactly ``job``, and the rerun is red too."""
        ...

    def ci_passes(self, head: str) -> None: ...

    def mr_discussion(self, iid: int, actor: Actor, body: str) -> str:
        """``actor`` opens a thread on a changed line of merge request ``iid``."""
        ...

    def thread_replies(self, iid: int, discussion: str) -> list[Note]:
        """Every non-system note in ``discussion`` after the opening one."""
        ...

    def published_texts(self) -> list[str]:
        """Every note body and merge request description this run can reach."""
        ...

    def break_credential(self) -> None:
        """The factory's token stops working (and, on the fake, every error
        body quotes the presented token from now on)."""
        ...

    def credential_was_refused(self) -> None:
        """Liveness of the failure probe: GitLab really refused the token."""
        ...


# --- the fake backend -----------------------------------------------------------------


@dataclass
class Served:
    url: str
    server: uvicorn.Server
    thread: threading.Thread
    sock: socket.socket

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)
        self.sock.close()


def _asgi(fake: gitlab_fake.GitLabAPI, origin: str) -> Starlette:
    """``fake.handle`` as an ASGI app, on the raw (still encoded) request path.

    GitLab addresses a branch or a project path by its URL-encoded name, and
    the fake routes on the raw path, so the path is rebuilt from the scope's
    ``raw_path`` rather than Starlette's decoded one.
    """

    async def endpoint(request: Request) -> Response:
        raw = request.scope.get("raw_path") or request.url.path.encode()
        query = request.scope.get("query_string") or b""
        url = origin + raw.decode("latin-1") + (f"?{query.decode('latin-1')}" if query else "")
        forwarded = httpx.Request(
            request.method,
            url,
            headers=[(k, v) for k, v in request.headers.items() if k.lower() != "host"],
            content=await request.body(),
        )
        answered = fake.handle(forwarded)
        headers = {
            k: v
            for k, v in answered.headers.items()
            if k.lower() not in {"content-length", "content-encoding", "transfer-encoding"}
        }
        return Response(content=answered.content, status_code=answered.status_code, headers=headers)

    methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]
    return Starlette(routes=[Route("/{path:path}", endpoint, methods=methods)])


def serve_fake() -> tuple[gitlab_fake.GitLabAPI, Served]:
    """A `GitLabAPI` whose base URL is its own uvicorn socket on 127.0.0.1."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    origin = f"http://127.0.0.1:{sock.getsockname()[1]}"
    fake = gitlab_fake.GitLabAPI(base_url=f"{origin}/api/v4")
    server = uvicorn.Server(
        uvicorn.Config(_asgi(fake, origin), lifespan="off", log_level="critical", access_log=False)
    )
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        assert thread.is_alive(), "the GitLab fake exited during startup"
        assert time.monotonic() < deadline, "the GitLab fake did not start"
        time.sleep(0.02)
    return fake, Served(origin, server, thread, sock)


class FakeBackend:
    """The GitLab fake served over a socket, with Developer and Reporter seeded."""

    host: str
    project_id = PROJECT_ID
    project_path = PROJECT_PATH
    label = LABEL
    token = TOKEN
    bot = BOT
    developer = DEVELOPER
    reporter = REPORTER
    ca_bundle = ""
    wait_budget_s = 30

    def __init__(self) -> None:
        self.fake, self._served = serve_fake()
        self.origin = self._served.url
        self.host = self.origin.removeprefix("http://")
        self.api_url = f"{self.origin}/api/v4"
        self.fake.add_user(DEVELOPER)
        self.fake.set_member(DEVELOPER, 30)
        self.fake.add_user(REPORTER)
        self.fake.set_member(REPORTER, 20)

    def forges_document(self) -> dict[str, Any]:
        return gitlab_fake.forges_document(api_url=self.api_url, host=self.host)

    def stop(self) -> None:
        self._served.stop()

    def default_head(self) -> str:
        return self.fake.branches[gitlab_fake.DEFAULT_BRANCH]

    def add_issue(self, title: str, description: str = "", *, author: Actor | None = None) -> int:
        return self.fake.add_issue(title, description, author=author)

    def label_issue(self, iid: int, label: str, actor: Actor, *, add: bool = True) -> None:
        self.fake.label(iid, label, actor, add=add)

    def issue_note(self, iid: int, actor: Actor, body: str) -> int:
        return self.fake.issue_note(iid, actor, body)

    def issue_labels(self, iid: int) -> set[str]:
        return set(self.fake.issues[iid].labels)

    def issue_notes(self, iid: int) -> list[Note]:
        return [Note(n.id, str(n.author), n.body) for n in self.fake.issue_notes(iid)]

    def push(self, branch: str, message: str, *, fix: bool = False) -> str:
        return self.fake.push(branch, message)

    def merge_request(self, branch: str) -> MergeRequest:
        (mr,) = [m for m in self.fake.merge_requests.values() if m.source_branch == branch]
        return MergeRequest(mr.iid, mr.source_branch, mr.target_branch, mr.description)

    def ci_fails(self, head: str) -> str:
        return str(self.fake.report_job(head, "test", "failed"))

    def rerun_fails(self, head: str, job: str) -> None:
        fake = self.fake
        assert [
            fake.api_path(r) for r in fake.calls("POST", "/projects/*") if "retry" in str(r.url)
        ] == [f"/projects/{PROJECT_ID}/jobs/{job}/retry"]
        fake.report_job(head, "test", "failed")

    def ci_passes(self, head: str) -> None:
        self.fake.report_job(head, "test", "success")

    def mr_discussion(self, iid: int, actor: Actor, body: str) -> str:
        return self.fake.mr_discussion(iid, actor, body, diff=True)

    def thread_replies(self, iid: int, discussion: str) -> list[Note]:
        return [
            Note(n.id, str(n.author), n.body)
            for n in self.fake.thread_notes(discussion)[1:]
            if not n.system
        ]

    def published_texts(self) -> list[str]:
        return [n.body for n in self.fake.notes.values()] + [
            mr.description for mr in self.fake.merge_requests.values()
        ]

    def break_credential(self) -> None:
        self.fake.echo_token_in_errors = True
        self.fake.tokens[TOKEN].revoked = True

    def credential_was_refused(self) -> None:
        assert any(TOKEN in r.headers.get("private-token", "") for r in self.fake.requests[-5:]), (
            "the API stopped calling GitLab"
        )


# --- the install ----------------------------------------------------------------------


@dataclass
class GitLabInstall:
    client: TestClient
    backend: Backend
    agents: dict[str | None, uuid.UUID]  # trigger label (None: the tracker's) -> agent
    responses: list[httpx.Response] = field(default_factory=list)

    @property
    def agent_id(self) -> uuid.UUID:
        return self.agents[None]

    @property
    def fake(self) -> gitlab_fake.GitLabAPI:
        """The fake behind a `FakeBackend` install (offline suites only)."""

        assert isinstance(self.backend, FakeBackend), "this install is not on the fake"
        return self.backend.fake

    @property
    def origin(self) -> str:
        return self.backend.origin

    @property
    def host(self) -> str:
        return self.backend.host

    @property
    def label(self) -> str:
        return self.backend.label

    def issue_ref(self, iid: int) -> TrackerIssueRef:
        backend = self.backend
        return TrackerIssueRef(
            types.GITLAB,
            backend.host,
            str(backend.project_id),
            str(iid),
            f"{backend.project_path}#{iid}",
        )

    def issue_url(self, iid: int) -> str:
        return f"{self.origin}/{self.backend.project_path}/-/issues/{iid}"

    def mr_url(self, iid: int) -> str:
        return f"{self.origin}/{self.backend.project_path}/-/merge_requests/{iid}"

    def call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """One worker call to an internal route; every answer is kept so the
        test can prove no response carries the token."""

        kwargs.setdefault("headers", WORKER)
        response = self.client.request(method, path, **kwargs)
        self.responses.append(response)
        return response

    # GitLab observations -------------------------------------------------------

    def bot_notes(self, iid: int) -> list[Note]:
        """The factory's own notes on issue ``iid`` (its status notes)."""

        return [n for n in self.backend.issue_notes(iid) if n.author == self.backend.bot.id]

    def state_labels(self, iid: int) -> set[str]:
        return self.backend.issue_labels(iid) & STATE_LABELS

    def merge_request(self, branch: str) -> MergeRequest:
        return self.backend.merge_request(branch)


def _create_agent(client: TestClient, path: str, label: str | None) -> httpx.Response:
    channel: dict[str, Any] = {"kind": "gitlab", "address": path}
    if label is not None:
        channel["trigger_label"] = label
    return client.post(
        "/agents",
        headers={"X-API-Key": get_settings().api_key},
        json={"name": f"acme-gitlab-factory-{uuid.uuid4().hex[:8]}", "channel": channel},
    )


def gitlab_install(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    labels: tuple[str | None, ...] = (None,),
    backend: Backend | None = None,
) -> Iterator[GitLabInstall]:
    """Boot a gitlab-only install on ``backend`` (default: a served fake); one
    agent per surface label.

    ``labels`` names each agent's ``gitlab=<path>`` surface trigger label; None
    serves the tracker's own label. The install owns the backend and stops it.
    """

    backend = backend if backend is not None else FakeBackend()
    try:
        document = backend.forges_document()
        # Validated and built by the production builder before the app boots on it.
        registry = build_registry(
            ForgesConfig.model_validate(copy.deepcopy(document)), ca_bundle=backend.ca_bundle
        )
        assert registry.code_host(gitlab_fake.CODE_HOST_NAME).config.api_url == backend.api_url
        for name in (
            "GITHUB_REPO_ALLOWLIST",
            "GITHUB_API_URL",
            "GITHUB_APP_ID",
            "GITHUB_APP_PRIVATE_KEY",
            "GITHUB_TOKEN",
        ):
            monkeypatch.delenv(name, raising=False)
        env = {
            "FACTORY_ENABLED": "true",
            "FACTORY_INTAKE": "poll",
            "CURIE_FORGES": forges_env_json(document, tmp_path / "forges"),
            "CURIE_CODE_HOST_CA_BUNDLE": backend.ca_bundle,
            # A non-default secret: the published dev secret is refused at boot,
            # and this install configures no GitHub webhook at all.
            "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
            "GITHUB_REVIEW_INGRESS_ENABLED": "false",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": str(backend.wait_budget_s),
            "RESUME_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "INTERNAL_WORKER_TOKEN": WORKER["X-Curie-Worker-Token"],
            "RUNS_STREAM": f"test:curie:gitlab-lifecycle:{uuid.uuid4().hex}",
        }
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        get_settings.cache_clear()
        with TestClient(create_app()) as client:
            agents: dict[str | None, uuid.UUID] = {}
            for label in labels:
                created = _create_agent(client, backend.project_path, label)
                assert created.status_code == 201, created.text
                agents[label] = uuid.UUID(created.json()["id"])
            try:
                yield GitLabInstall(client, backend, agents)
            finally:
                _clear_ci_keys()
                drop_stream()
    finally:
        backend.stop()
        get_settings.cache_clear()


def create_agent(install: GitLabInstall, label: str | None) -> httpx.Response:
    """One more agent on the project's ``gitlab=`` surface under ``label``."""

    return _create_agent(install.client, install.backend.project_path, label)


# --- persisted state ------------------------------------------------------------------


def work_items(iid: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, base_branch, base_commit, trigger_label, "
        "cancelled_at, publication_lineage_id, conversation_id, agent_id, version "
        "FROM curie.work_items WHERE tracker_kind = 'gitlab' AND tracker_issue_id = :iid",
        {"iid": str(iid)},
    )


def requests_of(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, sequence, status, terminal_cause, objective, requester, reply_kind, "
        "reply_address, reply_conversation_id, reply_target_kind, reply_target_pr_number, "
        "reply_target_url, runtime_epoch "
        "FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def lineage_of(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT l.id, l.branch, l.pr_number, l.pr_url, l.head_sha, l.status, l.version, "
        "l.code_host_kind, l.code_host_host, l.repository_project_id, l.code_host_pr_id, "
        "l.base_ref "
        "FROM curie.thread_publication_lineages l "
        "JOIN curie.work_items w ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def publication_lineage(publication_id: str) -> dict[str, Any]:
    """The lineage a publication advances, whether or not a WorkItem links it."""

    return _rows(
        "SELECT l.id, l.pr_number, l.pr_url, l.head_sha, l.status "
        "FROM curie.thread_publication_lineages l "
        "JOIN curie.publications p ON p.lineage_id = l.id WHERE p.id = :id",
        {"id": uuid.UUID(publication_id)},
    )[0]


def turns() -> list[dict[str, Any]]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


def drop_stream() -> None:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        valkey.delete(get_settings().runs_stream)
    finally:
        valkey.close()


def ci_turns(request_id: uuid.UUID) -> list[dict[str, Any]]:
    prefix = f"work-item-{request_id}-ci-"
    return [t for t in turns() if t["event_id"].startswith(prefix)]


def execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def start_request(request_id: uuid.UUID) -> int:
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
                    owner="gitlab-owner",
                    generation=generation,
                    wake=await factory_wake(session, request_id),
                    context=None,
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="gitlab-owner",
                    generation=generation,
                    claim_name="claim-gitlab",
                    sandbox_name="sbx-gitlab",
                    context=acquired.context,
                )
                assert not isinstance(started, DispatchConflict), started
                return int(started.runtime_epoch)
        finally:
            await engine.dispose()

    return asyncio.run(go())


# --- the worker's workspace and publication ---------------------------------------------


def deployment(install: GitLabInstall, agent_id: uuid.UUID) -> str:
    headers = {"X-API-Key": get_settings().api_key}
    version = install.client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    created = install.client.post(
        "/deployments",
        json={
            "agent_id": str(agent_id),
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def select_workspace(
    install: GitLabInstall, deployment_id: str, item: dict[str, Any], author: str
) -> httpx.Response:
    """The worker's selection: it sends the WorkItem's repository path and
    nothing else (`curie_worker.workspace.select`), never a kind."""

    return install.call(
        "POST",
        f"/v1/internal/workspaces/{deployment_id}/selection",
        json={
            "conversation_id": item["conversation_id"],
            "author": author,
            "repo_full_name": item["repository_path"],
        },
    )


def workspace_credential(
    install: GitLabInstall, deployment_id: str, item: dict[str, Any]
) -> httpx.Response:
    return install.call(
        "POST",
        f"/v1/internal/workspaces/{deployment_id}/credential",
        json={"conversation_id": item["conversation_id"]},
    )


def publish(
    install: GitLabInstall,
    deployment_id: str,
    item: dict[str, Any],
    request: dict[str, Any],
    *,
    title: str = "Keep the last field of a line",
    observed: dict[str, Any] | None = None,
    base_sha: str | None = None,
) -> httpx.Response:
    """The worker's publication for the running request, as the bundle sends it.

    A first publication is based on the default branch head; a revision on
    the merge request's current head, which the worker passes as ``base_sha``.
    """

    payload: dict[str, Any] = {
        "deployment_id": deployment_id,
        "conversation_id": item["conversation_id"],
        "reply_conversation_id": request["reply_conversation_id"],
        "repo_full_name": item["repository_path"],
        "author": request["requester"],
        "summary": "Publish the widget parser fix",
        "reply_kind": request["reply_kind"],
        "reply_channel": request["reply_address"],
        "reply_placeholder": f"gitlab-{item['tracker_issue_id']}",
        "dedupe_key": f"gitlab-publication-{uuid.uuid4().hex}",
        "base_sha": base_sha or install.backend.default_head(),
        "work_item_request_id": str(request["id"]),
        "work_item_runtime_epoch": request["runtime_epoch"],
        "patch_b64": PATCH_B64,
        "changed_paths": ["README.md"],
        "expires_in_seconds": 600,
        "title": title,
        "body": "Parsing a three-field line now yields three fields.",
    }
    if observed is not None:
        payload.update(observed)
    return install.call("POST", "/v1/internal/publications", json=payload)


def approve(publication_id: str) -> None:
    """The approval the publication Job path records (not offline)."""

    execute(
        "UPDATE curie.publications SET status = 'approved' WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )


def outcome_durable(publication_id: str) -> None:
    """The worker's publication loop marking the outcome durable in thread history."""

    execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(publication_id)},
    )


def publication_credential(install: GitLabInstall, publication_id: str) -> httpx.Response:
    return install.call("POST", f"/v1/internal/publications/{publication_id}/credential")


def recover_pull_request(
    install: GitLabInstall, publication_id: str, head_sha: str
) -> httpx.Response:
    return install.call(
        "POST",
        f"/v1/internal/publications/{publication_id}/pull-request",
        json={"expected_head_sha": head_sha, "adopt_only": False},
    )


def advance_lineage(
    install: GitLabInstall,
    publication: dict[str, Any],
    *,
    pr_number: int,
    pr_url: str,
    head_sha: str,
    expected_head_sha: str | None,
) -> httpx.Response:
    """The publication Job's lineage advance through its route, after it took
    the publication's lease (taken here by SQL)."""

    owner = "gitlab-lifecycle-publisher"
    execute(
        "UPDATE curie.publications SET lease_owner = :owner, "
        "lease_expires_at = now() + interval '1 minute', version = version + 1 WHERE id = :id",
        {"id": uuid.UUID(publication["id"]), "owner": owner},
    )
    version = _rows(
        "SELECT version FROM curie.publications WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )[0]["version"]
    return install.call(
        "PATCH",
        f"/v1/internal/publications/{publication['id']}/lineage",
        json={
            "expected_version": publication["lineage_version"],
            "expected_head_sha": expected_head_sha,
            "expected_publication_version": version,
            "lease_owner": owner,
            "state": "open",
            "pr_number": pr_number,
            "pr_url": pr_url,
            "head_sha": head_sha,
            "metadata_updated_at": None,
        },
    )


def read_lineage(
    install: GitLabInstall, deployment_id: str, item: dict[str, Any]
) -> httpx.Response:
    """The worker's lineage read before a turn, which refreshes the stored MR."""

    return install.call(
        "GET",
        "/v1/internal/publications/lineage",
        params={
            "deployment_id": deployment_id,
            "conversation_id": item["conversation_id"],
            "repo_full_name": item["repository_path"],
        },
    )


def mint_precheck(
    install: GitLabInstall,
    deployment_id: str,
    item: dict[str, Any],
    request_id: uuid.UUID,
    epoch: int,
    queued_event_id: str,
) -> httpx.Response:
    return install.call(
        "POST",
        "/v1/internal/publications/precheck/context",
        json={
            "deployment_id": deployment_id,
            "work_item_id": str(item["id"]),
            "execution_request_id": str(request_id),
            "runtime_epoch": epoch,
            "queued_event_id": queued_event_id,
        },
    )


# --- the lifecycle, on either backend ----------------------------------------------------


def reconcile() -> None:
    """One real reconciler pass: `run_once` then the status note sync."""

    # Imported here: test modules are siblings on sys.path, not a package.
    from test_factory_terminus import _reconcile

    _reconcile()


def basic_oauth2(token: str) -> str:
    return "Basic " + base64.b64encode(f"oauth2:{token}".encode()).decode()


def review_body(install: GitLabInstall) -> str:
    return f"@{install.backend.bot.login} please rename the helper before merge"


def mention_body(install: GitLabInstall) -> str:
    return f"@{install.backend.bot.login} please also handle tab separated lines"


def admit(install: GitLabInstall, title: str = "Widget parser drops the last field") -> int:
    """A developer's issue, labelled by the developer and admitted by the poll."""

    backend = install.backend
    iid = backend.add_issue(
        title, "Parsing a three-field line yields two fields.", author=backend.developer
    )
    backend.label_issue(iid, install.label, backend.developer)
    reconcile()
    assert len(work_items(iid)) == 1, work_items(iid)
    return iid


def no_token_anywhere(install: GitLabInstall, caplog: pytest.LogCaptureFixture) -> None:
    token = install.backend.token
    assert token not in caplog.text
    for response in install.responses:
        assert token not in response.text, response.request.url
    assert token not in json.dumps(turns())
    assert all(token not in text for text in install.backend.published_texts())


def run_label_to_merge_request_ci_review_and_cancel(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    backend = install.backend
    developer = backend.developer
    iid = backend.add_issue(
        "Widget parser drops the last field",
        "Parsing a three-field line yields two fields.",
        author=developer,
    )

    # 1. The developer labels the issue; the real poll pass admits one WorkItem
    #    on the bound project, replying on the gitlab surface.
    backend.label_issue(iid, install.label, developer)
    reconcile()
    [item] = work_items(iid)
    assert (
        item["tracker_host"],
        item["tracker_scope_id"],
        item["tracker_display_key"],
        item["trigger_label"],
    ) == (install.host, str(backend.project_id), f"{backend.project_path}#{iid}", install.label)
    assert (
        item["code_host_kind"],
        item["code_host_host"],
        item["repository_project_id"],
        item["repository_path"],
    ) == (types.GITLAB, install.host, str(backend.project_id), backend.project_path)
    assert item["code_host_installation_id"] is None
    assert item["base_branch"] == "main"
    assert item["base_commit"] == backend.default_head()
    assert item["agent_id"] == install.agent_id
    [first] = requests_of(item["id"])
    assert first["objective"].splitlines()[0] == install.issue_url(iid)
    assert (first["reply_kind"], first["reply_address"], first["reply_conversation_id"]) == (
        "gitlab",
        backend.project_path,
        f"issue-{iid}",
    )
    assert first["requester"] == f"gitlab:{developer.id}:{developer.login}"
    assert first["status"] == "waiting"
    assert [t for t in turns() if str(first["id"]) in t["event_id"]]
    [note] = install.bot_notes(iid)
    assert install.state_labels(iid) == {"curie-factory:queued"}
    posted = note.body

    # 2. The run starts: the same note is edited in place, the state label moves.
    epoch = start_request(first["id"])
    reconcile()
    assert requests_of(item["id"])[0]["status"] == "running"
    [same] = install.bot_notes(iid)
    assert same.id == note.id and same.body != posted
    assert install.state_labels(iid) == {"curie-factory:running"}

    # 3. The workspace: the worker selects the WorkItem's path (no kind) and is
    #    issued the GitLab code host's clone credential, Basic oauth2.
    deployment_id = deployment(install, item["agent_id"])
    selected = select_workspace(install, deployment_id, item, first["requester"])
    assert selected.status_code == 200, selected.text
    assert selected.json()["repo_full_name"] == backend.project_path
    clone = workspace_credential(install, deployment_id, item)
    assert clone.status_code == 200, clone.text
    issued = clone.json()
    assert issued["clone_url"] == f"{install.origin}/{backend.project_path}.git"
    assert issued["origin"] == install.origin
    assert issued["header_form"] == types.CredentialHeader.AUTHORIZATION_BASIC.value
    assert issued["authorization_header"] == basic_oauth2(backend.token)
    assert (issued["base_branch"], issued["base_commit"]) == ("main", item["base_commit"])

    # 4. The publication carries the closing reference; its push credential is
    #    the GitLab code host's; the merge request opens through the recover route
    #    and the lineage records the GitLab MR URL.
    request_row = {**requests_of(item["id"])[0], "runtime_epoch": epoch}
    created = publish(install, deployment_id, item, request_row)
    assert created.status_code == 201, created.text
    publication = created.json()
    assert publication["body"].count(f"Closes #{iid}") == 1
    approve(publication["id"])
    push = publication_credential(install, publication["id"])
    assert push.status_code == 200, push.text
    assert push.json()["clone_url"] == f"{install.origin}/{backend.project_path}.git"
    assert push.json()["authorization_header"] == basic_oauth2(backend.token)
    branch = publication["branch"]
    head = backend.push(branch, f"Fix the widget parser\n\nCurie-Revision: {publication['id']}")
    recovered = recover_pull_request(install, publication["id"], head)
    assert recovered.status_code == 200, recovered.text
    pull = recovered.json()
    mr = install.merge_request(branch)
    assert (pull["number"], pull["url"], pull["head_sha"]) == (mr.iid, install.mr_url(mr.iid), head)
    assert (mr.source_branch, mr.target_branch) == (branch, "main")
    assert f"Closes #{iid}" in mr.description
    advanced = advance_lineage(
        install,
        publication,
        pr_number=mr.iid,
        pr_url=pull["url"],
        head_sha=head,
        expected_head_sha=None,
    )
    assert advanced.status_code == 200, advanced.text
    lineage = lineage_of(item["id"])
    assert (lineage["pr_number"], lineage["pr_url"], lineage["head_sha"], lineage["status"]) == (
        mr.iid,
        install.mr_url(mr.iid),
        head,
        "open",
    )
    # The lineage records the GitLab identity, which is what links it to the
    # WorkItem (installation is GitHub's alone).
    assert (
        lineage["code_host_kind"],
        lineage["code_host_host"],
        lineage["repository_project_id"],
        lineage["base_ref"],
    ) == (types.GITLAB, install.host, str(backend.project_id), "main")
    assert lineage["code_host_pr_id"]
    # The worker's lineage read refreshes the stored MR through the GitLab code host.
    read = read_lineage(install, deployment_id, item)
    assert read.status_code == 200, read.text
    assert (read.json()["pr_url"], read.json()["head_sha"]) == (install.mr_url(mr.iid), head)

    # 5. CI goes red on the head. GitLab can rerun a failed job, so the gate
    #    retries it once; when the rerun fails too, a fix round is asked for.
    failed_job = backend.ci_fails(head)
    reconcile()
    assert ci_turns(first["id"]) == []
    backend.rerun_fails(head, failed_job)
    reconcile()
    assert requests_of(item["id"])[0]["status"] == "running"
    fix_turns = ci_turns(first["id"])
    assert [t["event_id"] for t in fix_turns] == [f"work-item-{first['id']}-ci-2"]
    assert install.mr_url(mr.iid) in fix_turns[0]["text"]
    assert head in fix_turns[0]["text"]

    #    The fix round publishes a revision of the same MR the way the worker does:
    #    lineage read, precheck mint (read through the GitLab code host), the
    #    revision publication, the push, the lineage advance. Then CI is green.
    assert read_lineage(install, deployment_id, item).status_code == 200
    minted = mint_precheck(
        install, deployment_id, item, first["id"], epoch, fix_turns[0]["event_id"]
    )
    assert minted.status_code == 200, minted.text
    context = minted.json()
    assert context["observed_title"] == publication["title"]
    outcome_durable(publication["id"])
    revised = publish(
        install,
        deployment_id,
        item,
        request_row,
        title="Fix the widget parser off-by-one",
        base_sha=head,
        observed={
            "observed_title": context["observed_title"],
            "observed_body_sha256": context["observed_body_sha256"],
            "observed_lineage_id": context["lineage_id"],
            "observed_lineage_version": context["lineage_version"],
        },
    )
    assert revised.status_code == 201, revised.text
    revision_publication = revised.json()
    approve(revision_publication["id"])
    fix_head = backend.push(
        branch,
        f"Fix the widget parser off-by-one\n\nCurie-Revision: {revision_publication['id']}",
        fix=True,
    )
    fixed = advance_lineage(
        install,
        revision_publication,
        pr_number=mr.iid,
        pr_url=install.mr_url(mr.iid),
        head_sha=fix_head,
        expected_head_sha=head,
    )
    assert fixed.status_code == 200, fixed.text
    backend.ci_passes(fix_head)
    reconcile()
    assert requests_of(item["id"])[0]["status"] == "completed"
    assert len(ci_turns(first["id"])) == 1
    [final] = install.bot_notes(iid)
    assert final.id == note.id
    assert install.mr_url(mr.iid) in final.body
    assert install.state_labels(iid) == {"curie-factory:pr-open"}

    # 6. A developer's diff comment becomes a revision, answered by exactly one
    #    reply in its discussion.
    discussion = backend.mr_discussion(mr.iid, developer, review_body(install))
    reconcile()
    rows = requests_of(item["id"])
    assert len(rows) == 2, rows
    revision = rows[1]
    assert (revision["reply_kind"], revision["reply_address"]) == ("gitlab", backend.project_path)
    assert revision["reply_target_kind"] == "review_thread"
    assert revision["reply_target_pr_number"] == str(mr.iid)
    assert revision["status"] == "waiting"
    assert install.mr_url(mr.iid) in revision["objective"]
    assert revision["requester"].startswith(f"gitlab:{developer.id}:")
    reconcile()  # the same note again admits nothing new
    assert len(requests_of(item["id"])) == 2
    assert len(work_items(iid)) == 1
    start_request(revision["id"])
    reconcile()
    reconcile()
    replies = backend.thread_replies(mr.iid, discussion)
    assert [n.author for n in replies] == [backend.bot.id]

    # 7. Removing the label cancels the WorkItem.
    backend.label_issue(iid, install.label, developer, add=False)
    reconcile()
    [cancelled] = work_items(iid)
    assert cancelled["cancelled_at"] is not None
    assert all(r["status"] not in {"waiting", "running"} for r in requests_of(item["id"]))

    # The token never left the API: not in a log line, a raised error's log, a
    # route answer, a turn, a note or the MR.
    no_token_anywhere(install, caplog)


def run_reporter_label_admits_nothing(install: GitLabInstall) -> None:
    backend = install.backend
    iid = backend.add_issue("Widget parser drops a quoted field", "Quoted commas split.")

    backend.label_issue(iid, install.label, backend.reporter)
    reconcile()
    reconcile()

    assert work_items(iid) == []
    assert install.state_labels(iid) == set()
    assert install.bot_notes(iid) == []

    # Liveness: the developer's label on the next issue is admitted.
    other = admit(install, "Widget parser drops a trailing field")
    assert install.state_labels(other) == {"curie-factory:queued"}


def run_two_surfaces_admit_only_their_own_label(install: GitLabInstall) -> None:
    backend = install.backend
    (fast,) = [label for label in install.agents if label is not None]
    plain = backend.add_issue("Widget parser drops the last field")
    quick = backend.add_issue("Widget parser drops a quoted field")

    backend.label_issue(plain, install.label, backend.developer)
    backend.label_issue(quick, fast, backend.developer)
    reconcile()
    reconcile()

    [plain_item] = work_items(plain)
    [quick_item] = work_items(quick)
    assert (plain_item["agent_id"], plain_item["trigger_label"]) == (
        install.agents[None],
        install.label,
    )
    assert (quick_item["agent_id"], quick_item["trigger_label"]) == (install.agents[fast], fast)

    # A second surface with the same project and label is refused, whether the
    # label is named or is the tracker's own; another label binds.
    for label in (None, install.label, fast):
        refused = create_agent(install, label)
        assert refused.status_code == 409, (label, refused.text)
    assert create_agent(install, f"{install.label}-slow").status_code == 201


def run_issue_mention_links_the_note(install: GitLabInstall) -> None:
    from curie_api.factory_reply_target import is_issue_follow_up
    from curie_api.forges.trackers import comment_prefix

    backend = install.backend
    iid = admit(install)
    [item] = work_items(iid)

    # A reporter's mention starts nothing.
    backend.issue_note(iid, backend.reporter, mention_body(install))
    reconcile()
    assert len(requests_of(item["id"])) == 1

    note_id = backend.issue_note(iid, backend.developer, mention_body(install))
    reconcile()
    rows = requests_of(item["id"])
    assert len(rows) == 2, rows
    link = install.issue_url(iid)
    assert rows[1]["objective"].splitlines()[0] == f"{link}#note_{note_id}"
    assert is_issue_follow_up(
        rows[1]["objective"], issue_link=link, comment_prefix=comment_prefix(types.GITLAB)
    )


def run_failures_log_and_answer_no_token(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    backend = install.backend
    iid = admit(install)
    [item] = work_items(iid)
    [first] = requests_of(item["id"])
    start_request(first["id"])
    deployment_id = deployment(install, item["agent_id"])
    assert select_workspace(install, deployment_id, item, first["requester"]).status_code == 200

    # The token stops working (on the fake every GitLab error body also quotes
    # the presented token and URL): each pass and route fails, and none repeats it.
    backend.break_credential()
    other = backend.add_issue("Widget parser drops a trailing field")
    backend.label_issue(other, install.label, backend.developer)
    reconcile()
    refused = workspace_credential(install, deployment_id, item)
    assert refused.status_code == 502, refused.text

    assert work_items(other) == []
    no_token_anywhere(install, caplog)
    backend.credential_was_refused()
