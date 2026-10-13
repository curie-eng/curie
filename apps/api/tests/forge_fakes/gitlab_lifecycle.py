"""A gitlab+gitlab install booted on the GitLab fake, for the offline lifecycle (#4399 AC 8).

`forge_fakes.gitlab.GitLabAPI` is served by uvicorn on 127.0.0.1 on an
ephemeral port, so every HTTP client the API makes (the app's shared client,
the reconciler's own) reaches it over a socket. ``CURIE_FORGES`` holds only the
GitLab tracker, code host and binding `forge_fakes.gitlab.forges_document`
writes, pointed at that port and validated and built by the production
builder before the app boots. Nothing GitHub is configured: no App, no token,
no ``GITHUB_REPO_ALLOWLIST``. No seam inside the API is replaced; in
particular the WorkItem's code host is resolved by the core itself
(`forges.hosts`, `forges.trackers.code_host_for_repository` are untouched).

`gitlab_install` is a fixture body (a generator); the helpers below read and
drive the install the way the worker and the publication Job do, through the
internal HTTP routes. Two shortcuts sit outside the factory core, as in
``tests/forges/test_jira_github_lifecycle.py``: a publication's approval is
recorded by SQL (the approval Job path is not offline), and a publication's
lease is taken by SQL before its lineage advance, which itself goes through
``PATCH /v1/internal/publications/{id}/lineage``. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import copy
import json
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


# --- serving the fake -----------------------------------------------------------------


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


# --- the install ----------------------------------------------------------------------


@dataclass
class GitLabInstall:
    client: TestClient
    fake: gitlab_fake.GitLabAPI
    origin: str  # the fake's web base, http://127.0.0.1:<port>
    host: str  # the configured host, 127.0.0.1:<port>
    agents: dict[str | None, uuid.UUID]  # trigger label (None: the tracker's) -> agent
    responses: list[httpx.Response] = field(default_factory=list)

    @property
    def agent_id(self) -> uuid.UUID:
        return self.agents[None]

    def issue_ref(self, iid: int) -> TrackerIssueRef:
        return TrackerIssueRef(
            types.GITLAB, self.host, str(PROJECT_ID), str(iid), f"{PROJECT_PATH}#{iid}"
        )

    def issue_url(self, iid: int) -> str:
        return f"{self.origin}/{PROJECT_PATH}/-/issues/{iid}"

    def mr_url(self, iid: int) -> str:
        return f"{self.origin}/{PROJECT_PATH}/-/merge_requests/{iid}"

    def call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """One worker call to an internal route; every answer is kept so the
        test can prove no response carries the token."""

        kwargs.setdefault("headers", WORKER)
        response = self.client.request(method, path, **kwargs)
        self.responses.append(response)
        return response

    # GitLab observations -------------------------------------------------------

    def bot_notes(self, iid: int) -> list[Any]:
        """The factory's own notes on issue ``iid`` (its status notes)."""

        return [n for n in self.fake.issue_notes(iid) if n.author == int(BOT.id)]

    def state_labels(self, iid: int) -> set[str]:
        return set(self.fake.issues[iid].labels) & STATE_LABELS

    def merge_request(self) -> Any:
        (mr,) = self.fake.merge_requests.values()
        return mr


def gitlab_document(origin: str, host: str) -> dict[str, Any]:
    """The GitLab tracker, code host and binding on the served fake; nothing else."""

    return gitlab_fake.forges_document(api_url=f"{origin}/api/v4", host=host)


def _seed_people(fake: gitlab_fake.GitLabAPI) -> None:
    fake.add_user(DEVELOPER)
    fake.set_member(DEVELOPER, 30)
    fake.add_user(REPORTER)
    fake.set_member(REPORTER, 20)


def _create_agent(client: TestClient, label: str | None) -> httpx.Response:
    channel: dict[str, Any] = {"kind": "gitlab", "address": PROJECT_PATH}
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
) -> Iterator[GitLabInstall]:
    """Boot a gitlab-only install on a served fake; one agent per surface label.

    ``labels`` names each agent's ``gitlab=<path>`` surface trigger label; None
    serves the tracker's own label (``factory``).
    """

    fake, served = serve_fake()
    host = served.url.removeprefix("http://")
    document = gitlab_document(served.url, host)
    # Validated and built by the production builder before the app boots on it.
    registry = build_registry(ForgesConfig.model_validate(copy.deepcopy(document)))
    assert registry.code_host(gitlab_fake.CODE_HOST_NAME).config.api_url == f"{served.url}/api/v4"
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
        # A non-default secret: the published dev secret is refused at boot,
        # and this install configures no GitHub webhook at all.
        "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": "30",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "INTERNAL_WORKER_TOKEN": WORKER["X-Curie-Worker-Token"],
        "RUNS_STREAM": f"test:curie:gitlab-lifecycle:{uuid.uuid4().hex}",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _seed_people(fake)
    try:
        with TestClient(create_app()) as client:
            agents: dict[str | None, uuid.UUID] = {}
            for label in labels:
                created = _create_agent(client, label)
                assert created.status_code == 201, created.text
                agents[label] = uuid.UUID(created.json()["id"])
            try:
                yield GitLabInstall(client, fake, served.url, host, agents)
            finally:
                _clear_ci_keys()
                drop_stream()
    finally:
        served.stop()
        get_settings.cache_clear()


def create_agent(install: GitLabInstall, label: str | None) -> httpx.Response:
    """One more agent on the project's ``gitlab=`` surface under ``label``."""

    return _create_agent(install.client, label)


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
                    session, request_id, owner="gitlab-owner", generation=generation
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="gitlab-owner",
                    generation=generation,
                    claim_name="claim-gitlab",
                    sandbox_name="sbx-gitlab",
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
) -> httpx.Response:
    """The worker's publication for the running request, as the bundle sends it."""

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
        "base_sha": install.fake.branches[gitlab_fake.DEFAULT_BRANCH],
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


def publication_credential(install: GitLabInstall, publication_id: str) -> httpx.Response:
    return install.call("POST", f"/v1/internal/publications/{publication_id}/credential")


def recover_pull_request(
    install: GitLabInstall, publication_id: str, head_sha: str
) -> httpx.Response:
    return install.call(
        "POST",
        f"/v1/internal/publications/{publication_id}/pull-request",
        json={"expected_head_sha": head_sha},
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
