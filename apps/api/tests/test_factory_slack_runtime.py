"""Durable factory Slack run progress (#5414).

The runtime boundary: the real ``create_app()`` lifespan (disposable migrated
Postgres, real Valkey, real RustFS) served on a 127.0.0.1 socket by uvicorn,
whose agent and internal WorkItem routes the tests drive, and the production
``FactorySlackProgressLoop`` over ``PostgresFactorySlackStore`` and
``SlackReplyAdapter``. Revision admission is the production
``workitem_dispatch.admit_revision``, and phase reports come from the
production progress route and the production CI rerun writer.

Only external services are substituted:

1. The Slack Web API is an aiohttp server answering the documented shape of
   https://docs.slack.dev/reference/methods/chat.postMessage (``ok``,
   ``channel``, ``ts``). A refused send answers ``{"ok": false, "error":
   "not_in_channel"}`` on demand, a documented chat.postMessage error.
2. GitHub REST is ``forge_fakes.github.GitHubAPI``, installed as the API's
   outbound client.

Slack message escaping:
https://docs.slack.dev/messaging/formatting-message-text#escaping-text
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import itertools
import json
import secrets
import socket
import sys
import threading
import time
import urllib.parse
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_api import factory_progress
from curie_api.config import get_settings
from curie_api.factory_ci import RERUN_REQUESTED_NOTE, rerun_refused_note
from curie_api.forges.types import RepositoryRef, TrackerIssueRef
from curie_api.main import create_app
from curie_api.models import Approval, Publication
from curie_api.workitem_dispatch import Admission, admit_revision
from curie_api.workitems import lifecycle
from curie_internal import sandbox_token
from curie_test_support.valkey import connect_or_skip
from curie_worker.factory_slack_progress import (
    FactorySlackProgressLoop,
    PostgresFactorySlackStore,
)
from curie_worker.reply_sink import build_reply_sink
from curie_worker.run import _build_factory_slack_progress
from curie_worker.slack_sink import SlackReplyAdapter, UnconfiguredSlackIdentityError
from fastapi.testclient import TestClient
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    INSTALLATION_ID,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubAPI,
    factory_env,
    fake_installation_tokens,
)

from apps.worker.tests.kernel.conftest import make_config  # noqa: E402

WORKER_TOKEN = "factory-slack-worker-token"
DEFAULT_TOKEN = "xoxb-EXAMPLE"
CHANNEL = "C0EXAMPLE1"
FACTORY_BOT_ID = "B0EXAMPLE1"
HEAD = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
OWNER = "factory-slack-owner"

DECLARATION: dict[str, Any] = {
    "phases": [
        {"id": "plan", "label": "Plan"},
        {"id": "implement", "label": "Implement"},
        {"id": "review_diff", "label": "Review diff"},
    ],
    "loops": [{"start": "implement", "review": "review_diff", "cap": 3}],
}
ACTIVITY: dict[str, Any] = {
    "model": "glm-5.3-flash",
    "turns": 4,
    "tool_calls": 9,
    "last_tool": "Bash",
}

_ISSUES = itertools.count(5401)
_PULLS = itertools.count(7401)


# --- Slack Web API stand-in ----------------------------------------------------


@dataclass
class SlackCall:
    method: str
    params: dict[str, Any]
    token: str | None
    response: dict[str, Any]

    @property
    def ok(self) -> bool:
        return self.response.get("ok") is True

    @property
    def text(self) -> str:
        return str(self.params.get("text") or "")

    @property
    def haystack(self) -> str:
        """Everything a reader sees: the text and any Block Kit content."""

        blocks = self.params.get("blocks")
        if isinstance(blocks, str):
            return f"{self.text}\n{blocks}"
        return f"{self.text}\n{json.dumps(blocks)}" if blocks else self.text


class FakeSlack:
    """Records every Web API call with its method, body and bot token."""

    def __init__(self) -> None:
        self.calls: list[SlackCall] = []
        self.base_url = ""
        self._failures: list[tuple[str, str | None, str]] = []
        self._lock = threading.Lock()
        self._ts = itertools.count(1)
        # Set by the receiver as calls arrive, for a waiter on another thread.
        self.chat_called = threading.Event()
        self.thread_replied = threading.Event()

    def fail_next(self, method: str, *, token: str | None, error: str = "not_in_channel") -> None:
        with self._lock:
            self._failures.append((method, token, error))

    def mint_ts(self) -> str:
        return f"1800000000.{next(self._ts):06d}"

    async def handle(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        params: dict[str, Any] = dict(request.query)
        raw = await request.read()
        if raw:
            if request.content_type == "application/json":
                params.update(json.loads(raw))
            else:
                params.update(dict(urllib.parse.parse_qsl(raw.decode())))
        authorization = request.headers.get("Authorization", "")
        token = authorization.removeprefix("Bearer ") if authorization else params.get("token")
        response: dict[str, Any] | None = None
        with self._lock:
            for index, (failing, only, error) in enumerate(self._failures):
                if failing == method and (only is None or only == token):
                    del self._failures[index]
                    response = {"ok": False, "error": error}
                    break
        if response is None:
            response = self._answer(method, params)
        self.calls.append(SlackCall(method, params, token, response))
        if method.startswith("chat."):
            self.chat_called.set()
            if method == "chat.postMessage" and response.get("ok") and params.get("thread_ts"):
                self.thread_replied.set()
        return web.json_response(response)

    def _answer(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "chat.postMessage":
            ts = self.mint_ts()
            return {
                "ok": True,
                "channel": params.get("channel"),
                "ts": ts,
                "message": {"ts": ts, "text": params.get("text"), "bot_id": FACTORY_BOT_ID},
            }
        return {"ok": True}

    # Views -----------------------------------------------------------------

    def delivered_posts(self, token: str) -> list[SlackCall]:
        return [
            call
            for call in self.calls
            if call.method == "chat.postMessage" and call.ok and call.token == token
        ]

    def roots(self, token: str) -> list[SlackCall]:
        return [call for call in self.delivered_posts(token) if not call.params.get("thread_ts")]

    def thread_posts(self, token: str, root_ts: str) -> list[SlackCall]:
        return [
            call for call in self.delivered_posts(token) if call.params.get("thread_ts") == root_ts
        ]


# --- worker side -------------------------------------------------------------------


@dataclass
class Worker:
    engine: Any
    progress: FactorySlackProgressLoop
    progress_factory: Callable[[Any], FactorySlackProgressLoop]
    # Disposal of a restarted loop's engine finishes at teardown, off the
    # test's clock: closing an asyncpg pool waits on each connection's close.
    retired: list[asyncio.Task[None]] = field(default_factory=list)


@contextlib.asynccontextmanager
async def _servers(app: Any, slack: FakeSlack) -> AsyncIterator[str]:
    import uvicorn

    slack_app = web.Application()
    slack_app.router.add_route("*", "/api/{method}", slack.handle)
    slack_server = TestServer(slack_app, host="127.0.0.1")
    await slack_server.start_server()
    slack.base_url = f"http://127.0.0.1:{slack_server.port}/api/"
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    server = uvicorn.Server(
        uvicorn.Config(app, lifespan="off", log_level="critical", access_log=False)
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                await asyncio.sleep(0)
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        except (TimeoutError, asyncio.CancelledError):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        sock.close()
        await slack_server.close()


@contextlib.asynccontextmanager
async def _worker(
    *, slack_url: str, identity: str, factory_token: str, lease_owner: str
) -> AsyncIterator[Worker]:
    engine = create_async_engine(get_settings().database_url)
    schema = get_settings().db_schema

    def progress_factory(progress_engine: Any) -> FactorySlackProgressLoop:
        return FactorySlackProgressLoop(
            store=PostgresFactorySlackStore(
                progress_engine, schema=schema, lease_owner=lease_owner
            ),
            replies=SlackReplyAdapter(
                DEFAULT_TOKEN, identity_tokens={identity: factory_token}, base_url=slack_url
            ),
            interval_seconds=0.05,
            batch_limit=50,
        )

    worker = Worker(engine, progress_factory(engine), progress_factory)
    try:
        yield worker
    finally:
        await asyncio.gather(*worker.retired, return_exceptions=True)
        await engine.dispose()


async def _drain_progress(worker: Worker, fresh: bool) -> None:
    """One progress pass, then deliver whatever is still due.

    ``fresh`` models a restarted process: a new engine, store and loop.
    """

    engine = create_async_engine(get_settings().database_url) if fresh else None
    loop = worker.progress_factory(engine) if engine is not None else worker.progress
    try:
        await loop.run_pass()
        for _ in range(100):
            if not await loop.deliver_once():
                break
    finally:
        if engine is not None:
            worker.retired.append(asyncio.create_task(engine.dispose()))


# --- database helpers ------------------------------------------------------------


@dataclass
class _SharedDb:
    """One engine for the test's own reads and fixture writes.

    It lives on the API's event loop (the TestClient portal) for the whole
    test, so a helper call costs one pooled query instead of an engine build
    and disposal, which is the slow part of a short-lived asyncpg engine.
    """

    portal: Any
    engine: AsyncEngine


_DB: _SharedDb | None = None


def _db[T](go: Callable[[AsyncEngine], Awaitable[T]]) -> T:
    assert _DB is not None, "the stack fixture owns the shared engine"
    return _DB.portal.call(go, _DB.engine)


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def go(engine: AsyncEngine) -> list[dict[str, Any]]:
        async with engine.begin() as connection:
            result = await connection.execute(text(statement), params or {})
            return [dict(row) for row in result.mappings()] if result.returns_rows else []

    return _db(go)


def _code(response: httpx.Response) -> str | None:
    body = response.json()
    if isinstance(body, dict):
        if isinstance(body.get("code"), str):
            return str(body["code"])
        detail = body.get("detail")
        if isinstance(detail, dict) and isinstance(detail.get("code"), str):
            return str(detail["code"])
    return None


# --- the stack ----------------------------------------------------------------------


@dataclass
class Run:
    number: int
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    epoch: int
    pr: int
    pr_url: str
    root_ts: str


class Stack:
    def __init__(
        self,
        *,
        client: TestClient,
        slack: FakeSlack,
        identity: str,
        factory_token: str,
        tag: str,
        agent_id: str,
        deployment_id: str,
    ) -> None:
        self.client = client
        self.slack = slack
        self.identity = identity
        self.factory_token = factory_token
        self.tag = tag
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self.worker: Worker

    # API surfaces ------------------------------------------------------------

    @property
    def platform(self) -> dict[str, str]:
        return {"X-API-Key": get_settings().api_key}

    @property
    def internal(self) -> dict[str, str]:
        return {"X-Curie-Worker-Token": WORKER_TOKEN}

    def admit(self, number: int, *, requester: str | None = None) -> tuple[uuid.UUID, uuid.UUID]:
        response = self.client.post(
            "/v1/internal/work-items/admissions",
            headers=self.internal,
            json={
                "agent_id": self.agent_id,
                "kind": "github",
                "address": REPO,
                "reply_conversation_id": f"issue-{number}",
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": str(REPO_ID),
                    "issue_id": str(number),
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": str(REPO_ID),
                    "path": REPO,
                },
                "code_host_installation_id": INSTALLATION_ID,
                "objective": f"Resolve acme-corp/acme-bot#{number}",
                "requester": requester or f"github:{SENDER_ID}:{SENDER}",
                "request_id": str(uuid.uuid4()),
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        return uuid.UUID(body["request"]["id"]), uuid.UUID(body["work_item"]["id"])

    def request_view(self, request_id: uuid.UUID) -> dict[str, Any]:
        response = self.client.get(
            f"/v1/internal/work-items/requests/{request_id}", headers=self.internal
        )
        assert response.status_code == 200, response.text
        return response.json()

    def start(self, request_id: uuid.UUID) -> int:
        view = self.request_view(request_id)
        generation = view["dispatch_generation"]
        acquired = self.client.post(
            f"/v1/internal/work-items/requests/{request_id}/acquire",
            headers=self.internal,
            json={
                "owner": OWNER,
                "generation": generation,
                "wake": {
                    "kind": view["reply_kind"],
                    "address": view["reply_address"],
                    "adapter": None,
                    "conversation_id": view["reply_conversation_id"],
                },
                "context": None,
            },
        )
        assert acquired.status_code == 200, acquired.text
        started = self.client.post(
            f"/v1/internal/work-items/requests/{request_id}/start",
            headers=self.internal,
            json={
                "owner": OWNER,
                "generation": generation,
                "claim_name": f"claim-{request_id.hex[:12]}",
                "sandbox_name": f"sbx-{request_id.hex[:12]}",
                "context": acquired.json()["context"],
            },
        )
        assert started.status_code == 200, started.text
        return int(started.json()["runtime_epoch"])

    def finish(self, request_id: uuid.UUID, epoch: int, *, published: bool = False) -> None:
        """End a running request as the worker's settlement does.

        The API completes a request only when it published on the WorkItem's
        lineage (``lifecycle._opened_pull_request``), so a completed run first
        records the succeeded publication the publication loop leaves behind.
        Every other run fails with ``runner_failed``, the worker's cause for a
        turn that ended in error.
        """

        if published:
            _seed_succeeded_publication(self, request_id)
            body = {"runtime_epoch": epoch, "outcome": "completed", "cause": "completed"}
        else:
            body = {"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_failed"}
        response = self.client.post(
            f"/v1/internal/work-items/requests/{request_id}/finish",
            headers=self.internal,
            json=body,
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == body["outcome"], response.text

    def report(self, request_id: uuid.UUID, phase: str, note: str) -> None:
        token = sandbox_token.mint(
            get_settings().api_key,
            agent=str(request_id),
            scope="work_item.progress",
            exp=int(time.time()) + 3600,
        )
        response = self.client.post(
            f"/v1/work-item-progress/{request_id}",
            headers={"X-API-Key": token},
            json={"phase": phase, "note": note, "declaration": DECLARATION, "activity": ACTIVITY},
        )
        assert response.status_code == 201, response.text

    def enable_destination(self, channel: str = CHANNEL) -> httpx.Response:
        return self.client.patch(
            f"/agents/{self.agent_id}",
            headers=self.platform,
            json={"factory_slack_channel": channel},
        )

    def own_pull_request(self, work_item_id: uuid.UUID, pr: int) -> tuple[uuid.UUID, str]:
        """Fixture state a factory publication leaves behind: an open PR lineage."""

        branch = f"curie/factory-acme-{pr}"
        lineage_id = uuid.uuid4()
        (item,) = _rows(
            "SELECT conversation_id FROM curie.work_items WHERE id = :id", {"id": work_item_id}
        )
        _rows(
            "INSERT INTO curie.thread_publication_lineages "
            "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
            "pr_number, pr_url, head_sha, status, version, latest_revision, code_host_kind, "
            "code_host_host, repository_project_id, code_host_installation_id, "
            "code_host_pr_id, base_ref) VALUES "
            "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, :pr, :url, "
            ":head, 'open', 1, 1, 'github', 'github.com', :repo_id, :installation, :node, 'main')",
            {
                "id": lineage_id,
                "agent": uuid.UUID(self.agent_id),
                "deployment": uuid.UUID(self.deployment_id),
                "conversation": item["conversation_id"],
                "repo": REPO,
                "base": BASE_SHA,
                "branch": branch,
                "pr": pr,
                "url": f"https://github.com/{REPO}/pull/{pr}",
                "head": HEAD,
                "repo_id": str(REPO_ID),
                "installation": INSTALLATION_ID,
                "node": f"PR_acme_{pr}",
            },
        )
        _rows(
            "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
            "version = version + 1 WHERE id = :id",
            {"lineage": lineage_id, "id": work_item_id},
        )
        return lineage_id, branch

    def open_run(self) -> Run:
        """A running factory request with an open PR and its posted Slack root."""

        number = next(_ISSUES)
        request_id, work_item_id = self.admit(number)
        epoch = self.start(request_id)
        pr = next(_PULLS)
        self.own_pull_request(work_item_id, pr)
        enabled = self.enable_destination()
        assert enabled.status_code == 200, enabled.text
        self.drain()
        (thread,) = self.threads(work_item_id)
        assert thread["root_ts"], thread
        return Run(
            number,
            work_item_id,
            request_id,
            epoch,
            pr,
            f"https://github.com/{REPO}/pull/{pr}",
            thread["root_ts"],
        )

    # Worker ----------------------------------------------------------------

    def drain(self, *, fresh: bool = False) -> None:
        self.client.portal.call(_drain_progress, self.worker, fresh)

    # Reads -----------------------------------------------------------------

    def requests(self) -> list[dict[str, Any]]:
        return _rows(
            "SELECT id, work_item_id, sequence, status, terminal_at "
            "FROM curie.execution_requests ORDER BY created_at, sequence"
        )

    def threads(self, work_item_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
        rows = _rows(
            "SELECT work_item_id, agent_id, channel_identity, channel, root_ts, "
            "first_sequence FROM curie.factory_slack_threads ORDER BY created_at"
        )
        if work_item_id is None:
            return rows
        return [row for row in rows if row["work_item_id"] == work_item_id]


def _seed_succeeded_publication(stack: Stack, request_id: uuid.UUID) -> None:
    """Fixture state a factory automatic publication leaves behind on the lineage.

    Publication is not a component this change touches. The row is settled in
    every outbox (approval card, result, history, cleanup) so no reconciler
    treats it as owed. Its approval carries the automatic policy identity
    (``approvals_policy_identity_ck``); ``revision_number`` is the next free one
    on the lineage (``uq_publications_lineage_revision``).
    """

    (row,) = _rows(
        "SELECT w.publication_lineage_id AS lineage_id, w.conversation_id, l.base_sha, "
        "l.pr_url, (SELECT count(*) FROM curie.publications p "
        "WHERE p.lineage_id = w.publication_lineage_id) AS published "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id = r.work_item_id "
        "JOIN curie.thread_publication_lineages l ON l.id = w.publication_lineage_id "
        "WHERE r.id = :id",
        {"id": request_id},
    )

    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            # Server time, in each column's own type.
            now = func.now()
            approval = Approval(
                agent_id=uuid.UUID(stack.agent_id),
                conversation_id=row["conversation_id"],
                author=f"github:{SENDER_ID}:{SENDER}",
                summary="Publish the factory change",
                reply_kind="github",
                reply_channel=REPO,
                dedupe_key=f"factory-slack-publication-{request_id}",
                status="approved",
                purpose="publication",
                policy_identity="publication:auto",
                policy_version=1,
                resolved_at=now,
                resumed_at=now,
            )
            session.add(approval)
            await session.flush()
            session.add(
                Publication(
                    approval_id=approval.id,
                    deployment_id=uuid.UUID(stack.deployment_id),
                    workspace_conversation_id=row["conversation_id"],
                    lineage_id=row["lineage_id"],
                    execution_request_id=request_id,
                    revision_number=int(row["published"]) + 1,
                    repo_full_name=REPO,
                    status="succeeded",
                    base_sha=row["base_sha"],
                    changed_paths=["README.md"],
                    title="Factory change",
                    body="Automatic factory publication.",
                    reply_kind="github",
                    reply_channel=REPO,
                    result_url=row["pr_url"],
                    approval_card_reported_at=now,
                    resource_cleanup_completed_at=now,
                    result_reported_at=now,
                    outcome_history_ready_at=now,
                    terminal_at=now,
                )
            )
            await session.commit()

    _db(go)


async def _shared_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url)


def _release_db() -> None:
    global _DB
    shared, _DB = _DB, None
    if shared is not None:
        shared.portal.call(shared.engine.dispose)


def _clear_valkey(valkey: redis.Redis, prefixes: list[str], keys: list[str]) -> None:
    doomed = [key for key in keys if key]
    for prefix in prefixes:
        doomed.extend(valkey.scan_iter(match=f"{prefix}*"))
    if doomed:
        valkey.delete(*doomed)


@pytest.fixture
def stack(clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Stack]:
    tag = secrets.token_hex(3)
    identity = f"acme-factory-{tag}"
    factory_token = f"xoxb-{tag}"
    api_stream = f"test:curie:factory-slack:{tag}:api-runs"
    env = {
        **factory_env(tmp_path, intake="webhook"),
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "RUNS_STREAM": api_stream,
        "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": "600",
        "CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS": "600",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        # The factory identity is declared but its token is not in this
        # process's env, so no static bootstrap row races the binding.
        "CURIE_SLACK_IDENTITIES": json.dumps(
            [
                {
                    "name": "default",
                    "app_token_env": "SLACK_APP_TOKEN",
                    "bot_token_env": "SLACK_BOT_TOKEN",
                    "signing_secret_env": "SLACK_SIGNING_SECRET",
                },
                {
                    "name": identity,
                    "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
                    "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
                    "signing_secret_env": None,
                },
            ]
        ),
        "CURIE_SLACK_BOT_TOKEN__0": "",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    fake_installation_tokens(monkeypatch)
    valkey = connect_or_skip(decode_responses=True)
    with ExitStack() as owned:
        # Cleanup is registered before anything starts.
        owned.callback(get_settings.cache_clear)
        owned.callback(valkey.close)
        owned.callback(lambda: _clear_valkey(valkey, [], [api_stream, f"{api_stream}:dead"]))
        client = owned.enter_context(TestClient(create_app()))
        global _DB
        _DB = _SharedDb(client.portal, client.portal.call(_shared_engine))
        owned.callback(_release_db)
        external = httpx.AsyncClient(transport=httpx.MockTransport(GitHubAPI().handle))
        owned.callback(client.portal.call, external.aclose)
        client.app.state.http_client = external
        slack = FakeSlack()
        owned.enter_context(client.portal.wrap_async_context_manager(_servers(client.app, slack)))
        auth = {"X-API-Key": get_settings().api_key}
        agent = client.post(
            "/agents",
            headers=auth,
            json={
                "name": f"acme-factory-{tag}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert agent.status_code == 201, agent.text
        agent_id = agent.json()["id"]
        bound = client.post(
            f"/agents/{agent_id}/channels",
            headers=auth,
            json={"kind": "slack", "address": CHANNEL, "adapter": identity},
        )
        assert bound.status_code == 201, bound.text
        version = client.post(
            f"/agents/{agent_id}/versions",
            headers=auth,
            json={"version_label": "fixture", "created_by": "operator"},
        )
        assert version.status_code == 201, version.text
        deployment = client.post(
            "/deployments",
            headers=auth,
            json={"agent_id": agent_id, "version_id": version.json()["id"], "environment": "dev"},
        )
        assert deployment.status_code == 201, deployment.text
        built = Stack(
            client=client,
            slack=slack,
            identity=identity,
            factory_token=factory_token,
            tag=tag,
            agent_id=agent_id,
            deployment_id=deployment.json()["id"],
        )
        built.worker = owned.enter_context(
            client.portal.wrap_async_context_manager(
                _worker(
                    slack_url=slack.base_url,
                    identity=identity,
                    factory_token=factory_token,
                    lease_owner=f"factory-slack-{tag}",
                )
            )
        )
        yield built


def _thread_texts(stack: Stack, root_ts: str) -> list[str]:
    return [call.haystack for call in stack.slack.thread_posts(stack.factory_token, root_ts)]


# --- AC1, AC5: durable progress ---------------------------------------------------


def test_progress_posts_once_in_one_root_across_revisions_failures_and_restarts(
    stack: Stack,
) -> None:
    """AC1, AC5: root, phases, PR link and terminals in one root, durably owed."""

    number = next(_ISSUES)
    first, work_item_id = stack.admit(number)
    first_epoch = stack.start(first)
    stack.report(first, "plan", "Drafting the plan")
    pr = next(_PULLS)
    _lineage, _branch = stack.own_pull_request(work_item_id, pr)
    pr_url = f"https://github.com/{REPO}/pull/{pr}"

    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text
    assert enabled.json()["factory_slack_channel"] == CHANNEL
    stack.drain()

    (root,) = stack.slack.roots(stack.factory_token)
    assert root.params["channel"] == CHANNEL
    assert f"#{number}" in root.haystack
    root_ts = root.response["ts"]
    (thread,) = stack.threads()
    assert (thread["work_item_id"], thread["channel_identity"], thread["channel"]) == (
        work_item_id,
        stack.identity,
        CHANNEL,
    )
    assert thread["root_ts"] == root_ts
    # Nothing was ever posted with another identity's token.
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []

    # A refused send stays owed and succeeds once the bot can post again.
    stack.slack.fail_next("chat.postMessage", token=stack.factory_token)
    stack.report(first, "implement", "<!channel> rebuild & retest")
    stack.drain()
    (report,) = _rows(
        "SELECT id FROM curie.execution_request_phase_reports "
        "WHERE execution_request_id = :id AND phase = 'implement'",
        {"id": first},
    )
    (owed,) = _rows(
        "SELECT attempts, posted_at, failed_at, last_error FROM curie.factory_slack_posts "
        "WHERE work_item_id = :id AND post_key = :key",
        {"id": work_item_id, "key": f"phase:{report['id']}"},
    )
    assert owed["posted_at"] is None and owed["failed_at"] is None, owed
    assert owed["attempts"] >= 1
    assert [c for c in stack.slack.calls if c.method == "chat.postMessage" and not c.ok]
    # Skip the retry backoff rather than sleep through it.
    _rows(
        "UPDATE curie.factory_slack_posts SET not_before = now() - interval '1 second' "
        "WHERE work_item_id = :id AND posted_at IS NULL",
        {"id": work_item_id},
    )
    stack.drain()

    # A revision admitted while the first run is live is queued, then promoted.
    revision = uuid.uuid4()
    _admit_revision_directly(stack, number, revision)
    stack.finish(first, first_epoch, published=True)
    _promote(work_item_id)
    assert {r["id"]: r["status"] for r in stack.requests()}[revision] == "waiting"
    stack.drain(fresh=True)
    revision_epoch = stack.start(revision)
    stack.report(revision, "review_diff", "Reviewing the revision")
    stack.finish(revision, revision_epoch, published=True)
    stack.drain(fresh=True)

    texts = _thread_texts(stack, root_ts)
    assert len(texts) == 6, texts
    assert "Drafting the plan" in texts[0]
    assert pr_url in texts[1]
    assert "&lt;!channel&gt; rebuild &amp; retest" in texts[2]
    # texts[3] is the first run's terminal, before any revision post.
    assert "Reviewing the revision" in texts[4]
    # texts[5] is the revision's terminal.
    for call in stack.slack.delivered_posts(stack.factory_token):
        assert "<!channel>" not in call.haystack
    assert len(stack.slack.roots(stack.factory_token)) == 1
    assert len(stack.threads()) == 1

    # A restarted process owes nothing: no second root, no repeated post.
    calls = len(stack.slack.calls)
    stack.drain(fresh=True)
    assert [c for c in stack.slack.calls[calls:] if c.method.startswith("chat.")] == []
    # The first eligible sequence was set once at discovery and never advances
    # with delivery; the post keys alone record what was already owed.
    (resumed,) = stack.threads()
    assert resumed["root_ts"] == root_ts
    assert resumed["first_sequence"] == thread["first_sequence"]

    # The tracker status row still records the run as before.
    (status,) = _rows(
        "SELECT declaration, activity FROM curie.factory_terminal_notices "
        "WHERE execution_request_id = :id",
        {"id": first},
    )
    assert status["declaration"] == DECLARATION
    assert status["activity"] == ACTIVITY


def _admit_revision_directly(stack: Stack, number: int, request_id: uuid.UUID) -> None:
    """Production revision admission, as review feedback reaches it."""

    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            result = await admit_revision(
                session,
                Admission(
                    agent_id=uuid.UUID(stack.agent_id),
                    kind="github",
                    address=REPO,
                    reply_conversation_id=f"issue-{number}",
                    issue=TrackerIssueRef("github", "github.com", str(REPO_ID), str(number)),
                    repository=RepositoryRef("github", "github.com", str(REPO_ID), REPO),
                    code_host_installation_id=INSTALLATION_ID,
                    objective="Revise the open pull request",
                    requester=f"github:{SENDER_ID}:{SENDER}",
                    request_id=request_id,
                ),
            )
            assert getattr(result, "request", None) is not None, result
            await session.commit()

    _db(go)


def _promote(work_item_id: uuid.UUID) -> None:
    """The reconciler's promotion step for one WorkItem's oldest queued revision."""

    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            now = await lifecycle.database_now(session)
            await lifecycle.admit_next_revision(
                session,
                work_item_id=work_item_id,
                wait_deadline=now + timedelta(seconds=get_settings().work_item_wait_budget_seconds),
            )

    _db(go)


def test_destination_enabled_mid_run_delivers_owed_posts_and_skips_runs_already_ended(
    stack: Stack,
) -> None:
    """R2.1, AC1, AC5: a run live at enablement is delivered with no loop running."""

    ended_number = next(_ISSUES)
    ended, ended_item = stack.admit(ended_number)
    ended_epoch = stack.start(ended)
    stack.report(ended, "plan", "Ended before the destination")
    stack.finish(ended, ended_epoch)

    live_number = next(_ISSUES)
    live, live_item = stack.admit(live_number)
    live_epoch = stack.start(live)
    stack.report(live, "plan", "Planned before the destination")
    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text
    stack.report(live, "implement", "Implemented after the destination")
    stack.finish(live, live_epoch)

    stack.drain(fresh=True)

    assert [row["work_item_id"] for row in stack.threads()] == [live_item]
    assert stack.threads(ended_item) == []
    (root,) = stack.slack.roots(stack.factory_token)
    assert f"#{live_number}" in root.haystack
    texts = _thread_texts(stack, root.response["ts"])
    assert len(texts) == 3, texts
    assert "Planned before the destination" in texts[0]
    assert "Implemented after the destination" in texts[1]
    for call in stack.slack.delivered_posts(stack.factory_token):
        assert "Ended before the destination" not in call.haystack
        assert f"#{ended_number}" not in call.haystack


def test_an_agent_without_a_destination_posts_nothing_and_an_unbound_channel_is_refused(
    stack: Stack,
) -> None:
    """AC1 negative: existing behavior without a destination; write-time refusal."""

    request_id, _work_item_id = stack.admit(next(_ISSUES))
    epoch = stack.start(request_id)
    stack.report(request_id, "plan", "No destination configured")
    stack.finish(request_id, epoch)
    stack.drain(fresh=True)

    assert stack.threads() == []
    assert _rows("SELECT id FROM curie.factory_slack_posts") == []
    assert stack.slack.calls == []
    (status,) = _rows(
        "SELECT declaration FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert status["declaration"] == DECLARATION

    refused = stack.enable_destination("C0EXAMPLE3")
    assert refused.status_code == 409, refused.text
    assert _code(refused) == "factory_slack_channel_unbound"
    agent = stack.client.get(f"/agents/{stack.agent_id}", headers=stack.platform)
    assert agent.status_code == 200, agent.text
    assert agent.json()["factory_slack_channel"] is None


@dataclass
class _LateCommit:
    early_id: int
    late_id: int
    texts_before_commit: list[str]


async def _late_commit_passes(
    worker: Worker, slack: FakeSlack, token: str, root_ts: str, request_id: uuid.UUID
) -> _LateCommit:
    """Two production phase writers whose commit order differs from their id order.

    Session A records the CI rerun note inside an outer transaction it has not
    committed (``record_ci_rerun``'s own commit releases a savepoint), so its
    report id is assigned first but stays invisible. Session B, another
    ``record_ci_rerun`` on the same running request, commits. ``record_ci_rerun``
    takes no request lock, so neither waits on the other.
    """

    assert _DB is not None
    engine = _DB.engine
    async with engine.connect() as connection:
        outer = await connection.begin()
        try:
            async with AsyncSession(
                bind=connection,
                join_transaction_mode="create_savepoint",
                expire_on_commit=False,
            ) as session_a:
                await factory_progress.record_ci_rerun(session_a, request_id, RERUN_REQUESTED_NOTE)
            async with async_sessionmaker(engine, expire_on_commit=False)() as session_b:
                await factory_progress.record_ci_rerun(
                    session_b, request_id, rerun_refused_note("rerun_rejected")
                )
            ids = {
                row.note: row.id
                for row in await connection.execute(
                    text(
                        "SELECT id, note FROM curie.execution_request_phase_reports "
                        "WHERE execution_request_id = :id AND phase = :phase"
                    ),
                    {"id": request_id, "phase": factory_progress.WAIT_CI_PHASE},
                )
            }
            await _drain_progress(worker, False)
            before = [call.haystack for call in slack.thread_posts(token, root_ts)]
            await outer.commit()
        except BaseException:
            await outer.rollback()
            raise
    await _drain_progress(worker, False)
    return _LateCommit(ids[RERUN_REQUESTED_NOTE], ids[rerun_refused_note("rerun_rejected")], before)


def test_a_phase_report_committed_after_a_higher_one_is_still_posted(stack: Stack) -> None:
    """AC1, AC5: no phase report is skipped when commits land out of id order.

    The progress route's ``record_report`` locks the request ``FOR UPDATE``,
    which waits on the uncommitted rerun row's foreign key ``FOR KEY SHARE``
    lock, so the route cannot commit ahead of session A. The production writer
    that can is a second ``record_ci_rerun`` on the same running request.

    Falsifiable: a cursor that advanced to the largest report id it had seen
    would move past the late report's id on the first pass (the higher report
    was the only one visible) and never select the early report again once it
    committed. Request sequence eligibility plus the per report anti-join
    finds it on the next pass.
    """

    run = stack.open_run()
    refused = rerun_refused_note("rerun_rejected")

    outcome = stack.client.portal.call(
        _late_commit_passes,
        stack.worker,
        stack.slack,
        stack.factory_token,
        run.root_ts,
        run.request_id,
    )

    # The interleaving this test needs: the early id is lower, and only the
    # higher report was visible to the first pass.
    assert outcome.early_id < outcome.late_id, outcome
    assert any(refused in text_ for text_ in outcome.texts_before_commit)
    assert not any(RERUN_REQUESTED_NOTE in text_ for text_ in outcome.texts_before_commit)

    texts = _thread_texts(stack, run.root_ts)
    assert sum(RERUN_REQUESTED_NOTE in text_ for text_ in texts) == 1, texts
    assert sum(refused in text_ for text_ in texts) == 1, texts
    assert len(stack.slack.roots(stack.factory_token)) == 1


# --- restart after a long publisher outage ----------------------------------------


def test_a_report_owed_across_a_long_outage_is_delivered_once_after_restart(
    stack: Stack,
) -> None:
    """AC1, AC5: owed posts survive any outage length, and a replay sends nothing.

    An age cutoff on ``terminal_at`` (for example, only requests that ended in
    the last hour) would permanently exclude this report and terminal: the run
    ended while no progress loop ran, and its end is now two hours old.
    Materialization is request sequence eligibility plus a per report anti
    join, with no age window.
    """

    run = stack.open_run()
    before = _thread_texts(stack, run.root_ts)
    assert len(before) == 1 and run.pr_url in before[0], before

    # No progress loop runs from here: the publisher is down.
    stack.report(run.request_id, "implement", "Implemented during the outage")
    stack.finish(run.request_id, run.epoch, published=True)
    # Fixture state: the run ended more than an hour before the publisher returns.
    _rows(
        "UPDATE curie.execution_requests SET terminal_at = now() - interval '2 hours' "
        "WHERE id = :id AND terminal_at IS NOT NULL",
        {"id": run.request_id},
    )
    (ended,) = _rows(
        "SELECT status, terminal_at < now() - interval '1 hour' AS old "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": run.request_id},
    )
    assert ended == {"status": "completed", "old": True}, ended

    # A restarted process: a new engine, store and loop.
    stack.drain(fresh=True)
    texts = _thread_texts(stack, run.root_ts)
    assert len(texts) == 3, texts
    assert "Implemented during the outage" in texts[1]
    assert "Run completed" in texts[2]
    assert len(stack.slack.roots(stack.factory_token)) == 1

    # Replay safe: a further pass, again from a fresh process, sends nothing.
    calls = len(stack.slack.calls)
    stack.drain(fresh=True)
    assert [c for c in stack.slack.calls[calls:] if c.method.startswith("chat.")] == []


# --- a worker without the factory identity's token ----------------------------------


async def _tokenless_progress_pass(worker: Worker, slack_url: str, lease_owner: str) -> None:
    """One production pass by a loop whose sink holds no factory identity token."""

    loop = FactorySlackProgressLoop(
        store=PostgresFactorySlackStore(
            worker.engine, schema=get_settings().db_schema, lease_owner=lease_owner
        ),
        replies=SlackReplyAdapter(DEFAULT_TOKEN, identity_tokens={}, base_url=slack_url),
        interval_seconds=0.05,
        batch_limit=50,
    )
    await loop.run_pass()


def test_a_worker_without_the_identity_token_leaves_the_root_owed_for_one_that_has_it(
    stack: Stack,
) -> None:
    """AC1, AC5: a sink missing the factory identity's token is not a permanent failure.

    Mixed rollout: a worker whose sink was built without the factory identity's
    bot token runs a pass. The send is refused before any Slack request
    (``UnconfiguredSlackIdentityError``), and the root stays owed with a
    backoff, so a worker that holds the token delivers it and every post behind
    it. A permanent ``failed_at`` here would strand the WorkItem's whole thread.
    """

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    stack.start(request_id)
    stack.report(request_id, "plan", "Planned on a mixed rollout")
    stack.own_pull_request(work_item_id, next(_PULLS))
    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text

    stack.client.portal.call(
        _tokenless_progress_pass, stack.worker, stack.slack.base_url, f"tokenless-{stack.tag}"
    )

    assert [c for c in stack.slack.calls if c.method.startswith("chat.")] == []
    (root,) = _rows(
        "SELECT attempts, posted_at, failed_at, last_error, not_before > now() AS backing_off "
        "FROM curie.factory_slack_posts WHERE work_item_id = :id AND post_key = 'root'",
        {"id": work_item_id},
    )
    assert root["posted_at"] is None and root["failed_at"] is None, root
    assert root["attempts"] == 1, root
    assert root["backing_off"] is True, root
    assert UnconfiguredSlackIdentityError.__name__ in (root["last_error"] or ""), root
    assert (
        _rows(
            "SELECT id FROM curie.factory_slack_posts "
            "WHERE work_item_id = :id AND failed_at IS NOT NULL",
            {"id": work_item_id},
        )
        == []
    )

    # A worker that holds the token: skip the backoff rather than sleep through it.
    _rows(
        "UPDATE curie.factory_slack_posts SET not_before = now() - interval '1 second' "
        "WHERE work_item_id = :id AND posted_at IS NULL",
        {"id": work_item_id},
    )
    stack.drain(fresh=True)

    (posted,) = stack.slack.roots(stack.factory_token)
    assert f"#{number}" in posted.haystack
    texts = _thread_texts(stack, posted.response["ts"])
    assert len(texts) == 2, texts
    assert "Planned on a mixed rollout" in texts[0]
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []


# --- a CI report on an ended request does not hide a later run's phase ---------------


def _record_ci_rerun(request_id: uuid.UUID, note: str) -> None:
    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            await factory_progress.record_ci_rerun(session, request_id, note)

    _db(go)


def test_a_ci_report_on_an_ended_request_does_not_hide_the_running_revisions_phase(
    stack: Stack,
) -> None:
    """AC1, AC5: report ids do not follow request order, so no id floor may gate posts.

    Request 1 runs and ends before the destination exists. Revision request 2
    is promoted and reports a phase through the production progress route.
    Then the production CI writer appends a report to request 1, which already
    ended, so that report's id is higher than request 2's. The destination is
    enabled while request 2 runs. A floor computed at discovery as the largest
    report id of the requests before the first eligible one would sit above
    request 2's report and skip it for good.
    """

    number = next(_ISSUES)
    first, work_item_id = stack.admit(number)
    first_epoch = stack.start(first)
    stack.own_pull_request(work_item_id, next(_PULLS))
    revision = uuid.uuid4()
    _admit_revision_directly(stack, number, revision)
    stack.finish(first, first_epoch, published=True)
    _promote(work_item_id)
    stack.start(revision)
    stack.report(revision, "implement", "Revising on the open pull request")
    _record_ci_rerun(first, RERUN_REQUESTED_NOTE)

    reports = {
        row["execution_request_id"]: row["id"]
        for row in _rows(
            "SELECT id, execution_request_id FROM curie.execution_request_phase_reports "
            "WHERE execution_request_id IN (:first, :revision)",
            {"first": first, "revision": revision},
        )
    }
    # The interleaving: the ended request's report is the higher id.
    assert reports[revision] < reports[first], reports

    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text
    stack.drain(fresh=True)

    (thread,) = stack.threads(work_item_id)
    (root,) = stack.slack.roots(stack.factory_token)
    assert thread["root_ts"] == root.response["ts"]
    texts = _thread_texts(stack, root.response["ts"])
    assert sum("Revising on the open pull request" in text_ for text_ in texts) == 1, texts
    # Request 1 ended before the destination, so its CI note is not this thread's.
    assert not any(RERUN_REQUESTED_NOTE in text_ for text_ in texts), texts
    (posted,) = _rows(
        "SELECT posted_at FROM curie.factory_slack_posts "
        "WHERE work_item_id = :id AND post_key = :key",
        {"id": work_item_id, "key": f"phase:{reports[revision]}"},
    )
    assert posted["posted_at"] is not None, posted


# --- the worker switch -----------------------------------------------------------------


async def _switched_progress_pass(
    worker: Worker, slack_url: str, identity: str, factory_token: str, *, enabled: bool
) -> bool:
    """One pass by the loop ``run.build`` assembles, or False when it builds none."""

    tag = secrets.token_hex(3)
    names = {
        "stream": f"test:curie:factory-slack:{tag}:runs",
        "group": f"test:curie:factory-slack:{tag}:runs:group",
        "prefix": f"test:curie:factory-slack:{tag}:worker",
    }
    config = make_config(
        names,
        slack_api_base_url=slack_url,
        slack_bot_token=DEFAULT_TOKEN,
        factory_slack_progress_enabled=enabled,
    )
    # The production sink: the factory identity's token answers its posts.
    sink = build_reply_sink(config, slack_tokens={identity: factory_token})
    try:
        loop = _build_factory_slack_progress(config, worker.engine, sink)
        if loop is None:
            return False
        await loop.run_pass()
        for _ in range(100):
            if not await loop.deliver_once():
                break
        return True
    finally:
        await sink.aclose()


def test_the_switch_off_posts_nothing_and_a_worker_with_it_on_posts_what_is_owed(
    stack: Stack,
) -> None:
    """AC1, AC5: the worker switch gates only the lane; what is owed comes from records.

    A worker with ``CURIE_FACTORY_SLACK_PROGRESS_ENABLED`` off builds no loop,
    so a run that reports, publishes and ends meanwhile records nothing owed.
    A worker built with it on, through the production wiring and reply sink,
    still finds that run and posts its root, phase, pull request and terminal.
    """

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    epoch = stack.start(request_id)
    pr = next(_PULLS)
    stack.own_pull_request(work_item_id, pr)
    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text
    stack.report(request_id, "plan", "Planned while the switch was off")
    stack.finish(request_id, epoch, published=True)

    built = stack.client.portal.call(
        functools.partial(
            _switched_progress_pass,
            stack.worker,
            stack.slack.base_url,
            stack.identity,
            stack.factory_token,
            enabled=False,
        )
    )
    assert built is False
    assert stack.threads() == []
    assert stack.slack.calls == []

    built = stack.client.portal.call(
        functools.partial(
            _switched_progress_pass,
            stack.worker,
            stack.slack.base_url,
            stack.identity,
            stack.factory_token,
            enabled=True,
        )
    )
    assert built is True
    (root,) = stack.slack.roots(stack.factory_token)
    assert f"#{number}" in root.haystack
    texts = _thread_texts(stack, root.response["ts"])
    assert len(texts) == 3, texts
    assert sum("Planned while the switch was off" in text_ for text_ in texts) == 1, texts
    assert sum(f"https://github.com/{REPO}/pull/{pr}" in text_ for text_ in texts) == 1, texts
    assert "Run completed" in texts[-1], texts
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []


# --- changing or clearing the destination ---------------------------------------------

OTHER_CHANNEL = "C0EXAMPLE2"


def _channel_posts(stack: Stack, channel: str) -> list[SlackCall]:
    return [
        call
        for call in stack.slack.delivered_posts(stack.factory_token)
        if call.params.get("channel") == channel
    ]


def test_changing_the_destination_keeps_existing_threads_and_roots_new_work_in_the_new_channel(
    stack: Stack,
) -> None:
    """AC1, AC5: a thread stays in its root's channel while the agent has a destination.

    The agent is bound in two channels, each with exactly one Slack binding and
    the factory identity. A WorkItem roots in the first; the destination moves
    to the second through the production agents route; a revision of that
    WorkItem still posts in the first channel's thread, while a WorkItem started
    after the change roots in the second. Clearing the destination stops every
    thread.

    Falsifiable: a live thread join that required the agent's current
    destination to equal the thread's channel would drop the first WorkItem's
    revision posts the moment the destination moved.
    """

    bound = stack.client.post(
        f"/agents/{stack.agent_id}/channels",
        headers=stack.platform,
        json={"kind": "slack", "address": OTHER_CHANNEL, "adapter": stack.identity},
    )
    assert bound.status_code == 201, bound.text

    run = stack.open_run()
    stack.report(run.request_id, "plan", "Planned in the first channel")
    stack.drain()
    (root,) = stack.slack.roots(stack.factory_token)
    assert root.params["channel"] == CHANNEL
    assert root.response["ts"] == run.root_ts

    moved = stack.client.patch(
        f"/agents/{stack.agent_id}",
        headers=stack.platform,
        json={"factory_slack_channel": OTHER_CHANNEL},
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["factory_slack_channel"] == OTHER_CHANNEL

    # A revision of the same WorkItem records a phase and ends after the move.
    revision = uuid.uuid4()
    _admit_revision_directly(stack, run.number, revision)
    stack.finish(run.request_id, run.epoch, published=True)
    _promote(run.work_item_id)
    revision_epoch = stack.start(revision)
    stack.report(revision, "review_diff", "Revised after the destination moved")
    stack.finish(revision, revision_epoch, published=True)
    stack.drain(fresh=True)

    texts = _thread_texts(stack, run.root_ts)
    assert sum("Planned in the first channel" in text_ for text_ in texts) == 1, texts
    assert sum("Revised after the destination moved" in text_ for text_ in texts) == 1, texts
    # Both requests' terminals reached the original thread.
    assert sum("Run completed" in text_ for text_ in texts) == 2, texts
    for call in stack.slack.thread_posts(stack.factory_token, run.root_ts):
        assert call.params["channel"] == CHANNEL, call.params
    (thread,) = stack.threads(run.work_item_id)
    assert (thread["channel"], thread["root_ts"]) == (CHANNEL, run.root_ts)
    # Nothing for this WorkItem landed in the new channel.
    assert _channel_posts(stack, OTHER_CHANNEL) == []

    # A WorkItem discovered after the move roots in the new channel.
    number = next(_ISSUES)
    later, later_item = stack.admit(number)
    later_epoch = stack.start(later)
    stack.report(later, "plan", "Planned in the second channel")
    stack.drain()
    (later_thread,) = stack.threads(later_item)
    assert later_thread["channel"] == OTHER_CHANNEL
    (later_root,) = [
        call for call in stack.slack.roots(stack.factory_token) if f"#{number}" in call.haystack
    ]
    assert later_root.params["channel"] == OTHER_CHANNEL
    assert later_thread["root_ts"] == later_root.response["ts"]
    later_texts = _thread_texts(stack, later_root.response["ts"])
    assert len(later_texts) == 1 and "Planned in the second channel" in later_texts[0]
    for call in stack.slack.thread_posts(stack.factory_token, later_root.response["ts"]):
        assert call.params["channel"] == OTHER_CHANNEL, call.params
    assert len(stack.slack.roots(stack.factory_token)) == 2

    # Clearing the destination stops every thread: a further phase posts nowhere.
    cleared = stack.client.patch(
        f"/agents/{stack.agent_id}", headers=stack.platform, json={"factory_slack_channel": None}
    )
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["factory_slack_channel"] is None
    calls = len(stack.slack.calls)
    stack.report(later, "implement", "Implemented after the destination was cleared")
    stack.finish(later, later_epoch)
    stack.drain(fresh=True)
    assert [c for c in stack.slack.calls[calls:] if c.method.startswith("chat.")] == []
    for call in stack.slack.calls:
        assert "Implemented after the destination was cleared" not in call.haystack


# --- the production worker runtime -----------------------------------------------------


def _thread_texts_of(stack: Stack, root: SlackCall) -> list[str]:
    return _thread_texts(stack, str(root.response["ts"]))


@dataclass
class _RuntimeRun:
    lanes: set[str]
    heartbeat_seen: bool
    signaled: bool


async def _run_production_worker(
    stack: Stack,
    monkeypatch: pytest.MonkeyPatch,
    heartbeat: Path,
    *,
    signal: threading.Event,
    within_s: float,
) -> _RuntimeRun:
    """The worker process's own composition: ``run.build``, then ``run._run_runtime``.

    This is ``run._run`` minus its schema assertion: the real runtime with every
    lane it launches (runs, evals, heartbeat, cron and the rest), over this
    stack's Postgres and Valkey, posting to the Slack stand-in. The factory
    identity's token reaches the sink the way a deployed worker gets it, from
    the declared identity's env var. Nothing is called on the progress loop.
    It runs until the Slack stand-in sets ``signal`` or ``within_s`` passes, in
    one wait raced against the runtime's own return, then shuts down through
    the resources owner, as ``run._run`` does.
    """

    from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
    from curie_worker import run
    from curie_worker.config import WorkerConfig
    from curie_worker.sandbox.k8s import k8s_config
    from curie_worker.worker_lifecycle import WorkerResources
    from redis.asyncio import Redis as AsyncValkey

    tag = secrets.token_hex(3)
    base = f"test:curie:factory-slack:{tag}"
    # The switch is read from the environment, as a deployed worker reads it.
    config = WorkerConfig(
        database_url=get_settings().database_url,
        db_schema=get_settings().db_schema,
        valkey_host=VALKEY_HOST,
        valkey_port=VALKEY_PORT,
        valkey_password=VALKEY_PW,
        stream=f"{base}:runs",
        eval_stream=f"{base}:evals",
        consumer_group=f"{base}:group",
        consumer_name=f"factory-slack-runtime-{tag}",
        key_prefix=f"{base}:worker",
        read_block_ms=100,
        internal_worker_token="",
        workspace_enabled=False,
        publication_enabled=False,
        slack_api_base_url=stack.slack.base_url,
        slack_bot_token=DEFAULT_TOKEN,
        factory_slack_interval_s=0.05,
        heartbeat_file=str(heartbeat),
    )
    env = {"CURIE_SANDBOX_SUBSTRATE": "kubernetes", "CURIE_SLACK_BOT_TOKEN__0": stack.factory_token}
    owner = WorkerResources()
    prior = set(asyncio.all_tasks())
    # No cluster exists here, so its in-cluster configuration is the external
    # boundary skipped at build, as the worker composition tests do. The runs
    # lane's orphan reap then fails its cluster list each maintenance tick and
    # logs it; no lane this test observes depends on a cluster.
    monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None)
    runtime = run.build(config, env, resources=owner)
    task = asyncio.create_task(run._run_runtime(runtime, config, owner))
    waiter = asyncio.ensure_future(asyncio.to_thread(signal.wait, within_s))
    try:
        done, _ = await asyncio.wait(
            {task, waiter}, timeout=within_s + 5, return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            await task
            raise AssertionError("the worker runtime returned before shutdown")
        lanes = {
            task_.get_name()
            for task_ in asyncio.all_tasks()
            if task_ not in prior and task_.get_name().startswith("curie-worker:")
        }
        return _RuntimeRun(lanes, heartbeat.exists(), signal.is_set())
    finally:
        await waiter
        try:
            await owner.aclose()
            await asyncio.wait_for(task, 15)
        finally:
            valkey = AsyncValkey(**config.valkey_client_kwargs())
            try:
                doomed = [key async for key in valkey.scan_iter(match=f"{base}*")]
                if doomed:
                    await valkey.delete(*doomed)
            finally:
                await valkey.aclose()


def test_the_production_worker_runtime_posts_owed_progress_only_with_the_switch_on(
    stack: Stack, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC1, AC5: the running worker, as ``run.build`` and ``run._run_runtime`` start it.

    With ``CURIE_FACTORY_SLACK_PROGRESS_ENABLED`` false the runtime runs every
    other lane for many progress intervals and posts nothing. With it true the
    same composition posts the owed root and its thread on its own.

    Falsifiable: a ``build`` that stopped assembling the progress loop, or a
    ``_run_runtime`` that stopped launching its lane, posts nothing here.
    """

    number = next(_ISSUES)
    request_id, work_item_id = stack.admit(number)
    stack.start(request_id)
    stack.report(request_id, "plan", "Planned before the worker started")
    enabled = stack.enable_destination()
    assert enabled.status_code == 200, enabled.text

    def rooted() -> bool:
        roots = stack.slack.roots(stack.factory_token)
        return bool(roots) and bool(_thread_texts_of(stack, roots[0]))

    monkeypatch.setenv("CURIE_FACTORY_SLACK_PROGRESS_ENABLED", "false")
    # Any chat call ends the wait early; with the switch off none may arrive in
    # a window of many progress intervals.
    off = asyncio.run(
        _run_production_worker(
            stack,
            monkeypatch,
            tmp_path / "heartbeat-off",
            signal=stack.slack.chat_called,
            within_s=1.5,
        )
    )
    # The runtime was up and its lanes ran: only the progress lane is absent.
    assert not off.signaled, stack.slack.calls
    assert off.heartbeat_seen, off
    assert {"curie-worker:runs", "curie-worker:heartbeat", "curie-worker:cron"} <= off.lanes, off
    assert "curie-worker:factory-slack-progress" not in off.lanes, off
    assert [c for c in stack.slack.calls if c.method.startswith("chat.")] == []
    assert stack.threads() == []

    monkeypatch.setenv("CURIE_FACTORY_SLACK_PROGRESS_ENABLED", "true")
    # The first reply posted in a thread means the root was posted before it.
    on = asyncio.run(
        _run_production_worker(
            stack,
            monkeypatch,
            tmp_path / "heartbeat-on",
            signal=stack.slack.thread_replied,
            within_s=10,
        )
    )
    assert on.signaled, stack.slack.calls
    assert {"curie-worker:runs", "curie-worker:factory-slack-progress"} <= on.lanes, on
    assert rooted(), stack.slack.calls
    (root,) = stack.slack.roots(stack.factory_token)
    assert root.params["channel"] == CHANNEL
    assert f"#{number}" in root.haystack
    (thread,) = stack.threads(work_item_id)
    assert thread["root_ts"] == root.response["ts"]
    texts = _thread_texts(stack, root.response["ts"])
    assert sum("Planned before the worker started" in text_ for text_ in texts) == 1, texts
    assert stack.slack.delivered_posts(DEFAULT_TOKEN) == []
