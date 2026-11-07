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
import logging
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
# The dispatcher's placeholder, and the text of every factory run root this
# Slack stand-in answers ``conversations.replies`` with.
PLACEHOLDER = "Working on it."
ROOT_TEXT = "Factory run root"
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
        self._hold: Callable[[str, dict[str, Any]], bool] | None = None
        self.hold_entered = threading.Event()
        self.hold_release = threading.Event()

    def hold_next(self, matches: Callable[[str, dict[str, Any]], bool]) -> None:
        """Hold the next matching call open until ``hold_release`` is set.

        The wait runs on a thread, so the caller's event loop (and the worker's
        lease heartbeat on it) keeps running while the call is held.
        """

        self.hold_entered.clear()
        self.hold_release.clear()
        self._hold = matches

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
        hold = self._hold
        if hold is not None and hold(method, params):
            self._hold = None
            self.hold_entered.set()
            if not await asyncio.to_thread(self.hold_release.wait, 20):
                raise web.HTTPServiceUnavailable(text="held Slack call was never released")
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
        if method == "chat.update":
            # https://docs.slack.dev/reference/methods/chat.update
            return {
                "ok": True,
                "channel": params.get("channel"),
                "ts": params.get("ts"),
                "text": params.get("text"),
            }
        if method == "conversations.replies":
            # https://docs.slack.dev/reference/methods/conversations.replies: the
            # parent message first. Every thread here was rooted by the factory
            # bot, so the dispatcher quotes that root into the turn text.
            return {
                "ok": True,
                "messages": [{"ts": params.get("ts"), "bot_id": FACTORY_BOT_ID, "text": ROOT_TEXT}],
                "has_more": False,
            }
        if method == "chat.postEphemeral":
            return {"ok": True, "message_ts": self.mint_ts()}
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
            call
            for call in self.delivered_posts(token)
            if call.params.get("thread_ts") == root_ts and call.text != PLACEHOLDER
        ]

    def edits_of(self, placeholder: str) -> list[SlackCall]:
        return [
            call
            for call in self.calls
            if call.method == "chat.update" and call.ok and call.params.get("ts") == placeholder
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


@dataclass
class _Base:
    """What every stack in this file shares: the API, the Slack stand-in, one agent."""

    client: TestClient
    slack: FakeSlack
    api_url: str
    valkey: redis.Redis
    owned: ExitStack
    tag: str
    identity: str
    factory_token: str
    agent_id: str
    deployment_id: str
    # Read at teardown: callers add their own keys and prefixes before using them.
    doomed_prefixes: list[str]
    doomed_keys: list[str]


@contextlib.contextmanager
def _stack_base(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    github: httpx.AsyncBaseTransport,
) -> Iterator[_Base]:
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
    prefixes: list[str] = []
    keys: list[str] = [api_stream, f"{api_stream}:dead"]
    with ExitStack() as owned:
        # Cleanup is registered before anything starts.
        owned.callback(get_settings.cache_clear)
        owned.callback(valkey.close)
        owned.callback(lambda: _clear_valkey(valkey, prefixes, keys))
        client = owned.enter_context(TestClient(create_app()))
        global _DB
        _DB = _SharedDb(client.portal, client.portal.call(_shared_engine))
        owned.callback(_release_db)
        external = httpx.AsyncClient(transport=github)
        owned.callback(client.portal.call, external.aclose)
        client.app.state.http_client = external
        slack = FakeSlack()
        api_url = owned.enter_context(
            client.portal.wrap_async_context_manager(_servers(client.app, slack))
        )
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
        yield _Base(
            client=client,
            slack=slack,
            api_url=api_url,
            valkey=valkey,
            owned=owned,
            tag=tag,
            identity=identity,
            factory_token=factory_token,
            agent_id=agent_id,
            deployment_id=deployment.json()["id"],
            doomed_prefixes=prefixes,
            doomed_keys=keys,
        )


@pytest.fixture
def stack(clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Stack]:
    with _stack_base(monkeypatch, tmp_path, github=httpx.MockTransport(GitHubAPI().handle)) as base:
        built = Stack(
            client=base.client,
            slack=base.slack,
            identity=base.identity,
            factory_token=base.factory_token,
            tag=base.tag,
            agent_id=base.agent_id,
            deployment_id=base.deployment_id,
        )
        built.worker = base.owned.enter_context(
            base.client.portal.wrap_async_context_manager(
                _worker(
                    slack_url=base.slack.base_url,
                    identity=base.identity,
                    factory_token=base.factory_token,
                    lease_owner=f"factory-slack-{base.tag}",
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


# =====================================================================================
# Steering a factory run from its Slack root (#5414): AC2, AC3, AC4, AC6
# =====================================================================================
#
# The worker under test is the worker process's own composition: ``run.build``
# over a ``WorkerConfig`` and an environment, exactly as ``run._run`` builds it,
# driven one stream entry at a time through its real ``Consumer`` (``_dispatch``
# for a new entry, ``_reclaim_once`` for a redelivery). Nothing inside it is
# replaced. Every failure below is a real one, reached through what the worker
# is configured with:
#
# 1. Valkey: the worker's Valkey endpoint is a TCP hop (``ValkeyHop``) to the
#    real Valkey that, on demand, drops the connection carrying one named
#    command. redis-py retries a dropped connection, so one failed call is one
#    drop per attempt the client makes (``_drops_for_one_failed_call``).
# 2. Postgres: the worker connects as its own login role (``_worker_role``),
#    whose SELECT on ``factory_slack_threads`` a test revokes, through a TCP hop
#    (``PostgresHop``) a test closes, so a new connection is refused by the
#    kernel (``ConnectionRefusedError`` raised by asyncpg itself).
# 3. The API: the worker's API base URL is an HTTP hop (``ApiHop``) to the live
#    API that records each answer and, on demand, resets the connection after
#    the API answered, so the route ran and committed and the worker never
#    reads the answer.
#
# External services only are stand-ins: the Slack Web API (``FakeSlack``), the
# GitHub REST API (``GitHubTruth``) and the Kubernetes API (``KubernetesApi``,
# which records every request and refuses each with a documented 403 Status,
# so an ordinary turn is seen claiming a sandbox and a steering turn is seen
# never doing so).

STEER_PATH = "/v1/internal/factory-slack/steer"
TEAM = "T0EXAMPLE1"
OTHER_TEAM = "T0EXAMPLE9"
FACTORY_BOT_USER = "U0EXAMPLEBOT"
SIBLING_BOT_ID = "B0EXAMPLE2"
SIBLING_BOT_USER = "U0EXAMPLE8"
APP_TOKEN = "xapp-EXAMPLE"
GHES_HOST = "github.example.com"
REPLACED_REPO_ID = 4499
ROLE_PASSWORD = secrets.token_hex(8)
UNMAPPED_THREAD = "1800000999.000001"
# The chart's generic warm pool and the template it references.
CHART_POOL = "curie-runner-pool"
CHART_TEMPLATE = "curie-runner-template"
# The label the substrate puts on each claim naming the agent it serves.
AGENT_LABEL = "curietech.ai/agent"
# Worker-local outcome codes, named in the reply text (``Outcome: <code>``).
EVIDENCE_MISSING = "steering_evidence_missing"
UNCONFIGURED = "steering_unconfigured"
BUDGET_EXPIRED = "steering_budget_expired"
# A reply on a path an earlier attempt of the same delivery may have admitted
# on never claims nothing was queued (plan addendum R1-1).
FALSE_NOTHING_QUEUED = ("could not be queued", "nothing was queued", "was not queued")


def _steering() -> Any:
    """The worker's steering module, imported where a test needs its texts."""

    from curie_worker import factory_steering

    return factory_steering


def _receipt_key(delivery_id: str) -> str:
    from curie_internal.slack_mention_receipt import receipt_key

    return str(receipt_key(delivery_id))


def _slack_escaped(body: str) -> str:
    """``body`` as a Slack event's ``text`` carries it.

    https://docs.slack.dev/messaging/formatting-message-text#escaping-text
    """

    return body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# --- GitHub REST stand-in ----------------------------------------------------------


@dataclass
class Account:
    account_id: int
    login: str
    permission: str


class GitHubTruth(GitHubAPI):
    """The factory reads ``GitHubAPI`` serves plus accounts, permissions and pulls.

    Shapes:
    https://docs.github.com/en/rest/users/users#get-a-user-using-their-id (``id``,
    ``login``),
    https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
    (``permission``, ``user``) and
    https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request (``state``,
    ``head.ref``, and ``head.repo`` / ``base.repo`` objects with an integer ``id``).
    """

    def __init__(self) -> None:
        super().__init__()
        self.accounts: dict[int, Account] = {}
        self.pulls: dict[int, dict[str, Any]] = {}
        self.paths: list[str] = []
        self._armed = False
        self.permission_entered = threading.Event()
        self.permission_release = threading.Event()

    def add_account(self, account: Account) -> None:
        self.accounts[account.account_id] = account

    def open_pull(self, pr: int, branch: str) -> None:
        # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
        repo = {"id": REPO_ID, "full_name": REPO}
        self.pulls[pr] = {
            "number": pr,
            "state": "open",
            "merged": False,
            "node_id": f"PR_acme_{pr}",
            "html_url": f"https://github.com/{REPO}/pull/{pr}",
            "title": "Factory change",
            "body": "",
            "head": {"sha": HEAD, "ref": branch, "repo": dict(repo)},
            "base": {"ref": "main", "repo": dict(repo)},
        }

    def pause_next_permission(self) -> None:
        self.permission_entered.clear()
        self.permission_release.clear()
        self._armed = True

    def take_pause(self, request: httpx.Request) -> bool:
        if self._armed and request.url.path.endswith("/permission"):
            self._armed = False
            return True
        return False

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        if request.method == "GET" and path.startswith("/user/"):
            account = self.accounts.get(int(path.removeprefix("/user/")))
            if account is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(
                200, json={"id": account.account_id, "login": account.login, "type": "User"}
            )
        prefix = f"/repos/{REPO}/collaborators/"
        if request.method == "GET" and path.startswith(prefix) and path.endswith("/permission"):
            login = urllib.parse.unquote(path[len(prefix) : -len("/permission")])
            account = next((a for a in self.accounts.values() if a.login == login), None)
            if account is not None:
                return httpx.Response(
                    200,
                    json={
                        "permission": account.permission,
                        "role_name": account.permission,
                        "user": {"id": account.account_id, "login": account.login},
                    },
                )
        pulls = f"/repos/{REPO}/pulls/"
        if request.method == "GET" and path.startswith(pulls) and path[len(pulls) :].isdigit():
            pull = self.pulls.get(int(path[len(pulls) :]))
            if pull is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json=pull)
        return super().handle(request)


class GitHubTransport(httpx.AsyncBaseTransport):
    """Serve ``GitHubTruth``; a paused permission read waits on a thread, not the loop."""

    def __init__(self, truth: GitHubTruth) -> None:
        self._truth = truth

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await request.aread()
        if self._truth.take_pause(request):
            self._truth.permission_entered.set()
            if not await asyncio.to_thread(self._truth.permission_release.wait, 20):
                raise httpx.ReadTimeout("paused permission was never released", request=request)
        return self._truth.handle(request)


# --- Kubernetes API stand-in -----------------------------------------------------------


class SandboxAddress:
    """The address a bound sandbox's ``serviceFQDN`` resolves to, with no runner on it.

    The runner and the model behind it are outside this suite's boundary. The
    listener records each request the kernel sends to the sandbox it claimed
    and answers ``503 Service Unavailable``, so the kernel's bounded retry
    ends the turn as a runner failure without a model turn.
    """

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, Any]] = []
        self.port = 0

    async def handle(self, request: web.Request) -> web.Response:
        raw = await request.read()
        try:
            body: Any = json.loads(raw) if raw else None
        except ValueError:
            body = None
        self.requests.append((request.method, request.path, body))
        return web.Response(status=503, text="no runner behind this address")


class KubernetesApi:
    """Records every request the worker's sandbox substrate makes and answers it.

    By default every request is refused with the documented ``Status`` kind a
    forbidden request answers with:
    https://kubernetes.io/docs/reference/using-api/api-concepts/#response-status-kind

    ``accept`` turns it into a control plane that serves the chart's warm pool
    and template and stores what the substrate writes, answering with the
    documented verb shapes (same page, "Standard API terminology" and "Resource
    URIs"): a create (``POST`` on a collection) answers ``201 Created`` with
    the stored object and its server-set ``metadata.uid``,
    ``metadata.creationTimestamp`` and ``metadata.resourceVersion``; a get
    answers ``200`` with the object or ``404`` with a ``NotFound`` ``Status``;
    a ``PATCH`` with an ``application/merge-patch+json`` body merges per
    RFC 7386 and answers ``200`` with the result; a delete answers ``200`` with
    a ``Success`` ``Status``. The agent-sandbox controller's part is played
    for each created ``SandboxClaim``: it binds a ``Sandbox`` of the claim's
    name whose ``status.serviceFQDN`` is ``service_fqdn``, and reports the
    claim ``Ready`` naming that sandbox (``status.sandbox.name``), the fields
    the substrate reads to hand back a route.
    """

    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []
        self.answers: list[tuple[str, str, int, Any]] = []
        self.url = ""
        self._objects: dict[str, dict[str, Any]] | None = None
        self._service_fqdn = ""
        self.sandbox_address = SandboxAddress()

    def accept(self, *, pool: str, template: str, service_fqdn: str) -> None:
        """Serve the chart's ``pool`` and its ``template``, and bind every claim."""

        self._service_fqdn = service_fqdn
        ext = "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/default"
        self._objects = {
            f"{ext}/sandboxwarmpools/{pool}": self._stored(
                "SandboxWarmPool",
                {"name": pool},
                {"replicas": 1, "sandboxTemplateRef": {"name": template}},
            ),
            f"{ext}/sandboxtemplates/{template}": self._stored(
                "SandboxTemplate",
                {"name": template},
                {
                    "podTemplate": {
                        "metadata": {"labels": {}},
                        "spec": {"containers": [{"name": "runner", "image": "runner:test"}]},
                    }
                },
            ),
        }

    def created(self, plural: str) -> list[tuple[int, dict[str, Any]]]:
        """Each create of ``plural`` the substrate made: its status and the object."""

        return [
            (status, body)
            for method, path, status, body in self.answers
            if method == "POST" and path.endswith(f"/{plural}")
        ]

    @staticmethod
    def _stored(kind: str, metadata: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        group = "agents.x-k8s.io" if kind == "Sandbox" else "extensions.agents.x-k8s.io"
        return {
            "apiVersion": f"{group}/v1beta1",
            "kind": kind,
            "metadata": {
                **metadata,
                "namespace": "default",
                "uid": str(uuid.uuid4()),
                "resourceVersion": "1",
                "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            "spec": spec,
        }

    @staticmethod
    def _status(code: int, reason: str) -> web.Response:
        return web.json_response(
            {
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {},
                "status": "Failure",
                "message": reason.lower(),
                "reason": reason,
                "code": code,
            },
            status=code,
        )

    def _answer(self, request: web.Request, status: int, body: Any) -> web.Response:
        self.answers.append((request.method, request.path, status, body))
        return web.json_response(body, status=status)

    def _bind(self, claim: dict[str, Any]) -> None:
        assert self._objects is not None
        name = claim["metadata"]["name"]
        sandbox = self._stored("Sandbox", {"name": name}, {"operatingMode": "Running"})
        sandbox["status"] = {
            "serviceFQDN": self._service_fqdn,
            "conditions": [{"type": "Ready", "status": "True", "reason": "Ready"}],
        }
        self._objects[f"/apis/agents.x-k8s.io/v1beta1/namespaces/default/sandboxes/{name}"] = (
            sandbox
        )
        claim["status"] = {
            "conditions": [{"type": "Ready", "status": "True", "reason": "Ready"}],
            "sandbox": {"name": name},
        }

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((request.method, request.path))
        objects = self._objects
        if objects is None:
            return self._status(403, "Forbidden")
        path = request.path
        if request.method == "POST":
            body = await request.json()
            stored = self._stored(body["kind"], dict(body["metadata"]), body.get("spec") or {})
            stored["apiVersion"] = body["apiVersion"]
            for key in ("type", "stringData", "data"):
                if key in body:
                    stored[key] = body[key]
            key = f"{path}/{stored['metadata']['name']}"
            if key in objects:
                return self._status(409, "AlreadyExists")
            objects[key] = stored
            created = json.loads(json.dumps(stored))
            if stored["kind"] == "SandboxClaim":
                self._bind(stored)
            return self._answer(request, 201, created)
        found = objects.get(path)
        if found is None:
            return self._status(404, "NotFound")
        if request.method == "GET":
            return self._answer(request, 200, found)
        if request.method == "PATCH":
            assert request.content_type == "application/merge-patch+json", request.content_type

            def merge(target: dict[str, Any], patch: dict[str, Any]) -> None:
                for name, value in patch.items():
                    if value is None:
                        target.pop(name, None)
                    elif isinstance(value, dict) and isinstance(target.get(name), dict):
                        merge(target[name], value)
                    else:
                        target[name] = value

            merge(found, await request.json())
            return self._answer(request, 200, found)
        if request.method == "DELETE":
            del objects[path]
            done = {
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {},
                "status": "Success",
                "details": {"name": path.rsplit("/", 1)[-1], "kind": path.split("/")[-2]},
            }
            return self._answer(request, 200, done)
        return self._status(405, "MethodNotAllowed")


# --- real-failure hops between the worker and its stores -------------------------------


_HOP_HEADERS = {"host", "content-length", "transfer-encoding", "connection", "keep-alive"}


class ApiHop:
    """The worker's API base URL: forwards to the live API, records each answer.

    ``drop_next_answer`` resets the worker's connection after the live API
    answered the next steering call, the error a connection reset after the
    server replied surfaces as. The route ran and committed; only the answer is
    lost.
    """

    def __init__(self, upstream: str) -> None:
        self.upstream = upstream.rstrip("/")
        self.url = ""
        self.exchanges: list[tuple[str, int, Any]] = []
        self.dropped: list[tuple[int, Any]] = []
        self._drop = 0
        self._client: httpx.AsyncClient | None = None

    def drop_next_answer(self) -> None:
        self._drop += 1

    def steer_answers(self) -> list[tuple[int, Any]]:
        return [(status, body) for path, status, body in self.exchanges if path == STEER_PATH]

    async def handle(self, request: web.Request) -> web.StreamResponse:
        assert self._client is not None
        body = await request.read()
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_HEADERS}
        upstream = await self._client.request(
            request.method, self.upstream + request.path_qs, content=body, headers=headers
        )
        content = upstream.content
        try:
            parsed: Any = json.loads(content) if content else None
        except ValueError:
            parsed = None
        if request.path == STEER_PATH and self._drop:
            self._drop -= 1
            self.dropped.append((upstream.status_code, parsed))
            assert request.transport is not None
            request.transport.abort()
            return web.Response(status=502)
        self.exchanges.append((request.path, upstream.status_code, parsed))
        kept = {
            k: v
            for k, v in upstream.headers.items()
            if k.lower() in ("content-type", "retry-after", "cache-control")
        }
        return web.Response(status=upstream.status_code, body=content, headers=kept)


@dataclass
class _DropRule:
    matches: Callable[[list[str]], bool]
    remaining: int


def _resp_commands(buffer: bytearray) -> tuple[list[list[str]], int]:
    """Complete RESP command arrays at the head of ``buffer``, and their length.

    https://redis.io/docs/latest/develop/reference/protocol-spec/#sending-commands-to-a-redis-server
    """

    commands: list[list[str]] = []
    pos = 0
    while pos < len(buffer):
        if buffer[pos : pos + 1] != b"*":
            return commands, len(buffer)  # not an array: forward it as it is
        end = buffer.find(b"\r\n", pos)
        if end < 0:
            break
        count = int(buffer[pos + 1 : end])
        cursor = end + 2
        parts: list[str] = []
        complete = True
        for _ in range(count):
            line = buffer.find(b"\r\n", cursor)
            if line < 0 or buffer[cursor : cursor + 1] != b"$":
                complete = False
                break
            length = int(buffer[cursor + 1 : line])
            start = line + 2
            if len(buffer) < start + length + 2:
                complete = False
                break
            parts.append(bytes(buffer[start : start + length]).decode("utf-8", "replace"))
            cursor = start + length + 2
        if not complete:
            break
        commands.append(parts)
        pos = cursor
    return commands, pos


class ValkeyHop:
    """A TCP hop to the real Valkey that drops a connection on a named command.

    ``drop(matches, times)`` drops each connection that sends a matching
    command, ``times`` times, before forwarding it. Every command it forwarded
    is kept in ``seen``.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host, self._port = host, port
        self.port = 0
        self.seen: list[list[str]] = []
        self._rules: list[_DropRule] = []
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    def drop(self, matches: Callable[[list[str]], bool], *, times: int) -> _DropRule:
        rule = _DropRule(matches, times)
        self._rules.append(rule)
        return rule

    def touched(self, key: str) -> list[list[str]]:
        return [command for command in self.seen if key in command[1:]]

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = int(self._server.sockets[0].getsockname()[1])

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
        for writer in list(self._writers):
            writer.transport.abort()

    def _armed(self, commands: list[list[str]]) -> _DropRule | None:
        for rule in self._rules:
            if rule.remaining > 0 and any(rule.matches(command) for command in commands):
                return rule
        return None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection(self._host, self._port)
        self._writers.update({writer, up_writer})

        async def downstream() -> None:
            try:
                while data := await up_reader.read(65536):
                    writer.write(data)
                    await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.transport.abort()

        async def upstream() -> None:
            buffer = bytearray()
            try:
                while data := await reader.read(65536):
                    buffer += data
                    commands, used = _resp_commands(buffer)
                    rule = self._armed(commands)
                    if rule is not None:
                        rule.remaining -= 1
                        return
                    self.seen.extend(commands)
                    up_writer.write(bytes(buffer[:used]))
                    del buffer[:used]
                    await up_writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.transport.abort()
                up_writer.transport.abort()

        try:
            await asyncio.gather(downstream(), upstream())
        finally:
            self._writers.discard(writer)
            self._writers.discard(up_writer)


class PostgresHop:
    """A TCP hop to the real Postgres that a test closes and reopens on the same port.

    ``go_down_after(marker)`` closes it at a chosen point of the worker's own
    traffic: once a client message carrying ``marker`` was sent and that
    transaction's ``ROLLBACK`` forwarded, the next client message is never
    forwarded; instead the hop stops listening and resets every connection, so
    the pool's next checkout fails its pre-ping and its reconnect is refused.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host, self._port = host, port
        self.port = 0
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._marker: bytes | None = None
        self._stage = "idle"
        self.went_down = False

    def go_down_after(self, marker: bytes) -> None:
        self._marker, self._stage, self.went_down = marker, "marker", False

    def _down_now(self, data: bytes) -> bool:
        """Advance the armed point on one client message; True once it is reached."""

        if self._stage == "marker" and self._marker is not None and self._marker in data:
            self._stage = "rollback"
        elif self._stage == "rollback" and b"ROLLBACK" in data:
            self._stage = "next"
        elif self._stage == "next":
            self._stage, self.went_down = "idle", True
            if self._server is not None:
                self._server.close()
                self._server = None
            for writer in list(self._writers):
                writer.transport.abort()
            return True
        return False

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", self.port)
        self.port = int(self._server.sockets[0].getsockname()[1])

    async def close(self) -> None:
        """Stop listening and reset every open connection: a down database."""

        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.transport.abort()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection(self._host, self._port)
        self._writers.update({writer, up_writer})

        async def pump(source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
            try:
                while data := await source.read(65536):
                    if sink is up_writer and self._down_now(bytes(data)):
                        return
                    sink.write(data)
                    await sink.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                sink.transport.abort()

        try:
            await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))
        finally:
            self._writers.discard(writer)
            self._writers.discard(up_writer)


@contextlib.asynccontextmanager
async def _hops(
    api_url: str, k8s: KubernetesApi, valkey: ValkeyHop, postgres: PostgresHop, api: ApiHop
) -> AsyncIterator[None]:
    del api_url
    api_app = web.Application()
    api_app.router.add_route("*", "/{tail:.*}", api.handle)
    api_server = TestServer(api_app, host="127.0.0.1")
    k8s_app = web.Application()
    k8s_app.router.add_route("*", "/{tail:.*}", k8s.handle)
    k8s_server = TestServer(k8s_app, host="127.0.0.1")
    address_app = web.Application()
    address_app.router.add_route("*", "/{tail:.*}", k8s.sandbox_address.handle)
    address_server = TestServer(address_app, host="127.0.0.1")
    api._client = httpx.AsyncClient(timeout=30.0)
    try:
        await api_server.start_server()
        api.url = f"http://127.0.0.1:{api_server.port}"
        await k8s_server.start_server()
        k8s.url = f"http://127.0.0.1:{k8s_server.port}"
        await address_server.start_server()
        assert address_server.port is not None
        k8s.sandbox_address.port = address_server.port
        await valkey.start()
        await postgres.start()
        yield
    finally:
        await valkey.close()
        await postgres.close()
        await api_server.close()
        await k8s_server.close()
        await address_server.close()
        await api._client.aclose()


def _drops_for_one_failed_call() -> int:
    """How many dropped connections one failed call of the worker's Valkey client takes.

    The worker's runs client keeps redis-py's default retry (``run.build``), so a
    call fails only after its first attempt and every retry each lost the
    connection. Read from a client built the way ``run.build`` builds it.
    """

    from redis.asyncio import Redis as AsyncValkey

    return 1 + int(AsyncValkey(host="127.0.0.1", socket_timeout=5.0).get_retry().get_retries())


# --- the steering stack ---------------------------------------------------------------


@dataclass
class Person:
    principal_id: uuid.UUID
    slack_user: str
    slack_link_id: uuid.UUID | None
    account_id: int | None
    login: str | None
    github_link_ids: list[str] = field(default_factory=list)

    @property
    def requester(self) -> str:
        return f"github:{self.account_id}:{self.login}"


@dataclass
class Delivery:
    event_id: str
    delivery_id: str
    message_ts: str
    placeholder: str | None


@dataclass
class Attempt:
    delivery: Delivery
    replies: list[tuple[int, Any]]
    new_requests: list[dict[str, Any]]
    edits: list[SlackCall]

    @property
    def reply(self) -> dict[str, Any] | None:
        if not self.replies:
            return None
        status, body = self.replies[-1]
        assert status == 200, (status, body)
        assert isinstance(body, dict), body
        return body


@dataclass
class SteerRun:
    number: int
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    epoch: int
    pr: int
    branch: str
    pr_url: str
    lineage_id: uuid.UUID
    root_ts: str


@dataclass
class SteeringWorker:
    """One worker process: ``run.build``'s runtime and the config it was built from."""

    runtime: Any
    config: Any


@contextlib.asynccontextmanager
async def _steering_worker(
    stack: SteeringStack, *, consumer_name: str, token: bool = True
) -> AsyncIterator[SteeringWorker]:
    """The worker process's composition, ``run.build``, over this stack's stores.

    Every endpoint comes from the config: Postgres through the stack's hop as
    the stack's worker role, Valkey through its hop, the API through its hop,
    Slack at the stand-in. ``token`` decides whether the worker holds the
    internal worker token, as ``INTERNAL_WORKER_TOKEN`` decides for a deployed
    worker.
    """

    from curie_test_support.valkey import VALKEY_PW
    from curie_worker import run
    from curie_worker.config import WorkerConfig
    from curie_worker.worker_lifecycle import WorkerResources

    config = WorkerConfig(
        database_url=stack.worker_database_url,
        db_schema=get_settings().db_schema,
        valkey_host="127.0.0.1",
        valkey_port=stack.valkey_hop.port,
        valkey_password=VALKEY_PW,
        stream=stack.names["stream"],
        eval_stream=f"{stack.names['base']}:evals",
        consumer_group=stack.names["group"],
        consumer_name=consumer_name,
        key_prefix=stack.names["prefix"],
        read_block_ms=100,
        internal_worker_token=WORKER_TOKEN if token else "",
        api_base_url=stack.api_hop.url,
        workspace_enabled=False,
        publication_enabled=False,
        slack_api_base_url=stack.slack.base_url,
        slack_bot_token=DEFAULT_TOKEN,
        slack_edit_min_interval_s=0.0,
        # A pending entry is redelivered by the next reclaim pass, with no wait.
        reclaim_min_idle_ms=0,
    )
    env = {
        "CURIE_SANDBOX_SUBSTRATE": "kubernetes",
        # The factory identity's token, from its declared env var.
        "CURIE_SLACK_BOT_TOKEN__0": stack.factory_token,
        # Every sandbox listens on the fleet-wide port; here, the stand-in address's.
        "CURIE_RUNNER_PORT": str(stack.k8s.sandbox_address.port),
    }
    owner = WorkerResources()
    try:
        runtime = run.build(config, env, resources=owner)
        await runtime.consumer.ensure_group()
        yield SteeringWorker(runtime, config)
    finally:
        await owner.aclose()


async def _consume_one(worker: SteeringWorker) -> str | None:
    """Read one new entry as the runs consumer and handle it to completion."""

    config = worker.config
    consumer = worker.runtime.consumer
    rows = await worker.runtime.async_redis.xreadgroup(
        config.consumer_group, config.consumer_name, {config.stream: ">"}, count=1
    )
    if not rows:
        return None
    entry_id, fields = rows[0][1][0]
    await consumer._dispatch(entry_id, fields)
    async with asyncio.timeout(25):
        await asyncio.gather(*list(consumer._inflight), return_exceptions=True)
    return str(entry_id)


async def _reclaim_and_settle(worker: SteeringWorker) -> int:
    """The consumer's own reclaim pass picks the pending entry up and handles it."""

    consumer = worker.runtime.consumer
    reclaimed = int(await consumer._reclaim_once())
    async with asyncio.timeout(25):
        await asyncio.gather(*list(consumer._inflight), return_exceptions=True)
    return reclaimed


class _SocketClient:
    """Captures Bolt's envelope acks; the Socket Mode transport is the only stand-in."""

    def __init__(self) -> None:
        self.logger = logging.getLogger("factory-slack-socket")
        self.acked: list[str] = []

    def send_socket_mode_response(self, response: Any) -> None:
        self.acked.append(response.envelope_id)


class SteeringStack(Stack):
    def __init__(
        self,
        base: _Base,
        *,
        github: GitHubTruth,
        names: dict[str, str],
        dispatcher_config: Any,
        operator_id: uuid.UUID,
        api_hop: ApiHop,
        valkey_hop: ValkeyHop,
        postgres_hop: PostgresHop,
        k8s: KubernetesApi,
        role: str,
        worker_database_url: str,
    ) -> None:
        super().__init__(
            client=base.client,
            slack=base.slack,
            identity=base.identity,
            factory_token=base.factory_token,
            tag=base.tag,
            agent_id=base.agent_id,
            deployment_id=base.deployment_id,
        )
        self.base = base
        self.github = github
        self.valkey = base.valkey
        self.names = names
        self.dispatcher_config = dispatcher_config
        self.operator_id = operator_id
        self.api_hop = api_hop
        self.valkey_hop = valkey_hop
        self.postgres_hop = postgres_hop
        self.k8s = k8s
        self.role = role
        self.worker_database_url = worker_database_url
        self.steerer: SteeringWorker
        self._people = itertools.count(1)
        self._ts = itertools.count(1)
        self._workers = itertools.count(1)

    # Runs ----------------------------------------------------------------------

    def open_steered_run(self, *, requester: str | None = None) -> SteerRun:
        """A running factory request, its open PR on GitHub, and its posted Slack root."""

        number = next(_ISSUES)
        request_id, work_item_id = self.admit(number, requester=requester)
        epoch = self.start(request_id)
        pr = next(_PULLS)
        lineage_id, branch = self.own_pull_request(work_item_id, pr)
        self.github.open_pull(pr, branch)
        enabled = self.enable_destination()
        assert enabled.status_code == 200, enabled.text
        self.drain()
        (thread,) = self.threads(work_item_id)
        assert thread["root_ts"], thread
        return SteerRun(
            number,
            work_item_id,
            request_id,
            epoch,
            pr,
            branch,
            f"https://github.com/{REPO}/pull/{pr}",
            lineage_id,
            thread["root_ts"],
        )

    # Identity ----------------------------------------------------------------

    def person(
        self,
        *,
        slack: bool = True,
        github: bool = True,
        permission: str = "write",
        host: str = "github.com",
    ) -> Person:
        n = next(self._people)
        slack_user = f"U0{self.tag.upper()}{n:02d}"
        principal_id, link_id = _seed_principal(slack_user if slack else None)
        person = Person(principal_id, slack_user, link_id, None, None)
        if github:
            account_id = int(f"7{secrets.randbelow(10**7):07d}{n:02d}")
            login = f"acme-dev-{self.tag}-{n}"
            self.github.add_account(Account(account_id, login, permission))
            person.account_id, person.login = account_id, login
            person.github_link_ids.append(self.link_github(person, account_id, host=host))
        return person

    def link_github(self, person: Person, account_id: int, *, host: str = "github.com") -> str:
        from curie_api.models import DEFAULT_TENANT_ID

        response = self.client.post(
            f"/identity/admin/{DEFAULT_TENANT_ID}/code-host-links",
            headers=self.platform,
            json={
                "principal_id": str(person.principal_id),
                "code_host_kind": "github",
                "host": host,
                "account_id": str(account_id),
                "created_by_principal_id": str(self.operator_id),
            },
        )
        assert response.status_code == 201, response.text
        return str(response.json()["link_id"])

    def revoke(self, link_id: str | uuid.UUID) -> None:
        from curie_api.models import DEFAULT_TENANT_ID

        response = self.client.post(
            f"/identity/admin/{DEFAULT_TENANT_ID}/links/{link_id}/revoke", headers=self.platform
        )
        assert response.status_code == 200, response.text

    # Slack ingress: the real Bolt app the dispatcher builds ---------------------

    def _deliver(self, event: dict[str, Any], event_id: str) -> None:
        from curie_dispatcher.app import build_app
        from curie_dispatcher.identities import SlackIdentityCredentials
        from slack_bolt.adapter.socket_mode import SocketModeHandler
        from slack_bolt.authorization import AuthorizeResult
        from slack_sdk.socket_mode.request import SocketModeRequest
        from slack_sdk.web import WebClient

        def authorize(**_kwargs: Any) -> AuthorizeResult:
            # Bolt's resolved bot identity for this app, as the dispatcher
            # suite's conftest fixes it, so no auth.test is called.
            return AuthorizeResult(
                enterprise_id=None,
                team_id=TEAM,
                bot_token=self.factory_token,
                bot_id=FACTORY_BOT_ID,
                bot_user_id=FACTORY_BOT_USER,
            )

        app = build_app(
            self.dispatcher_config,
            web_client=WebClient(token=self.factory_token, base_url=self.slack.base_url),
            redis_client=self.valkey,
            identity=SlackIdentityCredentials(
                name=self.identity, app_token=APP_TOKEN, bot_token=self.factory_token
            ),
            authorize=authorize,
            identity_bots={SIBLING_BOT_ID: SIBLING_BOT_USER},
        )
        errors: list[BaseException] = []

        @app.error
        def _collect(error: Exception) -> None:
            errors.append(error)

        handler = SocketModeHandler(app, app_token=APP_TOKEN)
        handler.handle(
            _SocketClient(),
            SocketModeRequest(
                type="events_api",
                envelope_id=f"env-{uuid.uuid4().hex[:8]}",
                payload={
                    "type": "event_callback",
                    "event_id": event_id,
                    "team_id": TEAM,
                    "event": event,
                },
            ),
        )
        app.listener_runner.listener_executor.shutdown(wait=True)
        assert errors == []

    def mention(
        self,
        user: str | None,
        text_: str,
        *,
        thread_ts: str,
        team: str = TEAM,
        bot_id: str | None = None,
        event_id: str | None = None,
        message_ts: str | None = None,
    ) -> Delivery:
        """One ``app_mention`` (https://docs.slack.dev/reference/events/app_mention).

        ``text_`` is the message text after the mention, as Slack sends it.
        """

        from curie_dispatcher.identities import delivery_key

        event_id = event_id or f"Ev0{self.tag.upper()}{uuid.uuid4().hex[:8].upper()}"
        delivery_id = delivery_key(event_id, self.identity)
        self.base.doomed_keys.append(_receipt_key(delivery_id))
        message_ts = message_ts or f"1800000500.{next(self._ts):06d}"
        event: dict[str, Any] = {
            "type": "app_mention",
            "channel": CHANNEL,
            "text": f"<@{FACTORY_BOT_USER}> {text_}",
            "ts": message_ts,
            "thread_ts": thread_ts,
            "team": team,
        }
        if user is not None:
            event["user"] = user
            event["user_team"] = team
        if bot_id is not None:
            event["bot_id"] = bot_id
        before = len(self.slack.calls)
        self._deliver(event, event_id)
        placeholders = [
            call
            for call in self.slack.calls[before:]
            if call.method == "chat.postMessage"
            and call.ok
            and call.text == PLACEHOLDER
            and call.params.get("thread_ts") == thread_ts
        ]
        placeholder = placeholders[-1].response["ts"] if placeholders else None
        return Delivery(event_id, delivery_id, message_ts, placeholder)

    def plain_reply(self, user: str, body: str, *, thread_ts: str) -> None:
        """A thread reply that mentions no one: a ``message`` event, never ``app_mention``."""

        event = {
            "type": "message",
            "channel": CHANNEL,
            "channel_type": "channel",
            "user": user,
            "user_team": TEAM,
            "team": TEAM,
            "text": body,
            "ts": f"1800000500.{next(self._ts):06d}",
            "thread_ts": thread_ts,
        }
        self._deliver(event, f"Ev0{self.tag.upper()}{uuid.uuid4().hex[:8].upper()}")

    # Worker ------------------------------------------------------------------

    def worker_process(self, *, token: bool = True) -> SteeringWorker:
        """Another worker process in the same consumer group (a replacement, or a peer)."""

        name = f"factory-steer-{self.tag}-{next(self._workers)}"
        return self.base.owned.enter_context(
            self.client.portal.wrap_async_context_manager(
                _steering_worker(self, consumer_name=name, token=token)
            )
        )

    def consume(self, worker: SteeringWorker | None = None) -> str | None:
        return self.client.portal.call(_consume_one, worker or self.steerer)

    def reclaim(self, worker: SteeringWorker | None = None) -> int:
        return self.client.portal.call(_reclaim_and_settle, worker or self.steerer)

    def steer(
        self,
        user: str | None,
        text_: str,
        *,
        thread_ts: str,
        before_consume: Callable[[Delivery], None] | None = None,
        worker: SteeringWorker | None = None,
        **mention: Any,
    ) -> Attempt:
        known = {row["id"] for row in self.requests()}
        replies = len(self.api_hop.steer_answers())
        calls = len(self.slack.calls)
        delivery = self.mention(user, text_, thread_ts=thread_ts, **mention)
        if before_consume is not None:
            before_consume(delivery)
        assert self.consume(worker) is not None, "the dispatcher enqueued nothing"
        return self._attempt(delivery, known, replies, calls)

    def steer_paused(
        self, user: str, text_: str, *, thread_ts: str, during: Callable[[], None]
    ) -> Attempt:
        """Steer while the API's write permission read is held open."""

        known = {row["id"] for row in self.requests()}
        replies = len(self.api_hop.steer_answers())
        calls = len(self.slack.calls)
        self.github.pause_next_permission()
        delivery = self.mention(user, text_, thread_ts=thread_ts)
        future = self.client.portal.start_task_soon(_consume_one, self.steerer)
        try:
            assert self.github.permission_entered.wait(20), "the permission read never started"
            during()
        finally:
            self.github.permission_release.set()
        assert future.result(timeout=30) is not None
        return self._attempt(delivery, known, replies, calls)

    def _attempt(self, delivery: Delivery, known: set[Any], replies: int, calls: int) -> Attempt:
        new = [row for row in self.requests() if row["id"] not in known]
        edits = [
            call
            for call in self.slack.calls[calls:]
            if call.method == "chat.update"
            and delivery.placeholder is not None
            and call.params.get("ts") == delivery.placeholder
        ]
        return Attempt(delivery, self.api_hop.steer_answers()[replies:], new, edits)

    def pending(self) -> int:
        summary = self.valkey.xpending(self.names["stream"], self.names["group"])
        return int(summary["pending"])

    def is_terminal(self, event_id: str) -> bool:
        """The kernel's done marker for this delivery, read straight from Valkey."""

        return bool(self.valkey.exists(self.steerer.config.done_key(event_id)))

    def steering_sent_key(self, event_id: str) -> str:
        return str(self.steerer.config.steering_sent_key(event_id))

    def not_started(self) -> str:
        return str(self.steerer.config.turn_not_started_text)

    def resend_invitations(self) -> list[SlackCall]:
        return [call for call in self.slack.calls if self.not_started() in call.haystack]

    def placeholder_texts(self, placeholder: str | None) -> list[str]:
        assert placeholder is not None, "the dispatcher posted no placeholder"
        return [call.text for call in self.slack.edits_of(placeholder)]

    # Reads -----------------------------------------------------------------

    def requests(self) -> list[dict[str, Any]]:
        return _rows(
            "SELECT id, work_item_id, sequence, status, objective, requester, reply_kind, "
            "reply_address, reply_conversation_id, reply_target_kind, reply_target_pr_number, "
            "reply_target_url, wait_deadline, terminal_at "
            "FROM curie.execution_requests ORDER BY created_at, sequence"
        )

    def revisions(self, run: SteerRun) -> list[uuid.UUID]:
        return [row["id"] for row in self.requests() if row["id"] != run.request_id]

    def expected_request_id(self, delivery: Delivery) -> uuid.UUID:
        return uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"curie-slack://factory/steer/{self.identity}/{delivery.delivery_id}",
        )


def _seed_principal(slack_user: str | None) -> tuple[uuid.UUID, uuid.UUID | None]:
    """A human principal and, optionally, its Slack link in the factory workspace."""

    from curie_api.identity.service import find_or_create_namespace
    from curie_api.models import DEFAULT_TENANT_ID, IdentityLink, Principal

    async def go(engine: AsyncEngine) -> tuple[uuid.UUID, uuid.UUID | None]:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            principal = Principal(
                tenant_id=DEFAULT_TENANT_ID,
                idp_subject=f"acme-person-{uuid.uuid4().hex}",
                type="human",
            )
            session.add(principal)
            await session.flush()
            link_id: uuid.UUID | None = None
            if slack_user is not None:
                namespace = await find_or_create_namespace(
                    session,
                    tenant_id=DEFAULT_TENANT_ID,
                    provider="slack",
                    authority="",
                    kind="slack_workspace",
                    key=TEAM,
                )
                link = IdentityLink(
                    tenant_id=DEFAULT_TENANT_ID,
                    identity_namespace_id=namespace.id,
                    provider_native_id=slack_user,
                    principal_id=principal.id,
                    created_by_principal_id=principal.id,
                    verification_source="admin_mapped",
                )
                session.add(link)
                await session.flush()
                link_id = link.id
            await session.commit()
            return principal.id, link_id

    return _db(go)


def _seed_receiving_identity(identity: str) -> None:
    """The channel identity that receives mentions, attached to its installation."""

    from curie_api.identity.service import find_or_create_namespace
    from curie_api.models import DEFAULT_TENANT_ID, ChannelIdentity, ProviderInstallation
    from sqlalchemy import select

    async def go(engine: AsyncEngine) -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            installation = await session.scalar(
                select(ProviderInstallation).where(
                    ProviderInstallation.tenant_id == DEFAULT_TENANT_ID,
                    ProviderInstallation.provider == "slack",
                    ProviderInstallation.external_account_id == TEAM,
                )
            )
            if installation is None:
                installation = ProviderInstallation(
                    tenant_id=DEFAULT_TENANT_ID, provider="slack", external_account_id=TEAM
                )
                session.add(installation)
                await session.flush()
            await find_or_create_namespace(
                session,
                tenant_id=DEFAULT_TENANT_ID,
                provider="slack",
                authority=installation.authority,
                kind="slack_workspace",
                key=TEAM,
            )
            session.add(
                ChannelIdentity(
                    tenant_id=DEFAULT_TENANT_ID,
                    provider="slack",
                    name=identity,
                    provider_installation_id=installation.id,
                )
            )
            await session.commit()

    _db(go)


def _worker_role(base: _Base) -> str:
    """The worker's own Postgres login role, with the DML the worker needs.

    Its drop is registered before it is created; the worker's engines are
    disposed before the drop runs (the worker is entered after it).
    """

    role = f"curie_steer_{base.tag}"
    schema = get_settings().db_schema
    base.owned.callback(
        _rows,
        "DO $do$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = "
        f"'{role}') THEN EXECUTE 'DROP OWNED BY \"{role}\"'; "
        f"EXECUTE 'DROP ROLE \"{role}\"'; END IF; END $do$",
    )
    _rows(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{ROLE_PASSWORD}'")
    _rows(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
    _rows(f'GRANT ALL ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"')
    _rows(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{role}"')
    return role


def _set_mapping_readable(stack: SteeringStack, readable: bool) -> None:
    schema = get_settings().db_schema
    verb = "GRANT SELECT ON" if readable else "REVOKE SELECT ON"
    preposition = "TO" if readable else "FROM"
    _rows(f'{verb} "{schema}".factory_slack_threads {preposition} "{stack.role}"')


@contextlib.contextmanager
def _steering_stack(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, token: bool = True
) -> Iterator[SteeringStack]:
    from curie_dispatcher.config import DispatcherConfig
    from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
    from curie_worker.sandbox.k8s import k8s_config
    from kubernetes import client as k8s_client
    from sqlalchemy.engine import make_url

    github = GitHubTruth()
    with _stack_base(monkeypatch, tmp_path, github=GitHubTransport(github)) as base:
        tag = base.tag
        names = {
            "base": f"test:curie:factory-slack:{tag}",
            "stream": f"test:curie:factory-slack:{tag}:runs",
            "group": f"test:curie:factory-slack:{tag}:runs:group",
            "prefix": f"test:curie:factory-slack:{tag}:worker",
        }
        dedupe_prefix = f"{names['base']}:dedupe:"
        admission_prefix = f"{names['base']}:admission:"
        context_prefix = f"{names['base']}:thread-context:"
        base.doomed_prefixes.extend(
            [
                names["prefix"],
                f"{names['base']}:evals",
                dedupe_prefix,
                admission_prefix,
                context_prefix,
            ]
        )
        base.doomed_keys.extend([names["stream"], f"{names['stream']}:dead"])

        # The Kubernetes control plane is external: the substrate's in-cluster
        # configuration points the real client at the stand-in.
        k8s = KubernetesApi()
        previous = k8s_client.Configuration.get_default_copy()
        base.owned.callback(k8s_client.Configuration.set_default, previous)

        def incluster() -> None:
            configuration = k8s_client.Configuration()
            configuration.host = k8s.url
            k8s_client.Configuration.set_default(configuration)

        monkeypatch.setattr(k8s_config, "load_incluster_config", incluster)

        settings_url = make_url(get_settings().database_url)
        api_hop = ApiHop(base.api_url)
        valkey_hop = ValkeyHop(VALKEY_HOST, VALKEY_PORT)
        postgres_hop = PostgresHop(settings_url.host or "127.0.0.1", settings_url.port or 5432)
        base.owned.enter_context(
            base.client.portal.wrap_async_context_manager(
                _hops(base.api_url, k8s, valkey_hop, postgres_hop, api_hop)
            )
        )
        role = _worker_role(base)
        worker_url = settings_url.set(
            username=role, password=ROLE_PASSWORD, host="127.0.0.1", port=postgres_hop.port
        ).render_as_string(hide_password=False)
        _seed_receiving_identity(base.identity)
        operator_id, _ = _seed_principal(None)
        dispatcher_config = DispatcherConfig(
            slack_app_token=APP_TOKEN,
            slack_bot_token=DEFAULT_TOKEN,
            valkey_host=VALKEY_HOST,
            valkey_port=VALKEY_PORT,
            valkey_password=VALKEY_PW,
            stream=names["stream"],
            dedupe_prefix=dedupe_prefix,
            dedupe_ttl_seconds=600,
            placeholder_text=PLACEHOLDER,
            approval_chat_attester_secret=secrets.token_hex(16),
            api_base_url=base.api_url,
            api_key=get_settings().api_key,
            admission_cache_prefix=admission_prefix,
            thread_context_cache_prefix=context_prefix,
        )
        built = SteeringStack(
            base,
            github=github,
            names=names,
            dispatcher_config=dispatcher_config,
            operator_id=operator_id,
            api_hop=api_hop,
            valkey_hop=valkey_hop,
            postgres_hop=postgres_hop,
            k8s=k8s,
            role=role,
            worker_database_url=worker_url,
        )
        # The progress loop posts each run's root, as a worker with the
        # progress switch on does.
        built.worker = base.owned.enter_context(
            base.client.portal.wrap_async_context_manager(
                _worker(
                    slack_url=base.slack.base_url,
                    identity=base.identity,
                    factory_token=base.factory_token,
                    lease_owner=f"factory-slack-{tag}",
                )
            )
        )
        built.steerer = built.worker_process(token=token)
        yield built


@pytest.fixture
def steering(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[SteeringStack]:
    with _steering_stack(monkeypatch, tmp_path) as built:
        yield built


@pytest.fixture
def tokenless_steering(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[SteeringStack]:
    """The same stack, its worker deployed without the internal worker token."""

    with _steering_stack(monkeypatch, tmp_path, token=False) as built:
        yield built


def _assert_no_sandbox(stack: SteeringStack) -> None:
    assert stack.k8s.requests == [], "a steering turn reached the sandbox substrate"


def _reached_sandbox(stack: SteeringStack, since: int) -> bool:
    """Whether a turn since ``since`` went to the substrate to claim a sandbox."""

    return any("sandbox" in path for _method, path in stack.k8s.requests[since:])


def _admitted(attempt: Attempt) -> None:
    reply = attempt.reply
    assert reply is not None and reply["outcome"] == "admitted", attempt.replies
    assert len(attempt.new_requests) == 1, attempt.new_requests


def _api_refused(attempt: Attempt, outcome: str, reason: str | None = None) -> None:
    reply = attempt.reply
    assert reply is not None, "the steering route was never asked"
    assert reply["outcome"] == outcome, reply
    if reason is not None:
        assert reply["reason"] == reason, reply
    assert attempt.new_requests == [], attempt.new_requests
    assert attempt.edits, "the placeholder was not edited with the refusal"
    assert outcome in attempt.edits[-1].text, attempt.edits[-1].text


def _worker_refused(attempt: Attempt, code: str = EVIDENCE_MISSING) -> None:
    assert attempt.replies == [], "the turn reached the steering route"
    assert attempt.new_requests == [], attempt.new_requests
    assert attempt.edits, "the placeholder was not edited with the refusal"
    assert code in attempt.edits[-1].text, attempt.edits[-1].text


def _no_false_nothing_queued(texts: list[str]) -> None:
    for text_ in texts:
        assert not any(claim in text_.lower() for claim in FALSE_NOTHING_QUEUED), text_


def _objective_reply(objective: str) -> dict[str, Any]:
    """The JSON object the objective carries the person's reply in."""

    start = objective.index('{"')
    reply, _end = json.JSONDecoder().raw_decode(objective, start)
    assert isinstance(reply, dict), reply
    return reply


def _disable(person: Person) -> None:
    _rows(
        "UPDATE curie.principals SET status = 'disabled' WHERE id = :id",
        {"id": person.principal_id},
    )


# --- AC2: an authorized mention revises the same pull request -----------------------


def test_an_authorized_mention_admits_a_revision_of_the_same_pull_request(
    steering: SteeringStack,
) -> None:
    """AC2, AC6: the complete quoted multiline body, same PR and branch, original route.

    Falsifiable: without the kernel diversion the mention claims a sandbox and
    no revision exists; an objective that drops or rewrites the body, or a
    revision whose reply route is the Slack thread, fails the field checks.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    (original,) = stack.requests()
    (lineage_before,) = _rows(
        "SELECT branch, pr_number, pr_url, status, head_sha, version "
        "FROM curie.thread_publication_lineages"
    )
    body = (
        'Please keep the "acme_helper" name.\n'
        "Also:\n"
        "> a quoted line\n"
        "> and a second one\n"
        "```\n"
        "def acme_helper() -> None: ...\n"
        "```\n"
        "Thanks & regards, naïve café ✓"
    )
    sent = _slack_escaped(body)

    attempt = stack.steer(writer.slack_user, sent, thread_ts=run.root_ts)

    reply = attempt.reply
    assert reply is not None and reply["outcome"] == "admitted", attempt.replies
    assert reply["pull_request_url"] == run.pr_url
    expected_id = stack.expected_request_id(attempt.delivery)
    (admitted,) = attempt.new_requests
    assert admitted["id"] == expected_id
    assert reply["request_id"] == str(expected_id)
    assert admitted["work_item_id"] == run.work_item_id
    assert admitted["status"] == "queued"
    objective = admitted["objective"]
    # The person's complete message, exactly as Slack delivered it and before
    # the dispatcher quoted the bot-rooted root into the turn text.
    carried = _objective_reply(objective)
    assert carried["body"] == sent, carried
    assert json.dumps(sent, ensure_ascii=False) in objective
    assert ROOT_TEXT not in objective
    assert run.pr_url in objective and run.branch in objective
    assert "same" in objective.lower(), objective
    assert admitted["requester"] == writer.requester
    assert (admitted["reply_target_kind"], admitted["reply_target_pr_number"]) == (
        "pull_request",
        str(run.pr),
    )
    assert admitted["reply_target_url"] == run.pr_url
    # The original tracker binding and conversation; Slack is never its identity.
    assert (
        admitted["reply_kind"],
        admitted["reply_address"],
        admitted["reply_conversation_id"],
    ) == (original["reply_kind"], original["reply_address"], original["reply_conversation_id"])
    assert original["reply_kind"] == "github"
    assert _rows("SELECT id FROM curie.work_items") == [{"id": run.work_item_id}]
    assert _rows(
        "SELECT branch, pr_number, pr_url, status, head_sha, version "
        "FROM curie.thread_publication_lineages"
    ) == [lineage_before]
    # Automatic publication adds no approval.
    assert _rows("SELECT id FROM curie.approvals") == []
    assert attempt.edits, "the placeholder was not edited with the outcome"
    assert all(call.token == stack.factory_token for call in attempt.edits)
    assert "admitted" in attempt.edits[-1].text and run.pr_url in attempt.edits[-1].text
    assert stack.pending() == 0
    assert stack.is_terminal(attempt.delivery.delivery_id)
    _assert_no_sandbox(stack)


# --- AC3: every refusal beside a valid liveness admission ----------------------------

REFUSALS = [
    "unlinked_channel_member",
    "revoked_github_link",
    "revoked_slack_link",
    "wrong_workspace",
    "inactive_principal",
    "disabled_after_enqueue",
    "read_permission",
    "wrong_host_link",
    "two_github_links",
    "tagger_without_write",
    "cancelled_work_item",
    "closed_lineage",
    "provider_pull_request_closed",
    "provider_head_branch_differs",
    "revoked_while_permission_paused",
    "receipt_deleted",
    "wrong_app_receipt",
    "http_identity_context",
    "bot_mention",
]


def _carry_http_identity(stack: SteeringStack) -> None:
    """Re-deliver the just-enqueued mention as an HTTP-selected turn arrives.

    The stream entry gains the ``curie_identity_context_v1`` field the HTTP
    receiver lane carries (#5444), while the native receipt for it still exists.
    """

    stream = stack.names["stream"]
    entry_id, fields = stack.valkey.xrange(stream)[-1]
    stack.valkey.xdel(stream, entry_id)
    stack.valkey.xadd(stream, {**fields, "curie_identity_context_v1": "ZXhhbXBsZS1jb250ZXh0"})


def _receipt_from_another_app(stack: SteeringStack, delivery: Delivery) -> None:
    """The stored receipt now names another receiving app than the turn's."""

    key = _receipt_key(delivery.delivery_id)
    stored = stack.valkey.get(key)
    assert stored is not None, "the dispatcher wrote no receipt"
    receipt = json.loads(stored)
    receipt["channel_identity"] = f"acme-other-app-{stack.tag}"
    stack.valkey.set(key, json.dumps(receipt), keepttl=True)


@pytest.mark.parametrize("case", REFUSALS)
def test_each_refusal_fails_closed_beside_a_valid_liveness_admission(
    steering: SteeringStack, case: str
) -> None:
    """AC3, AC4, AC6: no request, a named outcome, no sandbox; a valid mention admits."""

    stack = steering
    writer = stack.person()
    tagger: Person | None = None
    if case == "tagger_without_write":
        tagger = stack.person(permission="read")
    run = stack.open_steered_run(requester=tagger.requester if tagger is not None else None)
    root = run.root_ts
    own_requests = {run.request_id}

    def live(body: str = "Please also update the changelog.") -> None:
        _admitted(stack.steer(writer.slack_user, body, thread_ts=root))

    if case == "unlinked_channel_member":
        # A member of the channel with no link: membership confers nothing.
        member = stack.person(slack=False, github=False)
        _api_refused(
            stack.steer(member.slack_user, "Rename it.", thread_ts=root),
            "sender_unverified",
            "no_link",
        )
        live()
    elif case == "revoked_github_link":
        live()
        stack.revoke(writer.github_link_ids[0])
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root),
            "code_host_identity_missing",
        )
    elif case == "revoked_slack_link":
        live()
        assert writer.slack_link_id is not None
        stack.revoke(writer.slack_link_id)
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root),
            "sender_unverified",
            "no_link",
        )
    elif case == "wrong_workspace":
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root, team=OTHER_TEAM),
            "sender_unverified",
            "namespace_unknown",
        )
        live()
    elif case == "inactive_principal":
        inactive = stack.person()
        _disable(inactive)
        _api_refused(
            stack.steer(inactive.slack_user, "Rename it.", thread_ts=root),
            "sender_unverified",
            "principal_inactive",
        )
        live()
    elif case == "disabled_after_enqueue":
        # Authority is revalidated when the worker consumes the delivery.
        later = stack.person()
        _api_refused(
            stack.steer(
                later.slack_user,
                "Rename it.",
                thread_ts=root,
                before_consume=lambda _delivery: _disable(later),
            ),
            "sender_unverified",
            "principal_inactive",
        )
        live()
    elif case == "read_permission":
        reader = stack.person(permission="read")
        _api_refused(stack.steer(reader.slack_user, "Rename it.", thread_ts=root), "not_authorized")
        live()
    elif case == "wrong_host_link":
        elsewhere = stack.person(host=GHES_HOST)
        _api_refused(
            stack.steer(elsewhere.slack_user, "Rename it.", thread_ts=root),
            "code_host_identity_missing",
        )
        live()
    elif case == "two_github_links":
        twice = stack.person()
        second = int(f"8{secrets.randbelow(10**8):08d}")
        stack.github.add_account(Account(second, f"acme-alt-{stack.tag}", "write"))
        twice.github_link_ids.append(stack.link_github(twice, second))
        _api_refused(
            stack.steer(twice.slack_user, "Rename it.", thread_ts=root),
            "code_host_identity_ambiguous",
        )
        live()
    elif case == "tagger_without_write":
        # The person who started the run is linked, but tagging it conferred
        # nothing: current write permission decides.
        assert tagger is not None
        _api_refused(stack.steer(tagger.slack_user, "Rename it.", thread_ts=root), "not_authorized")
        live()
    elif case == "cancelled_work_item":
        live()
        (item,) = _rows(
            "SELECT version FROM curie.work_items WHERE id = :id", {"id": run.work_item_id}
        )
        cancelled = stack.client.post(
            f"/v1/internal/work-items/{run.work_item_id}/cancel",
            headers=stack.internal,
            json={"expected_version": item["version"]},
        )
        assert cancelled.status_code == 200, cancelled.text
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root), "work_item_cancelled"
        )
    elif case == "closed_lineage":
        live()
        _rows(
            "UPDATE curie.thread_publication_lineages SET status = 'closed' WHERE id = :id",
            {"id": run.lineage_id},
        )
        _api_refused(stack.steer(writer.slack_user, "Rename it.", thread_ts=root), "lineage_closed")
    elif case == "provider_pull_request_closed":
        live()
        stack.github.pulls[run.pr]["state"] = "closed"
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root), "pull_request_not_open"
        )
    elif case == "provider_head_branch_differs":
        live()
        stack.github.pulls[run.pr]["head"]["ref"] = "curie/another-branch"
        _api_refused(
            stack.steer(writer.slack_user, "Rename it.", thread_ts=root),
            "pull_request_branch_mismatch",
        )
    elif case == "revoked_while_permission_paused":
        _admitted(
            stack.steer_paused(
                writer.slack_user, "Paused, then released.", thread_ts=root, during=lambda: None
            )
        )
        _api_refused(
            stack.steer_paused(
                writer.slack_user,
                "Paused, then revoked.",
                thread_ts=root,
                during=lambda: stack.revoke(writer.github_link_ids[0]),
            ),
            "authority_changed",
        )
    elif case == "receipt_deleted":
        _worker_refused(
            stack.steer(
                writer.slack_user,
                "Rename it.",
                thread_ts=root,
                before_consume=lambda d: stack.valkey.delete(_receipt_key(d.delivery_id)),
            )
        )
        assert stack.pending() == 0
        live()
    elif case == "wrong_app_receipt":
        # The receiving app is what establishes the trusted sender: a receipt
        # naming another app is refused, locally or by the API.
        attempt = stack.steer(
            writer.slack_user,
            "Rename it.",
            thread_ts=root,
            before_consume=lambda d: _receipt_from_another_app(stack, d),
        )
        assert attempt.new_requests == [], attempt.new_requests
        assert attempt.reply is None or attempt.reply["outcome"] == "receipt_mismatch"
        assert attempt.edits, "the placeholder was not edited with the refusal"
        last = attempt.edits[-1].text
        assert "receipt_mismatch" in last or EVIDENCE_MISSING in last, last
        assert stack.pending() == 0
        live()
    elif case == "http_identity_context":
        # An HTTP-selected turn stays outside native steering (#5444): whichever
        # layer refuses it, it asks no steering decision and claims no sandbox.
        # The carried context cannot be checked here, so the #5444 identity
        # layer keeps the delivery for redelivery (TurnAuthorityUnavailable);
        # it is never settled as steering. That pending entry parks later
        # turns of this thread behind it, so liveness uses another run root.
        attempt = stack.steer(
            writer.slack_user,
            "Rename it.",
            thread_ts=root,
            before_consume=lambda _d: _carry_http_identity(stack),
        )
        assert attempt.replies == [], attempt.replies
        assert attempt.new_requests == []
        assert not stack.is_terminal(attempt.delivery.delivery_id)
        assert stack.pending() == 1
        other = stack.open_steered_run()
        own_requests.add(other.request_id)
        _admitted(
            stack.steer(
                writer.slack_user, "Please also update the changelog.", thread_ts=other.root_ts
            )
        )
    elif case == "bot_mention":
        _worker_refused(stack.steer(None, "Rename it.", thread_ts=root, bot_id=SIBLING_BOT_ID))
        assert stack.pending() == 0
        live()
    else:  # pragma: no cover - the parameter list is closed
        raise AssertionError(case)

    # Exactly the one liveness revision was admitted, by the authorized writer.
    admitted = [row for row in stack.requests() if row["id"] not in own_requests]
    assert len(admitted) == 1, admitted
    assert all(row["requester"] == writer.requester for row in admitted)
    _assert_no_sandbox(stack)


# --- AC4: idempotency, queueing, promotion, ordinary paths ----------------------------


def test_steering_is_idempotent_queues_successors_and_leaves_ordinary_paths_intact(
    steering: SteeringStack,
) -> None:
    """AC4, AC6: once per delivery, queued behind the live run, no approval.

    A plain reply in the mapped root is never a turn, and a mention in an
    unmapped thread of the same channel takes the ordinary path to a sandbox.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()

    first = stack.steer(writer.slack_user, "First revision request.", thread_ts=run.root_ts)
    _admitted(first)
    (queued_first,) = first.new_requests
    assert queued_first["status"] == "queued"

    # Slack redelivers the same event: the dispatcher claims it once.
    length = stack.valkey.xlen(stack.names["stream"])
    stack.mention(
        writer.slack_user,
        "First revision request.",
        thread_ts=run.root_ts,
        event_id=first.delivery.event_id,
        message_ts=first.delivery.message_ts,
    )
    assert stack.valkey.xlen(stack.names["stream"]) == length
    assert stack.consume() is None
    # A redelivered stream copy of the same turn admits nothing new either.
    copies = [
        fields
        for _entry, fields in stack.valkey.xrange(stack.names["stream"])
        if first.delivery.delivery_id in json.dumps(fields)
    ]
    assert len(copies) == 1, copies
    stack.valkey.xadd(stack.names["stream"], copies[0])
    known = len(stack.requests())
    answers = len(stack.api_hop.steer_answers())
    assert stack.consume() is not None
    assert len(stack.requests()) == known
    assert all(
        body.get("request_id") == str(queued_first["id"])
        for _status, body in stack.api_hop.steer_answers()[answers:]
    )

    second = stack.steer(writer.slack_user, "Second revision request.", thread_ts=run.root_ts)
    _admitted(second)
    (queued_second,) = second.new_requests
    assert queued_second["status"] == "queued"
    assert queued_second["sequence"] > queued_first["sequence"]

    # The predecessor ends; promotion takes the oldest queued revision only.
    stack.finish(run.request_id, run.epoch)
    _promote(run.work_item_id)
    statuses = {row["id"]: row for row in stack.requests()}
    assert statuses[queued_first["id"]]["status"] == "waiting"
    assert statuses[queued_first["id"]]["wait_deadline"] is not None
    assert statuses[queued_second["id"]]["status"] == "queued"
    assert _rows("SELECT id FROM curie.approvals") == []
    assert stack.pending() == 0
    _assert_no_sandbox(stack)

    # A plain reply in the mapped root is never delivered as a turn.
    length = stack.valkey.xlen(stack.names["stream"])
    stack.plain_reply(writer.slack_user, "Just chatting in the thread.", thread_ts=run.root_ts)
    assert stack.valkey.xlen(stack.names["stream"]) == length

    # A mention in an unmapped thread of the same channel is an ordinary turn.
    before = len(stack.requests())
    answers = len(stack.api_hop.steer_answers())
    # Here the cluster accepts sandbox claims, so the ordinary start is observed
    # succeeding: the claim is created and bound, and the kernel sends this
    # message to the sandbox it claimed. No runner listens there; the kernel's
    # bounded retry then ends the turn as a runner failure.
    stack.k8s.accept(pool=CHART_POOL, template=CHART_TEMPLATE, service_fqdn="127.0.0.1")
    question = "What does this service do?"
    ordinary = stack.mention(writer.slack_user, question, thread_ts=UNMAPPED_THREAD)
    assert stack.consume() is not None
    claims = stack.k8s.created("sandboxclaims")
    assert [status for status, _claim in claims] == [201], claims
    labels = claims[0][1]["metadata"]["labels"]
    assert labels[AGENT_LABEL] == f"acme-factory-{stack.tag}", labels
    # The kernel hands the message to the runner of the sandbox it claimed:
    # first as a runner steer (a live turn would take it), then as a new turn.
    # A runner steer is the kernel's own, not the factory steering route.
    delivered = [
        body
        for method, path, body in stack.k8s.sandbox_address.requests
        if method == "POST" and path in ("/v1/steer", "/v1/event")
    ]
    assert delivered, stack.k8s.sandbox_address.requests
    assert all(question in str(body.get("text")) for body in delivered), delivered
    texts = stack.placeholder_texts(ordinary.placeholder)
    assert texts[0] == stack.steerer.config.booting_text, texts
    assert stack.not_started() not in texts, texts
    assert "runner-error" in texts[-1], texts
    assert stack.api_hop.steer_answers()[answers:] == []
    assert len(stack.requests()) == before


# --- production wiring: the worker as ``run.build`` composes it --------------------------


def test_a_worker_without_the_internal_token_refuses_a_mapped_root_locally(
    tokenless_steering: SteeringStack,
) -> None:
    """A mapped root never starts ordinary chat, even where steering cannot ask the API.

    Both workers here are ``run.build``'s composition; only
    ``INTERNAL_WORKER_TOKEN`` differs. Without it the mapped root is refused
    locally with ``steering_unconfigured``: its receipt is never read, the
    steering route never asked, no sandbox claimed. With it the next mention in
    the same root admits. Liveness: an unmapped mention on the tokenless worker
    takes the ordinary path to a sandbox.

    Falsifiable: a ``run.build`` that stopped wiring ``factory_steering`` sends
    the mapped mention to a sandbox claim, and the token worker admits nothing.
    """

    stack = tokenless_steering
    writer = stack.person()
    run = stack.open_steered_run()

    attempt = stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts)

    assert stack.api_hop.exchanges == [], "the tokenless worker called the API"
    assert attempt.new_requests == [], attempt.new_requests
    assert stack.valkey_hop.touched(_receipt_key(attempt.delivery.delivery_id)) == []
    assert stack.valkey.exists(_receipt_key(attempt.delivery.delivery_id)) == 1
    assert attempt.edits, "the placeholder was not edited with the refusal"
    assert all(call.token == stack.factory_token for call in attempt.edits)
    assert UNCONFIGURED in attempt.edits[-1].text, attempt.edits[-1].text
    _no_false_nothing_queued([call.text for call in attempt.edits])
    assert stack.pending() == 0
    _assert_no_sandbox(stack)

    # The same composition holding the token admits the next mention.
    with_token = stack.worker_process(token=True)
    _admitted(
        stack.steer(
            writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts, worker=with_token
        )
    )
    assert stack.pending() == 0
    _assert_no_sandbox(stack)

    # Liveness on the tokenless worker: an unmapped mention is ordinary chat.
    answers = len(stack.api_hop.steer_answers())
    stack.mention(writer.slack_user, "What does this service do?", thread_ts=UNMAPPED_THREAD)
    assert stack.consume() is not None
    assert _reached_sandbox(stack, 0), stack.k8s.requests
    assert stack.api_hop.steer_answers()[answers:] == []


# --- control 3: the diversion settles through the kernel's fenced terminal path ------


def test_a_diverted_refusal_completes_with_the_kernels_completion_record(
    steering: SteeringStack,
) -> None:
    """A refusal and an admission both settle: done marker, acked, nothing owed.

    Each settles through the lease-fenced completion (the next test proves the
    fence); the entry is acked only after the done marker is written.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()

    refused = stack.steer(
        writer.slack_user,
        "Rename it.",
        thread_ts=run.root_ts,
        before_consume=lambda d: stack.valkey.delete(_receipt_key(d.delivery_id)),
    )
    _worker_refused(refused)
    assert _settled_completion(stack, refused.delivery) == "dropped"
    assert stack.is_terminal(refused.delivery.delivery_id)
    assert stack.pending() == 0

    admitted = stack.steer(writer.slack_user, "Please rename it.", thread_ts=run.root_ts)
    _admitted(admitted)
    assert stack.is_terminal(admitted.delivery.delivery_id)
    assert stack.pending() == 0
    _assert_no_sandbox(stack)


def _settled_completion(stack: SteeringStack, delivery: Delivery) -> str:
    """The outcome of the one completion record the kernel's fenced settle wrote.

    Read from the worker's own Valkey traffic: ``Markers.settle_fenced`` is one
    EVAL whose first key is the delivery's done marker and whose arguments carry
    the outbox record, then the record is emitted and cleared. Asserts the
    settle happened exactly once, that it carried this delivery's
    ``turn.completed`` record, and that nothing is left owed in the real store.
    """

    done_key = stack.steerer.config.done_key(delivery.delivery_id)
    settles = [
        command
        for command in stack.valkey_hop.seen
        if command[0].upper() == "EVAL" and command[3:4] == [done_key]
    ]
    assert len(settles) == 1, f"expected one fenced settle, saw {len(settles)}"
    (settle,) = settles
    keys = int(settle[2])
    records = []
    for argument in settle[3 + keys :]:
        try:
            decoded = json.loads(argument)
        except ValueError:
            continue
        if isinstance(decoded, dict) and "event" in decoded:
            records.append(decoded)
    assert len(records) == 1, "the settle carried no completion record"
    (record,) = records
    event = record["event"]
    assert record["event_id"] == delivery.delivery_id, record
    assert (event["event"], event["event_id"]) == ("turn.completed", delivery.delivery_id), event
    # Emitted and cleared: the record and its pending-set membership are gone.
    storage_key, pending_key = settle[4], settle[5]
    assert stack.valkey.exists(storage_key) == 0, "the completion record is still owed"
    assert not stack.valkey.sismember(pending_key, delivery.delivery_id)
    return str(event["outcome"])


def test_an_admitted_steering_completes_delivered_and_releases_the_thread(
    steering: SteeringStack,
) -> None:
    """Control 12: an admission settles as the kernel's ``delivered`` completion.

    The admitted delivery's one fenced settle writes the done marker and a
    ``turn.completed`` record whose outcome is ``delivered``, which is emitted and
    cleared, and the entry is acked. The thread's order is released: the next
    mention in the same root is handled at once and settles the same way.

    Falsifiable: an admission completed as ``dropped`` reads ``dropped`` here,
    and one that writes a bare done marker without a completion record shows
    no fenced settle at all.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()

    first = stack.steer(writer.slack_user, "Please rename it.", thread_ts=run.root_ts)
    _admitted(first)
    assert _settled_completion(stack, first.delivery) == "delivered"
    assert stack.is_terminal(first.delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.placeholder_texts(first.delivery.placeholder)[-1] == _steering().render_reply(
        "admitted", pull_request_url=run.pr_url
    )

    following = stack.steer(writer.slack_user, "And add a test.", thread_ts=run.root_ts)
    _admitted(following)
    assert _settled_completion(stack, following.delivery) == "delivered"
    assert stack.is_terminal(following.delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.revisions(run) == [
        stack.expected_request_id(first.delivery),
        stack.expected_request_id(following.delivery),
    ]
    assert stack.resend_invitations() == []
    _assert_no_sandbox(stack)


async def _peer_takes_lease(stack: SteeringStack, entry_id: str, peer: str) -> None:
    """A peer worker takes the delivery as a lease-expiry reclaim would.

    The PEL row moves to the peer (``XCLAIM``), the stale owner's lease key is
    gone (its expiry), and the peer acquires through the production
    ``DeliveryLeaseStore``, which bumps the fencing generation.
    """

    from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from redis.asyncio import Redis as AsyncValkey

    config = stack.steerer.config
    stream, group = config.stream, config.consumer_group
    client = AsyncValkey(
        host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None, decode_responses=True
    )
    try:
        claimed = await client.xclaim(stream, group, peer, 0, [entry_id], justid=True)
        assert [str(row) for row in claimed] == [entry_id], claimed
        await client.delete(config.delivery_lease_key(stream, group, entry_id))
        lease = await DeliveryLeaseStore(client, config).acquire(
            stream, group, entry_id, consumer=peer
        )
        assert lease.generation >= 2, lease.generation
    finally:
        await client.aclose()


def test_a_stale_owner_of_a_diverted_turn_settles_nothing_after_a_peer_took_the_lease(
    steering: SteeringStack,
) -> None:
    """ADR-0131 on the diversion: a fenced-out owner marks, completes and acks nothing.

    The refusal's placeholder edit is held open while a peer takes the
    delivery lease in real Valkey. Once the edit returns, the stale owner's
    settle is refused by the fence: no done marker, and the entry is still
    pending, now under the peer. Liveness: an unheld refusal settles.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    stream, group = stack.names["stream"], stack.names["group"]
    peer = f"peer-{stack.tag}"

    delivery = stack.mention(writer.slack_user, "Rename it.", thread_ts=run.root_ts)
    assert delivery.placeholder is not None
    stack.valkey.delete(_receipt_key(delivery.delivery_id))
    stack.slack.hold_next(
        lambda method, params: method == "chat.update" and params.get("ts") == delivery.placeholder
    )
    future = stack.client.portal.start_task_soon(_consume_one, stack.steerer)
    entry_id: str | None = None
    try:
        assert stack.slack.hold_entered.wait(20), "the refusal edit never started"
        (pending,) = stack.valkey.xpending_range(stream, group, "-", "+", 10)
        entry_id = str(pending["message_id"])
        stack.client.portal.call(_peer_takes_lease, stack, entry_id, peer)
    finally:
        stack.slack.hold_release.set()
    assert future.result(timeout=25) == entry_id

    # The edit itself went out: it was in flight before the transfer.
    assert any(EVIDENCE_MISSING in text_ for text_ in stack.placeholder_texts(delivery.placeholder))
    assert not stack.is_terminal(delivery.delivery_id), "the stale owner wrote the done marker"
    (still,) = stack.valkey.xpending_range(stream, group, "-", "+", 10)
    assert (str(still["message_id"]), still["consumer"]) == (entry_id, peer), still
    assert [row["id"] for row in stack.requests()] == [run.request_id]

    # Liveness: the same refusal without a transfer settles. The first entry is
    # still pending under the peer, and same-thread ordering parks any later
    # turn of that root behind it, so the liveness refusal uses another root.
    other = stack.open_steered_run()
    settled = stack.steer(
        writer.slack_user,
        "Rename it again.",
        thread_ts=other.root_ts,
        before_consume=lambda d: stack.valkey.delete(_receipt_key(d.delivery_id)),
    )
    _worker_refused(settled)
    assert stack.is_terminal(settled.delivery.delivery_id)
    _assert_no_sandbox(stack)


# --- control 2: a delivery with too little budget left to steer -------------------------


def _spend_budget(stack: SteeringStack, remaining_ms: int) -> None:
    """Fixture state of a redelivered entry that already spent most of its budget.

    The delivery state hash's ``deadline_ms`` is create-if-absent on acquire
    (``delivery_lease._ACQUIRE_LUA``), so the consumer's lease inherits this
    deadline exactly as a reclaimed delivery inherits its predecessor's.
    """

    config = stack.steerer.config
    stream, group = config.stream, config.consumer_group
    entry_id, _fields = stack.valkey.xrange(stream)[-1]
    seconds, micros = stack.valkey.time()
    now_ms = int(seconds) * 1000 + int(micros) // 1000
    stack.valkey.hset(
        config.delivery_state_key(stream, group, entry_id),
        "deadline_ms",
        str(now_ms + remaining_ms),
    )


def test_a_mention_with_no_steering_budget_left_is_refused_before_any_api_call(
    steering: SteeringStack,
) -> None:
    """A delivery with at most the minimum attempt budget left never steers.

    Refused locally with ``steering_budget_expired``: no steering request, no
    GitHub read, no admission, and no claim that nothing was queued (an earlier
    attempt may have). Liveness: the same mention with the full budget admits.
    """

    from curie_worker.kernel import constants as kernel_constants

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    remaining_ms = int(kernel_constants._MIN_ATTEMPT_BUDGET_S * 1000) - 2000
    assert remaining_ms > 0
    github_reads = len(stack.github.paths)

    attempt = stack.steer(
        writer.slack_user,
        "Please rename the helper.",
        thread_ts=run.root_ts,
        before_consume=lambda _d: _spend_budget(stack, remaining_ms),
    )

    assert attempt.replies == [], "a delivery without budget reached the steering route"
    assert attempt.new_requests == [], attempt.new_requests
    assert stack.github.paths[github_reads:] == []
    assert attempt.edits, "the placeholder was not edited with the refusal"
    assert BUDGET_EXPIRED in attempt.edits[-1].text, attempt.edits[-1].text
    _no_false_nothing_queued([call.text for call in attempt.edits])
    assert stack.pending() == 0

    _admitted(stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts))
    assert len(stack.revisions(run)) == 1
    _assert_no_sandbox(stack)


# --- control 1: the code host, repository and pull request are the lineage's -----------


def test_steering_refuses_a_lineage_on_another_code_host_than_the_configured_one(
    steering: SteeringStack,
) -> None:
    """The configured GitHub code host must be the host the lineage's repository is on.

    The lineage records its repository on a GitHub Enterprise placeholder host
    while the configured GitHub code host is github.com; the person holds a
    link and write permission on the lineage's host. Refused
    ``code_host_mismatch``. Liveness: on the matching host the same person admits.
    """

    stack = steering
    run = stack.open_steered_run()
    elsewhere = stack.person(host=GHES_HOST)
    _rows(
        "UPDATE curie.thread_publication_lineages SET code_host_host = :host WHERE id = :id",
        {"host": GHES_HOST, "id": run.lineage_id},
    )

    _api_refused(
        stack.steer(elsewhere.slack_user, "Rename it.", thread_ts=run.root_ts),
        "code_host_mismatch",
    )
    assert stack.revisions(run) == []

    _rows(
        "UPDATE curie.thread_publication_lineages SET code_host_host = 'github.com' WHERE id = :id",
        {"id": run.lineage_id},
    )
    assert elsewhere.account_id is not None
    stack.revoke(elsewhere.github_link_ids[0])
    elsewhere.github_link_ids.append(stack.link_github(elsewhere, elsewhere.account_id))
    _admitted(stack.steer(elsewhere.slack_user, "Rename it.", thread_ts=run.root_ts))
    _assert_no_sandbox(stack)


def _replace_repository(stack: SteeringStack, pr: int, repository_id: int) -> None:
    """GitHub answers the stored path with another repository (a move, then a new one).

    https://docs.github.com/en/rest/repos/repos#get-a-repository (``id``) and
    https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
    (``base.repo.id``, ``head.repo.id``).
    """

    stack.github.repository_id = repository_id
    pull = stack.github.pulls[pr]
    pull["head"]["repo"]["id"] = repository_id
    pull["base"]["repo"]["id"] = repository_id


def test_steering_refuses_a_repository_replaced_at_the_lineages_path(
    steering: SteeringStack,
) -> None:
    """The repository at the lineage's path must still be the stored repository id."""

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    _replace_repository(stack, run.pr, REPLACED_REPO_ID)

    _api_refused(
        stack.steer(writer.slack_user, "Rename it.", thread_ts=run.root_ts),
        "repository_mismatch",
    )
    assert stack.revisions(run) == []

    _replace_repository(stack, run.pr, REPO_ID)
    _admitted(stack.steer(writer.slack_user, "Rename it.", thread_ts=run.root_ts))
    _assert_no_sandbox(stack)


_ABSENT = object()
_NULL_REPO = object()

# (sides, reported): what the pull request read reports in place of the
# repository's integer id. https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
REPOSITORY_ID_DEFECTS: list[tuple[tuple[str, ...], object]] = [
    (("head", "base"), REPLACED_REPO_ID),
    (("head", "base"), _ABSENT),
    (("head", "base"), None),
    (("head", "base"), str(REPO_ID)),
    (("head", "base"), True),
    (("head", "base"), float(REPO_ID)),
    (("head",), _ABSENT),
    (("base",), _ABSENT),
    (("head", "base"), _NULL_REPO),
]


@pytest.mark.parametrize(
    ("sides", "reported"),
    REPOSITORY_ID_DEFECTS,
    ids=[
        "other_repository",
        "absent",
        "null",
        "string",
        "bool",
        "float",
        "head_only_missing",
        "base_only_missing",
        "repo_object_null",
    ],
)
def test_steering_refuses_a_pull_request_whose_repository_id_is_not_the_lineages(
    steering: SteeringStack, sides: tuple[str, ...], reported: object
) -> None:
    """The pull request read must report the stored repository id on both sides.

    The repository read still answers the stored id, so only the pull request
    payload's ``head.repo`` / ``base.repo`` is wrong: another repository's id,
    absent, null, a string, a bool, a float, or no repository object at all.
    Each is refused ``repository_mismatch``. Liveness: with the integer ids
    restored, the mention admits.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    pull = stack.github.pulls[run.pr]
    for side in sides:
        if reported is _NULL_REPO:
            pull[side]["repo"] = None
        elif reported is _ABSENT:
            pull[side]["repo"].pop("id")
        else:
            pull[side]["repo"]["id"] = reported

    _api_refused(
        stack.steer(writer.slack_user, "Rename it.", thread_ts=run.root_ts),
        "repository_mismatch",
    )
    assert stack.revisions(run) == []

    for side in ("head", "base"):
        pull[side]["repo"] = {"id": REPO_ID, "full_name": REPO}
    _admitted(stack.steer(writer.slack_user, "Rename it.", thread_ts=run.root_ts))
    _assert_no_sandbox(stack)


# --- controls 5, 7: an admission whose answer the worker never read --------------------


def _lose_one_admission(stack: SteeringStack, writer: Person, run: SteerRun) -> Delivery:
    """Attempt 1: the steering route commits the admission, the answer is lost.

    Returns the delivery, still pending, its placeholder saying the request is
    not confirmed, never inviting a resend.
    """

    stack.api_hop.drop_next_answer()
    lost = stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts)
    delivery = lost.delivery
    expected_id = stack.expected_request_id(delivery)
    # The route really ran and committed; the worker never read its answer.
    ((status, answer),) = stack.api_hop.dropped
    assert status == 200 and answer["outcome"] == "admitted", answer
    assert answer["request_id"] == str(expected_id)
    assert lost.replies == [], "the worker read the answer the hop dropped"
    assert stack.revisions(run) == [expected_id]
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts and texts[-1] == _steering().UNCONFIRMED_TEXT, texts
    assert all(call.token == stack.factory_token for call in lost.edits)
    assert stack.resend_invitations() == []
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    return delivery


def _replay_completes_once(
    stack: SteeringStack, worker: SteeringWorker, run: SteerRun, delivery: Delivery
) -> None:
    """A healthy redelivery: the admission replays, once, and the delivery settles."""

    expected_id = stack.expected_request_id(delivery)
    answers = len(stack.api_hop.steer_answers())
    assert stack.reclaim(worker) == 1
    ((status, replayed),) = stack.api_hop.steer_answers()[answers:]
    assert status == 200, replayed
    assert replayed["outcome"] == "admitted" and replayed.get("replayed") is True, replayed
    assert replayed["request_id"] == str(expected_id)
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == _steering().render_reply(
        "admitted", pull_request_url=run.pr_url, replayed=True
    ), texts
    _no_false_nothing_queued(texts)
    assert stack.is_terminal(delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.revisions(run) == [expected_id]
    assert stack.resend_invitations() == []
    _assert_no_sandbox(stack)


def test_a_lost_admission_answer_never_invites_a_resend_and_admits_once(
    steering: SteeringStack,
) -> None:
    """A steering answer lost after the admission committed is not a resend invitation.

    The worker cannot tell an admitted request from an unsent one, so the
    placeholder says the request is not confirmed and the delivery stays
    pending; the consumer's generic not-started notice (a resend is a new
    delivery id, so a second revision) is never posted. The consumer's reclaim
    pass redelivers it, the route replays the admission, and the WorkItem holds
    exactly one revision.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    assert "send it again" in stack.not_started()

    delivery = _lose_one_admission(stack, writer, run)
    _replay_completes_once(stack, stack.steerer, run, delivery)


def test_a_lost_answer_replay_after_a_login_rename_reports_the_committed_revision(
    steering: SteeringStack,
) -> None:
    """Plan addendum R1-1: a replay never turns a committed revision into a refusal.

    Attempt 1 commits the admission and its answer is lost. GitHub then
    reports a new login for the same numeric account, so the redelivery's
    requester differs from the committed request's. The route answers the
    committed request as admitted and replayed; the worker says so, with no
    refusal and no resend invitation, and the WorkItem holds one revision.

    Falsifiable: a route that passes the replay's identity mismatch through as
    a refusal answers ``admission_refused`` here.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    delivery = _lose_one_admission(stack, writer, run)
    assert writer.account_id is not None
    stack.github.accounts[writer.account_id].login = f"acme-renamed-{stack.tag}"

    _replay_completes_once(stack, stack.steerer, run, delivery)
    (revision,) = [row for row in stack.requests() if row["id"] != run.request_id]
    assert revision["requester"] == writer.requester


def test_a_refused_redelivery_after_a_lost_answer_never_claims_nothing_was_queued(
    steering: SteeringStack,
) -> None:
    """Plan addendum R1-1: a refusal reachable on a redelivery stays neutral.

    Attempt 1 committed the admission with its answer lost; before the
    redelivery the person's code host link is revoked, so the redelivery is
    refused. Its reply names the refusal and never claims nothing was queued,
    because a revision was. Liveness: the committed revision is still the
    WorkItem's one revision.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    delivery = _lose_one_admission(stack, writer, run)
    expected_id = stack.expected_request_id(delivery)
    stack.revoke(writer.github_link_ids[0])

    answers = len(stack.api_hop.steer_answers())
    assert stack.reclaim() == 1
    ((status, refused),) = stack.api_hop.steer_answers()[answers:]
    assert status == 200 and refused["outcome"] == "code_host_identity_missing", refused
    texts = stack.placeholder_texts(delivery.placeholder)
    assert "code_host_identity_missing" in texts[-1], texts
    _no_false_nothing_queued(texts)
    assert stack.resend_invitations() == []
    assert stack.revisions(run) == [expected_id]
    assert stack.pending() == 0
    _assert_no_sandbox(stack)


# --- controls 5, 6: a redelivery whose steering prerequisite fails ----------------------


@contextlib.contextmanager
def _prerequisite_fails(
    stack: SteeringStack, prerequisite: str, delivery: Delivery
) -> Iterator[Callable[[], bool]]:
    """One real failure of a steering prerequisite, for the attempt inside the block.

    Yields a check that the failure was actually met. ``receipt_read`` drops
    the worker's Valkey connection on each try of the receipt read;
    ``mapping_lookup`` revokes the worker role's SELECT on the factory run
    mapping; ``mapping_connection_refused`` closes the worker's Postgres
    endpoint, so the mapped root read gets the driver's own
    ``ConnectionRefusedError``.
    """

    if prerequisite == "receipt_read":
        key = _receipt_key(delivery.delivery_id)
        rule = stack.valkey_hop.drop(
            lambda command: command[0].upper() == "GET" and command[1:] == [key],
            times=_drops_for_one_failed_call(),
        )
        yield lambda: rule.remaining == 0
        rule.remaining = 0
    elif prerequisite == "mapping_lookup":
        _set_mapping_readable(stack, False)
        try:
            yield lambda: True
        finally:
            _set_mapping_readable(stack, True)
    else:
        stack.client.portal.call(stack.postgres_hop.close)
        try:
            yield lambda: True
        finally:
            stack.client.portal.call(stack.postgres_hop.start)


@pytest.mark.parametrize("redelivery", ["same_worker", "replacement_worker"])
@pytest.mark.parametrize(
    "prerequisite", ["receipt_read", "mapping_lookup", "mapping_connection_refused"]
)
def test_a_redelivery_whose_steering_prerequisite_fails_keeps_the_unconfirmed_reply(
    steering: SteeringStack, prerequisite: str, redelivery: str
) -> None:
    """A lost admission answer keeps its no-resend reply across a failed redelivery.

    Attempt 2, the consumer's reclaim (in the same process or a replacement
    that never saw attempt 1), cannot read a steering prerequisite, so it
    never asks the API and the delivery stays pending; the generic
    not-started notice never replaces the unconfirmed reply. Attempt 3,
    healthy, replays the admission once. Liveness: an ordinary turn in an
    unmapped thread that fails for an unrelated reason (the cluster refuses
    its sandbox) still gets the generic notice.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    delivery = _lose_one_admission(stack, writer, run)
    expected_id = stack.expected_request_id(delivery)
    worker = stack.steerer if redelivery == "same_worker" else stack.worker_process()

    answers = len(stack.api_hop.steer_answers())
    with _prerequisite_fails(stack, prerequisite, delivery) as failed:
        assert stack.reclaim(worker) == 1
        assert failed(), "attempt 2 never read the prerequisite"
    assert stack.api_hop.steer_answers()[answers:] == [], "attempt 2 reached the steering route"
    assert stack.resend_invitations() == [], "the generic notice invited a resend"
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == _steering().UNCONFIRMED_TEXT, texts
    _no_false_nothing_queued(texts)
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    assert stack.revisions(run) == [expected_id]

    _replay_completes_once(stack, worker, run, delivery)

    ordinary = stack.mention(
        writer.slack_user, "What does this service do?", thread_ts=UNMAPPED_THREAD
    )
    since = len(stack.k8s.requests)
    assert stack.consume(worker) is not None
    assert _reached_sandbox(stack, since), stack.k8s.requests
    assert stack.placeholder_texts(ordinary.placeholder)[-1] == stack.not_started()
    assert stack.revisions(run) == [expected_id]


def test_a_first_attempt_whose_mapping_connection_is_refused_stays_pending_without_a_notice(
    steering: SteeringStack,
) -> None:
    """Control 6: a refused Postgres connection on the very first attempt fails closed.

    No earlier attempt and so no steering-sent marker: the worker's Postgres
    endpoint goes down after the turn's identity preparation read it, so the
    mapped root read's reconnect gets the driver's own raw
    ``ConnectionRefusedError``. The turn is treated as possibly mapped: no
    steering request, no marker, no sandbox claim, no revision, the delivery
    stays pending, and the placeholder is left as it was, with no generic
    not-started notice. Liveness: once the database is reachable again, the
    consumer's reclaim admits the same delivery exactly once and settles it.

    Falsifiable: a lookup that catches only SQLAlchemy's errors lets the raw
    error out as an ordinary turn failure, and the consumer posts the generic
    notice inviting a resend.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    assert "send it again" in stack.not_started()

    # The database goes down right after the turn's binding classification
    # (``identity.prepare``) read it, so the mapped root read is the first
    # checkout to meet it: its pre-ping fails and its reconnect is refused.
    stack.postgres_hop.go_down_after(b"AS binding_generation")
    try:
        attempt = stack.steer(writer.slack_user, "Please rename the helper.", thread_ts=run.root_ts)
        assert stack.postgres_hop.went_down, "the turn never read its binding classification"
    finally:
        stack.client.portal.call(stack.postgres_hop.start)
    delivery = attempt.delivery
    assert stack.resend_invitations() == [], "the generic notice invited a resend"
    assert attempt.edits == [], [call.text for call in attempt.edits]
    assert stack.api_hop.steer_answers() == [], "the turn reached the steering route"
    assert stack.api_hop.dropped == []
    assert stack.valkey.exists(stack.steering_sent_key(delivery.delivery_id)) == 0
    assert stack.revisions(run) == []
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    _assert_no_sandbox(stack)

    assert stack.reclaim() == 1
    ((status, admitted),) = stack.api_hop.steer_answers()
    assert status == 200 and admitted["outcome"] == "admitted", admitted
    assert admitted.get("replayed") is not True, admitted
    assert admitted["request_id"] == str(stack.expected_request_id(delivery))
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == _steering().render_reply("admitted", pull_request_url=run.pr_url), texts
    assert stack.revisions(run) == [stack.expected_request_id(delivery)]
    assert stack.is_terminal(delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.resend_invitations() == []
    _assert_no_sandbox(stack)


@pytest.mark.parametrize("redelivery", ["same_worker", "replacement_worker"])
def test_a_redelivery_whose_done_check_fails_after_a_lost_answer_never_invites_a_resend(
    steering: SteeringStack, redelivery: str
) -> None:
    """The durable steering-sent marker alone keeps the resend invitation off.

    Attempt 2, in the same process or a replacement that never saw attempt 1, fails its
    initial done-check (its Valkey connection drops on each try of that read),
    so it raises before steering is consulted. The consumer's notice-path
    terminality read then succeeds and answers "not terminal", which alone
    would post the generic not-started notice. The marker written before
    attempt 1's request keeps it off. Attempt 3 replays once.

    Falsifiable: without the marker check in ``notify_turn_not_started`` the
    generic notice replaces the unconfirmed reply here.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    delivery = _lose_one_admission(stack, writer, run)
    worker = stack.steerer if redelivery == "same_worker" else stack.worker_process()
    done_key = worker.config.done_key(delivery.delivery_id)

    answers = len(stack.api_hop.steer_answers())
    rule = stack.valkey_hop.drop(
        lambda command: command[0].upper() == "EXISTS" and command[1:] == [done_key],
        times=_drops_for_one_failed_call(),
    )
    assert stack.reclaim(worker) == 1
    assert rule.remaining == 0, "attempt 2 never read the done-check"
    assert stack.api_hop.steer_answers()[answers:] == [], "attempt 2 reached the steering route"
    assert stack.resend_invitations() == [], "the generic notice invited a resend"
    assert stack.placeholder_texts(delivery.placeholder)[-1] == _steering().UNCONFIRMED_TEXT
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)

    _replay_completes_once(stack, worker, run, delivery)


# --- control 4: no steering request leaves before its intent is durable ----------------


def test_a_steering_attempt_whose_intent_cannot_be_recorded_sends_nothing(
    steering: SteeringStack,
) -> None:
    """A failed steering-sent marker write sends nothing and keeps the delivery pending.

    The marker is what later attempts trust to keep the resend invitation off
    a thread whose admission may have committed, so no request leaves without
    it: the steering route is never called, no revision is admitted, and the
    generic notice is not posted either. A healthy reclaim then records the
    intent, admits once and settles, and the marker outlives it as long as the
    done marker does.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    rules: list[_DropRule] = []

    def fail_the_marker_write(delivery: Delivery) -> None:
        key = stack.steering_sent_key(delivery.delivery_id)
        rules.append(
            stack.valkey_hop.drop(
                lambda command: command[0].upper() == "SET" and command[1:2] == [key],
                times=_drops_for_one_failed_call(),
            )
        )

    attempt = stack.steer(
        writer.slack_user,
        "Please rename the helper.",
        thread_ts=run.root_ts,
        before_consume=fail_the_marker_write,
    )
    delivery = attempt.delivery
    (rule,) = rules
    assert rule.remaining == 0, "the steering-sent marker was never written"
    assert stack.api_hop.steer_answers() == [], "the request left without its intent"
    assert stack.api_hop.dropped == []
    assert stack.revisions(run) == [], "a revision was admitted without its intent recorded"
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    assert stack.resend_invitations() == [], "the generic notice invited a resend"

    answers = len(stack.api_hop.steer_answers())
    assert stack.reclaim() == 1
    ((status, admitted),) = stack.api_hop.steer_answers()[answers:]
    assert status == 200 and admitted["outcome"] == "admitted", admitted
    assert admitted.get("replayed") is not True, admitted
    assert admitted["request_id"] == str(stack.expected_request_id(delivery))
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == _steering().render_reply("admitted", pull_request_url=run.pr_url), texts
    assert stack.is_terminal(delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.revisions(run) == [stack.expected_request_id(delivery)]
    assert stack.resend_invitations() == []
    sent_ttl = stack.valkey.ttl(stack.steering_sent_key(delivery.delivery_id))
    done_ttl = stack.valkey.ttl(stack.steerer.config.done_key(delivery.delivery_id))
    assert sent_ttl > 0 and done_ttl > 0, (sent_ttl, done_ttl)
    assert sent_ttl >= done_ttl - 60, (sent_ttl, done_ttl)
    _assert_no_sandbox(stack)


def test_an_ordinary_turn_in_an_unmapped_thread_still_gets_the_generic_notice(
    steering: SteeringStack,
) -> None:
    """The steering intent marker never reaches ordinary chat.

    An ordinary turn in a thread no factory run maps fails before it starts
    (the cluster refuses its sandbox). No steering request was sent and no
    steering-sent marker exists; the generic not-started notice is posted, as
    for every other failed turn.
    """

    stack = steering
    writer = stack.person()
    stack.open_steered_run()
    ordinary = stack.mention(
        writer.slack_user, "What does this service do?", thread_ts=UNMAPPED_THREAD
    )
    assert stack.consume() is not None
    assert _reached_sandbox(stack, 0), stack.k8s.requests
    assert stack.api_hop.steer_answers() == []
    assert stack.placeholder_texts(ordinary.placeholder)[-1] == stack.not_started()
    assert stack.pending() == 1
    assert stack.valkey.exists(stack.steering_sent_key(ordinary.delivery_id)) == 0


# --- a persistence failure at admission never leaks the Slack body ---------------------


@contextlib.contextmanager
def _api_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """Every log record the in-process servers emit at DEBUG, uvicorn's own included.

    ``_servers`` builds uvicorn at ``critical`` with its default logging
    config, whose ``uvicorn`` logger does not propagate to the root. A
    deployed API logs ``uvicorn.error`` at ``info``, where an unhandled route
    error is logged as "Exception in ASGI application" with its traceback, so
    the capture lowers those loggers to DEBUG and listens on ``uvicorn`` too.
    SQLAlchemy keeps its own WARNING level: below it, the engine echoes every
    statement with its parameters, which is an operator's choice, not the
    deployed default.
    """

    uvicorn_logger = logging.getLogger("uvicorn")
    caplog.set_level(logging.DEBUG)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.asgi", "uvicorn.access"):
        caplog.set_level(logging.DEBUG, logger=name)
    uvicorn_logger.addHandler(caplog.handler)
    try:
        # uvicorn logs an unhandled route error on ``uvicorn.error``; it must reach us.
        error_logger: logging.Logger | None = logging.getLogger("uvicorn.error")
        assert error_logger is not None and error_logger.isEnabledFor(logging.ERROR)
        reaches: list[logging.Handler] = []
        while error_logger is not None:
            reaches.extend(error_logger.handlers)
            error_logger = error_logger.parent if error_logger.propagate else None
        assert caplog.handler in reaches, "uvicorn's error records would not be captured"
        yield
    finally:
        uvicorn_logger.removeHandler(caplog.handler)


def _records_carrying(caplog: pytest.LogCaptureFixture, needle: str) -> list[str]:
    """The captured records whose written form (message, traceback, stack) has ``needle``."""

    formatter = logging.Formatter("%(name)s %(levelname)s %(message)s")
    return [
        f"{record.name}: {record.getMessage()[:80]}"
        for record in caplog.records
        if needle in formatter.format(record)
    ]


def test_a_persistence_failure_at_admission_answers_503_and_never_logs_the_slack_body(
    steering: SteeringStack, caplog: pytest.LogCaptureFixture
) -> None:
    """Security review r1: a failed admission write keeps the Slack body out of logs.

    A real Postgres failure at the write that persists the revision: a trigger
    on ``execution_requests`` raises for this WorkItem's row carrying the
    person's message. The SQLAlchemy error carries that statement's bound
    parameters, so letting it out of the route has uvicorn log a traceback
    with the complete Slack body. The route answers a fixed retryable 503
    ``persistence_unavailable`` instead; the body is in no log record and not
    in the answer. The worker keeps the delivery pending with the unconfirmed
    reply, never a resend invitation or a "nothing was queued" claim; no
    revision exists and no sandbox is claimed. Liveness: with the trigger
    gone, the consumer's reclaim admits the same delivery exactly once.

    Falsifiable: a route that lets the database error escape answers 500 and
    uvicorn's "Exception in ASGI application" record carries the marker.
    """

    stack = steering
    writer = stack.person()
    run = stack.open_steered_run()
    marker = f"acme-private-slack-body-{uuid.uuid4().hex}"
    body = f"Please rename the helper.\nInternal note: {marker}"
    schema = get_settings().db_schema
    function = f'"{schema}".steer_persist_fails_{stack.tag}'
    # Dropping the function drops its trigger with it.
    drop = f"DROP FUNCTION IF EXISTS {function}() CASCADE"
    stack.base.owned.callback(_rows, drop)
    _rows(
        f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN "
        f"IF NEW.work_item_id = '{run.work_item_id}' AND NEW.objective LIKE '%{marker}%' "
        "THEN RAISE EXCEPTION 'execution request write refused'; END IF; "
        "RETURN NEW; END $fn$"
    )
    _rows(
        f"CREATE TRIGGER steer_persist_fails_{stack.tag} BEFORE INSERT OR UPDATE "
        f'ON "{schema}".execution_requests FOR EACH ROW EXECUTE FUNCTION {function}()'
    )

    with _api_logs(caplog):
        attempt = stack.steer(writer.slack_user, body, thread_ts=run.root_ts)
    delivery = attempt.delivery
    leaked = _records_carrying(caplog, marker)
    assert leaked == [], f"the Slack body was logged by {leaked}"
    ((status, answer),) = attempt.replies
    assert marker not in json.dumps(answer), answer
    assert (status, _detail_code(answer)) == (503, "persistence_unavailable"), (status, answer)
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts and texts[-1] == _steering().UNCONFIRMED_TEXT, texts
    assert all(marker not in text_ for text_ in texts), texts
    _no_false_nothing_queued(texts)
    assert stack.resend_invitations() == [], "the generic notice invited a resend"
    assert stack.revisions(run) == []
    assert stack.pending() == 1
    assert not stack.is_terminal(delivery.delivery_id)
    _assert_no_sandbox(stack)

    _rows(drop)
    answers = len(stack.api_hop.steer_answers())
    assert stack.reclaim() == 1
    ((status, admitted),) = stack.api_hop.steer_answers()[answers:]
    assert status == 200 and admitted["outcome"] == "admitted", admitted
    assert admitted.get("replayed") is not True, admitted
    expected_id = stack.expected_request_id(delivery)
    assert admitted["request_id"] == str(expected_id)
    assert stack.revisions(run) == [expected_id]
    (revision,) = [row for row in stack.requests() if row["id"] == expected_id]
    assert _objective_reply(revision["objective"])["body"] == body
    texts = stack.placeholder_texts(delivery.placeholder)
    assert texts[-1] == _steering().render_reply("admitted", pull_request_url=run.pr_url), texts
    assert stack.is_terminal(delivery.delivery_id)
    assert stack.pending() == 0
    assert stack.resend_invitations() == []
    _assert_no_sandbox(stack)


def _detail_code(answer: Any) -> str | None:
    detail = answer.get("detail") if isinstance(answer, dict) else None
    return detail.get("code") if isinstance(detail, dict) else None
