"""Factory Slack kickoff helpers over the runtime harness (#5380).

``test_factory_slack_runtime``'s steering stack serves the real
``create_app()`` (a disposable migrated Postgres, real Valkey and RustFS) on
uvicorn, the dispatcher's Bolt app through ``SocketModeHandler``, and the
worker as ``run.build`` composes it. This module adds what a kickoff needs on
top of it, imported by name (the repository precedent for shared fixtures
under ``--import-mode=importlib``): mentions at a channel root or in an
unmapped thread, delivered natively or as the receipt the dispatcher writes
for the kickoff route; enrollment headers; per-issue GitHub answers; a paused
or failing GitHub read; a root key held by an uncommitted row; and the
production enrollment disconnect.

Only external services are stand-ins: the Slack Web API (``FakeSlack``) and
GitHub REST (``GitHubTruth``). Event shapes follow
https://docs.slack.dev/reference/events/app_mention. A declared driver's
mention follows the shape observed over Socket Mode on a real test workspace,
2026-11-12: ``user`` is the sending bot's own bot user, ``bot_id`` and
``bot_profile`` are set, ``team`` is the workspace, and there is no
``user_team``; the envelope carries ``authorizations`` and
``is_ext_shared_channel: false``. Enrollment headers are seeded with raw SQL,
standing in for #5377's enrollment writes. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.db import create_sessionmaker
from curie_api.factory_poll_intake import poll_once
from curie_api.models import DEFAULT_TENANT_ID
from curie_api.personal_enrollments import (
    Custody,
    EnrollmentRefusal,
    EnrollmentStatus,
    disconnect,
)
from curie_channel_gateway.slack.identities import delivery_key, minted_adapter
from curie_channel_gateway.slack.identity import driver_evidence, slack_evidence
from curie_internal.driver_declaration import DeclaredDriver
from curie_internal.slack_mention_receipt import SlackMentionReceipt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import REPO, PlantedIssue  # noqa: E402
from personal_enrollment_support import (  # noqa: E402
    GRANT_NAMESPACE,
    STAGE_LEASE_SECONDS,
    FakeCustodyStore,
)

from apps.api.tests import test_factory_slack_runtime as runtime  # noqa: E402
from apps.api.tests.test_factory_slack_runtime import (  # noqa: E402
    CHANNEL,
    FACTORY_BOT_USER,
    OWNER,
    PLACEHOLDER,
    TEAM,
    Delivery,
    Person,
    SlackCall,
    SteeringStack,
    _receipt_key,
    _rows,
)

KICKOFF_PATH = "/v1/internal/factory-slack/kickoff"
# The owner's enrollment generation. Not 1, so a tuple that names a default
# generation instead of the enrolled one is caught.
GENERATION = 2
OTHER_REPO = "acme-corp/acme-elsewhere"
DRIVER_BOT_ID = "B0EXAMPLE5"
DRIVER_BOT_USER = "U0EXAMPLE5"
DRIVER_APP_ID = "A0EXAMPLE5"

_ISSUES = itertools.count(8401)
_TS = itertools.count(1)
_ASKS = ThreadPoolExecutor(max_workers=4, thread_name_prefix="kickoff-ask")


# --- inputs --------------------------------------------------------------------------


def next_issue() -> int:
    return next(_ISSUES)


def issue_url(number: int, repo: str = REPO) -> str:
    return f"https://github.com/{repo}/issues/{number}"


def linking(number: int, repo: str = REPO, *, lead: str = "Please start") -> str:
    """Message text naming one issue, as Slack wraps a typed URL."""

    return f"{lead} <{issue_url(number, repo)}>"


def fresh_ts() -> str:
    return f"1800000800.{next(_TS):06d}"


def plant_issue(stack: SteeringStack, number: int, **answers: Any) -> PlantedIssue:
    """The issue's own GitHub answers: open with no labels unless ``answers`` say."""

    planted = PlantedIssue(**answers)
    stack.github.planted_issues[number] = planted
    return planted


def _event_id(stack: SteeringStack) -> str:
    return f"Ev0{stack.tag.upper()}{uuid.uuid4().hex[:8].upper()}"


# --- identity, enrollment and bindings ------------------------------------------------


def enroll(principal_id: uuid.UUID, *, generation: int = GENERATION, active: bool = True) -> None:
    """An enrollment header, standing in for #5377's enrollment writes."""

    _rows(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, :active, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": DEFAULT_TENANT_ID,
            "principal": principal_id,
            "generation": generation,
            "active": active,
        },
    )


def enrolled(stack: SteeringStack, *, github: bool = False) -> Person:
    """A Slack-linked person enrolled at `GENERATION`; a kickoff needs no code host link."""

    person = stack.person(github=github)
    enroll(person.principal_id)
    return person


def bind_github_surface(stack: SteeringStack, repo: str, *, agent_id: str | None = None) -> None:
    """Bind ``repo`` as one more GitHub surface of the agent, through the agents API."""

    response = stack.client.post(
        f"/agents/{agent_id or stack.agent_id}/channels",
        headers=stack.platform,
        json={"kind": "github", "address": repo},
    )
    assert response.status_code == 201, response.text


def report_auth_test(stack: SteeringStack) -> None:
    """What the dispatcher's preflight reports of its own ``auth.test``, which a
    mention with no ``user_team`` (a bot's) is judged against."""

    reported = stack.client.post(
        "/identity/slack-reports",
        headers=stack.platform,
        json={
            "name": stack.identity,
            "team_id": TEAM,
            "enterprise_id": None,
            "enterprise_id_present": True,
            "is_enterprise_install": False,
        },
    )
    assert reported.status_code == 200, reported.text


def declare_driver(
    stack: SteeringStack, monkeypatch: pytest.MonkeyPatch, *, api: bool = True
) -> DeclaredDriver:
    """ADR 0202: declare one driver bot in this channel at the dispatcher and,
    with ``api``, at the API too."""

    driver = DeclaredDriver(channel_id=CHANNEL, bot_id=DRIVER_BOT_ID, bot_user_id=DRIVER_BOT_USER)
    if api:
        settings = get_settings()
        monkeypatch.setattr(settings, "test_installation_enabled", True)
        monkeypatch.setattr(settings, "test_installation_drivers", (driver,))
    stack.dispatcher_config = stack.dispatcher_config.model_copy(
        update={"test_installation_enabled": True, "test_installation_drivers": (driver,)}
    )
    report_auth_test(stack)
    return driver


def disconnect_owner(stack: SteeringStack, principal_id: uuid.UUID) -> Future[Any]:
    """Start the production enrollment disconnect on the API's event loop."""

    (header,) = _rows(
        "SELECT version FROM curie.personal_claude_enrollments "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"tenant": DEFAULT_TENANT_ID, "principal": principal_id},
    )
    assert runtime._DB is not None, "the stack fixture owns the shared engine"
    engine = runtime._DB.engine

    async def go() -> EnrollmentStatus | EnrollmentRefusal:
        custody = Custody(
            sessionmaker=create_sessionmaker(engine),
            store=FakeCustodyStore(),
            namespace=GRANT_NAMESPACE,
            stage_lease_seconds=STAGE_LEASE_SECONDS,
        )
        return await disconnect(
            custody,
            tenant_id=DEFAULT_TENANT_ID,
            principal_id=principal_id,
            expected_version=int(header["version"]),
        )

    future: Future[Any] = stack.client.portal.start_task_soon(go)
    return future


# --- mentions as the kickoff route reads them ------------------------------------------


@dataclass(frozen=True)
class Mention:
    """One mention as the worker hands it to the kickoff route."""

    delivery_id: str
    channel: str
    root_ts: str
    message_ts: str
    author: str
    receipt: dict[str, Any]

    def payload(self, identity: str, **overrides: Any) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "channel_identity": identity,
            "channel": self.channel,
            "root_ts": self.root_ts,
            "author": self.author,
            "receipt": self.receipt,
            **overrides,
        }


def _mention(
    stack: SteeringStack,
    body: dict[str, Any],
    *,
    version: Literal[1, 2],
    user_id: str,
    evidence: dict[str, Any] | None,
    driver_bot_id: str | None,
) -> Mention:
    """The receipt the dispatcher writes for ``body``, built by its own types."""

    event = body["event"]
    message_ts = str(event["ts"])
    root = str(event.get("thread_ts") or message_ts)
    delivery_id = delivery_key(str(body["event_id"]), stack.identity)
    assert evidence is not None, "the event names no sender"
    receipt = SlackMentionReceipt(
        version=version,
        delivery_id=delivery_id,
        channel_identity=minted_adapter(stack.identity),
        channel=str(event["channel"]),
        root_ts=root,
        message_ts=message_ts,
        user_id=user_id,
        body=str(event["text"]),
        evidence=evidence,
        driver_bot_id=driver_bot_id,
    )
    return Mention(
        delivery_id=delivery_id,
        channel=str(event["channel"]),
        root_ts=root,
        message_ts=message_ts,
        author=user_id,
        receipt=receipt.model_dump(mode="json"),
    )


def person_mention(
    stack: SteeringStack,
    user: str,
    text_: str,
    *,
    thread_ts: str | None = None,
    team: str = TEAM,
    channel: str = CHANNEL,
) -> Mention:
    """A person's top-level mention, or a reply in ``thread_ts``, and its version 1 receipt."""

    event: dict[str, Any] = {
        "type": "app_mention",
        "channel": channel,
        "text": f"<@{FACTORY_BOT_USER}> {text_}",
        "ts": fresh_ts(),
        "team": team,
        "user": user,
        "user_team": team,
    }
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
    body = {"type": "event_callback", "event_id": _event_id(stack), "team_id": team, "event": event}
    return _mention(
        stack, body, version=1, user_id=user, evidence=slack_evidence(body), driver_bot_id=None
    )


def driver_mention(
    stack: SteeringStack,
    text_: str,
    *,
    thread_ts: str | None = None,
    channel: str = CHANNEL,
    bot_id: str = DRIVER_BOT_ID,
    event_user: str = DRIVER_BOT_USER,
) -> Mention:
    """A declared driver's marked mention and its version 2 receipt.

    The envelope and event follow the shape observed over Socket Mode on a real
    test workspace, 2026-11-12. ``event_user`` is what the event claims its
    user is; the receipt names the declared bot user whatever it says, as
    the dispatcher's ``driver_evidence`` does.
    """

    message_ts = fresh_ts()
    event: dict[str, Any] = {
        "app_id": DRIVER_APP_ID,
        "blocks": [],
        "bot_id": bot_id,
        "bot_profile": {
            "app_id": DRIVER_APP_ID,
            "deleted": False,
            "icons": {},
            "id": bot_id,
            "name": "acme-driver",
            "team_id": TEAM,
            "updated": 1800000000,
            "user_id": event_user,
        },
        "channel": channel,
        "event_ts": message_ts,
        "team": TEAM,
        "text": f"<@{FACTORY_BOT_USER}> [test action] {text_}",
        "ts": message_ts,
        "type": "app_mention",
        "user": event_user,
    }
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
        event["parent_user_id"] = FACTORY_BOT_USER
    body: dict[str, Any] = {
        "api_app_id": "A0EXAMPLE1",
        "authorizations": [
            {
                "enterprise_id": None,
                "is_bot": True,
                "is_enterprise_install": False,
                "team_id": TEAM,
                "user_id": FACTORY_BOT_USER,
            }
        ],
        "event": event,
        "event_context": "EC0EXAMPLE1",
        "event_id": _event_id(stack),
        "event_time": 1800000000,
        "is_ext_shared_channel": False,
        "team_id": TEAM,
        "token": "placeholder",
        "type": "event_callback",
    }
    return _mention(
        stack,
        body,
        version=2,
        user_id=DRIVER_BOT_USER,
        evidence=driver_evidence(body, DRIVER_BOT_USER),
        driver_bot_id=bot_id,
    )


def ask(stack: SteeringStack, mention: Mention, **overrides: Any) -> httpx.Response:
    """POST the mention to the kickoff route, as the worker does."""

    return stack.client.post(
        KICKOFF_PATH, headers=stack.internal, json=mention.payload(stack.identity, **overrides)
    )


def decide(stack: SteeringStack, mention: Mention, **overrides: Any) -> dict[str, Any]:
    """The route's 200 decision for ``mention``."""

    response = ask(stack, mention, **overrides)
    assert response.status_code == 200, response.text
    assert response.headers.get("cache-control") == "no-store", response.headers
    body: dict[str, Any] = response.json()
    return body


def ask_over_http(stack: SteeringStack, mention: Mention) -> Future[httpx.Response]:
    """The same POST over the API's socket from another thread, so the test can
    act while the route is paused or blocked."""

    return _ASKS.submit(
        httpx.post,
        f"{stack.base.api_url}{KICKOFF_PATH}",
        headers=stack.internal,
        json=mention.payload(stack.identity),
        timeout=60,
    )


# --- native delivery through the dispatcher and the worker ------------------------------


def kickoff_mention(
    stack: SteeringStack,
    user: str,
    text_: str,
    *,
    thread_ts: str | None = None,
    message_ts: str | None = None,
    team: str = TEAM,
) -> Delivery:
    """One person's ``app_mention`` at a channel root (no ``thread_ts``), or in a
    thread, delivered through the dispatcher's Bolt app."""

    event_id = _event_id(stack)
    delivery_id = delivery_key(event_id, stack.identity)
    stack.base.doomed_keys.append(_receipt_key(delivery_id))
    message_ts = message_ts or fresh_ts()
    root = thread_ts or message_ts
    event: dict[str, Any] = {
        "type": "app_mention",
        "channel": CHANNEL,
        "text": f"<@{FACTORY_BOT_USER}> {text_}",
        "ts": message_ts,
        "team": team,
        "user": user,
        "user_team": team,
    }
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
    before = len(stack.slack.calls)
    stack._deliver(event, event_id)
    placeholders = [
        call
        for call in stack.slack.calls[before:]
        if call.method == "chat.postMessage"
        and call.ok
        and call.text == PLACEHOLDER
        and call.params.get("thread_ts") == root
    ]
    placeholder = str(placeholders[-1].response["ts"]) if placeholders else None
    return Delivery(event_id, delivery_id, message_ts, placeholder, str(event["text"]))


def kickoff_answers(stack: SteeringStack) -> list[tuple[int, Any]]:
    """Every answer the worker received from the kickoff route, in order."""

    return [
        (status, body) for path, status, body in stack.api_hop.exchanges if path == KICKOFF_PATH
    ]


@dataclass
class Kick:
    delivery: Delivery
    answers: list[tuple[int, Any]]
    edits: list[SlackCall]

    @property
    def answer(self) -> dict[str, Any]:
        assert self.answers, "the kickoff route was never asked"
        status, body = self.answers[-1]
        assert status == 200, (status, body)
        assert isinstance(body, dict), body
        return body


def kick(stack: SteeringStack, user: str, text_: str, *, thread_ts: str | None = None) -> Kick:
    """Deliver natively, let the worker consume it, and collect what it got and said."""

    answered = len(kickoff_answers(stack))
    calls = len(stack.slack.calls)
    delivery = kickoff_mention(stack, user, text_, thread_ts=thread_ts)
    assert stack.consume() is not None, "the dispatcher enqueued nothing"
    edits = [
        call
        for call in stack.slack.calls[calls:]
        if call.method == "chat.update"
        and delivery.placeholder is not None
        and call.params.get("ts") == delivery.placeholder
    ]
    return Kick(delivery, kickoff_answers(stack)[answered:], edits)


# --- state reads ----------------------------------------------------------------------


def counts() -> dict[str, int]:
    (row,) = _rows(
        "SELECT (SELECT count(*) FROM curie.work_items) AS work_items, "
        "(SELECT count(*) FROM curie.execution_requests) AS requests, "
        "(SELECT count(*) FROM curie.factory_slack_threads) AS threads"
    )
    return {key: int(value) for key, value in row.items()}


def work_items_of(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, trigger_label, base_branch, base_source, base_commit, model_auth_mode, "
        "owner_principal_id, enrollment_generation, cancelled_at, code_host_installation_id "
        "FROM curie.work_items WHERE tracker_issue_id = :number",
        {"number": str(number)},
    )


def requests_of(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, sequence, status, objective, requester, reply_kind, reply_address, "
        "reply_conversation_id FROM curie.execution_requests WHERE work_item_id = :id "
        "ORDER BY sequence",
        {"id": work_item_id},
    )


def threads_at(root_ts: str) -> list[dict[str, Any]]:
    return _rows(
        "SELECT work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence, "
        "origin FROM curie.factory_slack_threads WHERE root_ts = :root",
        {"root": root_ts},
    )


def start_refusal(stack: SteeringStack, request_id: uuid.UUID) -> str | None:
    """Acquire and start the request as a worker does: the refusal code, or None
    when it started."""

    view = stack.request_view(request_id)
    generation = view["dispatch_generation"]
    acquired = stack.client.post(
        f"/v1/internal/work-items/requests/{request_id}/acquire",
        headers=stack.internal,
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
    if acquired.status_code != 200:
        return runtime._code(acquired) or f"http_{acquired.status_code}"
    started = stack.client.post(
        f"/v1/internal/work-items/requests/{request_id}/start",
        headers=stack.internal,
        json={
            "owner": OWNER,
            "generation": generation,
            "claim_name": f"claim-{request_id.hex[:12]}",
            "sandbox_name": f"sbx-{request_id.hex[:12]}",
            "context": acquired.json()["context"],
        },
    )
    if started.status_code != 200:
        return runtime._code(started) or f"http_{started.status_code}"
    return None


def poll_pass(stack: SteeringStack) -> None:
    """One production factory poll pass over the stack's GitHub stand-in."""

    app = stack.client.app
    stack.client.portal.call(
        poll_once, app.state.sessionmaker, get_settings(), app.state.http_client
    )


# --- interleavings ---------------------------------------------------------------------


@dataclass(frozen=True)
class GitHubPause:
    entered: threading.Event
    release: threading.Event


def pause_github(
    stack: SteeringStack,
    monkeypatch: pytest.MonkeyPatch,
    matches: Callable[[httpx.Request], bool],
    *,
    skip: int = 0,
) -> GitHubPause:
    """Hold the API's next GitHub read that ``matches`` (after ``skip`` matching
    reads) until ``release`` is set. The wait runs on a thread, so the API's
    event loop keeps serving while the read is held."""

    truth = stack.github
    truth.permission_entered.clear()
    truth.permission_release.clear()
    seen = itertools.count()
    armed = [True]

    def take_pause(request: httpx.Request) -> bool:
        if armed[0] and matches(request) and next(seen) >= skip:
            armed[0] = False
            return True
        return False

    monkeypatch.setattr(truth, "take_pause", take_pause)
    return GitHubPause(truth.permission_entered, truth.permission_release)


def fail_github(
    stack: SteeringStack,
    monkeypatch: pytest.MonkeyPatch,
    matches: Callable[[httpx.Request], bool],
    *,
    times: int = 1,
) -> list[str]:
    """Answer the next ``times`` matching GitHub reads 503, GitHub's answer when
    it cannot serve a request now
    (https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api).
    Returns the failed paths as they happen."""

    truth = stack.github
    original = truth.handle
    failed: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if len(failed) < times and matches(request):
            failed.append(request.url.path)
            return httpx.Response(503, json={"message": "Service Unavailable"})
        return original(request)

    monkeypatch.setattr(truth, "handle", handle)
    return failed


def issue_read(number: int) -> Callable[[httpx.Request], bool]:
    path = f"/repos/{REPO}/issues/{number}"
    return lambda request: request.method == "GET" and request.url.path == path


def branch_read(request: httpx.Request) -> bool:
    return request.url.path.startswith(f"/repos/{REPO}/git/ref/heads/")


class HeldRoot:
    """An uncommitted ``factory_slack_threads`` row at one root key, on its own
    connection and event loop, so a kickoff's association insert at that key
    waits on it until it ends."""

    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values
        self.pid = 0
        self.commit = False
        self._ready = threading.Event()
        self._release = threading.Event()
        self._errors: list[BaseException] = []
        self._thread = threading.Thread(target=self._run, name="held-root", daemon=True)

    def _run(self) -> None:
        try:
            asyncio.run(self._hold())
        except BaseException as exc:  # noqa: BLE001 - reported to the test thread
            self._errors.append(exc)
        finally:
            self._ready.set()

    async def _hold(self) -> None:
        engine: AsyncEngine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                transaction = await connection.begin()
                self.pid = int(
                    (await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                await connection.execute(
                    text(
                        "INSERT INTO curie.factory_slack_threads (work_item_id, agent_id, "
                        "channel_identity, channel, root_ts, first_sequence, origin) VALUES "
                        "(:work_item, :agent, :identity, :channel, :root, 1, 'configured')"
                    ),
                    self._values,
                )
                self._ready.set()
                await asyncio.to_thread(self._release.wait, 60)
                if self.commit:
                    await transaction.commit()
                else:
                    await transaction.rollback()
        finally:
            await engine.dispose()

    def start(self) -> None:
        self._thread.start()
        assert self._ready.wait(20), "the root key was never held"
        if self._errors:
            raise self._errors[0]

    def end(self) -> None:
        self._release.set()
        self._thread.join(30)
        assert not self._thread.is_alive(), "the held root never ended"
        if self._errors:
            raise self._errors[0]


@contextlib.contextmanager
def held_root(stack: SteeringStack, root_ts: str, work_item_id: uuid.UUID) -> Iterator[HeldRoot]:
    """Hold this channel's ``root_ts`` with ``work_item_id``'s row; rolled back on
    exit unless the caller sets ``commit``."""

    held = HeldRoot(
        {
            "work_item": work_item_id,
            "agent": uuid.UUID(stack.agent_id),
            "identity": stack.identity,
            "channel": CHANNEL,
            "root": root_ts,
        }
    )
    held.start()
    try:
        yield held
    finally:
        held.end()


def blocked_by(pid: int) -> list[int]:
    """The backends waiting on a lock ``pid`` holds."""

    rows = _rows(
        "SELECT pid FROM pg_stat_activity WHERE CAST(:holder AS integer) = "
        "ANY(pg_blocking_pids(pid)) ORDER BY pid",
        {"holder": pid},
    )
    return [int(row["pid"]) for row in rows]


def wait_blocked_by(pid: int, *, timeout_s: float = 20.0) -> int:
    """The one backend that comes to wait on ``pid``, polled without sleeping."""

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        waiting = blocked_by(pid)
        if waiting:
            return waiting[0]
    raise AssertionError(f"nothing came to wait on backend {pid}")


# --- the stack -------------------------------------------------------------------------


@contextlib.contextmanager
def kickoff_stack(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, token: bool = True
) -> Iterator[SteeringStack]:
    """The steering stack with the receiving identity's ``auth.test`` reported."""

    with runtime._steering_stack(monkeypatch, tmp_path, token=token) as stack:
        report_auth_test(stack)
        yield stack


@pytest.fixture
def kickoff(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[SteeringStack]:
    with kickoff_stack(monkeypatch, tmp_path) as stack:
        yield stack
