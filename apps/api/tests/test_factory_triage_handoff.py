"""A published triage ticket is handed to implementation once, on the labeller's authority (#5588).

Real Postgres, the real API app and only GitHub's HTTP API substituted
(`TriageGitHub`, extending the intake notice fixture's `NoticeGitHub`). The
source issue is admitted by a real poll pass; the triage run's ticket is
delivered by the real `deliver` against an in-memory tracker that stands in
for the triage session's tracker writes, so the receipt, its transaction and
the internal selection event it writes (`open_handoff`) are the production
ones. The created output issue is then mirrored into `TriageGitHub`, as the
App-authored issue the handoff reads, labels and admits over GitHub HTTP.

`TriageGitHub` holds those output issues with their labels (list, add and
remove label routes), their label timeline, a body edit and a close, a
revocable collaborator permission, a permission read that fails, and
injected failures on the label writes and the issue read.

Criteria (names carry the receipt keywords): AC1 ``delegation``, AC2
``admitted``, AC3 ``replay``, AC4 ``refus``, AC11 ``migration``. Time is moved
by SQL on the handoff and lease rows; nothing sleeps.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import itertools
import json
import tarfile
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.factory_tracker_result_delivery import (
    deliver,
    output_identity_for,
    source_fingerprint,
    submit_result,
)
from curie_api.factory_tracker_result_runtime import source_content_sha256
from curie_api.factory_tracker_results import ResultProposal, parse_proposal
from curie_api.factory_triage_handoff import (
    hand_off,
    implementation_request_id,
    open_handoff,
    selection_event_id,
)
from curie_api.factory_triage_status import recorded_handoff
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.types import ReplyTarget, SourceContent, TrackerIssueRef
from curie_api.main import create_app
from curie_api.models import ExecutionRequest, FactoryTrackerResultGrant, WorkItem
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from forge_fakes.github import (
    APP_ID,
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    github_repository_binding,
)
from forge_fakes.github import factory_env as _factory_env
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_factory_intake_notices import NoticeGitHub, _client_type, _pass, _sql, _sync_client_type
from test_factory_poll_intake import _iso
from test_github_factory_ingress import _rows

# The implementation agent serves the tracker's own label; the triage agent an
# explicit one. The triage policy names the implementation label.
IMPL = LABEL
TRIAGE = "factory-triage"
BASE = "base:next"
WITH_TICKET: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["ticket", "findings"],
    "implementation_label": IMPL,
}
FINDINGS_ONLY: dict[str, Any] = {
    "schema_version": 1,
    "contract": "tracker_result",
    "results": ["findings"],
}
SOURCE_TITLE = "Export drops a row"
SOURCE_BODY = "Reported symptom."
REVISION = "3f2a9c0d4b5e6f708192a3b4c5d6e7f8091a2b3c"
APP_BOT = {"id": 1001, "login": "curie-fixture[bot]", "type": "Bot"}
OTHER_APP_ID = 7707
MANIFEST = '{"name": "triage-plugin", "version": "0.1.0"}'
SKILL = "---\nname: triage\ndescription: investigates one issue\n---\n\n# triage\n"

_EVENT_IDS = itertools.count(960000)


# GitHub ---------------------------------------------------------------------


@dataclass
class _Output:
    """An issue the App created: the delegated output, or an unrelated one."""

    number: int
    title: str
    body: str
    labels: set[str]
    state: str = "open"
    events: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TriageGitHub(NoticeGitHub):
    """`NoticeGitHub` plus App-created issues with labels, their label timeline,
    a body edit, a close, and injected failures.

    https://docs.github.com/en/rest/issues/issues#get-an-issue
    https://docs.github.com/en/rest/issues/issues#list-repository-issues
    https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
    https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
    https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
    https://docs.github.com/en/rest/issues/events#list-issue-events
    https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
    """

    outputs: dict[int, _Output] = field(default_factory=dict)
    permission_unreadable: bool = False
    label_post_status: int | None = None
    label_delete_status: int | None = None
    issue_read_failures: set[int] = field(default_factory=set)
    # Who GitHub says added a label through the label route, and when.
    label_app_id: int = int(APP_ID)
    # Bumped when the repository's issues changed in a way the listing shows
    # (GitHub's ``updated_at``), so a poll's conditional read is not a 304.
    listing_generation: int = 0
    label_clock_offset: timedelta = timedelta(0)
    on_label_added: Callable[[int, str], None] | None = None

    def own_issue(self, number: int, title: str, body: str, labels: set[str]) -> _Output:
        output = _Output(number, title, body, set(labels))
        self.outputs[number] = output
        return output

    def app_label(self, number: int, name: str, *, app_id: int | None = None) -> None:
        """A label event the installation's identity (or another App's) performs."""

        output = self.outputs[number]
        output.labels.add(name)
        output.events.append(
            {
                "id": next(_EVENT_IDS),
                "event": "labeled",
                "label": {"name": name},
                "actor": APP_BOT,
                "performed_via_github_app": {"id": app_id or self.label_app_id},
                "created_at": _iso(datetime.now(UTC) + self.label_clock_offset),
            }
        )

    def _unlabel(self, number: int, name: str) -> None:
        output = self.outputs[number]
        output.labels.discard(name)
        output.events.append(
            {
                "id": next(_EVENT_IDS),
                "event": "unlabeled",
                "label": {"name": name},
                "actor": APP_BOT,
                "performed_via_github_app": {"id": self.label_app_id},
                "created_at": _iso(datetime.now(UTC)),
            }
        )

    def _issue_json(self, output: _Output) -> dict[str, Any]:
        return {
            "number": output.number,
            "title": output.title,
            "body": output.body,
            "state": output.state,
            "labels": [{"name": name} for name in sorted(output.labels)],
            "user": APP_BOT,
            "performed_via_github_app": {"id": int(APP_ID)},
        }

    def _listing(self, request: httpx.Request) -> httpx.Response:
        wanted = request.url.params.get("labels")
        listed = [
            self._issue_body(issue)
            for issue in self.issues.values()
            if issue.state == "open" and issue.labeled and wanted in (None, issue.label)
        ] + [
            self._issue_json(output)
            for output in self.outputs.values()
            if output.state == "open" and wanted in (None, *output.labels)
        ]
        per_page = int(request.url.params.get("per_page", "30"))
        page = int(request.url.params.get("page", "1"))
        state = json.dumps([self.listing_generation, listed], sort_keys=True)
        etag = '"' + hashlib.sha256(state.encode()).hexdigest() + '"'
        if request.headers.get("if-none-match") == etag:
            return httpx.Response(304, headers={"ETag": etag})
        start = (page - 1) * per_page
        return httpx.Response(200, json=listed[start : start + per_page], headers={"ETag": etag})

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        root = f"/repos/{REPO}"
        if path == f"{root}/collaborators/{SENDER}/permission" and self.permission_unreadable:
            self.calls.append(request)
            return httpx.Response(502, json={"message": "Permission unavailable"})
        if path == f"{root}/issues" and request.method == "GET":
            self.calls.append(request)
            return self._listing(request)
        tail = (
            path.removeprefix(f"{root}/issues/").split("/")
            if path.startswith(f"{root}/issues/")
            else []
        )
        if tail and tail[0].isdigit() and int(tail[0]) in self.outputs:
            self.calls.append(request)
            return self._output_route(request, self.outputs[int(tail[0])], tail[1:])
        return super().handle(request)

    def _output_route(
        self, request: httpx.Request, output: _Output, rest: list[str]
    ) -> httpx.Response:
        number = output.number
        if not rest:
            if number in self.issue_read_failures:
                return httpx.Response(503, json={"message": "Issue unavailable"})
            return httpx.Response(200, json=self._issue_json(output))
        if rest == ["events"]:
            return httpx.Response(200, json=list(output.events))
        if rest == ["comments"]:
            return super().handle(request)
        if rest[0] != "labels":
            return httpx.Response(404, json={"message": "missing fixture"})
        if len(rest) == 1 and request.method == "GET":
            return httpx.Response(200, json=[{"name": name} for name in sorted(output.labels)])
        if len(rest) == 1 and request.method == "POST":
            if self.label_post_status is not None:
                return httpx.Response(self.label_post_status, json={"message": "injected"})
            for name in json.loads(request.content)["labels"]:
                self.app_label(number, name)
                if self.on_label_added is not None:
                    self.on_label_added(number, name)
            return httpx.Response(200, json=[{"name": name} for name in sorted(output.labels)])
        if len(rest) == 2 and request.method == "DELETE":
            if self.label_delete_status is not None:
                return httpx.Response(self.label_delete_status, json={"message": "injected"})
            name = httpx.URL(f"https://x/{rest[1]}").path.lstrip("/")
            if name not in output.labels:
                return httpx.Response(404, json={"message": "Label does not exist"})
            self._unlabel(number, name)
            return httpx.Response(200, json=[{"name": name} for name in sorted(output.labels)])
        return httpx.Response(404, json={"message": "missing fixture"})

    def label_writes(self, number: int) -> list[httpx.Request]:
        return [
            call
            for call in self.calls
            if call.method in {"POST", "DELETE"}
            and call.url.path.startswith(f"/repos/{REPO}/issues/{number}/labels")
        ]


# Environment ----------------------------------------------------------------


_KEY: list[str] = []


def _app_key() -> str:
    if not _KEY:
        _KEY.append(
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
    return _KEY[0]


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _bundle(declaration: dict[str, Any] | None) -> bytes:
    files = {".claude-plugin/plugin.json": MANIFEST, "skills/triage/SKILL.md": SKILL}
    if declaration is not None:
        files["workflow/result.json"] = json.dumps(declaration)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for rel, content in files.items():
            data = content.encode()
            info = tarfile.TarInfo(f"triage-plugin/{rel}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _digest(declaration: dict[str, Any]) -> str:
    canonical = json.dumps(declaration, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass
class Env:
    client: TestClient
    github: TriageGitHub
    impl_agent: str
    triage_agent: str
    memory: InMemoryTracker | None = None

    def agent(self, channel: dict[str, Any]) -> str:
        created = self.client.post(
            "/agents",
            headers=_headers(),
            json={
                "name": f"acme-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": channel,
            },
        )
        assert created.status_code == 201, created.text
        return str(created.json()["id"])

    def deploy(self, agent_id: str, declaration: dict[str, Any] | None) -> None:
        patched = self.client.patch(
            f"/agents/{agent_id}", json={"publication_policy": "auto"}, headers=_headers()
        )
        assert patched.status_code == 200, patched.text
        version = self.client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": f"v-{uuid.uuid4().hex[:6]}", "created_by": "triage-test"},
            headers=_headers(),
        )
        assert version.status_code == 201, version.text
        version_id = str(version.json()["id"])
        uploaded = self.client.put(
            f"/agents/{agent_id}/versions/{version_id}/bundle",
            files={"file": ("triage.tar.gz", _bundle(declaration))},
            headers=_headers(),
        )
        assert uploaded.status_code == 201, uploaded.text
        deployed = self.client.post(
            "/deployments",
            json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
            headers=_headers(),
        )
        assert deployed.status_code == 201, deployed.text


@contextmanager
def triage_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Env]:
    """The install the handoff runs on: an implementation agent on the tracker's
    label and a triage agent whose in-force version allows a ticket.

    Usable from another module's test, with its own ``clean_db`` and ``valkey``.
    """

    key = _app_key()
    bindings = [github_repository_binding(bases=["main", "next"], default_base="main")]
    for name, value in _factory_env(
        tmp_path, intake="webhook", private_key=key, bindings=bindings
    ).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", key)
    get_settings.cache_clear()
    github = TriageGitHub()
    monkeypatch.setattr(httpx, "Client", _sync_client_type(github))
    monkeypatch.setattr(httpx, "AsyncClient", _client_type(github))
    try:
        with TestClient(create_app()) as client:
            env = Env(client, github, "", "")
            env.impl_agent = env.agent({"kind": "github", "address": REPO})
            env.triage_agent = env.agent(
                {"kind": "github", "address": REPO, "trigger_label": TRIAGE}
            )
            env.deploy(env.triage_agent, WITH_TICKET)
            yield env
    finally:
        get_settings.cache_clear()


@pytest.fixture
def env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None, valkey: object
) -> Iterator[Env]:
    with triage_env(monkeypatch, tmp_path) as built:
        yield built


# Service drivers ------------------------------------------------------------


def _run[T](work: Callable[[async_sessionmaker[AsyncSession]], Awaitable[T]]) -> T:
    """Service code on this thread's own loop and engine; a `BaseException`
    raised inside (a modelled process death) propagates after disposal."""

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            return await work(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    return asyncio.run(go())


def run_hand_off(handoff_id: uuid.UUID, owner: str = "reconciler-a") -> str | None:
    async def work(maker: async_sessionmaker[AsyncSession]) -> str | None:
        async with httpx.AsyncClient() as client:
            return await hand_off(
                maker, handoff_id, settings=get_settings(), client=client, owner=owner
            )

    return _run(work)


def make_due(handoff_id: uuid.UUID) -> None:
    """A lapsed lease and a passed backoff, as a restart after waiting would see."""

    _sql(
        "UPDATE curie.factory_triage_handoffs SET next_attempt_at = clock_timestamp() "
        "- interval '1 second', lease_expires_at = CASE WHEN lease_owner IS NULL THEN NULL "
        "ELSE clock_timestamp() - interval '1 second' END WHERE id = :id",
        {"id": handoff_id},
    )


def _start_running(client: TestClient, request_id: uuid.UUID) -> int:
    async def go() -> int:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            started = await start(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                claim_name=f"claim-{request_id.hex[:12]}",
                sandbox_name=f"sbx-{request_id.hex[:12]}",
                context=acquired.context,
            )
            assert not isinstance(started, DispatchConflict), started
            return int(started.runtime_epoch)

    return int(client.portal.call(go))


def _ticket() -> ResultProposal:
    return parse_proposal(
        {
            "kind": "ticket",
            "title": "Export drops the last row",
            "body": "## Problem\n\nThe export omits the final row.",
            "reproduction": {
                "command": "uv run pytest tests/test_export.py -q",
                "observed": "1 failed: expected 3 rows, got 2",
                "revision": REVISION,
            },
        }
    )


@dataclass
class Triage:
    number: int
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    source: TrackerIssueRef
    delivery_id: uuid.UUID | None = None
    output: int | None = None
    handoff_id: uuid.UUID | None = None


def admit_source(env: Env) -> Triage:
    """A person applies the triage label and ``base:next``; a poll pass admits it."""

    number = env.github.plant_issue(label=TRIAGE, base_labels=(BASE,))
    _pass(env.github, "poll")
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id WHERE w.tracker_issue_id = :n",
        {"n": str(number)},
    )
    if env.memory is None:
        env.memory = InMemoryTracker(
            kind=row["tracker_kind"], host=row["tracker_host"], scope_id=row["tracker_scope_id"]
        )
    source = env.memory.seed_issue(
        SOURCE_TITLE, SOURCE_BODY, issue_id=str(number), labels={TRIAGE, BASE}
    )
    return Triage(number, row["work_item_id"], row["request_id"], source)


def _grant(request_id: uuid.UUID) -> uuid.UUID:
    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            request = await session.get(ExecutionRequest, request_id)
            assert request is not None
            item = await session.get(WorkItem, request.work_item_id)
            assert item is not None
            grant = FactoryTrackerResultGrant(
                id=uuid.uuid4(),
                work_item_id=item.id,
                execution_request_id=request.id,
                source_fingerprint=source_fingerprint(item, request),
                repository_id=item.repository_project_id,
                allowed_result_kinds=["findings", "ticket"],
                output_identity=output_identity_for(request.id),
                implementation_label=IMPL,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                source_content_sha256=source_content_sha256(
                    SourceContent(SOURCE_TITLE, SOURCE_BODY)
                ),
            )
            session.add(grant)
            await session.commit()
            return grant.id

    return _run(work)


def run_deliver(env: Env, delivery_id: uuid.UUID) -> Any:
    assert env.memory is not None
    memory = env.memory

    async def work(maker: async_sessionmaker[AsyncSession]) -> Any:
        return await deliver(
            maker, delivery_id, tracker=memory, settings=get_settings(), owner="deliverer"
        )

    return _run(work)


def publish(env: Env, triage: Triage) -> Triage:
    """The triage run submits a ticket and the real delivery publishes it; the
    created issue is mirrored into GitHub, where the handoff reads it."""

    assert env.memory is not None
    memory = env.memory
    epoch = _start_running(env.client, triage.request_id)
    # The one model session the runner's contract call reserves (#5586).
    _sql(
        "UPDATE curie.execution_requests SET model_session_reserved_at = clock_timestamp() "
        "WHERE id = :id",
        {"id": triage.request_id},
    )
    grant_id = _grant(triage.request_id)

    async def submit(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            return await submit_result(
                session,
                grant_id=grant_id,
                execution_request_id=triage.request_id,
                runtime_epoch=epoch,
                proposal=_ticket(),
                tracker=memory,
                settings=get_settings(),
            )

    triage.delivery_id = _run(submit)
    outcome = run_deliver(env, triage.delivery_id)
    assert outcome.state == "delivered", outcome
    (receipt,) = _rows(
        "SELECT output_issue_id, output_body_sha256 FROM curie.factory_tracker_result_receipts "
        "WHERE delivery_id = :id",
        {"id": triage.delivery_id},
    )
    (created,) = [
        ref
        for ref in memory.issues(author=memory.identity)
        if ref.issue_id == receipt["output_issue_id"]
    ]
    body = memory.issue_body(created)
    assert hashlib.sha256(body.encode()).hexdigest() == receipt["output_body_sha256"]
    triage.output = int(created.issue_id)
    env.github.own_issue(
        triage.output, "Export drops the last row", body, set(memory.labels(created))
    )
    handoffs = _rows(
        "SELECT id FROM curie.factory_triage_handoffs WHERE source_execution_request_id = :r",
        {"r": triage.request_id},
    )
    triage.handoff_id = handoffs[0]["id"] if handoffs else None
    return triage


def published(env: Env) -> Triage:
    return publish(env, admit_source(env))


# Reads ----------------------------------------------------------------------


def delegations(request_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
    if request_id is None:
        return _rows("SELECT * FROM curie.factory_triage_delegations")
    return _rows(
        "SELECT * FROM curie.factory_triage_delegations WHERE source_execution_request_id = :r",
        {"r": request_id},
    )


def handoff(handoff_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows("SELECT * FROM curie.factory_triage_handoffs WHERE id = :id", {"id": handoff_id})
    return row


def work_items(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT * FROM curie.work_items WHERE tracker_scope_id = :s AND tracker_issue_id = :n",
        {"s": str(REPO_ID), "n": str(number)},
    )


def requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.* FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.tracker_scope_id = :s AND w.tracker_issue_id = :n "
        "ORDER BY r.sequence",
        {"s": str(REPO_ID), "n": str(number)},
    )


def binding(agent_id: str) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT * FROM curie.agent_channels WHERE agent_id = :a AND kind = 'github'",
        {"a": uuid.UUID(agent_id)},
    )
    return row


def recorded(triage: Triage) -> Any:
    """The handoff outcome the source's status comment and Slack line render."""

    async def work(maker: async_sessionmaker[AsyncSession]) -> Any:
        async with maker() as session:
            return await recorded_handoff(session, triage.request_id)

    return _run(work)


def _source_snapshot(triage: Triage) -> tuple[Any, ...]:
    return (work_items(triage.number), requests(triage.number))


def _assert_one_triage_session(triage: Triage) -> None:
    (request,) = requests(triage.number)
    assert request["id"] == triage.request_id
    assert request["completion_contract"] == "tracker_result"
    assert request["model_session_reserved_at"] is not None


# AC1: delegation ------------------------------------------------------------


def test_delegation_is_recorded_once_with_the_frozen_target(env: Env) -> None:
    triage = admit_source(env)
    (row,) = delegations()
    (request,) = requests(triage.number)
    (item,) = work_items(triage.number)
    target = binding(env.impl_agent)
    event_id = env.github.issues[triage.number].event_id
    assert row["source_execution_request_id"] == triage.request_id
    assert row["work_item_id"] == triage.work_item_id
    assert (row["actor_id"], row["actor_login"]) == (str(SENDER_ID), SENDER)
    assert row["selection_event"] == f"github-label-event:{event_id}"
    assert (row["tracker_kind"], row["tracker_scope_id"], row["tracker_issue_id"]) == (
        "github",
        str(REPO_ID),
        str(triage.number),
    )
    assert row["tracker_host"] == item["tracker_host"]
    assert row["repository_id"] == str(REPO_ID)
    assert row["installation_id"] == INSTALLATION_ID
    assert row["result_policy_digest"] == _digest(WITH_TICKET) == request["result_policy_digest"]
    assert row["source_fingerprint"] is not None
    assert row["target_label"] == IMPL
    assert (
        row["target_binding_id"],
        row["target_binding_generation"],
        row["target_agent_id"],
    ) == (target["id"], target["generation"], uuid.UUID(env.impl_agent))
    assert row["consumed_at"] is None

    # A replayed observation of the same label stores no second one.
    env.github._etags["events"] = '"replayed"'
    _pass(env.github, "poll")
    assert len(delegations()) == 1

    # Control: a human label on the implementation binding admits a run, but
    # an implementation request never obtains a delegation.
    number = env.github.plant_issue(label=IMPL)
    _pass(env.github, "poll")
    (implementation,) = requests(number)
    assert implementation["completion_contract"] == "implementation"
    assert delegations(implementation["id"]) == []
    assert len(delegations()) == 1


def test_delegation_is_not_stored_for_a_findings_only_declaration(env: Env) -> None:
    env.deploy(env.triage_agent, FINDINGS_ONLY)
    triage = admit_source(env)
    (request,) = requests(triage.number)
    assert request["completion_contract"] == "tracker_result"
    assert request["result_policy_digest"] == _digest(FINDINGS_ONLY)
    assert delegations() == []
    # Control: the same agent with a ticket policy stores one.
    env.deploy(env.triage_agent, WITH_TICKET)
    assert len(delegations(admit_source(env).request_id)) == 1


def _unresolvable(env: Env, case: str) -> None:
    if case == "no_binding":
        _sql(
            "DELETE FROM curie.agent_channels WHERE agent_id = :a", {"a": uuid.UUID(env.impl_agent)}
        )
    elif case == "two_bindings":
        other = env.agent({"kind": "github", "address": REPO, "trigger_label": "factory-other"})
        _sql(
            "UPDATE curie.agent_channels SET trigger_label = :label WHERE agent_id = :a",
            {"a": uuid.UUID(other), "label": IMPL},
        )
    else:
        env.deploy(env.impl_agent, FINDINGS_ONLY)


@pytest.mark.parametrize("case", ["no_binding", "two_bindings", "tracker_result_target"])
def test_delegation_without_a_resolvable_target_is_stored_with_a_null_target(
    env: Env, case: str
) -> None:
    _unresolvable(env, case)
    triage = admit_source(env)
    # Triage itself is not refused.
    assert len(work_items(triage.number)) == 1
    (row,) = delegations()
    assert row["target_label"] == IMPL
    assert row["target_binding_id"] is None
    assert row["target_binding_generation"] is None
    assert row["target_agent_id"] is None
    # Its handoff is refused target_unavailable once the ticket is published.
    publish(env, triage)
    assert triage.handoff_id is not None and triage.output is not None
    assert run_hand_off(triage.handoff_id) == "refused"
    assert handoff(triage.handoff_id)["reason"] == "target_unavailable"
    assert work_items(triage.output) == []
    assert IMPL not in env.github.outputs[triage.output].labels


def test_delegation_absent_for_a_source_admitted_before_the_change_opens_no_handoff(
    env: Env,
) -> None:
    triage = admit_source(env)
    # A source admitted before #5588 has no delegation.
    _sql("DELETE FROM curie.factory_triage_delegations")
    publish(env, triage)
    assert triage.handoff_id is None
    assert _rows("SELECT id FROM curie.factory_triage_handoffs") == []
    # Control: the same publication with its delegation opens one.
    assert published(env).handoff_id is not None


def test_delegation_of_a_pending_readmit_is_finalized_by_the_receipt(env: Env) -> None:
    """A delegation stored before its request existed has no fingerprint and the
    admission-time digest; the receipt transaction completes it from the request."""

    triage = admit_source(env)
    (stored,) = delegations(triage.request_id)
    (request,) = requests(triage.number)
    _sql(
        "UPDATE curie.factory_triage_delegations SET source_fingerprint = NULL, "
        "result_policy_digest = :stale WHERE id = :id",
        {"id": stored["id"], "stale": "ee" * 32},
    )
    assert delegations(triage.request_id)[0]["source_fingerprint"] is None

    publish(env, triage)

    (finalized,) = delegations(triage.request_id)
    # Computed from the request as it stands at the receipt (its base pinned),
    # the same facts the grant was minted against.
    (grant,) = _rows(
        "SELECT source_fingerprint FROM curie.factory_tracker_result_grants "
        "WHERE execution_request_id = :r",
        {"r": triage.request_id},
    )
    assert finalized["source_fingerprint"] == grant["source_fingerprint"]
    assert finalized["result_policy_digest"] == request["result_policy_digest"]
    assert triage.handoff_id is not None


# AC2: an admitted handoff ---------------------------------------------------


def _assert_admitted(env: Env, triage: Triage) -> dict[str, Any]:
    assert triage.handoff_id is not None and triage.output is not None
    row = handoff(triage.handoff_id)
    expected = implementation_request_id(triage.handoff_id)
    assert (row["outcome"], row["reason"]) == ("admitted", None)
    assert row["implementation_request_id"] == expected
    (item,) = work_items(triage.output)
    (request,) = requests(triage.output)
    (source,) = work_items(triage.number)
    assert item["agent_id"] == uuid.UUID(env.impl_agent)
    assert item["id"] != triage.work_item_id
    assert item["conversation_id"] != source["conversation_id"]
    assert item["code_host_installation_id"] == INSTALLATION_ID
    assert request["id"] == expected
    assert request["requester"] == f"github:{SENDER_ID}:{SENDER}"
    assert request["completion_contract"] == "implementation"
    assert request["result_policy_digest"] is None
    # Fresh bounds: its own request, not yet started, no model session spent.
    assert request["sequence"] == 1
    assert request["started_at"] is None and request["execution_attempts"] == 0
    assert request["model_session_reserved_at"] is None
    assert env.github.outputs[triage.output].labels == {IMPL, BASE}
    assert delegations(triage.request_id)[0]["consumed_at"] is not None
    ending = recorded(triage)
    assert (ending.admitted, ending.refusal_reason) == (True, None)
    assert ending.implementation_status_url == row["output_url"]
    return request


def test_admitted_handoff_starts_one_implementation_run_on_the_output(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id == selection_event_id(
        triage.request_id,
        TrackerIssueRef("github", triage.source.host, str(REPO_ID), str(triage.output)),
    )
    before = _source_snapshot(triage)
    # The handoff keeps the delivery's own deadline.
    (delivery,) = _rows(
        "SELECT deadline_at FROM curie.factory_tracker_result_deliveries WHERE id = :d",
        {"d": triage.delivery_id},
    )
    assert handoff(triage.handoff_id)["deadline_at"] == delivery["deadline_at"]
    # Control: the receipt alone admits nothing on the output.
    assert triage.output is not None and work_items(triage.output) == []
    assert handoff(triage.handoff_id)["outcome"] is None

    assert run_hand_off(triage.handoff_id) == "admitted"

    _assert_admitted(env, triage)
    assert TRIAGE not in env.github.outputs[triage.output].labels
    assert _source_snapshot(triage) == before
    # The internal path wrote no intake notice anywhere.
    assert _rows("SELECT id FROM curie.factory_intake_notices") == []


def test_admitted_after_a_failed_label_write_retries_into_exactly_one(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None
    env.github.label_post_status = 502

    assert run_hand_off(triage.handoff_id) is None

    row = handoff(triage.handoff_id)
    assert row["associated_at"] is not None
    assert row["labelled_at"] is None and row["outcome"] is None
    assert row["last_error"] == "label_write_unavailable"
    assert work_items(triage.output) == []
    assert IMPL not in env.github.outputs[triage.output].labels
    # Not due until its backoff passes.
    assert run_hand_off(triage.handoff_id) is None
    assert work_items(triage.output) == []

    env.github.label_post_status = None
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) == "admitted"
    _assert_admitted(env, triage)


# AC3: replay ----------------------------------------------------------------


class _Death(BaseException):
    """The process dies here; nothing below it runs."""


def _die_once(
    monkeypatch: pytest.MonkeyPatch, target: str, name: str, *, after: bool
) -> list[bool]:
    import importlib

    module = importlib.import_module(target)
    real = getattr(module, name)
    died: list[bool] = []

    async def patched(*args: Any, **kwargs: Any) -> Any:
        if not died and not after:
            died.append(True)
            raise _Death(name)
        result = await real(*args, **kwargs)
        if not died:
            died.append(True)
            raise _Death(name)
        return result

    monkeypatch.setattr(module, name, patched)
    return died


DEATHS = {
    # 6.1 committed, before the label write.
    "after_association": ("curie_api.factory_triage_handoff", "_add_label", False),
    # The label is on the output, ``labelled_at`` not yet committed.
    "after_label": ("curie_api.factory_triage_handoff", "_add_label", True),
    # ``labelled_at`` committed, before admission.
    "after_label_commit": ("curie_api.github_factory", "admit_delegated", False),
    # Admitted inside the transaction, before its commit.
    "after_admission_before_commit": ("curie_api.github_factory", "admit_delegated", True),
}


@pytest.mark.parametrize("point", list(DEATHS))
def test_replay_after_a_process_death_admits_the_same_request_once(
    env: Env, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None
    module, name, after = DEATHS[point]
    died = _die_once(monkeypatch, module, name, after=after)

    with pytest.raises(_Death):
        run_hand_off(triage.handoff_id)

    assert died == [True]
    assert handoff(triage.handoff_id)["outcome"] is None
    assert work_items(triage.output) == []
    # The lease the dead process held keeps another attempt out until it lapses.
    assert run_hand_off(triage.handoff_id, owner="reconciler-b") is None
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id, owner="reconciler-b") == "admitted"
    _assert_admitted(env, triage)
    assert env.memory is not None
    assert len(env.memory.issues(author=env.memory.identity)) == 1


def reopen(
    triage: Triage, *, delivery_id: uuid.UUID | None = None, deleted: bool = False
) -> uuid.UUID | None:
    """The receipt transaction's internal selection event, written again for
    ``triage``'s output (``deleted``: as if its first write had not happened)."""

    assert triage.output is not None and triage.delivery_id is not None
    (receipt,) = _rows(
        "SELECT output_tracker_kind, output_tracker_host, output_scope_id, output_issue_id, "
        "output_url, output_body_sha256 FROM curie.factory_tracker_result_receipts "
        "WHERE delivery_id = :d",
        {"d": triage.delivery_id},
    )
    output = TrackerIssueRef(
        receipt["output_tracker_kind"],
        receipt["output_tracker_host"],
        receipt["output_scope_id"],
        receipt["output_issue_id"],
    )
    if deleted:
        _sql(
            "DELETE FROM curie.factory_triage_handoffs WHERE source_execution_request_id = :r",
            {"r": triage.request_id},
        )

    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID | None:
        async with maker() as session:
            found = await open_handoff(
                session,
                delivery_id=delivery_id or triage.delivery_id,
                execution_request_id=triage.request_id,
                output=output,
                output_url=receipt["output_url"],
                output_body_sha256=receipt["output_body_sha256"],
            )
            await session.commit()
            return found

    return _run(work)


def test_replay_of_the_selection_event_receipt_and_handoff_admits_once(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None and env.memory is not None

    # A duplicated internal event before consumption is the same event.
    assert reopen(triage) == triage.handoff_id
    assert len(_rows("SELECT id FROM curie.factory_triage_handoffs")) == 1
    # A receipt naming no delivery row writes no event.
    assert reopen(triage, delivery_id=uuid.uuid4()) is None
    assert len(_rows("SELECT id FROM curie.factory_triage_handoffs")) == 1
    # A redelivered receipt publishes nothing again.
    assert triage.delivery_id is not None
    assert run_deliver(env, triage.delivery_id).state == "delivered"
    assert len(env.memory.issues(author=env.memory.identity)) == 1

    assert run_hand_off(triage.handoff_id) == "admitted"
    # A duplicated handoff, a later reconciler and a replayed event change nothing.
    assert run_hand_off(triage.handoff_id, owner="reconciler-b") is None
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id, owner="reconciler-c") is None
    assert reopen(triage) is None
    assert run_deliver(env, triage.delivery_id).state == "delivered"
    _assert_admitted(env, triage)
    assert len(_rows("SELECT id FROM curie.factory_triage_handoffs")) == 1
    assert len(env.memory.issues(author=env.memory.identity)) == 1
    assert len([c for c in env.github.label_writes(triage.output) if c.method == "POST"]) == 1


def test_replay_while_another_attempt_holds_the_lease_does_nothing(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None
    _sql(
        "UPDATE curie.factory_triage_handoffs SET lease_owner = 'reconciler-z', "
        "lease_expires_at = clock_timestamp() + interval '1 minute' WHERE id = :id",
        {"id": triage.handoff_id},
    )
    assert run_hand_off(triage.handoff_id) is None
    assert env.github.label_writes(triage.output) == []
    assert work_items(triage.output) == []
    # Control: once the lease lapses, the same attempt admits.
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) == "admitted"


# AC4: refusals --------------------------------------------------------------


def _replace_binding(agent_id: str) -> None:
    """The binding is deleted and created again for the same agent and label."""

    columns = [
        row["column_name"]
        for row in _rows(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'agent_channels' ORDER BY ordinal_position"
        )
    ]
    rest = ", ".join(name for name in columns if name != "id")
    params = {"a": uuid.UUID(agent_id)}

    async def work(maker: async_sessionmaker[AsyncSession]) -> None:
        async with maker() as session:
            await session.execute(
                text(
                    "CREATE TEMP TABLE replaced ON COMMIT DROP AS SELECT * FROM "
                    "curie.agent_channels WHERE agent_id = :a AND kind = 'github'"
                ),
                params,
            )
            await session.execute(
                text("DELETE FROM curie.agent_channels WHERE agent_id = :a AND kind = 'github'"),
                params,
            )
            await session.execute(
                text(
                    f"INSERT INTO curie.agent_channels (id, {rest}) "
                    f"SELECT gen_random_uuid(), {rest} FROM replaced"
                )
            )
            await session.commit()

    _run(work)


def _refuse_setup(env: Env, triage: Triage, case: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """Change the one fact ``case`` names; the reason it is refused with."""

    assert triage.output is not None
    output = env.github.outputs[triage.output]
    impl = uuid.UUID(env.impl_agent)
    if case == "authority_revoked":
        env.github.permission = "read"
    elif case == "source_cancelled":
        env.github.unlabel_issue(triage.number)
        _pass(env.github, "poll")
        (item,) = work_items(triage.number)
        assert item["cancelled_at"] is not None
    elif case == "output_edited":
        output.body += "\n\nEdited after delivery."
        return "output_changed"
    elif case == "output_closed":
        output.state = "closed"
        return "output_changed"
    elif case == "binding_removed":
        _sql("DELETE FROM curie.agent_channels WHERE agent_id = :a", {"a": impl})
        return "target_unavailable"
    elif case == "binding_regenerated":
        _sql(
            "UPDATE curie.agent_channels SET generation = generation + 1 WHERE agent_id = :a",
            {"a": impl},
        )
        return "target_unavailable"
    elif case == "binding_replaced":
        _replace_binding(env.impl_agent)
        return "target_unavailable"
    elif case == "target_redeployed":
        env.deploy(env.impl_agent, FINDINGS_ONLY)
        return "target_unavailable"
    elif case == "label_conflict":
        output.labels.add(TRIAGE)
    elif case == "scope_lost":
        monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["other-corp/*"]')
        get_settings.cache_clear()
    return case


REFUSALS = [
    "authority_revoked",
    "source_cancelled",
    "output_edited",
    "output_closed",
    "binding_removed",
    "binding_regenerated",
    "binding_replaced",
    "target_redeployed",
    "label_conflict",
    "scope_lost",
]


def _assert_refused(env: Env, triage: Triage, reason: str) -> None:
    assert triage.handoff_id is not None and triage.output is not None and env.memory is not None
    row = handoff(triage.handoff_id)
    assert (row["outcome"], row["reason"]) == ("refused", reason)
    assert row["implementation_request_id"] is None
    assert delegations(triage.request_id)[0]["consumed_at"] is not None
    ending = recorded(triage)
    assert (ending.admitted, ending.refusal_reason, ending.implementation_status_url) == (
        False,
        reason,
        None,
    )
    # The ticket stays published and linked from the source.
    (receipt,) = _rows(
        "SELECT output_issue_id FROM curie.factory_tracker_result_receipts WHERE delivery_id = :d",
        {"d": triage.delivery_id},
    )
    assert receipt["output_issue_id"] == str(triage.output)
    assert len(env.memory.marked_comments.comments(ReplyTarget.on_issue(triage.source))) == 1
    assert len(env.memory.issues(author=env.memory.identity)) == 1
    assert triage.output in env.github.outputs
    assert IMPL not in env.github.outputs[triage.output].labels
    assert work_items(triage.output) == []
    _assert_one_triage_session(triage)
    # A refusal is final: another attempt changes nothing.
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id, owner="reconciler-b") is None
    assert work_items(triage.output) == []


@pytest.mark.parametrize("case", REFUSALS)
def test_refused_handoff_keeps_the_ticket_and_starts_nothing(
    env: Env, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    triage = published(env)
    assert triage.handoff_id is not None
    reason = _refuse_setup(env, triage, case, monkeypatch)

    assert run_hand_off(triage.handoff_id) == "refused"

    _assert_refused(env, triage, reason)


def test_refused_permission_unreadable_only_at_the_deadline(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None
    env.github.permission_unreadable = True

    # A failed read is never approval: it is retried, and nothing is written.
    assert run_hand_off(triage.handoff_id) is None
    row = handoff(triage.handoff_id)
    assert row["outcome"] is None and row["last_error"] == "permission_unreadable"
    assert row["associated_at"] is None
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) is None
    assert handoff(triage.handoff_id)["outcome"] is None
    assert env.github.label_writes(triage.output) == []

    _sql(
        "UPDATE curie.factory_triage_handoffs SET deadline_at = clock_timestamp() "
        "- interval '1 second' WHERE id = :id",
        {"id": triage.handoff_id},
    )
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) == "refused"
    _assert_refused(env, triage, "permission_unreadable")


def test_refused_permission_unreadable_on_the_first_attempt_after_a_late_receipt(
    env: Env,
) -> None:
    """The receipt of a delivery that settled at its deadline: the handoff keeps
    that deadline, so a failing permission read refuses at once. The receipt
    transaction's event is written again here for a delivery whose deadline has
    passed, as a receipt settled at that moment writes it."""

    triage = published(env)
    assert triage.output is not None
    _sql(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET submitted_at = clock_timestamp() - interval '2 seconds', "
        "deadline_at = clock_timestamp() - interval '1 second' WHERE id = :d",
        {"d": triage.delivery_id},
    )
    triage.handoff_id = reopen(triage, deleted=True)
    assert triage.handoff_id is not None
    (delivery,) = _rows(
        "SELECT deadline_at FROM curie.factory_tracker_result_deliveries WHERE id = :d",
        {"d": triage.delivery_id},
    )
    assert handoff(triage.handoff_id)["deadline_at"] == delivery["deadline_at"]
    env.github.permission_unreadable = True

    assert run_hand_off(triage.handoff_id) == "refused"

    assert handoff(triage.handoff_id)["attempts"] == 1
    assert handoff(triage.handoff_id)["associated_at"] is None
    assert env.github.label_writes(triage.output) == []
    _assert_refused(env, triage, "permission_unreadable")


def test_refused_with_its_own_reason_when_the_label_cannot_be_removed_by_the_deadline(
    env: Env,
) -> None:
    """Authority is revoked after the label was added and its removal fails: the
    handoff defers while the removal is owed, and past the deadline refuses
    with the original reason. The label left behind starts nothing."""

    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None

    def revoke(number: int, name: str) -> None:
        env.github.permission = "read"

    env.github.on_label_added = revoke
    env.github.label_delete_status = 502
    assert run_hand_off(triage.handoff_id) is None
    row = handoff(triage.handoff_id)
    assert (row["outcome"], row["last_error"]) == (None, "label_removal_unavailable")
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) is None
    assert handoff(triage.handoff_id)["outcome"] is None

    _sql(
        "UPDATE curie.factory_triage_handoffs SET deadline_at = clock_timestamp() "
        "- interval '1 second' WHERE id = :id",
        {"id": triage.handoff_id},
    )
    make_due(triage.handoff_id)
    assert run_hand_off(triage.handoff_id) == "refused"

    row = handoff(triage.handoff_id)
    assert (row["outcome"], row["reason"]) == ("refused", "authority_revoked")
    assert IMPL in env.github.outputs[triage.output].labels
    assert work_items(triage.output) == []
    _assert_one_triage_session(triage)
    assert recorded(triage).refusal_reason == "authority_revoked"


def test_refused_after_the_label_was_added_takes_the_label_off_again(env: Env) -> None:
    triage = published(env)
    assert triage.handoff_id is not None and triage.output is not None

    def revoke(number: int, name: str) -> None:
        env.github.permission = "read"

    # Authority is revoked between the label write and admission.
    env.github.on_label_added = revoke
    assert run_hand_off(triage.handoff_id) == "refused"

    assert handoff(triage.handoff_id)["labelled_at"] is not None
    assert [c.method for c in env.github.label_writes(triage.output)] == ["POST", "DELETE"]
    _assert_refused(env, triage, "authority_revoked")


# AC11: migration ------------------------------------------------------------


def test_triage_handoff_migration_upgrades_from_0168_and_downgrades_when_empty(
    isolated_migration_db: Any,
) -> None:
    from _migration_support import alembic_config
    from alembic import command

    def tables() -> list[Any]:
        return [
            row["name"]
            for row in _rows(
                "SELECT to_regclass('curie.factory_triage_delegations') AS name UNION ALL "
                "SELECT to_regclass('curie.factory_triage_handoffs')"
            )
        ]

    isolated_migration_db.at("0168")
    assert tables() == [None, None]
    config = alembic_config()
    command.upgrade(config, "0171")
    assert tables() == ["curie.factory_triage_delegations", "curie.factory_triage_handoffs"]
    unique = _rows(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'factory_triage_handoffs'"
    )
    defs = " ".join(row["indexdef"] for row in unique)
    assert "(output_tracker_host, output_scope_id, output_issue_id)" in defs
    assert "(source_execution_request_id)" in defs
    assert "WHERE (outcome IS NULL)" in defs
    command.downgrade(config, "0168")
    assert tables() == [None, None]
    command.upgrade(config, "0171")
    assert tables() == ["curie.factory_triage_delegations", "curie.factory_triage_handoffs"]
