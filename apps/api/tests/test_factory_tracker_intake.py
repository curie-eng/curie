"""The port-driven intake pass for non-GitHub trackers (#4358 plan D1, D2, D14, D15).

`curie_api.factory_tracker_intake.poll_trackers` polls every configured
non-GitHub tracker binding through `Tracker.poll_marked`, applies each notice
through start authority, the surface for its label, the label refusal rules,
repository choice and the shared WorkItem dispatch, and keeps one
opaque cursor per (tracker scope, label) in ``factory_poll_cursors.etags``
under ``marked:<label>``. A sweep re-reads every currently labelled issue every
``factory_reconcile_interval_s``, so a repaired intake refusal retries under
the original label event's request id. Base selection waits until acquisition.

Here the tracker is the real Jira Cloud adapter, built by the production
registry from the install's `ForgesConfig`, over the Jira fake
(`forge_fakes/jira.py`) through ASGI; the code host is the real GitHub adapter
over `GitHubRepositoryFake`, and a second code host is the real Bitbucket Data
Center adapter over `BitbucketDcFake`. The pass gets that one HTTP client, Postgres is
real, and the App's installation mint is `fake_installation_tokens`.
Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.forges import types
from curie_api.forges.errors import Unavailable
from curie_api.forges.identity import notice_request_id
from curie_api.forges.registry import ForgeRegistry
from curie_api.forges.types import Disposition, MarkedNotice, TrackerIssueRef
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes import bitbucket_dc
from forge_fakes import jira as jira_fake
from forge_fakes.github import (
    INSTALLATION_ID,
    REPO,
    REPO_ID,
    GitHubRepositoryFake,
    fake_installation_tokens,
    forges_document,
    forges_env_json,
    github_repository_binding,
)
from forge_fakes.github_comments import _rows
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_factory_poll_intake import _acquire
from test_github_factory_base import _execute

pytestmark = pytest.mark.usefixtures("clean_db")

LABEL = jira_fake.LABEL
FAST = "factory-fast"
BINDING = jira_fake.BINDING_NAME
TRACKER = jira_fake.TRACKER_NAME
STARTER_ID = "5b10a0000000000000000a11"
OUTSIDER_ID = "5b10a0000000000000000a22"
STARTERS_GROUP = "f1e2d3c4-0000-4000-8000-0000000000aa"
SWEEP = 300.0
LABEL_REFUSAL_MARKER = "curie-factory-label-refusal"
HOLD_MARKER = "curie-factory-intake-hold"
REPO_REFUSAL_MARKER = "<!-- curie-factory-repo-refusal -->"
# What the DC outage answers: a body echoing the token, so a leak is visible.
DC_SENTINEL = f"upstream echoed {bitbucket_dc.FACTORY_TOKEN}"
DC_REPO = {
    "alias": "dc",
    "code_host": bitbucket_dc.CODE_HOST_NAME,
    "project_id": str(bitbucket_dc.REPO_ID),
    "path": bitbucket_dc.REPO_PATH,
}


# --- the install ------------------------------------------------------------------


class _GitHub(GitHubRepositoryFake):
    """The GitHub repository fake, also readable by the repository's immutable id."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if request.url.path == f"/repositories/{REPO_ID}":
            request = httpx.Request(
                request.method,
                request.url.copy_with(path=f"/repos/{REPO}"),
                headers=request.headers,
            )
        return super().handle(request)


class _ByHost(httpx.AsyncBaseTransport):
    """The Jira site to the Jira fake, the DC host to the DC fake, every other
    host to the GitHub fake. With ``dc_down`` every DC request answers
    ``dc_status`` (503 unless set)."""

    def __init__(
        self,
        jira: jira_fake.JiraFake,
        github: _GitHub,
        dc: bitbucket_dc.BitbucketDcFake,
        *,
        dc_down: bool,
        dc_status: int = 503,
    ) -> None:
        self._jira = httpx.ASGITransport(app=jira.app)
        self._github = httpx.MockTransport(github.handle)
        self._dc = dc.transport()
        self._dc_down = dc_down
        self._dc_status = dc_status

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == jira_fake.SITE_HOST:
            return await self._jira.handle_async_request(request)
        if request.url.host == bitbucket_dc.HOST:
            if self._dc_down:
                return httpx.Response(
                    self._dc_status, json={"errors": [{"message": DC_SENTINEL}]}, request=request
                )
            return await self._dc.handle_async_request(request)
        return await self._github.handle_async_request(request)


def _jira_binding(
    *,
    start_authority: dict[str, Any] | None = None,
    repos: list[dict[str, Any]] | None = None,
    bases: list[str] | None = None,
) -> dict[str, Any]:
    bot = {
        "alias": "bot",
        "code_host": "github",
        "project_id": str(REPO_ID),
        "path": REPO,
        "bases": list(bases or []),
    }
    return {
        "name": BINDING,
        "tracker": TRACKER,
        "tracker_scope_id": jira_fake.CLOUD_ID,
        "repos": [bot, *(repos or [])],
        "default_repo": "bot",
        "start_authority": start_authority or {"type": "allowlist", "accounts": [STARTER_ID]},
    }


def _document(
    *,
    jira_binding: dict[str, Any] | None = None,
    before: list[dict[str, Any]] | None = None,
    label: str = LABEL,
) -> dict[str, Any]:
    """Today's GitHub entries, any ``before`` bindings, then the Jira site's binding."""

    document = forges_document()
    document["trackers"].append(jira_fake.jira_tracker_entry(label=label))
    document["bindings"] = [*(before or []), jira_binding or _jira_binding()]
    return document


def _dc_document(
    *, poll_interval_s: float | None = None, repos: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """`_document` plus the DC code host and its ``dc`` alias on the Jira binding,
    then any further ``repos``."""

    document = _document(jira_binding=_jira_binding(repos=[dict(DC_REPO), *(repos or [])]))
    document["code_hosts"].append(bitbucket_dc.code_host_entry())
    if poll_interval_s is not None:
        [tracker] = [t for t in document["trackers"] if t["name"] == TRACKER]
        tracker["poll_interval_s"] = poll_interval_s
    return document


@dataclass
class Intake:
    client: TestClient
    jira: jira_fake.JiraFake
    github: _GitHub
    starter: jira_fake.JiraUser
    outsider: jira_fake.JiraUser
    key_dir: Path
    monkeypatch: pytest.MonkeyPatch
    dc: bitbucket_dc.BitbucketDcFake = field(default_factory=bitbucket_dc.BitbucketDcFake)
    dc_down: bool = False
    dc_status: int = 503
    due: dict[str, float] = field(default_factory=dict)
    clock: float = 10_000.0

    def configure(self, document: dict[str, Any]) -> None:
        self.key_dir = self.key_dir / "next"
        self.monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, self.key_dir))
        get_settings.cache_clear()

    def acquire(self, request_id: uuid.UUID) -> httpx.Response:
        external = httpx.AsyncClient(
            transport=_ByHost(
                self.jira, self.github, self.dc, dc_down=self.dc_down, dc_status=self.dc_status
            )
        )
        original = self.client.app.state.http_client
        self.client.app.state.http_client = external
        try:
            return _acquire(self.client, request_id)
        finally:
            self.client.app.state.http_client = original
            self.client.portal.call(external.aclose)

    def agent(self, *, trigger_label: str | None = None) -> uuid.UUID:
        channel: dict[str, Any] = {"kind": "jira", "address": BINDING}
        if trigger_label is not None:
            channel["trigger_label"] = trigger_label
        created = self.client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-jira-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": channel,
            },
        )
        assert created.status_code == 201, created.text
        return uuid.UUID(created.json()["id"])

    def run(self, *, advance: float) -> None:
        """One pass, ``advance`` seconds after the last on the pass's clock."""

        from curie_api import factory_tracker_intake

        self.clock += advance

        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            try:
                maker = async_sessionmaker(engine, expire_on_commit=False)
                transport = _ByHost(
                    self.jira, self.github, self.dc, dc_down=self.dc_down, dc_status=self.dc_status
                )
                async with httpx.AsyncClient(transport=transport) as client:
                    await factory_tracker_intake.poll_trackers(
                        maker, get_settings(), client, due=self.due, clock=self.clock
                    )
            finally:
                await engine.dispose()

        asyncio.run(go())

    def poll(self) -> None:
        """A pass where only the poll interval has passed, not the sweep's."""

        self.run(advance=60.0)

    def sweep(self) -> None:
        """A pass after the sweep interval: every labelled issue is re-read."""

        self.run(advance=SWEEP + 60.0)

    def ref(self, issue: jira_fake.JiraIssue) -> TrackerIssueRef:
        return self.jira.ref(issue)


@pytest.fixture
def make_intake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[Callable[[dict[str, Any]], Intake]]:
    stack: list[TestClient] = []

    def build(document: dict[str, Any]) -> Intake:
        env = {
            "GITHUB_APP_ID": "51",
            "GITHUB_APP_PRIVATE_KEY": "example-private-key",
            "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
            "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
            "GITHUB_TOKEN": "",
            "GITHUB_REVIEW_INGRESS_ENABLED": "false",
            "FACTORY_ENABLED": "true",
            "FACTORY_INTAKE": "webhook",
            "FACTORY_RECONCILE_INTERVAL_S": str(SWEEP),
            "FACTORY_BASE_LABEL_SETTLE_S": "0",
            "INTERNAL_WORKER_TOKEN": "factory-intake-worker",
            "CURIE_FORGES": forges_env_json(document, tmp_path / "forges"),
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "RESUME_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "RUNS_STREAM": f"test:curie:tracker-intake:{uuid.uuid4().hex}",
        }
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        monkeypatch.delenv("GITHUB_API_URL", raising=False)
        get_settings.cache_clear()
        fake_installation_tokens(monkeypatch)
        client = TestClient(create_app())
        client.__enter__()
        stack.append(client)
        jira = jira_fake.JiraFake()
        github = _GitHub()
        return Intake(
            client=client,
            jira=jira,
            github=github,
            starter=jira.add_user(STARTER_ID, "Ada Starter"),
            outsider=jira.add_user(OUTSIDER_ID, "Oscar Outsider"),
            key_dir=tmp_path / "forges",
            monkeypatch=monkeypatch,
        )

    try:
        yield build
    finally:
        for client in stack:
            client.__exit__(None, None, None)
        get_settings.cache_clear()


@pytest.fixture
def intake(make_intake: Callable[[dict[str, Any]], Intake]) -> Intake:
    return make_intake(_document())


# --- observations ------------------------------------------------------------------


def _work_items(issue: jira_fake.JiraIssue) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, agent_id, tracker_kind, tracker_host, tracker_scope_id, tracker_display_key, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, base_branch, base_source, base_commit, "
        "base_resolved_request_id, trigger_label, cancelled_at "
        "FROM curie.work_items WHERE tracker_kind = 'jira_cloud' AND tracker_issue_id = :id",
        {"id": str(issue.id)},
    )


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, status, objective, requester, reply_kind, reply_address, "
        "reply_conversation_id FROM curie.execution_requests WHERE work_item_id = :id "
        "ORDER BY sequence",
        {"id": work_item_id},
    )


def _cursor(label: str = LABEL) -> str | None:
    rows = _rows(
        "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'jira_cloud' "
        "AND tracker_host = :host AND tracker_scope_id = :scope",
        {"host": jira_fake.SITE_HOST, "scope": jira_fake.CLOUD_ID},
    )
    if not rows:
        return None
    value = (rows[0]["etags"] or {}).get(f"marked:{label}")
    return None if value is None else str(value)


def _bot_comments(intake: Intake, issue: jira_fake.JiraIssue) -> list[jira_fake.JiraComment]:
    return intake.jira.own_comments(issue.id)


def _texts(intake: Intake, issue: jira_fake.JiraIssue) -> list[str]:
    return [jira_fake.flatten(comment.body) for comment in _bot_comments(intake, issue)]


def _marker(comment: jira_fake.JiraComment) -> str:
    return str(comment.properties["curie.factory"]["marker"])


def _code_spans(node: Any) -> list[str]:
    """The text of every ADF text node carrying a ``code`` mark: a markdown code
    span in the core's body becomes one (decision 15)."""

    if isinstance(node, list):
        return [text for child in node for text in _code_spans(child)]
    if not isinstance(node, dict):
        return []
    if node.get("type") == "text":
        marks = {mark.get("type") for mark in node.get("marks") or []}
        return [str(node.get("text", ""))] if "code" in marks else []
    return _code_spans(node.get("content", []))


def _admit_id(intake: Intake, issue: jira_fake.JiraIssue, event_id: str) -> uuid.UUID:
    """The request id the label event ``event_id`` admits under (ADR 0187)."""

    return notice_request_id(
        MarkedNotice(
            issue=intake.ref(issue),
            marker=LABEL,
            actor=intake.starter.actor,
            event_id=event_id,
            disposition=Disposition.ADMIT,
            cursor="",
            occurred_at=None,
        )
    )


def _issue(
    intake: Intake, title: str = "Widget parser drops the last field"
) -> jira_fake.JiraIssue:
    return intake.jira.seed_issue(title, "Parsing a three-field line yields two fields.")


# --- admission --------------------------------------------------------------------------


def test_an_allowlisted_label_admits_one_work_item_on_the_bound_repository(
    intake: Intake,
) -> None:
    agent = intake.agent()
    issue = _issue(intake)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)

    intake.poll()

    [item] = _work_items(issue)
    assert item["agent_id"] == agent
    assert (item["tracker_host"], item["tracker_scope_id"], item["tracker_display_key"]) == (
        jira_fake.SITE_HOST,
        jira_fake.CLOUD_ID,
        issue.key,
    )
    assert (item["code_host_kind"], item["repository_project_id"], item["repository_path"]) == (
        types.GITHUB,
        str(REPO_ID),
        REPO,
    )
    assert item["code_host_installation_id"] == INSTALLATION_ID
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (None, None, None)
    assert item["base_resolved_request_id"] is None
    assert item["trigger_label"] == LABEL
    assert not any("/git/ref/heads/" in request.url.path for request in intake.github.calls)
    [request] = _requests(item["id"])
    assert request["id"] == _admit_id(intake, issue, event)
    assert request["status"] == "waiting"
    assert request["objective"].splitlines()[0] == f"{jira_fake.SITE}/browse/{issue.key}"
    assert request["requester"] == f"jira_cloud:{STARTER_ID}:{intake.starter.display_name}"
    assert (request["reply_kind"], request["reply_address"], request["reply_conversation_id"]) == (
        "jira",
        BINDING,
        f"issue-{issue.id}",
    )
    assert _cursor() is not None
    # Admission writes nothing on Jira: the status comment is the sync's.
    assert intake.jira.writes == 0

    acquired = intake.acquire(request["id"])

    assert acquired.status_code == 200, acquired.text
    [claimed] = _work_items(issue)
    assert (claimed["base_branch"], claimed["base_source"], claimed["base_commit"]) == (
        "main",
        "default",
        intake.github.branches["main"],
    )
    assert claimed["base_resolved_request_id"] == request["id"]
    assert [row["id"] for row in _requests(item["id"])] == [request["id"]]
    assert intake.jira.writes == 0


@pytest.mark.parametrize("who", ["outsider", "bot", "app", "customer"])
def test_a_label_from_an_account_without_start_authority_admits_nothing(
    intake: Intake, who: str
) -> None:
    intake.agent()
    issue = _issue(intake)
    marker = {
        "outsider": intake.outsider,
        "bot": intake.jira.bot,
        "app": intake.jira.add_user("5b10a0000000000000000a33", "Some App", account_type="app"),
        "customer": intake.jira.add_user("qm:0000-customer", "A Customer", account_type="customer"),
    }[who]
    if who != "outsider":
        # Allowlisted by mistake: the account type still keeps it out (decision 10).
        intake.configure(
            _document(
                jira_binding=_jira_binding(
                    start_authority={
                        "type": "allowlist",
                        "accounts": [STARTER_ID, marker.account_id],
                    }
                )
            )
        )
    intake.jira.add_label(issue.id, LABEL, marker)

    intake.poll()
    intake.sweep()

    assert _work_items(issue) == []
    assert _bot_comments(intake, issue) == []
    assert intake.jira.writes == 0
    # Liveness: the allowlisted account's label on the same issue admits.
    intake.jira.remove_label(issue.id, LABEL, marker)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    assert len(_work_items(issue)) == 1


def test_label_removal_cancels_the_work_item(intake: Intake) -> None:
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    assert item["cancelled_at"] is None

    intake.jira.remove_label(issue.id, LABEL, intake.starter)
    intake.poll()

    [cancelled] = _work_items(issue)
    assert cancelled["cancelled_at"] is not None
    assert all(r["status"] not in {"waiting", "running"} for r in _requests(item["id"]))


@pytest.mark.parametrize("change", ["removed", "rescoped"])
def test_work_on_a_scope_no_binding_serves_is_cancelled(intake: Intake, change: str) -> None:
    """r1 finding 3: with its binding gone the WorkItem can no longer be polled
    or cancelled through a tracker, so the next poll of the tracker cancels it."""

    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    # Liveness: a poll while the binding still serves the scope cancels nothing.
    intake.poll()
    [kept] = _work_items(issue)
    assert kept["cancelled_at"] is None

    document = _document()
    if change == "removed":
        document["bindings"] = [b for b in document["bindings"] if b["name"] != BINDING]
    else:
        document["bindings"][-1]["tracker_scope_id"] = "a1b2c3d4-0000-4000-8000-0000000000ff"
    intake.configure(document)
    requests_before = len(intake.jira.requests)
    intake.poll()

    [cancelled] = _work_items(issue)
    assert cancelled["cancelled_at"] is not None
    assert all(r["status"] not in {"waiting", "running"} for r in _requests(item["id"]))
    # The issue still carries its label; the cancel came from the missing binding.
    assert LABEL in intake.jira.issue(issue.id).labels
    if change == "removed":
        assert len(intake.jira.requests) == requests_before


# --- refusals ------------------------------------------------------------------------


def test_label_conflict_and_other_agent_refusals_comment_once_with_the_github_texts(
    intake: Intake,
) -> None:
    first = intake.agent()
    intake.agent(trigger_label=FAST)
    conflicted = _issue(intake)
    intake.jira.add_label(conflicted.id, FAST, intake.starter)
    intake.jira.add_label(conflicted.id, LABEL, intake.starter)

    intake.poll()

    assert _work_items(conflicted) == []
    [comment] = _bot_comments(intake, conflicted)
    assert _marker(comment) == LABEL_REFUSAL_MARKER
    text = jira_fake.flatten(comment.body)
    assert (
        "Curie did not start this issue: it carries factory, factory-fast, which start "
        "different agents" in text
    )
    assert _code_spans(comment.body) == ["factory", "factory-fast"]
    writes = intake.jira.writes
    intake.sweep()
    assert intake.jira.writes == writes, "a refusal repeated by the sweep writes nothing"
    assert len(_bot_comments(intake, conflicted)) == 1

    # The issue's WorkItem belongs to the first agent; a relabel for the other
    # agent is refused with the other-agent text.
    owned = _issue(intake, "Widget parser drops a quoted field")
    intake.jira.add_label(owned.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(owned)
    assert item["agent_id"] == first
    intake.jira.remove_label(owned.id, LABEL, intake.starter)
    intake.poll()
    intake.jira.add_label(owned.id, FAST, intake.starter)
    intake.poll()
    [still] = _work_items(owned)
    assert still["agent_id"] == first and still["cancelled_at"] is not None
    refusals = [c for c in _bot_comments(intake, owned) if _marker(c) == LABEL_REFUSAL_MARKER]
    assert len(refusals) == 1
    assert (
        "Curie did not start this issue under factory-fast: it belongs to the run the "
        "factory label started, which is another agent's."
    ) in jira_fake.flatten(refusals[0].body)
    assert _code_spans(refusals[0].body)[:2] == ["factory-fast", "factory"]


def test_a_repository_refusal_comments_the_refusal_body(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    web = {"alias": "web", "code_host": "github", "project_id": "4402", "path": "acme-corp/web"}
    intake = make_intake(_document(jira_binding=_jira_binding(repos=[web])))
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "repo:bot", intake.starter)
    intake.jira.add_label(issue.id, "repo:web", intake.starter)
    intake.jira.add_label(issue.id, LABEL, intake.starter)

    intake.poll()

    assert _work_items(issue) == []
    [text] = _texts(intake, issue)
    assert text.startswith("Curie did not start this issue: the issue has more than one")
    assert "repo:bot" in text and "repo:web" in text
    # Saved past the refused event: the label is not stuck behind it.
    assert _cursor() is not None


def test_binding_missing_with_two_surfaces_logs_both_agents(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    explicit = intake.agent(trigger_label=FAST)
    default = intake.agent()
    # The tracker label is changed to equal the explicit one: two surfaces
    # resolve to one label, and neither is picked.
    intake.configure(_document(label=FAST))
    issue = _issue(intake)
    intake.jira.add_label(issue.id, FAST, intake.starter)

    with caplog.at_level(logging.WARNING):
        intake.poll()

    assert _work_items(issue) == []
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert str(explicit) in logged and str(default) in logged
    assert _bot_comments(intake, issue) == []


# --- the cursor ---------------------------------------------------------------------


def test_the_cursor_is_written_only_after_a_fully_applied_page(intake: Intake) -> None:
    intake.agent()
    first = _issue(intake)
    intake.jira.add_label(first.id, LABEL, intake.starter)
    assert _cursor() is None
    intake.poll()
    saved = _cursor()
    assert saved is not None and len(_work_items(first)) == 1

    second = _issue(intake, "Widget parser drops a quoted field")
    intake.jira.advance(5)
    intake.jira.add_label(second.id, LABEL, intake.starter)
    # The search's next page fails: Unavailable stops the label, cursor unchanged.
    intake.jira.fail_next("search", status=503)
    intake.poll()
    assert _work_items(second) == []
    assert _cursor() == saved

    intake.poll()
    assert len(_work_items(second)) == 1
    assert _cursor() != saved
    # The first issue was never admitted twice.
    assert len(_work_items(first)) == 1
    assert len(_requests(_work_items(first)[0]["id"])) == 1


def test_an_unauthorized_token_leaves_the_cursor_and_logs_no_token(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.jira.fail_next("search", status=401)

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    assert _work_items(issue) == []
    assert _cursor() is None
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert jira_fake.TOKEN not in logged
    assert TRACKER in logged or BINDING in logged


# --- refused, repaired, admitted by the sweep (r1 finding 3) ---------------------------


def test_refused_repo_label_is_admitted_after_repair(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    web = {"alias": "web", "code_host": "github", "project_id": "4402", "path": "acme-corp/web"}
    intake = make_intake(_document(jira_binding=_jira_binding(repos=[web])))
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "repo:bot", intake.starter)
    intake.jira.add_label(issue.id, "repo:web", intake.starter)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    assert _work_items(issue) == []
    assert len(_bot_comments(intake, issue)) == 1
    refused_cursor = _cursor()
    assert refused_cursor is not None

    intake.jira.remove_label(issue.id, "repo:web", intake.starter)
    # The cursor moved past the refused event: a poll alone does not re-read it.
    intake.poll()
    assert _work_items(issue) == []
    # The sweep re-reads the labelled issue and admits it under the original event.
    intake.sweep()

    [item] = _work_items(issue)
    assert item["repository_path"] == REPO
    [request] = _requests(item["id"])
    assert request["id"] == _admit_id(intake, issue, event)


def test_label_conflict_is_admitted_after_repair(intake: Intake) -> None:
    owner = intake.agent()
    intake.agent(trigger_label=FAST)
    issue = _issue(intake)
    intake.jira.add_label(issue.id, FAST, intake.starter)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    assert _work_items(issue) == []

    intake.jira.remove_label(issue.id, FAST, intake.starter)
    intake.poll()
    assert _work_items(issue) == []
    intake.sweep()

    [item] = _work_items(issue)
    assert item["agent_id"] == owner
    assert [r["id"] for r in _requests(item["id"])] == [_admit_id(intake, issue, event)]


def test_group_grant_after_refusal_is_admitted_by_sweep(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    group = {"type": "group", "group_id": STARTERS_GROUP}
    intake = make_intake(_document(jira_binding=_jira_binding(start_authority=group)))
    intake.agent()
    issue = _issue(intake)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    assert _work_items(issue) == []
    # No comment: an actor without authority is ignored, as GitHub ignores a non-writer.
    assert _bot_comments(intake, issue) == []

    intake.jira.set_group_member(STARTERS_GROUP, STARTER_ID, True)
    intake.poll()
    assert _work_items(issue) == []
    intake.sweep()

    [item] = _work_items(issue)
    assert [r["id"] for r in _requests(item["id"])] == [_admit_id(intake, issue, event)]
    assert any(r.kind == "user_groups" for r in intake.jira.requests)


def test_sweep_replays_an_admitted_issue(intake: Intake) -> None:
    intake.agent()
    issue = _issue(intake)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    before = _requests(item["id"])

    intake.sweep()
    intake.sweep()

    assert len(_work_items(issue)) == 1
    after = _requests(item["id"])
    assert (
        [r["id"] for r in after] == [r["id"] for r in before] == [_admit_id(intake, issue, event)]
    )
    assert after[0]["status"] == before[0]["status"]


# --- mentions (r1 finding 1) -----------------------------------------------------------


def test_mention_by_an_actor_without_start_authority_revises_nothing(intake: Intake) -> None:
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    assert len(_requests(item["id"])) == 1

    intake.jira.add_comment(
        issue.id,
        intake.outsider,
        jira_fake.adf_mention(
            jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" please also handle tabs"
        ),
    )
    intake.poll()
    intake.sweep()
    assert len(_requests(item["id"])) == 1

    # Liveness: the same mention by an allowlisted account revises the WorkItem.
    comment_id = intake.jira.add_comment(
        issue.id,
        intake.starter,
        jira_fake.adf_mention(
            jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" please also handle tabs"
        ),
    )
    intake.poll()
    rows = _requests(item["id"])
    assert len(rows) == 2
    link = f"{jira_fake.SITE}/browse/{issue.key}"
    assert rows[1]["objective"].splitlines()[0] == f"{link}?focusedCommentId={comment_id}"
    assert (rows[1]["reply_kind"], rows[1]["reply_address"]) == ("jira", BINDING)


def test_polled_base_changes_can_return_to_an_earlier_base_without_replaying_a_request(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_document(jira_binding=_jira_binding(bases=["main", "next"])))
    intake.github.push_head("next")
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "base:main", intake.starter)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    [initial] = _requests(item["id"])
    assert intake.acquire(initial["id"]).status_code == 200
    seen = [initial["id"]]
    previous_base = "main"

    for branch in ("next", "main", "next"):
        intake.jira.remove_label(issue.id, f"base:{previous_base}", intake.starter)
        intake.jira.add_label(issue.id, f"base:{branch}", intake.starter)
        intake.poll()

        requests = _requests(item["id"])
        assert len(requests) == len(seen) + 1, requests
        assert all(row["status"] == "cancelled" for row in requests[:-1])
        latest = requests[-1]
        assert latest["status"] == "waiting"
        assert latest["id"] not in seen
        seen.append(latest["id"])
        acquired = intake.acquire(latest["id"])
        assert acquired.status_code == 200, acquired.text
        [selected] = _work_items(issue)
        assert (selected["base_branch"], selected["base_commit"]) == (
            branch,
            intake.github.branches[branch],
        )
        intake.poll()
        assert [row["id"] for row in _requests(item["id"])] == seen
        previous_base = branch


@pytest.mark.parametrize("finished_base", ["main", "next"])
def test_base_poll_reads_active_selection_without_reopening_or_reading_finished_work(
    make_intake: Callable[[dict[str, Any]], Intake], finished_base: str
) -> None:
    intake = make_intake(_document(jira_binding=_jira_binding(bases=["main", "next"])))
    intake.github.push_head("next")
    intake.agent()
    issues = [_issue(intake, title) for title in ("Finished", "Pinned", "Unresolved", "Pending")]
    for issue in issues:
        intake.jira.add_label(issue.id, "base:main", intake.starter)
        intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    items = [_work_items(issue)[0] for issue in issues]
    requests = [_requests(item["id"])[0] for item in items]
    grants = {}
    for index in (0, 1, 3):
        acquired = intake.acquire(requests[index]["id"])
        assert acquired.status_code == 200, acquired.text
        grants[index] = acquired.json()
    started = intake.client.post(
        f"/v1/internal/work-items/requests/{requests[1]['id']}/start",
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
        json={
            "owner": "factory-intake-owner",
            "generation": grants[1]["generation"],
            "claim_name": "claim-active-poll",
            "sandbox_name": "sandbox-active-poll",
            "context": grants[1]["context"],
        },
    )
    assert started.status_code == 200, started.text
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "started_at = clock_timestamp(), "
        "execution_deadline = clock_timestamp() + interval '1 hour', "
        "execution_attempts = 1, terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "acquire_owner = NULL, acquire_expires_at = NULL WHERE id = :id",
        {"id": requests[0]["id"]},
    )
    _execute(
        "UPDATE curie.work_items SET base_requested_branch = 'next', "
        "base_requested_source = 'label' WHERE id = :id",
        {"id": items[3]["id"]},
    )
    for index, branch in ((0, finished_base), (3, "next")):
        if branch != "main":
            intake.jira.remove_label(issues[index].id, "base:main", intake.starter)
            intake.jira.add_label(issues[index].id, f"base:{branch}", intake.starter)
    issues[0].status = "Done"
    recorded = len(intake.jira.requests)

    intake.poll()

    assert [(row["id"], row["status"]) for row in _requests(items[0]["id"])] == [
        (requests[0]["id"], "completed")
    ]
    label_reads = [
        read
        for read in intake.jira.requests[recorded:]
        if read.params.get("fields") == "labels,components"
    ]
    assert sorted(read.path for read in label_reads) == sorted(
        f"/rest/api/3/issue/{issue.id}" for issue in issues[1:]
    )
    assert _requests(items[1]["id"])[0]["id"] == requests[1]["id"]
    assert _requests(items[1]["id"])[0]["status"] == "running"
    assert _requests(items[2]["id"])[0]["id"] == requests[2]["id"]
    pending = _requests(items[3]["id"])
    assert [row["status"] for row in pending] == ["cancelled", "waiting"]
    acquired = intake.acquire(pending[-1]["id"])
    assert acquired.status_code == 200, acquired.text
    assert _work_items(issues[3])[0]["base_branch"] == "next"


# --- base and code host from the selected binding (r1 findings 4 and 6) ----------------


@pytest.mark.parametrize(
    ("jira_bases", "claimed_on"),
    [([], None), (["main", "next"], "next")],
    ids=["selected_binding_lists_no_bases", "selected_binding_allows_next"],
)
def test_base_policy_comes_from_the_selected_binding(
    make_intake: Callable[[dict[str, Any]], Intake],
    jira_bases: list[str],
    claimed_on: str | None,
) -> None:
    # Another binding on the same repository, listed first, allows `next`. A
    # first-match repository lookup would borrow its bases.
    github_binding = github_repository_binding(bases=["main", "next"])
    intake = make_intake(
        _document(jira_binding=_jira_binding(bases=jira_bases), before=[github_binding])
    )
    intake.github.push_head("next")
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "base:next", intake.starter)
    intake.jira.add_label(issue.id, LABEL, intake.starter)

    intake.poll()

    [item] = _work_items(issue)
    [request] = _requests(item["id"])
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (None, None, None)
    assert item["base_resolved_request_id"] is None
    assert not any("/git/ref/heads/" in call.url.path for call in intake.github.calls)
    if claimed_on is None:
        [refusal] = _texts(intake, issue)
        assert "next" in refusal
        assert "not an allowed base" in refusal
    else:
        assert _bot_comments(intake, issue) == []

    acquired = intake.acquire(request["id"])

    if claimed_on is None:
        assert acquired.status_code == 409, acquired.text
        [waiting] = _work_items(issue)
        assert (waiting["base_branch"], waiting["base_commit"]) == (None, None)
        assert waiting["base_resolved_request_id"] is None
        assert [(row["id"], row["status"]) for row in _requests(item["id"])] == [
            (request["id"], "waiting")
        ]
        assert not any("/git/ref/heads/" in call.url.path for call in intake.github.calls)
        assert _texts(intake, issue) == [refusal]
        refused_again = intake.acquire(request["id"])
        assert refused_again.status_code == 409, refused_again.text
        assert _texts(intake, issue) == [refusal]
        intake.jira.remove_label(issue.id, "base:next", intake.starter)
        intake.sweep()
        repaired = intake.acquire(request["id"])
        assert repaired.status_code == 200, repaired.text
        [claimed] = _work_items(issue)
        assert (claimed["base_branch"], claimed["base_source"], claimed["base_commit"]) == (
            "main",
            "default",
            intake.github.branches["main"],
        )
    else:
        assert acquired.status_code == 200, acquired.text
        [claimed] = _work_items(issue)
        assert (claimed["base_branch"], claimed["base_source"], claimed["base_commit"]) == (
            claimed_on,
            "label",
            intake.github.branches[claimed_on],
        )
    assert claimed["base_resolved_request_id"] == request["id"]
    assert [row["id"] for row in _requests(item["id"])] == [request["id"]]


def test_code_host_and_reply_route_follow_the_bound_repository(
    intake: Intake, monkeypatch: pytest.MonkeyPatch
) -> None:
    intake.agent()
    issue = _issue(intake)
    event = intake.jira.add_label(issue.id, LABEL, intake.starter)
    original = ForgeRegistry.code_host

    def unconfigured(self: ForgeRegistry, name: str) -> Any:
        raise Unavailable("code_host_unconfigured")

    # The bound repository's code host entry cannot be built: the admission
    # defers and the cursor stays, so nothing is lost.
    monkeypatch.setattr(ForgeRegistry, "code_host", unconfigured)
    intake.poll()
    assert _work_items(issue) == []
    assert _cursor() is None
    # The held issue says so once (decision 3).
    [hold] = _bot_comments(intake, issue)
    assert _marker(hold) == HOLD_MARKER

    monkeypatch.setattr(ForgeRegistry, "code_host", original)
    asked: list[str] = []

    def recorded(self: ForgeRegistry, name: str) -> Any:
        asked.append(name)
        return original(self, name)

    monkeypatch.setattr(ForgeRegistry, "code_host", recorded)
    intake.poll()

    [item] = _work_items(issue)
    assert "github" in asked
    registry_host = get_settings().forges.code_host("github").host
    assert (item["code_host_kind"], item["code_host_host"]) == (types.GITHUB, registry_host)
    [request] = _requests(item["id"])
    assert request["id"] == _admit_id(intake, issue, event)
    assert (request["reply_kind"], request["reply_address"]) == ("jira", BINDING)


def test_the_github_poll_never_reads_a_jira_binding(intake: Intake) -> None:
    # GitHub behavior is unchanged: the pass touches only non-GitHub trackers,
    # so a GitHub-only install's GitHub fake sees no request from it.
    intake.configure(copy.deepcopy(forges_document()))
    intake.poll()
    assert intake.jira.requests == []


# --- one issue's deferral holds only that issue (#4621) ---------------------------------


def _held_tickets(intake: Intake) -> tuple[jira_fake.JiraIssue, jira_fake.JiraIssue, str]:
    """A ticket on the DC repository labelled first, then one on the GitHub
    default; returns both and the healthy ticket's label event."""

    intake.agent()
    dc_issue = _issue(intake, "Widget exporter drops the header row")
    healthy = _issue(intake)
    intake.jira.add_label(dc_issue.id, "repo:dc", intake.starter)
    intake.jira.add_label(dc_issue.id, LABEL, intake.starter)
    intake.jira.advance(5)
    event = intake.jira.add_label(healthy.id, LABEL, intake.starter)
    return dc_issue, healthy, event


def _assert_admitted_once(intake: Intake, issue: jira_fake.JiraIssue, event: str) -> None:
    [item] = _work_items(issue)
    assert item["cancelled_at"] is None
    assert [r["id"] for r in _requests(item["id"])] == [_admit_id(intake, issue, event)]


def _assert_held(intake: Intake, issue: jira_fake.JiraIssue) -> None:
    assert _work_items(issue) == []
    [hold] = _bot_comments(intake, issue)
    assert _marker(hold) == HOLD_MARKER
    text = jira_fake.flatten(hold.body)
    assert text.strip()
    for leaked in (DC_SENTINEL, bitbucket_dc.FACTORY_TOKEN, jira_fake.TOKEN, "503", "401"):
        assert leaked not in text


def _assert_on_dc(issue: jira_fake.JiraIssue) -> None:
    [item] = _work_items(issue)
    assert (
        item["code_host_kind"],
        item["code_host_host"],
        item["repository_project_id"],
        item["repository_path"],
    ) == (types.BITBUCKET_DC, bitbucket_dc.HOST, str(bitbucket_dc.REPO_ID), bitbucket_dc.REPO_PATH)
    assert item["cancelled_at"] is None


def test_missing_repository_comments_once(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    intake.dc_status = 404

    intake.poll()

    assert _work_items(dc_issue) == []
    _assert_admitted_once(intake, healthy, event)
    assert _bot_comments(intake, healthy) == []
    [comment] = _bot_comments(intake, dc_issue)
    assert _marker(comment) == REPO_REFUSAL_MARKER
    assert _code_spans(comment.body) == [DC_REPO["alias"]]
    body = jira_fake.flatten(comment.body)
    assert "repository" in body and "re-add" in body
    for leaked in (DC_SENTINEL, bitbucket_dc.FACTORY_TOKEN, jira_fake.TOKEN, "NotFound"):
        assert leaked not in body
    assert _cursor() is not None
    writes = intake.jira.writes

    intake.sweep()
    intake.sweep()

    assert intake.jira.writes == writes
    assert len(_bot_comments(intake, dc_issue)) == 1
    assert _work_items(dc_issue) == []
    _assert_admitted_once(intake, healthy, event)

    intake.dc_down = False
    intake.jira.remove_label(dc_issue.id, LABEL, intake.starter)
    repaired_event = intake.jira.add_label(dc_issue.id, LABEL, intake.starter)
    intake.poll()

    _assert_on_dc(dc_issue)
    _assert_admitted_once(intake, dc_issue, repaired_event)
    _assert_admitted_once(intake, healthy, event)
    assert len(_bot_comments(intake, dc_issue)) == 1


def test_missing_repository_logs_warning(
    make_intake: Callable[[dict[str, Any]], Intake], caplog: pytest.LogCaptureFixture
) -> None:
    intake = make_intake(_dc_document())
    intake.agent()
    intake.poll()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "repo:dc", intake.starter)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.dc_down = True
    intake.dc_status = 404

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    [warning] = [
        record
        for record in caplog.records
        if record.name == "curie_api.factory_tracker_intake" and record.levelno == logging.WARNING
    ]
    message = warning.getMessage()
    assert all(value in message for value in (BINDING, DC_REPO["alias"], DC_REPO["path"]))
    assert warning.exc_info is None
    for leaked in (DC_SENTINEL, bitbucket_dc.FACTORY_TOKEN, jira_fake.TOKEN, "NotFound"):
        assert leaked not in caplog.text
    assert _work_items(issue) == []
    [comment] = _bot_comments(intake, issue)
    assert _marker(comment) == REPO_REFUSAL_MARKER


@pytest.mark.parametrize("read_after", [0, 1, 2], ids=["verify", "labels", "repository-probe"])
def test_missing_issue_skips_silently(
    make_intake: Callable[[dict[str, Any]], Intake],
    caplog: pytest.LogCaptureFixture,
    read_after: int,
) -> None:
    intake = make_intake(_dc_document())
    intake.agent()
    intake.poll()
    saved = _cursor()
    missing = _issue(intake, "Widget exporter drops the header row")
    healthy = _issue(intake)
    intake.jira.add_label(missing.id, "repo:dc", intake.starter)
    intake.jira.add_label(missing.id, LABEL, intake.starter)
    intake.jira.advance(5)
    event = intake.jira.add_label(healthy.id, LABEL, intake.starter)
    intake.dc_down = True
    intake.dc_status = 404
    intake.jira.fail_next(
        "issue", status=404, after=read_after, body={"errorMessages": [DC_SENTINEL]}
    )

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    reads = [
        request
        for request in intake.jira.requests_of("issue")
        if request.path.endswith(f"/issue/{missing.id}")
    ]
    assert [request.status for request in reads] == [200] * read_after + [404]
    assert _work_items(missing) == []
    assert _bot_comments(intake, missing) == []
    _assert_admitted_once(intake, healthy, event)
    assert intake.jira.requests_of("comment_create") == []
    assert intake.jira.requests_of("comment_update") == []
    assert len(
        [
            request
            for request in intake.jira.requests_of("comments")
            if request.path.endswith(f"/issue/{missing.id}/comment")
        ]
    ) == 1
    assert intake.jira.writes == 0
    assert _cursor() not in (None, saved)
    assert not any(record.levelno >= logging.WARNING for record in caplog.records)
    for leaked in (DC_SENTINEL, bitbucket_dc.FACTORY_TOKEN, jira_fake.TOKEN):
        assert leaked not in caplog.text


@pytest.mark.parametrize("failure", ["probe", "feedback"])
def test_missing_repository_failure_holds_only_its_issue(
    make_intake: Callable[[dict[str, Any]], Intake], failure: str
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    intake.dc_status = 404
    if failure == "probe":
        intake.jira.fail_next("issue", status=503, after=2)
    else:
        intake.jira.fail_next("comment_create", status=503)

    intake.poll()

    _assert_held(intake, dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is None

    intake.poll()

    assert _work_items(dc_issue) == []
    assert {_marker(comment) for comment in _bot_comments(intake, dc_issue)} == {
        HOLD_MARKER,
        REPO_REFUSAL_MARKER,
    }
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is not None


def test_unavailable_notice_does_not_starve_other_repositories(
    make_intake: Callable[[dict[str, Any]], Intake], caplog: pytest.LogCaptureFixture
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert _bot_comments(intake, healthy) == []
    # The held notice keeps the label's cursor where it was.
    assert _cursor() is None
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert DC_SENTINEL not in logged
    assert bitbucket_dc.FACTORY_TOKEN not in logged

    intake.dc_down = False
    intake.poll()

    _assert_on_dc(dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is not None


def test_a_held_issue_replays_across_polls_and_a_restart_without_duplicates(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True

    intake.poll()
    intake.poll()
    # A restart: the in-memory schedule starts empty, every key due.
    intake.due = {}
    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    # The hold is upserted, never repeated.
    _assert_held(intake, dc_issue)
    assert _cursor() is None

    intake.dc_down = False
    intake.poll()
    _assert_on_dc(dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is not None

    # A later removal on the repaired issue is applied in order.
    intake.jira.advance(5)
    intake.jira.remove_label(dc_issue.id, LABEL, intake.starter)
    intake.poll()
    [cancelled] = _work_items(dc_issue)
    assert cancelled["cancelled_at"] is not None
    _assert_admitted_once(intake, healthy, event)


def test_a_sweep_only_pass_holds_one_issue_and_admits_the_other(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    # The poll interval outlasts the sweep's, so the second pass only sweeps.
    intake = make_intake(_dc_document(poll_interval_s=100_000.0))
    intake.agent()
    intake.poll()
    polled = _cursor()

    dc_issue = _issue(intake, "Widget exporter drops the header row")
    healthy = _issue(intake)
    intake.jira.add_label(dc_issue.id, "repo:dc", intake.starter)
    intake.jira.add_label(dc_issue.id, LABEL, intake.starter)
    intake.jira.advance(5)
    event = intake.jira.add_label(healthy.id, LABEL, intake.starter)
    intake.dc_down = True
    searches = len(intake.jira.requests_of("search"))

    intake.sweep()

    # Only the sweep read the label: no cursor moved.
    assert len(intake.jira.requests_of("search")) > searches
    assert _cursor() == polled
    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)


def test_a_sweep_after_a_held_poll_still_runs(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    web = {"alias": "web", "code_host": "github", "project_id": "4402", "path": "acme-corp/web"}
    intake = make_intake(_dc_document(repos=[web]))
    intake.agent()
    # Refused while the DC is healthy; the cursor saves past the refused add.
    healthy = _issue(intake)
    intake.jira.add_label(healthy.id, "repo:bot", intake.starter)
    intake.jira.add_label(healthy.id, "repo:web", intake.starter)
    event = intake.jira.add_label(healthy.id, LABEL, intake.starter)
    intake.poll()
    assert _work_items(healthy) == []
    assert _cursor() is not None

    # Repaired: a poll alone does not re-read the add behind the cursor.
    intake.jira.remove_label(healthy.id, "repo:web", intake.starter)
    intake.poll()
    assert _work_items(healthy) == []
    saved = _cursor()
    assert saved is not None

    dc_issue = _issue(intake, "Widget exporter drops the header row")
    intake.jira.advance(5)
    intake.jira.add_label(dc_issue.id, "repo:dc", intake.starter)
    intake.jira.add_label(dc_issue.id, LABEL, intake.starter)
    intake.dc_down = True

    # The poll and the sweep are both due. The poll reads only the DC ticket's
    # add and holds it, so only the sweep can admit the repaired ticket.
    intake.sweep()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert _cursor() == saved


def test_a_held_issues_later_notices_wait_in_order(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    intake.agent()
    # A first poll saves a cursor, so the next poll delivers mentions.
    intake.poll()
    saved = _cursor()
    assert saved is not None

    dc_issue = _issue(intake, "Widget exporter drops the header row")
    healthy = _issue(intake)
    intake.jira.add_label(dc_issue.id, "repo:dc", intake.starter)
    dc_event = intake.jira.add_label(dc_issue.id, LABEL, intake.starter)
    comment_id = intake.jira.add_comment(
        dc_issue.id,
        intake.starter,
        jira_fake.adf_mention(
            jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" please also handle tabs"
        ),
    )
    intake.jira.advance(5)
    event = intake.jira.add_label(healthy.id, LABEL, intake.starter)
    intake.dc_down = True

    def mention_checks() -> int:
        return sum(
            1
            for r in intake.jira.requests_of("comment_list")
            if int(comment_id) in ((r.json or {}).get("ids") or [])
        )

    # One page carries [admit, mention] for the DC ticket: the admission is
    # held, so the mention is not even verified.
    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert mention_checks() == 0
    assert _cursor() == saved

    intake.dc_down = False
    intake.poll()

    # Replayed in order: the admission first, then the mention revises it.
    _assert_on_dc(dc_issue)
    [item] = _work_items(dc_issue)
    rows = _requests(item["id"])
    assert len(rows) == 2
    assert rows[0]["id"] == _admit_id(intake, dc_issue, dc_event)
    assert rows[1]["objective"].splitlines()[0].endswith(f"?focusedCommentId={comment_id}")
    assert mention_checks() >= 1
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() not in (None, saved)


def test_an_unauthorized_dc_notice_holds_only_its_issue(
    make_intake: Callable[[dict[str, Any]], Intake], caplog: pytest.LogCaptureFixture
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    # A refused credential, its error body echoing the token.
    intake.dc_status = 401

    with caplog.at_level(logging.DEBUG):
        intake.poll()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert _cursor() is None
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert DC_SENTINEL not in logged
    assert bitbucket_dc.FACTORY_TOKEN not in logged
    assert "authentication rejected" in logged

    intake.dc_down = False
    intake.poll()

    _assert_on_dc(dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is not None


# --- a rejected or unreadable credential names the operator's action (#4755) ---


# The two hold bodies, as independent literals: a change to either module
# constant must stay visible here rather than following it silently.
REPLACE_CREDENTIAL = (
    "Curie could not start this issue yet: a tracker or code host credential it "
    "needs was rejected or could not be read. The operator must replace that "
    "credential. Curie retries on its next poll."
)
DID_NOT_ANSWER = (
    "Curie could not start this issue yet: a tracker or code host it needs did not "
    "answer. Curie retries on its next poll; nothing is needed unless this persists."
)


def _hold_text(intake: Intake, issue: jira_fake.JiraIssue) -> str:
    """The one bot comment on ``issue``, flattened to text; ``_assert_held`` checks
    its marker and that it leaks nothing."""

    [hold] = _bot_comments(intake, issue)
    return jira_fake.flatten(hold.body).strip()


def test_a_rejected_credential_hold_names_the_operator_action(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    # A refused credential is not an outage: the hold tells the operator to act.
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    # A refused credential, its error body echoing the token.
    intake.dc_status = 401

    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert _hold_text(intake, dc_issue) == REPLACE_CREDENTIAL


def test_an_unreadable_credential_hold_names_the_operator_action(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    # The DC token file the env loader reads, emptied so the adapter's read of
    # it fails before any request.
    token_file = intake.key_dir / "code_hosts-bitbucket-dc" / "token"
    assert token_file.exists()
    token_file.write_text("")

    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    _assert_held(intake, dc_issue)
    assert _hold_text(intake, dc_issue) == REPLACE_CREDENTIAL


def test_an_unavailable_hold_keeps_the_did_not_answer_text(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    intake.dc_status = 401

    intake.poll()
    [first] = _bot_comments(intake, dc_issue)
    assert _hold_text(intake, dc_issue) == REPLACE_CREDENTIAL

    # The host itself did not answer: the same marked comment, updated in
    # place, switches to today's text.
    intake.dc_status = 503
    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    [second] = _bot_comments(intake, dc_issue)
    assert second.id == first.id
    assert jira_fake.flatten(second.body).strip() == DID_NOT_ANSWER


def test_a_failed_hold_comment_does_not_starve_other_issues(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    intake = make_intake(_dc_document())
    dc_issue, healthy, event = _held_tickets(intake)
    intake.dc_down = True
    intake.jira.fail_next("comment_create", status=503)

    intake.poll()

    _assert_admitted_once(intake, healthy, event)
    assert _work_items(dc_issue) == []
    assert _bot_comments(intake, dc_issue) == []
    assert _cursor() is None

    intake.poll()

    _assert_held(intake, dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is None



def _start_polled_request(intake: Intake, request_id: uuid.UUID) -> int:
    acquired = intake.acquire(request_id)
    assert acquired.status_code == 200, acquired.text
    grant = acquired.json()
    started = intake.client.post(
        f"/v1/internal/work-items/requests/{request_id}/start",
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
        json={
            "owner": "factory-intake-owner",
            "generation": grant["generation"],
            "claim_name": "claim-completed-poll",
            "sandbox_name": "sandbox-completed-poll",
            "context": grant["context"],
        },
    )
    assert started.status_code == 200, started.text
    return int(started.json()["runtime_epoch"])


def _complete_polled_ci(intake: Intake, request_id: uuid.UUID, authority: str) -> None:
    import redis.asyncio as aioredis
    from curie_api import factory_ci
    from curie_api.workitem_reconciler import WorkItemReconciler
    from curie_api.workitems import lifecycle
    from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW

    async def complete() -> None:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        reconciler = WorkItemReconciler(maker, valkey, get_settings())
        try:
            async with maker() as session:
                settlement = await lifecycle.claim_publication_settlement(
                    session, exclude=frozenset()
                )
                assert settlement is not None and settlement.request_id == request_id
                assert settlement.subject.kind == authority
                await session.rollback()
            async with httpx.AsyncClient(
                transport=_ByHost(intake.jira, intake.github, intake.dc, dc_down=False)
            ) as external:
                result = await factory_ci.gate(
                    maker,
                    valkey,
                    get_settings(),
                    external,
                    settlement,
                    owner="completed-base-poll",
                    next_poll={},
                    dispatch=reconciler._dispatch_ci_turn,
                    may_observe=lambda _request: True,
                )
            assert result == "settled", result
        finally:
            await valkey.aclose()
            await engine.dispose()

    intake.client.portal.call(complete)
    assert _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    ) == [{"status": "completed", "terminal_cause": "completed"}]


def _completed_open_polled_authority(
    intake: Intake, authority: str
) -> tuple[jira_fake.JiraIssue, dict[str, Any], uuid.UUID, dict[str, Any]]:
    from curie_api.models import FactoryBaseTransition, ThreadPublicationLineage, WorkItem
    from sqlalchemy import text
    from test_factory_terminus import _acknowledge_publication_history, _attach_publication
    from test_github_factory_base import _open_lineage

    intake.github.push_head("next")
    intake.agent()
    issue = _issue(intake)
    intake.jira.add_label(issue.id, "base:main", intake.starter)
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    [request] = _requests(item["id"])
    epoch = _start_polled_request(intake, request["id"])
    [item] = _work_items(issue)
    head = intake.github.push_head("curie/completed-poll-source")
    if authority == "publication":
        _attach_publication(item["id"], status="succeeded", pr=77, head_sha=head)
    else:
        _open_lineage(item["id"])
    (lineage,) = _rows(
        "SELECT l.* FROM curie.thread_publication_lineages l JOIN curie.work_items w "
        "ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": item["id"]},
    )
    intake.github.branches[lineage["branch"]] = head
    intake.github.pulls[77] = {
        "head": lineage["branch"],
        "base": "main",
        "state": "open",
        "merged_at": None,
        "title": "Verify the completed factory change",
        "body": "Factory change",
        "draft": False,
    }
    intake.github.report_check(head, "unit-tests", "success")
    if authority == "publication":
        _acknowledge_publication_history(intake.client, request)
    else:
        async def applied() -> None:
            from curie_api.factory_base_reconcile import reconcile_base_transitions

            engine = create_async_engine(get_settings().database_url)
            maker = async_sessionmaker(engine, expire_on_commit=False)
            try:
                async with maker() as session:
                    stored = await session.get(WorkItem, item["id"])
                    selected = await session.get(ThreadPublicationLineage, lineage["id"])
                    assert stored is not None and selected is not None
                    now = await session.scalar(text("SELECT clock_timestamp()"))
                    selected.base_ref, selected.base_sha, selected.head_sha = (
                        "main", item["base_commit"], head
                    )
                    selected.code_host_kind = stored.code_host_kind
                    selected.code_host_host = stored.code_host_host
                    selected.repository_project_id = stored.repository_project_id
                    selected.code_host_installation_id = stored.code_host_installation_id
                    selected.code_host_pr_id = "P0EXAMPLE77"
                    session.add(
                        FactoryBaseTransition(
                            work_item_id=item["id"],
                            request_id=request["id"],
                            ci_request_id=request["id"],
                            lineage_id=lineage["id"],
                            phase="applied",
                            version=1,
                            repository_snapshot={
                                "kind": stored.code_host_kind,
                                "host": stored.code_host_host,
                                "project_id": stored.repository_project_id,
                                "path": stored.repository_path,
                                "installation_id": stored.code_host_installation_id,
                                "pull_request_id": selected.code_host_pr_id,
                            },
                            old_base_ref="next",
                            old_base_sha=intake.github.branches["next"],
                            old_head_sha=intake.github.branches["main"],
                            target_base_ref="main",
                            target_base_sha=item["base_commit"],
                            base_source="label",
                            branch=lineage["branch"],
                            pull_request_number="77",
                            committer_name="Curie",
                            committer_email="factory@example.com",
                            committer_time=now,
                            rebased_head_sha=intake.github.branches["main"],
                            verification_head_sha=head,
                            changed_paths=["README.md"],
                            ci_pushed_at=now,
                            applied_at=now,
                        )
                    )
                    await session.commit()
                async with httpx.AsyncClient(
                    transport=_ByHost(intake.jira, intake.github, intake.dc, dc_down=False)
                ) as external:
                    await reconcile_base_transitions(
                        maker, settings=get_settings(), client=external
                    )
            finally:
                await engine.dispose()

        intake.client.portal.call(applied)
        handed_off = intake.client.post(
            f"/v1/internal/work-items/requests/{request['id']}/finish",
            headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
            json={
                "runtime_epoch": epoch,
                "outcome": "failed",
                "cause": "no_pull_request",
                "detail": "No changes needed: verification passed on the moved head.",
            },
        )
        assert handed_off.status_code == 409, handed_off.text
        assert "publication_pending" in handed_off.text
    _complete_polled_ci(
        intake, request["id"], authority if authority == "publication" else "base_transition"
    )
    (lineage,) = _rows(
        "SELECT l.* FROM curie.thread_publication_lineages l JOIN curie.work_items w "
        "ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": item["id"]},
    )
    if authority == "transition":
        assert _rows(
            "SELECT id FROM curie.publications WHERE lineage_id = :id", {"id": lineage["id"]}
        ) == []
        (move,) = _rows(
            "SELECT outcome_ready_at, ci_settled_at FROM curie.factory_base_transitions "
            "WHERE lineage_id = :id",
            {"id": lineage["id"]},
        )
        assert move["outcome_ready_at"] is not None and move["ci_settled_at"] is not None
    return issue, item, request["id"], lineage


def _complete_polled_unchanged_follow_up(
    intake: Intake, issue: jira_fake.JiraIssue, item: dict[str, Any]
) -> uuid.UUID:
    intake.jira.add_comment(
        issue.id,
        intake.starter,
        jira_fake.adf_mention(
            jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" verify the existing change"
        ),
    )
    intake.poll()
    latest = _requests(item["id"])[-1]
    epoch = _start_polled_request(intake, latest["id"])
    finished = intake.client.post(
        f"/v1/internal/work-items/requests/{latest['id']}/finish",
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "no_pull_request",
            "detail": "No changes needed: the existing pull request satisfies this follow-up.",
        },
    )
    assert finished.status_code == 200, finished.text
    assert _requests(item["id"])[-1]["status"] == "completed"
    return latest["id"]


def _targeted_base_poll_reads(
    intake: Intake, jira_since: int, github_since: int
) -> tuple[list[str], list[str]]:
    labels = [
        request.path
        for request in intake.jira.requests[jira_since:]
        if request.params.get("fields") == "labels,components"
    ]
    repositories = [
        request.url.path
        for request in intake.github.calls[github_since:]
        if request.method == "GET"
        and request.url.path in {f"/repositories/{REPO_ID}", f"/repos/{REPO}"}
    ]
    return labels, repositories


@pytest.mark.parametrize("authority", ["publication", "transition"])
@pytest.mark.parametrize(
    "older_owner", [False, True], ids=["current-owner", "older-no-change-owner"]
)
def test_polled_base_change_readmits_completed_current_open_authority(
    make_intake: Callable[[dict[str, Any]], Intake], authority: str, older_owner: bool
) -> None:
    intake = make_intake(_document(jira_binding=_jira_binding(bases=["main", "next"])))
    issue, item, owner, lineage = _completed_open_polled_authority(intake, authority)
    latest = _complete_polled_unchanged_follow_up(intake, issue, item) if older_owner else owner
    before = _requests(item["id"])
    assert before[-1]["id"] == latest and before[-1]["status"] == "completed"
    intake.jira.remove_label(issue.id, "base:main", intake.starter)
    intake.jira.add_label(issue.id, "base:next", intake.starter)
    jira_since, github_since = len(intake.jira.requests), len(intake.github.calls)

    intake.poll()

    after = _requests(item["id"])
    assert len(after) == len(before) + 1, after
    assert after[:-1] == before
    successor = after[-1]
    assert successor["id"] not in {row["id"] for row in before}
    assert successor["status"] == "waiting"
    assert _targeted_base_poll_reads(intake, jira_since, github_since) == (
        [f"/rest/api/3/issue/{issue.id}"], [f"/repositories/{REPO_ID}"]
    )
    intake.poll()
    assert _requests(item["id"]) == after
    acquired = intake.acquire(successor["id"])
    assert acquired.status_code == 409, acquired.text
    (move,) = _rows(
        "SELECT id, request_id, phase, old_head_sha, target_base_ref "
        "FROM curie.factory_base_transitions WHERE work_item_id = :item AND request_id = :request",
        {"item": item["id"], "request": successor["id"]},
    )
    assert move["request_id"] == successor["id"]
    assert move["phase"] == "pending"
    assert move["old_head_sha"] == lineage["head_sha"]
    assert move["target_base_ref"] == "next"
    intake.poll()
    assert _requests(item["id"]) == after
    assert _rows(
        "SELECT id FROM curie.factory_base_transitions WHERE request_id = :id",
        {"id": successor["id"]},
    ) == [{"id": move["id"]}]
    assert _rows(
        "SELECT l.id, l.branch, l.pr_number, l.head_sha, l.base_ref "
        "FROM curie.thread_publication_lineages l WHERE l.id = :id", {"id": lineage["id"]}
    ) == [{key: lineage[key] for key in ("id", "branch", "pr_number", "head_sha", "base_ref")}]
    if authority == "transition":
        assert _rows(
            "SELECT id FROM curie.publications WHERE lineage_id = :id", {"id": lineage["id"]}
        ) == []



def _foreign_completed_polled_request(intake: Intake) -> uuid.UUID:
    issue = _issue(intake, "A separate completed ticket")
    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    [request] = _requests(item["id"])
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "started_at = clock_timestamp(), execution_attempts = 1, "
        "execution_deadline = clock_timestamp() + interval '1 hour', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed' WHERE id = :id",
        {"id": request["id"]},
    )
    return request["id"]


@pytest.mark.parametrize(
    ("authority", "damage"),
    [
        pytest.param(authority, damage, id=f"{authority}-{damage}")
        for authority, damages in (
            (
                "publication",
                (
                    "closed", "latest_failed", "latest_cancelled", "stale_base",
                    "stale_revision", "wrong_result", "unacknowledged",
                    "foreign_owner", "failed_owner", "later_applied_move",
                ),
            ),
            (
                "transition",
                (
                    "closed", "latest_failed", "latest_cancelled", "stale_base",
                    "stale_head", "unacknowledged", "unsettled", "foreign_owner", "failed_owner",
                ),
            ),
        )
        for damage in damages
    ],
)
def test_base_poll_skips_completed_items_without_current_open_authority(
    make_intake: Callable[[dict[str, Any]], Intake], authority: str, damage: str
) -> None:
    intake = make_intake(_document(jira_binding=_jira_binding(bases=["main", "next"])))
    issue, item, owner, lineage = _completed_open_polled_authority(intake, authority)
    latest = (
        _complete_polled_unchanged_follow_up(intake, issue, item)
        if damage == "failed_owner" else owner
    )
    foreign = _foreign_completed_polled_request(intake) if damage == "foreign_owner" else None
    if authority == "publication":
        table = "publications"
        (subject,) = _rows(
            "SELECT * FROM curie.publications WHERE execution_request_id = :id", {"id": owner}
        )
        base_column, outcome_column, owner_column = (
            "base_sha", "outcome_history_ready_at", "execution_request_id"
        )
    else:
        table = "factory_base_transitions"
        (subject,) = _rows(
            "SELECT * FROM curie.factory_base_transitions WHERE ci_request_id = :id", {"id": owner}
        )
        base_column, outcome_column, owner_column = (
            "target_base_sha", "outcome_ready_at", "ci_request_id"
        )
    target_id = subject["id"]
    if damage == "closed":
        table, target_id = "thread_publication_lineages", lineage["id"]
        before, broken = {"status": "open"}, {"status": "closed"}
    elif damage in {"latest_failed", "latest_cancelled", "failed_owner"}:
        table, target_id = "execution_requests", owner if damage == "failed_owner" else latest
        before = {"status": "completed", "terminal_cause": "completed"}
        broken = {
            "status": "cancelled" if damage == "latest_cancelled" else "failed",
            "terminal_cause": "issue_cancelled" if damage == "latest_cancelled" else "ci_failed",
        }
        if damage == "latest_cancelled":
            (timing,) = _rows(
                "SELECT terminal_at, cancellation_requested_at, termination_observation "
                "FROM curie.execution_requests WHERE id = :id",
                {"id": target_id},
            )
            (clock,) = _rows("SELECT clock_timestamp() AS now")
            before.update(timing)
            broken.update(
                terminal_at=clock["now"],
                cancellation_requested_at=clock["now"],
                termination_observation="runtime stopped",
            )
    elif damage == "stale_revision":
        table, target_id = "thread_publication_lineages", lineage["id"]
        before = {"latest_revision": lineage["latest_revision"]}
        broken = {"latest_revision": lineage["latest_revision"] + 1}
    elif damage == "later_applied_move":
        table, target_id = "factory_base_transitions", uuid.uuid4()
        _execute(
            "INSERT INTO curie.factory_base_transitions "
            "(id, work_item_id, request_id, ci_request_id, lineage_id, phase, "
            "old_base_ref, old_base_sha, old_head_sha, target_base_ref, target_base_sha, "
            "base_source, branch, pull_request_number, committer_name, committer_email, "
            "committer_time, verification_head_sha, applied_at, outcome_ready_at, "
            "ci_ready_at, ci_settled_at) VALUES "
            "(:id, :item, :request, :request, :lineage, 'applied', 'main', :base, :head, "
            "'main', :base, 'label', :branch, '77', 'Curie', 'factory@example.com', "
            "clock_timestamp(), :stale, clock_timestamp(), clock_timestamp(), "
            "clock_timestamp(), clock_timestamp())",
            {
                "id": target_id, "item": item["id"], "request": owner,
                "lineage": lineage["id"], "base": lineage["base_sha"],
                "head": lineage["head_sha"], "branch": lineage["branch"], "stale": "9" * 40,
            },
        )
        before = {"verification_head_sha": lineage["head_sha"]}
        broken = {"verification_head_sha": "9" * 40}
    else:
        column, value = {
            "stale_base": (base_column, "9" * 40),
            "stale_head": ("verification_head_sha", "9" * 40),
            "wrong_result": ("result_url", f"https://github.com/{REPO}/pull/78"),
            "unacknowledged": (outcome_column, None),
            "unsettled": ("ci_settled_at", None),
            "foreign_owner": (owner_column, foreign),
        }[damage]
        before, broken = {column: subject[column]}, {column: value}
    statement = (
        f"UPDATE curie.{table} SET "
        + ", ".join(f"{column} = :{column}" for column in before)
        + " WHERE id = :id"
    )
    _execute(statement, {"id": target_id, **broken})
    before_requests = _requests(item["id"])
    intake.jira.remove_label(issue.id, "base:main", intake.starter)
    intake.jira.add_label(issue.id, "base:next", intake.starter)
    jira_since, github_since = len(intake.jira.requests), len(intake.github.calls)

    intake.poll()
    intake.poll()

    assert _targeted_base_poll_reads(intake, jira_since, github_since) == ([], [])
    assert _requests(item["id"]) == before_requests
    _execute(statement, {"id": target_id, **before})
    jira_since, github_since = len(intake.jira.requests), len(intake.github.calls)

    intake.poll()

    after = _requests(item["id"])
    assert len(after) == len(before_requests) + 1, after
    assert after[-1]["status"] == "waiting"
    assert _targeted_base_poll_reads(intake, jira_since, github_since) == (
        [f"/rest/api/3/issue/{issue.id}"], [f"/repositories/{REPO_ID}"]
    )
