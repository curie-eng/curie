"""The port-driven intake pass for non-GitHub trackers (#4358 plan D1, D2, D14, D15).

`curie_api.factory_tracker_intake.poll_trackers` polls every configured
non-GitHub tracker binding through `Tracker.poll_marked`, applies each notice
through start authority, the surface for its label, the label refusal rules,
repository and base choice, and the shared WorkItem dispatch, and keeps one
opaque cursor per (tracker scope, label) in ``factory_poll_cursors.etags``
under ``marked:<label>``. A sweep re-reads every currently labelled issue every
``factory_reconcile_interval_s``, so a refusal repaired on the ticket admits
under the original label event's request id.

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

    def handle(self, request: httpx.Request) -> httpx.Response:
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
        "code_host_installation_id, base_branch, base_source, trigger_label, cancelled_at "
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
    assert (item["base_branch"], item["trigger_label"]) == ("main", LABEL)
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


# --- base and code host from the selected binding (r1 findings 4 and 6) ----------------


@pytest.mark.parametrize(
    ("jira_bases", "admitted_on"),
    [([], None), (["main", "next"], "next")],
    ids=["selected_binding_lists_no_bases", "selected_binding_allows_next"],
)
def test_base_policy_comes_from_the_selected_binding(
    make_intake: Callable[[dict[str, Any]], Intake],
    jira_bases: list[str],
    admitted_on: str | None,
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

    if admitted_on is None:
        assert _work_items(issue) == []
        [text] = _texts(intake, issue)
        assert "next" in text
    else:
        [item] = _work_items(issue)
        assert (item["base_branch"], item["base_source"]) == (admitted_on, "label")


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
    assert "Unauthorized" in logged

    intake.dc_down = False
    intake.poll()

    _assert_on_dc(dc_issue)
    _assert_admitted_once(intake, healthy, event)
    assert _cursor() is not None


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
