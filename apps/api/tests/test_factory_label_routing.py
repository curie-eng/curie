"""Two factory agents share one GitHub repository by trigger label (#4361 AC 4,
decisions 8 to 15, as amended by #4397 decision 10).

Agent ``full`` holds the repository's GitHub surface with no label, so it serves
the install's tracker label ``curie-factory``. Agent ``fast`` holds it with the
explicit label ``curie-factory-fast``. Every AC 4 item runs through both intake
doors: the signed ``POST /github/webhook`` and one locked ``poll_once`` pass.

GitHub is ``LabelGitHub``, ``test_factory_poll_intake.PollGitHub`` with each
issue carrying its own labels and its own ``labeled``/``unlabeled`` timeline.
It replays only the shapes the shared fakes already record: the issue, the
issue listing (honoring GitHub's ``labels`` filter), the issue events (the
``unlabeled`` event in ``forge_fakes.github.GitHubRepositoryFake.label_event``'s
shape), comments, pulls and reviews. Every listing's ETag is a digest of what
it returned, as GitHub's conditional requests behave, so a changed issue is
re-read on the next pass.

https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/events#list-issue-events
https://docs.github.com/en/webhooks/webhook-events-and-payloads#issues

Machine fixtures stand in for GitHub. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import itertools
import json
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_poll_intake import poll_once
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    INSTALLATION_ID,
    MENTION,
    REPO,
    REPO_ID,
    _comment_event,
    _issue_event,
    _post,
    factory_env,
    fake_installation_tokens,
)
from test_factory_poll_intake import (  # noqa: E402
    PollGitHub,
    _client_type,
    _iso,
    _public_review,
    _user,
)
from test_github_factory_ingress import _code, _rows  # noqa: E402
from test_github_factory_review import _complete, _execute, _own_pull_request  # noqa: E402

INSTALL = "curie-factory"
FAST = "curie-factory-fast"
INTAKES = ("webhook", "poll")
_COMMENT_IDS = itertools.count(763000)
_REVIEW_IDS = itertools.count(764000)


def _digest(items: Any) -> str:
    return '"' + hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest() + '"'


@dataclass
class LabelGitHub(PollGitHub):
    """``PollGitHub`` with per-issue labels and a per-issue label timeline."""

    labels_of: dict[int, list[str]] = field(default_factory=dict)
    timeline: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    _event_ids: Iterator[int] = field(default_factory=lambda: itertools.count(940000))

    # Seeding -------------------------------------------------------------------

    def open_issue(self, *labels: str, age: timedelta = timedelta(seconds=5)) -> int:
        number = self.plant_issue(age=age)
        self.labels_of[number] = []
        self.timeline[number] = []
        for name in labels:
            self.label(number, name, at=datetime.now(UTC) - age)
        return number

    def label(self, number: int, name: str, *, at: datetime | None = None) -> None:
        if name not in self.labels_of[number]:
            self.labels_of[number].append(name)
        self._record(number, "labeled", name, at)

    def unlabel(self, number: int, name: str) -> None:
        if name in self.labels_of[number]:
            self.labels_of[number].remove(name)
        self._record(number, "unlabeled", name, None)

    def _record(self, number: int, kind: str, name: str, at: datetime | None) -> None:
        self.timeline[number].append(
            {
                "id": next(self._event_ids),
                "event": kind,
                "label": {"name": name},
                "actor": _user(),
                "performed_via_github_app": None,
                "created_at": _iso(at or datetime.now(UTC)),
            }
        )

    def bot_comments(self, number: int) -> list[dict[str, Any]]:
        """The App's own comments on issue ``number``, oldest first."""

        return sorted(
            (
                comment
                for comment in self.comments.values()
                if str(comment.get("issue_url", "")).endswith(f"/issues/{number}")
                and comment.get("performed_via_github_app") is not None
            ),
            key=lambda comment: comment["id"],
        )

    def listings(self) -> list[httpx.Request]:
        return [
            request
            for request in self.calls
            if request.method == "GET" and request.url.path == f"/repos/{REPO}/issues"
        ]

    # Shapes ------------------------------------------------------------------

    def _issue_body(self, issue: Any) -> dict[str, Any]:
        names = list(self.labels_of.get(issue.number, [])) + list(issue.base_labels)
        return {
            "number": issue.number,
            "state": issue.state,
            "labels": [{"name": name} for name in names],
            "user": _user(),
        }

    def _events(self, issue: Any) -> list[dict[str, Any]]:
        events = list(self.timeline.get(issue.number, []))
        if issue.closed_event_id is not None:
            events.append(
                {
                    "id": issue.closed_event_id,
                    "event": "closed",
                    "actor": _user(),
                    "performed_via_github_app": None,
                    "created_at": _iso(datetime.now(UTC)),
                }
            )
        return events

    def _page(self, request: httpx.Request, family: str, items: list[Any]) -> httpx.Response:
        # GitHub's ETag changes with the representation; a fixed tag would
        # answer 304 for an issue whose labels just changed.
        self._etags[family] = _digest(items)
        return super()._page(request, family, items)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path == f"/repos/{REPO}/issues":
            self.calls.append(request)
            wanted = {name for name in request.url.params.get("labels", "").split(",") if name}
            listed = [
                self._issue_body(issue)
                for issue in self.issues.values()
                if issue.state == "open"
                and self.labels_of.get(issue.number)
                and wanted <= set(self.labels_of[issue.number])
            ]
            return self._page(request, "issues:" + ",".join(sorted(wanted)), listed)
        return super().handle(request)


# --- the install --------------------------------------------------------------


class LabelFactory:
    """One install on one database. ``reboot`` reapplies the forge config and
    starts a fresh app, as a rollout does."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, key_dir: Path, github: LabelGitHub):
        self.monkeypatch = monkeypatch
        self.key_dir = key_dir
        self.github = github
        self._stack: contextlib.ExitStack | None = None
        self.client: TestClient

    def configure(self, *, label: str = INSTALL, allowlist: str | None = None) -> None:
        env = factory_env(self.key_dir, label=label, poll_interval_s=30.0)
        if allowlist is not None:
            env["GITHUB_REPO_ALLOWLIST"] = allowlist
        for key, value in env.items():
            self.monkeypatch.setenv(key, value)
        get_settings.cache_clear()

    def boot(self) -> None:
        self.close()
        self._stack = contextlib.ExitStack()
        self.client = self._stack.enter_context(TestClient(create_app()))

    def reboot(self, **config: Any) -> None:
        self.configure(**config)
        self.boot()

    def close(self) -> None:
        if self._stack is not None:
            self._stack.close()
            self._stack = None

    # Agents and surfaces ---------------------------------------------------------

    def headers(self) -> dict[str, str]:
        return {"X-API-Key": get_settings().api_key}

    def agent(self, channel: dict[str, Any]) -> str:
        created = self.client.post(
            "/agents",
            headers=self.headers(),
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": "acme-corp/factory-home",
                "channel": channel,
            },
        )
        assert created.status_code == 201, created.text
        return str(created.json()["id"])

    def two_agents(self) -> tuple[str, str]:
        """(full, fast): one repository, the install label and the fast label."""

        full = self.agent({"kind": "github", "address": REPO})
        fast = self.agent({"kind": "github", "address": REPO, "trigger_label": FAST})
        return full, fast

    def agent_with_spare_binding(self, label: str | None = None) -> str:
        """An agent bound to Slack and to the repository, so its GitHub surface
        can be removed without removing its last binding."""

        agent_id = self.agent({"kind": "slack", "address": f"C0{uuid.uuid4().hex[:8].upper()}"})
        channel: dict[str, Any] = {"kind": "github", "address": REPO}
        if label is not None:
            channel["trigger_label"] = label
        added = self.client.post(
            f"/agents/{agent_id}/channels", headers=self.headers(), json=channel
        )
        assert added.status_code == 201, added.text
        return agent_id

    def unbind(self, agent_id: str) -> None:
        removed = self.client.delete(
            f"/agents/{agent_id}/channels",
            headers=self.headers(),
            params={"kind": "github", "address": REPO},
        )
        assert removed.status_code == 204, removed.text

    def move_off(self, agent_id: str) -> None:
        """Move the agent's GitHub surface off the repository, onto a Slack
        channel, so the agent keeps a binding but no surface on it."""

        moved = self.client.patch(
            f"/agents/{agent_id}/channels",
            headers=self.headers(),
            params={"kind": "github", "address": REPO},
            json={"kind": "slack", "address": f"C0{uuid.uuid4().hex[:8].upper()}"},
        )
        assert moved.status_code == 200, moved.text

    def change_owner_surface(self, agent_id: str, change: str) -> None:
        """Take the agent's surface off the repository: ``removed`` deletes it,
        ``moved`` moves it to another location, and ``relabelled`` removes it
        and binds the repository again under the fast label."""

        if change == "moved":
            self.move_off(agent_id)
            return
        self.unbind(agent_id)
        if change == "relabelled":
            added = self.client.post(
                f"/agents/{agent_id}/channels",
                headers=self.headers(),
                json={"kind": "github", "address": REPO, "trigger_label": FAST},
            )
            assert added.status_code == 201, added.text

    # Intake doors ---------------------------------------------------------------

    def poll(self) -> None:
        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            client = httpx.AsyncClient(transport=httpx.MockTransport(self.github.handle))
            try:
                await poll_once(sessions, get_settings(), client)
            finally:
                await client.aclose()
                await engine.dispose()

        asyncio.run(go())

    def labelled(self, intake: str, number: int, name: str) -> httpx.Response | None:
        self.github.label(number, name)
        if intake == "webhook":
            return _post(
                self.client, "issues", _issue_event("labeled", number, label={"name": name})
            )
        self.poll()
        return None

    def unlabelled(self, intake: str, number: int, name: str) -> httpx.Response | None:
        self.github.unlabel(number, name)
        if intake == "webhook":
            return _post(
                self.client, "issues", _issue_event("unlabeled", number, label={"name": name})
            )
        self.poll()
        return None

    def closed(self, intake: str, number: int) -> httpx.Response | None:
        self.github.close_issue(number)
        if intake == "webhook":
            payload = _issue_event("closed", number)
            payload["issue"]["state"] = "closed"
            return _post(self.client, "issues", payload)
        self.poll()
        return None

    def opened(self, intake: str, *labels: str) -> tuple[int, httpx.Response | None]:
        """A new issue carrying ``labels``, delivered through ``intake``.

        The webhook door delivers the last label's ``labeled`` event, the one
        GitHub sends once all of them are on the issue."""

        if intake == "poll":
            number = self.github.open_issue(*labels)
            self.poll()
            return number, None
        number = self.github.open_issue(*labels[:-1])
        return number, self.labelled(intake, number, labels[-1])


def label_factory(monkeypatch: pytest.MonkeyPatch, key_dir: Path) -> Iterator[LabelFactory]:
    """The install every multi-label suite boots; each module wraps it in its own
    ``factory`` fixture (with ``clean_db`` and ``valkey``)."""

    fake_installation_tokens(monkeypatch)
    github = LabelGitHub()
    monkeypatch.setattr(httpx, "AsyncClient", _client_type(github))
    install = LabelFactory(monkeypatch, key_dir, github)
    install.reboot()
    try:
        yield install
    finally:
        install.close()
        get_settings.cache_clear()


@pytest.fixture
def factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None, valkey: object
) -> Iterator[LabelFactory]:
    yield from label_factory(monkeypatch, tmp_path)


# --- observations --------------------------------------------------------------


def _work(number: int) -> list[dict[str, Any]]:
    """The issue's WorkItems with their requests, in sequence order."""

    return _rows(
        "SELECT w.id AS work_item_id, w.agent_id, w.cancelled_at, r.id, r.sequence, r.status, "
        "r.terminal_cause FROM curie.work_items w "
        "LEFT JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number "
        "ORDER BY r.sequence",
        {"scope": str(REPO_ID), "number": str(number)},
    )


def _owner(number: int) -> str | None:
    rows = _work(number)
    if not rows:
        return None
    assert len({row["work_item_id"] for row in rows}) == 1, rows
    return str(rows[0]["agent_id"])


def _cancelled(number: int) -> bool:
    rows = _work(number)
    assert rows, f"issue {number} has no WorkItem"
    return rows[0]["cancelled_at"] is not None


def _requests(number: int) -> int:
    return len([row for row in _work(number) if row["id"] is not None])


def _admitted(response: httpx.Response | None) -> None:
    if response is not None:
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text


def _refused(response: httpx.Response | None, code: str) -> None:
    if response is not None:
        assert response.status_code == 200, response.text
        assert _code(response) == code, response.text


def _names_both(body: str) -> bool:
    """``curie-factory`` is a prefix of ``curie-factory-fast``, so the install
    label counts only where it is not the start of the fast one."""

    return FAST in body and INSTALL in body.replace(FAST, "")


def _refusal_comments(github: LabelGitHub, number: int) -> list[dict[str, Any]]:
    return [comment for comment in github.bot_comments(number) if FAST in comment["body"]]


def _admit(factory: LabelFactory, intake: str, label: str) -> int:
    number, response = factory.opened(intake, label)
    _admitted(response)
    return number


# --- AC 4.1: each label admits on its own agent ---------------------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_each_label_admits_on_its_own_agent(
    factory: LabelFactory, intake: str
) -> None:
    full, fast = factory.two_agents()

    first = _admit(factory, intake, INSTALL)
    second = _admit(factory, intake, FAST)

    assert _owner(first) == full
    assert _owner(second) == fast
    assert not _cancelled(first)
    assert not _cancelled(second)


# --- AC 4.2: both labels at once ------------------------------------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_both_labels_at_once_is_a_label_conflict(
    factory: LabelFactory, intake: str
) -> None:
    factory.two_agents()

    number, response = factory.opened(intake, INSTALL, FAST)

    _refused(response, "label_conflict")
    assert _owner(number) is None
    (comment,) = _refusal_comments(factory.github, number)
    assert _names_both(comment["body"]), comment["body"]


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_label_conflict_comment_is_one_comment_across_passes(
    factory: LabelFactory, intake: str
) -> None:
    factory.two_agents()
    number, _response = factory.opened(intake, INSTALL, FAST)

    again = factory.labelled(intake, number, INSTALL)
    factory.poll()

    _refused(again, "label_conflict")
    assert len(_refusal_comments(factory.github, number)) == 1
    assert _owner(number) is None


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_label_conflict_does_not_stop_an_admitted_run(
    factory: LabelFactory, intake: str
) -> None:
    """A run already admitted keeps running when a second bound label arrives."""

    full, _fast = factory.two_agents()
    number = _admit(factory, intake, INSTALL)

    factory.labelled(intake, number, FAST)

    assert _owner(number) == full
    assert not _cancelled(number)


# --- AC 4.3: another agent's label on an issue with a WorkItem ---------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_another_agents_label_on_a_running_issue_is_refused(
    factory: LabelFactory, intake: str
) -> None:
    full, _fast = factory.two_agents()
    number = _admit(factory, intake, INSTALL)
    requests = _requests(number)

    refused = factory.labelled(intake, number, FAST)

    _refused(refused, "work_item_other_agent")
    assert _owner(number) == full
    assert _requests(number) == requests
    (comment,) = _refusal_comments(factory.github, number)
    assert _names_both(comment["body"]), comment["body"]


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_another_agents_label_after_cancellation_is_still_refused(
    factory: LabelFactory, intake: str
) -> None:
    """The WorkItem, its conversation and its PR lineage belong to one agent,
    cancelled or not: a new issue is needed to run it under the other label."""

    full, _fast = factory.two_agents()
    number = _admit(factory, intake, INSTALL)
    factory.unlabelled(intake, number, INSTALL)
    assert _cancelled(number)

    refused = factory.labelled(intake, number, FAST)

    _refused(refused, "work_item_other_agent")
    assert _owner(number) == full
    assert _cancelled(number)
    # Liveness: with the other label gone, the WorkItem's own label readmits
    # it on its own agent.
    factory.github.unlabel(number, FAST)
    _admitted(factory.labelled(intake, number, INSTALL))
    assert _owner(number) == full
    assert not _cancelled(number)


@pytest.mark.parametrize("intake", INTAKES)
@pytest.mark.parametrize("change", ["removed", "moved"])
def test_label_routing_another_agents_label_is_refused_after_the_owner_left(
    factory: LabelFactory, intake: str, change: str
) -> None:
    """R1 finding 6: the WorkItem belongs to its agent whether or not that
    agent still has a surface on the repository, so another agent's label is
    refused with the marked comment, never admitted and never left to a
    dispatch ``identity_mismatch``."""

    full = factory.agent_with_spare_binding()
    fast = factory.agent({"kind": "github", "address": REPO, "trigger_label": FAST})
    number = _admit(factory, intake, INSTALL)
    requests = _requests(number)
    factory.change_owner_surface(full, change)

    refused = factory.labelled(intake, number, FAST)

    _refused(refused, "work_item_other_agent")
    assert _owner(number) == full != fast
    assert _requests(number) == requests
    (comment,) = _refusal_comments(factory.github, number)
    assert _names_both(comment["body"]), comment["body"]
    # Liveness: the fast agent's label still admits a new issue.
    fresh, _response = factory.opened(intake, FAST)
    assert _owner(fresh) == fast


# --- AC 4.4: removing one agent's label ---------------------------------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_removing_one_agents_label_cancels_only_its_work_item(
    factory: LabelFactory, intake: str
) -> None:
    factory.two_agents()
    full_issue = _admit(factory, intake, INSTALL)
    fast_issue = _admit(factory, intake, FAST)

    removed = factory.unlabelled(intake, fast_issue, FAST)

    if removed is not None:
        assert removed.json()["status"] == "factory_cancelled", removed.text
    assert _cancelled(fast_issue)
    assert not _cancelled(full_issue)


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_removing_another_agents_label_does_not_cancel(
    factory: LabelFactory, intake: str
) -> None:
    """Decision 10: removing a bound label that is not the WorkItem's own does
    nothing to it."""

    factory.two_agents()
    number = _admit(factory, intake, INSTALL)
    factory.labelled(intake, number, FAST)

    removed = factory.unlabelled(intake, number, FAST)

    if removed is not None:
        assert removed.status_code == 200, removed.text
        assert _code(removed) == "label_not_admitting", removed.text
    assert not _cancelled(number)
    # Liveness: removing the WorkItem's own label cancels it.
    factory.unlabelled(intake, number, INSTALL)
    assert _cancelled(number)


# --- AC 4.5: closing ------------------------------------------------------------------


@pytest.mark.parametrize("intake", INTAKES)
@pytest.mark.parametrize("label", [INSTALL, FAST])
def test_label_routing_closing_an_issue_cancels_its_work_item(
    factory: LabelFactory, intake: str, label: str
) -> None:
    factory.two_agents()
    number = _admit(factory, intake, label)
    other = _admit(factory, intake, FAST if label == INSTALL else INSTALL)

    closed = factory.closed(intake, number)

    if closed is not None:
        assert closed.json()["status"] == "factory_cancelled", closed.text
    assert _cancelled(number)
    assert not _cancelled(other)


# --- AC 4.6: mentions -----------------------------------------------------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_a_mention_routes_to_the_work_items_agent(
    factory: LabelFactory, intake: str
) -> None:
    full, fast = factory.two_agents()
    issues = {full: _admit(factory, intake, INSTALL), fast: _admit(factory, intake, FAST)}

    for agent_id, number in issues.items():
        before = _requests(number)
        comment_id = next(_COMMENT_IDS)
        body = f"@{MENTION} please revise the helper"
        factory.github.add_issue_comment(number, comment_id, body)
        if intake == "webhook":
            response = _post(
                factory.client, "issue_comment", _comment_event(number, body, comment_id)
            )
            assert response.status_code == 200, response.text
        else:
            factory.poll()

        assert _requests(number) == before + 1, (agent_id, _work(number))
        assert _owner(number) == agent_id


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_a_mention_on_an_issue_without_a_work_item_is_ignored(
    factory: LabelFactory, intake: str
) -> None:
    factory.two_agents()
    number = factory.github.open_issue()
    comment_id = next(_COMMENT_IDS)
    body = f"@{MENTION} please start"
    factory.github.add_issue_comment(number, comment_id, body)

    if intake == "webhook":
        response = _post(factory.client, "issue_comment", _comment_event(number, body, comment_id))
        assert response.status_code == 200, response.text
    else:
        factory.poll()

    assert _owner(number) is None


# --- AC 4.7: review feedback --------------------------------------------------------


def _review_reaches(factory: LabelFactory, intake: str, number: int) -> None:
    (first, *_rest) = _work(number)
    pull = factory.github.plant_pull()
    _own_pull_request(first["work_item_id"], pull)
    _complete(factory.client, first["id"])
    before = _requests(number)
    review = factory.github.add_review(pull, next(_REVIEW_IDS), f"@{MENTION} please rename it")
    if intake == "webhook":
        payload = {
            "action": "submitted",
            "installation": {"id": INSTALLATION_ID},
            "repository": {"id": REPO_ID, "full_name": REPO},
            "sender": _user(),
            "pull_request": factory.github.pulls[pull],
            "review": _public_review(review),
        }
        response = _post(factory.client, "pull_request_review", payload)
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text
    else:
        factory.poll()
    assert _requests(number) == before + 1, _work(number)


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_review_feedback_on_each_agents_pr_reaches_that_agent(
    factory: LabelFactory, intake: str
) -> None:
    full, fast = factory.two_agents()
    issues = {full: _admit(factory, intake, INSTALL), fast: _admit(factory, intake, FAST)}

    for agent_id, number in issues.items():
        _review_reaches(factory, intake, number)
        assert _owner(number) == agent_id


# --- AC 4.8: the install label collides with an explicit label ------------------------


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_install_label_changed_to_collide_admits_nothing(
    factory: LabelFactory, intake: str
) -> None:
    """Decision 8: two bindings resolving to one label at run time admit nothing
    on the repository for that label, and the poller lists it once."""

    factory.two_agents()
    factory.reboot(label=FAST)
    factory.github.calls.clear()

    number, response = factory.opened(intake, FAST)

    _refused(response, "binding_missing")
    assert _owner(number) is None
    if intake == "poll":
        collided = [
            request
            for request in factory.github.listings()
            if request.url.params.get("labels") == FAST
        ]
        assert len(collided) == 1, factory.github.listings()


def test_label_routing_collision_recovers_after_config_restored(factory: LabelFactory) -> None:
    """R1 finding 15: once the tracker label is restored, both labels admit
    again on the next poll pass."""

    full, fast = factory.two_agents()
    factory.reboot(label=FAST)
    stranded, _response = factory.opened("poll", FAST)
    assert _owner(stranded) is None

    factory.reboot(label=INSTALL)
    fresh_full = factory.github.open_issue(INSTALL)
    fresh_fast = factory.github.open_issue(FAST)
    factory.poll()

    assert _owner(fresh_full) == full
    assert _owner(fresh_fast) == fast


# --- recovery and cancellation semantics ----------------------------------------------


def test_label_routing_conflict_then_one_label_removed_admits_on_next_poll(
    factory: LabelFactory,
) -> None:
    """R1 finding 15: an issue refused ``label_conflict`` is admitted on the
    next pass after one of its labels is removed, and the refusal comment is
    updated rather than left standing."""

    full, _fast = factory.two_agents()
    number, _none = factory.opened("poll", INSTALL, FAST)
    (refusal,) = _refusal_comments(factory.github, number)
    refused_body = refusal["body"]
    assert _owner(number) is None

    factory.github.unlabel(number, FAST)
    factory.poll()

    assert _owner(number) == full
    assert not _cancelled(number)
    edited = factory.github.comments[refusal["id"]]
    assert edited["body"] != refused_body, edited["body"]


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_unlabel_after_binding_removed_still_cancels(
    factory: LabelFactory, intake: str
) -> None:
    """R1 finding 9, R2 finding 3: cancellation never resolves a binding. With
    the last binding on the repository gone, the poller still walks the
    repository for its open WorkItems and the webhook still cancels."""

    agent_id = factory.agent_with_spare_binding()
    number = _admit(factory, intake, INSTALL)
    factory.unbind(agent_id)
    factory.github.calls.clear()

    removed = factory.unlabelled(intake, number, INSTALL)

    if removed is not None:
        assert removed.json()["status"] == "factory_cancelled", removed.text
    else:
        # Cancellation only: nothing serves the repository, so it is not listed.
        assert factory.github.listings() == []
    assert _cancelled(number)
    assert _owner(number) == agent_id


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_tracker_label_changed_then_old_label_removed_cancels(
    factory: LabelFactory, intake: str
) -> None:
    """R2 finding 5: a WorkItem admitted under the tracker label ``curie-factory``
    is cancelled by removing ``curie-factory`` after the tracker label became
    ``curie-factory-v2``, because the stored label is what admitted it."""

    agent_id = factory.agent({"kind": "github", "address": REPO})
    number = _admit(factory, intake, INSTALL)
    factory.reboot(label="curie-factory-v2")

    removed = factory.unlabelled(intake, number, INSTALL)

    if removed is not None:
        assert removed.json()["status"] == "factory_cancelled", removed.text
    assert _cancelled(number)
    assert _owner(number) == agent_id


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_readmission_overwrites_stored_label(
    factory: LabelFactory, intake: str
) -> None:
    """Readmission under a new label records that label: afterwards only the
    new label cancels."""

    agent_id = factory.agent({"kind": "github", "address": REPO})
    number = _admit(factory, intake, INSTALL)
    factory.reboot(label="curie-factory-v2")
    factory.unlabelled(intake, number, INSTALL)
    assert _cancelled(number)

    readmitted = factory.labelled(intake, number, "curie-factory-v2")

    _admitted(readmitted)
    assert not _cancelled(number)
    assert _owner(number) == agent_id
    # The old label coming and going no longer cancels the readmitted run.
    factory.labelled(intake, number, INSTALL)
    stale = factory.unlabelled(intake, number, INSTALL)
    if stale is not None:
        assert _code(stale) == "label_not_admitting", stale.text
    assert not _cancelled(number)
    # Liveness: the label that readmitted it does.
    factory.unlabelled(intake, number, "curie-factory-v2")
    assert _cancelled(number)


def _forget_stored_label(number: int) -> None:
    """A WorkItem admitted before the migration has no stored label."""

    _execute(
        "UPDATE curie.work_items SET trigger_label = NULL "
        "WHERE tracker_scope_id = :scope AND tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )


@pytest.mark.parametrize("intake", INTAKES)
@pytest.mark.parametrize("change", ["removed", "moved", "relabelled"])
def test_label_routing_null_row_cancels_under_the_tracker_label_whatever_the_binding(
    factory: LabelFactory, intake: str, change: str
) -> None:
    """R1 finding 5: a WorkItem with no stored label predates the column, when
    the tracker label was the only install label, so removing that label
    cancels it however its agent's surface changed since. ``relabelled`` binds
    the repository again under the fast label, which a binding lookup would
    have compared with instead."""

    agent_id = factory.agent_with_spare_binding()
    number = _admit(factory, intake, INSTALL)
    _forget_stored_label(number)
    factory.change_owner_surface(agent_id, change)

    removed = factory.unlabelled(intake, number, INSTALL)

    if removed is not None:
        assert removed.json()["status"] == "factory_cancelled", removed.text
    assert _cancelled(number)
    assert _owner(number) == agent_id


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_null_row_is_not_cancelled_by_the_bindings_label(
    factory: LabelFactory, intake: str
) -> None:
    """The negative twin: with its agent's surface rebound under the fast
    label, removing the fast label from a NULL row's issue does not cancel it."""

    agent_id = factory.agent_with_spare_binding()
    number = _admit(factory, intake, INSTALL)
    _forget_stored_label(number)
    factory.change_owner_surface(agent_id, "relabelled")
    factory.github.label(number, FAST)

    ignored = factory.unlabelled(intake, number, FAST)

    if ignored is not None:
        assert _code(ignored) == "label_not_admitting", ignored.text
    assert not _cancelled(number)
    factory.unlabelled(intake, number, INSTALL)
    assert _cancelled(number)


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_null_row_without_binding_ignores_a_label_that_is_not_the_trackers(
    factory: LabelFactory, intake: str
) -> None:
    """The negative twin: with no binding left, a pre-migration WorkItem that an
    explicit label admitted is not cancelled by removing that label, because
    the tracker label is the only reference left. Closing still cancels."""

    agent_id = factory.agent_with_spare_binding(FAST)
    number = _admit(factory, intake, FAST)
    _forget_stored_label(number)
    factory.unbind(agent_id)

    ignored = factory.unlabelled(intake, number, FAST)

    if ignored is not None:
        assert _code(ignored) == "label_not_admitting", ignored.text
    assert not _cancelled(number)
    factory.closed(intake, number)
    assert _cancelled(number)


@pytest.mark.parametrize("intake", INTAKES)
def test_label_routing_a_label_no_binding_owns_stays_unrelated(
    factory: LabelFactory, intake: str
) -> None:
    """Edge case 5's last line: a ``labeled`` event for an unbound label admits
    nothing; the liveness neighbour is any admission above."""

    factory.two_agents()

    number, response = factory.opened(intake, "documentation")

    _refused(response, "unrelated_label")
    assert _owner(number) is None
