"""#4629: a reinstall with empty factory storage must not republish a published issue.

Released v0.13.0 rediscovered every still-labelled issue after a reinstall or a
lost database and opened a second pull or merge request beside the first. These
tests reproduce that from the outside and check the repair by its external
effects only: pull and merge requests on the forge, issue comments, database
rows, and the Valkey runs stream. No symbol the repair introduces is imported.

What runs is the product: the API app (`curie_api.main.create_app`) on a fresh
Postgres database migrated to head, the real Valkey, the polling entry points
(`factory_poll_intake.poll_once`, `factory_tracker_intake.poll_trackers`), the
work item reconciler that publishes execute wakes, the internal worker routes a
worker uses to acquire and start a request and to create its publication, the
agent's automatic publication policy, and the worker's real
`publication_loop.PublicationReconciler` with its real API clients
(`publication_clients`) against the in-process API. Exactly two things are not
the product:

1. External HTTP (GitHub, GitLab, Jira): in-process fixtures with
   provider-documented shapes (`c4_reinstall_forges.py`, `forge_fakes`).
2. The worker's Kubernetes Job seam (`PublicationCluster`): `JobCluster`
   performs the Job's push on the fake forge and reports its
   ``CURIE_COMMIT_SHA=`` log marker. The model turn is stood in for by creating
   the publication through the internal route the sandbox uses.

Each test provisions its own database on the ``DATABASE_URL`` server and drops
it afterwards, uses a runs stream of its own on the ``TEST_VALKEY_*`` Valkey and
deletes the keys it owns. Missing Postgres or Valkey is a fixture error.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import secrets
import sys
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
import redis.asyncio as aioredis
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "apps/api/tests"))
sys.path.insert(0, str(HERE))

from _migration_support import admin_execute, alembic_config, render_url  # noqa: E402
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn  # noqa: E402
from alembic import command  # noqa: E402
from c4_reinstall_forges import (  # noqa: E402
    GITHUB_API,
    GITHUB_HOST,
    HUMAN,
    REPO,
    REPO_ID,
    C4GitLab,
    C4Jira,
    GitHubFake,
    GitLabHost,
    JobCluster,
    Router,
    SharedClock,
    commit_message,
    generate_app_key,
)
from c4_reinstall_forges import (  # noqa: E402
    INSTALLATION_ID as GITHUB_INSTALLATION_ID,
)
from channel_protocol.work_item_events import parse_work_item_event_id  # noqa: E402
from curie_api import (  # noqa: E402
    factory_label_reconcile,
    factory_poll_intake,
    factory_tracker_intake,
)
from curie_api.config import get_settings  # noqa: E402
from curie_api.github_app import _RESOLVERS  # noqa: E402
from curie_api.main import create_app  # noqa: E402
from curie_api.workitem_reconciler import WorkItemReconciler  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from forge_fakes import gitlab as gitlab_fake  # noqa: E402
from forge_fakes import jira as jira_fake  # noqa: E402
from forge_fakes.github import LABEL as GITHUB_LABEL  # noqa: E402
from forge_fakes.github import _post as post_webhook  # noqa: E402
from forge_fakes.github import (  # noqa: E402
    forges_document,
    forges_env_json,
    github_repository_binding,
)

LABEL = "factory"
assert GITHUB_LABEL == LABEL == gitlab_fake.LABEL == jira_fake.LABEL
STATE_LABEL = "curie-factory:pr-open"
WORKER_TOKEN = "c4-reinstall-worker-token"
WORKER = {"X-Curie-Worker-Token": WORKER_TOKEN}
API_BASE = "http://api.example.test"
DEVELOPER = gitlab_fake.Actor("31", "dev-dana")
JIRA_STARTER = "5b10a0000000000000000a11"
SECOND_PROJECT_ID = 4343
SECOND_PROJECT_PATH = "acme/platform/gadgets"
PATCH_B64 = base64.b64encode(b"diff --git a/README.md b/README.md\n").decode()
BINDINGS = ("github-github", "gitlab-gitlab", "jira-gitlab")
PROSE = "Parsing a three-field line now yields three fields."
# The one wall clock a Jira site and its GitLab project share: GitLab's fixture epoch.
SHARED_EPOCH = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)

_REAL_ASYNC_CLIENT = httpx.AsyncClient
_REAL_CLIENT = httpx.Client


class InfrastructureError(RuntimeError):
    """Postgres or Valkey is missing: a fixture error, never a skip."""


def _publication_branch() -> str:
    return f"curie/publication-{uuid.uuid4().hex}"


# --- infrastructure -----------------------------------------------------------------


def _valkey_settings() -> tuple[str, int, str | None]:
    host = os.environ.get("TEST_VALKEY_HOST", "localhost")
    port = int(os.environ.get("TEST_VALKEY_PORT", "26379"))
    password = os.environ.get("TEST_VALKEY_PW", "valkeypass") or None
    return host, port, password


def _valkey() -> redis.Redis:
    host, port, password = _valkey_settings()
    client = redis.Redis(host=host, port=port, password=password, socket_connect_timeout=5)
    try:
        client.ping()
    except redis.RedisError as exc:
        client.close()
        raise InfrastructureError(f"Valkey at {host}:{port} is required: {exc}") from exc
    return client


@pytest.fixture
def fresh_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """A database of this test's own on the ``DATABASE_URL`` server, migrated to head."""

    base = make_url(os.environ.get("DATABASE_URL") or get_settings().database_url)
    name = f"curie_test_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(3)}"
    try:
        asyncio.run(admin_execute(base, f'CREATE DATABASE "{name}"'))
    except Exception as exc:
        raise InfrastructureError(
            f"Postgres at {base.host}:{base.port} is required: {exc}"
        ) from exc
    monkeypatch.setenv("DATABASE_URL", render_url(base.set(database=name)))
    get_settings.cache_clear()
    try:
        command.upgrade(alembic_config(), "head")
        yield name
    finally:
        get_settings.cache_clear()
        asyncio.run(admin_execute(base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def go() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings()]
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _count(table: str) -> int:
    return int(_rows(f"SELECT count(*) AS n FROM curie.{table}")[0]["n"])


# --- scenarios: seeding and observing one forge pairing -------------------------------


class Scenario:
    """Seeds issues and publications as v0.13.0 left them, and observes the forge."""

    def __init__(self, install: Install) -> None:
        self.install = install

    def issue(self, *, state_label: bool = False) -> Any:
        raise NotImplementedError

    def closing_reference(self, ref: Any) -> str:
        raise NotImplementedError

    def legacy_body(self, ref: Any) -> str:
        """The v0.13.0 body: prose, then the tracker's closing reference, no marker."""

        return f"{PROSE}\n\n{self.closing_reference(ref)}"

    def factory_pr(
        self, ref: Any, *, state: str = "open", human: bool = False, body: str | None = None
    ) -> int:
        raise NotImplementedError

    def relabel(self, ref: Any) -> None:
        raise NotImplementedError

    def open_prs(self) -> dict[int, tuple[int, str]]:
        raise NotImplementedError

    def all_prs(self) -> dict[int, str]:
        """Every pull or merge request: number -> body."""
        raise NotImplementedError

    def pr_url(self, number: int) -> str:
        raise NotImplementedError

    def bot_comments(self, ref: Any) -> list[str]:
        raise NotImplementedError

    def listing_fault(self, kind: str) -> None:
        raise NotImplementedError

    def clear_faults(self) -> None:
        raise NotImplementedError

    def repository_listings(self) -> int:
        raise NotImplementedError

    def cursor(self) -> str | None:
        raise NotImplementedError

    def default_head(self, repo: str) -> str:
        raise NotImplementedError

    def push(self, env: dict[str, str]) -> str:
        raise NotImplementedError

    def open_existing(self, env: dict[str, str], sha: str, title: str, body: str) -> None:
        raise NotImplementedError

    def prs_naming(self, ref: Any) -> list[int]:
        reference = self.closing_reference(ref)
        return [n for n, body in self.all_prs().items() if reference in body.splitlines()]


class GitHubScenario(Scenario):
    def __init__(self, install: Install, fake: GitHubFake) -> None:
        super().__init__(install)
        self.fake = fake

    def issue(self, *, state_label: bool = False, labeled_at: datetime | None = None) -> int:
        return self.fake.add_issue(
            "Widget parser drops the last field",
            labels=(LABEL,),
            labeled_at=labeled_at or datetime.now(UTC) - timedelta(days=2),
            extra_labels=(STATE_LABEL,) if state_label else (),
        )

    def closing_reference(self, ref: int) -> str:
        return f"Closes #{ref}"

    def factory_pr(
        self, ref: int, *, state: str = "open", human: bool = False, body: str | None = None
    ) -> int:
        created = datetime.now(UTC) - timedelta(days=1)
        return self.fake.add_pull(
            body=self.legacy_body(ref) if body is None else body,
            author=dict(HUMAN) if human else self.fake.bot_user(),
            head=_publication_branch(),
            created_at=created,
            state="open" if state == "open" else "closed",
            merged=state == "merged",
            closed_at=created + timedelta(hours=2),
        )

    def relabel(self, ref: int) -> None:
        now = datetime.now(UTC) - timedelta(minutes=10)
        self.fake.label(ref, LABEL, dict(HUMAN), at=now, add=False)
        self.fake.label(ref, LABEL, dict(HUMAN), at=now + timedelta(minutes=1))

    def open_prs(self) -> dict[int, tuple[int, str]]:
        return self.fake.open_pulls()

    def all_prs(self) -> dict[int, str]:
        return {n: p.body for n, p in self.fake.pulls.items()}

    def pr_url(self, number: int) -> str:
        return self.fake.pull_url(number)

    def bot_comments(self, ref: int) -> list[str]:
        return self.fake.bot_comments(ref)

    def listing_fault(self, kind: str) -> None:
        if kind == "server_error":
            self.fake.listing_status = 503
        elif kind == "later_page":
            # More than one page at GitHub's 100 per page maximum; the factory's
            # pull request, the oldest, sits on the page that fails.
            for index in range(100):
                self.fake.add_pull(
                    body=f"Unrelated change {index}",
                    author=dict(HUMAN),
                    head=f"feature/unrelated-{index}",
                    created_at=datetime.now(UTC) - timedelta(hours=1),
                    state="closed",
                )
            self.fake.listing_fail_page = 2
        elif kind == "malformed_author":
            self.fake.malformed_author.update(self.fake.open_pulls())
        elif kind == "not_found":
            self.fake.listing_status = 404
        elif kind in ("body_missing", "body_not_text"):
            fault = kind.removeprefix("body_")
            self.fake.body_fault.update(dict.fromkeys(self.fake.open_pulls(), fault))
        else:
            raise ValueError(kind)

    def clear_faults(self) -> None:
        self.fake.listing_status = None
        self.fake.listing_fail_page = None
        self.fake.malformed_author.clear()
        self.fake.body_fault.clear()

    def repository_listings(self) -> int:
        return len(self.fake.repository_listings())

    def labelled_event(self, ref: int) -> dict[str, Any]:
        """The ``issues`` ``labeled`` delivery GitHub sends for the latest add of
        the trigger label (https://docs.github.com/en/webhooks/webhook-events-and-payloads#issues)."""

        issue = self.fake.issues[ref]
        event = next(
            e
            for e in reversed(issue.events)
            if e["event"] == "labeled" and e["label"]["name"] == LABEL
        )
        return {
            "action": "labeled",
            "installation": {"id": GITHUB_INSTALLATION_ID},
            "repository": {"id": REPO_ID, "full_name": REPO},
            "sender": dict(event["actor"]),
            "label": dict(event["label"]),
            "issue": {
                "number": ref,
                "state": issue.state,
                "title": issue.title,
                "body": "The widget parser drops the last field of a line.",
                "labels": [{"name": name} for name in issue.labels],
            },
        }

    def cursor(self) -> str | None:
        rows = _rows(
            "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'github' "
            "AND tracker_scope_id = :scope",
            {"scope": str(REPO_ID)},
        )
        return None if not rows else (rows[0]["etags"] or {}).get(f"issues:{LABEL}")

    def default_head(self, repo: str) -> str:
        assert repo == REPO
        return self.fake.branches["main"]

    def push(self, env: dict[str, str]) -> str:
        assert env["REPO_FULL_NAME"] == REPO
        return self.fake.push(env["BRANCH"], commit_message(env), parent=env["BASE_SHA"])

    def open_existing(self, env: dict[str, str], sha: str, title: str, body: str) -> None:
        self.fake.add_pull(
            body=body,
            author=self.fake.bot_user(),
            head=env["BRANCH"],
            created_at=datetime.now(UTC),
            title=title,
        )


class GitLabScenario(Scenario):
    def __init__(self, install: Install, fake: C4GitLab, *projects: C4GitLab) -> None:
        super().__init__(install)
        self.fake = fake
        self.projects = projects or (fake,)

    def issue(self, *, state_label: bool = False) -> int:
        iid = self.fake.add_issue("Widget parser drops the last field", author=DEVELOPER)
        self.fake.label(iid, LABEL, DEVELOPER)
        if state_label:
            self.fake.label(iid, STATE_LABEL, gitlab_fake.BOT)
        return iid

    def closing_reference(self, ref: int) -> str:
        return f"Closes #{ref}"

    def factory_pr(
        self, ref: Any, *, state: str = "open", human: bool = False, body: str | None = None
    ) -> int:
        iid = self.fake.add_merge_request(
            _publication_branch(),
            title="Fix the widget parser",
            description=self.legacy_body(ref) if body is None else body,
            author=DEVELOPER if human else gitlab_fake.BOT,
        )
        if state != "open":
            self.fake.set_mr_state(iid, state)
        return iid

    def relabel(self, ref: int) -> None:
        self.fake.label(ref, LABEL, DEVELOPER, add=False)
        self.fake.label(ref, LABEL, DEVELOPER)

    def open_prs(self) -> dict[int, tuple[int, str]]:
        return self.fake.open_mrs()

    def all_prs(self) -> dict[int, str]:
        return {iid: mr.description for iid, mr in self.fake.merge_requests.items()}

    def pr_url(self, number: int) -> str:
        return self.fake.mr_url(number)

    def bot_comments(self, ref: int) -> list[str]:
        return self.fake.bot_notes(ref)

    def listing_fault(self, kind: str) -> None:
        if kind == "server_error":
            self.fake.listing_status = 503
        elif kind == "later_page":
            # Two more factory merge requests (other work) make the listing span
            # pages of two; the published one, the oldest, is on the failing page.
            for _ in range(2):
                self.factory_pr(self.unlabelled_issue(), state="merged")
            self.fake.listing_page_size = 2
            self.fake.listing_fail_page = 2
        elif kind == "malformed_author":
            self.fake.malformed_author.update(self.fake.open_mrs())
        elif kind == "not_found":
            self.fake.listing_status = 404
        elif kind in ("body_missing", "body_not_text"):
            fault = kind.removeprefix("body_")
            self.fake.body_fault.update(dict.fromkeys(self.fake.open_mrs(), fault))
        else:
            raise ValueError(kind)

    def unlabelled_issue(self) -> Any:
        """Other work: an issue the factory does not start."""

        return self.fake.add_issue("Other work", author=DEVELOPER)

    def tracker_name(self) -> str:
        return gitlab_fake.TRACKER_NAME

    def clear_faults(self) -> None:
        self.fake.listing_status = None
        self.fake.listing_fail_page = None
        self.fake.malformed_author.clear()
        self.fake.body_fault.clear()

    def repository_listings(self) -> int:
        return sum(len(p.repository_listings()) for p in self.projects)

    def cursor(self) -> str | None:
        rows = _rows(
            "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'gitlab' "
            "AND tracker_scope_id = :scope",
            {"scope": str(self.fake.project_id)},
        )
        return None if not rows else (rows[0]["etags"] or {}).get(f"marked:{LABEL}")

    def project(self, path: str) -> C4GitLab:
        (found,) = [p for p in self.projects if p.project_path == path]
        return found

    def default_head(self, repo: str) -> str:
        return self.project(repo).branches[gitlab_fake.DEFAULT_BRANCH]

    def push(self, env: dict[str, str]) -> str:
        fake = self.project(env["REPO_FULL_NAME"])
        sha = fake.add_commit((env["BASE_SHA"],), commit_message(env))
        fake.branches[env["BRANCH"]] = sha
        return sha

    def open_existing(self, env: dict[str, str], sha: str, title: str, body: str) -> None:
        fake = self.project(env["REPO_FULL_NAME"])
        fake.add_merge_request(env["BRANCH"], title=title, description=body, author=gitlab_fake.BOT)


class JiraScenario(GitLabScenario):
    """A Jira site's issues published to the GitLab project."""

    def __init__(self, install: Install, jira: jira_fake.JiraFake, fake: C4GitLab) -> None:
        super().__init__(install, fake)
        self.jira = jira
        self.starter = jira.add_user(JIRA_STARTER, "Ada Starter")

    def issue(self, *, state_label: bool = False) -> jira_fake.JiraIssue:
        issue = self.jira.seed_issue("Widget parser drops the last field")
        self.jira.add_label(issue.id, LABEL, self.starter)
        if state_label:
            self.jira.add_label(issue.id, STATE_LABEL, self.jira.bot)
        return issue

    def closing_reference(self, ref: jira_fake.JiraIssue) -> str:
        return f"Jira: {ref.key} {jira_fake.SITE}/browse/{ref.key}"

    def relabel(self, ref: jira_fake.JiraIssue) -> None:
        self.jira.remove_label(ref.id, LABEL, self.starter)
        self.jira.add_label(ref.id, LABEL, self.starter)

    def bot_comments(self, ref: jira_fake.JiraIssue) -> list[str]:
        return [json.dumps(c.body) for c in self.jira.own_comments(ref.id)]

    def unlabelled_issue(self) -> jira_fake.JiraIssue:
        return self.jira.seed_issue("Other work")

    def tracker_name(self) -> str:
        return jira_fake.TRACKER_NAME

    def cursor(self) -> str | None:
        rows = _rows(
            "SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'jira_cloud' "
            "AND tracker_scope_id = :scope",
            {"scope": jira_fake.CLOUD_ID},
        )
        return None if not rows else (rows[0]["etags"] or {}).get(f"marked:{LABEL}")


# --- the install ----------------------------------------------------------------------


@dataclass
class Executed:
    request_id: uuid.UUID
    publication_id: str
    status: str
    error: str | None


@dataclass
class Install:
    binding: str
    client: TestClient
    router: Router
    scenario: Scenario
    stream: str
    deployments: dict[tuple[str, str], str] = field(default_factory=dict)
    executed: set[str] = field(default_factory=set)
    cluster: JobCluster | None = None

    # The product's passes ---------------------------------------------------------

    def poll(self) -> None:
        """One completed polling pass through the real entry points.

        GitHub polling (`factory_poll_intake.poll_once`) and every other
        tracker's port-driven pass (`factory_tracker_intake.poll_trackers`) with
        no due times recorded, so both the poll and the reconciliation sweep run.
        """

        async def go() -> None:
            settings = get_settings()
            maker = self.client.app.state.sessionmaker
            async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
                await factory_poll_intake.poll_once(maker, settings, client)
                await factory_tracker_intake.poll_trackers(
                    maker, settings, client, due={}, clock=asyncio.get_running_loop().time()
                )

        self.client.portal.call(go)

    def sweep_only(self) -> None:
        """One `factory_tracker_intake.poll_trackers` pass in which the tracker's
        poll is not due (it ran an interval ago) and every reconciliation sweep
        is, the pass a running API makes between two polls."""

        scenario = self.scenario
        assert isinstance(scenario, GitLabScenario)

        async def go() -> None:
            settings = get_settings()
            maker = self.client.app.state.sessionmaker
            clock = asyncio.get_running_loop().time()
            due = {f"poll:{scenario.tracker_name()}": clock + 3600.0}
            async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
                await factory_tracker_intake.poll_trackers(
                    maker, settings, client, due=due, clock=clock
                )

        self.client.portal.call(go)

    def reconcile_missed_labels(self) -> int:
        """One `factory_label_reconcile.reconcile_missed_labels` pass, the
        webhook install's backstop for a label delivery that never arrived."""

        async def go() -> int:
            settings = get_settings()
            maker = self.client.app.state.sessionmaker
            async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
                return await factory_label_reconcile.reconcile_missed_labels(
                    maker, settings, client, now=datetime.now(UTC)
                )

        return int(self.client.portal.call(go))

    def deliver_label(self, ref: int) -> httpx.Response:
        """GitHub's signed ``issues`` ``labeled`` delivery for ``ref`` to
        ``POST /github/webhook``."""

        scenario = self.scenario
        assert isinstance(scenario, GitHubScenario)
        return post_webhook(self.client, "issues", scenario.labelled_event(ref))

    def dispatch(self, caplog: pytest.LogCaptureFixture) -> None:
        """One `WorkItemReconciler.run_once` pass: it polls once more, then
        publishes every due execute wake onto the runs stream. A failed step is
        a harness error, never a silent pass."""

        async def go() -> None:
            host, port, password = _valkey_settings()
            valkey = aioredis.Redis(host=host, port=port, password=password)
            try:
                await WorkItemReconciler(
                    self.client.app.state.sessionmaker, valkey, get_settings()
                ).run_once()
            finally:
                await valkey.aclose()

        caplog.clear()
        with caplog.at_level(logging.ERROR, logger="curie_api.workitem_reconciler"):
            self.client.portal.call(go)
        failed = [
            r.getMessage()
            for r in caplog.records
            if r.name == "curie_api.workitem_reconciler" and r.levelno >= logging.ERROR
        ]
        assert not failed, f"the work item reconciler pass failed: {failed}"

    def stream_entries(self) -> list[QueuedTurn]:
        client = _valkey()
        try:
            return [
                QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD.encode()])
                for _id, fields in client.xrange(self.stream)
            ]
        finally:
            client.close()

    def execute(
        self,
        *,
        before_reconcile: Callable[[str], None] | None = None,
    ) -> list[Executed]:
        """Run every execute wake on the runs stream as a worker would.

        Acquire and start through the internal routes, select the workspace,
        stand in for the model turn by creating the publication through the
        sandbox's internal route (approved by the agent's automatic policy),
        then reconcile it with the real worker `PublicationReconciler`.
        """

        turns = [
            turn
            for turn in self.stream_entries()
            if turn.event_id not in self.executed
            and (parsed := parse_work_item_event_id(turn.event_id)) is not None
            and parsed.kind == "execute"
        ]
        results: list[Executed] = []
        for turn in turns:
            self.executed.add(turn.event_id)
            parsed = parse_work_item_event_id(turn.event_id)
            assert parsed is not None and parsed.number is not None
            publication_id = self.client.portal.call(self._run_turn, turn, parsed)
            if before_reconcile is not None:
                before_reconcile(publication_id)
            self.client.portal.call(self._reconcile)
            row = _rows(
                "SELECT status, error FROM curie.publications WHERE id = :id",
                {"id": uuid.UUID(publication_id)},
            )[0]
            results.append(
                Executed(parsed.request_id, publication_id, str(row["status"]), row["error"])
            )
        return results

    def _api(self) -> httpx.AsyncClient:
        return _REAL_ASYNC_CLIENT(
            transport=httpx.ASGITransport(app=self.client.app), base_url=API_BASE, timeout=60
        )

    async def _run_turn(self, turn: QueuedTurn, parsed: Any) -> str:
        request_id, generation = parsed.request_id, parsed.number
        owner = f"c4-worker-{uuid.uuid4().hex[:8]}"
        handle = turn.reply_handle
        assert handle is not None
        async with self._api() as http:
            acquired = await http.post(
                f"/v1/internal/work-items/requests/{request_id}/acquire",
                headers=WORKER,
                json={
                    "owner": owner,
                    "generation": generation,
                    "wake": {
                        "kind": handle.kind,
                        "address": handle.channel,
                        "adapter": handle.adapter,
                        "conversation_id": turn.conversation_id,
                    },
                    "context": None,
                },
            )
            assert acquired.status_code == 200, acquired.text
            work = acquired.json()
            started = await http.post(
                f"/v1/internal/work-items/requests/{request_id}/start",
                headers=WORKER,
                json={
                    "owner": owner,
                    "generation": generation,
                    "claim_name": f"curie-thread-{request_id.hex[:12]}",
                    "sandbox_name": f"sbx-{request_id.hex[:12]}",
                    "context": work["context"],
                },
            )
            assert started.status_code == 200, started.text
            epoch = started.json()["runtime_epoch"]
            deployment_id = self.deployments[(handle.kind, handle.channel)]
            selected = await http.post(
                f"/v1/internal/workspaces/{deployment_id}/selection",
                headers=WORKER,
                json={
                    "conversation_id": work["conversation_id"],
                    "author": turn.author,
                    "repo_full_name": work["repository_path"],
                },
            )
            assert selected.status_code == 200, selected.text
            payload: dict[str, Any] = {
                "deployment_id": deployment_id,
                "conversation_id": work["conversation_id"],
                "reply_conversation_id": turn.conversation_id,
                "repo_full_name": work["repository_path"],
                "author": turn.author,
                "summary": "Publish the widget parser fix",
                "reply_kind": handle.kind,
                "reply_channel": handle.channel,
                "dedupe_key": f"c4-publication-{request_id}",
                "base_sha": self.scenario.default_head(work["repository_path"]),
                "work_item_request_id": str(request_id),
                "work_item_runtime_epoch": epoch,
                "patch_b64": PATCH_B64,
                "changed_paths": ["README.md"],
                "expires_in_seconds": 600,
                "title": "Fix the widget parser",
                "body": PROSE,
            }
            if handle.endpoint is not None:
                payload["reply_endpoint"] = handle.endpoint
            if handle.adapter is not None:
                payload["reply_adapter"] = handle.adapter
            created = await http.post("/v1/internal/publications", headers=WORKER, json=payload)
            assert created.status_code in (200, 201), created.text
            publication = created.json()
            assert publication["status"] == "approved", publication
            return str(publication["id"])

    async def _reconcile(self) -> None:
        from curie_worker.publication_clients import (
            PublicationCodeHostClient,
            PublicationCredentialClient,
            PublicationLineageClient,
            PublicationTranscriptClient,
        )
        from curie_worker.publication_k8s import PublicationJobSettings
        from curie_worker.publication_loop import PublicationReconciler
        from curie_worker.publication_store import PostgresPublicationStore
        from curie_worker.reply_sink import (
            TRACKER_KINDS,
            HttpReplyAdapter,
            ReplySinkRouter,
            TrackerReplySink,
        )

        settings = PublicationJobSettings(
            namespace="curie",
            runner_image="ghcr.io/curie-eng/curie-runner:test",
            image_pull_policy="IfNotPresent",
            image_pull_secrets=(),
            priority_class_name="curie-publication",
            service_account_name="curie-publication",
            owner_name="curie-publication",
            git_user_name="Curie Publisher",
            git_user_email="publisher@example.test",
            cpu_request="100m",
            cpu_limit="1",
            memory_request="256Mi",
            memory_limit="1Gi",
            ephemeral_request="1Gi",
            ephemeral_limit="4Gi",
        )
        assert self.cluster is not None
        engine = create_async_engine(get_settings().database_url)
        try:
            store = PostgresPublicationStore(
                engine, schema="curie", lease_owner=f"c4-publisher-{uuid.uuid4().hex[:8]}"
            )
            async with self._api() as http:
                reconciler = PublicationReconciler(
                    store=store,
                    credentials=PublicationCredentialClient(
                        api_base_url=API_BASE, worker_token=WORKER_TOKEN, client=http
                    ),
                    cluster=self.cluster,
                    code_host=PublicationCodeHostClient(
                        api_base_url=API_BASE, worker_token=WORKER_TOKEN, client=http
                    ),
                    lineage=PublicationLineageClient(
                        api_base_url=API_BASE, worker_token=WORKER_TOKEN, client=http
                    ),
                    replies=ReplySinkRouter(
                        adapters={kind: TrackerReplySink() for kind in TRACKER_KINDS},
                        default=HttpReplyAdapter({}),
                    ),
                    transcript=PublicationTranscriptClient(
                        api_base_url=API_BASE, api_key=get_settings().api_key, client=http
                    ),
                    job_settings=settings,
                )
                for _ in range(10):
                    work = await store.claim_next()
                    if work is None:
                        break
                    await reconciler.reconcile(work)
        finally:
            await engine.dispose()

    # Observations -----------------------------------------------------------------

    def stream_length(self) -> int:
        client = _valkey()
        try:
            return int(client.xlen(self.stream))
        finally:
            client.close()


def _agent(client: TestClient, channel: dict[str, Any], repo: str | None = None) -> str:
    headers = {"X-API-Key": get_settings().api_key}
    body: dict[str, Any] = {"name": f"acme-factory-{uuid.uuid4().hex[:8]}", "channel": channel}
    if repo is not None:
        body["repo_full_name"] = repo
    created = client.post("/agents", headers=headers, json=body)
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    policy = client.patch(
        f"/agents/{agent_id}", headers=headers, json={"publication_policy": "auto"}
    )
    assert policy.status_code == 200, policy.text
    version = client.post(
        f"/agents/{agent_id}/versions",
        headers=headers,
        json={"version_label": "v1", "created_by": "operator"},
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        headers=headers,
        json={
            "agent_id": agent_id,
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
    )
    assert deployment.status_code == 201, deployment.text
    return str(deployment.json()["id"])


def _gitlab_project(
    project_id: int = gitlab_fake.PROJECT_ID,
    path: str = gitlab_fake.PROJECT_PATH,
    *,
    clock: SharedClock | None = None,
) -> C4GitLab:
    fake = C4GitLab(project_id=project_id, project_path=path, clock=clock)
    fake.add_user(DEVELOPER)
    fake.set_member(DEVELOPER, 30)
    return fake


def _gitlab_binding(name: str, project_id: int, path: str) -> dict[str, Any]:
    return {
        "name": name,
        "tracker": gitlab_fake.TRACKER_NAME,
        "tracker_scope_id": str(project_id),
        "repos": [
            {
                "alias": name,
                "code_host": gitlab_fake.CODE_HOST_NAME,
                "project_id": str(project_id),
                "path": path,
            }
        ],
    }


def _patch_outbound_http(monkeypatch: pytest.MonkeyPatch, router: Router) -> None:
    """Every ``httpx`` client the product builds without a transport of its own
    (the app's shared client, the pollers', the App credential resolver's sync
    client) reaches the forge fixtures through ``router``."""

    class _AsyncClient(_REAL_ASYNC_CLIENT):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if kwargs.get("transport") is None:
                kwargs["transport"] = router
            super().__init__(*args, **kwargs)

    class _Client(_REAL_CLIENT):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if kwargs.get("transport") is None:
                kwargs["transport"] = router
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _AsyncClient)
    monkeypatch.setattr(httpx, "Client", _Client)


@pytest.fixture
def install(
    request: pytest.FixtureRequest,
    fresh_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Iterator[Install]:
    binding = str(request.param)
    probe = _valkey()
    probe.close()
    host, port, password = _valkey_settings()
    stream = f"test:curie:c4-reinstall:{uuid.uuid4().hex}"
    router = Router()
    env = {
        "FACTORY_ENABLED": "true",
        "FACTORY_INTAKE": "poll",
        "FACTORY_RECONCILE_INTERVAL_S": "300",
        "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "RUNS_STREAM": stream,
        "VALKEY_HOST": host,
        "VALKEY_PORT": str(port),
        "VALKEY_PASSWORD": password or "",
        "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "rustfs"),
        "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "rustfssecret"),
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": "approval-chat-attester-test-secret",
    }
    for name in (
        "GITHUB_REPO_ALLOWLIST",
        "GITHUB_API_URL",
        "GITHUB_APP_ID",
        "GITHUB_APP_PRIVATE_KEY",
        "GITHUB_TOKEN",
        "CURIE_CODE_HOST_CA_BUNDLE",
    ):
        monkeypatch.delenv(name, raising=False)
    github: GitHubFake | None = None
    gitlab: list[C4GitLab] = []
    jira: jira_fake.JiraFake | None = None
    if binding in ("github-github", "github-webhook"):
        if binding == "github-webhook":
            env["FACTORY_INTAKE"] = "webhook"
        pem, public_key = generate_app_key()
        github = GitHubFake(public_key)
        router.handlers[GITHUB_HOST] = github.handle
        document = forges_document(
            api_url=GITHUB_API,
            private_key=pem,
            poll_interval_s=30.0,
            bindings=[github_repository_binding()],
        )
        env.update(
            {
                "GITHUB_API_URL": GITHUB_API,
                "GITHUB_APP_ID": "51",
                "GITHUB_APP_PRIVATE_KEY": pem,
                "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
                "GITHUB_TOKEN": "",
            }
        )
    elif binding in ("gitlab-gitlab", "gitlab-two-projects"):
        gitlab.append(_gitlab_project())
        bindings = [
            _gitlab_binding(
                gitlab_fake.BINDING_NAME, gitlab_fake.PROJECT_ID, gitlab_fake.PROJECT_PATH
            )
        ]
        if binding == "gitlab-two-projects":
            gitlab.append(_gitlab_project(SECOND_PROJECT_ID, SECOND_PROJECT_PATH))
            bindings.append(_gitlab_binding("gadgets", SECOND_PROJECT_ID, SECOND_PROJECT_PATH))
        document = {
            "code_hosts": [gitlab_fake.code_host_entry()],
            "trackers": [gitlab_fake.tracker_entry()],
            "bindings": bindings,
        }
    elif binding == "jira-gitlab":
        clock = SharedClock(SHARED_EPOCH)
        gitlab.append(_gitlab_project(clock=clock))
        jira = C4Jira(clock)
        router.asgi[jira_fake.SITE_HOST] = httpx.ASGITransport(app=jira.app)
        document = {
            "code_hosts": [gitlab_fake.code_host_entry()],
            "trackers": [jira_fake.jira_tracker_entry(label=LABEL)],
            "bindings": [
                {
                    "name": jira_fake.BINDING_NAME,
                    "tracker": jira_fake.TRACKER_NAME,
                    "tracker_scope_id": jira_fake.CLOUD_ID,
                    "repos": [
                        {
                            "alias": "widgets",
                            "code_host": gitlab_fake.CODE_HOST_NAME,
                            "project_id": str(gitlab_fake.PROJECT_ID),
                            "path": gitlab_fake.PROJECT_PATH,
                        }
                    ],
                    "default_repo": "widgets",
                    "start_authority": {"type": "allowlist", "accounts": [JIRA_STARTER]},
                }
            ],
        }
    else:
        raise ValueError(binding)
    if gitlab:
        router.handlers[gitlab_fake.HOST] = GitLabHost(*gitlab).handle
    env["CURIE_FORGES"] = forges_env_json(document, tmp_path / "forges")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _RESOLVERS.clear()
    _patch_outbound_http(monkeypatch, router)
    client = TestClient(create_app())
    client.__enter__()
    try:
        scenario: Scenario
        placeholder = Install(binding, client, router, Scenario.__new__(Scenario), stream)
        if github is not None:
            scenario = GitHubScenario(placeholder, github)
            placeholder.deployments[("github", REPO)] = _agent(
                client, {"kind": "github", "address": REPO}, REPO
            )
        elif jira is not None:
            scenario = JiraScenario(placeholder, jira, gitlab[0])
            placeholder.deployments[("jira", jira_fake.BINDING_NAME)] = _agent(
                client, {"kind": "jira", "address": jira_fake.BINDING_NAME}
            )
        else:
            scenario = GitLabScenario(placeholder, gitlab[0], *gitlab)
            for project in gitlab:
                placeholder.deployments[("gitlab", project.project_path)] = _agent(
                    client, {"kind": "gitlab", "address": project.project_path}
                )
        placeholder.scenario = scenario
        placeholder.cluster = JobCluster(scenario.push)
        yield placeholder
        assert not router.unknown, f"requests left the fixtures: {router.unknown}"
    finally:
        try:
            client.__exit__(None, None, None)
        finally:
            _cleanup_valkey(stream)
            _RESOLVERS.clear()
            get_settings.cache_clear()


def _cleanup_valkey(stream: str) -> None:
    """Delete the keys this test owns: its streams, and any key that names one of
    its own work items, requests, publications or lineages."""

    try:
        ids = [
            str(row["id"])
            for table in (
                "work_items",
                "execution_requests",
                "publications",
                "thread_publication_lineages",
            )
            for row in _rows(f"SELECT id FROM curie.{table}")
        ]
    except Exception:  # noqa: BLE001 - cleanup keeps going without the id list
        ids = []
    client = _valkey()
    try:
        for pattern in [f"{stream}*", *(f"*{identifier}*" for identifier in ids)]:
            owned = list(client.scan_iter(match=pattern, count=500))
            if owned:
                client.delete(*owned)
    finally:
        client.close()


# --- shared steps ----------------------------------------------------------------------


def _drive(install: Install, caplog: pytest.LogCaptureFixture, passes: int = 2) -> list[Executed]:
    """``passes`` polling passes, one reconciler pass that dispatches, then a
    worker run of every queued request."""

    for _ in range(passes):
        install.poll()
    install.dispatch(caplog)
    return install.execute()


def _names_url(comments: list[str], url: str) -> bool:
    return any(url in body for body in comments)


def _new_prs(before: dict[int, Any], after: dict[int, Any]) -> list[int]:
    return sorted(set(after) - set(before))


# --- AC 1: the fix pin -------------------------------------------------------------------


@pytest.mark.parametrize("state_label", [True, False], ids=["pr-open-label", "no-state-label"])
@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_empty_database_does_not_republish_issue(
    install: Install, state_label: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """A published, still-labelled issue meets an empty database: nothing reruns."""

    scenario = install.scenario
    published = [
        (ref, scenario.factory_pr(ref))
        for ref in [
            scenario.issue(state_label=state_label)
            for _ in range(2 if install.binding == "gitlab-gitlab" else 1)
        ]
    ]
    before = scenario.open_prs()
    assert len(before) == len(published)
    assert _count("work_items") == 0 and _count("execution_requests") == 0

    _drive(install, caplog)

    assert scenario.open_prs() == before, (
        "a reinstall with empty storage opened another pull/merge request for an issue "
        f"already published: before {before}, after {scenario.open_prs()}"
    )
    assert _count("work_items") == 0, "the published issue was admitted again"
    assert _count("execution_requests") == 0, "the published issue was queued again"
    assert install.stream_length() == 0, "an execution was dispatched for the published issue"
    assert install.cluster is not None and install.cluster.applied == [], (
        "a publication Job pushed a branch for the published issue"
    )
    for ref, number in published:
        assert _names_url(scenario.bot_comments(ref), scenario.pr_url(number)), (
            "the issue carries no factory status comment naming its existing publication"
        )


# --- AC 2: completion, intentional reruns, fresh issues -----------------------------------


@pytest.mark.parametrize("completion", ["closed", "merged"])
@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_completed_publication_suppresses_replay_of_old_trigger(
    install: Install, completion: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The trigger label predates the completed publication; it replays, nothing runs.

    A merged publication closes its issue on both forges; this issue was reopened
    afterwards and still carries the old label.
    """

    scenario = install.scenario
    ref = scenario.issue()
    scenario.factory_pr(ref, state=completion)
    before = scenario.all_prs()

    _drive(install, caplog)

    assert scenario.all_prs() == before, "the old trigger republished completed work"
    assert _count("work_items") == 0
    assert _count("execution_requests") == 0
    assert install.stream_length() == 0


@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_new_label_after_completion_reruns_once(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """A human adds the label again strictly after completion: one new run, one new PR."""

    scenario = install.scenario
    ref = scenario.issue()
    scenario.factory_pr(ref, state="closed")
    scenario.relabel(ref)
    before = scenario.all_prs()

    executed = _drive(install, caplog)

    created = _new_prs(before, scenario.all_prs())
    assert len(created) == 1, f"expected exactly one new publication, got {created}"
    assert [e.status for e in executed] == ["succeeded"], executed
    assert scenario.closing_reference(ref) in scenario.all_prs()[created[0]].splitlines()
    assert _count("work_items") == 1


@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_fresh_issue_admits_and_publishes_once(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    scenario = install.scenario
    ref = scenario.issue()

    executed = _drive(install, caplog)
    created = _new_prs({}, scenario.all_prs())
    assert len(created) == 1, f"a fresh issue should publish once, got {created}"
    assert [e.status for e in executed] == ["succeeded"], executed
    assert scenario.closing_reference(ref) in scenario.all_prs()[created[0]].splitlines()

    # Later passes find the publication they made and run nothing more.
    again = _drive(install, caplog)
    assert again == []
    assert _new_prs({}, scenario.all_prs()) == created
    assert _count("work_items") == 1
    assert _count("execution_requests") == 1


# --- AC 3: scopes, ownership, ambiguity, outages, races -------------------------------------


@pytest.mark.parametrize("install", ["gitlab-two-projects"], indirect=True)
def test_same_short_number_in_another_scope_does_not_suppress(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """Issue #1 of project A is published (``Closes #1`` in A); issue #1 of project B
    is fresh. B publishes exactly once; A gets nothing new.

    A GitLab binding is native (its tracker scope is its repository), so two
    tracker projects cannot route to one code project; the two scopes here are
    two projects on one GitLab host whose issues share the number 1.
    """

    assert isinstance(install.scenario, GitLabScenario)
    first, second = install.scenario.projects
    a = GitLabScenario(install, first)
    b = GitLabScenario(install, second)
    a_issue, b_issue = a.issue(), b.issue()
    assert a_issue == b_issue == 1
    published = a.factory_pr(a_issue)
    a_before, b_before = a.all_prs(), b.all_prs()

    executed = _drive(install, caplog)

    assert a.all_prs() == a_before, "project A's published issue #1 was republished"
    created = _new_prs(b_before, b.all_prs())
    assert len(created) == 1, f"project B's issue #1 should publish once, got {created}"
    assert [e.status for e in executed] == ["succeeded"], executed
    assert _names_url(a.bot_comments(a_issue), a.pr_url(published))


@pytest.mark.parametrize("install", ["github-github", "gitlab-gitlab"], indirect=True)
def test_human_lookalike_does_not_suppress(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """A person's pull request with the factory's branch shape and body is not proof."""

    scenario = install.scenario
    ref = scenario.issue()
    lookalike = scenario.factory_pr(ref, human=True)
    before = scenario.all_prs()

    executed = _drive(install, caplog)

    created = _new_prs(before, scenario.all_prs())
    assert len(created) == 1, f"the issue should publish once, got {created}"
    assert [e.status for e in executed] == ["succeeded"], executed
    assert lookalike in scenario.open_prs(), "the person's pull request was touched"
    assert not _names_url(scenario.bot_comments(ref), scenario.pr_url(lookalike))


@pytest.mark.parametrize("install", ["github-github", "gitlab-gitlab"], indirect=True)
def test_two_open_factory_publications_refuse(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    scenario = install.scenario
    ref = scenario.issue()
    first, second = scenario.factory_pr(ref), scenario.factory_pr(ref)
    before = scenario.all_prs()

    _drive(install, caplog)

    assert scenario.all_prs() == before, "an ambiguous issue was published again"
    assert _count("work_items") == 0
    assert _count("execution_requests") == 0
    assert install.stream_length() == 0
    comments = scenario.bot_comments(ref)
    assert _names_url(comments, scenario.pr_url(first))
    assert _names_url(comments, scenario.pr_url(second))


@pytest.mark.parametrize(
    "fault",
    [
        "server_error",
        "later_page",
        "malformed_author",
        "not_found",
        "body_missing",
        "body_not_text",
    ],
)
@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_unreadable_publication_listing_defers_until_it_recovers(
    install: Install, fault: str, caplog: pytest.LogCaptureFixture
) -> None:
    """An incomplete read of the code host's publications admits nothing and keeps
    the poll cursor; the pass after recovery suppresses the published issue.

    ``not_found`` is a 404 for the listing of a repository the pass just
    resolved: incomplete evidence, never a missing issue. ``body_missing`` and
    ``body_not_text`` are a pull or merge request whose body cannot be read
    (no ``body``/``description`` key, or a value that is neither text nor null).
    """

    scenario = install.scenario
    ref = scenario.issue()
    published = scenario.factory_pr(ref)
    scenario.listing_fault(fault)
    before = scenario.all_prs()
    cursor = scenario.cursor()

    for _ in range(2):
        install.poll()
        assert _count("work_items") == 0, "admitted on an incomplete publication listing"
        assert _count("execution_requests") == 0
        assert scenario.cursor() == cursor, "the poll cursor moved past a deferred issue"
    assert scenario.repository_listings() > 0

    scenario.clear_faults()
    executed = _drive(install, caplog, passes=1)

    assert executed == []
    assert scenario.all_prs() == before
    assert _count("work_items") == 0
    assert scenario.cursor() is not None, "the recovered pass did not store its cursor"
    assert _names_url(scenario.bot_comments(ref), scenario.pr_url(published))


@pytest.mark.parametrize("install", BINDINGS, indirect=True)
def test_publication_appearing_before_publish_opens_nothing(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """A matching factory publication appears between admission and publication:
    the worker's pull request step is refused and nothing new opens."""

    scenario = install.scenario
    ref = scenario.issue()
    install.poll()
    install.poll()
    install.dispatch(caplog)
    assert _count("execution_requests") == 1
    raced: list[int] = []

    def race(_publication_id: str) -> None:
        raced.append(scenario.factory_pr(ref))

    before_open = scenario.open_prs()
    executed = install.execute(before_reconcile=race)

    assert len(raced) == 1
    after = scenario.open_prs()
    assert set(after) == set(before_open) | set(raced), (
        f"a second publication opened beside the one that appeared: {after}"
    )
    assert len(executed) == 1 and executed[0].status != "succeeded", executed
    assert executed[0].error and "409" in executed[0].error, executed


@pytest.mark.parametrize("install", ["github-github", "gitlab-gitlab"], indirect=True)
def test_same_branch_recovery_survives_listing_outage(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """The publication's own branch already has its pull request (an earlier attempt
    opened it and lost the answer). Recovery adopts it by branch while the
    repository-wide listing is down."""

    scenario = install.scenario
    ref = scenario.issue()
    install.poll()
    install.poll()
    install.dispatch(caplog)
    assert install.cluster is not None

    def opened_earlier(env: dict[str, str], sha: str) -> None:
        row = _rows(
            "SELECT title, body FROM curie.publications WHERE id = :id",
            {"id": uuid.UUID(env["REVISION_ID"])},
        )[0]
        scenario.open_existing(env, sha, row["title"], row["body"])
        scenario.listing_fault("server_error")

    install.cluster.after_push = opened_earlier
    executed = install.execute()

    assert [e.status for e in executed] == ["succeeded"], executed
    assert len(scenario.prs_naming(ref)) == 1
    assert len(_new_prs({}, scenario.all_prs())) == 1


# --- every admission route: webhook delivery, missed-label reconcile, tracker sweep ----------


def _assert_refused(install: Install, ref: Any, published: int, before: dict[int, Any]) -> None:
    scenario = install.scenario
    assert scenario.open_prs() == before, (
        f"another pull/merge request opened: before {before}, after {scenario.open_prs()}"
    )
    assert _count("work_items") == 0, "the published issue was admitted again"
    assert _count("execution_requests") == 0, "the published issue was queued again"
    assert install.stream_length() == 0
    assert _names_url(scenario.bot_comments(ref), scenario.pr_url(published)), (
        "the issue carries no factory status comment naming its existing publication"
    )


@pytest.mark.parametrize("install", ["github-webhook"], indirect=True)
def test_webhook_label_delivery_does_not_republish_issue(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """A signed ``issues.labeled`` delivery for a published issue meets empty
    storage: refused, and the issue names its open pull request. A fresh issue
    delivered the same way admits, so the route itself works."""

    scenario = install.scenario
    ref = scenario.issue()
    published = scenario.factory_pr(ref)
    before = scenario.open_prs()

    response = install.deliver_label(ref)

    assert response.status_code == 200, response.text
    assert response.json()["status"] != "factory_admitted", response.json()
    _assert_refused(install, ref, published, before)
    assert scenario.repository_listings() > 0, "the delivery never read the publications"

    fresh = scenario.issue()
    admitted = install.deliver_label(fresh)
    assert admitted.status_code == 200, admitted.text
    assert admitted.json()["status"] == "factory_admitted", admitted.json()
    rows = _rows("SELECT tracker_issue_id FROM curie.work_items")
    assert [r["tracker_issue_id"] for r in rows] == [str(fresh)]
    executed = _drive(install, caplog, passes=0)
    assert [e.status for e in executed] == ["succeeded"], executed
    assert scenario.prs_naming(ref) == [published]
    assert len(scenario.prs_naming(fresh)) == 1


@pytest.mark.parametrize("install", ["github-webhook"], indirect=True)
def test_missed_label_reconcile_does_not_republish_issue(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """The webhook install's missed-label pass finds a still-labelled, published
    issue with no WorkItem and no delivery: it admits nothing and names the
    pull request. A fresh issue in the same pass is admitted."""

    scenario = install.scenario
    ref = scenario.issue()
    published = scenario.factory_pr(ref)
    before = scenario.open_prs()

    assert install.reconcile_missed_labels() == 0
    _assert_refused(install, ref, published, before)
    assert scenario.repository_listings() > 0, "the pass never read the publications"

    fresh = scenario.issue()
    assert install.reconcile_missed_labels() == 1
    rows = _rows("SELECT tracker_issue_id FROM curie.work_items")
    assert [r["tracker_issue_id"] for r in rows] == [str(fresh)]
    # The reconciler's own pass (it runs the same reconcile on this install)
    # dispatches only the fresh issue.
    executed = _drive(install, caplog, passes=0)
    assert [e.status for e in executed] == ["succeeded"], executed
    assert scenario.prs_naming(ref) == [published]
    assert len(scenario.prs_naming(fresh)) == 1


@pytest.mark.parametrize("install", ["gitlab-gitlab", "jira-gitlab"], indirect=True)
def test_tracker_sweep_defers_on_an_unreadable_listing_then_recovers(
    install: Install, caplog: pytest.LogCaptureFixture
) -> None:
    """A sweep-only pass (the poll not due, the reconciliation sweep due) while
    the code host's publication listing fails admits nothing, for the published
    issue or a fresh one. Once the listing recovers, a sweep refuses the
    published issue (naming its merge request) and admits the fresh one, which
    then publishes exactly once."""

    scenario = install.scenario
    ref = scenario.issue()
    published = scenario.factory_pr(ref)
    fresh = scenario.issue()
    before = scenario.open_prs()
    scenario.listing_fault("server_error")
    cursor = scenario.cursor()

    for _ in range(2):
        install.sweep_only()
        assert _count("work_items") == 0, "a sweep admitted on an unreadable listing"
        assert _count("execution_requests") == 0
        assert scenario.cursor() == cursor, "a sweep moved the poll cursor"
    assert scenario.repository_listings() > 0, "the sweep never read the publications"
    assert scenario.open_prs() == before
    assert install.stream_length() == 0

    scenario.clear_faults()
    install.sweep_only()

    assert _count("work_items") == 1, "the recovered sweep did not admit the fresh issue"
    assert scenario.cursor() == cursor, "a sweep moved the poll cursor"
    assert scenario.open_prs() == before
    assert _names_url(scenario.bot_comments(ref), scenario.pr_url(published))
    install.dispatch(caplog)
    executed = install.execute()
    assert [e.status for e in executed] == ["succeeded"], executed
    assert scenario.prs_naming(ref) == [published], "the published issue was republished"
    assert len(scenario.prs_naming(fresh)) == 1
