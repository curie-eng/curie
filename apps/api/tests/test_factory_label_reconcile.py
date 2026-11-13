"""A factory label whose delivery was lost is admitted by reconciliation (#3081).

GitHub REST shapes follow:
https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/issues/events#list-issue-events
https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user

Machine fixtures stand in for GitHub. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import itertools
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_label_reconcile import reconcile_missed_labels
from curie_api.github_app import GitHubInstallationRefused
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes.github import (
    LABEL_ACTOR_PERMISSIONS,
    LABEL_ACTOR_REFUSAL_IDS,
    LABEL_ACTOR_REFUSALS,
    factory_env,
    fake_installation_tokens,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
REPO_ID = 4401
INSTALLATION_ID = 5501
SENDER_ID = 6601
SENDER = "octocat"
LABEL = "factory"
_ISSUES = itertools.count(9700)


class GitHubAPI:
    """In-process stand-in for the GitHub REST reads reconciliation makes."""

    def __init__(self) -> None:
        self.open_issues: dict[int, datetime] = {}
        self.permission = "write"
        self.label_actor: Any = {"id": SENDER_ID, "login": SENDER, "type": "User"}
        self.label_app: Any = None
        self.permission_status = 200
        self.role_name = "write"
        self.permission_user_id = SENDER_ID
        # Older events that precede the labeled event, and a newer relabel.
        self.padding_events = 0
        self.relabel_by_bot = False

    def label(self, number: int, *, age: timedelta) -> None:
        self.open_issues[number] = datetime.now(UTC) - age

    @staticmethod
    def _page(request: httpx.Request, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        per_page = int(request.url.params.get("per_page", "30"))
        page = int(request.url.params.get("page", "1"))
        return items[(page - 1) * per_page : page * per_page]

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}":
            return httpx.Response(
                200, json={"id": REPO_ID, "full_name": REPO, "default_branch": "main"}
            )
        if path.startswith(f"/repos/{REPO}/git/ref/heads/"):
            # Admission resolves the base and reads its commit (#3095).
            name = path.removeprefix(f"/repos/{REPO}/git/ref/heads/")
            return httpx.Response(
                200, json={"ref": f"refs/heads/{name}", "object": {"sha": "0" * 39 + "1"}}
            )
        if path == f"/repos/{REPO}/issues":
            assert request.url.params.get("labels") == LABEL
            assert request.url.params.get("state") == "open"
            issues = [
                {"number": n, "state": "open", "labels": [{"name": LABEL}]}
                for n in self.open_issues
            ]
            return httpx.Response(200, json=self._page(request, issues))
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            labeled_at = self.open_issues[number].isoformat().replace("+00:00", "Z")
            human = {"id": SENDER_ID, "login": SENDER, "type": "User"}
            events: list[dict[str, Any]] = [
                {
                    "id": 700000 + i,
                    "event": "labeled",
                    "label": {"name": LABEL},
                    "actor": human,
                    "performed_via_github_app": None,
                    "created_at": labeled_at,
                }
                for i in range(self.padding_events)
            ]
            events.append(
                {
                    "id": 800000 + number,
                    "event": "labeled",
                    "label": {"name": LABEL},
                    "actor": {"id": 42, "login": "some-bot[bot]", "type": "Bot"}
                    if self.relabel_by_bot
                    else self.label_actor,
                    "performed_via_github_app": self.label_app,
                    "created_at": labeled_at,
                }
            )
            return httpx.Response(200, json=self._page(request, events))
        if path.startswith(f"/repos/{REPO}/issues/"):
            number = int(path.rsplit("/", 1)[1])
            labeled = number in self.open_issues
            return httpx.Response(
                200,
                json={
                    "number": number,
                    "state": "open",
                    "labels": [{"name": LABEL}] if labeled else [],
                },
            )
        if path == f"/repos/{REPO}/pulls" and request.method == "GET":
            # Rediscovery's repository-wide listing (#4629): no factory
            # publication exists in this stand-in.
            return httpx.Response(200, json=[])
        if path == f"/repos/{REPO}/collaborators/{SENDER}/permission":
            return httpx.Response(
                self.permission_status,
                json={
                    "permission": self.permission,
                    "role_name": self.role_name,
                    "user": {"id": self.permission_user_id, "login": SENDER},
                },
            )
        return httpx.Response(404, json={"message": "missing fixture"})


class _Credentials:
    def fresh_installation_token(
        self, repo: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        if repo != REPO or expected_installation_id not in (None, INSTALLATION_ID):
            raise GitHubInstallationRefused("installation was not rediscovered")
        return INSTALLATION_ID, "fixture-installation-token"

    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        return self.fresh_installation_token(repo, installation_id)[1]

    def token_for(self, repo: str) -> str:
        return self.fresh_installation_token(repo)[1]


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None) -> Any:
    for key, value in factory_env(tmp_path, label=LABEL, mention="curie").items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    creds = _Credentials()
    fake_installation_tokens(monkeypatch, creds)
    github = GitHubAPI()
    with TestClient(create_app()) as client:
        external = httpx.AsyncClient(transport=httpx.MockTransport(github.handle))
        client.app.state.http_client = external
        created = client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert created.status_code == 201, created.text
        yield client, github
        client.portal.call(external.aclose)
    get_settings.cache_clear()


def _reconcile(github: GitHubAPI) -> int:
    async def go() -> int:
        engine = create_async_engine(get_settings().database_url)
        client = httpx.AsyncClient(transport=httpx.MockTransport(github.handle))
        try:
            return await reconcile_missed_labels(
                async_sessionmaker(engine, expire_on_commit=False),
                get_settings(),
                client,
                now=datetime.now(UTC),
            )
        finally:
            await client.aclose()
            await engine.dispose()

    return asyncio.run(go())


def _requests(number: int) -> list[dict[str, Any]]:
    async def go() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(
                    text(
                        "SELECT r.status, r.objective, r.requester "
                        "FROM curie.work_items w "
                        "JOIN curie.execution_requests r ON r.work_item_id = w.id "
                        "WHERE w.tracker_scope_id = :repo "
                        "AND w.tracker_issue_id = :number"
                    ),
                    {"repo": str(REPO_ID), "number": str(number)},
                )
                return [dict(row) for row in result.mappings()]
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _deliver_label(client: TestClient, number: int) -> httpx.Response:
    body = json.dumps(
        {
            "action": "labeled",
            "installation": {"id": INSTALLATION_ID},
            "repository": {"id": REPO_ID, "full_name": REPO},
            "sender": {"id": SENDER_ID, "login": SENDER, "type": "User"},
            "issue": {"number": number, "state": "open"},
            "label": {"name": LABEL},
        }
    ).encode()
    secret = get_settings().github_webhook_secret.encode()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest(),
            "Content-Type": "application/json",
        },
    )


def test_a_label_whose_delivery_was_lost_is_admitted_once(
    factory: tuple[TestClient, GitHubAPI],
) -> None:
    _client, github = factory
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    assert _requests(number) == []

    assert _reconcile(github) == 1
    assert _reconcile(github) == 0

    rows = _requests(number)
    assert len(rows) == 1
    assert rows[0]["status"] == "waiting"
    assert rows[0]["objective"] == f"https://github.com/{REPO}/issues/{number}"
    assert rows[0]["requester"] == f"github:{SENDER_ID}:{SENDER}"


def test_a_delivered_label_is_not_admitted_again(
    factory: tuple[TestClient, GitHubAPI],
) -> None:
    client, github = factory
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    delivered = _deliver_label(client, number)
    assert delivered.json()["status"] == "factory_admitted", delivered.text

    assert _reconcile(github) == 0

    assert len(_requests(number)) == 1


def test_a_fresh_label_waits_for_its_delivery(
    factory: tuple[TestClient, GitHubAPI],
) -> None:
    _client, github = factory
    number = next(_ISSUES)
    github.label(number, age=timedelta(seconds=5))

    assert _reconcile(github) == 0

    assert _requests(number) == []


def test_a_labeler_without_write_permission_is_not_admitted(
    factory: tuple[TestClient, GitHubAPI],
) -> None:
    _client, github = factory
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    github.permission = "read"

    assert _reconcile(github) == 0

    assert _requests(number) == []


def test_a_newer_bot_relabel_on_a_later_event_page_is_not_admitted(
    factory: tuple[TestClient, GitHubAPI],
) -> None:
    """The newest label event decides, even past the first page of events."""

    _client, github = factory
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    github.padding_events = 120
    github.relabel_by_bot = True

    assert _reconcile(github) == 0

    assert _requests(number) == []


# GitHub User-with-App attribution replays the same recorded shape and provider
# references as test_github_factory_ingress.py (#5510 and #4485).
@pytest.mark.parametrize("app", [None, {"id": 52}])
def test_label_actor_admission(
    factory: tuple[TestClient, GitHubAPI], app: dict[str, Any] | None
) -> None:
    _client, github = factory
    github.label_app = app
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    assert _reconcile(github) == 1
    assert _reconcile(github) == 0
    assert [r["requester"] for r in _requests(number)] == [f"github:{SENDER_ID}:{SENDER}"]


@pytest.mark.parametrize("permission", LABEL_ACTOR_PERMISSIONS)
def test_label_actor_permission(factory: tuple[TestClient, GitHubAPI], permission: str) -> None:
    _client, github = factory
    github.label_app = {"id": 52}
    github.permission = "write" if permission == "maintain" else permission
    github.role_name = permission
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    admits = int(permission in {"write", "maintain", "admin"})
    assert _reconcile(github) == admits
    assert len(_requests(number)) == admits


def test_label_actor_permission_retry(factory: tuple[TestClient, GitHubAPI]) -> None:
    _client, github = factory
    github.label_app, github.permission_status = {"id": 52}, 500
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    assert _reconcile(github) == 0
    assert _requests(number) == []
    github.permission_status = 200
    assert _reconcile(github) == 1
    assert len(_requests(number)) == 1


@pytest.mark.parametrize("actor,app,reason", LABEL_ACTOR_REFUSALS, ids=LABEL_ACTOR_REFUSAL_IDS)
def test_label_actor_own_app_bot_or_unverifiable(
    factory: tuple[TestClient, GitHubAPI],
    actor: Any,
    app: Any,
    reason: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO", logger="curie_api.factory_label_reconcile")
    _client, github = factory
    github.label_actor, github.label_app = actor, app
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    assert _reconcile(github) == 0
    assert _requests(number) == []
    assert (REPO, number, reason) in [
        record.args
        for record in caplog.records
        if record.name == "curie_api.factory_label_reconcile"
    ]
    github.label_actor = {"id": SENDER_ID, "login": SENDER, "type": "User"}
    github.label_app = {"id": 52}
    assert _reconcile(github) == 1
    assert len(_requests(number)) == 1


def test_label_actor_permission_identity_mismatch(factory: tuple[TestClient, GitHubAPI]) -> None:
    _client, github = factory
    github.label_app, github.permission_user_id = {"id": 52}, SENDER_ID + 1
    number = next(_ISSUES)
    github.label(number, age=timedelta(minutes=30))
    assert _reconcile(github) == 0
    github.permission_user_id = SENDER_ID
    assert _reconcile(github) == 1
