"""Exercise signed ingress, production poll and reconciliation against Postgres.

Only GitHub HTTP is substituted, including installation discovery and token
minting. Payloads follow the GitHub REST issue-event, App, issue-comment and
pull-request resources: https://docs.github.com/en/rest
App authentication response contracts:
https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app
https://docs.github.com/en/rest/apps/installations#create-an-installation-access-token
"""

from __future__ import annotations

import asyncio
import itertools
import json
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from curie_api.config import get_settings
from curie_api.factory_intake_notices import METRIC
from curie_api.factory_label_reconcile import reconcile_missed_labels
from curie_api.factory_poll_intake import poll_once
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_telemetry import configure_meter_provider
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from forge_fakes.github import (
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    SENDER,
    _issue_event,
    _post,
    factory_env,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_factory_poll_intake import PollGitHub, _client_type, _iso
from test_github_factory_ingress import _rows

REASONS = (
    "actor_unverifiable",
    "actor_bot",
    "actor_own_app",
    "actor_no_permission",
    "personal_owner_required",
    "binding_ambiguous",
    "publication_completed",
    "installation_changed",
)
PATHS = ("webhook", "poll", "reconcile")


class NoticeGitHub(PollGitHub):
    installation = INSTALLATION_ID
    comment_failure = False
    lose_write = False
    missing_event = False
    unknown_event_time = False
    incomplete_listing = False
    superseded = False
    completed_for: int | None = None
    completed_at: datetime | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/app":
            return httpx.Response(200, json={"id": 51, "slug": "curie"})
        if path == f"/repos/{REPO}/installation":
            return httpx.Response(200, json={"id": self.installation})
        if path == f"/app/installations/{self.installation}/access_tokens":
            return httpx.Response(
                201,
                json={
                    "token": "fixture-installation-token",
                    "expires_at": _iso(datetime.now(UTC) + timedelta(hours=1)),
                },
            )
        if path.endswith("/events"):
            if self.incomplete_listing:
                page = int(request.url.params.get("page", "1"))
                return httpx.Response(
                    200,
                    json=[
                        {
                            "id": page * 100 + i,
                            "event": "mentioned",
                            "actor": self.label_actor,
                            "created_at": _iso(datetime.now(UTC)),
                        }
                        for i in range(100)
                    ],
                )
            response = super().handle(request)
            if self.unknown_event_time:
                return httpx.Response(
                    200, json=[{**event, "created_at": None} for event in response.json()]
                )
            if self.missing_event:
                return httpx.Response(
                    200, json=[{**event, "id": None} for event in response.json()]
                )
            if self.superseded:
                events = response.json()
                events.append(
                    {
                        **events[0],
                        "id": events[0]["id"] + 1,
                        "actor": {"id": 6601, "login": SENDER, "type": "User"},
                    }
                )
                return httpx.Response(200, json=events)
            return response
        if path == f"/repos/{REPO}/pulls" and self.completed_for is not None:
            side = {"id": REPO_ID, "full_name": REPO}
            return httpx.Response(
                200,
                json=[
                    {
                        "number": 76,
                        "html_url": f"https://github.com/{REPO}/pull/76",
                        "user": {"login": "curie[bot]", "type": "Bot"},
                        "head": {"ref": "publication-" + "a" * 32, "sha": "1" * 40, "repo": side},
                        "base": {"ref": "main", "repo": side},
                        "state": "closed",
                        "merged": True,
                        "merged_at": _iso(self.completed_at) if self.completed_at else None,
                        "body": f"Closes #{self.completed_for}",
                    }
                ],
            )
        if path.endswith("/comments") and request.method == "POST":
            if self.comment_failure:
                return httpx.Response(
                    403, json={"message": "Resource not accessible by integration"}
                )
            result = super().handle(request)
            if self.lose_write:
                self.lose_write = False
                raise httpx.ReadTimeout("response lost", request=request)
            return result
        return super().handle(request)


def _sync_client_type(github: NoticeGitHub) -> type[httpx.Client]:
    class GitHubClient(httpx.Client):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(github.handle)
            super().__init__(*args, **kwargs)

    return GitHubClient


@pytest.fixture(scope="session")
def app_key() -> str:
    return (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )


@pytest.fixture
def factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None, valkey: object, app_key: str
) -> Any:
    for key, value in factory_env(tmp_path, intake="webhook", private_key=app_key).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", app_key)
    get_settings.cache_clear()
    github = NoticeGitHub()
    monkeypatch.setattr(httpx, "Client", _sync_client_type(github))
    monkeypatch.setattr(httpx, "AsyncClient", _client_type(github))
    with TestClient(create_app()) as client:
        response = client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert response.status_code == 201, response.text
        yield client, github, response.json()["id"]
    get_settings.cache_clear()


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def go() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _pass(github: NoticeGitHub, path: str) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with httpx.AsyncClient() as client:
                if path == "poll":
                    await poll_once(maker, get_settings(), client)
                elif path == "reconcile":
                    await reconcile_missed_labels(
                        maker, get_settings(), client, now=datetime.now(UTC)
                    )
                else:
                    valkey = Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
                    try:
                        await WorkItemReconciler(
                            maker, valkey, get_settings()
                        )._sync_status_comments()
                    finally:
                        await valkey.aclose()
        finally:
            await engine.dispose()

    asyncio.run(go())


def _observe(factory: Any, path: str, number: int, delivery: str | None = None) -> None:
    client, github, _agent = factory
    if path == "webhook":
        payload = _issue_event("labeled", number, label={"name": LABEL})
        payload["installation"] = {"id": github.installation}
        result = _post(client, "issues", payload, delivery=delivery)
        assert result.status_code == 200, result.text
    else:
        _pass(github, path)


def _notices(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT * FROM curie.factory_intake_notices WHERE tracker_issue_id = :n ORDER BY id",
        {"n": str(number)},
    )


def _work(number: int) -> list[dict[str, Any]]:
    return _rows("SELECT * FROM curie.work_items WHERE tracker_issue_id = :n", {"n": str(number)})


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT e.* FROM curie.execution_requests e JOIN curie.work_items w "
        "ON e.work_item_id = w.id WHERE w.tracker_issue_id = :n ORDER BY sequence",
        {"n": str(number)},
    )


def _comments(github: NoticeGitHub, number: int) -> list[dict[str, Any]]:
    return [c for c in github.comments.values() if c["issue_url"].endswith(f"/issues/{number}")]


def _configure(factory: Any, reason: str, number: int, monkeypatch: pytest.MonkeyPatch) -> None:
    client, github, _agent = factory
    if reason == "actor_unverifiable":
        github.label_actor = None
    elif reason == "actor_bot":
        github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    elif reason == "actor_own_app":
        github.label_app = {"id": 51}
    elif reason == "actor_no_permission":
        github.permission = "triage"
    elif reason == "personal_owner_required":
        monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
        get_settings.cache_clear()
    elif reason == "binding_ambiguous":
        result = client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-other-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO, "trigger_label": "other-factory"},
            },
        )
        assert result.status_code == 201, result.text
        _sql(
            "UPDATE curie.agent_channels SET trigger_label = :label WHERE agent_id = :id",
            {"id": uuid.UUID(result.json()["id"]), "label": LABEL},
        )
    elif reason == "publication_completed":
        github.completed_for = number
        github.completed_at = github.issues[number].labeled_at + timedelta(seconds=1)
    elif reason == "installation_changed":
        _observe(factory, "webhook", number)
        github.installation = INSTALLATION_ID + 1
        github.issues[number].event_id += 1
        github.issues[number].labeled_at = datetime.now(UTC) - timedelta(minutes=5)


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("reason", REASONS)
def test_each_reason(factory: Any, monkeypatch: pytest.MonkeyPatch, path: str, reason: str) -> None:
    _client, github, agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    _configure(factory, reason, number, monkeypatch)
    before_work, before_requests = _work(number), _requests(number)
    _observe(factory, path, number)
    rows = _notices(number)
    assert len(rows) == 1
    assert rows[0]["reason"] == reason
    assert rows[0]["tracker_scope_id"] == str(REPO_ID)
    assert rows[0]["label_event_id"] == github.issues[number].event_id
    assert rows[0]["agent_id"] == (None if reason == "binding_ambiguous" else uuid.UUID(agent))
    assert rows[0]["posted_at"] is not None
    comments = [c for c in _comments(github, number) if "curie-factory-intake-" in c["body"]]
    assert len(comments) == 1
    assert comments[0]["performed_via_github_app"]["id"] == 51
    assert f"reason={reason}" in comments[0]["body"]
    visible = comments[0]["body"].split("<!--", 1)[0]
    facts = {
        "actor_bot": (REPO,),
        "actor_own_app": ("GitHub App",),
        "actor_no_permission": (SENDER, REPO),
        "actor_unverifiable": (REPO,),
        "personal_owner_required": (SENDER,),
        "binding_ambiguous": (f"label {LABEL}", REPO),
        "publication_completed": (f"https://github.com/{REPO}/pull/76",),
        "installation_changed": ("GitHub App",),
    }
    assert all(fact in visible for fact in facts[reason])
    if reason == "publication_completed":
        assert rows[0]["publication_url"] == f"https://github.com/{REPO}/pull/76"
        assert rows[0]["publication_url"] in comments[0]["body"]
    assert _work(number) == before_work
    assert _requests(number) == before_requests
    assert not [
        r for r in github.calls if r.method in {"POST", "DELETE"} and "/labels" in r.url.path
    ]


@pytest.mark.parametrize("path", PATHS)
def test_one_per_event_and_relabel(factory: Any, path: str) -> None:
    _client, github, _agent = factory
    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    number = github.plant_issue(age=timedelta(minutes=5))
    delivery = str(uuid.uuid4())
    _observe(factory, "webhook", number, delivery)
    _observe(factory, "webhook", number, delivery)
    _observe(factory, path, number)
    assert len(_notices(number)) == len(_comments(github, number)) == 1
    github.issues[number].event_id += 2
    github._etags["issues"] = '"new-label"'
    _observe(factory, path, number)
    assert len(_notices(number)) == len(_comments(github, number)) == 2


def test_one_per_event_restart_lost_success_and_forged_marker(factory: Any) -> None:
    _client, github, _agent = factory
    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    number = github.plant_issue(age=timedelta(minutes=5))
    github.comment_failure = True
    _observe(factory, "webhook", number)
    row = _notices(number)[0]
    assert row["comment_id"] is None and row["posted_at"] is None
    from curie_api.factory_intake_notices import marker_for
    from curie_api.models import FactoryIntakeNotice

    forged = github.add_issue_comment(
        number, 700, f"<!-- {marker_for(FactoryIntakeNotice(**row))} -->"
    )
    forged_snapshot = dict(forged)
    _sql("UPDATE curie.factory_intake_notices SET not_before = NULL")
    github.comment_failure = False
    restarted = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, sys, httpx
sys.path.insert(0, "apps/api/tests")
from test_factory_intake_notices import NoticeGitHub, _sync_client_type, _client_type, _pass
provider = NoticeGitHub()
provider.comments = {int(key): value for key, value in json.load(sys.stdin).items()}
provider.lose_write = True
httpx.Client = _sync_client_type(provider)
httpx.AsyncClient = _client_type(provider)
_pass(provider, "delivery")
print(json.dumps(provider.comments))
""",
        ],
        input=json.dumps(github.comments),
        text=True,
        capture_output=True,
        timeout=20,
        check=True,
    )
    github.comments = {int(key): value for key, value in json.loads(restarted.stdout).items()}
    github._comment_ids = itertools.count(max(github.comments) + 1)
    assert len(_comments(github, number)) == 2
    assert _notices(number)[0]["posted_at"] is None
    _sql("UPDATE curie.factory_intake_notices SET not_before = NULL")
    get_settings.cache_clear()
    _pass(github, "delivery")
    assert len(_comments(github, number)) == 2
    assert _notices(number)[0]["comment_id"] != forged["id"]
    assert _notices(number)[0]["posted_at"] is not None
    assert github.comments[forged["id"]] == forged_snapshot


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("case", ("closed", "absent", "superseded"))
def test_silent(factory: Any, path: str, case: str) -> None:
    _client, github, _agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    if case == "closed":
        github.close_issue(number)
    elif case == "absent":
        github.unlabel_issue(number)
    else:
        github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
        github.superseded = True
    _observe(factory, path, number)
    assert _notices(number) == []
    if case != "superseded":
        assert _work(number) == []


@pytest.mark.parametrize(
    "failure", ("permission", "rate_limit", "events", "missing_id", "incomplete")
)
def test_retried_poll_does_not_save_etag(factory: Any, failure: str) -> None:
    _client, github, _agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    if failure == "missing_id":
        github.missing_event = True
    elif failure == "incomplete":
        github.incomplete_listing = True
    else:
        suffix = (
            f"/issues/{number}/events"
            if failure == "events"
            else f"/collaborators/{SENDER}/permission"
        )
        github.failures[f"/repos/{REPO}{suffix}"] = 429 if failure == "rate_limit" else 503
    _pass(github, "poll")
    assert _notices(number) == _work(number) == []
    cursors = _rows("SELECT etags FROM curie.factory_poll_cursors")
    assert not cursors or f"issues:{LABEL}" not in cursors[0]["etags"]
    github.failures.clear()
    github.missing_event = False
    github.incomplete_listing = False
    github.permission = "read"
    _pass(github, "poll")
    assert [r["reason"] for r in _notices(number)] == ["actor_no_permission"]


@pytest.mark.parametrize("path", PATHS)
def test_precedence_actor_before_labels_personal_and_installation(
    factory: Any, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    _client, github, _agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    _configure(factory, "binding_ambiguous", number, monkeypatch)
    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    _observe(factory, path, number)
    assert [r["reason"] for r in _notices(number)] == ["actor_bot"]
    number = github.plant_issue(age=timedelta(minutes=5))
    github.label_actor = {"id": 6601, "login": SENDER, "type": "User"}
    github.permission = "read"
    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
    get_settings.cache_clear()
    github._etags["issues"] = '"another-issue"'
    _observe(factory, path, number)
    assert [r["reason"] for r in _notices(number)] == ["actor_no_permission"]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("order", ("after", "same", "unknown_event", "unknown_completion"))
def test_precedence_completed_publication_order(factory: Any, path: str, order: str) -> None:
    _client, github, _agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    github.completed_for = number
    github.completed_at = github.issues[number].labeled_at
    if order == "after":
        github.completed_at -= timedelta(seconds=1)
    elif order == "unknown_completion":
        github.completed_at = None
    github.unknown_event_time = order == "unknown_event"
    _observe(factory, path, number)
    if order == "after":
        assert len(_work(number)) == len(_requests(number)) == 1
        assert _notices(number) == []
    else:
        assert _work(number) == _requests(number) == []
        assert [r["reason"] for r in _notices(number)] == ["publication_completed"]
        assert _notices(number)[0]["publication_url"] == f"https://github.com/{REPO}/pull/76"


@pytest.mark.parametrize("path", PATHS)
def test_installation_changed_preserves_work_and_current_installation_liveness(
    factory: Any, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    _client, github, _agent = factory
    number = github.plant_issue(age=timedelta(minutes=5))
    _configure(factory, "installation_changed", number, monkeypatch)
    before_work, before_requests = _work(number), _requests(number)
    _observe(factory, path, number)
    assert [r["reason"] for r in _notices(number)] == ["installation_changed"]
    assert _work(number) == before_work and _requests(number) == before_requests
    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    github.issues[number].event_id += 1
    github._etags["issues"] = '"bot-relabel"'
    _observe(factory, path, number)
    assert [r["reason"] for r in _notices(number)] == ["installation_changed", "actor_bot"]
    assert _work(number) == before_work and _requests(number) == before_requests


@pytest.mark.parametrize("path", PATHS)
def test_delegated_output_seam(factory: Any, monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    _client, github, _agent = factory
    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    number = github.plant_issue(age=timedelta(minutes=5))
    _observe(factory, path, number)
    assert [r["reason"] for r in _notices(number)] == ["actor_bot"]
    seen = []

    async def delegated(session: Any, issue: Any, notice: Any) -> bool:
        seen.append((issue.issue_id, notice.label_event_id))
        return True

    monkeypatch.setattr("curie_api.factory_intake_notices.delegated_output_before_label", delegated)
    number = github.plant_issue(age=timedelta(minutes=5))
    github._etags["issues"] = '"delegated-label"'
    _observe(factory, path, number)
    assert (str(number), github.issues[number].event_id) in seen
    assert _notices(number) == _work(number) == []


def test_owed_backoff_and_metric(factory: Any) -> None:
    _client, github, _agent = factory
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    configure_meter_provider(provider)

    def value() -> float:
        data = reader.get_metrics_data()
        return next(
            point.value
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == METRIC
            for point in metric.data.data_points
        )

    github.label_actor = {"id": 99, "login": "acme[bot]", "type": "Bot"}
    number = github.plant_issue(age=timedelta(minutes=5))
    github.comment_failure = True
    _observe(factory, "webhook", number)
    row = _notices(number)[0]
    assert row["attempts"] == 1 and row["not_before"] > row["observed_at"]
    _pass(github, "delivery")
    assert _notices(number)[0]["attempts"] == 1
    _sql("UPDATE curie.factory_intake_notices SET observed_at = observed_at - interval '1 minute'")
    _pass(github, "delivery")
    assert value() >= 60
    github.comment_failure = False
    _sql("UPDATE curie.factory_intake_notices SET not_before = NULL")
    _pass(github, "delivery")
    assert len(_comments(github, number)) == 1
    assert value() == 0
    assert _notices(number)[0]["posted_at"] is not None

    provider.shutdown()
    configure_meter_provider(MeterProvider())


def test_duplicate_silent_base_label_reconciles(factory: Any) -> None:
    _client, github, _agent = factory
    number = github.plant_issue()
    _observe(factory, "webhook", number)
    before = _requests(number)
    before_work = _work(number)[0]
    github.issues[number].base_labels = ("base:next",)
    _pass(github, "poll")
    assert _notices(number) == []
    assert _requests(number) == before
    assert (
        _work(number)[0]["base_selection_fingerprint"] != before_work["base_selection_fingerprint"]
    )


@pytest.mark.parametrize("path", ("webhook", "poll"))
def test_installation_changed_current_identity_liveness(factory: Any, path: str) -> None:
    _client, github, _agent = factory
    number = github.plant_issue()
    _observe(factory, "webhook", number)
    previous = _requests(number)
    github.issues[number].event_id += 1
    github.issues[number].labeled_at = datetime.now(UTC) + timedelta(seconds=1)
    github._etags["issues"] = '"fresh-event"'
    _observe(factory, path, number)
    assert _notices(number) == []
    assert len(_requests(number)) == len(previous) + 1


def test_intake_notice_migration_refuses_lossy_downgrade(isolated_migration_db: Any) -> None:
    from _migration_support import alembic_config
    from alembic import command
    from curie_api.schema_compat import SchemaServingUnavailable, assert_servable

    isolated_migration_db.at("0160")
    with pytest.raises(SchemaServingUnavailable):
        asyncio.run(assert_servable())
    config = alembic_config()
    command.upgrade(config, "0164")
    _sql(
        "INSERT INTO curie.factory_intake_notices "
        "(tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, label_event_id, "
        "repository_path, issue_url, label, reason) VALUES "
        "('github', 'github.com', '4401', '1', 1, 'acme-corp/acme-bot', "
        "'https://github.com/acme-corp/acme-bot/issues/1', 'factory', 'actor_bot')"
    )
    with pytest.raises(RuntimeError, match="notices exist"):
        command.downgrade(config, "0160")
    assert _rows("SELECT reason FROM curie.factory_intake_notices") == [{"reason": "actor_bot"}]
    _sql("DELETE FROM curie.factory_intake_notices")
    command.downgrade(config, "0160")
    assert _rows("SELECT to_regclass('curie.factory_intake_notices') AS table_name") == [
        {"table_name": None}
    ]
    command.upgrade(config, "0164")


@pytest.mark.parametrize("case", ("signature", "disabled", "unserved", "outside", "pull_request"))
def test_silent_untrusted_or_unserved(
    factory: Any, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    client, github, _agent = factory
    number = github.plant_issue()
    payload = _issue_event("labeled", number, label={"name": LABEL})
    if case == "signature":
        result = client.post(
            "/github/webhook",
            json=payload,
            headers={
                "X-GitHub-Event": "issues",
                "X-GitHub-Delivery": str(uuid.uuid4()),
                "X-Hub-Signature-256": "sha256=" + "0" * 64,
            },
        )
        assert result.status_code == 401
        assert _rows("SELECT delivery_id FROM curie.github_review_deliveries") == []
    else:
        if case == "disabled":
            monkeypatch.setenv("FACTORY_ENABLED", "false")
            get_settings.cache_clear()
        elif case == "unserved":
            payload["label"]["name"] = "unserved-label"
        elif case == "outside":
            payload["repository"]["full_name"] = "other-corp/other-bot"
        else:
            payload["issue"]["pull_request"] = {
                "url": "https://api.github.com/repos/acme-corp/acme-bot/pulls/1"
            }
        assert _post(client, "issues", payload).status_code == 200
    assert _notices(number) == _work(number) == _requests(number) == []
    assert _comments(github, number) == []
