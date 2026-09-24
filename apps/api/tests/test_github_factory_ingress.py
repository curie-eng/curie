"""Signed GitHub issue intake for one WorkItem (#2574).

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads
Signatures follow:
https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
Sender permission follows:
https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user

Machine fixtures drive the events. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import itertools
import json
import threading
import uuid
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.github_app import GitHubInstallationRefused
from curie_api.github_factory import handle_factory_delivery
from curie_api.main import create_app
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
OTHER_REPO = "attacker/other-bot"
REPO_ID = 4401
INSTALLATION_ID = 5501
SENDER_ID = 6601
SENDER = "octocat"
LABEL = "factory"
MENTION = "curie"
_ISSUES = itertools.count(9100)

_ENV = {
    "GITHUB_FACTORY_INGRESS_ENABLED": "true",
    "GITHUB_FACTORY_LABEL": LABEL,
    "GITHUB_FACTORY_MENTION": MENTION,
    "GITHUB_REVIEW_INGRESS_ENABLED": "false",
    "GITHUB_APP_ID": "51",
    "GITHUB_APP_PRIVATE_KEY": "example-private-key",
    "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
    "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
    "GITHUB_TOKEN": "",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "RESUME_RECONCILER_ENABLED": "false",
    "APPROVAL_SWEEP_INTERVAL_S": "0",
    "DEAD_LETTER_WATCH_INTERVAL_S": "0",
}


class GitHubAPI:
    """In-process stand-in for the GitHub REST reads the intake verifier makes."""

    def __init__(self) -> None:
        self.issue_state = "open"
        self.labels = [LABEL]
        self.permission = "write"
        self.permission_user_id = SENDER_ID
        self.comment_body: str | None = None
        self.comment_app: dict[str, Any] | None = None
        self.repository_id = REPO_ID
        self.issue_number = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}":
            return httpx.Response(200, json={"id": self.repository_id, "full_name": REPO})
        if path.startswith(f"/repos/{REPO}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "id": comment_id,
                    "body": self.comment_body,
                    "user": {"id": SENDER_ID, "login": SENDER, "type": "User"},
                    "performed_via_github_app": self.comment_app,
                    "issue_url": (
                        f"https://api.github.com/repos/{REPO}/issues/{self.issue_number}"
                    ),
                },
            )
        if path.startswith(f"/repos/{REPO}/issues/"):
            number = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "number": number,
                    "state": self.issue_state,
                    "labels": [{"name": name} for name in self.labels],
                },
            )
        if path == f"/repos/{REPO}/collaborators/{SENDER}/permission":
            return httpx.Response(
                200,
                json={
                    "permission": self.permission,
                    "user": {"id": self.permission_user_id, "login": SENDER},
                },
            )
        return httpx.Response(404, json={"message": "missing fixture"})


class _Credentials:
    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id != INSTALLATION_ID:
            raise GitHubInstallationRefused("installation was not rediscovered")
        return "fixture-installation-token"


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


def _signature(body: bytes) -> str:
    secret = get_settings().github_webhook_secret.encode()
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _post(
    client: TestClient,
    event: str,
    payload: dict[str, Any],
    *,
    delivery: str | None = None,
    signature: str | None = None,
) -> httpx.Response:
    body = json.dumps(payload).encode()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": delivery or str(uuid.uuid4()),
            "X-GitHub-Event": event,
            "X-Hub-Signature-256": signature if signature is not None else _signature(body),
            "Content-Type": "application/json",
        },
    )


def _sender(
    sender_type: str = "User", login: str = SENDER, sender_id: int = SENDER_ID
) -> dict[str, Any]:
    return {"id": sender_id, "login": login, "type": sender_type}


def _issue_event(action: str, number: int, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": action,
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _sender(),
        "issue": {
            "number": number,
            "state": "open",
            "body": "do not store this issue body",
        },
    }
    payload.update(extra)
    return payload


def _comment_event(number: int, body: str, comment_id: int) -> dict[str, Any]:
    return _issue_event(
        "created",
        number,
        comment={
            "id": comment_id,
            "body": body,
            "user": _sender(),
            "performed_via_github_app": None,
        },
    )


@pytest.fixture
def github_api() -> GitHubAPI:
    return GitHubAPI()


@pytest.fixture
def factory_app(monkeypatch: pytest.MonkeyPatch, clean_db: None, github_api: GitHubAPI) -> Any:
    for key, value in _ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    monkeypatch.setattr(
        "curie_api.github_factory.credentials_for",
        lambda _settings: _Credentials(),
    )
    with TestClient(create_app()) as client:
        external = httpx.AsyncClient(transport=httpx.MockTransport(github_api.handle))
        client.app.state.http_client = external
        headers = {"X-API-Key": get_settings().api_key}
        created = client.post(
            "/agents",
            headers=headers,
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert created.status_code == 201, created.text
        yield client, github_api
        client.portal.call(external.aclose)
    get_settings.cache_clear()


def _code(response: httpx.Response) -> str:
    body = response.json()
    errors = body.get("errors") or []
    if errors:
        return str(errors[0]["code"])
    detail = body.get("detail")
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return str(detail["code"])
    raise AssertionError(body)


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.status, r.terminal_cause, r.objective, r.requester, "
        "r.reply_kind, w.id AS work_item_id, w.cancelled_at, "
        "w.publication_lineage_id, w.github_issue_number "
        "FROM curie.work_items w "
        "LEFT JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number "
        "ORDER BY r.sequence",
        {"repo": REPO_ID, "number": number},
    )


def test_signed_label_admits_one_waiting_work_item_and_redelivery_does_not(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    """Admission is durable in SQL. The runs consumer group is not required."""

    client, _api = factory_app
    number = next(_ISSUES)
    payload = _issue_event("labeled", number, label={"name": LABEL})
    delivery = str(uuid.uuid4())

    first = _post(client, "issues", payload, delivery=delivery)
    second = _post(client, "issues", payload, delivery=delivery)

    assert first.status_code == 200, first.text
    assert first.json()["status"] == "factory_admitted"
    assert second.status_code == 200, second.text
    assert second.json()["status"] == "factory_duplicate"
    rows = _requests(number)
    assert len(rows) == 1
    assert rows[0]["status"] == "waiting"
    assert rows[0]["reply_kind"] == "github"
    assert rows[0]["objective"] == f"https://github.com/{REPO}/issues/{number}"
    assert "do not store this issue body" not in rows[0]["objective"]
    assert rows[0]["requester"] == f"github:{SENDER_ID}:{SENDER}"


def test_bad_signature_is_rejected_before_a_work_item_exists(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, _api = factory_app
    number = next(_ISSUES)
    response = _post(
        client,
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        signature="sha256=" + "0" * 64,
    )

    assert response.status_code == 401, response.text
    assert _requests(number) == []


def test_factory_disabled_issues_stay_ignored(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-factory-hmac-secret")
    get_settings.cache_clear()
    body = b'{"action":"labeled"}'
    response = client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": _signature(body),
            "Content-Type": "application/json",
        },
    )
    get_settings.cache_clear()

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"


def test_unknown_installation_does_not_admit(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, _api = factory_app
    number = next(_ISSUES)
    payload = _issue_event("labeled", number, label={"name": LABEL})
    payload["installation"] = {"id": INSTALLATION_ID + 9}

    response = _post(client, "issues", payload)

    assert response.status_code == 200, response.text
    assert _code(response) == "installation_unverified"
    assert _requests(number) == []


def test_repository_outside_the_allowlist_does_not_admit(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, _api = factory_app
    number = next(_ISSUES)
    payload = _issue_event("labeled", number, label={"name": LABEL})
    payload["repository"] = {"id": 999001, "full_name": OTHER_REPO}

    response = _post(client, "issues", payload)

    assert response.status_code == 200, response.text
    assert _code(response) == "repository_not_allowed"
    assert _rows("SELECT id FROM curie.work_items WHERE github_repository_id = 999001") == []


def test_sender_without_write_permission_does_not_admit(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    api.permission = "read"
    number = next(_ISSUES)

    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))

    assert response.status_code == 200, response.text
    assert _code(response) == "sender_permission_refused"
    assert _requests(number) == []


def test_ordinary_comment_and_app_sender_do_not_execute(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    admitted = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert admitted.json()["status"] == "factory_admitted"

    ordinary = _post(
        client,
        "issue_comment",
        _comment_event(number, "thanks, looking later", 7001),
    )
    app = _post(
        client,
        "issues",
        _issue_event(
            "labeled",
            number,
            label={"name": LABEL},
            sender=_sender("Bot", "curie", 4242),
        ),
    )

    assert ordinary.status_code == 200, ordinary.text
    assert _code(ordinary) == "ordinary_comment"
    assert app.status_code == 200, app.text
    assert _code(app) == "app_authored"
    assert len(_requests(number)) == 1


def test_authorized_mention_waits_until_the_active_request_finishes(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    assert (
        _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL})).json()[
            "status"
        ]
        == "factory_admitted"
    )
    api.comment_body = f"Please revise @{MENTION}"
    mention = _post(
        client,
        "issue_comment",
        _comment_event(number, api.comment_body, 7002),
    )

    assert mention.status_code == 200, mention.text
    assert _code(mention) == "active_request"
    assert len(_requests(number)) == 1


def test_label_removal_cancels_waiting_work_and_blocks_publication(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    assert (
        _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL})).json()[
            "status"
        ]
        == "factory_admitted"
    )
    api.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))

    assert removed.status_code == 200, removed.text
    assert removed.json()["status"] == "factory_cancelled"
    rows = _requests(number)
    assert rows[0]["status"] == "cancelled"
    assert rows[0]["terminal_cause"] == "issue_cancelled"
    assert rows[0]["cancelled_at"] is not None
    assert rows[0]["publication_lineage_id"] is None
    _assert_publication_refused(rows[0]["work_item_id"])

    api.comment_body = f"Again @{MENTION}"
    api.issue_state = "open"
    later = _post(client, "issue_comment", _comment_event(number, api.comment_body, 7003))
    assert _code(later) == "work_item_cancelled"
    assert len(_requests(number)) == 1


def test_closure_requests_termination_until_the_runtime_confirms_it(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    assert (
        _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL})).json()[
            "status"
        ]
        == "factory_admitted"
    )
    request_id = uuid.UUID(str(_requests(number)[0]["id"]))
    _mark_running(request_id)
    api.issue_state = "closed"
    closed = _post(client, "issues", _issue_event("closed", number))

    assert closed.status_code == 200, closed.text
    assert closed.json()["status"] == "factory_cancellation_requested"
    rows = _requests(number)
    assert rows[0]["status"] == "cancellation_requested"
    assert rows[0]["terminal_cause"] == "issue_cancelled"
    assert rows[0]["publication_lineage_id"] is None
    _assert_publication_refused(rows[0]["work_item_id"])


def test_closure_after_the_channel_moves_still_cancels(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    assert (
        _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL})).json()[
            "status"
        ]
        == "factory_admitted"
    )
    agent_id = _rows(
        "SELECT id FROM curie.agents WHERE repo_full_name = :repo",
        {"repo": REPO},
    )[0]["id"]
    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "github", "address": REPO},
        headers={"X-API-Key": get_settings().api_key},
        json={"kind": "github", "address": "acme-corp/other-bot"},
    )
    assert moved.status_code == 200, moved.text
    api.issue_state = "closed"
    closed = _post(client, "issues", _issue_event("closed", number))

    assert closed.status_code == 200, closed.text
    assert closed.json()["status"] == "factory_cancelled"
    assert _requests(number)[0]["status"] == "cancelled"


class _PermissionGate(httpx.AsyncBaseTransport):
    """Block the first permission read so a closure can arrive mid-admission."""

    def __init__(self, api: GitHubAPI, entered: asyncio.Event, release: asyncio.Event) -> None:
        self._api = api
        self._entered = entered
        self._release = release
        self._blocked = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permission") and not self._blocked:
            self._blocked = True
            self._entered.set()
            await self._release.wait()
        return self._api.handle(request)


def test_closure_during_admission_waits_and_then_cancels(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    _client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    label_delivery = str(uuid.uuid4())
    close_delivery = str(uuid.uuid4())

    async def go() -> tuple[Any, Any]:
        engine = create_async_engine(get_settings().database_url)
        entered = asyncio.Event()
        release = asyncio.Event()
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with httpx.AsyncClient(
                transport=_PermissionGate(api, entered, release),
                base_url="https://api.github.com",
            ) as http:
                async with sessions() as admit_session, sessions() as cancel_session:
                    admit_task = asyncio.create_task(
                        handle_factory_delivery(
                            admit_session,
                            settings=get_settings(),
                            client=http,
                            event="issues",
                            delivery_id=label_delivery,
                            body=json.dumps(
                                _issue_event("labeled", number, label={"name": LABEL})
                            ).encode(),
                            payload=_issue_event("labeled", number, label={"name": LABEL}),
                        )
                    )
                    await asyncio.wait_for(entered.wait(), timeout=5)
                    api.issue_state = "closed"
                    api.labels = []
                    cancel_task = asyncio.create_task(
                        handle_factory_delivery(
                            cancel_session,
                            settings=get_settings(),
                            client=http,
                            event="issues",
                            delivery_id=close_delivery,
                            body=json.dumps(_issue_event("closed", number)).encode(),
                            payload=_issue_event("closed", number),
                        )
                    )
                    done, _pending = await asyncio.wait({cancel_task}, timeout=1)
                    still_waiting = cancel_task not in done
                    release.set()
                    admitted, cancelled = await asyncio.wait_for(
                        asyncio.gather(admit_task, cancel_task), timeout=10
                    )
                    assert still_waiting
                    return admitted, cancelled
        finally:
            release.set()
            await engine.dispose()

    admitted, cancelled = asyncio.run(go())

    assert admitted.status == "factory_admitted"
    assert cancelled.status == "factory_cancelled"
    assert _requests(number)[0]["status"] == "cancelled"


def test_pull_request_comment_stays_on_the_review_arm(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, _api = factory_app
    number = next(_ISSUES)
    payload = _comment_event(number, f"@{MENTION}", 7004)
    payload["issue"]["pull_request"] = {}

    response = _post(client, "issue_comment", payload)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "feedback_disabled"
    assert _requests(number) == []


def test_concurrent_label_deliveries_leave_one_active_execution(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    payload = _issue_event("labeled", number, label={"name": LABEL})
    responses: list[httpx.Response] = []
    errors: list[BaseException] = []

    def post_once() -> None:
        try:
            responses.append(_post(client, "issues", payload, delivery=str(uuid.uuid4())))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=post_once) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert [response.status_code for response in responses] == [200, 200]
    statuses = [response.json()["status"] for response in responses]
    assert statuses == ["factory_admitted", "factory_admitted"]
    rows = _requests(number)
    assert len({row["work_item_id"] for row in rows}) == 1
    assert [row["status"] for row in rows].count("waiting") == 1


def _mark_running(request_id: uuid.UUID) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(session, request_id, owner="factory-owner", generation=1)
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="factory-owner",
                    generation=1,
                    claim_name="claim-factory",
                    sandbox_name="sbx-factory",
                )
                assert not isinstance(started, DispatchConflict), started
        finally:
            await engine.dispose()

    asyncio.run(go())


def _assert_publication_refused(work_item_id: uuid.UUID) -> None:
    from curie_api import crud

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                row = (
                    await session.execute(
                        text(
                            "SELECT agent_id, conversation_id FROM curie.work_items WHERE id = :id"
                        ),
                        {"id": work_item_id},
                    )
                ).one()
                with pytest.raises(crud.PublicationLineageConflict) as caught:
                    await crud._refuse_fenced_work_item(
                        session,
                        agent_id=row.agent_id,
                        conversation_id=row.conversation_id,
                        request_id=None,
                        runtime_epoch=None,
                    )
                assert caught.value.code == "publication.work_item_cancelled"
        finally:
            await engine.dispose()

    asyncio.run(go())
