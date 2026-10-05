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
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.forges.github.identity import _issue_lock_keys
from curie_api.github_factory import (
    admit_notice,
    handle_factory_delivery,
    lock_issue,
    verify_current,
)
from curie_api.github_factory_events import parse_factory_event
from curie_api.main import create_app
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from fastapi.testclient import TestClient
from forge_fakes.github import (
    _ENV,
    INSTALLATION_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubAPI,
    _comment_event,
    _Credentials,
    _issue_event,
    _post,
    _sender,
    _signature,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

OTHER_REPO = "attacker/other-bot"
_ISSUES = itertools.count(9100)


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


def _signed_webhook(client: TestClient, secret: str) -> httpx.Response:
    body = b'{"action":"labeled"}'
    signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )


def test_empty_webhook_secret_rejects_its_own_hmac(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            response = _signed_webhook(client, "")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 401, response.text


def test_configured_webhook_secret_accepts_a_valid_signature(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, valkey: object
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("GITHUB_FACTORY_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_REVIEW_INGRESS_ENABLED", "false")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "example-factory-hmac-secret")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            response = _signed_webhook(client, "example-factory-hmac-secret")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"


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


@pytest.mark.parametrize("permission", ["read", "none"])
def test_issue_mention_with_revoked_write_permission_creates_no_revision(
    factory_app: tuple[TestClient, GitHubAPI], permission: str
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    admitted = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert admitted.status_code == 200, admitted.text
    assert admitted.json()["status"] == "factory_admitted"
    before = _rows("SELECT * FROM curie.execution_requests ORDER BY id")
    assert len(before) == 1
    assert len(api.permission_requests) == 1

    api.permission = permission
    api.comment_body = f"Please revise @{MENTION}"
    payload = _comment_event(number, api.comment_body, 7005)
    payload["comment"]["author_association"] = "NONE"
    mention = _post(client, "issue_comment", payload)

    assert mention.status_code == 200, mention.text
    assert mention.json()["status"] == "factory_ignored", mention.text
    assert _code(mention) == "sender_permission_refused"
    assert [request.headers["Authorization"] for request in api.permission_requests] == [
        "Bearer fixture-installation-token",
        "Bearer fixture-installation-token",
    ]
    assert _rows("SELECT * FROM curie.execution_requests ORDER BY id") == before


@pytest.mark.parametrize("permission", ["write", "admin"])
def test_authorized_issue_mention_is_queued_while_a_request_is_active(
    factory_app: tuple[TestClient, GitHubAPI], permission: str
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
    api.permission = permission
    api.comment_body = f"Please revise @{MENTION}"
    payload = _comment_event(number, api.comment_body, 7002)
    payload["comment"]["author_association"] = "NONE"
    delivery = str(uuid.uuid4())
    mention = _post(
        client,
        "issue_comment",
        payload,
        delivery=delivery,
    )
    same = _post(client, "issue_comment", payload, delivery=delivery)
    redelivery = _post(client, "issue_comment", payload, delivery=str(uuid.uuid4()))

    assert mention.status_code == 200, mention.text
    assert mention.json()["status"] == "factory_queued", mention.text
    assert same.json()["status"] == "factory_duplicate", same.text
    assert redelivery.json()["status"] == "factory_duplicate", redelivery.text
    rows = _requests(number)
    assert [row["status"] for row in rows] == ["waiting", "queued"]
    assert rows[1]["work_item_id"] == rows[0]["work_item_id"]
    assert rows[1]["objective"].endswith("#issuecomment-7002")


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


def test_admission_retains_issue_lock_until_caller_commit(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    number = next(_ISSUES)
    notice = parse_factory_event(
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        str(uuid.uuid4()),
        label=LABEL,
        mention=MENTION,
    )
    classid, objid = _issue_lock_keys(REPO_ID, number)
    _, api = factory_app

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with (
                sessions() as session,
                engine.connect() as observer,
                httpx.AsyncClient(transport=httpx.MockTransport(api.handle)) as github,
            ):
                caller_pid = await session.scalar(text("SELECT pg_backend_pid()"))
                await lock_issue(session, REPO_ID, number)
                verified = await verify_current(notice, settings=get_settings(), client=github)
                admitted = await admit_notice(session, notice, get_settings(), verified, github)
                assert admitted.status == "factory_admitted", admitted
                assert not await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM curie.work_items "
                        "WHERE github_repository_id = :repo AND github_issue_number = :number)"
                    ),
                    {"repo": REPO_ID, "number": number},
                )
                request_visible = text(
                    "SELECT EXISTS (SELECT 1 FROM curie.execution_requests WHERE id = :request_id)"
                )
                assert not await observer.scalar(request_visible, {"request_id": notice.request_id})
                assert await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' "
                        "AND granted AND pid = :caller_pid AND objsubid = 2 "
                        "AND classid::bigint = :classid AND objid::bigint = :objid "
                        "AND database = (SELECT oid FROM pg_database "
                        "WHERE datname = current_database()))"
                    ),
                    {
                        "caller_pid": caller_pid,
                        "classid": classid & 0xFFFFFFFF,
                        "objid": objid & 0xFFFFFFFF,
                    },
                )
                await session.commit()
                assert await observer.scalar(request_visible, {"request_id": notice.request_id})
        finally:
            await engine.dispose()

    asyncio.run(go())


class _PermissionGate(httpx.AsyncBaseTransport):
    """Block the first permission read so a closure can arrive mid-admission."""

    def __init__(self, api: GitHubAPI, entered: threading.Event, release: asyncio.Event) -> None:
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
        entered = threading.Event()
        release = asyncio.Event()
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with httpx.AsyncClient(
                transport=_PermissionGate(api, entered, release),
                base_url="https://api.github.com",
            ) as http:
                async with (
                    sessions() as admit_session,
                    sessions() as cancel_session,
                    asyncio.TaskGroup() as deliveries,
                ):
                    admit_pid = await admit_session.scalar(text("SELECT pg_backend_pid()"))
                    cancel_pid = await cancel_session.scalar(text("SELECT pg_backend_pid()"))
                    admit_task = deliveries.create_task(
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
                    assert await asyncio.to_thread(entered.wait, 5)
                    api.issue_state = "closed"
                    api.labels = []
                    cancel_task = deliveries.create_task(
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
                    # Observe the real issue lock, rather than inferring waiting
                    # from how often the scheduler runs the cancellation task.
                    async with engine.connect() as observer, asyncio.timeout(5):
                        while not await observer.scalar(
                            text("SELECT :admit_pid = ANY(pg_blocking_pids(:cancel_pid))"),
                            {"admit_pid": admit_pid, "cancel_pid": cancel_pid},
                        ):
                            await asyncio.sleep(0.01)
                    assert not cancel_task.done()
                    release.set()
                    admitted, cancelled = await asyncio.wait_for(
                        asyncio.gather(admit_task, cancel_task), timeout=10
                    )
                    return admitted, cancelled
        finally:
            release.set()
            await engine.dispose()

    admitted, cancelled = asyncio.run(go())

    assert admitted.status == "factory_admitted", admitted
    assert cancelled.status == "factory_cancelled", cancelled
    assert _requests(number)[0]["status"] == "cancelled"


def test_signed_closure_during_admission_waits_and_then_cancels(
    factory_app: tuple[TestClient, GitHubAPI],
) -> None:
    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    entered = threading.Event()
    release = asyncio.Event()
    classid, objid = _issue_lock_keys(REPO_ID, number)

    async def observe_cancellation_blocked() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as observer, asyncio.timeout(5):
                while not await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks waiting "
                        "JOIN pg_locks holder ON holder.locktype = waiting.locktype "
                        "AND holder.database = waiting.database "
                        "AND holder.classid = waiting.classid "
                        "AND holder.objid = waiting.objid "
                        "AND holder.objsubid = waiting.objsubid "
                        "WHERE waiting.locktype = 'advisory' AND NOT waiting.granted "
                        "AND holder.granted AND waiting.objsubid = 2 "
                        "AND waiting.database = (SELECT oid FROM pg_database "
                        "WHERE datname = current_database()) "
                        "AND waiting.classid::bigint = :classid "
                        "AND waiting.objid::bigint = :objid "
                        "AND holder.pid = ANY(pg_blocking_pids(waiting.pid)))"
                    ),
                    {"classid": classid & 0xFFFFFFFF, "objid": objid & 0xFFFFFFFF},
                ):
                    await asyncio.sleep(0.01)
        finally:
            await engine.dispose()

    previous = client.app.state.http_client
    github = httpx.AsyncClient(transport=_PermissionGate(api, entered, release))
    client.app.state.http_client = github
    try:
        with ThreadPoolExecutor(max_workers=2) as deliveries:
            try:
                admission = deliveries.submit(
                    _post, client, "issues", _issue_event("labeled", number, label={"name": LABEL})
                )
                assert entered.wait(5)
                api.issue_state = "closed"
                api.labels = []
                cancellation = deliveries.submit(
                    _post, client, "issues", _issue_event("closed", number)
                )
                asyncio.run(observe_cancellation_blocked())
                assert not cancellation.done()
                client.portal.call(release.set)
                admitted = admission.result(timeout=10)
                cancelled = cancellation.result(timeout=10)
            finally:
                client.portal.call(release.set)
    finally:
        client.app.state.http_client = previous
        client.portal.call(github.aclose)

    assert admitted.status_code == 200, admitted.text
    assert admitted.json()["status"] == "factory_admitted", admitted.text
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "factory_cancelled", cancelled.text
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
    body = json.dumps(payload).encode()

    async def go() -> list[str]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def deliver() -> str:
            async with maker() as session:
                result = await handle_factory_delivery(
                    session,
                    settings=get_settings(),
                    client=client.app.state.http_client,
                    event="issues",
                    delivery_id=str(uuid.uuid4()),
                    body=body,
                    payload=payload,
                )
            return result.status

        try:
            return list(await asyncio.gather(deliver(), deliver()))
        finally:
            await engine.dispose()

    statuses = asyncio.run(go())
    # Both deliveries name the same timeline event, so they share one request.
    assert "factory_admitted" in statuses, statuses
    assert set(statuses) <= {"factory_admitted", "factory_duplicate"}
    rows = _requests(number)
    assert len({row["work_item_id"] for row in rows}) == 1
    assert [row["status"] for row in rows].count("waiting") == 1, (statuses, rows)
    assert len({row["id"] for row in rows}) == 1


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
    from curie_api.crud import errors as crud_errors
    from curie_api.crud import publications as crud_publications

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
                with pytest.raises(crud_errors.PublicationLineageConflict) as caught:
                    await crud_publications._refuse_fenced_work_item(
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


# --- #3097 AC5: a factory push to its own PR starts no execution request.
#
# Payload shapes follow GitHub's webhook catalog:
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#push
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#check_run
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#check_suite
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#status

_FIX_HEAD = "b2" * 20
_LINEAGE_BRANCH = f"curie/publication-{uuid.UUID(int=3097).hex}"


def _fix_push_deliveries(number: int) -> list[tuple[str, dict[str, Any]]]:
    repository = {
        "id": REPO_ID,
        "full_name": REPO,
        "clone_url": f"https://github.com/{REPO}.git",
    }
    bot = _sender("Bot", "curie-factory[bot]", 7701)
    pull_request = {
        "number": 77,
        "state": "open",
        "head": {"ref": _LINEAGE_BRANCH, "sha": _FIX_HEAD},
        "base": {"ref": "main"},
        "body": f"Closes #{number}",
    }
    installation = {"id": INSTALLATION_ID}
    return [
        (
            "push",
            {
                "ref": f"refs/heads/{_LINEAGE_BRANCH}",
                "before": "a1" * 20,
                "after": _FIX_HEAD,
                "repository": repository,
                "installation": installation,
                "sender": bot,
                "commits": [{"id": _FIX_HEAD, "message": "Fix the failing check"}],
            },
        ),
        (
            "pull_request",
            {
                "action": "synchronize",
                "number": 77,
                "pull_request": pull_request,
                "repository": repository,
                "installation": installation,
                "sender": bot,
            },
        ),
        (
            "check_run",
            {
                "action": "completed",
                "check_run": {
                    "id": 9001,
                    "name": "unit-tests",
                    "head_sha": _FIX_HEAD,
                    "status": "completed",
                    "conclusion": "failure",
                },
                "repository": repository,
                "installation": installation,
                "sender": bot,
            },
        ),
        (
            "check_suite",
            {
                "action": "completed",
                "check_suite": {
                    "id": 9101,
                    "head_sha": _FIX_HEAD,
                    "status": "completed",
                    "conclusion": "failure",
                },
                "repository": repository,
                "installation": installation,
                "sender": bot,
            },
        ),
        (
            "status",
            {
                "sha": _FIX_HEAD,
                "state": "failure",
                "context": "ci/jenkins",
                "repository": repository,
                "installation": installation,
                "sender": bot,
            },
        ),
    ]


@pytest.mark.parametrize("event", ["push", "pull_request", "check_run", "check_suite", "status"])
def test_a_factory_fix_push_and_its_ci_events_start_no_execution_request(
    factory_app: tuple[TestClient, GitHubAPI], event: str
) -> None:
    """Already true on the base: the CI fix loop relies on it, so it is pinned."""

    client, api = factory_app
    number = next(_ISSUES)
    api.issue_number = number
    admitted = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert admitted.json()["status"] == "factory_admitted"
    before = _rows("SELECT id FROM curie.execution_requests")

    (payload,) = [body for name, body in _fix_push_deliveries(number) if name == event]
    response = _post(client, event, payload)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"
    assert _rows("SELECT id FROM curie.execution_requests") == before
    assert len(_requests(number)) == 1
