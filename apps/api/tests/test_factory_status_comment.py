"""One bot-authored status comment per request, edited in place (#3077).

The real reconciler drives the threaded GitHub fake from
test_factory_terminus.py, which records every comment create, comment edit,
label add and label removal it receives:
https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue

Admission is a signed issues webhook; progress arrives through the real
``/v1/work-item-progress`` route. Machine fixtures drive the events.
"""

from __future__ import annotations

import asyncio
import logging
import re
import socket
import sys
import uuid
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_notices import FINAL_MARKER, marker_for, result_section, sync_status_comments
from curie_api.github_app import GitHubAppError
from curie_api.workitem_dispatch import DispatchConflict, acquire, defer
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_factory_progress import ACTIVITY, DECLARATION, STAGED_DECLARATION, progress_token, report
from test_factory_terminus import (  # noqa: F401  (fixtures)
    _LABELS,
    HEAD_A,
    REPO,
    _attach_publication,
    _attach_revision_publication,
    _Credentials,
    _insert_revision,
    _notices,
    _patches,
    _posts,
    _published_issue,
    _reconcile,
    _request,
    _revision_objective,
    _rows,
    _set_base_ref,
    _start_running,
    admitted,
    check_run,
    ci_entry,
    comments,
)
from test_github_factory_ingress import LABEL, _issue_event, _post

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}
STATE_LABELS = {
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
}
WAITING = "_Waiting for the agent to report progress._"
CARD_BASE = "https://curie.example.com"


def _writes(sink: Any) -> list[tuple[str, str, str | None]]:
    return [r for r in sink.requests if r[0] in {"POST", "PATCH", "DELETE"}]


def _label_writes(sink: Any) -> list[tuple[str, str, str | None]]:
    return [r for r in sink.requests if r[0] in {"POST", "DELETE"} and _LABELS.match(r[1])]


def _curie_labels(sink: Any, number: int) -> set[str]:
    return sink.issue_labels.get(number, set()) & STATE_LABELS


def _marked(sink: Any, request_id: uuid.UUID) -> list[dict[str, Any]]:
    everything = list(sink.comments) + [c for listed in sink.lists.values() for c in listed]
    return [c for c in everything if marker_for(request_id) in (c.get("body") or "")]


def _admit(client: Any, github: Any, sink: Any, number: int) -> uuid.UUID:
    github.issue_number = number
    github.labels = [LABEL]
    github.advance_label_event(number)
    sink.issue_labels[number] = {LABEL, "bug"}
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.json()["status"] == "factory_admitted", response.text
    return _request(number)["id"]


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _finish_failed(
    client: Any, request_id: uuid.UUID, epoch: int, cause: str, detail: str | None = None
) -> None:
    body: dict[str, Any] = {"runtime_epoch": epoch, "outcome": "failed", "cause": cause}
    if detail is not None:
        body["detail"] = detail
    finished = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish", headers=WORKER, json=body
    )
    assert finished.status_code == 200, finished.text


@asynccontextmanager
async def _blocked_status_sync(
    client: Any, sink: Any, number: int, *, path: str | None = None
) -> AsyncIterator[tuple[asyncio.Task[None], asyncio.Event]]:
    """Hold the pass on its GET of ``path``, by default the issue read."""

    entered, released = asyncio.Event(), asyncio.Event()
    sink.get_barrier = (
        path or f"/repos/{REPO}/issues/{number}",
        asyncio.get_running_loop(),
        entered,
        released,
    )
    task = asyncio.create_task(client.app.state.work_item_reconciler._sync_status_comments())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        yield task, released
    finally:
        released.set()
        sink.get_barrier = None
        await asyncio.wait_for(task, 10)


def test_blocked_github_has_no_idle_transaction_and_progress_completes(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9951
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)

    async def go() -> None:
        inspector = create_async_engine(get_settings().database_url)
        try:
            async with _blocked_status_sync(client, sink, number) as (sync, _released):
                assert client.app.state.engine.pool.checkedout() == 0
                async with inspector.connect() as connection:
                    idle = (
                        await connection.execute(
                            text(
                                "SELECT pid, query FROM pg_stat_activity "
                                "WHERE datname = current_database() "
                                "AND state = 'idle in transaction' AND pid <> pg_backend_pid()"
                            )
                        )
                    ).all()
                assert idle == [], idle
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
                ) as api:
                    response = await asyncio.wait_for(
                        api.post(
                            f"/v1/work-item-progress/{request_id}",
                            headers={"X-API-Key": progress_token(request_id)},
                            json={
                                "phase": "read_issue",
                                "declaration": DECLARATION,
                                "activity": ACTIVITY,
                            },
                        ),
                        2,
                    )
                    assert response.status_code == 201, response.text
                    changed = {
                        **DECLARATION,
                        "phases": [*DECLARATION["phases"], {"id": "extra", "label": "Extra"}],
                    }
                    rejected = await asyncio.wait_for(
                        api.post(
                            f"/v1/work-item-progress/{request_id}",
                            headers={"X-API-Key": progress_token(request_id)},
                            json={
                                "phase": "read_issue",
                                "declaration": changed,
                                "activity": ACTIVITY,
                            },
                        ),
                        2,
                    )
                    assert rejected.status_code == 409, rejected.text
                    assert "declaration_changed" in rejected.text
                assert not sync.done(), "GitHub must still be blocked when progress returns"
        finally:
            await inspector.dispose()

    client.portal.call(go)
    row = _rows(
        "SELECT declaration, activity FROM curie.factory_terminal_notices "
        "WHERE execution_request_id = :id",
        {"id": request_id},
    )[0]
    assert row["declaration"] == DECLARATION
    assert row["activity"] == ACTIVITY


def test_blocked_github_does_not_delay_heartbeat_or_relax_epoch_fencing(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9952
    request_id = _admit(client, github, sink, number)
    epoch = _start_running(request_id)

    async def go() -> None:
        async with _blocked_status_sync(client, sink, number) as (sync, _released):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
            ) as api:
                path = f"/v1/internal/work-items/requests/{request_id}/heartbeat"
                heartbeat = await asyncio.wait_for(
                    api.post(path, headers=WORKER, json={"runtime_epoch": epoch}), 2
                )
                assert heartbeat.status_code == 200, heartbeat.text
                assert heartbeat.json()["status"] == "running"
                stale = await asyncio.wait_for(
                    api.post(path, headers=WORKER, json={"runtime_epoch": epoch + 1}), 2
                )
                assert stale.status_code == 409, stale.text
                assert "stale_owner" in stale.text
            assert not sync.done(), "GitHub must still be blocked when the heartbeat returns"

    client.portal.call(go)


def test_concurrent_status_passes_claim_one_row_and_post_one_comment(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9953
    request_id = _admit(client, github, sink, number)

    async def go() -> None:
        async with _blocked_status_sync(client, sink, number) as (first, released):
            second = await asyncio.wait_for(
                sync_status_comments(
                    client.app.state.sessionmaker, get_settings(), owner="status-owner-b", limit=1
                ),
                2,
            )
            assert second == 0
            assert _posts(sink) == []
            assert not first.done()
            released.set()
            await asyncio.wait_for(first, 5)

    client.portal.call(go)
    assert len(_posts(sink)) == 1
    assert len(_marked(sink, request_id)) == 1
    row = _rows(
        "SELECT sync_owner, sync_lease_expires_at, attempts "
        "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )[0]
    assert row == {"sync_owner": None, "sync_lease_expires_at": None, "attempts": 1}


def test_expired_status_claim_is_taken_over_and_old_writeback_is_dropped(
    admitted: Any,  # noqa: F811
    caplog: pytest.LogCaptureFixture,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9954
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    caplog.set_level(logging.INFO, logger="curie_api.factory_notices")

    async def go() -> None:
        maker = client.app.state.sessionmaker
        async with _blocked_status_sync(client, sink, number) as (first, released):
            async with maker() as session:
                owner = await session.scalar(
                    text(
                        "SELECT sync_owner FROM curie.factory_terminal_notices "
                        "WHERE execution_request_id = :id"
                    ),
                    {"id": request_id},
                )
                assert owner.startswith("work-item-reconciler:")
                await session.execute(
                    text(
                        "UPDATE curie.factory_terminal_notices "
                        "SET sync_lease_expires_at = clock_timestamp() - interval '1 second' "
                        "WHERE execution_request_id = :id"
                    ),
                    {"id": request_id},
                )
                await session.commit()
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
            ) as api:
                response = await api.post(
                    f"/v1/work-item-progress/{request_id}",
                    headers={"X-API-Key": progress_token(request_id)},
                    json={"phase": "read_issue", "declaration": DECLARATION, "activity": ACTIVITY},
                )
                assert response.status_code == 201, response.text
            assert (
                await asyncio.wait_for(
                    sync_status_comments(maker, get_settings(), owner="status-owner-b", limit=1), 5
                )
                > 0
            )
            async with maker() as session:
                before = (
                    (
                        await session.execute(
                            text(
                                "SELECT * FROM curie.factory_terminal_notices "
                                "WHERE execution_request_id = :id"
                            ),
                            {"id": request_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                before = dict(before)
            assert before["sync_owner"] is None
            assert before["sync_lease_expires_at"] is None
            assert before["declaration"] == DECLARATION
            assert before["activity"] == ACTIVITY
            assert not first.done()
            released.set()
            await asyncio.wait_for(first, 5)
            async with maker() as session:
                after = (
                    (
                        await session.execute(
                            text(
                                "SELECT * FROM curie.factory_terminal_notices "
                                "WHERE execution_request_id = :id"
                            ),
                            {"id": request_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert dict(after) == before

    client.portal.call(go)
    assert any(
        record.levelno == logging.INFO
        and re.search(r"(?:stale|lost|drop|skip)", record.getMessage(), re.IGNORECASE)
        for record in caplog.records
        if record.name == "curie_api.factory_notices"
    )


def test_state_label_change_reads_once_adds_once_and_removes_only_old_label(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9955
    request_id = _admit(client, github, sink, number)
    _reconcile()
    sink.requests.clear()
    _start_running(request_id)
    _reconcile()
    path = f"/repos/{REPO}/issues/{number}/labels"
    label_calls = [
        (method, called) for method, called, _body in sink.requests if _LABELS.match(called)
    ]
    assert label_calls == [
        ("GET", path),
        ("POST", path),
        ("DELETE", f"{path}/curie-factory:queued"),
    ]
    assert sink.issue_labels[number] == {LABEL, "bug", "curie-factory:running"}
    sink.requests.clear()
    _reconcile()
    assert _label_writes(sink) == []


def test_state_labels_on_later_pages_are_removed_and_human_labels_are_preserved(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9962
    request_id = _admit(client, github, sink, number)
    _reconcile()
    human_labels = {f"aaa-example-label-{index:03d}" for index in range(100)} | {LABEL, "bug"}
    sink.issue_labels[number] = human_labels | {"curie-factory:queued"}
    sink.requests.clear()
    sink.label_pages.clear()
    _start_running(request_id)

    _reconcile()

    pages = [page for issue, page in sink.label_pages if issue == number]
    assert len(pages) >= 2
    assert pages == list(range(1, len(pages) + 1))
    path = f"/repos/{REPO}/issues/{number}/labels"
    assert _label_writes(sink) == [
        ("POST", path, '["curie-factory:running"]'),
        ("DELETE", f"{path}/curie-factory:queued", None),
    ]
    assert sink.issue_labels[number] == human_labels | {"curie-factory:running"}
    assert _notices(request_id)[0]["applied_label"] == "curie-factory:running"


@pytest.mark.parametrize("status", [403, 500])
def test_failed_second_label_page_does_not_write_or_settle_and_next_pass_recovers(
    admitted: Any,  # noqa: F811
    status: int,
) -> None:
    client, github, sink = admitted
    number = 9963
    request_id = _admit(client, github, sink, number)
    _reconcile()
    human_labels = {f"aaa-example-label-{index:03d}" for index in range(100)} | {LABEL, "bug"}
    original = human_labels | {"curie-factory:queued"}
    sink.issue_labels[number] = original.copy()
    sink.label_page_statuses[(number, 2)] = status
    sink.requests.clear()
    sink.label_pages.clear()
    _start_running(request_id)

    _reconcile()

    assert sink.label_pages == [(number, 1), (number, 2)]
    assert _label_writes(sink) == []
    assert sink.issue_labels[number] == original
    assert _notices(request_id)[0]["applied_label"] == "curie-factory:queued"

    sink.label_page_statuses.clear()
    sink.requests.clear()
    sink.label_pages.clear()
    _reconcile()

    pages = [page for issue, page in sink.label_pages if issue == number]
    assert len(pages) >= 2
    assert pages == list(range(1, len(pages) + 1))
    assert sink.issue_labels[number] == human_labels | {"curie-factory:running"}
    assert _notices(request_id)[0]["applied_label"] == "curie-factory:running"
    assert len(_label_writes(sink)) == 2


def test_ready_and_heartbeat_use_liveness_pool_when_all_main_connections_are_busy(
    admitted: Any,  # noqa: F811
) -> None:  # noqa: F811
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 9956)
    epoch = _start_running(request_id)

    async def go() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
        ) as api:
            path = f"/v1/internal/work-items/requests/{request_id}/heartbeat"
            async with AsyncExitStack() as checked_out:
                for _ in range(15):
                    session = await checked_out.enter_async_context(client.app.state.sessionmaker())
                    await session.execute(text("SELECT 1"))
                assert client.app.state.engine.pool.checkedout() == 15
                ready, heartbeat = await asyncio.wait_for(
                    asyncio.gather(
                        api.get("/ready"),
                        api.post(path, headers=WORKER, json={"runtime_epoch": epoch}),
                    ),
                    2,
                )
                assert ready.status_code == 200, ready.text
                assert heartbeat.status_code == 200, heartbeat.text
                unauthorized = await asyncio.wait_for(
                    api.post(
                        path,
                        headers={"X-Curie-Worker-Token": "wrong-worker"},
                        json={"runtime_epoch": epoch},
                    ),
                    2,
                )
                assert unauthorized.status_code == 401, unauthorized.text
                stale = await asyncio.wait_for(
                    api.post(path, headers=WORKER, json={"runtime_epoch": epoch + 1}), 2
                )
                assert stale.status_code == 409, stale.text
                assert "stale_owner" in stale.text
                assert client.app.state.engine.pool.checkedout() == 15
            assert client.app.state.engine.pool.checkedout() == 0
            # Readiness still requires its own database pool to be available.
            async with AsyncExitStack() as checked_out:
                for _ in range(4):
                    session = await checked_out.enter_async_context(
                        client.app.state.liveness_sessionmaker()
                    )
                    await session.execute(text("SELECT 1"))
                unavailable = await asyncio.wait_for(api.get("/ready"), 3)
                assert unavailable.status_code == 503, unavailable.text
            assert (await asyncio.wait_for(api.get("/ready"), 2)).status_code == 200

    client.portal.call(go)


def test_liveness_database_failure_rejects_ready_and_does_not_renew_heartbeat(
    admitted: Any,  # noqa: F811
) -> None:  # noqa: F811
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 9960)
    epoch = _start_running(request_id)

    async def go() -> None:
        async with client.app.state.sessionmaker() as session:
            before = await session.scalar(
                text(
                    "SELECT runtime_heartbeat_expires_at FROM curie.execution_requests "
                    "WHERE id = :id"
                ),
                {"id": request_id},
            )
        # A bound TCP socket without listen reserves a port that refuses
        # connections. This exercises the real database driver failure path.
        with socket.socket() as unavailable_port:
            unavailable_port.bind(("127.0.0.1", 0))
            database = make_url(get_settings().database_url).set(
                host="127.0.0.1", port=unavailable_port.getsockname()[1]
            )
            unavailable = create_async_engine(database, connect_args={"timeout": 0.25})
            original = client.app.state.liveness_sessionmaker
            client.app.state.liveness_sessionmaker = async_sessionmaker(unavailable)
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=client.app, raise_app_exceptions=False),
                    base_url="http://testserver",
                ) as api:
                    ready = await asyncio.wait_for(api.get("/ready"), 2)
                    assert ready.status_code == 503, ready.text
                    heartbeat = await asyncio.wait_for(
                        api.post(
                            f"/v1/internal/work-items/requests/{request_id}/heartbeat",
                            headers=WORKER,
                            json={"runtime_epoch": epoch},
                        ),
                        2,
                    )
                    assert heartbeat.status_code == 500, heartbeat.text
            finally:
                client.app.state.liveness_sessionmaker = original
                await unavailable.dispose()
        async with client.app.state.sessionmaker() as session:
            after = await session.scalar(
                text(
                    "SELECT runtime_heartbeat_expires_at FROM curie.execution_requests "
                    "WHERE id = :id"
                ),
                {"id": request_id},
            )
        assert after == before

    client.portal.call(go)


def test_credential_mint_failure_releases_status_claim_and_next_pass_retries(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 9961)

    class RefusedCredentials(_Credentials):
        def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
            raise GitHubAppError("fixture installation token mint refused")

    async def go() -> None:
        monkeypatch.setattr(
            "curie_api.factory_notices.credentials_for", lambda _settings: RefusedCredentials()
        )
        assert (
            await sync_status_comments(
                client.app.state.sessionmaker, get_settings(), owner="retry-owner", limit=1
            )
            == 0
        )
        async with client.app.state.sessionmaker() as session:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT attempts, sync_owner, sync_lease_expires_at, refused_at "
                            "FROM curie.factory_terminal_notices WHERE execution_request_id = :id"
                        ),
                        {"id": request_id},
                    )
                )
                .mappings()
                .one()
            )
        assert dict(row) == {
            "attempts": 1,
            "sync_owner": None,
            "sync_lease_expires_at": None,
            "refused_at": None,
        }
        assert sink.requests == []
        monkeypatch.setattr(
            "curie_api.factory_notices.credentials_for", lambda _settings: _Credentials()
        )
        assert (
            await sync_status_comments(
                client.app.state.sessionmaker, get_settings(), owner="retry-owner", limit=1
            )
            > 0
        )

    client.portal.call(go)
    assert len(_posts(sink)) == 1
    assert _notices(request_id)[0]["attempts"] == 2


def test_slow_github_call_logs_one_sanitized_warning_and_row_summary(
    admitted: Any,  # noqa: F811
    caplog: pytest.LogCaptureFixture,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9957
    _admit(client, github, sink, number)
    caplog.set_level(logging.INFO, logger="curie_api.factory_notices")

    async def go() -> None:
        async with _blocked_status_sync(client, sink, number) as (sync, released):
            await asyncio.sleep(2.05)
            released.set()
            await asyncio.wait_for(sync, 5)

    client.portal.call(go)
    warnings = [
        record
        for record in caplog.records
        if record.name == "curie_api.factory_notices" and record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "GET" in message
    assert "200" in message
    assert "elapsed" in message
    assert "{" in message and "}" in message
    assert str(number) not in message
    assert REPO not in message
    assert "ghs_factory_terminus_fixture" not in caplog.text
    summaries = [
        record.getMessage()
        for record in caplog.records
        if record.name == "curie_api.factory_notices"
        and record.levelno == logging.INFO
        and "elapsed" in record.getMessage()
        and "call" in record.getMessage()
    ]
    assert len(summaries) == 1


def _reconcile_later(seconds: int) -> None:
    """Advance only the reconciler clock. Stored deadlines stay write-once."""

    import curie_api.workitems as workitems

    original = workitems._database_now

    async def later(session: AsyncSession) -> Any:
        real = await original(session)
        return real + timedelta(seconds=seconds)

    workitems._database_now = later
    try:
        _reconcile()
    finally:
        workitems._database_now = original


def _observe_termination(client: Any, request_id: uuid.UUID) -> None:
    claimed = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=WORKER,
        json={"owner": "factory-owner"},
    )
    assert claimed.status_code == 200, claimed.text
    recorded = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination",
        headers=WORKER,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "runtime stopped",
        },
    )
    assert recorded.status_code == 200, recorded.text


# --- 1: created at admission ------------------------------------------------------


def test_admission_creates_one_queued_status_comment(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9901
    request_id = _admit(client, github, sink, number)
    _reconcile()

    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    body = posts[0][1] or ""
    assert marker_for(request_id) in body
    assert "Status: QUEUED" in body
    assert WAITING in body
    assert FINAL_MARKER not in body
    assert "![Curie status]" not in body
    assert body.rstrip().endswith(marker_for(request_id))
    assert _curie_labels(sink, number) == {"curie-factory:queued"}
    # Human and admission labels are never touched.
    assert {LABEL, "bug"} <= sink.issue_labels[number]
    row = _notices(request_id)[0]
    assert row["comment_list"] == "issue"
    assert row["finalized_at"] is None


def test_the_card_image_is_linked_when_a_base_url_is_set(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    client, github, sink = admitted
    monkeypatch.setenv("GITHUB_FACTORY_CARD_BASE_URL", CARD_BASE)
    get_settings.cache_clear()
    number = 9902
    request_id = _admit(client, github, sink, number)
    _reconcile()

    token = _notices(request_id)[0]["card_token"]
    body = _posts(sink)[0][1] or ""
    assert f"![Curie status]({CARD_BASE}/v1/factory/cards/{token}.svg)" in body
    assert "width" not in body
    # The card carries the phases, so the comment adds no placeholder (#3125).
    assert WAITING not in body
    assert "Status: QUEUED" in body


def test_a_card_url_replaces_the_checklist_as_phases_advance(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    client, github, sink = admitted
    monkeypatch.setenv("GITHUB_FACTORY_CARD_BASE_URL", CARD_BASE)
    get_settings.cache_clear()
    number = 9912
    request_id = _admit(client, github, sink, number)
    _reconcile()
    token = _notices(request_id)[0]["card_token"]
    sink.requests.clear()

    _start_running(request_id)
    for phase, loop_round in (("read_issue", None), ("plan", 1)):
        assert report(client, request_id, phase, round=loop_round).status_code == 201
    _reconcile()

    body = _patches(sink)[-1][1] or ""
    assert f"![Curie status]({CARD_BASE}/v1/factory/cards/{token}.svg)" in body
    assert "Status: RUNNING" in body
    assert "- [" not in body
    assert "Read issue" not in body
    assert WAITING not in body
    assert body.rstrip().endswith(marker_for(request_id))


# --- 2: edited in place as phases advance --------------------------------------------


def test_progress_edits_the_same_comment_and_moves_the_label_to_running(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9903
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    _start_running(request_id)
    for phase, loop_round in (("read_issue", None), ("pin_criteria", None), ("plan", 1)):
        assert report(client, request_id, phase, round=loop_round).status_code == 201
    _reconcile()

    assert _posts(sink) == []
    patches = _patches(sink)
    assert [path for path, _ in patches] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    body = patches[0][1] or ""
    assert "- [x] Read issue" in body
    assert "- [x] Pin acceptance criteria" in body
    assert "- [ ] **Plan** (in progress, round 1 of 3)" in body
    assert "- [ ] Plan review" in body
    assert "- [ ] Wait for CI" in body
    assert "Status: RUNNING" in body
    assert WAITING not in body
    assert FINAL_MARKER not in body
    # Model notes never reach the Markdown, only the card.
    assert marker_for(request_id) in body
    assert (
        "POST",
        f"/repos/{REPO}/issues/{number}/labels",
        '["curie-factory:running"]',
    ) in sink.requests
    assert (
        "DELETE",
        f"/repos/{REPO}/issues/{number}/labels/curie-factory:queued",
        None,
    ) in sink.requests
    assert _curie_labels(sink, number) == {"curie-factory:running"}
    assert {LABEL, "bug"} <= sink.issue_labels[number]

    sink.requests.clear()
    _reconcile()
    assert _writes(sink) == []


def test_a_note_is_not_rendered_into_the_comment(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9904
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    note = "ping @octocat and see [this](https://evil.example)"
    assert report(client, request_id, "read_issue", note=note).status_code == 201
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "@octocat" not in comment["body"]
    assert "evil.example" not in comment["body"]


def test_diff_review_failure_keeps_completed_phases_after_renewed_plan_review(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 9924)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()
    epoch = _start_running(request_id)
    for phase, loop_round in (
        ("plan", 1),
        ("plan_review", 1),
        ("failing_test", None),
        ("implement", 1),
        ("review_diff", 1),
        ("plan_review", 2),
    ):
        response = report(
            client, request_id, phase, round=loop_round, declaration=STAGED_DECLARATION
        )
        assert response.status_code == 201, response.text
    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()

    assert _posts(sink) == []
    assert [path for path, _ in _patches(sink)] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    for label in ("Plan", "Plan review", "Failing test", "Implement"):
        assert f"[x] {label}" in body
    assert "[ ] **Review diff** (in progress)" in body
    assert "[ ] Publish PR" in body
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body

    token = _notices(request_id)[0]["card_token"]
    card = client.get(f"/v1/factory/cards/{token}.svg")
    assert card.status_code == 200, card.text
    stages = {
        node.get("data-stage"): set((node.get("class") or "").split())
        for node in ET.fromstring(card.text).iter()
        if node.get("data-stage")
    }
    assert all("done" in stages[stage] for stage in ("plan", "plan_review", "implement"))
    assert "blocked" in stages["review_diff"]
    assert "pending" in stages["wait_ci"]


def test_phase_labels_are_markdown_escaped(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9905
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    declaration = {
        "phases": [
            {"id": "look", "label": "Look *here* [x](y)"},
            {"id": "done_now", "label": "Done"},
        ],
        "loops": [],
    }
    assert report(client, request_id, "done_now", declaration=declaration).status_code == 201
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert r"- [x] Look \*here\* \[x\]\(y\)" in comment["body"]


# --- 3: completed, one comment, no separate final comment ---------------------------------


def test_completion_patches_the_pr_link_and_finalizes(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9906
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    _start_running(request_id)
    assert report(client, request_id, "publish").status_code == 201
    work_item_id = _request(number)["work_item_id"]
    _attach_publication(work_item_id, status="succeeded", pr=77)
    sink.requests.clear()
    _reconcile()

    assert _request(number)["status"] == "completed"
    assert _posts(sink) == []
    patches = _patches(sink)
    assert patches and {path for path, _ in patches} == {
        f"/repos/{REPO}/issues/comments/{comment_id}"
    }
    body = patches[-1][1] or ""
    pr_url = f"https://github.com/{REPO}/pull/77"
    assert f"Completed: {pr_url}" in body
    assert "Status: SUCCEEDED" in body
    assert "- [x] Wait for CI" in body
    assert (
        body.index("Completed:")
        < body.index("Status: SUCCEEDED")
        < body.index(FINAL_MARKER)
        < body.index(marker_for(request_id))
    )
    assert _notices(request_id)[0]["finalized_at"] is not None
    assert _curie_labels(sink, number) == {"curie-factory:pr-open"}
    assert sink.posts == 1
    assert len(_marked(sink, request_id)) == 1

    sink.requests.clear()
    _reconcile()
    assert _writes(sink) == []


def test_a_preexisting_base_failure_is_named_in_the_final_comment(admitted: Any) -> None:  # noqa: F811
    """#4105 AC5: a check also failing on the base branch completes with a note."""

    client, github, sink = admitted
    number = 9923
    base_head = "d4" * 20
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    _start_running(request_id)
    assert report(client, request_id, "publish").status_code == 201
    work_item_id = _request(number)["work_item_id"]
    sink.ci_scripts = {
        HEAD_A: [ci_entry(check_run("pip-audit", conclusion="failure"), check_run("lint"))],
        base_head: [ci_entry(check_run("pip-audit", conclusion="failure"), check_run("lint"))],
    }
    sink.branches = {"main": base_head}
    _attach_publication(work_item_id, status="succeeded", pr=77)
    _set_base_ref(work_item_id, "main")
    sink.requests.clear()
    _reconcile()

    assert _request(number)["status"] == "completed"
    patches = _patches(sink)
    assert patches and {path for path, _ in patches} == {
        f"/repos/{REPO}/issues/comments/{comment_id}"
    }
    body = patches[-1][1] or ""
    assert f"Completed: https://github.com/{REPO}/pull/77" in body
    assert "Status: SUCCEEDED" in body
    assert "Also failing on the base branch, not caused by this change: pip-audit" in body
    assert _notices(request_id)[0]["finalized_at"] is not None


def test_a_pending_publication_shows_publishing_and_stays_live(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9907
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    _attach_publication(_request(number)["work_item_id"], status="approved", pr=None)
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert FINAL_MARKER not in comment["body"]
    assert "Status: PUBLISHING" in comment["body"]
    assert _notices(request_id)[0]["finalized_at"] is None


# --- 4 and 5: failure and cancel -----------------------------------------------------------


def test_a_failure_patches_the_plain_reason_and_needs_a_human(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9908
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    _finish_failed(
        client, request_id, epoch, "runner_escalated", detail="the tool call kept failing"
    )
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert body.startswith("Could not complete:")
    assert "Provider message: the tool call kept failing" in body
    assert "Cause: runner_escalated" in body
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}
    assert sink.posts == 1


def test_sandbox_termination_comment_shows_kubernetes_reason(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9922
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    epoch = _start_running(request_id)
    detail = 'Kubernetes reason: Evicted: EmptyDir volume "workspace" exceeded its limit'
    _finish_failed(client, request_id, epoch, "sandbox_terminated", detail=detail)
    _reconcile()

    assert _posts(sink) == []
    assert [path for path, _ in _patches(sink)] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert body.startswith("Could not complete: the sandbox terminated")
    assert f"Details: {detail}" in body
    assert "Provider message:" not in body
    assert "Cause: sandbox_terminated" in body
    assert "Failure class: sandbox-terminated" in body
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}
    token = _notices(request_id)[0]["card_token"]
    card = client.get(f"/v1/factory/cards/{token}.svg")
    assert card.status_code == 200, card.text
    assert "NEEDS HUMAN" in card.text


@pytest.mark.parametrize(
    ("number", "cause"),
    [
        (9930, "publication_failed"),
        (9931, "approval_create_failed"),
        (9933, "no_pull_request"),
        (9934, "runner_escalated"),
    ],
)
def test_every_terminal_failure_cause_shows_needs_human_status(
    admitted: Any,  # noqa: F811
    number: int,
    cause: str,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    _finish_failed(client, request_id, epoch, cause)
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}


def _issue_requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.sequence, r.status, r.terminal_cause "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_issue_number = :n ORDER BY r.sequence",
        {"n": number},
    )


def _lose_owner(client: Any, request_id: uuid.UUID) -> None:
    """Start the request, lapse its heartbeat, and observe the owner_lost teardown."""

    _start_running(request_id)
    _execute(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = clock_timestamp() - CAST(:elapsed AS interval) "
        "WHERE id = :id AND status = 'running'",
        {
            "id": request_id,
            "elapsed": timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5),
        },
    )
    _reconcile()
    (row,) = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    assert (row["status"], row["terminal_cause"]) == ("cancellation_requested", "owner_lost")
    _observe_termination(client, request_id)
    _reconcile()


RETRIED = (
    "the worker running this request stopped responding. Curie started the work "
    "again as a new run (attempt {attempt} of 3)."
)
EXHAUSTED = "the worker running this request stopped responding 3 times."


def test_a_retried_owner_lost_hands_the_label_to_the_successor(
    admitted: Any,  # noqa: F811
) -> None:
    """ADR 0206: the first loss names the retry; the successor speaks for the issue."""

    client, github, sink = admitted
    number = 9936
    lost_id = _admit(client, github, sink, number)
    _reconcile()
    _lose_owner(client, lost_id)

    requests = _issue_requests(number)
    assert [(r["id"], r["status"], r["terminal_cause"]) for r in requests[:1]] == [
        (lost_id, "failed", "owner_lost")
    ]
    assert len(requests) == 2, requests
    successor_id = requests[1]["id"]
    assert requests[1]["status"] == "waiting"

    (lost,) = _marked(sink, lost_id)
    body = lost["body"]
    assert "Status: RETRYING" in body
    assert f"Retrying: {RETRIED.format(attempt=2)}" in body
    assert "Could not complete:" not in body
    assert "NEEDS HUMAN" not in body
    assert EXHAUSTED not in body
    assert "Cause: owner_lost" in body
    assert FINAL_MARKER in body
    (successor,) = _marked(sink, successor_id)
    assert "Status: QUEUED" in successor["body"]
    assert FINAL_MARKER not in successor["body"]
    assert sink.posts == 2
    assert _curie_labels(sink, number) == {"curie-factory:queued"}


def test_owner_lost_shows_needs_human_status(admitted: Any) -> None:  # noqa: F811
    """ADR 0206: two losses are retried; the third needs a human."""

    client, github, sink = admitted
    number = 9932
    current = _admit(client, github, sink, number)
    _reconcile()
    lost: list[uuid.UUID] = []
    for attempt in (2, 3):
        _lose_owner(client, current)
        lost.append(current)
        requests = _issue_requests(number)
        assert len(requests) == attempt, requests
        (comment,) = _marked(sink, current)
        body = comment["body"]
        assert "Status: RETRYING" in body
        assert f"Retrying: {RETRIED.format(attempt=attempt)}" in body
        assert "Could not complete:" not in body
        assert "NEEDS HUMAN" not in body
        assert "Cause: owner_lost" in body
        assert FINAL_MARKER in body
        assert _curie_labels(sink, number) == {"curie-factory:queued"}
        current = requests[-1]["id"]

    _lose_owner(client, current)
    requests = _issue_requests(number)
    assert [(r["status"], r["terminal_cause"]) for r in requests] == [
        ("failed", "owner_lost")
    ] * 3
    (comment,) = _marked(sink, current)
    body = comment["body"]
    assert f"Could not complete: {EXHAUSTED}" in body
    assert "Curie started the work again" not in body
    assert "Cause: owner_lost" in body
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}
    # The earlier comments keep their retry text once the third loss lands.
    for attempt, request_id in zip((2, 3), lost, strict=True):
        (earlier,) = _marked(sink, request_id)
        assert "Status: RETRYING" in earlier["body"]
        assert f"Retrying: {RETRIED.format(attempt=attempt)}" in earlier["body"]
        assert "Could not complete:" not in earlier["body"]
        assert "NEEDS HUMAN" not in earlier["body"]
        assert "Cause: owner_lost" in earlier["body"]
        assert FINAL_MARKER in earlier["body"]


def test_an_expired_run_shows_needs_human_status(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9935
    request_id = _admit(client, github, sink, number)
    _reconcile_later(31)
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("expired", "capacity_wait_expired")

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert "Status: NEEDS HUMAN" in body
    assert "Status: EXPIRED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}


@pytest.mark.parametrize(
    ("number", "detail", "field", "value", "remedy"),
    [
        (
            9916,
            "output token budget exceeded (max_output_tokens_per_run=64000)",
            "max_output_tokens_per_run",
            "64000",
            "curie cluster budget <agent> --output-tokens <tokens>",
        ),
        (
            9917,
            "USD budget exceeded (max_usd_per_day=6.5)",
            "max_usd_per_day",
            "6.5",
            "curie cluster budget <agent> --limit <usd>",
        ),
        (9923, "run failed", None, None, None),
    ],
)
def test_budget_failure_comment_identifies_the_limit_or_admits_it_is_unknown(
    admitted: Any,  # noqa: F811
    number: int,
    detail: str,
    field: str | None,
    value: str | None,
    remedy: str | None,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    epoch = _start_running(request_id)
    _finish_failed(client, request_id, epoch, "budget_exceeded", detail=detail)
    _reconcile()

    assert _posts(sink) == []
    assert [path for path, _ in _patches(sink)] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    headline = body.splitlines()[0]
    assert headline.startswith("Could not complete:")
    if field is None:
        assert "cannot identify" in headline.lower()
        assert "curie cluster budget" not in body
        assert "--limit" not in body
        assert "--output-tokens" not in body
    else:
        assert field in headline
        assert value is not None and value in headline
        assert f"`{remedy}`" in headline
        other_remedy = "--limit" if field == "max_output_tokens_per_run" else "--output-tokens"
        assert other_remedy not in body
    assert f"Provider message: {detail}" in body
    assert "Cause: budget_exceeded" in body
    assert "Status: NEEDS HUMAN" in body
    assert "Status: FAILED" not in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}
    assert sink.posts == 1
    assert len(_marked(sink, request_id)) == 1


def test_history_capacity_failure_notice_explains_retry(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9921
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    _finish_failed(
        client,
        request_id,
        epoch,
        "history_capacity",
        detail="conversation history capacity exceeded",
    )
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"].lower()
    assert "history capacity exceeded" in body
    assert body.count("history capacity exceeded") == 1
    assert "provider message:" not in body
    assert "retry" in body
    assert "cause: history_capacity" in body
    assert "status: needs human" in body
    assert "status: failed" not in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}


START_FAILED_SENTENCE = (
    "Could not complete: the sandbox did not start after 5 attempts. "
    "Last reason: not_started:classified_failure."
)


def _defer_start(request_id: uuid.UUID, times: int) -> None:
    """Drive the real worker defer path: each attempt is acquired, then deferred."""

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                for _ in range(times):
                    generation = await session.scalar(
                        text(
                            "SELECT dispatch_generation FROM curie.execution_requests "
                            "WHERE id = :id"
                        ),
                        {"id": request_id},
                    )
                    acquired = await acquire(
                        session, request_id, owner="factory-owner", generation=generation
                    )
                    assert not isinstance(acquired, DispatchConflict), acquired
                    deferred = await defer(
                        session,
                        request_id,
                        owner="factory-owner",
                        generation=generation,
                        reason="not_started:classified_failure",
                        capacity=False,
                    )
                    assert not isinstance(deferred, DispatchConflict), deferred
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_start_failed_comment_names_the_attempts_and_last_reason(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9936
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    _defer_start(request_id, 5)
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("failed", "start_failed")
    _reconcile()

    assert _posts(sink) == []
    assert [path for path, _ in _patches(sink)] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert body.splitlines()[0] == START_FAILED_SENTENCE
    assert "Cause: start_failed" in body
    assert "Provider message:" not in body
    assert "Status: NEEDS HUMAN" in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie-factory:needs-human"}
    assert sink.posts == 1


def test_start_failed_result_section_uses_the_detail_as_the_sentence() -> None:
    detail = (
        "the sandbox did not start after 5 attempts. Last reason: not_started:classified_failure."
    )
    body = result_section("start_failed", pr_url=None, detail=detail)
    assert body.startswith(f"{START_FAILED_SENTENCE}\n")
    assert "Cause: start_failed" in body
    assert "Provider message:" not in body
    assert body.count("did not start") == 1


def test_unlabel_while_waiting_stops_and_clears_every_state_label(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9909
    request_id = _admit(client, github, sink, number)
    _reconcile()
    assert _curie_labels(sink, number) == {"curie-factory:queued"}
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled"
    _reconcile()

    (comment,) = _marked(sink, request_id)
    assert "Stopped:" in comment["body"]
    assert "Cause: issue_cancelled" in comment["body"]
    assert "Status: CANCELLED" in comment["body"]
    assert FINAL_MARKER in comment["body"]
    assert _curie_labels(sink, number) == set()
    assert (
        "DELETE",
        f"/repos/{REPO}/issues/{number}/labels/curie-factory:queued",
        None,
    ) in sink.requests
    assert "bug" in sink.issue_labels[number]
    assert sink.posts == 1


def test_a_running_cancellation_shows_stopping(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9910
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: STOPPING" in comment["body"]
    assert FINAL_MARKER not in comment["body"]
    assert _curie_labels(sink, number) == {"curie-factory:running"}


# --- 6: relabel supersedes ------------------------------------------------------------------


def test_relabel_while_waiting_finalizes_the_old_comment_and_opens_a_new_one(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9911
    old_id = _admit(client, github, sink, number)
    _reconcile()
    github.advance_label_event(number)
    again = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert again.json()["status"] == "factory_admitted"
    rows = _rows(
        "SELECT r.id FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.github_issue_number = :n ORDER BY r.sequence",
        {"n": number},
    )
    new_id = rows[1]["id"]
    _reconcile()
    _reconcile()

    (old,) = _marked(sink, old_id)
    assert "a new run replaced this one" in old["body"]
    assert "Cause: issue_cancelled" in old["body"]
    assert FINAL_MARKER in old["body"]
    (new,) = _marked(sink, new_id)
    assert "Status: QUEUED" in new["body"]
    assert FINAL_MARKER not in new["body"]
    assert sink.posts == 2
    assert _curie_labels(sink, number) == {"curie-factory:queued"}
    # The superseded row never cleared the successor's label.
    assert (
        "DELETE",
        f"/repos/{REPO}/issues/{number}/labels/curie-factory:queued",
        None,
    ) not in sink.requests


# --- legacy labels are rewritten on the next pass ---------------------------------------------


def test_legacy_state_label_is_replaced_on_the_next_pass(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9916
    request_id = _admit(client, github, sink, number)
    _reconcile()

    # _rows does not commit. This update has to, or the next pass still sees the new name.
    _execute(
        "UPDATE curie.factory_terminal_notices SET applied_label = :label "
        "WHERE execution_request_id = :id",
        {"label": "curie:queued", "id": request_id},
    )
    assert _notices(request_id)[0]["applied_label"] == "curie:queued"
    labels = sink.issue_labels[number]
    labels.discard("curie-factory:queued")
    # LABEL is "factory" in this fixture. "curie-factory" is the admission name a
    # prefix delete would also remove, so it has to stay beside LABEL.
    labels.update({LABEL, "curie-factory", "bug", "curie:queued", "curie:custom"})
    sink.requests.clear()
    _reconcile()

    assert (
        "POST",
        f"/repos/{REPO}/issues/{number}/labels",
        '["curie-factory:queued"]',
    ) in sink.requests
    assert "curie:queued" not in sink.issue_labels[number]
    assert {LABEL, "curie-factory", "curie:custom", "bug"} <= sink.issue_labels[number]


def test_legacy_pr_open_label_is_replaced_after_finalize(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9917
    request_id = _admit(client, github, sink, number)
    _reconcile()
    _start_running(request_id)
    assert report(client, request_id, "publish").status_code == 201
    work_item_id = _request(number)["work_item_id"]
    _attach_publication(work_item_id, status="succeeded", pr=77)
    _reconcile()
    assert _request(number)["status"] == "completed"
    assert _notices(request_id)[0]["finalized_at"] is not None

    _execute(
        "UPDATE curie.factory_terminal_notices SET applied_label = :label "
        "WHERE execution_request_id = :id",
        {"label": "curie:pr-open", "id": request_id},
    )
    assert _notices(request_id)[0]["applied_label"] == "curie:pr-open"
    labels = sink.issue_labels[number]
    labels.discard("curie-factory:pr-open")
    labels.add("curie:pr-open")
    sink.requests.clear()
    _reconcile()

    assert (
        "POST",
        f"/repos/{REPO}/issues/{number}/labels",
        '["curie-factory:pr-open"]',
    ) in sink.requests
    assert "curie:pr-open" not in sink.issue_labels[number]


# --- 7 and 8: lost edit responses and deleted comments ----------------------------------------


def test_a_failed_edit_is_retried_and_never_becomes_a_second_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9912
    request_id = _admit(client, github, sink, number)
    _reconcile()
    _start_running(request_id)
    sink.patch_statuses = [500]
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: QUEUED" in comment["body"]
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: RUNNING" in comment["body"]
    assert len(_patches(sink)) == 2
    assert sink.posts == 1


def test_a_comment_deleted_by_a_human_is_recreated_once(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9913
    request_id = _admit(client, github, sink, number)
    _reconcile()
    first_id = _notices(request_id)[0]["comment_id"]
    sink.delete_comment(first_id)
    _start_running(request_id)
    sink.requests.clear()
    _reconcile()
    _reconcile()

    assert sink.posts == 2
    (comment,) = _marked(sink, request_id)
    assert comment["id"] != first_id
    assert "Status: RUNNING" in comment["body"]
    methods = [(method, path) for method, path, _ in sink.requests]
    patch_at = methods.index(("PATCH", f"/repos/{REPO}/issues/comments/{first_id}"))
    list_path = f"/repos/{REPO}/issues/{number}/comments"
    post_at = methods.index(("POST", list_path))
    assert ("GET", list_path) in methods[patch_at:post_at]
    row = _notices(request_id)[0]
    assert row["comment_id"] == comment["id"]

    sink.requests.clear()
    _reconcile()
    assert _posts(sink) == []


# --- 9: revision threads -------------------------------------------------------------------


def test_queued_review_revision_replies_that_it_waits_for_the_current_run(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    running = _insert_revision(
        first["work_item_id"], number, _revision_objective(pr, "discussion_r88203")
    )
    _start_running(running)
    assert report(client, running, "implement", round=1).status_code == 201
    _reconcile()
    assert _curie_labels(sink, number) == {"curie-factory:running"}
    queued = uuid.uuid4()
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, requester, reply_kind, "
        "reply_address, reply_conversation_id) VALUES "
        "(:id, :work_item, 3, 'queued', NULL, :objective, "
        "'github:6601:octocat', 'github', :repo, :conversation)",
        {
            "id": queued,
            "work_item": first["work_item_id"],
            "objective": _revision_objective(pr, "discussion_r88204"),
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    _execute(
        "UPDATE curie.work_items SET next_sequence = 4, version = version + 1 WHERE id = :id",
        {"id": first["work_item_id"]},
    )
    _execute(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id) VALUES (:id, :work_item)",
        {"id": queued, "work_item": first["work_item_id"]},
    )
    sink.requests.clear()

    _reconcile()

    (comment,) = _marked(sink, queued)
    body = comment["body"].lower()
    assert "waiting" in body and "current run" in body
    assert FINAL_MARKER not in comment["body"]
    assert [path for path, _ in _posts(sink)] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88204/replies"
    ]
    assert _notices(queued)[0]["posted_at"] is not None
    assert _curie_labels(sink, number) == {"curie-factory:running"}

    github.labels = []
    cancelled = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert cancelled.json()["status"] == "factory_cancellation_requested", cancelled.text
    _reconcile()
    queued_row = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": queued},
    )[0]
    assert queued_row == {"status": "cancelled", "terminal_cause": "issue_cancelled"}
    assert _curie_labels(sink, number) == {"curie-factory:running"}


def test_closed_lineage_queue_keeps_the_completed_runs_pr_open_label(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    assert _curie_labels(sink, number) == {"curie-factory:pr-open"}
    queued = uuid.uuid4()
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, requester, reply_kind, "
        "reply_address, reply_conversation_id) VALUES "
        "(:id, :work_item, 2, 'queued', NULL, :objective, "
        "'github:6601:octocat', 'github', :repo, :conversation)",
        {
            "id": queued,
            "work_item": first["work_item_id"],
            "objective": _revision_objective(pr, "issuecomment-88205"),
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    _execute(
        "UPDATE curie.work_items SET next_sequence = 3, version = version + 1 WHERE id = :id",
        {"id": first["work_item_id"]},
    )
    _execute(
        "INSERT INTO curie.factory_terminal_notices "
        "(execution_request_id, work_item_id) VALUES (:id, :work_item)",
        {"id": queued, "work_item": first["work_item_id"]},
    )
    _execute(
        "UPDATE curie.thread_publication_lineages SET status = 'closed', "
        "version = version + 1 WHERE id = "
        "(SELECT publication_lineage_id FROM curie.work_items WHERE id = :work_item)",
        {"work_item": first["work_item_id"]},
    )
    sink.requests.clear()

    _reconcile()

    queued_row = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": queued},
    )[0]
    assert queued_row == {"status": "cancelled", "terminal_cause": "lineage_closed"}
    assert _curie_labels(sink, number) == {"curie-factory:pr-open"}


def _live_revision(
    client: Any, github: Any, sink: Any, fragment: str
) -> tuple[int, int, uuid.UUID, Any]:
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    _start_running(revision)
    # A revision inserted without a status row gets one from its first report.
    assert report(client, revision, "implement", round=1).status_code == 201
    _reconcile()
    return number, pr, revision, first["work_item_id"]


def test_a_review_thread_revision_replies_then_edits_the_review_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, revision, work_item_id = _live_revision(client, github, sink, "discussion_r88201")
    assert [path for path, _ in _posts(sink)] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88201/replies"
    ]
    row = _notices(revision)[0]
    assert row["comment_list"] == "review"
    reply_id = row["comment_id"]

    _attach_revision_publication(work_item_id, revision)
    sink.requests.clear()
    _reconcile()

    assert _posts(sink) == []
    assert {path for path, _ in _patches(sink)} == {f"/repos/{REPO}/pulls/comments/{reply_id}"}
    (comment,) = _marked(sink, revision)
    assert FINAL_MARKER in comment["body"]
    assert "The requested revision is pushed to this pull request." in comment["body"]
    assert _curie_labels(sink, pr) == set()
    assert _curie_labels(sink, number) == {"curie-factory:pr-open"}


def test_a_refused_thread_reply_lives_on_and_is_edited_on_the_conversation(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    for candidate in range(501, 600):
        sink.refuse_paths[f"/repos/{REPO}/pulls/{candidate}/comments/88202/replies"] = 422
    number, pr, revision, work_item_id = _live_revision(client, github, sink, "discussion_r88202")
    assert [path for path, _ in _posts(sink)] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88202/replies",
        f"/repos/{REPO}/issues/{pr}/comments",
    ]
    row = _notices(revision)[0]
    assert row["comment_list"] == "issue"

    _attach_revision_publication(work_item_id, revision)
    sink.requests.clear()
    _reconcile()
    assert {path for path, _ in _patches(sink)} == {
        f"/repos/{REPO}/issues/comments/{row['comment_id']}"
    }
    assert _curie_labels(sink, pr) == set()


# --- 10: refusal ---------------------------------------------------------------------------


def test_a_refused_edit_records_the_refusal_and_stops_writing(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9914
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    sink.patch_statuses = [403]
    _reconcile()
    row = _notices(request_id)[0]
    assert row["refused_at"] is not None
    assert row["refusal"] == "http_403"

    _finish_failed(client, request_id, epoch, "runner_failed")
    sink.requests.clear()
    _reconcile()
    _reconcile()
    assert _patches(sink) == []
    assert _posts(sink) == []


# --- 11: a request admitted before the status row existed -----------------------------------------


def test_a_request_without_an_admission_row_still_gets_exactly_one_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9915
    request_id = _admit(client, github, sink, number)
    _execute(
        "DELETE FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert len(_notices(request_id)) == 1
    _reconcile()
    _reconcile()
    assert sink.posts == 1
    (comment,) = _marked(sink, request_id)
    assert "Stopped:" in comment["body"]
    assert FINAL_MARKER in comment["body"]


def test_declaration_fixture_matches_nine_phases() -> None:
    assert len(DECLARATION["phases"]) == 9
