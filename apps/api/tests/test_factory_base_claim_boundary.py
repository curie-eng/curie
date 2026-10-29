"""Base relabels preserve existing publication authority until moves are supported."""

# ruff: noqa: F811

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from forge_fakes.github import BASE_SHA, LABEL, REPO, REPO_ID, _issue_event, _post, _signature
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: F401
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_factory_base_lifecycle import _item, _labelled, _requests, _train
from test_factory_status_comment import _marked
from test_factory_terminus import (
    _attach_publication,
    _observe_termination,
    _reconcile,
    _start_running,
)
from test_github_factory_base import _acquire_body, _execute, _open_lineage

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}


def _lineage(number: int) -> dict[str, Any]:
    (lineage,) = _rows(
        "SELECT l.id, l.base_ref, l.base_sha, l.head_sha, l.branch, l.pr_number, l.pr_url, "
        "l.code_host_pr_id, l.status, l.version "
        "FROM curie.thread_publication_lineages l JOIN curie.work_items w "
        "ON w.publication_lineage_id = l.id "
        "WHERE w.tracker_scope_id = :repo AND w.tracker_issue_id = :number",
        {"repo": str(REPO_ID), "number": str(number)},
    )
    return lineage


def _running_pull(client: Any, github: Any, number: int, pr: int) -> uuid.UUID:
    admitted = _labelled(client, github, number)
    assert admitted.json()["status"] == "factory_admitted", admitted.text
    request_id = _requests(number)[0]["id"]
    _start_running(client, request_id)
    _attach_publication(_item(number)["id"], status="succeeded", pr=pr)
    return request_id


def _acquire(client: Any, number: int) -> Any:
    request_id = _requests(number)[-1]["id"]
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/acquire",
        headers=WORKER,
        json=_acquire_body(number, owner="factory-owner"),
    )


def _attach_empty_lineage(item: dict[str, Any]) -> None:
    _open_lineage(item["id"])
    _execute(
        "UPDATE curie.thread_publication_lineages SET head_sha = NULL, pr_number = NULL, "
        "pr_url = NULL, base_sha = :base WHERE id = "
        "(SELECT publication_lineage_id FROM curie.work_items WHERE id = :item)",
        {"base": item["base_commit"], "item": item["id"]},
    )


def _observe_base_label(client: Any, number: int, forge: Any) -> httpx.Response:
    async def exercise() -> httpx.Response:
        original = client.app.state.http_client
        async with httpx.AsyncClient(transport=httpx.MockTransport(forge)) as external:
            client.app.state.http_client = external
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=client.app), base_url="http://testserver"
                ) as api:
                    body = json.dumps(
                        _issue_event("labeled", number, label={"name": "base:next"})
                    ).encode()
                    return await asyncio.wait_for(
                        api.post(
                            "/github/webhook",
                            content=body,
                            headers={
                                "X-GitHub-Delivery": str(uuid.uuid4()),
                                "X-GitHub-Event": "issues",
                                "X-Hub-Signature-256": _signature(body),
                                "Content-Type": "application/json",
                            },
                        ),
                        5,
                    )
            finally:
                client.app.state.http_client = original

    return client.portal.call(exercise)


def test_an_open_pull_request_keeps_its_running_request_when_the_base_label_changes(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, github, sink = admitted
    _train(monkeypatch, tmp_path, client)
    sink.by_path = True
    number, pr = 32101, 42101
    request_id = _running_pull(client, github, number, pr)
    before_item, before_lineage = _item(number), _lineage(number)
    client.portal.call(client.app.state.work_item_reconciler._sync_status_comments)
    sink.requests.clear()
    github.labels = [LABEL, "base:next"]

    changed = _post(
        client, "issues", _issue_event("labeled", number, label={"name": "base:next"})
    )

    assert changed.status_code == 200, changed.text
    assert _requests(number) == [{"id": request_id, "status": "running"}]
    after_item = _item(number)
    assert tuple(after_item[key] for key in ("base_branch", "base_source", "base_commit")) == tuple(
        before_item[key] for key in ("base_branch", "base_source", "base_commit")
    )
    assert _lineage(number) == before_lineage

    client.portal.call(client.app.state.work_item_reconciler._sync_status_comments)

    (comment,) = _marked(sink, request_id)
    base_line = next(line for line in comment["body"].splitlines() if line.startswith("Base:"))
    assert "`main`" in base_line
    assert "`next`" in base_line
    assert "requested" in base_line.lower()
    assert "refus" in base_line.lower()
    assert _requests(number) == [{"id": request_id, "status": "running"}]
    assert _lineage(number) == before_lineage
    assert not any(
        method != "GET" and path == f"/repos/{REPO}/pulls/{pr}"
        for method, path, _body in sink.requests
    )

    github.labels = [LABEL]
    corrected = _post(
        client, "issues", _issue_event("unlabeled", number, label={"name": "base:next"})
    )
    assert corrected.status_code == 200, corrected.text
    client.portal.call(client.app.state.work_item_reconciler._sync_status_comments)

    assert _requests(number) == [{"id": request_id, "status": "running"}]
    assert _lineage(number) == before_lineage
    (comment,) = _marked(sink, request_id)
    base_line = next(line for line in comment["body"].splitlines() if line.startswith("Base:"))
    assert "`main`" in base_line
    assert "`next`" not in base_line
    assert "refus" not in base_line.lower()


def test_a_base_relabel_before_publication_still_restarts_and_claims_the_requested_base(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32102
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _requests(number)[0]["id"]
    _start_running(client, request_id)
    github.labels = [LABEL, "base:next"]

    changed = _post(
        client, "issues", _issue_event("labeled", number, label={"name": "base:next"})
    )

    assert changed.status_code == 200, changed.text
    assert _requests(number) == [{"id": request_id, "status": "cancellation_requested"}]
    assert _item(number)["base_branch"] == "main"
    _observe_termination(client, request_id)
    _reconcile()
    predecessor, successor = _requests(number)
    assert predecessor == {"id": request_id, "status": "cancelled"}
    assert successor["id"] != request_id
    assert successor["status"] == "waiting"

    acquired = _acquire(client, number)

    assert acquired.status_code == 200, acquired.text
    assert (_item(number)["base_branch"], _item(number)["base_source"]) == ("next", "label")


@pytest.mark.parametrize("finished_status", ["completed", "failed"])
def test_a_base_webhook_cannot_restart_finished_work_without_a_fresh_factory_marking(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    finished_status: str,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32110
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _requests(number)[0]["id"]
    _start_running(client, request_id)
    original = _item(number)
    cause = "completed" if finished_status == "completed" else "no_pull_request"
    _execute(
        "UPDATE curie.execution_requests SET status = :status, terminal_cause = :cause, "
        "terminal_at = clock_timestamp() WHERE id = :id",
        {"status": finished_status, "cause": cause, "id": request_id},
    )
    github.labels = [LABEL, "base:next"]

    for _ in range(2):
        observed = _post(
            client, "issues", _issue_event("labeled", number, label={"name": "base:next"})
        )
        assert observed.status_code == 200, observed.text
        assert _requests(number) == [{"id": request_id, "status": finished_status}]
        current = _item(number)
        assert (current["base_branch"], current["base_commit"]) == (
            original["base_branch"], original["base_commit"]
        )

    marked = _labelled(client, github, number, "base:next")

    assert marked.status_code == 200, marked.text
    assert marked.json()["status"] == "factory_admitted", marked.text
    predecessor, replacement = _requests(number)
    assert predecessor == {"id": request_id, "status": finished_status}
    assert replacement["id"] != request_id
    assert replacement["status"] == "waiting"
    acquired = _acquire(client, number)
    assert acquired.status_code == 200, acquired.text
    assert (_item(number)["base_branch"], _item(number)["base_commit"]) == (
        "next", BASE_SHA
    )


def test_finished_published_work_still_records_a_refusal_without_starting_a_request(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32111
    request_id = _running_pull(client, github, number, 90312)
    _execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_cause = 'completed', terminal_at = clock_timestamp() WHERE id = :id",
        {"id": request_id},
    )
    item = _item(number)
    lineage = _lineage(number)
    github.labels = [LABEL, "base:next"]

    observed = _post(
        client, "issues", _issue_event("labeled", number, label={"name": "base:next"})
    )

    assert observed.status_code == 200, observed.text
    assert _requests(number) == [{"id": request_id, "status": "completed"}]
    assert _lineage(number) == lineage
    assert (_item(number)["base_branch"], _item(number)["base_commit"]) == (
        item["base_branch"], item["base_commit"]
    )
    [requested] = _rows(
        "SELECT base_requested_branch, base_requested_source "
        "FROM curie.work_items WHERE id = :id",
        {"id": item["id"]},
    )
    assert requested == {"base_requested_branch": "next", "base_requested_source": "label"}


@pytest.mark.parametrize(("target", "status_code"), [("next", 409), ("main", 200)])
def test_factory_label_readmission_cannot_repin_an_existing_pull_request(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target: str,
    status_code: int,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32103
    request_id = _running_pull(client, github, number, pr=42103)
    before_lineage = _lineage(number)
    before_base = _item(number)["base_commit"]

    reapplied = _labelled(client, github, number, f"base:{target}")

    assert reapplied.json()["status"] == "factory_readmit_pending", reapplied.text
    _observe_termination(client, request_id)
    _reconcile()
    predecessor, successor = _requests(number)
    assert predecessor == {"id": request_id, "status": "cancelled"}
    assert successor["status"] == "waiting"

    acquired = _acquire(client, number)

    assert acquired.status_code == status_code, acquired.text
    assert (_item(number)["base_branch"], _item(number)["base_commit"]) == ("main", before_base)
    assert _lineage(number) == before_lineage
    if target == "next":
        (deferred,) = _rows(
            "SELECT dispatch_not_before, updated_at FROM curie.execution_requests WHERE id = :id",
            {"id": successor["id"]},
        )
        assert (deferred["dispatch_not_before"] - deferred["updated_at"]).total_seconds() >= 60
        github.labels = [LABEL]
        corrected = _post(
            client, "issues", _issue_event("unlabeled", number, label={"name": "base:next"})
        )
        assert corrected.status_code == 200, corrected.text
        assert _requests(number)[-1]["id"] == successor["id"]
        _execute(
            "UPDATE curie.execution_requests SET dispatch_not_before = clock_timestamp(), "
            "published_generation = dispatch_generation WHERE id = :id",
            {"id": successor["id"]},
        )

        healthy = _acquire(client, number)

        assert healthy.status_code == 200, healthy.text
        assert _requests(number)[-1]["id"] == successor["id"]
        assert (_item(number)["base_branch"], _item(number)["base_commit"]) == (
            "main",
            before_base,
        )
        assert _lineage(number) == before_lineage


@pytest.mark.parametrize("publication_wins", [True, False])
def test_an_empty_lineage_is_rechecked_after_the_external_branch_read(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    publication_wins: bool,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32104
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _requests(number)[0]["id"]
    _start_running(client, request_id)
    item = _item(number)
    _attach_empty_lineage(item)
    before_lineage = _lineage(number)
    github.labels = [LABEL, "base:next"]
    branch_reads: list[str] = []

    async def forge(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/git/ref/heads/{before_lineage['branch']}":
            branch_reads.append(request.url.path)
            if publication_wins:
                engine = create_async_engine(get_settings().database_url)
                try:
                    async with asyncio.timeout(5), engine.begin() as connection:
                        await connection.execute(
                            text(
                                "UPDATE curie.thread_publication_lineages "
                                "SET head_sha = :head, version = version + 1 WHERE id = :id"
                            ),
                            {"head": "6" * 40, "id": before_lineage["id"]},
                        )
                finally:
                    await engine.dispose()
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            return httpx.Response(404, json={"message": "Not Found"})
        return github.handle(request)

    changed = _observe_base_label(client, number, forge)

    assert changed.status_code == 200, changed.text
    assert len(branch_reads) == 1
    assert _item(number)["base_commit"] == item["base_commit"]
    after_lineage = _lineage(number)
    if publication_wins:
        assert _requests(number) == [{"id": request_id, "status": "running"}]
        assert after_lineage == {
            **before_lineage,
            "head_sha": "6" * 40,
            "version": before_lineage["version"] + 1,
        }
    else:
        assert _requests(number) == [{"id": request_id, "status": "cancellation_requested"}]
        assert after_lineage == before_lineage


@pytest.mark.parametrize("hold", ["approved_publication", "remote_head", "outage_then_absence"])
def test_headless_publication_or_uncertain_remote_state_prevents_a_base_restart(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    hold: str,
) -> None:
    client, github, _sink = admitted
    _train(monkeypatch, tmp_path, client)
    number = 32105
    assert _labelled(client, github, number).json()["status"] == "factory_admitted"
    request_id = _requests(number)[0]["id"]
    _start_running(client, request_id)
    item = _item(number)
    if hold == "approved_publication":
        _attach_publication(item["id"], status="approved", pr=None)
        _execute(
            "UPDATE curie.work_items w SET publication_lineage_id = p.lineage_id, "
            "version = w.version + 1 FROM curie.publications p "
            "WHERE w.id = :item AND p.execution_request_id = :request",
            {"item": item["id"], "request": request_id},
        )
        _execute(
            "UPDATE curie.publications SET terminal_at = NULL WHERE execution_request_id = :id",
            {"id": request_id},
        )
    else:
        _attach_empty_lineage(item)
    before_lineage = _lineage(number)
    authority_sql = (
        "SELECT p.id, p.status, p.base_sha, p.lineage_id, p.terminal_at, "
        "p.outcome_history_ready_at, a.status AS approval_status, a.resolved_at "
        "FROM curie.publications p JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.execution_request_id = :id"
    )
    authority = _rows(authority_sql, {"id": request_id})
    if hold == "approved_publication":
        (publication,) = authority
        assert (publication["status"], publication["approval_status"]) == ("approved", "approved")
    assert before_lineage["head_sha"] is before_lineage["pr_number"] is None
    github.labels = [LABEL, "base:next"]
    branch_status = {
        "approved_publication": 404,
        "remote_head": 200,
        "outage_then_absence": 503,
    }[hold]
    branch_reads: list[int] = []

    def forge(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/git/ref/heads/{before_lineage['branch']}":
            branch_reads.append(branch_status)
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            if branch_status == 200:
                return httpx.Response(
                    200,
                    json={
                        "ref": f"refs/heads/{before_lineage['branch']}",
                        "object": {"sha": "7" * 40},
                    },
                )
            message = "Not Found" if branch_status == 404 else "Service Unavailable"
            return httpx.Response(branch_status, json={"message": message})
        return github.handle(request)

    changed = _observe_base_label(client, number, forge)

    assert changed.status_code == 200, changed.text
    assert branch_reads == [branch_status]
    assert _requests(number) == [{"id": request_id, "status": "running"}]
    after_item = _item(number)
    assert tuple(after_item[key] for key in ("base_branch", "base_source", "base_commit")) == tuple(
        item[key] for key in ("base_branch", "base_source", "base_commit")
    )
    assert _lineage(number) == before_lineage
    assert _rows(authority_sql, {"id": request_id}) == authority
    if hold == "outage_then_absence":
        branch_status = 404

        healthy = _observe_base_label(client, number, forge)

        assert healthy.status_code == 200, healthy.text
        assert branch_reads == [503, 404]
        assert _requests(number) == [{"id": request_id, "status": "cancellation_requested"}]
        assert (_item(number)["base_branch"], _item(number)["base_commit"]) == (
            item["base_branch"],
            item["base_commit"],
        )
        assert _lineage(number) == before_lineage
