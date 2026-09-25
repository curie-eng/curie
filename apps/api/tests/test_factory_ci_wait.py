"""A published factory run waits on its pull request's CI (#3097).

The request is not terminal at PR open. The reconciler's CI gate observes the
checks and statuses on the published head through a scripted fake of the
GitHub Checks and Statuses APIs (``test_factory_terminus._CommentServer``):
https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
https://docs.github.com/en/rest/checks/runs#list-check-run-annotations

Green, or no checks inside the grace period, completes. A failure enqueues one
continuation turn for the SAME request (``work-item-{id}-ci-{round}``), up to
3 rounds. Time moves only through ``workitems._database_now`` offsets; nothing
here waits in real time. Machine fixtures drive the events.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sys
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.factory_notices import FINAL_MARKER, marker_for
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_factory_terminus import (  # noqa: F401  (fixtures)
    HEAD_A,
    HEAD_B,
    HEAD_C,
    _attach_publication,
    _CommentServer,
    _label,
    _notices,
    _observe_termination,
    _reconcile,
    _reconcile_later,
    _request,
    _rows,
    _start_running,
    admitted,
    check_run,
    ci_empty,
    ci_entry,
    ci_failing,
    ci_green,
    ci_pending,
    comments,
)
from test_github_factory_ingress import LABEL, REPO, REPO_ID, _issue_event, _post

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}
MARKER_RE = re.compile(r"^Curie wait_ci round ([23]) of 3: ")
_PRS = iter(range(701, 799))


# --- helpers -------------------------------------------------------------------


def _assert_no_final_result(sink: Any) -> None:
    """Only the live status comment exists while CI runs; nothing is final (#3077)."""

    assert sink.posts <= 1
    assert not any(FINAL_MARKER in comment["body"] for comment in sink.comments)


def _terminal_notices(request_id: Any) -> list[dict[str, Any]]:
    """The status row exists from admission (#3077); a result is a terminal cause."""

    return [row for row in _notices(request_id) if row["terminal_cause"] is not None]


def _all(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.status, r.terminal_cause, r.execution_deadline, "
        "r.runtime_heartbeat_expires_at, r.reply_conversation_id, "
        "w.id AS work_item_id, w.readmit_request_id "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number "
        "ORDER BY r.sequence",
        {"repo": REPO_ID, "number": number},
    )


def _stream_turns() -> list[dict[str, Any]]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


def _ci_turns(request_id: uuid.UUID) -> list[dict[str, Any]]:
    prefix = f"work-item-{request_id}-ci-"
    return [turn for turn in _stream_turns() if turn["event_id"].startswith(prefix)]


def _ci_key(request_id: uuid.UUID, round_: int) -> str:
    return f"curie:work-item:ci:{request_id}:{round_}"


def _key_exists(key: str) -> bool:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return bool(valkey.exists(key))
    finally:
        valkey.close()


def _count_requests(number: int) -> int:
    return len(_all(number))


def _published(
    client: Any, github: Any, sink: _CommentServer, number: int, *, head_sha: str = HEAD_A
) -> dict[str, Any]:
    """A running request whose first publication opened a pull request."""

    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    pr = next(_PRS)
    _attach_publication(row["work_item_id"], status="succeeded", pr=pr, head_sha=head_sha)
    return {**row, "pr": pr, "pr_url": f"https://github.com/{REPO}/pull/{pr}"}


async def _attach_fix_async(
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    *,
    revision: int,
    head_sha: str,
    title: str,
    paths: list[str],
    status: str = "succeeded",
) -> uuid.UUID:
    """A fix round's publication for the SAME request, advancing the lineage head."""

    engine = create_async_engine(get_settings().database_url)
    publication_id, approval_id = uuid.uuid4(), uuid.uuid4()
    try:
        async with engine.begin() as conn:
            item = (
                (
                    await conn.execute(
                        text(
                            "SELECT w.agent_id, w.conversation_id, w.publication_lineage_id, "
                            "l.deployment_id, l.pr_url "
                            "FROM curie.work_items w JOIN curie.thread_publication_lineages l "
                            "ON l.id = w.publication_lineage_id WHERE w.id = :id"
                        ),
                        {"id": work_item_id},
                    )
                )
                .mappings()
                .one()
            )
            await conn.execute(
                text(
                    "INSERT INTO curie.approvals "
                    "(id, agent_id, conversation_id, author, summary, reply_kind, "
                    "reply_channel, dedupe_key, status, purpose) VALUES "
                    "(:id, :agent, :conversation, 'U0REQUEST1', "
                    "'Publish repository changes', 'github', :channel, :dedupe, "
                    "'approved', 'publication')"
                ),
                {
                    "id": approval_id,
                    "agent": item["agent_id"],
                    "conversation": item["conversation_id"],
                    "channel": REPO,
                    "dedupe": f"ci-fix-{publication_id.hex}",
                },
            )
            terminal = status in {"succeeded", "failed", "denied", "expired"}
            await conn.execute(
                text(
                    "INSERT INTO curie.publications "
                    "(id, approval_id, deployment_id, workspace_conversation_id, "
                    "lineage_id, execution_request_id, revision_number, repo_full_name, "
                    "status, base_sha, changed_paths, title, body, reply_kind, "
                    "reply_channel, result_url, terminal_at) "
                    "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                    ":request_id, :revision, :repo, :status, :base, "
                    "CAST(:paths AS jsonb), :title, 'Approved platform publication.', "
                    "'github', :channel, :result, "
                    + ("clock_timestamp()" if terminal else "NULL")
                    + ")"
                ),
                {
                    "id": publication_id,
                    "approval": approval_id,
                    "deployment": item["deployment_id"],
                    "conversation": item["conversation_id"],
                    "lineage": item["publication_lineage_id"],
                    "request_id": request_id,
                    "revision": revision,
                    "repo": REPO,
                    "status": status,
                    "base": "0123456789abcdef0123456789abcdef01234567",
                    "paths": json.dumps(paths),
                    "title": title,
                    "channel": REPO,
                    "result": item["pr_url"] if terminal else None,
                },
            )
            if status == "succeeded":
                await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET "
                        "latest_revision = :revision, head_sha = :head WHERE id = :id"
                    ),
                    {
                        "id": item["publication_lineage_id"],
                        "revision": revision,
                        "head": head_sha,
                    },
                )
    finally:
        await engine.dispose()
    return publication_id


def _attach_fix(
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    *,
    revision: int,
    head_sha: str,
    title: str,
    paths: list[str],
    status: str = "succeeded",
) -> uuid.UUID:
    return asyncio.run(
        _attach_fix_async(
            work_item_id,
            request_id,
            revision=revision,
            head_sha=head_sha,
            title=title,
            paths=paths,
            status=status,
        )
    )


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _database_now() -> datetime:
    return _rows("SELECT clock_timestamp() AS now")[0]["now"]


@contextlib.contextmanager
def _clock_offset(seconds: float) -> Iterator[None]:
    """Shift the database clock both the reconciler and the finish route read."""

    import curie_api.workitem_dispatch as workitem_dispatch
    import curie_api.workitems as workitems

    original = workitems._database_now

    async def later(session: AsyncSession) -> Any:
        real = await original(session)
        return real + timedelta(seconds=seconds)

    workitems._database_now = later
    dispatch_original = getattr(workitem_dispatch, "_database_now", None)
    if dispatch_original is not None:
        workitem_dispatch._database_now = later  # type: ignore[attr-defined]
    try:
        yield
    finally:
        workitems._database_now = original
        if dispatch_original is not None:
            workitem_dispatch._database_now = dispatch_original  # type: ignore[attr-defined]


def _body(sink: _CommentServer, request_id: uuid.UUID) -> str:
    bodies = [c["body"] for c in sink.comments if marker_for(request_id) in c["body"]]
    assert len(bodies) == 1, bodies
    return bodies[0]


def _terminal(number: int) -> tuple[str, str | None]:
    row = _request(number)
    return row["status"], row["terminal_cause"]


def _finish(client: Any, request_id: uuid.UUID, epoch: int, cause: str) -> Any:
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers=WORKER,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": cause},
    )


def _epoch(request_id: uuid.UUID) -> int:
    return int(
        _rows(
            "SELECT runtime_epoch FROM curie.execution_requests WHERE id = :id",
            {"id": request_id},
        )[0]["runtime_epoch"]
    )


# --- AC3: green and no-CI ------------------------------------------------------


def test_pending_ci_keeps_the_request_running_until_green_completes_it(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9701
    sink.ci_script = [ci_pending(), ci_green()]
    published = _published(client, github, sink, number)

    _reconcile()

    assert _terminal(number) == ("running", None)
    assert _terminal_notices(published["id"]) == []
    _assert_no_final_result(sink)
    assert sink.ci_observations == [HEAD_A]

    _reconcile()

    assert _terminal(number) == ("completed", "completed")
    body = _body(sink, published["id"])
    assert body.startswith(f"Completed: {published['pr_url']}")
    assert "Note:" not in body
    assert _count_requests(number) == 1
    assert _ci_turns(published["id"]) == []


def test_green_ci_ignores_the_combined_status_pending_state(admitted: Any) -> None:
    """GitHub's combined ``state`` reads pending with no statuses; only the list counts."""

    client, github, sink = admitted
    number = 9702
    green = ci_green()
    assert green[3]["state"] == "pending" and green[3]["statuses"] == []
    sink.ci_script = [green]
    published = _published(client, github, sink, number)

    _reconcile()

    assert _terminal(number) == ("completed", "completed")
    assert _body(sink, published["id"]).startswith("Completed:")


def test_a_failing_commit_status_is_a_failure_even_with_green_check_runs(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9703
    sink.ci_script = [
        ci_entry(
            check_run("build"),
            statuses=({"context": "ci/jenkins", "state": "error", "description": "boom"},),
        )
    ]
    published = _published(client, github, sink, number)

    _reconcile()

    assert _terminal(number) == ("running", None)
    turns = _ci_turns(published["id"])
    assert [t["event_id"] for t in turns] == [f"work-item-{published['id']}-ci-2"]
    assert "ci/jenkins" in turns[0]["text"]


def test_no_checks_within_the_grace_period_completes_with_a_note(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9704
    sink.ci_script = [ci_empty()]
    published = _published(client, github, sink, number)

    _reconcile()

    assert _terminal(number) == ("running", None)
    _assert_no_final_result(sink)

    _reconcile_later(130)

    assert _terminal(number) == ("completed", "completed")
    body = _body(sink, published["id"])
    assert body.startswith(f"Completed: {published['pr_url']}")
    assert "Note:" in body
    assert _ci_turns(published["id"]) == []


# --- AC2: the fix loop, capped at 3 rounds ---------------------------------------


def test_ci_failure_loops_the_same_request_to_the_cap(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9705
    sink.ci_scripts = {
        HEAD_A: [ci_failing(run_id=81001)],
        HEAD_B: [ci_failing(run_id=81002)],
        HEAD_C: [ci_failing(run_id=81003)],
    }
    sink.annotations = {
        81001: [
            {
                "path": "src/widget.py",
                "start_line": 12,
                "end_line": 12,
                "annotation_level": "failure",
                "message": "AssertionError: expected 2, got 1",
            }
        ]
    }
    published = _published(client, github, sink, number)
    request_id = published["id"]

    # Round 1 fails: the request stays running, one continuation for round 2.
    _reconcile()

    row = _all(number)[0]
    assert (row["status"], row["terminal_cause"]) == ("running", None)
    assert _terminal_notices(request_id) == []
    _assert_no_final_result(sink)
    turns = _ci_turns(request_id)
    assert [t["event_id"] for t in turns] == [f"work-item-{request_id}-ci-2"]
    lines = turns[0]["text"].split("\n")
    assert lines[0] == f"https://github.com/{REPO}/issues/{number}"
    assert lines[1] == (
        f"Curie wait_ci round 2 of 3: the checks on {published['pr_url']} failed at {HEAD_A}."
    )
    assert "unit-tests" in turns[0]["text"]
    assert "AssertionError: expected 2, got 1" in turns[0]["text"]
    assert turns[0]["conversation_id"] == row["reply_conversation_id"]
    # The fix turn holds the lease to the ORIGINAL execution deadline.
    assert row["runtime_heartbeat_expires_at"] == row["execution_deadline"]

    # A second pass while the fix turn runs dispatches nothing more.
    _reconcile()
    assert len(_ci_turns(request_id)) == 1
    assert _terminal(number) == ("running", None)

    # The fix publishes to the same PR; round 2 fails too.
    _attach_fix(
        published["work_item_id"],
        request_id,
        revision=2,
        head_sha=HEAD_B,
        title="Fix the widget parser off-by-one",
        paths=["src/widget.py"],
    )
    _reconcile()

    assert _terminal(number) == ("running", None)
    turns = _ci_turns(request_id)
    assert [t["event_id"] for t in turns] == [
        f"work-item-{request_id}-ci-2",
        f"work-item-{request_id}-ci-3",
    ]
    assert f"failed at {HEAD_B}." in turns[1]["text"].split("\n")[1]
    assert MARKER_RE.match(turns[1]["text"].split("\n")[1]).group(1) == "3"  # type: ignore[union-attr]

    # Round 3 fails: the cap ends the request with one notice.
    _attach_fix(
        published["work_item_id"],
        request_id,
        revision=3,
        head_sha=HEAD_C,
        title="Handle empty widget input",
        paths=["src/widget.py", "tests/test_widget.py"],
    )
    _reconcile()

    assert _terminal(number) == ("failed", "ci_failed")
    assert len(_ci_turns(request_id)) == 2
    body = _body(sink, request_id)
    assert body.startswith("Could not complete:")
    assert "Rounds: 3" in body
    assert "Fix the widget parser off-by-one" in body
    assert "Handle empty widget input" in body
    assert "Failing checks:" in body
    assert "unit-tests" in body
    assert published["pr_url"] in body
    # AC5: every round ran inside the one request.
    assert _count_requests(number) == 1
    _reconcile()
    assert len(_notices(request_id)) == 1
    assert sink.posts == 1


@pytest.mark.parametrize("actions_status", [302, 403])
def test_ci_fix_turn_reads_failing_actions_job_log_when_available(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, actions_status: int
) -> None:
    """The published head reaches the observer, report, and queued fix turn."""

    client, github, sink = admitted
    number = 9784 + (actions_status == 403)
    job_id = 81784
    run = check_run(
        "unit-tests",
        conclusion="failure",
        title="1 test failed",
        summary="Process completed with exit code 1.",
        run_id=job_id,
    )
    run["app"] = {"slug": "github-actions"}
    sink.ci_script = [ci_entry(run)]
    sink.annotations[job_id] = [
        {
            "path": "src/widget.py",
            "start_line": 12,
            "end_line": 12,
            "annotation_level": "failure",
            "message": "Process completed with exit code 1.",
        }
    ]
    published = _published(client, github, sink, number)

    signed_url = "https://pipelines.actions.githubusercontent.com/acme-example/job.txt?sig=example"
    diagnostic = "AssertionError: expected 2, got 1"
    secret = "ghs_" + "AbCd1234" * 5
    original_send = httpx.AsyncClient.send
    requests: list[httpx.Request] = []

    async def github_and_signed_storage(
        self: httpx.AsyncClient, request: httpx.Request, **kwargs: Any
    ) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{job_id}/logs":
            requests.append(request)
            if actions_status == 403:
                return httpx.Response(
                    403,
                    json={"message": "Resource not accessible by integration"},
                    request=request,
                )
            return httpx.Response(302, headers={"Location": signed_url}, request=request)
        if str(request.url) == signed_url:
            requests.append(request)
            return httpx.Response(
                200, text=f"{diagnostic}\nGITHUB_TOKEN={secret}\n", request=request
            )
        return await original_send(self, request, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", github_and_signed_storage)

    _reconcile()

    assert _terminal(number) == ("running", None)
    turns = _ci_turns(published["id"])
    assert [turn["event_id"] for turn in turns] == [f"work-item-{published['id']}-ci-2"]
    prompt = turns[0]["text"]
    assert "Process completed with exit code 1." in prompt
    assert [request.url.path for request in requests if "/actions/jobs/" in request.url.path] == [
        f"/repos/{REPO}/actions/jobs/{job_id}/logs"
    ]
    report = json.loads(prompt.splitlines()[3])
    failing = report["failing_checks"][0]
    if actions_status == 403:
        assert failing["job_log"] == "Job log unavailable."
        assert diagnostic not in prompt
        assert len(requests) == 1
    else:
        assert diagnostic in failing["job_log"]
        assert secret not in prompt
        assert "GITHUB_TOKEN=[REDACTED:secret_assignment]" in failing["job_log"]
        assert len(requests) == 2
        assert "authorization" not in requests[1].headers


def test_a_green_fix_round_completes_the_same_request(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9706
    sink.ci_scripts = {HEAD_A: [ci_failing()], HEAD_B: [ci_green()]}
    published = _published(client, github, sink, number)

    _reconcile()
    assert len(_ci_turns(published["id"])) == 1

    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_B,
        title="Fix the test",
        paths=["src/widget.py"],
    )
    _reconcile()

    assert _terminal(number) == ("completed", "completed")
    assert _body(sink, published["id"]).startswith(f"Completed: {published['pr_url']}")
    assert len(_notices(published["id"])) == 1
    assert _count_requests(number) == 1


# --- A fix turn that ends without publishing ---------------------------------------


def test_an_unpublished_fix_turn_before_the_deadline_is_terminal(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9707
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    _reconcile()
    assert len(_ci_turns(published["id"])) == 1

    finished = _finish(client, published["id"], _epoch(published["id"]), "ci_fix_unpublished")

    assert finished.status_code == 200, finished.text
    assert _terminal(number) == ("failed", "ci_fix_unpublished")
    _reconcile()
    body = _body(sink, published["id"])
    assert body.startswith("Could not complete:")
    assert "without pushing a fix" in body.splitlines()[0]


def test_an_unpublished_fix_turn_with_a_publication_in_flight_defers(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9708
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    _reconcile()
    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_B,
        title="Fix the test",
        paths=["src/widget.py"],
        status="pending",
    )

    finished = _finish(client, published["id"], _epoch(published["id"]), "ci_fix_unpublished")

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    assert _terminal(number) == ("running", None)


def test_an_unpublished_fix_turn_past_the_deadline_expires_instead(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9709
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    _reconcile()
    assert len(_ci_turns(published["id"])) == 1
    row = _all(number)[0]
    past = (row["execution_deadline"] - _database_now()).total_seconds() + 1

    with _clock_offset(past):
        finished = _finish(client, published["id"], _epoch(published["id"]), "ci_fix_unpublished")
        assert finished.status_code == 409, finished.text
        _reconcile()

    assert _terminal(number) == ("cancellation_requested", "execution_deadline")
    causes = _rows(
        "SELECT terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": published["id"]},
    )
    assert causes[0]["terminal_cause"] != "ci_fix_unpublished"
    _observe_termination(client, published["id"])
    assert _terminal(number) == ("expired", "execution_deadline")


# --- Exactly one continuation per round --------------------------------------------


def _racing_pass(count: int) -> None:
    async def go() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        clients = [
            aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
            for _ in range(count)
        ]
        reconcilers = [WorkItemReconciler(maker, c, get_settings()) for c in clients]
        try:
            await asyncio.gather(*(r.run_once() for r in reconcilers))
        finally:
            for c in clients:
                await c.aclose()
            await engine.dispose()

    asyncio.run(go())


def test_two_reconcilers_racing_publish_exactly_one_continuation(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9710
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)

    _racing_pass(2)

    assert [t["event_id"] for t in _ci_turns(published["id"])] == [
        f"work-item-{published['id']}-ci-2"
    ]
    assert _terminal(number) == ("running", None)
    _racing_pass(2)
    assert len(_ci_turns(published["id"])) == 1


def test_a_failed_xadd_clears_the_claim_so_the_next_pass_retries(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github, sink = admitted
    number = 9711
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    original = WorkItemReconciler._xadd
    failures: list[str] = []

    async def flaky(
        self: WorkItemReconciler, turn: Any, *, marker: tuple[str, int] | None = None
    ) -> None:
        if "-ci-" in turn.event_id and not failures:
            failures.append(turn.event_id)
            raise redis.exceptions.ConnectionError("valkey went away")
        await original(self, turn, marker=marker)

    monkeypatch.setattr(WorkItemReconciler, "_xadd", flaky)

    with contextlib.suppress(redis.exceptions.ConnectionError):
        _reconcile()

    assert failures == [f"work-item-{published['id']}-ci-2"]
    assert _ci_turns(published["id"]) == []
    assert not _key_exists(_ci_key(published["id"], 2))
    assert _terminal(number) == ("running", None)

    _reconcile()

    assert [t["event_id"] for t in _ci_turns(published["id"])] == [
        f"work-item-{published['id']}-ci-2"
    ]
    assert _key_exists(_ci_key(published["id"], 2))


# --- Settlement is fenced to the observed publication and head ----------------------


def _push_during_observation(
    monkeypatch: pytest.MonkeyPatch, published: dict[str, Any]
) -> list[str]:
    """Land a fix push (head B) while the gate is observing head A."""

    from curie_api import workitem_outcomes

    original = workitem_outcomes.observe_ci_detail
    pushed: list[str] = []

    async def racing(*args: Any, **kwargs: Any) -> Any:
        detail = await original(*args, **kwargs)
        if not pushed:
            pushed.append(HEAD_B)
            await _attach_fix_async(
                published["work_item_id"],
                published["id"],
                revision=2,
                head_sha=HEAD_B,
                title="A newer push",
                paths=["src/widget.py"],
            )
        return detail

    monkeypatch.setattr(workitem_outcomes, "observe_ci_detail", racing)
    return pushed


def test_a_stale_green_after_a_new_push_leaves_the_request_running(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github, sink = admitted
    number = 9712
    sink.ci_scripts = {HEAD_A: [ci_green()], HEAD_B: [ci_failing()]}
    published = _published(client, github, sink, number)
    pushed = _push_during_observation(monkeypatch, published)

    _reconcile()

    assert pushed == [HEAD_B]
    assert _terminal(number) == ("running", None)
    assert _terminal_notices(published["id"]) == []
    _assert_no_final_result(sink)

    _reconcile()

    assert sink.ci_observations[-1] == HEAD_B
    turns = _ci_turns(published["id"])
    assert len(turns) == 1
    assert turns[0]["event_id"] in {
        f"work-item-{published['id']}-ci-2",
        f"work-item-{published['id']}-ci-3",
    }
    assert f"failed at {HEAD_B}." in turns[0]["text"].split("\n")[1]
    assert _terminal(number) == ("running", None)


def test_a_stale_failure_after_a_new_push_publishes_no_fix_turn(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, github, sink = admitted
    number = 9713
    sink.ci_scripts = {HEAD_A: [ci_failing()], HEAD_B: [ci_green()]}
    published = _published(client, github, sink, number)
    _push_during_observation(monkeypatch, published)

    _reconcile()

    assert _ci_turns(published["id"]) == []
    assert _terminal(number) == ("running", None)
    assert _terminal_notices(published["id"]) == []

    _reconcile()

    assert sink.ci_observations[-1] == HEAD_B
    assert _ci_turns(published["id"]) == []
    assert _terminal(number) == ("completed", "completed")


# --- Relabel and unlabel during the wait ----------------------------------------------


def test_relabel_during_the_ci_wait_cancels_then_readmits(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9714
    sink.ci_script = [ci_pending()]
    published = _published(client, github, sink, number)
    _reconcile()
    assert _terminal(number) == ("running", None)

    github.labels = [LABEL]
    again = _post(
        client,
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        delivery=str(uuid.uuid4()),
    )

    assert again.json()["status"] == "factory_readmit_pending"
    rows = _all(number)
    assert len(rows) == 1
    assert (rows[0]["status"], rows[0]["terminal_cause"]) == (
        "cancellation_requested",
        "issue_cancelled",
    )
    assert rows[0]["readmit_request_id"] is not None
    observed = len(sink.ci_observations)

    _reconcile()

    # The gate leaves a cancelling request alone, and no second request exists yet.
    assert len(sink.ci_observations) == observed
    assert len(_all(number)) == 1
    assert _ci_turns(published["id"]) == []

    _observe_termination(client, published["id"])
    _reconcile()

    rows = _all(number)
    assert [r["status"] for r in rows] == ["cancelled", "waiting"]
    assert rows[1]["id"] != published["id"]


def test_relabel_during_a_fix_turn_waits_for_the_old_request_to_settle(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9715
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    _reconcile()
    assert len(_ci_turns(published["id"])) == 1

    github.labels = [LABEL]
    again = _post(
        client,
        "issues",
        _issue_event("labeled", number, label={"name": LABEL}),
        delivery=str(uuid.uuid4()),
    )
    assert again.json()["status"] == "factory_readmit_pending"

    _reconcile()
    _reconcile_later(130)

    rows = _all(number)
    assert len(rows) == 1, rows
    assert rows[0]["status"] == "cancellation_requested"
    assert len(_ci_turns(published["id"])) == 1

    _observe_termination(client, published["id"])
    _reconcile()

    rows = _all(number)
    assert [r["status"] for r in rows] == ["cancelled", "waiting"]


def test_unlabel_during_the_ci_wait_stops_with_one_notice(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9716
    sink.ci_script = [ci_pending()]
    published = _published(client, github, sink, number)
    _reconcile()

    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))

    assert removed.json()["status"] == "factory_cancellation_requested"
    observed = len(sink.ci_observations)
    _reconcile()
    assert len(sink.ci_observations) == observed
    assert _terminal(number) == ("cancellation_requested", "issue_cancelled")

    _observe_termination(client, published["id"])
    _reconcile()

    assert _terminal(number) == ("cancelled", "issue_cancelled")
    assert "Stopped:" in _body(sink, published["id"])


# --- Unreadable CI is never success ------------------------------------------------------


@pytest.mark.parametrize(
    ("entry", "reason"),
    [
        (ci_entry(check_status=403), "github_forbidden"),
        (
            (200, "BODYTEXT this is not json", 200, {"state": "pending", "statuses": []}),
            "malformed_response",
        ),
        (ci_entry(check_run("build"), total=101), "too_many_check_runs"),
    ],
)
def test_unreadable_ci_ends_unverified_at_once(admitted: Any, entry: Any, reason: str) -> None:
    client, github, sink = admitted
    number = 9717
    sink.ci_script = [entry]
    published = _published(client, github, sink, number)

    _reconcile()

    assert _terminal(number) == ("failed", "ci_unverified")
    body = _body(sink, published["id"])
    assert body.startswith("Could not complete:")
    assert "unverified" in body
    assert f"Reason: {reason}" in body
    assert "BODYTEXT" not in body
    assert "Completed:" not in body
    assert _ci_turns(published["id"]) == []


# --- AC4: bounded, cannot hang --------------------------------------------------------------


def test_pending_forever_times_out_inside_the_ci_wait(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9720
    sink.ci_script = [ci_pending()]
    published = _published(client, github, sink, number)

    _reconcile()
    _reconcile_later(600)
    assert _terminal(number) == ("running", None)

    _reconcile_later(1300)

    assert _terminal(number) == ("failed", "ci_timeout")
    body = _body(sink, published["id"])
    assert body.startswith("Could not complete:")
    assert "Pending checks:" in body
    assert "build" in body


def test_ci_timeout_wins_over_the_execution_deadline(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9721
    sink.ci_script = [ci_pending()]
    published = _published(client, github, sink, number)

    _reconcile_later(1900)

    assert _terminal(number) == ("failed", "ci_timeout")
    assert "Pending checks:" in _body(sink, published["id"])


def test_transient_errors_until_the_deadline_end_as_ci_timeout(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9722
    sink.ci_script = [ci_entry(check_status=502)]
    published = _published(client, github, sink, number)

    _reconcile()
    assert _terminal(number) == ("running", None)
    _reconcile_later(600)
    assert _terminal(number) == ("running", None)

    _reconcile_later(1300)

    assert _terminal(number) == ("failed", "ci_timeout")
    assert "github_error" in _body(sink, published["id"])


def test_a_lapsed_heartbeat_does_not_lose_a_request_waiting_on_ci(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9723
    sink.ci_script = [ci_pending()]
    published = _published(client, github, sink, number)
    _execute(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": published["id"]},
    )

    _reconcile()
    _reconcile()

    assert _terminal(number) == ("running", None)
    assert _terminal_notices(published["id"]) == []


# --- Fairness ----------------------------------------------------------------------------------


def test_a_pending_request_does_not_starve_a_green_one(admitted: Any) -> None:
    client, github, sink = admitted
    pending = _published(client, github, sink, 9724, head_sha=HEAD_A)
    green = _published(client, github, sink, 9725, head_sha=HEAD_B)
    sink.ci_scripts = {HEAD_A: [ci_pending()], HEAD_B: [ci_green()]}

    _reconcile()

    assert _terminal(9724) == ("running", None)
    assert _terminal(9725) == ("completed", "completed")
    assert _terminal_notices(pending["id"]) == []
    assert len(_notices(green["id"])) == 1


# --- Code review 1 regressions -------------------------------------------------------


def test_a_fix_turn_finishing_after_its_publication_succeeded_stays_running(
    admitted: Any,
) -> None:
    """The fix push landed before the turn ended; CI on it decides, not the worker."""

    client, github, sink = admitted
    number = 9720
    sink.ci_scripts = {HEAD_A: [ci_failing()], HEAD_B: [ci_pending()]}
    published = _published(client, github, sink, number)
    _reconcile()
    assert len(_ci_turns(published["id"])) == 1
    _attach_fix(
        published["work_item_id"],
        published["id"],
        revision=2,
        head_sha=HEAD_B,
        title="Fix the test",
        paths=["src/widget.py"],
        status="succeeded",
    )

    finished = _finish(client, published["id"], _epoch(published["id"]), "ci_fix_unpublished")

    assert finished.status_code == 409, finished.text
    assert _terminal(number) == ("running", None)


def _passes_on_one_reconciler(count: int) -> None:
    """Several passes on ONE reconciler, so its CI poll schedule persists."""

    async def go() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        reconciler = WorkItemReconciler(maker, valkey, get_settings())
        try:
            for _ in range(count):
                await reconciler.run_once()
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(go())


def test_a_later_green_request_is_observed_while_older_ones_wait_on_ci(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    assert get_settings().work_item_batch_limit >= 6
    waiting = []
    for offset in range(5):
        head = f"{offset + 1:02x}" * 20
        sink.ci_scripts[head] = [ci_pending()]
        waiting.append(_published(client, github, sink, 9730 + offset, head_sha=head))
    green_head = "9e" * 20
    sink.ci_scripts[green_head] = [ci_green()]
    later = _published(client, github, sink, 9739, head_sha=green_head)

    # Three passes inside one 20 s poll interval: the five pending requests are
    # not due after their first observation, so they must not use the slots.
    _passes_on_one_reconciler(3)

    assert green_head in sink.ci_observations
    assert _terminal(9739) == ("completed", "completed")
    assert _body(sink, later["id"]).startswith(f"Completed: {later['pr_url']}")
    assert all(_terminal(9730 + i) == ("running", None) for i in range(5))


def test_a_claim_lost_during_a_slow_dispatch_enqueues_the_round_once(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The round claim expires mid-dispatch and a second reconciler runs meanwhile."""

    import redis.asyncio as aioredis
    from curie_api import factory_ci

    client, github, sink = admitted
    number = 9740
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    key = _ci_key(published["id"], 2)
    original_dispatch = WorkItemReconciler._dispatch_ci_turn
    original_gate = factory_ci.gate
    results: list[str] = []
    stalled: list[bool] = []

    async def gate(*args: Any, **kwargs: Any) -> Any:
        result = await original_gate(*args, **kwargs)
        results.append(result)
        return result

    async def slow(self: WorkItemReconciler, request: Any, round_: int, text_: str) -> bool:
        if not stalled:
            stalled.append(True)
            # The claim's TTL runs out while this dispatch is still in flight.
            other = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
            engine = create_async_engine(get_settings().database_url)
            try:
                await other.delete(key)
                second = WorkItemReconciler(
                    async_sessionmaker(engine, expire_on_commit=False), other, get_settings()
                )
                await second.run_once()
            finally:
                await other.aclose()
                await engine.dispose()
        return await original_dispatch(self, request, round_, text_)

    monkeypatch.setattr(factory_ci, "gate", gate)
    monkeypatch.setattr(WorkItemReconciler, "_dispatch_ci_turn", slow)

    _reconcile()

    assert stalled == [True]
    assert [t["event_id"] for t in _ci_turns(published["id"])] == [
        f"work-item-{published['id']}-ci-2"
    ]
    # The first reconciler's gate finishes last; it lost its claim, so it must
    # not report the continuation as its own success.
    assert results and results[-1] != "continued", results
    assert _terminal(number) == ("running", None)


def test_a_reconciler_cancelled_after_the_enqueue_marker_still_enqueues_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancellation between the round marker and the stream write loses no turn."""

    client, github, sink = admitted
    number = 9741
    sink.ci_script = [ci_failing()]
    published = _published(client, github, sink, number)
    original = WorkItemReconciler._dispatch_ci_turn
    cancelled: list[int] = []

    async def cancelled_before_xadd(
        self: WorkItemReconciler, request: Any, round_: int, text_: str
    ) -> bool:
        if not cancelled:
            cancelled.append(round_)
            raise asyncio.CancelledError
        return await original(self, request, round_, text_)

    monkeypatch.setattr(WorkItemReconciler, "_dispatch_ci_turn", cancelled_before_xadd)

    with contextlib.suppress(asyncio.CancelledError):
        _reconcile()

    assert cancelled == [2]
    assert _ci_turns(published["id"]) == []
    # The cancelled reconciler never released its claim; let its TTL run out.
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        valkey.delete(_ci_key(published["id"], 2))
    finally:
        valkey.close()

    _reconcile()
    _reconcile()

    assert [t["event_id"] for t in _ci_turns(published["id"])] == [
        f"work-item-{published['id']}-ci-2"
    ]
    assert _terminal(number) == ("running", None)


def test_a_later_green_request_is_observed_when_slow_github_keeps_older_ones_due(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each CI read takes a full poll interval, so older requests are due every pass."""

    import curie_api.workitems as workitems
    from curie_api import factory_ci, workitem_outcomes

    client, github, sink = admitted
    assert get_settings().work_item_batch_limit >= 5
    for offset in range(4):
        head = f"{offset + 0x11:02x}" * 20
        sink.ci_scripts[head] = [ci_pending()]
        _published(client, github, sink, 9750 + offset, head_sha=head)
    green_head = "9f" * 20
    sink.ci_scripts[green_head] = [ci_green()]
    later = _published(client, github, sink, 9759, head_sha=green_head)

    elapsed = [0.0]
    original_now = workitems._database_now
    original_observe = workitem_outcomes.observe_ci_detail

    async def now(session: AsyncSession) -> Any:
        return await original_now(session) + timedelta(seconds=elapsed[0])

    async def slow_observe(*args: Any, **kwargs: Any) -> Any:
        detail = await original_observe(*args, **kwargs)
        elapsed[0] += factory_ci.CI_POLL_SECONDS
        return detail

    monkeypatch.setattr(workitems, "_database_now", now)
    monkeypatch.setattr(workitem_outcomes, "observe_ci_detail", slow_observe)

    _passes_on_one_reconciler(3)

    assert green_head in sink.ci_observations
    assert _terminal(9759) == ("completed", "completed")
    assert _body(sink, later["id"]).startswith(f"Completed: {later['pr_url']}")
    assert all(_terminal(9750 + i) == ("running", None) for i in range(4))


# --- A CI wait is not an orphan (#3097 x #3076) --------------------------------

_OWNERS_URL = "/v1/internal/work-items/runtime-owners"


def _owner_lost(client: Any, request_id: uuid.UUID, epoch: int) -> Any:
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/owner-lost",
        headers=WORKER,
        json={"owner": "factory-owner", "runtime_epoch": epoch},
    )


def _listed_owner_ids(client: Any) -> list[str]:
    listed = client.get(_OWNERS_URL, headers=WORKER)
    assert listed.status_code == 200, listed.text
    return [row["request_id"] for row in listed.json()["requests"]]


@pytest.mark.parametrize("check", ["listed", "declared"])
@pytest.mark.parametrize("publication", ["succeeded", "pending"])
def test_a_request_awaiting_ci_is_not_an_orphan(
    admitted: Any, publication: str, check: str
) -> None:
    """The worker released the run at publish; its orphan sweep must not kill the CI wait."""

    client, github, sink = admitted
    number = {"succeeded": 9790, "pending": 9791}[publication] + (10 if check == "declared" else 0)
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    _attach_publication(
        row["work_item_id"],
        status=publication,
        pr=next(_PRS) if publication == "succeeded" else None,
    )

    if check == "listed":
        assert str(row["id"]) not in _listed_owner_ids(client)
    else:
        refused = _owner_lost(client, row["id"], epoch)
        assert refused.status_code == 409, refused.text
        assert _terminal(number) == ("running", None)


def test_a_request_with_no_publication_is_still_an_orphan_candidate(
    admitted: Any,
) -> None:
    client, github, _sink = admitted
    number = 9792
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])

    assert str(row["id"]) in _listed_owner_ids(client)

    declared = _owner_lost(client, row["id"], epoch)
    assert declared.status_code == 200, declared.text
    assert _terminal(number) == ("cancellation_requested", "owner_lost")


# --- #3179: the platform reports wait_ci -----------------------------------------------


def _phases(request_id: uuid.UUID) -> list[tuple[str, int | None]]:
    return [
        (row["phase"], row["loop_round"])
        for row in _rows(
            "SELECT phase, loop_round FROM curie.execution_request_phase_reports "
            "WHERE execution_request_id = :id ORDER BY id",
            {"id": request_id},
        )
    ]


def test_pending_ci_shows_publish_done_and_wait_for_ci_in_progress(admitted: Any) -> None:
    from test_factory_progress import report

    client, github, sink = admitted
    number = 9790
    sink.ci_scripts = {HEAD_A: [ci_failing(run_id=81090)], HEAD_B: [ci_pending()]}
    published = _published(client, github, sink, number)
    request_id = published["id"]
    for phase, round_ in (("implement", 1), ("review_diff", 1), ("publish", None)):
        assert report(client, request_id, phase, round=round_).status_code == 201

    # The agent's turn ended at publication; the platform records wait_ci.
    _reconcile()
    _reconcile()
    assert _phases(request_id)[-1] == ("wait_ci", None)
    body = _body(sink, request_id)
    assert "- [x] Publish PR" in body
    assert "- [ ] **Wait for CI** (in progress)" in body

    # The CI failure resumes the run: the agent loops back to implement, round 2.
    assert len(_ci_turns(request_id)) == 1
    assert report(client, request_id, "implement", round=2).status_code == 201
    _reconcile()
    body = _body(sink, request_id)
    assert "- [ ] **Implement** (in progress, round 2 of 3)" in body
    assert "- [ ] Wait for CI\n" in body
    assert _phases(request_id).count(("wait_ci", None)) == 1

    # The fix round publishes; its CI is pending, so wait_ci is recorded again.
    _attach_fix(
        published["work_item_id"],
        request_id,
        revision=2,
        head_sha=HEAD_B,
        title="Fix the widget",
        paths=["src/widget.py"],
    )
    _reconcile()
    _reconcile()
    assert _phases(request_id)[-1] == ("wait_ci", None)
    assert "- [ ] **Wait for CI** (in progress)" in _body(sink, request_id)
    assert _terminal(number) == ("running", None)
