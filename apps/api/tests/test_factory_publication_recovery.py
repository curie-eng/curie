"""A failed publication recovers its retained candidate as one fresh request (#5429).

Every scenario drives the real paths: the real app from ``create_app()``,
requests admitted through the internal admissions route, the real reconciler
pass that wakes a due request, the real acquire and start routes, the real
candidate capture routes, and the real settlement entry points (the finish
route, the reconciler's publication settlement and ``lifecycle.fail_execution``,
the call that settlement makes). Postgres and Valkey are real. State outside
the code under test is arranged by SQL, as ``test_factory_retry.py`` does;
nothing inside the API is patched.

Each refusal runs beside a liveness control on the same fixture that admits.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import redis
from curie_api.config import get_settings
from curie_api.factory_notices import (
    RetrySuccessor,
    cause_text,
    result_section,
    retry_successor,
    utc_text,
)
from curie_api.main import create_app
from curie_api.models import ExecutionRequest
from curie_api.workitems import lifecycle
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from test_factory_retry import (
    OWNER,
    REQUESTER,
    WORKER,
    Ref,
    _admit,
    _agent,
    _call,
    _configure,
    _db,
    _execute,
    _finish,
    _lose_owner,
    _make_due,
    _published_wake,
    _reconcile,
    _request,
    _requests,
    _rows,
    _successor,
    _used,
    _versions,
    _wake,
)
from test_publication_candidates import (
    ARCHIVE_LIMIT,
    BASE,
    BODY,
    PATHS,
    REPOSITORY,
    TITLE,
    _completed,
    _expiry,
    _listing,
    _ok,
    _post,
    _refused,
)
from test_workitems import _attach_own_publication

OTHER_BASE = "b" * 40
IMMUTABLE = "execution request recovery identity is immutable"


@pytest.fixture
def api(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    _configure(monkeypatch)
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", str(ARCHIVE_LIMIT))
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", raising=False)
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


@dataclass(frozen=True)
class Run:
    """A started request, its runtime authority and the acquire answer."""

    ref: Ref
    epoch: int
    generation: int
    grant: dict[str, Any]

    def authority(self) -> dict[str, Any]:
        return {
            "work_item_id": str(self.ref.work_item_id),
            "request_id": str(self.ref.request_id),
            "runtime_epoch": self.epoch,
            "dispatch_generation": self.generation,
        }


def _item(api: TestClient, agent_id: uuid.UUID, issue: int) -> Ref:
    """Admit a WorkItem and give it the recorded base its captures retain."""

    ref = _admit(api, agent_id, issue)
    _execute(
        "UPDATE curie.work_items SET base_branch = 'main', base_source = 'default', "
        "base_commit = :base WHERE id = :id",
        {"base": BASE, "id": ref.work_item_id},
    )
    return ref


async def _acquire_and_start(app: Any, request_id: uuid.UUID, generation: int) -> tuple[Any, int]:
    """Acquire and start through the raw worker routes, keeping the acquire answer."""

    wake = (await _wake(request_id)).model_dump(mode="json")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as http:
        acquired = await http.post(
            f"/v1/internal/work-items/requests/{request_id}/acquire",
            json={"owner": OWNER, "generation": generation, "wake": wake, "context": None},
            headers=WORKER,
        )
        assert acquired.status_code == 200, acquired.text
        grant = acquired.json()
        started = await http.post(
            f"/v1/internal/work-items/requests/{request_id}/start",
            json={
                "owner": OWNER,
                "generation": generation,
                "claim_name": f"curie-thread-{request_id.hex[:12]}",
                "sandbox_name": f"sbx-curie-thread-{request_id.hex[:12]}",
                "context": grant["context"],
            },
            headers=WORKER,
        )
        assert started.status_code == 200, started.text
        return grant, int(started.json()["runtime_epoch"])


def _run(api: TestClient, request_id: uuid.UUID, work_item_id: uuid.UUID) -> Run:
    """Make the request due, let the reconciler wake it, then acquire and start it."""

    _make_due(request_id)
    _reconcile()
    generation = _published_wake(request_id)
    grant, epoch = _call(api, _acquire_and_start, api.app, request_id, generation)
    return Run(Ref(request_id, work_item_id), epoch, generation, grant)


def _capture(api: TestClient, run: Run, *, base: bool = True) -> dict[str, Any]:
    """Retain one complete candidate through the real capture routes."""

    return _completed(api, run.authority(), base=base)


def _candidate(reservation: dict[str, Any]) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT * FROM curie.publication_candidates WHERE id = :id",
        {"id": uuid.UUID(reservation["candidate_id"])},
    )
    return row


def _settle(ref: Ref, cause: str) -> None:
    """The settlement the reconciler makes for a terminal publication: no worker fact."""

    work_version, request_version = _versions(ref.request_id)

    async def body(session: AsyncSession) -> lifecycle.WorkItemResult:
        return await lifecycle.fail_execution(
            session,
            work_item_id=ref.work_item_id,
            request_id=ref.request_id,
            cause=cause,
            expected_work_item_version=work_version,
            expected_request_version=request_version,
            retry_safety=None,
            retry_condition=None,
        )

    result = _db(body)
    assert isinstance(result, lifecycle.WorkItemOutcome), result


def _settled(request_id: uuid.UUID) -> dict[str, Any]:
    row = _request(request_id)
    return {
        key: row[key]
        for key in ("status", "terminal_at", "terminal_cause", "version", "retry_safety")
    }


def _assert_recovery(
    predecessor_id: uuid.UUID, reservation: dict[str, Any], *, used: int
) -> dict[str, Any]:
    """The one recovery successor admitted for ``predecessor_id``, holding the candidate."""

    predecessor = _request(predecessor_id)
    successor = _successor(predecessor_id)
    assert successor is not None
    candidate = _candidate(reservation)
    assert (successor["status"], successor["blocked_on"]) == ("waiting", None)
    assert successor["execution_kind"] == "publication_recovery"
    assert successor["recovery_candidate_id"] == candidate["id"]
    assert successor["recovery_candidate_sha256"] == candidate["content_sha256"]
    assert successor["completion_contract"] == "implementation"
    assert successor["owner_lost_retry"] is False
    assert successor["retry_origin_request_id"] == (
        predecessor["retry_origin_request_id"] or predecessor_id
    )
    for column in ("objective", "requester", "reply_conversation_id", "reply_target_kind"):
        assert successor[column] == predecessor[column], column
    low, high = {1: (24, 36), 2: (96, 144)}[used]
    due = successor["dispatch_not_before"] - predecessor["terminal_at"]
    assert timedelta(seconds=low) <= due <= timedelta(seconds=high + 1)
    assert _used(predecessor["work_item_id"]) == used
    assert candidate["owner_request_id"] == successor["id"]
    assert candidate["previous_owner_request_id"] == predecessor_id
    assert candidate["owner_transferred_at"] is not None
    assert candidate["expires_at"] is None
    assert candidate["status"] == "complete"
    return successor


def _assert_no_recovery(ref: Ref, *, used: int = 0) -> None:
    assert _successor(ref.request_id) is None
    assert _used(ref.work_item_id) == used
    assert not any(row["execution_kind"] != "implementation" for row in _requests(ref.work_item_id))


def _captured(api: TestClient, agent_id: uuid.UUID, issue: int) -> tuple[Run, dict[str, Any]]:
    item = _item(api, agent_id, issue)
    run = _run(api, item.request_id, item.work_item_id)
    return run, _capture(api, run)


# -- admission ----------------------------------------------------------------


def test_publication_failure_admits_one_recovery_that_holds_the_candidate(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    run, reservation = _captured(api, _agent(api, auth_headers), 54700)
    assert run.grant["execution_kind"] == "implementation"
    assert run.grant["recovery_candidate_id"] is None
    predecessor_id = run.ref.request_id

    async def publish(session: AsyncSession) -> None:
        await _attach_own_publication(
            session, run.ref.work_item_id, predecessor_id, status="failed"
        )

    _db(publish)
    # The reconciler settles the failed publication with no worker fact.
    _reconcile()
    settled = _settled(predecessor_id)
    assert (settled["status"], settled["terminal_cause"], settled["retry_safety"]) == (
        "failed",
        "publication_failed",
        None,
    )
    successor = _assert_recovery(predecessor_id, reservation, used=1)
    assert len(_requests(run.ref.work_item_id)) == 2

    recovery = _run(api, successor["id"], run.ref.work_item_id)
    assert recovery.grant["execution_kind"] == "publication_recovery"
    assert recovery.grant["recovery_candidate_id"] == reservation["candidate_id"]
    assert recovery.epoch >= 1
    answer = _ok(_post(api, "recovery", recovery.authority()))
    assert answer["candidate_id"] == reservation["candidate_id"]
    # The terminal predecessor is never rewritten by the recovery's admission or start.
    assert _settled(predecessor_id) == settled


@pytest.mark.parametrize("safety", ["no_effects", "flagged"])
def test_approval_create_failure_admits_recovery_without_an_uncertain_effect(
    api: TestClient, auth_headers: dict[str, str], safety: str
) -> None:
    run, reservation = _captured(api, _agent(api, auth_headers), 54701)
    finished = _finish(api, run.ref.request_id, run.epoch, "approval_create_failed", safety)
    assert finished.status_code == 200, finished.text
    assert _request(run.ref.request_id)["retry_safety"] == safety
    _assert_recovery(run.ref.request_id, reservation, used=1)


def test_an_uncertain_effect_admits_no_recovery(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    uncertain, kept = _captured(api, agent_id, 54702)
    clean, control = _captured(api, agent_id, 54703)
    finished = _finish(
        api, uncertain.ref.request_id, uncertain.epoch, "approval_create_failed", "uncertain"
    )
    assert finished.status_code == 200, finished.text
    _assert_no_recovery(uncertain.ref)
    assert _candidate(kept)["owner_request_id"] == uncertain.ref.request_id
    # The same failure with a proved clean run recovers.
    finished = _finish(
        api, clean.ref.request_id, clean.epoch, "approval_create_failed", "no_effects"
    )
    assert finished.status_code == 200, finished.text
    _assert_recovery(clean.ref.request_id, control, used=1)


def test_recovery_failures_spend_the_shared_allowance_then_keep_the_candidate(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    run, reservation = _captured(api, _agent(api, auth_headers), 54704)
    _settle(run.ref, "publication_failed")
    first = _assert_recovery(run.ref.request_id, reservation, used=1)
    original = _settled(run.ref.request_id)

    # The recovery request's own publication failure recovers again.
    second_run = _run(api, first["id"], run.ref.work_item_id)
    _settle(second_run.ref, "publication_failed")
    second = _assert_recovery(first["id"], reservation, used=2)
    recovered = _settled(first["id"])

    third_run = _run(api, second["id"], run.ref.work_item_id)
    assert third_run.grant["recovery_candidate_id"] == reservation["candidate_id"]
    _settle(third_run.ref, "publication_failed")
    assert _successor(second["id"]) is None
    assert _used(run.ref.work_item_id) == 2
    assert len(_requests(run.ref.work_item_id)) == 3

    # Retained for its window, fixed exactly from the last owner's disposition.
    listing = _listing(api, auth_headers, {"work_item_id": str(run.ref.work_item_id)})
    (public,) = listing["candidates"]
    assert public["owner_request_id"] == str(second["id"])
    assert public["available"] is True
    candidate = _candidate(reservation)
    assert candidate["owner_request_id"] == second["id"]
    assert candidate["previous_owner_request_id"] == first["id"]
    expiry = _expiry(reservation)
    assert expiry["exact"] is True and expiry["past"] is False
    assert _settled(run.ref.request_id) == original
    assert _settled(first["id"]) == recovered


@pytest.mark.parametrize("origin", ["ordinary", "owner_loss"])
def test_a_recovery_request_never_buys_an_implementation_run(
    api: TestClient, auth_headers: dict[str, str], origin: str
) -> None:
    agent_id = _agent(api, auth_headers)
    run, reservation = _captured(api, agent_id, 54720 if origin == "ordinary" else 54722)
    _settle(run.ref, "publication_failed")
    recovery = _assert_recovery(run.ref.request_id, reservation, used=1)
    recovery_run = _run(api, recovery["id"], run.ref.work_item_id)
    control, _ = _captured(api, agent_id, 54721 if origin == "ordinary" else 54723)
    if origin == "ordinary":
        for ref, epoch in ((recovery_run.ref, recovery_run.epoch), (control.ref, control.epoch)):
            finished = _finish(api, ref.request_id, epoch, "model_unreachable", "no_effects")
            assert finished.status_code == 200, finished.text
    else:
        _lose_owner(api, recovery["id"], started=True)
        _lose_owner(api, control.ref.request_id, started=True)
    # A recovery request's own non-publication failure admits nothing.
    assert _successor(recovery["id"]) is None
    assert _used(run.ref.work_item_id) == 1
    assert len(_requests(run.ref.work_item_id)) == 2
    assert _candidate(reservation)["owner_request_id"] == recovery["id"]
    # The same failure of an implementation request admits its ordinary successor.
    successor = _successor(control.ref.request_id)
    assert successor is not None
    assert successor["execution_kind"] == "implementation"
    assert _used(control.ref.work_item_id) == 1


@pytest.mark.parametrize("variant", ["no_candidate", "expired", "base_changed"])
def test_no_exact_retained_candidate_admits_no_recovery(
    api: TestClient, auth_headers: dict[str, str], variant: str
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _item(api, agent_id, 54705)
    refused = _run(api, item.request_id, item.work_item_id)
    control, control_reservation = _captured(api, agent_id, 54706)
    reservation = None if variant == "no_candidate" else _capture(api, refused)
    if variant == "expired":
        assert reservation is not None
        _execute(
            "UPDATE curie.publication_candidates "
            "SET expires_at = clock_timestamp() - interval '1 second' WHERE id = :id",
            {"id": uuid.UUID(reservation["candidate_id"])},
        )
    elif variant == "base_changed":
        _execute(
            "UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
            {"base": OTHER_BASE, "id": item.work_item_id},
        )
    _settle(refused.ref, "publication_failed")
    _assert_no_recovery(refused.ref)
    if reservation is not None:
        assert _candidate(reservation)["owner_request_id"] == refused.ref.request_id
    # The unchanged neighbor with its own exact candidate recovers.
    _settle(control.ref, "publication_failed")
    _assert_recovery(control.ref.request_id, control_reservation, used=1)


def test_a_candidate_owned_by_an_earlier_request_admits_no_recovery(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    runs: list[tuple[Run, dict[str, Any]]] = []
    for issue in (54707, 54708):
        first, earlier = _captured(api, agent_id, issue)
        finished = _finish(api, first.ref.request_id, first.epoch, "model_unreachable")
        assert finished.status_code == 200, finished.text
        retry = _successor(first.ref.request_id)
        assert retry is not None and retry["execution_kind"] == "implementation"
        runs.append((_run(api, retry["id"], first.ref.work_item_id), earlier))

    # The retry captured nothing, and the earlier capture is not its own.
    (bare, bare_earlier), (control, control_earlier) = runs
    _settle(bare.ref, "publication_failed")
    _assert_no_recovery(bare.ref, used=1)
    assert _candidate(bare_earlier)["owner_request_id"] != bare.ref.request_id

    # A retry with its own capture recovers that capture, not the earlier one.
    own = _capture(api, control, base=False)
    _settle(control.ref, "publication_failed")
    _assert_recovery(control.ref.request_id, own, used=2)
    earlier = _candidate(control_earlier)
    assert earlier["owner_request_id"] == earlier["origin_request_id"] != control.ref.request_id


@pytest.mark.parametrize("fence", ["cancellation", "closed_lineage", "queued_revision"])
def test_a_fenced_work_item_admits_no_recovery(
    api: TestClient, auth_headers: dict[str, str], fence: str
) -> None:
    agent_id = _agent(api, auth_headers)
    fenced, kept = _captured(api, agent_id, 54709)
    control, control_reservation = _captured(api, agent_id, 54710)
    ref = fenced.ref
    work_version, _ = _versions(ref.request_id)
    if fence == "cancellation":

        async def cancel(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.request_cancellation(
                session, work_item_id=ref.work_item_id, expected_work_item_version=work_version
            )

        assert isinstance(_db(cancel), lifecycle.WorkItemOutcome)
    elif fence == "closed_lineage":
        _execute(
            "UPDATE curie.execution_requests SET reply_target_kind = 'review_thread', "
            "reply_target_pr_number = '123', reply_target_comment_id = '88201', "
            "reply_target_url = 'https://github.com/acme-corp/acme-bot/pull/123' "
            "WHERE id = :id",
            {"id": ref.request_id},
        )
    else:
        conversation = _request(ref.request_id)["reply_conversation_id"]

        async def revise(session: AsyncSession) -> lifecycle.WorkItemResult:
            return await lifecycle.create_revision_request(
                session,
                work_item_id=ref.work_item_id,
                request_id=uuid.uuid4(),
                wait_deadline=datetime.now(UTC) + timedelta(hours=1),
                expected_work_item_version=work_version,
                snapshot={
                    "objective": "Address the follow up comment",
                    "requester": REQUESTER,
                    "reply_kind": "slack",
                    "reply_address": "C0EXAMPLE1",
                    "reply_conversation_id": conversation,
                },
            )

        assert isinstance(_db(revise), lifecycle.WorkItemOutcome)
    finished = _finish(api, ref.request_id, fenced.epoch, "approval_create_failed")
    if fence == "cancellation":
        assert finished.status_code == 409, finished.text
        assert finished.json()["detail"]["code"] == "work_item_cancelled"
    else:
        assert finished.status_code == 200, finished.text
    _assert_no_recovery(ref)
    assert _candidate(kept)["owner_request_id"] == ref.request_id
    finished = _finish(api, control.ref.request_id, control.epoch, "approval_create_failed")
    assert finished.status_code == 200, finished.text
    _assert_recovery(control.ref.request_id, control_reservation, used=1)


@pytest.mark.parametrize("cause", ["publication_denied", "publication_expired"])
def test_a_refused_publication_stays_terminal(
    api: TestClient, auth_headers: dict[str, str], cause: str
) -> None:
    agent_id = _agent(api, auth_headers)
    refused, kept = _captured(api, agent_id, 54711)
    control, control_reservation = _captured(api, agent_id, 54712)
    _settle(refused.ref, cause)
    assert _request(refused.ref.request_id)["terminal_cause"] == cause
    _assert_no_recovery(refused.ref)
    assert _candidate(kept)["owner_request_id"] == refused.ref.request_id
    _settle(control.ref, "publication_failed")
    _assert_recovery(control.ref.request_id, control_reservation, used=1)


# -- the recovery route ---------------------------------------------------------


def _recovery_run(api: TestClient, agent_id: uuid.UUID, issue: int) -> tuple[Run, Run, Any]:
    """An implementation run that captured and failed publication, and its started recovery."""

    run, reservation = _captured(api, agent_id, issue)
    _settle(run.ref, "publication_failed")
    successor = _successor(run.ref.request_id)
    assert successor is not None
    return run, _run(api, successor["id"], run.ref.work_item_id), reservation


def test_recovery_route_answers_only_the_exact_candidate_of_a_live_recovery(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    item = _item(api, agent_id, 54713)
    implementation = _run(api, item.request_id, item.work_item_id)
    _refused(_post(api, "recovery", implementation.authority()), 409, "recovery_not_requested")
    reservation = _capture(api, implementation)
    _settle(implementation.ref, "publication_failed")
    successor = _successor(item.request_id)
    assert successor is not None
    recovery = _run(api, successor["id"], item.work_item_id)

    patch = next(obj for obj in reservation["objects"] if obj["kind"] == "patch")
    held = _candidate(reservation)
    expected = {
        "candidate_id": reservation["candidate_id"],
        "content_sha256": held["content_sha256"],
        "repository": REPOSITORY,
        "base_commit": BASE,
        "target_branch": "main",
        "changed_paths": PATHS,
        "contains_workflow_files": False,
        "title": TITLE,
        "body": BODY,
        "patch": patch,
    }
    assert _ok(_post(api, "recovery", recovery.authority())) == expected

    stale = {**recovery.authority(), "runtime_epoch": recovery.epoch + 1}
    _refused(_post(api, "recovery", stale), 409, "source_identity")

    candidate_id = uuid.UUID(reservation["candidate_id"])
    _execute(
        "UPDATE curie.publication_candidates SET owner_request_id = :owner, "
        "previous_owner_request_id = NULL, owner_transferred_at = NULL WHERE id = :id",
        {"owner": item.request_id, "id": candidate_id},
    )
    _refused(_post(api, "recovery", recovery.authority()), 409, "recovery_candidate_unavailable")
    _execute(
        "UPDATE curie.publication_candidates SET owner_request_id = :owner, "
        "previous_owner_request_id = :previous, owner_transferred_at = :at WHERE id = :id",
        {
            "owner": held["owner_request_id"],
            "previous": held["previous_owner_request_id"],
            "at": held["owner_transferred_at"],
            "id": candidate_id,
        },
    )
    assert _ok(_post(api, "recovery", recovery.authority())) == expected

    _execute(
        "UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
        {"base": OTHER_BASE, "id": item.work_item_id},
    )
    _refused(_post(api, "recovery", recovery.authority()), 409, "recovery_base_changed")
    _execute(
        "UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
        {"base": BASE, "id": item.work_item_id},
    )
    assert _ok(_post(api, "recovery", recovery.authority())) == expected

    # Reads never renew retention or move ownership.
    after = _candidate(reservation)
    for column in ("owner_request_id", "previous_owner_request_id", "expires_at", "version"):
        assert after[column] == held[column], column

    # A refused recovery is terminal and its authority ends with it.
    finished = _finish(api, recovery.ref.request_id, recovery.epoch, "publication_recovery_refused")
    assert finished.status_code == 200, finished.text
    assert _request(recovery.ref.request_id)["terminal_cause"] == "publication_recovery_refused"
    assert _successor(recovery.ref.request_id) is None
    _refused(_post(api, "recovery", recovery.authority()), 409, "expired_source")


def test_recovery_identity_is_immutable(api: TestClient, auth_headers: dict[str, str]) -> None:
    _, recovery, reservation = _recovery_run(api, _agent(api, auth_headers), 54714)
    request_id = recovery.ref.request_id

    def update(assignment: str) -> str:
        async def body(session: AsyncSession) -> str:
            try:
                await session.execute(
                    text(f"UPDATE curie.execution_requests SET {assignment} WHERE id = :id"),
                    {"id": request_id},
                )
                await session.commit()
            except DBAPIError as error:
                await session.rollback()
                return str(error.orig)
            return ""

        return _db(body)

    assert IMMUTABLE in update("execution_kind = 'implementation'")
    assert IMMUTABLE in update("recovery_candidate_id = gen_random_uuid()")
    assert IMMUTABLE in update(f"recovery_candidate_sha256 = '{'0' * 64}'")
    # Writing the same identity back still succeeds.
    assert (
        update("execution_kind = execution_kind, recovery_candidate_id = recovery_candidate_id")
        == ""
    )
    row = _request(request_id)
    assert row["execution_kind"] == "publication_recovery"
    assert row["recovery_candidate_id"] == uuid.UUID(reservation["candidate_id"])


# -- adoptable revisions --------------------------------------------------------


async def _publication_on(
    session: AsyncSession,
    lineage_id: uuid.UUID,
    request_id: uuid.UUID | None,
    *,
    status: str,
    revision: int,
) -> uuid.UUID:
    """One more terminal publication row on ``lineage_id``, by ``request_id``."""

    (lineage,) = (
        await session.execute(
            text(
                "SELECT agent_id, deployment_id, conversation_id, repo_full_name "
                "FROM curie.thread_publication_lineages WHERE id = :id"
            ),
            {"id": lineage_id},
        )
    ).mappings()
    approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.approvals (id, agent_id, conversation_id, author, summary, "
            "reply_kind, reply_channel, dedupe_key, status, purpose) VALUES "
            "(:id, :agent, :conversation, 'U0REQUEST1', 'Publish repository changes', "
            "'github', :channel, :dedupe, 'approved', 'publication')"
        ),
        {
            "id": approval_id,
            "agent": lineage["agent_id"],
            "conversation": lineage["conversation_id"],
            "channel": lineage["repo_full_name"],
            "dedupe": f"adoptable-{publication_id.hex}",
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.publications (id, approval_id, deployment_id, "
            "workspace_conversation_id, lineage_id, execution_request_id, revision_number, "
            "repo_full_name, status, base_sha, changed_paths, title, body, reply_kind, "
            "reply_channel) VALUES (:id, :approval, :deployment, :conversation, :lineage, "
            ":request, :revision, :repo, :status, :base_sha, "
            "CAST('[\"README.md\"]' AS jsonb), 'Update README', "
            "'Approved platform publication.', 'github', :repo)"
        ),
        {
            "id": publication_id,
            "approval": approval_id,
            "deployment": lineage["deployment_id"],
            "conversation": lineage["conversation_id"],
            "lineage": lineage_id,
            "request": request_id,
            "revision": revision,
            "repo": lineage["repo_full_name"],
            "status": status,
            "base_sha": BASE,
        },
    )
    await session.commit()
    return publication_id


def _adoptable(request_id: uuid.UUID, lineage_id: uuid.UUID) -> list[uuid.UUID]:
    from curie_api.crud.publications import adoptable_revision_ids

    async def body(session: AsyncSession) -> list[uuid.UUID]:
        request = await session.get(ExecutionRequest, request_id)
        assert request is not None
        return await adoptable_revision_ids(session, request, lineage_id)

    return _db(body)


def test_adoptable_revisions_follow_the_candidate_ownership_chain(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """A recovery may adopt only failed revisions its candidate's owners created (#5429)."""

    agent_id = _agent(api, auth_headers)
    run, _reservation = _captured(api, agent_id, 54717)
    origin_id = run.ref.request_id

    async def publish(session: AsyncSession) -> None:
        await _attach_own_publication(session, run.ref.work_item_id, origin_id, status="failed")

    _db(publish)
    ((origin_publication, lineage_id),) = (
        (row["id"], row["lineage_id"])
        for row in _rows(
            "SELECT id, lineage_id FROM curie.publications WHERE execution_request_id = :id",
            {"id": origin_id},
        )
    )
    _reconcile()
    first = _successor(origin_id)
    assert first is not None and first["execution_kind"] == "publication_recovery"

    # Never adoptable: a revision that never pushed, one outside the chain, and
    # a revision of an unrelated implementation request on the same lineage.
    stranger = _item(api, agent_id, 54718)

    async def others(session: AsyncSession) -> None:
        await _publication_on(session, lineage_id, origin_id, status="denied", revision=2)
        await _publication_on(session, lineage_id, None, status="failed", revision=3)
        await _publication_on(session, lineage_id, stranger.request_id, status="failed", revision=4)

    _db(others)

    # An implementation request adopts nothing, even its own failed revision.
    assert _adoptable(origin_id, lineage_id) == []
    assert _adoptable(first["id"], lineage_id) == [origin_publication]

    # The first recovery's own revision failed too; the next owner may adopt both.
    first_run = _run(api, first["id"], run.ref.work_item_id)

    async def recovered(session: AsyncSession) -> uuid.UUID:
        return await _publication_on(session, lineage_id, first["id"], status="failed", revision=5)

    first_publication = _db(recovered)
    _settle(first_run.ref, "publication_failed")
    second = _successor(first["id"])
    assert second is not None and second["execution_kind"] == "publication_recovery"
    assert _adoptable(second["id"], lineage_id) == [origin_publication, first_publication]
    # Scoped to the lineage the new publication extends.
    assert _adoptable(second["id"], uuid.uuid4()) == []


# -- notices --------------------------------------------------------------------


def _notice_successor(request_id: uuid.UUID) -> RetrySuccessor | None:
    async def body(session: AsyncSession) -> RetrySuccessor | None:
        row = await session.get(ExecutionRequest, request_id)
        assert row is not None
        return await retry_successor(session, row)

    return _db(body)


def test_retry_notice_names_the_candidate_and_the_recovery_slot(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    run, recovery, reservation = _recovery_run(api, agent_id, 54715)
    successor = _notice_successor(run.ref.request_id)
    assert successor is not None
    assert (successor.publication_recovery, successor.slot) == (True, 1)
    assert successor.candidate_id == uuid.UUID(reservation["candidate_id"])
    body = result_section("publication_failed", pr_url=None, successor=successor)
    assert body.startswith(
        "Recovering publication: this run's finished change is retained as candidate "
        f"`{reservation['candidate_id']}`, so Curie admitted publication recovery 1 of 2 "
        "to verify and publish it without another model run.\n"
    )
    assert (
        "Scheduled: the automatic retry starts at or after "
        f"{utc_text(successor.dispatch_not_before)}."
    ) in body
    assert "Cause: publication_failed\n" in body
    assert "Could not complete:" not in body

    # An ordinary failure's successor keeps the ordinary sentence.
    ordinary = _item(api, agent_id, 54716)
    started = _run(api, ordinary.request_id, ordinary.work_item_id)
    finished = _finish(api, ordinary.request_id, started.epoch, "model_unreachable")
    assert finished.status_code == 200, finished.text
    plain = _notice_successor(ordinary.request_id)
    assert plain is not None
    assert (plain.publication_recovery, plain.candidate_id) == (False, None)
    text_ = result_section("model_unreachable", pr_url=None, successor=plain)
    assert text_.startswith("Retrying: this run failed with no side effects")

    # A refused recovery reads as its own cause, never as an unknown one.
    finished = _finish(api, recovery.ref.request_id, recovery.epoch, "publication_recovery_refused")
    assert finished.status_code == 200, finished.text
    assert _notice_successor(recovery.ref.request_id) is None
    sentence = cause_text("publication_recovery_refused")
    assert sentence != cause_text("not-a-cause")
    refused = result_section(
        "publication_recovery_refused",
        pr_url=None,
        detail="publication_candidates.recovery_base_changed",
    )
    assert refused.startswith(f"Could not complete: {sentence}\n")
    assert "Details: publication_candidates.recovery_base_changed\n" in refused
    assert "Cause: publication_recovery_refused\n" in refused
