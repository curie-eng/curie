"""Versioned revalidation of a personal WorkItem's enrollment generation (#5376 AC3).

The owner never changes; only ``POST /v1/internal/work-items/{id}/personal-context/
revalidate`` moves ``enrollment_generation``, and only when the caller's expected
WorkItem, request and enrollment versions are current, the named owner is the
WorkItem's, the owner's enrollment is active and has advanced, and no runtime of
the WorkItem can still be live (running, stopping, holding an acquire lease, or
force-settled without a teardown receipt). It fails closed: a request that ever
started counts as stopped only once the worker's termination chain has recorded
it terminal with a termination observation. A terminal status alone is not that
evidence; a normally finished run keeps its claim and its sandbox may outlive
the finish, and an expired heartbeat proves no more. Every refusal is HTTP 409
``{"detail": {"code": ...}}`` and leaves the durable rows untouched; each is paired
with the call that passes in the same state. Concurrent writers holding the same
expected versions bind exactly once.

``GET .../personal-context`` is the token-free read the worker revalidates from.
Missing, wrong-tenant, wrong-agent, wrong-owner and service items answer the same
404. Both routes sit behind the internal worker token.

The enrollment row is advanced with SQL, standing in for #5377's replacement.
Postgres is real; the concurrency test uses two independent engines.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
import personal_execution_support as support
import pytest
from curie_api import personal_enrollments, workitem_dispatch
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import ExecutionRequest
from curie_api.workitem_dispatch import (
    AcquireGrant,
    DeferResult,
    DispatchConflict,
    HeartbeatResult,
    TerminatePublish,
)
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import WorkItemOutcome
from fastapi.testclient import TestClient
from forge_fakes import factory_wake, no_external_calls
from forge_fakes.github import LABEL, GitHubAPI, _issue_event, _post, _sender
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from test_personal_execution_admission import (
    DEFAULT_TENANT,
    GitHubFactory,
    _github_items,
    _seed_default_tenant_owner,
    github_factory,  # noqa: F401
    owned_rows,  # noqa: F401
)
from test_personal_execution_lifecycle import _seed_transcript, _transcripts

WORKER_TOKEN = "personal-revalidation-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
NOT_FOUND = {"detail": {"code": "not_found"}}
SERVICE_CONVERSATION = "1700000000.000200"
REVISION_OBJECTIVE = "Address the follow up comment on the issue"
RELABEL_OBJECTIVE = "Run the task again after the label was added back"


def _github_personal_failure(
    github_factory: GitHubFactory,
    number: int,
    *,
    failure: Literal["authentication_required", "enrollment_fenced", "start_failed"] = (
        "authentication_required"
    ),
) -> tuple[TestClient, GitHubAPI, UUID, UUID, int]:
    owner = _seed_default_tenant_owner()
    client, github = github_factory("personal")
    account = int(github.label_actor["id"])
    _link_github_account(client, owner, account)
    first = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert first.json()["status"] == "factory_admitted", first.text
    [item] = _github_items(number)
    [request] = _resume_requests(item["id"])
    if failure == "authentication_required":
        _fail_github_personal_request(item["id"], request["id"])
    elif failure == "enrollment_fenced":
        _start_github_personal_request(item["id"], request["id"])

    async def replace_enrollment(session: AsyncSession) -> None:
        await personal_enrollments._lock_owner(session, DEFAULT_TENANT, owner)
        await session.execute(
            text(
                "UPDATE curie.personal_claude_enrollments "
                "SET generation = 2, version = version + 1 WHERE principal_id = :owner"
            ),
            {"owner": owner},
        )
        await lifecycle.fence_enrollment_generation(
            session, tenant_id=DEFAULT_TENANT, owner_principal_id=owner
        )
        await session.commit()

    support.with_session(replace_enrollment)
    if failure == "start_failed":
        support.rows(
            "UPDATE curie.execution_requests SET status = 'failed', terminal_at = now(), "
            "terminal_cause = 'start_failed', last_deferral_reason = "
            "'not_started:enrollment_generation_mismatch' WHERE id = :id",
            {"id": request["id"]},
        )
    return client, github, item["id"], request["id"], account


def _link_github_account(client: TestClient, owner: UUID, account: int) -> None:
    linked = client.post(
        f"/identity/admin/{DEFAULT_TENANT}/code-host-links",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "principal_id": str(owner),
            "code_host_kind": "github",
            "host": "github.com",
            "account_id": str(account),
            "created_by_principal_id": str(owner),
        },
    )
    assert linked.status_code == 201, linked.text


def _start_github_personal_request(item_id: UUID, request_id: UUID) -> int:
    support.rows(
        "UPDATE curie.work_items SET base_resolved_request_id = :request WHERE id = :id",
        {"id": item_id, "request": request_id},
    )
    return support.start_request(request_id)


def _fail_github_personal_request(item_id: UUID, request_id: UUID) -> None:
    epoch = _start_github_personal_request(item_id, request_id)
    support.finish_failed(request_id, epoch, cause="authentication_required")


def _resume_requests(item_id: UUID) -> list[dict[str, Any]]:
    return support.rows(
        "SELECT id, status, terminal_cause, enrollment_generation, version "
        "FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": item_id},
    )


def _confirm_resume_teardown(request_id: UUID) -> None:
    support.rows(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = clock_timestamp() "
        "WHERE id = :id",
        {"id": request_id},
    )
    support.record_termination(request_id)


def test_owner_github_relabel_resumes_at_current_enrollment_generation(
    github_factory: GitHubFactory, owned_rows: None
) -> None:
    number = 58881
    client, github, item_id, failed_id, account = _github_personal_failure(github_factory, number)
    _confirm_resume_teardown(failed_id)
    [item] = support.rows("SELECT agent_id FROM curie.work_items WHERE id = :id", {"id": item_id})
    agent = item["agent_id"]
    thread = _seed_transcript(agent, item_id)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled", removed.text
    github.labels = [LABEL]
    github.advance_label_event(number)
    relabel = _issue_event(
        "labeled", number, label={"name": LABEL}, sender=_sender(sender_id=account + 1)
    )

    [before] = _resume_requests(item_id)
    resumed = _post(client, "issues", relabel)
    assert resumed.json()["status"] == "factory_admitted", resumed.text
    assert _github_items(number)[0]["enrollment_generation"] == 2
    predecessor, successor = _resume_requests(item_id)
    assert predecessor == before
    assert (predecessor["enrollment_generation"], predecessor["terminal_cause"]) == (
        1,
        "authentication_required",
    )
    assert (successor["enrollment_generation"], successor["status"]) == (2, "waiting")
    assert _transcripts(agent, thread) == 1

    async def refusal(session: AsyncSession) -> str | None:
        request = await session.get(ExecutionRequest, successor["id"])
        assert request is not None
        return await workitem_dispatch._personal_enrollment_refusal(session, request)

    assert support.with_session(refusal) is None


def test_owner_github_relabel_waits_for_worker_teardown(
    github_factory: GitHubFactory, owned_rows: None
) -> None:
    number = 58883
    client, github, item_id, failed_id, _ = _github_personal_failure(github_factory, number)
    github.advance_label_event(number)
    relabel = _issue_event("labeled", number, label={"name": LABEL})
    before = _resume_requests(item_id)
    refused = _post(client, "issues", relabel)
    assert refused.json()["errors"] == [{"code": "runtime_live"}], refused.text
    assert any(
        "try again shortly" in comment["body"].lower() for comment in github.issue_comments[number]
    )
    assert _github_items(number)[0]["enrollment_generation"] == 1
    assert _resume_requests(item_id) == before

    _confirm_resume_teardown(failed_id)
    github.advance_label_event(number)
    resumed = _post(client, "issues", relabel)
    assert resumed.json()["status"] == "factory_admitted", resumed.text
    assert [request["enrollment_generation"] for request in _resume_requests(item_id)] == [1, 2]


def test_owner_github_relabel_recovers_a_stale_start_failure(
    github_factory: GitHubFactory, owned_rows: None
) -> None:
    number = 58885
    client, github, item_id, failed_id, _ = _github_personal_failure(
        github_factory, number, failure="start_failed"
    )
    [item] = support.rows("SELECT agent_id FROM curie.work_items WHERE id = :id", {"id": item_id})
    agent = item["agent_id"]
    thread = _seed_transcript(agent, item_id)
    [before] = _resume_requests(item_id)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled", removed.text
    github.labels = [LABEL]
    github.advance_label_event(number)

    resumed = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))

    assert resumed.json()["status"] == "factory_admitted", resumed.text
    predecessor, successor = _resume_requests(item_id)
    assert (successor["enrollment_generation"], successor["status"]) == (2, "waiting")
    assert predecessor == before
    assert (predecessor["id"], predecessor["terminal_cause"]) == (failed_id, "start_failed")
    assert _transcripts(agent, thread) == 1


@pytest.mark.parametrize("close_before_teardown", [False, True], ids=["resume", "close"])
def test_owner_unlabel_preserves_a_stopping_enrollment_fence(
    github_factory: GitHubFactory, owned_rows: None, close_before_teardown: bool
) -> None:
    number = 58886
    client, github, item_id, stopping_id, _ = _github_personal_failure(
        github_factory, number, failure="enrollment_fenced"
    )
    [item] = support.rows("SELECT agent_id FROM curie.work_items WHERE id = :id", {"id": item_id})
    agent = item["agent_id"]
    thread = _seed_transcript(agent, item_id)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancellation_requested", removed.text
    github.labels = [LABEL]
    github.advance_label_event(number)
    relabel = _issue_event("labeled", number, label={"name": LABEL})
    before = _resume_requests(item_id)

    refused = _post(client, "issues", relabel)
    assert refused.json()["errors"] == [{"code": "runtime_live"}], refused.text
    assert _resume_requests(item_id) == before
    assert _github_items(number)[0]["enrollment_generation"] == 1
    assert before[0]["terminal_cause"] == "enrollment_fenced"

    if close_before_teardown:
        github.issue_state = "closed"
        closed = _post(client, "issues", _issue_event("closed", number))
        assert closed.json()["status"] == "factory_duplicate", closed.text

    _confirm_resume_teardown(stopping_id)
    [predecessor] = _resume_requests(item_id)
    if close_before_teardown:
        assert (predecessor["status"], predecessor["terminal_cause"]) == (
            "cancelled",
            "issue_cancelled",
        )
        assert _transcripts(agent, thread) == 0
    else:
        assert (predecessor["status"], predecessor["terminal_cause"]) == (
            "failed",
            "enrollment_fenced",
        )
        github.advance_label_event(number)
        resumed = _post(client, "issues", relabel)
        assert resumed.json()["status"] == "factory_admitted", resumed.text
        old, successor = _resume_requests(item_id)
        assert old == predecessor
        assert successor["enrollment_generation"] == 2
        assert _transcripts(agent, thread) == 1


def test_non_owner_github_relabel_keeps_the_old_enrollment_generation(
    github_factory: GitHubFactory, owned_rows: None
) -> None:
    number = 58882
    client, github, item_id, failed_id, account = _github_personal_failure(github_factory, number)
    _confirm_resume_teardown(failed_id)
    other = _seed_default_tenant_owner()
    _link_github_account(client, other, account + 1)
    github.advance_label_event(number)
    github.label_actor = _sender(login="acme-other-writer", sender_id=account + 1)
    github.permission_user_id = account + 1

    relabel = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))

    assert relabel.json()["status"] == "factory_admitted", relabel.text
    assert _github_items(number)[0]["enrollment_generation"] == 1
    predecessor, successor = _resume_requests(item_id)
    assert predecessor["id"] == failed_id
    assert successor["enrollment_generation"] == 1


def test_polled_close_expires_a_personal_transcript_retained_through_unlabel(
    owned_rows: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from test_factory_poll_intake import _admit_label, _factory, _run_once

    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
    with closing(_factory(monkeypatch, tmp_path, "poll")) as factory:
        client, github = next(factory)
        owner = _seed_default_tenant_owner()
        _link_github_account(client, owner, github.permission_user_id)
        number, request = _admit_label(client, github)
        item_id = request["work_item_id"]
        _fail_github_personal_request(item_id, request["id"])
        [item] = support.rows(
            "SELECT agent_id FROM curie.work_items WHERE id = :id", {"id": item_id}
        )
        agent = item["agent_id"]
        thread = _seed_transcript(agent, item_id)
        github.unlabel_issue(number)
        _run_once(github)
        [removed] = support.rows(
            "SELECT cancelled_at FROM curie.work_items WHERE id = :id", {"id": item_id}
        )
        assert removed["cancelled_at"] is not None
        assert _transcripts(agent, thread) == 1

        github.close_issue(number)
        _run_once(github)
        assert _transcripts(agent, thread) == 0


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


@pytest.fixture
def api(world: support.World, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


# --- route helpers -------------------------------------------------------------------


def _context_path(work_item_id: UUID) -> str:
    return f"{support.INTERNAL_PREFIX}/{work_item_id}/personal-context"


def _revalidate_path(work_item_id: UUID) -> str:
    return f"{support.INTERNAL_PREFIX}/{work_item_id}/personal-context/revalidate"


def _context(
    api: TestClient,
    work_item_id: UUID,
    *,
    tenant_id: UUID,
    agent_id: UUID,
    owner_principal_id: UUID,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return api.get(
        _context_path(work_item_id),
        params={
            "tenant_id": str(tenant_id),
            "agent_id": str(agent_id),
            "owner_principal_id": str(owner_principal_id),
        },
        headers=WORKER_HEADERS if headers is None else headers,
    )


def _read(api: TestClient, world: support.World, agent_id: UUID, work_item_id: UUID) -> Any:
    response = _context(
        api,
        work_item_id,
        tenant_id=world.tenant_id,
        agent_id=agent_id,
        owner_principal_id=world.owner_id,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _body(world: support.World, agent_id: UUID, context: Any, **overrides: Any) -> dict[str, Any]:
    """Revalidation of the context's latest request at the context's versions."""

    enrollment = context["enrollment"]
    body: dict[str, Any] = {
        "tenant_id": str(world.tenant_id),
        "agent_id": str(agent_id),
        "owner_principal_id": str(world.owner_id),
        "request_id": context["request"]["id"],
        "expected_work_item_version": context["work_item_version"],
        "expected_request_version": context["request"]["version"],
        "expected_enrollment_version": enrollment["version"] if enrollment else 1,
    }
    body.update(
        {key: str(value) if isinstance(value, UUID) else value for key, value in overrides.items()}
    )
    return body


def _revalidate(
    api: TestClient,
    work_item_id: UUID,
    body: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return api.post(
        _revalidate_path(work_item_id),
        json=body,
        headers=WORKER_HEADERS if headers is None else headers,
    )


def _assert_conflict(response: httpx.Response, code: str) -> None:
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": {"code": code}}


def _bindings(work_item_id: UUID) -> dict[str, Any]:
    """The durable binding facts every refusal must leave untouched."""

    [item] = support.rows(
        "SELECT version, enrollment_generation FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )
    requests = support.rows(
        "SELECT id, status, version, enrollment_generation FROM curie.execution_requests "
        "WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )
    return {"work_item": item, "requests": requests}


def _versions(request_id: UUID) -> tuple[int, int]:
    [row] = support.rows(
        "SELECT w.version AS work_item_version, r.version AS request_version "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE r.id = :id",
        {"id": request_id},
    )
    return int(row["work_item_version"]), int(row["request_version"])


# --- the token-free read ---------------------------------------------------------------


def test_personal_context_is_token_free_and_answers_like_an_absent_item_when_unscoped(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    personal = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53771)
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=53772, conversation=SERVICE_CONVERSATION
    )
    work_item_version, request_version = _versions(personal.request_id)

    found = _context(
        api,
        personal.work_item_id,
        tenant_id=world.tenant_id,
        agent_id=agent_id,
        owner_principal_id=world.owner_id,
    )
    assert found.status_code == 200, found.text
    body = found.json()
    assert body == {
        "work_item_id": str(personal.work_item_id),
        "agent_id": str(agent_id),
        "tenant_id": str(world.tenant_id),
        "model_auth_mode": "personal",
        "owner_principal_id": str(world.owner_id),
        "enrollment_generation": world.generation,
        "work_item_version": work_item_version,
        "request": {
            "id": str(personal.request_id),
            "sequence": 1,
            "status": "waiting",
            "version": request_version,
            "enrollment_generation": world.generation,
        },
        "enrollment": {"generation": world.generation, "active": True, "version": 1},
    }
    assert support.credential_material(body) == []

    missing = _context(
        api,
        uuid4(),
        tenant_id=world.tenant_id,
        agent_id=agent_id,
        owner_principal_id=world.owner_id,
    )
    assert missing.status_code == 404, missing.text
    assert missing.json() == NOT_FOUND
    for label, work_item_id, tenant_id, scoped_agent, owner in (
        ("wrong tenant", personal.work_item_id, world.other_tenant_id, agent_id, world.owner_id),
        (
            "other tenant's agent",
            personal.work_item_id,
            world.other_tenant_id,
            world.foreign_agent_id,
            world.owner_id,
        ),
        (
            "wrong agent",
            personal.work_item_id,
            world.tenant_id,
            world.foreign_agent_id,
            world.owner_id,
        ),
        (
            "wrong owner",
            personal.work_item_id,
            world.tenant_id,
            agent_id,
            world.other_principal_id,
        ),
        ("service item", service.work_item_id, world.tenant_id, agent_id, world.owner_id),
    ):
        refused = _context(
            api,
            work_item_id,
            tenant_id=tenant_id,
            agent_id=scoped_agent,
            owner_principal_id=owner,
        )
        assert refused.status_code == 404, (label, refused.text)
        assert refused.content == missing.content, label


def test_revalidation_answers_unscoped_and_service_items_like_an_absent_item(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    personal = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53773)
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=53774, conversation=SERVICE_CONVERSATION
    )
    support.advance_enrollment(world)
    context = _read(api, world, agent_id, personal.work_item_id)
    valid = _body(world, agent_id, context)
    before = _bindings(personal.work_item_id)
    service_before = _bindings(service.work_item_id)
    service_version, service_request_version = _versions(service.request_id)

    missing = _revalidate(api, uuid4(), valid)
    assert missing.status_code == 404, missing.text
    assert missing.json() == NOT_FOUND
    for label, work_item_id, body in (
        ("wrong tenant", personal.work_item_id, {**valid, "tenant_id": str(world.other_tenant_id)}),
        ("wrong agent", personal.work_item_id, {**valid, "agent_id": str(world.foreign_agent_id)}),
        (
            "service item",
            service.work_item_id,
            {
                **valid,
                "request_id": str(service.request_id),
                "expected_work_item_version": service_version,
                "expected_request_version": service_request_version,
            },
        ),
        ("absent request", personal.work_item_id, {**valid, "request_id": str(uuid4())}),
    ):
        refused = _revalidate(api, work_item_id, body)
        assert refused.status_code == 404, (label, refused.text)
        assert refused.content == missing.content, label
    assert _bindings(personal.work_item_id) == before
    assert _bindings(service.work_item_id) == service_before

    # Liveness: the same call, correctly scoped, binds.
    bound = _revalidate(api, personal.work_item_id, valid)
    assert bound.status_code == 200, bound.text
    assert bound.json()["enrollment_generation"] == world.generation + 1


def test_context_and_revalidation_refuse_without_the_worker_token(
    api: TestClient, world: support.World, auth_headers: dict[str, str]
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    personal = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53775)
    context = _read(api, world, agent_id, personal.work_item_id)
    body = _body(world, agent_id, context)
    before = _bindings(personal.work_item_id)

    for label, headers in (
        ("missing", {}),
        ("wrong", {"X-Curie-Worker-Token": "example-wrong-worker-token"}),
        ("platform key", dict(auth_headers)),
    ):
        read = _context(
            api,
            personal.work_item_id,
            tenant_id=world.tenant_id,
            agent_id=agent_id,
            owner_principal_id=world.owner_id,
            headers=headers,
        )
        assert read.status_code == 401, (label, read.text)
        write = _revalidate(api, personal.work_item_id, body, headers=headers)
        assert write.status_code == 401, (label, write.text)
    assert _bindings(personal.work_item_id) == before

    # Liveness: the worker token reaches both routes; nothing has advanced yet.
    assert _read(api, world, agent_id, personal.work_item_id) == context
    _assert_conflict(_revalidate(api, personal.work_item_id, body), "generation_current")
    assert _bindings(personal.work_item_id) == before


# --- refusals and the single rebind ---------------------------------------------------


def test_revalidation_refuses_stale_mismatched_inactive_and_live_then_binds_once(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53776)
    old = world.generation

    # Nothing has advanced: the bound generation is current.
    context = _read(api, world, agent_id, first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, context)), "generation_current"
    )

    # The old runtime is live: running, then with a revision queued behind it, then stopping.
    support.start_request(first.request_id)
    generation, _ = support.advance_enrollment(world)
    assert generation == old + 1
    running = _read(api, world, agent_id, first.work_item_id)
    assert running["request"]["status"] == "running"
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, running)), "runtime_live"
    )
    revision = support.facts(
        agent_id,
        issue_number=53776,
        requester=support.OTHER_REQUESTER,
        objective=REVISION_OBJECTIVE,
    )
    queued = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(queued, WorkItemOutcome), queued
    behind = _read(api, world, agent_id, first.work_item_id)
    assert (behind["request"]["id"], behind["request"]["status"]) == (
        str(revision.request_id),
        "queued",
    )
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, behind)), "runtime_live"
    )
    support.relabel_while_live(
        agent_id,
        issue_number=53776,
        live_request_id=first.request_id,
        objective=RELABEL_OBJECTIVE,
    )
    stopping = _read(api, world, agent_id, first.work_item_id)
    assert (stopping["request"]["id"], stopping["request"]["status"]) == (
        str(revision.request_id),
        "queued",
    )
    assert support.request_column(first.request_id, "status") == "cancellation_requested"
    stopping_bindings = _bindings(first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, stopping)), "runtime_live"
    )
    assert _bindings(first.work_item_id) == stopping_bindings

    # The worker records the old runtime's termination; from here only the inputs decide.
    assert support.record_termination(first.request_id).status == "cancelled"
    current = _read(api, world, agent_id, first.work_item_id)
    assert current["enrollment"] == {"generation": generation, "active": True, "version": 2}
    before = _bindings(first.work_item_id)
    assert before["work_item"]["enrollment_generation"] == old
    assert running["work_item_version"] < current["work_item_version"]

    for label, overrides, code in (
        (
            "stale work item version",
            # Read before the revision was queued, which moved the WorkItem on.
            {"expected_work_item_version": running["work_item_version"]},
            "stale_version",
        ),
        (
            "stale request version",
            {"expected_request_version": current["request"]["version"] + 1},
            "stale_version",
        ),
        ("stale enrollment version", {"expected_enrollment_version": 1}, "stale_version"),
        ("another owner", {"owner_principal_id": world.other_principal_id}, "owner_mismatch"),
    ):
        refused = _revalidate(api, first.work_item_id, _body(world, agent_id, current, **overrides))
        _assert_conflict(refused, code)
        assert _bindings(first.work_item_id) == before, label

    # An inactive, then a missing, enrollment never binds.
    inactive_version = support.set_enrollment_active(world, False)
    inactive = _read(api, world, agent_id, first.work_item_id)
    assert inactive["enrollment"] == {
        "generation": generation,
        "active": False,
        "version": inactive_version,
    }
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, inactive)),
        "enrollment_inactive",
    )
    assert _bindings(first.work_item_id) == before
    support.set_enrollment_active(world, True)
    support.delete_enrollment(world)
    absent = _read(api, world, agent_id, first.work_item_id)
    assert absent["enrollment"] is None
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, absent)), "enrollment_inactive"
    )
    assert _bindings(first.work_item_id) == before
    support.insert_enrollment(world, generation=generation)

    # Liveness: current versions, the owner, an active advanced enrollment.
    ready = _read(api, world, agent_id, first.work_item_id)
    assert ready["enrollment"] == {"generation": generation, "active": True, "version": 1}
    bound = _revalidate(api, first.work_item_id, _body(world, agent_id, ready))
    assert bound.status_code == 200, bound.text
    work_item_version = ready["work_item_version"] + 1
    request_version = ready["request"]["version"] + 1
    assert bound.json() == {
        "work_item_id": str(first.work_item_id),
        "request_id": str(revision.request_id),
        "previous_generation": old,
        "enrollment_generation": generation,
        "work_item_version": work_item_version,
        "request_version": request_version,
        "enrollment_version": 1,
    }
    terminal = next(row for row in before["requests"] if row["id"] == first.request_id)
    assert _bindings(first.work_item_id) == {
        "work_item": {"version": work_item_version, "enrollment_generation": generation},
        "requests": [
            # The terminal snapshot keeps the generation it ran under.
            terminal,
            {
                "id": revision.request_id,
                "status": "queued",
                "version": request_version,
                "enrollment_generation": generation,
            },
        ],
    }
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={
            first.request_id: world.personal(old),
            revision.request_id: world.personal(generation),
        },
    )
    rebound = _read(api, world, agent_id, first.work_item_id)
    assert (rebound["enrollment_generation"], rebound["request"]["enrollment_generation"]) == (
        generation,
        generation,
    )

    # Exactly once: the old expected versions are stale, fresh ones find nothing to do.
    after = _bindings(first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, ready)), "stale_version"
    )
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, rebound)), "generation_current"
    )
    assert _bindings(first.work_item_id) == after


def test_revalidation_refuses_a_normally_finished_run_without_a_recorded_termination(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53779)
    old = world.generation
    epoch = support.start_request(first.request_id)
    generation, _ = support.advance_enrollment(world)
    assert generation == old + 1

    # The worker reports the run finished; its sandbox cleanup afterwards is best effort.
    finished = api.post(
        f"{support.INTERNAL_PREFIX}/requests/{first.request_id}/finish",
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "deadline_halted",
            "retry_safety": "uncertain",
        },
        headers=WORKER_HEADERS,
    )
    assert finished.status_code == 200, finished.text
    assert (finished.json()["status"], finished.json()["termination_observation"]) == (
        "failed",
        None,
    )
    runtime_facts = (
        "SELECT terminal_at IS NOT NULL AS terminal, runtime_claim_name, "
        "runtime_heartbeat_expires_at > clock_timestamp() AS heartbeat_live, "
        "termination_observation FROM curie.execution_requests WHERE id = :id"
    )
    # Terminal, yet the claim is still recorded, the heartbeat unexpired, and no
    # termination observed: nothing shows the runtime stopped.
    assert support.rows(runtime_facts, {"id": first.request_id}) == [
        {
            "terminal": True,
            "runtime_claim_name": support.CLAIM_NAME,
            "heartbeat_live": True,
            "termination_observation": None,
        }
    ]
    finished_context = _read(api, world, agent_id, first.work_item_id)
    assert finished_context["request"]["status"] == "failed"
    assert finished_context["enrollment"] == {
        "generation": generation,
        "active": True,
        "version": 2,
    }
    before = _bindings(first.work_item_id)
    assert before["work_item"]["enrollment_generation"] == old
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, finished_context)),
        "runtime_live",
    )
    assert _bindings(first.work_item_id) == before

    # An expired heartbeat alone is not evidence that the runtime stopped either.
    support.rows(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = clock_timestamp() - interval '1 hour' "
        "WHERE id = :id",
        {"id": first.request_id},
    )
    assert support.rows(runtime_facts, {"id": first.request_id}) == [
        {
            "terminal": True,
            "runtime_claim_name": support.CLAIM_NAME,
            "heartbeat_live": False,
            "termination_observation": None,
        }
    ]
    expired_context = _read(api, world, agent_id, first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, expired_context)),
        "runtime_live",
    )
    assert _bindings(first.work_item_id) == before
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(old),
        requests={first.request_id: world.personal(old)},
    )


def test_revalidation_binds_once_the_worker_records_the_runtime_termination(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53780)
    old = world.generation
    support.start_request(first.request_id)
    generation, enrollment_version = support.advance_enrollment(world)

    # A relabel stops the live run; until its termination is recorded it is live.
    relabel = support.relabel_while_live(
        agent_id,
        issue_number=53780,
        live_request_id=first.request_id,
        objective=RELABEL_OBJECTIVE,
    )
    stopping = _read(api, world, agent_id, first.work_item_id)
    assert (stopping["request"]["id"], stopping["request"]["status"]) == (
        str(first.request_id),
        "cancellation_requested",
    )
    stopping_bindings = _bindings(first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, stopping)), "runtime_live"
    )
    assert _bindings(first.work_item_id) == stopping_bindings

    # The worker's termination chain records the runtime gone.
    assert support.record_termination(first.request_id).status == "cancelled"

    # The stored relabel waits, and a revision queues behind it, both under the old generation.
    admitted = support.admit_pending_readmit(first.work_item_id)
    assert admitted.request is not None
    assert (admitted.request.id, admitted.request.status) == (relabel.request_id, "waiting")
    revision = support.facts(
        agent_id,
        issue_number=53780,
        requester=support.OTHER_REQUESTER,
        objective=REVISION_OBJECTIVE,
    )
    queued = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(queued, WorkItemOutcome), queued
    assert queued.request is not None
    assert (queued.request.id, queued.request.status) == (revision.request_id, "queued")

    ready = _read(api, world, agent_id, first.work_item_id)
    assert (ready["request"]["id"], ready["request"]["status"]) == (
        str(revision.request_id),
        "queued",
    )
    assert ready["enrollment"] == {
        "generation": generation,
        "active": True,
        "version": enrollment_version,
    }
    before = _bindings(first.work_item_id)
    terminal, waiting, behind = before["requests"]
    assert (terminal["id"], terminal["status"], terminal["enrollment_generation"]) == (
        first.request_id,
        "cancelled",
        old,
    )
    assert (waiting["id"], waiting["status"], waiting["enrollment_generation"]) == (
        relabel.request_id,
        "waiting",
        old,
    )
    assert (behind["id"], behind["status"], behind["enrollment_generation"]) == (
        revision.request_id,
        "queued",
        old,
    )

    bound = _revalidate(api, first.work_item_id, _body(world, agent_id, ready))
    assert bound.status_code == 200, bound.text
    work_item_version = ready["work_item_version"] + 1
    assert bound.json() == {
        "work_item_id": str(first.work_item_id),
        "request_id": str(revision.request_id),
        "previous_generation": old,
        "enrollment_generation": generation,
        "work_item_version": work_item_version,
        "request_version": behind["version"] + 1,
        "enrollment_version": enrollment_version,
    }
    assert _bindings(first.work_item_id) == {
        "work_item": {"version": work_item_version, "enrollment_generation": generation},
        "requests": [
            # The terminated request keeps the generation it ran under.
            terminal,
            {**waiting, "version": waiting["version"] + 1, "enrollment_generation": generation},
            {**behind, "version": behind["version"] + 1, "enrollment_generation": generation},
        ],
    }
    assert (
        support.request_column(first.request_id, "termination_observation")
        == support.TERMINATION_OBSERVATION
    )
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={
            first.request_id: world.personal(old),
            relabel.request_id: world.personal(generation),
            revision.request_id: world.personal(generation),
        },
    )


def test_revalidation_waits_for_a_force_settled_teardown_to_be_confirmed(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53777)
    old = world.generation
    epoch = support.start_request(first.request_id)
    held = support.with_session(
        lambda session: workitem_dispatch.hold_for_approval(
            session, first.request_id, runtime_epoch=epoch
        )
    )
    assert isinstance(held, HeartbeatResult), held

    # A relabel stops the run and releases its parked lease; the relabel waits.
    relabel = support.facts(
        agent_id,
        issue_number=53777,
        requester=support.OTHER_REQUESTER,
        objective=RELABEL_OBJECTIVE,
    )
    stopped = support.with_session(lambda session: workitem_dispatch.readmit(session, relabel))
    assert isinstance(stopped, WorkItemOutcome), stopped
    assert stopped.request is not None
    assert (stopped.request.id, stopped.request.status) == (
        first.request_id,
        "cancellation_requested",
    )
    published = support.with_session(
        lambda session: workitem_dispatch.claim_terminate_publishes(
            session, retry_seconds=60, limit=50
        )
    )
    assert [wake.request_id for wake in published] == [first.request_id]
    assert all(isinstance(wake, TerminatePublish) for wake in published)

    # No teardown receipt arrives: the control plane settles the cancellation.
    _, request_version = _versions(first.request_id)
    settled = support.with_session(
        lambda session: lifecycle.settle_overdue_cancellation(
            session,
            work_item_id=first.work_item_id,
            request_id=first.request_id,
            expected_request_version=request_version,
            settle_seconds=0,
        )
    )
    assert isinstance(settled, WorkItemOutcome), settled
    assert settled.request is not None and settled.request.status == "cancelled"
    assert support.request_column(first.request_id, "teardown_unconfirmed_at IS NOT NULL")

    generation, _ = support.advance_enrollment(world)
    unconfirmed = _read(api, world, agent_id, first.work_item_id)
    assert unconfirmed["request"]["status"] == "cancelled"
    before = _bindings(first.work_item_id)
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, unconfirmed)), "runtime_live"
    )
    assert _bindings(first.work_item_id) == before

    # The real termination-recording path confirms the teardown and clears the flag.
    work_item_version, request_version = _versions(first.request_id)
    confirmed = support.with_session(
        lambda session: lifecycle.record_runtime_termination(
            session,
            work_item_id=first.work_item_id,
            request_id=first.request_id,
            termination_observation=support.TERMINATION_OBSERVATION,
            expected_work_item_version=work_item_version,
            expected_request_version=request_version,
        )
    )
    assert isinstance(confirmed, WorkItemOutcome), confirmed
    assert support.request_column(first.request_id, "teardown_unconfirmed_at") is None

    ready = _read(api, world, agent_id, first.work_item_id)
    bound = _revalidate(api, first.work_item_id, _body(world, agent_id, ready))
    assert bound.status_code == 200, bound.text
    assert (bound.json()["previous_generation"], bound.json()["enrollment_generation"]) == (
        old,
        generation,
    )

    # The relabel admitted afterwards snapshots the newly bound generation.
    admitted = support.admit_pending_readmit(first.work_item_id)
    assert admitted.request is not None and admitted.request.id == relabel.request_id
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={
            first.request_id: world.personal(old),
            relabel.request_id: world.personal(generation),
        },
    )


# --- concurrent writers -----------------------------------------------------------------


async def _http_race(app: Any, path: str, body: dict[str, Any]) -> list[httpx.Response]:
    """Two writers through the app on its own loop, in flight together."""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as client:
        return list(
            await asyncio.gather(
                client.post(path, json=body, headers=WORKER_HEADERS),
                client.post(path, json=body, headers=WORKER_HEADERS),
            )
        )


def test_concurrent_revalidations_with_identical_versions_bind_exactly_once(
    api: TestClient, world: support.World
) -> None:
    personal_execution = importlib.import_module("curie_api.personal_execution")
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53778)
    generation, _ = support.advance_enrollment(world)
    context = _read(api, world, agent_id, first.work_item_id)
    expected = {
        "work_item_id": first.work_item_id,
        "tenant_id": world.tenant_id,
        "agent_id": agent_id,
        "owner_principal_id": world.owner_id,
        "request_id": first.request_id,
        "expected_work_item_version": context["work_item_version"],
        "expected_request_version": context["request"]["version"],
        "expected_enrollment_version": context["enrollment"]["version"],
    }
    url = get_settings().database_url

    async def race() -> list[Any]:
        barrier = asyncio.Barrier(2)
        engines = [create_async_engine(url), create_async_engine(url)]

        async def contender(index: int) -> Any:
            async with AsyncSession(engines[index]) as session:
                # Both sessions hold an open connection before either writes.
                await session.execute(text("SELECT 1"))
                await session.commit()
                await barrier.wait()
                return await personal_execution.revalidate(session, **expected)

        try:
            return list(
                await asyncio.wait_for(asyncio.gather(contender(0), contender(1)), timeout=20)
            )
        finally:
            for engine in engines:
                await engine.dispose()

    results = asyncio.run(race())
    codes = sorted(str(getattr(result, "code", None)) for result in results)
    assert codes == ["None", "stale_version"], results
    [winner] = [result for result in results if getattr(result, "code", None) is None]
    assert winner.enrollment_generation == generation
    assert _bindings(first.work_item_id) == {
        "work_item": {
            "version": context["work_item_version"] + 1,
            "enrollment_generation": generation,
        },
        "requests": [
            {
                "id": first.request_id,
                "status": "waiting",
                "version": context["request"]["version"] + 1,
                "enrollment_generation": generation,
            }
        ],
    }

    # The losing writer's versions map to the HTTP conflict.
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, context)), "stale_version"
    )

    # Two HTTP writers in flight together: one 200, one 409, one rebind.
    next_generation, _ = support.advance_enrollment(world)
    fresh = _read(api, world, agent_id, first.work_item_id)
    portal = api.portal
    assert portal is not None
    responses = portal.call(
        _http_race, api.app, _revalidate_path(first.work_item_id), _body(world, agent_id, fresh)
    )
    assert sorted(response.status_code for response in responses) == [200, 409], [
        response.text for response in responses
    ]
    [loser] = [response for response in responses if response.status_code == 409]
    assert loser.json() == {"detail": {"code": "stale_version"}}
    [won] = [response for response in responses if response.status_code == 200]
    assert won.json()["enrollment_generation"] == next_generation
    assert _bindings(first.work_item_id)["work_item"] == {
        "version": fresh["work_item_version"] + 1,
        "enrollment_generation": next_generation,
    }


# --- #5378: a start refused for a moved enrollment is rebindable -------------------------


def _acquire_as_worker(request_id: UUID) -> tuple[int, Any]:
    async def body(session: AsyncSession) -> tuple[int, Any]:
        generation = await session.scalar(
            text("SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id"),
            {"id": request_id},
        )
        assert isinstance(generation, int), request_id
        async with no_external_calls() as http_client:
            acquired = await workitem_dispatch.acquire(
                session,
                request_id,
                owner=support.RUNTIME_OWNER,
                generation=generation,
                wake=await factory_wake(session, request_id),
                context=None,
                client=http_client,
            )
        assert isinstance(acquired, AcquireGrant), acquired
        return generation, acquired.context

    return support.with_session(body)


def test_a_start_refused_for_a_moved_enrollment_rebinds_after_its_defer_and_starts_fresh(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53792)
    generation, context = _acquire_as_worker(first.request_id)
    # The owner replaces the subscription after the worker claimed a runtime.
    new_generation, _ = support.advance_enrollment(world)

    refused = support.with_session(
        lambda session: workitem_dispatch.start(
            session,
            first.request_id,
            owner=support.RUNTIME_OWNER,
            generation=generation,
            claim_name=support.CLAIM_NAME,
            sandbox_name=support.SANDBOX_NAME,
            context=context,
            delivered_limits=lifecycle.NO_DELIVERED_LIMITS,
        )
    )
    assert isinstance(refused, DispatchConflict), refused
    assert refused.code == "enrollment_generation_mismatch"

    # While the worker still holds the acquire, nothing rebinds the request.
    held = _read(api, world, agent_id, first.work_item_id)
    assert (held["request"]["id"], held["request"]["status"]) == (str(first.request_id), "waiting")
    _assert_conflict(
        _revalidate(api, first.work_item_id, _body(world, agent_id, held)), "runtime_live"
    )

    # The worker releases its claim and defers: still waiting, not terminal.
    deferred = support.with_session(
        lambda session: workitem_dispatch.defer(
            session,
            first.request_id,
            owner=support.RUNTIME_OWNER,
            generation=generation,
            reason="not_started:enrollment_generation_mismatch",
            capacity=False,
            start_failure_detail=None,
        )
    )
    assert isinstance(deferred, DeferResult), deferred
    assert deferred.terminal_cause is None
    assert support.request_column(first.request_id, "status") == "waiting"

    current = _read(api, world, agent_id, first.work_item_id)
    bound = _revalidate(api, first.work_item_id, _body(world, agent_id, current))
    assert bound.status_code == 200, bound.text
    assert bound.json()["enrollment_generation"] == new_generation
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(new_generation),
        requests={first.request_id: world.personal(new_generation)},
    )

    # The next dispatch starts a fresh runtime at the rebound generation, once
    # the defer's backoff (a NOT NULL ``dispatch_not_before``) has passed.
    support.rows(
        "UPDATE curie.execution_requests SET dispatch_not_before = clock_timestamp() "
        "WHERE id = :id",
        {"id": first.request_id},
    )
    assert support.start_request(first.request_id) >= 1
    assert support.request_column(first.request_id, "status") == "running"
