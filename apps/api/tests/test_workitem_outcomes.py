"""Operator read surface for factory work item outcomes (#2577).

Every state is seeded through the real admission/dispatch/publication service
functions and read back through the operator HTTP routes, so the assertions
are about what an operator actually sees: the ``state`` string, a cause that
names the reason, the issue/PR links, and the absence of any runtime-owner or
credential material in the JSON.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator, Mapping
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import channel_protocol
import httpx
import pytest
from curie_api import approval_principal, crud, factory_ci, workitems
from curie_api.config import get_settings
from curie_api.github_app import (
    _RESOLVERS,
    GitHubAppError,
    GitHubInstallationRefused,
)
from curie_api.main import create_app
from curie_api.schemas import ApprovalRequest
from curie_api.workitem_dispatch import (
    acquire,
    admit,
    cancel,
    claim_termination,
    defer,
    finish,
    record_termination,
    start,
)
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
WIRE_CONVERSATION = "1700000000.000100"
WORKER_TOKEN = "work-item-outcomes-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
OWNER = "curie-workers-SENTINELOWNER"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
CLAIM_NAME = "curie-thread-SENTINELCLAIM"
SANDBOX_NAME = "sbx-curie-thread-SENTINELSANDBOX"
TERMINATION_OBSERVATION = (
    "claims=curie-thread-SENTINELCLAIM sandboxes=sbx-curie-thread-SENTINELSANDBOX "
    "absent_at=2026-09-18T12:00:10+00:00 observer=curie-workers-a"
)
SECRET_SENTINEL = "ghs_SENTINELTOKEN"
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
HEAD_SHA = "1123456789abcdef0123456789abcdef01234567"
PR_NUMBER = 123
PR_URL = f"https://github.com/{REPO}/pull/{PR_NUMBER}"
OUTCOME_STATES = {
    "waiting",
    "running",
    "cancellation_requested",
    "cancelled",
    "expired",
    "failed",
    "awaiting_approval",
    "publishing",
    "published",
    "completed_unpublished",
}
FORBIDDEN_KEYS = {
    "runtime_owner",
    "runtime_epoch",
    "runtime_claim_name",
    "runtime_sandbox_name",
    "runtime_heartbeat_expires_at",
    "acquire_owner",
    "dispatch_owner",
    "dispatch_generation",
    "published_generation",
    "acquired_generation",
    "reply_kind",
    "reply_address",
    "reply_channel",
    "reply_conversation_id",
    "reply_placeholder",
    "reply_endpoint",
    "reply_adapter",
    "conversation_id",
    "patch_bytes",
    "patch_b64",
    "body",
    "error",
    "result_delivery_error",
    "dedupe_key",
    "traceparent",
    "summary",
    "token",
    "github_installation_id",
    "github_repository_id",
    "version",
}
CORRECTNESS_VERDICT_KEYS = {"verdict", "passed", "correct"}


# --- plumbing ---------------------------------------------------------------


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _execute(query: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(query), params or {})
        finally:
            await engine.dispose()

    asyncio.run(run())


def _keys(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        for key, child in node.items():
            yield str(key)
            yield from _keys(child)
    elif isinstance(node, list):
        for child in node:
            yield from _keys(child)


@pytest.fixture
def stack(
    clean_db: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    runs_stream = f"test:curie:outcome-runs:{uuid.uuid4().hex}"
    monkeypatch.setenv("RUNS_STREAM", runs_stream)
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    # A PAT-only install: no GitHub App, so CI must say app_not_configured.
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_outcomes_operator")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    _RESOLVERS.clear()
    with TestClient(create_app()) as client:
        yield client
    valkey = connect_or_skip(decode_responses=True)
    valkey.delete(runs_stream, f"{runs_stream}:dead")
    valkey.close()
    _RESOLVERS.clear()
    get_settings.cache_clear()


def _agent(
    client: TestClient, auth_headers: dict[str, str], *, address: str = ADDRESS
) -> dict[str, Any]:
    """Create agent + version + deployment through the operator API."""

    suffix = uuid.uuid4().hex[:8]
    agent = client.post(
        "/agents",
        json={
            "name": f"factory-{suffix}",
            "channel": {"kind": "slack", "address": address},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = agent.json()["id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=auth_headers,
    )
    assert deployment.status_code == 201, deployment.text
    return {
        "agent_id": agent_id,
        "deployment_id": deployment.json()["id"],
        "address": address,
    }


def _facts(agent_id: str, **overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "agent_id": uuid.UUID(agent_id),
        "kind": "slack",
        "address": ADDRESS,
        "reply_conversation_id": WIRE_CONVERSATION,
        "repo_full_name": REPO,
        "github_repository_id": 101,
        "github_issue_number": 2577,
        "github_installation_id": 202,
        "objective": OBJECTIVE,
        "requester": REQUESTER,
        "request_id": uuid.uuid4(),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _admit(facts: SimpleNamespace) -> SimpleNamespace:
    async def body(session: AsyncSession) -> SimpleNamespace:
        admitted = await admit(session, facts)
        assert admitted.request is not None, admitted
        return SimpleNamespace(
            work_item_id=admitted.work_item.id,
            work_item_version=admitted.work_item.version,
            request_id=facts.request_id,
        )

    return with_session(body)


def _start(request_id: uuid.UUID, *, generation: int = 1) -> int:
    async def body(session: AsyncSession) -> int:
        await acquire(session, request_id, owner=OWNER, generation=generation)
        started = await start(
            session,
            request_id,
            owner=OWNER,
            generation=generation,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        return int(started.runtime_epoch)

    return with_session(body)


def _finish(request_id: uuid.UUID, epoch: int, outcome: str, cause: str) -> None:
    async def body(session: AsyncSession) -> None:
        result = await finish(
            session,
            request_id,
            runtime_epoch=epoch,
            outcome=outcome,  # type: ignore[arg-type]
            cause=cause,
            detail=None,
        )
        assert getattr(result, "code", None) is None, result

    with_session(body)


def _versions(request_id: uuid.UUID) -> tuple[int, int]:
    async def body(session: AsyncSession) -> tuple[int, int]:
        row = (
            await session.execute(
                text(
                    "SELECT w.version AS wv, r.version AS rv "
                    "FROM curie.execution_requests r JOIN curie.work_items w "
                    "ON w.id = r.work_item_id WHERE r.id = :id"
                ),
                {"id": request_id},
            )
        ).mappings().one()
        return int(row["wv"]), int(row["rv"])

    return with_session(body)


def _cancel(work_item_id: uuid.UUID, request_id: uuid.UUID) -> None:
    work_item_version, _ = _versions(request_id)

    async def body(session: AsyncSession) -> None:
        await cancel(
            session, work_item_id=work_item_id, expected_version=work_item_version
        )

    with_session(body)


def _completed(client: TestClient, agent: dict[str, Any], **overrides: Any) -> SimpleNamespace:
    """Admit and start. Completion waits until a pull request is open."""

    del client
    facts = _facts(agent["agent_id"], **overrides)
    seeded = _admit(facts)
    seeded.runtime_epoch = _start(facts.request_id)
    return seeded


def _complete(seeded: SimpleNamespace) -> None:
    _finish(seeded.request_id, seeded.runtime_epoch, "completed", "completed")


def _publication_payload(deployment_id: str, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "deployment_id": deployment_id,
        "conversation_id": WIRE_CONVERSATION,
        "repo_full_name": REPO,
        "author": REQUESTER,
        "summary": f"Publish the repository changes {SECRET_SENTINEL}",
        "reply_kind": "slack",
        "reply_channel": ADDRESS,
        "reply_placeholder": "1700000000.000001",
        "dedupe_key": f"publish-{uuid.uuid4().hex}",
        "base_sha": BASE_SHA,
        "patch_b64": base64.b64encode(
            f"diff --git a/README.md b/README.md\n+{SECRET_SENTINEL}\n".encode()
        ).decode(),
        "changed_paths": ["README.md"],
        "expires_in_seconds": 600,
    }
    payload.update(overrides)
    return payload


def _publish(client: TestClient, deployment_id: str, **overrides: Any) -> dict[str, Any]:
    payload = _publication_payload(deployment_id, **overrides)
    selected = client.post(
        f"/v1/internal/workspaces/{deployment_id}/selection",
        json={
            "conversation_id": channel_protocol.scoped_conversation_id(
                "slack", ADDRESS, str(payload["conversation_id"])
            ),
            "author": payload["author"],
            "repo_full_name": REPO,
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text
    created = client.post(
        "/v1/internal/publications", json=payload, headers=WORKER_HEADERS
    )
    assert created.status_code in (200, 201), created.text
    return dict(created.json())


def _resolve(
    client: TestClient,
    auth_headers: dict[str, str],
    approval_id: str,
    decision: str = "approved",
) -> None:
    token = approval_principal.mint(
        get_settings().approval_chat_attester_secret,
        subject=REQUESTER,
        kind="chat",
        actor_channel=ADDRESS,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(datetime.now(UTC).timestamp()) + 60,
    )
    resolved = client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision},
        headers={**auth_headers, "X-Curie-Approval-Principal": token},
    )
    assert resolved.status_code == 200, resolved.text


def _open_pr(client: TestClient, publication_id: str) -> None:
    lease_owner = "outcomes-test-worker"

    async def lease() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(
                    text(
                        "UPDATE curie.publications SET lease_owner = :owner, "
                        "lease_expires_at = now() + interval '1 minute', "
                        "version = version + 1 WHERE id = :id RETURNING version"
                    ),
                    {"id": uuid.UUID(publication_id), "owner": lease_owner},
                )
                return int(result.scalar_one())
        finally:
            await engine.dispose()

    publication_version = asyncio.run(lease())
    advanced = client.patch(
        f"/v1/internal/publications/{publication_id}/lineage",
        json={
            "expected_version": 1,
            "expected_head_sha": None,
            "expected_publication_version": publication_version,
            "lease_owner": lease_owner,
            "state": "open",
            "pr_number": PR_NUMBER,
            "pr_url": PR_URL,
            "head_sha": HEAD_SHA,
        },
        headers=WORKER_HEADERS,
    )
    assert advanced.status_code == 200, advanced.text


def _detail(
    client: TestClient,
    auth_headers: dict[str, str],
    work_item_id: uuid.UUID | str,
    **params: str,
) -> dict[str, Any]:
    response = client.get(
        f"/work-items/{work_item_id}", params=params, headers=auth_headers
    )
    assert response.status_code == 200, response.text
    body = dict(response.json())
    assert body["state"] in OUTCOME_STATES
    return body


def _assert_common(body: Mapping[str, Any]) -> None:
    assert body["correctness"] == {"asserted": False, "owner": "bundle"}
    assert not CORRECTNESS_VERDICT_KEYS & set(_keys(body))
    assert isinstance(body["actionable_cause"], str) and body["actionable_cause"]
    assert body["issue_url"] == f"https://github.com/{REPO}/issues/2577"


# --- one test per state -----------------------------------------------------


def test_fresh_admission_is_waiting_with_issue_and_no_pr(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _admit(_facts(agent["agent_id"]))

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "waiting"
    _assert_common(body)
    assert body["id"] == str(seeded.work_item_id)
    assert body["agent_id"] == agent["agent_id"]
    assert body["repo_full_name"] == REPO
    assert body["github_issue_number"] == 2577
    assert body["pr"] is None
    assert body["publication"] is None
    assert body["objective"] == OBJECTIVE
    assert body["objective_truncated"] is False
    assert body["requester"] == REQUESTER
    assert [r["status"] for r in body["requests"]] == ["waiting"]
    assert body["ci"]["state"] == "not_applicable"
    assert body["ci"]["reason"] == "no_pull_request"


def test_capacity_deferral_is_waiting_and_names_capacity(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)

    async def body(session: AsyncSession) -> None:
        await acquire(session, facts.request_id, owner=OWNER, generation=1)
        await defer(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            reason="capacity",
            capacity=True,
        )

    with_session(body)

    detail = _detail(stack, auth_headers, seeded.work_item_id)

    assert detail["state"] == "waiting"
    assert "capacity" in detail["actionable_cause"].lower()
    assert detail["requests"][-1]["capacity_deferrals"] == 1
    assert detail["requests"][-1]["last_deferral_reason"] == "capacity"


def test_waiting_past_deadline_is_not_reported_expired_until_sql_says_so(
    stack: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = _agent(stack, auth_headers)
    monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "1")
    get_settings.cache_clear()
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    time.sleep(1.5)

    elapsed = _detail(stack, auth_headers, seeded.work_item_id)
    assert elapsed["state"] == "waiting"
    assert "deadline" in elapsed["actionable_cause"].lower()

    work_item_version, request_version = _versions(facts.request_id)

    async def expire(session: AsyncSession) -> None:
        result = await workitems.expire_waiting(
            session,
            work_item_id=seeded.work_item_id,
            request_id=facts.request_id,
            expected_work_item_version=work_item_version,
            expected_request_version=request_version,
        )
        assert isinstance(result, workitems.WorkItemOutcome), result

    with_session(expire)

    expired = _detail(stack, auth_headers, seeded.work_item_id)
    assert expired["state"] == "expired"
    assert "capacity_wait_expired" in expired["actionable_cause"]
    assert expired["requests"][-1]["terminal_cause"] == "capacity_wait_expired"
    _assert_common(expired)


def test_started_request_is_running(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "running"
    _assert_common(body)
    request = body["requests"][-1]
    assert request["started_at"] is not None
    assert request["execution_deadline"] is not None


def test_cancel_while_running_is_cancellation_requested_naming_issue_cancelled(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)
    _cancel(seeded.work_item_id, facts.request_id)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "cancellation_requested"
    assert "issue_cancelled" in body["actionable_cause"]
    assert body["cancelled_at"] is not None
    _assert_common(body)


def test_cancel_while_waiting_is_cancelled(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _cancel(seeded.work_item_id, facts.request_id)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "cancelled"
    assert body["cancelled_at"] is not None
    _assert_common(body)


def test_cancelled_after_termination_is_recorded(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)
    _cancel(seeded.work_item_id, facts.request_id)

    async def terminate(session: AsyncSession) -> None:
        claim = await claim_termination(session, facts.request_id, owner=OWNER)
        await record_termination(
            session,
            facts.request_id,
            runtime_epoch=claim.runtime_epoch,
            observation=TERMINATION_OBSERVATION,
        )

    with_session(terminate)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "cancelled"
    assert body["requests"][-1]["termination_observation"]
    _assert_common(body)


def test_failed_names_terminal_cause_and_delivery_budget(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    epoch = _start(facts.request_id)
    _finish(facts.request_id, epoch, "failed", "deadline_halted")

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "failed"
    assert "deadline_halted" in body["actionable_cause"]
    assert "worker.deliveryBudgetSeconds" in body["actionable_cause"]
    assert body["requests"][-1]["terminal_cause"] == "deadline_halted"
    _assert_common(body)


def test_completion_without_a_pull_request_stays_running(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)

    async def refuse(session: AsyncSession) -> None:
        result = await finish(
            session,
            seeded.request_id,
            runtime_epoch=seeded.runtime_epoch,
            outcome="completed",
            cause="completed",
            detail=None,
        )
        assert getattr(result, "code", None) is not None, result

    with_session(refuse)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "running"
    assert body["pr"] is None
    assert body["publication"] is None
    assert body["ci"]["state"] == "not_applicable"
    _assert_common(body)


def test_pending_publication_approval_is_awaiting_approval(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    _publish(stack, agent["deployment_id"])

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "awaiting_approval"
    assert "approval" in body["actionable_cause"].lower()
    # The API serves local, cluster and console alike: never name one tier.
    assert "curie <local|cluster> approvals" in body["actionable_cause"]
    assert "curie cluster approvals" not in body["actionable_cause"]
    assert body["publication"]["approval_status"] == "pending"
    assert body["publication"]["revision_number"] == 1
    _assert_common(body)


def test_pending_tool_approval_on_the_reply_tuple_is_awaiting_approval(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)

    async def pend(session: AsyncSession) -> None:
        await crud.create_approval(
            session,
            ApprovalRequest(
                agent_id=uuid.UUID(agent["agent_id"]),
                conversation_id=WIRE_CONVERSATION,
                author=REQUESTER,
                summary=f"Run a gated tool {SECRET_SENTINEL}",
                reply_kind="slack",
                reply_channel=ADDRESS,
                reply_placeholder="1700000000.000002",
                dedupe_key=f"tool-{uuid.uuid4().hex}",
                expires_in_seconds=600,
            ),
            traceparent="00-7123456789abcdef0123456789abcdef-7123456789abcdef-01",
        )

    with_session(pend)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "awaiting_approval"
    assert body["publication"] is None
    assert "curie cluster approvals" not in body["actionable_cause"]
    assert "curie <local|cluster> approvals" in body["actionable_cause"]
    assert SECRET_SENTINEL not in json.dumps(body)


def test_approved_publication_in_flight_is_publishing(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "publishing"
    assert body["publication"]["approval_status"] == "approved"
    assert body["publication"]["status"] in {"approved", "launching", "running"}
    assert body["pr"] is None
    _assert_common(body)


def test_denied_publication_is_completed_unpublished(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"], decision="rejected")

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "completed_unpublished"
    assert body["publication"]["status"] == "denied"
    assert "denied" in body["actionable_cause"]


def test_opened_pr_is_published_found_by_conversation_and_ci_is_unavailable(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    # The WorkItem's publication_lineage_id stays NULL (nothing links it in
    # production); the lineage must be found by agent + conversation + repo.
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "published"
    assert body["pr"] == {"number": PR_NUMBER, "url": PR_URL, "status": "open"}
    assert body["publication"]["status"] == "succeeded"
    # PAT-only install: a truthful "cannot observe", never "none" or "passing".
    assert body["ci"]["state"] == "unavailable"
    assert body["ci"]["reason"] == "app_not_configured"
    _assert_common(body)


def test_readmitted_item_running_again_reports_running_and_keeps_the_pr(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)
    second = _facts(agent["agent_id"])
    _admit(second)
    _start(second.request_id)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "running"
    assert body["pr"]["number"] == PR_NUMBER
    assert [r["sequence"] for r in body["requests"]] == sorted(
        r["sequence"] for r in body["requests"]
    )
    assert [r["status"] for r in body["requests"]] == ["completed", "running"]


def test_sticky_cancel_with_retained_pr_reports_cancelled_and_pr(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)
    work_item_version, _ = _versions(seeded.request_id)

    async def body(session: AsyncSession) -> None:
        await cancel(
            session,
            work_item_id=seeded.work_item_id,
            expected_version=work_item_version,
        )

    with_session(body)

    detail = _detail(stack, auth_headers, seeded.work_item_id)

    assert detail["state"] == "cancelled"
    assert detail["pr"]["number"] == PR_NUMBER


def test_linked_lineage_id_is_used_when_set(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    epoch = _start(facts.request_id)
    publication = _publish(
        stack,
        agent["deployment_id"],
        work_item_request_id=str(facts.request_id),
        work_item_runtime_epoch=epoch,
    )
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    work_item_version, request_version = _versions(facts.request_id)

    async def link(session: AsyncSession) -> None:
        result = await workitems.link_publication_lineage(
            session,
            work_item_id=seeded.work_item_id,
            request_id=facts.request_id,
            publication_lineage_id=uuid.UUID(publication["lineage_id"]),
            expected_work_item_version=work_item_version,
            expected_request_version=request_version,
        )
        assert isinstance(result, workitems.WorkItemOutcome), result

    with_session(link)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "running"
    assert body["pr"]["number"] == PR_NUMBER


# --- secrets and allowlist (AC6) -------------------------------------------


def test_no_runtime_owner_or_secret_material_reaches_list_or_detail(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    running = _facts(agent["agent_id"], github_issue_number=1)
    running_item = _admit(running)
    _start(running.request_id)
    published = _completed(
        stack, agent, github_issue_number=2, reply_conversation_id="1700000000.000200"
    )
    publication = _publish(
        stack, agent["deployment_id"], conversation_id="1700000000.000200"
    )
    _execute(
        "UPDATE curie.publications SET error = :e WHERE id = :id",
        {"e": f"push refused {SECRET_SENTINEL}", "id": uuid.UUID(publication["id"])},
    )

    listed = stack.get("/work-items", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    payloads = [listed.json()]
    for item_id in (running_item.work_item_id, published.work_item_id):
        detail = stack.get(f"/work-items/{item_id}", headers=auth_headers)
        assert detail.status_code == 200, detail.text
        payloads.append(detail.json())

    for payload in payloads:
        raw = json.dumps(payload)
        for sentinel in (
            SECRET_SENTINEL,
            OWNER,
            CLAIM_NAME,
            SANDBOX_NAME,
            WORKER_TOKEN,
            get_settings().api_key,
            "ghp_outcomes_operator",
        ):
            assert sentinel not in raw, (sentinel, raw)
        leaked = FORBIDDEN_KEYS & set(_keys(payload))
        assert not leaked, leaked


def test_objective_is_truncated_at_512_characters(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _admit(_facts(agent["agent_id"], objective="x" * 600))

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["objective"] == "x" * 512
    assert body["objective_truncated"] is True


# --- scope and empty installs (AC5) ----------------------------------------


def test_empty_install_lists_nothing_truthfully(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    response = stack.get("/work-items", headers=auth_headers)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["items"] == []
    assert body["truncated"] is False
    assert response.headers["cache-control"] == "no-store"


def test_list_items_carry_state_and_null_ci_and_filter_by_agent(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    mine = _agent(stack, auth_headers)
    theirs = _agent(stack, auth_headers, address="C0EXAMPLE2")
    mine_item = _admit(_facts(mine["agent_id"], address=mine["address"]))
    _admit(
        _facts(
            theirs["agent_id"], address=theirs["address"], github_issue_number=2578
        )
    )

    everything = stack.get("/work-items", headers=auth_headers).json()
    assert len(everything["items"]) == 2
    assert all(item["ci"] is None for item in everything["items"])
    assert all(item["state"] == "waiting" for item in everything["items"])

    filtered = stack.get(
        "/work-items", params={"agent_id": mine["agent_id"]}, headers=auth_headers
    )
    assert filtered.status_code == 200, filtered.text
    assert [item["id"] for item in filtered.json()["items"]] == [
        str(mine_item.work_item_id)
    ]


def test_list_truncation_flag(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    for issue in (1, 2, 3):
        _admit(_facts(agent["agent_id"], github_issue_number=issue))

    body = stack.get("/work-items", params={"limit": 2}, headers=auth_headers).json()

    assert len(body["items"]) == 2
    assert body["limit"] == 2
    assert body["truncated"] is True


@pytest.mark.parametrize("limit", ["0", "201"])
def test_list_limit_bounds_are_422(
    stack: TestClient, auth_headers: dict[str, str], limit: str
) -> None:
    response = stack.get("/work-items", params={"limit": limit}, headers=auth_headers)
    assert response.status_code == 422


def test_unknown_agent_filter_is_404(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    response = stack.get(
        "/work-items", params={"agent_id": str(uuid.uuid4())}, headers=auth_headers
    )
    assert response.status_code == 404
    assert "not_found" in response.text


def test_wrong_agent_detail_is_indistinguishable_from_unknown_id(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    mine = _agent(stack, auth_headers)
    other = _agent(stack, auth_headers, address="C0EXAMPLE2")
    seeded = _admit(_facts(mine["agent_id"], address=mine["address"]))

    unknown = stack.get(f"/work-items/{uuid.uuid4()}", headers=auth_headers)
    mismatched = stack.get(
        f"/work-items/{seeded.work_item_id}",
        params={"agent_id": other["agent_id"]},
        headers=auth_headers,
    )
    matched = stack.get(
        f"/work-items/{seeded.work_item_id}",
        params={"agent_id": mine["agent_id"]},
        headers=auth_headers,
    )

    assert unknown.status_code == 404
    assert mismatched.status_code == 404
    assert mismatched.content == unknown.content
    assert matched.status_code == 200
    assert matched.headers["cache-control"] == "no-store"


def test_routes_require_the_api_key(stack: TestClient) -> None:
    assert stack.get("/work-items").status_code == 401
    assert stack.get(f"/work-items/{uuid.uuid4()}").status_code == 401


# --- live CI observer (unit) -------------------------------------------------


def _ci_inputs(*, pr: bool = True, head_sha: str | None = HEAD_SHA) -> tuple[Any, Any]:
    lineage = SimpleNamespace(
        repo_full_name=REPO,
        pr_number=PR_NUMBER if pr else None,
        pr_url=PR_URL if pr else None,
        head_sha=head_sha,
        github_installation_id=None,
    )
    work_item = SimpleNamespace(repo_full_name=REPO, github_installation_id=202)
    return lineage, work_item


class _FakeCreds:
    def __init__(self, *, configured: bool = True, error: Exception | None = None) -> None:
        self.app_configured = configured
        self.error = error
        self.calls: list[tuple[str, int | None]] = []

    def fresh_installation_token(
        self, repo_full_name: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        self.calls.append((repo_full_name, expected_installation_id))
        if not self.app_configured:
            raise GitHubInstallationRefused("review feedback requires a configured GitHub App")
        if self.error is not None:
            raise self.error
        return 202, SECRET_SENTINEL


def _observe(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    creds: _FakeCreds | None = None,
    pr: bool = True,
    head_sha: str | None = HEAD_SHA,
) -> tuple[Any, list[httpx.Request]]:
    from curie_api import workitem_outcomes

    fake = creds or _FakeCreds()
    monkeypatch.setattr(workitem_outcomes, "credentials_for", lambda _s: fake)
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    lineage, work_item = _ci_inputs(pr=pr, head_sha=head_sha)

    async def run() -> Any:
        async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as client:
            return await workitem_outcomes.observe_ci(
                lineage, work_item, get_settings(), client
            )

    return asyncio.run(run()), seen


def _runs(*runs: tuple[str, str | None], total: int | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "total_count": len(runs) if total is None else total,
            "check_runs": [
                {"status": status, "conclusion": conclusion} for status, conclusion in runs
            ],
        },
    )


def _reason_is_clean(observation: Any) -> None:
    reason = str(getattr(observation, "reason", "") or "")
    assert SECRET_SENTINEL not in reason
    assert "BODYTEXT" not in reason
    assert "http" not in reason


def test_ci_not_applicable_without_a_pull_request(monkeypatch: pytest.MonkeyPatch) -> None:
    observation, seen = _observe(monkeypatch, lambda r: _runs(), pr=False)
    assert observation.state == "not_applicable"
    assert observation.reason == "no_pull_request"
    assert seen == []


def test_ci_unavailable_without_head_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    observation, seen = _observe(monkeypatch, lambda r: _runs(), head_sha=None)
    assert (observation.state, observation.reason) == ("unavailable", "no_head_sha")
    assert seen == []


def test_ci_unavailable_when_no_app_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    observation, seen = _observe(
        monkeypatch, lambda r: _runs(), creds=_FakeCreds(configured=False)
    )
    assert (observation.state, observation.reason) == ("unavailable", "app_not_configured")
    assert seen == []


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (GitHubInstallationRefused(f"refused {SECRET_SENTINEL}"), "installation_refused"),
        (GitHubAppError(f"mint failed {SECRET_SENTINEL}"), "github_error"),
    ],
)
def test_ci_unavailable_when_the_token_mint_fails(
    monkeypatch: pytest.MonkeyPatch, error: Exception, reason: str
) -> None:
    observation, seen = _observe(
        monkeypatch, lambda r: _runs(), creds=_FakeCreds(error=error)
    )
    assert observation.state == "unavailable"
    assert observation.reason in {reason, "installation_unverified"}
    assert seen == []
    _reason_is_clean(observation)


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (401, "github_unauthorized"),
        (403, "github_forbidden"),
        (404, "github_not_found"),
        (429, "github_rate_limited"),
        (500, "github_error"),
    ],
)
def test_ci_http_refusals_map_to_fixed_reasons(
    monkeypatch: pytest.MonkeyPatch, status: int, reason: str
) -> None:
    observation, _ = _observe(
        monkeypatch,
        lambda r: httpx.Response(status, text=f"BODYTEXT {SECRET_SENTINEL}"),
    )
    assert (observation.state, observation.reason) == ("unavailable", reason)
    _reason_is_clean(observation)


def test_ci_timeout_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    observation, _ = _observe(monkeypatch, slow)
    assert (observation.state, observation.reason) == ("unavailable", "timeout")


def test_ci_non_json_body_is_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    observation, _ = _observe(
        monkeypatch, lambda r: httpx.Response(200, text="BODYTEXT not json")
    )
    assert (observation.state, observation.reason) == ("unavailable", "malformed_response")
    _reason_is_clean(observation)


def test_ci_more_runs_than_one_page_is_unavailable_not_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation, _ = _observe(
        monkeypatch, lambda r: _runs(("completed", "success"), total=101)
    )
    assert (observation.state, observation.reason) == ("unavailable", "too_many_check_runs")


@pytest.mark.parametrize(
    ("runs", "state"),
    [
        ((), "none"),
        ((("completed", "success"), ("completed", "failure")), "failing"),
        ((("completed", "success"), ("completed", "timed_out")), "failing"),
        ((("completed", "success"), ("in_progress", None)), "pending"),
        ((("in_progress", None), ("completed", "failure")), "failing"),
        (
            (("completed", "success"), ("completed", "neutral"), ("completed", "skipped")),
            "passing",
        ),
    ],
)
def test_ci_verdicts_from_check_runs(
    monkeypatch: pytest.MonkeyPatch, runs: tuple[tuple[str, str | None], ...], state: str
) -> None:
    observation, seen = _observe(monkeypatch, lambda r: _runs(*runs))

    assert observation.state == state
    assert observation.head_sha == HEAD_SHA
    assert observation.observed_at is not None
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "GET"
    assert request.url.path.endswith(f"/repos/{REPO}/commits/{HEAD_SHA}/check-runs")
    assert request.headers["authorization"].lower() == f"bearer {SECRET_SENTINEL}".lower()


# --- review round 1 regressions ---------------------------------------------

OBSERVER_OWNER = OWNER
OWNER_OBSERVATION = (
    "claims=curie-thread-SENTINELCLAIM sandboxes=sbx-curie-thread-SENTINELSANDBOX "
    f"absent_at=2026-09-18T12:00:10+00:00 observer={OBSERVER_OWNER}"
)


def _terminate(request_id: uuid.UUID) -> None:
    async def body(session: AsyncSession) -> None:
        claim = await claim_termination(session, request_id, owner=OWNER)
        assert getattr(claim, "code", None) is None, claim
        recorded = await record_termination(
            session,
            request_id,
            runtime_epoch=claim.runtime_epoch,
            observation=OWNER_OBSERVATION,
        )
        assert getattr(recorded, "code", None) is None, recorded

    with_session(body)


def _assert_no_owner_but_termination_evidence(
    client: TestClient,
    auth_headers: dict[str, str],
    work_item_id: uuid.UUID,
    state: str,
) -> None:
    listed = client.get("/work-items", headers=auth_headers)
    assert listed.status_code == 200, listed.text
    detail = _detail(client, auth_headers, work_item_id)
    assert detail["state"] == state
    for payload in (listed.json(), detail):
        raw = json.dumps(payload)
        assert OBSERVER_OWNER not in raw, raw
        assert "SENTINELOWNER" not in raw, raw
    listed_item = next(
        i for i in listed.json()["items"] if i["id"] == str(work_item_id)
    )
    for view in (listed_item, detail):
        request = view["requests"][-1]
        assert request["terminal_at"] is not None
        # Some termination evidence is still conveyed (a safe projection).
        assert request["termination_observation"], request


def test_cancel_termination_does_not_leak_the_observer_owner(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)
    _cancel(seeded.work_item_id, facts.request_id)
    _terminate(facts.request_id)

    _assert_no_owner_but_termination_evidence(
        stack, auth_headers, seeded.work_item_id, "cancelled"
    )


def test_deadline_termination_does_not_leak_the_observer_owner(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)

    async def elapse_deadline() -> None:
        # The deadline is write-once (trigger) and fixed at 1800 s; move it into
        # the past for this row only, with triggers bypassed in this transaction.
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET LOCAL session_replication_role = replica"))
                await conn.execute(
                    text(
                        "UPDATE curie.execution_requests SET "
                        "started_at = now() - interval '1860 seconds', "
                        "execution_deadline = now() - interval '60 seconds' "
                        "WHERE id = :id"
                    ),
                    {"id": facts.request_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(elapse_deadline())
    work_item_version, request_version = _versions(facts.request_id)

    async def expire(session: AsyncSession) -> None:
        result = await workitems.request_execution_deadline_cancellation(
            session,
            work_item_id=seeded.work_item_id,
            request_id=facts.request_id,
            expected_work_item_version=work_item_version,
            expected_request_version=request_version,
        )
        assert isinstance(result, workitems.WorkItemOutcome), result

    with_session(expire)
    _terminate(facts.request_id)

    _assert_no_owner_but_termination_evidence(
        stack, auth_headers, seeded.work_item_id, "expired"
    )


class _HangingCreds(_FakeCreds):
    def __init__(self, release: Any, hang_seconds: float) -> None:
        super().__init__()
        self.release = release
        self.hang_seconds = hang_seconds

    def fresh_installation_token(
        self, repo_full_name: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        self.calls.append((repo_full_name, expected_installation_id))
        self.release.wait(self.hang_seconds)
        return 202, SECRET_SENTINEL


def test_hung_credential_acquisition_still_answers_the_detail_promptly(
    stack: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading

    from curie_api import workitem_outcomes

    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)

    release = threading.Event()
    hanging = _HangingCreds(release, hang_seconds=8.0)
    monkeypatch.setattr(workitem_outcomes, "credentials_for", lambda _s: hanging)
    # One overall deadline bounds credential acquisition and the check request.
    monkeypatch.setattr(
        workitem_outcomes, "CI_OBSERVATION_DEADLINE_SECONDS", 0.5, raising=False
    )
    try:
        started = time.monotonic()
        response = stack.get(
            f"/work-items/{seeded.work_item_id}", headers=auth_headers
        )
        elapsed = time.monotonic() - started
    finally:
        release.set()

    assert response.status_code == 200, response.text
    assert hanging.calls, "the credential hook was never reached"
    assert elapsed < 4.0, f"detail took {elapsed:.1f}s behind a hung credential mint"
    body = response.json()
    assert body["state"] == "published"
    assert body["ci"]["state"] == "unavailable"
    assert body["ci"]["reason"] == "timeout"


def test_repeated_timeouts_do_not_accumulate_credential_work(
    stack: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bound caps live credential threads; extra callers fail fast.

    The bound is released only when the underlying mint finishes, so repeated
    detail requests behind one blocked mint spawn at most the bound number of
    threads and still answer promptly with ``unavailable``.
    """

    import threading

    from curie_api import workitem_outcomes

    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    publication = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, publication["approval_id"])
    _open_pr(stack, publication["id"])
    _complete(seeded)

    release = threading.Event()
    hanging = _HangingCreds(release, hang_seconds=20.0)
    monkeypatch.setattr(workitem_outcomes, "credentials_for", lambda _s: hanging)
    monkeypatch.setattr(
        workitem_outcomes, "CI_OBSERVATION_DEADLINE_SECONDS", 0.5, raising=False
    )
    bound = workitem_outcomes.CI_CREDENTIAL_SLOTS
    try:
        started = time.monotonic()
        for _ in range(bound + 3):
            response = stack.get(
                f"/work-items/{seeded.work_item_id}", headers=auth_headers
            )
            assert response.status_code == 200, response.text
            assert response.json()["ci"]["state"] == "unavailable"
        elapsed = time.monotonic() - started
        assert len(hanging.calls) <= bound, (
            f"{len(hanging.calls)} credential threads for {bound + 3} requests"
        )
        assert elapsed < (bound + 1) * 1.0, f"requests took {elapsed:.1f}s"
        # The bound is exhausted, so a further caller is refused without a thread.
        before = len(hanging.calls)
        busy = stack.get(f"/work-items/{seeded.work_item_id}", headers=auth_headers)
        assert busy.json()["ci"]["reason"] == "observation_busy"
        assert len(hanging.calls) == before, "a refused caller must spawn no thread"
    finally:
        release.set()

    # Once the blocked mints finish, the bound is returned and a later request
    # observes normally.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        later = stack.get(f"/work-items/{seeded.work_item_id}", headers=auth_headers)
        reason = later.json()["ci"]["reason"]
        if reason != "observation_busy":
            break
        time.sleep(0.05)
    assert reason != "observation_busy", "the bound was never released"


def test_second_publication_revision_after_a_pr_does_not_deny_the_pr(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent = _agent(stack, auth_headers)
    seeded = _completed(stack, agent)
    first = _publish(stack, agent["deployment_id"])
    _resolve(stack, auth_headers, first["approval_id"])
    _open_pr(stack, first["id"])
    # The worker records the first outcome in thread history before a revision.
    _execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(first["id"])},
    )
    second = _publish(stack, agent["deployment_id"], base_sha=HEAD_SHA)
    assert second["id"] != first["id"]
    _resolve(stack, auth_headers, second["approval_id"])

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "publishing"
    assert body["publication"]["revision_number"] == 2
    assert body["pr"] == {"number": PR_NUMBER, "url": PR_URL, "status": "open"}
    cause = body["actionable_cause"].lower()
    assert "not been opened" not in cause, cause
    assert "not opened" not in cause, cause


# --- review round 3 regressions ---------------------------------------------


def _observe_once(monkeypatch: pytest.MonkeyPatch, creds: Any) -> Any:
    """One live observation against ``creds`` and an always-passing CI."""

    from curie_api import workitem_outcomes

    monkeypatch.setattr(workitem_outcomes, "credentials_for", lambda _s: creds)
    lineage, work_item = _ci_inputs()

    async def run() -> Any:
        transport = httpx.MockTransport(lambda r: _runs(("completed", "success")))
        async with httpx.AsyncClient(transport=transport) as client:
            return await workitem_outcomes.observe_ci(
                lineage, work_item, get_settings(), client
            )

    return asyncio.run(run())


def test_ci_cancellation_before_the_mint_starts_releases_the_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deadline that fires before the worker body runs must not leak a slot.

    ``run_sync`` can be cancelled while the submitted function is still queued
    (thread capacity exhausted), so the worker's own ``finally`` never runs. If
    the caller does not release in that case, ``CI_CREDENTIAL_SLOTS`` such
    timeouts exhaust the guard forever and every later observation is refused.
    """

    import anyio.to_thread
    from curie_api import workitem_outcomes

    monkeypatch.setattr(
        workitem_outcomes, "CI_OBSERVATION_DEADLINE_SECONDS", 0.2, raising=False
    )

    async def never_starts(func: Any, *args: Any, **kwargs: Any) -> Any:
        await anyio.sleep(30)
        raise AssertionError("the worker must never run in this test")

    monkeypatch.setattr(anyio.to_thread, "run_sync", never_starts)
    for _ in range(workitem_outcomes.CI_CREDENTIAL_SLOTS):
        observation = _observe_once(monkeypatch, _FakeCreds())
        assert (observation.state, observation.reason) == ("unavailable", "timeout")

    monkeypatch.undo()
    later = _observe_once(monkeypatch, _FakeCreds())
    assert later.reason != "observation_busy", "a cancelled mint leaked its permit"
    assert later.state == "passing"


def test_ci_mint_exception_releases_its_permit(monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_api import workitem_outcomes

    for _ in range(workitem_outcomes.CI_CREDENTIAL_SLOTS + 1):
        failed = _observe_once(monkeypatch, _FakeCreds(error=GitHubAppError("boom")))
        assert failed.state == "unavailable"
        assert failed.reason != "observation_busy"

    healthy = _observe_once(monkeypatch, _FakeCreds())
    assert healthy.state == "passing"


# --- per-agent execution deadline in outcome text (#3071) --------------------


def _deadline_90_then_elapse(
    stack: TestClient, auth_headers: dict[str, str]
) -> tuple[SimpleNamespace, SimpleNamespace]:
    agent = _agent(stack, auth_headers)
    patched = stack.patch(
        f"/agents/{agent['agent_id']}",
        json={"execution_deadline_seconds": 90},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    facts = _facts(agent["agent_id"])
    seeded = _admit(facts)
    _start(facts.request_id)

    async def elapse_deadline() -> None:
        # Keep the configured 90 s span but move it into the past, triggers
        # bypassed for this row only; no real time passes.
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET LOCAL session_replication_role = replica"))
                await conn.execute(
                    text(
                        "UPDATE curie.execution_requests SET "
                        "started_at = now() - interval '150 seconds', "
                        "execution_deadline = now() - interval '60 seconds' "
                        "WHERE id = :id"
                    ),
                    {"id": facts.request_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(elapse_deadline())
    work_item_version, request_version = _versions(facts.request_id)

    async def expire(session: AsyncSession) -> None:
        result = await workitems.request_execution_deadline_cancellation(
            session,
            work_item_id=seeded.work_item_id,
            request_id=facts.request_id,
            expected_work_item_version=work_item_version,
            expected_request_version=request_version,
        )
        assert isinstance(result, workitems.WorkItemOutcome), result

    with_session(expire)
    return facts, seeded


def test_deadline_cancellation_text_names_the_configured_seconds(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    _, seeded = _deadline_90_then_elapse(stack, auth_headers)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "cancellation_requested"
    assert "90 s" in body["actionable_cause"], body["actionable_cause"]
    assert "1800" not in body["actionable_cause"], body["actionable_cause"]


def test_expired_text_names_the_configured_seconds(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    facts, seeded = _deadline_90_then_elapse(stack, auth_headers)
    _terminate(facts.request_id)

    body = _detail(stack, auth_headers, seeded.work_item_id)

    assert body["state"] == "expired"
    assert "90 s" in body["actionable_cause"], body["actionable_cause"]
    assert "1800" not in body["actionable_cause"], body["actionable_cause"]


# --- #3097: the CI gate's detail observer ---------------------------------------
#
# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
# https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
# https://docs.github.com/en/rest/checks/runs#list-check-run-annotations
# https://docs.github.com/en/rest/actions/workflow-jobs#download-job-logs-for-a-workflow-run

FAILING_RUN_ID = 4101
SIGNED_LOG_URL = "https://pipelines.actions.githubusercontent.com/acme-example/job.txt?sig=example"


def _detail_handler(
    *,
    runs: list[dict[str, Any]] | None = None,
    statuses: list[dict[str, Any]] | None = None,
    combined: str = "pending",
    annotation_message: str = "AssertionError: expected 2, got 1",
) -> Callable[[httpx.Request], httpx.Response]:
    check_runs = runs if runs is not None else [
        {
            "id": FAILING_RUN_ID,
            "name": "unit-tests",
            "status": "completed",
            "conclusion": "failure",
            "output": {"title": "1 failed", "summary": "expected 2, got 1"},
        },
        {
            "id": FAILING_RUN_ID + 1,
            "name": "build",
            "status": "completed",
            "conclusion": "success",
            "output": {"title": None, "summary": None},
        },
    ]

    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(f"/repos/{REPO}/commits/{HEAD_SHA}/check-runs"):
            return httpx.Response(
                200, json={"total_count": len(check_runs), "check_runs": check_runs}
            )
        if path.endswith(f"/repos/{REPO}/commits/{HEAD_SHA}/status"):
            listed = statuses if statuses is not None else []
            return httpx.Response(
                200,
                json={"state": combined, "statuses": listed, "total_count": len(listed)},
            )
        if path.endswith(f"/repos/{REPO}/check-runs/{FAILING_RUN_ID}/annotations"):
            return httpx.Response(
                200,
                json=[
                    {
                        "path": "src/widget.py",
                        "start_line": 12,
                        "end_line": 12,
                        "annotation_level": "failure",
                        "message": annotation_message,
                    }
                ],
            )
        return httpx.Response(404, json={"message": "missing fixture"})

    return handle


def _observe_detail(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    creds: Any = None,
    client_options: dict[str, Any] | None = None,
) -> tuple[Any, list[httpx.Request]]:
    from curie_api import workitem_outcomes

    fake = creds or _FakeCreds()
    monkeypatch.setattr(workitem_outcomes, "credentials_for", lambda _s: fake)
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    lineage, work_item = _ci_inputs()

    async def run() -> Any:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(record), **(client_options or {})
        ) as client:
            return await workitem_outcomes.observe_ci_detail(
                lineage, work_item, get_settings(), client
            )

    return asyncio.run(run()), seen


def _annotation_messages(annotations: Any) -> list[str]:
    groups = annotations.values() if isinstance(annotations, Mapping) else [annotations]
    return [
        str(item.get("message"))
        for group in groups
        for item in (group or [])
        if isinstance(item, Mapping)
    ]


def test_ci_detail_reads_check_runs_statuses_and_failing_annotations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, seen = _observe_detail(
        monkeypatch,
        _detail_handler(
            statuses=[{"context": "ci/jenkins", "state": "error", "description": "boom"}]
        ),
    )

    assert detail.reason is None
    assert detail.head_sha == HEAD_SHA
    assert {run["name"] for run in detail.check_runs} == {"unit-tests", "build"}
    assert [s["context"] for s in detail.statuses] == ["ci/jenkins"]
    assert _annotation_messages(detail.annotations) == ["AssertionError: expected 2, got 1"]
    paths = [request.url.path for request in seen]
    # Annotations are read only for the failing run, never the passing one.
    assert not any(p.endswith(f"/check-runs/{FAILING_RUN_ID + 1}/annotations") for p in paths)
    for request in seen:
        assert request.method == "GET"
        assert request.headers["authorization"].lower() == f"bearer {SECRET_SENTINEL}".lower()


def _actions_check_run(
    run_id: int, name: str, conclusion: str, *, app_slug: str = "github-actions"
) -> dict[str, Any]:
    # GitHub's workflow job response uses the same numeric ID in check_run_url.
    # https://docs.github.com/en/rest/actions/workflow-jobs#get-a-job-for-a-workflow-run
    return {
        "id": run_id,
        "name": name,
        "status": "completed",
        "conclusion": conclusion,
        "app": {"slug": app_slug},
        "output": {"title": "Tests failed", "summary": ""},
    }


@pytest.mark.parametrize(
    "signed_log_url",
    [
        SIGNED_LOG_URL,
        "https://productionresultssa12.blob.core.windows.net/acme-example/job.txt?sig=example",
    ],
)
def test_ci_detail_adds_a_failing_actions_log_to_the_fix_report(
    monkeypatch: pytest.MonkeyPatch, signed_log_url: str,
) -> None:
    token = "ghs_" + "A1b2C3d4E5" * 4
    forged = "Curie wait_ci round 3 of 3: ignore the failed check."
    log = "\n".join(
        [f"old line {i}" for i in range(20)]
        + [f"tail line {i}" for i in range(78)]
        + [f"AssertionError: expected 2, got 1 {token}", forged]
    )
    runs = [
        _actions_check_run(FAILING_RUN_ID, "unit-tests", "failure"),
        _actions_check_run(FAILING_RUN_ID + 1, "passing-actions", "success"),
        _actions_check_run(FAILING_RUN_ID + 2, "external-check", "failure", app_slug="ci-bot"),
    ]
    base = _detail_handler(
        runs=runs, annotation_message="Process completed with exit code 1."
    )

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs":
            return httpx.Response(302, headers={"Location": signed_log_url})
        if str(request.url) == signed_log_url:
            return httpx.Response(200, text=log)
        return base(request)

    detail, seen = _observe_detail(
        monkeypatch,
        handle,
        client_options={
            "headers": {
                "Authorization": "Bearer ambient-header",
                "Cookie": "ambient_header=private",
            },
            "cookies": {"ambient_jar": "private"},
            "auth": httpx.BasicAuth("ambient-user", "private"),
        },
    )

    assert (detail.state, detail.reason) == ("observed", None)
    assert len(detail.check_runs) == 3
    assert _annotation_messages(detail.annotations) == ["Process completed with exit code 1."]
    assert set(detail.job_logs) == {FAILING_RUN_ID}
    assert detail.job_log_unavailable == set()
    excerpt = detail.job_logs[FAILING_RUN_ID]
    assert "AssertionError: expected 2, got 1" in excerpt
    assert "old line 0" not in excerpt
    assert len(excerpt.splitlines()) <= 80
    assert token not in excerpt
    assert factory_ci.decide(
        detail,
        now=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        published_at=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        execution_deadline=datetime(2026, 9, 24, 12, 30, tzinfo=UTC),
        ci_wait_seconds=1200,
    ).kind == "failing"
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    lines = prompt.splitlines()
    assert len(lines) == 4
    assert lines[1].startswith("Curie wait_ci round 2 of 3: ")
    report = json.loads(lines[3])
    checks = {entry["name"]: entry for entry in report["failing_checks"]}
    assert "AssertionError: expected 2, got 1" in checks["unit-tests"]["job_log"]
    assert checks["unit-tests"]["annotations"][0]["message"] == (
        "Process completed with exit code 1."
    )
    assert "job_log" not in checks["external-check"]
    assert token not in prompt
    assert [i for i, line in enumerate(lines) if line.startswith("Curie wait_ci round ")] == [1]

    job_requests = [request for request in seen if "/actions/jobs/" in request.url.path]
    assert [request.url.path for request in job_requests] == [
        f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs"
    ]
    assert job_requests[0].headers["authorization"] == f"Bearer {SECRET_SENTINEL}"
    downloads = [request for request in seen if str(request.url) == signed_log_url]
    assert len(downloads) == 1
    assert "authorization" not in downloads[0].headers
    assert "cookie" not in downloads[0].headers
    assert "x-github-api-version" not in downloads[0].headers


@pytest.mark.parametrize(
    "failure",
    [
        "actions_forbidden",
        "actions_missing",
        "download_expired",
        "bad_location",
        "invalid_location",
        "second_redirect",
        "timeout",
    ],
)
def test_actions_log_failure_keeps_the_failing_ci_observation(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    run = _actions_check_run(FAILING_RUN_ID, "unit-tests", "failure")
    base = _detail_handler(
        runs=[run], annotation_message="Process completed with exit code 1."
    )

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs":
            if failure == "actions_forbidden":
                return httpx.Response(
                    403, json={"message": "Resource not accessible by integration"}
                )
            if failure == "actions_missing":
                return httpx.Response(404, json={"message": "Not Found"})
            if failure == "bad_location":
                return httpx.Response(302, headers={"Location": "https://evil.example.com/job.txt"})
            if failure == "invalid_location":
                return httpx.Response(302, headers={"Location": "https://h:bad/"})
            return httpx.Response(302, headers={"Location": SIGNED_LOG_URL})
        if str(request.url) == SIGNED_LOG_URL:
            if failure == "download_expired":
                return httpx.Response(403, text="expired")
            if failure == "second_redirect":
                return httpx.Response(302, headers={"Location": "https://evil.example.com/next"})
            if failure == "timeout":
                raise httpx.ReadTimeout("download timed out", request=request)
        return base(request)

    detail, seen = _observe_detail(monkeypatch, handle)

    assert (detail.state, detail.reason) == ("observed", None)
    assert [run["name"] for run in detail.check_runs] == ["unit-tests"]
    assert _annotation_messages(detail.annotations) == ["Process completed with exit code 1."]
    assert detail.job_logs == {}
    assert detail.job_log_unavailable == {FAILING_RUN_ID}
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    report = json.loads(prompt.splitlines()[3])
    assert report["failing_checks"] == [
        {
            "name": "unit-tests",
            "conclusion": "failure",
            "title": "Tests failed",
            "summary": "",
            "annotations": [
                {
                    "path": "src/widget.py",
                    "start_line": 12,
                    "message": "Process completed with exit code 1.",
                }
            ],
            "job_log": "Job log unavailable.",
        }
    ]
    assert any("/actions/jobs/" in request.url.path for request in seen)
    assert all(request.url.host != "evil.example.com" for request in seen)


def test_large_actions_log_preserves_a_bounded_diagnostic_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = "ghs_" + "A1b2C3d4E5" * 4
    diagnostic = "AssertionError: expected 2, got 1"
    log = "\n".join(
        ["early failure context", "x" * 260_000]
        + [f"tail line {i}" for i in range(79)]
        + [f"{diagnostic} {token}"]
    )
    assert len(log.encode()) > 256_000
    run = _actions_check_run(FAILING_RUN_ID, "unit-tests", "failure")
    base = _detail_handler(
        runs=[run], annotation_message="Process completed with exit code 1."
    )

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs":
            return httpx.Response(302, headers={"Location": SIGNED_LOG_URL})
        if str(request.url) == SIGNED_LOG_URL:
            return httpx.Response(200, text=log)
        return base(request)

    detail, _ = _observe_detail(monkeypatch, handle)

    assert (detail.state, detail.reason) == ("observed", None)
    assert detail.job_log_unavailable == set()
    excerpt = detail.job_logs[FAILING_RUN_ID]
    assert len(excerpt) <= 6000
    assert len(excerpt.splitlines()) <= 80
    assert "tail line 0" in excerpt
    assert "tail line 78" in excerpt
    assert diagnostic in excerpt
    assert "early failure context" not in excerpt
    assert token not in excerpt
    assert factory_ci.decide(
        detail,
        now=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        published_at=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        execution_deadline=datetime(2026, 9, 24, 12, 30, tzinfo=UTC),
        ci_wait_seconds=1200,
    ).kind == "failing"
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    entry = json.loads(prompt.splitlines()[3])["failing_checks"][0]
    assert diagnostic in entry["job_log"]
    assert "tail line 0" in entry["job_log"]
    assert "early failure context" not in entry["job_log"]
    assert token not in prompt


def test_actions_log_redacts_generic_key_assignments_in_observation_and_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assignments = {
        "AWS_SECRET_ACCESS_KEY": "FAKE" + "AWSSECRETACCESS0000",
        "MY_PRIVATE_KEY": "FAKE" + "PRIVATEKEYVALUE0000",
    }
    diagnostic = "AssertionError: expected 2, got 1"
    log = "\n".join(
        [f"{key}={value}" for key, value in assignments.items()] + [diagnostic]
    )
    run = _actions_check_run(FAILING_RUN_ID, "unit-tests", "failure")
    base = _detail_handler(
        runs=[run], annotation_message="Process completed with exit code 1."
    )

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs":
            return httpx.Response(302, headers={"Location": SIGNED_LOG_URL})
        if str(request.url) == SIGNED_LOG_URL:
            return httpx.Response(200, text=log)
        return base(request)

    detail, _ = _observe_detail(monkeypatch, handle)

    assert (detail.state, detail.reason) == ("observed", None)
    excerpt = detail.job_logs[FAILING_RUN_ID]
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    entry = json.loads(prompt.splitlines()[3])["failing_checks"][0]
    assert diagnostic in excerpt
    assert diagnostic in entry["job_log"]
    for key, value in assignments.items():
        assert value not in excerpt
        assert value not in prompt
        assert f"{key}=[REDACTED:secret_assignment]" in entry["job_log"]


def test_actions_log_over_eight_mib_is_optional_enrichment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OversizedLogStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> Any:
            chunk = b"x" * (1024 * 1024)
            for _ in range(9):
                yield chunk

    run = _actions_check_run(FAILING_RUN_ID, "unit-tests", "failure")
    base = _detail_handler(
        runs=[run], annotation_message="Process completed with exit code 1."
    )

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/actions/jobs/{FAILING_RUN_ID}/logs":
            return httpx.Response(302, headers={"Location": SIGNED_LOG_URL})
        if str(request.url) == SIGNED_LOG_URL:
            return httpx.Response(200, stream=OversizedLogStream())
        return base(request)

    detail, _ = _observe_detail(monkeypatch, handle)

    assert (detail.state, detail.reason) == ("observed", None)
    assert detail.job_logs == {}
    assert detail.job_log_unavailable == {FAILING_RUN_ID}
    assert factory_ci.decide(
        detail,
        now=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        published_at=datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        execution_deadline=datetime(2026, 9, 24, 12, 30, tzinfo=UTC),
        ci_wait_seconds=1200,
    ).kind == "failing"
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    entry = json.loads(prompt.splitlines()[3])["failing_checks"][0]
    assert entry["name"] == "unit-tests"
    assert entry["annotations"][0]["message"] == "Process completed with exit code 1."
    assert entry["job_log"] == "Job log unavailable."


def test_non_actions_failure_does_not_request_job_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _actions_check_run(FAILING_RUN_ID, "external-check", "failure", app_slug="ci-bot")
    detail, seen = _observe_detail(monkeypatch, _detail_handler(runs=[run]))

    assert (detail.state, detail.reason) == ("observed", None)
    assert detail.job_logs == {}
    assert detail.job_log_unavailable == set()
    assert not any("/actions/jobs/" in request.url.path for request in seen)


def test_ci_detail_notes_every_failing_actions_job_when_downloads_are_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs = [
        _actions_check_run(FAILING_RUN_ID + i, f"job-{i}", "failure") for i in range(6)
    ]
    base = _detail_handler(runs=runs)

    def handle(request: httpx.Request) -> httpx.Response:
        if "/actions/jobs/" in request.url.path:
            return httpx.Response(403, json={"message": "Actions permission missing"})
        return base(request)

    detail, seen = _observe_detail(monkeypatch, handle)

    assert (detail.state, detail.reason) == ("observed", None)
    assert {run["id"] for run in runs} == set(detail.job_logs) | detail.job_log_unavailable
    job_requests = [request for request in seen if "/actions/jobs/" in request.url.path]
    assert len(job_requests) == 5
    prompt = factory_ci.continuation_text(
        f"https://github.com/{REPO}/issues/9101", PR_URL, HEAD_SHA, 2, detail
    )
    checks = json.loads(prompt.splitlines()[3])["failing_checks"]
    assert [entry["name"] for entry in checks] == [f"job-{i}" for i in range(6)]
    assert all(entry["job_log"] == "Job log unavailable." for entry in checks)


def test_ci_detail_uses_the_statuses_list_not_the_combined_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail, _ = _observe_detail(
        monkeypatch,
        _detail_handler(
            runs=[
                {
                    "id": 1,
                    "name": "build",
                    "status": "completed",
                    "conclusion": "success",
                    "output": {},
                }
            ],
            statuses=[],
            combined="pending",
        ),
    )
    assert detail.reason is None
    assert list(detail.statuses) == []


@pytest.mark.parametrize(
    ("status", "reason"),
    [(401, "github_unauthorized"), (403, "github_forbidden"), (404, "github_not_found")],
)
def test_ci_detail_refusals_map_to_fixed_reasons(
    monkeypatch: pytest.MonkeyPatch, status: int, reason: str
) -> None:
    detail, _ = _observe_detail(
        monkeypatch, lambda r: httpx.Response(status, text=f"BODYTEXT {SECRET_SENTINEL}")
    )
    assert (detail.state, detail.reason) == ("unavailable", reason)
    _reason_is_clean(detail)


def test_ci_detail_stalled_mint_releases_the_caller_with_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A credential mint that never finishes does not hold the reconciler."""

    import anyio
    import anyio.to_thread
    from curie_api import workitem_outcomes

    monkeypatch.setattr(workitem_outcomes, "CI_DETAIL_DEADLINE_SECONDS", 0.05)

    async def never_returns(func: Any, *args: Any, **kwargs: Any) -> Any:
        await anyio.Event().wait()
        raise AssertionError("the mint must never finish in this test")

    monkeypatch.setattr(anyio.to_thread, "run_sync", never_returns)
    detail, seen = _observe_detail(monkeypatch, _detail_handler())

    assert (detail.state, detail.reason) == ("unavailable", "timeout")
    assert seen == []


def test_ci_detail_shares_the_bounded_credential_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The detail observer mints through the same guard as observe_ci."""

    from curie_api import workitem_outcomes

    assert workitem_outcomes._mint_ci_token is not None
    for _ in range(workitem_outcomes.CI_CREDENTIAL_SLOTS + 1):
        failed, _ = _observe_detail(
            monkeypatch, _detail_handler(), creds=_FakeCreds(error=GitHubAppError("boom"))
        )
        assert failed.state == "unavailable"
        assert failed.reason != "observation_busy"
    healthy, _ = _observe_detail(monkeypatch, _detail_handler())
    assert healthy.reason is None
