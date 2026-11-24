"""Exercise the worker routes against a persisted maintenance successor and its pull request."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api import factory_ci
from curie_api.config import get_settings
from curie_api.factory_conflict import VerifiedConflict
from curie_api.factory_conflict_authority import conflict_publication_authority
from curie_api.factory_maintenance import (
    MaintenanceObservation,
    MaintenanceReason,
    MaintenanceRepair,
)
from curie_api.factory_maintenance_observer import MaintenanceObserver
from curie_api.factory_maintenance_store import commit_observation
from curie_api.forges.registry import forges_registry
from curie_api.models import (
    Agent,
    ExecutionRequest,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
)
from curie_api.workitems import lifecycle
from forge_fakes import github

from apps.api.tests.test_publication_precheck import INSTALLATION_ID, REPOSITORY_ID
from apps.api.tests.test_publication_precheck import precheck_case as precheck_case
from apps.api.tests.test_publications import (
    BASE_SHA,
    FIRST_REVISION_SHA,
    PR_NUMBER,
    WORKER_HEADERS,
    _execute,
    _publication_payload,
    _rows,
)
from apps.api.tests.test_publications import publication_stack as publication_stack

TARGET = "4" * 40


@pytest.fixture
def repair_forge(
    precheck_case: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[github.GitHubRepositoryFake]:
    case = precheck_case
    state = case["client"].app.state
    binding = github.github_repository_binding(repo_id=REPOSITORY_ID)
    monkeypatch.setenv(
        "CURIE_FORGES", github.forges_env_json(github.forges_document(bindings=[binding]), tmp_path)
    )
    monkeypatch.setenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", "true")
    monkeypatch.setattr(github, "REPO_ID", REPOSITORY_ID)
    monkeypatch.setattr(github, "INSTALLATION_ID", INSTALLATION_ID)
    github.fake_installation_tokens(monkeypatch)
    get_settings.cache_clear()
    forge = github.GitHubRepositoryFake()
    branch = case["lineage"]["branch"]
    forge.branches = {"main": TARGET, branch: FIRST_REVISION_SHA}
    seeded = forge.seed_pull(branch, "main")
    forge.pulls[PR_NUMBER] = forge.pulls.pop(seeded)
    forge.issues[3215] = {"number": 3215, "state": "open", "labels": []}

    def github_response(request: httpx.Request) -> httpx.Response:
        response = forge.handle(request)
        if request.url.path.endswith(f"/pulls/{PR_NUMBER}"):
            return httpx.Response(
                200,
                json={
                    **response.json(),
                    "mergeable": case["truth"]["mergeable"],
                    "mergeable_state": "clean" if case["truth"]["mergeable"] else "dirty",
                    "node_id": case["truth"]["node_id"],
                },
            )
        return response

    http = httpx.AsyncClient(transport=httpx.MockTransport(github_response))
    monkeypatch.setattr(state, "forges", forges_registry(get_settings()))
    original = state.http_client
    state.http_client = http
    try:
        yield forge
    finally:
        state.http_client = original
        case["client"].portal.call(http.aclose)
        get_settings.cache_clear()


async def _successor(
    case: dict[str, Any], forge: github.GitHubRepositoryFake, reason: MaintenanceReason
) -> uuid.UUID:
    state = case["client"].app.state
    async with state.sessionmaker() as session:
        item = await session.get(WorkItem, case["work_item_id"])
        source = await session.get(ExecutionRequest, case["request_id"])
        lineage = await session.get(ThreadPublicationLineage, uuid.UUID(case["lineage_id"]))
        original = await session.get(Publication, uuid.UUID(case["publication_id"]))
        assert item and source and original and lineage
        agent = await session.get(Agent, item.agent_id)
        assert agent
        item.tenant_id = source.tenant_id = agent.tenant_id
        item.base_branch, item.base_source, item.base_commit = "main", "default", BASE_SHA
        item.base_resolved_request_id = source.id
        item.next_sequence = source.sequence + 1
        source.objective, source.requester = "Update the guide", "factory"
        source.reply_kind, source.reply_address = "slack", "C0EXAMPLE1"
        source.reply_conversation_id = case["reply_conversation_id"]
        lineage.head_sha = forge.branches[lineage.branch]
        original.execution_request_id = source.id
        original.terminal_at = (await lifecycle.database_now(session)).replace(tzinfo=None)
        await session.flush()
        settlement = await lifecycle.claim_publication_settlement(session, exclude=frozenset())
        assert settlement and settlement.request_id == source.id
        completed = await lifecycle.settle_ci_verdict(
            session,
            work_item_id=item.id,
            request_id=source.id,
            expected_work_item_version=settlement.work_item_version,
            expected_request_version=settlement.request_version,
            expected_subject=settlement.subject,
            status="completed",
            cause="completed",
            detail=None,
            verified_ci="green" if reason == "late_ci_red" else "no_ci",
        )
        assert isinstance(completed, lifecycle.WorkItemOutcome) and completed.request, completed
        observation = MaintenanceObservation(
            work_item_id=item.id,
            request_id=source.id,
            lineage_id=lineage.id,
            work_item_version=completed.work_item.version,
            request_version=completed.request.version,
            lineage_version=lineage.version,
            head_sha=lineage.head_sha,
            base_ref="main",
            base_sha=forge.branches["main"],
            reason=reason,
        )
        case["truth"]["mergeable"] = reason == "late_ci_red"
        if reason == "base_conflict":
            receipt = await commit_observation(
                session,
                observation,
                objective=VerifiedConflict("Resolve the conflict in the guide", ("README.md",)),
                refusal=None,
            )
        else:
            forge.report_check(TARGET, "build", "success")
            forge.report_check(FIRST_REVISION_SHA, "build", "failure")
            forge.check_runs[FIRST_REVISION_SHA]["build"]["started_at"] = (
                await lifecycle.database_now(session)
            ).isoformat()
            observer = MaintenanceObserver(
                state.sessionmaker, get_settings(), state.http_client, state.valkey
            )
            receipt = await observer(observation)
    assert isinstance(receipt, MaintenanceRepair) and receipt.status == "admitted", receipt
    return receipt.request_id


def test_sha256_successor_retains_both_pins_and_conflict_paths(
    precheck_case: dict[str, Any], repair_forge: github.GitHubRepositoryFake
) -> None:
    head, base = "a" * 64, "b" * 64
    repair_forge.branches.update({precheck_case["lineage"]["branch"]: head, "main": base})

    async def resolve() -> None:
        request_id = await _successor(precheck_case, repair_forge, "base_conflict")
        async with precheck_case["client"].app.state.sessionmaker() as session:
            request = await session.get(ExecutionRequest, request_id)
            assert request is not None
            authority = await conflict_publication_authority(session, request)
            assert authority is not None
            assert (authority.head_sha, authority.base_sha, authority.conflict_paths) == (
                head,
                base,
                ("README.md",),
            )

    precheck_case["client"].portal.call(resolve)


def _acquire_conflict_successor(case: dict[str, Any], repair_id: uuid.UUID) -> httpx.Response:
    claim = {
        "owner": "factory",
        "generation": 1,
        "wake": {
            "kind": "slack",
            "adapter": "default",
            "address": "C0EXAMPLE1",
            "conversation_id": case["reply_conversation_id"],
        },
        "context": None,
    }
    return case["client"].post(
        f"/v1/internal/work-items/requests/{repair_id}/acquire",
        headers=WORKER_HEADERS,
        json=claim,
    )


def test_acquire_settles_mismatched_conflict_authority(
    precheck_case: dict[str, Any], repair_forge: github.GitHubRepositoryFake
) -> None:
    case = precheck_case
    client = case["client"]
    repair_id = client.portal.call(_successor, case, repair_forge, "base_conflict")
    case["truth"]["node_id"] = "PR_other"
    _execute(
        "UPDATE curie.thread_publication_lineages SET code_host_pr_id = :pr WHERE id = :id",
        {"id": case["lineage_id"], "pr": "PR_other"},
    )
    for _ in range(2):
        refused = _acquire_conflict_successor(case, repair_id)
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"]["code"] == "factory_start_failed"

    view = client.get(f"/v1/internal/work-items/requests/{repair_id}", headers=WORKER_HEADERS)
    assert view.is_success, view.text
    assert view.json()["status"] == "failed"
    assert view.json()["terminal_cause"] == "maintenance_refused"
    assert view.json()["started_at"] is None
    assert _rows(
        "SELECT outcome FROM curie.factory_maintenance_events WHERE repair_request_id = :id "
        "ORDER BY id",
        {"id": repair_id},
    ) == [{"outcome": "admitted"}, {"outcome": "conflict_authority_mismatch"}]


def test_acquire_preserves_a_settled_conflict_publication(
    precheck_case: dict[str, Any], repair_forge: github.GitHubRepositoryFake
) -> None:
    case = precheck_case
    client = case["client"]
    repair_id = client.portal.call(_successor, case, repair_forge, "base_conflict")
    approval_id = uuid.uuid4()
    _execute(
        """
        WITH approval AS (
            INSERT INTO curie.approvals
                (id, agent_id, conversation_id, author, summary, reply_kind, reply_channel,
                 dedupe_key, status, purpose)
            SELECT :approval, a.agent_id, a.conversation_id, a.author, a.summary, a.reply_kind,
                   a.reply_channel, :dedupe, 'approved', 'publication'
            FROM curie.approvals a JOIN curie.publications p ON p.approval_id = a.id
            WHERE p.id = :source
            RETURNING id
        )
        INSERT INTO curie.publications
            (id, approval_id, deployment_id, lineage_id, execution_request_id, revision_number,
             repo_full_name, status, publication_mode, base_sha, expected_prior_head, changed_paths,
             title, body, reply_kind, reply_channel, terminal_at)
        SELECT :publication, approval.id, p.deployment_id, p.lineage_id, :repair, 2,
               p.repo_full_name, 'failed', 'base_conflict_merge', :base, :head, p.changed_paths,
               p.title, p.body, p.reply_kind, p.reply_channel, now()
        FROM curie.publications p CROSS JOIN approval WHERE p.id = :source
        """,
        {
            "approval": approval_id,
            "dedupe": f"conflict-repair-{approval_id}",
            "publication": uuid.uuid4(),
            "repair": repair_id,
            "source": uuid.UUID(case["publication_id"]),
            "head": FIRST_REVISION_SHA,
            "base": TARGET,
        },
    )
    refused = _acquire_conflict_successor(case, repair_id)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "not_dispatchable"
    view = client.get(f"/v1/internal/work-items/requests/{repair_id}", headers=WORKER_HEADERS)
    assert view.is_success, view.text
    assert view.json()["terminal_cause"] is None
    assert _rows(
        "SELECT outcome FROM curie.factory_maintenance_events WHERE repair_request_id = :id",
        {"id": repair_id},
    ) == [{"outcome": "admitted"}]


@pytest.mark.parametrize(
    ("reason", "foreign_ci_head"),
    [("base_conflict", False), ("base_conflict", True), ("late_ci_red", False)],
)
def test_persisted_successor_selects_workspace_and_publication_authority(
    precheck_case: dict[str, Any],
    repair_forge: github.GitHubRepositoryFake,
    reason: MaintenanceReason,
    foreign_ci_head: bool,
) -> None:
    case = precheck_case
    client = case["client"]
    repair_id = client.portal.call(_successor, case, repair_forge, reason)
    is_conflict = reason == "base_conflict"
    wake = {
        "kind": "slack",
        "adapter": "default",
        "address": "C0EXAMPLE1",
        "conversation_id": case["reply_conversation_id"],
    }
    requests = "/v1/internal/work-items/requests"
    acquire_url = f"{requests}/{repair_id}/acquire"
    claim = {"owner": "factory", "generation": 1, "wake": wake, "context": None}
    acquire = client.post(acquire_url, headers=WORKER_HEADERS, json=claim)
    assert acquire.status_code == 200, acquire.text
    start = client.post(
        f"{requests}/{repair_id}/start",
        headers=WORKER_HEADERS,
        json={
            "owner": "factory",
            "generation": 1,
            "claim_name": "repair-claim",
            "sandbox_name": "repair-sandbox",
            "context": acquire.json()["context"],
            "delivered_limits": {
                "memory_bytes": None,
                "ephemeral_storage_bytes": None,
                "scratch_bytes": {},
                "max_turns": None,
            },
        },
    )
    assert start.status_code == 200, start.text
    running = client.get(
        "/v1/internal/work-items/running",
        headers=WORKER_HEADERS,
        params={
            "conversation_id": case["lineage"]["conversation_id"],
            **{f"wake_{key}": value for key, value in wake.items()},
        },
    )
    assert running.status_code == 200, running.text
    for response in (acquire, running):
        assert response.json()["conflict_paths"] == (["README.md"] if is_conflict else None)
        assert response.json()["conflict_base_sha"] == (TARGET if is_conflict else None)
        assert response.json()["conflict_head_sha"] == (FIRST_REVISION_SHA if is_conflict else None)

    patch = b"diff --git a/README.md b/README.md\n"
    payload = _publication_payload(
        case["deployment"]["id"],
        base_sha=TARGET,
        patch=patch,
        conversation_id=case["lineage"]["conversation_id"],
    )
    payload.update(
        work_item_request_id=str(repair_id),
        work_item_runtime_epoch=start.json()["runtime_epoch"],
        reply_conversation_id=case["reply_conversation_id"],
    )
    url = "/v1/internal/publications"
    if is_conflict:
        empty = client.post(
            url, headers=WORKER_HEADERS, json={**payload, "patch_b64": "", "changed_paths": []}
        )
        assert empty.status_code == 409, empty.text
        assert empty.json()["detail"]["code"] == "publication.conflict_patch_required"
        stale = client.post(url, headers=WORKER_HEADERS, json={**payload, "base_sha": BASE_SHA})
        assert stale.status_code == 409, stale.text
        assert stale.json()["detail"]["code"] == "publication.conflict_authority_stale"
    else:
        forged = client.post(
            url,
            headers=WORKER_HEADERS,
            json={
                **payload,
                "publication_mode": "base_conflict_merge",
                "conflict_base_sha": TARGET,
            },
        )
        assert forged.status_code == 409, forged.text
        assert forged.json()["detail"]["code"] == "publication.lineage_stale"
        payload["base_sha"] = FIRST_REVISION_SHA

    created = client.post(url, headers=WORKER_HEADERS, json=payload)
    assert created.status_code == 201, created.text
    publication = created.json()
    assert "expected_merge_sha" not in publication
    assert publication["expected_prior_head"] == FIRST_REVISION_SHA
    assert publication["base_sha"] == payload["base_sha"]
    assert publication["publication_mode"] == ("base_conflict_merge" if is_conflict else None)
    arguments = _rows(
        "SELECT granted_arguments FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(publication["approval_id"])},
    )[0]["granted_arguments"]
    assert arguments == (
        {
            "publication_mode": "base_conflict_merge",
            "expected_prior_head": FIRST_REVISION_SHA,
            "base_sha": TARGET,
            "patch_sha256": hashlib.sha256(patch).hexdigest(),
        }
        if is_conflict
        else None
    )
    replay = client.post(url, headers=WORKER_HEADERS, json=payload)
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == publication["id"]
    other = client.post(
        url,
        headers=WORKER_HEADERS,
        json={**payload, "work_item_request_id": str(case["request_id"])},
    )
    assert other.status_code == 409, other.text
    assert other.json()["detail"] == "publication replay has a different execution request"
    if is_conflict:
        _check_published_successor(
            case,
            repair_forge,
            repair_id,
            publication,
            payload,
            wake,
            foreign_ci_head=foreign_ci_head,
        )


def _check_published_successor(
    case: dict[str, Any],
    forge: github.GitHubRepositoryFake,
    repair_id: uuid.UUID,
    publication: dict[str, Any],
    payload: dict[str, Any],
    wake: dict[str, str],
    *,
    foreign_ci_head: bool,
) -> None:
    client = case["client"]
    merged = "5" * 40
    _execute(
        "UPDATE curie.publications SET status = 'launching', expected_merge_sha = :merged "
        "WHERE id = :id",
        {"id": uuid.UUID(publication["id"]), "merged": merged},
    )
    _execute(
        "UPDATE curie.publications SET status = 'succeeded', patch_bytes = NULL, "
        "terminal_at = now(), outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )
    case["truth"]["mergeable"] = True
    branch = case["lineage"]["branch"]
    for head in (merged, "6" * 40, FIRST_REVISION_SHA):
        forge.branches[branch] = head
        _execute(
            "UPDATE curie.thread_publication_lineages SET head_sha = :head WHERE id = :id",
            {"head": head, "id": case["lineage_id"]},
        )
        running = client.get(
            "/v1/internal/work-items/running",
            headers=WORKER_HEADERS,
            params={
                "conversation_id": case["lineage"]["conversation_id"],
                **{f"wake_{key}": value for key, value in wake.items()},
            },
        )
        assert running.status_code == 409, running.text
        assert running.json()["detail"]["code"] == "execution_ended"
        repeat = client.post(
            "/v1/internal/publications",
            headers=WORKER_HEADERS,
            json={**payload, "dedupe_key": f"another-{head}"},
        )
        assert repeat.status_code == 409, repeat.text
        expected_code = (
            "publication.conflict_authority_stale" if head == FIRST_REVISION_SHA else "foreign_head"
        )
        assert repeat.json()["detail"]["code"] == expected_code
    assert _rows(
        "SELECT id FROM curie.publications WHERE execution_request_id = :repair",
        {"repair": repair_id},
    ) == [{"id": uuid.UUID(publication["id"])}]

    forge.branches[branch] = merged
    _execute(
        "UPDATE curie.thread_publication_lineages SET head_sha = :head WHERE id = :id",
        {"head": merged, "id": case["lineage_id"]},
    )
    forge.report_check(FIRST_REVISION_SHA, "build", "success")
    forge.report_check(merged, "build", "pending")
    assert client.portal.call(_observe_repaired_head, case, repair_id, merged) == "waiting"
    assert _rows(
        "SELECT status FROM curie.execution_requests WHERE id = :id", {"id": repair_id}
    ) == [{"status": "running"}]
    forge.report_check(FIRST_REVISION_SHA, "build", "failure")
    observed = "7" * 40 if foreign_ci_head else merged
    if foreign_ci_head:
        forge.branches[branch] = observed
        _execute(
            "UPDATE curie.thread_publication_lineages SET head_sha = :head WHERE id = :id",
            {"head": observed, "id": case["lineage_id"]},
        )
    forge.report_check(observed, "build", "success")
    assert client.portal.call(_observe_repaired_head, case, repair_id, observed) == "settled"
    if foreign_ci_head:
        assert _rows(
            "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
            {"id": repair_id},
        ) == [{"status": "failed", "terminal_cause": "ci_unverified"}]
        return
    assert _rows(
        "SELECT r.status, m.latest_green_head_sha FROM curie.execution_requests r "
        "JOIN curie.factory_maintenance_lineages m ON m.latest_settled_request_id = r.id "
        "WHERE r.id = :id",
        {"id": repair_id},
    ) == [{"status": "completed", "latest_green_head_sha": merged}]


async def _observe_repaired_head(
    case: dict[str, Any], repair_id: uuid.UUID, merged: str
) -> factory_ci.GateResult:
    state = case["client"].app.state
    async with state.sessionmaker() as session:
        settlement = await lifecycle.claim_publication_settlement(session, exclude=frozenset())
        assert settlement and settlement.request_id == repair_id
        assert settlement.subject.head_sha == merged
        await session.rollback()

    async def unexpected_continuation(request: ExecutionRequest, round_: int, message: str) -> bool:
        pytest.fail(f"CI scheduled a continuation for request {request.id}, round {round_}")

    return await factory_ci.gate(
        state.sessionmaker,
        state.valkey,
        get_settings(),
        state.http_client,
        settlement,
        owner="observer",
        next_poll={},
        dispatch=unexpected_continuation,
        may_observe=lambda _: True,
    )
