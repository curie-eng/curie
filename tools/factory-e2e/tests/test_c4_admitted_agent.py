"""#4619: label admission must survive real factory execution and CI adoption.

Run this file on its own: it builds the checkout it is executing, provisions
an owned kind cluster on the configured Docker engine, and deletes it. Only
provider HTTP endpoints are scripted. Infrastructure exceptions occur in
fixture setup or teardown and cannot become an inverse-pin behavioral red.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402
import scripted_kind as sk  # noqa: E402
from aci_protocol import (  # noqa: E402
    RUNS_STREAM_DEFAULT,
    STREAM_ENV,
    STREAM_PAYLOAD_FIELD,
    QueuedTurn,
)
from c4_admitted_kind import (  # noqa: E402
    REPOSITORY,
    ROOT,
    AdmittedKind,
    Ticket,
    configure_harness,
    query_path,
)
from channel_protocol.work_item_events import ci_event_id, terminate_event_id  # noqa: E402


@pytest.fixture(autouse=True)
def candidate_curie() -> None:
    """Override the directory fixture that installs a fake CURIE_BIN."""


def finalize_fixture(run: AdmittedKind) -> None:
    errors = run.cleanup()
    if errors:
        for error in errors:
            run.record_failure("owned fixture cleanup", RuntimeError(error))
        known_errors = {
            "unrelated running containers disappeared during owned cleanup": (
                "unrelated_container_disappearance"
            ),
            "a pre-existing shared candidate image tag changed": "preexisting_tag_change",
            "a session-owned candidate image tag survived cleanup": (
                "owned_candidate_tag_removal_failure"
            ),
        }
        for command in ("docker ps", "docker images", "docker rm", "kind delete", "kind get"):
            known_errors[
                f"candidate infrastructure command {command} failed; "
                "redacted details retained in the private fixture diagnostics database"
            ] = "command_failure"
        known_errors[
            "candidate infrastructure command docker image failed; "
            "redacted details retained in the private fixture diagnostics database"
        ] = "image_tag_removal_command_failure"
        categories = ", ".join(sorted({known_errors.get(error, "unknown") for error in errors}))
        raise RuntimeError(
            f"owned fixture cleanup failed ({categories}); private diagnostics retained"
        )
    # A failed setup keeps its private diagnostic even when every resource was
    # successfully removed. The session fixture also preserves child records.
    for path in run.root.iterdir():
        if path.name == "diagnostics.sqlite3":
            continue
        if path.is_dir():
            if any(path.rglob("diagnostics.sqlite3")):
                continue
            shutil.rmtree(path)
        else:
            path.unlink()
    if not any(run.root.iterdir()):
        run.root.rmdir()
    if run.infrastructure_errors:
        raise RuntimeError("runtime infrastructure failed; private diagnostics retained")


@pytest.fixture(scope="session")
def source_candidate(request: pytest.FixtureRequest) -> AdmittedKind:
    root = ROOT / ".projects" / "4619" / f"candidate-{uuid.uuid4().hex[:10]}"
    root.mkdir(parents=True)
    temporary = root / "tmp"
    temporary.mkdir()
    target = root / "cargo-target"
    run = AdmittedKind(root, target / "debug/curie")
    run.env.update(TMPDIR=str(temporary), CARGO_TARGET_DIR=str(target))

    def cleanup() -> None:
        finalize_fixture(run)

    request.addfinalizer(cleanup)
    run.prerequisites()
    return run


@pytest.fixture
def kind_factory(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    source_candidate: AdmittedKind,
) -> AdmittedKind:
    root = source_candidate.root / f"case-{uuid.uuid4().hex[:10]}"
    root.mkdir()
    temporary = root / "tmp"
    temporary.mkdir()
    monkeypatch.setenv("TMPDIR", str(temporary))
    monkeypatch.setenv("CURIE_BIN", source_candidate.curie)
    run = AdmittedKind(root, Path(source_candidate.curie))
    run.source_candidate = source_candidate
    run.images = source_candidate.images
    run.tag_map = dict(zip(sk.IMAGES, run.images.values(), strict=True))
    run.env.update(TMPDIR=str(temporary))

    def cleanup() -> None:
        finalize_fixture(run)

    request.addfinalizer(cleanup)
    configure_harness(monkeypatch, run)
    run.execute()
    if run.valkey("PING") != "PONG":
        raise RuntimeError("the authenticated fixture Valkey did not answer PING")
    deployments = json.loads(run.kubectl("-n", run.namespace, "get", "deployments", "-o", "json"))
    for deployment in deployments["items"]:
        run.kubectl(
            "-n",
            run.namespace,
            "rollout",
            "status",
            "deployment/" + deployment["metadata"]["name"],
            "--timeout=300s",
        )
    # Verify the candidate rather than inferring provenance from tag names.
    pods = json.loads(run.kubectl("-n", run.namespace, "get", "pods", "-o", "json"))["items"]
    for component in ("api", "worker", "ui"):
        containers = [
            c for pod in pods for c in pod["spec"]["containers"] if c["name"] == component
        ]
        if not containers or any(c["image"] != run.images[component] for c in containers):
            raise RuntimeError(
                f"the running {component} does not use this execution's candidate image"
            )
    run.deadline = None
    run.fixture_ready = True
    return run


def observe_until(predicate: Callable[[], Any], *, seconds: int = 180) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(1)
    return None


def execution_observed(run: AdmittedKind, ticket: Ticket) -> dict[str, Any] | None:
    row = run.request(ticket)
    if row and (row["started_at"] is not None or row["last_deferral_reason"] is not None):
        return row
    return None


def assert_started(run: AdmittedKind, ticket: Ticket) -> dict[str, Any]:
    row = observe_until(lambda: execution_observed(run, ticket))
    assert row is not None, f"{ticket.kind}: admitted request produced no execution disposition"
    ambiguous_execute_drop = False
    if row["started_at"] is None:
        event_prefix = f"work-item-{row['id']}-execute-"
        ambiguous_execute_drop = any(
            event_prefix in line
            and "agents are bound to" in line
            and "names no adapter to choose one" in line
            for line in run.worker_logs().splitlines()
        )
    assert row["started_at"] is not None, (
        f"{ticket.kind}: labelled request did not execute as its admitted agent; "
        f"reason={row['last_deferral_reason']}; "
        f"ambiguous_execute_drop={ambiguous_execute_drop}"
    )
    assert row["agent_id"] == ticket.agent.id
    assert row["execution_attempts"] == 1
    assert row["runtime_epoch"] >= 1
    return row


def assert_published(run: AdmittedKind, row: dict[str, Any], count: int) -> list[dict[str, Any]]:
    def published() -> Any:
        rows = run.publication_rows(row)
        return (
            rows
            if len(rows) >= count
            and all(p["head_sha"] and p["status"] == "succeeded" for p in rows)
            else None
        )

    rows = observe_until(published, seconds=300)
    if rows is None:
        run.record_publication_failure(row, count)
        raise AssertionError(
            f"{row['reply_kind']}: the actual runner did not publish its bundle marker revision "
            f"(expected {count}); private diagnostics retained"
        )
    assert len(rows) <= 2
    return rows


def assert_marker(run: AdmittedKind, ticket: Ticket, round_: int) -> None:
    observations = observe_until(
        lambda: [
            marker
            for lane, round_seen, marker in run.model.observations()
            if ticket.url in lane and round_seen == round_
        ]
    )
    if observations == [ticket.agent.marker]:
        return
    run.record_marker_failure(ticket, round_)
    if not observations:
        raise AssertionError(
            f"{ticket.kind}: round {round_} has no marker observation "
            "correlated to the tracker URL; "
            "private diagnostics retained"
        )
    if any(marker != ticket.agent.marker for marker in observations):
        raise AssertionError(
            f"{ticket.kind}: round {round_} read a different agent's actual bundle marker; "
            "private diagnostics retained"
        )
    raise AssertionError(
        f"{ticket.kind}: round {round_} has multiple marker observations for the tracker URL; "
        "private diagnostics retained"
    )


def assert_status_on_original_tracker(run: AdmittedKind, ticket: Ticket, request_id: str) -> None:
    notice = run.sql(
        "SELECT row_to_json(n) FROM curie.factory_terminal_notices n "
        f"WHERE n.execution_request_id='{uuid.UUID(request_id)}'"
    )
    assert notice["posted_at"] is not None
    assert notice["comment_id"] is not None
    assert notice["refusal"] is None
    assert notice["attempts"] >= 1
    if ticket.kind == "github":
        comments = [c for c in run.stub._rows("comment") if c["id"] == notice["comment_id"]]
        assert len(comments) == 1
        assert comments[0]["issue_number"] == ticket.number
    elif ticket.kind == "gitlab":
        comments = run.stub.gitlab.issue_notes(ticket.number)
        assert any(c.id == notice["comment_id"] for c in comments)
    else:
        comments = run.stub.jira.own_comments(ticket.number)
        assert any(int(c.id) == notice["comment_id"] for c in comments)


def assert_run_persisted(run: AdmittedKind, ticket: Ticket, request_id: str) -> None:
    row = run.request(ticket)
    assert row is not None and row["id"] == request_id
    assert row["agent_id"] == ticket.agent.id
    assert row["runtime_sandbox_name"] is not None
    session = f"agent-{ticket.agent.id}-thread-{row['conversation_id']}"
    listed: list[dict[str, Any]] = []
    details: dict[str, Any] = {}

    def runner_ids(nodes: list[dict[str, Any]]) -> set[str]:
        identifiers: set[str] = set()
        for node in nodes:
            if node["name"] == "agent.run":
                identifiers.add(node["id"])
            identifiers.update(runner_ids(node["children"]))
        return identifiers

    def trace_for_request() -> Any:
        nonlocal listed
        traces = run.api(
            "GET", query_path("/langfuse/traces", agent_id=ticket.agent.id, limit="100")
        )
        listed = traces
        for trace in traces:
            if trace.get("sessionId") != session:
                continue
            status, candidate = run.api_result("GET", f"/langfuse/traces/{trace['id']}")
            details[trace["id"]] = {"http_status": status, "body": candidate}
            # The Runs API names this 404 while trace/observation ingest catches up.
            if status == 404 and candidate.get("detail") == "trace has no observations yet":
                continue
            if status != 200:
                failure = RuntimeError(f"Runs trace lookup returned HTTP {status}")
                run.record_failure("Runs trace lookup", failure)
                raise failure
            if (
                candidate["trace"].get("sessionId") != session
                or candidate["trace"].get("name") != f"curie-run:{session}"
            ):
                continue
            visible_ids = runner_ids(candidate["tree"])
            if not visible_ids:
                continue
            observations = run.read_trace_observations([trace["id"]])[trace["id"]]
            details[trace["id"]]["upstream_observations"] = observations
            for observation in observations:
                if (
                    observation.get("id") not in visible_ids
                    or observation.get("name") != "agent.run"
                ):
                    continue
                # #4619 runtime diagnostics place the runner's actual sandbox
                # at metadata.attributes on this exact visible observation id.
                metadata = observation.get("metadata")
                attributes = metadata.get("attributes") if isinstance(metadata, dict) else None
                if (
                    isinstance(attributes, dict)
                    and attributes.get("curie.sandbox_id") == row["runtime_sandbox_name"]
                ):
                    return candidate, observation
        return None

    observed = observe_until(trace_for_request)
    if observed is None:
        run.record_runs_failure(row, session, listed, details)
        raise AssertionError(
            "Runs has no persisted runner trace matching the agent, request session, "
            "and runtime sandbox; private diagnostics retained"
        )
    tree, observation = observed
    assert tree["trace"]["sessionId"] == session
    assert tree["trace"]["name"] == f"curie-run:{session}"
    assert observation["id"] in runner_ids(tree["tree"])
    assert observation["name"] == "agent.run"
    assert observation["metadata"]["attributes"]["curie.sandbox_id"] == row["runtime_sandbox_name"]


def complete_ticket(run: AdmittedKind, ticket: Ticket) -> dict[str, Any]:
    admitted = run.await_admission(ticket)
    assert admitted["agent_id"] == ticket.agent.id
    assert admitted["trigger_label"] == ticket.agent.label
    row = assert_started(run, ticket)
    first = assert_published(run, row, 1)
    assert first[0]["deployment_id"] == ticket.agent.deployment_id
    assert first[0]["agent_id"] == ticket.agent.id
    assert_marker(run, ticket, 1)
    run.mark_ci(row, green=False)
    publications = assert_published(run, row, 2)
    assert_marker(run, ticket, 2)
    assert {p["deployment_id"] for p in publications} == {ticket.agent.deployment_id}
    assert {p["agent_id"] for p in publications} == {ticket.agent.id}
    assert {p["execution_request_id"] for p in publications} == {row["id"]}
    assert len({p["pr_number"] for p in publications}) == 1
    assert len({p["branch"] for p in publications}) == 1
    run.mark_ci(row, green=True)
    final = observe_until(
        lambda: r if (r := run.request(ticket)) and r["status"] == "completed" else None
    )
    assert final is not None, "green CI did not settle the original execution request"
    assert final["id"] == row["id"]
    assert final["runtime_epoch"] == row["runtime_epoch"]
    assert final["execution_attempts"] == 1
    assert observe_until(lambda: run.comments(ticket)), "original tracker has no status comment"
    assert_status_on_original_tracker(run, ticket, row["id"])
    assert_run_persisted(run, ticket, row["id"])
    return final


def test_labelled_agents_execute_as_admitted_agent(kind_factory: AdmittedKind) -> None:
    run = kind_factory
    assert len({a.bundle_ref for a in run.agents}) == 2
    bindings = run.sql(
        "SELECT json_agg(json_build_object("
        "'kind', kind, 'adapter', adapter, 'label', trigger_label)) "
        "FROM curie.agent_channels WHERE kind IN ('github','gitlab','jira')"
    )
    assert len(bindings) == 6
    assert all(b["adapter"] is None for b in bindings)
    for kind in ("github", "gitlab", "jira"):
        tickets = [run.seed(kind, agent) for agent in run.agents]
        # Both labels are present before either completion; this is not a
        # sequential single-binding scenario disguised as multi-agent proof.
        for ticket in tickets:
            assert run.await_admission(ticket)["agent_id"] == ticket.agent.id
        for ticket in tickets:
            complete_ticket(run, ticket)


@pytest.fixture
def paused_factory(kind_factory: AdmittedKind) -> AdmittedKind:
    kind_factory.worker_replicas(0)
    return kind_factory


def waiting_ticket(
    run: AdmittedKind, kind: str = "github"
) -> tuple[Ticket, dict[str, Any], dict[str, Any]]:
    ticket = run.seed(kind, run.agents[0])
    row = run.await_admission(ticket)
    wake = run.take_execute_wake(row)
    current = run.request(ticket)
    assert current is not None
    return ticket, current, wake


def progress_neighbor(run: AdmittedKind) -> None:
    complete_ticket(run, run.seed("gitlab", run.agents[1]))


def request_state(run: AdmittedKind, request_id: str) -> dict[str, Any]:
    return run.sql(
        "SELECT row_to_json(r) FROM curie.execution_requests r "
        f"WHERE id='{uuid.UUID(request_id)}'"
    )


def no_execution_effects(run: AdmittedKind, ticket: Ticket, row: dict[str, Any]) -> None:
    current = request_state(run, row["id"])
    assert current["started_at"] is None
    assert current["execution_attempts"] == 0
    assert current["runtime_epoch"] == 0
    assert current["runtime_owner"] is None
    assert current["runtime_claim_name"] is None
    assert current["runtime_sandbox_name"] is None
    assert run.publication_rows(row) == []
    assert not [lane for lane, _, _ in run.model.observations() if ticket.url in lane]
    assert run.valkey("GET", "curie:sandbox:route:" + row["conversation_id"]) is None
    assert run.sql(
        "SELECT count(*) FROM curie.approvals "
        f"WHERE dedupe_key LIKE '%{uuid.UUID(row['id'])}%'"
    ) == 0


def factory_deferral(
    run: AdmittedKind, ticket: Ticket, before: dict[str, Any], reason: str
) -> dict[str, Any]:
    row = observe_until(
        lambda: r if (r := run.request(ticket)) and r["last_deferral_reason"] else None
    )
    assert row is not None, "request has no correlated factory authority refusal"
    assert row["last_deferral_reason"] == "not_started:" + reason
    assert row["status"] == "waiting"
    assert row["start_deferrals"] == before["start_deferrals"] + 1
    assert row["capacity_deferrals"] == before["capacity_deferrals"]
    assert row["dispatch_generation"] == before["dispatch_generation"] + 1
    assert row["acquire_owner"] is None
    assert row["acquired_generation"] is None
    assert row["acquire_expires_at"] is None
    assert row["terminal_cause"] is None
    # Keep the observed first deferral stable while the independent neighbor
    # exercises the real worker and runner, including its red/green CI cycle.
    run.write_sql(
        "UPDATE curie.execution_requests SET dispatch_not_before=now()+interval '1 hour', "
        "published_generation=dispatch_generation "
        f"WHERE id='{uuid.UUID(row['id'])}'"
    )
    no_execution_effects(run, ticket, row)
    return row


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("agent_tenant", "factory_tenant_mismatch"),
        ("binding_tenant", "factory_tenant_mismatch"),
        ("request_route", "factory_surface_mismatch"),
        ("request_conversation", "factory_surface_mismatch"),
        ("tracker_surface", "factory_surface_mismatch"),
        ("removed_binding", "factory_binding_missing"),
        ("moved_binding", "factory_binding_missing"),
        ("binding_owner", "factory_binding_missing"),
        ("deployment_missing", "deployment_missing"),
    ],
)
def test_execute_revalidates_current_authority(
    paused_factory: AdmittedKind, change: str, reason: str
) -> None:
    run = paused_factory
    ticket, before, wake = waiting_ticket(run)
    agent_id = uuid.UUID(ticket.agent.id)
    binding = f"agent_id='{agent_id}' AND kind='github'"
    if change in {"agent_tenant", "binding_tenant"}:
        tenant = uuid.uuid4()
        table = "agents" if change == "agent_tenant" else "agent_channels"
        where = f"id='{agent_id}'" if change == "agent_tenant" else binding
        run.write_sql(
            "INSERT INTO curie.tenants (id,deployment_id,status) "
            f"VALUES ('{tenant}','acme-other','active'); "
            f"UPDATE curie.{table} SET tenant_id='{tenant}' WHERE {where}"
        )
    elif change == "request_route":
        run.write_sql(
            "UPDATE curie.execution_requests SET reply_address='acme-corp/other-bot' "
            f"WHERE id='{uuid.UUID(before['id'])}'"
        )
        wake["reply_handle"]["channel"] = "acme-corp/other-bot"
    elif change == "request_conversation":
        run.write_sql(
            "UPDATE curie.execution_requests SET reply_conversation_id='acme-other-thread' "
            f"WHERE id='{uuid.UUID(before['id'])}'"
        )
        wake["conversation_id"] = "acme-other-thread"
    elif change == "tracker_surface":
        run.write_sql(
            "UPDATE curie.work_items SET tracker_scope_id='999999' "
            f"WHERE id='{uuid.UUID(before['work_item_id'])}'"
        )
    elif change == "removed_binding":
        run.api(
            "DELETE", query_path(f"/agents/{agent_id}/channels", kind="github", address=REPOSITORY),
            expected=204,
        )
    elif change == "moved_binding":
        run.api(
            "PATCH", query_path(f"/agents/{agent_id}/channels", kind="github", address=REPOSITORY),
            {"kind": "github", "address": "acme-corp/other-bot"},
        )
    elif change == "binding_owner":
        run.write_sql(
            f"UPDATE curie.agent_channels SET agent_id='{uuid.UUID(run.agents[1].id)}' "
            f"WHERE {binding}"
        )
    else:
        run.write_sql(f"UPDATE curie.deployments SET status='inactive' WHERE agent_id='{agent_id}'")
        assert run.sql(
            "SELECT count(*) FROM curie.deployments "
            f"WHERE agent_id='{uuid.UUID(run.agents[1].id)}' AND status='active'"
        ) == 1
    comments = run.comments(ticket)
    run.enqueue(wake)
    run.worker_replicas(1)
    factory_deferral(run, ticket, before, reason)
    frozen = request_state(run, before["id"])
    progress_neighbor(run)
    assert request_state(run, before["id"]) == frozen
    no_execution_effects(run, ticket, before)
    assert run.comments(ticket) == comments


@pytest.mark.parametrize("change", ["request", "conversation", "kind", "address", "adapter"])
def test_foreign_wake_claims_are_read_only(paused_factory: AdmittedKind, change: str) -> None:
    run = paused_factory
    ticket, row, wake = waiting_ticket(run)
    protected = [(ticket, row)]
    if change == "request":
        other = run.seed("github", run.agents[1])
        other_row = run.await_admission(other)
        run.take_execute_wake(other_row)
        wake["event_id"] = f"work-item-{other_row['id']}-execute-{other_row['dispatch_generation']}"
        current = run.request(other)
        assert current is not None
        protected.append((other, current))
    elif change == "conversation":
        wake["conversation_id"] = "acme-foreign-thread"
    elif change == "kind":
        wake["reply_handle"]["kind"] = "gitlab"
    elif change == "address":
        wake["reply_handle"]["channel"] = "acme-corp/other-bot"
    else:
        wake["reply_handle"].update(adapter="acme-other", endpoint=run.stub.base_url + "/reply")
    snapshots = {r["id"]: request_state(run, r["id"]) for _, r in protected}
    comments = {r["id"]: run.comments(t) for t, r in protected}
    run.enqueue(wake)
    run.worker_replicas(1)
    assert observe_until(
        lambda: any(
            wake["event_id"] in line and "factory_wake_mismatch" in line
            for line in run.worker_logs().splitlines()
        )
    ), "the delivered event lacks its named wake mismatch refusal"
    progress_neighbor(run)
    for protected_ticket, request in protected:
        assert request_state(run, request["id"]) == snapshots[request["id"]]
        no_execution_effects(run, protected_ticket, request)
        assert run.comments(protected_ticket) == comments[request["id"]]


@pytest.mark.parametrize(
    ("control", "reason"),
    [("stale_generation", "not_published"), ("foreign_owner", "duplicate")],
)
def test_existing_acquire_fences_remain_read_only(
    paused_factory: AdmittedKind, control: str, reason: str
) -> None:
    """Existing generation/owner controls, not evidence of the new authority guard."""
    run = paused_factory
    ticket, row, wake = waiting_ticket(run)
    if control == "stale_generation":
        run.write_sql(
            "UPDATE curie.execution_requests SET dispatch_generation=dispatch_generation+1, "
            "published_generation=dispatch_generation+1 "
            f"WHERE id='{uuid.UUID(row['id'])}'"
        )
    else:
        run.api(
            "POST", f"/v1/internal/work-items/requests/{row['id']}/acquire",
            acquire_body(row, wake, owner="acme-other-worker"), internal=True,
        )
        run.write_sql(
            "UPDATE curie.execution_requests SET acquire_expires_at=now()+interval '1 hour' "
            f"WHERE id='{uuid.UUID(row['id'])}'"
        )
    frozen = request_state(run, row["id"])
    comments = run.comments(ticket)
    run.enqueue(wake)
    run.worker_replicas(1)
    assert observe_until(
        lambda: any(
            wake["event_id"] in line and line.rstrip().endswith(": " + reason)
            for line in run.worker_logs().splitlines()
        )
    ), f"existing {control} fence did not name its refusal for this event"
    progress_neighbor(run)
    assert request_state(run, row["id"]) == frozen
    no_execution_effects(run, ticket, row)
    assert run.comments(ticket) == comments


def test_duplicate_effective_label_is_refused_at_admission(paused_factory: AdmittedKind) -> None:
    run = paused_factory
    run.write_sql(
        "UPDATE curie.agent_channels SET trigger_label=NULL "
        f"WHERE agent_id='{uuid.UUID(run.agents[0].id)}' AND kind='github'; "
        "UPDATE curie.agent_channels SET trigger_label='factory' "
        f"WHERE agent_id='{uuid.UUID(run.agents[1].id)}' AND kind='github'"
    )
    ticket = run.seed("github", run.agents[0], label="factory")
    result = run.label_webhook(ticket, "factory")
    assert result["status"] == "factory_ignored"
    assert result["errors"] == [{"code": "binding_missing"}]
    run.worker_replicas(1)
    progress_neighbor(run)
    assert run.request(ticket) is None
    assert run.sql(
        "SELECT count(*) FROM curie.work_items "
        f"WHERE tracker_kind='github' AND tracker_issue_id='{ticket.number}'"
    ) == 0
    assert run.comments(ticket) == []
    assert not [lane for lane, _, _ in run.model.observations() if ticket.url in lane]


def test_ordinary_ambiguous_chat_still_has_no_agent_or_reply(paused_factory: AdmittedKind) -> None:
    run = paused_factory
    ticket, row, wake = waiting_ticket(run)
    wake.update(event_id="acme-chat-" + uuid.uuid4().hex, source="slack")
    frozen = request_state(run, row["id"])
    comments = run.comments(ticket)
    run.enqueue(wake)
    run.worker_replicas(1)
    assert observe_until(
        lambda: any(
            wake["event_id"] in line
            and "agents are bound to" in line
            and "names no adapter to choose one" in line
            for line in run.worker_logs().splitlines()
        )
    ), "ordinary chat no longer names its ambiguous-route refusal"
    progress_neighbor(run)
    assert request_state(run, row["id"]) == frozen
    no_execution_effects(run, ticket, row)
    assert run.comments(ticket) == comments


def adapter_bindings(run: AdmittedKind, *, distinct: bool) -> None:
    for index, agent in enumerate(run.agents):
        run.api(
            "PATCH", query_path(f"/agents/{agent.id}/channels", kind="github", address=REPOSITORY),
            {
                "kind": "github", "address": REPOSITORY,
                "endpoint": run.stub.base_url + "/reply",
                "adapter": f"acme-tracker-{index}" if distinct else "acme-tracker",
            },
        )


def test_adapter_bearing_shared_location_keeps_the_admitted_agent(
    paused_factory: AdmittedKind,
) -> None:
    run = paused_factory
    adapter_bindings(run, distinct=False)
    # Make the sibling the ordinary resolver's first row; both still deploy.
    run.write_sql(
        "UPDATE curie.deployments SET deployed_at=now()+interval '1 day' "
        f"WHERE id='{uuid.UUID(run.agents[1].deployment_id)}'"
    )
    ticket, _, wake = waiting_ticket(run)
    assert wake["reply_handle"]["adapter"] == "acme-tracker"
    run.enqueue(wake)
    run.worker_replicas(1)
    complete_ticket(run, ticket)


def test_acquired_killswitch_uses_existing_interrupted_deferral(
    paused_factory: AdmittedKind,
) -> None:
    run = paused_factory
    ticket, before, wake = waiting_ticket(run)
    assert run.api("POST", f"/agents/{ticket.agent.id}/kill")["killed"] is True
    run.enqueue(wake)
    run.worker_replicas(1)
    factory_deferral(run, ticket, before, "interrupted")
    frozen = request_state(run, before["id"])
    progress_neighbor(run)
    assert request_state(run, before["id"]) == frozen
    no_execution_effects(run, ticket, before)
    assert run.api("GET", f"/agents/{ticket.agent.id}/kill")["killed"] is True


def acquire_body(
    row: dict[str, Any],
    wake: dict[str, Any],
    *,
    owner: str = "acme-test-worker",
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    handle = wake["reply_handle"]
    return {
        "owner": owner,
        "generation": row["dispatch_generation"],
        "wake": {
            "kind": handle["kind"],
            "address": handle["channel"],
            "adapter": handle.get("adapter"),
            "conversation_id": wake["conversation_id"],
        },
        "context": context,
    }


def notice_state(run: AdmittedKind, request_id: str) -> dict[str, Any]:
    return run.sql(
        "SELECT row_to_json(n) FROM curie.factory_terminal_notices n "
        f"WHERE n.execution_request_id='{uuid.UUID(request_id)}'"
    )


def event_done(run: AdmittedKind, event_id: str) -> bool:
    return run.valkey("GET", "curie:worker:done:" + event_id) is not None


def parked_ticket(
    run: AdmittedKind,
) -> tuple[Ticket, dict[str, Any], dict[str, Any]]:
    ticket, _, wake = waiting_ticket(run)
    run.enqueue(wake)
    run.worker_replicas(1)
    started = assert_started(run, ticket)
    publications = assert_published(run, started, 1)
    assert len(publications) == 1
    assert publications[0]["deployment_id"] == ticket.agent.deployment_id
    assert publications[0]["agent_id"] == ticket.agent.id
    assert_marker(run, ticket, 1)

    def parked() -> Any:
        current = run.request(ticket)
        if (
            current
            and current["status"] == "running"
            and current["execution_deadline"] is not None
            and current["runtime_heartbeat_expires_at"] == current["execution_deadline"]
            and event_done(run, wake["event_id"])
        ):
            return current
        return None

    row = observe_until(parked)
    assert row is not None, "the executed request did not park with its original deadline"
    assert row["id"] == started["id"]
    assert row["runtime_epoch"] == started["runtime_epoch"]
    assert row["runtime_claim_name"] is not None
    assert row["runtime_sandbox_name"] is not None
    return ticket, row, wake


def assert_execution_identity(before: dict[str, Any], after: dict[str, Any]) -> None:
    for key in (
        "id", "work_item_id", "agent_id", "conversation_id", "dispatch_generation",
        "wait_deadline", "started_at", "execution_deadline", "runtime_epoch",
        "execution_attempts", "admitted_repository_path", "reply_kind", "reply_address",
        "reply_conversation_id",
    ):
        assert after[key] == before[key], f"continuation changed execution identity: {key}"
    assert after["execution_attempts"] == 1
    assert after["admitted_repository_path"] == REPOSITORY


def assert_running_snapshot(
    run: AdmittedKind,
    row: dict[str, Any],
    wake: dict[str, Any],
    *,
    refusal: str | None = None,
) -> None:
    handle = wake["reply_handle"]
    status, snapshot = run.api_result(
        "GET", query_path(
            "/v1/internal/work-items/running", conversation_id=row["conversation_id"],
            wake_kind=handle["kind"], wake_address=handle["channel"],
            wake_conversation_id=wake["conversation_id"], wake_adapter=handle.get("adapter") or "",
        ), internal=True,
    )
    assert status == 200
    assert snapshot["request_id"] == row["id"]
    assert snapshot["work_item_id"] == row["work_item_id"]
    assert snapshot["agent_id"] == row["agent_id"]
    assert snapshot["generation"] == row["dispatch_generation"]
    assert snapshot["conversation_id"] == row["conversation_id"]
    assert snapshot["repository_path"] == row["admitted_repository_path"]
    assert snapshot["runtime_epoch"] == row["runtime_epoch"]
    for field in ("wait_deadline", "execution_deadline"):
        assert datetime.fromisoformat(snapshot[field]) == datetime.fromisoformat(row[field])
    assert snapshot["refusal"] == refusal
    if refusal is None:
        assert snapshot["context"]["agent_id"] == row["agent_id"]
    else:
        assert snapshot["context"] is None, "a refused running lookup returned runnable authority"


def complete_continuation(
    run: AdmittedKind, ticket: Ticket, before: dict[str, Any]
) -> dict[str, Any]:
    run.mark_ci(before, green=False)
    publications = assert_published(run, before, 2)
    assert_marker(run, ticket, 2)
    assert {p["agent_id"] for p in publications} == {ticket.agent.id}
    assert {p["deployment_id"] for p in publications} == {ticket.agent.deployment_id}
    assert {p["execution_request_id"] for p in publications} == {before["id"]}
    assert len({p["pr_number"] for p in publications}) == 1
    assert len({p["branch"] for p in publications}) == 1
    run.mark_ci(before, green=True)
    final = observe_until(
        lambda: r if (r := run.request(ticket)) and r["status"] == "completed" else None
    )
    assert final is not None, "the real CI continuation did not complete its original request"
    assert_execution_identity(before, final)
    assert observe_until(
        lambda: n if (n := notice_state(run, before["id"])) and n["finalized_at"] else None
    ), "the resumed request has no finalized tracker status"
    assert_status_on_original_tracker(run, ticket, before["id"])
    return final


def remint_and_change_default_label(run: AdmittedKind, ticket: Ticket) -> None:
    def binding() -> Any:
        return run.sql(
            "SELECT json_build_object('id', id, 'generation', generation, "
            "'adapter', adapter, 'label', trigger_label) FROM curie.agent_channels "
            f"WHERE agent_id='{uuid.UUID(ticket.agent.id)}' AND kind='github'"
        )

    before = binding()
    for _ in range(2):
        token = run.api(
            "POST", "/channels/token",
            {"kind": "github", "address": REPOSITORY, "adapter": before["adapter"]},
        )["token"]
        run.secrets.append(token)
    after = binding()
    assert after["id"] == before["id"]
    assert after["generation"] == before["generation"] + 2
    assert after["label"] == ticket.agent.label

    def change(configuration: dict[str, Any]) -> None:
        tracker = next(t for t in configuration["trackers"] if t["kind"] == "github")
        tracker["label"] = "acme-new-default"

    run.change_forges(change)


@pytest.mark.parametrize("phase", ["before_execute", "held", "restarted"])
def test_token_remint_and_default_label_changes_preserve_authority(
    paused_factory: AdmittedKind, phase: str
) -> None:
    run = paused_factory
    adapter_bindings(run, distinct=True)
    if phase == "before_execute":
        ticket, _, wake = waiting_ticket(run)
        remint_and_change_default_label(run, ticket)
        run.enqueue(wake)
        run.worker_replicas(1)
        complete_ticket(run, ticket)
        return
    ticket, before, wake = parked_ticket(run)
    worker_before = run.worker_identity()
    assert worker_before, "the parked request has no observable worker process"
    if phase == "restarted":
        run.worker_replicas(0)
    remint_and_change_default_label(run, ticket)
    if phase == "restarted":
        run.worker_replicas(1)
        assert run.worker_identity() != worker_before
    else:
        assert run.worker_identity() == worker_before, "the held worker restarted before adoption"
    assert_running_snapshot(run, before, wake)
    complete_continuation(run, ticket, before)
    if phase == "held":
        assert run.worker_identity() == worker_before, "cold adoption replaced the held-path proof"


def invalidate_continuation_authority(
    run: AdmittedKind, ticket: Ticket, row: dict[str, Any], change: str
) -> None:
    if change == "removed_binding":
        run.api(
            "DELETE", query_path(
                f"/agents/{ticket.agent.id}/channels", kind="github", address=REPOSITORY
            ), expected=204,
        )
    elif change == "binding_tenant":
        tenant = uuid.uuid4()
        run.write_sql(
            "INSERT INTO curie.tenants (id,deployment_id,status) "
            f"VALUES ('{tenant}','acme-other','active'); "
            f"UPDATE curie.agent_channels SET tenant_id='{tenant}' "
            f"WHERE agent_id='{uuid.UUID(ticket.agent.id)}' AND kind='github'"
        )
    else:
        assert change == "tracker_surface"
        run.write_sql(
            "UPDATE curie.work_items SET tracker_scope_id='999999' "
            f"WHERE id='{uuid.UUID(row['work_item_id'])}'"
        )


def ci_delivery(row: dict[str, Any], wake: dict[str, Any]) -> dict[str, Any]:
    delivery = copy.deepcopy(wake)
    delivery["event_id"] = f"work-item-{row['id']}-ci-2"
    delivery["text"] += "\nCurie wait_ci round 2 of 3: recheck the candidate change."
    return delivery


@pytest.mark.parametrize("continuation", ["held", "restarted"])
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("removed_binding", "factory_binding_missing"),
        ("binding_tenant", "factory_tenant_mismatch"),
        ("tracker_surface", "factory_surface_mismatch"),
    ],
)
def test_continuation_revalidates_current_authority(
    paused_factory: AdmittedKind, continuation: str, change: str, reason: str
) -> None:
    run = paused_factory
    ticket, before, wake = parked_ticket(run)
    worker_before = run.worker_identity()
    assert worker_before
    if continuation == "restarted":
        run.worker_replicas(0)
    publications = run.publication_rows(before)
    observations = [o for o in run.model.observations() if ticket.url in o[0]]
    foreign = ci_delivery({"id": str(uuid.uuid4())}, wake)
    frozen = request_state(run, before["id"])
    run.enqueue(foreign)
    if continuation == "restarted":
        run.worker_replicas(1)
    assert observe_until(lambda: event_done(run, foreign["event_id"]))
    assert any(
        foreign["event_id"] in line and "is stale: running request is" in line
        for line in run.worker_logs().splitlines()
    ), "the CI delivery did not name its request-identity mismatch"
    assert request_state(run, before["id"]) == frozen
    assert run.publication_rows(before) == publications
    assert [o for o in run.model.observations() if ticket.url in o[0]] == observations
    if continuation == "restarted":
        run.worker_replicas(0)
    invalidate_continuation_authority(run, ticket, before, change)
    assert_running_snapshot(run, before, wake, refusal=reason)
    # Deliver a CI wake at the production queue boundary, with the original
    # authenticated route claims. No running request or claim is fabricated.
    wake = ci_delivery(before, wake)
    run.enqueue(wake)
    if continuation == "restarted":
        run.worker_replicas(1)
        assert run.worker_identity() != worker_before
    assert observe_until(lambda: event_done(run, wake["event_id"])), (
        "the refused continuation did not finish its delivery"
    )
    assert any(
        wake["event_id"] in line and reason in line for line in run.worker_logs().splitlines()
    ), "the continuation lacks its named current-authority refusal"
    final = run.request(ticket)
    assert final is not None
    assert final["status"] == "failed"
    assert final["terminal_cause"] == "runner_escalated"
    assert final["last_deferral_reason"] is None
    assert_execution_identity(before, final)
    assert run.publication_rows(before) == publications
    assert [o for o in run.model.observations() if ticket.url in o[0]] == observations
    frozen = request_state(run, before["id"])
    progress_neighbor(run)
    assert request_state(run, before["id"]) == frozen
    assert run.publication_rows(before) == publications
    assert [o for o in run.model.observations() if ticket.url in o[0]] == observations
    if continuation == "held":
        assert run.worker_identity() == worker_before, "held revalidation ran in a new process"


def worker_lane_identity(
    run: AdmittedKind, component: str
) -> list[tuple[str, str, str, int]]:
    pods = json.loads(run.kubectl(
        "-n", run.namespace, "get", "pods", "-o", "json",
        "-l", "app.kubernetes.io/component=" + component,
    ))["items"]
    return sorted(
        (
            pod["metadata"]["name"], pod["metadata"]["uid"],
            c["containerID"], c["restartCount"],
        )
        for pod in pods
        for c in pod["status"].get("containerStatuses", [])
        if c["name"] == "worker"
    )


@pytest.fixture
def two_worker_ci(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    paused_factory: AdmittedKind,
) -> tuple[AdmittedKind, str, str]:
    run = paused_factory
    owner_stream = RUNS_STREAM_DEFAULT + ":c4-owner"
    continuation_stream = RUNS_STREAM_DEFAULT + ":c4-holder"
    deployment_name = "c4-held-worker"
    component = "c4-held-worker"

    def cleanup() -> None:
        run.kubectl(
            "-n", run.namespace, "delete", "deployment", deployment_name,
            "--ignore-not-found", "--wait=true",
        )
        run.kubectl(
            "-n", run.namespace, "wait", "--for=delete", "pod",
            "-l", "app.kubernetes.io/component=" + component, "--timeout=180s",
        )
        remaining = run.kubectl(
            "-n", run.namespace, "get", "deployment", deployment_name,
            "--ignore-not-found", "-o", "name",
        ).strip()
        pods = json.loads(run.kubectl(
            "-n", run.namespace, "get", "pods", "-o", "json",
            "-l", "app.kubernetes.io/component=" + component,
        ))["items"]
        if remaining or pods:
            failure = RuntimeError("the additional owned worker survived cleanup")
            run.record_failure("two-worker CI fixture cleanup", failure)
            raise failure

    request.addfinalizer(cleanup)
    # CURIE_STREAM is the worker's production input. Separate real streams
    # let the test schedule unchanged producer wakes without stopping either
    # process or changing either kernel's internal state.
    run.kubectl(
        "-n", run.namespace, "set", "env", "deployment/curie-worker",
        f"{STREAM_ENV}={owner_stream}",
    )
    installed = json.loads(run.kubectl(
        "-n", run.namespace, "get", "deployment/curie-worker", "-o", "json",
    ))
    spec = copy.deepcopy(installed["spec"])
    spec["replicas"] = 1
    spec["selector"]["matchLabels"]["app.kubernetes.io/component"] = component
    spec["template"]["metadata"]["labels"]["app.kubernetes.io/component"] = component
    worker = next(c for c in spec["template"]["spec"]["containers"] if c["name"] == "worker")
    worker["env"] = [e for e in worker["env"] if e["name"] != STREAM_ENV]
    worker["env"].append({"name": STREAM_ENV, "value": continuation_stream})
    manifest = {
        "apiVersion": installed["apiVersion"],
        "kind": installed["kind"],
        "metadata": {"name": deployment_name, "namespace": run.namespace},
        "spec": spec,
    }
    path = run.root / "held-worker.json"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(manifest, stream)
    run.kubectl("-n", run.namespace, "apply", "-f", str(path))
    run.worker_replicas(1)
    run.kubectl(
        "-n", run.namespace, "rollout", "status", "deployment/" + deployment_name,
        "--timeout=300s",
    )
    for label in ("worker", component):
        if len(worker_lane_identity(run, label)) != 1:
            raise RuntimeError("the independent worker lane has no unique running process")

    scripted_reply = run.model._match

    def model_reply(method: str, path: str, body: bytes) -> Any:
        reply = scripted_reply(method, path, body)
        if reply is not None:
            for block in reply["message"]["content"]:
                if block.get("type") == "tool_use" and block.get("name") == "Bash":
                    # A replacement sandbox makes a real new diff even when
                    # the same agent reads its unchanged bundle marker again.
                    block["input"]["command"] += (
                        ' && printf "%s" "$HOSTNAME" >> c4-runtime-marker.txt'
                    )
        return reply

    monkeypatch.setattr(run.model, "_match", model_reply)
    return run, owner_stream, continuation_stream


def forward_worker_wake(run: AdmittedKind, stream: str, wake: dict[str, Any]) -> None:
    turn = QueuedTurn.model_validate(wake)
    run.valkey("XADD", stream, "*", STREAM_PAYLOAD_FIELD, turn.model_dump_json())


def produced_wake(run: AdmittedKind, event_id: str) -> dict[str, Any]:
    def take() -> Any:
        for stream_id, fields in run.valkey("XRANGE", RUNS_STREAM_DEFAULT, "-", "+"):
            payload = json.loads(dict(zip(fields[::2], fields[1::2], strict=True))[
                STREAM_PAYLOAD_FIELD
            ])
            if payload["event_id"] == event_id:
                if run.valkey("XDEL", RUNS_STREAM_DEFAULT, stream_id) != 1:
                    failure = RuntimeError("the owned producer entry could not be transferred")
                    run.record_failure("capturing a real factory wake", failure)
                    raise failure
                return payload
        return None

    wake = observe_until(take)
    assert wake is not None, "the real factory producer did not emit the expected wake"
    return wake


def ci_waiting_execution(
    run: AdmittedKind, ticket: Ticket, wake: dict[str, Any]
) -> dict[str, Any]:
    def ci_waiting() -> Any:
        row = run.request(ticket)
        if (
            row
            and row["status"] == "running"
            and row["execution_deadline"] is not None
            and row["runtime_heartbeat_expires_at"] == row["execution_deadline"]
            and event_done(run, wake["event_id"])
        ):
            return row
        return None

    row = observe_until(ci_waiting)
    assert row is not None, "the actual worker did not park its executed request"
    return row


def change_github_fixture_label(run: AdmittedKind, ticket: Ticket, *, present: bool) -> None:
    with run.stub.provider_lock:
        issue = next(r for r in run.stub._rows("issue") if r["number"] == ticket.number)
        labels = [label for label in issue["labels"] if label["name"] != ticket.agent.label]
        if present:
            labels.append({"name": ticket.agent.label})
        issue["labels"] = labels
        run.stub._put("issue", ticket.number, issue)
        # Shape/provenance: tools/github-stub/github_stub.py::_label_event.
        run.stub._label_event(
            ticket.number, ticket.agent.label,
            "labeled" if present else "unlabeled", issue["user"],
        )


def test_two_worker_ci_preserves_successor_identity_and_rejects_stale_predecessor(
    two_worker_ci: tuple[AdmittedKind, str, str],
) -> None:
    run, owner_stream, continuation_stream = two_worker_ci
    owner_before = worker_lane_identity(run, "worker")
    continuation_worker_before = worker_lane_identity(run, "c4-held-worker")
    ticket, _, execute = waiting_ticket(run)
    forward_worker_wake(run, owner_stream, execute)
    predecessor = assert_started(run, ticket)
    assert predecessor["runtime_owner"].startswith(owner_before[0][0] + "-")
    assert_published(run, predecessor, 1)
    assert_marker(run, ticket, 1)
    predecessor = ci_waiting_execution(run, ticket, execute)

    run.mark_ci(predecessor, green=False)
    first_ci = produced_wake(run, ci_event_id(uuid.UUID(predecessor["id"]), 2))
    forward_worker_wake(run, continuation_stream, first_ci)
    predecessor_publications = assert_published(run, predecessor, 2)
    assert_marker(run, ticket, 2)
    continued_predecessor = ci_waiting_execution(run, ticket, first_ci)
    assert_execution_identity(predecessor, continued_predecessor)
    assert continued_predecessor["runtime_owner"] == predecessor["runtime_owner"]
    run.mark_ci(predecessor, green=False)
    stale_ci = produced_wake(run, ci_event_id(uuid.UUID(predecessor["id"]), 3))

    change_github_fixture_label(run, ticket, present=False)
    terminate = produced_wake(run, terminate_event_id(uuid.UUID(predecessor["id"])))
    forward_worker_wake(run, owner_stream, terminate)
    ended = observe_until(
        lambda: r if (r := request_state(run, predecessor["id"]))["status"] == "cancelled"
        and r["termination_observation"] and r["teardown_unconfirmed_at"] is None else None
    )
    assert ended is not None, "the actual owner did not terminate the predecessor sandbox"
    assert ended["runtime_epoch"] > predecessor["runtime_epoch"]
    assert observe_until(
        lambda: n if (n := notice_state(run, predecessor["id"])) and n["finalized_at"] else None
    )

    before_calls = max(call["id"] for call in run.model.call_evidence())
    change_github_fixture_label(run, ticket, present=True)
    successor = observe_until(
        lambda: r if (r := run.request(ticket)) and r["id"] != predecessor["id"] else None
    )
    assert successor is not None, "the real relabel did not admit a distinct successor request"
    assert successor["work_item_id"] == predecessor["work_item_id"]
    assert successor["conversation_id"] == predecessor["conversation_id"]
    assert successor["sequence"] > predecessor["sequence"]
    assert successor["agent_id"] == ticket.agent.id
    assert successor["trigger_label"] == ticket.agent.label
    successor_execute = run.take_execute_wake(successor)
    forward_worker_wake(run, owner_stream, successor_execute)
    successor = assert_started(run, ticket)
    successor_publications = assert_published(run, successor, 1)
    assert_marker(run, ticket, 1)
    assert successor["runtime_sandbox_name"] != predecessor["runtime_sandbox_name"]
    assert observe_until(lambda: any(
        call["id"] > before_calls and ticket.url in call["lane"] and call["round"] == 1
        and call["matched_marker"] == ticket.agent.marker
        for call in run.model.call_evidence()
    )), "the successor has no fresh actual bundle-marker tool result"
    successor = ci_waiting_execution(run, ticket, successor_execute)
    assert_running_snapshot(run, successor, successor_execute)
    predecessor_frozen = request_state(run, predecessor["id"])
    successor_frozen = request_state(run, successor["id"])
    previous_publications = run.publication_rows(predecessor)
    calls = run.model.call_evidence()
    route = run.valkey("GET", "curie:sandbox:route:" + successor["conversation_id"])

    for action in ("heartbeat", "hold-approval", "finish"):
        body: dict[str, Any] = {"runtime_epoch": predecessor["runtime_epoch"]}
        if action == "finish":
            body.update(outcome="failed", cause="runner_escalated")
        status, result = run.api_result(
            "POST", f"/v1/internal/work-items/requests/{predecessor['id']}/{action}",
            body, internal=True,
        )
        assert status == 409
        assert result["detail"]["code"] == "stale_owner"
        assert request_state(run, predecessor["id"]) == predecessor_frozen
        assert request_state(run, successor["id"]) == successor_frozen

    forward_worker_wake(run, continuation_stream, stale_ci)
    assert observe_until(lambda: event_done(run, stale_ci["event_id"]))
    assert any(
        stale_ci["event_id"] in line and successor["id"] in line
        and "is stale: running request is" in line
        for line in run.kubectl(
            "-n", run.namespace, "logs", "deployment/c4-held-worker", "--tail=2000",
        ).splitlines()
    ), "the predecessor wake did not identify the different running successor"
    assert request_state(run, predecessor["id"]) == predecessor_frozen
    assert request_state(run, successor["id"]) == successor_frozen
    assert run.publication_rows(predecessor) == previous_publications
    assert run.publication_rows(successor) == successor_publications
    assert run.model.call_evidence() == calls
    assert run.valkey("GET", "curie:sandbox:route:" + successor["conversation_id"]) == route

    before_valid_calls = max(call["id"] for call in calls)
    run.mark_ci(successor, green=False)
    valid_ci = produced_wake(run, ci_event_id(uuid.UUID(successor["id"]), 2))
    assert datetime.now(UTC) < datetime.fromisoformat(predecessor["execution_deadline"])
    forward_worker_wake(run, continuation_stream, valid_ci)
    publications = assert_published(run, successor, 2)
    assert_marker(run, ticket, 2)
    assert observe_until(lambda: any(
        call["id"] > before_valid_calls and ticket.url in call["lane"] and call["round"] == 2
        and call["matched_marker"] == ticket.agent.marker
        for call in run.model.call_evidence()
    )), "the successor CI delivery has no fresh actual bundle-marker tool result"
    assert observe_until(lambda: event_done(run, valid_ci["event_id"]))
    for rows, expected_request in (
        (predecessor_publications, predecessor["id"]), (publications, successor["id"]),
    ):
        assert {p["agent_id"] for p in rows} == {ticket.agent.id}
        assert {p["deployment_id"] for p in rows} == {ticket.agent.deployment_id}
        assert {p["execution_request_id"] for p in rows} == {expected_request}
    assert len({p["pr_number"] for p in [*predecessor_publications, *publications]}) == 1
    assert len({p["branch"] for p in [*predecessor_publications, *publications]}) == 1
    assert not {p["id"] for p in predecessor_publications} & {p["id"] for p in publications}
    run.mark_ci(successor, green=True)
    final = observe_until(
        lambda: r if (r := run.request(ticket)) and r["status"] == "completed" else None
    )
    assert final is not None, "the valid successor continuation did not complete"
    assert_execution_identity(successor, final)
    assert final["runtime_owner"] == successor["runtime_owner"]
    assert final["runtime_claim_name"] == successor["runtime_claim_name"]
    assert final["runtime_sandbox_name"] == successor["runtime_sandbox_name"]
    assert request_state(run, predecessor["id"]) == predecessor_frozen
    assert observe_until(
        lambda: n if (n := notice_state(run, successor["id"])) and n["finalized_at"] else None
    )
    assert_status_on_original_tracker(run, ticket, successor["id"])
    assert_run_persisted(run, ticket, successor["id"])
    assert worker_lane_identity(run, "worker") == owner_before
    assert worker_lane_identity(run, "c4-held-worker") == continuation_worker_before


@pytest.mark.parametrize("action", ["renew", "late_start"])
@pytest.mark.parametrize(
    ("change", "reason"),
    [("removed", "factory_binding_missing"), ("recreated", "factory_binding_changed")],
)
def test_renewal_and_late_start_revalidate_granted_binding(
    paused_factory: AdmittedKind, action: str, change: str, reason: str
) -> None:
    run = paused_factory
    ticket, before, wake = waiting_ticket(run)
    body = acquire_body(before, wake)
    path = f"/v1/internal/work-items/requests/{before['id']}"
    status, grant = run.api_result("POST", path + "/acquire", body, internal=True)
    assert status == 200, "the valid neighbor acquire did not grant the admitted request"
    assert grant["context"]["agent_id"] == ticket.agent.id
    assert grant["generation"] == before["dispatch_generation"]
    assert grant["repository_path"] == REPOSITORY
    assert datetime.fromisoformat(grant["wait_deadline"]) == datetime.fromisoformat(
        before["wait_deadline"]
    )
    run.api(
        "DELETE", query_path(
            f"/agents/{ticket.agent.id}/channels", kind="github", address=REPOSITORY
        ), expected=204,
    )
    if change == "recreated":
        run.api(
            "POST", f"/agents/{ticket.agent.id}/channels",
            run.binding("github", ticket.agent.label), expected=201,
        )
        new_id = run.sql(
            "SELECT to_json(id) FROM curie.agent_channels "
            f"WHERE agent_id='{uuid.UUID(ticket.agent.id)}' AND kind='github'"
        )
        assert new_id != grant["context"]["binding_id"]
    if action == "renew":
        body["context"] = grant["context"]
        target = "/acquire"
    else:
        body = {
            "owner": body["owner"], "generation": body["generation"],
            "context": grant["context"], "claim_name": "acme-unstarted-claim",
            "sandbox_name": "acme-unstarted-sandbox",
        }
        target = "/start"
    status, refusal = run.api_result("POST", path + target, body, internal=True)
    assert status == 409
    assert refusal["detail"]["code"] == reason
    factory_deferral(run, ticket, before, reason)
    # A late start with the old grant also loses its owner fence after renewal
    # clears the acquisition. It must not charge a second start deferral.
    frozen = request_state(run, before["id"])
    status, refusal = run.api_result(
        "POST", path + "/start",
        {
            "owner": "acme-test-worker", "generation": before["dispatch_generation"],
            "context": grant["context"], "claim_name": "acme-unstarted-claim",
            "sandbox_name": "acme-unstarted-sandbox",
        }, internal=True,
    )
    assert status == 409
    assert refusal["detail"]["code"] == "not_dispatchable"
    assert request_state(run, before["id"]) == frozen
    if action == "late_start" and change == "removed":
        # Each cycle proves a fresh valid acquire before removing its binding.
        # The fifth real start deferral must report the settled disposition.
        for attempt in range(2, 6):
            current = run.request(ticket)
            assert current is not None
            run.api(
                "POST", f"/agents/{ticket.agent.id}/channels",
                run.binding("github", ticket.agent.label), expected=201,
            )
            status, grant = run.api_result(
                "POST", path + "/acquire", acquire_body(current, wake), internal=True,
            )
            assert status == 200
            assert grant["context"]["agent_id"] == ticket.agent.id
            run.api(
                "DELETE", query_path(
                    f"/agents/{ticket.agent.id}/channels", kind="github", address=REPOSITORY
                ), expected=204,
            )
            status, refusal = run.api_result(
                "POST", path + "/start",
                {
                    "owner": "acme-test-worker", "generation": current["dispatch_generation"],
                    "context": grant["context"], "claim_name": "acme-unstarted-claim",
                    "sandbox_name": "acme-unstarted-sandbox",
                }, internal=True,
            )
            assert status == 409
            assert refusal["detail"]["code"] == (
                "factory_start_failed" if attempt == 5 else reason
            )
            if attempt < 5:
                factory_deferral(run, ticket, current, reason)
        terminal = run.request(ticket)
        assert terminal is not None
        assert terminal["status"] == "failed"
        assert terminal["terminal_cause"] == "start_failed"
        assert terminal["start_deferrals"] == 5
        assert terminal["dispatch_generation"] == before["dispatch_generation"] + 5
        assert terminal["last_deferral_reason"] == "not_started:" + reason
        assert terminal["acquire_owner"] is None
        assert terminal["acquired_generation"] is None
        assert terminal["acquire_expires_at"] is None
        assert notice_state(run, before["id"])["terminal_cause"] == "start_failed"
        no_execution_effects(run, ticket, terminal)
        frozen = request_state(run, before["id"])
    run.worker_replicas(1)
    progress_neighbor(run)
    assert request_state(run, before["id"]) == frozen
    no_execution_effects(run, ticket, before)


def test_removed_tracker_keeps_existing_status_outbox_retrying(
    paused_factory: AdmittedKind,
) -> None:
    run = paused_factory
    ticket, before, wake = waiting_ticket(run, "jira")

    def remove_tracker(configuration: dict[str, Any]) -> None:
        removed = {t["name"] for t in configuration["trackers"] if t["kind"] == "jira_cloud"}
        configuration["bindings"] = [
            b for b in configuration["bindings"] if b["tracker"] not in removed
        ]
        configuration["trackers"] = [
            t for t in configuration["trackers"] if t["name"] not in removed
        ]

    run.change_forges(remove_tracker)
    notice_before = notice_state(run, before["id"])
    comments_before = run.comments(ticket)
    run.enqueue(wake)
    run.worker_replicas(1)
    final = observe_until(
        lambda: r if (r := run.request(ticket)) and r["status"] == "failed" else None,
        seconds=600,
    )
    assert final is not None, "removed tracker authority stranded its waiting request"
    assert final["terminal_cause"] == "start_failed"
    assert final["start_deferrals"] == 5
    assert final["capacity_deferrals"] == before["capacity_deferrals"]
    assert final["dispatch_generation"] == before["dispatch_generation"] + 5
    assert final["last_deferral_reason"] == "not_started:factory_surface_mismatch"
    no_execution_effects(run, ticket, final)
    assert observe_until(
        lambda: n if (n := notice_state(run, before["id"]))
        and n["terminal_cause"] == "start_failed"
        and n["attempts"] > notice_before["attempts"] else None
    ), "the existing outbox did not record its terminal cause and another delivery attempt"
    notice = notice_state(run, before["id"])
    assert notice["execution_request_id"] == before["id"]
    assert notice["work_item_id"] == before["work_item_id"]
    assert notice["finalized_at"] is None
    assert notice["refusal"] is None
    assert notice["refused_at"] is None
    assert notice["rendered_digest"] == notice_before["rendered_digest"]
    assert run.comments(ticket) == comments_before
    attempts = notice["attempts"]
    progress_neighbor(run)
    assert notice_state(run, before["id"])["attempts"] > attempts
    assert run.comments(ticket) == comments_before
    assert request_state(run, before["id"])["status"] == "failed"
    no_execution_effects(run, ticket, final)
