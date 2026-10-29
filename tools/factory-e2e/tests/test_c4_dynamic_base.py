"""Executable dynamic-base acceptance against a candidate factory install.

Run this module separately. Its session fixture builds the candidate once;
each case owns and removes its own cluster. Provider HTTP and model responses
are the only doubles. Missing infrastructure is a fixture error, never a skip.
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402
import scripted_kind as sk  # noqa: E402
from c4_admitted_kind import REPOSITORY, AdmittedKind, Ticket  # noqa: E402
from c4_dynamic_base import (  # noqa: E402
    CHANGE,
    SETTLE_SECONDS,
    TERMINAL,
    DynamicKind,
    assert_ci_still_owns_request,
    assert_replayed_history,
    configure_dynamic,
    wait,
)
from test_c4_admitted_agent import (  # noqa: E402
    finalize_fixture,
    source_candidate,  # noqa: F401
)


@pytest.fixture(autouse=True)
def candidate_curie() -> None:
    """Use the built candidate instead of the directory's fake CLI fixture."""


@pytest.fixture
def dynamic_factory(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    source_candidate: AdmittedKind,  # noqa: F811
) -> DynamicKind:
    root = source_candidate.root / f"dynamic-{uuid.uuid4().hex[:10]}"
    root.mkdir()
    temporary = root / "tmp"
    temporary.mkdir()
    monkeypatch.setenv("TMPDIR", str(temporary))
    monkeypatch.setenv("CURIE_BIN", source_candidate.curie)
    run = DynamicKind(root, Path(source_candidate.curie))
    run.source_candidate = source_candidate
    run.images = source_candidate.images
    run.tag_map = dict(zip(sk.IMAGES, run.images.values(), strict=True))
    run.env.update(TMPDIR=str(temporary))
    request.addfinalizer(lambda: finalize_fixture(run))
    configure_dynamic(monkeypatch, run)
    run.execute()
    if run.valkey("PING") != "PONG":
        raise RuntimeError("the owned factory Valkey did not answer PING")
    pods = json.loads(run.kubectl("-n", run.namespace, "get", "pods", "-o", "json"))["items"]
    for component in ("api", "worker", "ui"):
        images = [
            container["image"]
            for pod in pods
            for container in pod["spec"]["containers"]
            if container["name"] == component
        ]
        if not images or any(image != run.images[component] for image in images):
            raise RuntimeError(f"the running {component} does not use this candidate image")
    api = run.kubectl(
        "-n",
        run.namespace,
        "exec",
        "deployment/curie-api",
        "--",
        "python",
        "-c",
        "from curie_api.config import get_settings; "
        "print(get_settings().factory_base_label_settle_s)",
    )
    if int(api.strip()) != SETTLE_SECONDS:
        raise RuntimeError("the API did not consume factory.baseLabelSettleSeconds from the chart")
    policy = json.loads(
        run.kubectl(
            "-n",
            run.namespace,
            "exec",
            "deployment/curie-api",
            "--",
            "python",
            "-c",
            "import json; from curie_api.config import get_settings; "
            "from curie_api.forges.registry import forges_registry; "
            f"policy = forges_registry(get_settings()).github_repository_policy({REPOSITORY!r}); "
            "print(json.dumps(policy.model_dump(include={'bases', 'default_base', 'ci'}) "
            "if policy is not None else None))",
        )
    )
    if (
        policy is None
        or policy["bases"] != ["*"]
        or policy["default_base"] != "main"
        or policy["ci"]["required"]
        != {"key": "dynamic-ci", "paths": [CHANGE], "pending_key_prefix": None}
    ):
        raise RuntimeError("the API did not consume the dynamic-base GitHub repository policy")
    run.deadline = None
    run.fixture_ready = True
    return run


def started(run: DynamicKind, ticket: Ticket, *, after: str | None = None) -> dict[str, Any] | None:
    row = run.request(ticket)
    return row if row and row["started_at"] and row["id"] != after else None


def wait_started(
    run: DynamicKind, ticket: Ticket, *, after: str | None = None
) -> dict[str, Any] | None:
    row = wait(lambda: started(run, ticket, after=after))
    if row is None:
        run.record_start_failure(ticket)
    return row


def observed_checkout(run: DynamicKind, head: str) -> dict[str, Any] | None:
    return next((row for row in run.model.observations() if row["head"] == head), None)


def published(run: DynamicKind, ticket: Ticket) -> tuple[dict[str, Any], dict[str, Any]]:
    request = wait_started(run, ticket)
    assert request is not None, "the selected ticket did not execute"

    def ready() -> Any:
        rows = run.publication_rows(request)
        return rows[0] if len(rows) == 1 and rows[0]["status"] == "succeeded" else None

    publication = wait(ready, seconds=360)
    assert publication is not None, "the actual runner did not publish its example change"
    assert run.stub.git("show", publication["head_sha"] + ":" + CHANGE) == 'VALUE = "factory"'
    return request, publication


def latest_transition(run: DynamicKind, ticket: Ticket, phase: str) -> dict[str, Any] | None:
    rows = run.transitions(ticket)
    return rows[-1] if rows and rows[-1]["phase"] == phase else None


def request_by_id(run: DynamicKind, ticket: Ticket, identifier: str) -> dict[str, Any]:
    return next(row for row in run.requests(ticket) if row["id"] == identifier)


def nochange_ready(
    run: DynamicKind,
    ticket: Ticket,
    transition: dict[str, Any],
    *,
    expected_content: str,
) -> dict[str, Any]:
    row = wait(lambda: latest_transition(run, ticket, "applied"))
    if row is None or row["id"] != transition["id"]:
        run.record_nochange_failure(ticket, transition, stage="transition_applied")
    assert row is not None and row["id"] == transition["id"], (
        "the platform did not apply the expected base move"
    )
    row = wait(
        lambda: (
            current
            if (current := latest_transition(run, ticket, "applied"))
            and current["id"] == transition["id"]
            and current["ci_ready_at"]
            else None
        )
    )
    if row is None:
        run.record_nochange_failure(ticket, transition, stage="ci_ready")
    assert row is not None, "the real no-change finish path did not hand CI its transition subject"
    assert row["ci_settled_at"] is None
    assert run.publication_rows({"id": row["ci_request_id"]}) == [], (
        "a base move fabricated a publication"
    )
    checkout = observed_checkout(run, row["verification_head_sha"])
    if checkout is None:
        run.record_nochange_failure(ticket, row, stage="current_checkout")
    assert checkout is not None, "the successor reused the old checkout"
    assert (checkout["target"], checkout["value"]) == (
        row["target_base_ref"],
        expected_content,
    )
    assert row["outcome_ready_at"] is not None, "execution bypassed the base-change outcome"
    assert_ci_still_owns_request(request_by_id(run, ticket, row["ci_request_id"]), row)
    preflight = run.sql(
        "SELECT coalesce(json_agg(note::json), '[]'::json) "
        "FROM curie.execution_request_phase_reports "
        f"WHERE execution_request_id='{uuid.UUID(row['ci_request_id'])}' "
        "AND phase='verification_preflight'"
    )
    preflight_passed = any(observation.get("outcome") == "passed" for observation in preflight)
    if not preflight_passed:
        run.record_nochange_failure(ticket, row, stage="verification_preflight")
    assert preflight_passed, "the successor has no fresh repository verification"
    return row


def ci_observed(run: DynamicKind, head: str, state: str, *, after: int = 0, count: int = 1) -> bool:
    return (
        sum(
            row["head"] == head and state in row["detail"].split(",") and row["id"] > after
            for row in run.stub.events("ci_read")
        )
        >= count
    )


def assert_running_after_gate(
    run: DynamicKind, ticket: Ticket, transition: dict[str, Any], state: str
) -> None:
    cursor = max((row["id"] for row in run.stub.events()), default=0)
    assert wait(
        lambda: ci_observed(run, transition["verification_head_sha"], state, after=cursor, count=2)
    ), "CI did not observe the controlled final-head verdict"
    request = request_by_id(run, ticket, transition["ci_request_id"])
    assert_ci_still_owns_request(request, latest_transition(run, ticket, "applied"))


def assert_completed(run: DynamicKind, ticket: Ticket, request_id: str) -> None:
    def terminal() -> Any:
        row = request_by_id(run, ticket, request_id)
        return row if row["status"] in TERMINAL else None

    row = wait(terminal)
    if row is None or row["status"] != "completed":
        transition = next(
            (
                candidate
                for candidate in reversed(run.transitions(ticket))
                if candidate["ci_request_id"] == request_id and candidate["phase"] == "applied"
            ),
            None,
        )
        if transition is not None:
            run.record_nochange_failure(ticket, transition, stage="ci_settlement")
    assert row is not None, "fresh final-head green CI did not settle the request"
    assert row["status"] == "completed", f"fresh final-head CI settled {row['status']}"


def wait_new_transition(
    run: DynamicKind,
    ticket: Ticket,
    *,
    target: str,
    previous_transition_id: str | None,
) -> dict[str, Any]:
    transition = wait(
        lambda: (
            rows[-1]
            if (rows := run.transitions(ticket))
            and rows[-1]["id"] != previous_transition_id
            and rows[-1]["target_base_ref"] == target
            else None
        )
    )
    assert transition is not None, "a relabel did not create a durable platform base move"
    return transition


def start_move(
    run: DynamicKind,
    ticket: Ticket,
    *,
    target: str,
    previous_transition_id: str | None,
) -> dict[str, Any]:
    run.model.mode("nochange")
    run.label(ticket, "base:" + target)
    return wait_new_transition(
        run, ticket, target=target, previous_transition_id=previous_transition_id
    )


def assert_applied_git(
    run: DynamicKind,
    before: dict[str, Any],
    transition: dict[str, Any],
    *,
    expected_content: str,
    expected_push_history: tuple[str, ...],
    expected_replay_heads: tuple[str, ...],
) -> None:
    candidate, final = transition["rebased_head_sha"], transition["verification_head_sha"]
    assert candidate and final and candidate != final
    assert run.stub.git("rev-parse", final + "^") == candidate
    assert run.stub.git("rev-parse", candidate + "^{tree}") == run.stub.git(
        "rev-parse", final + "^{tree}"
    ), "the CI trigger changed the replayed tree"
    ancestors = set(run.stub.git("rev-list", final).splitlines())
    assert transition["target_base_sha"] in ancestors, (
        "the factory branch was not rebased onto the selected remote base"
    )
    assert transition["old_head_sha"] not in ancestors, (
        "the old factory commit was retained without replay"
    )
    assert datetime.fromisoformat(transition["retargeted_at"]) < datetime.fromisoformat(
        transition["ci_pushed_at"]
    ), "the final verification head was pushed before durable retarget confirmation"
    changed_paths = run.stub.git(
        "diff", "--name-only", transition["target_base_sha"], final
    ).splitlines()
    assert changed_paths == transition["changed_paths"] == [CHANGE], (
        "the same-tree verification commit hid the replayed Python change from CI"
    )
    assert run.stub.git("show", final + ":" + CHANGE) == expected_content
    source = run.stub.history(transition["old_base_sha"], transition["old_head_sha"])
    replayed = run.stub.history(transition["target_base_sha"], candidate)
    assert_replayed_history(source, replayed, expected_source_heads=expected_replay_heads)
    final_history = run.stub.history(transition["target_base_sha"], final)
    assert final_history[:-1] == replayed and final_history[-1]["sha"] == final
    assert final_history[-1]["empty"], "the fresh verification commit changed the replayed tree"
    pull = run.pull(int(before["pr_number"]))
    assert pull["head"]["sha"] == final and pull["base"]["ref"] == transition["target_base_ref"]
    assert pull["head"]["ref"] == before["branch"]
    writes = [event for event in run.stub.events("push") if event["detail"] == before["branch"]]
    assert [event["head"] for event in writes] == [*expected_push_history, candidate, final], (
        "recovery wrote an extra or unexpected remote head"
    )
    assert writes[-2]["target"] == transition["old_base_ref"]
    assert writes[-1]["target"] == transition["target_base_ref"], (
        "the final-head CI event ran against the old target"
    )


def relabel_completed_request(
    run: DynamicKind,
    ticket: Ticket,
    publication: dict[str, Any],
    *,
    completed_request_id: str,
    previous_transition_id: str,
    expected_content: str,
    expected_push_history: tuple[str, ...],
    expected_replay_heads: tuple[str, ...],
) -> None:
    predecessor = request_by_id(run, ticket, completed_request_id)
    assert predecessor["status"] == "completed"
    request_ids = [request["id"] for request in run.requests(ticket)]
    transition_ids = [transition["id"] for transition in run.transitions(ticket)]
    assert request_ids[-1] == completed_request_id
    assert transition_ids[-1] == previous_transition_id
    before = run.pull(int(publication["pr_number"]))
    assert before["state"] == "open" and before["base"]["ref"] == "next"
    assert before["head"]["sha"] == expected_replay_heads[-1]
    lineage_id = run.item(ticket)["publication_lineage_id"]
    old_lineage = run.sql(
        "SELECT row_to_json(l) FROM curie.thread_publication_lineages l "
        f"WHERE id='{uuid.UUID(lineage_id)}'"
    )
    assert old_lineage["head_sha"] == before["head"]["sha"]
    assert ticket.agent.label in {
        label["name"] for label in run.stub._get("issue", ticket.number)["labels"]
    }
    run.model.mode("nochange")
    run.label(ticket, "base:next", present=False)
    transition = wait_new_transition(
        run, ticket, target="main", previous_transition_id=previous_transition_id
    )
    row = nochange_ready(run, ticket, transition, expected_content=expected_content)
    successor = request_by_id(run, ticket, row["ci_request_id"])
    assert successor["id"] != completed_request_id
    assert successor["sequence"] == predecessor["sequence"] + 1
    assert successor["started_at"] is not None
    assert row["request_id"] == row["ci_request_id"] == successor["id"]
    assert row["old_head_sha"] == before["head"]["sha"]
    assert (row["old_base_ref"], row["target_base_ref"], row["base_source"]) == (
        "next", "main", "default"
    )
    assert [request["id"] for request in run.requests(ticket)] == [
        *request_ids, successor["id"]
    ]
    assert [candidate["id"] for candidate in run.transitions(ticket)] == [
        *transition_ids, row["id"]
    ]
    assert request_by_id(run, ticket, completed_request_id) == predecessor, (
        "a completed predecessor changed during the new base move"
    )
    assert_applied_git(
        run,
        publication,
        row,
        expected_content=expected_content,
        expected_push_history=expected_push_history,
        expected_replay_heads=expected_replay_heads,
    )
    current = run.pull(before["number"])
    assert current["state"] == "open"
    assert (current["id"], current["number"], current["html_url"], current["head"]["ref"]) == (
        before["id"], before["number"], before["html_url"], before["head"]["ref"]
    )
    item = run.item(ticket)
    assert (item["publication_lineage_id"], item["base_resolved_request_id"]) == (
        lineage_id, successor["id"]
    )
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "main", "default", row["target_base_sha"]
    )
    lineage = run.sql(
        "SELECT row_to_json(l) FROM curie.thread_publication_lineages l "
        f"WHERE id='{uuid.UUID(lineage_id)}'"
    )
    assert (lineage["base_ref"], lineage["base_sha"], lineage["head_sha"]) == (
        "main", row["target_base_sha"], row["verification_head_sha"]
    )
    assert lineage["latest_revision"] == old_lineage["latest_revision"]
    identity = ("id", "branch", "pr_number", "pr_url", "code_host_pr_id")
    assert tuple(lineage[key] for key in identity) == tuple(old_lineage[key] for key in identity)
    assert wait(
        lambda: any(
            line.startswith("Base:") and "`main`" in line and "deployment default" in line
            for comment in run.comments(ticket)
            for line in comment.splitlines()
        )
    ), "the completed-request relabel omitted the selected default base"
    assert_running_after_gate(run, ticket, row, "queued")
    run.stub.check(row["verification_head_sha"], "success", target="main")
    assert_completed(run, ticket, successor["id"])
    assert latest_transition(run, ticket, "applied")["ci_settled_at"] is not None
    assert request_by_id(run, ticket, completed_request_id) == predecessor


def test_delayed_label_and_pre_pr_change_replace_real_checkout(
    dynamic_factory: DynamicKind,
) -> None:
    run = dynamic_factory
    run.model.mode("hold")
    ticket = run.seed("github", run.agents[0])
    first = run.await_admission(ticket)
    remaining = run.sql(
        "SELECT to_json(extract(epoch FROM (base_settle_until-clock_timestamp()))) "
        f"FROM curie.execution_requests WHERE id='{uuid.UUID(first['id'])}'"
    )
    assert 0 < remaining <= SETTLE_SECONDS, (
        "the initial factory marking did not schedule its configured settle interval"
    )
    assert first["started_at"] is None
    assert run.item(ticket)["base_resolved_request_id"] is None
    run.label(ticket, "base:next")
    first = wait_started(run, ticket)
    assert first is not None
    item = run.item(ticket)
    assert item["base_branch"] == "next" and item["base_source"] == "label"
    assert item["base_commit"] == run.stub._ref("next")
    assert item["base_resolved_request_id"] == first["id"]
    assert datetime.fromisoformat(first["started_at"]) >= datetime.fromisoformat(
        first["base_settle_until"]
    )
    assert wait(lambda: observed_checkout(run, item["base_commit"]))
    assert observed_checkout(run, item["base_commit"])["target"] == "next"
    old_sandbox = first["runtime_sandbox_name"]
    assert old_sandbox
    assert run.stub._rows("pull") == []

    run.label(ticket, "base:next", present=False)
    run.label(ticket, "base:main")
    successor = wait_started(run, ticket, after=first["id"])
    assert successor is not None, "pre-PR relabel did not start a fresh request"
    predecessor = request_by_id(run, ticket, first["id"])
    assert predecessor["status"] == "cancelled" and predecessor["teardown_unconfirmed_at"] is None
    assert successor["sequence"] == first["sequence"] + 1
    assert successor["runtime_sandbox_name"] != old_sandbox, (
        "the new request reused its predecessor's runner"
    )
    item = run.item(ticket)
    assert (item["base_branch"], item["base_source"], item["base_resolved_request_id"]) == (
        "main",
        "label",
        successor["id"],
    )
    assert wait(lambda: observed_checkout(run, item["base_commit"]))
    assert observed_checkout(run, item["base_commit"])["target"] == "main"
    assert run.stub._rows("pull") == []
    request_ids = [row["id"] for row in run.requests(ticket)]
    run.label(ticket, "base:main", present=False)
    assert wait(lambda: run.item(ticket)["base_source"] == "default")
    assert [row["id"] for row in run.requests(ticket)] == request_ids, (
        "same-branch provenance change restarted execution"
    )


def test_clean_move_recovers_push_and_retarget_then_waits_for_final_head_ci(
    dynamic_factory: DynamicKind,
) -> None:
    run = dynamic_factory
    ticket = run.seed("github", run.agents[0])
    old_request, publication = published(run, ticket)
    old_item = run.item(ticket)
    old_pull = run.pull(int(publication["pr_number"]))
    run.stub.control("lose_push", "once")
    run.stub.control("retarget", "unavailable")
    transition = start_move(run, ticket, target="next", previous_transition_id=None)
    pushed = wait(lambda: latest_transition(run, ticket, "pushed"))
    assert pushed is not None, "the uncertain successful push was not recovered"
    assert len(run.stub.events("lost_push_response")) == 1
    assert run.stub._ref(publication["branch"]) == pushed["rebased_head_sha"]
    assert run.pull(old_pull["number"])["base"]["ref"] == "main"
    assert run.request(ticket)["started_at"] is None, "a retarget outage allowed a new execution"
    assert request_by_id(run, ticket, old_request["id"])["status"] == "cancelled"
    assert pushed["request_id"] == transition["request_id"]
    assert pushed["ci_request_id"] == transition["ci_request_id"]
    run.stub.check(pushed["rebased_head_sha"], "success")
    run.stub.check(pushed["rebased_head_sha"], "queued", name="old-target-queued", target="main")
    run.stub.check(pushed["rebased_head_sha"], "failure", name="old-target-rerun", target="main")
    run.stub.control("retarget", "lost_once")
    assert wait(lambda: bool(run.stub.events("retarget"))), (
        "the provider did not receive a retarget"
    )
    row = nochange_ready(run, ticket, transition, expected_content='VALUE = "factory"')
    run.stub.check(pushed["rebased_head_sha"], "success", name="old-target-queued")
    run.stub.check(pushed["rebased_head_sha"], "success", name="old-target-rerun")
    old_checks = {check["name"]: check for check in run.stub.checks(pushed["rebased_head_sha"])}
    retargeted_at = datetime.fromisoformat(row["retargeted_at"])
    assert datetime.fromisoformat(old_checks["dynamic-ci"]["completed"]) < retargeted_at
    assert datetime.fromisoformat(old_checks["old-target-queued"]["started"]) > retargeted_at
    assert datetime.fromisoformat(old_checks["old-target-rerun"]["started"]) > retargeted_at
    assert {check["target"] for check in old_checks.values()} == {"main"}
    assert row["id"] == pushed["id"]
    assert_applied_git(
        run,
        publication,
        row,
        expected_content='VALUE = "factory"',
        expected_push_history=(publication["head_sha"],),
        expected_replay_heads=(publication["head_sha"],),
    )
    pull = run.pull(old_pull["number"])
    assert (pull["id"], pull["number"], pull["html_url"]) == (
        old_pull["id"],
        old_pull["number"],
        old_pull["html_url"],
    )
    assert len(run.stub.events("retarget")) == 1, "a lost retarget reply caused duplicate mutation"
    item = run.item(ticket)
    assert item["publication_lineage_id"] == row["lineage_id"] == old_item[
        "publication_lineage_id"
    ]
    lineage = run.sql(
        "SELECT row_to_json(l) FROM curie.thread_publication_lineages l "
        f"WHERE id='{uuid.UUID(row['lineage_id'])}'"
    )
    assert (lineage["base_ref"], lineage["base_sha"], lineage["head_sha"], lineage["branch"]) == (
        "next",
        row["target_base_sha"],
        row["verification_head_sha"],
        publication["branch"],
    )
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "next",
        "label",
        row["target_base_sha"],
    )
    assert wait(
        lambda: any(
            line.startswith("Base:") and "`next`" in line and "label" in line.lower()
            for comment in run.comments(ticket)
            for line in comment.splitlines()
        )
    ), "status omitted the selected base or label provenance"
    assert_running_after_gate(run, ticket, row, "queued")
    run.stub.check(row["verification_head_sha"], "success", target="next")
    assert_completed(run, ticket, row["ci_request_id"])
    assert latest_transition(run, ticket, "applied")["ci_settled_at"] is not None
    relabel_completed_request(
        run,
        ticket,
        publication,
        completed_request_id=row["ci_request_id"],
        previous_transition_id=row["id"],
        expected_content='VALUE = "factory"',
        expected_push_history=(
            publication["head_sha"], row["rebased_head_sha"], row["verification_head_sha"]
        ),
        expected_replay_heads=(row["rebased_head_sha"], row["verification_head_sha"]),
    )


def test_conflict_writes_nothing_and_requires_fresh_factory_marking(
    dynamic_factory: DynamicKind,
) -> None:
    run = dynamic_factory
    ticket = run.seed("github", run.agents[0])
    _, publication = published(run, ticket)
    before = run.pull(int(publication["pr_number"]))
    writes = list(run.stub.events("push"))
    start_move(run, ticket, target="release/blocked", previous_transition_id=None)
    refused = wait(lambda: latest_transition(run, ticket, "conflict"))
    assert refused is not None, "the actual conflicting replay was not refused"
    assert run.stub._ref(publication["branch"]) == publication["head_sha"]
    assert run.pull(before["number"]) == before, "a conflict changed the existing PR"
    assert run.stub.events("push") == writes and run.stub.events("retarget") == []
    assert wait(
        lambda: any(
            f"curie-base-transition-{refused['id']}" in comment for comment in run.comments(ticket)
        )
    )
    assert (
        sum(f"curie-base-transition-{refused['id']}" in comment for comment in run.comments(ticket))
        == 1
    )
    assert wait(
        lambda: any(
            line.startswith("Base:")
            and "`main`" in line
            and "`release/blocked`" in line
            and "deployment default" in line.lower()
            and "conflict" in line.lower()
            for comment in run.comments(ticket)
            for line in comment.splitlines()
        )
    ), "conflict status omitted the selected base, requested target, or provenance"
    run.label(ticket, "base:release/blocked", present=False)
    run.label(ticket, "base:next")
    assert wait(lambda: run.item(ticket)["base_requested_branch"] == "next")
    assert len(run.transitions(ticket)) == 1, "editing the base label retried a conflict hold"
    assert run.stub.events("push") == writes
    run.label(ticket, ticket.agent.label, present=False)
    # The signed next marking is the retry authority. Polling must first have
    # observed removal so this is an actual new factory-label application.
    assert wait(lambda: run.item(ticket)["cancelled_at"] is not None)
    run.label(ticket, ticket.agent.label)
    moved = wait(
        lambda: (
            rows[-1]
            if len(rows := run.transitions(ticket)) == 2 and rows[-1]["phase"] == "applied"
            else None
        )
    )
    assert moved is not None, "fresh factory marking did not retry the corrected current selection"
    row = nochange_ready(run, ticket, moved, expected_content='VALUE = "factory"')
    assert_applied_git(
        run,
        publication,
        row,
        expected_content='VALUE = "factory"',
        expected_push_history=(publication["head_sha"],),
        expected_replay_heads=(publication["head_sha"],),
    )
    assert row["target_base_ref"] == "next"
    run.stub.check(row["verification_head_sha"], "success", target="next")
    assert_completed(run, ticket, row["ci_request_id"])


def test_red_nochange_subject_continues_and_new_publication_owns_ci(
    dynamic_factory: DynamicKind,
) -> None:
    run = dynamic_factory
    ticket = run.seed("github", run.agents[0])
    _, publication = published(run, ticket)
    transition = start_move(run, ticket, target="next", previous_transition_id=None)
    row = nochange_ready(run, ticket, transition, expected_content='VALUE = "factory"')
    assert_applied_git(
        run,
        publication,
        row,
        expected_content='VALUE = "factory"',
        expected_push_history=(publication["head_sha"],),
        expected_replay_heads=(publication["head_sha"],),
    )
    run.model.mode("fix")
    run.stub.check(row["verification_head_sha"], "failure", target="next")
    assert wait(lambda: ci_observed(run, row["verification_head_sha"], "failure"))
    assert request_by_id(run, ticket, row["ci_request_id"])["status"] != "completed", (
        "red final-head CI completed the no-change successor"
    )

    def fixed() -> Any:
        rows = run.publication_rows({"id": row["ci_request_id"]})
        return rows[-1] if rows and rows[-1]["status"] == "succeeded" else None

    revision = wait(fixed, seconds=360)
    if revision is None:
        run.record_nochange_failure(ticket, row, stage="fix_publication")
    assert revision is not None, "red no-change CI did not enter the normal fix publication path"
    assert revision["head_sha"] != row["verification_head_sha"]
    assert run.stub.git("show", revision["head_sha"] + ":" + CHANGE) == 'VALUE = "factory-fixed"'
    assert run.stub.git("rev-parse", revision["head_sha"] + "^") == row["verification_head_sha"]
    assert (
        row["verification_head_sha"] in run.stub.git("rev-list", revision["head_sha"]).splitlines()
    )
    assert run.pull(int(publication["pr_number"]))["head"]["sha"] == revision["head_sha"]
    run.stub.check(row["verification_head_sha"], "success", target="next")
    cursor = max(event["id"] for event in run.stub.events())
    assert wait(lambda: ci_observed(run, revision["head_sha"], "queued", after=cursor, count=2))
    assert request_by_id(run, ticket, row["ci_request_id"])["status"] == "running", (
        "the older transition settled over a newer publication"
    )
    run.stub.check(revision["head_sha"], "success", target="next")
    assert_completed(run, ticket, row["ci_request_id"])
    assert run.pull(int(publication["pr_number"]))["head"]["sha"] == revision["head_sha"]
    relabel_completed_request(
        run,
        ticket,
        publication,
        completed_request_id=row["ci_request_id"],
        previous_transition_id=row["id"],
        expected_content='VALUE = "factory-fixed"',
        expected_push_history=(
            publication["head_sha"],
            row["rebased_head_sha"],
            row["verification_head_sha"],
            revision["head_sha"],
        ),
        expected_replay_heads=(
            row["rebased_head_sha"], row["verification_head_sha"], revision["head_sha"]
        ),
    )
