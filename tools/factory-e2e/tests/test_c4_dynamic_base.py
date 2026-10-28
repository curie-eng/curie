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


def request_by_id(run: DynamicKind, ticket: Ticket, identifier: str) -> dict[str, Any]:
    return next(row for row in run.requests(ticket) if row["id"] == identifier)


def ci_observed(run: DynamicKind, head: str, state: str, *, after: int = 0, count: int = 1) -> bool:
    return (
        sum(
            row["head"] == head and state in row["detail"].split(",") and row["id"] > after
            for row in run.stub.events("ci_read")
        )
        >= count
    )


def assert_completed(run: DynamicKind, ticket: Ticket, request_id: str) -> None:
    def terminal() -> Any:
        row = request_by_id(run, ticket, request_id)
        return row if row["status"] in TERMINAL else None

    row = wait(terminal)
    assert row is not None, "green CI did not settle the published request"
    assert row["status"] == "completed", f"green CI settled {row['status']}"


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


def test_published_base_change_is_refused_then_correction_allows_original_ci_to_complete(
    dynamic_factory: DynamicKind,
) -> None:
    run = dynamic_factory
    ticket = run.seed("github", run.agents[0])
    request, publication = published(run, ticket)
    head = publication["head_sha"]
    assert wait(lambda: ci_observed(run, head, "queued")), "ordinary CI did not observe the head"
    assert request_by_id(run, ticket, request["id"])["status"] == "running"
    original_item = run.item(ticket)
    original_pull = run.pull(int(publication["pr_number"]))
    original_writes = list(run.stub.events("push"))
    original_request_ids = [row["id"] for row in run.requests(ticket)]

    def lineage() -> dict[str, Any]:
        return run.sql(
            "SELECT row_to_json(observation) FROM ("
            "SELECT l.id, l.base_ref, l.base_sha, l.head_sha, l.version, l.branch, "
            "l.pr_number, l.pr_url, l.code_host_pr_id "
            "FROM curie.thread_publication_lineages l JOIN curie.work_items w "
            "ON w.publication_lineage_id=l.id "
            f"WHERE w.id='{uuid.UUID(original_item['id'])}') observation"
        )

    original_lineage = lineage()
    selection = ("base_branch", "base_source", "base_commit", "base_resolved_request_id")

    def refused_comments() -> list[str]:
        return [
            comment
            for comment in run.comments(ticket)
            if any(
                line.startswith("Base:")
                and "`main`" in line
                and "`next`" in line
                and "requested" in line.lower()
                and "refus" in line.lower()
                for line in comment.splitlines()
            )
        ]

    run.label(ticket, "base:next")
    assert wait(lambda: run.item(ticket)["base_requested_branch"] == "next")
    assert wait(refused_comments), "status did not expose the refused requested base"
    cursor = max(event["id"] for event in run.stub.events())
    run.label(ticket, "base:next")
    assert wait(lambda: ci_observed(run, head, "queued", after=cursor, count=2)), (
        "the frozen selection stopped ordinary publication CI"
    )
    assert len(refused_comments()) == 1, "the repeated label observation posted another refusal"
    assert [row["id"] for row in run.requests(ticket)] == original_request_ids
    current = request_by_id(run, ticket, request["id"])
    assert (current["status"], current["sequence"], current["cancellation_requested_at"]) == (
        "running",
        request["sequence"],
        None,
    )
    item = run.item(ticket)
    assert tuple(item[key] for key in selection) == tuple(original_item[key] for key in selection)
    assert lineage() == original_lineage
    assert run.pull(original_pull["number"]) == original_pull
    assert run.stub._ref(publication["branch"]) == head
    assert run.stub.events("push") == original_writes
    assert run.stub.events("pull_edit") == []

    run.label(ticket, "base:next", present=False)
    assert wait(lambda: run.item(ticket)["base_requested_branch"] is None)
    assert wait(
        lambda: any(
            line.startswith("Base:")
            and "`main`" in line
            and "`next`" not in line
            and "refus" not in line.lower()
            for comment in run.comments(ticket)
            for line in comment.splitlines()
        )
    ), "returning to the selected base left the refusal stuck"
    assert [row["id"] for row in run.requests(ticket)] == original_request_ids
    assert request_by_id(run, ticket, request["id"])["status"] == "running"
    assert lineage() == original_lineage
    assert run.pull(original_pull["number"]) == original_pull

    run.stub.check(head, "success", target="main")
    assert wait(lambda: ci_observed(run, head, "success"))
    assert_completed(run, ticket, request["id"])
    assert [row["id"] for row in run.requests(ticket)] == original_request_ids
    assert run.stub._ref(publication["branch"]) == head
    assert run.pull(original_pull["number"])["base"]["ref"] == "main"
    assert run.stub.events("push") == original_writes
    assert run.stub.events("pull_edit") == []
