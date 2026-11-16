"""Live candidate-install driver for the factory's automatic retries (ADR 0269).

Black box proof of #5424, #5426 and #5431 on a real install: the provider and
transient failure classes, the shared two-retry allowance and the operator
retry command, driven through the tracker, the compiled CLI and the cluster.

Opt in with ``CURIE_FACTORY_RETRY_LIVE=1``. Unset, the module skips. Set,
every input below is required, and a missing, malformed or unusable one fails
setup with a message naming it before anything is created.

Everything inside the changed path is the candidate: API, worker, reconciler,
runner and chart run from the caller's candidate images and chart, driven
through the candidate CLI. Two fixtures stand in for external services: the
model is the runner's offline fake tier (no paid call; a standing
``[fake:model-fault ...]`` directive per agent makes every turn fail the way
the provider does), and the forge and tracker is the recorded TLS GitHub stub
(tools/github-stub), one Pod in the owned namespace. Each leg owns one agent,
one trigger label and one issue (``factory_retry_live.LEGS``).

The legs
--------
a. Timed rate limit and timed daily budget, together. The failed row carries
   the condition and its reset; the successor is due at reset plus 60 s; the
   API and worker are restarted during the wait; nothing starts before due and
   the successor starts exactly once after.
b. An untimed rate limit takes the plain backoff (30 s, 20 percent jitter).
c. A generic 403 settles terminal: no successor, no slot spent, one
   needs-human notice.
d. A transport failure chain: three attempts, each with its own identity,
   claim and deadlines; a fourth is refused; one needs-human notice naming
   two of two retries used.
e. A mixed chain: an owner loss (the worker pod deleted while the first
   attempt holds its runner) and one transient failure spend the same two
   slots; a fourth is refused.
f. Closing the issue while the successor waits cancels it before it starts.
g. The compiled CLI's ``work-items ID --rerun``: a refusal on the exhausted
   item (leg d), a refusal on the terminal item (leg c), and a dry run that
   plans a successor and writes nothing. The dry run's item is a timed rate
   limit whose reset plus grace fell past the successor's waiting deadline
   when it failed, so automatic recovery admitted nothing and spent no slot;
   once the clock brings the reset inside a fresh waiting deadline, the
   operator plan schedules it.

Each leg rewrites only its own agent's runner SandboxTemplate directive. The
legs run in file order against one install; a later leg's failure does not
hide an earlier leg's result.

Runbook
-------
Run only against a throwaway cluster the caller owns::

    CURIE_FACTORY_RETRY_LIVE=1 \\
    CURIE_FACTORY_KUBE_CONTEXT=<kube context> \\
    CURIE_FACTORY_RETRY_NAMESPACE=<new namespace> \\
    CURIE_FACTORY_RETRY_RELEASE=curie-<suffix> \\
    CURIE_FACTORY_RETRY_PULL_POLICY=IfNotPresent \\
    CURIE_C4_CANDIDATE_CLI=<absolute path to the compiled candidate curie> \\
    CURIE_C4_CANDIDATE_IMAGES='<registry>/curie-{component}:<tag or @digest>' \\
    uv run pytest -q tools/factory-e2e/tests/test_factory_retry_live.py --reruns 0 -s

Inputs (see ``candidate_install.read_inputs``): the kube context of a cluster
that runs Kubernetes 1.30 or newer, the agent-sandbox controller and every
CRD in ``charts/curie/crds`` (none is installed here); a namespace that does
not exist yet; a Helm release ``curie-<suffix>`` of at most 30 characters,
never ``curie``; the compiled candidate CLI; an image reference template
expanded for ``api``, ``worker`` and ``runner``; and the pull policy.

Teardown is registered before each resource it removes is created and runs on
every exit: stop the port-forward and the pod watch, uninstall, delete the
namespace, delete every cluster-scoped object of the release, and prove each
absent. Evidence lines (``retry-live {...}``) carry ids, causes, times and
counts, never a credential or issue text.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import candidate_install as ci
import factory_e2e as fe
import factory_retry_live as frl
import pytest
import resource_retry_support as rrs
import tls_code_host as tch

if os.environ.get(frl.FLAG) != "1":
    pytest.skip(f"set {frl.FLAG}=1 to run the live factory retry driver", allow_module_level=True)

TEMPLATE_RESOURCE = "sandboxtemplates.extensions.agents.x-k8s.io"
ADMISSION_TIMEOUT_S = 300.0
# A fault turn fails as soon as its runner boots; a cold sandbox may take minutes.
FAILURE_TIMEOUT_S = 900.0
# How far ahead a timed leg's reset lies when its issue is seeded: room for
# admission, a cold sandbox, the failure and the restarts before it is due.
TIMED_LEAD_S = 420.0
# How far past the waiting deadline leg g's reset lies when its issue is seeded.
OPERATOR_LEAD_S = 300.0
# A settlement admits its successor in the same transaction; this covers the
# reconciler and the notice pass that follow.
SETTLE_S = 60.0
NOTICE_TIMEOUT_S = 180.0
ROLLOUT_TIMEOUT = "600s"
CLI_TIMEOUT_S = 120


@dataclass
class Live(ci.CandidateInstall):
    log_name: str = "retry-live"
    agents: dict[str, str] = field(default_factory=dict)
    issues: dict[str, int] = field(default_factory=dict)

    # --- setup -------------------------------------------------------------

    def deploy_agents(self) -> None:
        for leg in frl.LEGS:
            self.agents[leg.key] = self.create_agent(leg.agent, leg.label)
            self.deploy_bundle(leg.agent)
            self.log(f"agent {leg.agent} deployed for leg {leg.key}")

    def template(self, leg: frl.Leg) -> dict[str, Any]:
        raw = self.kubectl(
            "-n",
            self.inputs.namespace,
            "get",
            TEMPLATE_RESOURCE,
            frl.template_name(self.inputs.release, leg.agent),
            "-o",
            "json",
        )
        template: dict[str, Any] = json.loads(raw)
        return template

    def set_directive(self, leg: frl.Leg, directive: str) -> None:
        """Make the agent's next runner fail (or hold) the way ``directive`` says."""

        patch = frl.directive_patch(self.template(leg), directive)
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "patch",
            TEMPLATE_RESOURCE,
            frl.template_name(self.inputs.release, leg.agent),
            "--type=json",
            "-p",
            json.dumps(patch),
        )
        seen = frl.standing_directive(self.template(leg))
        assert seen == directive, f"{leg.agent}'s template reads back {seen!r}"
        self.log(f"leg {leg.key}: runner directive {directive}")

    def seed(self, leg: frl.Leg) -> int:
        issue = self.seed_issue(leg.title, frl.SEED_ISSUE, leg.label)
        self.issues[leg.key] = issue
        self.log(f"leg {leg.key}: issue {issue} labelled {leg.label}")
        return issue

    # --- reads ---------------------------------------------------------------

    def rows(self, leg: frl.Leg) -> list[dict[str, Any]]:
        return self.requests(self.agents[leg.key], self.issues[leg.key])

    def work_item(self, leg: frl.Leg) -> dict[str, Any]:
        agent = uuid.UUID(self.agents[leg.key])
        issue = int(self.issues[leg.key])
        row = self.sql(
            "SELECT coalesce(json_agg(json_build_object("
            "'id', w.id, 'version', w.version, "
            "'automatic_retries_used', w.automatic_retries_used, "
            "'cancelled_at', w.cancelled_at, 'readmit_request_id', w.readmit_request_id)), "
            "'[]'::json) FROM curie.work_items w "
            f"WHERE w.agent_id = '{agent}' AND w.tracker_kind = 'github' "
            f"AND w.tracker_issue_id = '{issue}'"
        )
        assert isinstance(row, list) and len(row) == 1, f"expected one work item, got {row!r}"
        item: dict[str, Any] = row[0]
        return item

    def comments(self, leg: frl.Leg) -> list[dict[str, Any]]:
        status, body = self.stub_api(
            "GET", f"/repos/{tch.STUB_REPO}/issues/{self.issues[leg.key]}/comments?per_page=100"
        )
        assert status == 200 and isinstance(body, list), f"comment listing answered {status}"
        return body

    def labels(self, leg: frl.Leg) -> set[str]:
        status, body = self.stub_api("GET", f"/repos/{tch.STUB_REPO}/issues/{self.issues[leg.key]}")
        assert status == 200, f"issue read answered {status}"
        return {label["name"] for label in body.get("labels") or []}

    def close_issue(self, leg: frl.Leg) -> None:
        status, issue = self.stub_api(
            "PATCH", f"/repos/{tch.STUB_REPO}/issues/{self.issues[leg.key]}", {"state": "closed"}
        )
        assert status == 200 and issue.get("state") == "closed", f"close answered {status}"

    # --- disruption ---------------------------------------------------------

    def restart(self, component: str) -> None:
        workload = f"deployment/{self.inputs.release}-{component}"
        self.kubectl("-n", self.inputs.namespace, "rollout", "restart", workload)
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "rollout",
            "status",
            workload,
            f"--timeout={ROLLOUT_TIMEOUT}",
        )
        self.log(f"{component} restarted")

    def delete_worker_pods(self) -> list[str]:
        names = self.kubectl(
            "-n",
            self.inputs.namespace,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/component=worker",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ).split()
        assert names, "no worker pod to delete"
        self.kubectl(
            "-n",
            self.inputs.namespace,
            "delete",
            "pod",
            *names,
            "--grace-period=1",
            "--wait=false",
        )
        self.log(f"worker pods deleted: {names}")
        return names

    # --- the compiled CLI ---------------------------------------------------

    def rerun(self, work_item: str, version: int, *, dry_run: bool) -> frl.CliResult:
        config_dir = Path(tempfile.mkdtemp(prefix="cli-", dir=self.workdir))
        env = {**os.environ, "CURIE_API_KEY": self.api_key, "CURIE_CONFIG_DIR": str(config_dir)}
        argv = [
            self.inputs.cli,
            "--json",
            "cluster",
            "--context",
            self.inputs.context,
            "work-items",
            work_item,
            "--rerun",
            "--request-id",
            str(uuid.uuid4()),
            "--expected-version",
            str(version),
            "--reason",
            "Factory retry live proof",
            "--namespace",
            self.inputs.namespace,
            "--release",
            self.inputs.release,
            "--api-url",
            self.api_url,
            "--dry-run" if dry_run else "--yes",
        ]
        result = subprocess.run(
            argv, capture_output=True, text=True, env=env, check=False, timeout=CLI_TIMEOUT_S
        )
        stdout = fe._redact(result.stdout, [self.api_key])
        stderr = fe._redact(result.stderr, [self.api_key])
        return frl.CliResult(result.returncode, stdout, stderr)

    def evidence(self, leg: str, **facts: Any) -> None:
        fe.log("retry-live " + json.dumps({"leg": leg, **facts}, sort_keys=True, default=str))


# --------------------------------------------------------------------------
# Waits
# --------------------------------------------------------------------------


def wait_for(
    what: str, timeout: float, probe: Callable[[], Any], observed: Callable[[], Any]
) -> Any:
    return ci.wait_for(what, timeout, probe, observed)


def require(problems: list[str], what: str, observed: Any) -> None:
    assert not problems, f"{what}: {'; '.join(problems)}; observed {observed}"


def briefs(live: Live, leg: frl.Leg) -> Callable[[], list[dict[str, Any] | None]]:
    return lambda: [frl.brief(row) for row in live.rows(leg)]


def nth(
    live: Live, leg: frl.Leg, count: int, *, terminal: bool, timeout: float
) -> list[dict[str, Any]]:
    """The first ``count`` requests of the leg's item, the last terminal when asked."""

    def probe() -> list[dict[str, Any]] | None:
        rows = live.rows(leg)
        if len(rows) < count:
            return None
        if terminal and rows[count - 1]["status"] not in ci.TERMINAL:
            return None
        return rows

    rows: list[dict[str, Any]] = wait_for(
        f"leg {leg.key} request {count}{' to settle' if terminal else ''}",
        timeout,
        probe,
        briefs(live, leg),
    )
    return rows


def hold_count(live: Live, leg: frl.Leg, count: int) -> list[dict[str, Any]]:
    """Watch the item for a settle window and require it to hold ``count`` requests."""

    def check() -> None:
        rows = live.rows(leg)
        assert len(rows) == count, (
            f"leg {leg.key}: expected {count} requests, the item holds {len(rows)}: "
            f"{[frl.brief(r) for r in rows]}"
        )

    rrs.observe_for(SETTLE_S, check, interval=5)
    rows = live.rows(leg)
    check()
    return rows


def wait_notices(live: Live, leg: frl.Leg, *, retrying: int, needs_human: int) -> frl.Notices:
    def probe() -> frl.Notices | None:
        found = frl.notices(live.comments(leg))
        return found if (found.retrying, found.needs_human) == (retrying, needs_human) else None

    found: frl.Notices = wait_for(
        f"leg {leg.key}: {retrying} retrying and {needs_human} needs-human notices",
        NOTICE_TIMEOUT_S,
        probe,
        lambda: frl.notices(live.comments(leg)),
    )
    return found


def wait_terminal(live: Live, leg: frl.Leg, request_id: str, timeout: float) -> dict[str, Any]:
    row: dict[str, Any] = wait_for(
        f"leg {leg.key} request {request_id} to settle",
        timeout,
        lambda: next(
            (r for r in live.rows(leg) if r["id"] == request_id and r["status"] in ci.TERMINAL),
            None,
        ),
        briefs(live, leg),
    )
    return row


def wait_started(live: Live, leg: frl.Leg, request_id: str, timeout: float) -> dict[str, Any]:
    row: dict[str, Any] = wait_for(
        f"leg {leg.key} request {request_id} to start",
        timeout,
        lambda: next(
            (r for r in live.rows(leg) if r["id"] == request_id and r.get("started_at")), None
        ),
        briefs(live, leg),
    )
    return row


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

PROVED: dict[str, dict[str, Any]] = {}


@pytest.fixture(scope="module")
def inputs() -> ci.Inputs:
    try:
        return ci.read_inputs(dict(os.environ), frl.NAMES)
    except ci.Refused as exc:
        refusal = str(exc)
    pytest.fail(refusal, pytrace=False)


@pytest.fixture(autouse=True)
def candidate_curie(inputs: ci.Inputs, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The real compiled candidate CLI, in place of conftest's fake CURIE_BIN."""

    monkeypatch.setenv("CURIE_BIN", inputs.cli)
    return Path(inputs.cli)


@pytest.fixture(scope="module")
def live(inputs: ci.Inputs) -> Iterator[Live]:
    workdir = Path(tempfile.mkdtemp(prefix="retry-live-"))
    workdir.chmod(0o700)
    harness = Live(inputs=inputs, workdir=workdir)
    harness.teardown.push("private workdir", lambda: shutil.rmtree(workdir, ignore_errors=True))
    try:
        try:
            harness.preflight()
        except ci.Refused as exc:
            refusal: str | None = f"refused: {exc}"
        else:
            refusal = None
        if refusal is not None:
            pytest.fail(refusal, pytrace=False)
        harness.create_namespace()
        harness.start_stub(frl.SEED_FILES, frl.SEED_ISSUE)
        harness.install(frl.install_values(harness.base_values()))
        harness.port_forward()
        harness.start_pod_watch()
        harness.deploy_agents()
        yield harness
    finally:
        results = harness.teardown.run()
        failed = [f"{r['step']}: {r['detail']}" for r in results if not r["ok"]]
        for result in results:
            harness.log(f"teardown {result['step']}: {'ok' if result['ok'] else 'FAILED'}")
        if failed:
            pytest.fail("teardown did not prove every owned resource absent: " + "; ".join(failed))


# --------------------------------------------------------------------------
# The legs
# --------------------------------------------------------------------------


def test_c_generic_403_settles_terminal_with_no_slot_spent(live: Live) -> None:
    leg = frl.LEG_C
    live.set_directive(leg, frl.GENERIC_403)
    live.seed(leg)
    rows = nth(live, leg, 1, terminal=True, timeout=ADMISSION_TIMEOUT_S + FAILURE_TIMEOUT_S)
    first = rows[0]
    require(
        frl.carrier_problems(first, cause=frl.CAUSE_CREDENTIAL, reason=None, retry_at=None),
        "the generic 403 carrier",
        frl.brief(first),
    )
    rows = hold_count(live, leg, 1)
    item = live.work_item(leg)
    assert item["automatic_retries_used"] == 0, f"a terminal failure spent a slot: {item}"
    found = wait_notices(live, leg, retrying=0, needs_human=1)
    require(frl.notice_problems(found, retrying=0, needs_human=1), "leg c notices", found)
    assert frl.NEEDS_HUMAN_LABEL in live.labels(leg), "the issue carries no needs-human label"
    wait = frl.seconds_between(first.get("created_at"), first.get("wait_deadline"))
    PROVED["c"] = {"item": item, "request": first["id"], "wait_budget_s": wait}
    live.evidence("c", request=frl.brief(rows[0]), work_item=item, notices=found.needs_human)


def test_d_unreachable_chain_stops_after_three_attempts(live: Live) -> None:
    leg = frl.LEG_D
    live.set_directive(leg, frl.UNREACHABLE)
    live.seed(leg)
    rows = nth(live, leg, 3, terminal=True, timeout=ADMISSION_TIMEOUT_S + 3 * FAILURE_TIMEOUT_S)
    rows = hold_count(live, leg, 3)
    origin = rows[0]
    for row in rows:
        require(
            frl.carrier_problems(row, cause=frl.CAUSE_UNREACHABLE, reason=None, retry_at=None),
            f"attempt {row['sequence']}'s carrier",
            frl.brief(row),
        )
    for slot, (before, after) in enumerate(zip(rows, rows[1:], strict=False), start=1):
        require(
            frl.successor_problems(before, after, origin=origin),
            f"retry {slot}'s lineage",
            [frl.brief(before), frl.brief(after)],
        )
        require(
            frl.scheduled_problems(before, after, due=None, work_slot=slot),
            f"retry {slot}'s backoff",
            [frl.brief(before), frl.brief(after)],
        )
        require(frl.started_once_problems(after), f"retry {slot}'s start", frl.brief(after))
    require(frl.fresh_attempt_problems(rows), "fresh attempts", [frl.brief(r) for r in rows])
    item = live.work_item(leg)
    assert item["automatic_retries_used"] == frl.AUTOMATIC_RETRY_LIMIT, f"slots: {item}"
    found = wait_notices(live, leg, retrying=2, needs_human=1)
    require(
        frl.notice_problems(found, retrying=2, needs_human=1, used=frl.AUTOMATIC_RETRY_LIMIT),
        "leg d notices",
        found,
    )
    assert frl.NEEDS_HUMAN_LABEL in live.labels(leg), "the issue carries no needs-human label"
    PROVED["d"] = {"item": item, "request": rows[-1]["id"]}
    live.evidence("d", requests=[frl.brief(r) for r in rows], work_item=item)


def test_b_untimed_rate_limit_takes_the_plain_backoff(live: Live) -> None:
    leg = frl.LEG_B
    live.set_directive(leg, frl.RATE_LIMIT_UNTIMED)
    live.seed(leg)
    rows = nth(live, leg, 2, terminal=False, timeout=ADMISSION_TIMEOUT_S + FAILURE_TIMEOUT_S)
    # The successor ends terminal, so the chain stops after one retry.
    live.set_directive(leg, frl.GENERIC_403)
    first, successor = rows[0], rows[1]
    require(
        frl.carrier_problems(
            first, cause=frl.CAUSE_RATE_LIMITED, reason="rate_limited", retry_at=None
        ),
        "the untimed rate limit carrier",
        frl.brief(first),
    )
    require(frl.successor_problems(first, successor, origin=first), "lineage", frl.brief(successor))
    require(
        frl.scheduled_problems(first, successor, due=None, work_slot=1),
        "the plain backoff",
        [frl.brief(first), frl.brief(successor)],
    )
    started = wait_started(live, leg, successor["id"], FAILURE_TIMEOUT_S)
    require(frl.started_once_problems(started), "the successor's start", frl.brief(started))
    wait_terminal(live, leg, successor["id"], FAILURE_TIMEOUT_S)
    rows = hold_count(live, leg, 2)
    item = live.work_item(leg)
    assert item["automatic_retries_used"] == 1, f"slots: {item}"
    live.evidence("b", requests=[frl.brief(r) for r in rows], work_item=item)


def test_a_timed_waits_start_once_after_reset_through_restarts(live: Live) -> None:
    legs = (frl.LEG_A_RATE, frl.LEG_A_BUDGET)
    reset = int(time.time() + TIMED_LEAD_S)
    due = reset + frl.RESET_GRACE_S
    live.set_directive(frl.LEG_A_RATE, frl.rate_limit(reset))
    live.set_directive(frl.LEG_A_BUDGET, frl.daily_budget(reset))
    for leg in legs:
        live.seed(leg)
    expected = {
        frl.LEG_A_RATE.key: (frl.CAUSE_RATE_LIMITED, "rate_limited"),
        frl.LEG_A_BUDGET.key: (frl.CAUSE_CREDIT, "usage_reset"),
    }
    successors: dict[str, dict[str, Any]] = {}
    for leg in legs:
        rows = nth(live, leg, 2, terminal=False, timeout=ADMISSION_TIMEOUT_S + FAILURE_TIMEOUT_S)
        first, successor = rows[0], rows[1]
        cause, reason = expected[leg.key]
        require(
            frl.carrier_problems(first, cause=cause, reason=reason, retry_at=float(reset)),
            f"leg {leg.key} carrier",
            frl.brief(first),
        )
        require(
            frl.successor_problems(first, successor, origin=first), "lineage", frl.brief(successor)
        )
        require(
            frl.scheduled_problems(first, successor, due=due, work_slot=1),
            f"leg {leg.key} due time",
            frl.brief(successor),
        )
        successors[leg.key] = successor
        # The successor ends terminal, so the chain stops after one retry.
        live.set_directive(leg, frl.GENERIC_403)
    claims_before = live.sandbox_claims()
    for component in ("api", "worker"):
        live.restart(component)
    live.port_forward()
    assert time.time() < due - 30, (
        f"the restarts ended {time.time() - due + 30:.0f}s too late to prove the wait; "
        "raise TIMED_LEAD_S"
    )
    generations: dict[str, set[int]] = {leg.key: set() for leg in legs}

    def still_waiting() -> None:
        for leg in legs:
            row = next(r for r in live.rows(leg) if r["id"] == successors[leg.key]["id"])
            generations[leg.key].add(int(row["dispatch_generation"]))
            not_before = frl.epoch(row.get("dispatch_not_before"))
            assert row["status"] == "waiting" and not frl.unstarted_problems(row), (
                f"leg {leg.key}'s successor moved before its due time: {frl.brief(row)}"
            )
            assert not_before is not None and not_before + frl.TIME_SLACK_S >= due, (
                f"leg {leg.key}'s due time moved earlier: {frl.brief(row)}"
            )

    rrs.observe_for(max(0.0, due - time.time() - 5), still_waiting, interval=5)
    new_claims = live.sandbox_claims() - claims_before
    assert not new_claims, f"a sandbox was claimed during the wait: {sorted(new_claims)}"
    for leg in legs:
        started = wait_started(live, leg, successors[leg.key]["id"], FAILURE_TIMEOUT_S)
        require(frl.started_once_problems(started), f"leg {leg.key} start", frl.brief(started))
        started_at = frl.epoch(started["started_at"])
        assert started_at is not None and started_at + frl.TIME_SLACK_S >= due, (
            f"leg {leg.key}'s successor started before reset plus grace: {frl.brief(started)}"
        )
        wait_terminal(live, leg, successors[leg.key]["id"], FAILURE_TIMEOUT_S)
        rows = hold_count(live, leg, 2)
        item = live.work_item(leg)
        assert item["automatic_retries_used"] == 1, f"slots: {item}"
        live.evidence(
            leg.key,
            reset=reset,
            due=due,
            dispatch_generations=sorted(generations[leg.key]),
            requests=[frl.brief(r) for r in rows],
            work_item=item,
        )


def test_f_closing_the_issue_cancels_the_waiting_successor(live: Live) -> None:
    leg = frl.LEG_F
    reset = int(time.time() + TIMED_LEAD_S + 600)
    live.set_directive(leg, frl.rate_limit(reset))
    live.seed(leg)
    rows = nth(live, leg, 2, terminal=False, timeout=ADMISSION_TIMEOUT_S + FAILURE_TIMEOUT_S)
    live.set_directive(leg, frl.GENERIC_403)
    first, successor = rows[0], rows[1]
    require(
        frl.scheduled_problems(first, successor, due=reset + frl.RESET_GRACE_S, work_slot=1),
        "the waiting successor",
        frl.brief(successor),
    )
    live.close_issue(leg)
    cancelled = wait_terminal(live, leg, successor["id"], ADMISSION_TIMEOUT_S)
    assert (
        cancelled["status"] == "cancelled" and cancelled["terminal_cause"] in frl.CANCEL_CAUSES
    ), f"the closed issue's successor ended {frl.brief(cancelled)}"
    require(frl.unstarted_problems(cancelled), "the cancelled successor", frl.brief(cancelled))
    rows = hold_count(live, leg, 2)
    item = live.work_item(leg)
    assert item["cancelled_at"], f"the work item is not cancelled: {item}"
    live.evidence("f", reset=reset, requests=[frl.brief(r) for r in rows], work_item=item)


def test_e_owner_loss_and_a_transient_share_two_slots(live: Live) -> None:
    leg = frl.LEG_E
    live.set_directive(leg, frl.HOLD)
    live.seed(leg)
    first = nth(live, leg, 1, terminal=False, timeout=ADMISSION_TIMEOUT_S)[0]
    first = wait_started(live, leg, first["id"], FAILURE_TIMEOUT_S)
    claim = str(first["runtime_claim_name"])

    def runner_running() -> bool:
        return any(
            (pod.get("status") or {}).get("phase") == "Running" for pod in live.pods_for(claim)
        )

    wait_for("the holding runner to run", 600, runner_running, briefs(live, leg))
    # Every later attempt fails as a transport error.
    live.set_directive(leg, frl.UNREACHABLE)
    deleted = live.delete_worker_pods()
    lost = wait_terminal(live, leg, first["id"], FAILURE_TIMEOUT_S + 900)
    assert lost["terminal_cause"] == frl.CAUSE_OWNER_LOST, (
        f"the first attempt did not end in owner loss: {frl.brief(lost)}"
    )
    rows = nth(live, leg, 3, terminal=True, timeout=ADMISSION_TIMEOUT_S + 3 * FAILURE_TIMEOUT_S)
    rows = hold_count(live, leg, 3)
    origin, owner_loss_retry, transient_retry = rows
    require(
        frl.successor_problems(origin, owner_loss_retry, origin=origin, owner_loss=True),
        "the owner-loss successor",
        frl.brief(owner_loss_retry),
    )
    require(
        frl.successor_problems(owner_loss_retry, transient_retry, origin=origin),
        "the transient successor",
        frl.brief(transient_retry),
    )
    for row in (owner_loss_retry, transient_retry):
        require(
            frl.carrier_problems(row, cause=frl.CAUSE_UNREACHABLE, reason=None, retry_at=None),
            f"attempt {row['sequence']}'s carrier",
            frl.brief(row),
        )
        require(frl.started_once_problems(row), f"attempt {row['sequence']}", frl.brief(row))
    require(
        frl.scheduled_problems(owner_loss_retry, transient_retry, due=None, work_slot=2),
        "the transient retry's backoff",
        [frl.brief(owner_loss_retry), frl.brief(transient_retry)],
    )
    require(frl.fresh_attempt_problems(rows), "fresh attempts", [frl.brief(r) for r in rows])
    item = live.work_item(leg)
    assert item["automatic_retries_used"] == frl.AUTOMATIC_RETRY_LIMIT, f"slots: {item}"
    found = wait_notices(live, leg, retrying=2, needs_human=1)
    require(
        frl.notice_problems(found, retrying=2, needs_human=1, used=frl.AUTOMATIC_RETRY_LIMIT),
        "leg e notices",
        found,
    )
    live.evidence(
        "e", deleted_worker_pods=len(deleted), requests=[frl.brief(r) for r in rows], work_item=item
    )


def test_g_operator_rerun_refuses_exhausted_and_terminal_and_plans_a_dry_run(live: Live) -> None:
    missing = [key for key in ("c", "d") if key not in PROVED]
    assert not missing, f"leg g needs legs {missing} to have passed first"
    budget = PROVED["c"]["wait_budget_s"]
    assert isinstance(budget, float) and budget > 0, f"unreadable waiting budget {budget!r}"

    # Refusals: nothing is written to either item.
    for key, code in (("d", "allowance_exhausted"), ("c", "not_retryable")):
        leg = frl.LEG_D if key == "d" else frl.LEG_C
        before, count = live.work_item(leg), len(live.rows(leg))
        result = live.rerun(str(before["id"]), int(before["version"]), dry_run=False)
        require(frl.refusal_problems(result, code), f"the {key} refusal", result)
        after = live.work_item(leg)
        require(
            frl.unchanged_problems(before, after, count, len(live.rows(leg))),
            f"the {key} refusal wrote",
            after,
        )
        live.evidence("g", item=key, exit=result.code, refusal=code)

    # The dry run's item: a reset whose grace lands past a fresh waiting deadline.
    leg = frl.LEG_G
    reset = int(time.time() + budget + OPERATOR_LEAD_S)
    due = reset + frl.RESET_GRACE_S
    live.set_directive(leg, frl.rate_limit(reset))
    live.seed(leg)
    first = nth(live, leg, 1, terminal=True, timeout=ADMISSION_TIMEOUT_S + FAILURE_TIMEOUT_S)[0]
    live.set_directive(leg, frl.GENERIC_403)
    require(
        frl.carrier_problems(
            first, cause=frl.CAUSE_RATE_LIMITED, reason="rate_limited", retry_at=reset
        ),
        "leg g carrier",
        frl.brief(first),
    )
    settled = frl.epoch(first["terminal_at"])
    assert settled is not None and settled + budget <= due, (
        f"leg g failed {settled + budget - due if settled else '?'}s too late for its reset to "
        "fall past the waiting deadline; raise OPERATOR_LEAD_S"
    )
    hold_count(live, leg, 1)
    before = live.work_item(leg)
    assert before["automatic_retries_used"] == 0, f"an unadmitted retry spent a slot: {before}"
    # Wait until a waiting deadline set now would cover the reset plus grace.
    rrs.observe_for(max(0.0, due - budget - time.time() + 10), lambda: None, interval=10)
    count = len(live.rows(leg))
    result = live.rerun(str(before["id"]), int(before["version"]), dry_run=True)
    require(
        frl.dry_run_problems(
            result,
            work_item_id=str(before["id"]),
            predecessor_id=str(first["id"]),
            cause=frl.CAUSE_RATE_LIMITED,
            due=float(due),
        ),
        "the dry run plan",
        result,
    )
    after = live.work_item(leg)
    require(
        frl.unchanged_problems(before, after, count, len(live.rows(leg))),
        "the dry run wrote",
        after,
    )
    live.evidence("g", item="g", exit=result.code, plan=result.payload(), work_item=after)
