"""Drive the ``triage-to-pr`` scenario on the scripted command's shared install.

It runs after the issue-to-pull-request scenario, on the same cluster, stub,
model endpoint and API. Three seeded reports are labelled at once:

1. a reproducible defect, which ends in a published ticket, an automatic
   implementation run on it and that run's pull request;
2. the same defect labelled by a second person whose permission the stub
   revokes as soon as the triage session asks its first model turn, so the
   ticket is published and the handoff is refused;
3. a report that does not reproduce, which ends in findings.

Every observation is external: GitHub stub state, the Slack recorder, the
model lanes and read-only SQL. ``triage_scenario`` holds the assertions.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

import factory_e2e as fe
import scripted_lanes as lanes
import triage_scenario as ts

REPO_ROOT = Path(__file__).resolve().parents[2]
TRIAGE_BUNDLE = REPO_ROOT / "examples/factory-triage"
TICKET_TRANSCRIPT = REPO_ROOT / "tools/model-script/transcripts/triage-ticket.json"
FINDINGS_TRANSCRIPT = REPO_ROOT / "tools/model-script/transcripts/triage-findings.json"
TRIAGE_AGENT = "factory-triage"
RECORDED_ISSUE = 3815
REPOSITORY = "acme-corp/acme-bot"
# The intake's open-issue list read, once per served label per pass. Both
# agents on the fixture repository serve one label each.
INTAKE_LIST_PATH = f"/repos/{REPOSITORY}/issues"
INTAKE_LABELS = (ts.IMPLEMENTATION_LABEL, ts.TRIAGE_LABEL)
# Replay is judged quiet only after this many intake passes have read the
# tracker since the replay.
REPLAY_POLLS = 3
RUNTIME_SETTLE_SECONDS = 180

# Run inside the API pod with the source request id as its only argument. It
# replays the internal selection event of that request's delivered ticket:
# open_handoff again for the same receipt in a committed session, then one
# hand_off attempt on the handoff, built from the API's own settings, engine
# and code host trust. It prints one JSON line.
REPLAY_SELECTION_SNIPPET = """
import asyncio
import json
import sys
import uuid

import httpx
from sqlalchemy import select

from curie_api.code_host_trust import code_host_verify
from curie_api.config import get_settings
from curie_api.db import create_engine, create_sessionmaker
from curie_api.factory_triage_handoff import hand_off, open_handoff
from curie_api.forges.types import TrackerIssueRef
from curie_api.models import FactoryTrackerResultReceipt, FactoryTriageHandoff


async def replay(request_id):
    settings = get_settings()
    engine = create_engine()
    sessionmaker = create_sessionmaker(engine)
    try:
        async with sessionmaker() as session:
            receipts = (await session.scalars(select(FactoryTrackerResultReceipt).where(
                FactoryTrackerResultReceipt.execution_request_id == request_id
            ))).all()
            handoffs = (await session.scalars(select(FactoryTriageHandoff).where(
                FactoryTriageHandoff.source_execution_request_id == request_id
            ))).all()
            if len(receipts) != 1 or len(handoffs) != 1:
                raise SystemExit("expected one receipt and one handoff for the source request")
            receipt, handoff = receipts[0], handoffs[0]
            if receipt.output_issue_id is None or receipt.output_url is None:
                raise SystemExit("the source receipt names no output issue")
            handoff_id, before = handoff.id, handoff.outcome
            output = TrackerIssueRef(
                kind=receipt.output_tracker_kind,
                host=receipt.output_tracker_host,
                scope_id=receipt.output_scope_id,
                issue_id=receipt.output_issue_id,
            )
            reopened = await open_handoff(
                session,
                delivery_id=receipt.delivery_id,
                execution_request_id=request_id,
                output=output,
                output_url=receipt.output_url,
                output_body_sha256=receipt.output_body_sha256,
            )
            await session.commit()
        async with httpx.AsyncClient(
            timeout=settings.github_app_timeout_seconds, verify=code_host_verify(settings)
        ) as client:
            attempt = await hand_off(
                sessionmaker,
                handoff_id,
                settings=settings,
                client=client,
                owner=f"scripted-replay-{uuid.uuid4().hex}",
            )
        async with sessionmaker() as session:
            after = await session.scalar(
                select(FactoryTriageHandoff.outcome).where(FactoryTriageHandoff.id == handoff_id)
            )
    finally:
        await engine.dispose()
    print(json.dumps({
        "handoff_id": str(handoff_id),
        "reopened": None if reopened is None else str(reopened),
        "attempt": attempt,
        "outcome_before": before,
        "outcome_after": after,
    }))


asyncio.run(replay(uuid.UUID(sys.argv[1])))
"""

SOURCES = (
    (
        ts.SOURCE_TICKET,
        "Kelvin conversions are rejected",
        "Running `python3 -m unitconv 0 c k` prints an error instead of 273.15. "
        "`unitconv` should accept Kelvin like Celsius and Fahrenheit.\n",
        "ticket",
    ),
    (
        ts.SOURCE_REFUSED,
        "Kelvin input fails",
        "Converting 0 Celsius to Kelvin with `python3 -m unitconv 0 c k` fails.\n",
        "refused",
    ),
    (
        ts.SOURCE_FINDINGS,
        "Celsius to Fahrenheit looks wrong for 100 degrees",
        "A user reports that `convert(100, \"c\", \"f\")` does not return 212. "
        "Please check whether this reproduces on the default branch.\n",
        "findings",
    ),
)


class TriageScenarioError(RuntimeError):
    """The triage scenario could not be driven to an observation."""


def build_router(model_module: Any) -> lanes.LaneRouter:
    """The triage lanes; the recorded issue keeps the endpoint's own transcript."""

    router = lanes.LaneRouter({lanes.TRIAGE: lanes.bundle_system_prompt(TRIAGE_BUNDLE)})
    ticket = model_module.load_transcript(TICKET_TRANSCRIPT)
    findings = model_module.load_transcript(FINDINGS_TRANSCRIPT)
    router.add(lanes.Lane("triage-ticket", lanes.TRIAGE, ts.SOURCE_TICKET, list(ticket)))
    router.add(lanes.Lane("triage-refused", lanes.TRIAGE, ts.SOURCE_REFUSED, list(ticket)))
    router.add(lanes.Lane("triage-findings", lanes.TRIAGE, ts.SOURCE_FINDINGS, list(findings)))
    return router


def output_resolver(stub: Any, recorded: list[dict[str, Any]]) -> Any:
    """Route the implementation agent on the ticket source's linked output issue.

    The output's number exists only once the platform creates it, so the lane
    is created on its first request, from the recorded issue's lane.
    """

    def resolve(agent: str, issue: int) -> lanes.Lane | None:
        if agent != lanes.IMPLEMENTATION:
            return None
        request_id = source_request_id(stub, ts.SOURCE_TICKET)
        found = stub._get("issue", issue)
        if request_id is None or found is None:
            return None
        if ts.linked_marker(request_id) not in (found.get("body") or ""):
            return None
        exchanges = lanes.retarget(recorded, RECORDED_ISSUE, issue)
        return lanes.Lane("implementation-output", lanes.IMPLEMENTATION, issue, exchanges)

    return resolve


def revoke_on_first_turn(stub: Any) -> Any:
    """Revoke the refused source's labeller after admission, before publication."""

    def hook(lane: lanes.Lane, ordinal: int) -> None:
        if lane.name == "triage-refused" and ordinal == 0:
            stub.revoke_permission(ts.REFUSED_LABELLER["login"])

    return hook


def source_request_id(stub: Any, source: int) -> str | None:
    """The source's request, read from its marked result comment."""

    for comment in stub._rows("comment"):
        if comment["issue_number"] != source:
            continue
        marker = "curie-tracker-result:"
        at = comment["body"].find(marker)
        if at >= 0:
            candidate = comment["body"][at + len(marker) : at + len(marker) + 36]
            try:
                return str(uuid.UUID(candidate))
            except ValueError:
                continue
    return None


def observation_sql(issues: list[int]) -> str:
    """One read-only JSON observation of the scenario's durable records."""

    numbers = ",".join(f"'{int(number)}'" for number in issues)
    return (
        "SELECT json_build_object("
        "'work_items',(SELECT coalesce(json_agg(json_build_object("
        "'id',w.id,'agent_id',w.agent_id,'trigger_label',w.trigger_label,"
        "'issue',w.tracker_issue_id,'cancelled_at',w.cancelled_at)),'[]'::json) "
        f"FROM curie.work_items w WHERE w.tracker_issue_id IN ({numbers})),"
        "'requests',(SELECT coalesce(json_agg(json_build_object("
        "'id',r.id,'work_item_id',r.work_item_id,'issue',w.tracker_issue_id,"
        "'requester',r.requester,'status',r.status,'completion_contract',r.completion_contract,"
        "'model_session_reserved_at',r.model_session_reserved_at,"
        "'execution_deadline',r.execution_deadline,'created_at',r.created_at,"
        "'terminal_at',r.terminal_at)),'[]'::json) "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
        f"WHERE w.tracker_issue_id IN ({numbers})),"
        "'receipts',(SELECT coalesce(json_agg(json_build_object("
        "'execution_request_id',c.execution_request_id,'result_kind',c.result_kind,"
        "'reason',c.reason,'revision',c.revision,'result_comment_id',c.result_comment_id,"
        "'output_issue_id',c.output_issue_id,'output_url',c.output_url,"
        "'output_body_sha256',c.output_body_sha256)),'[]'::json) "
        "FROM curie.factory_tracker_result_receipts c),"
        "'publication_requests',(SELECT coalesce(json_agg(p.execution_request_id),'[]'::json) "
        "FROM curie.publications p),"
        "'threads',(SELECT coalesce(json_agg(json_build_object("
        "'work_item_id',t.work_item_id,'channel',t.channel,'root_ts',t.root_ts)),'[]'::json) "
        "FROM curie.factory_slack_threads t),"
        "'totals',json_build_object("
        "'work_items',(SELECT count(*) FROM curie.work_items),"
        "'execution_requests',(SELECT count(*) FROM curie.execution_requests),"
        "'receipts',(SELECT count(*) FROM curie.factory_tracker_result_receipts)))"
    )


def by_issue(rows: list[dict[str, Any]], issue: int) -> list[dict[str, Any]]:
    return [row for row in rows if str(row.get("issue")) == str(issue)]


class TriageToPr:
    """One run of the scenario against a ScriptedRun's live install."""

    def __init__(self, driver: Any, router: lanes.LaneRouter, recorder: lanes.SlackRecorder):
        self.driver = driver
        self.stub = driver.stub
        self.router = router
        self.recorder = recorder
        self.before: dict[int, dict[str, Any]] = {}
        self.evidence: dict[str, Any] = {"scenario": "triage-to-pr", "result": "failed"}
        self.triage_agent: str | None = None
        self.output: int | None = None

    # Setup -----------------------------------------------------------------

    def api(self, method: str, path: str, body: Any = None) -> Any:
        status, answer = fe.http_json(
            method,
            self.driver.env["CURIE_API_URL"] + path,
            headers={"X-API-Key": self.driver.env["CURIE_API_KEY"]},
            body=body,
        )
        if status not in {200, 201}:
            raise TriageScenarioError(f"{method} {path.split('/')[1]} returned HTTP {status}")
        return answer

    def setup(self) -> None:
        driver = self.driver
        bundle = driver.workdir / "triage-bundle"
        driver.command(
            driver.curie, "example", "factory-triage", "render",
            "--out", str(bundle), "--implementation-label", ts.IMPLEMENTATION_LABEL,
        )
        if lanes.bundle_system_prompt(bundle) not in self.router.agent_markers.values():
            raise TriageScenarioError("the rendered triage bundle names another system prompt")
        agent = self.api(
            "POST",
            "/agents",
            {
                "name": TRIAGE_AGENT,
                "repo_full_name": "acme-corp/acme-bot",
                "channel": {
                    "kind": "github",
                    "address": "acme-corp/acme-bot",
                    "trigger_label": ts.TRIAGE_LABEL,
                },
            },
        )
        self.triage_agent = str(agent["id"])
        common = [
            "--namespace", driver.namespace, "--release", "curie",
            "--api-url", driver.env["CURIE_API_URL"],
        ]
        driver.command(
            driver.curie, "cluster", "--context", driver.context, "deploy",
            "--plugin-dir", str(bundle), "--agent", TRIAGE_AGENT, "--env", "prod",
            "--chart", str(REPO_ROOT / "charts/curie"), *common, timeout=300,
        )
        driver.kubectl(
            "-n", driver.namespace, "rollout", "status",
            "deployment/curie-channel-gateway", "--timeout=300s",
        )
        self.api(
            "POST", f"/agents/{self.triage_agent}/channels",
            {"kind": "slack", "address": ts.SLACK_CHANNEL},
        )
        self.api(
            "PATCH", f"/agents/{self.triage_agent}", {"factory_slack_channel": ts.SLACK_CHANNEL}
        )
        self.stub.set_permission(
            ts.REFUSED_LABELLER["login"], "write", user=ts.REFUSED_LABELLER
        )
        self.evidence.update(triage_agent_id=self.triage_agent, slack_channel=ts.SLACK_CHANNEL)

    def label_sources(self, github: Any) -> None:
        """One human selection per source: the base label, then the triage label."""

        for number, title, body, kind in SOURCES:
            labeller = ts.REFUSED_LABELLER if kind == "refused" else github.HUMAN
            issue = self.stub._issue(number, title, body, [ts.BASE_LABEL, ts.TRIAGE_LABEL])
            issue["user"] = labeller
            self.stub._put("issue", number, issue)
            self.before[number] = json.loads(json.dumps(issue))
            self.stub._label_event(number, ts.BASE_LABEL, "labeled", labeller)
            self.stub._label_event(number, ts.TRIAGE_LABEL, "labeled", labeller)

    # Waiting ---------------------------------------------------------------

    def check_boundaries(self) -> None:
        if self.driver.model.unexpected:
            raise TriageScenarioError("unexpected model request")
        if self.router.unrouted:
            raise TriageScenarioError(f"unrouted model request: {self.router.unrouted[0]}")
        if self.stub.unknown_requests:
            raise TriageScenarioError("unsupported stub request")

    def settled(self) -> bool:
        request_id = source_request_id(self.stub, ts.SOURCE_TICKET)
        refused_id = source_request_id(self.stub, ts.SOURCE_REFUSED)
        if request_id is None or refused_id is None:
            return False
        if source_request_id(self.stub, ts.SOURCE_FINDINGS) is None:
            return False
        issues = self.stub._rows("issue")
        outputs = ts.outputs_of(issues, request_id)
        refused = ts.outputs_of(issues, refused_id)
        if len(outputs) != 1 or len(refused) != 1:
            return False
        self.output = int(outputs[0]["number"])
        final = [
            c for c in self.stub._rows("comment")
            if c["issue_number"] == self.output and fe._NOTICE_FINAL in c["body"]
        ]
        posts = [post.__dict__ for post in self.recorder.posts()]
        return bool(
            final
            and ts.triage_lines(posts, outputs[0]["html_url"])
            and ts.triage_lines(posts, refused[0]["html_url"])
        )

    def wait(self) -> None:
        deadline = self.driver.deadline
        assert deadline is not None
        while time.monotonic() < deadline:
            self.check_boundaries()
            if self.settled():
                return
            time.sleep(2)
        raise TriageScenarioError("triage-to-pr did not settle inside the command bound")

    # Observation -----------------------------------------------------------

    def observe(self) -> dict[str, Any]:
        numbers = [number for number, *_ in SOURCES]
        if self.output is not None:
            numbers.append(self.output)
        numbers.extend(
            int(issue["number"])
            for issue in self.stub._rows("issue")
            if "curie-linked-issue:" in (issue.get("body") or "")
        )
        observed: dict[str, Any] = self.driver.sql(observation_sql(sorted(set(numbers))))
        return observed

    def counts(self) -> dict[str, int]:
        totals = self.observe()["totals"]
        return {
            **{key: int(value) for key, value in totals.items()},
            "issues": len(self.stub._rows("issue")),
            "comments": len(self.stub._rows("comment")),
            "pulls": len(self.stub._rows("pull")),
        }

    def checks_by_head(self, pulls: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """The check runs the stub reports on each pull request head, as the factory reads them."""

        checks: dict[str, list[dict[str, Any]]] = {}
        for pull in pulls:
            head = str(pull["head"]["sha"])
            status, answer = self.stub._rest(
                "GET",
                f"/repos/{REPOSITORY}/commits/{head}/check-runs",
                {"per_page": ["100"]},
                {},
            )
            runs = answer.get("check_runs") if isinstance(answer, dict) else None
            if status != 200 or not isinstance(runs, list) or answer["total_count"] != len(runs):
                raise TriageScenarioError("the pull request check runs were unavailable")
            checks[head] = runs
        return checks

    def assert_outcome(self, github: Any) -> None:
        observed = self.observe()
        issues, comments = self.stub._rows("issue"), self.stub._rows("comment")
        pulls = [self.stub._fresh_pull(row) for row in self.stub._rows("pull")]
        posts = [post.__dict__ for post in self.recorder.posts()]
        lane_evidence = self.router.evidence()
        receipts = {str(r["execution_request_id"]): r for r in observed["receipts"]}
        threads = {str(t["work_item_id"]): t for t in observed["threads"]}

        def source(number: int) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
            items = by_issue(observed["work_items"], number)
            requests = by_issue(observed["requests"], number)
            name = {
                ts.SOURCE_TICKET: "triage-ticket",
                ts.SOURCE_REFUSED: "triage-refused",
                ts.SOURCE_FINDINGS: "triage-findings",
            }[number]
            request = ts.assert_one_selection(items, requests, lane_evidence[name])
            receipt = receipts.get(str(request["id"]))
            if receipt is None:
                raise ts.TriageAssertionError(f"source #{number} has no delivered result")
            return dict(request), dict(receipt), threads.get(str(items[0]["id"]), {})

        request, receipt, thread = source(ts.SOURCE_TICKET)
        ts.assert_reproduction_ran(
            lane_evidence["triage-ticket"], self.stub.main_revision(), receipt
        )
        output = ts.assert_one_output(
            issues, comments, ts.SOURCE_TICKET, str(request["id"]), receipt, github.BOT["login"]
        )
        output_number = int(output["number"])
        sources = [number for number, *_ in SOURCES]
        triage_ids = {str(row["id"]) for row in observed["requests"]
                      if row.get("completion_contract") == "tracker_result"}
        ts.assert_no_triage_publication(
            pulls,
            self.stub.branches(),
            len([r for r in observed["publication_requests"] if str(r) in triage_ids]),
            sources,
        )
        events = [e for e in self.stub._rows("event") if e["issue_number"] == output_number]
        implementation = ts.assert_handoff_admitted(
            by_issue(observed["work_items"], output_number),
            by_issue(observed["requests"], output_number),
            str(self.driver.evidence["agent_id"]),
            request,
            events,
        )
        final = [
            c for c in comments
            if c["issue_number"] == output_number and fe._NOTICE_FINAL in c["body"]
        ]
        pull = ts.assert_implementation_terminus(
            implementation, request, pulls, final, output_number, self.checks_by_head(pulls)
        )
        statuses = [
            c for c in comments
            if c["issue_number"] == ts.SOURCE_TICKET and "Implementation admitted" in c["body"]
        ]
        if len(statuses) != 1:
            raise ts.TriageAssertionError("the source status comment does not admit the handoff")
        for number, before in self.before.items():
            ts.assert_source_preserved(before, self.stub._get("issue", number))
        ts.assert_slack_line(posts, thread, output["html_url"], "Implementation admitted.")

        refused_request, _, refused_thread = source(ts.SOURCE_REFUSED)
        refused = ts.assert_refused_handoff(
            issues, comments, ts.SOURCE_REFUSED, str(refused_request["id"]), [],
            posts, refused_thread, lane_evidence["triage-refused"],
        )
        refused_items = by_issue(observed["work_items"], int(refused["number"]))
        if refused_items:
            raise ts.TriageAssertionError("the refused ticket has an implementation WorkItem")

        findings_request, findings_receipt, _ = source(ts.SOURCE_FINDINGS)
        ts.assert_findings_ending(
            issues, comments, ts.SOURCE_FINDINGS, str(findings_request["id"]),
            findings_receipt,
            len([r for r in observed["requests"]
                 if r.get("completion_contract") == "implementation"
                 and str(r["issue"]) not in {str(output_number), str(RECORDED_ISSUE)}]),
        )
        self.evidence.update(
            source_request_id=str(request["id"]),
            output_issue=output_number,
            implementation_request_id=str(implementation["id"]),
            pull_request=pull["html_url"],
            refused_output_issue=int(refused["number"]),
            findings_request_id=str(findings_request["id"]),
            lanes=lane_evidence,
            slack_posts=posts,
        )

    def replay_selection(self) -> dict[str, Any]:
        """Replay the internal selection event of the admitted handoff on the live API."""

        request_id = str(uuid.UUID(str(self.evidence["source_request_id"])))
        output = self.driver.kubectl(
            "-n",
            self.driver.namespace,
            "exec",
            "deployment/curie-api",
            "--",
            "python",
            "-c",
            REPLAY_SELECTION_SNIPPET,
            request_id,
        )
        lines = [line for line in output.splitlines() if line.strip()]
        if not lines:
            raise TriageScenarioError("the selection replay printed no observation")
        replay: dict[str, Any] = json.loads(lines[-1])
        if replay.get("outcome_before") != "admitted" or replay.get("outcome_after") != "admitted":
            raise TriageScenarioError(
                f"the replayed handoff was {replay.get('outcome_before')!r} and became "
                f"{replay.get('outcome_after')!r}, expected admitted throughout"
            )
        if replay.get("reopened") not in {None, replay.get("handoff_id")}:
            raise TriageScenarioError("the replay opened another selection event")
        return replay

    def intake_reads(self) -> int:
        return sum(
            1
            for entry in self.stub.request_log
            if entry["method"] == "GET" and entry["path"].endswith(INTAKE_LIST_PATH)
        )

    def wait_for_intake_passes(self, since: int) -> None:
        """Wait until the intake has read the tracker REPLAY_POLLS times since ``since``.

        Nothing happening is only evidence once the intake has looked. Each
        pass lists open issues once per served label, so the wait counts those
        reads. REPLAY_POLLS passes take REPLAY_POLLS poll intervals; the wait
        allows four times that before it fails, so a slow pass after the API
        restart is not mistaken for a missing one.
        """

        poll = float(self.driver.tracker_poll_seconds)
        wanted = REPLAY_POLLS * len(INTAKE_LABELS)
        bound = time.monotonic() + 4 * REPLAY_POLLS * poll
        if self.driver.deadline is not None:
            bound = min(bound, self.driver.deadline)
        while self.intake_reads() - since < wanted:
            self.check_boundaries()
            if time.monotonic() >= bound:
                raise TriageScenarioError(
                    f"the intake read the tracker {self.intake_reads() - since} times after "
                    f"the replay, expected {wanted}"
                )
            time.sleep(poll / 5)

    def assert_replay(self, github: Any) -> None:
        """Point 7: an API restart, the internal event and bot label events create nothing."""

        assert self.output is not None
        before = self.counts()
        self.driver.kubectl(
            "-n", self.driver.namespace, "rollout", "restart", "deployment/curie-api"
        )
        self.driver.kubectl(
            "-n", self.driver.namespace, "rollout", "status", "deployment/curie-api",
            "--timeout=300s",
        )
        replay = self.replay_selection()
        since = self.intake_reads()
        # The factory's own label again on its output, and an App label event on
        # the source: neither is a human selection.
        self.stub._label_event(self.output, ts.IMPLEMENTATION_LABEL, "labeled", github.BOT)
        self.stub._label_event(ts.SOURCE_TICKET, ts.TRIAGE_LABEL, "labeled", github.BOT)
        self.wait_for_intake_passes(since)
        after = self.counts()
        comments = [c for c in self.stub._rows("comment") if c["issue_number"] == self.output]
        ts.assert_no_replay_effects(before, after, comments)
        self.evidence.update(replay_counts=after, selection_replay=replay)

    def assert_runtime_drained(self) -> None:
        """Point 10, in-cluster half: no sandbox or publication resource remains."""

        settle = time.monotonic() + RUNTIME_SETTLE_SECONDS
        claims = resources = -1
        while time.monotonic() < settle:
            claims = len(json.loads(
                self.driver.kubectl("get", "sandboxclaims", "-A", "-o", "json")
            )["items"])
            resources = len(json.loads(self.driver.kubectl(
                "get", "jobs,pods", "-A", "-l", "curietech.ai/component=publication",
                "-o", "json",
            ))["items"])
            if claims == 0 and resources == 0:
                break
            time.sleep(5)
        self.evidence.update(sandbox_claims=claims, publication_resources=resources)
        if claims or resources:
            raise ts.TriageAssertionError(
                f"{claims} sandbox claims and {resources} publication resources remain"
            )

    def execute(self, github: Any) -> None:
        self.setup()
        self.label_sources(github)
        self.wait()
        self.assert_outcome(github)
        self.assert_replay(github)
        self.assert_runtime_drained()

    def finish(self, cleanup: dict[str, Any], scenario_error: str | None) -> bool:
        """Point 10 after teardown, and the evidence file the CI upload carries."""

        self.evidence["cleanup_verified"] = cleanup.get("cleanup_verified") is True
        self.evidence["slack_recorder_closed"] = self.recorder_closed()
        passed = scenario_error is None
        if passed:
            try:
                ts.assert_runtime_cleared(
                    self.evidence.get("sandbox_claims", -1),
                    self.evidence.get("publication_resources", -1),
                    cleanup,
                )
                if not self.evidence["slack_recorder_closed"]:
                    raise ts.TriageAssertionError("the Slack recorder still listens")
            except ts.TriageAssertionError as exc:
                passed = False
                self.evidence["error"] = str(exc)
        else:
            self.evidence["error"] = scenario_error
        self.evidence["result"] = "passed" if passed else "failed"
        return passed

    def recorder_closed(self) -> bool:
        import socket

        with socket.socket() as sock:
            sock.settimeout(1)
            return sock.connect_ex((self.recorder.host, self.recorder.port)) != 0
