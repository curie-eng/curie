"""Run factory PR maintenance against one disposable candidate install and a real Git repository."""

from __future__ import annotations

import argparse
import json
import os
import signal
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import candidate_install as ci
import factory_e2e as fe
import tls_code_host as tch

HERE = Path(__file__).resolve().parent
CHECK = "example-tests"
DELEGATED = "example-integration"
CHANGE = "revision.py"
LABEL = "factory"
NAMES = ci.InputNames(
    "factory-e2e maintenance",
    "CURIE_FACTORY_MAINTENANCE_NAMESPACE",
    "CURIE_FACTORY_MAINTENANCE_RELEASE",
    "CURIE_FACTORY_MAINTENANCE_PULL_POLICY",
)
OWNER = ("tenant_id", "model_auth_mode", "owner_principal_id", "enrollment_generation")
ORIGIN = ("origin_request_id", "origin_head_sha", "origin_completed_at", "expires_at")


def evidence(step: str, **facts: Any) -> None:
    print(
        json.dumps({"step": step, "ingress": "local replay", **facts}, sort_keys=True), flush=True
    )


def wait(what: str, probe: Callable[[], Any], seconds: float = 600) -> Any:
    return fe._wait(what, seconds, probe, 2)


class MaintenanceInstall(ci.CandidateInstall):
    agent_id: str

    def stub_manifests(
        self, seed_files: Mapping[str, str], seed_issue: str
    ) -> list[dict[str, Any]]:
        manifests = super().stub_manifests(
            seed_files, seed_issue, entrypoint="/stub/maintenance_fixture.py"
        )
        source = next(m["data"] for m in manifests if m["kind"] == "ConfigMap")
        source["maintenance_fixture.py"] = HERE.joinpath("maintenance_fixture.py").read_text()
        source["model_script.py"] = (HERE.parent / "model-script/model_script.py").read_text()
        source["maintenance-transcript.json"] = '{"version":1,"exchanges":[]}'
        recording = json.loads(source[tch.STUB_RECORDING.name])
        recording["statuses"] = []
        source[tch.STUB_RECORDING.name] = json.dumps(recording)
        return manifests

    def fixture(self, operation: str, *args: str) -> Any:
        return json.loads(
            self.stub_exec("python3", "/stub/maintenance_fixture.py", operation, *args)
        )

    def git(self, *args: str) -> str:
        return self.stub_exec("git", "-C", tch.STUB_BARE_REPO, *args).strip()

    def human_push(self, operation: str, branch: str, expected: str, value: str) -> str:
        host = tch.stub_host(self.inputs.namespace)
        url = f"https://{host}:{ci.STUB_PORT}/{tch.STUB_REPO}.git"
        return self.fixture(operation, url, tch.STUB_CA_FILE, branch, expected, value)

    def commit_branch(
        self, branch: str, files: dict[str, str], *, expected: str | None = None
    ) -> str:
        parent = expected or self.git("rev-parse", "refs/heads/" + branch)
        return self.human_push("commit", branch, parent, json.dumps(files))

    def forge(self, method: str, tail: str, body: Any = None) -> Any:
        status, result = self.stub_api(method, f"/repos/{tch.STUB_REPO}/{tail}", body)
        assert 200 <= status < 300, f"fixture {method} {tail} returned {status}"
        return result

    def pull_ids(self) -> set[str]:
        return {pr["node_id"] for pr in self.forge("GET", "pulls?state=all&per_page=100")}

    def checks(self, *checks: tuple[str, str, str]) -> None:
        self.fixture("set-checks", json.dumps(checks))

    def model_changes(self, files: Mapping[str, str], verification: list[str]) -> None:
        """Program the external model with Conflict's PR edits and repository check."""
        self.fixture("plan", json.dumps({"files": files, "verification": verification}))

    def start(self, case: str) -> None:
        self.preflight()
        self.create_namespace()
        self.start_stub(
            {
                CHANGE: "REVISION = 0\n",
                "test_revision.py": "import unittest\nfrom revision import REVISION\n"
                "class RevisionTest(unittest.TestCase):\n"
                "    def test_revision(self):\n        self.assertGreaterEqual(REVISION, 0)\n",
            },
            "Update the example revision.\n",
        )
        declaration = {
            "checks": [
                {"id": "unit", "paths": [CHANGE], "command": ["python3", "-B", "-m", "unittest"]},
            ]
        }
        if case == "active":
            declaration["checks"].append(
                {
                    "id": "integration",
                    "paths": [CHANGE],
                    "command": ["example-ci-integration"],
                    "delegated_to": DELEGATED,
                }
            )
        base = self.commit_branch(
            "main",
            {".curie/verification.json": json.dumps(declaration), ".gitignore": "__pycache__/\n"},
        )
        self.checks((base, "success", CHECK))
        self.install(self.maintenance_values(case))
        self.port_forward()
        self.agent_id = self.create_agent(fe.FACTORY_AGENT, LABEL)
        self.deploy_bundle(fe.FACTORY_AGENT)
        channel = "C0EXAMPLE7"
        self.api(
            "POST",
            f"/agents/{self.agent_id}/channels",
            {"kind": "slack", "address": channel},
            expected=201,
        )
        self.api("PATCH", f"/agents/{self.agent_id}", {"factory_slack_channel": channel})

    def maintenance_values(self, case: str) -> dict[str, Any]:
        ci_paths = [CHANGE]
        if case == "conflict":
            from maintenance_conflict import fixture

            ci_paths = list(fixture.INITIAL_PR_FILES)
        values = self.base_values(model_base_url=f"http://{self.stub_ip}:8123")
        values["factory"].update(maintenanceRepairEnabled=True, ciWaitSeconds=180)
        values["api"].setdefault("extraEnv", []).append(
            {"name": "FACTORY_LINEAGE_RECONCILE_INTERVAL_S", "value": "3"}
        )
        values["factory"]["bindings"] = [
            {
                "name": "example",
                "tracker": "github",
                "trackerScopeId": "4401",
                "repos": [
                    {
                        "alias": "example",
                        "codeHost": "github",
                        "projectId": "4401",
                        "path": tch.STUB_REPO,
                        "ci": {"required": {"key": CHECK, "paths": ci_paths}},
                    }
                ],
            }
        ]
        values["agentSandbox"]["runner"]["workspace"] = {"enabled": True}
        return values

    def seed(self, name: str) -> Subject:
        issue = self.seed_issue(name, "Increase the example revision and run its unit test.", LABEL)
        execution = wait("label admission", lambda: self.requests(self.agent_id, issue))[0]
        subject = Subject(self, issue, uuid.UUID(execution["work_item_id"]))
        first = subject.published(1)
        assert first["requests"][0]["id"] == execution["id"]
        assert first["requests"][0]["status"] == "running"
        return subject

    def api_replicas(self, replicas: int) -> None:
        deployment = "deployment/" + self.inputs.release + "-api"
        self.kubectl("-n", self.inputs.namespace, "scale", deployment, f"--replicas={replicas}")
        self.kubectl("-n", self.inputs.namespace, "rollout", "status", deployment, "--timeout=600s")
        if replicas:
            self.port_forward()
        else:
            selector = (
                f"app.kubernetes.io/instance={self.inputs.release},app.kubernetes.io/component=api"
            )
            wait(
                "API pods gone before the fixture changes the PR",
                lambda: (
                    not self.kubectl(
                        "-n", self.inputs.namespace, "get", "pods", "-l", selector, "-o", "name"
                    ).strip()
                ),
                seconds=120,
            )

    def restart(self) -> None:
        name = "deployment/" + self.inputs.release + "-api"
        self.kubectl("-n", self.inputs.namespace, "rollout", "restart", name)
        self.kubectl("-n", self.inputs.namespace, "rollout", "status", name, "--timeout=600s")
        self.port_forward()


@dataclass(frozen=True)
class Subject:
    run: MaintenanceInstall
    issue: int
    item: uuid.UUID

    def snapshot(self) -> dict[str, Any]:
        """Read request history, lineage and maintenance effects at one database snapshot."""
        return self.run.sql(f"""
            WITH w AS (SELECT * FROM curie.work_items WHERE id='{self.item}'),
                 q AS (SELECT * FROM curie.execution_requests WHERE work_item_id='{self.item}'),
                 l AS (SELECT * FROM curie.thread_publication_lineages
                        WHERE id=(SELECT publication_lineage_id FROM w))
            SELECT json_build_object(
                'requests', (SELECT json_agg(row_to_json(q) ORDER BY sequence) FROM q),
                'lineage', (SELECT row_to_json(l) FROM l),
                'window', (SELECT row_to_json(m) FROM curie.factory_maintenance_lineages m
                           WHERE work_item_id='{self.item}'),
                'admissions', (SELECT coalesce(json_agg(a ORDER BY admitted_at),'[]')
                               FROM curie.factory_maintenance_admissions a
                               WHERE work_item_id='{self.item}'),
                'events', (SELECT coalesce(json_agg(e ORDER BY id),'[]')
                           FROM curie.factory_maintenance_events e
                           WHERE work_item_id='{self.item}'),
                'posts', (SELECT coalesce(json_object_agg(post_key, payload),'{{}}')
                          FROM curie.factory_slack_posts
                          WHERE work_item_id='{self.item}' AND kind='maintenance'),
                'progress_count', (SELECT count(*) FROM curie.factory_slack_posts
                                   WHERE work_item_id='{self.item}'),
                'publications', (SELECT coalesce(json_agg(
                                     json_build_array(id, execution_request_id, status)
                                     ORDER BY revision_number),'[]') FROM curie.publications
                                 WHERE execution_request_id IN (SELECT id FROM q)),
                'detail', (SELECT n.detail FROM curie.factory_terminal_notices n
                           JOIN q ON q.id=n.execution_request_id ORDER BY q.sequence DESC LIMIT 1),
                'turns', (SELECT count(DISTINCT turn_id) FROM curie.execution_request_model_usage
                          WHERE execution_request_id IN (SELECT id FROM q)))
            """)

    def published(self, count: int) -> dict[str, Any]:
        def probe() -> Any:
            state = self.snapshot()
            publications = state["publications"]
            assert len(publications) <= count, "extra publication"
            if len(publications) == count and publications[-1][2] == "succeeded":
                return state
            return None

        return wait(f"{count} successful publications", probe, seconds=1200)

    def pull(self, state: dict[str, Any]) -> dict[str, Any]:
        line = state["lineage"]
        pr = self.run.forge("GET", f"pulls/{line['pr_number']}")
        assert pr["node_id"] == line["code_host_pr_id"] and pr["head"]["ref"] == line["branch"]
        assert self.run.git("rev-parse", "refs/heads/" + line["branch"]) == pr["head"]["sha"]
        return pr

    def event(self, outcome: str, head: str) -> dict[str, Any]:
        def probe() -> Any:
            state = self.snapshot()
            matching = [
                e for e in state["events"] if e["outcome"] == outcome and e["head_sha"] == head
            ]
            assert len(matching) <= 1, f"duplicate {outcome} history"
            if matching:
                event = matching[0]
                post = state["posts"].get("maintenance:" + str(event["id"]))
                if post is not None:
                    expected = {"reason": event["reason"], "outcome": outcome}
                    if outcome in {"green", "no_ci"}:
                        expected["head_sha"] = head
                    assert post == expected
                    return state
            return None

        state = wait(f"{outcome} event and progress outbox on {head}", probe)
        view = self.run.api("GET", f"/work-items/{self.item}")
        ids = [e["id"] for e in state["events"]]
        assert [e["id"] for e in view["maintenance_history"]][: len(ids)] == ids
        assert any(
            e["outcome"] == outcome and e["head_sha"] == head for e in view["maintenance_history"]
        )
        return state

    def settled(self, status: str) -> dict[str, Any]:
        def probe() -> Any:
            state = self.snapshot()
            current = state["requests"][-1]
            if current["status"] in ci.TERMINAL:
                assert current["status"] == status, (
                    f"unexpected terminal cause {current['terminal_cause']}"
                )
                return state
            return None

        return wait(f"execution {status}", probe)

    def green(self, state: dict[str, Any]) -> dict[str, Any]:
        head = state["lineage"]["head_sha"]
        self.run.checks((head, "success", CHECK))
        self.settled("completed")
        final = self.event("green", head)
        assert final["window"]["latest_green_head_sha"] == head
        assert self.pull(final)["head"]["sha"] == head
        verdict = self.run.api("GET", f"/work-items/{self.item}")["ci"]
        assert verdict["head_sha"] == head and verdict["state"] == "passing"
        return final


def check_pushes(before: dict, after: dict, human_pushes: int) -> None:
    assert after["factory_push"] == before["factory_push"], "unexpected factory push"
    assert after["human_push"] == before["human_push"] + human_pushes, "unexpected human push count"


def unchanged(
    before: dict[str, Any], after: dict[str, Any], counts: dict, later: dict, human_pushes: int = 0
) -> None:
    for key in ("requests", "publications", "turns", "admissions", "window"):
        assert after[key] == before[key], f"refusal changed {key}"
    check_pushes(counts, later, human_pushes)
    for key in ("model", "pr_write"):
        assert later[key] == counts[key], f"refusal caused {key}"


def refusal(
    subject: Subject,
    before: dict[str, Any],
    outcome: str,
    stimulus: Callable[[], Any],
    *,
    counts: dict | None = None,
    human_pushes: int = 0,
) -> None:
    head = before["lineage"]["head_sha"]
    if counts is None:
        counts = subject.run.fixture("counts", head)
    stimulus()
    after = subject.event(outcome, head)
    if outcome == "window_expired":

        def final_read() -> Any:
            snapshot = subject.snapshot()
            return snapshot if snapshot["window"]["final_poll_at"] else None

        after = wait("final CI observation persisted", final_read)
        final_poll_at = after["window"]["final_poll_at"]
        assert before["window"]["final_poll_at"] is None
        assert datetime.fromisoformat(final_poll_at) >= datetime.fromisoformat(
            before["window"]["expires_at"]
        )
        before = {**before, "window": {**before["window"], "final_poll_at": final_poll_at}}
    later = subject.run.fixture("counts", head)
    unchanged(before, after, counts, later, human_pushes)
    evidence(
        outcome,
        item=str(subject.item),
        head=head,
        new_requests=0,
        model_calls=0,
        factory_pushes=0,
        human_pushes=human_pushes,
    )


def summary(subject: Subject, state: dict[str, Any]) -> None:
    pr = subject.pull(state)
    head = pr["head"]["sha"]
    checks = subject.run.forge("GET", f"commits/{head}/check-runs")["check_runs"]
    assert head == state["lineage"]["head_sha"] and pr["state"] == "open" and not pr["merged"]
    evidence(
        "snapshot",
        item=str(subject.item),
        source=state["requests"][-1]["id"],
        lineage=state["lineage"]["id"],
        pr=pr["number"],
        head=head,
        base_ref=pr["base"]["ref"],
        base_sha=pr["base"]["sha"],
        owner={k: state["requests"][-1][k] for k in OWNER},
        checks={c["name"]: c["conclusion"] or c["status"] for c in checks},
        requests=len(state["requests"]),
        publications=len(state["publications"]),
        progress=state["progress_count"],
        admissions=len(state["admissions"]),
    )


def active(run: MaintenanceInstall) -> None:
    subject = run.seed("Active CI repair rounds")
    current = subject.published(1)
    summary(subject, current)
    initial = subject.pull(current)
    pull_ids = run.pull_ids()
    for number in (1, 2, 3):
        head = current["lineage"]["head_sha"]
        run.checks((head, "failure", CHECK))
        wait(
            "accepted failed-job rerun", lambda head=head: run.fixture("counts", head).get("rerun")
        )
        run.checks((head, "failure", CHECK))
        if number < 3:
            current = subject.published(number + 1)
            assert subject.pull(current)["number"] == initial["number"]
            assert run.git("rev-parse", current["lineage"]["head_sha"] + "^") == head
        assert run.fixture("counts", head)["rerun"] == 1
    final = subject.settled("failed")
    assert final["requests"][0]["terminal_cause"] == "ci_failed" and len(final["requests"]) == 1
    assert len(final["publications"]) == 3 and not final["admissions"]
    assert run.pull_ids() == pull_ids
    summary(subject, final)
    run.forge("PATCH", f"pulls/{initial['number']}", {"state": "closed"})

    for name in ("additional-lint", CHECK, DELEGATED):
        subject = run.seed("Matching base failure for " + name)
        before = subject.published(1)
        pr = subject.pull(before)
        head, base = pr["head"]["sha"], pr["base"]["sha"]
        counts = run.fixture("counts", head)
        run.checks((head, "success", CHECK), (head, "failure", name), (base, "failure", name))
        status = "completed" if name == "additional-lint" else "failed"
        final = subject.settled(status)
        if status == "failed":
            assert final["requests"][0]["terminal_cause"] == "ci_unverified"
            reason = (
                "required_python_ci_failed_on_base"
                if name == CHECK
                else "delegated_ci_failed_on_base"
            )
            assert reason in (final["detail"] or ""), f"wrong unverified reason for {name}"
        assert len(final["requests"]) == len(final["publications"]) == 1 and not final["admissions"]
        later = run.fixture("counts", head)
        check_pushes(counts, later, human_pushes=0)
        assert later["model"] == counts["model"]
        run.checks((base, "success", name), (head, "success", name))
        evidence("active base exclusion", check=name, status=status, head=head)
        run.forge("PATCH", f"pulls/{pr['number']}", {"state": "closed"})

    subject = run.seed("Active request with a conflicting base advance")
    before = subject.published(1)
    pr = subject.pull(before)
    head = pr["head"]["sha"]
    counts = run.fixture("counts", head)
    run.commit_branch(pr["base"]["ref"], {CHANGE: "REVISION = 999\n"}, expected=pr["base"]["sha"])
    assert subject.pull(before)["mergeable"] is False
    failed = subject.settled("failed")
    assert failed["requests"][0]["terminal_cause"] == "merge_conflict"
    assert len(failed["requests"]) == 1 and not failed["admissions"]
    assert failed["publications"] == before["publications"] and failed["turns"] == before["turns"]
    after = run.fixture("counts", head)
    check_pushes(counts, after, human_pushes=1)
    assert all(after[k] == counts[k] for k in ("model", "pr_write"))
    evidence(
        "active conflict",
        head=head,
        new_requests=0,
        model_calls=0,
        factory_pushes=0,
        human_pushes=1,
    )


def recovery(run: MaintenanceInstall) -> None:
    subject = run.seed("Read-only CI timeout observation")
    before = subject.published(1)
    summary(subject, before)
    head = before["lineage"]["head_sha"]
    failed = subject.settled("failed")
    assert failed["requests"][0]["terminal_cause"] == "ci_timeout"
    counts = run.fixture("counts", head)
    run.checks((head, "success", CHECK))

    def observed() -> Any:
        result = run.api("GET", f"/work-items/{subject.item}")
        return result if result["actionable_cause"] == "ci_observed_after_run" else None

    view = wait("item-level read-only CI recovery", observed)
    assert (
        view["ci_observation"]["head_sha"] == head
        and view["ci_observation"]["status"] == "completed"
    )
    assert view["requests"][-1]["status"] == "failed"
    final = subject.snapshot()
    unchanged(failed, final, counts, run.fixture("counts", head))
    assert subject.pull(final)["head"]["sha"] == head
    evidence(
        "read-only late green", head=head, item=str(subject.item), model_calls=0, publications=1
    )


def repair(subject: Subject, before: dict[str, Any], *, fail: bool = False) -> None:
    run = subject.run
    pr = subject.pull(before)
    head, base = pr["head"]["sha"], pr["base"]["sha"]
    source = before["requests"][-1]
    pull_ids = run.pull_ids()
    model_before = run.fixture("counts", head)["starts"]
    run.fixture("mode", "hold")
    run.checks((head, "failure", CHECK))
    admitted = subject.event("admitted", head)
    (receipt,) = admitted["admissions"]
    assert receipt["reason"] == "late_ci_red" and receipt["mode"] == "successor"
    assert receipt["source_request_id"] == source["id"] and receipt["work_item_id"] == str(
        subject.item
    )
    assert (
        receipt["lineage_id"] == before["lineage"]["id"]
        and receipt["code_host_pr_id"] == pr["node_id"]
    )
    assert (receipt["head_sha"], receipt["base_ref"], receipt["base_sha"]) == (
        head,
        pr["base"]["ref"],
        base,
    )
    run.checks((head, "failure", CHECK))
    reads = run.fixture("counts", head).get("pull", 0)
    run.restart()
    wait(
        "two repeated reads of the authorized PR head after restart",
        lambda: run.fixture("counts", head).get("pull", 0) >= reads + 2,
    )
    duplicate = subject.snapshot()
    assert duplicate["admissions"] == [receipt]
    assert len(duplicate["requests"]) == len(before["requests"]) + 1
    run.fixture("mode", "edit")
    published = subject.published(len(before["publications"]) + 1)
    current = subject.pull(published)
    new_head = current["head"]["sha"]
    assert current["node_id"] == pr["node_id"] and current["base"] == pr["base"]
    assert run.pull_ids() == pull_ids
    assert run.git("rev-parse", new_head + "^") == head
    old_revision = int(run.git("show", head + ":" + CHANGE).split("=")[1])
    assert run.git("show", new_head + ":" + CHANGE) == f"REVISION = {old_revision + 1}"
    if fail:
        counts = run.fixture("counts", new_head)
        run.checks((new_head, "failure", CHECK))
        final = subject.settled("failed")
        assert final["requests"][-1]["terminal_cause"] == "ci_failed"
        run.restart()
        wait(
            "a fresh pull read after failed repair",
            lambda: run.fixture("counts", new_head).get("pull", 0) > counts.get("pull", 0),
        )
        final = subject.snapshot()
        assert final["publications"] == published["publications"]
        later = run.fixture("counts", new_head)
        assert later["model"] == counts["model"] and later.get("rerun", 0) == 0
    else:
        final = subject.green(published)
    assert final["admissions"] == [receipt] and final["window"]["repairs_used"] == 1
    assert run.fixture("counts", head)["starts"] == model_before + 1
    assert final["requests"][:-1] == before["requests"]
    assert final["requests"][-1]["id"] == receipt["repair_request_id"]
    assert all(final["requests"][-1][key] == receipt[key] == source[key] for key in OWNER)
    assert all(final["window"][key] == before["window"][key] for key in ORIGIN)
    summary(subject, final)
    if not fail:
        refusal(
            subject, final, "budget_exhausted", lambda: run.checks((new_head, "failure", CHECK))
        )


def late(run: MaintenanceInstall, control: str) -> None:
    if control == "no-ci":
        run.fixture("mode", "no-ci")
    subject = run.seed("Late CI " + control)
    if control == "no-ci":
        first = subject.published(1)
        head = first["lineage"]["head_sha"]
        settled = subject.settled("completed")
        assert settled["window"]["latest_green_head_sha"] is None
        final = subject.event("no_ci", head)
        run.fixture("mode", "edit")
        refusal(subject, final, "prior_green_missing", lambda: run.checks((head, "failure", CHECK)))
        return
    original = subject.green(subject.published(1))
    window = original["window"]
    assert window["origin_request_id"] == original["requests"][0]["id"]
    assert datetime.fromisoformat(window["origin_completed_at"]) == datetime.fromisoformat(
        original["requests"][0]["terminal_at"]
    )
    assert datetime.fromisoformat(window["expires_at"]) - datetime.fromisoformat(
        window["origin_completed_at"]
    ) == timedelta(hours=24)
    summary(subject, original)
    pr = subject.pull(original)
    head, base = pr["head"]["sha"], pr["base"]["sha"]
    red = (head, "failure", CHECK)
    if control == "later-publication":
        run.fixture(
            "comment", str(pr["number"]), tch.stub_host(run.inputs.namespace), str(ci.STUB_PORT)
        )
        later = subject.green(subject.published(2))
        assert len(later["requests"]) == 2 and later["requests"][0] == original["requests"][0]
        assert all(later["window"][key] == window[key] for key in ORIGIN)
        repair(subject, later)
    elif control == "foreign-head":
        branch = pr["head"]["ref"]
        counts = run.fixture("counts", head)
        foreign = run.commit_branch(branch, {"human.txt": "example edit\n"}, expected=head)
        refusal(
            subject,
            original,
            "foreign_head",
            lambda: run.checks(red, (foreign, "failure", CHECK)),
            counts=counts,
            human_pushes=1,
        )
        assert subject.pull(original)["head"]["sha"] == foreign
        run.fixture("mode", "hold")
        run.human_push("restore", branch, foreign, head)
        assert subject.pull(original)["head"]["sha"] == head
        restored = run.fixture("counts", head)
        check_pushes(counts, restored, human_pushes=2)
        assert restored["pr_write"] == counts["pr_write"]
        evidence(
            "restored authorized head",
            head=head,
            foreign_head=foreign,
            human_pushes=2,
            factory_pushes=0,
        )
        repair(subject, original)
    elif control == "base-red":
        refusal(
            subject,
            original,
            "failure_not_attributable",
            lambda: run.checks(red, (base, "failure", CHECK)),
        )
        run.checks((head, "success", CHECK), (base, "success", CHECK))
    elif control == "closed":

        def close_with_failure() -> None:
            run.api_replicas(0)
            try:
                run.forge("PATCH", f"pulls/{pr['number']}", {"state": "closed"})
                # Only the driver can issue this PR write while the API is down.
                counts["pr_write"] += 1
                assert run.fixture("counts", head)["pr_write"] == counts["pr_write"]
                run.checks(red)
            finally:
                run.api_replicas(1)

        counts = run.fixture("counts", head)
        refusal(subject, original, "pr_closed", close_with_failure, counts=counts)
        assert subject.pull(original)["state"] == "closed"
    elif control == "cancelled":
        run.forge("DELETE", f"issues/{subject.issue}/labels/{LABEL}")
        refusal(subject, original, "work_item_cancelled", lambda: run.checks(red))
    elif control == "expiry":
        evidence("waiting for database deadline", expires_at=window["expires_at"])
        expiry = datetime.fromisoformat(window["expires_at"])

        # Stop the observer before its final read. The late check arrives after
        # the real deadline, while the API is offline, and survives its restart.
        def db_time() -> datetime:
            return datetime.fromisoformat(run.sql("SELECT to_json(clock_timestamp())"))

        fe._wait(
            "last five minutes of the real maintenance window",
            timedelta(hours=24, minutes=10).total_seconds(),
            lambda: db_time() >= expiry - timedelta(minutes=5),
            interval=60,
        )

        def fail_after_expiry() -> None:
            run.api_replicas(0)
            try:
                fe._wait("real database expiry", 600, lambda: db_time() >= expiry, interval=5)
                run.checks(red)
            finally:
                run.api_replicas(1)

        refusal(subject, original, "window_expired", fail_after_expiry)
    else:
        repair(subject, original, fail=control == "failed-repair")


def main(args: argparse.Namespace) -> int:
    env = dict(os.environ)
    suffix = uuid.uuid4().hex[:8]
    env.setdefault(NAMES.namespace, f"test-maintenance-{suffix}")
    env.setdefault(NAMES.release, f"curie-{suffix}")
    env.setdefault(NAMES.pull_policy, "IfNotPresent")
    if args.context:
        env[NAMES.context] = args.context
    try:
        inputs = ci.read_inputs(env, NAMES)
    except ci.Refused as exc:
        raise fe.ConfigError(str(exc)) from None
    with tempfile.TemporaryDirectory(prefix="factory-maintenance-") as temp:
        run = MaintenanceInstall(inputs, Path(temp))

        def interrupted(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt

        old = signal.signal(signal.SIGTERM, interrupted)
        try:
            run.start(args.case)
            if args.case == "active":
                active(run)
            elif args.case == "recovery":
                recovery(run)
            elif args.case == "conflict":
                from maintenance_conflict import conflict

                for control in args.control:
                    conflict(run, control)
            else:
                for control in args.control:
                    late(run, control)
        finally:
            try:
                results = run.teardown.run()
                evidence("teardown", steps=[{"step": r["step"], "ok": r["ok"]} for r in results])
                failures = [r for r in results if not r["ok"]]
                if failures:
                    raise fe.PreflightFailed(f"maintenance teardown failed: {failures}")
            finally:
                signal.signal(signal.SIGTERM, old)
    return 0
