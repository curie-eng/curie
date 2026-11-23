"""Conflict controls on the shared maintenance installation and its GitHub fixture."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import factory_e2e as fe
import maintenance as m
import tls_code_host as tch

_STIMULUS = Path(__file__).resolve().parent / "tests" / "c4_conflict_stimulus.py"
fixture = fe._load_by_path("c4_conflict_stimulus", _STIMULUS)
_HANDOFF = "Human review needed: the PR and base tests require different casing for the same tag."
_RECOVERY_CONTROLS = {"recover-push-without-job", "refuse-foreign-merge"}

# The model may resolve only a real squash conflict in the workspace it was given.
_PREPARE = """
import subprocess, sys
def git(*args, check=True):
    return subprocess.run(
        ["git", "-c", "user.name=Example Author", "-c", "user.email=author@example.com", *args],
        check=check, capture_output=True, text=True,
    )
head, base, changed = sys.argv[1:]
assert git("rev-parse", "HEAD").stdout.strip() == base
assert git("rev-parse", "refs/curie/conflict-head").stdout.strip() == head
assert git("merge", "--squash", "refs/curie/conflict-head", check=False).returncode == 1
assert git("diff", "--name-only", "--diff-filter=U").stdout.splitlines() == [changed]
"""

_VERIFY = """
import json, subprocess, sys
from release_tag import normalize_tag
paths, command = map(json.loads, sys.argv[1:])
subprocess.run(["git", "add", "--", *paths], check=True)
assert not subprocess.check_output(["git", "ls-files", "-u"]).strip()
subprocess.run(command, check=True)
assert normalize_tag("  Daily_Build  ") == "daily-build"
"""


def repair_plan(control: str, head: str, base: str) -> dict[str, Any]:
    files = fixture.PROTECTED_REPAIR_FILES if control == "protected" else fixture.RESOLVED_FILES
    plan = {
        "prepare": ["python3", "-B", "-c", _PREPARE, head, base, fixture.CHANGE],
        "files": files,
        "verification": [
            "python3",
            "-B",
            "-c",
            _VERIFY,
            json.dumps(list(files)),
            json.dumps(fixture.VERIFICATION_COMMAND),
        ],
    }
    if control == "incompatible":
        case = fixture.BASE_TEST.removesuffix(".py") + ".ReleaseTagTest.test_normalize_tag"
        plan["failure_handoff"] = {
            "contains": f"({case})",
            "text": _HANDOFF,
        }
    return plan


def _dirty(subject: m.Subject, state: dict[str, Any], target: str) -> dict[str, Any]:
    pr = subject.pull(state)
    assert pr["base"]["sha"] == target and pr["state"] == "open"
    assert pr["mergeable"] is False and pr["mergeable_state"] == "dirty"
    return pr


def _same_pull(
    original: dict[str, Any],
    current: dict[str, Any],
    *,
    head: str,
    base: str,
) -> None:
    for key in ("id", "node_id", "number"):
        assert current[key] == original[key]
    assert current["head"]["ref"] == original["head"]["ref"] and current["head"]["sha"] == head
    assert current["base"]["ref"] == original["base"]["ref"] and current["base"]["sha"] == base


def _closed_graph_conflicts(run: m.MaintenanceInstall, head: str, target: str) -> None:
    command = """
import subprocess, sys
repo, base, head, expected_path = sys.argv[1:]
merge = subprocess.run(
    ["git", "-C", repo, "merge-tree", "--write-tree", "--name-only", base, head],
    capture_output=True, text=True, check=False,
)
assert merge.returncode == 1 and expected_path in merge.stdout.splitlines()
"""
    run.stub_exec("python3", "-c", command, tch.STUB_BARE_REPO, target, head, fixture.CHANGE)


def _failure(subject: m.Subject) -> tuple[dict[str, Any], str, str]:
    failed = subject.settled("failed")
    cause = failed["requests"][-1]["terminal_cause"]
    assert cause
    view = subject.run.api("GET", f"/work-items/{subject.item}")
    visible = view["requests"][-1]
    assert visible["status"] == "failed" and visible["terminal_cause"] == cause
    assert visible["sequence"] == failed["requests"][-1]["sequence"]

    def posted() -> str | None:
        for comment in subject.run.forge("GET", f"issues/{subject.issue}/comments?per_page=100"):
            body = comment["body"]
            if f"Cause: {cause}" in body.splitlines():
                return str(body)
        return None

    return failed, cause, m.wait("terminal cause on the issue", posted)


def _admission(
    subject: m.Subject,
    source: dict[str, Any],
    pr: dict[str, Any],
    target: str,
) -> dict[str, Any]:
    head = pr["head"]["sha"]
    state = subject.event("admitted", head)
    [receipt] = state["admissions"]
    original = source["requests"][-1]
    assert len(state["requests"]) == len(source["requests"]) + 1
    assert state["requests"][:-1] == source["requests"]
    assert receipt["reason"] == "base_conflict" and receipt["mode"] == "successor"
    assert receipt["source_request_id"] == original["id"]
    assert receipt["repair_request_id"] == state["requests"][-1]["id"]
    assert receipt["work_item_id"] == str(subject.item)
    assert receipt["lineage_id"] == source["lineage"]["id"]
    assert (receipt["code_host_pr_id"], receipt["pr_number"]) == (pr["node_id"], pr["number"])
    assert (receipt["head_sha"], receipt["base_ref"], receipt["base_sha"]) == (
        head,
        pr["base"]["ref"],
        target,
    )
    assert state["window"]["repairs_used"] == 1
    assert all(state["window"][k] == source["window"][k] for k in m.ORIGIN)
    assert all(receipt[k] == original[k] == state["requests"][-1][k] for k in m.OWNER)
    return receipt


def _verify_merge(
    subject: m.Subject,
    source: dict[str, Any],
    original: dict[str, Any],
    target: str,
    tip: str,
    state: dict[str, Any],
) -> str:
    run = subject.run
    current = subject.pull(state)
    assert current["mergeable"] is True and state["lineage"]["id"] == source["lineage"]["id"]
    [publication, request, status] = state["publications"][-1]
    assert request == state["requests"][-1]["id"] and status == "succeeded"
    stored = run.sql(f"""
        SELECT json_build_object(
            'expected_merge_sha',p.expected_merge_sha,'mode',p.publication_mode,
            'prior_head',p.expected_prior_head,'base',p.base_sha,
            'lineage_id',l.id,'lineage_head',l.head_sha,'pr_id',l.code_host_pr_id,
            'status',a.status,'purpose',a.purpose,'tool',a.granted_tool,'grant',a.granted_arguments)
        FROM curie.publications p JOIN curie.approvals a ON a.id=p.approval_id
        JOIN curie.thread_publication_lineages l ON l.id=p.lineage_id
        WHERE p.id='{uuid.UUID(publication)}'
        """)
    # Even a direct database write cannot discard the result of prepare.
    unchanged_sha = run.sql(f"""
        BEGIN;
        DO $$ BEGIN
            UPDATE curie.publications SET expected_merge_sha=NULL
                WHERE id='{uuid.UUID(publication)}';
            RAISE EXCEPTION 'the prepared SHA was writable';
        EXCEPTION WHEN check_violation THEN
            IF SQLERRM <> 'prepared conflict merge SHA is immutable' THEN RAISE; END IF;
        END $$;
        SELECT to_json(expected_merge_sha) FROM curie.publications
            WHERE id='{uuid.UUID(publication)}';
        ROLLBACK;
        """)
    assert unchanged_sha == stored["expected_merge_sha"]
    head = stored["expected_merge_sha"]
    assert head and head == stored["lineage_head"] == state["lineage"]["head_sha"]
    assert (
        stored["lineage_id"] == source["lineage"]["id"] and stored["pr_id"] == original["node_id"]
    )
    assert (stored["mode"], stored["prior_head"], stored["base"]) == (
        "base_conflict_merge",
        original["head"]["sha"],
        target,
    )
    assert (stored["status"], stored["purpose"], stored["tool"]) == (
        "approved",
        "publication",
        "mcp__curie__publish_changes",
    )
    _same_pull(original, current, head=head, base=tip)
    markers = [
        line
        for line in run.git("show", "-s", "--format=%B", head).splitlines()
        if line.startswith("Curie-Revision:")
    ]
    assert markers == [f"Curie-Revision: {publication}"]
    # Run the shared oracle beside the bare forge, on a fresh clone of the actual pushed ref.
    command = (
        _STIMULUS.read_text()
        + """
import hashlib
from tempfile import TemporaryDirectory
repository, original, current, target, head = sys.argv[1:]
original = json.loads(original)
with TemporaryDirectory() as tmp:
    assert_published_repair(
        Path(repository), Path(tmp) / "checkout", original_pull=original,
        current_pull=json.loads(current), accepted_base=original["base"]["ref"],
        admitted_base_head=target, expected_final_head=head,
    )
patch = subprocess.check_output([
    "git", "-C", repository, "diff", "--binary", "--full-index", "--no-renames",
    "--no-ext-diff", "--no-textconv", "--no-color", "--src-prefix=a/", "--dst-prefix=b/",
    target, head, "--",
])
assert patch
print(hashlib.sha256(patch).hexdigest())
"""
    )
    result = run.stub_exec(
        "python3",
        "-c",
        command,
        tch.STUB_BARE_REPO,
        json.dumps(original),
        json.dumps(current),
        target,
        head,
    )
    assert stored["grant"] == {
        "publication_mode": "base_conflict_merge",
        "expected_prior_head": original["head"]["sha"],
        "base_sha": target,
        "patch_sha256": result.strip(),
    }
    return head


def _publication_state(run: m.MaintenanceInstall, request: uuid.UUID) -> dict[str, Any]:
    state = run.sql(f"""
        SELECT json_build_object(
            'id',p.id,'status',p.status,'version',p.version,'lease_owner',p.lease_owner,
            'attempts',p.reconcile_attempts,'expected_merge_sha',p.expected_merge_sha,
            'error',p.error,'request_status',q.status)
        FROM curie.publications p JOIN curie.execution_requests q ON q.id=p.execution_request_id
        WHERE q.id='{request}' AND p.publication_mode='base_conflict_merge'
        """)
    assert isinstance(state, dict)
    return state


def _owned_jobs(run: m.MaintenanceInstall, publication: uuid.UUID) -> dict[str, Any]:
    selector = (
        "app.kubernetes.io/managed-by=curie,curietech.ai/component=publication,"
        f"curietech.ai/publication-id={publication}"
    )
    found = run.kubectl(
        "-n", run.inputs.publication_namespace, "get", "jobs", "-l", selector, "-o", "json"
    )
    return {job["metadata"]["name"]: job for job in json.loads(found)["items"]}


def _remove_push_job(run: m.MaintenanceInstall, publication: uuid.UUID, owned_uid: str) -> None:
    name = "curie-publication-" + publication.hex[:20]
    owned = _owned_jobs(run, publication)[name]
    assert owned["metadata"]["uid"] == owned_uid and owned["status"]["active"] == 1
    namespace = run.inputs.publication_namespace
    run.kubectl("-n", namespace, "delete", f"job/{name}", "--cascade=foreground", "--timeout=30s")
    assert name not in _owned_jobs(run, publication)
    pods = run.kubectl("-n", namespace, "get", "pods", "-l", f"job-name={name}", "-o", "json")
    assert not json.loads(pods)["items"], "push Job log Pods still exist"


def _wait_gate(run: m.MaintenanceInstall, branch: str, phase: str, seconds: float) -> dict:
    def advanced() -> dict | None:
        state = run.fixture("push-gate", branch)
        assert state is not None and state["phase"] != "refused", state
        return state if state["phase"] == phase else None

    return m.wait(f"repair receive-pack {phase}", advanced, seconds=seconds)


def _end_publication_owner(subject: m.Subject, request: uuid.UUID) -> dict[str, Any]:
    run = subject.run
    version = run.sql(f"SELECT to_json(version) FROM curie.work_items WHERE id='{subject.item}'")
    # Cancellation is a worker-authenticated route. Keep its credential inside the worker.
    cancel = """
import os, sys, httpx
item, version = sys.argv[1:]
url = os.environ["CURIE_API_URL"].rstrip("/") + f"/v1/internal/work-items/{item}/cancel"
response = httpx.post(url, json={"expected_version": int(version)},
                      headers={"X-Curie-Worker-Token": os.environ["CURIE_INTERNAL_WORKER_TOKEN"]})
response.raise_for_status()
print(response.json()["request"]["id"])
"""
    cancelled = run.kubectl(
        "-n",
        run.inputs.namespace,
        "exec",
        f"deployment/{run.inputs.release}-worker",
        "--",
        "/app/.venv/bin/python",
        "-c",
        cancel,
        str(subject.item),
        str(version),
    )
    assert cancelled.strip() == str(request)

    def ended() -> dict | None:
        state = _publication_state(run, request)
        return state if state["request_status"] == "cancelled" else None

    return m.wait("repair owner cancellation", ended, seconds=45)


def _recover_without_job(
    subject: m.Subject,
    control: str,
    source: dict[str, Any],
    pr: dict[str, Any],
    target: str,
    receipt: dict[str, Any],
    counts: dict[str, Any],
) -> dict[str, Any]:
    run = subject.run
    head, branch = pr["head"]["sha"], pr["head"]["ref"]
    ref = "refs/heads/" + branch
    request = uuid.UUID(receipt["repair_request_id"])
    assert run.fixture("arm-push-gate", branch, head)
    try:
        run.fixture("plan", json.dumps(repair_plan(control, head, target)))
        waiting = _wait_gate(run, branch, "waiting", seconds=240)
        prepared = _publication_state(run, request)
        merge = prepared["expected_merge_sha"]
        publication = uuid.UUID(prepared["id"])
        jobs = _owned_jobs(run, publication)
        name = "curie-publication-" + publication.hex[:20]
        prepare_job = jobs[name + "-verify"]
        push_job = jobs[name]
        assert prepare_job["status"].get("succeeded") == 1
        assert push_job["status"].get("active") == 1
        push_uid = push_job["metadata"]["uid"]
        env = {
            setting["name"]: setting.get("value")
            for container in push_job["spec"]["template"]["spec"]["containers"]
            for setting in container.get("env", [])
        }
        assert (
            waiting["old_head"] == head
            and env["EXPECTED_MERGE_SHA"] == waiting["new_head"] == merge
        )
        assert prepared["request_status"] == "running" and run.git("rev-parse", ref) == head
        m.evidence("prepared SHA persisted before push", publication=str(publication), merge=merge)
        ended = _end_publication_owner(subject, request)

        def reconciled_while_live() -> dict | None:
            state = _publication_state(run, request)
            assert state["status"] in {"launching", "running"}, state
            return (
                state
                if state["version"] > ended["version"] and state["lease_owner"] is None
                else None
            )

        observed = m.wait(
            "ended owner observed its live push Job", reconciled_while_live, seconds=20
        )
        assert observed["attempts"] == ended["attempts"] == prepared["attempts"]
        assert observed["expected_merge_sha"] == merge and run.git("rev-parse", ref) == head
        assert run.fixture("proceed-push-gate", branch)
        assert _wait_gate(run, branch, "accepted", seconds=15)["new_head"] == merge
        assert run.git("rev-parse", ref) == merge
        remote = merge
        if control == "refuse-foreign-merge":
            remote = run.human_push("copy-merge", branch, merge, str(publication))
            assert remote != merge and run.git("rev-parse", ref) == remote
            for format_ in ("%T", "%P", "%B"):
                assert run.git("show", "-s", f"--format={format_}", remote) == run.git(
                    "show", "-s", f"--format={format_}", merge
                )
        before_read = run.fixture("counts", remote)
        _remove_push_job(run, publication, push_uid)
    finally:
        run.fixture("clear-push-gate", branch)

    def terminal() -> dict | None:
        state = _publication_state(run, request)
        return state if state["status"] in {"succeeded", "failed"} else None

    settled = m.wait("reconciled without the push Job or its logs", terminal, seconds=120)
    after_read = run.fixture("counts", remote)
    assert after_read.get("branch_read", 0) > before_read.get("branch_read", 0)
    assert after_read["factory_push"] == counts["factory_push"] + 1
    assert after_read["pr_write"] == counts["pr_write"]
    assert settled["expected_merge_sha"] == merge and settled["request_status"] == "cancelled"
    final = subject.snapshot()
    if control == "recover-push-without-job":
        assert settled["status"] == "succeeded", settled
        assert after_read.get("commit_read", 0) > before_read.get("commit_read", 0)
        assert _verify_merge(subject, source, pr, target, target, final) == merge
        assert after_read["human_push"] == counts["human_push"] + 1
    else:
        assert settled["status"] == "failed" and settled["error"].startswith("foreign_head:")
        assert final["lineage"]["head_sha"] == head
        assert final["publications"][-1] == [str(publication), str(request), "failed"]
        assert after_read["human_push"] == counts["human_push"] + 2
        _same_pull(pr, subject.pull(final), head=remote, base=target)
    m.evidence(
        control,
        request=str(request),
        publication=str(publication),
        prepared_merge=merge,
        remote_head=remote,
        publication_status=settled["status"],
        owner_status=settled["request_status"],
        push_job_removed=True,
    )
    return final


def _age_published_head(run: m.MaintenanceInstall, state: dict[str, Any]) -> None:
    publication, _, status = state["publications"][-1]
    assert status == "succeeded"
    aged = run.sql(f"""
        WITH aged AS (
            UPDATE curie.publications
            SET terminal_at=terminal_at - interval '121 seconds'
            WHERE id='{uuid.UUID(publication)}' AND status='succeeded'
                AND terminal_at IS NOT NULL
            RETURNING id
        )
        SELECT coalesce(json_agg(id), '[]') FROM aged
        """)
    assert aged == [publication]
    m.evidence("fixture conflict grace elapsed", publication=publication, seconds=121)


def _case(run: m.MaintenanceInstall, control: str, base_ref: str) -> m.Subject:
    starting = run.commit_branch(base_ref, fixture.BASELINE_FILES)
    run.checks((starting, "success", m.CHECK))
    run.model_changes(fixture.INITIAL_PR_FILES, list(fixture.VERIFICATION_COMMAND))
    subject = run.seed("Release tag conflict " + control)
    published = subject.published(1)
    _age_published_head(run, published)
    source = published if control == "active" else subject.green(published)
    pr = subject.pull(source)
    head, branch = pr["head"]["sha"], pr["head"]["ref"]
    assert pr["base"]["ref"] == base_ref and pr["base"]["sha"] == starting
    ids = run.pull_ids()
    counts = run.fixture("counts", head)
    run.fixture("mode", "hold")
    if control == "closed":
        run.api_replicas(0)
        try:
            run.forge("PATCH", f"pulls/{pr['number']}", {"state": "closed"})
            counts = run.fixture("counts", head)
            target = fixture.advance_conflicting_base(run.commit_branch, pr)
            _closed_graph_conflicts(run, head, target)
            # The forge reports mergeability only on open pulls. A failed check
            # triggers a poll of the closed pull with the conflicting Git graph.
            run.checks((head, "failure", m.CHECK))
        finally:
            run.api_replicas(1)
        final = subject.event("pr_closed", head)
        assert subject.pull(final)["state"] == "closed"
        m.unchanged(source, final, counts, run.fixture("counts", head), human_pushes=1)
        _same_pull(pr, subject.pull(final), head=head, base=target)
        assert run.pull_ids() == ids
        m.evidence(
            "closed conflict",
            head=head,
            admitted=0,
            model_calls=0,
            factory_pushes=0,
            human_pushes=1,
        )
        return subject

    files = {
        "protected": fixture.PROTECTED_BASE_ADVANCE_FILES,
        "incompatible": fixture.INCOMPATIBLE_BASE_FILES,
    }.get(control, fixture.BASE_ADVANCE_FILES)
    target = fixture.advance_conflicting_base(run.commit_branch, pr, files=files)
    _dirty(subject, source, target)
    if control == "active":
        final, cause, _ = _failure(subject)
        assert cause == "merge_conflict" and len(final["requests"]) == 1
        assert final["requests"][0]["id"] == source["requests"][0]["id"]
        assert final["publications"] == source["publications"] and not final["admissions"]
        assert final["window"] is None or final["window"]["repairs_used"] == 0
        later = run.fixture("counts", head)
        m.check_pushes(counts, later, human_pushes=1)
        assert all(later[k] == counts[k] for k in ("model", "starts", "pr_write"))
        _same_pull(pr, subject.pull(final), head=head, base=target)
        assert run.pull_ids() == ids
        m.evidence(
            "active conflict",
            terminal_cause=cause,
            head=head,
            model_calls=0,
            factory_pushes=0,
            human_pushes=1,
        )
        return subject

    receipt = _admission(subject, source, pr, target)
    # Wait for the model's blocked HTTP request. The worker already prepared T and checked H.
    m.wait(
        "successor entered the model",
        lambda: run.fixture("counts", head)["model"] > counts["model"],
    )
    tip = target
    expected_head = head
    if control in {"completed", "exhausted"}:
        tip = run.commit_branch(base_ref, {"release-notes.txt": "Later base revision\n"})
        assert tip != target
    if control == "human-before-push":
        expected_head = run.commit_branch(
            branch, {"reviewer.txt": "Human revision\n"}, expected=head
        )
        assert run.git("rev-parse", expected_head + "^") == head
    if control not in _RECOVERY_CONTROLS:
        run.fixture("plan", json.dumps(repair_plan(control, head, target)))

    if control in _RECOVERY_CONTROLS:
        final = _recover_without_job(subject, control, source, pr, target, receipt, counts)
    elif control in {"completed", "exhausted"}:
        repaired = subject.published(2)
        merged = _verify_merge(subject, source, pr, target, tip, repaired)
        final = subject.green(repaired)
        assert subject.pull(final)["head"]["sha"] == merged
        later = run.fixture("counts", head)
        assert later["factory_push"] == counts["factory_push"] + 1
        assert later["human_push"] == counts["human_push"] + 2
        m.evidence(
            "conflict published",
            head=merged,
            parents=[head, target],
            base_tip=tip,
            pr=pr["number"],
            publication=final["publications"][-1][0],
            fresh_ci="passing",
            model_turns=1,
            factory_pushes=1,
            human_pushes=2,
        )
        if control == "exhausted":
            _age_published_head(run, final)

            def new_conflict() -> None:
                nonlocal tip
                tip = run.commit_branch(
                    base_ref, {fixture.CHANGE: fixture.INITIAL_PR_FILES[fixture.CHANGE]}
                )
                _dirty(subject, final, tip)

            m.refusal(subject, final, "budget_exhausted", new_conflict, human_pushes=1)
            _same_pull(pr, subject.pull(final), head=merged, base=tip)
    else:
        final, cause, notice = _failure(subject)
        if control == "protected":
            assert cause == "publication_refused"
            protection = "GitHub metadata changes cannot be published by this capability"
            assert protection in notice and protection in final["detail"]
            assert final["publications"] == source["publications"]
        elif control == "incompatible":
            assert cause == "revision_unchanged"
            assert _HANDOFF in notice and _HANDOFF in final["detail"]
            assert final["publications"] == source["publications"]
        else:
            assert expected_head != head
            assert run.git("show", expected_head + ":reviewer.txt") == "Human revision"
            assert cause == "approval_create_failed"
            observed = subject.event("foreign_head", head)
            [refusal] = [
                event
                for event in observed["events"]
                if event["outcome"] == "foreign_head" and event["head_sha"] == head
            ]
            assert refusal["reason"] == "base_conflict"
            assert refusal["repair_request_id"] == receipt["repair_request_id"]
            assert observed["publications"] == final["publications"] == source["publications"]
        _same_pull(pr, subject.pull(final), head=expected_head, base=tip)
        later = run.fixture("counts", head)
        human_pushes = 2 if control == "human-before-push" else 1
        m.check_pushes(counts, later, human_pushes=human_pushes)
        assert later["pr_write"] == counts["pr_write"]
        m.evidence(
            control,
            admitted_head=head,
            remote_head=expected_head,
            terminal_cause=cause,
            request=receipt["repair_request_id"],
            model_turns=1,
            factory_pushes=0,
            human_pushes=human_pushes,
        )
    assert run.pull_ids() == ids
    assert final["requests"][:-1] == source["requests"]
    assert final["requests"][-1]["id"] == receipt["repair_request_id"]
    assert final["publications"][0] == source["publications"][0]
    assert final["admissions"] == [receipt] and final["window"]["repairs_used"] == 1
    assert all(final["window"][k] == source["window"][k] for k in m.ORIGIN)
    assert run.fixture("counts", head)["starts"] == counts["starts"] + 1
    return subject


def conflict(run: m.MaintenanceInstall, control: str) -> None:
    if control not in fe.MAINTENANCE_CONTROLS["conflict"]:
        raise ValueError(f"unknown conflict control {control}")
    base_ref = run.git("symbolic-ref", "--short", "HEAD")
    ref = "refs/heads/" + base_ref
    baseline = run.git("rev-parse", ref)
    subject = _case(run, control, base_ref)
    current = subject.pull(subject.snapshot())
    if current["state"] == "open":
        run.forge("PATCH", f"pulls/{current['number']}", {"state": "closed"})
    # Each control seeds a fresh issue from the installation's original repository.
    run.human_push("restore", base_ref, current["base"]["sha"], baseline)
    assert run.git("rev-parse", ref) == baseline
    run.fixture("clear-plan")
