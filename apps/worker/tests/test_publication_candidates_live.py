"""Live proof of publication candidate retention (#5428, acceptance criterion 4).

Opt in with ``CURIE_FACTORY_CANDIDATE_RETENTION_LIVE=1``. Unset, the module
skips. Set, every input is required and setup errors, never skips, when one is
missing or fails its probe.

The run drives the real factory on images built from ``CURIE_FACTORY_CANDIDATE_SHA``
through the factory end to end driver (tools/factory-e2e/factory_e2e.py): an owned
throwaway namespace, the candidate chart with candidate retention on, poll intake
under a label unique to this run, the real supported model and the shared public
fixture repository. One issue asks for a one-line change to a tracked file the
install protects, so publication validation refuses the change after the
candidate was captured. The test then shows exactly one complete candidate in the
private metadata route, no publication and no pull request, no model call after
capture, the runtime and its claim base gone, and the candidate's base and patch
read back inside the worker through the real reader, digest checked and applied.

The fixture repository is shared, so the driver's fixture reset, webhook repoint,
tunnel and card steps are never used. The test removes only what it created: its
issue (closed), branches under its own publication prefix or named by its own
WorkItem, their pull requests, and its label.

Every target comes from the caller's environment: the factory driver's own
variables (kube context, App kit, fixture repository, actor token, layer
registry, buildx builder) plus ``CURIE_FACTORY_CANDIDATE_SHA``, ``CURIE_BIN``,
``MODEL_KEY_FILE`` and ``CURIE_FACTORY_SPEND_LEDGER_USD``. Credentials are read
from files or the environment inside this process and never printed; failure
text is scrubbed of them. Evidence lines carry ids, digests, sizes and counts.
"""

from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from curie_worker.publication_validation import OPERATOR_PROTECTED_REFUSAL

from apps.worker.tests.publication_candidates_live_support import (
    ADMISSION_TIMEOUT_S,
    FLAG,
    HEX64,
    LEDGER_LIMIT_USD,
    POLL_S,
    PUBLISHED_STATES,
    QUIET_WINDOW_S,
    REPO_ROOT,
    RUN_TIMEOUT_S,
    RUNTIME_GONE_TIMEOUT_S,
    SHA40,
    WORKER_PROGRAM,
    Live,
    SetupRefused,
    Watch,
    admitted,
    candidate_revision_check,
    choose_protected_file,
    decimal_value,
    driver,
    ended,
    evidence,
    forge_cleanup,
    items_of,
    missing_inputs,
    observe_runtime,
    probe_label,
    quiet,
    read_candidates,
    read_usage,
    spend,
    usage_shape,
    which,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get(FLAG), reason=f"live candidate retention proof; set {FLAG}=1 to run"
)


@pytest.fixture
def live() -> Iterator[Live]:
    if os.environ.get(FLAG) != "1":
        pytest.fail(f"{FLAG} is set but not 1; set it to 1 to run, or unset it", pytrace=False)
    fe = driver()
    missing = missing_inputs(fe, os.environ)
    if missing:
        pytest.fail("live inputs missing: " + "; ".join(missing), pytrace=False)
    env = os.environ
    candidate = env["CURIE_FACTORY_CANDIDATE_SHA"]
    nonce = uuid.uuid4().hex[:8]
    model_key = fe._read_secret_file(Path(env["MODEL_KEY_FILE"])) or ""
    holder: dict[str, Any] = {}
    failure = ""
    try:
        ledger = decimal_value(env["CURIE_FACTORY_SPEND_LEDGER_USD"])
        assert ledger is not None
        if ledger < 0 or ledger > LEDGER_LIMIT_USD:
            raise SetupRefused(f"the spend ledger is {ledger} USD; at most 20.00 may remain")
        if not model_key:
            raise SetupRefused("MODEL_KEY_FILE is empty")
        wired = env.get("CURIE_FACTORY_MODEL_API_KEY")
        if wired and wired != model_key:
            raise SetupRefused(
                "CURIE_FACTORY_MODEL_API_KEY differs from MODEL_KEY_FILE; the spend reading "
                "must measure the key the install uses"
            )
        head = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if head != candidate:
            raise SetupRefused("CURIE_FACTORY_CANDIDATE_SHA is not the checkout HEAD")
        curie_bin, version = candidate_revision_check(fe, candidate, env["CURIE_BIN"])
        spend_probe = spend(fe, model_key)
        label = f"curie-cand-{nonce}"
        config = dataclasses.replace(
            fe.load_config(env, context=None),
            label=label,
            model_api_key=model_key,
            curie_bin=curie_bin,
        )
        namespace = fe.validate_namespace(f"{fe.NAMESPACE_PREFIX}cand-{nonce}")
        p = fe.Preflight(
            config,
            repo_root=REPO_ROOT,
            candidate=candidate,
            namespace=namespace,
            evidence_path=Path(os.devnull),
            admission_timeout=ADMISSION_TIMEOUT_S,
            candidate_images="build",
        )
        holder["p"] = p
        p._lock(f"app-{config.app_id}", "this App and its fixture repository")
        p._lock(f"context-{config.kube_context}", "this kube context")
        absent = [tool for tool in ("kubectl", "helm", "git", "openssl") if not which(tool)]
        if absent:
            raise SetupRefused(f"required tools not on PATH: {', '.join(absent)}")
        p.check_build_tools()
        p.check_app()
        protected = choose_protected_file(p)
        probe_label(fe, p, nonce)
        holder["live"] = Live(
            fe=fe,
            p=p,
            candidate=candidate,
            model_key=model_key,
            ledger_usd=ledger,
            protected_path=protected,
            label=label,
            branch_prefix=f"cand-{nonce}/",
            nonce=nonce,
            curie_version=version,
            spend_before=spend_probe,
        )
    except Exception as exc:  # noqa: BLE001 - every probe failure is a setup error
        failure = f"{type(exc).__name__}: {exc}"
    if failure:
        secrets = [model_key, os.environ.get("CURIE_FACTORY_ACTOR_TOKEN")]
        if "p" in holder:
            holder["p"].teardown.run()
            shutil.rmtree(holder["p"].workdir, ignore_errors=True)
            secrets += [holder["p"].config.actor_token, holder["p"].config.webhook_secret]
        redacted, _ = fe.redact_agent_text(failure, secrets)
        pytest.fail(f"live setup refused: {redacted}", pytrace=False)
    state: Live = holder["live"]
    evidence(
        "setup",
        candidate=state.candidate,
        curie_version=state.curie_version,
        ledger_usd=str(state.ledger_usd),
        spend_probe_usd=str(state.spend_before),
        namespace=state.p.namespace,
        default_branch_read=True,
        probe_label="created and deleted",
    )
    try:
        yield state
    finally:
        state.p.teardown.run()
        shutil.rmtree(state.p.workdir, ignore_errors=True)


# --------------------------------------------------------------------------
# The flow
# --------------------------------------------------------------------------


def _install_values(protected_path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def extra(values: dict[str, Any]) -> dict[str, Any]:
        worker = values.setdefault("worker", {})
        worker.setdefault("workspace", {})["publicationCandidates"] = {"enabled": True}
        # CURIE_PUBLICATION_PROTECTED_PATHS is chart-owned: worker.extraEnv refuses
        # it, so the chart value that renders it is set instead.
        worker.setdefault("publication", {})["protectedPaths"] = [protected_path]
        return values

    return extra


def _step_install(live: Live) -> str:
    fe, p = live.fe, live.p
    live.spend_before = spend(fe, live.model_key)
    # Registered before the first creation, so it runs last on every exit.
    p.teardown.push("remove owned fixture objects", lambda: forge_cleanup(live))
    p.build_candidate_images()
    images = p.candidate_images or {}
    tag = fe.candidate_image_tag(live.candidate)
    assert set(images) == set(fe.CANDIDATE_BUILDS), "not every candidate image was built"
    for image in images.values():
        assert image.tag == tag, f"{image.name} is not tagged for the candidate"
        assert image.digests and all(fe.digest_of(d) == d for d in image.digests), image.name
    p.create_namespace()
    p.install(extra_values=_install_values(live.protected_path))
    records = p.evidence.get("running_images") or []
    assert records, "no running container uses a built candidate image"
    for record in records:
        assert fe.digest_of(record["image_id"]) in record["expected"], record["pod"]
    p.port_forward()
    p.bind_agent()
    agent_id = str(p.evidence["agent_id"])
    status, _ = p.api(
        "PATCH",
        f"/agents/{agent_id}",
        headers={"X-API-Key": p.api_key},
        body={"publication_branch_prefix": live.branch_prefix},
    )
    assert status == 200, f"setting the run's publication branch prefix answered HTTP {status}"
    evidence(
        "1-install",
        spend_before_usd=str(live.spend_before),
        image_tag=tag,
        images={name: list(image.digests) for name, image in images.items()},
        running_containers=len(records),
        namespace=p.namespace,
        agent_id=agent_id,
    )
    return agent_id


def _step_run(live: Live, agent_id: str, watch: Watch) -> tuple[str, dict[str, Any]]:
    fe, p = live.fe, live.p
    repo = f"/repos/{p.config.repo}"
    status, _ = p.as_actor("POST", f"{repo}/labels", {"name": live.label, "color": "5319e7"})
    assert status == 201, f"creating the run label answered HTTP {status}"
    head = p.default_branch_head()
    live.issue_title = f"Candidate retention check {live.nonce}"
    live.issue_body = (
        f"Append exactly one line, `candidate retention {live.nonce}`, to the end of "
        f"`{live.protected_path}`. Change no other file and make no other edit."
    )
    p.issue_spec = (live.issue_title, live.issue_body)
    number = p.open_labelled_issue()
    live.issue_numbers.add(int(number))
    work_item_id = str(
        fe._wait(
            "the WorkItem admission",
            ADMISSION_TIMEOUT_S,
            lambda: admitted(live, agent_id, number),
            10,
        )
    )
    evidence("2-admitted", issue_number=number, work_item_id=work_item_id)

    def probe() -> dict[str, Any] | None:
        observe_runtime(live, watch)
        if watch.capture_usage is None:
            _, listing = read_candidates(live, work_item_id)
            if any(c.get("status") == "complete" for c in listing.get("candidates") or []):
                watch.capture_usage = read_usage(live, work_item_id)
        detail = p.work_item_detail(work_item_id)
        return detail if ended(detail, fe) else None

    detail = fe._wait("the execution request to end", RUN_TIMEOUT_S, probe, POLL_S)
    if watch.capture_usage is None:
        watch.capture_usage = read_usage(live, work_item_id)
        watch.capture_usage_after_terminal = True
    last = detail["requests"][-1]
    evidence(
        "2-ended",
        work_item_id=work_item_id,
        requests=len(detail["requests"]),
        status=last.get("status"),
        terminal_cause=last.get("terminal_cause"),
        claims_seen=len(watch.claims),
        default_head=head,
    )
    return work_item_id, detail


def _step_metadata(live: Live, work_item_id: str) -> dict[str, Any]:
    p = live.p
    text, body = read_candidates(live, work_item_id)
    for forbidden in (
        live.issue_title,
        live.issue_body,
        live.protected_path,
        "publication-candidates/",
        "http",
    ):
        assert forbidden not in text, "candidate metadata carries a private value"
    assert body.get("capture_outcome") == "complete", body.get("capture_outcome")
    candidates = body.get("candidates") or []
    assert len(candidates) == 1, f"{len(candidates)} candidates, expected exactly one"
    candidate = candidates[0]
    assert candidate.get("status") == "complete", candidate.get("status")
    assert candidate.get("available") is True, "the candidate is not available"
    patch, base = candidate.get("patch") or {}, candidate.get("base") or {}
    for part in (patch, base):
        assert int(part.get("size_bytes") or 0) > 0, "a candidate object has no size"
        assert HEX64.fullmatch(str(part.get("sha256") or "")), "a candidate digest is missing"
    assert SHA40.fullmatch(str(base.get("base_commit") or "")), "the base commit is missing"
    assert int(candidate.get("changed_path_count") or 0) >= 1, "the candidate changes nothing"

    detail = p.work_item_detail(work_item_id) or {}
    if isinstance(detail.get("pr"), dict):
        # Attributed to this run, so the forge cleanup closes it and its branch.
        live.pr_numbers.add(int(detail["pr"]["number"]))
    assert detail.get("pr") is None, "the WorkItem records a pull request"
    assert detail.get("state") not in PUBLISHED_STATES, detail.get("state")
    publication = detail.get("publication") or {}
    assert publication.get("status") != "published", "the WorkItem was published"
    repo = f"/repos/{p.config.repo}"
    pulls = [
        pull
        for pull in p._paged(f"{repo}/pulls?state=all")
        if str((pull.get("head") or {}).get("ref") or "").startswith(live.branch_prefix)
    ]
    assert not pulls, f"{len(pulls)} pull requests exist for the run"
    sources = _refusal_sources(live, detail)
    assert sources, "no record shows validation refusing the protected path"
    evidence(
        "3-metadata",
        work_item_id=work_item_id,
        candidate_id=candidate.get("candidate_id"),
        origin_request_id=candidate.get("origin_request_id"),
        patch_sha256=patch.get("sha256"),
        patch_bytes=patch.get("size_bytes"),
        base_sha256=base.get("sha256"),
        base_bytes=base.get("size_bytes"),
        base_commit=base.get("base_commit"),
        changed_path_count=candidate.get("changed_path_count"),
        state=detail.get("state"),
        pull_requests=0,
        refusal_seen_in=sources,
    )
    return dict(candidate)


def _refusal_sources(live: Live, detail: Mapping[str, Any]) -> list[str]:
    """Where validation's protected-path refusal is recorded. Only names are returned."""

    p = live.p
    found: list[str] = []
    for request in detail.get("requests") or []:
        fields = (request.get("last_start_failure_detail"), request.get("termination_observation"))
        if any(OPERATOR_PROTECTED_REFUSAL in str(value or "") for value in fields):
            found.append("work_item_request")
            break
    number = max(live.issue_numbers)
    comments = p._paged(f"/repos/{p.config.repo}/issues/{number}/comments")
    if any(OPERATOR_PROTECTED_REFUSAL in str(c.get("body") or "") for c in comments):
        found.append("issue_comment")
    logs = p.kubectl(
        "-n",
        p.namespace,
        "logs",
        f"deployment/{p.release}-worker",
        "-c",
        "worker",
        "--since=3h",
        check=False,
    )
    if OPERATOR_PROTECTED_REFUSAL in logs:
        found.append("worker_log")
    return found


def _step_no_model_calls(
    live: Live, work_item_id: str, candidate: Mapping[str, Any], watch: Watch
) -> None:
    fe = live.fe
    quiet(fe, QUIET_WINDOW_S, "the post-terminal quiet window")
    after = read_usage(live, work_item_id)
    assert watch.capture_usage is not None
    before_shape, after_shape = usage_shape(watch.capture_usage), usage_shape(after)
    assert before_shape["total_tokens"] > 0, "no model usage was recorded by capture time"
    assert before_shape == after_shape, "model usage changed after the candidate was captured"
    completed = fe._parse_time(candidate.get("completed_at"))
    assert completed is not None, "the candidate has no completed_at"
    detail = live.p.work_item_detail(work_item_id) or {}
    later_requests = [
        r
        for r in detail.get("requests") or []
        if (fe._parse_time(r.get("started_at")) or completed) > completed
    ]
    assert not later_requests, "a request started after the candidate was captured"
    later_pods = [
        name
        for name, created in watch.pods.items()
        if name.split("/", 1)[0] in watch.claims
        and (fe._parse_time(created) or completed) > completed
    ]
    assert not later_pods, "a runner pod started after the candidate was captured"
    evidence(
        "4-no-model-calls",
        work_item_id=work_item_id,
        tokens_at_capture=before_shape["total_tokens"],
        tokens_after_quiet=after_shape["total_tokens"],
        model_rows=len(after_shape["models"]),
        capture_read_after_terminal=watch.capture_usage_after_terminal,
        requests_started_after_capture=0,
        runner_pods_started_after_capture=0,
        turn_timing="not exposed by the API; usage, request starts and pods are the evidence",
    )


def _step_runtime_cleanup(
    live: Live, work_item_id: str, candidate: Mapping[str, Any], watch: Watch
) -> None:
    fe, p = live.fe, live.p
    assert watch.claims, "no sandbox claim was observed for the run"

    def gone() -> bool:
        claims, pods = observe_runtime(live, watch)
        return not (claims & watch.claims) and not any(
            name.split("/", 1)[0] in watch.claims for name in pods
        )

    fe._wait("the run's sandbox claim and runner pod to go", RUNTIME_GONE_TIMEOUT_S, gone, POLL_S)
    params = {
        "work_item_id": work_item_id,
        "candidate_id": str(candidate["candidate_id"]),
        "protected_path": live.protected_path,
    }
    program = WORKER_PROGRAM.replace("__PARAMS__", repr(json.dumps(params)))
    result = subprocess.run(
        [
            "kubectl",
            "--context",
            p.config.kube_context,
            "-n",
            p.namespace,
            "exec",
            "-i",
            f"deployment/{p.release}-worker",
            "-c",
            "worker",
            "--",
            "python",
            "-",
        ],
        input=program,
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert lines, f"the worker read printed nothing (exit {result.returncode})"
    out = json.loads(lines[-1])
    assert result.returncode == 0 and "error" not in out, f"the worker read failed: {out}"
    patch, base = candidate["patch"], candidate["base"]
    assert out["claim_archives"] == 0, "a claim base object remains in the object store"
    assert out["ownership_records"] == 0, "a claim ownership record remains"
    for kind, ledger in (("patch", patch), ("base", base)):
        read = out[kind]
        assert read["sha256"] == read["ledger_sha256"] == ledger["sha256"], kind
        assert read["size_bytes"] == read["ledger_size_bytes"] == ledger["size_bytes"], kind
    assert out["base_head"] == base["base_commit"], "the base is not at its recorded commit"
    assert out["changed_files"] == candidate["changed_path_count"], "changed file count differs"
    assert out["protected_in_base"] is True, "the protected file is not in the base"
    assert out["protected_changed"] is True, "the agent's change to the protected file is lost"
    evidence(
        "5-runtime-cleanup",
        work_item_id=work_item_id,
        claims_gone=len(watch.claims),
        claim_archives=0,
        ownership_records=0,
        patch_sha256=out["patch"]["sha256"],
        patch_bytes=out["patch"]["size_bytes"],
        base_sha256=out["base"]["sha256"],
        base_bytes=out["base"]["size_bytes"],
        changed_files=out["changed_files"],
        protected_changed=True,
    )


def _flow(live: Live) -> None:
    watch = Watch()
    agent_id = _step_install(live)
    work_item_id, _detail = _step_run(live, agent_id, watch)
    candidate = _step_metadata(live, work_item_id)
    _step_no_model_calls(live, work_item_id, candidate, watch)
    _step_runtime_cleanup(live, work_item_id, candidate, watch)


def _teardown(live: Live) -> str:
    """Run the driver's teardown stack (the forge cleanup runs last) and verify it."""

    p = live.p
    results = p.teardown.run()
    failed = {r["step"]: live.scrub(str(r["detail"])) for r in results if not r["ok"]}
    created = bool(p.evidence.get("run_id"))
    left: list[str] = []
    if created:
        for name in (p.namespace, p.publication_namespace()):
            if not p._namespace_absent(name):
                left.append("namespace")
        kinds = "clusterroles,clusterrolebindings,priorityclasses"
        for item in items_of(p.kubectl("get", kinds, "-o", "json", check=False)):
            annotations = (item.get("metadata") or {}).get("annotations") or {}
            if annotations.get("meta.helm.sh/release-namespace") == p.namespace:
                left.append("cluster object")
    forge: Any = next(
        (r["detail"] for r in results if r["step"] == "remove owned fixture objects"), {}
    )
    evidence(
        "6-teardown",
        steps=len(results),
        failed_steps=sorted(failed),
        namespace_created=created,
        leftovers=len(left),
        forge=forge if isinstance(forge, dict) else "failed",
    )
    problems = [f"{step}: {detail}" for step, detail in failed.items()] + left
    return "teardown incomplete: " + "; ".join(problems) if problems else ""


def _report_spend(live: Live) -> None:
    try:
        quiet(live.fe, QUIET_WINDOW_S, "the spend settle window")
        after = spend(live.fe, live.model_key)
    except Exception as exc:  # noqa: BLE001 - the reading is evidence, not a gate
        evidence("6-spend", spend_after_usd=None, reason=type(exc).__name__)
        return
    before = live.spend_before
    evidence(
        "6-spend",
        spend_before_usd=str(before),
        spend_after_usd=str(after),
        delta_usd=str(after - before) if before is not None else None,
    )


def test_publication_candidate_survives_refused_validation_live(live: Live) -> None:
    failure = ""
    try:
        _flow(live)
    except Exception as exc:  # noqa: BLE001 - scrubbed below, then failed
        failure = live.scrub(f"{type(exc).__name__}: {exc}")
    teardown_failure = _teardown(live)
    _report_spend(live)
    problems = [text for text in (failure, teardown_failure) if text]
    if problems:
        pytest.fail(" | ".join(problems), pytrace=False)
