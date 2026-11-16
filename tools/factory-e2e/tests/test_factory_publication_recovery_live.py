"""Live candidate-install driver for #5429 acceptance criterion 3 (ADR 0269).

A factory run whose publication fails on the forge after the model produced
its candidate is recovered from the retained candidate: one successor request
of execution kind ``publication_recovery`` publishes the identical bytes with
no model call, and the forge ends with exactly one branch, one publication and
one pull request.

Opt in with ``CURIE_FACTORY_PUBLICATION_RECOVERY_LIVE=1``. Unset, the module
skips. Set, every input below is required, and a missing, malformed or
unusable one fails setup with a message naming it before anything is created.

Everything inside the changed path is the candidate: API, worker, reconciler,
runner and chart run from the caller's candidate images and chart, driven
through the candidate CLI. This rehearsal leg uses two fixtures: the model is
the runner's offline fake tier (no paid call; the agent's standing directive
writes one file into the checkout), and the forge is the recorded TLS GitHub
stub (tools/github-stub), one Pod in the owned namespace. The paid leg AC3
also requires (the real supported model and fixture forge, with the spend
check and credentials) is a separate run.

The failure injection
---------------------
The forge fault proxy (``tools/factory-e2e/forge_fault_proxy.py``), one more
Pod, stands in for the forge for the whole install: ``api.githubApiUrl``,
``api.githubCloneBase``, the factory's code host and tracker entries and the
stub address in every egress rule all name the proxy instead of the stub. It
forwards every request, REST and git alike, to the stub unchanged except the
pull request creates it is armed to fail, and rewrites the stub's origin to
its own in JSON answers. Routing only the REST base through it would not
work: the API derives its GitHub identity host from ``api.githubApiUrl`` and
refuses a pull request whose ``html_url`` sits on another host, and refuses
to start when a factory code host is on another host. The install runs with
``worker.publication.reconcileMaxAttempts=2`` and the proxy answers exactly the
first two pull request creates for the fixture repository with HTTP 502.

Why that lands on ``publication_failed`` with a retained candidate:

1. Capture happens at the publication pause, before the publication exists, so
   the candidate is complete whatever the forge does afterwards.
2. The publication Job pushes the branch; the forge never sees a failure there,
   so the pushed commit is on the forge before anything fails.
3. The API's pull request create then fails. The API answers the worker 503,
   and the worker's code host client charges a reconcile attempt for every
   non-success answer (PublicationReconcileError, not the uncharged identity
   escape). The first charged failure backs off 15 s; the second reaches the
   ceiling and the store marks the publication failed.
4. The API settles the running request ``publication_failed``, which the retry
   policy admits as a publication recovery for an exact retained candidate.

Two alternatives do not work. A protected-path refusal (the #5428 proof) is a
validation refusal, terminal for recovery. A plain forge outage under the
default ceiling of ten charged attempts backs off 15 s doubling to 300 s,
about 28 minutes in all, which runs into the 1800 s execution deadline after
the model's own run: the request then ends ``expired`` and admits nothing.
Lowering the ceiling is an operator chart value, not a test path; it changes
how soon the same failure becomes visible, not what it is. The proxy's two
failures are consumed by the original request; the recovery's create passes.

What it asserts
---------------
1. The original request ended failed with ``publication_failed``; its one
   publication failed after exactly two charged attempts, both answered by the
   proxy's injected failures, and exactly one complete candidate is retained.
2. Exactly one successor exists: execution kind ``publication_recovery``,
   ``retry_of_request_id`` the original, its own runtime epoch and sandbox
   claim, started after the original ended, completed, no model session
   reserved, recovering the captured candidate id with the captured content
   digest, which now owns the candidate.
3. Identical bytes: the candidate's patch and base digests, sizes and base
   commit are unchanged; the recovery publication's base is the candidate's
   base commit and its title and body are the candidate's.
4. No model call after capture: the WorkItem usage at capture equals the
   usage after recovery, and the recovery request recorded no tokens.
5. Exactly one branch, publication and pull request: the forge's one branch
   still holds the commit the original attempt pushed (adopted, not
   rewritten), one publication succeeded, the forge holds one pull request on
   that branch and commit, and exactly one pull request create reached the
   forge. No third request appears in a settle window afterwards.

Runbook
-------
Run only against a throwaway cluster the caller owns::

    CURIE_FACTORY_PUBLICATION_RECOVERY_LIVE=1 \\
    CURIE_FACTORY_KUBE_CONTEXT=<kube context> \\
    CURIE_FACTORY_PUBLICATION_RECOVERY_NAMESPACE=<new namespace> \\
    CURIE_FACTORY_PUBLICATION_RECOVERY_RELEASE=curie-<suffix> \\
    CURIE_FACTORY_PUBLICATION_RECOVERY_PULL_POLICY=IfNotPresent \\
    CURIE_C4_CANDIDATE_CLI=<absolute path to the compiled candidate curie> \\
    CURIE_C4_CANDIDATE_IMAGES='<registry>/curie-{component}:<tag or @digest>' \\
    uv run pytest -q tools/factory-e2e/tests/test_factory_publication_recovery_live.py \\
      --reruns 0 -s

Inputs (see ``candidate_install.read_inputs``): the kube context of a cluster
that runs Kubernetes 1.30 or newer, the agent-sandbox controller and every CRD
in ``charts/curie/crds`` (none is installed here); a namespace that does not
exist yet; a Helm release ``curie-<suffix>`` of at most 30 characters, never
``curie``; the compiled candidate CLI; an image reference template expanded
for ``api``, ``worker`` and ``runner``, each resolvable with ``docker buildx
imagetools inspect`` from this process and pullable by the cluster; and the
pull policy for every candidate image.

The install values this driver sets over the candidate base values:
``agentSandbox.runner.workspace.enabled=true``,
``worker.workspace.publicationCandidates.enabled=true`` (candidate retention
is off by default and needs workspaces), ``worker.publication.
reconcileMaxAttempts=2``, every forge address the proxy (above), the fake
model, and a per-agent ``FAKE_MODEL_DIRECTIVE`` through
``agentSandbox.runnerEnv``. The
agent's publication policy is ``auto``.

Teardown is registered before each resource it removes is created and runs on
every exit: stop the port-forward and the pod watch, uninstall, delete the
namespace (the stub, the proxy and their ConfigMaps go with it), delete every
cluster-scoped object of the release, and prove each absent. Evidence lines
(``publication-recovery-live {...}``) carry ids, digests, sizes, counts and
statuses, never a credential, issue text or file content.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import candidate_install as ci
import factory_e2e as fe
import publication_recovery_live as prl
import pytest
import resource_retry_support as rrs
import tls_code_host as tch

if prl.FLAG not in os.environ:
    pytest.skip(
        f"set {prl.FLAG}=1 to run the live publication recovery driver", allow_module_level=True
    )

ADMISSION_TIMEOUT_S = 300.0
# The model run, capture, the push and two charged attempts (15 s and 30 s backoff).
ORIGINAL_TIMEOUT_S = 1800.0
SUCCESSOR_TIMEOUT_S = 600.0
RECOVERY_TIMEOUT_S = 1800.0
SETTLE_S = 60.0
POLL_S = 5.0
# Stub replay time: the recorded pull request checks are complete a day in.
REPLAY_ADVANCE_S = 86400


def evidence(step: str, **facts: Any) -> None:
    line = json.dumps({"step": step, **facts}, sort_keys=True, default=str)
    print(f"publication-recovery-live {line}", flush=True)


@dataclass
class Recovery(ci.CandidateInstall):
    log_name: str = "publication-recovery"
    agent_id: str = ""
    issue: int = 0
    work_item_id: str = ""
    proxy_ip: str = ""
    proxy_logs: str = field(default="", repr=False)

    # --- the fault proxy ------------------------------------------------

    def start_proxy(self) -> None:
        ns = self.inputs.namespace
        self.kubectl(
            "-n",
            ns,
            "create",
            "configmap",
            prl.PROXY_UPSTREAM_CONFIG_MAP,
            f"--from-file=ca.crt={self.private_file('stub-ca.crt', self.stub_ca)}",
        )
        owner_key, owner_value = fe.OWNER_LABEL.split("=", 1)
        manifests = prl.proxy_manifests(
            ns,
            self.inputs.images["worker"],
            self.inputs.pull_policy,
            owner={owner_key: owner_value},
        )
        listing = {"apiVersion": "v1", "kind": "List", "items": manifests}
        self.kubectl("create", "-f", "-", input_text=json.dumps(listing))
        self.kubectl(
            "-n", ns, "wait", "--for=condition=Ready", f"pod/{prl.PROXY_NAME}", "--timeout=600s"
        )

        def started() -> dict[str, Any] | None:
            for line in self.read_proxy_logs().splitlines():
                with contextlib.suppress(ValueError):
                    parsed = json.loads(line)
                    if isinstance(parsed, dict) and parsed.get("event") == "startup":
                        return parsed
            return None

        startup = fe._wait("the fault proxy startup line", 120, started, 2)
        assert startup["base_url"] == prl.proxy_origin(ns), (
            f"the proxy advertises {startup['base_url']}"
        )
        pod = json.loads(self.kubectl("-n", ns, "get", "pod", prl.PROXY_NAME, "-o", "json"))
        self.assert_digest(pod, prl.PROXY_NAME, "worker")
        # The headless Service resolves to this Pod IP; egress rules name it.
        self.proxy_ip = str((pod.get("status") or {}).get("podIP") or "")
        assert self.proxy_ip, "the fault proxy Pod has no IP"
        ca = self.proxy_exec("cat", f"{prl.PROXY_STATE}/ca.pem")
        self.trust_code_host_ca(ca)
        self.log("fault proxy serving in front of the whole stub forge; its CA is trusted")

    def proxy_exec(self, *argv: str) -> str:
        return self.kubectl(
            "-n", self.inputs.namespace, "exec", prl.PROXY_NAME, "-c", prl.PROXY_NAME, "--", *argv
        )

    def read_proxy_logs(self) -> str:
        logs = self.kubectl(
            "-n",
            self.inputs.namespace,
            "logs",
            prl.PROXY_NAME,
            "-c",
            prl.PROXY_NAME,
            check=False,
        )
        if logs.strip():
            self.proxy_logs = logs
        return self.proxy_logs

    def arm_pull_create_failures(self) -> dict[str, Any]:
        out = self.proxy_exec(
            "python3",
            "/proxy/forge_fault_proxy.py",
            "fault",
            "--state",
            prl.PROXY_STATE,
            "--method",
            "POST",
            "--path",
            prl.PULL_CREATE_PATH,
            "--status",
            str(prl.FAULT_STATUS),
            "--count",
            str(prl.RECONCILE_MAX_ATTEMPTS),
        )
        return dict(json.loads(out.strip().splitlines()[-1]))

    def armed_faults(self) -> list[dict[str, Any]]:
        out = self.proxy_exec(
            "python3", "/proxy/forge_fault_proxy.py", "faults", "--state", prl.PROXY_STATE
        )
        return list(json.loads(out.strip().splitlines()[-1]))

    # --- reads ------------------------------------------------------------

    def rows(self) -> list[dict[str, Any]]:
        return self.requests(self.agent_id, self.issue)

    def brief(self) -> list[dict[str, Any]]:
        keys = (
            "id",
            "sequence",
            "status",
            "terminal_cause",
            "execution_kind",
            "retry_of_request_id",
            "retry_safety",
            "runtime_epoch",
            "runtime_claim_name",
            "started_at",
            "terminal_at",
            "last_start_failure_detail",
        )
        return [{key: row.get(key) for key in keys} for row in self.rows()]

    def publications(self) -> list[dict[str, Any]]:
        rows = self.sql(
            "SELECT coalesce(json_agg(row_to_json(r) ORDER BY r.created_at), '[]'::json) FROM ("
            "SELECT p.id, p.execution_request_id, p.status, p.reconcile_attempts, p.error, "
            "p.base_sha, p.revision_number, p.created_at, l.branch, l.pr_number, l.head_sha, "
            "encode(sha256(convert_to(p.title, 'UTF8')), 'hex') AS title_sha256, "
            "encode(sha256(convert_to(p.body, 'UTF8')), 'hex') AS body_sha256 "
            "FROM curie.publications p "
            "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
            "JOIN curie.execution_requests e ON e.id = p.execution_request_id "
            f"WHERE e.work_item_id = '{self.require_work_item()}') r"
        )
        assert isinstance(rows, list), f"unexpected publication listing {rows!r}"
        return rows

    def candidate_row(self) -> dict[str, Any]:
        rows = self.sql(
            "SELECT coalesce(json_agg(row_to_json(r)), '[]'::json) FROM ("
            "SELECT c.id, c.origin_request_id, c.owner_request_id, c.previous_owner_request_id, "
            "c.status, c.content_sha256, "
            "encode(sha256(convert_to(c.title, 'UTF8')), 'hex') AS title_sha256, "
            "encode(sha256(convert_to(c.body, 'UTF8')), 'hex') AS body_sha256 "
            "FROM curie.publication_candidates c "
            f"WHERE c.work_item_id = '{self.require_work_item()}') r"
        )
        assert isinstance(rows, list) and len(rows) == 1, f"candidate ledger rows: {rows!r}"
        return dict(rows[0])

    def require_work_item(self) -> str:
        assert self.work_item_id, "the WorkItem is not admitted yet"
        return str(uuid.UUID(self.work_item_id))

    def candidates(self) -> dict[str, Any]:
        listing = self.api("GET", f"/work-items/{self.work_item_id}/publication-candidates")
        assert isinstance(listing, dict), "the candidate listing is not an object"
        return listing

    def usage(self) -> dict[str, Any]:
        usage = self.api("GET", f"/work-items/{self.work_item_id}/usage")
        assert isinstance(usage, dict), "the WorkItem usage is not an object"
        return usage

    def forge_heads(self) -> dict[str, str]:
        out = self.stub_exec(
            "git",
            "--git-dir",
            tch.STUB_BARE_REPO,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads",
        )
        return prl.branch_heads(out)

    def forge_pulls(self) -> list[dict[str, Any]]:
        status, body = self.stub_api("GET", f"/repos/{tch.STUB_REPO}/pulls?state=all&per_page=100")
        assert status == 200 and isinstance(body, list), f"the stub pull listing answered {status}"
        return [pull for pull in body if isinstance(pull, dict)]

    def advance_replay(self, seconds: int) -> None:
        """Move the stub's recorded check lifecycle forward, as its own ``advance`` does."""

        self.stub_exec(
            "python3",
            "-c",
            "import sqlite3, sys\n"
            f"c = sqlite3.connect({tch.STUB_DATABASE!r})\n"
            "c.execute('UPDATE clock SET seconds=seconds+?', (float(sys.argv[1]),))\n"
            "c.commit()\n",
            str(seconds),
        )


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def inputs() -> ci.Inputs:
    if os.environ.get(prl.FLAG) != "1":
        pytest.fail(f"{prl.FLAG} is set but not 1; set it to 1 to run, or unset it", pytrace=False)
    try:
        return ci.read_inputs(dict(os.environ), prl.NAMES)
    except ci.Refused as exc:
        refusal = str(exc)
    pytest.fail(refusal, pytrace=False)


@pytest.fixture(autouse=True)
def candidate_curie(inputs: ci.Inputs, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The real compiled candidate CLI, in place of conftest's fake CURIE_BIN."""

    monkeypatch.setenv("CURIE_BIN", inputs.cli)
    return Path(inputs.cli)


@pytest.fixture
def live(inputs: ci.Inputs) -> Iterator[Recovery]:
    workdir = Path(tempfile.mkdtemp(prefix="publication-recovery-"))
    workdir.chmod(0o700)
    harness = Recovery(inputs=inputs, workdir=workdir)
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
        yield harness
    finally:
        results = harness.teardown.run()
        failed = [f"{r['step']}: {r['detail']}" for r in results if not r["ok"]]
        for result in results:
            harness.log(f"teardown {result['step']}: {'ok' if result['ok'] else 'FAILED'}")
        evidence("teardown", steps=len(results), failed=len(failed))
        if failed:
            pytest.fail("teardown did not prove every owned resource absent: " + "; ".join(failed))


# --------------------------------------------------------------------------
# The proof
# --------------------------------------------------------------------------


def _install(live: Recovery) -> None:
    live.create_namespace()
    live.start_stub(prl.SEED_FILES, prl.SEED_ISSUE)
    live.start_proxy()
    live.install(
        prl.recovery_values(
            live.base_values(),
            namespace=live.inputs.namespace,
            stub_ip=live.stub_ip,
            proxy_ip=live.proxy_ip,
        )
    )
    live.port_forward()
    live.start_pod_watch()
    live.agent_id = live.create_agent(prl.AGENT, prl.TRIGGER_LABEL)
    live.deploy_bundle(prl.AGENT, policy="auto")
    evidence(
        "1-install",
        namespace=live.inputs.namespace,
        release=live.inputs.release,
        agent_id=live.agent_id,
        images={name: sorted(digests) for name, digests in live.digests.items()},
        reconcile_max_attempts=prl.RECONCILE_MAX_ATTEMPTS,
        github_api_url=prl.proxy_base_url(live.inputs.namespace),
        forge_origin=prl.proxy_origin(live.inputs.namespace),
    )


def _original(live: Recovery) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Run the original request into ``publication_failed``; return it, its candidate
    and the usage read when the candidate completed."""

    rule = live.arm_pull_create_failures()
    assert rule["remaining"] == prl.RECONCILE_MAX_ATTEMPTS, rule
    live.issue = live.seed_issue("Publication recovery proof", prl.SEED_ISSUE, prl.TRIGGER_LABEL)
    first = ci.wait_for(
        "the labelled issue to be admitted",
        ADMISSION_TIMEOUT_S,
        lambda: live.rows()[:1],
        live.brief,
    )[0]
    live.work_item_id = str(first["work_item_id"])
    evidence("2-admitted", issue=live.issue, work_item_id=live.work_item_id, request_id=first["id"])
    usage_at_capture: dict[str, Any] = {}

    def ended() -> dict[str, Any] | None:
        if not usage_at_capture:
            listing = live.candidates()
            if any(c.get("status") == "complete" for c in listing.get("candidates") or []):
                usage_at_capture.update(live.usage())
        row = next((r for r in live.rows() if r["id"] == first["id"]), None)
        return row if row is not None and row["status"] in ci.TERMINAL else None

    original = ci.wait_for("the original request to end", ORIGINAL_TIMEOUT_S, ended, live.brief)
    if not usage_at_capture:
        # The request ended between two polls; nothing runs a model after it ends.
        usage_at_capture.update(live.usage())
    candidate = prl.complete_candidate(live.candidates())
    creates = prl.pull_creates(live.read_proxy_logs())
    facts = prl.judge_original(original, live.publications(), creates)
    remaining = [rule["remaining"] for rule in live.armed_faults()]
    assert remaining == [0], f"armed faults left unconsumed: {remaining}"
    evidence(
        "3-original-failed",
        **facts,
        **prl.candidate_identity(candidate),
        tokens_at_capture=prl.usage_shape(usage_at_capture)["total_tokens"],
    )
    return original, candidate, usage_at_capture


def _recovery(
    live: Recovery,
    original: dict[str, Any],
    candidate: dict[str, Any],
    usage_at_capture: dict[str, Any],
) -> None:
    heads_after_failure = live.forge_heads()
    evidence(
        "4-forge-after-failure",
        branches=len(heads_after_failure),
        default_branch_present=prl.DEFAULT_BRANCH in heads_after_failure,
    )

    def successors() -> list[dict[str, Any]]:
        return [r for r in live.rows() if r.get("retry_of_request_id") == original["id"]]

    found = ci.wait_for("the recovery successor", SUCCESSOR_TIMEOUT_S, successors, live.brief)
    successor_id = found[0]["id"]
    advanced = False

    def done() -> dict[str, Any] | None:
        nonlocal advanced
        if not advanced and live.forge_pulls():
            # The pull request exists; finish its recorded checks so CI settles.
            live.advance_replay(REPLAY_ADVANCE_S)
            advanced = True
        row = next((r for r in live.rows() if r["id"] == successor_id), None)
        return row if row is not None and row["status"] in ci.TERMINAL else None

    ci.wait_for("the recovery request to end", RECOVERY_TIMEOUT_S, done, live.brief)

    def no_further_request() -> None:
        rows = live.rows()
        assert len(rows) == 2, f"a request beyond the one successor appeared: {live.brief()}"

    rrs.observe_for(SETTLE_S, no_further_request, interval=POLL_S)
    listing = live.candidates()
    listed = [c for c in listing.get("candidates") or [] if isinstance(c, dict)]
    candidate_after = next(
        (c for c in listed if str(c.get("candidate_id")) == str(candidate["candidate_id"])), None
    )
    facts = prl.judge_recovery(
        requests=live.rows(),
        original=original,
        candidate=candidate,
        candidate_row=live.candidate_row(),
        candidate_after=candidate_after,
        publications=live.publications(),
        heads_after_failure=heads_after_failure,
        heads_after_recovery=live.forge_heads(),
        pulls=live.forge_pulls(),
        creates=prl.pull_creates(live.read_proxy_logs()),
        usage_at_capture=usage_at_capture,
        usage_after=live.usage(),
        default_branch=prl.DEFAULT_BRANCH,
    )
    evidence("5-recovered", **facts, candidate_listed_after=candidate_after is not None)


def test_failed_publication_recovers_from_the_retained_candidate(
    live: Recovery, candidate_curie: Path
) -> None:
    assert os.environ["CURIE_BIN"] == str(candidate_curie)
    driver = Path(__file__).read_bytes()
    evidence("0-driver", driver_sha256=hashlib.sha256(driver).hexdigest())
    _install(live)
    original, candidate, usage_at_capture = _original(live)
    _recovery(live, original, candidate, usage_at_capture)
