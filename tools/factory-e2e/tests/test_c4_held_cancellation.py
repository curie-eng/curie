"""#4627: a held factory run cancelled after its worker was replaced must settle.

A running WorkItem ExecutionRequest is HELD when its runtime lease is parked at
its execution deadline (``hold_for_approval``). Here a production tool gate holds
it: after deploy the test gates ``Edit`` with ``curie cluster approvals
factory-e2e --gate Edit``, the runner denies the transcript's first Edit, the
turn ends awaiting approval and the worker parks the lease. The publication
policy stays ``auto`` in every test. The tracker cancellation moves it to
``cancellation_requested/issue_cancelled``. If the owning worker pod was
replaced, the new worker's terminate wake must be able to claim termination,
tear the sandbox down and record it. Before the fix the
parked lease made every claim ``duplicate`` until the execution deadline, so the
request never settled and its SandboxClaim stayed.

Each test owns one disposable kind cluster, registry, GitHub stub and scripted
model (``scripted_kind.ScriptedRun``), installed from api, worker, ui and runner
images built from this checkout. Only the external forge and model are fixtures.

Configured timings (API ``extraEnv``):

1. ``CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS=45``
2. ``CURIE_WORK_ITEM_RECONCILER_INTERVAL_SECONDS=1``
3. ``CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS=5``
4. ``CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS=300``, longer than the 90 second
   settlement window, so the forced settle fallback cannot satisfy the pin.

The agent keeps the default 1800 second execution deadline; the test asserts at
hold time that the deadline leaves at least 600 seconds of span.

Two controls guard against a fix that orphans work it should not touch:

1. ``test_uncancelled_held_run_recovers_after_worker_replacement`` holds the
   run the same way and replaces the worker without cancelling. For more than
   twice the runtime TTL the request stays running, parked, under the same
   epoch and owner, with its Edit approval pending and its SandboxClaim
   present. Approval continuation is then driven by break-glass recovery
   (#2753, ``api.approvalRecovery.enabled`` on this install only), because a
   GitHub-surfaced approval admits no verifiable approver (ADR-0177 amendment,
   ``NoVerifiableApprovers``). A rejection resume is still an approval
   continuation of the same request: the replacement worker adopts it under
   the same epoch and replaces the parked lease with a live short one.
2. ``test_pending_ci_settles_after_worker_replacement`` sets no tool gate.
   Once the publication succeeded and CI is still pending on the stub's
   frozen replay clock, the worker is replaced. The request stays running for
   more than twice the runtime TTL, then completes once the clock
   advances past the recorded check runs.

Every infrastructure step and scenario premise (install, labelling, reaching
the held or pending CI state, the worker replacement and its premises) runs in
fixture setup, so it fails as a setup error. Each test body holds only the
behavior under test, so the reversed Fix pin run can fail only on it.

Runtime prerequisites: Docker (inheriting DOCKER_HOST), kind v0.24.0, kubectl,
Helm, Cargo, git and openssl. A remote Docker engine needs CURIE_LOCAL_STUB_HOST.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import scripted_kind as sk

ROOT = Path(__file__).resolve().parents[3]
ISSUE_NUMBER = 3815
LABEL = "factory"
COMPONENTS = ("api", "worker", "ui", "runner")
DEFAULT_IMAGES = {
    "curie-api:local": "api",
    "curie-worker:local": "worker",
    "curie-ui:local": "ui",
    "curie-runner:latest": "runner",
}
RUNTIME_TTL_SECONDS = 45
RECONCILER_INTERVAL_SECONDS = 1
TERMINATE_RETRY_SECONDS = 5
CANCEL_SETTLE_SECONDS = 300
API_TIMING_ENV = (
    ("CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS", RUNTIME_TTL_SECONDS),
    ("CURIE_WORK_ITEM_RECONCILER_INTERVAL_SECONDS", RECONCILER_INTERVAL_SECONDS),
    ("CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS", TERMINATE_RETRY_SECONDS),
    ("CURIE_WORK_ITEM_CANCEL_SETTLE_SECONDS", CANCEL_SETTLE_SECONDS),
)
HOLD_BOUND_SECONDS = 1500
MIN_DEADLINE_SPAN_SECONDS = 600
MIN_DEADLINE_REMAINING_SECONDS = 300
SETTLE_BOUND_SECONDS = 90
SURVIVE_SECONDS = 2 * RUNTIME_TTL_SECONDS + 20
# Fixture scenarios: the pin's hold, the recovery control's hold, pending CI.
SCENARIOS = ("cancel", "recover", "ci")
RECOVERY_BOUND_SECONDS = 180
# A live lease is renewed to now plus the runtime TTL; the margin covers one
# polling interval and clock skew between the database and this process.
LIVE_LEASE_MAX_SECONDS = RUNTIME_TTL_SECONDS + 15
RECOVERY_SUBJECT = "U0FACTORYE2E"
ORPHAN_CAUSES = {"owner_lost", "issue_cancelled", "execution_deadline"}
WORKER_SELECTOR = "app.kubernetes.io/component=worker,app.kubernetes.io/instance=curie"
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
SANDBOX_RESOURCE = "sandboxes.agents.x-k8s.io"


class InfrastructureError(RuntimeError):
    """An unavailable prerequisite or a broken premise, never the pinned verdict."""


def _clean_env() -> dict[str, str]:
    """This process's environment without inherited Curie, model or forge state.

    DOCKER_HOST stays inherited. CURIE_LOCAL_STUB_HOST is the one Curie input
    kept: kind pods on a remote engine reach the in-process fixtures through it.
    """

    env = os.environ.copy()
    stub_host = env.get("CURIE_LOCAL_STUB_HOST")
    for name in list(env):
        if name.startswith(
            ("CURIE_", "ANTHROPIC_", "OPENROUTER_", "CLAUDE_", "GITHUB_", "GH_", "SLACK_")
        ) or name in {"HELM_DRIVER", "KUBECONFIG"}:
            env.pop(name)
    if stub_host:
        env["CURIE_LOCAL_STUB_HOST"] = stub_host
    # The default context honours DOCKER_HOST, as ScriptedRun does.
    env["DOCKER_CONTEXT"] = "default"
    env["BUILDX_BUILDER"] = "default"
    env["HELM_DRIVER"] = "secret"
    env["NO_COLOR"] = "1"
    return env


def _command(
    argv: list[str],
    *,
    env: dict[str, str],
    timeout: int = 300,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        detail, _ = fe.redact_agent_text((result.stderr or result.stdout)[-3000:], [])
        raise InfrastructureError(
            f"{Path(argv[0]).name} {argv[1]} failed ({result.returncode}): {detail}"
        )
    return result


@dataclass(frozen=True)
class Candidate:
    run: str
    binary: Path
    images: dict[str, str]
    env: dict[str, str]


def _remove_owned_tag(tag: str, env: dict[str, str]) -> list[str]:
    """Remove one tag this test created and confirm its image is gone.

    A cached build can produce an image another tag already names. Only the
    tag is ours then, so only the tag's absence is confirmed.
    """

    found = _command(
        ["docker", "image", "inspect", "--format", "{{.Id}}", tag], env=env, check=False
    )
    if found.returncode:
        return []
    image_id = found.stdout.strip()
    listing = _command(
        ["docker", "image", "ls", "--no-trunc", "--format", "{{.ID}} {{.Repository}}:{{.Tag}}"],
        env=env,
    ).stdout.splitlines()
    shared = any(
        line.split(" ", 1)[0] == image_id and line.split(" ", 1)[1] != tag
        for line in listing
        if " " in line
    )
    _command(["docker", "image", "rm", tag], env=env)
    errors = []
    if not _command(["docker", "image", "inspect", tag], env=env, check=False).returncode:
        errors.append(f"owned candidate tag {tag} survived removal")
    if not shared:
        remaining = _command(
            ["docker", "image", "ls", "--quiet", "--no-trunc"], env=env
        ).stdout.split()
        if image_id in remaining:
            errors.append(f"owned candidate image for {tag} survived removal")
    return errors


@pytest.fixture
def candidate(request: pytest.FixtureRequest) -> Candidate:
    env = _clean_env()
    # 1. Every prerequisite is checked before anything is created. A missing
    # one is an error, never a skip.
    for tool in ("docker", "kind", "kubectl", "helm", "cargo", "git", "openssl"):
        if shutil.which(tool) is None:
            raise InfrastructureError(f"#4627 runtime prerequisite missing: {tool}")
    if not sk.docker_engine_is_local(env.get("DOCKER_HOST")) and not env.get(
        "CURIE_LOCAL_STUB_HOST"
    ):
        raise InfrastructureError(
            "a remote Docker engine requires CURIE_LOCAL_STUB_HOST for the in-process fixtures"
        )
    _command(["docker", "info", "--format", "{{.ServerVersion}}"], env=env, timeout=60)
    if "v0.24.0" not in _command(["kind", "version"], env=env, timeout=30).stdout:
        raise InfrastructureError("#4627 requires kind v0.24.0")
    common_dir = Path(
        _command(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], env=env
        ).stdout.strip()
    )
    if not common_dir.is_absolute():
        raise InfrastructureError("git did not resolve the checkout's absolute common directory")
    shared = common_dir.parent / ".projects"
    shared.mkdir(parents=True, exist_ok=True)
    run = uuid.uuid4().hex[:12]
    images = {component: f"curie-test-4627-{component}:{run}" for component in COMPONENTS}
    config = Path(tempfile.mkdtemp(prefix=f"test-4627-{run}-", dir=shared))
    env.update(
        {
            "CURIE_CONFIG_DIR": str(config / "curie-config"),
            "XDG_CONFIG_HOME": str(config / "config"),
        }
    )
    started: list[str] = []

    # 2. The finalizer is registered before the first build, and it removes
    # exactly the tags this fixture started building.
    def cleanup() -> None:
        errors: list[str] = []
        try:
            for tag in reversed(started):
                try:
                    errors.extend(_remove_owned_tag(tag, env))
                except (InfrastructureError, OSError, subprocess.SubprocessError) as exc:
                    errors.append(str(exc))
        finally:
            shutil.rmtree(config, ignore_errors=True)
        if errors:
            raise InfrastructureError("; ".join(errors))

    request.addfinalizer(cleanup)

    # 3. The CLI comes from this checkout. The target directory is shared on
    # purpose so reruns, including the reversed Fix pin run, are incremental.
    target = shared / "test-4627-cargo-target"
    env["CARGO_TARGET_DIR"] = str(target)
    _command(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(ROOT / "cli/Cargo.toml"),
            "--bin",
            "curie",
            "--target-dir",
            str(target),
        ],
        env=env,
        timeout=2400,
    )
    binary = target / "debug/curie"
    if not binary.is_file():
        raise InfrastructureError("cargo did not produce the candidate curie binary")
    _command([str(binary), "--version"], env=env)

    # 4. The images come from this checkout too, so a reversed product file in
    # a scratch worktree is what the cluster runs.
    for component in ("api", "worker", "ui"):
        started.append(images[component])
        _command(
            [
                "docker",
                "build",
                "-f",
                f"apps/{component}/Dockerfile",
                "-t",
                images[component],
                str(ROOT),
            ],
            env=env,
            timeout=2400,
        )
    started.append(images["runner"])
    _command([str(binary), "build", "--tag", images["runner"]], env=env, timeout=2400)
    for tag in started:
        _command(["docker", "image", "inspect", "--format", "{{.Id}}", tag], env=env)
    return Candidate(run, binary, images, env)


@pytest.fixture(autouse=True)
def candidate_curie(candidate: Candidate, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Override this directory's autouse shell double for this runtime module."""

    monkeypatch.setenv("CURIE_BIN", str(candidate.binary))
    return candidate.binary


@dataclass
class Held:
    request_id: str
    owner: str
    epoch: int
    claim: str
    sandbox: str
    execution_deadline: str
    approval_id: str
    old_pod: str = ""
    new_pod: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def _private_chart_values(values: dict[str, Any], candidate: Candidate) -> None:
    """Point the chart at this test's images and shorten the WorkItem cadence."""

    for component in ("api", "worker", "ui"):
        repository, tag = candidate.images[component].rsplit(":", 1)
        values[component]["image"].update(repository=repository, tag=tag, pullPolicy="Never")
    env = list(values["api"].get("extraEnv") or [])
    env.extend({"name": name, "value": str(value)} for name, value in API_TIMING_ENV)
    values["api"]["extraEnv"] = env


def _ready(pod: dict[str, Any]) -> bool:
    if pod["metadata"].get("deletionTimestamp") or pod.get("status", {}).get("phase") != "Running":
        return False
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod["status"].get("conditions", [])
    )


class HeldRun(sk.ScriptedRun):
    """ScriptedRun on private images, an optional tool gate and a scenario."""

    def __init__(self, args: argparse.Namespace, candidate: Candidate) -> None:
        super().__init__(args)
        self.curie = str(candidate.binary)
        # Inherited Curie and forge state never reaches the owned cluster.
        self.env = dict(candidate.env)
        self.env.pop("CURIE_CONFIG_DIR", None)
        self.env.pop("CARGO_TARGET_DIR", None)
        self.candidate = candidate
        self.scenario = ""
        self.gate_tool: str | None = None
        self.held: Held | None = None
        self.tolerate_model_errors = False
        self.approval_principal: str | None = None
        # An unresolved observation fault in a test body; the fixture teardown
        # raises it so it is reported as an error, never as a test failure.
        self.infra_error: str | None = None
        self.worker_token: str | None = None
        self.github: Any = None
        self.report: dict[str, Any] = {
            "issue": "#4627",
            "run": candidate.run,
            "run_id": self.run_id,
            "cluster": self.cluster,
            "timings": {name: value for name, value in API_TIMING_ENV},
            "verdict": "failed",
        }
        self.image_map = {
            default: candidate.images[component] for default, component in DEFAULT_IMAGES.items()
        }

    def command(self, *argv: str, timeout: float = 900) -> str:
        # 1. Default image names become this run's private tags. That covers the
        # runner registry source and the key generation container.
        rewritten = [self.image_map.get(item, item) for item in argv]
        output = super().command(*rewritten, timeout=timeout)
        # 2. The publication policy ScriptedRun writes (auto) is kept. Right
        # after it succeeds, a held scenario gates its tool through the same
        # CLI surface and connection flags deploy() uses.
        if "publication-policy" in rewritten and "--policy" in rewritten:
            self.report["publication_policy"] = rewritten[rewritten.index("--policy") + 1]
            if self.gate_tool is not None:
                api_url = rewritten[rewritten.index("--api-url") + 1]
                common = ["--namespace", self.namespace, "--release", "curie", "--api-url", api_url]
                super().command(
                    self.curie,
                    "cluster",
                    "--context",
                    self.context,
                    "approvals",
                    "factory-e2e",
                    "--gate",
                    self.gate_tool,
                    *common,
                    timeout=300,
                )
                self.report["approval_gate"] = {"tool": self.gate_tool, "set": True}
        return output

    def observe(self) -> None:
        """The test body asserts; observe only records."""

        self.report["observed_at_elapsed_seconds"] = self.elapsed()

    def elapsed(self) -> float:
        assert self.install_started is not None
        return round(time.monotonic() - self.install_started, 1)

    def wait_for_final(self) -> None:
        """Fixture setup only: reach the scenario's premise and replace the worker.

        execute() calls this, then observe(). Every step here runs during
        fixture setup, so any failure is a setup error. The behavior under test
        runs in the test body from the stored ``self.held``.
        """

        if self.scenario not in SCENARIOS:
            raise InfrastructureError(f"unknown #4627 scenario {self.scenario!r}")
        assert self.install_started is not None
        # The hold bound plus replacement and the longest scenario window; every
        # command stays clamped to it, as ScriptedRun clamps to its own bound.
        self.deadline = self.install_started + HOLD_BOUND_SECONDS + 1200
        self.github = sk._module(
            "factory_github_stub_4627", ROOT / "tools/github-stub/github_stub.py"
        )
        self.worker_token = self.read_worker_token()
        if self.scenario == "ci":
            held = self.await_pending_ci()
            self.replace_worker(held, held_premises=False)
        else:
            held = self.await_hold()
            self.replace_worker(held)
        self.held = held
        self.report["setup"] = "premises established"

    # Observation helpers. None of them prints or records a credential.

    def guard(self) -> None:
        # After a recovery resume the recorded transcript no longer matches the
        # conversation, so the scripted model's refusal is expected and recorded.
        if self.model.unexpected and not self.tolerate_model_errors:
            raise InfrastructureError("the scripted model received an unexpected request")
        if self.stub.unknown_requests:
            raise InfrastructureError("the GitHub stub received an unsupported request")

    def read_worker_token(self) -> str:
        secret = json.loads(
            self.kubectl("-n", self.namespace, "get", "secret", "curie-secrets", "-o", "json")
        )
        token = base64.b64decode(secret["data"]["internalWorkerToken"]).decode()
        self.secrets.append(token)
        return token

    def view(self, request_id: str) -> dict[str, Any]:
        assert self.worker_token is not None
        status, body = fe.http_json(
            "GET",
            f"{self.env['CURIE_API_URL']}/v1/internal/work-items/requests/{request_id}",
            headers={"X-Curie-Worker-Token": self.worker_token},
        )
        if status != 200 or not isinstance(body, dict):
            raise InfrastructureError(f"the internal request view returned HTTP {status}")
        return body

    def snapshot(self) -> dict[str, Any]:
        # UUID parsing keeps fixture text out of the SQL.
        agent = str(uuid.UUID(str(self.evidence["agent_id"])))
        scope = (
            "JOIN curie.work_items w ON w.id = r.work_item_id "
            f"WHERE w.agent_id = '{agent}'::uuid AND w.tracker_issue_id = '{ISSUE_NUMBER}'"
        )
        observed = self.sql(
            "SELECT json_build_object("
            "'requests', (SELECT coalesce(json_agg(json_build_object("
            "'id', r.id, 'status', r.status, 'terminal_cause', r.terminal_cause, "
            "'runtime_owner', r.runtime_owner, 'runtime_epoch', r.runtime_epoch, "
            "'parked', r.execution_deadline IS NOT NULL AND "
            "r.runtime_heartbeat_expires_at = r.execution_deadline, "
            "'claim', r.runtime_claim_name, 'sandbox', r.runtime_sandbox_name, "
            "'execution_deadline', r.execution_deadline, "
            "'deadline_span_seconds', extract(epoch FROM r.execution_deadline - r.started_at), "
            "'deadline_remaining_seconds', extract(epoch FROM r.execution_deadline - now()), "
            "'heartbeat_remaining_seconds', "
            "extract(epoch FROM r.runtime_heartbeat_expires_at - now()), "
            "'teardown_unconfirmed', r.teardown_unconfirmed_at IS NOT NULL, "
            "'terminal_before_deadline', r.terminal_at < r.execution_deadline, "
            "'terminate_published', r.terminate_published_at IS NOT NULL"
            f") ORDER BY r.sequence), '[]'::json) FROM curie.execution_requests r {scope}), "
            "'publications', (SELECT coalesce(json_agg(json_build_object("
            "'id', p.id, 'status', p.status, 'execution_request_id', p.execution_request_id, "
            "'approval_id', a.id, 'approval_status', a.status, 'approval_purpose', a.purpose"
            ") ORDER BY p.created_at), '[]'::json) FROM curie.publications p "
            "JOIN curie.approvals a ON a.id = p.approval_id "
            f"JOIN curie.execution_requests r ON r.id = p.execution_request_id {scope}), "
            # The owned cluster runs one agent on one issue, so the agent's
            # approvals are this WorkItem's approvals.
            "'approvals', (SELECT coalesce(json_agg(json_build_object("
            "'id', a.id, 'status', a.status, 'granted_tool', a.granted_tool, "
            "'gate_kind', a.gate_kind, 'purpose', a.purpose, "
            "'resumed', a.resumed_at IS NOT NULL"
            ") ORDER BY a.created_at), '[]'::json) FROM curie.approvals a "
            f"WHERE a.agent_id = '{agent}'::uuid))"
        )
        if not isinstance(observed, dict):
            raise InfrastructureError("the WorkItem observation was not an object")
        return observed

    def sandbox_names(self) -> tuple[set[str], set[str]]:
        names = []
        for resource in (CLAIM_RESOURCE, SANDBOX_RESOURCE):
            listing = json.loads(self.kubectl("get", resource, "-A", "-o", "json", timeout=60))
            names.append({item["metadata"]["name"] for item in listing.get("items", [])})
        return names[0], names[1]

    def worker_pods(self) -> list[dict[str, Any]]:
        listing = json.loads(
            self.kubectl("-n", self.namespace, "get", "pods", "-l", WORKER_SELECTOR, "-o", "json")
        )
        items: list[dict[str, Any]] = listing.get("items", [])
        return items

    def worker_terminate_log(self) -> list[str]:
        """Bounded, redacted worker log lines about WorkItem termination."""

        try:
            output = self.kubectl(
                "-n",
                self.namespace,
                "logs",
                "-l",
                WORKER_SELECTOR,
                "--tail=400",
                "--prefix",
                timeout=30,
            )
        # Diagnostics never raise, so they cannot replace a verdict or a fault.
        except Exception as exc:  # noqa: BLE001 - diagnostics cannot replace the failure
            return [f"unavailable ({type(exc).__name__})"]
        try:
            lines = [
                line
                for line in output.splitlines()
                if "work-item terminate" in line or "record_termination" in line
            ][-20:]
            safe = []
            for line in lines:
                redacted = fe.record_agent_text(line, self.secrets)[0] or ""
                safe.append(
                    re.sub(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"<>]+", "<url>", redacted)[:400]
                )
            return safe
        except Exception as exc:  # noqa: BLE001 - diagnostics cannot replace the failure
            return [f"unavailable ({type(exc).__name__})"]

    # 1. The held state.

    def await_hold(self) -> Held:
        assert self.install_started is not None
        deadline = self.install_started + HOLD_BOUND_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            self.guard()
            observed = self.snapshot()
            requests = observed["requests"]
            if len(requests) > 1:
                raise InfrastructureError(
                    f"expected one execution request for issue {ISSUE_NUMBER}, saw {len(requests)}"
                )
            if requests:
                request = requests[0]
                last = {key: request[key] for key in ("status", "terminal_cause", "parked")}
                if request["status"] not in {"waiting", "running"}:
                    raise InfrastructureError(
                        f"the run ended before it was held: {request['status']} "
                        f"({request['terminal_cause']})"
                    )
                pending = self.pending_gate_approvals(observed)
                last["approvals"] = observed["approvals"]
                if request["status"] == "running" and request["parked"] and len(pending) == 1:
                    return self.record_hold(request, pending[0])
            time.sleep(5)
        raise InfrastructureError(
            f"no held run with a pending {self.gate_tool} approval within {HOLD_BOUND_SECONDS}s "
            f"of install; last observation {last}"
        )

    def pending_gate_approvals(self, observed: dict[str, Any]) -> list[dict[str, Any]]:
        """Pending tool-gate approvals for the configured gate, never publications."""

        return [
            item
            for item in observed["approvals"]
            if item["status"] == "pending"
            and item["granted_tool"] == self.gate_tool
            and item["purpose"] != "publication"
        ]

    def record_hold(self, request: dict[str, Any], pending: dict[str, Any]) -> Held:
        # The premises are checked here, before any cancellation, so the
        # reversed run cannot fail on them for a different reason.
        if request["deadline_span_seconds"] < MIN_DEADLINE_SPAN_SECONDS:
            raise InfrastructureError(
                f"execution deadline span {request['deadline_span_seconds']:.0f}s is under "
                f"{MIN_DEADLINE_SPAN_SECONDS}s"
            )
        if request["deadline_remaining_seconds"] < MIN_DEADLINE_REMAINING_SECONDS:
            raise InfrastructureError(
                f"only {request['deadline_remaining_seconds']:.0f}s remain before the "
                "execution deadline; the scenario windows would cross it"
            )
        for key in ("runtime_owner", "claim", "sandbox"):
            if not request[key]:
                raise InfrastructureError(f"the held request records no {key}")
        claims, sandboxes = self.sandbox_names()
        if request["claim"] not in claims or request["sandbox"] not in sandboxes:
            raise InfrastructureError(
                "the held request's SandboxClaim or Sandbox is absent before cancellation "
                f"(claim present {request['claim'] in claims}, "
                f"sandbox present {request['sandbox'] in sandboxes})"
            )
        view = self.view(request["id"])
        if view["status"] != "running" or view["runtime_epoch"] != request["runtime_epoch"]:
            raise InfrastructureError("the request view disagrees with the stored hold")
        held = Held(
            request_id=request["id"],
            owner=request["runtime_owner"],
            epoch=request["runtime_epoch"],
            claim=request["claim"],
            sandbox=request["sandbox"],
            execution_deadline=request["execution_deadline"],
            approval_id=pending["id"],
        )
        self.report["hold"] = {
            "request_id": held.request_id,
            "runtime_owner": held.owner,
            "runtime_epoch": held.epoch,
            "claim": held.claim,
            "sandbox": held.sandbox,
            "execution_deadline": held.execution_deadline,
            "deadline_span_seconds": request["deadline_span_seconds"],
            "deadline_remaining_seconds": request["deadline_remaining_seconds"],
            "publication_id": None,
            "approval_id": held.approval_id,
            "approval_granted_tool": pending["granted_tool"],
            "approval_gate_kind": pending["gate_kind"],
            "elapsed_seconds": self.elapsed(),
        }
        return held

    # 2. The owning worker is replaced while the run is held.

    def replace_worker(self, held: Held, *, held_premises: bool = True) -> None:
        """Delete the one worker pod and wait for a Ready replacement.

        ``held_premises`` checks the parked hold and its old owner. The pending
        CI scenario judges the request itself after the replacement instead.
        """

        live = [pod for pod in self.worker_pods() if not pod["metadata"].get("deletionTimestamp")]
        if len(live) != 1:
            raise InfrastructureError(f"expected one worker pod, saw {len(live)}")
        old = live[0]["metadata"]["name"]
        # The consumer name is <hostname>-<pid>, and the hostname is the pod name.
        if held_premises and not held.owner.startswith(f"{old}-"):
            raise InfrastructureError("the held request is not owned by the running worker pod")
        started = time.monotonic()
        self.kubectl(
            "-n", self.namespace, "delete", "pod", old, "--wait=true", "--timeout=240s", timeout=270
        )
        deadline = started + 300
        new = ""
        while time.monotonic() < deadline:
            pods = self.worker_pods()
            ready = [pod["metadata"]["name"] for pod in pods if _ready(pod)]
            if all(pod["metadata"]["name"] != old for pod in pods) and ready and old not in ready:
                new = ready[0]
                break
            time.sleep(3)
        if not new:
            raise InfrastructureError("no replacement worker pod became Ready within 300s")
        self.kubectl(
            "-n",
            self.namespace,
            "rollout",
            "status",
            "deployment/curie-worker",
            "--timeout=120s",
            timeout=150,
        )
        requests = self.snapshot()["requests"]
        if len(requests) != 1 or requests[0]["id"] != held.request_id:
            raise InfrastructureError("the held request changed identity during replacement")
        stored = requests[0]
        owner = str(stored["runtime_owner"] or "")
        if held_premises:
            if owner != held.owner or owner.startswith(f"{new}-"):
                raise InfrastructureError(
                    "the stored runtime owner no longer names the old consumer"
                )
            if stored["status"] != "running" or not stored["parked"]:
                observed: dict[str, Any] = {
                    key: stored.get(key)
                    for key in (
                        "status",
                        "terminal_cause",
                        "parked",
                        "runtime_epoch",
                        "heartbeat_remaining_seconds",
                    )
                }
                observed["publications"] = self.sql(
                    "SELECT coalesce(json_agg(json_build_object("
                    "'status', p.status, 'error', left(p.error, 400), "
                    "'approval', a.status, 'updated_at', p.updated_at)), '[]'::json) "
                    "FROM curie.publications p "
                    "LEFT JOIN curie.approvals a ON a.id = p.approval_id "
                    f"WHERE p.execution_request_id = '{held.request_id}'"
                )
                observed["approvals"] = [
                    {key: item[key] for key in ("status", "granted_tool", "gate_kind", "purpose")}
                    for item in self.snapshot()["approvals"]
                ]
                self.report["replacement_premise"] = observed
                raise InfrastructureError(
                    f"the request stopped being held during replacement: {observed}"
                )
        held.old_pod, held.new_pod = old, new
        self.report["replacement"] = {
            "old_pod": old,
            "new_pod": new,
            "seconds": round(time.monotonic() - started, 1),
            "stored_owner_is_old_consumer": owner.startswith(f"{old}-"),
        }

    # 3a. A human removes the admitting label; the held run must settle.

    def unlabel(self) -> None:
        issue = self.stub._get("issue", ISSUE_NUMBER)
        issue["labels"] = [item for item in issue.get("labels", []) if item.get("name") != LABEL]
        issue["updated_at"] = self.github._utc()
        self.stub._put("issue", ISSUE_NUMBER, issue)
        self.stub._label_event(ISSUE_NUMBER, LABEL, "unlabeled", self.github.HUMAN)

    def cancel_and_require_settlement(self, held: Held) -> None:
        self.deadline = time.monotonic() + 600
        self.unlabel()
        started = time.monotonic()
        deadline = started + SETTLE_BOUND_SECONDS
        last: dict[str, Any] = {}
        while True:
            self.guard()
            view = self.view(held.request_id)
            requests = self.snapshot()["requests"]
            claims, sandboxes = self.sandbox_names()
            stored = next((item for item in requests if item["id"] == held.request_id), None)
            last = {
                "status": view["status"],
                "terminal_cause": view["terminal_cause"],
                "runtime_epoch": view["runtime_epoch"],
                "runtime_owner": view["runtime_owner"],
                "termination_observation": bool(view["termination_observation"]),
                "request_count": len(requests),
                "teardown_unconfirmed": stored["teardown_unconfirmed"] if stored else None,
                "deadline_remaining_seconds": (
                    stored["deadline_remaining_seconds"] if stored else None
                ),
                "claim_present": held.claim in claims,
                "sandbox_present": held.sandbox in sandboxes,
                "seconds_since_unlabel": round(time.monotonic() - started, 1),
            }
            settled = (
                stored is not None
                and len(requests) == 1
                and view["status"] == "cancelled"
                and view["terminal_cause"] == "issue_cancelled"
                and bool(view["termination_observation"])
                and view["runtime_epoch"] > held.epoch
                and str(view["runtime_owner"] or "").startswith(f"{held.new_pod}-")
                and not stored["teardown_unconfirmed"]
                and stored["deadline_remaining_seconds"] > 0
                and not last["claim_present"]
                and not last["sandbox_present"]
            )
            # The bound is strict: the observation that satisfied every condition
            # must itself have completed within the window after the unlabel.
            completed = time.monotonic()
            last["observation_completed_seconds"] = round(completed - started, 1)
            if settled and completed <= deadline:
                self.report["settlement"] = last
                return
            if completed >= deadline:
                break
            time.sleep(3)
        self.report["settlement"] = last
        self.report["worker_terminate_log"] = self.worker_terminate_log()
        pytest.fail(
            f"held run {held.request_id} did not settle through the replacement worker within "
            f"{SETTLE_BOUND_SECONDS}s of the unlabel: status {last['status']}, terminal_cause "
            f"{last['terminal_cause']}, runtime_epoch {last['runtime_epoch']} (held "
            f"{held.epoch}), runtime_owner {last['runtime_owner']}, teardown_unconfirmed "
            f"{last['teardown_unconfirmed']}, SandboxClaim present {last['claim_present']}, "
            f"Sandbox present {last['sandbox_present']}"
        )

    # 3b. Control: an uncancelled hold keeps its authority across replacement.

    def require_hold_survives(self, held: Held) -> None:
        self.deadline = time.monotonic() + SURVIVE_SECONDS + 600
        started = time.monotonic()
        observations = 0
        while time.monotonic() - started < SURVIVE_SECONDS:
            self.guard()
            view = self.view(held.request_id)
            observed = self.snapshot()
            requests = observed["requests"]
            stored = requests[0] if len(requests) == 1 else None
            pending = [
                item
                for item in self.pending_gate_approvals(observed)
                if item["id"] == held.approval_id
            ]
            claims, _sandboxes = self.sandbox_names()
            state = {
                "status": view["status"],
                "terminal_cause": view["terminal_cause"],
                "runtime_epoch": view["runtime_epoch"],
                "runtime_owner": view["runtime_owner"],
                "request_count": len(requests),
                "parked": stored["parked"] if stored else None,
                "approval_pending": len(pending) == 1,
                "claim_present": held.claim in claims,
                "seconds_since_replacement": round(time.monotonic() - started, 1),
            }
            if not (
                stored is not None
                and stored["id"] == held.request_id
                and view["status"] == "running"
                and view["terminal_cause"] is None
                and view["runtime_epoch"] == held.epoch
                and view["runtime_owner"] == held.owner
                and stored["parked"]
                and len(pending) == 1
                and held.claim in claims
            ):
                self.report["survival"] = state
                pytest.fail(
                    f"uncancelled held run {held.request_id} lost its hold after the worker "
                    f"replacement: {state}"
                )
            observations += 1
            time.sleep(5)
        self.report["survival"] = {
            "seconds": round(time.monotonic() - started, 1),
            "observations": observations,
            "status": "running",
            "parked": True,
            "approval_pending": True,
            "claim_present": True,
        }

    # 3c. Control continuation: break-glass recovery resumes the same request.

    def cluster_approvals(self, *flags: str) -> str:
        """``curie cluster approvals factory-e2e`` with deploy()'s connection flags."""

        return self.command(
            self.curie,
            "cluster",
            "--context",
            self.context,
            "approvals",
            "factory-e2e",
            *flags,
            "--namespace",
            self.namespace,
            "--release",
            "curie",
            "--api-url",
            self.env["CURIE_API_URL"],
            timeout=300,
        )

    def mint_operator_principal(self) -> None:
        """Fixture setup: mint the operator principal recovery is attributed to."""

        minted = [
            line.strip()
            for line in self.cluster_approvals(
                "--mint-operator-principal", RECOVERY_SUBJECT
            ).splitlines()
            if line.strip()
        ]
        if not minted or " " in minted[-1]:
            raise InfrastructureError("the operator principal mint returned no token")
        self.approval_principal = minted[-1]
        self.secrets.append(self.approval_principal)
        self.report["operator_principal_minted"] = True

    def recover_pending_approval(self, held: Held) -> None:
        """Reject the stranded Edit approval through break-glass recovery (#2753).

        A GitHub-surfaced approval admits no verifiable approver, so the
        ordinary resolve path cannot answer it. Recovery settles it as rejected
        under the platform key, attributed to the operator principal minted in
        setup, and enqueues the ordinary approval resume turn for the same
        conversation.
        """

        if self.approval_principal is None:
            raise InfrastructureError("no operator principal was minted in setup")
        self.env["CURIE_APPROVAL_PRINCIPAL_TOKEN"] = self.approval_principal
        try:
            self.cluster_approvals(
                "--recover",
                held.approval_id,
                "--reason",
                "test-4627: a GitHub-surfaced approval admits no verifiable approver",
                "--recovery-key",
                f"test-4627-{self.run_id}",
            )
        finally:
            self.env.pop("CURIE_APPROVAL_PRINCIPAL_TOKEN", None)
        self.report["recovery"] = {"approval_id": held.approval_id, "recovered": True}

    def model_requests(self) -> dict[str, int]:
        """Model calls the in-process scripted model has received so far.

        ``served`` counts transcript exchanges answered (``ModelScript._used``)
        and ``refused`` the requests it answered as unexpected. Both are calls a
        runner actually made; only a runner reaches this endpoint.
        """

        served = len(self.model._used)
        refused = len(self.model.unexpected)
        return {"served": served, "refused": refused, "total": served + refused}

    def worker_lines_for(self, pod: str, request_id: str) -> list[str]:
        """Bounded, redacted lines of one worker pod's log naming this request."""

        try:
            output = self.kubectl(
                "-n", self.namespace, "logs", pod, "--tail=1000", "--limit-bytes=262144", timeout=30
            )
        # Diagnostics never raise, so they cannot replace a verdict or a fault.
        except Exception as exc:  # noqa: BLE001 - diagnostics cannot replace the failure
            return [f"unavailable ({type(exc).__name__})"]
        try:
            safe = []
            for line in [line for line in output.splitlines() if request_id in line][-20:]:
                redacted = fe.record_agent_text(line, self.secrets)[0] or ""
                safe.append(
                    re.sub(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"<>]+", "<url>", redacted)[:400]
                )
            return safe
        except Exception as exc:  # noqa: BLE001 - diagnostics cannot replace the failure
            return [f"unavailable ({type(exc).__name__})"]

    def require_recovered_continuation(self, held: Held) -> None:
        self.deadline = time.monotonic() + RECOVERY_BOUND_SECONDS + 600
        # The old pod is gone, so any model call after recovery comes from the
        # resumed runner the replacement worker started for this request.
        before = self.model_requests()
        self.recover_pending_approval(held)
        # The resumed conversation need not match the recorded transcript; the
        # scripted model's refusals are recorded, not failed on.
        self.tolerate_model_errors = True
        started = time.monotonic()
        last: dict[str, Any] = {}
        while True:
            self.guard()
            view = self.view(held.request_id)
            observed = self.snapshot()
            requests = observed["requests"]
            stored = requests[0] if len(requests) == 1 else None
            approval = next(
                (item for item in observed["approvals"] if item["id"] == held.approval_id), None
            )
            after = self.model_requests()
            status = view["status"]
            last = {
                "approval_status": approval["status"] if approval else None,
                "approval_resumed": approval["resumed"] if approval else None,
                "request_count": len(requests),
                "same_request": stored is not None and stored["id"] == held.request_id,
                "status": status,
                "terminal_cause": view["terminal_cause"],
                "runtime_epoch": view["runtime_epoch"],
                "parked": stored["parked"] if stored else None,
                "heartbeat_remaining_seconds": (
                    stored["heartbeat_remaining_seconds"] if stored else None
                ),
                "model_requests_before": before,
                "model_requests_after": after,
                "seconds_since_recovery": round(time.monotonic() - started, 1),
            }
            resumed_runner_called_model = after["total"] > before["total"]
            live_lease = (
                status == "running"
                and stored is not None
                and not stored["parked"]
                and stored["heartbeat_remaining_seconds"] is not None
                and stored["heartbeat_remaining_seconds"] <= LIVE_LEASE_MAX_SECONDS
            )
            # A terminal state reached through the same request also counts.
            # A terminal write leaves the lease untouched, so the parked column
            # proves nothing here; the resumed runner's model call does.
            same_request_terminal = (
                status not in {"waiting", "running", "cancellation_requested"}
                and stored is not None
                and view["terminal_cause"] not in ORPHAN_CAUSES
            )
            continued = (
                approval is not None
                and approval["status"] == "rejected"
                and approval["resumed"]
                and last["same_request"]
                and view["runtime_epoch"] == held.epoch
                and resumed_runner_called_model
                and (live_lease or same_request_terminal)
            )
            completed = time.monotonic()
            if continued and completed - started <= RECOVERY_BOUND_SECONDS:
                last["continuation"] = "live_lease" if live_lease else "terminal"
                break
            if completed - started >= RECOVERY_BOUND_SECONDS:
                break
            time.sleep(2)
        self.report["continuation"] = last
        # Recorded support only: the new pod's log lines naming the request.
        self.report["new_worker_request_log"] = self.worker_lines_for(held.new_pod, held.request_id)
        if "continuation" not in last:
            pytest.fail(
                f"recovered approval {held.approval_id} did not continue request "
                f"{held.request_id} through the replacement worker within "
                f"{RECOVERY_BOUND_SECONDS}s: {last}"
            )

    # 3d. Pending CI: publication ownership survives a worker replacement.

    def await_pending_ci(self) -> Held:
        """The single request with a succeeded publication while CI is pending.

        The stub's replay clock is frozen in process (``_real_start`` unset)
        and every recorded check run starts at replay second 0, so the checks
        stay in progress until the clock is advanced.
        """

        assert self.install_started is not None
        deadline = self.install_started + HOLD_BOUND_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            self.guard()
            observed = self.snapshot()
            requests = observed["requests"]
            if len(requests) > 1:
                raise InfrastructureError(
                    f"expected one execution request for issue {ISSUE_NUMBER}, saw {len(requests)}"
                )
            if requests:
                request = requests[0]
                if request["status"] not in {"waiting", "running"}:
                    raise InfrastructureError(
                        f"the run ended before its publication: {request['status']} "
                        f"({request['terminal_cause']})"
                    )
                published = [
                    item
                    for item in observed["publications"]
                    if item["execution_request_id"] == request["id"]
                    and item["status"] == "succeeded"
                ]
                last = {"status": request["status"], "publications": len(published)}
                if published and len(self.stub._rows("pull")) == 1:
                    return self.record_pending_ci(request, published[0])
            time.sleep(5)
        raise InfrastructureError(
            f"no succeeded publication within {HOLD_BOUND_SECONDS}s of install; "
            f"last observation {last}"
        )

    def record_pending_ci(self, request: dict[str, Any], publication: dict[str, Any]) -> Held:
        checks = self.stub._checks()
        pending = [run for run in checks if run.get("status") != "completed"]
        if not checks or not pending:
            raise InfrastructureError("CI is not pending on the stub before the replacement")
        if request["deadline_remaining_seconds"] < MIN_DEADLINE_REMAINING_SECONDS:
            raise InfrastructureError(
                f"only {request['deadline_remaining_seconds']:.0f}s remain before the "
                "execution deadline; the scenario windows would cross it"
            )
        held = Held(
            request_id=request["id"],
            owner=str(request["runtime_owner"] or ""),
            epoch=request["runtime_epoch"],
            claim=str(request["claim"] or ""),
            sandbox=str(request["sandbox"] or ""),
            execution_deadline=request["execution_deadline"],
            approval_id=publication["approval_id"],
        )
        self.report["pending_ci"] = {
            "request_id": held.request_id,
            "runtime_epoch": held.epoch,
            "runtime_owner": held.owner,
            "parked": request["parked"],
            "execution_deadline": held.execution_deadline,
            "deadline_remaining_seconds": request["deadline_remaining_seconds"],
            "publication_id": publication["id"],
            "pending_check_runs": len(pending),
            "elapsed_seconds": self.elapsed(),
        }
        return held

    def require_ci_settles(self, held: Held) -> None:
        self.deadline = time.monotonic() + SURVIVE_SECONDS + 900
        # 1. Under publication and CI ownership the request stays live.
        started = time.monotonic()
        observations = 0
        while time.monotonic() - started < SURVIVE_SECONDS:
            self.guard()
            view = self.view(held.request_id)
            requests = self.snapshot()["requests"]
            state = {
                "status": view["status"],
                "terminal_cause": view["terminal_cause"],
                "runtime_epoch": view["runtime_epoch"],
                "request_count": len(requests),
                "seconds_since_replacement": round(time.monotonic() - started, 1),
            }
            if not (
                len(requests) == 1
                and requests[0]["id"] == held.request_id
                and view["status"] == "running"
                and view["terminal_cause"] is None
            ):
                self.report["ci_survival"] = state
                pytest.fail(
                    f"request {held.request_id} with pending CI did not stay running after "
                    f"the worker replacement: {state}"
                )
            observations += 1
            time.sleep(5)
        self.report["ci_survival"] = {
            "seconds": round(time.monotonic() - started, 1),
            "observations": observations,
            "status": "running",
        }
        # 2. CI completes green on the replay clock; the same request completes.
        self.stub.advance(1800)
        advanced = time.monotonic()
        last: dict[str, Any] = {}
        while time.monotonic() - advanced < 300:
            self.guard()
            view = self.view(held.request_id)
            requests = self.snapshot()["requests"]
            stored = requests[0] if len(requests) == 1 else None
            last = {
                "status": view["status"],
                "terminal_cause": view["terminal_cause"],
                "runtime_epoch": view["runtime_epoch"],
                "request_count": len(requests),
                "terminal_before_deadline": stored["terminal_before_deadline"] if stored else None,
                "seconds_since_advance": round(time.monotonic() - advanced, 1),
            }
            if view["status"] not in {"running", "completed"} or len(requests) != 1:
                break
            if view["status"] == "completed":
                break
            time.sleep(5)
        self.report["ci_completion"] = last
        if not (
            last.get("status") == "completed"
            and last.get("terminal_cause") == "completed"
            and last.get("request_count") == 1
            and last.get("terminal_before_deadline") is True
        ):
            pytest.fail(
                f"request {held.request_id} did not complete within 300s of CI turning green "
                f"after the worker replacement: {last}"
            )


def _capture_diagnostics(run: HeldRun) -> None:
    """Bounded pod diagnostics before the owned teardown; never masks the failure."""

    if not run.cluster_attempted:
        return
    # Diagnostics never raise: an error here must not replace a behavioral
    # failure or keep an observation fault from becoming the teardown error.
    if "worker_terminate_log" not in run.report:
        try:
            run.report["worker_terminate_log"] = run.worker_terminate_log()
        except Exception as exc:  # noqa: BLE001 - diagnostics cannot replace the failure
            run.report["worker_terminate_log"] = [f"unavailable ({type(exc).__name__})"]
    try:
        run.capture_failure_diagnostics()
    except Exception:  # noqa: BLE001 - diagnostics cannot replace the failure
        pass


def _prepare(run: HeldRun, scenario: str) -> HeldRun:
    """Fixture setup: install, label, reach the premise and replace the worker.

    Everything here runs before the test body, so a failure is a setup error
    and can never stand in for the behavior a test asserts.
    """

    run.scenario = scenario
    # A held scenario parks the run on a production Edit gate; pending CI sets none.
    run.gate_tool = None if scenario == "ci" else "Edit"
    try:
        run.execute()
    except BaseException:
        _capture_diagnostics(run)
        raise
    if run.held is None:
        raise InfrastructureError("the scenario premise was not recorded")
    return run


# Faults of the observation machinery itself: the API view, SQL through
# kubectl, Kubernetes polls, the stub and the CLI. None of them is a verdict
# on the behavior; pytest.fail (an OutcomeException) and AssertionError are
# deliberately absent, so only behavioral assertions can fail a test.
OBSERVATION_FAULTS: tuple[type[BaseException], ...] = (
    InfrastructureError,
    sk.ScriptedScenarioError,
    fe.ConfigError,
    subprocess.SubprocessError,
    OSError,
    http.client.HTTPException,
    # The in-process GitHub stub keeps its state in SQLite.
    sqlite3.Error,
    ValueError,
    KeyError,
    TypeError,
    RuntimeError,
)


def _observe(run: HeldRun, behavior: Callable[[Held], None]) -> bool:
    """Test body: run only the behavior under test.

    An observation fault is stored on the run and the body returns without
    asserting; the held_run teardown then raises it, so pytest reports a
    teardown error and the reversed Fix pin run cannot count it as the pinned
    failure. Behavioral failures propagate as test failures.
    """

    assert run.held is not None
    try:
        behavior(run.held)
    except OBSERVATION_FAULTS as exc:
        # Record the fault before anything else can raise, so teardown
        # always reports it as an error.
        run.infra_error = type(exc).__name__
        run.report["infra_error"] = run.infra_error
        try:
            detail = fe.redact_agent_text(str(exc), run.secrets)[0] or ""
            run.infra_error = f"{type(exc).__name__}: {detail}"[:2000]
            run.report["infra_error"] = run.infra_error
        except Exception:  # noqa: BLE001 - the class name alone still marks the fault
            pass
        _capture_diagnostics(run)
        return False
    except BaseException:
        _capture_diagnostics(run)
        raise
    run.report["verdict"] = "passed"
    return True


@pytest.fixture
def held_run(
    candidate: Candidate, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> HeldRun:
    args = argparse.Namespace(record=False, transcript=str(sk.TRANSCRIPT), build_images=False)
    try:
        sk.validate_configuration(args)
    except fe.ConfigError as exc:
        raise InfrastructureError(str(exc)) from None
    run = HeldRun(args, candidate)

    # 1. Teardown is registered before execute() creates anything. It runs the
    # driver's verified cleanup, then independently confirms each owned
    # resource is gone, and writes the evidence either way.
    def finish() -> None:
        errors: list[str] = []
        try:
            errors.extend(run.cleanup())
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
        env = candidate.env
        try:
            clusters = _command(["kind", "get", "clusters"], env=env, timeout=60).stdout.split()
            if run.cluster in clusters:
                errors.append("owned kind cluster remains")
            for selector in (
                f"label=curie.factory-scripted={run.run_id}",
                f"label=io.x-k8s.kind.cluster={run.cluster}",
                f"name=^/{run.registry}$",
            ):
                remaining = _command(
                    ["docker", "ps", "-aq", "--filter", selector], env=env, timeout=60
                ).stdout.strip()
                if remaining:
                    errors.append(f"owned containers remain for {selector.split('=', 1)[0]}")
        except (InfrastructureError, OSError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
        redacted = [fe.redact_agent_text(error, run.secrets)[0] or "" for error in errors]
        run.report["cleanup_errors"] = redacted
        run.report["cleanup_verified"] = not redacted
        evidence = ROOT / f"target/factory-e2e/c4-held-{candidate.run}.json"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        run.report["infra_error"] = run.infra_error
        evidence.write_text(json.dumps(run.report, indent=2, default=str) + "\n")
        faults = list(redacted)
        if run.infra_error is not None:
            faults.append(f"observation fault in the test body: {run.infra_error}")
        if faults:
            raise InfrastructureError("; ".join(faults))

    request.addfinalizer(finish)

    # 2. ScriptedRun installs the module's IMAGES and scripted_values; both
    # name this test's private images for the duration of the test. Only the
    # recovery control grants break-glass approval recovery on its install.
    monkeypatch.setattr(sk, "IMAGES", tuple(candidate.images[c] for c in COMPONENTS))
    original_values = sk.scripted_values

    def private_values(**kwargs: Any) -> dict[str, Any]:
        values = original_values(**kwargs)
        _private_chart_values(values, candidate)
        if run.scenario == "recover":
            values["api"]["approvalRecovery"] = {"enabled": True}
            run.report["approval_recovery_enabled"] = True
        return values

    monkeypatch.setattr(sk, "scripted_values", private_values)
    return run


@pytest.fixture
def held_for_cancel(held_run: HeldRun) -> HeldRun:
    return _prepare(held_run, "cancel")


@pytest.fixture
def held_for_recovery(held_run: HeldRun) -> HeldRun:
    run = _prepare(held_run, "recover")
    try:
        run.mint_operator_principal()
    except BaseException:
        _capture_diagnostics(run)
        raise
    return run


@pytest.fixture
def pending_ci(held_run: HeldRun) -> HeldRun:
    return _prepare(held_run, "ci")


def test_held_run_cancels_after_worker_replacement(held_for_cancel: HeldRun) -> None:
    """Fix pin: the replacement worker claims, tears down and records the cancel.

    The body is only the unlabel and the settlement assertions; the hold and
    the worker replacement are fixture setup.
    """

    if not _observe(held_for_cancel, held_for_cancel.cancel_and_require_settlement):
        return
    settlement = held_for_cancel.report["settlement"]
    assert settlement["status"] == "cancelled"
    assert settlement["observation_completed_seconds"] <= SETTLE_BOUND_SECONDS
    assert not settlement["claim_present"] and not settlement["sandbox_present"]


def test_uncancelled_held_run_recovers_after_worker_replacement(
    held_for_recovery: HeldRun,
) -> None:
    """Control: an uncancelled hold keeps its authority, then continues the same request.

    The hold first survives more than twice the runtime TTL after the
    replacement. Approval continuation is then driven by break-glass recovery,
    because a GitHub-surfaced approval admits no verifiable approver (ADR-0177
    amendment). A rejection resume is still an approval continuation of the
    same request: the replacement worker adopts it under the same epoch.
    """

    run = held_for_recovery

    def survive_then_continue(held: Held) -> None:
        run.require_hold_survives(held)
        run.require_recovered_continuation(held)

    if not _observe(run, survive_then_continue):
        return
    survival = run.report["survival"]
    assert survival["seconds"] >= SURVIVE_SECONDS
    assert survival["status"] == "running" and survival["parked"]
    assert survival["claim_present"]
    continuation = run.report["continuation"]
    assert continuation["same_request"]
    assert (
        continuation["model_requests_after"]["total"]
        > continuation["model_requests_before"]["total"]
    )


def test_pending_ci_settles_after_worker_replacement(pending_ci: HeldRun) -> None:
    """Control: with auto publication, pending CI survives replacement and then completes."""

    if not _observe(pending_ci, pending_ci.require_ci_settles):
        return
    assert pending_ci.report["publication_policy"] == "auto"
    assert pending_ci.report["ci_survival"]["seconds"] >= SURVIVE_SECONDS
    completion = pending_ci.report["ci_completion"]
    assert completion["status"] == "completed" and completion["request_count"] == 1
