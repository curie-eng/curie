"""Live acceptance driver for per-run Docker daemons (ADR 0216, ADR 0217).

One ``run`` proves on a disposable cluster that a repository opted into Docker
gets a private daemon whose life follows its sandbox claim, through the real
product path; only the GitHub stub and the scripted model are fixtures. The
scripted Bash turn waits for a go file, runs a nonce-labelled container, reports
``docker info`` and holds until a release file appears or the daemon is gone.

Scenarios, each fault observed for at most 300 seconds after injection:

1. success: the runner's daemon is the claim's daemon Pod, an independently
   issued client certificate is refused, and release removes every object.
2. daemon_death: deleting the daemon Pod ends the turn and its objects.
3. retained_next: a mention on the same issue takes its next turn on a new
   claim with a new working daemon.
4. cancel: removing the label cancels the request and retires the daemon.
5. worker_loss: deleting the worker Pod lets owner-lost recovery retire it.
6. orphan: daemon objects for a claim that never existed are kept while
   younger than the claim timeout and swept after it.
7. control: after a Helm upgrade removes the opt-in, a turn has no Docker.

``judge`` reads the evidence envelope offline; ``run`` passes only when the
judge finds nothing and teardown is verified.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import copy
import functools
import hashlib
import importlib.util
import inspect
import ipaddress
import json
import os
import re
import secrets
import selectors
import shutil
import ssl
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any, TypeGuard
from urllib.parse import quote
from urllib.request import Request, urlopen

import factory_e2e as fe
import scripted_kind as sk

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FIXTURES = HERE / "fixtures" / "daemon-lifecycle"
TRANSCRIPT = FIXTURES / "transcript.json"
ISSUE_FILE = FIXTURES / "issue.md"
SEED_TREE = HERE / "fixtures" / "unitconv"
RECORDING = ROOT / "tools/github-stub/recordings/curie-pr-3400.json"
FIXTURE_MODULE = ROOT / "charts/curie/ci/runtime/docker-daemon-fixture.py"

SCHEMA = "daemon-lifecycle/v1"
CATALOG_BUILDER = "curie::repo_runner::catalog_dockerfile"
LIMIT_SECONDS = 300
CLAIM_TIMEOUT_SECONDS = 180
YOUNG_CHECK_SECONDS = 120
DAEMON_PORT = 2376
REPO = "acme-corp/acme-bot"
REPO_ID = "4401"  # the stub's fixed repository id (github_stub.py _repository_row)
AGENT = fe.FACTORY_AGENT
MENTION = "curie-factory-bot"
ISSUES = {"success": 4101, "daemon_death": 4102, "cancel": 4103, "worker_loss": 4104}
CONTROL_ISSUE = 4105
TURN_KEYS = ("success", "daemon_death", "retained_next", "cancel", "worker_loss")
ROLES = ("daemon", "service", "ingress", "server-secret", "client-secret")
IMAGE_NAMES = ("daemon", "platform", "catalog", "runner", "api", "worker")
CLEANUP_KEYS = (
    "helm_uninstalled",
    "release_objects_absent",
    "fixture_cleanup_complete",
    "namespaces_absent",
    "docker_assets_absent",
)
TERMINAL = frozenset({"completed", "failed", "expired", "cancelled"})
DOCKER_ENV = ("DOCKER_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")
CLIENT_FILES = ("ca.pem", "cert.pem", "key.pem")
SHA256 = r"sha256:[0-9a-f]{64}"
CLAIM_LABEL = "curietech.ai/sandbox-claim"
THREAD_HASH_LABEL = "curietech.ai/thread-hash"
MANAGED_BY_LABEL = "curietech.ai/managed-by"
MANAGED = f"{MANAGED_BY_LABEL}=curie-sandbox-substrate"
INSTANCE_LABEL = "app.kubernetes.io/instance"
CLAIMS = "/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{ns}/sandboxclaims"
# (kind, API prefix, plural, role) of the four daemon-namespace objects per claim.
DAEMON_KINDS = (
    ("Pod", "/api/v1", "pods", "daemon"),
    ("Service", "/api/v1", "services", "service"),
    ("NetworkPolicy", "/apis/networking.k8s.io/v1", "networkpolicies", "ingress"),
    ("Secret", "/api/v1", "secrets", "server-secret"),
)
PATHS = {kind: (prefix, plural) for kind, prefix, plural, _ in DAEMON_KINDS}
CLUSTER_RELEASE_KINDS = (
    "clusterroles,clusterrolebindings,priorityclasses,"
    "validatingadmissionpolicies,validatingadmissionpolicybindings"
)
# Sealed connectorCaller and sealing inputs, generated inside the worker image so
# no key material reaches argv or a log (the recipe scripted_kind uses).
KEY_CODE = (
    "import base64,json; from nacl.signing import SigningKey; "
    "from nacl.public import PrivateKey; k=SigningKey.generate(); "
    "print(json.dumps({'signing':base64.b64encode(bytes(k)).decode(),"
    "'verify':base64.b64encode(bytes(k.verify_key)).decode(),"
    "'sealing':base64.b64encode(bytes(PrivateKey.generate())).decode()}))"
)
TRANSIENT = "inventory_unavailable"  # the fixture's get after one failed kubectl read
STDERR_TAIL, MESSAGE_LIMIT, DIAGNOSTIC_LIMIT = 1500, 200, 40
_BASE64_RUN = re.compile(r"[A-Za-z0-9+/=_-]{40,}")
_USERINFO_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s/@]+@[^\s\"'<>]*")

EPILOG = """\
usage: run --context <context> --namespace fd-proof-<nonce> --release fd-proof-<nonce>
  --daemon-namespace fd-proof-d-<nonce> --candidate HEAD
  --work-dir .projects/daemon-proof-<nonce>
The run owns only namespaces fd-proof-<nonce>, fd-proof-d-<nonce> and, without an
existing agent-sandbox controller, fd-proof-c-<nonce>. Its objects carry owner=factory-tools
and factory-tools/nonce=<nonce>. Images are built from HEAD into a run-owned registry,
the repository runner by the production catalog builder. CURIE_LOCAL_STUB_HOST is the
IPv4 the stub and model bind. Each fault is bounded at 300 s; evidence goes to
<work-dir>/evidence.json (0600); an unverified teardown fails the run.
"""


class IntakeError(ValueError):
    """The run's arguments or environment are unsafe; nothing was touched."""


def transient(exc: RuntimeError) -> bool:
    return str(exc) == TRANSIENT


def _load(name: str, path: Path) -> ModuleType:
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"missing_module:{path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def callback_host(env: dict[str, str] | os._Environ[str]) -> str:
    """The IPv4 address the stub and model bind, from CURIE_LOCAL_STUB_HOST only."""
    try:
        address = ipaddress.ip_address(env.get("CURIE_LOCAL_STUB_HOST", ""))
    except ValueError:
        raise IntakeError("CURIE_LOCAL_STUB_HOST must be an IPv4 address") from None
    if address.version != 4 or address.is_loopback or address.is_unspecified:
        raise IntakeError("CURIE_LOCAL_STUB_HOST must be a routable IPv4 address")
    return str(address)


def parse_run(argv: list[str], env: dict[str, str] | os._Environ[str]) -> argparse.Namespace:
    """The fixture's intake plus the callback host; no subprocess runs here."""
    try:
        fixture = _load("docker_daemon_fixture", FIXTURE_MODULE)
        args: argparse.Namespace = fixture.arguments(argv=argv)
    except RuntimeError as exc:
        raise IntakeError(str(exc)) from None
    except SystemExit as exc:
        if exc.code in (0, None):
            raise
        raise IntakeError("invalid_arguments") from None
    if args.slice != "proof":
        raise IntakeError("unsafe_intake")
    args.callback_host = callback_host(env)
    return args


def _number(value: object) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _judge_provenance(envelope: dict[str, Any], reasons: list[str]) -> dict[str, set[str]]:
    provenance = envelope.get("provenance") or {}
    candidate = provenance.get("candidate")
    if not (isinstance(candidate, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", candidate)):
        reasons.append("provenance: candidate is not a full commit id")
    if provenance.get("catalog_builder") != CATALOG_BUILDER:
        reasons.append("provenance: repository runner was not built by the catalog builder")
    images = provenance.get("images") or {}
    allowed: dict[str, set[str]] = {}
    for name in IMAGE_NAMES:
        image = images.get(name) or {}
        digests = [image.get(key) for key in ("digest", "manifest", "config")]
        ref = image.get("ref")
        found = re.fullmatch(rf"\S+@({SHA256})", ref) if isinstance(ref, str) else None
        if not all(isinstance(d, str) and re.fullmatch(SHA256, d) for d in digests):
            reasons.append(f"provenance: image {name} lacks recorded digests")
        elif found is None or found[1] != image.get("digest"):
            reasons.append(f"provenance: image {name} ref is not pinned to its digest")
        allowed[name] = {d for d in digests if isinstance(d, str)}
    return allowed


def _judge_image(where: str, image_id: object, allowed: set[str], reasons: list[str]) -> None:
    if image_id not in allowed:
        reasons.append(f"forged_provenance: {where} image is not a recorded digest")


def _judge_objects(where: str, claim: object, objects: Any, roles: tuple[str, ...]) -> list[str]:
    reasons = []
    found = [obj.get("role") for obj in objects or [] if isinstance(obj, dict)]
    named = [role for role in found if isinstance(role, str)]
    # A missing or non-text role is a missing observation, never a sort error.
    if len(named) != len(found) or sorted(named) != sorted(roles):
        reasons.append(f"missing_observation: {where} objects are not exactly {list(roles)}")
    for obj in objects or []:
        if obj.get("claim_label") != claim:
            reasons.append(f"wrong_claim_label: {where} {obj.get('role')} names another claim")
    return reasons


def _judge_settled(where: str, section: dict[str, Any], reasons: list[str]) -> None:
    settle = section.get("settle_seconds")
    if section.get("objects_absent") is not True or section.get("recreated"):
        reasons.append(f"incomplete_cleanup: {where} objects remain")
    if not _number(settle) or not 0 <= settle <= LIMIT_SECONDS:
        reasons.append(f"incomplete_cleanup: {where} did not settle within {LIMIT_SECONDS}s")


def _judge_turn(
    name: str, turn: dict[str, Any], nonce: object, allowed: dict[str, set[str]]
) -> list[str]:
    reasons: list[str] = []
    runner, daemon, seen = turn["runner"], turn["daemon"], turn["runner_docker"]
    pod = daemon.get("pod")
    ok = seen.get("ran") is True and bool(seen.get("id")) and bool(pod)
    ok = ok and seen.get("id") == daemon.get("docker_id") and seen.get("name") == pod
    ok = ok and daemon.get("docker_name") == pod and seen.get("container_nonce") == nonce
    if not (ok and runner.get("docker_env") is True):
        reasons.append(f"identity_mismatch: {name} runner did not reach its own daemon")
    reasons += _judge_objects(f"turn {name}", turn.get("claim"), turn.get("objects"), ROLES)
    _judge_settled(f"turn {name}", turn, reasons)
    if turn.get("successor_cleared") is False:
        reasons.append(f"incomplete_cleanup: {name} owner-lost successor left objects")
    state = turn.get("request_state")
    if state not in TERMINAL or (name == "cancel" and state != "cancelled"):
        reasons.append(f"incomplete_cleanup: {name} request ended {state!r}")
    _judge_image(f"{name} runner", runner.get("image_id"), allowed["runner"], reasons)
    _judge_image(f"{name} daemon", daemon.get("image_id"), allowed["daemon"], reasons)
    return reasons


def _judge_retained(turns: dict[str, Any]) -> list[str]:
    dead, fresh = turns.get("daemon_death"), turns.get("retained_next")
    if not (isinstance(dead, dict) and isinstance(fresh, dict)):
        return []
    reasons = []
    if fresh["claim"] == dead["claim"] or fresh["daemon"]["pod_uid"] == dead["daemon"]["pod_uid"]:
        reasons.append("dead_daemon_reused: retained_next did not get a fresh claim and daemon")
    if fresh.get("thread") != dead.get("thread") or fresh.get("issue") != dead.get("issue"):
        reasons.append("retained_thread_mismatch: retained_next ran on another thread")
    return reasons


def _judge_tls(envelope: dict[str, Any]) -> list[str]:
    tls = envelope["tls"]
    reasons = []
    if tls.get("claim") != envelope["turns"]["success"]["claim"]:
        reasons.append("missing_observation: tls probed another claim")
    if tls.get("independent_client_refused") is not True:
        reasons.append("cross_claim_tls_accepted: an independently issued client was accepted")
    if tls.get("claim_client_accepted") is False:
        reasons.append("missing_observation: the claim's own client was refused")
    return reasons


def _judge_orphan(envelope: dict[str, Any]) -> list[str]:
    orphan = envelope["orphan"]
    roles = ("service", "ingress")
    reasons = _judge_objects("orphan", orphan.get("claim"), orphan.get("objects"), roles)
    if orphan.get("kept_while_young") is not True:
        reasons.append("age_gate: the orphan was removed before the claim timeout")
    _judge_settled("orphan", orphan, reasons)
    return reasons


def _judge_control(envelope: dict[str, Any], allowed: dict[str, set[str]]) -> list[str]:
    control = envelope["control"]
    reasons = []
    if control.get("completed") is not True:
        reasons.append("missing_observation: the control turn did not complete")
    clean = control["runner"].get("docker_env") is False and control.get("daemon_objects") == 0
    if not (clean and control.get("client_docker_files") is False):
        reasons.append("control_docker: a repository without the opt-in got Docker")
    _judge_image("control runner", control["runner"].get("image_id"), allowed["runner"], reasons)
    return reasons


def _judge_cleanup(envelope: dict[str, Any]) -> list[str]:
    cleanup = envelope["cleanup"]
    return [f"teardown_unverified: {key}" for key in CLEANUP_KEYS if cleanup.get(key) is not True]


def _section(name: str, check: Callable[[], list[str]]) -> list[str]:
    try:
        return check()
    except (KeyError, TypeError, AttributeError):
        return [f"missing_observation: {name}"]


def judge(envelope: dict[str, Any]) -> list[str]:
    """Every reason the envelope fails ADR 0217's lifecycle; ``result`` is ignored."""
    reasons: list[str] = []
    if envelope.get("schema") != SCHEMA:
        reasons.append(f"schema: expected {SCHEMA}")
    allowed = _judge_provenance(envelope, reasons)
    timings = envelope.get("timings") or {}
    if not all(_number(timings.get(key)) for key in ("build_seconds", "install_seconds")):
        reasons.append("missing_observation: build and install timings")
    turns = envelope.get("turns") or {}
    for name in TURN_KEYS:
        turn = turns.get(name)
        if not isinstance(turn, dict):
            reasons.append(f"missing_observation: turn {name}")
            continue
        check = functools.partial(_judge_turn, name, turn, envelope.get("nonce"), allowed)
        reasons += _section(f"turn {name}", check)
    reasons += _section("retained_next", lambda: _judge_retained(turns))
    reasons += _section("tls", lambda: _judge_tls(envelope))
    reasons += _section("orphan", lambda: _judge_orphan(envelope))
    reasons += _section("control", lambda: _judge_control(envelope, allowed))
    reasons += _section("cleanup", lambda: _judge_cleanup(envelope))
    return reasons


def lifecycle_values(
    base: dict[str, Any],
    images: dict[str, dict[str, str]],
    *,
    nonce: str,
    daemon_namespace: str,
    release: str,
    docker: bool,
    arch: str = "amd64",
) -> dict[str, Any]:
    """scripted_values plus run images; ``docker`` False is the control. ``base`` is kept."""
    if not release.endswith("-" + nonce):
        raise ValueError("release must belong to this run's nonce")
    platform, runner = images["platform"], images["runner"]
    values = copy.deepcopy(base)
    # A short fullname keeps the repository runner pool name within 63 characters.
    # Cluster-scoped PriorityClasses have fixed names another release may own
    # (docker-daemon-config-runtime render()); the tenant quota is scoped to them.
    no_class = {"create": False, "name": ""}
    values |= {"fullnameOverride": f"fd-{nonce}", "resourceQuota": {"enabled": False}}
    values["priorityClasses"] = {"platform": dict(no_class), "sandbox": dict(no_class)}
    for component in ("api", "worker"):
        image = images[component]
        pinned = {"repository": image["ref"].split("@", 1)[0], "digest": image["digest"]}
        values.setdefault(component, {})["image"] = pinned | {"pullPolicy": "IfNotPresent"}
    values.setdefault("ui", {})["deploy"] = False
    values["worker"]["claimTimeoutSeconds"] = CLAIM_TIMEOUT_SECONDS
    values["agentSandbox"]["controller"] = {"deploy": False}  # the fixture owns the substrate
    sandbox_runner = values["agentSandbox"]["runner"]
    sandbox_runner.pop("tag", None)
    platform_repo = platform["ref"].split("@", 1)[0]
    sandbox_runner.update(image=platform_repo, digest=platform["digest"])
    sandbox_runner["imagePullPolicy"] = "IfNotPresent"
    # Single-platform images: the other platform entry only has to be a sha256
    # digest, so it carries the config digest (docker-daemon-config-runtime).
    other = "arm64" if arch == "amd64" else "amd64"
    digests = {f"linux/{arch}": runner["manifest"], f"linux/{other}": runner["config"]}
    repo_runner: dict[str, Any] = {"image": runner["ref"], "base": platform["digest"]}
    repo_runner["platformDigests"] = digests
    repo: dict[str, Any] = {"alias": "acme-bot", "codeHost": "github", "projectId": REPO_ID}
    repo["path"] = REPO
    repo["runner"] = repo_runner | {"tools": ["docker-cli"]}
    if docker:
        repo["docker"] = {"enabled": True}
    resources = {
        "requests": {"cpu": "500m", "memory": "768Mi", "ephemeral-storage": "2Gi"},
        "limits": {"cpu": "1500m", "memory": "1536Mi", "ephemeral-storage": "4Gi"},
    }
    daemon: dict[str, Any] = {"namespace": daemon_namespace, "daemonImage": images["daemon"]["ref"]}
    daemon |= {"createNamespace": False, "port": DAEMON_PORT, "resources": resources}
    daemon |= {"storageSizeLimit": "3Gi", "maxClaimLifetimeSeconds": 900, "pingTimeoutSeconds": 5}
    binding = {"name": "acme-bot", "tracker": "github", "trackerScopeId": REPO_ID, "repos": [repo]}
    values["factory"] |= {"docker": daemon, "bindings": [binding]}
    return values


def thread_key(issue: int) -> str:
    return f"github:{quote(REPO, safe='')}:issue-{issue}"


def thread_hash(thread: str) -> str:
    """The claim label the substrate derives from a thread (SandboxConfig.claim_name_for)."""
    return hashlib.sha256(thread.encode()).hexdigest()[:10]


def image_digest(status: dict[str, Any]) -> str:
    found = re.search(rf"({SHA256})$", str(status.get("imageID", "")))
    return found[1] if found else ""


def container_status(pod: dict[str, Any], name: str) -> dict[str, Any]:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return next((s for s in statuses if s.get("name") == name), {})


def parse_runner_docker(run_text: str, info_text: str, nonce: str) -> dict[str, Any]:
    """What the scripted turn wrote: the run, the labelled container, and docker info."""
    lines = [line.strip() for line in run_text.splitlines()]
    labelled = [m[1] for line in lines if (m := re.fullmatch(r"[0-9a-f]{12,64} (\S+)", line))]
    parts = info_text.strip().split()
    pair = parts if len(parts) == 2 else ["", ""]
    container = labelled[0] if labelled == [nonce] else ""
    return {"ran": "ran" in lines, "id": pair[0], "name": pair[1], "container_nonce": container}


def daemon_probe(payload: dict[str, Any]) -> dict[str, Any]:
    """Daemon /info and cross-claim refusal; self-contained, as it also runs under uv."""
    from curie_worker.sandbox import KubernetesSandboxClient, tool_daemon

    client = KubernetesSandboxClient(payload["namespace"], kubeconfig=payload["kubeconfig"])
    target = (client, payload["daemon_namespace"], payload["pod"], payload["port"], payload["host"])
    files = payload["files"]
    status, body = tool_daemon.request(*target, files, "/info", 30.0)
    info = json.loads(body) if status == 200 else {}
    out: dict[str, Any] = {"id": str(info.get("ID", "")), "name": str(info.get("Name", ""))}
    out["accepted"] = tool_daemon.ping(*target, files, 10.0)
    if payload.get("stranger"):
        issued = tool_daemon.issue(payload["stranger"], payload["daemon_namespace"], 600)
        mixed = {"ca.pem": files["ca.pem"]}
        mixed |= {key: issued.client_files[key] for key in ("cert.pem", "key.pem")}
        out["refused"] = not tool_daemon.ping(*target, mixed, 10.0)
    return out


PROBE_SCRIPT = (
    "from __future__ import annotations\nimport json, sys\nfrom typing import Any\n"
    + inspect.getsource(daemon_probe)
    + "\nprint(json.dumps(daemon_probe(json.load(sys.stdin))))\n"
)


def private_file(path: Path, content: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(content)


def redact(text: str, callback: str) -> str:
    """Drop base64-like runs, URLs carrying userinfo, and the callback address."""
    text = _BASE64_RUN.sub("<redacted>", _USERINFO_URL.sub("<url>", text))
    return text.replace(callback, "<callback>") if callback else text


def new_claim_name() -> str:
    """A name shaped like SandboxConfig.claim_name_for's, for no real thread."""
    return f"curie-thread-{secrets.token_hex(5)}-{secrets.token_hex(3)}"


class Lifecycle:
    """One run: fixture substrate, stub, model, release, scenarios, teardown."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.fx = _load("docker_daemon_fixture", FIXTURE_MODULE)
        stub_path = ROOT / "tools/github-stub/github_stub.py"
        self.github = _load("daemon_lifecycle_github_stub", stub_path)
        model_path = ROOT / "tools/model-script/model_script.py"
        self.models = _load("daemon_lifecycle_model_script", model_path)
        self.f: Any = None
        self.stub: Any = None
        self.model: Any = None
        self.forward: subprocess.Popen[str] | None = None
        self.nonce, self.ns, self.dns = args.nonce, args.namespace, args.daemon_namespace
        self.release, self.full = args.release, f"fd-{args.nonce}"
        self.images: dict[str, dict[str, str]] = {}
        self.base_values: dict[str, Any] = {}
        self.env = dict(os.environ, BUILDX_BUILDER="default")
        self.curie = self.publication_ns = ""
        self.install_attempted = False
        self.seen_claims: set[str] = set()
        self.daemon_labels: dict[str, str] = {}
        self.timings: dict[str, float] = {}
        self.evidence: dict[str, Any] = {}
        self.envelope: dict[str, Any] = {"schema": SCHEMA, "result": "failed", "nonce": self.nonce}
        self.envelope |= {"turns": {}, "cleanup": dict.fromkeys(CLEANUP_KEYS, False)}

    def check_fixtures(self) -> None:
        if self.model is not None and self.model.unexpected:
            raise RuntimeError("unexpected_model_request")
        if self.stub is not None and self.stub.unknown_requests:
            raise RuntimeError("unsupported_stub_request")

    def bounded[T](
        self, probe: Callable[[], T | None], seconds: float, category: str, *, audit: bool = True
    ) -> T:
        """Poll ``probe``, retrying transient reads; ``audit`` fails fast on fixtures."""

        deadline = time.monotonic() + seconds
        while True:
            if audit:
                self.check_fixtures()
            value, failing = self.attempt(probe)
            if value:
                return value
            if time.monotonic() >= deadline:
                raise RuntimeError(TRANSIENT if failing else category)
            time.sleep(2)

    @staticmethod
    def attempt[T](probe: Callable[[], T]) -> tuple[T | None, bool]:
        """``probe()``, or (None, True) when it hit a transient read failure."""
        try:
            return probe(), False
        except RuntimeError as exc:
            if not transient(exc):
                raise
            return None, True

    def k(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        result: subprocess.CompletedProcess[str] = self.f.k(*argv, **kwargs)
        return result

    def items(self, path: str, selector: str) -> list[dict[str, Any]]:
        found = self.f.get(f"{path}?labelSelector={quote(selector)}").get("items", [])
        return [item for item in found if not item["metadata"].get("deletionTimestamp")]

    def sql(self, query: str) -> Any:
        output = self.k(
            *("-n", self.ns, "exec", f"statefulset/{self.full}-postgres", "--"),
            *("psql", "-U", "postgres", "-d", "postgres", "-At", "-v", "ON_ERROR_STOP=1"),
            *("-c", "BEGIN READ ONLY; " + query + "; COMMIT;"),
            timeout=30,
        ).stdout
        lines = [line for line in output.splitlines() if line not in {"BEGIN", "COMMIT"}]
        self.fx.require(len(lines) == 1, "sql_observation_shape")
        return json.loads(lines[0])

    def requests(self, issue: int) -> list[dict[str, str]]:
        rows = self.sql(
            "SELECT coalesce(json_agg(json_build_object('id', r.id, 'status', r.status) "
            "ORDER BY r.created_at), '[]'::json) FROM curie.execution_requests r "
            "JOIN curie.work_items w ON w.id = r.work_item_id "
            f"WHERE w.tracker_kind = 'github' AND w.tracker_scope_id = '{REPO_ID}' "
            f"AND w.tracker_issue_id = '{int(issue)}'"
        )
        return [{"id": str(row["id"]), "status": str(row["status"])} for row in rows]

    def request_status(self, issue: int, request_id: str) -> str:
        return next((r["status"] for r in self.requests(issue) if r["id"] == request_id), "")

    def set_label(self, issue: int, on: bool) -> None:
        """A human labels (creating the issue if new) or unlabels a stub issue."""
        found = self.stub._get("issue", issue)
        if found is None and on:
            body = ISSUE_FILE.read_text()
            found = self.stub._issue(issue, f"Docker lifecycle probe {issue}", body, [])
        self.fx.require(found is not None, "stub_issue_missing")
        found["labels"] = [{"name": "factory"}] if on else []
        found["updated_at"] = self.github._utc()
        self.stub._put("issue", issue, found)
        event = "labeled" if on else "unlabeled"
        self.stub._label_event(issue, "factory", event, self.github.HUMAN)

    def mention(self, issue: int) -> None:
        comment_id = max((row["id"] for row in self.stub._rows("comment")), default=0) + 1
        base, now = self.stub.base_url, self.github._utc()
        body = f"@{MENTION} please run the Docker lifecycle probe again."
        comment: dict[str, Any] = {"id": comment_id, "body": body, "issue_number": issue}
        comment["user"] = self.github.HUMAN
        comment |= {"created_at": now, "updated_at": now}
        comment["issue_url"] = f"{base}/api/v3/repos/{REPO}/issues/{issue}"
        comment["html_url"] = f"{base}/{REPO}/issues/{issue}#issuecomment-{comment_id}"
        self.stub._put("comment", comment_id, comment)

    def claims(self, digest: str | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = self.f.get(CLAIMS.format(ns=self.ns)).get("items", [])
        if digest is None:
            return items
        return [
            c for c in items if c["metadata"].get("labels", {}).get(THREAD_HASH_LABEL) == digest
        ]

    def new_claim(self, digest: str, before: set[str]) -> str:
        fresh = [
            c
            for c in self.claims(digest)
            if c["metadata"]["name"] not in before and not c["metadata"].get("deletionTimestamp")
        ]
        fresh.sort(key=lambda c: c["metadata"]["creationTimestamp"])
        return fresh[-1]["metadata"]["name"] if fresh else ""

    def first_pod(
        self, ns: str, selector: str, ready: Callable[[dict[str, Any]], bool]
    ) -> dict[str, Any]:
        return next(
            (p for p in self.items(f"/api/v1/namespaces/{ns}/pods", selector) if ready(p)), {}
        )

    def runner_pod(self, claim: str) -> dict[str, Any]:
        def running(pod: dict[str, Any]) -> bool:
            state = container_status(pod, "runner").get("state", {})
            return pod.get("status", {}).get("phase") == "Running" and "running" in state

        return self.first_pod(self.ns, f"{CLAIM_LABEL}={claim}", running)

    def daemon_ready(self, claim: str) -> dict[str, Any]:
        return self.first_pod(
            self.dns,
            self.claim_selector(claim),
            lambda p: bool(container_status(p, "dockerd").get("ready")),
        )

    def runner_info(self, pod: dict[str, Any], *, every: bool) -> dict[str, Any]:
        container = next(c for c in pod["spec"]["containers"] if c["name"] == "runner")
        names = {entry["name"] for entry in container.get("env") or []}
        present = [name for name in DOCKER_ENV if name in names]
        meta, status = pod["metadata"], container_status(pod, "runner")
        info = {"pod": meta["name"], "pod_uid": meta["uid"], "image_id": image_digest(status)}
        return info | {"docker_env": len(present) == len(DOCKER_ENV) if every else bool(present)}

    def claim_selector(self, claim: str) -> str:
        return f"{CLAIM_LABEL}={claim},{MANAGED},{INSTANCE_LABEL}={self.release}"

    def daemon_items(self, selector: str) -> Iterator[tuple[str, str, dict[str, Any]]]:
        for kind, prefix, plural, role in DAEMON_KINDS:
            for item in self.items(f"{prefix}/namespaces/{self.dns}/{plural}", selector):
                yield kind, role, item["metadata"]

    def daemon_objects(self, claim: str) -> list[dict[str, Any]]:
        found = []
        for kind, role, meta in self.daemon_items(self.claim_selector(claim)):
            found.append(self.record(kind, role, meta))
            if kind == "Service":
                self.daemon_labels = {
                    key: value
                    for key, value in meta.get("labels", {}).items()
                    if key.startswith("app.kubernetes.io/") or key == MANAGED_BY_LABEL
                }
        return found

    @staticmethod
    def record(kind: str, role: str, meta: dict[str, Any]) -> dict[str, Any]:
        found: dict[str, Any] = {"kind": kind, "role": role, "name": meta["name"]}
        found["namespace"] = meta["namespace"]
        claim_label = meta.get("labels", {}).get(CLAIM_LABEL, "")
        return found | {"uid": meta["uid"], "claim_label": claim_label}

    def client_secret(self, claim: str) -> dict[str, Any]:
        secret: dict[str, Any] = self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{claim}-tokens")
        return secret

    def turn_objects(self, claim: str) -> list[dict[str, Any]] | None:
        found = self.daemon_objects(claim)
        secret = self.client_secret(claim)
        if secret:
            found.append(self.record("Secret", "client-secret", secret["metadata"]))
        return found if sorted(o["role"] for o in found) == sorted(ROLES) else None

    def remaining(self, objects: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
        """Recorded objects still present by uid, and same-name successors carrying the claim."""
        present, recreated = [], []
        for obj in objects:
            prefix, plural = PATHS[obj["kind"]]
            path = f"{prefix}/namespaces/{obj['namespace']}/{plural}/{obj['name']}"
            meta = self.f.get(path).get("metadata")
            if not meta:
                continue
            if meta["uid"] == obj["uid"]:
                present.append(f"{obj['kind']}/{obj['name']}")
            elif meta.get("labels", {}).get(CLAIM_LABEL) == obj["claim_label"]:
                recreated.append(f"{obj['kind']}/{obj['name']}")
        return present, recreated

    def runner_exec(self, pod: str, script: str) -> subprocess.CompletedProcess[str]:
        argv = ("-n", self.ns, "exec", pod, "-c", "runner", "--", "sh", "-c", script)
        return self.k(*argv, check=False, timeout=60)

    def signal(self, pod: str, name: str, content: str = "") -> None:
        tmp, final = f"/tmp/fd-{name}.tmp", f"/tmp/fd-{name}"
        script = f"printf %s '{content}' > {tmp} && mv {tmp} {final}"
        self.fx.require(self.runner_exec(pod, script).returncode == 0, f"signal_failed:{name}")

    def read_runner(self, pod: str, name: str) -> str:
        result = self.runner_exec(pod, f"cat /tmp/fd-{name}")
        return result.stdout if result.returncode == 0 else ""

    def runner_outputs(self, pod: str) -> tuple[str, str] | None:
        run, info = self.read_runner(pod, "run"), self.read_runner(pod, "info")
        return (run, info) if run and info else None

    @contextlib.contextmanager
    def worker_kubeconfig(self) -> Iterator[Path]:
        """A 0600 kubeconfig for the chart's worker ServiceAccount, deleted after use."""
        path = self.f.work / f"worker-{uuid.uuid4().hex}.kubeconfig"
        view = json.loads(self.k("config", "view", "--minify", "--flatten", "-o", "json").stdout)
        account = f"{self.full}-worker"
        token = self.k("-n", self.ns, "create", "token", account, "--duration=10m").stdout.strip()
        context = {"cluster": "run", "user": "worker", "namespace": self.ns}
        config: dict[str, Any] = {"apiVersion": "v1", "kind": "Config"}
        config["clusters"] = [{"name": "run", "cluster": view["clusters"][0]["cluster"]}]
        config["users"] = [{"name": "worker", "user": {"token": token}}]
        config["contexts"] = [{"name": "worker", "context": context}]
        config["current-context"] = "worker"
        try:
            private_file(path, json.dumps(config))
            yield path
        finally:
            path.unlink(missing_ok=True)

    def client_files(self, claim: str) -> dict[str, str]:
        data = self.client_secret(claim).get("data", {})
        self.fx.require(all(key in data for key in CLIENT_FILES), "claim_client_files_missing")
        return {key: base64.b64decode(data[key]).decode() for key in CLIENT_FILES}

    def probe(self, claim: str, pod: str, *, stranger: bool) -> dict[str, Any]:
        payload: dict[str, Any] = {"namespace": self.ns, "daemon_namespace": self.dns, "pod": pod}
        payload |= {"port": DAEMON_PORT, "host": f"{claim}.{self.dns}.svc"}
        payload |= {"files": self.client_files(claim)}
        payload["stranger"] = new_claim_name() if stranger else ""
        with self.worker_kubeconfig() as kubeconfig:
            payload["kubeconfig"] = str(kubeconfig)
            try:
                import curie_worker.sandbox  # noqa: F401 - only proves the package imports
            except ImportError:
                snapshot = self.f.snapshot
                argv = ["uv", "run", "--project", snapshot, "--frozen", "python3", "-c"]
                argv.append(PROBE_SCRIPT)
                result = self.fx.run(argv, data=json.dumps(payload), cwd=snapshot, timeout=300)
                return dict(json.loads(result.stdout.strip().splitlines()[-1]))
            return daemon_probe(payload)

    def before(self, issue: int) -> tuple[set[str], set[str]]:
        names = {c["metadata"]["name"] for c in self.claims()}
        return {r["id"] for r in self.requests(issue)}, names | self.seen_claims

    def acquire(
        self, issue: int, requests: set[str], claims: set[str]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        thread = thread_key(issue)
        digest = thread_hash(thread)

        def started() -> tuple[dict[str, str], str, dict[str, Any]] | None:
            request = next((r for r in self.requests(issue) if r["id"] not in requests), None)
            claim = self.new_claim(digest, claims)
            pod = self.runner_pod(claim) if request and claim else {}
            return (request, claim, pod) if request and pod else None

        request, claim, pod = self.bounded(started, LIMIT_SECONDS, "turn_not_started")
        self.seen_claims.add(claim)
        turn = {"issue": issue, "thread": thread, "claim": claim, "request_id": request["id"]}
        return turn, pod

    def observe(self, turn: dict[str, Any], pod: dict[str, Any], *, tls: bool = False) -> str:
        """Record identities and objects before any fault; return the runner Pod name."""
        claim = turn["claim"]
        turn["runner"] = self.runner_info(pod, every=True)
        objects = self.bounded(lambda: self.turn_objects(claim), 60, "daemon_objects_incomplete")
        turn["objects"] = objects
        daemon = self.bounded(lambda: self.daemon_ready(claim), LIMIT_SECONDS, "daemon_not_ready")
        meta, status = daemon["metadata"], container_status(daemon, "dockerd")
        turn["daemon"] = {
            "pod": meta["name"],
            "pod_uid": meta["uid"],
            "image_id": image_digest(status),
        }
        runner = str(turn["runner"]["pod"])
        self.signal(runner, "go", self.nonce)
        texts = self.bounded(lambda: self.runner_outputs(runner), LIMIT_SECONDS, "runner_silent")
        turn["runner_docker"] = parse_runner_docker(*texts, self.nonce)
        own = self.probe(claim, turn["daemon"]["pod"], stranger=tls)
        self.fx.require(own["accepted"], "claim_client_refused")
        turn["daemon"].update(docker_id=own["id"], docker_name=own["name"])
        if tls:
            refused, accepted = own["refused"], own["accepted"]
            tls_seen = {"independent_client_refused": refused, "claim_client_accepted": accepted}
            self.envelope["tls"] = {"claim": claim} | tls_seen
        return runner

    def settle(self, turn: dict[str, Any], fault: str, started: float) -> None:
        """Time from injection until the request is terminal and every object is gone."""
        turn["fault"] = fault
        state, present = "", [""]
        recreated: list[str] = []
        deadline = started + LIMIT_SECONDS
        while True:
            self.check_fixtures()
            try:
                state = self.request_status(turn["issue"], turn["request_id"])
                present, recreated = self.remaining(turn["objects"])
            except RuntimeError as exc:
                if not transient(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(2)
                continue
            if (state in TERMINAL and not present) or time.monotonic() >= deadline:
                break
            time.sleep(2)
        turn["settle_seconds"] = round(time.monotonic() - started, 1)
        turn |= {"objects_absent": not present, "request_state": state}
        if recreated:
            turn["recreated"] = recreated

    def docker_turn(
        self, issue: int, start: Callable[[], None], *, tls: bool = False
    ) -> tuple[dict[str, Any], str]:
        requests, claims = self.before(issue)
        start()
        turn, pod = self.acquire(issue, requests, claims)
        return turn, self.observe(turn, pod, tls=tls)

    def fault(self, key: str, turn: dict[str, Any], fault: str, inject: Callable[[], Any]) -> None:
        """Inject ``fault``, settle the turn from that instant, and record it."""
        started = time.monotonic()
        inject()
        self.settle(turn, fault, started)
        self.envelope["turns"][key] = turn

    def bounded_flag(self, probe: Callable[[], Any], category: str) -> bool:
        """``bounded`` over LIMIT_SECONDS, as False when only ``category`` expired."""
        try:
            return bool(self.bounded(probe, LIMIT_SECONDS, category))
        except RuntimeError as exc:
            if str(exc) != category:
                raise
            return False

    def scenario_success(self) -> None:
        issue = ISSUES["success"]
        turn, runner = self.docker_turn(issue, lambda: self.set_label(issue, True), tls=True)
        self.fault("success", turn, "release", lambda: self.signal(runner, "release"))

    def scenario_daemon_death(self) -> None:
        issue = ISSUES["daemon_death"]
        turn, _ = self.docker_turn(issue, lambda: self.set_label(issue, True))
        pod = turn["daemon"]["pod"]
        delete = ("-n", self.dns, "delete", "pod", pod, "--wait=false")
        self.fault("daemon_death", turn, "daemon_pod_deleted", lambda: self.k(*delete))
        fresh, runner = self.docker_turn(issue, lambda: self.mention(issue))
        self.fault("retained_next", fresh, "release", lambda: self.signal(runner, "release"))

    def scenario_cancel(self) -> None:
        issue = ISSUES["cancel"]
        turn, _ = self.docker_turn(issue, lambda: self.set_label(issue, True))
        self.fault("cancel", turn, "cancel", lambda: self.set_label(issue, False))

    def scenario_worker_loss(self) -> None:
        issue = ISSUES["worker_loss"]
        turn, _ = self.docker_turn(issue, lambda: self.set_label(issue, True))
        selector = f"{INSTANCE_LABEL}={self.release},app.kubernetes.io/component=worker"
        listed = self.items(f"/api/v1/namespaces/{self.ns}/pods", selector)
        pods = [p["metadata"]["name"] for p in listed]
        self.fx.require(pods, "worker_pod_missing")
        # A graceful delete lets the worker drain the held turn for its whole
        # grace period (1860 s); a forced delete models a crash.
        force = ("--grace-period=0", "--force", "--wait=false")
        delete = ("-n", self.ns, "delete", "pod", *pods, *force)
        turn["worker_deletion"] = "forced"
        self.fault("worker_loss", turn, "worker_pod_deleted", lambda: self.k(*delete))
        # Any owner-lost successor is cancelled by unlabel and must clear too.
        self.set_label(issue, False)
        digest = thread_hash(turn["thread"])

        def cleared() -> bool:
            states = {r["status"] for r in self.requests(issue)}
            prefix = f"curie-thread-{digest}-"
            found = self.daemon_items(f"{CLAIM_LABEL},{MANAGED}")
            labels = [meta["labels"][CLAIM_LABEL] for _, _, meta in found]
            leftovers = [label for label in labels if label.startswith(prefix)]
            return states <= TERMINAL and not self.claims(digest) and not leftovers

        turn["successor_cleared"] = self.bounded_flag(cleared, "successor_left")
        self.rollout("worker")

    def scenario_orphan(self) -> None:
        self.fx.require(self.daemon_labels, "daemon_labels_unobserved")
        claim = new_claim_name()
        labels = {**self.daemon_labels, CLAIM_LABEL: claim}
        meta = {"name": claim, "namespace": self.dns, "labels": labels}
        port = {"name": "docker", "port": DAEMON_PORT, "targetPort": DAEMON_PORT}
        service = {"clusterIP": "None", "selector": {CLAIM_LABEL: claim}, "ports": [port]}
        policy = {"podSelector": {"matchLabels": {CLAIM_LABEL: claim}}, "policyTypes": ["Ingress"]}

        def create(kind: str, role: str, api: str, spec: dict[str, Any]) -> dict[str, Any]:
            body = {"apiVersion": api, "kind": kind, "metadata": copy.deepcopy(meta), "spec": spec}
            return self.record(kind, role, self.f.create(body)["metadata"])

        created = time.monotonic()
        objects = [create("Service", "service", "v1", service)]
        objects.append(create("NetworkPolicy", "ingress", "networking.k8s.io/v1", policy))
        kept, observed = True, False
        while time.monotonic() - created < YOUNG_CHECK_SECONDS:
            self.check_fixtures()
            found, failing = self.attempt(lambda: self.remaining(objects)[0])
            if not failing:
                kept, observed = kept and len(found or []) == len(objects), True
            time.sleep(5)
        self.fx.require(observed, TRANSIENT)
        present: list[str] = ["unread"]
        while time.monotonic() - created < LIMIT_SECONDS:
            found, failing = self.attempt(lambda: self.remaining(objects)[0])
            if not failing:
                present = found or []
            if not present:
                break
            time.sleep(2)
        self.fx.require(present != ["unread"], TRANSIENT)
        orphan: dict[str, Any] = {"claim": claim, "age_gate_seconds": CLAIM_TIMEOUT_SECONDS}
        orphan["objects"] = objects
        orphan |= {"kept_while_young": kept, "settle_seconds": round(time.monotonic() - created, 1)}
        self.envelope["orphan"] = orphan | {"objects_absent": not present}

    def scenario_control(self) -> None:
        control = self.values(docker=False)
        values = self.f.work / "control.values.json"
        private_file(values, json.dumps({"factory": {"bindings": control["factory"]["bindings"]}}))
        chart = str(self.f.snapshot / "charts/curie")
        upgrade = (chart, "-n", self.ns, "--reuse-values", "-f", str(values), "--timeout", "15m")
        self.helm("upgrade", *upgrade)
        self.rollout("api", "worker")
        issue = CONTROL_ISSUE
        requests, claims = self.before(issue)
        self.set_label(issue, True)
        turn, pod = self.acquire(issue, requests, claims)
        claim = turn["claim"]
        secret = self.client_secret(claim).get("data", {})
        result: dict[str, Any] = {"issue": issue, "thread": turn["thread"], "claim": claim}
        result["runner"] = self.runner_info(pod, every=False)
        result["daemon_objects"] = len(self.daemon_objects(claim))
        result["client_docker_files"] = any(key in secret for key in CLIENT_FILES)
        runner = result["runner"]["pod"]
        self.signal(runner, "go", self.nonce)
        self.signal(runner, "release")

        def terminal() -> bool:
            return self.request_status(issue, turn["request_id"]) in TERMINAL

        result["completed"] = self.bounded_flag(terminal, "control_not_terminal")
        self.envelope["control"] = result

    def values(self, *, docker: bool) -> dict[str, Any]:
        return lifecycle_values(
            self.base_values,
            self.images,
            nonce=self.nonce,
            daemon_namespace=self.dns,
            release=self.release,
            docker=docker,
            arch=self.f.arch,
        )

    def build_image(self, name: str) -> dict[str, str]:
        """API and worker images, built exactly as Fixture.images() builds its own."""
        fx, f = self.fx, self.f
        tag = f"{f.registry}/fd-{self.nonce}/{name}:candidate"
        fx.require(tag not in fx.run(fx.TAGS).stdout.split(), "build_tag_occupied")
        request = uuid.uuid4().hex
        f.asset("tag_intent", tag, request)
        metadata = f.work / (name + "-build.json")
        fx.run(
            ["docker", "buildx", "build", "--builder", "default", "--platform", "linux/" + f.arch]
            + ["--label", f"{fx.REQUEST}={request}", "--label", f"{fx.NONCE}={self.nonce}"]
            + ["--provenance=false", "--push", "--metadata-file", str(metadata), "-t", tag]
            + ["-f", str(f.snapshot / "apps" / name / "Dockerfile"), str(f.snapshot)],
            timeout=7200,
        )
        top = str(fx.loads(metadata.read_text())["containerimage.digest"])
        fx.require(re.fullmatch(SHA256, top), "invalid_image_digest")
        ref = tag.rsplit(":", 1)[0] + "@" + top
        f.note(name + "_image", ref)
        return dict(f.resolve(name, ref))

    def prepare(self) -> None:
        started = time.monotonic()
        self.f = self.fx.Fixture(self.args)  # arms teardown before the first create
        self.f.setup()
        self.f.note("phase", "build")
        self.images = {name: dict(image) for name, image in self.f.images().items()}
        for name in ("api", "worker"):
            self.images[name] = self.build_image(name)
        self.fx.require(CATALOG_BUILDER in self.fx.CATALOG_EXAMPLE, "catalog_builder_changed")
        target = self.f.work / "cargo-cli"
        manifest = str(self.f.snapshot / "cli/Cargo.toml")
        argv = ["cargo", "build", "--locked", "--release", "--bin", "curie", "--manifest-path"]
        env = dict(os.environ, CARGO_TARGET_DIR=str(target), TMPDIR=str(self.f.work))
        self.fx.run([*argv, manifest], env=env, timeout=7200)
        self.curie = str(target / "release" / "curie")
        provenance = {"candidate": self.f.candidate, "catalog_builder": CATALOG_BUILDER}
        self.envelope["provenance"] = provenance | {"images": self.images}
        self.timings["build_seconds"] = round(time.monotonic() - started, 1)

    def start_servers(self) -> None:
        host = self.args.callback_host
        recording = json.loads(RECORDING.read_text())
        self.stub = self.github.GithubStub(
            self.f.work / "github", recording, host=host, seed_tree=SEED_TREE, issue_file=ISSUE_FILE
        )
        self.stub.start()
        # The stub seeds issue 3815 labelled; this run uses its own issues only.
        seeded = self.stub._get("issue", 3815)
        seeded["labels"] = []
        self.stub._put("issue", 3815, seeded)
        self.model = self.models.ModelScript(TRANSCRIPT, host=host)
        self.model.start()

    def rollout(self, *components: str) -> None:
        for component in components:
            deployment = f"deployment/{self.full}-{component}"
            self.k("-n", self.ns, "rollout", "status", deployment, "--timeout=300s", timeout=330)

    def helm(self, verb: str, *argv: str) -> None:
        """helm install or upgrade; a failure keeps a bounded, redacted stderr tail."""
        result = subprocess.run(
            ["helm", "--kube-context", self.args.context, verb, self.release, *argv],
            text=True,
            capture_output=True,
            timeout=1000,
            check=False,
        )
        if result.returncode:
            tail = redact(result.stderr[-STDERR_TAIL:], self.args.callback_host)
            self.evidence["helm_stderr"] = tail
            raise RuntimeError(f"helm_{verb}_failed")

    def secret_inputs(self) -> dict[str, Any]:
        def create(kind: str, name: str, body: dict[str, Any]) -> None:
            meta = {"name": name, "namespace": self.ns}
            self.f.create({"apiVersion": "v1", "kind": kind, "metadata": meta} | body)

        create("ConfigMap", "github-stub-ca", {"data": {"ca.pem": self.stub.ca_file.read_text()}})
        key_file = self.f.work / "app.pem"
        self.fx.run(["openssl", "genrsa", "-out", str(key_file), "2048"])
        key_file.chmod(0o600)
        try:
            body = {"type": "Opaque", "stringData": {"privateKey": key_file.read_text()}}
            create("Secret", "factory-app", body)
        finally:
            key_file.unlink(missing_ok=True)
        labels = ["--label", "owner=factory-tools", "--label", f"{self.fx.NONCE}={self.nonce}"]
        image = self.images["worker"]["ref"]
        argv = ["docker", "run", "--rm", *labels, image, "python", "-c", KEY_CODE]
        keys = json.loads(self.fx.run(argv, timeout=300).stdout)
        caller = {"signingKey": keys["signing"], "verifyKey": keys["verify"]}
        return {"connectorCaller": caller, "sealing": {"privateKey": keys["sealing"]}}

    def install(self) -> None:
        started = time.monotonic()
        self.start_servers()
        self.base_values = sk.scripted_values(
            model_base_url=self.model.base_url,
            github_api=self.stub.base_url + "/api/v3",
            clone_base=self.stub.base_url,
            ca_configmap="github-stub-ca",
            runner_image=self.images["platform"]["ref"].split("@", 1)[0],
        )
        values = self.values(docker=True) | self.secret_inputs()
        values_file = self.f.work / "values.json"
        private_file(values_file, json.dumps(values))
        # Chart workloads (Postgres, Valkey, RustFS) need baseline, not restricted.
        psa = "pod-security.kubernetes.io/enforce=baseline"
        self.k("label", "namespace", self.ns, "--overwrite", psa)
        self.f.note("app_namespace_psa", "baseline")
        self.install_attempted = True
        self.helm(
            "install",
            *(str(self.f.snapshot / "charts/curie"), "-n", self.ns, "--skip-crds"),
            *("-f", str(self.f.snapshot / "charts/curie/values-dev.yaml")),
            *("-f", str(values_file), "--timeout", "15m"),
        )
        self.rollout("api", "worker")
        path = f"/apis/apps/v1/namespaces/{self.ns}/deployments/{self.full}-worker"
        containers = self.f.get(path)["spec"]["template"]["spec"]["containers"]
        env = next(c for c in containers if c["name"] == "worker")["env"]
        self.publication_ns = next(
            (e.get("value", "") for e in env if e["name"] == "CURIE_PUBLICATION_NAMESPACE"), ""
        )
        self.deploy()
        self.timings["install_seconds"] = round(time.monotonic() - started, 1)

    def port_forward(self) -> str:
        argv = ["kubectl", "--context", self.args.context, "-n", self.ns, "port-forward"]
        argv += [f"svc/{self.full}-api", ":8000", "--address", "localhost"]
        pipe = subprocess.PIPE
        self.forward = subprocess.Popen(argv, stdout=pipe, stderr=subprocess.STDOUT, text=True)
        assert self.forward.stdout is not None
        with selectors.DefaultSelector() as selector:
            selector.register(self.forward.stdout, selectors.EVENT_READ)
            self.fx.require(selector.select(timeout=30), "api_forward_not_ready")
            line = self.forward.stdout.readline()
        found = re.search(r"from \S+:(\d+) -> 8000", line)
        self.fx.require(found, "api_forward_not_ready")
        assert found is not None
        return f"http://localhost:{found[1]}"

    def github_token(self) -> str:
        context = ssl.create_default_context(cafile=str(self.stub.ca_file))
        body = {"repositories": ["acme-bot"], "permissions": {"issues": "read"}}
        request = Request(
            self.stub.base_url + "/api/v3/app/installations/5501/access_tokens",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, context=context, timeout=30) as response:
            return str(json.load(response)["token"])

    def deploy(self) -> None:
        secret = self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{self.full}-secrets")
        api_key = base64.b64decode(secret["data"]["apiKey"]).decode()
        api_url = self.port_forward()
        channel = {"kind": "github", "address": REPO}
        agent = {"name": AGENT, "repo_full_name": REPO, "channel": channel}
        headers = {"X-API-Key": api_key}
        status, _ = fe.http_json("POST", api_url + "/agents", headers=headers, body=agent)
        self.fx.require(status == 201, f"agent_create_http_{status}")
        bundle = self.f.work / "bundle"
        shutil.copytree(self.f.snapshot / "examples/dark-factory", bundle)
        env = dict(self.env, CURIE_API_KEY=api_key, CURIE_API_URL=api_url)
        env["CURIE_CONFIG_DIR"] = str(self.f.work / "curie-config")
        env["GITHUB_PERSONAL_ACCESS_TOKEN"] = self.github_token()
        platform = ["--platform", "linux/" + self.f.arch]
        self.fx.run(
            [self.curie, "build", "--plugin-dir", str(bundle), "--registry", self.f.registry]
            + ["--runner-image", self.images["platform"]["ref"], *platform],
            env=env,
            timeout=1800,
        )
        self.fx.run(
            [self.curie, "cluster", "--context", self.args.context, "deploy"]
            + ["--plugin-dir", str(bundle), "--agent", AGENT, "--env", "prod"]
            + ["--chart", str(self.f.snapshot / "charts/curie")]
            + ["--secret", "GITHUB_PERSONAL_ACCESS_TOKEN", "--namespace", self.ns]
            + ["--release", self.release, "--api-url", api_url],
            env=env,
            timeout=600,
        )

    def stop_servers(self, errors: list[str]) -> None:
        if self.forward is not None:
            self.forward.terminate()
            try:
                self.forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.forward.kill()
                self.forward.wait(timeout=10)
            if self.forward.stdout is not None:
                self.forward.stdout.close()
        for server in (self.model, self.stub):
            try:
                if server is not None:
                    server.close()
            except (RuntimeError, OSError) as exc:
                # An audited unexpected request is already this run's failure.
                audited = getattr(server, "unexpected", None)
                if not (audited or getattr(server, "unknown_requests", None)):
                    errors.append(f"server_close:{type(exc).__name__}")

    def release_objects(self) -> list[str]:
        """Cluster-scoped release objects; namespaced leftovers (hooks, PVCs) go with
        the owned namespaces, which ``namespaces_absent`` checks."""
        selector = f"{INSTANCE_LABEL}={self.release}"
        result = self.k("get", CLUSTER_RELEASE_KINDS, "-l", selector, "-o", "name", check=False)
        self.fx.require(result.returncode == 0, TRANSIENT)
        return result.stdout.split()

    def uninstall(self, errors: list[str]) -> dict[str, bool]:
        out = {"helm_uninstalled": True, "release_objects_absent": True}
        if not self.install_attempted:
            return out
        argv = ["helm", "--kube-context", self.args.context, "uninstall", self.release]
        argv += ["-n", self.ns, "--wait", "--timeout", "10m"]
        result = self.fx.run(argv, check=False, timeout=700)
        if result.returncode and "not found" not in result.stderr.lower():
            out["helm_uninstalled"] = False
            errors.append("helm_uninstall_failed")
        remaining: list[str] = []

        def gone() -> bool:
            remaining[:] = self.release_objects()
            return not remaining

        try:
            self.bounded(gone, 180, "release_objects_remain", audit=False)
            self.clear_publication_namespace()
        except RuntimeError as exc:
            out["release_objects_absent"] = False
            errors.append(str(exc))
            if str(exc) == "publication_namespace_remains":
                remaining.append(f"namespace/{self.publication_ns}")
            self.evidence["release_objects_remaining"] = remaining[:DIAGNOSTIC_LIMIT]
        return out

    def clear_publication_namespace(self) -> None:
        name = self.publication_ns
        if not name or name in (self.ns, self.dns):
            return
        path = f"/api/v1/namespaces/{name}"
        found = self.f.get(path)
        if found:
            owner = found["metadata"].get("labels", {}).get(INSTANCE_LABEL)
            self.fx.require(owner == self.release, "publication_namespace_unowned")
            self.k("delete", "namespace", name, "--wait=false")
        self.bounded(
            lambda: not self.f.get(path), 300, "publication_namespace_remains", audit=False
        )

    def registry_tags(self) -> list[str]:
        """Local tags this run's registry name carries that the fixture does not own."""
        if not self.f.registry:
            return []
        rows = self.f.db.execute("SELECT name FROM assets WHERE kind='tag_intent'")
        owned = {row[0] for row in rows}
        tags = self.fx.run(self.fx.TAGS).stdout.split()
        return [t for t in tags if t.startswith(self.f.registry + "/") and t not in owned]

    def teardown(self, errors: list[str]) -> dict[str, bool]:
        try:
            return self.release_everything(errors)
        finally:
            self.scrub_work(errors)

    def scrub_work(self, errors: list[str]) -> None:
        """Delete work-dir key material on any outcome; evidence, build metadata,
        Dockerfiles and the snapshot (no keys) stay."""
        if self.f is None:
            return
        work = self.f.work
        files = [work / name for name in ("values.json", "control.values.json", "app.pem")]
        files += list(work.glob("worker-*.kubeconfig"))
        try:
            for path in files:
                path.unlink(missing_ok=True)
            for directory in ("github", "curie-config", "cargo-cli", "bundle"):
                shutil.rmtree(work / directory, ignore_errors=True)
            left = [p.name for p in files if p.exists()]
            left += [d for d in ("github", "curie-config") if (work / d).exists()]
            if left:
                errors.append("work_dir_key_material_remains")
        except OSError as exc:
            errors.append(f"work_dir_scrub:{type(exc).__name__}")

    def release_everything(self, errors: list[str]) -> dict[str, bool]:
        cleanup = dict.fromkeys(CLEANUP_KEYS, False)
        self.stop_servers(errors)
        if self.f is None:
            return cleanup
        try:
            cleanup |= self.uninstall(errors)
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            name = f"uninstall:{exc}" if isinstance(exc, RuntimeError) else type(exc).__name__
            errors.append(name)
        extra: list[str] = []
        try:
            for tag in self.registry_tags():
                self.fx.run(["docker", "image", "rm", tag])
            extra = self.registry_tags()
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            errors.append(f"registry_tags:{type(exc).__name__}")
            extra = ["unknown"]
        try:
            self.f.cleanup()
        except RuntimeError as exc:
            errors.append(str(exc))
        complete = self.f.meta().get("cleanup_complete") is True
        cleanup["fixture_cleanup_complete"] = complete
        cleanup["docker_assets_absent"] = complete and not extra
        cleanup["namespaces_absent"] = self.namespaces_absent(errors)
        return cleanup

    def namespaces_absent(self, errors: list[str]) -> bool:
        owned = {self.ns, self.dns, self.args.controller_namespace}
        try:
            listed = self.k("get", "ns", "-o", "name").stdout.split()
        except (RuntimeError, subprocess.SubprocessError) as exc:
            errors.append(f"namespace_inventory:{type(exc).__name__}")
            return False
        remaining = owned & {line.removeprefix("namespace/") for line in listed}
        if remaining:
            errors.append("owned_namespace_remains")
        return not remaining

    def scenarios(self) -> None:
        self.scenario_success()
        self.scenario_daemon_death()
        self.scenario_cancel()
        self.scenario_worker_loss()
        # The sweep runs only while a repository enables Docker, so before control.
        self.scenario_orphan()
        self.scenario_control()

    def diagnostics(self) -> list[dict[str, Any]]:
        """Pod, Job and Warning Event state only: no env, Secret data or logs."""
        namespaces = [self.ns, self.dns]
        if self.publication_ns and self.publication_ns not in namespaces:
            namespaces.append(self.publication_ns)
        entries: list[dict[str, Any]] = []
        for ns in namespaces:
            try:
                if self.f.get(f"/api/v1/namespaces/{ns}"):
                    entries += self.namespace_diagnostics(ns)
            except (RuntimeError, KeyError, ValueError, subprocess.SubprocessError) as exc:
                category = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                entries.append({"namespace": ns, "error": category})
        return entries[:DIAGNOSTIC_LIMIT]

    def namespace_diagnostics(self, ns: str) -> list[dict[str, Any]]:
        def short(text: object) -> str:
            return redact(str(text or ""), self.args.callback_host)[:MESSAGE_LIMIT]

        def listed(path: str) -> list[dict[str, Any]]:
            return list(self.f.get(f"/api/v1/namespaces/{ns}/{path}").get("items", []))

        entries: list[dict[str, Any]] = []
        for pod in listed("pods"):
            status = pod.get("status", {})
            containers = []
            init = status.get("initContainerStatuses", [])
            for item in init + status.get("containerStatuses", []):
                state = item.get("state", {})
                detail = state.get("waiting") or state.get("terminated") or {}
                reason = {"name": item.get("name"), "reason": detail.get("reason")}
                rest = {"exitCode": detail.get("exitCode"), "message": short(detail.get("message"))}
                containers.append(reason | rest)
            name, phase = pod["metadata"]["name"], status.get("phase")
            entries.append({"namespace": ns, "pod": name, "phase": phase, "containers": containers})
        jobs = self.f.get(f"/apis/batch/v1/namespaces/{ns}/jobs").get("items", [])
        for job in jobs:
            status = job.get("status", {})
            counts = {key: status.get(key, 0) for key in ("active", "failed", "succeeded")}
            entries.append({"namespace": ns, "job": job["metadata"]["name"]} | counts)
        for event in listed(f"events?fieldSelector={quote('type=Warning')}"):
            involved = event.get("involvedObject", {})
            entries.append(
                {"namespace": ns, "event": involved.get("kind"), "name": involved.get("name")}
                | {"reason": event.get("reason"), "message": short(event.get("message"))}
            )
        return entries

    def failure_evidence(self, failed: bool) -> dict[str, Any]:
        out: dict[str, Any] = dict(self.evidence)
        if failed and self.f is not None:
            try:
                out["diagnostics"] = self.diagnostics()
            except (RuntimeError, OSError, KeyError, ValueError, subprocess.SubprocessError) as exc:
                out["diagnostics_error"] = type(exc).__name__
            print(json.dumps({"diagnostics": out.get("diagnostics", [])}), file=sys.stderr)
        if self.model is not None and self.model.unexpected:
            host = self.args.callback_host
            out["unexpected_model_keys"] = [
                [str(part).replace(host, "<callback>") for part in item.get("key") or ()]
                for item in self.model.unexpected
            ]
        if self.stub is not None and self.stub.unknown_requests:
            out["unsupported_stub_requests"] = len(self.stub.unknown_requests)
        return out

    def execute(self) -> int:
        failures: list[str] = []
        try:
            self.prepare()
            self.install()
            self.scenarios()
            self.check_fixtures()
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
            failures.append("signal" if code > 128 else "exit")
        except RuntimeError as exc:
            failures.append(str(exc))
        except Exception as exc:  # noqa: BLE001 - every failure is recorded by class, then torn down
            failures.append(type(exc).__name__)
        evidence = self.failure_evidence(bool(failures))
        teardown_errors: list[str] = []
        try:
            self.envelope["cleanup"] = self.teardown(teardown_errors)
        except Exception as exc:  # noqa: BLE001 - teardown failure fails the run
            teardown_errors.append(type(exc).__name__)
        for key, value in self.evidence.items():
            evidence.setdefault(key, value)
        self.envelope["timings"] = self.timings
        reasons = judge(self.envelope)
        failures += teardown_errors
        passed = not failures and not reasons
        self.envelope["result"] = "passed" if passed else "failed"
        self.envelope["failures"] = failures + reasons
        if evidence:
            self.envelope["failure_evidence"] = evidence
        self.write_evidence()
        summary = {"result": self.envelope["result"], "nonce": self.nonce}
        summary |= {"failures": self.envelope["failures"]}
        summary["evidence"] = f"{self.args.work_dir}/evidence.json"
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 0 if passed else 1

    def write_evidence(self) -> None:
        if self.f is None:  # only a run whose Fixture created the work dir writes into it
            return
        path = self.f.work / "evidence.json"
        path.unlink(missing_ok=True)
        private_file(path, json.dumps(self.envelope, indent=2, sort_keys=True) + "\n")


def build_parser() -> argparse.ArgumentParser:
    description = "Live acceptance of per-run Docker daemon lifecycles (ADR 0216, ADR 0217)."
    raw = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        prog="daemon_lifecycle.py", description=description, epilog=EPILOG, formatter_class=raw
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run every scenario once, then tear down and judge")
    flags = "context=<context> namespace=fd-proof-<nonce> daemon-namespace=fd-proof-d-<nonce> "
    flags += "release=fd-proof-<nonce> candidate=HEAD work-dir=.projects/daemon-proof-<nonce>"
    for name, example in (flag.split("=") for flag in flags.split()):
        run.add_argument("--" + name, required=True, metavar=example)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if not argv or argv[0] != "run" or any(a in ("-h", "--help") for a in argv[1:]):
        parser.parse_args(argv)
        return 2
    try:
        args = parse_run(argv[1:], os.environ)
    except IntakeError as exc:
        print(json.dumps({"result": "refused", "error": str(exc)}), file=sys.stderr)
        return 2
    return Lifecycle(args).execute()


if __name__ == "__main__":
    sys.exit(main())
