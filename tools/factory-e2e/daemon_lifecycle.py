"""Live acceptance driver for per-run Docker daemons (ADR 0216, ADR 0217).

One ``run`` proves on a disposable cluster that a factory repository opted into
Docker gets a private daemon whose life follows its sandbox claim. The product
path is real end to end: a human label on the GitHub stub, API poll intake, an
execution request, the worker's claim on the repository runner pool (built by
the production catalog builder), the per-claim daemon, Docker from the runner,
then settlement and release. Only the code host (``tools/github-stub``) and the
model (``tools/model-script`` replaying ``fixtures/daemon-lifecycle``) are
fixtures. The scripted turn runs one Bash command that waits for a go file,
runs a nonce-labelled container through ``DOCKER_HOST``, reports ``docker
info`` and then holds until a release file appears or the daemon is gone; the
driver writes those files with ``kubectl exec`` and never patches product code.

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
from typing import Any
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
# The stub's fixed repository id (tools/github-stub/github_stub.py _repository_row).
REPO_ID = "4401"
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
NAMESPACED_RELEASE_KINDS = (
    "deployments,statefulsets,daemonsets,replicasets,jobs,cronjobs,pods,services,"
    "configmaps,secrets,serviceaccounts,roles,rolebindings,networkpolicies,"
    "resourcequotas,limitranges,poddisruptionbudgets"
)
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
# The mention turn (retained_next) opens a second model lane whose first user
# message is only known from a live run. Its exchanges are added to TRANSCRIPT
# from the failure evidence's scrubbed unexpected_model_keys; until then that
# turn fails with unexpected_model_request. None means not yet captured.
MENTION_LANE_KEY: str | None = None

EPILOG = """\
usage of run:
  daemon_lifecycle.py run --context <context> --namespace fd-proof-<nonce>
      --daemon-namespace fd-proof-d-<nonce> --release fd-proof-<nonce>
      --candidate HEAD --work-dir .projects/daemon-proof-<nonce>

ownership:
  The run creates and deletes only what it owns: namespaces fd-proof-<nonce>
  (release, restricted then baseline), fd-proof-d-<nonce> (daemons,
  privileged) and, when no agent-sandbox controller exists, fd-proof-c-<nonce>.
  Every object it creates carries owner=factory-tools and
  factory-tools/nonce=<nonce>. Images are built from HEAD (a git archive) into
  a run-owned registry on the Docker engine loopback: the daemon, the platform
  runner, the API, the worker, and the repository runner made by the
  production catalog builder with the Docker CLI tool.

environment:
  CURIE_LOCAL_STUB_HOST must be an IPv4 address the cluster reaches; the
  GitHub stub and the scripted model bind it. Nothing else is read from it.

bounds and evidence:
  Every fault is observed for at most 300 seconds after injection. Evidence
  is written to <work-dir>/evidence.json (mode 0600) on every outcome. A
  teardown that cannot verify Helm, release objects, namespaces and Docker
  assets are gone fails the run.
"""


class IntakeError(ValueError):
    """The run's arguments or environment are unsafe; nothing was touched."""


# -- loading -----------------------------------------------------------------------


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


def fixture_module() -> ModuleType:
    return _load("docker_daemon_fixture", FIXTURE_MODULE)


# -- intake ------------------------------------------------------------------------


def callback_host(env: dict[str, str] | os._Environ[str]) -> str:
    """The IPv4 address the stub and model bind, from CURIE_LOCAL_STUB_HOST only."""

    value = env.get("CURIE_LOCAL_STUB_HOST", "")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise IntakeError("CURIE_LOCAL_STUB_HOST must be an IPv4 address") from None
    if address.version != 4 or address.is_loopback or address.is_unspecified:
        raise IntakeError("CURIE_LOCAL_STUB_HOST must be a routable IPv4 address")
    return str(address)


def parse_run(argv: list[str], env: dict[str, str] | os._Environ[str]) -> argparse.Namespace:
    """The fixture's intake plus the callback host; no subprocess runs here."""

    fx = fixture_module()
    try:
        args = fx.arguments(argv=argv)
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


# -- judge -------------------------------------------------------------------------


def _number(value: object) -> bool:
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
    if sorted(found) != sorted(roles):
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
    if not (
        seen.get("ran") is True
        and seen.get("id")
        and seen.get("id") == daemon.get("docker_id")
        and pod
        and seen.get("name") == pod
        and daemon.get("docker_name") == pod
        and seen.get("container_nonce") == nonce
        and runner.get("docker_env") is True
    ):
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
    if (
        control["runner"].get("docker_env") is not False
        or control.get("daemon_objects") != 0
        or control.get("client_docker_files") is not False
    ):
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
    """Every reason the envelope fails ADR 0217's lifecycle; [] when it passes.

    ``result`` is ignored: the verdict comes only from the observations.
    """

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


# -- chart values ------------------------------------------------------------------


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
    """scripted_values plus run-built images and one repository on the Docker runner.

    ``docker`` False is the control: the same repository and runner without the
    opt-in. ``base`` is not changed.
    """

    if not release.endswith("-" + nonce):
        raise ValueError("release must belong to this run's nonce")
    values = copy.deepcopy(base)
    # A short fullname keeps the repository runner pool name within 63 characters.
    values["fullnameOverride"] = f"fd-{nonce}"
    for component in ("api", "worker"):
        image = images[component]
        values.setdefault(component, {})["image"] = {
            "repository": image["ref"].split("@", 1)[0],
            "digest": image["digest"],
            "pullPolicy": "IfNotPresent",
        }
    values.setdefault("ui", {})["deploy"] = False
    values["worker"]["claimTimeoutSeconds"] = CLAIM_TIMEOUT_SECONDS
    sandbox = values["agentSandbox"]
    sandbox["controller"] = {"deploy": False}  # the fixture owns or borrows the substrate
    platform, runner = images["platform"], images["runner"]
    sandbox["runner"].pop("tag", None)
    sandbox["runner"].update(
        image=platform["ref"].split("@", 1)[0],
        digest=platform["digest"],
        imagePullPolicy="IfNotPresent",
    )
    other = "arm64" if arch == "amd64" else "amd64"
    repo: dict[str, Any] = {
        "alias": "acme-bot",
        "codeHost": "github",
        "projectId": REPO_ID,
        "path": REPO,
        "runner": {
            "image": runner["ref"],
            "base": platform["digest"],
            # Single-platform images: the other entry only has to be a sha256
            # digest, so it carries the config digest (docker-daemon-config-runtime).
            "platformDigests": {
                "linux/" + arch: runner["manifest"],
                "linux/" + other: runner["config"],
            },
            "tools": ["docker-cli"],
        },
    }
    if docker:
        repo["docker"] = {"enabled": True}
    factory = values["factory"]
    factory["docker"] = {
        "namespace": daemon_namespace,
        "daemonImage": images["daemon"]["ref"],
        "createNamespace": False,
        "port": DAEMON_PORT,
        "resources": {
            "requests": {"cpu": "500m", "memory": "768Mi", "ephemeral-storage": "2Gi"},
            "limits": {"cpu": "1500m", "memory": "1536Mi", "ephemeral-storage": "4Gi"},
        },
        "storageSizeLimit": "3Gi",
        "maxClaimLifetimeSeconds": 900,
        "pingTimeoutSeconds": 5,
    }
    factory["bindings"] = [
        {"name": "acme-bot", "tracker": "github", "trackerScopeId": REPO_ID, "repos": [repo]}
    ]
    return values


# -- small pure helpers ------------------------------------------------------------


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
    return {
        "ran": "ran" in lines,
        "id": parts[0] if len(parts) == 2 else "",
        "name": parts[1] if len(parts) == 2 else "",
        "container_nonce": labelled[0] if labelled == [nonce] else "",
    }


def daemon_probe(payload: dict[str, Any]) -> dict[str, Any]:
    """The daemon's own /info and the cross-claim refusal, over the API port-forward.

    Self-contained (its source also runs under ``uv run`` when the worker
    package is not importable here). Certificate material arrives in
    ``payload`` and never leaves this function.
    """

    from curie_worker.sandbox import KubernetesSandboxClient, tool_daemon

    client = KubernetesSandboxClient(payload["namespace"], kubeconfig=payload["kubeconfig"])
    target = (client, payload["daemon_namespace"], payload["pod"], payload["port"], payload["host"])
    files = payload["files"]
    status, body = tool_daemon.request(*target, files, "/info", 30.0)
    info = json.loads(body) if status == 200 else {}
    out = {
        "id": str(info.get("ID", "")),
        "name": str(info.get("Name", "")),
        "accepted": tool_daemon.ping(*target, files, 10.0),
    }
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


STDERR_TAIL = 1500
MESSAGE_LIMIT = 200
DIAGNOSTIC_LIMIT = 40
_BASE64_RUN = re.compile(r"[A-Za-z0-9+/=_-]{40,}")
_USERINFO_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s/@]+@[^\s\"'<>]*")


def redact(text: str, callback: str) -> str:
    """Drop base64-like runs, URLs carrying userinfo, and the callback address."""

    text = _USERINFO_URL.sub("<url>", text)
    text = _BASE64_RUN.sub("<redacted>", text)
    return text.replace(callback, "<callback>") if callback else text


# -- the live run ------------------------------------------------------------------


class Lifecycle:
    """One run: fixture substrate, stub, model, release, scenarios, teardown."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.fx = fixture_module()
        self.github = _load(
            "daemon_lifecycle_github_stub", ROOT / "tools/github-stub/github_stub.py"
        )
        self.models = _load(
            "daemon_lifecycle_model_script", ROOT / "tools/model-script/model_script.py"
        )
        self.f: Any = None
        self.stub: Any = None
        self.model: Any = None
        self.forward: subprocess.Popen[str] | None = None
        self.nonce, self.ns, self.dns = args.nonce, args.namespace, args.daemon_namespace
        self.release, self.full = args.release, f"fd-{args.nonce}"
        self.images: dict[str, dict[str, str]] = {}
        self.base_values: dict[str, Any] = {}
        self.env = dict(os.environ, BUILDX_BUILDER="default")
        self.curie = ""
        self.install_attempted = False
        self.publication_ns = ""
        self.seen_claims: set[str] = set()
        self.daemon_labels: dict[str, str] = {}
        self.timings: dict[str, float] = {}
        self.evidence: dict[str, Any] = {}
        self.envelope: dict[str, Any] = {
            "schema": SCHEMA,
            "result": "failed",
            "nonce": self.nonce,
            "turns": {},
            "cleanup": dict.fromkeys(CLEANUP_KEYS, False),
        }

    # -- polling and fixtures --

    def check_fixtures(self) -> None:
        if self.model is not None and self.model.unexpected:
            raise RuntimeError("unexpected_model_request")
        if self.stub is not None and self.stub.unknown_requests:
            raise RuntimeError("unsupported_stub_request")

    def bounded[T](
        self, probe: Callable[[], T | None], seconds: float, category: str, *, audit: bool = True
    ) -> T:
        """Poll until ``probe`` is truthy; past ``seconds`` raise ``category``.

        ``audit`` also fails fast on an unexpected model or stub request.
        """

        deadline = time.monotonic() + seconds
        while True:
            if audit:
                self.check_fixtures()
            value = probe()
            if value:
                return value
            if time.monotonic() >= deadline:
                raise RuntimeError(category)
            time.sleep(2)

    def k(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.f.k(*argv, **kwargs)

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

    # -- stub actions --

    def label(self, issue: int) -> None:
        github = self.github
        found = self.stub._get("issue", issue)
        if found is None:
            body = ISSUE_FILE.read_text()
            found = self.stub._issue(issue, f"Docker lifecycle probe {issue}", body, [])
        found["labels"] = [{"name": "factory"}]
        found["updated_at"] = github._utc()
        self.stub._put("issue", issue, found)
        self.stub._label_event(issue, "factory", "labeled", github.HUMAN)

    def unlabel(self, issue: int) -> None:
        github = self.github
        found = self.stub._get("issue", issue)
        self.fx.require(found is not None, "stub_issue_missing")
        found["labels"] = []
        found["updated_at"] = github._utc()
        self.stub._put("issue", issue, found)
        self.stub._label_event(issue, "factory", "unlabeled", github.HUMAN)

    def mention(self, issue: int) -> None:
        github = self.github
        identifier = max((row["id"] for row in self.stub._rows("comment")), default=0) + 1
        now = github._utc()
        self.stub._put(
            "comment",
            identifier,
            {
                "id": identifier,
                "body": f"@{MENTION} please run the Docker lifecycle probe again.",
                "issue_number": issue,
                "user": github.HUMAN,
                "created_at": now,
                "updated_at": now,
                "issue_url": f"{self.stub.base_url}/api/v3/repos/{REPO}/issues/{issue}",
                "html_url": f"{self.stub.base_url}/{REPO}/issues/{issue}#issuecomment-{identifier}",
            },
        )

    # -- cluster observations --

    def claims(self) -> list[dict[str, Any]]:
        return self.f.get(CLAIMS.format(ns=self.ns)).get("items", [])

    def new_claim(self, digest: str, before: set[str]) -> str:
        fresh = [
            c
            for c in self.claims()
            if c["metadata"]["name"] not in before
            and c["metadata"].get("labels", {}).get(THREAD_HASH_LABEL) == digest
            and not c["metadata"].get("deletionTimestamp")
        ]
        fresh.sort(key=lambda c: c["metadata"]["creationTimestamp"])
        return fresh[-1]["metadata"]["name"] if fresh else ""

    def runner_pod(self, claim: str) -> dict[str, Any]:
        for pod in self.items(f"/api/v1/namespaces/{self.ns}/pods", f"{CLAIM_LABEL}={claim}"):
            state = container_status(pod, "runner").get("state", {})
            if pod.get("status", {}).get("phase") == "Running" and "running" in state:
                return pod
        return {}

    def runner_info(self, pod: dict[str, Any], *, every: bool) -> dict[str, Any]:
        container = next(c for c in pod["spec"]["containers"] if c["name"] == "runner")
        names = {entry["name"] for entry in container.get("env") or []}
        present = [name for name in DOCKER_ENV if name in names]
        return {
            "pod": pod["metadata"]["name"],
            "pod_uid": pod["metadata"]["uid"],
            "image_id": image_digest(container_status(pod, "runner")),
            "docker_env": len(present) == len(DOCKER_ENV) if every else bool(present),
        }

    def daemon_objects(self, claim: str) -> list[dict[str, Any]]:
        selector = f"{CLAIM_LABEL}={claim},{MANAGED},{INSTANCE_LABEL}={self.release}"
        found = []
        for kind, prefix, plural, role in DAEMON_KINDS:
            for item in self.items(f"{prefix}/namespaces/{self.dns}/{plural}", selector):
                meta = item["metadata"]
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
        return {
            "kind": kind,
            "role": role,
            "name": meta["name"],
            "namespace": meta["namespace"],
            "uid": meta["uid"],
            "claim_label": meta.get("labels", {}).get(CLAIM_LABEL, ""),
        }

    def client_secret(self, claim: str) -> dict[str, Any]:
        return self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{claim}-tokens")

    def turn_objects(self, claim: str) -> list[dict[str, Any]] | None:
        found = self.daemon_objects(claim)
        secret = self.client_secret(claim)
        if secret:
            found.append(self.record("Secret", "client-secret", secret["metadata"]))
        return found if sorted(o["role"] for o in found) == sorted(ROLES) else None

    def object_path(self, obj: dict[str, Any]) -> str:
        prefix, plural = PATHS[obj["kind"]]
        return f"{prefix}/namespaces/{obj['namespace']}/{plural}/{obj['name']}"

    def remaining(self, objects: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
        """Recorded objects still present by uid, and same-name successors carrying the claim."""

        present, recreated = [], []
        for obj in objects:
            current = self.f.get(self.object_path(obj))
            if not current:
                continue
            meta = current["metadata"]
            if meta["uid"] == obj["uid"]:
                present.append(f"{obj['kind']}/{obj['name']}")
            elif meta.get("labels", {}).get(CLAIM_LABEL) == obj["claim_label"]:
                recreated.append(f"{obj['kind']}/{obj['name']}")
        return present, recreated

    def daemon_ready(self, claim: str) -> dict[str, Any]:
        selector = f"{CLAIM_LABEL}={claim},{MANAGED},{INSTANCE_LABEL}={self.release}"
        for pod in self.items(f"/api/v1/namespaces/{self.dns}/pods", selector):
            if container_status(pod, "dockerd").get("ready"):
                return pod
        return {}

    # -- runner signal files --

    def runner_exec(self, pod: str, script: str) -> subprocess.CompletedProcess[str]:
        return self.k(
            *("-n", self.ns, "exec", pod, "-c", "runner", "--", "sh", "-c", script),
            check=False,
            timeout=60,
        )

    def signal(self, pod: str, name: str, content: str = "") -> None:
        script = (
            f"printf %s '{content}' > /tmp/fd-{name}.tmp && mv /tmp/fd-{name}.tmp /tmp/fd-{name}"
        )
        self.fx.require(self.runner_exec(pod, script).returncode == 0, f"signal_failed:{name}")

    def read_runner(self, pod: str, name: str) -> str:
        result = self.runner_exec(pod, f"cat /tmp/fd-{name}")
        return result.stdout if result.returncode == 0 else ""

    def runner_outputs(self, pod: str) -> tuple[str, str] | None:
        """The turn's /tmp/fd-run and /tmp/fd-info, once both are written."""

        run, info = self.read_runner(pod, "run"), self.read_runner(pod, "info")
        return (run, info) if run and info else None

    # -- the daemon probe --

    @contextlib.contextmanager
    def worker_kubeconfig(self) -> Iterator[Path]:
        """A 0600 kubeconfig for the chart's worker ServiceAccount, deleted after use."""

        path = self.f.work / f"worker-{uuid.uuid4().hex}.kubeconfig"
        view = json.loads(self.k("config", "view", "--minify", "--flatten", "-o", "json").stdout)
        token = self.k(
            "-n", self.ns, "create", "token", f"{self.full}-worker", "--duration=10m"
        ).stdout.strip()
        config = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": "run", "cluster": view["clusters"][0]["cluster"]}],
            "users": [{"name": "worker", "user": {"token": token}}],
            "contexts": [
                {
                    "name": "worker",
                    "context": {"cluster": "run", "user": "worker", "namespace": self.ns},
                }
            ],
            "current-context": "worker",
        }
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
        payload: dict[str, Any] = {
            "namespace": self.ns,
            "daemon_namespace": self.dns,
            "pod": pod,
            "port": DAEMON_PORT,
            "host": f"{claim}.{self.dns}.svc",
            "files": self.client_files(claim),
            "stranger": f"curie-thread-{secrets.token_hex(5)}-{secrets.token_hex(3)}"
            if stranger
            else "",
        }
        with self.worker_kubeconfig() as kubeconfig:
            payload["kubeconfig"] = str(kubeconfig)
            try:
                import curie_worker.sandbox  # noqa: F401 - only proves the package imports
            except ImportError:
                result = self.fx.run(
                    ["uv", "run", "--project", self.f.snapshot, "--frozen", "python3", "-c"]
                    + [PROBE_SCRIPT],
                    data=json.dumps(payload),
                    cwd=self.f.snapshot,
                    timeout=300,
                )
                return dict(json.loads(result.stdout.strip().splitlines()[-1]))
            return daemon_probe(payload)

    # -- one Docker turn --

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
        turn["objects"] = self.bounded(
            lambda: self.turn_objects(claim), 60, "daemon_objects_incomplete"
        )
        daemon = self.bounded(lambda: self.daemon_ready(claim), LIMIT_SECONDS, "daemon_not_ready")
        turn["daemon"] = {
            "pod": daemon["metadata"]["name"],
            "pod_uid": daemon["metadata"]["uid"],
            "image_id": image_digest(container_status(daemon, "dockerd")),
        }
        runner = turn["runner"]["pod"]
        self.signal(runner, "go", self.nonce)
        texts = self.bounded(lambda: self.runner_outputs(runner), LIMIT_SECONDS, "runner_silent")
        turn["runner_docker"] = parse_runner_docker(*texts, self.nonce)
        own = self.probe(claim, turn["daemon"]["pod"], stranger=tls)
        self.fx.require(own["accepted"], "claim_client_refused")
        turn["daemon"].update(docker_id=own["id"], docker_name=own["name"])
        if tls:
            self.envelope["tls"] = {
                "claim": claim,
                "independent_client_refused": own["refused"],
                "claim_client_accepted": own["accepted"],
            }
        return runner

    def settle(self, turn: dict[str, Any], fault: str, started: float) -> None:
        """Time from injection until the request is terminal and every object is gone."""

        turn["fault"] = fault
        state, present, recreated = "", [""], []
        deadline = started + LIMIT_SECONDS
        while True:
            self.check_fixtures()
            state = self.request_status(turn["issue"], turn["request_id"])
            present, recreated = self.remaining(turn["objects"])
            if (state in TERMINAL and not present) or time.monotonic() >= deadline:
                break
            time.sleep(2)
        turn.update(
            settle_seconds=round(time.monotonic() - started, 1),
            objects_absent=not present,
            request_state=state,
        )
        if recreated:
            turn["recreated"] = recreated

    def docker_turn(
        self, issue: int, start: Callable[[], None], *, tls: bool = False
    ) -> tuple[dict[str, Any], str]:
        requests, claims = self.before(issue)
        start()
        turn, pod = self.acquire(issue, requests, claims)
        runner = self.observe(turn, pod, tls=tls)
        return turn, runner

    # -- scenarios --

    def scenario_success(self) -> None:
        issue = ISSUES["success"]
        turn, runner = self.docker_turn(issue, lambda: self.label(issue), tls=True)
        started = time.monotonic()
        self.signal(runner, "release")
        self.settle(turn, "release", started)
        self.envelope["turns"]["success"] = turn

    def scenario_daemon_death(self) -> None:
        issue = ISSUES["daemon_death"]
        turn, _ = self.docker_turn(issue, lambda: self.label(issue))
        started = time.monotonic()
        self.k("-n", self.dns, "delete", "pod", turn["daemon"]["pod"], "--wait=false")
        self.settle(turn, "daemon_pod_deleted", started)
        self.envelope["turns"]["daemon_death"] = turn
        fresh, runner = self.docker_turn(issue, lambda: self.mention(issue))
        started = time.monotonic()
        self.signal(runner, "release")
        self.settle(fresh, "release", started)
        self.envelope["turns"]["retained_next"] = fresh

    def scenario_cancel(self) -> None:
        issue = ISSUES["cancel"]
        turn, _ = self.docker_turn(issue, lambda: self.label(issue))
        started = time.monotonic()
        self.unlabel(issue)
        self.settle(turn, "cancel", started)
        self.envelope["turns"]["cancel"] = turn

    def worker_pods(self) -> list[str]:
        selector = f"{INSTANCE_LABEL}={self.release},app.kubernetes.io/component=worker"
        pods = self.items(f"/api/v1/namespaces/{self.ns}/pods", selector)
        return [pod["metadata"]["name"] for pod in pods]

    def scenario_worker_loss(self) -> None:
        issue = ISSUES["worker_loss"]
        turn, _ = self.docker_turn(issue, lambda: self.label(issue))
        pods = self.worker_pods()
        self.fx.require(pods, "worker_pod_missing")
        started = time.monotonic()
        self.k("-n", self.ns, "delete", "pod", *pods, "--wait=false")
        self.settle(turn, "worker_pod_deleted", started)
        # Any owner-lost successor is cancelled by unlabel and must clear too.
        self.unlabel(issue)
        digest = thread_hash(turn["thread"])

        def cleared() -> bool:
            states = {r["status"] for r in self.requests(issue)}
            claims = [
                c
                for c in self.claims()
                if c["metadata"].get("labels", {}).get(THREAD_HASH_LABEL) == digest
            ]
            prefix = f"curie-thread-{digest}-"
            leftovers = [
                obj
                for _, path_prefix, plural, _ in DAEMON_KINDS
                for obj in self.items(
                    f"{path_prefix}/namespaces/{self.dns}/{plural}", f"{CLAIM_LABEL},{MANAGED}"
                )
                if obj["metadata"]["labels"][CLAIM_LABEL].startswith(prefix)
            ]
            return states <= TERMINAL and not claims and not leftovers

        try:
            turn["successor_cleared"] = self.bounded(cleared, LIMIT_SECONDS, "successor_left")
        except RuntimeError as exc:
            if str(exc) != "successor_left":
                raise
            turn["successor_cleared"] = False
        self.envelope["turns"]["worker_loss"] = turn
        self.rollout("worker")

    def scenario_orphan(self) -> None:
        self.fx.require(self.daemon_labels, "daemon_labels_unobserved")
        claim = f"curie-thread-{secrets.token_hex(5)}-{secrets.token_hex(3)}"
        labels = {**self.daemon_labels, CLAIM_LABEL: claim}
        meta = {"name": claim, "namespace": self.dns, "labels": labels}
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": copy.deepcopy(meta),
            "spec": {
                "clusterIP": "None",
                "selector": {CLAIM_LABEL: claim},
                "ports": [{"name": "docker", "port": DAEMON_PORT, "targetPort": DAEMON_PORT}],
            },
        }
        policy = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": copy.deepcopy(meta),
            "spec": {
                "podSelector": {"matchLabels": {CLAIM_LABEL: claim}},
                "policyTypes": ["Ingress"],
            },
        }
        created = time.monotonic()
        objects = [
            self.record("Service", "service", self.f.create(service)["metadata"]),
            self.record("NetworkPolicy", "ingress", self.f.create(policy)["metadata"]),
        ]
        kept = True
        while time.monotonic() - created < YOUNG_CHECK_SECONDS:
            self.check_fixtures()
            kept = kept and len(self.remaining(objects)[0]) == len(objects)
            time.sleep(5)
        present: list[str] = []
        while time.monotonic() - created < LIMIT_SECONDS:
            present = self.remaining(objects)[0]
            if not present:
                break
            time.sleep(2)
        self.envelope["orphan"] = {
            "claim": claim,
            "age_gate_seconds": CLAIM_TIMEOUT_SECONDS,
            "kept_while_young": kept,
            "objects": objects,
            "settle_seconds": round(time.monotonic() - created, 1),
            "objects_absent": not present,
        }

    def scenario_control(self) -> None:
        control = lifecycle_values(
            self.base_values,
            self.images,
            nonce=self.nonce,
            daemon_namespace=self.dns,
            release=self.release,
            docker=False,
            arch=self.f.arch,
        )
        values = self.f.work / "control.values.json"
        private_file(values, json.dumps({"factory": {"bindings": control["factory"]["bindings"]}}))
        self.helm_upgrade(values)
        issue = CONTROL_ISSUE
        requests, claims = self.before(issue)
        self.label(issue)
        turn, pod = self.acquire(issue, requests, claims)
        secret = self.client_secret(turn["claim"]).get("data", {})
        result = {
            "issue": issue,
            "thread": turn["thread"],
            "claim": turn["claim"],
            "runner": self.runner_info(pod, every=False),
            "daemon_objects": len(self.daemon_objects(turn["claim"])),
            "client_docker_files": any(key in secret for key in CLIENT_FILES),
        }
        runner = result["runner"]["pod"]
        self.signal(runner, "go", self.nonce)
        self.signal(runner, "release")
        try:
            self.bounded(
                lambda: self.request_status(issue, turn["request_id"]) in TERMINAL,
                LIMIT_SECONDS,
                "control_not_terminal",
            )
            result["completed"] = True
        except RuntimeError as exc:
            if str(exc) != "control_not_terminal":
                raise
            result["completed"] = False
        self.envelope["control"] = result

    # -- build, install, deploy --

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
        self.fx.run(
            ["cargo", "build", "--locked", "--release", "--bin", "curie", "--manifest-path"]
            + [str(self.f.snapshot / "cli/Cargo.toml")],
            env=dict(os.environ, CARGO_TARGET_DIR=str(target), TMPDIR=str(self.f.work)),
            timeout=7200,
        )
        self.curie = str(target / "release" / "curie")
        self.envelope["provenance"] = {
            "candidate": self.f.candidate,
            "catalog_builder": CATALOG_BUILDER,
            "images": self.images,
        }
        self.timings["build_seconds"] = round(time.monotonic() - started, 1)

    def start_servers(self) -> None:
        host = self.args.callback_host
        self.stub = self.github.GithubStub(
            self.f.work / "github",
            json.loads(RECORDING.read_text()),
            host=host,
            seed_tree=SEED_TREE,
            issue_file=ISSUE_FILE,
        )
        self.stub.start()
        # The stub seeds issue 3815 labelled; this run uses its own issues only.
        seeded = self.stub._get("issue", 3815)
        seeded["labels"] = []
        self.stub._put("issue", 3815, seeded)
        self.model = self.models.ModelScript(TRANSCRIPT, host=host)
        self.model.start()

    def rollout(self, component: str) -> None:
        self.k(
            *("-n", self.ns, "rollout", "status", f"deployment/{self.full}-{component}"),
            "--timeout=300s",
            timeout=330,
        )

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

    def helm_upgrade(self, values: Path) -> None:
        self.helm(
            "upgrade",
            *(str(self.f.snapshot / "charts/curie"), "-n", self.ns, "--reuse-values"),
            *("-f", str(values), "--timeout", "15m"),
        )
        for component in ("api", "worker"):
            self.rollout(component)

    def secret_inputs(self) -> dict[str, Any]:
        ns, f = self.ns, self.f
        f.create(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "github-stub-ca", "namespace": ns},
                "data": {"ca.pem": self.stub.ca_file.read_text()},
            }
        )
        key_file = f.work / "app.pem"
        self.fx.run(["openssl", "genrsa", "-out", str(key_file), "2048"])
        key_file.chmod(0o600)
        try:
            f.create(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": "factory-app", "namespace": ns},
                    "type": "Opaque",
                    "stringData": {"privateKey": key_file.read_text()},
                }
            )
        finally:
            key_file.unlink(missing_ok=True)
        keys = json.loads(
            self.fx.run(
                ["docker", "run", "--rm", "--label", "owner=factory-tools"]
                + ["--label", f"{self.fx.NONCE}={self.nonce}", self.images["worker"]["ref"]]
                + ["python", "-c", KEY_CODE],
                timeout=300,
            ).stdout
        )
        return {
            "connectorCaller": {"signingKey": keys["signing"], "verifyKey": keys["verify"]},
            "sealing": {"privateKey": keys["sealing"]},
        }

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
        values = lifecycle_values(
            self.base_values,
            self.images,
            nonce=self.nonce,
            daemon_namespace=self.dns,
            release=self.release,
            docker=True,
            arch=self.f.arch,
        )
        values |= self.secret_inputs()
        values_file = self.f.work / "values.json"
        private_file(values_file, json.dumps(values))
        # Chart workloads (Postgres, Valkey, RustFS) need baseline, not restricted.
        self.k(
            "label",
            "namespace",
            self.ns,
            "--overwrite",
            "pod-security.kubernetes.io/enforce=baseline",
        )
        self.f.note("app_namespace_psa", "baseline")
        self.install_attempted = True
        self.helm(
            "install",
            *(str(self.f.snapshot / "charts/curie"), "-n", self.ns, "--skip-crds"),
            *("-f", str(self.f.snapshot / "charts/curie/values-dev.yaml")),
            *("-f", str(values_file), "--timeout", "15m"),
        )
        for component in ("api", "worker"):
            self.rollout(component)
        deployment = self.f.get(
            f"/apis/apps/v1/namespaces/{self.ns}/deployments/{self.full}-worker"
        )
        container = next(
            c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "worker"
        )
        self.publication_ns = next(
            (
                e.get("value", "")
                for e in container["env"]
                if e["name"] == "CURIE_PUBLICATION_NAMESPACE"
            ),
            "",
        )
        self.deploy()
        self.timings["install_seconds"] = round(time.monotonic() - started, 1)

    def port_forward(self) -> str:
        self.forward = subprocess.Popen(
            ["kubectl", "--context", self.args.context, "-n", self.ns, "port-forward"]
            + [f"svc/{self.full}-api", ":8000", "--address", "localhost"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert self.forward.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(self.forward.stdout, selectors.EVENT_READ)
        try:
            self.fx.require(selector.select(timeout=30), "api_forward_not_ready")
            line = self.forward.stdout.readline()
        finally:
            selector.close()
        found = re.search(r"from \S+:(\d+) -> 8000", line)
        self.fx.require(found, "api_forward_not_ready")
        assert found is not None
        return f"http://localhost:{found[1]}"

    def github_token(self) -> str:
        context = ssl.create_default_context(cafile=str(self.stub.ca_file))
        request = Request(
            self.stub.base_url + "/api/v3/app/installations/5501/access_tokens",
            data=json.dumps(
                {"repositories": ["acme-bot"], "permissions": {"issues": "read"}}
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, context=context, timeout=30) as response:
            return str(json.load(response)["token"])

    def deploy(self) -> None:
        secret = self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{self.full}-secrets")
        api_key = base64.b64decode(secret["data"]["apiKey"]).decode()
        api_url = self.port_forward()
        status, _ = fe.http_json(
            "POST",
            api_url + "/agents",
            headers={"X-API-Key": api_key},
            body={
                "name": AGENT,
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        self.fx.require(status == 201, f"agent_create_http_{status}")
        bundle = self.f.work / "bundle"
        shutil.copytree(self.f.snapshot / "examples/dark-factory", bundle)
        env = dict(
            self.env,
            CURIE_API_KEY=api_key,
            CURIE_API_URL=api_url,
            CURIE_CONFIG_DIR=str(self.f.work / "curie-config"),
            GITHUB_PERSONAL_ACCESS_TOKEN=self.github_token(),
        )
        platform = self.images["platform"]["ref"]
        self.fx.run(
            [self.curie, "build", "--plugin-dir", str(bundle), "--registry", self.f.registry]
            + ["--runner-image", platform, "--platform", "linux/" + self.f.arch],
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

    # -- teardown --

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
            if server is None:
                continue
            try:
                server.close()
            except (RuntimeError, OSError) as exc:
                # An audited unexpected request is already this run's failure.
                if not (
                    getattr(server, "unexpected", None) or getattr(server, "unknown_requests", None)
                ):
                    errors.append(f"server_close:{type(exc).__name__}")

    def release_objects(self) -> list[str]:
        selector = f"{INSTANCE_LABEL}={self.release}"
        found: list[str] = []
        for scope in ((self.ns, NAMESPACED_RELEASE_KINDS), (self.dns, NAMESPACED_RELEASE_KINDS)):
            result = self.k(
                "-n", scope[0], "get", scope[1], "-l", selector, "-o", "name", check=False
            )
            self.fx.require(
                result.returncode == 0 or "NotFound" in result.stderr, "release_inventory"
            )
            found += result.stdout.split()
        result = self.k("get", CLUSTER_RELEASE_KINDS, "-l", selector, "-o", "name", check=False)
        self.fx.require(result.returncode == 0, "release_inventory")
        return found + result.stdout.split()

    def uninstall(self, errors: list[str]) -> dict[str, bool]:
        out = {"helm_uninstalled": True, "release_objects_absent": True}
        if not self.install_attempted:
            return out
        result = self.fx.run(
            ["helm", "--kube-context", self.args.context, "uninstall", self.release]
            + ["-n", self.ns, "--wait", "--timeout", "10m"],
            check=False,
            timeout=700,
        )
        if result.returncode and "not found" not in result.stderr.lower():
            out["helm_uninstalled"] = False
            errors.append("helm_uninstall_failed")
        try:
            self.bounded(
                lambda: not self.release_objects(), 180, "release_objects_remain", audit=False
            )
            self.clear_publication_namespace()
        except RuntimeError as exc:
            out["release_objects_absent"] = False
            errors.append(str(exc))
        return out

    def clear_publication_namespace(self) -> None:
        name = self.publication_ns
        if not name or name in (self.ns, self.dns):
            return
        path = f"/api/v1/namespaces/{name}"
        found = self.f.get(path)
        if found:
            labels = found["metadata"].get("labels", {})
            self.fx.require(
                labels.get(INSTANCE_LABEL) == self.release, "publication_namespace_unowned"
            )
            self.k("delete", "namespace", name, "--wait=false")
        self.bounded(
            lambda: not self.f.get(path), 300, "publication_namespace_remains", audit=False
        )

    def registry_tags(self) -> list[str]:
        """Local tags this run's registry name carries that the fixture does not own."""

        if not self.f.registry:
            return []
        owned = {
            row[0] for row in self.f.db.execute("SELECT name FROM assets WHERE kind='tag_intent'")
        }
        tags = self.fx.run(self.fx.TAGS).stdout.split()
        return [t for t in tags if t.startswith(self.f.registry + "/") and t not in owned]

    def teardown(self, errors: list[str]) -> dict[str, bool]:
        cleanup = dict.fromkeys(CLEANUP_KEYS, False)
        self.stop_servers(errors)
        if self.f is None:
            return cleanup
        try:
            cleanup |= self.uninstall(errors)
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            errors.append(
                f"uninstall:{exc}" if isinstance(exc, RuntimeError) else type(exc).__name__
            )
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
        for path in (self.f.work / "cargo-cli", self.f.work / "bundle"):
            shutil.rmtree(path, ignore_errors=True)
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

    # -- the whole run --

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

        def short(text: object) -> str:
            return redact(str(text or ""), self.args.callback_host)[:MESSAGE_LIMIT]

        namespaces = [self.ns, self.dns]
        if self.publication_ns and self.publication_ns not in namespaces:
            namespaces.append(self.publication_ns)
        entries: list[dict[str, Any]] = []
        for ns in namespaces:
            if not self.f.get(f"/api/v1/namespaces/{ns}"):
                continue
            for pod in self.f.get(f"/api/v1/namespaces/{ns}/pods").get("items", []):
                status = pod.get("status", {})
                containers = []
                for item in status.get("initContainerStatuses", []) + status.get(
                    "containerStatuses", []
                ):
                    state = item.get("state", {})
                    detail = state.get("waiting") or state.get("terminated") or {}
                    containers.append(
                        {
                            "name": item.get("name"),
                            "reason": detail.get("reason"),
                            "exitCode": detail.get("exitCode"),
                            "message": short(detail.get("message")),
                        }
                    )
                entries.append(
                    {
                        "namespace": ns,
                        "pod": pod["metadata"]["name"],
                        "phase": status.get("phase"),
                        "containers": containers,
                    }
                )
            for job in self.f.get(f"/apis/batch/v1/namespaces/{ns}/jobs").get("items", []):
                status = job.get("status", {})
                entries.append(
                    {"namespace": ns, "job": job["metadata"]["name"]}
                    | {key: status.get(key, 0) for key in ("active", "failed", "succeeded")}
                )
            warnings = self.f.get(
                f"/api/v1/namespaces/{ns}/events?fieldSelector={quote('type=Warning')}"
            ).get("items", [])
            for event in warnings:
                involved = event.get("involvedObject", {})
                entries.append(
                    {
                        "namespace": ns,
                        "event": involved.get("kind"),
                        "name": involved.get("name"),
                        "reason": event.get("reason"),
                        "message": short(event.get("message")),
                    }
                )
        return entries[:DIAGNOSTIC_LIMIT]

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
        self.envelope["timings"] = self.timings
        reasons = judge(self.envelope)
        failures += teardown_errors
        passed = not failures and not reasons
        self.envelope["result"] = "passed" if passed else "failed"
        self.envelope["failures"] = failures + reasons
        if evidence:
            self.envelope["failure_evidence"] = evidence
        self.write_evidence()
        summary = {
            "result": self.envelope["result"],
            "nonce": self.nonce,
            "failures": self.envelope["failures"],
            "evidence": f"{self.args.work_dir}/evidence.json",
        }
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 0 if passed else 1

    def write_evidence(self) -> None:
        # Only a run whose Fixture created the work directory writes into it.
        if self.f is None:
            return
        path = self.f.work / "evidence.json"
        path.unlink(missing_ok=True)
        private_file(path, json.dumps(self.envelope, indent=2, sort_keys=True) + "\n")


# -- CLI ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="daemon_lifecycle.py",
        description="Live acceptance of per-run Docker daemon lifecycles (ADR 0216, ADR 0217).",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run every scenario once, then tear down and judge")
    for name, example in (
        ("context", "<context>"),
        ("namespace", "fd-proof-<nonce>"),
        ("daemon-namespace", "fd-proof-d-<nonce>"),
        ("release", "fd-proof-<nonce>"),
        ("candidate", "HEAD"),
        ("work-dir", ".projects/daemon-proof-<nonce>"),
    ):
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
