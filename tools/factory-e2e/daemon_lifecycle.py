"""Live acceptance driver for per-run Docker daemons (ADR 0216, ADR 0217).

A disposable cluster proves, through the real product path with only the GitHub stub
and scripted model faked, that a Docker repository's private daemon follows its claim.
The scripted Bash turn waits for a go file, runs a labelled container, reports
``docker info`` and holds until released or the daemon is gone. Each fault is observed
for at most 300 s:

1. success: the runner reaches its claim's daemon, a foreign client cert is refused,
   and release removes every object.
2. daemon_death: the daemon dies under the turn's held first model call; dropping that
   call makes the worker retry in the same delivery and evict the dead claim.
3. retained_next: that same request runs on a fresh claim with a working daemon.
4. cancel: removing the label cancels the request and retires the daemon.
5. worker_loss: deleting the worker Pod lets owner-lost recovery retire it.
6. orphan: worker-provisioned objects of a claim that never existed are kept while
   younger than the claim timeout and swept after it.
7. control: after a Helm upgrade removes the opt-in, a turn has no Docker.

``run`` passes only when ``judge`` (offline) finds nothing and teardown is verified.
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
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any, TypeGuard, cast
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

import factory_e2e as fe
import scripted_kind as sk

Obj = dict[str, Any]

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
DAEMON_PORT = 2376
REPO = "acme-corp/acme-bot"
REPO_ID = "4401"  # the stub's fixed repository id (github_stub.py _repository_row)
AGENT = fe.FACTORY_AGENT
ISSUES = {"success": 4101, "daemon_death": 4102, "cancel": 4103, "worker_loss": 4104}
CONTROL_ISSUE = 4105
TURN_KEYS = ("success", "daemon_death", "retained_next", "cancel", "worker_loss")
# A delivered turn that publishes nothing settles failed/no_pull_request (completion.py).
DELIVERED, DELIVERY = ("success", "retained_next"), ("failed", "no_pull_request")
IDENTITY = ("kind", "name", "namespace", "uid")
ROLES = ("daemon", "service", "ingress", "server-secret", "client-secret")
IMAGE_NAMES = ("daemon", "platform", "catalog", "runner", "api", "worker")
CLEANUP_KEYS: tuple[str, ...] = ("helm_uninstalled", "release_objects_absent")
CLEANUP_KEYS += ("fixture_cleanup_complete", "namespaces_absent", "docker_assets_absent")
TERMINAL = frozenset({"completed", "failed", "expired", "cancelled"})
DOCKER_ENV = ("DOCKER_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")
EGRESS_LABEL = "curietech.ai/docker-egress"
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
    "clusterroles,clusterrolebindings,priorityclasses,validatingadmissionpolicies,"
)
CLUSTER_RELEASE_KINDS += "validatingadmissionpolicybindings"
# connectorCaller and sealing keys, made inside the worker image so none reach argv or a log.
KEY_CODE = (
    "import base64,json; from nacl.signing import SigningKey; from nacl.public import PrivateKey; "
    "k=SigningKey.generate(); print(json.dumps({'signing':base64.b64encode(bytes(k)).decode(),"
    "'verify':base64.b64encode(bytes(k.verify_key)).decode(),"
    "'sealing':base64.b64encode(bytes(PrivateKey.generate())).decode()}))"
)
TRANSIENT = "inventory_unavailable"  # the fixture's get after one failed kubectl read
STDERR_TAIL, PROBE_TAIL, MESSAGE_LIMIT, DIAGNOSTIC_LIMIT = 1500, 1000, 200, 40
_BASE64_RUN = re.compile(r"[A-Za-z0-9+/=_-]{40,}")
_USERINFO_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s/@]+@[^\s\"'<>]*")
_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]+-----.*?(-----END [A-Z0-9 ]+-----|\Z)", re.S)
_IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")

EPILOG = """\
usage: run --context <context> --namespace fd-proof-<nonce> --release fd-proof-<nonce>
  --daemon-namespace fd-proof-d-<nonce> --candidate HEAD --work-dir .projects/daemon-proof-<nonce>
Owns only fd-proof-<nonce>, fd-proof-d-<nonce> and, without an agent-sandbox controller,
fd-proof-c-<nonce>; objects carry owner=factory-tools and factory-tools/nonce=<nonce>. Images
build from HEAD (the repository runner by the production catalog builder) into a run-owned
registry. CURIE_LOCAL_STUB_HOST is the IPv4 the stub and model bind. Faults are bounded at
300 s; evidence is <work-dir>/evidence.json (0600); an unverified teardown fails the run.
"""


class IntakeError(ValueError):
    """The run's arguments or environment are unsafe; nothing was touched."""


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


def _judge_provenance(envelope: Obj, reasons: list[str]) -> dict[str, set[str]]:
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
        if not all(obj.get(key) for key in IDENTITY):
            reasons.append(f"missing_observation: {where} {obj.get('role')} lacks {IDENTITY}")
        if obj.get("claim_label") != claim:
            reasons.append(f"wrong_claim_label: {where} {obj.get('role')} names another claim")
    return reasons


def _judge_settled(where: str, section: Obj, reasons: list[str]) -> None:
    settle = section.get("settle_seconds")
    if section.get("objects_absent") is not True or section.get("recreated"):
        reasons.append(f"incomplete_cleanup: {where} objects remain")
    if not _number(settle) or not 0 <= settle <= LIMIT_SECONDS:
        reasons.append(f"incomplete_cleanup: {where} did not settle within {LIMIT_SECONDS}s")


def _judge_turn(name: str, turn: Obj, nonce: object, allowed: dict[str, set[str]]) -> list[str]:
    reasons: list[str] = []
    # daemon_death runs no Docker: its model call is dropped before any tool use.
    dead = name == "daemon_death"
    runner, daemon = turn["runner"], turn["daemon"]
    seen, pod = {} if dead else turn["runner_docker"], daemon.get("pod")
    ran = seen.get("ran") is True and bool(seen.get("id")) and seen.get("name") == pod
    ran = ran and seen.get("id") == daemon.get("docker_id") and seen.get("container_nonce") == nonce
    ok = (dead or ran) and bool(pod) and daemon.get("docker_name") == pod
    pods = [o for o in turn.get("objects") or [] if o.get("role") == "daemon"]
    bound = [(o.get("name"), o.get("uid")) for o in pods] == [(pod, daemon.get("pod_uid"))]
    if not (ok and bound and runner.get("docker_env") is True):
        reasons.append(f"identity_mismatch: {name} runner did not reach its own daemon")
    reasons += _judge_objects(f"turn {name}", turn.get("claim"), turn.get("objects"), ROLES)
    _judge_settled(f"turn {name}", turn, reasons)
    if turn.get("leaked") != []:
        reasons.append(f"incomplete_cleanup: {name} left claim objects {turn.get('leaked')}")
    if name in DELIVERED and turn.get("turn_delivered") is not True:
        reasons.append(f"missing_observation: {name} turn was not delivered")
    recovery, failures = turn.get("recovery_seconds"), turn.get("model_transport_failures")
    recovered = _number(recovery) and 0 <= recovery <= LIMIT_SECONDS
    if name == "worker_loss" and not (recovered and turn.get("successor_cleared") is True):
        reasons.append(f"incomplete_cleanup: {name} did not recover within {LIMIT_SECONDS}s")
    dropped = turn.get("fault") == "daemon_pod_deleted" and _number(failures) and failures == 1
    if dead and not dropped:
        reasons.append(f"missing_observation: {name} saw no single dropped model call")
    state = turn.get("request_state")
    if state not in TERMINAL and not dead:
        reasons.append(f"incomplete_cleanup: {name} request ended {state!r}")
    if name == "cancel" and state != "cancelled":
        reasons.append(f"missing_observation: {name} request ended {state!r}, not cancelled")
    _judge_image(f"{name} runner", runner.get("image_id"), allowed["runner"], reasons)
    _judge_image(f"{name} daemon", daemon.get("image_id"), allowed["daemon"], reasons)
    return reasons


def _judge_retained(turns: Obj) -> list[str]:
    dead, fresh = turns.get("daemon_death"), turns.get("retained_next")
    if not (isinstance(dead, dict) and isinstance(fresh, dict)):
        return []
    reasons = []
    if fresh["claim"] == dead["claim"] or fresh["daemon"]["pod_uid"] == dead["daemon"]["pod_uid"]:
        reasons.append("dead_daemon_reused: retained_next did not get a fresh claim and daemon")
    if fresh.get("thread") != dead.get("thread") or fresh.get("issue") != dead.get("issue"):
        reasons.append("retained_thread_mismatch: retained_next ran on another thread")
    if not fresh.get("request_id") or fresh.get("request_id") != dead.get("request_id"):
        reasons.append("retained_thread_mismatch: retained_next served another request")
    return reasons


def _judge_tls(envelope: Obj) -> list[str]:
    tls = envelope["tls"]
    reasons = []
    if tls.get("claim") != envelope["turns"]["success"]["claim"]:
        reasons.append("missing_observation: tls probed another claim")
    if tls.get("independent_client_refused") is not True:
        reasons.append("cross_claim_tls_accepted: an independently issued client was accepted")
    controls = ("claim_client_accepted", "claim_client_accepted_after")
    if not all(tls.get(key) is True for key in controls):
        reasons.append("tls_control_failed: the claim's own client was not accepted around it")
    return reasons


def _judge_orphan(envelope: Obj) -> list[str]:
    orphan = envelope["orphan"]
    reasons = _judge_objects("orphan", orphan.get("claim"), orphan.get("objects"), ROLES[:4])
    if orphan.get("provisioned_by") != "worker":
        reasons.append("missing_observation: the orphan was not provisioned by the worker")
    if orphan.get("leaked") != []:
        reasons.append(f"incomplete_cleanup: orphan left claim objects {orphan.get('leaked')}")
    removed, gate = orphan.get("removed_after_seconds"), orphan.get("age_gate_seconds")
    young = not (_number(removed) and _number(gate) and removed >= gate)
    if orphan.get("kept_while_young") is not True or young:
        reasons.append("age_gate: the orphan was removed before the claim timeout")
    _judge_settled("orphan", orphan, reasons)
    return reasons


def _judge_control(envelope: Obj, allowed: dict[str, set[str]]) -> list[str]:
    control = envelope["control"]
    reasons = []
    if control.get("completed") is not True:
        reasons.append("missing_observation: the control turn did not complete")
    if control.get("turn_delivered") is not True:
        reasons.append("missing_observation: the control turn was not delivered")
    clean = control["runner"].get("docker_env") is False and control.get("daemon_objects") == 0
    if not (clean and control.get("client_docker_files") is False):
        reasons.append("control_docker: a repository without the opt-in got Docker")
    _judge_image("control runner", control["runner"].get("image_id"), allowed["runner"], reasons)
    return reasons


def _judge_cleanup(envelope: Obj) -> list[str]:
    cleanup = envelope["cleanup"]
    return [f"teardown_unverified: {key}" for key in CLEANUP_KEYS if cleanup.get(key) is not True]


def _section(name: str, check: Callable[[], list[str]]) -> list[str]:
    try:
        return check()
    except (KeyError, TypeError, AttributeError):
        return [f"missing_observation: {name}"]


def judge(envelope: Obj) -> list[str]:
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
    base: Obj,
    images: dict[str, dict[str, str]],
    *,
    nonce: str,
    daemon_namespace: str,
    release: str,
    docker: bool,
    arch: str = "amd64",
) -> Obj:
    """scripted_values plus run images; ``docker`` False is the control. ``base`` is kept."""
    if not release.endswith("-" + nonce):
        raise ValueError("release must belong to this run's nonce")
    platform, runner = images["platform"], images["runner"]
    values = copy.deepcopy(base)
    # A short fullname keeps pool names within 63 characters; PriorityClasses (and the
    # quota scoped to them) are cluster-wide with fixed names another release may own.
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
    # The bundled Claude Code must not retry the one dropped model call itself.
    values["agentSandbox"]["runnerEnv"] = {AGENT: {"CLAUDE_CODE_MAX_RETRIES": "0"}}
    sandbox_runner = values["agentSandbox"]["runner"]
    sandbox_runner.pop("tag", None)
    platform_repo = platform["ref"].split("@", 1)[0]
    sandbox_runner.update(image=platform_repo, digest=platform["digest"])
    sandbox_runner["imagePullPolicy"] = "IfNotPresent"
    # Single-platform images: the other platform only needs a sha256, so the config digest.
    other = "arm64" if arch == "amd64" else "amd64"
    digests = {f"linux/{arch}": runner["manifest"], f"linux/{other}": runner["config"]}
    repo_runner: Obj = {"image": runner["ref"], "base": platform["digest"]}
    repo_runner["platformDigests"] = digests
    repo: Obj = {"alias": "acme-bot", "codeHost": "github", "projectId": REPO_ID}
    repo["path"] = REPO
    repo["runner"] = repo_runner | {"tools": ["docker-cli"]}
    if docker:
        repo["docker"] = {"enabled": True}
    resources = {"requests": {"cpu": "500m", "memory": "768Mi", "ephemeral-storage": "2Gi"}}
    resources["limits"] = {"cpu": "1500m", "memory": "1536Mi", "ephemeral-storage": "4Gi"}
    daemon: Obj = {"namespace": daemon_namespace, "daemonImage": images["daemon"]["ref"]}
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


def image_digest(status: Obj) -> str:
    found = re.search(rf"({SHA256})$", str(status.get("imageID", "")))
    return found[1] if found else ""


def container_status(pod: Obj, name: str) -> Obj:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return next((s for s in statuses if s.get("name") == name), {})


def parse_runner_docker(run_text: str, info_text: str, nonce: str) -> Obj:
    """What the scripted turn wrote: the run, the labelled container, and docker info."""
    lines = [line.strip() for line in run_text.splitlines()]
    labelled = [m[1] for line in lines if (m := re.fullmatch(r"[0-9a-f]{12,64} (\S+)", line))]
    parts = info_text.strip().split()
    pair = parts if len(parts) == 2 else ["", ""]
    container = labelled[0] if labelled == [nonce] else ""
    return {"ran": "ran" in lines, "id": pair[0], "name": pair[1], "container_nonce": container}


def docker_env_present(env: Mapping[str, str]) -> bool:
    return any(name in env for name in (*DOCKER_ENV, "DOCKER_PUBLISHED_HOST"))


def daemon_probe(payload: Obj) -> Obj:
    """Daemon /info and TLS refusal, or a worker-provisioned orphan; runs under uv too."""
    from curie_worker.sandbox import KubernetesSandboxClient, tool_daemon

    client = KubernetesSandboxClient(payload["namespace"], kubeconfig=payload["kubeconfig"])
    if payload.get("mode") == "provision":
        from curie_worker.run import _substrate_config

        config, egress = _substrate_config(payload["env"]), payload["egress"]
        daemon = next(p.daemon for p in config.repo_runner_pools.values() if p.daemon)
        claim = config.claim_name_for(payload["thread"], secrets.token_hex(3))
        where = {"claim": claim, "release_namespace": config.namespace, "egress_class": egress}
        tool_daemon.provision(client, daemon, deadline=time.monotonic() + 60, **where)
        return {"claim": claim}
    target = (client, payload["daemon_namespace"], payload["pod"], payload["port"], payload["host"])
    files = payload["files"]
    status, body = tool_daemon.request(*target, files, "/info", 30.0)
    info = json.loads(body) if status == 200 else {}
    out: Obj = {"id": str(info.get("ID", "")), "name": str(info.get("Name", ""))}
    out["accepted"] = tool_daemon.ping(*target, files, 10.0)
    if payload.get("stranger"):
        issued = tool_daemon.issue(payload["stranger"], payload["daemon_namespace"], 600)
        mixed = {"ca.pem": files["ca.pem"]}
        mixed |= {key: issued.client_files[key] for key in ("cert.pem", "key.pem")}
        out["refused"] = not tool_daemon.ping(*target, mixed, 10.0)
        out["accepted_after"] = tool_daemon.ping(*target, files, 10.0)
    return out


PROBE_SCRIPT = (
    "from __future__ import annotations\nimport json, os, secrets, sys, time\n"
    + inspect.getsource(daemon_probe)
    # Port-forward proxy threads outlive their streams; skip finalization so none aborts it.
    + "\nprint(json.dumps(daemon_probe(json.load(sys.stdin))), flush=True)\nos._exit(0)\n"
)


def private_file(path: Path, content: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(content)


def redact(text: str, callback: str) -> str:
    """Drop PEM blocks, base64 runs, userinfo URLs, the callback and other IPv4 literals."""
    text = _BASE64_RUN.sub("<redacted>", _USERINFO_URL.sub("<url>", _PEM.sub("<pem>", text)))
    return _IPV4.sub("<ip>", text.replace(callback, "<callback>") if callback else text)


class ModelProxy:
    """A byte relay to ModelScript that holds, then drops, the first marked model call."""

    def __init__(self, host: str, upstream: str) -> None:
        parts = urlsplit(upstream)
        self.target, self.lock = (str(parts.hostname), parts.port), threading.Lock()
        self.marker = b""
        self.held, self.release, self.dropped = threading.Event(), threading.Event(), 0
        self.hold_timed_out = False
        proxy = self

        class Relay(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                return

            def relay(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                messages = self.command == "POST" and self.path.split("?")[0] == "/v1/messages"
                with proxy.lock:
                    hit = messages and bool(proxy.marker) and proxy.marker in body
                    proxy.marker = b"" if hit else proxy.marker
                if hit:
                    proxy.held.set()
                    proxy.hold_timed_out = not proxy.release.wait(LIMIT_SECONDS)
                    proxy.dropped += 1
                    self.close_connection = True  # closed with no response at all
                    return
                upstream = HTTPConnection(*proxy.target, timeout=LIMIT_SECONDS)
                try:
                    upstream.request(self.command, self.path, body, dict(self.headers))
                    response = upstream.getresponse()
                    self.send_response_only(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("transfer-encoding", "connection"):
                            self.send_header(key, value)
                    self.end_headers()
                    while chunk := response.read1(65536):
                        self.wfile.write(chunk)
                        self.wfile.flush()
                finally:
                    upstream.close()

            do_GET = do_POST = do_PUT = do_DELETE = relay

        self.server = ThreadingHTTPServer((host, 0), Relay)
        self.base_url = f"http://{host}:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.release.set()
        self.server.shutdown()
        self.server.server_close()


class Lifecycle:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.fx = _load("docker_daemon_fixture", FIXTURE_MODULE)
        tools = ROOT / "tools"
        self.github = _load("daemon_lifecycle_github_stub", tools / "github-stub/github_stub.py")
        self.models = _load("daemon_lifecycle_model_script", tools / "model-script/model_script.py")
        self.f = self.stub = self.model = cast(Any, None)
        self.proxy = cast(ModelProxy, None)
        self.forward: subprocess.Popen[str] | None = None
        self.nonce, self.ns, self.dns = args.nonce, args.namespace, args.daemon_namespace
        self.release, self.full = args.release, f"fd-{args.nonce}"
        self.images, self.base_values, self.evidence = dict[str, dict[str, str]](), Obj(), Obj()
        self.env = dict(os.environ, BUILDX_BUILDER="default")
        self.curie = self.publication_ns = self.egress = ""
        self.install_attempted, self.worker_env = False, dict[str, str]()
        self.timings, self.seen_claims = dict[str, float](), set[str]()
        self.envelope: Obj = {"schema": SCHEMA, "result": "failed", "nonce": self.nonce}
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
        try:
            return probe(), False
        except RuntimeError as exc:
            if str(exc) != TRANSIENT:
                raise
            return None, True

    def k(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return cast("subprocess.CompletedProcess[str]", self.f.k(*argv, **kwargs))

    def items(self, path: str, selector: str) -> list[Obj]:
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
            "SELECT coalesce(json_agg(json_build_object('id', r.id, 'status', r.status, 'cause', "
            "coalesce(r.terminal_cause, '')) ORDER BY r.created_at), '[]'::json) "
            "FROM curie.execution_requests r JOIN curie.work_items w ON w.id = r.work_item_id "
            f"WHERE w.tracker_kind = 'github' AND w.tracker_scope_id = '{REPO_ID}' "
            f"AND w.tracker_issue_id = '{int(issue)}'"
        )
        return [{key: str(row[key]) for key in ("id", "status", "cause")} for row in rows]

    def request_row(self, issue: int, request_id: str) -> dict[str, str]:
        """The request's status and terminal cause (curie.execution_requests.terminal_cause)."""
        found = (r for r in self.requests(issue) if r["id"] == request_id)
        return next(found, {"status": "", "cause": ""})

    def set_label(self, issue: int, on: bool) -> None:
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

    def claims(self, digest: str | None = None) -> list[Obj]:
        items: list[Obj] = self.f.get(CLAIMS.format(ns=self.ns)).get("items", [])
        if digest is None:
            return items
        hashes = [c["metadata"].get("labels", {}).get(THREAD_HASH_LABEL) for c in items]
        return [c for c, found in zip(items, hashes, strict=True) if found == digest]

    def new_claim(self, digest: str, before: set[str]) -> str:
        live = [c for c in self.claims(digest) if not c["metadata"].get("deletionTimestamp")]
        fresh = [c for c in live if c["metadata"]["name"] not in before]
        fresh.sort(key=lambda c: c["metadata"]["creationTimestamp"])
        return fresh[-1]["metadata"]["name"] if fresh else ""

    def first_pod(self, ns: str, selector: str, ready: Callable[[Obj], bool]) -> Obj:
        pods = self.items(f"/api/v1/namespaces/{ns}/pods", selector)
        return next((p for p in pods if ready(p)), {})

    def runner_pod(self, claim: str) -> Obj:
        def running(pod: Obj) -> bool:
            state = container_status(pod, "runner").get("state", {})
            return pod.get("status", {}).get("phase") == "Running" and "running" in state

        return self.first_pod(self.ns, f"{CLAIM_LABEL}={claim}", running)

    def daemon_ready(self, claim: str) -> Obj:
        def ready(pod: Obj) -> bool:
            return bool(container_status(pod, "dockerd").get("ready"))

        return self.first_pod(self.dns, self.claim_selector(claim), ready)

    def runner_info(self, pod: Obj, *, every: bool) -> Obj:
        container = next(c for c in pod["spec"]["containers"] if c["name"] == "runner")
        names = {entry["name"]: "" for entry in container.get("env") or []}
        every_present = all(name in names for name in DOCKER_ENV)
        meta, status = pod["metadata"], container_status(pod, "runner")
        info = {"pod": meta["name"], "pod_uid": meta["uid"], "image_id": image_digest(status)}
        return info | {"docker_env": every_present if every else docker_env_present(names)}

    def claim_selector(self, claim: str) -> str:
        return f"{CLAIM_LABEL}={claim},{MANAGED},{INSTANCE_LABEL}={self.release}"

    def daemon_items(self, selector: str) -> Iterator[tuple[str, str, Obj]]:
        for kind, prefix, plural, role in DAEMON_KINDS:
            for item in self.items(f"{prefix}/namespaces/{self.dns}/{plural}", selector):
                yield kind, role, item["metadata"]

    def daemon_objects(self, claim: str) -> list[Obj]:
        return [self.record(*item) for item in self.daemon_items(self.claim_selector(claim))]

    @staticmethod
    def record(kind: str, role: str, meta: Obj) -> Obj:
        found: Obj = {"kind": kind, "role": role, "name": meta["name"]}
        found["namespace"] = meta["namespace"]
        claim_label = meta.get("labels", {}).get(CLAIM_LABEL, "")
        return found | {"uid": meta["uid"], "claim_label": claim_label}

    def client_secret(self, claim: str) -> Obj:
        return cast(Obj, self.f.get(f"/api/v1/namespaces/{self.ns}/secrets/{claim}-tokens"))

    def turn_objects(self, claim: str) -> list[Obj] | None:
        found = self.daemon_objects(claim)
        secret = self.client_secret(claim)
        if secret:
            found.append(self.record("Secret", "client-secret", secret["metadata"]))
        return found if sorted(o["role"] for o in found) == sorted(ROLES) else None

    def remaining(self, objects: list[Obj]) -> tuple[list[str], list[str]]:
        """Recorded objects still present by uid, and same-name successors carrying the claim."""
        present, recreated = [], []
        for obj in objects:
            prefix, plural = PATHS[obj["kind"]]
            path = f"{prefix}/namespaces/{obj['namespace']}/{plural}/{obj['name']}"
            meta = self.f.get(path).get("metadata") or {"uid": ""}
            if meta["uid"] == obj["uid"]:
                present.append(f"{obj['kind']}/{obj['name']}")
            elif meta["uid"] and meta.get("labels", {}).get(CLAIM_LABEL) == obj["claim_label"]:
                recreated.append(f"{obj['kind']}/{obj['name']}")
        return present, recreated

    def leaks(self, match: Callable[[str], bool]) -> list[str]:
        """Kind/name of matching daemon objects and app client Secrets, terminating included."""
        found = set()
        selector = quote(f"{CLAIM_LABEL},{MANAGED},{INSTANCE_LABEL}={self.release}")
        for kind, prefix, plural, _ in DAEMON_KINDS:
            path = f"{prefix}/namespaces/{self.dns}/{plural}?labelSelector={selector}"
            for item in self.f.get(path).get("items", []):
                if match(item["metadata"]["labels"][CLAIM_LABEL]):
                    found.add(f"{kind}/{item['metadata']['name']}")
        for item in self.f.get(f"/api/v1/namespaces/{self.ns}/secrets").get("items", []):
            name, labels = item["metadata"]["name"], item["metadata"].get("labels") or {}
            tokens = name.endswith("-tokens") and match(name.removesuffix("-tokens"))
            if tokens or (CLAIM_LABEL in labels and match(labels[CLAIM_LABEL])):
                found.add(f"Secret/{name}")
        return sorted(found)

    def runner_exec(self, pod: str, script: str) -> subprocess.CompletedProcess[str]:
        argv = ("-n", self.ns, "exec", pod, "-c", "runner", "--", "sh", "-c", script)
        return self.k(*argv, check=False, timeout=60)

    def signal(self, pod: str, name: str, content: str = "") -> None:
        tmp, final = f"/tmp/fd-{name}.tmp", f"/tmp/fd-{name}"
        script = f"printf %s '{content}' > {tmp} && mv {tmp} {final}"
        self.fx.require(self.runner_exec(pod, script).returncode == 0, f"signal_failed:{name}")

    def runner_outputs(self, pod: str) -> tuple[str, str] | None:
        found = [self.runner_exec(pod, f"cat /tmp/fd-{name}") for name in ("run", "info")]
        run, info = (r.stdout if r.returncode == 0 else "" for r in found)
        return (run, info) if run and info else None

    @contextlib.contextmanager
    def worker_kubeconfig(self) -> Iterator[Path]:
        """A 0600 kubeconfig for the chart's worker ServiceAccount, deleted after use."""
        path = self.f.work / f"worker-{uuid.uuid4().hex}.kubeconfig"
        view = json.loads(self.k("config", "view", "--minify", "--flatten", "-o", "json").stdout)
        account = f"{self.full}-worker"
        token = self.k("-n", self.ns, "create", "token", account, "--duration=10m").stdout.strip()
        context = {"cluster": "run", "user": "worker", "namespace": self.ns}
        config: Obj = {"apiVersion": "v1", "kind": "Config", "current-context": "worker"}
        config["clusters"] = [{"name": "run", "cluster": view["clusters"][0]["cluster"]}]
        config["users"] = [{"name": "worker", "user": {"token": token}}]
        config["contexts"] = [{"name": "worker", "context": context}]
        try:
            private_file(path, json.dumps(config))
            yield path
        finally:
            path.unlink(missing_ok=True)

    def client_files(self, claim: str) -> dict[str, str]:
        data = self.client_secret(claim).get("data", {})
        self.fx.require(all(key in data for key in CLIENT_FILES), "claim_client_files_missing")
        return {key: base64.b64decode(data[key]).decode() for key in CLIENT_FILES}

    def probe(self, payload: Obj, *, retry: bool = True) -> Obj:
        """``daemon_probe`` as the worker ServiceAccount; no secret goes on argv."""
        payload |= {"namespace": self.ns, "daemon_namespace": self.dns}
        with self.worker_kubeconfig() as kubeconfig:
            payload["kubeconfig"] = str(kubeconfig)
            try:
                import curie_worker.sandbox  # noqa: F401 - only proves the package imports
            except ImportError:
                return self.probe_subprocess(json.dumps(payload), (1, 2) if retry else (1,))
            return daemon_probe(payload) | {"attempts": 1}

    def probe_subprocess(self, data: str, attempts: tuple[int, ...]) -> Obj:
        """The probe under uv, retried once only when a crash left no result to judge."""
        snapshot = str(self.f.snapshot)
        argv = ["uv", "run", "--project", snapshot, "--frozen", "python3", "-c", PROBE_SCRIPT]
        for attempt in attempts:
            run = functools.partial(subprocess.run, text=True, capture_output=True, check=False)
            result = run(argv, input=data, cwd=snapshot, timeout=300)
            lines = result.stdout.strip().splitlines()
            with contextlib.suppress(ValueError):
                found = json.loads(lines[-1]) if lines else None
                if isinstance(found, dict):
                    return found | {"attempts": attempt}
            if result.returncode:
                tail = redact(result.stderr, self.args.callback_host)[-PROBE_TAIL:]
                self.evidence["probe_stderr"] = tail
            if attempt < len(attempts):
                time.sleep(5)
        raise RuntimeError(f"probe_failed:{result.returncode}")

    def before(self, issue: int) -> tuple[set[str], set[str]]:
        names = {c["metadata"]["name"] for c in self.claims()}
        return {r["id"] for r in self.requests(issue)}, names | self.seen_claims

    def acquire(self, issue: int, requests: set[str], claims: set[str]) -> tuple[Obj, Obj]:
        thread, digest = thread_key(issue), thread_hash(thread_key(issue))

        def started() -> tuple[dict[str, str], str, Obj] | None:
            request = next((r for r in self.requests(issue) if r["id"] not in requests), None)
            claim = self.new_claim(digest, claims)
            pod = self.runner_pod(claim) if request and claim else {}
            return (request, claim, pod) if request and pod else None

        request, claim, pod = self.bounded(started, LIMIT_SECONDS, "turn_not_started")
        self.seen_claims.add(claim)
        turn = {"issue": issue, "thread": thread, "claim": claim, "request_id": request["id"]}
        return turn, pod

    def observe(self, turn: Obj, pod: Obj, *, tls: bool = False, run: bool = True) -> str:
        """Record identities and objects before any fault; return the runner Pod name."""
        claim = turn["claim"]
        turn["runner"] = self.runner_info(pod, every=True)
        objects = self.bounded(lambda: self.turn_objects(claim), 60, "daemon_objects_incomplete")
        turn["objects"] = objects
        daemon = self.bounded(lambda: self.daemon_ready(claim), LIMIT_SECONDS, "daemon_not_ready")
        meta, status = daemon["metadata"], container_status(daemon, "dockerd")
        self.egress = self.egress or meta.get("labels", {}).get(EGRESS_LABEL, "")
        identity = {"pod": meta["name"], "pod_uid": meta["uid"]}
        turn["daemon"] = identity | {"image_id": image_digest(status)}
        runner = str(turn["runner"]["pod"])
        if run:
            self.signal(runner, "go", self.nonce)
            outputs = functools.partial(self.runner_outputs, runner)
            texts = self.bounded(outputs, LIMIT_SECONDS, "runner_silent")
            turn["runner_docker"] = parse_runner_docker(*texts, self.nonce)
        target = {"pod": meta["name"], "port": DAEMON_PORT, "host": f"{claim}.{self.dns}.svc"}
        # A stranger is shaped like SandboxConfig.claim_name_for's, for no real thread.
        stranger = f"curie-thread-{secrets.token_hex(5)}-{secrets.token_hex(3)}" if tls else ""
        target |= {"files": self.client_files(claim), "stranger": stranger}
        own = self.probe(target)
        turn["probe_attempts"] = own.pop("attempts")
        self.fx.require(own["accepted"], "claim_client_refused")
        turn["daemon"].update(docker_id=own["id"], docker_name=own["name"])
        if tls:
            seen = {"claim": claim, "independent_client_refused": own["refused"]}
            seen |= {"claim_client_accepted": own["accepted"]}
            self.envelope["tls"] = seen | {"claim_client_accepted_after": own["accepted_after"]}
        return runner

    def settle(self, turn: Obj, fault: str, started: float) -> None:
        """Until the request is terminal, recorded objects are gone by uid and nothing leaks."""
        turn["fault"] = fault
        row, present, leaked, recreated = {"status": "", "cause": ""}, [""], [""], list[str]()
        claim = turn["claim"]
        deadline = started + LIMIT_SECONDS
        while True:
            self.check_fixtures()
            try:
                row = self.request_row(turn["issue"], turn["request_id"])
                present, recreated = self.remaining(turn["objects"])
                leaked = self.leaks(lambda label: label == claim)
            except RuntimeError as exc:
                if str(exc) != TRANSIENT or time.monotonic() >= deadline:
                    raise
                time.sleep(2)
                continue
            settled = row["status"] in TERMINAL and not present and not leaked
            if settled or time.monotonic() >= deadline:
                break
            time.sleep(2)
        turn["settle_seconds"] = round(time.monotonic() - started, 1)
        turn |= {"objects_absent": not present, "request_state": row["status"]}
        turn |= {"settle_cause": row["cause"], "leaked": leaked}
        if recreated:
            turn["recreated"] = recreated

    def docker_turn(
        self, issue: int, start: Callable[[], None], *, tls: bool = False
    ) -> tuple[Obj, str]:
        requests, claims = self.before(issue)
        start()
        turn, pod = self.acquire(issue, requests, claims)
        return turn, self.observe(turn, pod, tls=tls)

    def fault(self, key: str, turn: Obj, fault: str, inject: Callable[[], Any]) -> float:
        """Inject ``fault``, settle the turn from that instant, record it; return the instant."""
        started = time.monotonic()
        inject()
        self.settle(turn, fault, started)
        if key in DELIVERED:
            turn["turn_delivered"] = (turn["request_state"], turn["settle_cause"]) == DELIVERY
        self.envelope["turns"][key] = turn
        return started

    def bounded_flag(self, probe: Callable[[], Any], category: str, seconds: float) -> bool:
        try:
            return bool(self.bounded(probe, seconds, category))
        except RuntimeError as exc:
            if str(exc) != category:
                raise
            return False

    def scenario_success(self) -> None:
        issue = ISSUES["success"]
        turn, runner = self.docker_turn(issue, lambda: self.set_label(issue, True), tls=True)
        self.fault("success", turn, "release", lambda: self.signal(runner, "release"))

    def scenario_daemon_death(self) -> None:
        """The daemon dies under a held model call; the dropped call's in-delivery retry must
        find the retained route unhealthy, evict the dead claim and bind a fresh one."""
        issue, proxy = ISSUES["daemon_death"], self.proxy
        proxy.marker = f"/issues/{issue}".encode()
        requests, claims = self.before(issue)
        self.set_label(issue, True)
        turn, pod = self.acquire(issue, requests, claims)
        self.observe(turn, pod, run=False)
        self.bounded(proxy.held.is_set, LIMIT_SECONDS, "model_call_not_held")
        claim, name, digest = turn["claim"], turn["daemon"]["pod"], thread_hash(turn["thread"])
        turn |= {"fault": "daemon_pod_deleted", "objects_absent": False, "leaked": ["unread"]}
        self.envelope["turns"]["daemon_death"] = turn
        deadline = (started := time.monotonic()) + LIMIT_SECONDS
        self.k("-n", self.dns, "delete", "pod", name, "--wait=false")
        gone = functools.partial(self.absent, f"/api/v1/namespaces/{self.dns}/pods/{name}")
        self.bounded(gone, LIMIT_SECONDS, "daemon_pod_remains")
        proxy.release.set()

        def replaced() -> str:
            successor = self.new_claim(digest, claims | {claim})
            gone = not self.remaining(turn["objects"])[0]
            return successor if gone and not self.leaks(lambda c: c == claim) else ""

        left = max(0.0, deadline - time.monotonic())
        successor = self.bounded(replaced, left, "retained_claim_not_replaced")
        turn |= {"settle_seconds": round(time.monotonic() - started, 1), "objects_absent": True}
        turn |= {"leaked": [], "model_transport_failures": proxy.dropped}
        turn["model_hold_timed_out"] = proxy.hold_timed_out
        self.seen_claims.add(successor)
        ids = [r["id"] for r in self.requests(issue) if r["id"] not in requests]
        fresh = {"issue": issue, "thread": turn["thread"], "claim": successor}
        fresh["request_id"] = ids[-1] if ids else ""
        pod = self.bounded(lambda: self.runner_pod(successor), LIMIT_SECONDS, "turn_not_started")
        runner = self.observe(fresh, pod)
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
        # A graceful delete drains the held turn for its 1860 s grace; forced models a crash.
        force = ("--grace-period=0", "--force", "--wait=false")
        delete = ("-n", self.ns, "delete", "pod", *pods, *force)
        turn["worker_deletion"] = "forced"
        started = self.fault("worker_loss", turn, "worker_pod_deleted", lambda: self.k(*delete))
        # Any owner-lost successor is cancelled by unlabel; all clears within 300 s of deletion.
        self.set_label(issue, False)
        digest = thread_hash(turn["thread"])
        prefix = f"curie-thread-{digest}-"

        def cleared() -> bool:
            states = {r["status"] for r in self.requests(issue)}
            leftovers = self.leaks(lambda label: label.startswith(prefix))
            return states <= TERMINAL and not self.claims(digest) and not leftovers

        left = max(0.0, started + LIMIT_SECONDS - time.monotonic())
        turn["successor_cleared"] = self.bounded_flag(cleared, "successor_left", left)
        if turn["successor_cleared"]:
            turn["recovery_seconds"] = round(time.monotonic() - started, 1)
        self.rollout("worker")

    def scenario_orphan(self) -> None:
        """Crash-before-claim: provision() as the worker (only it may create daemon Pods)."""
        self.fx.require(self.egress, "daemon_egress_unobserved")
        payload = {"mode": "provision", "env": self.worker_env, "egress": self.egress}
        before = time.monotonic()
        claim = self.probe(payload | {"thread": f"orphan-{self.nonce}"}, retry=False)["claim"]
        created = time.monotonic()  # ages from here never overstate removal

        def provisioned() -> list[Obj] | None:
            found = self.daemon_objects(claim)
            return found if len(found) == len(DAEMON_KINDS) else None

        objects = self.bounded(provisioned, 60, "orphan_not_provisioned")
        # Young means every successful read before the age gate saw all four objects.
        kept, observed, removed = True, False, None
        while True:
            self.check_fixtures()
            age = time.monotonic() - created
            found, failing = self.attempt(lambda: self.remaining(objects)[0])
            if not failing:
                observed = True
                if not found:
                    removed = round(age, 1)
                    break
                if time.monotonic() - before < CLAIM_TIMEOUT_SECONDS:
                    kept = kept and len(found) == len(objects)
            if age >= LIMIT_SECONDS:
                break
            time.sleep(2)
        self.fx.require(observed, TRANSIENT)
        orphan: Obj = {"claim": claim, "age_gate_seconds": CLAIM_TIMEOUT_SECONDS}
        orphan |= {"objects": objects, "provisioned_by": "worker", "kept_while_young": kept}
        orphan |= {"settle_seconds": round(time.monotonic() - created, 1)}
        orphan |= {"leaked": self.leaks(lambda label: label == claim), "objects_absent": False}
        if removed is not None:
            orphan |= {"removed_after_seconds": removed, "objects_absent": True}
        self.envelope["orphan"] = orphan

    def scenario_control(self) -> None:
        control = self.values(docker=False)
        values = self.f.work / "control.values.json"
        private_file(values, json.dumps({"factory": {"bindings": control["factory"]["bindings"]}}))
        chart = str(self.install_chart())
        upgrade = (chart, "-n", self.ns, "--reuse-values", "-f", str(values), "--timeout", "15m")
        self.helm("upgrade", *upgrade)
        self.rollout("api", "worker")
        issue = CONTROL_ISSUE
        requests, claims = self.before(issue)
        self.set_label(issue, True)
        turn, pod = self.acquire(issue, requests, claims)
        claim = turn["claim"]
        secret = self.client_secret(claim).get("data", {})
        result: Obj = {"issue": issue, "thread": turn["thread"], "claim": claim}
        result["runner"] = self.runner_info(pod, every=False)
        result["daemon_objects"] = len(self.daemon_objects(claim))
        result["client_docker_files"] = any(key in secret for key in CLIENT_FILES)
        runner = result["runner"]["pod"]
        self.signal(runner, "go", self.nonce)
        self.signal(runner, "release")

        row: dict[str, str] = {}

        def terminal() -> bool:
            row.update(self.request_row(issue, turn["request_id"]))
            return row["status"] in TERMINAL

        result["completed"] = self.bounded_flag(terminal, "control_not_terminal", LIMIT_SECONDS)
        result["settle_cause"] = row.get("cause", "")
        result["turn_delivered"] = (row.get("status"), row.get("cause")) == DELIVERY
        self.envelope["control"] = result

    def install_chart(self) -> Path:
        """The chart the release is installed and control-upgraded from."""
        return cast(Path, self.f.snapshot / "charts/curie")

    def values(self, *, docker: bool) -> Obj:
        names = {"nonce": self.nonce, "daemon_namespace": self.dns, "release": self.release}
        names |= {"arch": self.f.arch}
        return lifecycle_values(self.base_values, self.images, docker=docker, **names)

    def build_image(self, name: str) -> dict[str, str]:
        """API and worker images, built exactly as Fixture.images() builds its own."""
        fx, f = self.fx, self.f
        tag = f"{f.registry}/fd-{self.nonce}/{name}:candidate"
        fx.require(tag not in fx.run(fx.TAGS).stdout.split(), "build_tag_occupied")
        request = uuid.uuid4().hex
        f.asset("tag_intent", tag, request)
        metadata = f.work / (name + "-build.json")
        argv = ["docker", "buildx", "build", "--builder", "default", "--platform"]
        argv += ["linux/" + f.arch]
        argv += ["--label", f"{fx.REQUEST}={request}", "--label", f"{fx.NONCE}={self.nonce}"]
        argv += ["--provenance=false", "--push", "--metadata-file", str(metadata), "-t", tag]
        argv += ["-f", str(f.snapshot / "apps" / name / "Dockerfile"), str(f.snapshot)]
        fx.run(argv, timeout=7200)
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
        seeds = {"host": host, "seed_tree": SEED_TREE, "issue_file": ISSUE_FILE}
        self.stub = self.github.GithubStub(self.f.work / "github", recording, **seeds)
        self.stub.start()
        # The stub seeds issue 3815 labelled; this run uses its own issues only.
        self.stub._put("issue", 3815, self.stub._get("issue", 3815) | {"labels": []})
        self.model = self.models.ModelScript(TRANSCRIPT, host=host)
        self.model.start()
        self.proxy = ModelProxy(host, self.model.base_url)

    def rollout(self, *components: str) -> None:
        for component in components:
            deployment = f"deployment/{self.full}-{component}"
            self.k("-n", self.ns, "rollout", "status", deployment, "--timeout=300s", timeout=330)

    def helm(self, verb: str, *argv: str) -> None:
        """helm install or upgrade; a failure keeps a bounded, redacted stderr tail."""
        command = ["helm", "--kube-context", self.args.context, verb, self.release, *argv]
        result = subprocess.run(command, text=True, capture_output=True, timeout=1000, check=False)
        if result.returncode:
            tail = redact(result.stderr[-STDERR_TAIL:], self.args.callback_host)
            self.evidence["helm_stderr"] = tail
            raise RuntimeError(f"helm_{verb}_failed")

    def secret_inputs(self) -> Obj:
        def create(kind: str, name: str, body: Obj) -> None:
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
        stub, platform = self.stub.base_url, self.images["platform"]["ref"].split("@", 1)[0]
        scripted = {"model_base_url": self.proxy.base_url, "github_api": stub + "/api/v3"}
        scripted |= {"clone_base": stub, "ca_configmap": "github-stub-ca", "runner_image": platform}
        self.base_values = sk.scripted_values(**scripted)
        values = self.values(docker=True) | self.secret_inputs()
        values_file = self.f.work / "values.json"
        private_file(values_file, json.dumps(values))
        # Chart workloads (Postgres, Valkey, RustFS) need baseline, not restricted.
        psa = "pod-security.kubernetes.io/enforce=baseline"
        self.k("label", "namespace", self.ns, "--overwrite", psa)
        self.f.note("app_namespace_psa", "baseline")
        self.install_attempted = True
        chart = str(self.install_chart())
        files = ("-f", chart + "/values-dev.yaml", "-f", str(values_file), "--timeout", "15m")
        self.helm("install", chart, "-n", self.ns, "--skip-crds", *files)
        self.rollout("api", "worker")
        path = f"/apis/apps/v1/namespaces/{self.ns}/deployments/{self.full}-worker"
        containers = self.f.get(path)["spec"]["template"]["spec"]["containers"]
        env = next(c for c in containers if c["name"] == "worker")["env"]
        self.worker_env = {e["name"]: e["value"] for e in env if "value" in e}
        self.publication_ns = self.worker_env.get("CURIE_PUBLICATION_NAMESPACE", "")
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
        url = self.stub.base_url + "/api/v3/app/installations/5501/access_tokens"
        headers = {"Content-Type": "application/json"}
        request = Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
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
        build = [self.curie, "build", "--plugin-dir", str(bundle), "--registry", self.f.registry]
        build += ["--runner-image", self.images["platform"]["ref"], *platform]
        self.fx.run(build, env=env, timeout=1800)
        deploy = [self.curie, "cluster", "--context", self.args.context, "deploy"]
        deploy += ["--plugin-dir", str(bundle), "--agent", AGENT, "--env", "prod"]
        deploy += ["--chart", str(self.install_chart())]
        deploy += ["--secret", "GITHUB_PERSONAL_ACCESS_TOKEN", "--namespace", self.ns]
        deploy += ["--release", self.release, "--api-url", api_url]
        self.fx.run(deploy, env=env, timeout=600)

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
        for server in (self.proxy, self.model, self.stub):
            try:
                if server is not None:
                    server.close()
            except (RuntimeError, OSError) as exc:
                # An audited unexpected request is already this run's failure.
                audited = getattr(server, "unexpected", None)
                if not (audited or getattr(server, "unknown_requests", None)):
                    errors.append(f"server_close:{type(exc).__name__}")

    def release_objects(self) -> list[str]:
        """Cluster-scoped release objects; namespaced ones go with the owned namespaces."""
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
        if found := self.f.get(path):
            owner = found["metadata"].get("labels", {}).get(INSTANCE_LABEL)
            self.fx.require(owner == self.release, "publication_namespace_unowned")
            self.k("delete", "namespace", name, "--wait=false")
        gone = functools.partial(self.absent, path)
        self.bounded(gone, 300, "publication_namespace_remains", audit=False)

    def absent(self, path: str) -> bool:
        return not self.f.get(path)

    def registry_tags(self) -> list[str]:
        """Local tags this run's registry name carries that the fixture does not own."""
        if not self.f.registry:
            return []
        query = "SELECT name FROM assets WHERE kind='tag_intent'"
        owned = {row[0] for row in self.f.db.execute(query)}
        tags = self.fx.run(self.fx.TAGS).stdout.split()
        return [t for t in tags if t.startswith(self.f.registry + "/") and t not in owned]

    def teardown(self, errors: list[str]) -> dict[str, bool]:
        try:
            return self.release_everything(errors)
        finally:
            self.scrub_work(errors)

    def scrub_work(self, errors: list[str]) -> None:
        """Delete work-dir key material on any outcome; evidence, metadata and snapshot stay."""
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

    def diagnostics(self) -> list[Obj]:
        """Pod and Warning Event state only: no env, Secret data or logs."""
        entries: list[Obj] = []
        for ns in dict.fromkeys(n for n in (self.ns, self.dns, self.publication_ns) if n):
            try:
                if self.f.get(f"/api/v1/namespaces/{ns}"):
                    entries += self.namespace_diagnostics(ns)
            except (RuntimeError, KeyError, ValueError, subprocess.SubprocessError) as exc:
                category = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                entries.append({"namespace": ns, "error": category})
        return entries[:DIAGNOSTIC_LIMIT]

    def namespace_diagnostics(self, ns: str) -> list[Obj]:
        def short(text: object) -> str:
            return redact(str(text or ""), self.args.callback_host)[:MESSAGE_LIMIT]

        base, entries = f"/api/v1/namespaces/{ns}", list[Obj]()
        for pod in self.f.get(base + "/pods").get("items", []):
            status, containers = pod.get("status", {}), []
            init = status.get("initContainerStatuses", [])
            for item in init + status.get("containerStatuses", []):
                state = item.get("state", {})
                detail = state.get("waiting") or state.get("terminated") or {}
                code, message = detail.get("exitCode"), short(detail.get("message"))
                containers.append(f"{item.get('name')}:{detail.get('reason')}:{code}:{message}")
            name, phase = pod["metadata"]["name"], status.get("phase")
            entries.append({"namespace": ns, "pod": name, "phase": phase, "containers": containers})
        for event in self.f.get(f"{base}/events?fieldSelector={quote('type=Warning')}")["items"]:
            kind, name = event["involvedObject"].get("kind"), event["involvedObject"].get("name")
            what = f"{kind}/{name}: {event.get('reason')}: {short(event.get('message'))}"
            entries.append({"namespace": ns, "event": what})
        return entries

    def failure_evidence(self, failed: bool) -> Obj:
        out: Obj = dict(self.evidence)
        if failed and self.f is not None:
            try:
                out["diagnostics"] = self.diagnostics()
            except (RuntimeError, OSError, KeyError, ValueError, subprocess.SubprocessError) as exc:
                out["diagnostics_error"] = type(exc).__name__
            print(json.dumps({"diagnostics": out.get("diagnostics", [])}), file=sys.stderr)
        if self.model is not None and self.model.unexpected:
            host, cb = self.args.callback_host, "<callback>"
            keys = [item.get("key") or () for item in self.model.unexpected]
            out["unexpected_model_keys"] = [[str(p).replace(host, cb) for p in k] for k in keys]
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
        evidence = self.evidence | evidence
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
    parser = argparse.ArgumentParser("daemon_lifecycle.py", description=description, epilog=EPILOG)
    parser.formatter_class = raw
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
