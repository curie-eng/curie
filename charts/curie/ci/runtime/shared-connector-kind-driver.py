"""Production reconcile loop for shared connector applications on a real cluster.

Run only by ``shared-connector-kind-runtime.sh``, which owns the kind cluster,
the backing-store containers and every ``SCK_*`` variable this reads. ADR 0239,
cp5: AC3 and AC6 on a real API server, plus the AC5 network probes that need
reconciler-applied objects.

Nothing internal is faked. The API is the real application (``alembic upgrade
head``, then ``uvicorn curie_api.main:create_app``) on the run's Postgres,
Valkey and RustFS containers. The loop is the one the worker runs, built by
``curie_worker.run._build_connector_loop`` from a ``WorkerConfig`` read from
the environment, so its Kubernetes clients authenticate with a kubeconfig
holding the worker ServiceAccount's own token: the chart's Role and the
worker-secrets admission policy decide every write. Agents, bundles,
deployments, memberships and applications are created through the real API
routes. Membership always precedes the bundle upload, because upload
resolves membership.

Phases, each assertion printed as one PASS line:

1. The API boots with a configured caller verify key; the render route is
   the only way that key reaches the shared proxy (no key table).
2. Application 1 is registered; agents A and B become members, upload a
   bundle declaring the shared alias plus a remote ``url:`` connector, and
   deploy. The worker identity is checked (it may list and create Secrets,
   never get them) and the admission policy refuses it a malformed Secret.
3. Pass 1: the key Secret ``shared-key-<uuid>`` exists in its exact shape and
   its key ring parses with the resolver's own parser; the recipient the API
   records is sha256 of the public key derived from that Secret; A's and B's
   reach equal ``render_member_reach`` and the render route returns the same
   bytes; the database is not provisioned, so no Deployment is applied.
4. The operator provisions the bundled database. Pass 2: the DSN Secret
   ``shared-dsn-<uuid>`` holds exactly the sealed DSN the worker route plans
   and opens with the key Secret's key; the four application objects match
   ``render_application`` field for field (quantities compared by value), were
   applied by field manager curie-shared-connector-reconciler, carry no
   dedicated owner label, and the worker route's objects equal the frozen
   output byte for byte. Pass 3 changes no Secret (resourceVersion), not even
   the operator's grant-store Secret, and leaves the Deployment generation.
5. Network probes, with every root policy plus the frozen deny, database and
   reach policies active. The probe Pods carry the EXACT labels the frozen
   render gives the application Pods and member runners (busybox stand-ins;
   real proxy readiness in-cluster is cp8):
   member runners A and B reach the shared Service; a non-member runner, the
   generic mcp-connector Pod and a namespace peer are refused; the shared Pod
   resolves DNS, reaches Postgres 5432, the database target's 5432, the
   Valkey grant store and its configured provider port, and is refused on the
   target's unconfigured port 15432, on an unconfigured target and on the
   provider's unconfigured port, while a namespace peer reaches each of those
   listeners (so each refusal is the shared Pod's own egress); the generic
   connector is refused at Postgres.
5b. The GC race: the chart's root <fullname>-shared-app-deny-ingress policy
   is applied and empty; the shared Pod listens on its server container port
   (in-Pod control). The application's own deny policy is deleted with
   kubectl: a namespace peer is refused at the Service, the Pod's proxy port
   and its server port while member A still reaches the Service. Then every
   member ingress policy is deleted too: the peer is still refused on all
   three. Pass 3b restores the deny and the reach, and both members reach
   the Service again.
6. Adoption: applications 2 and 3 are registered; the cluster admin
   pre-creates a valid key Secret for 2 and a malformed one for 3; agents C
   and D become members and deploy (asserted declared through the render
   route before the pass). Pass 4 adopts 2's Secret untouched and registers
   its public key; 3 is refused with shared_key_secret_invalid in the pass
   log, its Secret untouched, no recipient, no Deployment, and the refusal
   counts as a failed pass.
7. Remove-one: B's membership is deleted, then a pass. B's reach is gone and
   B's runner is refused; A's reach (same uid), the Deployment (same uid,
   generation and template), the Service, both policies and both Secrets
   remain. B then deploys a version without the alias: same survivors. A
   then deploys a version without the alias: the application objects are
   deleted, over as many passes as the foreground Deployment deletion takes;
   the deny ingress policy exists whenever the Deployment is still listed,
   both are gone at the end, and the key and DSN Secrets remain unchanged.

``SCK_RECORD_STATUS_FIXTURES=<dir>`` also records the Deployment status
shapes AC3's suite replays (available, progressing, deadline exceeded).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import logging
import os
import secrets
import socket
import string
import subprocess
import sys
import tarfile
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import nacl.exceptions
import nacl.public
import nacl.signing
import yaml
from curie_connector_proxy.resolver.keys import KeyRing, strict_b64decode
from plugin_format.connectors import parse_quantity
from plugin_format.shared_connector_render import (
    SharedApplication,
    SharedMember,
    render_application,
    render_member_reach,
)

REPO_ROOT = Path(__file__).resolve().parents[4]

APP_LABEL = "curietech.ai/shared-application"
ROLE_LABEL = "curietech.ai/shared-secret"
AGENT_LABEL = "curietech.ai/agent-id"
COMPONENT = "shared-connector-application"
OWNER_LABEL = "curie.dev/connector-owner"
FIELD_MANAGER = "curie-shared-connector-reconciler"
ALIAS = "tracker"
TOOLS = ["issues.read"]
KEY_DATA = "private-keys"
DSN_DATA = "sealed-dsn"
PROXY_PORT = 8480


class Failure(Exception):
    """One assertion failed; the message names it."""


PASSED = 0


def ok(label: str) -> None:
    global PASSED
    PASSED += 1
    print(f"PASS {label}", flush=True)


def check(condition: bool, label: str, detail: str = "") -> None:
    if not condition:
        raise Failure(f"{label}{': ' + detail if detail else ''}")
    ok(label)


def env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise Failure(f"{name} is not set; run this through shared-connector-kind-runtime.sh")
    return value


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


# --- the run ---------------------------------------------------------------------


@dataclass
class Run:
    run_id: str = field(default_factory=lambda: env("SCK_RUN_ID"))
    work: Path = field(default_factory=lambda: Path(env("SCK_WORKDIR")))
    kubeconfig: str = field(default_factory=lambda: env("SCK_KUBECONFIG"))
    context: str = field(default_factory=lambda: env("SCK_CONTEXT"))
    namespace: str = field(default_factory=lambda: env("SCK_NAMESPACE"))
    release: str = field(default_factory=lambda: env("SCK_RELEASE"))
    fullname: str = field(default_factory=lambda: env("SCK_FULLNAME"))
    app_name: str = field(default_factory=lambda: env("SCK_APP_NAME"))
    apps: list[str] = field(default_factory=lambda: env("SCK_APPS").split())
    image: str = field(default_factory=lambda: env("SCK_IMAGE"))
    grant_secret: str = field(default_factory=lambda: env("SCK_GRANT_SECRET"))
    db_target_label: str = field(default_factory=lambda: env("SCK_DB_TARGET_LABEL"))
    secrets_policy: str = field(default_factory=lambda: env("SCK_SECRETS_POLICY"))
    # The API's trusted caller verify keys, current first: the only way they
    # reach the shared proxy is the render (rework 1).
    caller_keys: tuple[str, ...] = ()
    installation: str = field(
        default_factory=lambda: "".join(
            secrets.choice(string.ascii_letters + string.digits) for _ in range(32)
        )
    )

    @property
    def worker_sa(self) -> str:
        return f"{self.fullname}-worker"

    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://postgres:{env('SCK_PG_PASSWORD')}"
            f"@127.0.0.1:{env('SCK_PG_PORT')}/postgres"
        )

    def spec(self, app: str) -> dict[str, Any]:
        """The SharedApplication registered for ``app``, with the worker-managed names."""

        return {
            "installation": self.installation,
            "application": app,
            "release": self.release,
            "namespace": self.namespace,
            "app_name": self.app_name,
            "server_image": self.image,
            "proxy_image": self.image,
            "server_args": ["sleep", "36000"],
            "encryption_key": {"name": f"shared-key-{app}", "key": KEY_DATA},
            "database_dsn": {"name": f"shared-dsn-{app}", "key": DSN_DATA},
            "dsn_encoding": "bundled-sealed",
            "grant_store": {"name": self.grant_secret, "key": "url"},
            "provider_origins": ["https://provider.example.com"],
            "credential_slots": ["access_token"],
            "database_target": {
                "namespace": self.namespace,
                "pod_labels": {self.db_target_label: self.run_id},
                "port": 5432,
            },
        }

    def model(self, app: str) -> SharedApplication:
        return SharedApplication.model_validate_json(json.dumps(self.spec(app)))

    def runner_labels(self, agent: str) -> dict[str, str]:
        return {
            AGENT_LABEL: agent,
            "app.kubernetes.io/component": "runner-sandbox",
            "app.kubernetes.io/name": self.app_name,
            "app.kubernetes.io/instance": self.release,
        }

    def member(self, agent: str) -> SharedMember:
        return SharedMember(agent=agent, runner_labels=self.runner_labels(agent))


# --- kubectl ----------------------------------------------------------------------


class Kube:
    """kubectl against the run's own cluster, always with --kubeconfig and --context."""

    def __init__(self, kubeconfig: str, context: str, namespace: str) -> None:
        self._base = ["kubectl", "--kubeconfig", kubeconfig, "--context", context]
        self.namespace = namespace

    def run(
        self, *args: str, stdin: str | None = None, namespaced: bool = True, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        command = [*self._base, *(["-n", self.namespace] if namespaced else []), *args]
        done = subprocess.run(command, input=stdin, capture_output=True, text=True, check=False)
        if check and done.returncode:
            raise Failure(f"kubectl {' '.join(args[:3])} failed: {done.stderr.strip()[:400]}")
        return done

    def get(self, kind: str, name: str, *, managed_fields: bool = False) -> dict[str, Any] | None:
        """The live object, or None on NotFound.

        kubectl strips metadata.managedFields from its output unless asked, so
        a check that reads the field manager must pass ``managed_fields``.
        """

        flags = ["--show-managed-fields"] if managed_fields else []
        done = self.run("get", kind, name, "-o", "json", *flags, check=False)
        if done.returncode == 0:
            result: dict[str, Any] = json.loads(done.stdout)
            return result
        if "NotFound" in done.stderr:
            return None
        raise Failure(f"kubectl get {kind}/{name} failed: {done.stderr.strip()[:400]}")

    def must(self, kind: str, name: str) -> dict[str, Any]:
        found = self.get(kind, name)
        if found is None:
            raise Failure(f"{kind}/{name} does not exist")
        return found

    def list(self, kind: str, selector: str) -> list[dict[str, Any]]:
        done = self.run("get", kind, "-l", selector, "-o", "json")
        items: list[dict[str, Any]] = json.loads(done.stdout).get("items") or []
        return items

    def apply(self, obj: dict[str, Any]) -> None:
        self.run("apply", "-f", "-", stdin=json.dumps(obj))

    def create(self, obj: dict[str, Any]) -> None:
        self.run("create", "-f", "-", stdin=json.dumps(obj))

    def exec_ok(self, pod: str, *argv: str) -> bool:
        return self.run("exec", pod, "--", *argv, check=False).returncode == 0

    def wait_ready(self, *pods: str) -> None:
        self.run("wait", "--for=condition=Ready", *(f"pod/{p}" for p in pods), "--timeout=300s")


# --- API ----------------------------------------------------------------------------


class Api:
    def __init__(self, base: str, api_key: str, worker_token: str) -> None:
        self._client = httpx.Client(base_url=base, timeout=60.0)
        self._platform = {"X-API-Key": api_key}
        self._worker = {"X-Curie-Worker-Token": worker_token}

    def call(
        self, method: str, path: str, expect: tuple[int, ...], *, worker: bool = False, **kw: Any
    ) -> httpx.Response:
        headers = self._worker if worker else self._platform
        response = self._client.request(method, path, headers=headers, **kw)
        if response.status_code not in expect:
            raise Failure(
                f"{method} {path} answered {response.status_code}, want {expect}: "
                f"{response.text[:400]}"
            )
        return response

    def register(self, run: Run, app: str) -> None:
        body = {"application": run.spec(app), "policy": {"tools": TOOLS, "custody": "platform"}}
        self.call("POST", "/shared-connectors/applications", (201,), json=body)

    def application(self, app: str) -> dict[str, Any]:
        result: dict[str, Any] = self.call(
            "GET", f"/shared-connectors/applications/{app}", (200,)
        ).json()
        return result

    def add_member(self, app: str, agent: str) -> None:
        body = {"aliases": [{"alias": ALIAS, "tools": None}]}
        self.call(
            "PUT", f"/shared-connectors/applications/{app}/members/{agent}", (200,), json=body
        )

    def remove_member(self, app: str, agent: str) -> None:
        self.call("DELETE", f"/shared-connectors/applications/{app}/members/{agent}", (200,))

    def provision(self, app: str) -> None:
        self.call(
            "POST",
            f"/shared-connectors/applications/{app}/database",
            (201,),
            json={"mode": "bundled"},
        )

    def create_agent(self, name: str) -> str:
        body = {
            "name": name,
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex[:9].upper()}"},
        }
        return str(self.call("POST", "/agents", (201,), json=body).json()["id"])

    def deploy(self, agent: str, name: str, label: str, connectors_yaml: str) -> str:
        version = self.call(
            "POST",
            f"/agents/{agent}/versions",
            (201,),
            json={"version_label": label, "created_by": "shared-kind"},
        ).json()["id"]
        self.call(
            "PUT",
            f"/agents/{agent}/versions/{version}/bundle",
            (201,),
            files={"file": ("bundle.tar.gz", bundle(name, connectors_yaml))},
        )
        self.call(
            "POST",
            "/deployments",
            (201,),
            json={"agent_id": agent, "version_id": version, "environment": "dev"},
        )
        return str(version)

    def render(self, run: Run, agent: str, version: str) -> httpx.Response:
        params = {"release": run.release, "namespace": run.namespace, "app_name": run.app_name}
        return self.call(
            "GET", f"/agents/{agent}/versions/{version}/connectors", (200, 409, 422), params=params
        )

    def worker_render(self, run: Run, app: str) -> dict[str, Any]:
        params = {"namespace": run.namespace, "release": run.release}
        result: dict[str, Any] = self.call(
            "GET",
            f"/v1/internal/shared-connectors/applications/{app}/render",
            (200,),
            worker=True,
            params=params,
        ).json()
        return result


def connectors_yaml(app: str | None) -> str:
    lines = ["connectors:", "  docs:", "    url: https://mcp.example.com/mcp"]
    if app is not None:
        lines += [f"  {ALIAS}:", "    shared:", f"      application: {app}", "      contract: 1"]
    return "\n".join(lines) + "\n"


def bundle(name: str, connectors: str) -> bytes:
    files = {
        ".claude-plugin/plugin.json": json.dumps(
            {"name": name, "version": "0.1.0", "description": "shared connector kind proof"}
        ),
        f"skills/{name}/SKILL.md": f"---\nname: {name}\ndescription: t\n---\nhi\n",
        "connectors.yaml": connectors,
    }
    buf = io.BytesIO()
    directories = [name, f"{name}/.claude-plugin", f"{name}/skills", f"{name}/skills/{name}"]
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for directory in directories:
            entry = tarfile.TarInfo(directory)
            entry.type = tarfile.DIRTYPE
            entry.mode = 0o755
            archive.addfile(entry)
        for path, text in files.items():
            data = text.encode()
            info = tarfile.TarInfo(f"{name}/{path}")
            info.size = len(data)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))
    return buf.getvalue()


# --- comparisons ---------------------------------------------------------------------


def field_diff(want: Any, live: Any, path: str = "") -> list[str]:
    """Every field ``want`` sets that ``live`` does not hold with the same value.

    Fields the API server defaults are ignored; lists must match in length and
    order. Resource quantities compare by value, because the API server
    canonicalizes them (1000m reads back as 1). A desired ``False`` or ``[]``
    whose key the live object lacks is equal: the server drops those zero
    values for non-pointer omitempty fields. In the frozen render that is
    VolumeMount.readOnly (the proxy's resolver mount) and NetworkPolicy
    spec.ingress (the deny policy); every other ``False`` there is a pointer
    field and reads back, and emptyDir ``{}`` reads back too, so nothing
    broader is excused.
    """

    if isinstance(want, dict):
        if not isinstance(live, dict):
            return [f"{path}: want a mapping, live {type(live).__name__}"]
        out: list[str] = []
        for key, value in want.items():
            if key not in live:
                if value is False or (isinstance(value, list) and not value):
                    continue
                out.append(f"{path}.{key}: missing")
            else:
                out += field_diff(value, live[key], f"{path}.{key}")
        return out
    if isinstance(want, list):
        if not isinstance(live, list) or len(live) != len(want):
            return [f"{path}: want {len(want)} items, live {live!r:.200}"]
        out = []
        for index, (a, b) in enumerate(zip(want, live, strict=True)):
            out += field_diff(a, b, f"{path}[{index}]")
        return out
    if want == live:
        return []
    if ".resources." in path and isinstance(want, str) and isinstance(live, str):
        if parse_quantity(want) is not None and parse_quantity(want) == parse_quantity(live):
            return []
    return [f"{path}: want {want!r}, live {live!r}"]


def applied_by_reconciler(live: dict[str, Any]) -> bool:
    return any(
        entry.get("manager") == FIELD_MANAGER and entry.get("operation") == "Apply"
        for entry in live["metadata"].get("managedFields") or []
    )


def expect_applied(kube: Kube, desired: dict[str, Any], label: str) -> dict[str, Any]:
    live = kube.get(desired["kind"].lower(), desired["metadata"]["name"], managed_fields=True)
    if live is None:
        raise Failure(f"{label}: {desired['kind']}/{desired['metadata']['name']} was not applied")
    diffs = field_diff({k: desired[k] for k in ("metadata", "spec")}, live)
    if diffs:
        raise Failure(f"{label}: differs from the frozen render: {diffs[:5]}")
    if OWNER_LABEL in (live["metadata"].get("labels") or {}):
        raise Failure(f"{label}: carries the dedicated owner label {OWNER_LABEL}")
    if not applied_by_reconciler(live):
        raise Failure(f"{label}: not applied by field manager {FIELD_MANAGER}")
    ok(label)
    return live


def reach_selector(run: Run, agent: str) -> str:
    return (
        f"app.kubernetes.io/component={COMPONENT},"
        f"app.kubernetes.io/part-of={run.release},{AGENT_LABEL}={agent}"
    )


def app_objects(run: Run, app: str) -> list[dict[str, Any]]:
    return render_application(run.model(app), caller_public_keys=run.caller_keys)


def secret_labels(run: Run, app: str, role: str) -> dict[str, str]:
    return {
        APP_LABEL: app,
        ROLE_LABEL: role,
        "app.kubernetes.io/component": COMPONENT,
        "app.kubernetes.io/part-of": run.release,
    }


def expect_secret(
    run: Run, kube: Kube, app: str, role: str, data_key: str, label: str
) -> tuple[dict[str, Any], bytes]:
    """The worker-managed Secret in its exact shape; returns it and its decoded value."""

    prefix = "shared-key-" if role == "encryption-key" else "shared-dsn-"
    live = kube.get("secret", f"{prefix}{app}")
    if live is None:
        raise Failure(f"{label}: Secret {prefix}{app} does not exist")
    labels = live["metadata"].get("labels") or {}
    if labels != secret_labels(run, app, role):
        raise Failure(f"{label}: labels are {labels!r}")
    if live.get("type") != "Opaque" or set(live.get("data") or {}) != {data_key}:
        raise Failure(
            f"{label}: type {live.get('type')!r} with keys {sorted(live.get('data') or {})}"
        )
    ok(label)
    return live, base64.b64decode(live["data"][data_key])


def key_ring(content: bytes, label: str) -> nacl.public.PrivateKey:
    """The current private key of a key Secret, parsed by the resolver's own parser."""

    text = content.decode("utf-8")
    try:
        KeyRing.from_text(text)
        first = next(line.strip() for line in text.splitlines() if line.strip())
        key = nacl.public.PrivateKey(strict_b64decode(first))
    except (ValueError, StopIteration) as exc:
        raise Failure(
            f"{label}: the key Secret does not parse as a key ring ({type(exc).__name__})"
        ) from None
    return key


def recipient_of(key: nacl.public.PrivateKey) -> str:
    return hashlib.sha256(bytes(key.public_key)).hexdigest()


def current_recipients(api: Api, app: str) -> list[str]:
    return [r["recipient_id"] for r in api.application(app)["recipients"] if r["current"]]


def rv(obj: dict[str, Any]) -> str:
    return str(obj["metadata"]["resourceVersion"])


# --- processes ----------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


@contextlib.contextmanager
def api_process(run: Run, env_vars: dict[str, str]) -> Iterator[str]:
    port = free_port()
    log_path = run.work / "api.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "curie_api.main:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=REPO_ROOT,
            env=env_vars,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    (run.work / "api.pid").write_text(str(process.pid))
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 120
        while True:
            if process.poll() is not None:
                raise Failure(f"the API exited with {process.returncode}: {tail(log_path)}")
            with contextlib.suppress(httpx.HTTPError):
                if httpx.get(f"{base}/health", timeout=2).status_code == 200:
                    break
            if time.monotonic() > deadline:
                raise Failure(f"the API did not answer /health within 120s: {tail(log_path)}")
            time.sleep(1)
        yield base
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        (run.work / "api.pid").unlink(missing_ok=True)


def tail(path: Path, lines: int = 25) -> str:
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


class PassLog(logging.Handler):
    """Every record the loop logs, so a pass's refusal codes can be read back."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def worker_kubeconfig(run: Run, kube: Kube) -> Path:
    token = kube.run("create", "token", run.worker_sa, "--duration=2h").stdout.strip()
    view = json.loads(
        kube.run(
            "config", "view", "--raw", "--minify", "--flatten", "-o", "json", namespaced=False
        ).stdout
    )
    cluster = view["clusters"][0]["cluster"]
    config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": "sck", "cluster": cluster}],
        "users": [{"name": "sck-worker", "user": {"token": token}}],
        "contexts": [
            {
                "name": "sck-worker",
                "context": {"cluster": "sck", "user": "sck-worker", "namespace": run.namespace},
            }
        ],
        "current-context": "sck-worker",
    }
    path = run.work / "worker-kubeconfig"
    path.write_text(yaml.safe_dump(config))
    path.chmod(0o600)
    return path


# --- network probes ----------------------------------------------------------------------


def probe_pod(
    run: Run,
    name: str,
    labels: dict[str, str],
    port: int | None = None,
    server_port: int | None = None,
) -> dict[str, Any]:
    listeners = [p for p in (port, server_port) if p]
    container: dict[str, Any] = {
        "name": "probe",
        "image": run.image,
        "imagePullPolicy": "IfNotPresent",
        "command": [
            "sh",
            "-c",
            "".join(f"httpd -p {p} -h /tmp; " for p in listeners) + "exec sleep 36000",
        ],
    }
    if port:
        # Named like the frozen proxy container port, which the Service targets.
        container["ports"] = [{"name": "proxy", "containerPort": port, "protocol": "TCP"}]
        container["readinessProbe"] = {"tcpSocket": {"port": port}, "periodSeconds": 2}
    if server_port:
        # Named like the frozen server container port, which no Service targets.
        container.setdefault("ports", []).append(
            {"name": "server", "containerPort": server_port, "protocol": "TCP"}
        )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": run.namespace, "labels": labels},
        "spec": {"terminationGracePeriodSeconds": 0, "containers": [container]},
    }


def connects(kube: Kube, pod: str, ip: str, port: int) -> bool:
    return kube.exec_ok(pod, "nc", "-z", "-w", "3", ip, str(port))


def expect_reach(kube: Kube, label: str, pod: str, ip: str, port: int) -> None:
    for _ in range(20):
        if connects(kube, pod, ip, port):
            ok(label)
            return
        time.sleep(2)
    raise Failure(f"{label}: {pod} never reached {ip}:{port}")


def expect_refused(
    kube: Kube, label: str, pod: str, ip: str, port: int, *, settle: bool = False
) -> None:
    """Three refused attempts in a row.

    ``settle`` is for a refusal that follows a policy deletion: the CNI may
    still forward for a moment, so up to 60s is allowed for the first refusal.
    Without it any connection fails the check at once.
    """

    if settle:
        deadline = time.monotonic() + 60
        while connects(kube, pod, ip, port):
            if time.monotonic() > deadline:
                raise Failure(f"{label}: {pod} still reaches {ip}:{port} 60s after the change")
            time.sleep(2)
    for _ in range(3):
        if connects(kube, pod, ip, port):
            raise Failure(f"{label}: {pod} reached {ip}:{port}")
    ok(label)


def pod_ip(kube: Kube, name: str) -> str:
    ip = str(kube.must("pod", name)["status"].get("podIP") or "")
    if not ip:
        raise Failure(f"pod {name} has no IP")
    return ip


# --- the scenario ------------------------------------------------------------------------


@dataclass
class Agents:
    ids: dict[str, str] = field(default_factory=dict)
    versions: dict[str, str] = field(default_factory=dict)


async def run_pass(loop: Any, log: PassLog, label: str) -> tuple[Any, list[str]]:
    start = len(log.lines)
    summary = await loop.one_pass()
    lines = log.lines[start:]
    print(
        f"  {label}: applied={summary.applied} deleted={summary.deleted} "
        f"skipped={summary.skipped} failed={summary.failed}",
        flush=True,
    )
    return summary, lines


async def scenario(run: Run, kube: Kube, api: Api, loop: Any, log: PassLog, agents: Agents) -> None:
    app1, app2, app3 = run.apps
    spec1 = run.model(app1)
    objects1 = render_application(spec1, caller_public_keys=run.caller_keys)
    by_kind = {(o["kind"], o["metadata"]["name"].split("-")[1]): o for o in objects1}
    deployment = by_kind[("Deployment", "app")]
    service = by_kind[("Service", "service")]

    # -- the worker identity and its admission ------------------------------------------
    worker_cfg = str(run.work / "worker-kubeconfig")
    worker = ["kubectl", "--kubeconfig", worker_cfg, "--context", "sck-worker", "-n", run.namespace]

    def can_i(verb: str) -> bool:
        done = subprocess.run(
            [*worker, "auth", "can-i", verb, "secrets"], capture_output=True, text=True, check=False
        )
        return done.stdout.strip() == "yes"

    check(
        can_i("list") and can_i("create") and not can_i("get"),
        "the loop's kubeconfig is the worker ServiceAccount (list and create Secrets, never get)",
    )
    bad = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": f"shared-key-{app1}",
            "namespace": run.namespace,
            "labels": secret_labels(run, app1, "encryption-key"),
        },
        "data": {KEY_DATA: b64(b"x"), "extra": b64(b"x")},
    }
    refused = subprocess.run(
        [*worker, "create", "--dry-run=server", "-f", "-"],
        input=json.dumps(bad),
        capture_output=True,
        text=True,
        check=False,
    )
    check(
        refused.returncode != 0
        and f"ValidatingAdmissionPolicy '{run.secrets_policy}'" in refused.stderr,
        "the worker token is subject to the worker-secrets admission policy",
        refused.stderr[:300],
    )

    # -- phase 2/3: application 1, members A and B, pass 1 ------------------------------
    api.register(run, app1)
    for letter in ("a", "b"):
        agent = api.create_agent(f"sck-{letter}")
        agents.ids[letter] = agent
        api.add_member(app1, agent)
        agents.versions[letter] = api.deploy(agent, f"sck-{letter}", "v1", connectors_yaml(app1))
    ok(
        "application 1 registered; agents A and B are members and deployed a bundle "
        "declaring the shared alias plus a remote connector"
    )

    for letter in ("a", "b"):
        response = api.render(run, agents.ids[letter], agents.versions[letter])
        body = response.json()
        want = render_member_reach(spec1, run.member(agents.ids[letter]))
        shared = body.get("shared") if response.status_code == 200 else None
        check(
            response.status_code == 200
            and isinstance(shared, list)
            and len(shared) == 1
            and shared[0].get("name") == ALIAS
            and shared[0].get("application") == app1
            and canonical(shared[0].get("reach")) == canonical(want),
            f"the render route lists {letter.upper()}'s reach byte-equal to render_member_reach",
            response.text[:300],
        )

    summary, _ = await run_pass(loop, log, "pass 1")
    check(summary.failed == 0, "pass 1 reports no failure")
    _, key_content = expect_secret(
        run,
        kube,
        app1,
        "encryption-key",
        KEY_DATA,
        "pass 1 created shared-key-<app1> in its exact shape",
    )
    key1 = key_ring(key_content, "pass 1 key Secret")
    ok("the key Secret parses with the resolver's KeyRing.from_text")
    check(
        current_recipients(api, app1) == [recipient_of(key1)],
        "the API's current recipient is sha256 of the public key derived from the key Secret",
    )
    for letter in ("a", "b"):
        for desired in render_member_reach(spec1, run.member(agents.ids[letter])):
            role = desired["metadata"]["name"].rsplit("-", 1)[0].removeprefix("shared-")
            expect_applied(kube, desired, f"pass 1 applied {letter.upper()}'s {role} reach")
    check(
        kube.get("secret", f"shared-dsn-{app1}") is None,
        "no DSN Secret before the database is provisioned",
    )
    check(
        kube.get("deployment", deployment["metadata"]["name"]) is None,
        "no Deployment while the database is unprovisioned (pending)",
    )

    # -- phase 4: provision, pass 2 and pass 3 ------------------------------------------
    api.provision(app1)
    summary, _ = await run_pass(loop, log, "pass 2")
    check(summary.failed == 0, "pass 2 reports no failure")
    plan = api.worker_render(run, app1)
    check(
        canonical(plan.get("objects")) == canonical(objects1),
        "the worker render route's objects equal render_application byte for byte",
    )
    containers = {
        c["name"]: {e["name"]: e.get("value") for e in c.get("env") or []}
        for c in deployment["spec"]["template"]["spec"]["containers"]
    }
    check(
        containers["proxy"].get("CURIE_CALLER_PROXY_PUBLIC_KEYS") == ",".join(run.caller_keys)
        and not [name for name in containers["server"] if name.startswith("CURIE_CALLER_")],
        "the render carries the API's caller verify key to the proxy container only",
    )
    sealed = (plan.get("secrets") or {}).get("database", {}).get("sealed_dsn")
    dsn_live, dsn_content = expect_secret(
        run,
        kube,
        app1,
        "database-dsn",
        DSN_DATA,
        "pass 2 created shared-dsn-<app1> in its exact shape",
    )
    check(
        isinstance(sealed, str) and dsn_content.decode().strip() == sealed,
        "the DSN Secret holds exactly the sealed DSN the worker route plans",
    )
    try:
        opened = nacl.public.SealedBox(key1).decrypt(base64.b64decode(sealed or ""))
    except (ValueError, nacl.exceptions.CryptoError):
        opened = b""
    check(opened.startswith(b"postgres"), "the sealed DSN opens with the key Secret's private key")
    live: dict[str, dict[str, Any]] = {}
    for desired in objects1:
        live[desired["metadata"]["name"]] = expect_applied(
            kube,
            desired,
            f"pass 2 applied {desired['kind']} {desired['metadata']['name']} as rendered",
        )

    key_live = kube.must("secret", f"shared-key-{app1}")
    grant_before = kube.must("secret", run.grant_secret)
    generation = live[deployment["metadata"]["name"]]["metadata"]["generation"]
    summary, _ = await run_pass(loop, log, "pass 3")
    check(summary.failed == 0, "pass 3 reports no failure")
    check(
        rv(kube.must("secret", f"shared-key-{app1}")) == rv(key_live)
        and rv(kube.must("secret", f"shared-dsn-{app1}")) == rv(dsn_live),
        "pass 3 left both worker-managed Secrets unchanged (resourceVersion)",
    )
    check(
        rv(kube.must("secret", run.grant_secret)) == rv(grant_before),
        "the operator grant-store Secret is never written",
    )
    check(
        kube.must("deployment", deployment["metadata"]["name"])["metadata"]["generation"]
        == generation,
        "pass 3 did not roll the Deployment",
    )

    # -- phase 5: network probes ------------------------------------------------------
    template_labels = deployment["spec"]["template"]["metadata"]["labels"]
    outsider = str(uuid.uuid4())
    pods = {
        "sck-shared-app": probe_pod(
            run, "sck-shared-app", template_labels, PROXY_PORT, spec1.server_port
        ),
        "sck-runner-a": probe_pod(run, "sck-runner-a", run.runner_labels(agents.ids["a"])),
        "sck-runner-b": probe_pod(run, "sck-runner-b", run.runner_labels(agents.ids["b"])),
        "sck-runner-outsider": probe_pod(run, "sck-runner-outsider", run.runner_labels(outsider)),
    }
    for pod in pods.values():
        kube.apply(pod)
    kube.wait_ready(*pods)
    ok("probe pods carry the exact frozen application and member runner labels")
    service_ip = str(kube.must("service", service["metadata"]["name"])["spec"]["clusterIP"])
    ips = {
        name: pod_ip(kube, name)
        for name in (
            "sck-postgres",
            "sck-db-target-alt",
            "sck-db-unconfigured",
            "sck-valkey",
            "sck-provider",
            "sck-shared-app",
        )
    }

    expect_reach(
        kube, "member runner A reaches the shared Service", "sck-runner-a", service_ip, PROXY_PORT
    )
    expect_reach(
        kube, "member runner B reaches the shared Service", "sck-runner-b", service_ip, PROXY_PORT
    )
    expect_refused(
        kube,
        "a non-member runner is refused at the shared Service",
        "sck-runner-outsider",
        service_ip,
        PROXY_PORT,
    )
    expect_refused(
        kube,
        "the generic mcp-connector Pod is refused at the shared Service",
        "sck-generic-connector",
        service_ip,
        PROXY_PORT,
    )
    expect_refused(
        kube,
        "a namespace peer is refused at the shared Service",
        "sck-peer",
        service_ip,
        PROXY_PORT,
    )
    expect_refused(
        kube,
        "a namespace peer is refused at the shared Pod itself",
        "sck-peer",
        ips["sck-shared-app"],
        PROXY_PORT,
    )

    resolved = False
    for _ in range(10):
        if kube.exec_ok("sck-shared-app", "nslookup", "kubernetes.default.svc.cluster.local"):
            resolved = True
            break
        time.sleep(2)
    check(resolved, "the shared Pod resolves DNS (shared DNS root policy)")
    expect_reach(
        kube,
        "the shared Pod reaches the in-cluster Postgres on 5432",
        "sck-shared-app",
        ips["sck-postgres"],
        5432,
    )
    expect_refused(
        kube,
        "the generic mcp-connector Pod is refused at Postgres (data-tier ingress)",
        "sck-generic-connector",
        ips["sck-postgres"],
        5432,
    )
    expect_reach(
        kube,
        "the shared Pod reaches the Valkey grant store on 6379",
        "sck-shared-app",
        ips["sck-valkey"],
        6379,
    )
    expect_reach(
        kube,
        "the shared Pod reaches its configured provider port",
        "sck-shared-app",
        ips["sck-provider"],
        8443,
    )
    expect_reach(
        kube,
        "the shared Pod reaches the database target's 5432",
        "sck-shared-app",
        ips["sck-db-target-alt"],
        5432,
    )
    for target, port, what in (
        ("sck-db-target-alt", 15432, "the database target on an unconfigured port"),
        ("sck-db-unconfigured", 5432, "an unconfigured database target"),
        ("sck-provider", 9443, "the provider on an unconfigured port"),
    ):
        expect_reach(
            kube, f"control: a namespace peer reaches {what}", "sck-peer", ips[target], port
        )
        expect_refused(
            kube, f"the shared Pod is refused at {what}", "sck-shared-app", ips[target], port
        )

    # -- phase 5b: the GC race, ingress isolation held by the chart's root policy ----------
    # Another reconciler's GC can delete an application's own policies while its
    # Pods still run. The chart's <fullname>-shared-app-deny-ingress keeps every
    # shared application Pod ingress-isolated regardless, so a namespace peer is
    # refused even with the application's deny and every member ingress gone.
    server_port = spec1.server_port
    root_deny = kube.get("networkpolicy", f"{run.fullname}-shared-app-deny-ingress")
    check(
        root_deny is not None
        and root_deny["spec"].get("policyTypes") == ["Ingress"]
        and not root_deny["spec"].get("ingress"),
        "the chart's root deny-ingress policy for shared application Pods is applied",
    )
    check(
        kube.exec_ok("sck-shared-app", "nc", "-z", "-w", "3", "127.0.0.1", str(server_port)),
        "control: the shared Pod listens on its server container port",
    )
    app_deny = by_kind[("NetworkPolicy", "deny")]["metadata"]["name"]
    kube.run("delete", "networkpolicy", app_deny, "--wait=true")
    check(kube.get("networkpolicy", app_deny) is None, "the application's deny policy is deleted")
    for target, port, what in (
        (service_ip, PROXY_PORT, "the shared Service port"),
        (ips["sck-shared-app"], PROXY_PORT, "the shared Pod's proxy port"),
        (ips["sck-shared-app"], server_port, "the shared Pod's server container port"),
    ):
        expect_refused(
            kube,
            f"application deny deleted: a namespace peer is refused at {what}",
            "sck-peer",
            target,
            port,
        )
    expect_reach(
        kube,
        "application deny deleted: member runner A still reaches the shared Service",
        "sck-runner-a",
        service_ip,
        PROXY_PORT,
    )
    # The GC race proper: no member ingress either, only the chart's root policy.
    member_ingress = [
        item["metadata"]["name"]
        for letter in ("a", "b")
        for item in kube.list("networkpolicy", reach_selector(run, agents.ids[letter]))
        if "Ingress" in (item["spec"].get("policyTypes") or [])
    ]
    check(len(member_ingress) == 2, "each member has one reach ingress policy", str(member_ingress))
    for name in member_ingress:
        kube.run("delete", "networkpolicy", name, "--wait=true")
    for target, port, what in (
        (service_ip, PROXY_PORT, "the shared Service port"),
        (ips["sck-shared-app"], PROXY_PORT, "the shared Pod's proxy port"),
        (ips["sck-shared-app"], server_port, "the shared Pod's server container port"),
    ):
        expect_refused(
            kube,
            f"no application or member ingress policy: a namespace peer is refused at {what}",
            "sck-peer",
            target,
            port,
            settle=True,
        )
    summary, _ = await run_pass(loop, log, "pass 3b (restores the deleted policies)")
    check(summary.failed == 0, "pass 3b reports no failure")
    expect_applied(
        kube, by_kind[("NetworkPolicy", "deny")], "pass 3b restored the application deny"
    )
    for letter in ("a", "b"):
        for desired in render_member_reach(spec1, run.member(agents.ids[letter])):
            role = desired["metadata"]["name"].rsplit("-", 1)[0].removeprefix("shared-")
            expect_applied(kube, desired, f"pass 3b restored {letter.upper()}'s {role} reach")
    for letter in ("a", "b"):
        expect_reach(
            kube,
            f"after pass 3b member runner {letter.upper()} reaches the shared Service again",
            f"sck-runner-{letter}",
            service_ip,
            PROXY_PORT,
        )
    expect_refused(
        kube,
        "after pass 3b a namespace peer is still refused at the shared Pod's server port",
        "sck-peer",
        ips["sck-shared-app"],
        server_port,
    )

    # -- phase 6: adoption -----------------------------------------------------------
    api.register(run, app2)
    api.register(run, app3)
    adopted_key = nacl.public.PrivateKey.generate()
    for app, content in (
        (app2, b64(bytes(adopted_key)) + "\n"),
        (app3, "not-a-key\n"),
    ):
        kube.create(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "type": "Opaque",
                "metadata": {
                    "name": f"shared-key-{app}",
                    "namespace": run.namespace,
                    "labels": secret_labels(run, app, "encryption-key"),
                },
                "data": {KEY_DATA: b64(content.encode())},
            }
        )
    before = {app: kube.must("secret", f"shared-key-{app}") for app in (app2, app3)}
    for letter, app in (("c", app2), ("d", app3)):
        agent = api.create_agent(f"sck-{letter}")
        agents.ids[letter] = agent
        api.add_member(app, agent)
        agents.versions[letter] = api.deploy(agent, f"sck-{letter}", "v1", connectors_yaml(app))
        response = api.render(run, agent, agents.versions[letter])
        shared = response.json().get("shared") if response.status_code == 200 else None
        check(
            isinstance(shared, list) and [s.get("application") for s in shared] == [app],
            f"agent {letter.upper()} declares its application before the adoption pass",
            response.text[:300],
        )
    summary, lines = await run_pass(loop, log, "pass 4 (adoption)")
    check(
        current_recipients(api, app2) == [recipient_of(adopted_key)],
        "application 2's pre-existing key Secret is adopted and its public key registered",
    )
    check(
        rv(kube.must("secret", f"shared-key-{app2}")) == rv(before[app2]),
        "the adopted key Secret is untouched",
    )
    check(
        any("shared_key_secret_invalid" in line for line in lines),
        "application 3's malformed key Secret is refused with shared_key_secret_invalid",
    )
    check(
        rv(kube.must("secret", f"shared-key-{app3}")) == rv(before[app3]),
        "the malformed key Secret is never patched",
    )
    check(current_recipients(api, app3) == [], "no recipient is registered for application 3")
    deployment3 = next(o for o in app_objects(run, app3) if o["kind"] == "Deployment")
    check(
        kube.get("deployment", deployment3["metadata"]["name"]) is None,
        "no Deployment is applied for application 3",
    )
    check(summary.failed >= 1, "the refusal reaches the pass outcome (failed >= 1)")

    # -- phase 7: remove one ------------------------------------------------------------
    survivors = {
        name: kube.must(obj["kind"].lower(), name)
        for name, obj in ((o["metadata"]["name"], o) for o in objects1)
    }
    reach_a = {
        o["metadata"]["name"]: o["metadata"]["uid"]
        for o in kube.list("networkpolicy", reach_selector(run, agents.ids["a"]))
    }
    template_hash = hashlib.sha256(
        canonical(survivors[deployment["metadata"]["name"]]["spec"]["template"]).encode()
    ).hexdigest()
    secrets_before = {
        name: rv(kube.must("secret", name)) for name in (f"shared-key-{app1}", f"shared-dsn-{app1}")
    }

    def survivors_intact(label: str) -> None:
        for name, before_obj in survivors.items():
            now = kube.get(before_obj["kind"].lower(), name)
            if now is None or now["metadata"]["uid"] != before_obj["metadata"]["uid"]:
                raise Failure(f"{label}: {before_obj['kind']} {name} was deleted or replaced")
        live_dep = kube.must("deployment", deployment["metadata"]["name"])
        if (
            live_dep["metadata"]["generation"]
            != survivors[deployment["metadata"]["name"]]["metadata"]["generation"]
        ):
            raise Failure(f"{label}: the Deployment generation changed")
        if (
            hashlib.sha256(canonical(live_dep["spec"]["template"]).encode()).hexdigest()
            != template_hash
        ):
            raise Failure(f"{label}: the Deployment pod template changed")
        now_a = {
            o["metadata"]["name"]: o["metadata"]["uid"]
            for o in kube.list("networkpolicy", reach_selector(run, agents.ids["a"]))
        }
        if now_a != reach_a:
            raise Failure(f"{label}: A's reach changed: {sorted(now_a)}")
        for name, version in secrets_before.items():
            if rv(kube.must("secret", name)) != version:
                raise Failure(f"{label}: Secret {name} changed")
        ok(
            f"{label}: A's reach, the Deployment (same generation and template), Service, "
            "policies and both Secrets survive"
        )

    api.remove_member(app1, agents.ids["b"])
    await run_pass(loop, log, "pass 5 (B no longer a member)")
    check(
        kube.list("networkpolicy", reach_selector(run, agents.ids["b"])) == [],
        "removing B's membership deletes only B's reach",
    )
    survivors_intact("after removing B's membership")
    expect_refused(
        kube,
        "B's runner is now refused at the shared Service",
        "sck-runner-b",
        service_ip,
        PROXY_PORT,
        settle=True,
    )
    expect_reach(
        kube, "A's runner still reaches the shared Service", "sck-runner-a", service_ip, PROXY_PORT
    )

    api.deploy(agents.ids["b"], "sck-b", "v2", connectors_yaml(None))
    await run_pass(loop, log, "pass 6 (B's bundle drops the alias)")
    check(
        kube.list("networkpolicy", reach_selector(run, agents.ids["b"])) == [],
        "B's bundle without the alias leaves B without reach",
    )
    survivors_intact("after B's bundle dropped the alias")

    api.deploy(agents.ids["a"], "sck-a", "v2", connectors_yaml(None))
    await run_pass(loop, log, "pass 7 (A's bundle drops the alias)")
    check(
        kube.list("networkpolicy", reach_selector(run, agents.ids["a"])) == [],
        "A's reach is deleted once A no longer declares the application",
    )
    # The Deployment is deleted with foreground propagation, so it stays listed
    # (with a deletionTimestamp) until its ReplicaSets and Pods are gone, and
    # the application's ingress deny policy must outlive it. Later passes
    # finish the cleanup; every observation while the Deployment is listed
    # must still find the deny policy.
    deployment_name = deployment["metadata"]["name"]
    deny_name = by_kind[("NetworkPolicy", "deny")]["metadata"]["name"]
    listed_with_deny: list[bool] = []
    for number in range(30):
        still = kube.get("deployment", deployment_name)
        if still is not None:
            listed_with_deny.append(kube.get("networkpolicy", deny_name) is not None)
            terminating = still["metadata"].get("deletionTimestamp") is not None
            check(
                not terminating
                or "foregroundDeletion" in (still["metadata"].get("finalizers") or []),
                "a terminating application Deployment was deleted with foreground propagation",
                str(still["metadata"].get("finalizers")),
            )
        remaining = [
            name
            for name, obj in survivors.items()
            if kube.get(obj["kind"].lower(), name) is not None
        ]
        if not remaining:
            break
        time.sleep(2)
        await run_pass(loop, log, f"pass 7.{number + 1} (application cleanup)")
    check(
        all(listed_with_deny),
        "the application's deny ingress policy exists whenever its Deployment is still listed",
        f"{len(listed_with_deny)} observation(s): {listed_with_deny}",
    )
    gone = [
        name for name, obj in survivors.items() if kube.get(obj["kind"].lower(), name) is not None
    ]
    check(gone == [], "the last declaration removed deletes the application objects", str(gone))
    check(
        kube.get("deployment", deployment_name) is None
        and kube.get("networkpolicy", deny_name) is None,
        "the application Deployment and its deny ingress policy are both gone at the end",
    )
    check(
        all(rv(kube.must("secret", name)) == version for name, version in secrets_before.items()),
        "the key and DSN Secrets outlive the application objects, unchanged",
    )


# --- status fixtures (optional) ------------------------------------------------------------


def record_status_fixtures(run: Run, kube: Kube, out_dir: Path) -> None:
    """Record available, progressing and deadline-exceeded Deployment status shapes."""

    server = json.loads(kube.run("version", "-o", "json", namespaced=False).stdout)[
        "serverVersion"
    ]["gitVersion"]
    out_dir.mkdir(parents=True, exist_ok=True)

    def stand_in(name: str, ready: bool, deadline: int) -> dict[str, Any]:
        labels = {"app": name}
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": run.namespace, "labels": labels},
            "spec": {
                "replicas": 1,
                "progressDeadlineSeconds": deadline,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "terminationGracePeriodSeconds": 0,
                        "containers": [
                            {
                                "name": "c",
                                "image": run.image,
                                "command": ["sleep", "36000"],
                                "readinessProbe": {
                                    "exec": {"command": ["true" if ready else "false"]},
                                    "periodSeconds": 2,
                                },
                            }
                        ],
                    },
                },
            },
        }

    def wait_for(name: str, predicate: Any, timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            live = kube.must("deployment", name)
            conditions = {c["type"]: c for c in live.get("status", {}).get("conditions") or []}
            if predicate(live, conditions):
                return live
            time.sleep(2)
        raise Failure(f"status fixture {name}: the expected status never appeared")

    shapes = {
        "available": (
            stand_in("sck-status-available", True, 600),
            lambda d, c: (
                c.get("Available", {}).get("status") == "True"
                and d["status"].get("observedGeneration") == d["metadata"]["generation"]
            ),
        ),
        "progressing": (
            stand_in("sck-status-progressing", False, 600),
            lambda d, c: (
                c.get("Available", {}).get("status") == "False"
                and c.get("Progressing", {}).get("status") == "True"
                and d["status"].get("replicas") == 1
            ),
        ),
        "deadline_exceeded": (
            stand_in("sck-status-deadline", False, 10),
            lambda d, c: c.get("Progressing", {}).get("reason") == "ProgressDeadlineExceeded",
        ),
    }
    for shape, (obj, predicate) in shapes.items():
        kube.apply(obj)
        live = wait_for(obj["metadata"]["name"], predicate, 240)
        live["metadata"].pop("managedFields", None)
        live["_source"] = {
            "command": f"kubectl get deployment {obj['metadata']['name']} -o json",
            "server_version": server,
            "recorded": datetime.now(UTC).date().isoformat(),
            "by": "charts/curie/ci/runtime/shared-connector-kind-driver.py",
        }
        (out_dir / f"{shape}.json").write_text(json.dumps(live, indent=2, sort_keys=True) + "\n")
        kube.run("delete", "deployment", obj["metadata"]["name"], "--wait=false")
        ok(f"recorded the {shape} Deployment status shape from {server}")


# --- entry point --------------------------------------------------------------------------


async def main() -> None:
    run = Run()
    kube = Kube(run.kubeconfig, run.context, run.namespace)
    if len(run.apps) != 3:
        raise Failure("SCK_APPS must name three application UUIDs")

    api_key = secrets.token_hex(24)
    worker_token = secrets.token_hex(24)
    caller = nacl.signing.SigningKey.generate()
    caller_public = b64(bytes(caller.verify_key))
    run.caller_keys = (caller_public,)
    stores = {
        "DATABASE_URL": run.database_url(),
        "VALKEY_HOST": "127.0.0.1",
        "VALKEY_PORT": env("SCK_VALKEY_PORT"),
        "VALKEY_PASSWORD": env("SCK_VALKEY_PASSWORD"),
        "S3_ENDPOINT_URL": f"http://127.0.0.1:{env('SCK_S3_PORT')}",
        "S3_ACCESS_KEY": env("SCK_S3_ACCESS"),
        "S3_SECRET_KEY": env("SCK_S3_SECRET"),
    }
    api_env = {
        **os.environ,
        **stores,
        "ENVIRONMENT": "dev",
        "API_KEY": api_key,
        "CURIE_INTERNAL_WORKER_TOKEN": worker_token,
        "CURIE_INSTALLATION_ID": run.installation,
        "CURIE_INSTALLATION_ID_OBSERVED": "true",
        "CURIE_CONNECTOR_CALLER_PUBLIC_KEY": caller_public,
        "CURIE_CONNECTOR_PROXY_IMAGE": run.image,
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": secrets.token_hex(24),
        "CURIE_REMEDIATION_AUTHORITY_KEY": secrets.token_hex(24),
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
    }
    migrate = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=REPO_ROOT / "apps" / "api",
        env=api_env,
        capture_output=True,
        text=True,
        check=False,
    )
    if migrate.returncode:
        raise Failure(f"alembic upgrade head failed: {migrate.stderr[-800:]}")
    ok("alembic migrated the run's Postgres to head")

    worker_kubeconfig(run, kube)
    with api_process(run, api_env) as base:
        ok("the API (uvicorn curie_api.main:create_app) answers /health")
        api = Api(base, api_key, worker_token)

        os.environ.update(
            {
                **stores,
                "KUBECONFIG": str(run.work / "worker-kubeconfig"),
                "CURIE_CONNECTOR_RECONCILE": "true",
                "CURIE_CONNECTOR_RECONCILE_INTERVAL_S": "1",
                "CURIE_NAMESPACE": run.namespace,
                "CURIE_RELEASE": run.release,
                "CURIE_CONNECTOR_APP_NAME": run.app_name,
                "CURIE_API_URL": base,
                "CURIE_API_KEY": api_key,
                "CURIE_INTERNAL_WORKER_TOKEN": worker_token,
                "CURIE_INSTALLATION_ID": run.installation,
            }
        )
        from curie_worker import run as worker_run
        from curie_worker.config import WorkerConfig
        from sqlalchemy.ext.asyncio import create_async_engine

        config = WorkerConfig()
        engine = create_async_engine(config.database_url)
        log = PassLog()
        logging.getLogger().addHandler(log)
        logging.getLogger().setLevel(logging.INFO)
        file_log = logging.FileHandler(run.work / "worker.log")
        logging.getLogger().addHandler(file_log)
        try:
            loop = worker_run._build_connector_loop(config, engine)
            if loop is None:
                raise Failure("run._build_connector_loop returned no loop with the reconciler on")
            ok("the production connector loop is built by run._build_connector_loop")
            await scenario(run, kube, api, loop, log, Agents())
            fixtures = os.environ.get("SCK_RECORD_STATUS_FIXTURES", "")
            if fixtures:
                record_status_fixtures(run, kube, Path(fixtures))
        finally:
            await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Failure as failure:
        print(f"FAIL {failure}", file=sys.stderr, flush=True)
        sys.exit(1)
    print(f"driver: {PASSED} checks passed", flush=True)
