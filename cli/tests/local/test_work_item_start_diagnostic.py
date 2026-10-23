"""Local fix pin for #4930: a pre-start archive diagnostic survives capacity waits.

A factory request whose workspace archive is too large fails before it starts.
The worker logs a bounded, redacted diagnostic, and the request goes back to
waiting. Before #4930 the next capacity deferral overwrote
``last_deferral_reason`` and the archive failure was gone from every read.

This drives the real path on an owned kind cluster with the real chart: API,
worker, Postgres, Valkey, the object store and the agent-sandbox controller.
The API, worker and runner images are built from this checkout with per-run
tags. Only two seams are substituted, both outside the changed path: the
GitHub stub forge (``tools/github-stub``) and a test-owned model endpoint that
holds every Messages request open, so no model is ever called.

The scenario:

1. A holder issue is admitted from the small seed commit and parks on the
   held model. The namespace sandbox quota is one pod, so the holder owns all
   sandbox capacity.
2. A commit adds an anonymous, incompressible 2 MiB file. A target issue is
   admitted on that commit. The worker's fixture-only archive cap (1 MiB,
   chart value ``worker.workspace.maxArchiveBytes``) fails its workspace
   archive before any sandbox is claimed.
3. The fixture input is repaired by setting the archive cap back to the chart
   default in both places the chart rendered it: the base sandbox templates'
   workspace fetch, and the worker Deployment. The Deployment change replaces
   the worker pod (a rolling update: the old pod keeps draining the holder).
   The new worker prepares the workspace, meets the full quota and defers the
   target for capacity twice.
4. Both the internal request read and ``GET /work-items/{id}`` must still show
   the archive failure beside the capacity wait. Releasing the holder lets the
   target start, and the start clears the diagnostic.

Cleanup is registered before anything is created. Every invocation uses a new
run id, cluster, registry, image tags and work directory. A missing
prerequisite raises, so pytest reports a fixture error rather than a skip.
Teardown removes only what this run created and raises if any of it remains.
The Docker endpoint is the caller's: no Docker context is set and nothing is
bind mounted from this machine.
"""

from __future__ import annotations

import argparse
import base64
import getpass
import ipaddress
import json
import os
import re
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "tools/factory-e2e"))
sys.path.insert(0, str(REPO / "tools/github-stub"))

import factory_e2e as fe  # noqa: E402
import github_stub  # noqa: E402
import scripted_kind  # noqa: E402

NAMESPACE = "curie"
WORKER_DEPLOYMENT = "curie-worker"
WORKER_CONTAINER = "worker"
NODE_IMAGE = "kindest/node:v1.31.0"
REQUIRED_EXECUTABLES = ("docker", "kind", "helm", "kubectl", "git", "openssl")
# Images, the kind node, its copies of the loaded images and the run registry.
MIN_FREE_BYTES = 15 * 1024**3

HOLDER_ISSUE = 3815
TARGET_ISSUE = 3816
FIXTURE_ARCHIVE_CAP = 1024 * 1024
NOISE_BYTES = 2 * 1024 * 1024
ARCHIVE_DIAGNOSTIC = f"workspace archive-size failed: archive exceeds {FIXTURE_ARCHIVE_CAP} bytes"
CAPACITY_LOG = "sandbox capacity exhausted for event"
START_DEFERRAL_LIMIT = 5
ARCHIVE_ENV = "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES"
# A held Messages request is answered only at release or teardown.
GATE_HOLD_SECONDS = 4 * 3600
SANDBOX_TEMPLATES = "sandboxtemplates.extensions.agents.x-k8s.io"
SANDBOX_CLAIMS = "sandboxclaims.extensions.agents.x-k8s.io"
SANDBOXES = "sandboxes.agents.x-k8s.io"
# The worker labels the per-claim template it clones for one claim
# (apps/worker/src/curie_worker/sandbox/claim_tokens.py CLAIM_LABEL).
CLAIM_TEMPLATE_LABEL = "curietech.ai/sandbox-claim"
# The chart renders worker.workspace.maxArchiveBytes a second time, into the
# sandbox workspace-init fetch (charts/curie/templates/agent-sandbox.yaml), as
# a bare number Helm may print in float form (1.048576e+06).
_FETCH_CAP = re.compile(r"(if size > )([0-9][0-9.e+]*)(:)")
DIAGNOSTIC_LIMIT = 24000

_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"'<>]+")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_USER_AT_HOST = re.compile(r"\b[A-Za-z0-9._-]+@[A-Za-z0-9.-]+[A-Za-z0-9]\b")


def _chart_default_archive_bytes() -> int:
    values = yaml.safe_load((REPO / "charts/curie/values.yaml").read_text())
    return int(values["worker"]["workspace"]["maxArchiveBytes"])


# The effective Docker endpoint and context name, resolved once per fixture
# by ``resolve_docker_endpoint``. The scrubber names them in addition to the
# raw DOCKER_HOST and DOCKER_CONTEXT values.
_RESOLVED_DOCKER: dict[str, str] = {}


def _docker_endpoints() -> list[str]:
    """Every non-socket Docker endpoint string this process may talk to."""

    values = (os.environ.get("DOCKER_HOST", ""), _RESOLVED_DOCKER.get("endpoint", ""))
    return [value for value in dict.fromkeys(values) if value and not value.startswith("unix://")]


def _endpoint_terms(endpoint: str) -> set[str]:
    terms = {endpoint}
    # Split by hand as well, so a malformed endpoint still yields its parts.
    authority = endpoint.split("://", 1)[-1].split("/", 1)[0]
    terms.add(authority)
    user, _, host = authority.rpartition("@")
    terms.update((user, host, host.rsplit(":", 1)[0]))
    try:
        parsed = urlsplit(endpoint)
        terms.update(value for value in (parsed.netloc, parsed.username, parsed.hostname) if value)
    except ValueError:
        pass
    return terms


def resolve_docker_endpoint(env: dict[str, str]) -> str | None:
    """The endpoint every docker call in ``env`` reaches, or None if unknown.

    Docker's precedence: DOCKER_CONTEXT over DOCKER_HOST over the configured
    current context (no ``--context`` flag is used here). ``docker context
    inspect`` with no name reads the current context, honouring
    DOCKER_CONTEXT. Nothing is switched or written. Never raises.
    """

    _RESOLVED_DOCKER.clear()

    def probe(*argv: str) -> str | None:
        try:
            result = subprocess.run(
                ["docker", "context", *argv],
                env=env,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
        return result.stdout.strip() if result.returncode == 0 else None

    name = probe("show")
    if name:
        _RESOLVED_DOCKER["context"] = name
    if env.get("DOCKER_HOST") and not env.get("DOCKER_CONTEXT"):
        endpoint: str | None = env["DOCKER_HOST"]
    else:
        raw = probe("inspect", "--format", "{{json .Endpoints.docker.Host}}")
        try:
            endpoint = json.loads(raw) if raw else None
        except ValueError:
            endpoint = None
        if not isinstance(endpoint, str) or not endpoint:
            endpoint = None
    if endpoint is not None:
        _RESOLVED_DOCKER["endpoint"] = endpoint
    return endpoint


def _private_terms() -> list[str]:
    """Names of this machine and its user that must never reach test output."""

    terms: set[str] = set()
    for endpoint in _docker_endpoints():
        terms.update(_endpoint_terms(endpoint))
    terms.update(
        value
        for value in (os.environ.get("DOCKER_CONTEXT"), _RESOLVED_DOCKER.get("context"))
        if value and value != "default"
    )
    terms.update(value for value in (os.environ.get("USER"), os.environ.get("LOGNAME")) if value)
    for lookup in (getpass.getuser, socket.gethostname):
        try:
            terms.add(lookup())
        except (OSError, KeyError):
            pass
    return sorted(
        (term for term in terms if len(term) >= 3 and term not in {"localhost", "root"}),
        key=len,
        reverse=True,
    )


def _safe(value: Any, secrets: list[str | None], limit: int = 4000) -> str:
    """Anonymous text for failure messages. Never raises.

    Removes known secrets (the API key, worker token and generated keys) and
    credential shapes, the Docker endpoint and its user and host, every URL
    (ssh endpoints and the kube API server included), the home directory and
    paths under it, user@host pairs, this machine's user and host names, and
    IPv4 addresses. If scrubbing itself fails, nothing of the input survives.
    """

    try:
        return _scrub(value, secrets)[:limit]
    except Exception:  # noqa: BLE001 - an unscrubbable value is withheld, never shown
        return "[output withheld: it could not be scrubbed]"


def _scrub(value: Any, secrets: list[str | None]) -> str:
    text = value if isinstance(value, str) else json.dumps(value, default=str, sort_keys=True)
    text = fe.redact_agent_text(text, secrets)[0] or ""
    for endpoint in _docker_endpoints():
        text = text.replace(endpoint, "ssh://docker-host.invalid")
    text = _URL.sub("https://example.com", text)
    home = str(Path.home())
    if home not in {"", "/"}:
        text = text.replace(home, "~")
    text = _USER_AT_HOST.sub("user@host.invalid", text)
    for term in _private_terms():
        text = re.sub(rf"(?<![\w.-]){re.escape(term)}(?![\w-])", "<private>", text)
    text = _IPV4.sub("192.0.2.1", text)
    return text


def _close_bounded(close: Callable[[], Any], timeout: float = 30) -> None:
    """Run a server close in a daemon thread; a hang becomes a TimeoutError."""

    outcome: list[BaseException] = []

    def target() -> None:
        try:
            close()
        except BaseException as exc:  # noqa: BLE001 - handed back to the caller
            outcome.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError(f"server close did not finish within {timeout:.0f} s")
    if outcome:
        raise outcome[0]


class GatedModel:
    """A Messages endpoint that holds every request until it is released.

    Holding the holder's first model request keeps its runner, and so its
    sandbox pod, alive for as long as the scenario needs. No model is reached.
    """

    def __init__(self, host: str) -> None:
        self.host = host
        # ScriptedRun.cleanup audits this list; unknown paths are recorded here.
        self.unexpected: list[str] = []
        self.held = 0
        self._lock = threading.Lock()
        self._release = threading.Event()
        gate = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def _reply(self, status: int, body: dict[str, Any]) -> None:
                payload = json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except OSError:
                    pass

            def do_GET(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                if path in {"/", "/health"}:
                    self._reply(200, {"ok": True})
                    return
                gate._note(f"GET {path}")
                self._reply(404, {"type": "error", "error": {"type": "not_found_error"}})

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                path = self.path.split("?", 1)[0]
                if not path.startswith("/v1/messages"):
                    gate._note(f"POST {path}")
                    self._reply(404, {"type": "error", "error": {"type": "not_found_error"}})
                    return
                with gate._lock:
                    gate.held += 1
                gate._release.wait(timeout=GATE_HOLD_SECONDS)
                self._reply(
                    529,
                    {
                        "type": "error",
                        "error": {"type": "overloaded_error", "message": "test model released"},
                    },
                )

        class Server(ThreadingHTTPServer):
            daemon_threads = True

        self._server = Server((host, 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._closed = False

    def _note(self, request: str) -> None:
        with self._lock:
            self.unexpected.append(request)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def held_requests(self) -> int:
        with self._lock:
            return self.held

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._release.set()
        if self._thread.is_alive():
            self._server.shutdown()
            self._thread.join(timeout=10)
        self._server.server_close()


class _Run(scripted_kind.ScriptedRun):
    """ScriptedRun's owned cluster plumbing with this test's disk floor."""

    disk_path = "/"
    # Set from the resolved endpoint; unknown counts as remote.
    local_engine = False

    def check_disk(self, stage: str) -> None:
        if not self.local_engine:
            return
        if shutil.disk_usage(self.disk_path).free < MIN_FREE_BYTES:
            raise RuntimeError(
                f"{stage} requires at least {MIN_FREE_BYTES // 1024**3} GiB free "
                "on the local Docker host"
            )


class Harness:
    """Everything one invocation owns, and the reads the scenario makes."""

    def __init__(self) -> None:
        self.run = _Run(argparse.Namespace(record=False, transcript=None, build_images=False))
        # ScriptedRun.__init__ overrides the caller's Docker selection
        # (DOCKER_CONTEXT=default, BUILDX_BUILDER=default). Every subprocess
        # here must see the caller's environment exactly, so the env is
        # rebuilt from it, minus only the recording credential this
        # regression must never pass on. Values added later are run-scoped
        # (KUBECONFIG, CURIE_CONFIG_DIR, and the API URL, API key and forge
        # token ScriptedRun.deploy hands its own curie calls).
        self.run.env = {
            key: value for key, value in os.environ.items() if key != "CURIE_FACTORY_RECORD_KEY"
        }
        self.tag = f"fx4930-{self.run.run_id}"
        self.images = {
            component: f"curie-{component}:{self.tag}" for component in ("api", "worker", "runner")
        }
        self.built: list[str] = []
        self.volumes: set[str] = set()
        self.docker_ready = False
        self.docker_endpoint: str | None = None
        self.workdir: Path | None = None
        self.gate: GatedModel | None = None
        self.stub: Any = None
        self.registry_ref: str | None = None
        self.api_url = ""
        self.api_key = ""
        self.worker_token = ""
        self.sandbox_priority_class = ""
        self.previous_sigterm: Any = None
        # Request ids whose worker log lines diagnostics always keep.
        self.watched: list[str] = []
        # Scrubbed one-line summaries of errors raised on fixture server and
        # daemon threads, which would otherwise print raw tracebacks.
        self.background_errors: list[str] = []
        self._background_lock = threading.Lock()
        self._previous_handle_error: Any = None
        self._previous_excepthook: Any = None

    def note_background_error(self, source: str, exc: BaseException | None) -> None:
        name = type(exc).__name__ if exc is not None else "error"
        summary = f"{source}: {name}: {self.safe(str(exc)) if exc is not None else ''}"
        with self._background_lock:
            if len(self.background_errors) < 50:
                self.background_errors.append(summary)

    def install_quiet_errors(self) -> None:
        """Record server and thread errors scrubbed, for the fixture's lifetime.

        socketserver's default handle_error prints the client address and a
        traceback with absolute paths; threading's default hook prints the
        traceback of a failed daemon thread. Both are replaced until teardown.
        """

        harness = self

        def handle_error(_server: Any, _request: Any, _client_address: Any) -> None:
            harness.note_background_error("fixture server request", sys.exc_info()[1])

        def excepthook(args: Any) -> None:
            harness.note_background_error("background thread", args.exc_value)

        self._previous_handle_error = socketserver.BaseServer.handle_error
        socketserver.BaseServer.handle_error = handle_error  # type: ignore[method-assign]
        self._previous_excepthook = threading.excepthook
        threading.excepthook = excepthook

    def restore_quiet_errors(self) -> None:
        if self._previous_handle_error is not None:
            socketserver.BaseServer.handle_error = self._previous_handle_error  # type: ignore[method-assign]
            self._previous_handle_error = None
        if self._previous_excepthook is not None:
            threading.excepthook = self._previous_excepthook
            self._previous_excepthook = None

    # Output and failure text -------------------------------------------------

    def safe(self, value: Any) -> str:
        return _safe(value, self.run.secrets)

    def check(self, condition: bool, message: str) -> None:
        # A plain raise, not ``assert``: pytest's introspection would render
        # whole rows, which carry fixture addresses.
        __tracebackhide__ = True
        if not condition:
            raise AssertionError(self.safe(message))

    def poll(
        self,
        what: str,
        probe: Callable[[], tuple[bool, Any]],
        *,
        timeout: float,
        interval: float = 3.0,
    ) -> Any:
        __tracebackhide__ = True
        deadline = time.monotonic() + timeout
        last: Any = None
        while True:
            try:
                done, last = probe()
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                done, last = False, f"read failed: {type(exc).__name__}: {exc}"
            if done:
                return last
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    self.safe(f"{what} was not observed within {timeout:.0f} s; last observed: ")
                    + self.safe(last)
                )
            time.sleep(min(interval, remaining))

    # Lifecycle ---------------------------------------------------------------

    def prerequisites(self) -> None:
        for binary in REQUIRED_EXECUTABLES:
            if shutil.which(binary) is None:
                raise RuntimeError(f"missing required executable: {binary}")
        # Resolved once, before the first docker call, from exactly the env
        # every docker call uses. An unknown endpoint counts as remote.
        self.docker_endpoint = resolve_docker_endpoint(self.run.env)
        local = self.docker_endpoint is not None and self.docker_endpoint.startswith("unix://")
        self.run.local_engine = local
        self.run.command("docker", "info", timeout=60)
        self.docker_ready = True
        if local:
            root = self.run.command(
                "docker", "info", "--format", "{{.DockerRootDir}}", timeout=60
            ).strip()
            self.run.disk_path = root if root and Path(root).is_dir() else "/"
            self.run.check_disk("the kind diagnostic fixture")
        elif not self.run.env.get("CURIE_LOCAL_STUB_HOST"):
            raise RuntimeError(
                "a remote Docker engine requires CURIE_LOCAL_STUB_HOST so kind pods "
                "can reach the in-process fixture servers"
            )
        self.run.curie = self._curie_binary()

    def _curie_binary(self) -> str:
        supplied = os.environ.get("CURIE_BIN")
        if supplied:
            found = shutil.which(supplied)
            if found is None:
                raise RuntimeError("CURIE_BIN does not name an executable")
            return found
        if shutil.which("cargo") is None:
            raise RuntimeError("CURIE_BIN is unset and cargo is unavailable to build the CLI")
        self.run.command(
            "cargo",
            "build",
            "--release",
            "--locked",
            "--manifest-path",
            "cli/Cargo.toml",
            timeout=3600,
        )
        target = Path(os.environ.get("CARGO_TARGET_DIR") or "cli/target")
        if not target.is_absolute():
            target = REPO / target
        binary = target / "release/curie"
        if not binary.is_file():
            raise RuntimeError("the CLI build produced no curie binary")
        return str(binary)

    def record_volumes(self, container: str) -> None:
        """Remember a container's named and anonymous volumes. Never raises.

        Called as each container is created, and again (best effort) when
        teardown begins.
        """

        try:
            result = self.docker("inspect", container)
            if result.returncode:
                return
            for mount in json.loads(result.stdout)[0].get("Mounts") or []:
                if mount.get("Type") == "volume" and mount.get("Name"):
                    self.volumes.add(mount["Name"])
        except (OSError, subprocess.SubprocessError, ValueError, LookupError, AttributeError):
            return

    def docker(self, *argv: str) -> subprocess.CompletedProcess[str]:
        """A Docker probe that reports instead of raising."""

        return subprocess.run(
            ["docker", *argv],
            cwd=REPO,
            env=self.run.env,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def start(self) -> None:
        run = self.run
        self.workdir = Path(tempfile.mkdtemp(prefix=f"curie-fx4930-{run.run_id}-"))
        run.workdir = self.workdir
        run.env["KUBECONFIG"] = str(self.workdir / "kubeconfig")
        run.env["CURIE_CONFIG_DIR"] = str(self.workdir / "curie-config")

        # Candidate images from this checkout. Each tag is registered before
        # its build, so a partial build is still removed.
        for component in ("api", "worker"):
            image = self.images[component]
            self.built.append(image)
            run.check_disk(f"building {component}")
            run.command(
                "docker",
                "build",
                "-f",
                f"apps/{component}/Dockerfile",
                "-t",
                image,
                ".",
                timeout=2400,
            )
        self.built.append(self.images["runner"])
        run.check_disk("building runner")
        run.command(run.curie, "build", "--tag", self.images["runner"], timeout=2400)

        _port, registry_ref = run.start_registry()
        self.registry_ref = registry_ref
        self.record_volumes(run.registry)
        config = self.workdir / "kind.yaml"
        config_text = (
            "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n"
            "containerdConfigPatches:\n  - |\n"
            f'    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."{registry_ref}"]\n'
            f'      endpoint = ["http://{run.registry}:5000"]\n'
        )
        # No host directory is shared with the engine: images and files reach
        # the node through the Docker API, kind load and the run registry.
        if "extraMounts" in config_text or "hostPath" in config_text:
            raise RuntimeError("the generated kind config must not mount a host path")
        config.write_text(config_text)
        run.cluster_attempted = True
        run.command(
            "kind",
            "create",
            "cluster",
            "--name",
            run.cluster,
            "--image",
            NODE_IMAGE,
            "--config",
            str(config),
            "--kubeconfig",
            run.env["KUBECONFIG"],
            "--wait",
            "120s",
            timeout=600,
        )
        self.record_volumes(f"{run.cluster}-control-plane")
        run.command("docker", "network", "connect", "kind", run.registry)
        node = json.loads(run.command("docker", "inspect", f"{run.cluster}-control-plane"))[0]
        gateway = node["NetworkSettings"]["Networks"]["kind"].get("Gateway", "")
        if not gateway or ipaddress.ip_address(gateway).version != 4:
            network = json.loads(run.command("docker", "network", "inspect", "kind"))[0]
            gateway = next(
                item["Gateway"]
                for item in network["IPAM"]["Config"]
                if item.get("Gateway") and ipaddress.ip_address(item["Gateway"]).version == 4
            )
        callback = scripted_kind.in_process_callback_host(
            # The resolved endpoint, not DOCKER_HOST alone: a caller's context
            # can name a remote engine. Unknown is passed as remote.
            docker_host=(
                self.docker_endpoint if run.local_engine else "ssh://unresolved-or-remote.invalid"
            ),
            stub_host=run.env.get("CURIE_LOCAL_STUB_HOST"),
            gateway=gateway,
        )
        for component in ("api", "worker"):
            run.check_disk("loading a candidate image")
            run.command(
                "kind",
                "load",
                "docker-image",
                self.images[component],
                "--name",
                run.cluster,
                timeout=900,
            )
        runner_image = f"{registry_ref}/curie-runner"
        run.command("docker", "tag", self.images["runner"], f"{runner_image}:latest")
        run.command("docker", "push", f"{runner_image}:latest", timeout=1200)

        recording = json.loads(
            (REPO / "tools/github-stub/recordings/curie-pr-3400.json").read_text()
        )
        self.stub = github_stub.GithubStub(
            self.workdir / "github",
            recording,
            host=callback,
            seed_tree=scripted_kind.FIXTURE,
            issue_file=scripted_kind.ISSUE,
        )
        run.stub = self.stub
        self.stub.start()
        # Intake polls from install on; the holder stays unlabelled until the
        # agent and its bundle exist.
        issue = self.stub._get("issue", HOLDER_ISSUE)
        issue["labels"] = []
        self.stub._put("issue", HOLDER_ISSUE, issue)
        self.gate = GatedModel(callback)
        run.model = self.gate
        self.gate.start()

        run.kubectl("apply", "-f", str(REPO / "charts/curie/crds"))
        run.kubectl("create", "namespace", NAMESPACE)
        run.kubectl(
            "-n",
            NAMESPACE,
            "create",
            "configmap",
            "github-stub-ca",
            f"--from-file=ca.pem={self.stub.ca_file}",
        )
        app_key = self.workdir / "app.pem"
        run.command("openssl", "genrsa", "-out", str(app_key), "2048")
        app_key.chmod(0o600)
        run.kubectl(
            "-n",
            NAMESPACE,
            "create",
            "secret",
            "generic",
            "factory-app",
            f"--from-file=privateKey={app_key}",
        )

        values = scripted_kind.scripted_values(
            model_base_url=self.gate.base_url,
            github_api=self.stub.base_url + "/api/v3",
            clone_base=self.stub.base_url,
            ca_configmap="github-stub-ca",
            runner_image=runner_image,
        )
        values["api"]["image"].update(repository="curie-api", tag=self.tag, pullPolicy="Never")
        values["worker"]["image"].update(
            repository="curie-worker", tag=self.tag, pullPolicy="Never"
        )
        values.setdefault("ui", {})["deploy"] = False
        values.setdefault("resourceQuota", {}).setdefault("hard", {})["sandboxPodCount"] = "1"
        values.setdefault("worker", {}).setdefault("workspace", {})["maxArchiveBytes"] = (
            FIXTURE_ARCHIVE_CAP
        )
        extra = values["worker"].setdefault("extraEnv", [])
        if any(entry.get("name") == ARCHIVE_ENV for entry in extra):
            raise RuntimeError("the archive cap must come from the chart value, not extraEnv")

        key_code = (
            "import base64,json; from nacl.signing import SigningKey; "
            "from nacl.public import PrivateKey; k=SigningKey.generate(); "
            "print(json.dumps({'signing':base64.b64encode(bytes(k)).decode(),"
            "'verify':base64.b64encode(bytes(k.verify_key)).decode(),"
            "'sealing':base64.b64encode(bytes(PrivateKey.generate())).decode()}))"
        )
        keys = json.loads(
            run.command(
                "docker",
                "run",
                "--rm",
                "--name",
                f"{run.cluster}-keys",
                "--label",
                f"curie.factory-scripted={run.run_id}",
                self.images["worker"],
                "python",
                "-c",
                key_code,
                timeout=120,
            )
        )
        run.secrets.extend(keys.values())
        values["connectorCaller"] = {"signingKey": keys["signing"], "verifyKey": keys["verify"]}
        values["sealing"] = {"privateKey": keys["sealing"]}
        values_file = self.workdir / "values.json"
        values_file.write_text(json.dumps(values))
        values_file.chmod(0o600)
        run.command(
            "helm",
            "--kube-context",
            run.context,
            "install",
            "curie",
            "charts/curie",
            "-n",
            NAMESPACE,
            "-f",
            "charts/curie/values-dev.yaml",
            "-f",
            str(values_file),
            "--timeout",
            "15m",
            timeout=1000,
        )
        for deployment in ("curie-api", WORKER_DEPLOYMENT):
            run.kubectl(
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=600s",
                timeout=660,
            )

        quota = self.quota()
        self.check(
            quota["spec"]["hard"].get("pods") == "1",
            f"rendered sandbox quota hard pods is {quota['spec']['hard'].get('pods')!r}, want '1'",
        )
        expressions = quota["spec"]["scopeSelector"]["matchExpressions"]
        self.sandbox_priority_class = expressions[0]["values"][0]
        env = self.deployment_env()
        self.check(
            env.get(ARCHIVE_ENV) == str(FIXTURE_ARCHIVE_CAP),
            f"worker {ARCHIVE_ENV} renders {env.get(ARCHIVE_ENV)!r}, want {FIXTURE_ARCHIVE_CAP}",
        )

        run.deploy(registry_ref)
        self.api_url = run.env["CURIE_API_URL"]
        self.api_key = run.env["CURIE_API_KEY"]
        secret = json.loads(
            run.kubectl("-n", NAMESPACE, "get", "secret", "curie-secrets", "-o", "json")
        )
        self.worker_token = base64.b64decode(secret["data"]["internalWorkerToken"]).decode()
        run.secrets.append(self.worker_token)

    def diagnose(self) -> None:
        try:
            if self.run.cluster_attempted:
                self.snapshot("failure")
            unknown = list(self.stub.unknown_requests) if self.stub is not None else []
            unexpected = list(self.gate.unexpected) if self.gate is not None else []
            with self._background_lock:
                background = list(self.background_errors)
            print(
                self.safe(
                    f"fx4930 diagnostics: unsupported stub requests {unknown[:8]}; "
                    f"unexpected model requests {unexpected[:8]}; "
                    f"background errors {background[:20]}"
                ),
                file=sys.stderr,
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - diagnostics never block teardown
            print(f"fx4930 diagnostics unavailable: {type(exc).__name__}", file=sys.stderr)

    def snapshot(self, label: str) -> None:
        """Scrubbed, bounded cluster state on stderr. Never raises.

        Pod and claim specs carry boot env, so those are summarized from
        status only; nothing prints an environment.
        """

        def emit(title: str, value: Any) -> None:
            print(
                f"fx4930 snapshot [{label}] {title}\n"
                + _safe(value, self.run.secrets, DIAGNOSTIC_LIMIT),
                file=sys.stderr,
                flush=True,
            )

        def read(*argv: str, timeout: float = 30) -> str:
            try:
                return self.run.kubectl(*argv, timeout=timeout)
            except Exception as exc:  # noqa: BLE001 - one missing view must not hide the rest
                return f"unavailable ({type(exc).__name__})"

        def guarded(title: str, produce: Callable[[], Any]) -> None:
            try:
                emit(title, produce())
            except Exception as exc:  # noqa: BLE001 - one missing view must not hide the rest
                emit(title, f"unavailable ({type(exc).__name__})")

        def status_view(item: dict[str, Any]) -> dict[str, Any]:
            status = item.get("status") or {}
            view: dict[str, Any] = {
                "kind": item.get("kind"),
                "namespace": item["metadata"].get("namespace"),
                "name": item["metadata"].get("name"),
                "labels": item["metadata"].get("labels"),
                "deleting": item["metadata"].get("deletionTimestamp") is not None,
                "phase": status.get("phase"),
                "conditions": status.get("conditions"),
            }
            containers = (status.get("initContainerStatuses") or []) + (
                status.get("containerStatuses") or []
            )
            if containers:
                view["containers"] = [
                    {
                        "name": container.get("name"),
                        "ready": container.get("ready"),
                        "restarts": container.get("restartCount"),
                        "state": container.get("state"),
                        "last_state": container.get("lastState"),
                    }
                    for container in containers
                ]
            return view

        emit("pods -A -o wide", read("get", "pods", "-A", "-o", "wide"))
        guarded("worker pods", self.worker_pods)
        try:
            worker_names = [pod["name"] for pod in self.worker_pods()]
        except Exception:  # noqa: BLE001 - the pod list above already reported it
            worker_names = []
        for name in worker_names:
            for previous in (False, True):
                flags = ("--previous",) if previous else ()
                text = read(
                    "-n", NAMESPACE, "logs", name, "-c", WORKER_CONTAINER, *flags, timeout=60
                )
                lines = text.splitlines()
                tail_start = max(0, len(lines) - 400)
                kept = [
                    line
                    for index, line in enumerate(lines)
                    if index >= tail_start or any(watched in line for watched in self.watched)
                ]
                emit(
                    f"worker logs {name}{' --previous' if previous else ''} "
                    "(last 400 lines plus watched request lines)",
                    "\n".join(kept)[-DIAGNOSTIC_LIMIT:],
                )
        emit(
            "sandbox claims and sandboxes",
            read("get", f"{SANDBOX_CLAIMS},{SANDBOXES}", "-A", "-o", "wide"),
        )
        guarded(
            "sandbox claim and sandbox status",
            lambda: [
                status_view(item)
                for item in json.loads(
                    self.run.kubectl(
                        "get", f"{SANDBOX_CLAIMS},{SANDBOXES}", "-A", "-o", "json", timeout=30
                    )
                )["items"]
            ],
        )
        guarded(
            "sandbox pod status",
            lambda: [
                status_view(pod)
                for pod in json.loads(self.run.kubectl("get", "pods", "-A", "-o", "json"))["items"]
                if pod.get("spec", {}).get("priorityClassName") == self.sandbox_priority_class
            ],
        )
        guarded("sandbox template archive caps", self.template_caps)
        emit("resourcequota -A -o yaml", read("get", "resourcequota", "-A", "-o", "yaml"))
        api_logs = read(
            "-n", NAMESPACE, "logs", "deployment/curie-api", "-c", "api", "--tail=200", timeout=60
        )
        emit("api logs (last 200 lines)", api_logs)
        events = read("get", "events", "-A", "--sort-by=.lastTimestamp").splitlines()
        emit("events -A (latest 150)", "\n".join(events[-150:]))

    # Sandbox templates -------------------------------------------------------

    def sandbox_templates(self) -> list[dict[str, Any]]:
        return json.loads(self.run.kubectl("get", SANDBOX_TEMPLATES, "-A", "-o", "json"))["items"]

    def template_caps(self) -> list[dict[str, Any]]:
        return [
            {
                "namespace": template["metadata"].get("namespace"),
                "name": template["metadata"].get("name"),
                "per_claim": CLAIM_TEMPLATE_LABEL in (template["metadata"].get("labels") or {}),
                "fetch_caps": [
                    float(match.group(2))
                    for match in _FETCH_CAP.finditer(json.dumps(template.get("spec") or {}))
                ],
            }
            for template in self.sandbox_templates()
        ]

    def repair_sandbox_templates(self, cap: int) -> list[str]:
        """Set the base templates' workspace fetch cap to ``cap``.

        A per-claim template belongs to a claim that already booted (the
        holder's), so it is left exactly as that sandbox started. Later claims
        clone the repaired base template.
        """

        if self.workdir is None:
            raise RuntimeError("the run has no work directory")
        repaired: list[str] = []
        for template in self.sandbox_templates():
            metadata = template["metadata"]
            if CLAIM_TEMPLATE_LABEL in (metadata.get("labels") or {}):
                continue
            spec_text = json.dumps(template.get("spec") or {})
            caps = [float(match.group(2)) for match in _FETCH_CAP.finditer(spec_text)]
            if not caps:
                continue
            self.check(
                all(value == FIXTURE_ARCHIVE_CAP for value in caps),
                f"sandbox template {metadata.get('name')} fetch caps are {caps}, "
                f"want the fixture cap {FIXTURE_ARCHIVE_CAP}",
            )
            template["spec"] = json.loads(
                _FETCH_CAP.sub(lambda match: f"{match.group(1)}{cap}{match.group(3)}", spec_text)
            )
            path = self.workdir / f"sandbox-template-{metadata['name']}.json"
            path.write_text(json.dumps(template))
            self.run.kubectl("replace", "-f", str(path))
            repaired.append(f"{metadata.get('namespace')}/{metadata['name']}")
        return repaired

    def teardown(self) -> None:
        """Release everything this run owned, then prove it is gone.

        Each step is guarded on its own, so a failed or timed-out step never
        skips the ones after it. Order: the port-forward and fixture servers,
        the kind cluster, owned containers, recorded volumes, image tags, the
        work directory, then the inventory. One scrubbed error names every
        failed step and every remaining resource.
        """

        __tracebackhide__ = True
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        run = self.run
        errors: list[str] = []

        def step(name: str, action: Callable[[], Any]) -> None:
            try:
                action()
            except Exception as exc:  # noqa: BLE001 - later steps must still run
                errors.append(f"{name} failed: {type(exc).__name__}: {exc}")

        try:
            run.deadline = None
            if self.docker_ready:
                if run.cluster_attempted:
                    self.record_volumes(f"{run.cluster}-control-plane")
                if run.registry_attempted:
                    self.record_volumes(run.registry)
            step("stopping the API port-forward", self._stop_forward)
            if self.gate is not None:
                step("closing the model endpoint", lambda: _close_bounded(self.gate.close))
            if self.stub is not None:
                step("closing the forge stub", self._close_stub)
            if self.docker_ready:
                if run.cluster_attempted:
                    step("deleting the kind cluster", self._delete_cluster)
                for container in self._owned_containers(errors):
                    step(
                        "removing an owned container",
                        lambda container=container: self._remove("container", container),
                    )
                for volume in sorted(self.volumes):
                    step(
                        "removing an owned volume",
                        lambda volume=volume: self._remove("volume", volume),
                    )
                for image in [*self.built, *self._registry_tags(errors)]:
                    step(
                        f"removing image {image}", lambda image=image: self._remove("image", image)
                    )
            step("removing the work directory", self._remove_workdir)
            step("taking the teardown inventory", lambda: errors.extend(self.inventory()))
        finally:
            self.restore_quiet_errors()
            signal.signal(signal.SIGTERM, self.previous_sigterm or signal.SIG_DFL)
        if errors:
            raise RuntimeError(
                "teardown did not remove everything this run owned: "
                + "; ".join(self.safe(error) for error in errors)
            ) from None

    def _try_docker(self, *argv: str) -> subprocess.CompletedProcess[str] | None:
        try:
            return self.docker(*argv)
        except (OSError, subprocess.SubprocessError):
            return None

    def _stop_forward(self) -> None:
        forward = self.run.forward
        if forward is None:
            return
        try:
            if forward.poll() is None:
                forward.terminate()
                try:
                    forward.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    forward.kill()
                    forward.wait(timeout=10)
        finally:
            if forward.stdout is not None:
                forward.stdout.close()

    def _close_stub(self) -> None:
        stub = self.stub
        server = getattr(stub, "_server", None)
        thread = getattr(stub, "_thread", None)
        if server is not None and (thread is None or not thread.is_alive()):
            # Startup stopped before serve_forever ran, and shutdown() would
            # wait for it forever. Closing the socket is all there is to undo.
            stub._closed = True
            stub._server = None
            server.server_close()
            return
        try:
            _close_bounded(stub.close)
        except RuntimeError:
            # close() shuts the server down first, then re-raises its audit of
            # unsupported requests; diagnostics report those. The inventory
            # still proves the endpoint stopped listening.
            if not stub.unknown_requests:
                raise

    def _delete_cluster(self) -> None:
        self.run.command("kind", "delete", "cluster", "--name", self.run.cluster, timeout=300)

    def _owned_containers(self, errors: list[str]) -> list[str]:
        run = self.run
        found: list[str] = []
        for selector in (
            f"label=io.x-k8s.kind.cluster={run.cluster}",
            f"name={run.cluster}",
            f"label=curie.factory-scripted={run.run_id}",
        ):
            listed = self._try_docker("ps", "-aq", "--filter", selector)
            if listed is None or listed.returncode:
                errors.append(f"listing owned containers ({selector.split('=')[0]}) failed")
                continue
            found.extend(item for item in listed.stdout.split() if item not in found)
        return found

    def _registry_tags(self, errors: list[str]) -> list[str]:
        if self.registry_ref is None:
            return []
        listed = self._try_docker(
            "images",
            "--format",
            "{{.Repository}}:{{.Tag}}",
            "--filter",
            f"reference={self.registry_ref}/*",
        )
        if listed is None or listed.returncode:
            errors.append("listing the run registry's image tags failed")
            return []
        return listed.stdout.split()

    def _remove(self, kind: str, name: str) -> None:
        if self.docker(kind, "inspect", name).returncode:
            return
        flags = ("-f", "-v") if kind == "container" else ()
        removed = self.docker(kind, "rm", *flags, name)
        if removed.returncode:
            raise RuntimeError(f"docker {kind} rm exited {removed.returncode}")

    def _remove_workdir(self) -> None:
        if self.workdir is not None and self.workdir.exists():
            shutil.rmtree(self.workdir)

    def inventory(self) -> list[str]:
        """Everything this run could have created, listed again after removal."""

        run = self.run
        remaining: list[str] = []

        def present(*argv: str) -> bool:
            result = self._try_docker(*argv)
            if result is None or result.returncode:
                remaining.append(f"inventory command failed: docker {argv[0]} {argv[1]}")
                return False
            return bool(result.stdout.split())

        def exists(kind: str, name: str) -> bool:
            result = self._try_docker(kind, "inspect", name)
            if result is None:
                remaining.append(f"inventory command failed: docker {kind} inspect")
                return False
            return result.returncode == 0

        if self.docker_ready:
            if run.cluster_attempted:
                try:
                    clusters = subprocess.run(
                        ["kind", "get", "clusters"],
                        env=run.env,
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=60,
                    )
                    if clusters.returncode or run.cluster in clusters.stdout.split():
                        remaining.append("owned kind cluster remains or could not be listed")
                except (OSError, subprocess.SubprocessError):
                    remaining.append("owned kind clusters could not be listed")
            for selector in (
                f"label=io.x-k8s.kind.cluster={run.cluster}",
                f"name={run.cluster}",
                f"label=curie.factory-scripted={run.run_id}",
            ):
                if present("ps", "-aq", "--filter", selector):
                    remaining.append(f"owned containers remain ({selector.split('=')[0]})")
            if present("network", "ls", "-q", "--filter", f"name={run.cluster}"):
                remaining.append("owned network remains")
            if present(
                "volume", "ls", "-q", "--filter", f"label=io.x-k8s.kind.cluster={run.cluster}"
            ):
                remaining.append("owned kind volume remains")
            for volume in sorted(self.volumes):
                if exists("volume", volume):
                    remaining.append("owned volume remains")
            for image in self.built:
                if exists("image", image):
                    remaining.append(f"owned image tag remains: {image}")
            if self.registry_ref is not None and present(
                "images", "-q", "--filter", f"reference={self.registry_ref}/*"
            ):
                remaining.append("owned registry image tags remain")
        if self.workdir is not None and self.workdir.exists():
            remaining.append("owned work directory remains")
        if run.forward is not None and run.forward.poll() is None:
            remaining.append("API port-forward is still running")
        for service in (self.gate, self.stub):
            if service is None:
                continue
            try:
                with socket.socket() as sock:
                    sock.settimeout(1)
                    if sock.connect_ex((service.host, service.port)) == 0:
                        remaining.append("an owned fixture endpoint still listens")
            except OSError:
                remaining.append("an owned fixture endpoint could not be probed")
        return remaining

    # Reads -------------------------------------------------------------------

    def public(self, path: str) -> Any:
        status, body = fe.http_json("GET", self.api_url + path, headers={"X-API-Key": self.api_key})
        if status != 200:
            raise RuntimeError(f"GET {path} returned HTTP {status}")
        return body

    def internal(self, request_id: str) -> dict[str, Any]:
        path = f"/v1/internal/work-items/requests/{uuid.UUID(request_id)}"
        status, body = fe.http_json(
            "GET", self.api_url + path, headers={"X-Curie-Worker-Token": self.worker_token}
        )
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f"internal request read returned HTTP {status}")
        return body

    def requests_for_issue(self, number: int) -> list[dict[str, Any]]:
        # Read-only locator: which WorkItem and request the intake created.
        rows = self.run.sql(
            "SELECT coalesce(json_agg(json_build_object('work_item_id', w.id, "
            "'request_id', r.id, 'sequence', r.sequence) ORDER BY r.sequence), '[]'::json) "
            "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
            f"WHERE w.tracker_issue_id = '{int(number)}'"
        )
        return [{key: str(value) for key, value in row.items()} for row in rows]

    def start_deferrals(self, request_id: str) -> int:
        return int(
            self.run.sql(
                "SELECT to_json(start_deferrals) FROM curie.execution_requests "
                f"WHERE id = '{uuid.UUID(request_id)}'::uuid"
            )
        )

    def quota(self) -> dict[str, Any]:
        items = json.loads(
            self.run.kubectl(
                "get",
                "resourcequota",
                "-A",
                "-l",
                "app.kubernetes.io/component=tenant-capacity",
                "-o",
                "json",
            )
        )["items"]
        if len(items) != 1:
            raise RuntimeError(f"expected one sandbox ResourceQuota, found {len(items)}")
        return items[0]

    def used_pods(self) -> str | None:
        return (self.quota().get("status", {}).get("used") or {}).get("pods")

    def sandbox_pods(self) -> list[dict[str, Any]]:
        pods = json.loads(self.run.kubectl("get", "pods", "-A", "-o", "json"))["items"]
        return [
            {
                "namespace": pod["metadata"]["namespace"],
                "name": pod["metadata"]["name"],
                "uid": pod["metadata"]["uid"],
                "phase": pod.get("status", {}).get("phase"),
                "terminating": pod["metadata"].get("deletionTimestamp") is not None,
            }
            for pod in pods
            if pod.get("spec", {}).get("priorityClassName") == self.sandbox_priority_class
        ]

    def deployment_env(self) -> dict[str, str]:
        deployment = json.loads(
            self.run.kubectl(
                "-n", NAMESPACE, "get", f"deployment/{WORKER_DEPLOYMENT}", "-o", "json"
            )
        )
        container = next(
            item
            for item in deployment["spec"]["template"]["spec"]["containers"]
            if item["name"] == WORKER_CONTAINER
        )
        return {item["name"]: item.get("value", "") for item in container.get("env") or []}

    def worker_pods(self) -> list[dict[str, Any]]:
        deployment = json.loads(
            self.run.kubectl(
                "-n", NAMESPACE, "get", f"deployment/{WORKER_DEPLOYMENT}", "-o", "json"
            )
        )
        selector = ",".join(
            f"{key}={value}" for key, value in deployment["spec"]["selector"]["matchLabels"].items()
        )
        pods = json.loads(
            self.run.kubectl("-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "json")
        )["items"]
        views = []
        for pod in pods:
            container = next(
                item for item in pod["spec"]["containers"] if item["name"] == WORKER_CONTAINER
            )
            env = {item["name"]: item.get("value", "") for item in container.get("env") or []}
            ready = any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod.get("status", {}).get("conditions") or []
            )
            views.append(
                {
                    "name": pod["metadata"]["name"],
                    "uid": pod["metadata"]["uid"],
                    "terminating": pod["metadata"].get("deletionTimestamp") is not None,
                    "ready": ready,
                    "archive_cap": env.get(ARCHIVE_ENV),
                }
            )
        return views

    def worker_logs(self, pod: str) -> str:
        return self.run.kubectl("-n", NAMESPACE, "logs", pod, "-c", WORKER_CONTAINER, timeout=120)

    # Fixture inputs ----------------------------------------------------------

    def set_label(self, number: int, labelled: bool) -> None:
        issue = self.stub._get("issue", number)
        issue["labels"] = [{"name": "factory"}] if labelled else []
        issue["updated_at"] = github_stub._utc()
        self.stub._put("issue", number, issue)
        action = "labeled" if labelled else "unlabeled"
        self.stub._label_event(number, "factory", action, github_stub.HUMAN)

    def open_target_issue(self) -> None:
        issue = self.stub._issue(
            TARGET_ISSUE, "Second example factory change", self.stub.issue_body, []
        )
        self.stub._put("issue", TARGET_ISSUE, issue)
        self.set_label(TARGET_ISSUE, True)

    def push_incompressible_commit(self) -> str:
        """Add anonymous random bytes to the stub repository's main branch."""

        if self.workdir is None:
            raise RuntimeError("the run has no work directory")
        before = self.stub._ref("main")
        clone = self.workdir / "push-clone"
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("GIT_") and key not in {"DOCKER_CONTEXT"}
        }
        env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0")
        identity = (
            "-c",
            "user.name=Example Author",
            "-c",
            "user.email=author@example.com",
            "-c",
            "commit.gpgsign=false",
        )

        def git(*argv: str) -> None:
            result = subprocess.run(
                ["git", *identity, *argv],
                env=env,
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
            if result.returncode:
                raise RuntimeError(f"git {argv[0]} failed: {self.safe(result.stderr[-1000:])}")

        git("clone", "--quiet", str(self.stub._repository), str(clone))
        noise = clone / "fixture/noise.bin"
        noise.parent.mkdir(parents=True, exist_ok=True)
        noise.write_bytes(os.urandom(NOISE_BYTES))
        git("-C", str(clone), "add", "fixture/noise.bin")
        git("-C", str(clone), "commit", "--quiet", "-m", "Add incompressible fixture data")
        git("-C", str(clone), "push", "--quiet", "origin", "HEAD:refs/heads/main")
        after = self.stub._ref("main")
        self.check(after != before, "the stub repository main branch did not move")
        return after


@pytest.fixture
def diagnostic_cluster(request: pytest.FixtureRequest) -> Harness:
    harness = Harness()
    # Registered before any resource exists, so every exit path tears down.
    request.addfinalizer(harness.teardown)
    harness.install_quiet_errors()

    def interrupted(signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    harness.previous_sigterm = signal.signal(signal.SIGTERM, interrupted)
    # Every error leaving the fixture is rebuilt from scrubbed text, unchained,
    # so neither the original message nor its traceback reaches the report.
    # A missing prerequisite stays a fixture ERROR, never a skip.
    failure: str | None = None
    try:
        harness.prerequisites()
    except Exception as exc:  # noqa: BLE001 - rebuilt below as a scrubbed fixture error
        failure = harness.safe(f"prerequisite unavailable: {type(exc).__name__}: {exc}")
    if failure is not None:
        raise RuntimeError(failure) from None
    try:
        harness.start()
    except Exception as exc:  # noqa: BLE001 - rebuilt below as a scrubbed fixture error
        harness.diagnose()
        failure = harness.safe(f"fixture setup failed: {type(exc).__name__}: {exc}")
    if failure is not None:
        raise RuntimeError(failure) from None
    return harness


def test_archive_failure_survives_capacity_and_worker_replacement(
    diagnostic_cluster: Harness,
) -> None:
    h = diagnostic_cluster
    failure: BaseException | None = None
    try:
        _scenario(h)
    except AssertionError as exc:
        h.diagnose()
        failure = AssertionError(h.safe(str(exc)))
    except Exception as exc:  # noqa: BLE001 - rebuilt below as a scrubbed failure
        h.diagnose()
        failure = RuntimeError(h.safe(f"{type(exc).__name__}: {exc}"))
    if failure is not None:
        raise failure from None


def _single_request(h: Harness, number: int) -> Callable[[], tuple[bool, Any]]:
    def probe() -> tuple[bool, Any]:
        rows = h.requests_for_issue(number)
        if len(rows) > 1:
            raise AssertionError(h.safe(f"issue {number} has more than one request: {rows}"))
        return len(rows) == 1, (rows[0] if rows else "no request yet")

    return probe


def _scenario(h: Harness) -> None:
    chart_default = _chart_default_archive_bytes()
    h.check(chart_default > NOISE_BYTES * 4, "chart default archive cap is unexpectedly small")

    # 1. The holder takes the only sandbox slot and parks on the held model.
    h.check(h.used_pods() in {None, "0"}, "the sandbox quota is in use before any admission")
    h.set_label(HOLDER_ISSUE, True)
    holder_ref = h.poll("the holder request", _single_request(h, HOLDER_ISSUE), timeout=600)
    holder_id = holder_ref["request_id"]
    h.watched.append(holder_id)

    def holder_running() -> tuple[bool, Any]:
        row = h.internal(holder_id)
        if row["status"] not in {"waiting", "running"}:
            raise AssertionError(h.safe(f"holder request left the active states: {row}"))
        return row["status"] == "running", row

    holder = h.poll("the holder request running", holder_running, timeout=1500, interval=5)
    h.poll(
        "the holder runner parked on the held model",
        lambda: (h.gate.held_requests() >= 1, f"held model requests {h.gate.held_requests()}"),
        timeout=900,
        interval=5,
    )
    h.poll(
        "sandbox quota used.pods == 1 with the holder running",
        lambda: ((used := h.used_pods()) == "1", f"used.pods {used!r}"),
        timeout=180,
    )
    holder_pods = h.sandbox_pods()
    h.check(len(holder_pods) == 1, f"expected the holder's one sandbox pod, saw {holder_pods}")
    holder_pod = holder_pods[0]
    holder_epoch = holder["runtime_epoch"]

    # 2. Only now does the repository grow past the fixture-only archive cap,
    # so the holder's frozen base stays the small seed commit.
    h.push_incompressible_commit()

    # 3. The target is admitted on the oversized commit and fails its archive.
    h.open_target_issue()
    target_ref = h.poll("the target request", _single_request(h, TARGET_ISSUE), timeout=600)
    target_id = target_ref["request_id"]
    target_item = target_ref["work_item_id"]
    h.watched.append(target_id)

    def first_start_failure() -> tuple[bool, Any]:
        row = h.internal(target_id)
        if row["status"] != "waiting":
            raise AssertionError(h.safe(f"target request is not waiting: {row}"))
        return str(row.get("last_deferral_reason") or "").startswith("not_started:"), row

    failed = h.poll("the target's first start failure", first_start_failure, timeout=900)
    detail = failed.get("last_start_failure_detail")
    h.check(
        isinstance(detail, str) and detail.startswith(ARCHIVE_DIAGNOSTIC),
        "internal request read must keep the archive diagnostic after a not_started "
        f"deferral; last_start_failure_detail is {detail!r}, last_deferral_reason is "
        f"{failed.get('last_deferral_reason')!r}",
    )
    h.check(failed["started_at"] is None, f"target started before replacement: {failed}")
    recorded_capacity = int(failed["capacity_deferrals"])
    wait_deadline = failed["wait_deadline"]
    h.check(wait_deadline is not None, "a waiting target request has no wait deadline")

    # 4. Repair the fixture input and replace the worker in one rolling update.
    workers = [pod for pod in h.worker_pods() if not pod["terminating"]]
    h.check(len(workers) == 1, f"expected one live worker pod, saw {workers}")
    old_worker = workers[0]
    h.check(
        old_worker["archive_cap"] == str(FIXTURE_ARCHIVE_CAP),
        f"the original worker runs with archive cap {old_worker['archive_cap']!r}",
    )
    # The chart value also caps the sandbox workspace fetch. A repair of the
    # worker alone would let the new worker prepare the archive and then
    # fail every sandbox boot, so the base templates are repaired first; the
    # new worker reads them only after it starts.
    repaired = h.repair_sandbox_templates(chart_default)
    h.check(bool(repaired), "no base sandbox template carried the fixture archive cap")
    base_caps = [view for view in h.template_caps() if not view["per_claim"]]
    h.check(
        all(cap == chart_default for view in base_caps for cap in view["fetch_caps"]),
        f"base sandbox template fetch caps after repair: {base_caps}",
    )
    h.run.kubectl(
        "-n",
        NAMESPACE,
        "set",
        "env",
        f"deployment/{WORKER_DEPLOYMENT}",
        "-c",
        WORKER_CONTAINER,
        f"{ARCHIVE_ENV}={chart_default}",
    )

    def replacement_ready() -> tuple[bool, Any]:
        pods = h.worker_pods()
        fresh = [
            pod
            for pod in pods
            if pod["uid"] != old_worker["uid"] and not pod["terminating"] and pod["ready"]
        ]
        return len(fresh) == 1 and fresh[0]["archive_cap"] == str(chart_default), pods

    pods = h.poll("the replacement worker Ready", replacement_ready, timeout=600, interval=5)
    new_worker = next(pod for pod in pods if pod["uid"] != old_worker["uid"])

    def old_worker_draining() -> tuple[bool, Any]:
        pods = h.worker_pods()
        old = [pod for pod in pods if pod["uid"] == old_worker["uid"]]
        if not old:
            raise AssertionError("the old worker pod ended while it still owned the holder")
        return old[0]["terminating"], pods

    h.poll("the old worker pod draining", old_worker_draining, timeout=180)
    replaced = h.internal(target_id)
    h.check(
        replaced["status"] == "waiting" and h.start_deferrals(target_id) < START_DEFERRAL_LIMIT,
        f"target left waiting or ran out of start attempts during replacement: {replaced}",
    )

    # 5. Two capacity deferrals written by the new worker.
    def two_capacity_deferrals() -> tuple[bool, Any]:
        row = h.internal(target_id)
        if row["status"] != "waiting":
            raise AssertionError(h.safe(f"target request left waiting: {row}"))
        if row.get("last_start_failure_detail") != detail:
            raise AssertionError(h.safe(f"the archive diagnostic changed: {row}"))
        done = (
            int(row["capacity_deferrals"]) >= recorded_capacity + 2
            and row["last_deferral_reason"] == "capacity"
        )
        return done, row

    waiting = h.poll(
        "two capacity deferrals after replacement", two_capacity_deferrals, timeout=600
    )
    marker = f"work-item-{uuid.UUID(target_id)}-"

    def capacity_lines(pod: str) -> int:
        return sum(
            1 for line in h.worker_logs(pod).splitlines() if CAPACITY_LOG in line and marker in line
        )

    h.poll(
        "two capacity log lines for the target in the replacement worker",
        lambda: ((count := capacity_lines(new_worker["name"])) >= 2, f"{count} lines"),
        timeout=120,
    )
    old_lines = capacity_lines(old_worker["name"])
    h.check(old_lines == 0, f"the old worker logged {old_lines} capacity deferrals for the target")

    # 6. The holder kept its runtime and sandbox; the target kept its identity.
    holder_now = h.internal(holder_id)
    h.check(
        holder_now["status"] == "running" and holder_now["runtime_epoch"] == holder_epoch,
        f"holder lost its runtime across replacement: {holder_now}",
    )
    pods_now = h.sandbox_pods()
    h.check(
        [pod["uid"] for pod in pods_now] == [holder_pod["uid"]],
        f"holder sandbox pod changed across replacement: {pods_now}",
    )
    h.check(h.used_pods() == "1", "sandbox quota used.pods is not 1 while the holder runs")
    h.check(
        waiting["id"] == target_id
        and waiting["wait_deadline"] == wait_deadline
        and waiting["started_at"] is None
        and h.start_deferrals(target_id) < START_DEFERRAL_LIMIT,
        f"target request identity, deadline or start budget changed: {waiting}",
    )

    # 7. The operator read names the capacity wait and the archive failure.
    item = h.public(f"/work-items/{target_item}")
    h.check(item["state"] == "waiting", f"work item state is {item['state']!r}, want waiting")
    public_request = next(
        (row for row in item["requests"] if str(row["sequence"]) == str(waiting["sequence"])),
        None,
    )
    h.check(public_request is not None, "the work item read lists no target request")
    h.check(
        public_request.get("last_start_failure_detail") == detail,
        "GET /work-items request last_start_failure_detail is "
        f"{public_request.get('last_start_failure_detail')!r}",
    )
    cause = item["actionable_cause"]
    capacity_text = "time(s) for capacity, last reason: capacity"
    previous_text = f"previous start failure: {ARCHIVE_DIAGNOSTIC}"
    h.check(
        "waiting for sandbox capacity" in cause
        and capacity_text in cause
        and previous_text in cause
        and cause.index(capacity_text) < cause.index(previous_text),
        f"actionable_cause does not name both the capacity wait and the archive failure: {cause}",
    )

    # 8. Release the holder; the target starts and its start clears the detail.
    h.set_label(HOLDER_ISSUE, False)
    # Every state awaited below is stable once reached (a terminal holder, its
    # pod gone, a running target parked on the held model), so the time this
    # snapshot takes cannot make an observation miss its deadline. The global
    # quota is not awaited at zero: the new worker may take the freed slot for
    # the target at once.
    h.snapshot("holder released")

    def holder_released() -> tuple[bool, Any]:
        row = h.internal(holder_id)
        return row["status"] != "running" and row.get("terminal_at") is not None, row

    h.poll("the holder request terminal", holder_released, timeout=600, interval=5)

    def holder_pod_gone() -> tuple[bool, Any]:
        pods = h.sandbox_pods()
        return all(pod["uid"] != holder_pod["uid"] for pod in pods), pods

    h.poll("the holder's sandbox pod deleted", holder_pod_gone, timeout=600, interval=5)

    def target_running() -> tuple[bool, Any]:
        row = h.internal(target_id)
        if row["status"] not in {"waiting", "running"}:
            raise AssertionError(h.safe(f"target request ended before it ran: {row}"))
        return row["status"] == "running", row

    started = h.poll("the target request running", target_running, timeout=900, interval=5)
    h.check(
        "last_start_failure_detail" in started and started["last_start_failure_detail"] is None,
        f"a started request still carries a start failure: {started}",
    )
    item = h.public(f"/work-items/{target_item}")
    public_request = next(
        (row for row in item["requests"] if str(row["sequence"]) == str(started["sequence"])),
        None,
    )
    h.check(
        public_request is not None and public_request.get("last_start_failure_detail") is None,
        f"GET /work-items still reports a start failure for the started request: {item}",
    )
    h.check(
        "previous start failure" not in item["actionable_cause"],
        f"a started work item is still described as failing preparation: {item}",
    )
