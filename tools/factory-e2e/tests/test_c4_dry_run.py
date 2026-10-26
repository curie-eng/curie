"""Dry runs plan only what they observed (#4626).

Gauntlet finding on released v0.13.0: `curie cluster up --dry-run` printed
`kubectl label namespace agent-sandbox-system ...` as an unconditional step,
and `curie factory quickstart --dry-run --context ...` printed an
unconditional `curie cluster up ... --model <default>` with a credential line
claiming the release had no real model. The live runs did neither: they reused
an existing unowned controller without stamping its namespace, skipped
`cluster up` for a release already at the target, and kept the recorded model.
A dry run stays offline, so every fact it does not read must be planned as a
condition, never as an action.

What is real here: the `curie` CLI built from this checkout, kind, kubectl,
Helm with the secret driver, the chart from the copied source, the candidate API
image built from this checkout, the release's Postgres, Valkey and RustFS pods,
and the vendored agent-sandbox controller applied the way an operator who
installed agent-sandbox upstream would apply it. What is a fixture: only the
GitLab HTTP peer (shapes cite the GitLab REST documents) and, for the offline
check, a TCP listener standing in for a Kubernetes API endpoint so any
connection attempt is counted.

Every cluster, namespace, PriorityClass, container and image tag is created by
a fixture that registers its cleanup first and verifies the resource is gone
afterwards. A sentinel container proves unrelated resources survive. A missing
prerequisite or an infrastructure failure is a fixture error, never a skip.
Infrastructure helpers raise `InfrastructureError` (a RuntimeError naming the
command and its redacted output), never AssertionError, so a failed probe
cannot pass for a behavioral failure. Product assertions live in test bodies.
The Docker engine is whatever the environment configures.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
PREFIX = "test-4626"
NAMESPACE = "test-4626-dry-run"
# Every release state, so a dry run that left a pending or failed release is
# still seen. Helm 4 removed `list --all`; these state flags exist in Helm 3
# and Helm 4 alike, and a bare `list` shows only deployed releases.
RELEASE_LIST = (
    "list",
    "--deployed",
    "--failed",
    "--pending",
    "--superseded",
    "--uninstalled",
    "--uninstalling",
    "-o",
    "json",
)
RELEASE = "test-4626"
FULLNAME = "test-4626"
RECORDED_MODEL = "acme-recorded-model"
EXPLICIT_MODEL = "acme-explicit-model"
DEFAULT_MODEL = "z-ai/glm-5.3-flash"
ANTHROPIC_DEFAULT_MODEL = "claude-sonnet-5-5"
FIRST_AGENT = "acme-first"
GITLAB_REPO = "acme-corp/platform/acme-bot"
SHARED_NAMESPACE = "agent-sandbox-system"
CONTROLLER = "agent-sandbox-controller"
OPERATOR_LABEL_KEY = "acme.example/owner"
OPERATOR_LABEL = f"{OPERATOR_LABEL_KEY}=platform-team"
CREATED_BY = "curietech.ai/created-by"
CREATED_IN = "curietech.ai/created-in"
SKIP_LINE = "skip cluster up: the release is already at the target chart and values"
SKIP_NOTE = "release already at the target chart and values; skipping cluster up"
CREDENTIAL_PREFIX = (
    "model credential: if the release records a real model, keep it (no prompt); otherwise"
)


def primary_lines(release: str, namespace: str) -> list[str]:
    """The conditional plan for the install namespace, in order."""
    lines = [
        f"# not read under --dry-run: whether namespace {namespace} exists and which "
        "release owns it; a live run checks it before helm runs",
        f"#   if namespace {namespace} does not exist: create it with labels "
        f"{CREATED_BY}={release} {CREATED_IN}={namespace}",
        f"#   if namespace {namespace} is not terminating and already has this release's "
        "created-by and created-in labels, or its adopted-by and adopted-in labels: use it "
        "and leave its labels unchanged",
    ]
    if namespace != SHARED_NAMESPACE:
        lines.append(
            f"#   if namespace {namespace} is not terminating and exists with no ownership "
            "or foreign labels and only its default objects: add labels "
            f"{CREATED_BY}={release} {CREATED_IN}={namespace}"
        )
    lines.append(f"#   otherwise: refuse to install into namespace {namespace}")
    return lines


def shared_lines(release: str, namespace: str, context: str) -> list[str]:
    """The conditional plan for the shared controller namespace, in order."""
    return [
        f"# not read under --dry-run: whether namespace {SHARED_NAMESPACE} and Deployment "
        f"{CONTROLLER} already exist; a live run checks them before helm runs",
        f"#   if namespace {SHARED_NAMESPACE} does not exist before helm runs and exists "
        f"after: kubectl --context {context} label namespace {SHARED_NAMESPACE} "
        f"{CREATED_BY}={release} {CREATED_IN}={namespace} --overwrite",
        f"#   if namespace {SHARED_NAMESPACE} already exists: use it and leave its labels "
        "unchanged",
    ]


REUSE_LINE = (
    f"#   if Deployment {CONTROLLER} already runs there and another release or no Helm "
    "release owns it: reuse it with agentSandbox.controller.deploy=false and leave its "
    "ownership metadata unchanged; an unowned controller that is unhealthy or runs an "
    "incompatible image is refused"
)
REFUSE_LINE = (
    f"#   if Deployment {CONTROLLER} already runs there and this release does not own it: "
    "refuse, because agentSandbox.controller.deploy=true was set"
)


def unread_release_lines(release: str, namespace: str, model: str, *, kept: bool) -> list[str]:
    """The conditional quickstart lines that precede `otherwise: curie cluster up`."""
    lines = [
        f"release {release} in namespace {namespace} was not read under --dry-run; a live "
        "run reads it and decides whether cluster up runs",
    ]
    if kept:
        lines += [
            "if the release already runs the target chart and values: skip cluster up and "
            "keep the model the release records",
            "if the release records a real model, cluster up keeps that model and its "
            f"credential in place of --model {model}",
        ]
    else:
        lines.append("if the release already runs the target chart and values: skip cluster up")
    return lines


class InfrastructureError(RuntimeError):
    """An unavailable prerequisite must be an error during fixture setup."""


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in list(env):
        if name.startswith(
            (
                "CURIE_",
                "ANTHROPIC_",
                "OPENROUTER_",
                "CLAUDE_",
                "GITHUB_",
                "GH_",
                "SLACK_",
            )
        ) or name in {
            "HELM_DRIVER",
            "KUBECONFIG",
        }:
            env.pop(name)
    env["HELM_DRIVER"] = "secret"
    env["NO_COLOR"] = "1"
    return env


def _redact(text: str, secrets: tuple[str, ...] = ()) -> str:
    for secret in secrets:
        text = text.replace(secret, "<generated-sentinel>")
    return re.sub(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        "<generated-private-key>",
        text,
        flags=re.DOTALL,
    )


def _command(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path,
    stdin: str | None = None,
    timeout: int = 300,
    check: bool = True,
    secrets: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        # Values and Secret reads can carry fixture credentials even on error.
        detail = (
            ""
            if "get" in argv and any(x in argv for x in ("values", "secret"))
            else (_redact(result.stdout + result.stderr, secrets)[-6000:])
        )
        raise InfrastructureError(f"{argv[0]} {argv[1]} failed ({result.returncode}): {detail}")
    return result


def _shown(result: subprocess.CompletedProcess[str], secrets: tuple[str, ...]) -> str:
    return _redact(result.stdout + result.stderr, secrets)[-8000:]


def _leaks(result: subprocess.CompletedProcess[str], secrets: tuple[str, ...]) -> list[int]:
    """Indexes of the secrets that appear in a result; never the values."""
    text = result.stdout + result.stderr
    return [index for index, secret in enumerate(secrets) if secret in text]


# `cluster up` generates fresh chart secrets on every invocation and masks
# each one by design (`key=<random prefix>***`), so two dry runs can only agree
# once the masked segment is normalized. Nothing else is loosened.
MASKED = re.compile(r"=[^\s,]+\*\*\*")


def _unmasked(plan: list[str]) -> list[str]:
    return [MASKED.sub("=***", line) for line in plan]


def _human_lines(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in result.stdout.splitlines() if line.strip()]


def _json_plan(result: subprocess.CompletedProcess[str]) -> list[str]:
    body = json.loads(result.stdout)
    assert isinstance(body, dict), "a dry run emits one JSON object"
    assert body.get("dry_run") is True, "the JSON dry run says dry_run: true"
    plan = body.get("plan")
    assert isinstance(plan, list), "the JSON dry run carries a plan list"
    assert all(isinstance(line, str) for line in plan), "plan lines are strings"
    return [str(line) for line in plan]


def _count(plan: list[str], line: str) -> int:
    return sum(1 for candidate in plan if candidate == line)


def _no_namespace_stamp(plan: list[str]) -> bool:
    return not any(
        re.match(r"kubectl\b.*\s(?:label|annotate)\s+namespace\b", line) for line in plan
    )


@dataclass
class Candidate:
    directory: Path
    source: Path
    binary: Path
    image: str
    env: dict[str, str]


@pytest.fixture(scope="module")
def source_candidate(request: pytest.FixtureRequest) -> Candidate:
    env = _clean_env()
    for tool in ("docker", "kind", "kubectl", "helm", "cargo", "git"):
        if shutil.which(tool) is None:
            raise InfrastructureError(f"#4626 runtime prerequisite missing: {tool}")
    _command(["docker", "info", "--format", "{{.ServerVersion}}"], env=env, cwd=ROOT)
    # Docker's configured engine is inherited. A verifier checkout can be in
    # /tmp, which is not shared with the remote Docker host.
    common_dir = Path(
        _command(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            env=env,
            cwd=ROOT,
        ).stdout.strip()
    )
    if not common_dir.is_absolute():
        raise InfrastructureError("git did not resolve the checkout's absolute common directory")
    shared = common_dir.parent / ".projects"
    shared.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=f"{PREFIX}-", dir=shared))
    image = f"curie-{PREFIX}-api:{directory.name}"
    image_started = False

    def cleanup() -> None:
        try:
            if image_started:
                found = _command(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                    env=env,
                    cwd=ROOT,
                    check=False,
                )
                if found.returncode == 0:
                    _command(["docker", "image", "rm", image], env=env, cwd=ROOT)
                    # Check the owned tag, not the image ID: another tag may
                    # hold identical content and legitimately keep that ID.
                    survived = _command(
                        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                        env=env,
                        cwd=ROOT,
                        check=False,
                    )
                    if survived.returncode == 0:
                        raise InfrastructureError("owned candidate API image tag survived removal")
        finally:
            shutil.rmtree(directory)

    request.addfinalizer(cleanup)
    source = directory / "source"
    shutil.copytree(
        ROOT,
        source,
        ignore=shutil.ignore_patterns(
            ".git",
            ".worktrees",
            ".projects",
            ".venv",
            "venv",
            "node_modules",
            "target",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".env",
        ),
    )
    env.update(
        {
            "CARGO_TARGET_DIR": str(directory / "cargo-target"),
            "CARGO_BUILD_JOBS": "2",
            "CURIE_CONFIG_DIR": str(directory / "curie-config"),
            "XDG_CONFIG_HOME": str(directory / "config"),
            "XDG_CACHE_HOME": str(directory / "cache"),
        }
    )
    _command(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(source / "cli/Cargo.toml"),
            "--bin",
            "curie",
        ],
        env=env,
        cwd=source,
        timeout=1800,
    )
    binary = directory / "cargo-target/debug/curie"
    _command([str(binary), "--version"], env=env, cwd=source)
    image_started = True
    _command(
        ["docker", "build", "-f", str(source / "apps/api/Dockerfile"), "-t", image, str(source)],
        env=env,
        cwd=source,
        timeout=1800,
    )
    return Candidate(directory, source, binary, image, env)


@pytest.fixture(autouse=True)
def candidate_curie(source_candidate: Candidate, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Override this directory's autouse shell double for this runtime module."""
    monkeypatch.setenv("CURIE_BIN", str(source_candidate.binary))
    return source_candidate.binary


class Forge:
    """A GitLab REST v4 peer; shapes follow the GitLab documents cited below."""

    def __init__(self) -> None:
        self.token = "fixture-" + uuid.uuid4().hex
        self.labels: set[str] = set()
        self.requests: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        forge = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                self.respond()

            def do_POST(self) -> None:
                self.respond()

            def respond(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length)) if length else {}
                with forge.lock:
                    status, payload = forge.dispatch(
                        self.command,
                        self.path,
                        dict(self.headers.items()),
                        body,
                    )
                raw = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        if self.thread.is_alive():
            self.server.shutdown()
            self.thread.join(timeout=10)
        self.server.server_close()

    def posts(self) -> list[str]:
        with self.lock:
            return [path for method, path in self.requests if method != "GET"]

    def dispatch(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: dict[str, Any],
    ) -> tuple[int, Any]:
        path = unquote(urlsplit(raw_path).path).rstrip("/")
        headers = {key.lower(): value for key, value in headers.items()}
        self.requests.append((method, path))
        # GitLab REST v4: version, token self, current user, projects, members, labels.
        # https://docs.gitlab.com/api/version/
        # https://docs.gitlab.com/api/personal_access_tokens/#get-single-personal-access-token
        # https://docs.gitlab.com/api/users/#get-the-current-user
        # https://docs.gitlab.com/api/projects/#get-a-single-project
        # https://docs.gitlab.com/api/members/
        # https://docs.gitlab.com/api/labels/
        if not path.startswith("/api/v4/"):
            return 404, {"message": "404 Not Found"}
        if headers.get("private-token") != self.token:
            return 401, {"message": "401 Unauthorized"}
        if path == "/api/v4/version":
            return 200, {"version": "17.11.0"}
        if path == "/api/v4/personal_access_tokens/self":
            return 200, {
                "active": True,
                "revoked": False,
                "scopes": ["api", "write_repository"],
                "expires_at": None,
            }
        if path == "/api/v4/user":
            return 200, {"id": 42, "username": "acme-factory", "state": "active"}
        if path == f"/api/v4/projects/{GITLAB_REPO}":
            return 200, {
                "id": 1234,
                "path_with_namespace": GITLAB_REPO,
                "default_branch": "main",
                "issues_enabled": True,
            }
        if path == "/api/v4/projects/1234/members/all/42":
            return 200, {"id": 42, "access_level": 40}
        if path == "/api/v4/projects/1234/labels":
            if method == "POST":
                name = body["name"]
                if name in self.labels:
                    return 409, {"message": "already exists"}
                self.labels.add(name)
                return 201, {"id": len(self.labels), "name": name}
            return 200, [
                {"id": i, "name": label} for i, label in enumerate(sorted(self.labels), start=1)
            ]
        return 404, {"message": "404 Not Found"}


def _bundle(directory: Path) -> Path:
    bundle = directory / "bundle"
    (bundle / ".claude-plugin").mkdir(parents=True)
    (bundle / ".claude-plugin/plugin.json").write_text(
        json.dumps(
            {
                "name": "acme-dry-run",
                "version": "0.1.0",
                "description": "Factory dry-run fixture",
            }
        )
    )
    (bundle / "AGENTS.md").write_text("Persist this bundle without starting a worker turn.\n")
    return bundle


class Installation:
    """One owned kind cluster with an unowned upstream controller, no release yet."""

    def __init__(self, candidate: Candidate, directory: Path) -> None:
        self.candidate = candidate
        self.directory = directory
        self.chart = str(candidate.source / "charts/curie")
        # One name per test invocation, with no reuse of another run's resources.
        self.cluster = f"{PREFIX}-{uuid.uuid4().hex[:8]}"
        self.context = f"kind-{self.cluster}"
        self.namespace = NAMESPACE
        self.release = RELEASE
        self.fullname = FULLNAME
        self.config_dir = directory / "curie-config"
        self.config_dir.mkdir()
        self.env = dict(candidate.env)
        self.env.update(
            {
                "KUBECONFIG": str(directory / "kubeconfig"),
                "CURIE_BIN": str(candidate.binary),
                "CURIE_CONFIG_DIR": str(self.config_dir),
                "XDG_CONFIG_HOME": str(directory / "config"),
            }
        )
        self.forge = Forge()
        self.model_credentials = "sk-ant-fixture-" + uuid.uuid4().hex
        # Only `cluster up` sees the model credential; quickstart runs without
        # one so the release's recorded model decides.
        self.up_env = dict(self.env, CURIE_CREDENTIALS=self.model_credentials)
        # The chart generates the API key; `read_api_key` adds it here after
        # the live install. CURIE_API_KEY stays unset so the CLI discovers it
        # from the release Secret, and the key is never written to disk.
        self.secrets: tuple[str, ...] = (
            self.forge.token,
            self.model_credentials,
        )
        self.cluster_started = False
        self.namespace_started = False
        self.unrelated: set[str] = set()
        self.sentinel_name = f"{PREFIX}-sentinel-" + uuid.uuid4().hex
        self.sentinel_label = f"curie.test.sentinel={self.sentinel_name}"
        self.sentinel_id: str | None = None
        self.http: httpx.Client | None = None
        self.values_path = directory / "values.json"
        self.token_path = directory / "token"
        self.bundle = directory / "bundle"
        self.controller_snapshot: dict[str, Any] = {}

    def run(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return _command(
            list(argv),
            env=self.env,
            cwd=self.candidate.source,
            secrets=self.secrets,
            **kwargs,
        )

    def kubectl(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run("kubectl", "--context", self.context, *argv, **kwargs)

    def helm(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run(
            "helm",
            "--kube-context",
            self.context,
            "--namespace",
            self.namespace,
            *argv,
            **kwargs,
        )

    def owned_containers(self) -> set[str]:
        return set(
            self.run(
                "docker",
                "ps",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label=io.x-k8s.kind.cluster={self.cluster}",
            ).stdout.split()
        )

    def setup(self) -> None:
        self.unrelated = set(self.run("docker", "ps", "--quiet", "--no-trunc").stdout.split())
        chart_values = yaml.safe_load(
            (self.candidate.source / "charts/curie/values.yaml").read_text()
        )
        self.sentinel_id = self.run(
            "docker",
            "run",
            "--detach",
            "--name",
            self.sentinel_name,
            "--label",
            self.sentinel_label,
            chart_values["valkey"]["image"],
            "sleep",
            "86400",
        ).stdout.strip()
        if self.sentinel_id in self.unrelated:
            raise InfrastructureError("sentinel ownership overlaps the pre-existing inventory")
        self.forge.start()
        self.token_path.write_text(self.forge.token)
        self.token_path.chmod(0o600)
        _bundle(self.directory)
        config = {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "nodes": [
                {
                    "role": "control-plane",
                    "extraPortMappings": [
                        {"containerPort": 30080, "hostPort": 0, "listenAddress": "127.0.0.1"},
                    ],
                }
            ],
        }
        config_path = self.directory / "kind.json"
        config_path.write_text(json.dumps(config))
        for _attempt in range(8):
            if self.owned_containers():
                self.cluster = f"{PREFIX}-{uuid.uuid4().hex[:8]}"
                self.context = f"kind-{self.cluster}"
                continue
            self.cluster_started = True
            created = self.run(
                "kind",
                "create",
                "cluster",
                "--name",
                self.cluster,
                "--kubeconfig",
                self.env["KUBECONFIG"],
                "--config",
                str(config_path),
                "--wait",
                "180s",
                timeout=300,
                check=False,
            )
            if created.returncode == 0:
                break
            detail = created.stdout + created.stderr
            if "node(s) already exist for a cluster with the name" in detail:
                self.cluster_started = False
                self.cluster = f"{PREFIX}-{uuid.uuid4().hex[:8]}"
                self.context = f"kind-{self.cluster}"
                continue
            raise InfrastructureError(
                "kind create cluster failed: " + _redact(detail, self.secrets)
            )
        else:
            raise InfrastructureError("could not allocate an unused #4626 kind cluster name")
        # The live run creates the install namespace and the PriorityClasses;
        # cleanup deletes them with --ignore-not-found either way.
        self.namespace_started = True
        self.run("kind", "load", "docker-image", self.candidate.image, "--name", self.cluster)
        mapping = self.run(
            "docker",
            "port",
            f"{self.cluster}-control-plane",
            "30080/tcp",
        ).stdout.strip()
        port = int(mapping.rsplit(":", 1)[1])
        self.env["CURIE_API_URL"] = f"http://127.0.0.1:{port}"
        self.up_env["CURIE_API_URL"] = self.env["CURIE_API_URL"]
        self.plant_unowned_controller()
        self.values_path.write_text(json.dumps(self.values()))

    def plant_unowned_controller(self) -> None:
        """Install agent-sandbox the way an operator following upstream would."""
        source = self.candidate.source / "charts/curie"
        self.kubectl("apply", "--server-side", "-f", str(source / "crds"), timeout=180)
        self.kubectl(
            "apply",
            "--server-side",
            "-f",
            str(source / "files/agent-sandbox/controller.yaml"),
            timeout=180,
        )
        self.kubectl(
            "-n",
            SHARED_NAMESPACE,
            "rollout",
            "status",
            f"deployment/{CONTROLLER}",
            "--timeout=300s",
            timeout=330,
        )
        # Operator metadata the live run must leave in place.
        self.kubectl("label", "namespace", SHARED_NAMESPACE, OPERATOR_LABEL)
        self.kubectl("-n", SHARED_NAMESPACE, "label", "deployment", CONTROLLER, OPERATOR_LABEL)
        self.controller_snapshot = self.controller_metadata()
        deployment = self.controller_snapshot["deployment"]
        namespace = self.controller_snapshot["namespace"]
        if deployment["labels"].get("app.kubernetes.io/managed-by") == "Helm" or any(
            key.startswith("meta.helm.sh/") for key in deployment["annotations"]
        ):
            raise InfrastructureError("the planted controller already carries Helm ownership")
        if CREATED_BY in namespace["labels"] or CREATED_IN in namespace["labels"]:
            raise InfrastructureError("the planted controller namespace already carries ownership")
        if deployment["labels"].get(OPERATOR_LABEL_KEY) != "platform-team":
            raise InfrastructureError("the operator label did not reach the controller")

    def controller_metadata(self) -> dict[str, Any]:
        def metadata(*argv: str) -> dict[str, Any]:
            item = json.loads(self.kubectl(*argv, "-o", "json").stdout)
            meta = item.get("metadata", {})
            return {
                "labels": dict(meta.get("labels") or {}),
                "annotations": dict(meta.get("annotations") or {}),
            }

        return {
            "namespace": metadata("get", "namespace", SHARED_NAMESPACE),
            "deployment": metadata("-n", SHARED_NAMESPACE, "get", "deployment", CONTROLLER),
        }

    def namespace_labels(self, name: str) -> dict[str, str] | None:
        found = self.kubectl("get", "namespace", name, "--ignore-not-found=true", "-o", "json")
        if not found.stdout.strip():
            return None
        labels = json.loads(found.stdout).get("metadata", {}).get("labels") or {}
        return {str(key): str(value) for key, value in labels.items()}

    def values(self) -> dict[str, Any]:
        repository = self.candidate.image.rsplit(":", 1)[0]
        tag = self.candidate.image.rsplit(":", 1)[1]
        # No agentSandbox.controller key: the live run must infer reuse. No
        # model, fakeModel or credentials: `cluster up` sets them. No factory
        # section: the chart refuses factory.enabled without a code host, and
        # the GitLab quickstart writes its own.
        return {
            "fullnameOverride": self.fullname,
            "api": {
                "image": {
                    "repository": repository,
                    "tag": tag,
                    "digest": "",
                    "pullPolicy": "Never",
                },
                "commitPollIntervalSeconds": 0,
                "service": {"type": "NodePort", "port": 8000, "nodePort": 30080},
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": "1", "memory": "1Gi"},
                },
            },
            "worker": {"deploy": False},
            "dispatcher": {"deploy": False},
            "ui": {"deploy": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "postgres": {"persistence": {"enabled": False}},
            "valkey": {"persistence": {"enabled": False}},
            "rustfs": {
                "persistence": {"enabled": False},
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "1", "memory": "2Gi"},
                },
            },
            "agentSandbox": {
                "deploy": False,
                "runner": {"prewarm": {"enabled": False}},
            },
            "priorityClasses": {
                "platform": {"create": True, "name": f"{PREFIX}-platform"},
                "sandbox": {"create": True, "name": f"{PREFIX}-sandbox"},
            },
            "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
        }

    def cluster_up_argv(self, *extra: str, json_output: bool = False) -> list[str]:
        prefix = [str(self.candidate.binary)] + (["--json"] if json_output else [])
        return [
            *prefix,
            "cluster",
            "up",
            "--context",
            self.context,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
            "--chart",
            self.chart,
            "--model",
            RECORDED_MODEL,
            "--values-file",
            str(self.values_path),
            *extra,
        ]

    def cluster_up(
        self, *extra: str, json_output: bool = False
    ) -> subprocess.CompletedProcess[str]:
        return _command(
            self.cluster_up_argv(*extra, json_output=json_output),
            env=self.up_env,
            cwd=self.candidate.source,
            secrets=self.secrets,
            timeout=300,
            check=False,
        )

    def live_cluster_up(self) -> subprocess.CompletedProcess[str]:
        result = _command(
            self.cluster_up_argv(),
            env=self.up_env,
            cwd=self.candidate.source,
            secrets=self.secrets,
            timeout=900,
            check=False,
        )
        if result.returncode:
            return result
        api_key = self.read_api_key()
        self.http = httpx.Client(
            base_url=self.env["CURIE_API_URL"],
            headers={"X-API-Key": api_key},
            timeout=30,
            trust_env=False,
        )
        deadline = time.monotonic() + 120
        while True:
            try:
                response = self.http.get("/ready")
                if response.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() >= deadline:
                raise InfrastructureError("candidate API did not become ready through kind")
            time.sleep(0.25)
        return result

    def read_api_key(self) -> str:
        """The chart-generated API key, held in memory only and redacted."""
        name = self.kubectl(
            "-n",
            self.namespace,
            "get",
            "deployment",
            f"{self.fullname}-api",
            "-o",
            "jsonpath={.spec.template.spec.containers[*].env[?(@.name=='API_KEY')]"
            ".valueFrom.secretKeyRef.name}",
        ).stdout.strip()
        if not name:
            raise InfrastructureError("the API Deployment names no API_KEY Secret")
        encoded = self.kubectl(
            "-n",
            self.namespace,
            "get",
            "secret",
            name,
            "-o",
            "jsonpath={.data.apiKey}",
        ).stdout.strip()
        if not encoded:
            raise InfrastructureError("the API key Secret has no apiKey entry")
        api_key = base64.b64decode(encoded).decode()
        if not api_key:
            raise InfrastructureError("the API key Secret holds an empty apiKey")
        self.secrets = (*self.secrets, api_key)
        return api_key

    def quickstart_argv(self, *extra: str, json_output: bool = False) -> list[str]:
        prefix = [str(self.candidate.binary)] + (["--json"] if json_output else [])
        return [
            *prefix,
            "factory",
            "quickstart",
            "--code-host",
            "gitlab",
            "--tracker",
            "gitlab",
            "--context",
            self.context,
            "--namespace",
            self.namespace,
            "--release",
            self.release,
            "--agent",
            FIRST_AGENT,
            "--repo",
            GITLAB_REPO,
            "--token-file",
            str(self.token_path),
            "--forge-url",
            self.forge.url,
            "--plugin-dir",
            str(self.bundle),
            "--chart",
            self.chart,
            *extra,
        ]

    def quickstart(
        self, *extra: str, json_output: bool = False, timeout: int = 300
    ) -> subprocess.CompletedProcess[str]:
        return self.run(
            *self.quickstart_argv(*extra, json_output=json_output),
            check=False,
            timeout=timeout,
        )

    def release_values(self) -> dict[str, Any]:
        values = json.loads(self.helm("get", "values", self.release, "-o", "json").stdout)
        if not isinstance(values, dict):
            raise InfrastructureError("Helm did not return a release values object")
        return values

    def releases(self) -> list[dict[str, Any]]:
        listed = json.loads(self.helm(*RELEASE_LIST).stdout or "[]")
        if not isinstance(listed, list):
            raise InfrastructureError("Helm did not return a release list")
        return [entry for entry in listed if isinstance(entry, dict)]

    def history_length(self) -> int:
        history = json.loads(self.helm("history", self.release, "-o", "json").stdout)
        if not isinstance(history, list):
            raise InfrastructureError("Helm did not return a release history list")
        return len(history)

    def deployed_chart(self) -> str:
        matches = [entry for entry in self.releases() if entry.get("name") == self.release]
        if len(matches) != 1:
            raise InfrastructureError("Helm did not list exactly one owned release")
        return str(matches[0].get("chart"))

    def close(self) -> None:
        def verify_namespace() -> None:
            namespaces = json.loads(self.kubectl("get", "namespaces", "-o", "json").stdout)
            if any(x["metadata"]["name"] == self.namespace for x in namespaces["items"]):
                raise InfrastructureError("owned test namespace survived deletion")

        def delete_priority_class(name: str) -> None:
            self.kubectl("delete", "priorityclass", name, "--ignore-not-found=true")

        def verify_priority_class(name: str) -> None:
            if self.kubectl(
                "get",
                "priorityclass",
                name,
                "--ignore-not-found=true",
                "-o",
                "name",
            ).stdout.strip():
                raise InfrastructureError("owned PriorityClass survived deletion")

        def verify_cluster() -> None:
            if self.owned_containers():
                raise InfrastructureError("owned kind containers survived deletion")
            if self.cluster in self.run("kind", "get", "clusters").stdout.split():
                raise InfrastructureError("owned kind cluster survived deletion")

        def verify_sentinel() -> None:
            if self.sentinel_id is not None:
                running = self.run(
                    "docker",
                    "inspect",
                    "--format",
                    "{{.State.Running}}",
                    self.sentinel_id,
                ).stdout.strip()
                if running != "true":
                    raise InfrastructureError("unrelated sentinel did not survive kind cleanup")

        def sentinel_containers() -> list[str]:
            return self.run(
                "docker",
                "ps",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label={self.sentinel_label}",
            ).stdout.split()

        def delete_sentinel() -> None:
            owned = sentinel_containers()
            if owned:
                self.run("docker", "rm", "--force", *owned)

        def verify_sentinel_removed() -> None:
            if sentinel_containers():
                raise InfrastructureError("owned sentinel survived removal")

        operations: list[Callable[[], Any]] = []
        if self.http is not None:
            operations.append(self.http.close)
        operations.append(self.forge.close)
        if self.cluster_started:
            if self.namespace_started:
                # The Helm release lives in this namespace, so deleting it
                # removes the release record with it.
                operations.extend(
                    [
                        lambda: self.kubectl(
                            "delete",
                            "namespace",
                            self.namespace,
                            "--ignore-not-found=true",
                            "--wait=true",
                            "--timeout=120s",
                            timeout=150,
                        ),
                        verify_namespace,
                    ]
                )
                for name in (f"{PREFIX}-platform", f"{PREFIX}-sandbox"):
                    operations.extend(
                        [
                            partial(delete_priority_class, name),
                            partial(verify_priority_class, name),
                        ]
                    )
            operations.extend(
                [
                    lambda: self.run(
                        "kind", "delete", "cluster", "--name", self.cluster, timeout=180
                    ),
                    verify_cluster,
                ]
            )
        operations.extend([verify_sentinel, delete_sentinel, verify_sentinel_removed])
        # ExitStack attempts every registered cleanup even when an earlier
        # deletion or preservation check raises.
        with ExitStack() as cleanup:
            for operation in reversed(operations):
                cleanup.callback(operation)


@pytest.fixture
def install(request: pytest.FixtureRequest, source_candidate: Candidate) -> Installation:
    directory = Path(tempfile.mkdtemp(prefix="install-", dir=source_candidate.directory))
    instance = Installation(source_candidate, directory)
    request.addfinalizer(instance.close)
    instance.setup()
    return instance


@pytest.fixture
def forge() -> Iterator[Forge]:
    instance = Forge()
    try:
        instance.start()
        yield instance
    finally:
        instance.close()


class ConnectionRecorder:
    """A TCP listener standing in for a Kubernetes API; it only counts accepts."""

    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(16)
        self.socket.settimeout(0.2)
        self.port = int(self.socket.getsockname()[1])
        self.accepted = 0
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)

    def serve(self) -> None:
        while not self.stopping.is_set():
            try:
                connection, _address = self.socket.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.accepted += 1
            connection.close()

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stopping.set()
        self.thread.join(timeout=5)
        self.socket.close()
        if self.thread.is_alive():
            raise InfrastructureError("the connection recorder did not stop")


@pytest.fixture
def recorder() -> Iterator[ConnectionRecorder]:
    instance = ConnectionRecorder()
    try:
        instance.start()
        yield instance
    finally:
        instance.close()


def _assert_same_lines(
    human: subprocess.CompletedProcess[str],
    machine: subprocess.CompletedProcess[str],
    secrets: tuple[str, ...],
) -> list[str]:
    assert human.returncode == 0, _shown(human, secrets)
    assert machine.returncode == 0, _shown(machine, secrets)
    plan = _json_plan(machine)
    same = _unmasked(_human_lines(human)) == _unmasked(plan)
    assert same, "human and JSON dry runs must carry the same lines:\n" + _shown(human, secrets)
    return plan


def test_release_listing_parses_on_the_installed_helm(tmp_path: Path) -> None:
    """The listing that proves a dry run installed nothing parses on this helm.

    An unreachable context makes helm fail after flag parsing, so a refusal
    other than an unknown flag shows the arguments themselves are accepted.
    """
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("helm is not installed")
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("")
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "KUBECONFIG": str(kubeconfig),
        "HELM_DRIVER": "secret",
    }
    result = subprocess.run(
        [helm, "--kube-context", f"{PREFIX}-absent", "--namespace", NAMESPACE, *RELEASE_LIST],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert "unknown flag" not in result.stderr, result.stderr
    assert "unknown shorthand flag" not in result.stderr, result.stderr
    assert result.returncode != 0, "helm listed releases without a reachable cluster"


def test_offline_plan_does_not_claim_unobserved_live_actions(install: Installation) -> None:
    secrets = install.secrets

    # A. cluster up over an existing, unowned, compatible, healthy controller.
    human = install.cluster_up("--dry-run")
    machine = install.cluster_up("--dry-run", json_output=True)
    plan = _assert_same_lines(human, machine, secrets)
    for result in (human, machine):
        assert not _leaks(result, secrets), "a dry run printed a fixture credential"
    listing = "\n".join(plan)
    assert _no_namespace_stamp(plan), "a dry run planned an unobserved stamp:\n" + listing
    for line in shared_lines(RELEASE, NAMESPACE, install.context) + [REUSE_LINE]:
        assert _count(plan, line) == 1, f"expected exactly once: {line}\n" + "\n".join(plan)
    assert _count(plan, REFUSE_LINE) == 0, "\n".join(plan)
    for line in primary_lines(RELEASE, NAMESPACE):
        assert _count(plan, line) == 1, f"expected exactly once: {line}\n" + "\n".join(plan)

    # The dry runs changed nothing in the cluster.
    assert install.releases() == [], "a dry run installed a Helm release"
    assert install.namespace_labels(NAMESPACE) is None, "a dry run created the namespace"
    assert install.controller_metadata() == install.controller_snapshot

    live = install.live_cluster_up()
    assert live.returncode == 0, _shown(live, secrets)
    secrets = install.secrets
    assert not _leaks(live, secrets), "the live run printed a fixture credential"
    after = install.controller_metadata()
    deployment = after["deployment"]
    assert "app.kubernetes.io/managed-by" not in deployment["labels"]
    assert not [key for key in deployment["annotations"] if key.startswith("meta.helm.sh/")]
    assert deployment == install.controller_snapshot["deployment"]
    namespace = after["namespace"]
    assert CREATED_BY not in namespace["labels"]
    assert CREATED_IN not in namespace["labels"]
    assert namespace["labels"].get(OPERATOR_LABEL_KEY) == "platform-team"
    assert namespace["labels"] == install.controller_snapshot["namespace"]["labels"]
    values = install.release_values()
    assert values.get("agentSandbox", {}).get("controller", {}).get("deploy") is False
    runner = values.get("agentSandbox", {}).get("runner", {})
    assert runner.get("model") == RECORDED_MODEL
    assert runner.get("fakeModel") is False
    primary = install.namespace_labels(NAMESPACE)
    assert primary is not None, "the live run did not create the install namespace"
    assert primary.get(CREATED_BY) == RELEASE
    assert primary.get(CREATED_IN) == NAMESPACE

    # B. GitLab quickstart on the existing release with a recorded real model.
    # The environment carries no model credential and the config dir is empty.
    recorded_credential = runner.get("credentials")
    recorded_present = recorded_credential == install.model_credentials
    assert recorded_present, "cluster up did not record the fixture model credential"
    revisions = install.history_length()
    chart = install.deployed_chart()
    posts = len(install.forge.posts())
    human = install.quickstart("--dry-run")
    machine = install.quickstart("--dry-run", json_output=True)
    plan = _assert_same_lines(human, machine, secrets)
    for result in (human, machine):
        assert not _leaks(result, secrets), "a dry run printed a fixture credential"
    assert SKIP_LINE not in plan, "an unread release must not be claimed at target"
    ups = [line for line in plan if "curie cluster up" in line]
    assert len(ups) == 1, "\n".join(plan)
    assert ups[0].startswith("otherwise: curie cluster up "), "\n".join(plan)
    assert f"--model {DEFAULT_MODEL}" in ups[0], ups[0]
    conditional = unread_release_lines(RELEASE, NAMESPACE, DEFAULT_MODEL, kept=True)
    start = plan.index(conditional[0]) if conditional[0] in plan else -1
    assert start >= 0, "\n".join(plan)
    assert plan[start : start + len(conditional) + 1] == [*conditional, ups[0]], "\n".join(plan)
    credential = [line for line in plan if line.startswith("model credential: ")]
    assert len(credential) == 1, "\n".join(plan)
    assert credential[0].startswith(CREDENTIAL_PREFIX), credential[0]
    assert install.history_length() == revisions, "a dry run wrote a Helm revision"
    assert len(install.forge.posts()) == posts, "a dry run wrote to the forge"
    assert install.controller_metadata() == after

    live = install.quickstart(timeout=900)
    assert live.returncode == 0, _shown(live, secrets)
    assert not _leaks(live, secrets), "the live quickstart printed a fixture credential"
    assert SKIP_NOTE in live.stderr, _shown(live, secrets)
    values = install.release_values()
    runner = values.get("agentSandbox", {}).get("runner", {})
    assert runner.get("model") == RECORDED_MODEL
    assert runner.get("fakeModel") is False
    unchanged = runner.get("credentials") == recorded_credential
    assert unchanged, "the live quickstart replaced the recorded model credential"
    assert install.deployed_chart() == chart


def test_explicit_model_plans_conditional_install_without_keeping_recorded_model(
    install: Installation,
) -> None:
    secrets = install.secrets
    live = install.live_cluster_up()
    assert live.returncode == 0, _shown(live, secrets)
    secrets = install.secrets
    revisions = install.history_length()
    result = install.quickstart("--model", EXPLICIT_MODEL, "--dry-run", json_output=True)
    assert result.returncode == 0, _shown(result, secrets)
    assert not _leaks(result, secrets), "a dry run printed a fixture credential"
    plan = _json_plan(result)
    conditional = unread_release_lines(RELEASE, NAMESPACE, EXPLICIT_MODEL, kept=False)
    for line in conditional:
        assert _count(plan, line) == 1, f"expected exactly once: {line}\n" + "\n".join(plan)
    assert not [line for line in plan if "keeps that model" in line], "\n".join(plan)
    assert not [line for line in plan if "keep the model the release records" in line]
    assert SKIP_LINE not in plan
    ups = [line for line in plan if "curie cluster up" in line]
    assert len(ups) == 1, "\n".join(plan)
    assert ups[0].startswith("otherwise: curie cluster up "), ups[0]
    assert f"--model {EXPLICIT_MODEL}" in ups[0], ups[0]
    assert install.history_length() == revisions, "a dry run wrote a Helm revision"


def test_fresh_install_plan_is_unconditional(
    request: pytest.FixtureRequest, tmp_path: Path, source_candidate: Candidate, forge: Forge
) -> None:
    kind_name = f"{PREFIX}-{uuid.uuid4().hex[:8]}"
    env = _clean_env()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        'apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\ncurrent-context: ""\n'
    )
    config_dir = tmp_path / "curie-config"
    config_dir.mkdir()
    credential = "sk-ant-fixture-" + uuid.uuid4().hex
    env.update(
        {
            "KUBECONFIG": str(kubeconfig),
            "CURIE_CONFIG_DIR": str(config_dir),
            "XDG_CONFIG_HOME": str(tmp_path / "config"),
            "CURIE_CREDENTIALS": credential,
        }
    )
    secrets = (credential, forge.token)
    token = tmp_path / "token"
    token.write_text(forge.token)
    token.chmod(0o600)
    bundle = _bundle(tmp_path)

    def kind_clusters() -> list[str]:
        return _command(["kind", "get", "clusters"], env=env, cwd=tmp_path).stdout.split()

    if kind_name in kind_clusters():
        raise InfrastructureError("the generated kind cluster name is already in use")

    def remove_accidental_cluster() -> None:
        # Registered before the dry run, so a cluster it should never create
        # cannot outlive a failed assertion.
        if kind_name in kind_clusters():
            _command(
                ["kind", "delete", "cluster", "--name", kind_name],
                env=env,
                cwd=tmp_path,
                timeout=180,
            )
        if kind_name in kind_clusters():
            raise InfrastructureError("the accidental kind cluster survived deletion")

    request.addfinalizer(remove_accidental_cluster)
    result = _command(
        [
            str(source_candidate.binary),
            "--json",
            "factory",
            "quickstart",
            "--code-host",
            "gitlab",
            "--tracker",
            "gitlab",
            "--kind-name",
            kind_name,
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
            "--agent",
            FIRST_AGENT,
            "--repo",
            GITLAB_REPO,
            "--token-file",
            str(token),
            "--forge-url",
            forge.url,
            "--plugin-dir",
            str(bundle),
            "--chart",
            str(source_candidate.source / "charts/curie"),
            "--dry-run",
        ],
        env=env,
        cwd=source_candidate.source,
        secrets=secrets,
        check=False,
    )
    assert result.returncode == 0, _shown(result, secrets)
    assert not _leaks(result, secrets), "a dry run printed a fixture credential"
    plan = _json_plan(result)
    creates = [line for line in plan if line.startswith(f"kind create cluster --name {kind_name}")]
    assert creates, "\n".join(plan)
    ups = [line for line in plan if line.startswith("curie cluster up")]
    assert len(ups) == 1, "\n".join(plan)
    assert f"--model {ANTHROPIC_DEFAULT_MODEL}" in ups[0], ups[0]
    assert not [line for line in plan if "not read under --dry-run" in line], "\n".join(plan)
    assert not [line for line in plan if line.startswith("otherwise:")], "\n".join(plan)
    assert not forge.posts(), "a dry run wrote to the forge"
    assert kind_name not in kind_clusters(), "a dry run created a kind cluster"


def test_cluster_up_dry_run_is_offline_and_honors_disabled_controller(
    tmp_path: Path, source_candidate: Candidate, recorder: ConnectionRecorder
) -> None:
    context = f"{PREFIX}-offline"
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "name": context,
                        "cluster": {
                            "server": f"https://127.0.0.1:{recorder.port}",
                            "insecure-skip-tls-verify": True,
                        },
                    }
                ],
                "contexts": [
                    {
                        "name": context,
                        "context": {"cluster": context, "user": context, "namespace": "default"},
                    }
                ],
                # No credential: the endpoint only counts connections, and a
                # kubeconfig on disk must not hold a secret.
                "users": [{"name": context, "user": {}}],
                "current-context": context,
            }
        )
    )
    config_dir = tmp_path / "curie-config"
    config_dir.mkdir()
    credential = "sk-ant-fixture-" + uuid.uuid4().hex
    env = _clean_env()
    env.update(
        {
            "KUBECONFIG": str(kubeconfig),
            "CURIE_CONFIG_DIR": str(config_dir),
            "XDG_CONFIG_HOME": str(tmp_path / "config"),
            "CURIE_CREDENTIALS": credential,
        }
    )
    secrets = (credential,)

    def plan_for(*extra: str) -> list[str]:
        result = _command(
            [
                str(source_candidate.binary),
                "--json",
                "cluster",
                "up",
                "--context",
                context,
                "--namespace",
                NAMESPACE,
                "--release",
                RELEASE,
                "--chart",
                str(ROOT / "charts/curie"),
                "--dry-run",
                *extra,
            ],
            env=env,
            cwd=tmp_path,
            secrets=secrets,
            check=False,
        )
        assert result.returncode == 0, _shown(result, secrets)
        assert not _leaks(result, secrets), "a dry run printed a fixture credential"
        return _json_plan(result)

    plain = plan_for()
    disabled = plan_for("--set", "agentSandbox.controller.deploy=false")
    enabled = plan_for("--set", "agentSandbox.controller.deploy=true")
    assert recorder.accepted == 0, "a dry run connected to the Kubernetes API endpoint"

    for plan in (plain, disabled, enabled):
        assert _no_namespace_stamp(plan), "\n".join(plan)
        for line in primary_lines(RELEASE, NAMESPACE):
            assert _count(plan, line) == 1, f"expected exactly once: {line}\n" + "\n".join(plan)

    for line in shared_lines(RELEASE, NAMESPACE, context) + [REUSE_LINE]:
        assert _count(plain, line) == 1, f"expected exactly once: {line}\n" + "\n".join(plain)
    assert _count(plain, REFUSE_LINE) == 0, "\n".join(plain)

    assert not [line for line in disabled if SHARED_NAMESPACE in line], "\n".join(disabled)

    for line in shared_lines(RELEASE, NAMESPACE, context) + [REFUSE_LINE]:
        assert _count(enabled, line) == 1, f"expected exactly once: {line}\n" + "\n".join(enabled)
    assert _count(enabled, REUSE_LINE) == 0, "\n".join(enabled)
