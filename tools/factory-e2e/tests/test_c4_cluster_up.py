"""#4625: three fresh installs through the candidate CLI and actual Curie chart."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import uuid
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
KIND_NODE_IMAGE = (
    "kindest/node:v1.31.0@sha256:"
    "25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93"
)
WORKLOAD_KINDS = {"Deployment", "StatefulSet", "DaemonSet"}
CLUSTER_KINDS = {"PriorityClass", "CustomResourceDefinition"}
NAMESPACED_KINDS = {
    "ConfigMap",
    "Deployment",
    "Job",
    "NetworkPolicy",
    "PersistentVolumeClaim",
    "Pod",
    "PodDisruptionBudget",
    "ResourceQuota",
    "Role",
    "RoleBinding",
    "Secret",
    "Service",
    "ServiceAccount",
    "StatefulSet",
}
CLUSTER_INVENTORY = (
    "namespaces,priorityclasses,customresourcedefinitions,clusterroles,"
    "clusterrolebindings,runtimeclasses"
)


class InfrastructureError(RuntimeError):
    """Fixture prerequisites and cleanup failures must remain pytest errors."""


def _command(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path,
    stdin: str | None = None,
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        with subprocess.Popen(
            argv,
            env=env,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        ) as process:
            try:
                stdout, stderr = process.communicate(input=stdin, timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
                raise InfrastructureError(
                    f"{Path(argv[0]).name} exceeded {timeout}s; its process group was stopped"
                ) from None
            if process.returncode is None:
                raise InfrastructureError(f"{Path(argv[0]).name} did not report an exit code")
            result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    except OSError:
        raise InfrastructureError(
            f"{Path(argv[0]).name} could not execute"
        ) from None
    if check and result.returncode:
        # Stdout is withheld because Helm output can contain generated secrets.
        tail = "\n".join((result.stderr or "").strip().splitlines()[-20:])[-4000:].strip()
        raise InfrastructureError(
            f"{Path(argv[0]).name} exited {result.returncode}; stderr tail: {tail}"
        )
    return result


def _environment(directory: Path) -> dict[str, str]:
    transport = {
        "PATH",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "SSH_AUTH_SOCK",
        "SSH_AGENT_PID",
        "KIND_REAL",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    }
    env = {name: value for name, value in os.environ.items() if name in transport}
    original_home = Path.home()
    wrapped_kind = original_home / ".local/lib/kind/kind"
    if "KIND_REAL" not in env and wrapped_kind.is_file():
        env["KIND_REAL"] = str(wrapped_kind)
    env.update(
        {
            "HOME": str(directory / "home"),
            "CARGO_HOME": os.environ.get("CARGO_HOME", str(original_home / ".cargo")),
            "RUSTUP_HOME": os.environ.get("RUSTUP_HOME", str(original_home / ".rustup")),
            "DOCKER_CONFIG": os.environ.get("DOCKER_CONFIG", str(original_home / ".docker")),
            "CURIE_CONFIG_DIR": str(directory / "curie-config"),
            "XDG_CONFIG_HOME": str(directory / "config"),
            "XDG_CACHE_HOME": str(directory / "cache"),
            "XDG_DATA_HOME": str(directory / "data"),
            "XDG_STATE_HOME": str(directory / "state"),
            "HELM_CONFIG_HOME": str(directory / "helm-config"),
            "HELM_CACHE_HOME": str(directory / "helm-cache"),
            "HELM_DATA_HOME": str(directory / "helm-data"),
            "HELM_DRIVER": "secret",
            "KUBECONFIG": str(directory / "kubeconfig"),
            "TMPDIR": str(directory / "tmp"),
            "CURIE_BUILD_CHANNEL": "dev",
            "CARGO_BUILD_JOBS": "2",
            "NO_COLOR": "1",
            "TERM": "dumb",
        }
    )
    for name in ("HOME", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME"):
        Path(env[name]).mkdir(parents=True)
    return env


def _json(result: subprocess.CompletedProcess[str]) -> Any:
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def _sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@dataclass
class Candidate:
    directory: Path
    source: Path
    binary: Path
    image: str
    image_id: str
    env: dict[str, str] = field(repr=False)


@pytest.fixture(scope="module")
def source_candidate(request: pytest.FixtureRequest) -> Candidate:
    for tool in ("docker", "kind", "kubectl", "helm", "cargo", "python3"):
        if shutil.which(tool) is None:
            raise InfrastructureError(f"#4625 runtime prerequisite missing: {tool}")
    shared = ROOT / ".projects"
    if not shared.resolve().is_relative_to(ROOT):
        raise InfrastructureError("private fixture storage must stay inside the candidate checkout")
    shared.mkdir(exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="test-4625-", dir=shared))
    env: dict[str, str] = {}
    image = f"curie-test-4625-api:{uuid.uuid4().hex}"
    image_started = False

    def cleanup() -> None:
        try:
            if image_started:
                images = _command(
                    ["docker", "image", "ls", "--quiet", "--no-trunc", image],
                    env=env,
                    cwd=ROOT,
                ).stdout.split()
                if images:
                    _command(["docker", "image", "rm", image], env=env, cwd=ROOT)
                remaining = _command(
                    ["docker", "image", "ls", "--quiet", image], env=env, cwd=ROOT
                ).stdout.strip()
                if remaining:
                    raise InfrastructureError("owned candidate API image tag survived cleanup")
        finally:
            shutil.rmtree(directory)

    request.addfinalizer(cleanup)
    env = _environment(directory)
    _command(["docker", "info", "--format", "{{.ServerVersion}}"], env=env, cwd=ROOT)
    source = directory / "source"
    excluded = shutil.ignore_patterns(
        ".git",
        ".projects",
        ".worktrees",
        ".venv",
        "venv",
        "node_modules",
        "target",
        "dist",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".coverage*",
        "htmlcov",
        "test-results",
        "playwright-report",
        ".env*",
        ".ssh",
        ".aws",
        ".curie",
        ".claude",
        ".claude.json*",
        ".agents",
        ".codex",
        ".t3",
        ".cache",
        ".config",
        ".local",
        ".mcp.json",
        ".netrc",
        "*.pem",
        "*.key",
        "*.p12",
        "*.pfx",
    )

    def snapshot_ignore(parent: str, names: list[str]) -> set[str]:
        return excluded(parent, names) | {
            name for name in names if (Path(parent) / name).is_symlink()
        }

    shutil.copytree(ROOT, source, ignore=snapshot_ignore)
    source_hash = hashlib.sha256()
    for path in sorted(source.rglob("*")):
        if path.is_file():
            source_hash.update(path.relative_to(source).as_posix().encode() + b"\0")
            source_hash.update(bytes.fromhex(_sha256(path)))
    target = Path(os.environ.get("CARGO_TARGET_DIR", str(shared / "4625-cargo-target")))
    if not target.is_absolute():
        target = ROOT / target
    if not target.resolve().is_relative_to(ROOT):
        target = shared / "4625-cargo-target"
    if not target.resolve().is_relative_to(ROOT):
        raise InfrastructureError("Cargo target must stay inside the candidate checkout")
    env["CARGO_TARGET_DIR"] = str(target)
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
    binary = directory / "curie"
    shutil.copy2(target / "debug/curie", binary)
    _command([str(binary), "--version"], env=env, cwd=source)
    image_started = True
    _command(
        ["docker", "build", "-f", str(source / "apps/api/Dockerfile"), "-t", image, str(source)],
        env=env,
        cwd=source,
        timeout=1800,
    )
    image_id = _command(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image], env=env, cwd=source
    ).stdout.strip()
    if not image_id.startswith("sha256:"):
        raise InfrastructureError("candidate API image has no verifiable image ID")
    print(
        f"candidate source sha256={source_hash.hexdigest()}; "
        f"CLI sha256={_sha256(binary)}; API image={image_id}"
    )
    return Candidate(directory, source, binary, image, image_id, env)


@pytest.fixture(autouse=True)
def candidate_curie(source_candidate: Candidate, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Replace the directory's autouse shell double with this freshly built CLI."""
    monkeypatch.setenv("CURIE_BIN", str(source_candidate.binary))
    return source_candidate.binary


@dataclass(frozen=True)
class Install:
    namespace: str
    release: str
    values: Path

    @property
    def priority_classes(self) -> tuple[str, str]:
        return (f"{self.release}-platform", f"{self.release}-sandbox")


class Installations:
    def __init__(self, candidate: Candidate) -> None:
        self.candidate = candidate
        suffix = uuid.uuid4().hex[:12]
        self.cluster = f"test-4625-{suffix}"
        self.context = f"kind-{self.cluster}"
        self.directory = candidate.directory / "cluster"
        self.directory.mkdir()
        self.env = dict(candidate.env, HELM_KUBECONTEXT=self.context)
        self.chart = candidate.source / "charts/curie"
        self.installs = tuple(
            Install(
                f"test-4625-{suffix}-{index}",
                f"acme-{suffix}-{index}",
                self.directory / f"values-{index}.json",
            )
            for index in range(1, 4)
        )
        self.cluster_objects = {
            ("PriorityClass", name)
            for install in self.installs
            for name in install.priority_classes
        }
        for path in (self.chart / "crds").glob("*.yaml"):
            for document in yaml.safe_load_all(path.read_text()):
                if document:
                    if document.get("kind") != "CustomResourceDefinition":
                        raise InfrastructureError("unexpected object in candidate chart CRDs")
                    self.cluster_objects.add((document["kind"], document["metadata"]["name"]))
        self.cluster_started = False
        self.api_reachable = False
        self.owned_installs: list[Install] = []
        self.owned_cluster_objects: set[tuple[str, str]] = set()
        self.cluster_baseline: set[tuple[str, str, str]] = set()
        self.sentinel_name = f"test-4625-sentinel-{suffix}"
        self.sentinel_id: str | None = None
        self.sentinel_started = False
        self.sentinel_namespace = f"test-4625-preserve-{suffix}"
        self.sentinel_priority = f"test-4625-preserve-{suffix}"

    def run(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return _command(list(argv), env=self.env, cwd=self.candidate.source, **kwargs)

    def kubectl(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run("kubectl", "--context", self.context, *argv, **kwargs)

    def helm(self, install: Install, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run(
            "helm", "--kube-context", self.context, "--namespace", install.namespace,
            *argv, **kwargs,
        )

    def cli(self, install: Install, verb: str, *argv: str) -> subprocess.CompletedProcess[str]:
        return self.run(
            str(self.candidate.binary), "--json", "cluster", verb,
            "--context", self.context, "--namespace", install.namespace,
            "--release", install.release, *argv, check=False, timeout=720,
        )

    def containers(self) -> set[str]:
        args = ["docker", "ps", "--all", "--quiet", "--no-trunc",
                "--filter", f"label=io.x-k8s.kind.cluster={self.cluster}"]
        return set(self.run(*args).stdout.split())

    def cluster_inventory(self) -> set[tuple[str, str, str]]:
        inventory = _json(self.kubectl("get", CLUSTER_INVENTORY, "-o", "json"))
        if not isinstance(inventory, dict) or "items" not in inventory:
            raise InfrastructureError("could not inventory unrelated cluster objects")
        return {
            (item["kind"], item["metadata"]["name"], item["metadata"]["uid"])
            for item in inventory["items"]
        }

    def values(self, install: Install) -> dict[str, Any]:
        repository, tag = self.candidate.image.rsplit(":", 1)
        platform, sandbox = install.priority_classes
        return {
            "fullnameOverride": install.release,
            "api": {
                "deploy": True,
                "replicas": 1,
                "image": {"repository": repository, "tag": tag, "digest": "",
                          "pullPolicy": "Never"},
                "apiKey": "fixture-" + uuid.uuid4().hex,
                "commitPollIntervalSeconds": 0,
                "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                              "limits": {"cpu": "1", "memory": "1Gi"}},
            },
            "postgres": {"deploy": True, "persistence": {"enabled": False},
                         "auth": {"password": uuid.uuid4().hex}},
            "valkey": {"deploy": True, "persistence": {"enabled": False},
                       "password": uuid.uuid4().hex},
            "rustfs": {
                "deploy": True,
                "persistence": {"enabled": False},
                "auth": {"accessKey": "fixture" + uuid.uuid4().hex[:12],
                         "secretKey": uuid.uuid4().hex},
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                              "limits": {"cpu": "1", "memory": "2Gi"}},
            },
            "worker": {"deploy": False},
            "dispatcher": {"deploy": False},
            "mailAdapter": {"deploy": False},
            "ui": {"deploy": False},
            "factory": {"enabled": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "inference": {"deploy": False},
            "agentSandbox": {"deploy": False, "controller": {"deploy": False},
                             "runner": {"fakeModel": True, "prewarm": {"enabled": False}}},
            "e2eConnector": {"enabled": False},
            "e2eConnectorIdentity": {"enabled": False},
            "preflights": {"networkPolicyProbe": {"enabled": False}},
            "priorityClasses": {"platform": {"create": True, "name": platform},
                                "sandbox": {"create": True, "name": sandbox}},
            "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
        }

    def setup(self) -> None:
        if self.cluster in self.run("kind", "get", "clusters").stdout.split():
            raise InfrastructureError("generated kind cluster name is already in use")
        config = self.directory / "kind.json"
        config.write_text(json.dumps({"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
                                      "nodes": [{"role": "control-plane"}]}))
        self.cluster_started = True
        self.run(
            "kind", "create", "cluster", "--name", self.cluster, "--image", KIND_NODE_IMAGE,
            "--kubeconfig", self.env["KUBECONFIG"], "--config", str(config),
            "--wait", "180s", timeout=300,
        )
        self.kubectl("get", "--raw=/readyz")
        self.api_reachable = True
        self.kubectl("wait", "--for=condition=Ready", "nodes", "--all", "--timeout=120s",
                     timeout=150)
        self.run("kind", "load", "docker-image", self.candidate.image, "--name", self.cluster,
                 timeout=300)
        loaded_image = self.image(self.candidate.image)
        # A containerd image store reports the index digest as the Docker ID;
        # CRI then lists it among repoDigests while `id` is the config digest.
        if self.candidate.image_id not in loaded_image["identities"]:
            raise InfrastructureError("kind did not load the freshly built candidate API image")
        self.sentinel_started = True
        self.sentinel_id = self.run(
            "docker", "run", "--detach", "--name", self.sentinel_name,
            "--label", f"curie.test.preserve={self.cluster}",
            "--entrypoint", "/bin/sleep", KIND_NODE_IMAGE, "86400",
        ).stdout.strip()
        self.kubectl("create", "namespace", self.sentinel_namespace)
        self.kubectl("-n", self.sentinel_namespace, "create", "configmap", "preserve",
                     "--from-literal=fixture=preserve")
        self.kubectl(
            "create", "-f", "-", stdin=json.dumps({
                "apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
                "metadata": {"name": self.sentinel_priority}, "value": 1,
                "globalDefault": False,
            }),
        )
        self.cluster_baseline = self.cluster_inventory()
        existing = {(kind, name) for kind, name, _uid in self.cluster_baseline}
        if existing & self.cluster_objects:
            raise InfrastructureError("fixture ownership overlaps existing cluster resources")
        self.owned_cluster_objects = self.cluster_objects.copy()
        for install in self.installs:
            if ("Namespace", install.namespace) in existing:
                raise InfrastructureError("fresh installation namespace already exists")
            releases = _json(self.helm(install, "list", "--all", "-o", "json"))
            if releases != []:
                raise InfrastructureError("fresh installation namespace already contains a release")
            self.owned_installs.append(install)
            install.values.write_text(json.dumps(self.values(install)))
            install.values.chmod(0o600)
            rendered = self.helm(
                install, "template", install.release, str(self.chart), "--include-crds",
                "-f", str(install.values),
            )
            for document in yaml.safe_load_all(rendered.stdout):
                if not document:
                    continue
                if document["kind"] in CLUSTER_KINDS:
                    identity = (document["kind"], document["metadata"]["name"])
                    if identity not in self.cluster_objects:
                        raise InfrastructureError("rendered cluster object has no cleanup owner")
                elif document["kind"] not in NAMESPACED_KINDS:
                    raise InfrastructureError("rendered object is outside the owned fixture slice")

    def cluster_up(self, install: Install) -> int:
        return self.cli(
            install, "up", "--chart", str(self.chart), "--dev", "--fake-model",
            "--no-expose", "-f", str(install.values),
        ).returncode

    def history(self, install: Install) -> tuple[int, list[tuple[Any, Any]] | None]:
        # Helm documents revision/status history and its JSON output here:
        # https://helm.sh/docs/helm/helm_history/
        result = self.helm(install, "history", install.release, "-o", "json", check=False)
        entries = _json(result)
        records = None
        if isinstance(entries, list) and all(isinstance(entry, dict) for entry in entries):
            records = [
                (
                    entry.get("revision") if type(entry.get("revision")) is int else None,
                    "deployed" if entry.get("status") == "deployed" else "not-deployed",
                )
                for entry in entries
            ]
        return result.returncode, records

    def status(self, install: Install) -> tuple[int, bool]:
        result = self.cli(install, "status")
        payload = _json(result)
        return result.returncode, isinstance(payload, dict) and payload.get("healthy") is True

    def image(self, reference: str) -> dict[str, Any]:
        # inspecti reports the real CRI ImageStatus identity, tags and digests:
        # https://github.com/kubernetes-sigs/cri-tools/blob/v1.31.0/docs/crictl.md
        # https://github.com/kubernetes/cri-api/blob/v0.31.0/pkg/apis/runtime/v1/api.proto
        result = self.run(
            "docker", "exec", f"{self.cluster}-control-plane", "crictl", "inspecti",
            "-o", "json", reference,
        )
        payload = _json(result)
        if not isinstance(payload, dict) or not isinstance(payload.get("status"), dict):
            raise InfrastructureError("could not read the owned node's real image inventory")
        status = payload["status"]
        return {"id": status.get("id"),
                "references": status.get("repoTags", []) + status.get("repoDigests", []),
                "identities": {_digest(value) for value in
                               [status.get("id", ""), *status.get("repoDigests", [])]}}

    def _remove_install(self, install: Install) -> None:
        self.helm(install, "uninstall", install.release, "--ignore-not-found", "--wait",
                  "--timeout", "120s", timeout=150)
        if _json(self.helm(install, "list", "--all", "-o", "json")) != []:
            raise InfrastructureError("owned Helm release survived uninstall")
        if self.kubectl("-n", install.namespace, "get", "secrets", "-l",
                        f"owner=helm,name={install.release}", "-o", "name").stdout.strip():
            raise InfrastructureError("owned Helm release storage survived uninstall")

    def _delete_object(self, kind: str, name: str) -> None:
        self.kubectl("delete", kind, name, "--ignore-not-found=true", "--wait=true",
                     "--timeout=120s", timeout=150)

    def _verify_absent(self, kind: str, name: str) -> None:
        if self.kubectl("get", kind, name, "--ignore-not-found=true", "-o", "name").stdout.strip():
            raise InfrastructureError(f"owned {kind} survived cleanup")

    def _verify_preserved(self) -> None:
        if not self.cluster_baseline <= self.cluster_inventory():
            raise InfrastructureError("unrelated cluster resources were removed or replaced")
        sentinel = _json(self.kubectl("-n", self.sentinel_namespace, "get", "configmap",
                                      "preserve", "-o", "json"))
        if not isinstance(sentinel, dict) or sentinel.get("data") != {"fixture": "preserve"}:
            raise InfrastructureError("unrelated namespaced sentinel was changed")

    def _verify_cluster_removed(self) -> None:
        clusters = self.run("kind", "get", "clusters").stdout.split()
        if self.containers() or self.cluster in clusters:
            raise InfrastructureError("owned kind cluster survived cleanup")

    def _verify_containers_preserved(self) -> None:
        if self.sentinel_id is None:
            return
        running = self.run(
            "docker", "ps", "--quiet", "--no-trunc", "--filter",
            f"label=curie.test.preserve={self.cluster}",
        ).stdout.split()
        if self.sentinel_id not in running:
            raise InfrastructureError("unrelated Docker sentinel did not survive cluster cleanup")

    def _remove_sentinel(self) -> None:
        owned = self.run("docker", "ps", "--all", "--quiet", "--no-trunc", "--filter",
                         f"label=curie.test.preserve={self.cluster}").stdout.split()
        if owned:
            self.run("docker", "rm", "--force", *owned)
        if self.run("docker", "ps", "--all", "--quiet", "--filter",
                    f"label=curie.test.preserve={self.cluster}").stdout.strip():
            raise InfrastructureError("owned Docker sentinel survived cleanup")

    def close(self) -> None:
        operations: list[Callable[[], Any]] = []
        if self.api_reachable:
            for install in self.owned_installs:
                operations.extend([
                    partial(self._remove_install, install),
                    partial(self._delete_object, "namespace", install.namespace),
                    partial(self._verify_absent, "namespace", install.namespace),
                ])
            for kind, name in sorted(self.owned_cluster_objects):
                operations.extend([partial(self._delete_object, kind, name),
                                   partial(self._verify_absent, kind, name)])
            if self.cluster_baseline:
                operations.append(self._verify_preserved)
            for kind, name in (("namespace", self.sentinel_namespace),
                               ("priorityclass", self.sentinel_priority)):
                operations.extend([partial(self._delete_object, kind, name),
                                   partial(self._verify_absent, kind, name)])
        if self.cluster_started:
            operations.extend([
                partial(self.run, "kind", "delete", "cluster", "--name", self.cluster, timeout=180),
                self._verify_cluster_removed,
                self._verify_containers_preserved,
            ])
        if self.sentinel_started:
            operations.append(self._remove_sentinel)
        with ExitStack() as cleanup:
            for operation in reversed(operations):
                cleanup.callback(operation)


@pytest.fixture
def fresh_installations(
    source_candidate: Candidate, request: pytest.FixtureRequest
) -> Installations:
    installations = Installations(source_candidate)
    request.addfinalizer(installations.close)
    installations.setup()
    return installations


def _reference(image: str) -> str:
    image = image.removeprefix("docker-pullable://")
    if "/" not in image:
        image = "docker.io/library/" + image
    elif "." not in image.split("/")[0] and ":" not in image.split("/")[0]:
        image = "docker.io/" + image
    if "@" in image:
        repository, digest = image.split("@", 1)
        image = repository.rsplit("/", 1)[0] + "/" + repository.rsplit("/", 1)[1].split(":")[0]
        return image + "@" + digest
    return image


def _digest(image: str) -> str:
    return image.rsplit("@", 1)[-1].rsplit("://", 1)[-1]


def _selected(selector: dict[str, Any], pod: dict[str, Any]) -> bool:
    labels = pod["metadata"].get("labels", {})
    if not selector or not all(labels.get(key) == value
                               for key, value in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression["key"], expression["operator"]
        values = expression.get("values", [])
        if not {"In": key in labels and labels[key] in values,
                "NotIn": key not in labels or labels[key] not in values,
                "Exists": key in labels, "DoesNotExist": key not in labels}.get(operator, False):
            return False
    return True


def test_fresh_cluster_up_converges_without_rerun(fresh_installations: Installations) -> None:
    installations = fresh_installations
    for index, install in enumerate(installations.installs, start=1):
        exit_code = installations.cluster_up(install)
        assert exit_code == 0, f"fresh install {index} exited {exit_code}; no rerun was attempted"

        # Capture serving state before any status command or additional readiness wait.
        inventory_result = installations.kubectl(
            "-n", install.namespace, "get", "deployments,statefulsets,daemonsets,pods,jobs",
            "-o", "json", check=False,
        )
        inventory_code, inventory = inventory_result.returncode, _json(inventory_result)
        del inventory_result
        assert inventory_code == 0
        assert isinstance(inventory, dict) and isinstance(inventory.get("items"), list)
        history_code, records = installations.history(install)
        assert history_code == 0
        assert records is not None
        assert records == [(1, "deployed")], "one install must produce one deployed revision"
        assert type(records[0][0]) is int

        manifest_result = installations.helm(
            install, "get", "manifest", install.release, "--revision", "1", check=False,
        )
        manifest_code = manifest_result.returncode
        targets = [document for document in yaml.safe_load_all(manifest_result.stdout)
                   if document and document.get("kind") in WORKLOAD_KINDS]
        del manifest_result
        assert manifest_code == 0
        assert {(target["kind"], target["metadata"]["name"]) for target in targets} == {
            ("Deployment", f"{install.release}-api"),
            ("StatefulSet", f"{install.release}-postgres"),
            ("StatefulSet", f"{install.release}-valkey"),
            ("StatefulSet", f"{install.release}-rustfs"),
        }
        items = inventory["items"]
        workloads = {(item["kind"], item["metadata"]["name"]): item for item in items
                     if item["kind"] in WORKLOAD_KINDS}
        assert set(workloads) == {(target["kind"], target["metadata"]["name"])
                                  for target in targets}
        active_pods = [item for item in items if item["kind"] == "Pod"
                       and (item["status"].get("phase") not in {"Succeeded", "Failed"}
                            or item["metadata"].get("deletionTimestamp"))]
        selected_uids: set[str] = set()
        runtime_images: dict[str, dict[str, Any]] = {}
        for target in targets:
            key = (target["kind"], target["metadata"]["name"])
            workload = workloads[key]
            spec, status = workload["spec"], workload["status"]
            assert workload["metadata"]["generation"] > 0
            assert status.get("observedGeneration") == workload["metadata"]["generation"]
            desired = target["spec"].get("replicas", 1)
            assert desired == spec.get("replicas", 1) == 1
            assert status.get("replicas", 0) == desired
            assert status.get("updatedReplicas", 0) == desired
            assert status.get("readyReplicas", 0) == desired
            assert status.get("availableReplicas", 0) == desired
            assert status.get("unavailableReplicas", 0) == 0
            assert spec["selector"] == target["spec"]["selector"]
            if target["kind"] == "StatefulSet":
                assert status.get("currentReplicas", 0) == desired
                assert status.get("currentRevision")
                assert status["currentRevision"] == status.get("updateRevision")
            else:
                assert any(condition["type"] == "Available" and condition["status"] == "True"
                           for condition in status.get("conditions", []))
            selected = [pod for pod in active_pods if _selected(spec["selector"], pod)]
            assert len(selected) == desired
            for pod in selected:
                selected_uids.add(pod["metadata"]["uid"])
                assert pod["status"]["phase"] == "Running"
                assert not pod["metadata"].get("deletionTimestamp")
                assert any(condition["type"] == "Ready" and condition["status"] == "True"
                           for condition in pod["status"].get("conditions", []))
                for field_name, status_field in (("containers", "containerStatuses"),
                                                  ("initContainers", "initContainerStatuses")):
                    expected = {
                        container["name"]: container["image"]
                        for container in target["spec"]["template"]["spec"].get(field_name, [])
                    }
                    assert expected == {
                        container["name"]: container["image"]
                        for container in spec["template"]["spec"].get(field_name, [])
                    }
                    assert expected == {
                        container["name"]: container["image"]
                        for container in pod["spec"].get(field_name, [])
                    }
                    observed = {container["name"]: container
                                for container in pod["status"].get(status_field, [])}
                    assert set(observed) == set(expected)
                    for name, reference in expected.items():
                        container = observed[name]
                        if reference not in runtime_images:
                            runtime_images[reference] = installations.image(reference)
                        image = runtime_images[reference]
                        assert _reference(reference) in {
                            _reference(ref) for ref in image["references"]
                        }
                        assert container.get("imageID")
                        assert _digest(container["imageID"]) in image["identities"]
                        if reference == installations.candidate.image:
                            assert installations.candidate.image_id in image["identities"]
                        if field_name == "initContainers":
                            assert container["state"].get("terminated", {}).get("exitCode") == 0
                        else:
                            assert container.get("ready") is True
                            assert "running" in container["state"]
        assert selected_uids == {pod["metadata"]["uid"] for pod in active_pods}
        status_code, healthy = installations.status(install)
        assert status_code == 0
        assert healthy is True
        ready_result = installations.kubectl(
            "-n", install.namespace, "exec", f"deployment/{install.release}-api", "--",
            "python3", "-c", "import urllib.request; "
            "print(urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=10).status)",
            check=False,
        )
        ready_code, ready_status = ready_result.returncode, ready_result.stdout.strip()
        del ready_result
        assert ready_code == 0
        assert ready_status == "200"
        print(
            f"fresh install {index}: revision=1 status=deployed "
            "history=1 exact=true healthy=true"
        )
