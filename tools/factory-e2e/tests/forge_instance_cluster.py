"""Owned candidate builds and real kind/Helm fixtures for the C4 regression.

No command selects a Docker engine or changes a shared context. All product
builds consume invocation working-tree bytes, including Fix pin reversal.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import tarfile
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml

TESTS = Path(__file__).resolve().parent
NODE_IMAGE = "kindest/node:v1.31.0"
MODEL = "claude-sonnet-5"
RELEASE = "acme-factory"
# The chart fullname: the release name lacks the chart name, so it is appended.
FULLNAME = f"{RELEASE}-curie"
GROUPS = ("gitlab", "jira", "stale", "interrupted")
CLEANUP_RESERVE = 8 * 60
MODULE_SECONDS = 150 * 60
ROLLOUT_READY_SECONDS = 180
ROLLOUT_POLL_SECONDS = 1
# Option flags whose value is a separate argv token and never part of a label.
VALUE_OPTIONS = frozenset(
    ("--kubeconfig", "--context", "--namespace", "--kube-context", "-n", "-f", "-o", "--values")
)
STDERR_TAIL = 2000
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.DOTALL),
    re.compile(r"[0-9a-fA-F]{32,}"),
    re.compile(r"[A-Za-z0-9+/_-]{40,}={0,2}"),
)


class InfrastructureError(RuntimeError):
    """Provisioning, observation transport or cleanup failed, never a RED pin."""


@dataclass(repr=False)
class Result:
    returncode: int
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Result(exit={self.returncode}; output withheld)"


@dataclass(repr=False)
class Snapshot:
    values: dict[str, Any] = field(repr=False)
    history: list[dict[str, Any]] = field(repr=False)
    objects: dict[str, dict[str, Any]] = field(repr=False)
    agents: list[dict[str, Any]] = field(repr=False)
    label_writes: dict[str, int] = field(repr=False)

    def __repr__(self) -> str:
        return (
            "Snapshot(Helm, Secret/ConfigMap metadata/data, agents, label writes; values withheld)"
        )


@dataclass(repr=False)
class Peer:
    name: str
    kind: str
    port: int
    remote_port: int
    origin: str
    token_file: Path
    alternate_token_file: Path
    rotated_token_file: Path
    ca: Path | None
    config: dict[str, Any] = field(repr=False)

    def __repr__(self) -> str:
        return f"Peer({self.name}, {self.kind})"

    @property
    def path(self) -> str:
        return str(self.config.get("projects", [{"path": "acme-corp/acme-bot"}])[0]["path"])


def private_write(path: Path, text: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(text.encode() if isinstance(text, str) else text)


def object_state(obj: dict[str, Any], *, metadata: bool = True) -> dict[str, Any]:
    result = copy.deepcopy(obj)
    if not metadata:
        for key in ("uid", "resourceVersion", "creationTimestamp", "managedFields", "generation"):
            result.get("metadata", {}).pop(key, None)
    else:
        result.get("metadata", {}).pop("managedFields", None)
    return result


def argument_tokens(argv: Sequence[str | Path]) -> Iterator[str]:
    """Tokens after the program, minus the separate values of VALUE_OPTIONS."""
    skip = False
    for token in (str(a) for a in argv[1:]):
        if skip:
            skip = False
            continue
        skip = token in VALUE_OPTIONS
        yield token


def command_label(argv: Sequence[str | Path]) -> str:
    """Program plus up to three positionals, ending at the first option after one."""
    positionals: list[str] = []
    for token in argument_tokens(argv):
        if token.startswith("-"):
            if positionals:
                break
            continue
        positionals.append(Path(token).name if token.startswith("/") else token)
        if len(positionals) == 3:
            break
    program = Path(str(argv[0])).name if argv else ""
    return " ".join([program, *positionals])


def secret_forms(secrets: set[str]) -> list[str]:
    """Each registered value with its padded and unpadded base64 encodings, longest first."""
    forms: set[str] = set()
    for value in filter(None, secrets):
        forms.add(value)
        for encode in (base64.b64encode, base64.urlsafe_b64encode):
            encoded = encode(value.encode()).decode()
            forms.update((encoded, encoded.rstrip("=")))
    return sorted(forms, key=len, reverse=True)


def redact(text: str, secrets: set[str]) -> str:
    for form in secret_forms(secrets):
        text = text.replace(form, "<redacted>")
    for pattern in SECRET_PATTERNS:
        text = pattern.sub("<redacted>", text)
    return text


def failure_detail(
    argv: Sequence[str | Path], stderr: str | bytes | None, secrets: set[str]
) -> str:
    """Redacted stderr tail for a failure message; stdout never appears."""
    positionals = [t.lower() for t in argument_tokens(argv) if not t.startswith("-")]
    if "get" in positionals and any("secret" in t or t == "values" for t in positionals):
        return "stderr withheld for a Secret or values read"
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors="replace")
    # Redact the whole stream first so the tail cut cannot split a secret.
    return redact(stderr or "", secrets)[-STDERR_TAIL:]


def same_secret(left: dict[str, Any], right: dict[str, Any], *, version: bool = True) -> bool:
    keys = ["name", "uid"] + (["resourceVersion"] if version else [])
    return all(left["metadata"].get(k) == right["metadata"].get(k) for k in keys) and left.get(
        "data"
    ) == right.get("data")


class Candidate:
    def __init__(self, source: Path) -> None:
        self.source = source.resolve()
        self.started = time.monotonic()
        self.deadline = self.started + MODULE_SECONDS - CLEANUP_RESERVE
        self.identity = uuid.uuid4().hex[:12]
        self.env = dict(os.environ)
        self.faults: list[str] = []
        self.images: dict[str, str] = {}
        self.clusters: list[Cluster] = []
        self.root: Path | None = None
        self.binary: Path | None = None
        self.before_containers: set[str] = set()
        self.sentinel: str | None = None
        self.before_networks: set[str] = set()
        self.before_volumes: set[str] = set()
        self.before_clusters: set[str] = set()
        self.before_images: set[str] = set()
        self.ephemeral_names: list[str] = []
        self.secrets: set[str] = set()

    def register_secret(self, value: str) -> None:
        self.secrets.add(value)

    def fail(self, message: str) -> Any:
        self.faults.append(message)
        raise InfrastructureError(message)

    def record_diagnostic(self, operation: str, status: int, stdout: Any, stderr: Any) -> None:
        if self.root is None:
            return
        directory = self.root.parent / "diagnostics"
        directory.mkdir(mode=0o700, exist_ok=True)
        database = directory / f"{self.identity}.sqlite3"
        if not database.exists():
            descriptor = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS commands ("
                "id INTEGER PRIMARY KEY, operation TEXT, status INTEGER, stdout BLOB, stderr BLOB)"
            )
            connection.execute(
                "INSERT INTO commands (operation,status,stdout,stderr) VALUES (?,?,?,?)",
                (operation, status, stdout or "", stderr or ""),
            )

    def run(
        self,
        argv: Sequence[str | Path],
        *,
        timeout: float = 300,
        input_text: str | None = None,
        env: dict[str, str] | None = None,
        check: bool = True,
        cleanup: bool = False,
    ) -> Result:
        remaining = self.deadline - time.monotonic()
        if not cleanup and remaining <= 0:
            return self.fail("150-minute module budget reached its cleanup reserve")
        operation = " ".join(str(a) for a in argv[:2])
        result: subprocess.CompletedProcess[str] | None = None
        try:
            result = subprocess.run(
                [str(a) for a in argv],
                cwd=self.source,
                env=env or self.env,
                input=input_text,
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout if cleanup else min(timeout, max(1, remaining)),
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            self.record_diagnostic(
                operation, -1, getattr(error, "stdout", ""), getattr(error, "stderr", str(error))
            )
            error_name = type(error).__name__
            error_stderr = getattr(error, "stderr", None)
        # Fail outside the handler so the raised error chains no context holding argv.
        if result is None:
            detail = failure_detail(argv, error_stderr, self.secrets)
            return self.fail(f"{command_label(argv)}: {error_name}; {detail}")
        answer = Result(result.returncode, result.stdout, result.stderr)
        if answer.returncode:
            self.record_diagnostic(operation, answer.returncode, answer.stdout, answer.stderr)
        if check and answer.returncode:
            detail = failure_detail(argv, answer.stderr, self.secrets)
            return self.fail(f"{command_label(argv)}: exit {answer.returncode}; {detail}")
        return answer

    def names(self, kind: str, *, cleanup: bool = False) -> set[str]:
        args = {
            "containers": ["docker", "ps", "-aq", "--no-trunc"],
            "networks": ["docker", "network", "ls", "-q", "--no-trunc"],
            "volumes": ["docker", "volume", "ls", "-q"],
            "images": ["docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}"],
            "clusters": ["kind", "get", "clusters"],
        }
        return set(self.run(args[kind], cleanup=cleanup).stdout.splitlines())

    def prepare(self) -> None:
        for executable in ("git", "cargo", "docker", "kind", "kubectl", "helm", "openssl"):
            if shutil.which(executable) is None:
                self.fail(f"required executable unavailable: {executable}")
        common = Path(
            self.run(
                ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"]
            ).stdout.strip()
        ).resolve()
        if common.name == ".git":
            checkout = common.parent
        else:
            records = self.run(["git", "worktree", "list", "--porcelain"]).stdout
            first = next((r[9:] for r in records.splitlines() if r.startswith("worktree ")), None)
            if not first:
                self.fail("Git common checkout could not be identified")
            checkout = Path(first).resolve()
        self.root = checkout / ".projects" / "4620" / self.identity
        self.root.mkdir(parents=True, exist_ok=False)
        if shutil.disk_usage(self.root).free < 20 * 1024**3:
            self.fail("candidate builds require at least 20 GiB free disk")
        self.run(["docker", "info", "--format", "{{.ServerVersion}}"])
        self.run(["kind", "version"])
        self.run(["kubectl", "version", "--client", "-o", "json"])
        self.run(["helm", "version", "--short"])
        self.before_containers = self.names("containers")
        self.before_networks = self.names("networks")
        self.before_volumes = self.names("volumes")
        self.before_clusters = self.names("clusters")
        self.before_images = self.names("images")
        fingerprint = self.fingerprint()
        self.run(
            [
                "cargo",
                "build",
                "--locked",
                "--manifest-path",
                self.source / "cli/Cargo.toml",
                "--target-dir",
                self.root / "cargo-target",
            ],
            timeout=2400,
        )
        self.binary = self.root / "cargo-target/debug/curie"
        if not self.binary.is_file():
            self.fail("cargo did not produce the invocation candidate")
        for component, dockerfile in (
            ("api", "apps/api/Dockerfile"),
            ("worker", "apps/worker/Dockerfile"),
            ("runner", "runner/Dockerfile"),
        ):
            image = f"curie-c4-{component}:{self.identity}"
            self.images[component] = image
            self.build_image(image, dockerfile)
        context = self.root / "peer-image"
        context.mkdir()
        shutil.copyfile(TESTS / "forge_instance_peer.py", context / "peer.py")
        private_write(
            context / "Dockerfile",
            (
                "FROM python:3.13.15-slim-bookworm"
                "@sha256:ed86c82274b3c69b52fb5820f358f0bd7df0b603332063cb5c6e32bd220c3e6e\n"
                'COPY peer.py /peer.py\nUSER 1000:1000\nENTRYPOINT ["python3","/peer.py"]\n'
            ),
        )
        self.images["peer"] = f"curie-c4-peer:{self.identity}"
        self.run(["docker", "build", "-t", self.images["peer"], context], timeout=300)
        # An unrelated container that cluster cleanup must leave untouched.
        # Other sessions' containers come and go, so only this one is checked.
        sentinel = f"c4-4620-{self.identity}-sentinel"
        self.ephemeral_names.append(sentinel)
        self.run(["docker", "create", "--name", sentinel, self.images["peer"], "sentinel"])
        self.sentinel = sentinel
        if fingerprint != self.fingerprint():
            self.fail("candidate production inputs changed during the build")
        with sqlite3.connect(self.root / "build.sqlite3") as db:
            db.execute(
                "CREATE TABLE build (source TEXT, fingerprint TEXT, binary TEXT, images TEXT)"
            )
            db.execute(
                "INSERT INTO build VALUES (?,?,?,?)",
                (
                    str(self.source),
                    fingerprint,
                    hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    json.dumps(self.images, sort_keys=True),
                ),
            )
        print(f"C4 candidate inputs={fingerprint} build={self.identity}", flush=True)

    def source_files(self) -> Iterator[Path]:
        names = self.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
        ).stdout.split("\0")
        for name in sorted(set(names) - {""}):
            path = self.source / name
            if path.is_file() and not path.is_symlink():
                yield path

    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        for path in self.source_files():
            relative = path.relative_to(self.source)
            if relative.parts[0] not in ("cli", "apps", "packages", "runner", "charts") and (
                relative.as_posix() not in ("uv.lock", "pyproject.toml")
            ):
                continue
            digest.update(relative.as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
        return digest.hexdigest()

    def build_image(self, image: str, dockerfile: str) -> None:
        # Stream the actual working tree. A Docker daemon never needs a bind
        # mount of the invocation checkout, including a verifier's temp tree.
        # Excluding owned targets prevents recursive multi-gigabyte contexts.
        if self.root is None:
            self.fail("build root not initialized")
        log = self.root / "build-output"
        with log.open("wb") as output:
            process = subprocess.Popen(
                [
                    "docker",
                    "build",
                    "--label",
                    f"curie-c4-owner={self.identity}",
                    "-t",
                    image,
                    "-f",
                    dockerfile,
                    "-",
                ],
                cwd=self.source,
                env=self.env,
                stdin=subprocess.PIPE,
                stdout=output,
                stderr=output,
            )
            try:
                if process.stdin is None:
                    self.fail("Docker build stdin unavailable")
                with tarfile.open(
                    fileobj=process.stdin, mode="w|", format=tarfile.PAX_FORMAT
                ) as archive:
                    for path in self.source_files():
                        archive.add(
                            path, arcname=str(path.relative_to(self.source)), recursive=False
                        )
                process.stdin.close()
                status = process.wait(timeout=min(1800, max(1, self.deadline - time.monotonic())))
            except (OSError, subprocess.TimeoutExpired):
                process.kill()
                process.wait()
                output.flush()
                self.record_diagnostic("docker build", -1, b"", log.read_bytes())
                self.fail(f"Docker {Path(dockerfile).parent.name} build failed; output withheld")
        if status:
            self.record_diagnostic("docker build", status, b"", log.read_bytes())
            self.fail(f"Docker {Path(dockerfile).parent.name} build exit {status}; output withheld")
        log.unlink(missing_ok=True)

    def close(self) -> None:
        cleanup_errors: list[str] = []
        for cluster in reversed(self.clusters):
            try:
                cluster.close()
            except InfrastructureError as error:
                cleanup_errors.append(str(error))
        try:
            for name in self.ephemeral_names:
                result = self.run(["docker", "inspect", name], check=False, cleanup=True)
                if result.returncode == 0:
                    self.run(["docker", "rm", "-f", name], cleanup=True)
                if self.run(["docker", "inspect", name], check=False, cleanup=True).returncode == 0:
                    cleanup_errors.append("owned key-generation container remains")
            if self.images:
                for image in self.images.values():
                    self.run(["docker", "image", "rm", image], check=False, cleanup=True)
                remaining = self.names("images", cleanup=True)
                if set(self.images.values()) & remaining:
                    cleanup_errors.append("owned candidate image tags remain")
        except InfrastructureError as error:
            cleanup_errors.append(str(error))
        finally:
            if self.root is not None and self.root.exists():
                try:
                    shutil.rmtree(self.root)
                except OSError:
                    cleanup_errors.append("owned build/staging tree removal failed")
                if self.root.exists():
                    cleanup_errors.append("owned build/staging tree remains")
        if cleanup_errors or self.faults:
            raise InfrastructureError("; ".join(dict.fromkeys([*self.faults, *cleanup_errors])))


class Cluster:
    def __init__(self, candidate: Candidate, group: str) -> None:
        if group not in GROUPS or len(candidate.clusters) >= 4:
            candidate.fail("scenario cluster contract exceeded")
        if candidate.root is None or candidate.binary is None:
            candidate.fail("candidate build is unavailable")
        self.candidate = candidate
        candidate.clusters.append(self)
        self.name = f"c4-4620-{candidate.identity}-{group}"
        self.context = f"kind-{self.name}"
        self.namespace = f"test-4620-{group}"
        self.root = candidate.root / group
        self.root.mkdir()
        self.kubeconfig = self.root / "kubeconfig"
        self.env = dict(candidate.env)
        for key in list(self.env):
            if key.startswith(("CURIE_", "ANTHROPIC_", "OPENAI_", "OPENROUTER_")):
                self.env.pop(key)
        self.env.update(
            {
                "KUBECONFIG": str(self.kubeconfig),
                "CURIE_CONFIG_DIR": str(self.root / "config"),
                "XDG_CONFIG_HOME": str(self.root / "config"),
                "CURIE_BIN": str(candidate.binary),
                "HELM_CACHE_HOME": str(self.root / "helm-cache"),
                "HELM_CONFIG_HOME": str(self.root / "helm-config"),
                "HELM_DATA_HOME": str(self.root / "helm-data"),
            }
        )
        # Preserve the runner's existing Docker authentication/configuration.
        self.env["DOCKER_CONFIG"] = candidate.env.get(
            "DOCKER_CONFIG", str(Path(candidate.env.get("HOME", str(Path.home()))) / ".docker")
        )
        self.processes: list[subprocess.Popen[str]] = []
        self.reservations: list[socket.socket] = []
        self.peers: dict[str, Peer] = {}
        self.created = False
        self.closed = False
        self.nodes: set[str] = set()
        self.networks: set[str] = set()
        self.volumes: set[str] = set()
        self.api_port = 0
        self.api_relay_port = 18999
        self.api_key = f"example-c4-api-{uuid.uuid4().hex}"
        self.control_key = f"example-c4-control-{uuid.uuid4().hex}"
        candidate.register_secret(self.api_key)
        candidate.register_secret(self.control_key)
        self.bundle = self.root / "bundle"
        self.app_key = self.root / "app.pem"

    def run(self, argv: Sequence[str | Path], **kwargs: Any) -> Result:
        return self.candidate.run(argv, env=self.env, **kwargs)

    def kubectl(self, *argv: str, **kwargs: Any) -> Result:
        return self.run(
            [
                "kubectl",
                "--kubeconfig",
                self.kubeconfig,
                "--context",
                self.context,
                "--namespace",
                self.namespace,
                *argv,
            ],
            **kwargs,
        )

    def helm(self, *argv: str, **kwargs: Any) -> Result:
        return self.run(
            [
                "helm",
                "--kubeconfig",
                self.kubeconfig,
                "--kube-context",
                self.context,
                "--namespace",
                self.namespace,
                *argv,
            ],
            **kwargs,
        )

    def apply(self, obj: dict[str, Any]) -> None:
        # create or replace, never apply: apply records a last-applied
        # annotation, so a restored object would never equal its snapshot.
        current = self.kubectl(
            "get", obj["kind"], obj["metadata"]["name"], "-o", "json", check=False
        )
        if current.returncode:
            self.kubectl("create", "-f", "-", input_text=json.dumps(obj))
            return
        replacement = copy.deepcopy(obj)
        replacement["metadata"]["resourceVersion"] = json.loads(current.stdout)["metadata"][
            "resourceVersion"
        ]
        self.kubectl("replace", "-f", "-", input_text=json.dumps(replacement))

    def reserve_port(self) -> int:
        handle = socket.socket()
        handle.bind(("127.0.0.1", 0))
        self.reservations.append(handle)
        return int(handle.getsockname()[1])

    def release_port(self, port: int) -> None:
        for handle in self.reservations[:]:
            if handle.getsockname()[1] == port:
                handle.close()
                self.reservations.remove(handle)

    def certificate(self, name: str) -> tuple[Path, Path, Path]:
        root = self.root / "tls" / name
        root.mkdir(parents=True)
        ca, key, cert = root / "ca.pem", root / "server.key", root / "server.pem"
        self.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-keyout",
                root / "ca.key",
                "-out",
                ca,
                "-subj",
                f"/CN=Acme {name} test root",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]
        )
        self.run(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                key,
                "-out",
                root / "server.csr",
                "-subj",
                "/CN=localhost",
            ]
        )
        private_write(
            root / "extensions",
            (
                "subjectAltName=IP:127.0.0.1,DNS:localhost\n"
                "basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
                "extendedKeyUsage=serverAuth\n"
            ),
        )
        self.run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                root / "server.csr",
                "-CA",
                ca,
                "-CAkey",
                root / "ca.key",
                "-CAcreateserial",
                "-out",
                cert,
                "-days",
                "2",
                "-extfile",
                root / "extensions",
            ]
        )
        return ca, key, cert

    def prepare_peers(self) -> None:
        secret_data: dict[str, str] = {}
        containers = []
        ports = []
        for index, (name, kind, tls) in enumerate(
            (
                ("gitlab-a", "gitlab", True),
                ("gitlab-b", "gitlab", True),
                ("gitlab-unknown", "gitlab", True),
                ("jira-a", "jira", False),
                ("jira-b", "jira", False),
                ("jira-dc", "jira", False),
                ("github", "github", False),
                ("dc", "dc", False),
            )
        ):
            port = self.reserve_port()
            remote = 18000 + index
            tokens = [
                f"example-{name}-{role}-{uuid.uuid4().hex}"
                for role in ("current", "conflict", "rotated")
            ]
            for token in tokens:
                self.candidate.register_secret(token)
            files = [self.root / "tokens" / f"{name}-{i}" for i in range(3)]
            for path, token in zip(files, tokens, strict=True):
                private_write(path, token + "\n")
            origin = f"{'https' if tls else 'http'}://127.0.0.1:{port}"
            config: dict[str, Any] = {
                "kind": kind,
                "port": remote,
                "origin": origin,
                "tokens": tokens,
                "control": self.control_key,
                "content": f"original-content-{name}",
                "usernames": ["acme-bot@example.com", "acme-other@example.com"],
                "cloud_id": "00000000-0000-4000-8000-" + f"{index + 1:012d}",
                "projects": [
                    {"id": 41, "path": f"acme-corp/{name}-widgets"},
                    {"id": 42, "path": f"acme-corp/{name}-neighbor"},
                ],
                "tls": tls,
            }
            ca = None
            if tls:
                ca, key, cert = self.certificate(name)
                config.update(certificate=f"/inputs/{name}.pem", private_key=f"/inputs/{name}.key")
                secret_data[f"{name}.pem"] = base64.b64encode(cert.read_bytes()).decode()
                secret_data[f"{name}.key"] = base64.b64encode(key.read_bytes()).decode()
            secret_data[f"{name}.json"] = base64.b64encode(json.dumps(config).encode()).decode()
            self.peers[name] = Peer(name, kind, port, remote, origin, *files, ca, config)
            containers.append(
                {
                    "name": name,
                    "image": self.candidate.images["peer"],
                    "imagePullPolicy": "Never",
                    "args": ["serve", f"/inputs/{name}.json", "/state"],
                    "volumeMounts": [
                        {"name": "inputs", "mountPath": "/inputs", "readOnly": True},
                        {"name": f"state-{name}", "mountPath": "/state"},
                    ],
                    "ports": [{"name": name, "containerPort": remote}],
                    "readinessProbe": {"tcpSocket": {"port": remote}, "periodSeconds": 1},
                    "resources": {
                        "requests": {"cpu": "5m", "memory": "24Mi"},
                        "limits": {"memory": "96Mi"},
                    },
                }
            )
            ports.append({"name": name, "port": remote, "targetPort": remote})
        # Host API clients also need a stable bridge: a port-forward directly
        # to the API Service selects one pod and exits when Helm replaces it.
        containers.append(
            {
                "name": "api-relay",
                "image": self.candidate.images["peer"],
                "imagePullPolicy": "Never",
                "args": ["bridge", str(self.api_relay_port), f"{FULLNAME}-api", "8000"],
                "resources": {
                    "requests": {"cpu": "2m", "memory": "16Mi"},
                    "limits": {"memory": "64Mi"},
                },
            }
        )
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "forge-inputs"},
                "type": "Opaque",
                "data": secret_data,
            }
        )
        self.apply(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "forge-peers"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "forge-peers"}},
                    "template": {
                        "metadata": {"labels": {"app": "forge-peers"}},
                        "spec": {
                            "securityContext": {
                                "runAsUser": 1000,
                                "runAsGroup": 1000,
                                "fsGroup": 1000,
                            },
                            "containers": containers,
                            "volumes": [
                                {"name": "inputs", "secret": {"secretName": "forge-inputs"}},
                                *[{"name": f"state-{p}", "emptyDir": {}} for p in self.peers],
                            ],
                        },
                    },
                },
            }
        )
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "forge-peers"},
                "spec": {"selector": {"app": "forge-peers"}, "ports": ports},
            }
        )
        self.kubectl("rollout", "status", "deployment/forge-peers", "--timeout=180s")
        self.forward(
            "deployment/forge-peers",
            [
                *[(p.port, p.remote_port) for p in self.peers.values()],
                (self.api_port, self.api_relay_port),
            ],
        )
        for peer in self.peers.values():
            self.control(peer, "/__fixture/health")

    def forward(self, target: str, ports: list[tuple[int, int]]) -> None:
        for port, _ in ports:
            self.release_port(port)
        process = subprocess.Popen(
            [
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--context",
                self.context,
                "--namespace",
                self.namespace,
                "port-forward",
                "--address",
                "127.0.0.1",
                target,
                *[f"{left}:{right}" for left, right in ports],
            ],
            cwd=self.candidate.source,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.processes.append(process)
        end = time.monotonic() + 30
        while time.monotonic() < end:
            if process.poll() is not None:
                self.candidate.fail("owned port-forward exited before readiness")
            ready = True
            for port, _ in ports:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        pass
                except OSError:
                    ready = False
            if ready:
                return
            time.sleep(0.2)
        self.candidate.fail("owned port-forward did not become reachable")

    def control(self, peer: Peer, path: str, body: dict[str, Any] | None = None) -> Any:
        import ssl

        context = ssl.create_default_context(cafile=str(peer.ca)) if peer.ca else None
        request = Request(
            peer.origin + path,
            headers={"X-Fixture-Control": self.control_key, "Content-Type": "application/json"},
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urlopen(request, context=context, timeout=20) as response:
                return json.load(response)
        except (OSError, ValueError) as error:
            return self.candidate.fail(f"external fixture {peer.name}: {type(error).__name__}")

    def receipts(self, peer: Peer) -> dict[str, Any]:
        return dict(self.control(peer, "/__fixture/receipts"))

    def start(self) -> None:
        # The caller has already registered close() as a fixture finalizer.
        self.created = True
        self.run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                self.name,
                "--image",
                NODE_IMAGE,
                "--kubeconfig",
                self.kubeconfig,
                "--wait",
                "180s",
            ],
            timeout=360,
        )
        self.nodes = set(
            self.run(["kind", "get", "nodes", "--name", self.name]).stdout.splitlines()
        )
        for node in self.nodes:
            inspect = json.loads(self.run(["docker", "inspect", node]).stdout)[0]
            self.networks.update(inspect.get("NetworkSettings", {}).get("Networks", {}))
            self.volumes.update(
                m["Name"] for m in inspect.get("Mounts", []) if m["Type"] == "volume"
            )
        self.run(
            ["kind", "load", "docker-image", "--name", self.name, *self.candidate.images.values()],
            timeout=600,
        )
        self.api_port = self.reserve_port()
        self.kubectl("create", "namespace", self.namespace)
        self.prepare_peers()
        self.run(["openssl", "genrsa", "-out", self.app_key, "2048"])
        private_write(
            self.bundle / ".claude-plugin/plugin.json",
            json.dumps(
                {
                    "name": "acme-regression",
                    "version": "0.1.0",
                    "description": "Registration fixture",
                }
            ),
        )
        private_write(
            self.bundle / "skills/registration/SKILL.md",
            (
                "---\nname: registration\ndescription: Registration fixture with no turns.\n---\n"
                "Read the ticket.\n"
            ),
        )
        self.install()
        self.inject_bridges()
        self.env.update(
            CURIE_API_URL=f"http://127.0.0.1:{self.api_port}",
            CURIE_API_KEY=self.api_key,
            CURIE_GITHUB_API_URL=self.peers["github"].origin,
        )
        self.check_bridges()
        # API creation uses a neutral tracker surface accepted before any
        # GitLab/Jira entry exists. The actual quickstarts attach their surfaces.
        for name in ("acme-first", "acme-second", "acme-labelled", "acme-third", "acme-dc"):
            self.api(
                "POST",
                "/agents",
                {"name": name, "channel": {"kind": "github", "address": f"acme-corp/{name}-seed"}},
            )

    def install(self) -> None:
        def image(role: str) -> dict[str, str]:
            repository, tag = self.candidate.images[role].rsplit(":", 1)
            return {"repository": repository, "tag": tag, "pullPolicy": "Never"}

        runner, runner_tag = self.candidate.images["runner"].rsplit(":", 1)
        webhook_secret = f"example-c4-webhook-{uuid.uuid4().hex}"
        self.candidate.register_secret(webhook_secret)
        values = {
            "api": {
                "image": image("api"),
                "apiKey": self.api_key,
                "githubApiUrl": self.peers["github"].origin,
                # A real install gets one from cluster up; an enabled factory requires it.
                "githubWebhookSecret": webhook_secret,
            },
            "worker": {"image": image("worker")},
            "ui": {"deploy": False},
            "dispatcher": {"deploy": False},
            "mailAdapter": {"deploy": False},
            "langfuse": {"deploy": False, "host": "langfuse.example.com"},
            "clickhouse": {"deploy": False},
            "otelCollector": {"deploy": False, "telemetryDisabled": True},
            "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
            "agentSandbox": {
                "deploy": False,
                "controller": {"deploy": False},
                "runner": {
                    "image": runner,
                    "tag": runner_tag,
                    "imagePullPolicy": "Never",
                    "fakeModel": False,
                    "model": MODEL,
                    "prewarm": {"enabled": False},
                },
            },
            "factory": {"enabled": False, "intake": "poll"},
        }
        name = f"{self.name}-keys"
        self.candidate.ephemeral_names.append(name)
        keys = json.loads(
            self.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    name,
                    "--label",
                    f"curie-c4-owner={self.candidate.identity}",
                    self.candidate.images["worker"],
                    "python3",
                    "-c",
                    "import base64,json;from nacl.signing import SigningKey;"
                    "from nacl.public import PrivateKey;k=SigningKey.generate();"
                    "print(json.dumps({'signing':base64.b64encode(bytes(k)).decode(),"
                    "'verify':base64.b64encode(bytes(k.verify_key)).decode(),"
                    "'sealing':base64.b64encode(bytes(PrivateKey.generate())).decode()}))",
                ]
            ).stdout
        )
        for role in ("signing", "verify", "sealing"):
            self.candidate.register_secret(keys[role])
        values["connectorCaller"] = {"signingKey": keys["signing"], "verifyKey": keys["verify"]}
        values["sealing"] = {"privateKey": keys["sealing"]}
        path = self.root / "install.yaml"
        private_write(path, yaml.safe_dump(values))
        self.helm(
            "upgrade",
            "--install",
            RELEASE,
            str(self.candidate.source / "charts/curie"),
            "-f",
            str(self.candidate.source / "charts/curie/values-dev.yaml"),
            "-f",
            str(path),
            "--timeout",
            "480s",
            timeout=540,
        )
        self.rollout()

    def inject_bridges(self) -> None:
        containers = [
            {
                "name": f"bridge-{p.name}",
                "image": self.candidate.images["peer"],
                "imagePullPolicy": "Never",
                "args": ["bridge", str(p.port), "forge-peers", str(p.remote_port)],
                "readinessProbe": {
                    "exec": {
                        "command": [
                            "python3",
                            "-c",
                            "import socket;socket.create_connection(('127.0.0.1',"
                            f"{p.port}),2).close()",
                        ]
                    },
                    "periodSeconds": 2,
                },
                "resources": {
                    "requests": {"cpu": "2m", "memory": "16Mi"},
                    "limits": {"memory": "64Mi"},
                },
            }
            for p in self.peers.values()
        ]
        self.kubectl(
            "patch",
            "deployment",
            f"{FULLNAME}-api",
            "--type=strategic",
            "-p",
            json.dumps({"spec": {"template": {"spec": {"containers": containers}}}}),
        )
        self.rollout()

    def api_pods(self, **kwargs: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = json.loads(
            self.kubectl(
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=api",
                "-o",
                "json",
                **kwargs,
            ).stdout
        )["items"]
        return items

    @staticmethod
    def serving(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            p
            for p in items
            if not p["metadata"].get("deletionTimestamp")
            and any(
                c["type"] == "Ready" and c["status"] == "True"
                for c in p.get("status", {}).get("conditions", [])
            )
        ]

    def api_pod(self) -> str:
        ready = self.serving(self.api_pods())
        if len(ready) != 1:
            return self.candidate.fail("expected one serving candidate API pod")
        return str(ready[0]["metadata"]["name"])

    def rollout(self) -> None:
        for component in ("api", "worker"):
            self.kubectl(
                "rollout",
                "status",
                f"deployment/{FULLNAME}-{component}",
                "--timeout=240s",
                timeout=260,
            )
        # The replaced pod stays listed while its preStop hook runs; it must be gone,
        # not just unready, before the release counts as settled (`kubectl wait` latches onto it).
        deadline = time.monotonic() + ROLLOUT_READY_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            items = self.api_pods(timeout=max(1, remaining))
            listed, serving = len(items), len(self.serving(items))
            # A slow query that returns after the bound does not count.
            if time.monotonic() >= deadline:
                self.candidate.fail(
                    "API pods did not settle to one serving pod after rollout "
                    f"(pods={listed} serving={serving})"
                )
            if listed == 1 and serving == 1:
                return
            time.sleep(ROLLOUT_POLL_SECONDS)

    def probe(self, operation: str, **fields: Any) -> dict[str, Any]:
        result = self.kubectl(
            "exec",
            "-i",
            self.api_pod(),
            "-c",
            "api",
            "--",
            "python3",
            "-",
            json.dumps({"operation": operation, **fields}),
            input_text=(TESTS / "forge_instance_probe.py").read_text(),
            timeout=90,
        )
        lines = [
            line.removeprefix("FORGE_OBSERVATION=")
            for line in result.stdout.splitlines()
            if line.startswith("FORGE_OBSERVATION=")
        ]
        if len(lines) != 1:
            return self.candidate.fail("API observation did not emit one safe receipt")
        return dict(json.loads(lines[0]))

    def check_bridges(self) -> None:
        expected = {f"bridge-{name}" for name in self.peers}
        deployment = json.loads(
            self.kubectl("get", "deployment", f"{FULLNAME}-api", "-o", "json").stdout
        )
        present = {c["name"] for c in deployment["spec"]["template"]["spec"]["containers"]}
        pod = json.loads(self.kubectl("get", "pod", self.api_pod(), "-o", "json").stdout)
        serving = {c["name"] for c in pod["spec"]["containers"]}
        if not expected <= present or not expected <= serving:
            self.candidate.fail("Helm rollout did not preserve the external fixture bridges")
        if self.probe("bridge", urls=[p.origin for p in self.peers.values()]) != {
            "reachable": True
        }:
            self.candidate.fail("serving API cannot reach the independent external peers")
        for peer in self.peers.values():
            self.control(peer, "/__fixture/health")

    def api(self, method: str, path: str, body: Any = None) -> Any:
        request = Request(
            f"http://127.0.0.1:{self.api_port}{path}",
            method=method,
            headers={"X-API-Key": self.api_key, "Content-Type": "application/json"},
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except (HTTPError, URLError, ValueError) as error:
            return self.candidate.fail(f"real API {method} {path}: {type(error).__name__}")

    def values(self) -> dict[str, Any]:
        return dict(json.loads(self.helm("get", "values", RELEASE, "-o", "json").stdout))

    def replace_values(self, values: dict[str, Any], *, settle: bool = True) -> None:
        """An operator's values change. `settle=False` records the revision
        without waiting for workloads, for a change made while the CLI is held
        inside an external request bounded by its HTTP timeout; call
        `settle_release` once that request is released."""
        path = self.root / "operator-values.yaml"
        private_write(path, yaml.safe_dump(values))
        self.helm(
            "upgrade",
            RELEASE,
            str(self.candidate.source / "charts/curie"),
            "--reset-values",
            "-f",
            str(path),
            "--timeout",
            "240s",
            *(["--wait"] if settle else []),
            timeout=300,
        )
        if settle:
            self.settle_release()

    def settle_release(self) -> None:
        self.rollout()
        self.check_bridges()

    def objects(self) -> dict[str, dict[str, Any]]:
        result = json.loads(self.kubectl("get", "secrets,configmaps", "-o", "json").stdout)
        return {f"{o['kind']}/{o['metadata']['name']}": object_state(o) for o in result["items"]}

    def snapshot(self) -> Snapshot:
        return Snapshot(
            self.values(),
            json.loads(self.helm("history", RELEASE, "--max", "1024", "-o", "json").stdout),
            self.objects(),
            self.api("GET", "/agents"),
            {name: self.receipts(peer)["label_writes"] for name, peer in self.peers.items()},
        )

    def require_restoration(
        self,
        original: Snapshot,
        *,
        changed_objects: set[str] | None = None,
        changes_helm: bool = False,
        label_writes: dict[str, int] | None = None,
    ) -> None:
        restored = self.snapshot()
        allowed = set(changed_objects or ())
        old_objects, new_objects = original.objects, restored.objects
        if changes_helm:
            old_objects = {
                k: v
                for k, v in old_objects.items()
                if not k.startswith(f"Secret/sh.helm.release.v1.{RELEASE}.v")
            }
            new_objects = {
                k: v
                for k, v in new_objects.items()
                if not k.startswith(f"Secret/sh.helm.release.v1.{RELEASE}.v")
            }
            allowed.update(
                k
                for k, v in old_objects.items()
                if v["metadata"].get("annotations", {}).get("meta.helm.sh/release-name") == RELEASE
            )
        object_ok = old_objects.keys() == new_objects.keys() and all(
            object_state(old_objects[k], metadata=k not in allowed)
            == object_state(new_objects[k], metadata=k not in allowed)
            for k in old_objects
        )
        expected_writes = dict(original.label_writes)
        for peer, count in (label_writes or {}).items():
            expected_writes[peer] += count
        differing = [
            name
            for name, same in (
                ("Secret/ConfigMap objects", object_ok),
                ("Helm values", original.values == restored.values),
                ("agents", original.agents == restored.agents),
                ("forge label writes", expected_writes == restored.label_writes),
                ("Helm history", changes_helm or original.history == restored.history),
            )
            if not same
        ]
        if differing:
            # Names only: the compared documents can carry fixture credentials.
            self.candidate.fail(
                "operator vector did not restore all arranged state: " + ", ".join(differing)
            )

    def secret(self, entry: dict[str, Any]) -> dict[str, Any]:
        name = entry["credential"]["existingSecret"]
        return json.loads(self.kubectl("get", "secret", name, "-o", "json").stdout)

    def quickstart_args(
        self,
        peer: Peer,
        *,
        agent: str = "acme-first",
        token: Path | None = None,
        label: str | None = None,
        repo: str | None = None,
        url: str | None = None,
        ca: Path | None = None,
        app_flags: bool = False,
        jira: Peer | None = None,
        username: str = "acme-bot@example.com",
        accounts: Sequence[str] = ("acme-starter",),
    ) -> list[str]:
        if self.candidate.binary is None:
            self.candidate.fail("candidate binary unavailable")
        args = [
            str(self.candidate.binary),
            "factory",
            "quickstart",
            "--context",
            self.context,
            "--namespace",
            self.namespace,
            "--release",
            RELEASE,
            "--chart",
            str(self.candidate.source / "charts/curie"),
            "--plugin-dir",
            str(self.bundle),
            "--agent",
            agent,
        ]
        if peer.kind == "gitlab":
            args += [
                "--code-host",
                "gitlab",
                "--tracker",
                "gitlab",
                "--forge-url",
                url or peer.origin,
                "--repo",
                repo or peer.path,
                "--token-file",
                str(token or peer.token_file),
            ]
            if ca or peer.ca:
                args += ["--ca-file", str(ca or peer.ca)]
        elif peer.kind == "jira":
            args += [
                "--code-host",
                "github",
                "--tracker",
                "jira-cloud",
                "--forge-url",
                url or peer.origin,
                "--token-file",
                str(token or peer.token_file),
                "--forge-username",
                username,
                "--repo",
                repo or "acme-corp/acme-bot",
            ]
            for account in accounts:
                args += ["--start-account", account]
            if app_flags:
                args += [
                    "--app-id",
                    "51",
                    "--private-key-file",
                    str(self.app_key),
                    "--org",
                    "acme-corp",
                ]
        elif peer.kind == "dc":
            if jira is None:
                self.candidate.fail("DC fixture requires a Jira peer")
            args += [
                "--code-host",
                "bitbucket-dc",
                "--tracker",
                "jira-cloud",
                "--forge-url",
                peer.origin + "/bitbucket",
                "--token-file",
                str(peer.token_file),
                "--repo",
                "ACME/widgets",
                "--review-account",
                "17",
                "--tracker-url",
                jira.origin,
                "--tracker-token-file",
                str(token or jira.token_file),
                "--forge-username",
                username,
            ]
            for account in accounts:
                args += ["--start-account", account]
        if label:
            args += ["--label", label]
        return args

    def quickstart(self, peer: Peer, **kwargs: Any) -> Result:
        self.check_bridges()
        result = self.run(self.quickstart_args(peer, **kwargs), check=False, timeout=360)
        if result.returncode == 0:
            self.rollout()
            self.check_bridges()
        return result

    def seed_gitlab(self, peer: Peer | None = None) -> None:
        result = self.quickstart(peer or self.peers["gitlab-a"])
        if result.returncode:
            self.candidate.fail("first GitLab quickstart did not establish the scenario")
        values = self.values()
        for tracker in values["factory"]["trackers"]:
            if tracker["kind"] == "gitlab":
                tracker["pollIntervalSeconds"] = 2
        self.replace_values(values)

    def poll_receipts(self, peers: Sequence[Peer], *, timeout: float = 75) -> dict[str, Any]:
        # No CLI command or adapter probe runs during this observation window.
        since = {
            p.name: max((r["seq"] for r in self.receipts(p)["requests"]), default=0) for p in peers
        }
        end = min(time.monotonic() + timeout, self.candidate.deadline)
        observed: dict[str, Any] = {}
        while time.monotonic() < end:
            for peer in peers:
                rows = [
                    r
                    for r in self.receipts(peer)["requests"]
                    if r["seq"] > since[peer.name] and r["operation"] == "poll"
                ]
                if rows:
                    observed[peer.name] = rows
            if len(observed) == len(peers):
                return observed
            time.sleep(1)
        return observed

    @contextmanager
    def operator_vector(
        self,
        arrange: Callable[[], None],
        restore: Callable[[], None],
        *,
        changed_objects: set[str] | None = None,
        changes_helm: bool = False,
    ) -> Iterator[None]:
        original = self.snapshot()
        labels = {p.name: self.receipts(p)["labels"] for p in self.peers.values()}
        try:
            arrange()
            yield
        finally:
            restore()
            for peer in self.peers.values():
                self.control(peer, "/__fixture/restore", {"labels": labels[peer.name]})
            # Kubernetes resourceVersions and Helm revisions are monotonic.
            # Require exact receipts across the refused CLI call. Restoration
            # compares full state while allowing only the operator's enumerated
            # metadata/revision changes, then the next vector takes a new receipt.
            self.require_restoration(
                original, changed_objects=changed_objects, changes_helm=changes_helm
            )

    def close(self) -> None:
        if self.closed:
            return
        failures: list[str] = []
        for handle in self.reservations:
            handle.close()
        self.reservations.clear()
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=10)
            if process.poll() is None:
                failures.append("owned port-forward remains")
        if self.created:
            # Discover labels again so partial kind creation is owned even if
            # create failed before its node list could be recorded.
            partial = self.run(
                ["docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.name}"],
                cleanup=True,
            ).stdout.splitlines()
            for container in partial:
                detail = json.loads(
                    self.run(["docker", "inspect", container], cleanup=True).stdout
                )[0]
                self.nodes.add(container)
                self.networks.update(detail.get("NetworkSettings", {}).get("Networks", {}))
                self.volumes.update(
                    m["Name"] for m in detail.get("Mounts", []) if m["Type"] == "volume"
                )
            # The scenario namespace lives only inside this owned cluster, so
            # the verified cluster deletion below removes it with everything else.
            self.run(
                ["kind", "delete", "cluster", "--name", self.name, "--kubeconfig", self.kubeconfig],
                timeout=180,
                cleanup=True,
            )
            if self.name in self.candidate.names("clusters", cleanup=True):
                failures.append("owned kind cluster remains")
            if self.run(
                ["docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.name}"],
                cleanup=True,
            ).stdout.strip():
                failures.append("labelled kind node resources remain")
            for node in self.nodes:
                existing = self.run(["docker", "inspect", node], check=False, cleanup=True)
                if existing.returncode == 0:
                    failures.append("owned kind node remains")
            remaining_volumes = self.candidate.names("volumes", cleanup=True)
            for volume in self.volumes - self.candidate.before_volumes:
                if volume in remaining_volumes:
                    self.run(["docker", "volume", "rm", volume], cleanup=True)
            if (self.volumes - self.candidate.before_volumes) & self.candidate.names(
                "volumes", cleanup=True
            ):
                failures.append("owned kind volumes remain")
            # kind's shared network can be removed only if this invocation
            # created it and no unrelated node or concurrent job is attached.
            for network in self.networks:
                inspect = self.run(
                    ["docker", "network", "inspect", network], check=False, cleanup=True
                )
                if inspect.returncode == 0:
                    data = json.loads(inspect.stdout)[0]
                    if data["Id"] not in self.candidate.before_networks and not data.get(
                        "Containers"
                    ):
                        self.run(["docker", "network", "rm", data["Id"]], cleanup=True)
            sentinel = self.candidate.sentinel
            if (
                sentinel
                and self.run(["docker", "inspect", sentinel], check=False, cleanup=True).returncode
            ):
                failures.append("the unrelated sentinel container did not survive cleanup")
        if failures:
            self.candidate.fail("; ".join(failures))
        self.closed = True


def entries(values: dict[str, Any], section: str, kind: str) -> list[dict[str, Any]]:
    return [e for e in values["factory"].get(section, []) if e.get("kind") == kind]


def binding_for(values: dict[str, Any], peer: Peer) -> dict[str, Any]:
    trackers = entries(values, "trackers", "gitlab" if peer.kind == "gitlab" else "jira_cloud")
    tracker = next(
        e
        for e in trackers
        if e["apiUrl"].rstrip("/") == (peer.origin + ("/api/v4" if peer.kind == "gitlab" else ""))
    )
    return next(b for b in values["factory"]["bindings"] if b["tracker"] == tracker["name"])


def named(values: dict[str, Any], section: str, name: str) -> dict[str, Any]:
    return next(e for e in values["factory"][section] if e["name"] == name)


def diagnostic(result: Result) -> str:
    return result.stdout + "\n" + result.stderr


def refused_for(result: Result, *terms: str) -> bool:
    text = diagnostic(result).lower()
    return result.returncode != 0 and all(re.search(term, text) is not None for term in terms)
