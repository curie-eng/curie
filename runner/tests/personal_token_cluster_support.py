"""Owned exact-image resources and private observations for the personal proof."""

from __future__ import annotations

import base64
import contextlib
import http.client
import importlib.util
import io
import json
import os
import queue
import re
import secrets
import shlex
import shutil
import socket
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

__tracebackhide__ = True

MODEL = "claude-sonnet-5-5"
RUNNER_PORT = 8080
SINK_PORT = 4318
GUARD_PORT = 4319
GROUP = "extensions.agents.x-k8s.io"
VERSION = "v1beta1"
BUNDLE = "/bundle/custody"
PUBLIC = "PUBLIC-CONTROL-personal-custody"
BASH = "BASH-CONTROL-personal-custody"
HOOK = "HOOK-CONTROL-personal-custody"
HELPER = "HELPER-CONTROL-personal-custody"
CONNECTOR = "connector-control-permitted"
SYNTHETIC_GRANT = "synthetic-personal-cluster-custody-sentinel"
CONTAINER_SECURITY = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "runAsNonRoot": True,
    "capabilities": {"drop": ["ALL"]},
}
POD_SECURITY = {
    "runAsNonRoot": True,
    "runAsUser": 1000,
    "runAsGroup": 1000,
    "fsGroup": 1000,
    "seccompProfile": {"type": "RuntimeDefault"},
}


class ProofCheckError(AssertionError):
    """A fixed test-authored diagnostic safe for the public evidence receipt."""


def require(condition: object, message: str) -> None:
    __tracebackhide__ = True
    if not condition:
        raise ProofCheckError(message) from None


class HeldSecret:
    def __init__(self, value: str) -> None:
        self._value = value

    def __repr__(self) -> str:
        return "<held secret>"

    def reveal(self) -> str:
        return self._value

    def clear(self) -> None:
        self._value = ""


def scrubbed_env() -> dict[str, str]:
    """Subprocesses receive transport configuration, never caller credentials."""

    return {
        key: os.environ[key]
        for key in ("PATH", "HOME", "DOCKER_CONTEXT", "KUBECONFIG", "SSH_AUTH_SOCK")
        if key in os.environ
    } | {"PYTHONDONTWRITEBYTECODE": "1"}


class BudgetLedger:
    """A started session with unknown usage reserves the entire remaining budget."""

    def __init__(self, run_id: str) -> None:
        __tracebackhide__ = True
        require(bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", run_id)), "invalid proof run ID")
        directory = Path(
            os.environ.get(
                "CURIE_PERSONAL_PROOF_STATE_DIR",
                str(Path.home() / ".local/state/curie-personal-token-verification"),
            )
        )
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(directory.stat().st_mode & 0o077 == 0, "proof state directory must be private")
        path = directory / "budget.sqlite3"
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        require(path.stat().st_mode & 0o077 == 0, "proof ledger must be private")
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS sessions ("
            "run_id TEXT NOT NULL, sequence INTEGER NOT NULL, candidate TEXT NOT NULL, "
            "started REAL NOT NULL, reserved INTEGER NOT NULL, output_tokens INTEGER, "
            "assistant_turns INTEGER, elapsed REAL, PRIMARY KEY(run_id, sequence))"
        )
        self.run_id = run_id

    def start(self, candidate: str) -> tuple[int, int]:
        __tracebackhide__ = True
        self.db.execute("BEGIN IMMEDIATE")
        try:
            rows = self.db.execute(
                "SELECT output_tokens, reserved FROM sessions WHERE run_id=? ORDER BY sequence",
                (self.run_id,),
            ).fetchall()
            require(len(rows) < 2, "the logical proof has used its two-session allowance")
            spent = sum(output if output is not None else reserved for output, reserved in rows)
            remaining = 1024 - spent
            require(remaining > 0, "the logical proof has no remaining output-token budget")
            sequence = len(rows) + 1
            self.db.execute(
                "INSERT INTO sessions(run_id,sequence,candidate,started,reserved) "
                "VALUES(?,?,?,?,?)",
                (self.run_id, sequence, candidate, time.time(), remaining),
            )
            self.db.execute("COMMIT")
            return sequence, remaining
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def remaining(self) -> int:
        rows = self.db.execute(
            "SELECT output_tokens,reserved FROM sessions WHERE run_id=?", (self.run_id,)
        ).fetchall()
        require(len(rows) < 2, "the logical proof has used its two-session allowance")
        remaining = 1024 - sum(
            output if output is not None else reserved for output, reserved in rows
        )
        require(remaining > 0, "the logical proof has no remaining output-token budget")
        return remaining

    def finish(self, sequence: int, tokens: int, turns: int, elapsed: float) -> dict[str, Any]:
        self.db.execute(
            "UPDATE sessions SET output_tokens=?,assistant_turns=?,elapsed=? "
            "WHERE run_id=? AND sequence=?",
            (tokens, turns, elapsed, self.run_id, sequence),
        )
        rows = self.db.execute(
            "SELECT output_tokens,reserved FROM sessions WHERE run_id=?", (self.run_id,)
        ).fetchall()
        total = sum(output if output is not None else reserved for output, reserved in rows)
        require(len(rows) <= 2 and total <= 1024, "the logical proof exceeded its live budget")
        require(0 < turns <= 4 and elapsed <= 180, "the live session exceeded a session bound")
        return {
            "sessions": len(rows),
            "assistant_turns": turns,
            "output_tokens": tokens,
            "total_output_tokens": total,
            "elapsed_seconds": round(elapsed, 3),
        }


_HOOK_SCRIPT = """\
import os, pathlib, subprocess, sys
if "CLAUDE_CODE_OAUTH_TOKEN" in os.environ:
    raise RuntimeError("personal credential reached a command hook")
markers = pathlib.Path("/tmp/custody-markers")
markers.mkdir(exist_ok=True)
(markers / "hook.marker").write_text("HOOK-CONTROL-personal-custody")
subprocess.run([sys.executable, str(pathlib.Path(__file__).with_name("helper.py"))], check=True)
print("{}")
"""
_HELPER_SCRIPT = """\
import os, pathlib
if "CLAUDE_CODE_OAUTH_TOKEN" in os.environ:
    raise RuntimeError("personal credential reached a helper")
pathlib.Path("/tmp/custody-markers/helper.marker").write_text("HELPER-CONTROL-personal-custody")
"""
_MCP_SCRIPT = """\
import json, os, pathlib, sys
root = pathlib.Path(__file__).parent
for line in sys.stdin:
    request = json.loads(line)
    if "id" not in request:
        continue
    method = request["method"]
    if method == "initialize":
        result = {"protocolVersion": request.get("params", {}).get("protocolVersion", "2024-11-05"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "custody", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "public_file", "description": "Read the public control file",
                  "inputSchema": {"type": "object", "properties": {},
                                  "additionalProperties": False},
                  "annotations": {"readOnlyHint": True}}]}
    elif method == "tools/call":
        if os.environ.get("ACME_CONNECTOR_TOKEN") != "connector-control-permitted":
            raise RuntimeError("permitted connector unavailable")
        value = (root / "public.txt").read_text()
        markers = pathlib.Path("/tmp/custody-markers")
        markers.mkdir(exist_ok=True)
        (markers / "mcp.marker").write_text(value)
        result = {"content": [{"type": "text", "text": value}], "isError": False}
    elif method == "ping":
        result = {}
    else:
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"],
                          "error": {"code": -32601, "message": "unknown method"}}), flush=True)
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
"""
_STAGE_SCRIPT = """\
import json, pathlib
root = pathlib.Path("/bundle/custody")
for name, text in json.loads(pathlib.Path("/seed/files.json").read_text()).items():
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
"""
_SINK_SCRIPT = """\
import http.server, pathlib, threading
root = pathlib.Path("/tmp/custody-capture")
root.mkdir(exist_ok=True)
lock = threading.Lock()
class Sink(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with lock:
            if self.path == "/v1/traces":
                with (root / "traces.bin").open("ab") as stream: stream.write(data)
            elif self.path.endswith("/usage"):
                with (root / "usage.jsonl").open("ab") as stream: stream.write(data + b"\\n")
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()
class Guard(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_CONNECT(self):
        with (root / "provider-attempts").open("ab") as stream: stream.write(b"attempt\\n")
        self.send_error(502)
    def do_POST(self): self.do_CONNECT()
guard = http.server.ThreadingHTTPServer(("127.0.0.1", 4319), Guard)
threading.Thread(target=guard.serve_forever, daemon=True).start()
http.server.ThreadingHTTPServer(("127.0.0.1", 4318), Sink).serve_forever()
"""


def bundle_files() -> dict[str, str]:
    manifest = {
        "name": "custody",
        "version": "0.1.0",
        "mcpServers": {
            "public": {
                "command": "python3",
                "args": ["${CLAUDE_PLUGIN_ROOT}/mcp.py"],
                "env": {"ACME_CONNECTOR_TOKEN": "${ACME_CONNECTOR_TOKEN}"},
            }
        },
    }
    hooks = {
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": "Read|Bash",
                    "hooks": [
                        {
                            "type": "command",
                            "command": "python3 ${CLAUDE_PLUGIN_ROOT}/hook.py",
                            "timeout": 10,
                        }
                    ],
                }
            ]
        }
    }
    return {
        ".claude-plugin/plugin.json": json.dumps(manifest),
        "hooks/hooks.json": json.dumps(hooks),
        "public.txt": PUBLIC,
        "hook.py": _HOOK_SCRIPT,
        "helper.py": _HELPER_SCRIPT,
        "mcp.py": _MCP_SCRIPT,
    }


def value_env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


def secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


class ClusterProof:
    def __repr__(self) -> str:
        return "<private cluster proof>"

    def __init__(self, grant: HeldSecret, phase: str) -> None:
        self.grant = grant
        self.phase = phase
        self.env = scrubbed_env()
        self.root = Path(__file__).resolve().parents[2]
        self.context = os.environ["THROWAWAY_CONTEXT"]
        self.namespace = os.environ["CURIE_PERSONAL_TEST_NAMESPACE"]
        self.docker_context = os.environ["DOCKER_CONTEXT"]
        self.cli = os.environ["CURIE_BIN"]
        self.runner_auth = HeldSecret(secrets.token_urlsafe(32))
        self.progress_auth = HeldSecret(secrets.token_urlsafe(32))
        self.registry = ""
        self.registry_volumes: list[str] = []
        self.tags: list[str] = []
        self.processes: list[subprocess.Popen[bytes]] = []
        self.readers: list[threading.Thread] = []
        self.namespace_uid = ""
        self.namespace_requested = False
        self.ownership = secrets.token_hex(12)
        self.private_config = ""
        self.api_client: Any = None
        self.core: Any = None
        self.custom: Any = None
        self.worker: Any = None
        self.ledger: BudgetLedger | None = None
        self.candidate = ""
        self.digest = ""
        self.image = ""
        self.source: dict[str, Any] = {}
        self.source_pool: dict[str, Any] = {}
        self.captures: list[bytes] = []
        self.command_log: sqlite3.Connection | None = None
        self.evidence: dict[str, Any] = {
            "phase": phase,
            "live_proof_run": False,
            "budget": {
                "max_sessions": 2,
                "max_assistant_turns": 4,
                "max_total_output_tokens": 1024,
                "max_session_seconds": 180,
                "model": MODEL,
            },
        }

    def command(
        self,
        argv: Sequence[str],
        *,
        data: bytes | None = None,
        timeout: float = 120,
        check: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        __tracebackhide__ = True
        for secret in (self.grant, self.runner_auth, self.progress_auth):
            raw = secret.reveal()
            require(
                not raw or all(raw not in arg for arg in argv), "credential in subprocess argument"
            )
            require(
                not raw or all(raw not in val for val in self.env.values()),
                "credential in child environment",
            )
            require(
                not raw or data is None or raw.encode() not in data,
                "credential in subprocess input",
            )
        try:
            result = subprocess.run(
                list(argv),
                input=data,
                stdin=subprocess.DEVNULL if data is None else None,
                capture_output=True,
                timeout=timeout,
                cwd=self.root,
                env=self.env,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("private proof command could not complete") from None
        if self.command_log is not None:
            from curie_telemetry.redact import held_secret_literals

            outputs = [result.stdout, result.stderr]
            if "config" in argv and "--raw" in argv:
                outputs = [b"[private kubeconfig omitted]", b""]
            for secret in held_secret_literals(
                [
                    self.grant.reveal(),
                    self.runner_auth.reveal(),
                    self.progress_auth.reveal(),
                ]
            ):
                outputs = [
                    part.replace(secret.encode(), b"[held secret redacted]") for part in outputs
                ]
            try:
                self.command_log.execute(
                    "INSERT INTO commands(argv,exit_code,stdout,stderr) VALUES(?,?,?,?)",
                    (json.dumps(list(argv)), result.returncode, *outputs),
                )
                self.command_log.commit()
            except sqlite3.Error:
                self.evidence["command_transcript_failed"] = True
        if check:
            if result.returncode != 0:
                self.evidence["failed_command"] = {
                    "program": Path(argv[0]).name,
                    "exit_code": result.returncode,
                }
            require(result.returncode == 0, "private proof command failed; output withheld")
        return result

    def docker(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return self.command(["docker", "--context", self.docker_context, *args], **kwargs)

    def kubectl(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return self.command(
            [
                "kubectl",
                "--context",
                self.context,
                "--namespace",
                self.namespace,
                *args,
            ],
            **kwargs,
        )

    def setup(self) -> None:
        __tracebackhide__ = True
        from curie_worker.sandbox.k8s import KubernetesSandboxClient
        from kubernetes import client, config

        state = Path.home() / ".local/state/curie-personal-token-verification"
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(state.stat().st_mode & 0o077 == 0, "proof transcript directory must be private")
        log_path = state / f"commands-{self.ownership}.sqlite3"
        fd = os.open(log_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        self.command_log = sqlite3.connect(log_path)
        self.command_log.execute(
            "CREATE TABLE commands (ordinal INTEGER PRIMARY KEY, argv TEXT NOT NULL, "
            "exit_code INTEGER NOT NULL, stdout BLOB NOT NULL, stderr BLOB NOT NULL)"
        )

        require(
            bool(re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,48}[a-z0-9])?", self.namespace)),
            "a fresh DNS-label namespace is required",
        )
        for executable in ("docker", "kubectl", "ssh", "git", self.cli):
            require(
                shutil.which(executable) is not None, "a required proof executable is unavailable"
            )
        require(os.path.isabs(self.cli), "CURIE_BIN must name the source-built CLI")
        require(
            self.command(["git", "status", "--porcelain", "--untracked-files=all"]).stdout == b"",
            "candidate source must be clean before building",
        )
        self.candidate = self.command(["git", "rev-parse", "HEAD"]).stdout.decode().strip()
        require(bool(re.fullmatch(r"[0-9a-f]{40}", self.candidate)), "invalid candidate commit")
        self.evidence["candidate_sha"] = self.candidate
        if self.phase == "live":
            self.ledger = BudgetLedger(os.environ["CURIE_PERSONAL_PROOF_RUN_ID"])
            self.ledger.remaining()
        raw_config = self.kubectl("config", "view", "--minify", "--flatten", "--raw").stdout
        fd, self.private_config = tempfile.mkstemp(prefix="curie-private-kube-", suffix=".yaml")
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw_config)
        del raw_config
        configuration = client.Configuration()
        config.load_kube_config(config_file=self.private_config, client_configuration=configuration)
        configuration.retries = 0
        self.api_client = client.ApiClient(configuration)
        self.core = client.CoreV1Api(self.api_client)
        self.custom = client.CustomObjectsApi(self.api_client)
        try:
            self.core.read_namespace(self.namespace, _request_timeout=20)
        except client.ApiException as exc:
            require(exc.status == 404, "could not verify fresh namespace")
        else:
            raise RuntimeError("the requested namespace already exists; refusing adoption")
        require(
            self.kubectl("get", "--raw=/readyz").stdout.strip() == b"ok", "cluster is not ready"
        )
        self.build_image()
        self.namespace_requested = True
        created = self.core.create_namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": self.namespace,
                    "labels": {
                        "curie-test": "personal-custody",
                        "curie-proof-owner": self.ownership,
                    },
                },
            },
            _request_timeout=30,
        )
        self.namespace_uid = created.metadata.uid
        self.worker = KubernetesSandboxClient(self.namespace, kubeconfig=self.private_config)
        self.create_inputs()

    def build_image(self) -> None:
        __tracebackhide__ = True
        endpoint = json.loads(
            self.docker(
                "context",
                "inspect",
                self.docker_context,
                "--format",
                "{{json .Endpoints.docker.Host}}",
            ).stdout
        )
        parsed = urlsplit(endpoint)
        require(
            parsed.scheme == "ssh" and parsed.hostname,
            "scratch Docker requires an explicit SSH endpoint",
        )
        remote = (f"{parsed.username}@" if parsed.username else "") + str(parsed.hostname)
        ssh = ["ssh", "-o", "BatchMode=yes"]
        if parsed.port:
            ssh += ["-p", str(parsed.port)]
        ssh.append(remote)
        root = self.docker("info", "--format", "{{.DockerRootDir}}").stdout.decode().strip()
        disk = self.command([*ssh, shlex.join(["df", "-P", root])]).stdout.decode().splitlines()
        require(
            len(disk) >= 2 and int(disk[-1].split()[-2].rstrip("%")) <= 82,
            "scratch Docker storage exceeds the build threshold",
        )
        script = (
            "import socket\n"
            "for port in range(25100,26000):\n"
            " s=socket.socket()\n"
            " try: s.bind(('127.0.0.1',port))\n"
            " except OSError: s.close(); continue\n"
            " s.close(); print(port); break\n"
            "else: raise SystemExit(1)\n"
        )
        port = int(self.command([*ssh, shlex.join(["python3", "-c", script])]).stdout)
        registry = f"{self.namespace}-registry"
        require(
            self.docker("container", "inspect", registry, check=False).returncode != 0,
            "owned registry name is already in use",
        )
        self.docker("image", "inspect", "registry:2.8.3")
        self.registry = registry
        self.docker(
            "run",
            "--detach",
            "--rm",
            "--name",
            self.registry,
            "--label",
            f"curie-proof-owner={self.ownership}",
            "--publish",
            f"127.0.0.1:{port}:5000",
            "registry:2.8.3",
        )
        mounts = json.loads(
            self.docker(
                "container",
                "inspect",
                self.registry,
                "--format",
                "{{json .Mounts}}",
            ).stdout
        )
        self.registry_volumes = [mount["Name"] for mount in mounts if mount["Type"] == "volume"]
        tag = f"{self.namespace}-runner:{self.candidate[:12]}"
        published = f"127.0.0.1:{port}/curie-runner-candidate:{self.candidate[:12]}"
        for name in (tag, published):
            require(
                self.docker("image", "inspect", name, check=False).returncode != 0,
                "candidate image tag is already in use",
            )
            self.tags.append(name)
        self.command([self.cli, "build", "--tag", tag], timeout=1200)
        dockerfile = f"FROM {tag}\nLABEL org.opencontainers.image.revision={self.candidate}\n"
        self.docker(
            "build", "--file", "-", "--tag", published, ".", data=dockerfile.encode(), timeout=300
        )
        revision = (
            self.docker(
                "image",
                "inspect",
                published,
                "--format",
                '{{index .Config.Labels "org.opencontainers.image.revision"}}',
            )
            .stdout.decode()
            .strip()
        )
        require(revision == self.candidate, "candidate OCI revision does not match source")
        self.docker(
            "exec", self.registry, "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:5000/v2/"
        )
        self.docker("push", published, timeout=300)
        digests = json.loads(
            self.docker("image", "inspect", published, "--format", "{{json .RepoDigests}}").stdout
        )
        matches = [
            value.rsplit("@", 1)[1]
            for value in digests
            if value.startswith(f"127.0.0.1:{port}/curie-runner-candidate@")
        ]
        require(
            len(matches) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", matches[0]),
            "pushed candidate manifest digest is unavailable",
        )
        self.digest = matches[0]
        self.image = f"localhost:{port}/curie-runner-candidate@{self.digest}"
        require(
            self.command(["git", "status", "--porcelain", "--untracked-files=all"]).stdout == b"",
            "candidate source changed during the build",
        )
        require(
            self.command(["git", "rev-parse", "HEAD"]).stdout.decode().strip() == self.candidate,
            "candidate commit changed during the build",
        )
        self.evidence["manifest_digest"] = self.digest
        self.evidence["oci_revision_matches_source"] = True

    def secret(self, name: str, values: Mapping[str, str]) -> None:
        __tracebackhide__ = True
        self.core.create_namespaced_secret(
            self.namespace,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "type": "Opaque",
                "metadata": {"name": name},
                "stringData": dict(values),
            },
            _request_timeout=30,
        )

    def extension(self, plural: str, kind: str, name: str, spec: Mapping[str, Any]) -> None:
        self.custom.create_namespaced_custom_object(
            GROUP,
            VERSION,
            self.namespace,
            plural,
            {
                "apiVersion": f"{GROUP}/{VERSION}",
                "kind": kind,
                "metadata": {"name": name},
                "spec": spec,
            },
            _request_timeout=30,
        )

    def create_inputs(self) -> None:
        self.secret(
            "installation-model",
            {"grant": "installation-control-unusable", "declared": "declared-control"},
        )
        self.secret("acme-connector-secrets", {"token": CONNECTOR})
        self.secret("opaque-model", {"ANTHROPIC_API_KEY": "opaque-control"})
        self.core.create_namespaced_config_map(
            self.namespace,
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "custody-fixture"},
                "data": {
                    "files.json": json.dumps(bundle_files()),
                    "stage.py": _STAGE_SCRIPT,
                    "sink.py": _SINK_SCRIPT,
                },
            },
            _request_timeout=30,
        )
        self.source = {
            "service": True,
            "envVarsInjectionPolicy": "Overrides",
            "networkPolicyManagement": "Unmanaged",
            "podTemplate": {
                "metadata": {"labels": {"curie-test": "personal-custody"}},
                "spec": self.pod_spec(),
            },
        }
        self.extension("sandboxtemplates", "SandboxTemplate", "custody-source", self.source)
        self.extension(
            "sandboxwarmpools",
            "SandboxWarmPool",
            "custody-source-pool",
            {
                "replicas": 0,
                "sandboxTemplateRef": {"name": "custody-source"},
            },
        )
        self.source = self.custom.get_namespaced_custom_object(
            GROUP,
            VERSION,
            self.namespace,
            "sandboxtemplates",
            "custody-source",
            _request_timeout=30,
        )["spec"]
        self.source_pool = self.custom.get_namespaced_custom_object(
            GROUP,
            VERSION,
            self.namespace,
            "sandboxwarmpools",
            "custody-source-pool",
            _request_timeout=30,
        )["spec"]

    def pod_spec(self) -> dict[str, Any]:
        model_env = [
            secret_env("CURIE_CREDENTIALS", "installation-model", "grant"),
            value_env("CURIE_MODEL_ENV_KEY", "ACME_MODEL_TOKEN"),
            secret_env("ACME_MODEL_TOKEN", "installation-model", "declared"),
        ]
        mounts = [
            {"name": name, "mountPath": path}
            for name, path in (
                ("tmp", "/tmp"),
                ("home", "/home/runner"),
                ("bundle", "/bundle"),
            )
        ]
        seed = {"name": "seed", "mountPath": "/seed", "readOnly": True}
        common = {
            "image": self.image,
            "imagePullPolicy": "Always",
            "securityContext": CONTAINER_SECURITY,
            "resources": {
                "requests": {"cpu": "100m", "memory": "128Mi"},
                "limits": {"cpu": "2", "memory": "2Gi"},
            },
        }
        runner_env = [
            value_env("HOME", "/home/runner"),
            value_env("PYTHONDONTWRITEBYTECODE", "1"),
            value_env("CURIE_PLUGIN_DIR", BUNDLE),
            value_env("CURIE_SESSION_ID", "custody-unbound"),
            {"name": "CURIE_SANDBOX_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            value_env(
                "CURIE_BUDGET",
                json.dumps({"max_output_tokens_per_run": 1024, "max_usd_per_day": 1}),
            ),
            value_env("CURIE_MAX_TURNS", "4"),
            value_env("CURIE_MODEL", MODEL),
            value_env("CURIE_THINKING", "disabled"),
            value_env("CLAUDE_CODE_MAX_OUTPUT_TOKENS", "256"),
            value_env("CURIE_REVIEWER_MODEL", MODEL),
            value_env("CURIE_RUNNER_PORT", str(RUNNER_PORT)),
            value_env("CURIE_MODEL_AUTH_MODE", "service"),
            value_env("CURIE_PROGRESS_URL", f"http://127.0.0.1:{SINK_PORT}/progress"),
            value_env("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://127.0.0.1:{SINK_PORT}"),
            value_env(
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", f"http://127.0.0.1:{SINK_PORT}/v1/traces"
            ),
            value_env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
            secret_env("ACME_CONNECTOR_TOKEN", "acme-connector-secrets", "token"),
            *model_env,
        ]
        return {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "securityContext": POD_SECURITY,
            "terminationGracePeriodSeconds": 5,
            "volumes": [
                {"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}},
                {"name": "home", "emptyDir": {"sizeLimit": "1Gi"}},
                {"name": "bundle", "emptyDir": {"sizeLimit": "16Mi"}},
                {"name": "seed", "configMap": {"name": "custody-fixture"}},
            ],
            "initContainers": [
                {
                    **common,
                    "name": "fixture-bundle",
                    "command": ["python3", "/seed/stage.py"],
                    "env": model_env,
                    "envFrom": [{"secretRef": {"name": "opaque-model"}}],
                    "volumeMounts": [*mounts, seed],
                }
            ],
            "containers": [
                {
                    **common,
                    "name": "runner",
                    "env": runner_env,
                    "envFrom": [{"secretRef": {"name": "opaque-model"}}],
                    "volumeMounts": [
                        {**mount, **({"readOnly": True} if mount["name"] == "bundle" else {})}
                        for mount in mounts
                    ],
                    "readinessProbe": {
                        "httpGet": {"path": "/healthz", "port": RUNNER_PORT},
                        "periodSeconds": 1,
                        "failureThreshold": 180,
                    },
                },
                {
                    **common,
                    "name": "capture",
                    "command": ["python3", "/seed/sink.py"],
                    "env": model_env,
                    "envFrom": [{"secretRef": {"name": "opaque-model"}}],
                    "volumeMounts": [mounts[0], seed],
                    "readinessProbe": {"tcpSocket": {"port": SINK_PORT}, "periodSeconds": 1},
                },
            ],
        }

    def wait_pod(self, selector: str, timeout: int = 180) -> dict[str, Any]:
        __tracebackhide__ = True
        from kubernetes import watch

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            listing = self.core.list_namespaced_pod(
                self.namespace, label_selector=selector, _request_timeout=30
            )
            for pod in listing.items:
                if pod.status.phase in {"Failed", "Succeeded"}:
                    raise RuntimeError("owned candidate pod exited before becoming ready")
                if any(
                    item.type == "Ready" and item.status == "True"
                    for item in pod.status.conditions or []
                ):
                    obj = self.api_client.sanitize_for_serialization(pod)
                    self.assert_image(obj)
                    return obj
            watcher = watch.Watch()
            try:
                for _event in watcher.stream(
                    self.core.list_namespaced_pod,
                    self.namespace,
                    label_selector=selector,
                    resource_version=listing.metadata.resource_version,
                    timeout_seconds=max(1, min(30, int(deadline - time.monotonic()))),
                    _request_timeout=35,
                ):
                    break
            finally:
                watcher.stop()
        raise RuntimeError("owned candidate pod did not become ready within the setup bound")

    def assert_image(self, pod: Mapping[str, Any]) -> None:
        statuses = (pod.get("status") or {}).get("containerStatuses") or []
        statuses += (pod.get("status") or {}).get("initContainerStatuses") or []
        require(bool(statuses), "candidate pod has no immutable image observations")
        for status in statuses:
            observed = str(status.get("imageID", ""))
            if observed.startswith("docker-pullable://"):
                observed = observed.removeprefix("docker-pullable://")
            require(
                observed == self.image, "running image digest differs from the pushed candidate"
            )
        self.evidence["running_image_matches_manifest"] = True

    def claim(
        self, name: str, *, grant: str | None, output_limit: int = 1024
    ) -> tuple[str, dict[str, Any]]:
        __tracebackhide__ = True
        env = {
            "CURIE_RUNNER_TOKEN": self.runner_auth.reveal(),
            "CURIE_PROGRESS_TOKEN": self.progress_auth.reveal(),
            "CURIE_SESSION_ID": name,
            "CURIE_BUDGET": json.dumps(
                {"max_output_tokens_per_run": output_limit, "max_usd_per_day": 1}
            ),
            "CURIE_MAX_TURNS": "4",
            "CURIE_MODEL": MODEL,
            "CURIE_REVIEWER_MODEL": MODEL,
            "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(max(1, output_limit // 4)),
        }
        if grant is not None:
            env.update({"CURIE_MODEL_AUTH_MODE": "personal", "CLAUDE_CODE_OAUTH_TOKEN": grant})
        self.worker.create_claim(
            name, pool="custody-source-pool", env=env, deadline=time.monotonic() + 90
        )
        pod = self.wait_pod(f"sandbox.users.io/curie-admission-claim={name}")
        return str(pod["metadata"]["name"]), pod

    def observe_claim(
        self, name: str, pod: Mapping[str, Any], *, personal: bool, grant: str
    ) -> None:
        __tracebackhide__ = True
        from curie_worker.sandbox.claim_tokens import claim_object_names

        names = claim_object_names(name)
        claim = self.custom.get_namespaced_custom_object(
            GROUP, VERSION, self.namespace, "sandboxclaims", name, _request_timeout=30
        )
        template = self.custom.get_namespaced_custom_object(
            GROUP, VERSION, self.namespace, "sandboxtemplates", names.template, _request_timeout=30
        )
        self.audit_bytes(json.dumps([claim, template, pod]).encode(), values=(grant,))
        for spec in (template["spec"]["podTemplate"]["spec"], pod["spec"]):
            self.assert_hardening(spec)
            runner = next(item for item in spec["containers"] if item["name"] == "runner")
            entries = {item["name"]: item for item in runner.get("env", [])}
            require(
                entries["ACME_CONNECTOR_TOKEN"]["valueFrom"]["secretKeyRef"]["name"]
                == "acme-connector-secrets",
                "permitted connector reference was lost",
            )
            if personal:
                for container in [*spec["containers"], *spec.get("initContainers", [])]:
                    require(
                        not container.get("envFrom"),
                        "personal copy retained opaque environment sources",
                    )
                    env = {item["name"]: item for item in container.get("env", [])}
                    require(
                        not (
                            {"CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY", "ACME_MODEL_TOKEN"}
                            & env.keys()
                        ),
                        "personal copy retained an installation model credential",
                    )
                    if container["name"] == "runner":
                        require(
                            env["CURIE_MODEL_AUTH_MODE"].get("value") == "personal",
                            "personal marker missing",
                        )
                        ref = (
                            env["CLAUDE_CODE_OAUTH_TOKEN"]
                            .get("valueFrom", {})
                            .get("secretKeyRef", {})
                        )
                        require(
                            ref.get("name") == names.secret
                            and ref.get("key") == "CLAUDE_CODE_OAUTH_TOKEN",
                            "personal grant did not use the per-claim Secret",
                        )
                    else:
                        require(
                            "CLAUDE_CODE_OAUTH_TOKEN" not in env,
                            "grant reference reached another container",
                        )
            else:
                require(
                    entries["CURIE_CREDENTIALS"]["valueFrom"]["secretKeyRef"]["name"]
                    == "installation-model",
                    "service claim lost its installation credential reference",
                )
        source = self.custom.get_namespaced_custom_object(
            GROUP,
            VERSION,
            self.namespace,
            "sandboxtemplates",
            "custody-source",
            _request_timeout=30,
        )
        require(
            source["spec"] == self.source, "source SandboxTemplate changed during claim creation"
        )
        pool = self.custom.get_namespaced_custom_object(
            GROUP,
            VERSION,
            self.namespace,
            "sandboxwarmpools",
            "custody-source-pool",
            _request_timeout=30,
        )
        require(
            pool["spec"] == self.source_pool,
            "source warm pool changed during claim creation",
        )
        self.evidence["personal_claim" if personal else "service_claim"] = {
            "observed": True,
            "source_unchanged": True,
        }

    def assert_hardening(self, spec: Mapping[str, Any]) -> None:
        require(
            spec.get("automountServiceAccountToken") is False,
            "service-account token mount is enabled",
        )
        require(
            spec.get("securityContext") == POD_SECURITY, "candidate pod security context differs"
        )
        for volume in spec.get("volumes", []):
            require(
                "secret" not in volume and "projected" not in volume and "hostPath" not in volume,
                "candidate mounts a credential or host volume",
            )
            require("emptyDir" in volume or "configMap" in volume, "unexpected candidate volume")
        for container in [*spec["containers"], *spec.get("initContainers", [])]:
            security = container.get("securityContext", {})
            require(
                all(security.get(key) == val for key, val in CONTAINER_SECURITY.items()),
                "candidate container hardening differs",
            )

    def create_pod(self, name: str, spec: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        self.core.create_namespaced_pod(
            self.namespace,
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "labels": {"custody-step": name}},
                "spec": spec,
            },
            _request_timeout=30,
        )
        return name, self.wait_pod(f"custody-step={name}")

    def exec(self, pod: str, *args: str, data: bytes | None = None, timeout: float = 120) -> bytes:
        return self.kubectl(
            "exec", "-i", pod, "--container", "runner", "--", *args, data=data, timeout=timeout
        ).stdout

    def synthetic(self) -> None:
        __tracebackhide__ = True
        spec = self.pod_spec()
        spec.pop("initContainers")
        spec["containers"] = [spec["containers"][0]]
        runner = spec["containers"][0]
        runner["command"] = ["python3", "-c", "import threading; threading.Event().wait()"]
        runner.pop("readinessProbe")
        runner.pop("envFrom")
        runner["env"] = [
            value_env("HOME", "/home/runner"),
            value_env("PYTHONDONTWRITEBYTECODE", "1"),
        ]
        pod, _ = self.create_pod("synthetic-corpus", spec)
        self.assert_hardening(spec)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name in ("pytest", "_pytest", "pluggy", "iniconfig", "packaging", "pygments", "py"):
                found = importlib.util.find_spec(name)
                require(
                    found is not None and found.origin is not None,
                    "a synthetic harness dependency is unavailable",
                )
                origin = Path(found.origin)
                path = origin.parent if found.submodule_search_locations is not None else origin
                require(
                    "site-packages" in path.parts,
                    "harness dependency must come from installed site packages",
                )
                tar.add(
                    path,
                    arcname=path.name,
                    filter=lambda info: None if "__pycache__" in info.name else info,
                )
        self.exec(
            pod,
            "python3",
            "-c",
            "import pathlib,sys,tarfile; p=pathlib.Path('/tmp/testdeps'); p.mkdir(); "
            "tarfile.open(fileobj=sys.stdin.buffer,mode='r|').extractall(p,filter='data')",
            data=archive.getvalue(),
        )
        output = self.exec(
            pod,
            "env",
            "PYTHONPATH=/tmp/testdeps:/app/runner/tests",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1",
            "python3",
            "-m",
            "pytest",
            "/app/runner/tests/test_personal_token_containment.py::test_native_personal_token_containment",
            "-q",
            "-o",
            "addopts=",
            "-o",
            "cache_dir=/tmp/pytest-cache",
            timeout=600,
        )
        require(
            bool(re.search(rb"\b28 passed\b", output)),
            "candidate did not pass every native containment case",
        )
        require(
            not re.search(rb"\b(?:skipped|xfailed|xpassed)\b", output),
            "candidate containment corpus omitted a required case",
        )
        self.evidence["synthetic"] = {"passed": 28, "required": 28, "candidate_image": True}

    def refusal(self) -> None:
        __tracebackhide__ = True
        self.secret(
            "refusal-auth",
            {
                "CURIE_RUNNER_TOKEN": self.runner_auth.reveal(),
                "CURIE_PROGRESS_TOKEN": self.progress_auth.reveal(),
            },
        )
        spec = self.pod_spec()
        for container in [*spec["containers"], *spec["initContainers"]]:
            container.pop("envFrom", None)
            container["env"] = [
                item
                for item in container.get("env", [])
                if item["name"]
                not in {
                    "CURIE_CREDENTIALS",
                    "CURIE_MODEL_ENV_KEY",
                    "ACME_MODEL_TOKEN",
                    "CURIE_MODEL_AUTH_MODE",
                }
            ]
        runner = spec["containers"][0]
        runner["env"] += [
            value_env("CURIE_MODEL_AUTH_MODE", "personal"),
            value_env("CURIE_CREDENTIALS", "conflicting-service-control-unusable"),
            value_env("HTTPS_PROXY", f"http://127.0.0.1:{GUARD_PORT}"),
            secret_env("CURIE_RUNNER_TOKEN", "refusal-auth", "CURIE_RUNNER_TOKEN"),
            secret_env("CURIE_PROGRESS_TOKEN", "refusal-auth", "CURIE_PROGRESS_TOKEN"),
        ]
        pod, _ = self.create_pod("missing-grant", spec)
        with self.forward(pod) as port:
            events, raw, _elapsed = self.turn(
                port, "Refuse before model work because no personal grant exists."
            )
        self.captures.append(raw)
        errors = [event for event in events if event.get("type") == "error"]
        require(
            any(
                "authentication required" in str(item.get("message", "")).lower() for item in errors
            ),
            "missing personal grant did not produce authentication-required",
        )
        finals = [event for event in events if event.get("type") == "final"]
        require(
            len(finals) == 1 and finals[0].get("status") == "classified-failure",
            "missing-grant refusal did not return classified-failure",
        )
        require(
            all(event.get("type") != "tool_note" for event in events),
            "missing grant reached tool execution",
        )
        observations = self.observe_files(pod)
        require(observations["provider_attempts"] == 0, "missing grant attempted provider traffic")
        require(
            not observations["assistant_messages"], "missing grant produced an assistant message"
        )
        require(not observations["usage"], "missing grant reported paid model usage")
        self.evidence["missing_grant"] = {
            "authentication_required": True,
            "provider_calls": 0,
            "output_tokens": 0,
        }

    @contextlib.contextmanager
    def forward(self, pod: str) -> Iterator[int]:
        __tracebackhide__ = True
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.context,
                "--namespace",
                self.namespace,
                "port-forward",
                "--address",
                "127.0.0.1",
                f"pod/{pod}",
                f":{RUNNER_PORT}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
        )
        self.processes.append(process)
        lines: queue.Queue[bytes | None] = queue.Queue()

        def drain() -> None:
            require(process.stdout is not None, "port-forward output is unavailable")
            for line in process.stdout:
                lines.put(line)
            lines.put(None)

        reader = threading.Thread(target=drain, daemon=True)
        self.readers.append(reader)
        reader.start()
        try:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                try:
                    line = lines.get(timeout=max(0.01, deadline - time.monotonic()))
                except queue.Empty:
                    break
                require(line is not None, "owned port-forward exited during setup")
                match = re.search(rb"Forwarding from 127\.0\.0\.1:(\d+) ->", line)
                if match:
                    yield int(match[1])
                    return
            raise RuntimeError("owned port-forward did not become ready")
        finally:
            self.stop_process(process)
            reader.join(timeout=10)
            require(not reader.is_alive(), "owned port-forward reader did not stop")

    @staticmethod
    def stop_process(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    def turn(self, port: int, prompt: str) -> tuple[list[dict[str, Any]], bytes, float]:
        __tracebackhide__ = True
        started = time.monotonic()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=180)
        connection.connect()
        connected = connection.sock
        events: list[dict[str, Any]] = []
        raw = bytearray()

        def stop_at_deadline() -> None:
            if connected is not None:
                with contextlib.suppress(OSError):
                    connected.shutdown(socket.SHUT_RDWR)
            connection.close()
            interrupt = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            try:
                interrupt.request(
                    "POST",
                    "/v1/interrupt",
                    body=json.dumps(
                        {
                            "kind": "interrupt",
                            "reason": "proof deadline",
                        }
                    ).encode(),
                    headers={
                        "Authorization": f"Bearer {self.runner_auth.reveal()}",
                        "Content-Type": "application/json",
                    },
                )
            except (OSError, http.client.HTTPException):
                pass
            finally:
                interrupt.close()

        timer = threading.Timer(max(0.01, 180 - (time.monotonic() - started)), stop_at_deadline)
        timer.daemon = True
        timer.start()
        try:
            connection.request(
                "POST",
                "/v1/event",
                body=json.dumps(
                    {
                        "kind": "event",
                        "type": "message",
                        "text": prompt,
                        "user": "U0EXAMPLE1",
                        "ts": "1",
                    }
                ).encode(),
                headers={
                    "Authorization": f"Bearer {self.runner_auth.reveal()}",
                    "Content-Type": "application/json",
                },
            )
            response = connection.getresponse()
            require(response.status == 200, "ACI turn did not return a streaming response")
            while time.monotonic() - started < 180:
                line = response.readline()
                require(bool(line), "ACI stream ended without a final")
                raw.extend(line)
                event = json.loads(line)
                require(isinstance(event, dict), "ACI stream included an invalid event")
                events.append(event)
                if event.get("type") == "final":
                    break
            else:
                raise RuntimeError("ACI turn exceeded its wall ceiling")
        except (OSError, ValueError, http.client.HTTPException):
            raise RuntimeError("ACI turn could not complete; no paid retry is allowed") from None
        finally:
            timer.cancel()
            connection.close()
            timer.join(timeout=5)
        elapsed = time.monotonic() - started
        require(elapsed <= 180, "ACI session exceeded its wall ceiling")
        return events, bytes(raw), elapsed

    def observe_files(self, pod: str) -> dict[str, Any]:
        __tracebackhide__ = True
        script = """\
import base64,json,pathlib,stat
files={}
total=0
for root in ('/home/runner','/tmp','/bundle'):
 for path in pathlib.Path(root).rglob('*'):
  info=path.lstat()
  if not stat.S_ISREG(info.st_mode): continue
  total+=info.st_size
  if info.st_size>16777216 or total>134217728:
   raise RuntimeError('private file audit exceeds its bounded capture size')
  files[str(path)]=base64.b64encode(path.read_bytes()).decode()
for name in ('hook.marker','helper.marker','mcp.marker'):
 path=pathlib.Path('/tmp/custody-markers')/name
 if path.is_file(): files[name]=base64.b64encode(path.read_bytes()).decode()
print(json.dumps(files))
"""
        encoded = json.loads(self.exec(pod, "python3", "-c", script))
        files = {name: base64.b64decode(value) for name, value in encoded.items()}
        self.captures.extend(files.values())
        messages = []
        usage = []
        for name, content in files.items():
            if name.endswith("usage.jsonl"):
                usage += [json.loads(line) for line in content.splitlines() if line]
            elif name.endswith(".jsonl"):
                messages += [json.loads(line) for line in content.splitlines() if line]
        return {
            "files": files,
            "messages": messages,
            "usage": usage,
            "assistant_messages": [item for item in messages if item.get("type") == "assistant"],
            "provider_attempts": sum(
                len(value.splitlines())
                for name, value in files.items()
                if name.endswith("provider-attempts")
            ),
            "traces": sum(
                len(value) for name, value in files.items() if name.endswith("traces.bin")
            ),
        }

    def live(self) -> None:
        __tracebackhide__ = True
        from test_personal_claude_live import _same_model

        require(self.ledger is not None, "live session has no durable budget ledger")
        remaining = self.ledger.remaining()
        require(remaining >= 4, "remaining live budget cannot admit a bounded four-turn session")
        pod, manifest = self.claim(
            "personal-live", grant=self.grant.reveal(), output_limit=remaining
        )
        self.observe_claim("personal-live", manifest, personal=True, grant=self.grant.reveal())
        with self.forward(pod) as port:
            sequence, allowance = self.ledger.start(self.candidate)
            require(allowance == remaining, "live allowance changed before session admission")
            events, raw, elapsed = self.turn(
                port,
                (
                    f"Use Read to read {BUNDLE}/public.txt, use Bash to run "
                    f"printf '{BASH}\\n', and call the custody public_file MCP tool. "
                    "Execute all three tools, preferably together, then answer exactly "
                    "PUBLIC-CONTROL-personal-custody. "
                    "Do not use agents, other tools or any additional model."
                ),
            )
        self.captures.append(raw)
        observed = self.observe_files(pod)
        assistants = observed["assistant_messages"]
        unique = {item.get("message", {}).get("id") for item in assistants}
        require(
            None not in unique and bool(unique),
            "native transcript did not identify assistant turns",
        )
        models = {item.get("message", {}).get("model") for item in assistants}
        require(
            all(isinstance(model, str) and _same_model(model, MODEL) for model in models),
            "live transcript used an unexpected model",
        )
        finals = [event for event in events if event.get("type") == "final"]
        require(
            len(finals) == 1 and isinstance(finals[0].get("output_tokens"), int),
            "live turn did not report actual output tokens",
        )
        reported = [item for body in observed["usage"] for item in body.get("models", [])]
        require(bool(reported), "live runner emitted no independent model usage")
        reported_models = {item.get("model") for item in reported}
        require(
            all(isinstance(model, str) and _same_model(model, MODEL) for model in reported_models),
            "usage report used an unexpected model",
        )
        self.evidence["live_models"] = sorted(models | reported_models)
        tokens = max(
            finals[0]["output_tokens"], sum(item.get("output_tokens", 0) for item in reported)
        )
        require(tokens > 0, "live session reported no model output tokens")
        usage = self.ledger.finish(sequence, tokens, len(unique), elapsed)
        self.evidence["live_usage"] = usage
        require(
            finals[0].get("status") == "done", "authenticated live turn did not finish successfully"
        )
        require(
            not [event for event in events if event.get("type") == "error"],
            "live turn emitted a classified error",
        )
        blocks = [
            block
            for item in observed["messages"]
            for block in item.get("message", {}).get("content", [])
            if isinstance(block, dict)
        ]
        calls = {block["id"]: block["name"] for block in blocks if block.get("type") == "tool_use"}
        results = {
            block.get("tool_use_id"): block
            for block in blocks
            if block.get("type") == "tool_result"
        }
        for tool, marker in (("Read", PUBLIC), ("Bash", BASH), ("public_file", PUBLIC)):
            matching = [
                key for key, name in calls.items() if name == tool or name.endswith("__" + tool)
            ]
            require(
                any(
                    key in results
                    and not results[key].get("is_error")
                    and marker in json.dumps(results[key].get("content"))
                    for key in matching
                ),
                "a required live tool did not return its independent public control",
            )
        for name, expected in (
            ("hook.marker", HOOK),
            ("helper.marker", HELPER),
            ("mcp.marker", PUBLIC),
        ):
            require(
                observed["files"].get(name) == expected.encode(),
                "live hook, helper or connector control failed",
            )
        require(observed["traces"] > 0, "live runner exported no traces for the private audit")
        require(bool(observed["usage"]), "live runner emitted no independent usage report")
        self.evidence["live_proof_run"] = True
        self.evidence["live_controls"] = {
            "read": True,
            "bash": True,
            "hook": True,
            "helper": True,
            "declared_stdio": True,
            "connector": True,
        }

    def audit_bytes(self, data: bytes, *, values: Sequence[str] = ()) -> None:
        __tracebackhide__ = True
        for value in (self.grant.reveal(), *values):
            if not value:
                continue
            raw = value.encode()
            variants = {raw, base64.b64encode(raw), base64.urlsafe_b64encode(raw)}
            variants.update(item.rstrip(b"=") for item in tuple(variants))
            require(
                not any(item and item in data for item in variants),
                "credential material escaped trusted storage",
            )

    def audit(self) -> None:
        __tracebackhide__ = True
        for plural in ("sandboxclaims", "sandboxtemplates", "sandboxwarmpools"):
            objects = self.custom.list_namespaced_custom_object(
                GROUP, VERSION, self.namespace, plural, _request_timeout=30
            )
            self.captures.append(json.dumps(objects).encode())
        pods = self.core.list_namespaced_pod(self.namespace, _request_timeout=30)
        self.captures.append(json.dumps(self.api_client.sanitize_for_serialization(pods)).encode())
        self.captures.append(
            json.dumps(
                self.api_client.sanitize_for_serialization(
                    self.core.list_namespaced_event(self.namespace, _request_timeout=30)
                )
            ).encode()
        )
        for pod in pods.items:
            for container in [*pod.spec.containers, *(pod.spec.init_containers or [])]:
                self.captures.append(
                    self.core.read_namespaced_pod_log(
                        pod.metadata.name,
                        self.namespace,
                        container=container.name,
                        _request_timeout=30,
                    ).encode()
                )
        for capture in self.captures:
            self.audit_bytes(capture, values=(SYNTHETIC_GRANT,))
        allowed_grant_copies = 0
        for secret in self.core.list_namespaced_secret(self.namespace, _request_timeout=30).items:
            for key, encoded in (secret.data or {}).items():
                value = base64.b64decode(encoded)
                if self.grant.reveal() and value == self.grant.reveal().encode():
                    require(
                        self.phase == "live"
                        and secret.metadata.name == "personal-live-tokens"
                        and key == "CLAUDE_CODE_OAUTH_TOKEN",
                        "grant appeared in an unexpected storage Secret",
                    )
                    allowed_grant_copies += 1
                else:
                    self.audit_bytes(value)
        require(
            allowed_grant_copies == (1 if self.phase == "live" else 0),
            "unexpected trusted grant copy count",
        )
        self.evidence["output_audit"] = {
            "passed": True,
            "captures": len(self.captures),
            "allowed_live_storage_copies": allowed_grant_copies,
        }

    def run(self) -> None:
        self.evidence["stage"] = "synthetic_corpus"
        self.synthetic()
        self.evidence["stage"] = "personal_claim"
        _pod_name, personal_pod = self.claim("personal-synthetic", grant=SYNTHETIC_GRANT)
        self.observe_claim("personal-synthetic", personal_pod, personal=True, grant=SYNTHETIC_GRANT)
        self.evidence["stage"] = "service_claim"
        _pod_name, service_pod = self.claim("service-control", grant=None)
        self.observe_claim("service-control", service_pod, personal=False, grant=SYNTHETIC_GRANT)
        self.evidence["stage"] = "missing_grant"
        self.refusal()
        if self.phase == "live":
            self.evidence["stage"] = "live_turn"
            self.live()
        self.evidence["stage"] = "output_audit"
        self.audit()
        self.evidence["stage"] = "completed"

    def cleanup(self) -> None:
        __tracebackhide__ = True
        from kubernetes import client, watch

        problems = []
        for process in self.processes:
            try:
                self.stop_process(process)
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("port-forward cleanup failed")
        for reader in self.readers:
            reader.join(timeout=10)
            if reader.is_alive():
                problems.append("port-forward reader remains")
        if self.namespace_requested and not self.namespace_uid:
            try:
                current = self.core.read_namespace(self.namespace, _request_timeout=30)
                require(
                    (current.metadata.labels or {}).get("curie-proof-owner") == self.ownership,
                    "namespace creation did not establish ownership",
                )
                self.namespace_uid = current.metadata.uid
            except client.ApiException as exc:
                if exc.status != 404:
                    problems.append("namespace ownership could not be recovered")
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("namespace ownership could not be recovered")
        if self.namespace_uid:
            try:
                self.core.delete_namespace(
                    self.namespace,
                    body={
                        "preconditions": {"uid": self.namespace_uid},
                        "propagationPolicy": "Foreground",
                    },
                    _request_timeout=30,
                )
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    try:
                        current = self.core.read_namespace(self.namespace, _request_timeout=30)
                    except client.ApiException as exc:
                        if exc.status == 404:
                            break
                        raise
                    require(
                        current.metadata.uid == self.namespace_uid,
                        "owned namespace identity changed",
                    )
                    watcher = watch.Watch()
                    try:
                        for event in watcher.stream(
                            self.core.list_namespace,
                            field_selector=f"metadata.name={self.namespace}",
                            resource_version=current.metadata.resource_version,
                            timeout_seconds=30,
                            _request_timeout=35,
                        ):
                            if event["type"] == "DELETED":
                                break
                    finally:
                        watcher.stop()
                else:
                    raise RuntimeError("owned namespace still exists")
                require(
                    self.kubectl(
                        "get",
                        "secret,pod,sandboxclaim,sandboxtemplate,sandboxwarmpool",
                        "--ignore-not-found",
                        "-o",
                        "name",
                    ).stdout.strip()
                    == b"",
                    "owned resources remain after namespace deletion",
                )
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("namespace or grant-copy cleanup failed")
        if self.registry:
            try:
                owned = self.docker(
                    "container",
                    "inspect",
                    self.registry,
                    "--format",
                    '{{index .Config.Labels "curie-proof-owner"}}',
                    check=False,
                )
                if owned.returncode == 0:
                    require(
                        owned.stdout.decode().strip() == self.ownership,
                        "registry ownership changed",
                    )
                    self.docker("rm", "--force", "--volumes", self.registry)
                require(
                    self.docker("container", "inspect", self.registry, check=False).returncode != 0,
                    "owned registry remains",
                )
                for volume in self.registry_volumes:
                    require(
                        self.docker("volume", "inspect", volume, check=False).returncode != 0,
                        "owned registry volume remains",
                    )
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("owned registry cleanup failed")
        for tag in reversed(self.tags):
            try:
                exists = self.docker("image", "inspect", tag, check=False).returncode == 0
                if exists:
                    self.docker("image", "rm", tag)
                require(
                    self.docker("image", "inspect", tag, check=False).returncode != 0,
                    "owned candidate tag remains",
                )
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("owned candidate image cleanup failed")
        if self.private_config:
            try:
                Path(self.private_config).unlink(missing_ok=True)
                require(not Path(self.private_config).exists(), "private kubeconfig remains")
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("private kubeconfig cleanup failed")
        if self.api_client is not None:
            self.api_client.close()
        if self.ledger is not None:
            self.ledger.db.close()
        if self.command_log is not None:
            self.command_log.close()
            self.evidence["private_command_transcript_retained"] = True
        self.grant.clear()
        self.runner_auth.clear()
        self.progress_auth.clear()
        self.evidence["cleanup"] = {
            "passed": not problems,
            "namespace_absent": not problems,
            "registry_absent": not problems,
            "owned_tags_absent": not problems,
            "owned_registry_volumes_absent": not problems,
            "port_forwards_running": sum(p.poll() is None for p in self.processes),
        }
        require(not problems, "; ".join(problems))


def validate_inputs(grant: HeldSecret, phase: str) -> None:
    __tracebackhide__ = True
    required = [
        "DOCKER_CONTEXT",
        "KUBECONFIG",
        "THROWAWAY_CONTEXT",
        "CURIE_PERSONAL_TEST_NAMESPACE",
        "CURIE_BIN",
    ]
    if phase == "live":
        required += ["SUBSCRIPTION_TEST_GRANT_FILE", "CURIE_PERSONAL_PROOF_RUN_ID"]
    missing = [name for name in required if not os.environ.get(name)]
    require(not missing, "missing required proof inputs: " + ", ".join(missing))
    require(phase in {"synthetic", "live"}, "proof phase must be synthetic or live")
    if phase == "live":
        require(
            bool(grant.reveal()) and not any(char.isspace() for char in grant.reveal()),
            "a privately loaded personal grant is required",
        )
        try:
            info = Path(os.environ["SUBSCRIPTION_TEST_GRANT_FILE"]).stat()
        except OSError:
            raise RuntimeError("private grant input is unavailable") from None
        require(
            stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size > 0,
            "private grant input must be nonempty and mode 0600",
        )
        require(
            os.access(os.environ["SUBSCRIPTION_TEST_GRANT_FILE"], os.R_OK),
            "private grant input is unreadable",
        )
    else:
        require(not grant.reveal(), "synthetic proof must not receive a real personal grant")
