#!/usr/bin/env python3
"""Owned AWS Secrets Manager acceptance harness.

The harness keeps provider values out of process arguments and user visible
output. Every external command writes stdout and stderr to private files. The
SQLite ledger is the sole authority for cleanup targets.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import secrets
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Sequence
from typing import Any

OWNED_PREFIX = "curie-aws-secrets-e2e-"
PURPOSE = "curie-aws-secrets-e2e"
REGION = "us-east-1"
PROFILE = "theconnman"
ESO_VERSION = "2.11.0"
ESO_CHART = "oci://ghcr.io/external-secrets/charts/external-secrets"
MOTO_IMAGE = (
    "ghcr.io/getmoto/motoserver:5.1.12@"
    "sha256:e1cf8b624019e6eba25cb5b37efdf95a463fc24691978540a1c7008b7d02fda0"
)
NAMESPACE = "curie-aws-secrets-e2e"
ESO_NAMESPACE = "external-secrets"
ESO_SERVICE_ACCOUNT = "acme-harness-eso"
ROTATION_SERVICE_ACCOUNT = "acme-harness-rotation"
TARGET_SECRET = "acme-harness-acme-fixture-connector-secrets"
DENIED_SECRET = "acme-harness-denied"
CONNECTOR_DEPLOYMENT = "acme-harness-acme-fixture-mcp-digest"
ROTATION_POD = "acme-harness-rotation"
STATIC_KEY = "STATIC_KEY"
ROTATED_KEY = "ROTATED_KEY"
INTERRUPTED_EXIT = 75
SIGNALS = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")


class HarnessError(RuntimeError):
    """A failure whose message is safe to show."""


class HarnessInterrupted(BaseException):
    """Raised on the first termination signal so cleanup always runs."""

    def __init__(self, signum: int):
        super().__init__(f"provider harness interrupted by signal {signum}")
        self.signum = signum


@dataclasses.dataclass
class SignalGuard:
    previous: dict[signal.Signals, Any]
    received_signals: list[int] = dataclasses.field(default_factory=list)

    def record_only(self, signum: int, _frame: Any) -> None:
        self.received_signals.append(signum)

    def first(self, signum: int, _frame: Any) -> None:
        for watched in SIGNALS:
            signal.signal(watched, self.record_only)
        self.received_signals.append(signum)
        raise HarnessInterrupted(signum)

    def restore(self) -> None:
        for watched, handler in self.previous.items():
            signal.signal(watched, handler)


def install_signal_handlers() -> SignalGuard:
    previous = {watched: signal.getsignal(watched) for watched in SIGNALS}
    guard = SignalGuard(previous)
    for watched in SIGNALS:
        signal.signal(watched, guard.first)
    return guard


def load_seed(path: pathlib.Path | str) -> dict[str, str]:
    seed_path = pathlib.Path(path)
    try:
        parsed = json.loads(seed_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"seed file is not valid JSON: {seed_path}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("seed must be a JSON object")
    if set(parsed) != {STATIC_KEY, ROTATED_KEY}:
        raise ValueError(f"seed must contain exactly {STATIC_KEY} and {ROTATED_KEY}")
    if any(not isinstance(value, str) or not value for value in parsed.values()):
        raise ValueError("seed values must be nonempty strings")
    return {STATIC_KEY: parsed[STATIC_KEY], ROTATED_KEY: parsed[ROTATED_KEY]}


def require_owned_name(name: str) -> str:
    if not name.startswith(OWNED_PREFIX) or len(name) > 253 or SAFE_NAME.fullmatch(name) is None:
        raise ValueError(f"resource name is outside the harness prefix: {name!r}")
    return name


def write_private_file(path: pathlib.Path | str, value: str | bytes) -> pathlib.Path:
    target = pathlib.Path(path)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    payload = value.encode() if isinstance(value, str) else value
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)
    return target


def require_private_file(path: pathlib.Path | str) -> pathlib.Path:
    target = pathlib.Path(path)
    try:
        mode = target.stat().st_mode & 0o777
    except OSError as exc:
        raise PermissionError(f"private file is unavailable: {target}") from exc
    if not target.is_file() or mode != 0o600:
        raise PermissionError(f"private file must have mode 0600: {target}")
    return target


def digest_prefix(value: str | bytes) -> str:
    payload = value.encode() if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()[:12]


def full_digest(value: str | bytes) -> str:
    payload = value.encode() if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def format_tool_error(action: str, status: int, _private_detail: str | bytes = b"") -> str:
    return f"{action} failed with exit status {status}; provider output was withheld"


def tool_error_has_code(result: ToolResult, *codes: str) -> bool:
    if result.status == 0:
        return False
    private_error = result.stderr_path.read_text(encoding="utf-8", errors="replace")
    return any(f"({code})" in private_error or code in private_error for code in codes)


@dataclasses.dataclass(frozen=True)
class CleanupTarget:
    id: int
    kind: str
    identity: str
    state: str


class ResourceLedger:
    """Durable exact resource ownership record."""

    def __init__(self, path: pathlib.Path | str):
        self.path = pathlib.Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                identity TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('intent', 'created')),
                UNIQUE(kind, identity)
            );
            CREATE TABLE IF NOT EXISTS completion (
                orchestrator_status TEXT NOT NULL,
                cleanup_status TEXT NOT NULL
            );
            """
        )
        self.connection.commit()
        os.chmod(self.path, 0o600)

    def __enter__(self) -> ResourceLedger:
        return self

    def __exit__(self, _kind: Any, _value: Any, _traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()

    def record_intent(self, kind: str, identity: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO resources(kind, identity, state) VALUES (?, ?, 'intent')",
            (kind, identity),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def mark_created(self, resource_id: int, identity: str) -> None:
        cursor = self.connection.execute(
            "UPDATE resources SET identity = ?, state = 'created' WHERE id = ?",
            (identity, resource_id),
        )
        if cursor.rowcount != 1:
            raise HarnessError("ledger resource intent is missing")
        self.connection.commit()

    def cleanup_targets(self) -> list[CleanupTarget]:
        rows = self.connection.execute(
            "SELECT id, kind, identity, state FROM resources ORDER BY id DESC"
        ).fetchall()
        return [CleanupTarget(int(row[0]), row[1], row[2], row[3]) for row in rows]

    def record_completion(self, orchestrator_status: str, cleanup_status: str) -> None:
        self.connection.execute("DELETE FROM completion")
        self.connection.execute(
            "INSERT INTO completion(orchestrator_status, cleanup_status) VALUES (?, ?)",
            (orchestrator_status, cleanup_status),
        )
        self.connection.commit()


@dataclasses.dataclass(frozen=True)
class ToolResult:
    status: int
    stdout: bytes
    stderr_path: pathlib.Path


class ToolRunner:
    def __init__(self, private_dir: pathlib.Path, evidence_commands: list[str]):
        self.private_dir = private_dir
        self.evidence_commands = evidence_commands
        self.counter = 0

    def _command_for_evidence(self, argv: Sequence[str]) -> str:
        rendered: list[str] = []
        private = str(self.private_dir)
        for value in argv:
            item = value.replace(private, "<private>")
            if item.startswith("file://"):
                item = "file://<private-file>"
            item = re.sub(r"(?<![0-9])[0-9]{12}(?![0-9])", "<aws-account>", item)
            rendered.append(shlex.quote(item))
        return " ".join(rendered)

    def run(
        self,
        argv: Sequence[str],
        action: str,
        *,
        input_data: bytes | None = None,
        env: dict[str, str] | None = None,
        timeout: int = 300,
        allow_failure: bool = False,
    ) -> ToolResult:
        self.counter += 1
        stdout_path = self.private_dir / f"tool-{self.counter:04d}.stdout"
        stderr_path = self.private_dir / f"tool-{self.counter:04d}.stderr"
        write_private_file(stdout_path, b"")
        write_private_file(stderr_path, b"")
        self.evidence_commands.append(self._command_for_evidence(argv))
        with stdout_path.open("wb") as stdout_stream, stderr_path.open("wb") as stderr_stream:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.PIPE if input_data is not None else subprocess.DEVNULL,
                stdout=stdout_stream,
                stderr=stderr_stream,
                env=env,
                start_new_session=True,
            )
            try:
                process.communicate(input=input_data, timeout=timeout)
            except subprocess.TimeoutExpired:
                self._stop_group(process)
                raise HarnessError(format_tool_error(action, 124)) from None
            except BaseException:
                self._stop_group(process)
                raise
        stdout = stdout_path.read_bytes()
        result = ToolResult(process.returncode, stdout, stderr_path)
        if process.returncode != 0 and not allow_failure:
            raise HarnessError(format_tool_error(action, process.returncode))
        return result

    @staticmethod
    def _stop_group(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)


def safe_print(message: str, *, error: bool = False) -> None:
    try:
        print(message, file=sys.stderr if error else sys.stdout, flush=True)
    except BrokenPipeError:
        pass


def aws_environment(
    credentials_path: pathlib.Path | None = None,
    config_path: pathlib.Path | None = None,
) -> dict[str, str]:
    environment = os.environ.copy()
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    ):
        environment.pop(key, None)
    environment["AWS_EC2_METADATA_DISABLED"] = "true"
    if credentials_path is not None:
        environment["AWS_SHARED_CREDENTIALS_FILE"] = str(credentials_path)
    if config_path is not None:
        environment["AWS_CONFIG_FILE"] = str(config_path)
    return environment


def parse_json(payload: bytes, action: str) -> Any:
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HarnessError(f"{action} returned invalid JSON") from exc


def require_tools(names: Iterable[str]) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise HarnessError(f"required tool is unavailable: {', '.join(sorted(missing))}")


def wait_until(action: str, predicate: Any, timeout: int = 180, interval: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise HarnessError(f"timed out while waiting for {action}")


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def yaml_document(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


class HarnessCase:
    def __init__(
        self,
        repo_root: pathlib.Path,
        snapshot: pathlib.Path,
        commit: str,
        seed: dict[str, str],
        mode: str,
        real_aws: bool,
        curie_bin: pathlib.Path,
    ):
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")
        suffix = f"{stamp}-{secrets.token_hex(3)}"
        self.repo_root = repo_root
        self.snapshot = snapshot
        self.commit = commit
        self.seed = seed
        self.mode = mode
        self.real_aws = real_aws
        self.curie_bin = curie_bin
        self.suffix = suffix
        self.cluster = require_owned_name(f"{OWNED_PREFIX}{suffix}")
        self.context = f"kind-{self.cluster}"
        self.primary_name = require_owned_name(f"{OWNED_PREFIX}{suffix}-primary")
        self.backup_name = require_owned_name(f"{OWNED_PREFIX}{suffix}-rotated")
        self.role_name = require_owned_name(f"{OWNED_PREFIX}{suffix}-eso")
        self.policy_name = require_owned_name(f"{OWNED_PREFIX}{suffix}-sm")
        self.bucket_name = require_owned_name(f"{OWNED_PREFIX}{suffix}-oidc")
        self.work = pathlib.Path(tempfile.mkdtemp(prefix="curie-provider-harness-"))
        os.chmod(self.work, 0o700)
        evidence_root = repo_root / ".projects/aws-secrets/evidence/aws-sec-harness" / suffix
        evidence_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(evidence_root, 0o700)
        self.evidence_root = evidence_root
        self.commands: list[str] = []
        self.runner = ToolRunner(self.work, self.commands)
        self.admin_kubeconfig = self.work / "admin.kubeconfig"
        self.background: list[subprocess.Popen[bytes]] = []
        self.image_records: dict[str, dict[str, str]] = {}
        self.assertions: list[dict[str, Any]] = []
        self.prior_clusters: set[str] = set()
        self.account_id = ""
        self.oidc_arn = ""
        self.role_arn = ""
        self.issuer = ""
        self.aws_env = aws_environment()
        self.aws_endpoint: str | None = None
        self.cleanup_log = evidence_root / "cleanup.log"
        self.cleanup_signals: list[int] = []
        write_private_file(self.cleanup_log, b"")
        self.ledger_path = evidence_root / "resources.sqlite"
        self.ledger: ResourceLedger | None = None

    def log(self, message: str) -> None:
        safe_print(f"provider harness: {message}")

    def record_assertion(self, name: str, passed: bool, detail: str = "") -> None:
        self.assertions.append({"name": name, "passed": passed, "detail": detail})
        if not passed:
            raise HarnessError(f"assertion failed: {name}")

    def kargs(self, *args: str) -> list[str]:
        return [
            "kubectl",
            "--kubeconfig",
            str(self.admin_kubeconfig),
            "--context",
            self.context,
            *args,
        ]

    def kubectl(
        self,
        *args: str,
        action: str,
        input_data: bytes | None = None,
        allow_failure: bool = False,
        timeout: int = 180,
    ) -> ToolResult:
        return self.runner.run(
            self.kargs(*args),
            action,
            input_data=input_data,
            timeout=timeout,
            allow_failure=allow_failure,
        )

    def aws(
        self,
        service: str,
        operation: str,
        *args: str,
        action: str,
        allow_failure: bool = False,
        timeout: int = 180,
    ) -> ToolResult:
        command = [
            "aws",
            service,
            operation,
            "--profile",
            PROFILE,
            "--region",
            REGION,
        ]
        if self.aws_endpoint is not None:
            command.extend(["--endpoint-url", self.aws_endpoint])
        command.extend(args)
        return self.runner.run(
            command,
            action,
            env=self.aws_env,
            timeout=timeout,
            allow_failure=allow_failure,
        )

    def apply(self, manifest: bytes, action: str, namespace: str | None = NAMESPACE) -> None:
        args: list[str] = []
        if namespace is not None:
            args.extend(["-n", namespace])
        args.extend(["apply", "-f", "-"])
        self.kubectl(*args, action=action, input_data=manifest)

    def preflight(self) -> None:
        clusters = (
            self.runner.run(["kind", "get", "clusters"], "inventory kind clusters")
            .stdout.decode("utf-8", "strict")
            .splitlines()
        )
        self.prior_clusters = {value.strip() for value in clusters if value.strip()}
        if self.cluster in self.prior_clusters:
            raise HarnessError(f"owned kind cluster already exists: {self.cluster}")
        if self.real_aws:
            identity = self.aws(
                "sts",
                "get-caller-identity",
                "--output",
                "json",
                action="identify AWS account",
            )
            account = parse_json(identity.stdout, "AWS identity")
            self.account_id = str(account.get("Account", ""))
            if not re.fullmatch(r"[0-9]{12}", self.account_id):
                raise HarnessError("AWS account identity is invalid")
            tagged = self.aws(
                "resourcegroupstaggingapi",
                "get-resources",
                "--tag-filters",
                f"Key=purpose,Values={PURPOSE}",
                "--output",
                "json",
                action="inventory owned AWS tags",
            )
            mappings = parse_json(tagged.stdout, "AWS tag inventory").get(
                "ResourceTagMappingList", []
            )
            if mappings:
                raise HarnessError("preexisting tagged AWS resources must be removed first")
            block = self.aws(
                "s3control",
                "get-public-access-block",
                "--account-id",
                self.account_id,
                "--output",
                "json",
                action="read account S3 public access block",
                allow_failure=True,
            )
            if block.status == 0:
                configuration = parse_json(block.stdout, "S3 public access block").get(
                    "PublicAccessBlockConfiguration", {}
                )
                if configuration.get("BlockPublicPolicy") or configuration.get(
                    "RestrictPublicBuckets"
                ):
                    raise HarnessError(
                        "account S3 public access settings block the owned OIDC issuer"
                    )
            elif not tool_error_has_code(block, "NoSuchPublicAccessBlockConfiguration"):
                raise HarnessError(
                    format_tool_error("read account S3 public access block", block.status)
                )

    def run(self) -> None:
        try:
            self._run()
        except BaseException:
            safe_print(f"provider-harness-private-diagnostics: {self.work}", error=True)
            raise
        else:
            shutil.rmtree(self.work)

    def _run(self) -> None:
        self.preflight()
        orchestrator_status = "failed"
        cleanup_status = "failed"
        pending: BaseException | None = None
        with ResourceLedger(self.ledger_path) as ledger:
            self.ledger = ledger
            safe_print(f"provider-harness-ledger: {self.ledger_path}")
            try:
                self._run_body()
                orchestrator_status = "complete"
            except HarnessInterrupted as exc:
                orchestrator_status = "interrupted"
                pending = exc
            except BaseException as exc:
                pending = exc
            finally:
                self.enter_cleanup_signal_mode()
                try:
                    self.cleanup()
                    cleanup_status = "complete"
                except BaseException as exc:
                    cleanup_status = "failed"
                    if pending is None:
                        pending = exc
                ledger.record_completion(orchestrator_status, cleanup_status)
                self.write_evidence(orchestrator_status, cleanup_status)
        self.ledger = None
        if pending is not None:
            raise pending

    def enter_cleanup_signal_mode(self) -> None:
        def record_only(signum: int, _frame: Any) -> None:
            self.cleanup_signals.append(signum)

        for watched in SIGNALS:
            signal.signal(watched, record_only)

    def _run_body(self) -> None:
        self.write_seed_files()
        self.build_images()
        self.create_cluster()
        self.load_and_verify_images()
        if self.mode == "none":
            self.prove_eso_absent()
            return
        self.create_namespace_and_rbac()
        if self.real_aws:
            self.create_real_aws_identity()
        else:
            self.start_moto()
        self.install_eso()
        self.create_provider_entry()
        self.apply_sync_objects()
        self.wait_for_static_key()
        self.bootstrap_rotated_key()
        self.prove_rotation_rbac_denial()
        self.deploy_connector()
        initial = self.call_connector_digest()
        expected_initial = full_digest(self.seed[ROTATED_KEY])
        self.record_assertion("connector observed initial rotated key", initial == expected_initial)
        rotated_value = f"synthetic-rotation-{secrets.token_hex(24)}"
        rotated_path = write_private_file(self.work / "rotated-next.value", rotated_value)
        self.rotate_key(rotated_path, full_digest(rotated_value))
        self.rollout_connector()
        observed = self.call_connector_digest()
        self.record_assertion(
            "connector observed rotated key", observed == full_digest(rotated_value)
        )
        self.verify_static_key()
        self.wait_for_backup(full_digest(rotated_value))

    def write_seed_files(self) -> None:
        for key, filename in ((STATIC_KEY, "static.value"), (ROTATED_KEY, "rotated.value")):
            write_private_file(self.work / filename, self.seed[key])
            require_private_file(self.work / filename)

    def build_images(self) -> None:
        assert self.ledger is not None
        short = self.commit[:12]
        definitions = {
            "api": ("apps/api/Dockerfile", self.snapshot),
            "worker": ("apps/worker/Dockerfile", self.snapshot),
            "dispatcher": ("apps/dispatcher/Dockerfile", self.snapshot),
            "runner": ("runner/Dockerfile", self.snapshot),
        }
        for name, (dockerfile, context) in definitions.items():
            tag = f"curie-aws-secrets-e2e-{name}:{short}-{self.suffix}"
            row = self.ledger.record_intent("docker_image", tag)
            self.runner.run(
                ["docker", "build", "-f", str(self.snapshot / dockerfile), "-t", tag, str(context)],
                f"build candidate {name} image",
                timeout=1800,
            )
            image_id = self.docker_image_id(tag)
            self.ledger.mark_created(row, tag)
            self.image_records[name] = {"tag": tag, "id": image_id}

        fixture = self.snapshot / "cli/tests/fixtures/provider-bundle"
        self.runner.run(
            [str(self.curie_bin), "build", "--plugin-dir", str(fixture), "--force"],
            "build fixture connector and lock",
            timeout=1800,
        )
        self.validate_fixture(fixture)
        lock_text = (fixture / "connectors.lock.yaml").read_text(encoding="utf-8")
        match = re.search(r"(?m)^\s*image:\s*([^\s]+)\s*$", lock_text)
        if match is None:
            raise HarnessError("connector build lock did not record an image")
        source_image = match.group(1)
        tag = f"curie-aws-secrets-e2e-connector:{short}-{self.suffix}"
        row = self.ledger.record_intent("docker_image", tag)
        self.runner.run(["docker", "tag", source_image, tag], "tag fixture connector image")
        image_id = self.docker_image_id(tag)
        self.ledger.mark_created(row, tag)
        self.image_records["connector"] = {"tag": tag, "id": image_id}

    def validate_fixture(self, fixture: pathlib.Path) -> None:
        validator = (
            "import sys; from plugin_format import validate_bundle; "
            "r=validate_bundle(sys.argv[1]); "
            "raise SystemExit(0 if r.valid else 1)"
        )
        self.runner.run(
            [
                "uv",
                "run",
                "--project",
                str(self.snapshot),
                "--package",
                "plugin-format",
                "python",
                "-c",
                validator,
                str(fixture),
            ],
            "validate fixture bundle",
            timeout=300,
        )
        declaration = (fixture / "connectors.yaml").read_text(encoding="utf-8")
        if "rotation:" in declaration or "provider:" in declaration:
            raise HarnessError("fixture connector declaration extends the frozen shape")
        for key in (STATIC_KEY, ROTATED_KEY, "bearer_secret: STATIC_KEY"):
            if key not in declaration:
                raise HarnessError("fixture connector declaration is incomplete")

    def docker_image_id(self, tag: str) -> str:
        result = self.runner.run(
            ["docker", "image", "inspect", tag, "--format", "{{.Id}}"],
            "inspect candidate image",
        )
        image_id = result.stdout.decode("utf-8", "strict").strip()
        if re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
            raise HarnessError("candidate image identity is invalid")
        return image_id

    def create_cluster(self) -> None:
        assert self.ledger is not None
        row = self.ledger.record_intent("kind_cluster", self.cluster)
        write_private_file(self.admin_kubeconfig, b"")
        command = [
            "kind",
            "create",
            "cluster",
            "--name",
            self.cluster,
            "--kubeconfig",
            str(self.admin_kubeconfig),
            "--wait",
            "180s",
        ]
        if self.real_aws:
            self.issuer = f"https://{self.bucket_name}.s3.{REGION}.amazonaws.com"
            config = write_private_file(
                self.work / "kind.yaml",
                (
                    "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n"
                    "- role: control-plane\n  kubeadmConfigPatches:\n  - |\n"
                    "    kind: ClusterConfiguration\n    apiServer:\n      extraArgs:\n"
                    f"        service-account-issuer: {self.issuer}\n"
                    f"        service-account-jwks-uri: {self.issuer}/openid/v1/jwks\n"
                ),
            )
            command.extend(["--config", str(config)])
        self.runner.run(command, "create owned kind cluster", timeout=300)
        require_private_file(self.admin_kubeconfig)
        self.ledger.mark_created(row, self.cluster)

    def load_and_verify_images(self) -> None:
        for record in self.image_records.values():
            self.runner.run(
                ["kind", "load", "docker-image", "--name", self.cluster, record["tag"]],
                "load candidate image into kind",
                timeout=300,
            )
            inventory = self.runner.run(
                [
                    "docker",
                    "exec",
                    f"{self.cluster}-control-plane",
                    "crictl",
                    "inspecti",
                    "--output",
                    "json",
                    record["tag"],
                ],
                "inspect loaded candidate image",
            )
            loaded = json.dumps(parse_json(inventory.stdout, "kind image inventory"))
            self.record_assertion(
                f"kind loaded exact image {record['tag']}", record["id"] in loaded
            )

    def prove_eso_absent(self) -> None:
        self.kubectl(
            "get",
            "--raw=/readyz",
            action="verify Kubernetes API health before ESO absence proof",
        )
        self.kubectl(
            "get",
            "namespaces",
            "-o",
            "name",
            action="inventory namespaces before ESO absence proof",
        )
        self.kubectl(
            "get",
            "crds",
            "-o",
            "name",
            action="inventory CRDs before ESO absence proof",
        )
        checks = [
            ("namespace", ESO_NAMESPACE, None),
            ("deployment", "external-secrets", ESO_NAMESPACE),
            ("crd", "externalsecrets.external-secrets.io", None),
            ("crd", "secretstores.external-secrets.io", None),
            ("crd", "pushsecrets.external-secrets.io", None),
            ("secretstore", "acme-harness", NAMESPACE),
            ("externalsecret", "acme-harness-static", NAMESPACE),
            ("pushsecret", "acme-harness-rotated", NAMESPACE),
        ]
        for kind, name, namespace in checks:
            args: list[str] = []
            if namespace:
                args.extend(["-n", namespace])
            args.extend(["get", kind, name])
            result = self.kubectl(
                *args,
                action=f"prove {kind} absent",
                allow_failure=True,
            )
            absent = result.status != 0 and tool_error_has_code(
                result,
                "NotFound",
                "the server doesn't have a resource type",
            )
            self.record_assertion(f"{kind}/{name} is absent", absent)

    def create_namespace_and_rbac(self) -> None:
        assert self.ledger is not None
        row = self.ledger.record_intent("kubernetes_namespace", NAMESPACE)
        self.apply(
            yaml_document(
                {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}}
            ),
            "create harness namespace",
            namespace=None,
        )
        self.ledger.mark_created(row, NAMESPACE)
        rbac_path = (
            self.snapshot / "cli/tests/fixtures/provider-bundle/manifests/rotation-rbac.yaml"
        )
        self.apply(rbac_path.read_bytes(), "apply scoped rotation RBAC")

    def start_moto(self) -> None:
        credentials = write_private_file(
            self.work / "aws-credentials",
            "[theconnman]\naws_access_key_id = test\naws_secret_access_key = test\n",
        )
        config = write_private_file(
            self.work / "aws-config", "[profile theconnman]\nregion = us-east-1\n"
        )
        self.aws_env = aws_environment(credentials, config)
        manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "motosm"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "motosm"}},
                "template": {
                    "metadata": {"labels": {"app": "motosm"}},
                    "spec": {
                        "containers": [
                            {
                                "name": "motosm",
                                "image": MOTO_IMAGE,
                                "args": ["-H", "0.0.0.0", "-p", "5000"],
                                "ports": [{"containerPort": 5000}],
                            }
                        ]
                    },
                },
            },
        }
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "motosm"},
            "spec": {"selector": {"app": "motosm"}, "ports": [{"port": 5000}]},
        }
        self.apply(yaml_document(manifest) + b"\n---\n" + yaml_document(service), "start moto")
        self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            "deployment/motosm",
            "--timeout=180s",
            action="wait for moto",
        )
        port = free_loopback_port()
        self.start_port_forward("service/motosm", 5000, port, "moto")
        self.aws_endpoint = f"http://127.0.0.1:{port}"
        wait_until("moto endpoint", lambda: self.tcp_ready(port), timeout=60, interval=0.5)

    def start_port_forward(self, resource: str, remote: int, local: int, label: str) -> None:
        stdout_path = write_private_file(self.work / f"{label}-forward.stdout", b"")
        stderr_path = write_private_file(self.work / f"{label}-forward.stderr", b"")
        stdout = stdout_path.open("wb")
        stderr = stderr_path.open("wb")
        process = subprocess.Popen(
            self.kargs(
                "-n",
                NAMESPACE,
                "port-forward",
                "--address",
                "127.0.0.1",
                resource,
                f"{local}:{remote}",
            ),
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        stdout.close()
        stderr.close()
        self.background.append(process)

    @staticmethod
    def tcp_ready(port: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            return False

    def install_eso(self) -> None:
        assert self.ledger is not None
        row = self.ledger.record_intent("helm_release", f"{ESO_NAMESPACE}/external-secrets")
        self.runner.run(
            [
                "helm",
                "--kubeconfig",
                str(self.admin_kubeconfig),
                "--kube-context",
                self.context,
                "upgrade",
                "--install",
                "external-secrets",
                ESO_CHART,
                "--version",
                ESO_VERSION,
                "--namespace",
                ESO_NAMESPACE,
                "--create-namespace",
                "--set",
                "installCRDs=true",
                "--wait",
                "--timeout",
                "5m",
            ],
            "install pinned External Secrets",
            timeout=420,
        )
        self.ledger.mark_created(row, f"{ESO_NAMESPACE}/external-secrets")
        if not self.real_aws:
            endpoint = f"http://motosm.{NAMESPACE}.svc.cluster.local:5000"
            self.kubectl(
                "-n",
                ESO_NAMESPACE,
                "set",
                "env",
                "deployment/external-secrets",
                f"AWS_SECRETSMANAGER_ENDPOINT={endpoint}",
                action="configure moto endpoint for External Secrets",
            )
            self.kubectl(
                "-n",
                ESO_NAMESPACE,
                "rollout",
                "status",
                "deployment/external-secrets",
                "--timeout=180s",
                action="wait for External Secrets endpoint rollout",
            )

    def create_provider_entry(self) -> None:
        assert self.ledger is not None
        primary_json = write_private_file(
            self.work / "primary.json", json.dumps({STATIC_KEY: self.seed[STATIC_KEY]})
        )
        row = self.ledger.record_intent("secretsmanager", self.primary_name)
        arguments = [
            "--name",
            self.primary_name,
            "--secret-string",
            f"file://{primary_json}",
        ]
        if self.real_aws:
            arguments.extend(["--tags", f"Key=purpose,Value={PURPOSE}"])
        self.aws(
            "secretsmanager",
            "create-secret",
            *arguments,
            action="create provider entry",
        )
        self.ledger.mark_created(row, self.primary_name)
        self.ledger.record_intent("secretsmanager", self.backup_name)

    def create_real_aws_identity(self) -> None:
        assert self.ledger is not None
        bucket_row = self.ledger.record_intent("s3_bucket", self.bucket_name)
        self.aws(
            "s3api", "create-bucket", "--bucket", self.bucket_name, action="create OIDC bucket"
        )
        self.ledger.mark_created(bucket_row, self.bucket_name)
        self.aws(
            "s3api",
            "put-bucket-tagging",
            "--bucket",
            self.bucket_name,
            "--tagging",
            f"TagSet=[{{Key=purpose,Value={PURPOSE}}}]",
            action="tag OIDC bucket",
        )
        self.aws(
            "s3api",
            "put-public-access-block",
            "--bucket",
            self.bucket_name,
            "--public-access-block-configuration",
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=false,RestrictPublicBuckets=false",
            action="scope OIDC bucket public access",
        )
        bucket_policy = write_private_file(
            self.work / "bucket-policy.json",
            json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "OidcDiscoveryOnly",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": [
                                f"arn:aws:s3:::{self.bucket_name}/.well-known/openid-configuration",
                                f"arn:aws:s3:::{self.bucket_name}/openid/v1/jwks",
                            ],
                        }
                    ],
                }
            ),
        )
        self.aws(
            "s3api",
            "put-bucket-policy",
            "--bucket",
            self.bucket_name,
            "--policy",
            f"file://{bucket_policy}",
            action="publish scoped OIDC bucket policy",
        )
        discovery = self.kubectl(
            "get",
            "--raw",
            "/.well-known/openid-configuration",
            action="read cluster OIDC discovery",
        ).stdout
        jwks = self.kubectl(
            "get", "--raw", "/openid/v1/jwks", action="read cluster OIDC keys"
        ).stdout
        discovery_path = write_private_file(self.work / "openid-configuration", discovery)
        jwks_path = write_private_file(self.work / "jwks", jwks)
        for key, path in (
            (".well-known/openid-configuration", discovery_path),
            ("openid/v1/jwks", jwks_path),
        ):
            self.aws(
                "s3api",
                "put-object",
                "--bucket",
                self.bucket_name,
                "--key",
                key,
                "--body",
                str(path),
                "--content-type",
                "application/json",
                action="publish OIDC document",
            )
        self.prove_public_issuer()
        issuer_host = self.issuer.removeprefix("https://")
        expected_oidc_arn = f"arn:aws:iam::{self.account_id}:oidc-provider/{issuer_host}"
        oidc_row = self.ledger.record_intent("iam_oidc_provider", expected_oidc_arn)
        result = self.aws(
            "iam",
            "create-open-id-connect-provider",
            "--url",
            self.issuer,
            "--client-id-list",
            "sts.amazonaws.com",
            "--tags",
            f"Key=purpose,Value={PURPOSE}",
            "--output",
            "json",
            action="create IAM OIDC provider",
        )
        self.oidc_arn = str(
            parse_json(result.stdout, "IAM OIDC provider").get("OpenIDConnectProviderArn", "")
        )
        if self.oidc_arn != expected_oidc_arn:
            raise HarnessError("IAM OIDC provider identity is invalid")
        self.ledger.mark_created(oidc_row, self.oidc_arn)
        trust = write_private_file(
            self.work / "trust.json",
            json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Federated": self.oidc_arn},
                            "Action": "sts:AssumeRoleWithWebIdentity",
                            "Condition": {
                                "StringEquals": {
                                    f"{issuer_host}:sub": (
                                        f"system:serviceaccount:{NAMESPACE}:{ESO_SERVICE_ACCOUNT}"
                                    ),
                                    f"{issuer_host}:aud": "sts.amazonaws.com",
                                }
                            },
                        }
                    ],
                }
            ),
        )
        role_row = self.ledger.record_intent("iam_role", self.role_name)
        role = self.aws(
            "iam",
            "create-role",
            "--role-name",
            self.role_name,
            "--assume-role-policy-document",
            f"file://{trust}",
            "--tags",
            f"Key=purpose,Value={PURPOSE}",
            "--max-session-duration",
            "3600",
            "--output",
            "json",
            action="create ESO IAM role",
        )
        self.role_arn = str(parse_json(role.stdout, "ESO IAM role").get("Role", {}).get("Arn", ""))
        if not self.role_arn.startswith("arn:aws:iam::"):
            raise HarnessError("ESO IAM role identity is invalid")
        self.ledger.mark_created(role_row, self.role_name)
        policy = write_private_file(
            self.work / "permissions.json",
            json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "secretsmanager:DescribeSecret",
                                "secretsmanager:GetSecretValue",
                                "secretsmanager:CreateSecret",
                                "secretsmanager:TagResource",
                                "secretsmanager:PutSecretValue",
                            ],
                            "Resource": (
                                "arn:aws:secretsmanager:"
                                f"{REGION}:{self.account_id}:secret:{OWNED_PREFIX}*"
                            ),
                        }
                    ],
                }
            ),
        )
        policy_row = self.ledger.record_intent(
            "iam_role_policy", f"{self.role_name}/{self.policy_name}"
        )
        self.aws(
            "iam",
            "put-role-policy",
            "--role-name",
            self.role_name,
            "--policy-name",
            self.policy_name,
            "--policy-document",
            f"file://{policy}",
            action="attach scoped Secrets Manager policy",
        )
        self.ledger.mark_created(policy_row, f"{self.role_name}/{self.policy_name}")
        self.kubectl(
            "-n",
            NAMESPACE,
            "annotate",
            "serviceaccount",
            ESO_SERVICE_ACCOUNT,
            f"eks.amazonaws.com/role-arn={self.role_arn}",
            "--overwrite",
            action="bind ESO web identity role",
        )
        self.prove_web_identity()

    def prove_public_issuer(self) -> None:
        allowed = (
            f"{self.issuer}/.well-known/openid-configuration",
            f"{self.issuer}/openid/v1/jwks",
        )
        for url in allowed:
            with urllib.request.urlopen(url, timeout=30) as response:
                self.record_assertion("OIDC document is public", response.status == 200)
        try:
            urllib.request.urlopen(f"{self.issuer}/", timeout=30)
        except urllib.error.HTTPError as exc:
            self.record_assertion("OIDC bucket root is private", exc.code in (403, 404))
        else:
            self.record_assertion("OIDC bucket root is private", False)

    def prove_web_identity(self) -> None:
        good = self.service_account_token(ESO_SERVICE_ACCOUNT, "sts.amazonaws.com", "good.token")
        rogue = self.service_account_token(
            ROTATION_SERVICE_ACCOUNT, "sts.amazonaws.com", "rogue.token"
        )
        wrong = self.service_account_token(ESO_SERVICE_ACCOUNT, "curie.invalid", "wrong.token")

        def assume(path: pathlib.Path, label: str) -> ToolResult:
            return self.aws(
                "sts",
                "assume-role-with-web-identity",
                "--no-sign-request",
                "--role-arn",
                self.role_arn,
                "--role-session-name",
                label,
                "--web-identity-token",
                f"file://{path}",
                "--output",
                "json",
                action=f"prove {label} web identity",
                allow_failure=True,
            )

        good_result: ToolResult | None = None
        for _ in range(6):
            good_result = assume(good, "eso-subject")
            if good_result.status == 0:
                break
            time.sleep(10)
        self.record_assertion(
            "ESO subject assumes role", good_result is not None and good_result.status == 0
        )
        rogue_result = assume(rogue, "rotation-subject")
        wrong_result = assume(wrong, "wrong-audience")
        self.record_assertion(
            "rotation subject is denied",
            tool_error_has_code(rogue_result, "AccessDenied"),
        )
        self.record_assertion(
            "wrong audience is denied",
            tool_error_has_code(wrong_result, "AccessDenied", "InvalidIdentityToken"),
        )

    def service_account_token(self, account: str, audience: str, filename: str) -> pathlib.Path:
        result = self.kubectl(
            "-n",
            NAMESPACE,
            "create",
            "token",
            account,
            "--audience",
            audience,
            "--duration",
            "10m",
            action="mint web identity token",
        )
        path = write_private_file(self.work / filename, result.stdout.strip())
        require_private_file(path)
        return path

    def apply_sync_objects(self) -> None:
        # Provider shape follows the official AWS and PushSecret references:
        # https://external-secrets.io/latest/provider/aws-secrets-manager/
        # https://external-secrets.io/latest/guides/pushsecrets/
        if self.real_aws:
            provider = {
                "aws": {
                    "service": "SecretsManager",
                    "region": REGION,
                    "auth": {"jwt": {"serviceAccountRef": {"name": ESO_SERVICE_ACCOUNT}}},
                }
            }
        else:
            dummy = {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "acme-harness-aws"},
                "stringData": {"access-key": "test", "secret-key": "test"},
            }
            self.apply(yaml_document(dummy), "apply isolated moto credentials")
            provider = {
                "aws": {
                    "service": "SecretsManager",
                    "region": REGION,
                    "auth": {
                        "secretRef": {
                            "accessKeyIDSecretRef": {
                                "name": "acme-harness-aws",
                                "key": "access-key",
                            },
                            "secretAccessKeySecretRef": {
                                "name": "acme-harness-aws",
                                "key": "secret-key",
                            },
                        }
                    },
                }
            }
        store = {
            "apiVersion": "external-secrets.io/v1",
            "kind": "SecretStore",
            "metadata": {"name": "acme-harness"},
            "spec": {"provider": provider},
        }
        external = {
            "apiVersion": "external-secrets.io/v1",
            "kind": "ExternalSecret",
            "metadata": {"name": "acme-harness-static"},
            "spec": {
                "refreshInterval": "10s",
                "secretStoreRef": {"name": "acme-harness", "kind": "SecretStore"},
                "target": {"name": TARGET_SECRET, "creationPolicy": "CreateOrMerge"},
                "data": [
                    {
                        "secretKey": STATIC_KEY,
                        "remoteRef": {"key": self.primary_name, "property": STATIC_KEY},
                    }
                ],
            },
        }
        push = {
            "apiVersion": "external-secrets.io/v1alpha1",
            "kind": "PushSecret",
            "metadata": {"name": "acme-harness-rotated"},
            "spec": {
                "refreshInterval": "10s",
                "updatePolicy": "Replace",
                "deletionPolicy": "None",
                "secretStoreRefs": [{"name": "acme-harness", "kind": "SecretStore"}],
                "selector": {"secret": {"name": TARGET_SECRET}},
                "data": [
                    {
                        "match": {
                            "secretKey": ROTATED_KEY,
                            "remoteRef": {
                                "remoteKey": self.backup_name,
                                "property": ROTATED_KEY,
                            },
                        },
                        "metadata": {"secretPushFormat": "string"},
                    }
                ],
            },
        }
        self.apply(
            yaml_document(store)
            + b"\n---\n"
            + yaml_document(external)
            + b"\n---\n"
            + yaml_document(push),
            "apply External Secrets sync objects",
        )

    def secret_document(self, name: str) -> dict[str, Any] | None:
        result = self.kubectl(
            "-n",
            NAMESPACE,
            "get",
            "secret",
            name,
            "-o",
            "json",
            action="read owned Secret",
            allow_failure=True,
        )
        if result.status != 0:
            return None
        parsed = parse_json(result.stdout, "owned Secret")
        return parsed if isinstance(parsed, dict) else None

    def decoded_secret_key(self, name: str, key: str) -> bytes | None:
        document = self.secret_document(name)
        if document is None:
            return None
        encoded = document.get("data", {}).get(key)
        if not isinstance(encoded, str):
            return None
        try:
            return base64.b64decode(encoded, validate=True)
        except ValueError:
            return None

    def wait_for_static_key(self) -> None:
        expected = self.seed[STATIC_KEY].encode()
        wait_until(
            "ExternalSecret static sync",
            lambda: self.decoded_secret_key(TARGET_SECRET, STATIC_KEY) == expected,
            timeout=180,
        )
        self.record_assertion("ExternalSecret synced static key", True, digest_prefix(expected))

    def rotation_pod_manifest(self) -> bytes:
        image = self.image_records["connector"]["tag"]
        return yaml_document(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": ROTATION_POD},
                "spec": {
                    "serviceAccountName": ROTATION_SERVICE_ACCOUNT,
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "rotation",
                            "image": image,
                            "imagePullPolicy": "Never",
                            "command": ["python", "-c", "import time; time.sleep(3600)"],
                        }
                    ],
                },
            }
        )

    def start_rotation_pod(self) -> None:
        self.kubectl(
            "-n",
            NAMESPACE,
            "delete",
            "pod",
            ROTATION_POD,
            "--ignore-not-found",
            action="clear rotation pod",
        )
        self.apply(self.rotation_pod_manifest(), "start rotation pod")
        self.kubectl(
            "-n",
            NAMESPACE,
            "wait",
            "--for=condition=Ready",
            f"pod/{ROTATION_POD}",
            "--timeout=120s",
            action="wait for rotation pod",
        )

    def run_rotator(
        self,
        value_path: pathlib.Path,
        expected_digest: str,
        *,
        bootstrap: bool = False,
        target: str | None = None,
        expect_success: bool = True,
    ) -> None:
        self.start_rotation_pod()
        arguments = [
            "-n",
            NAMESPACE,
            "exec",
            "-i",
            ROTATION_POD,
            "--",
            "python",
            "/app/rotate.py",
        ]
        if bootstrap:
            arguments.append("--bootstrap")
        if target is not None:
            arguments.extend(["--target", target])
        result = self.kubectl(
            *arguments,
            action="run scoped rotation",
            input_data=require_private_file(value_path).read_bytes(),
            allow_failure=not expect_success,
        )
        self.kubectl(
            "-n",
            NAMESPACE,
            "delete",
            "pod",
            ROTATION_POD,
            "--wait=true",
            action="delete rotation pod",
        )
        if expect_success:
            observed = result.stdout.decode("ascii", "strict").strip()
            self.record_assertion(
                "rotation digest matches local value", observed == expected_digest
            )
        else:
            denied = result.status != 0 and tool_error_has_code(
                result,
                "Kubernetes API refused Secret access with status 403",
            )
            self.record_assertion(
                "rotation ServiceAccount denied other Secret",
                denied,
            )

    def bootstrap_rotated_key(self) -> None:
        value = self.work / "rotated.value"
        self.run_rotator(value, full_digest(self.seed[ROTATED_KEY]), bootstrap=True)

    def prove_rotation_rbac_denial(self) -> None:
        denied = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": DENIED_SECRET},
            "stringData": {"marker": "synthetic"},
        }
        self.apply(yaml_document(denied), "create RBAC denial control")
        self.run_rotator(
            self.work / "rotated.value",
            full_digest(self.seed[ROTATED_KEY]),
            target=DENIED_SECRET,
            expect_success=False,
        )

    def rotate_key(self, value_path: pathlib.Path, expected_digest: str) -> None:
        self.run_rotator(value_path, expected_digest)

    def deploy_connector(self) -> None:
        image = self.image_records["connector"]["tag"]
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": CONNECTOR_DEPLOYMENT},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": CONNECTOR_DEPLOYMENT}},
                "template": {
                    "metadata": {"labels": {"app": CONNECTOR_DEPLOYMENT}},
                    "spec": {
                        "containers": [
                            {
                                "name": "digest",
                                "image": image,
                                "imagePullPolicy": "Never",
                                "ports": [{"containerPort": 8000}],
                                "env": [
                                    {
                                        "name": key,
                                        "valueFrom": {
                                            "secretKeyRef": {"name": TARGET_SECRET, "key": key}
                                        },
                                    }
                                    for key in (STATIC_KEY, ROTATED_KEY)
                                ],
                                "readinessProbe": {
                                    "tcpSocket": {"port": 8000},
                                    "periodSeconds": 2,
                                },
                            }
                        ]
                    },
                },
            },
        }
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": CONNECTOR_DEPLOYMENT},
            "spec": {
                "selector": {"app": CONNECTOR_DEPLOYMENT},
                "ports": [{"port": 8000, "targetPort": 8000}],
            },
        }
        self.apply(
            yaml_document(deployment) + b"\n---\n" + yaml_document(service),
            "deploy fixture connector",
        )
        self.wait_connector()

    def wait_connector(self) -> None:
        self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            f"deployment/{CONNECTOR_DEPLOYMENT}",
            "--timeout=180s",
            action="wait for fixture connector",
        )

    def rollout_connector(self) -> None:
        self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "restart",
            f"deployment/{CONNECTOR_DEPLOYMENT}",
            action="restart fixture connector",
        )
        self.wait_connector()

    def call_connector_digest(self) -> str:
        port = free_loopback_port()
        self.start_port_forward(f"service/{CONNECTOR_DEPLOYMENT}", 8000, port, "connector")
        wait_until("connector endpoint", lambda: self.tcp_ready(port), timeout=60, interval=0.5)
        return mcp_digest(f"http://127.0.0.1:{port}/mcp", self.seed[STATIC_KEY])

    def verify_static_key(self) -> None:
        observed = self.decoded_secret_key(TARGET_SECRET, STATIC_KEY)
        self.record_assertion(
            "static key stayed unchanged", observed == self.seed[STATIC_KEY].encode()
        )

    def wait_for_backup(self, expected_digest: str) -> None:
        assert self.ledger is not None

        def backup_digest() -> str | None:
            result = self.aws(
                "secretsmanager",
                "get-secret-value",
                "--secret-id",
                self.backup_name,
                "--output",
                "json",
                action="read rotated backup",
                allow_failure=True,
            )
            if result.status != 0:
                return None
            payload = parse_json(result.stdout, "rotated backup")
            raw = payload.get("SecretString")
            if raw is None and isinstance(payload.get("SecretBinary"), str):
                raw = base64.b64decode(payload["SecretBinary"]).decode("utf-8")
            if not isinstance(raw, str):
                return None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return full_digest(raw)
            value = parsed.get(ROTATED_KEY) if isinstance(parsed, dict) else None
            return full_digest(value) if isinstance(value, str) else None

        wait_until("PushSecret backup", lambda: backup_digest() == expected_digest, timeout=180)
        target = next(
            (
                row
                for row in self.ledger.cleanup_targets()
                if row.kind == "secretsmanager" and row.identity == self.backup_name
            ),
            None,
        )
        if target is None:
            raise HarnessError("backup ledger intent is missing")
        if self.real_aws:
            description = self.aws(
                "secretsmanager",
                "describe-secret",
                "--secret-id",
                self.backup_name,
                "--output",
                "json",
                action="describe rotated backup",
            )
            arn = str(parse_json(description.stdout, "rotated backup identity").get("ARN", ""))
            self.aws(
                "secretsmanager",
                "tag-resource",
                "--secret-id",
                arn,
                "--tags",
                f"Key=purpose,Value={PURPOSE}",
                action="tag rotated backup",
            )
        self.ledger.mark_created(target.id, self.backup_name)
        self.record_assertion("PushSecret backed up rotated key", True, expected_digest[:12])

    def cleanup(self) -> None:
        assert self.ledger is not None
        with self.cleanup_log.open("ab") as log:
            log.write(b"cleanup start\n")
            log.flush()
            os.fsync(log.fileno())
        targets = self.ledger.cleanup_targets()
        for target in targets:
            if target.kind == "secretsmanager":
                self.aws(
                    "secretsmanager",
                    "delete-secret",
                    "--secret-id",
                    target.identity,
                    "--force-delete-without-recovery",
                    action="delete exact provider entry",
                    allow_failure=True,
                )
                if not self.real_aws:
                    wait_until(
                        f"emulator provider deletion {target.identity}",
                        lambda target=target: self.aws_reports_absent(
                            "secretsmanager",
                            "describe-secret",
                            ["--secret-id", target.identity],
                            "verify emulator provider entry absent",
                            ["ResourceNotFoundException"],
                        ),
                        timeout=60,
                    )
            elif target.kind == "iam_role_policy":
                role, policy = target.identity.split("/", 1)
                self.aws(
                    "iam",
                    "delete-role-policy",
                    "--role-name",
                    role,
                    "--policy-name",
                    policy,
                    action="delete exact IAM role policy",
                    allow_failure=True,
                )
            elif target.kind == "iam_role":
                self.aws(
                    "iam",
                    "delete-role",
                    "--role-name",
                    target.identity,
                    action="delete exact IAM role",
                    allow_failure=True,
                )
            elif target.kind == "iam_oidc_provider" and target.identity.startswith("arn:"):
                self.aws(
                    "iam",
                    "delete-open-id-connect-provider",
                    "--open-id-connect-provider-arn",
                    target.identity,
                    action="delete exact IAM OIDC provider",
                    allow_failure=True,
                )
            elif target.kind == "s3_bucket":
                for key in (".well-known/openid-configuration", "openid/v1/jwks"):
                    self.aws(
                        "s3api",
                        "delete-object",
                        "--bucket",
                        target.identity,
                        "--key",
                        key,
                        action="delete exact OIDC object",
                        allow_failure=True,
                    )
                self.aws(
                    "s3api",
                    "delete-bucket-policy",
                    "--bucket",
                    target.identity,
                    action="delete exact OIDC bucket policy",
                    allow_failure=True,
                )
                self.aws(
                    "s3api",
                    "delete-bucket",
                    "--bucket",
                    target.identity,
                    action="delete exact OIDC bucket",
                    allow_failure=True,
                )
            elif target.kind == "docker_image":
                self.runner.run(
                    ["docker", "image", "rm", "--force", target.identity],
                    "delete exact candidate image",
                    allow_failure=True,
                )
            elif target.kind == "kind_cluster":
                self.runner.run(
                    ["kind", "delete", "cluster", "--name", target.identity],
                    "delete owned kind cluster",
                    allow_failure=True,
                    timeout=180,
                )
        for process in reversed(self.background):
            ToolRunner._stop_group(process)
        self.background.clear()
        self.verify_cleanup(targets)
        with self.cleanup_log.open("ab") as log:
            log.write(b"cleanup complete\n")
            log.flush()
            os.fsync(log.fileno())

    def verify_cleanup(self, targets: list[CleanupTarget]) -> None:
        wait_until(
            "owned kind cluster deletion",
            lambda: self.current_clusters() == self.prior_clusters,
            timeout=180,
        )
        self.record_assertion("post run kind set equals exact prior set", True)
        for target in targets:
            if target.kind == "secretsmanager" and self.real_aws:
                wait_until(
                    f"Secrets Manager deletion {target.identity}",
                    lambda target=target: self.aws_reports_absent(
                        "secretsmanager",
                        "describe-secret",
                        ["--secret-id", target.identity],
                        "verify provider entry absent",
                        ["ResourceNotFoundException"],
                    ),
                    timeout=180,
                )
            elif target.kind == "iam_role":
                wait_until(
                    f"IAM role deletion {target.identity}",
                    lambda target=target: self.aws_reports_absent(
                        "iam",
                        "get-role",
                        ["--role-name", target.identity],
                        "verify IAM role absent",
                        ["NoSuchEntity"],
                    ),
                    timeout=120,
                )
            elif target.kind == "iam_oidc_provider" and target.identity.startswith("arn:"):
                wait_until(
                    "IAM OIDC provider deletion",
                    lambda target=target: self.aws_reports_absent(
                        "iam",
                        "get-open-id-connect-provider",
                        ["--open-id-connect-provider-arn", target.identity],
                        "verify IAM OIDC provider absent",
                        ["NoSuchEntity"],
                    ),
                    timeout=120,
                )
            elif target.kind == "s3_bucket":
                wait_until(
                    f"S3 bucket deletion {target.identity}",
                    lambda target=target: self.aws_reports_absent(
                        "s3api",
                        "head-bucket",
                        ["--bucket", target.identity],
                        "verify OIDC bucket absent",
                        ["404", "NoSuchBucket"],
                    ),
                    timeout=120,
                )
        if self.real_aws:
            tagged = self.aws(
                "resourcegroupstaggingapi",
                "get-resources",
                "--tag-filters",
                f"Key=purpose,Values={PURPOSE}",
                "--output",
                "json",
                action="verify owned AWS tags absent",
            )
            mappings = parse_json(tagged.stdout, "post cleanup tag inventory").get(
                "ResourceTagMappingList", []
            )
            self.record_assertion("post cleanup tag inventory is empty", not mappings)

    def current_clusters(self) -> set[str]:
        result = self.runner.run(["kind", "get", "clusters"], "verify kind inventory")
        return {line.strip() for line in result.stdout.decode().splitlines() if line.strip()}

    def aws_reports_absent(
        self,
        service: str,
        operation: str,
        arguments: Sequence[str],
        action: str,
        codes: Sequence[str],
    ) -> bool:
        result = self.aws(
            service,
            operation,
            *arguments,
            action=action,
            allow_failure=True,
        )
        return tool_error_has_code(result, *codes)

    def write_evidence(self, status: str, cleanup_status: str) -> None:
        evidence = {
            "candidate_commit": self.commit,
            "mode": "real-aws" if self.real_aws else self.mode,
            "profile": PROFILE,
            "region": REGION,
            "owned_resources": {
                "cluster": self.cluster,
                "primary": self.primary_name,
                "backup": self.backup_name,
                "bucket": self.bucket_name if self.real_aws else None,
                "role": self.role_name if self.real_aws else None,
            },
            "images": self.image_records,
            "seed_digests": {
                STATIC_KEY: digest_prefix(self.seed[STATIC_KEY]),
                ROTATED_KEY: digest_prefix(self.seed[ROTATED_KEY]),
            },
            "assertions": self.assertions,
            "orchestrator_status": status,
            "cleanup_status": cleanup_status,
            "commands": self.commands,
        }
        write_private_file(
            self.evidence_root / "evidence.json",
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        )


def mcp_digest(url: str, bearer_value: str) -> str:
    state: dict[str, str | None] = {"session": None, "version": "2024-11-05"}

    def post(body: dict[str, Any], notification: bool = False) -> dict[str, Any] | None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": str(state["version"]),
            "Authorization": f"Bearer {bearer_value}",
        }
        if state["session"]:
            headers["mcp-session-id"] = str(state["session"])
        request = urllib.request.Request(
            url, data=json.dumps(body).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                session = response.headers.get("mcp-session-id")
                if session:
                    state["session"] = session
                raw = response.read().decode("utf-8", "replace")
        except (OSError, urllib.error.URLError) as exc:
            raise HarnessError("fixture connector MCP request failed") from exc
        if notification:
            return None
        for line in raw.splitlines():
            if line.startswith("data:"):
                return json.loads(line[5:].strip())
        return json.loads(raw)

    initialized = post(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": state["version"],
                "capabilities": {},
                "clientInfo": {"name": "curie-provider-harness", "version": "0"},
            },
        }
    )
    if not isinstance(initialized, dict) or "result" not in initialized:
        raise HarnessError("fixture connector MCP initialize failed")
    state["version"] = initialized["result"].get("protocolVersion", state["version"])
    post({"jsonrpc": "2.0", "method": "notifications/initialized"}, notification=True)
    listed = post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = (
        sorted(tool.get("name") for tool in listed.get("result", {}).get("tools", []))
        if isinstance(listed, dict)
        else []
    )
    if tools != ["rotated_key_digest"]:
        raise HarnessError("fixture connector exposed an unexpected MCP tool surface")
    called = post(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "rotated_key_digest", "arguments": {}},
        }
    )
    if not isinstance(called, dict):
        raise HarnessError("fixture connector MCP call failed")
    result = called.get("result", {})
    structured = result.get("structuredContent") if isinstance(result, dict) else None
    candidates: list[str] = []
    if isinstance(structured, dict):
        candidates.extend(value for value in structured.values() if isinstance(value, str))
    if isinstance(result, dict):
        for block in result.get("content", []):
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                candidates.append(block["text"])
    for candidate in candidates:
        match = re.search(r"\b[0-9a-f]{64}\b", candidate)
        if match:
            return match.group(0)
    raise HarnessError("fixture connector returned no digest")


def candidate_snapshot(
    repo_root: pathlib.Path, runner: ToolRunner
) -> tuple[str, pathlib.Path, tempfile.TemporaryDirectory[str]]:
    status = runner.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        "inspect candidate tree",
    ).stdout
    if status.strip():
        raise HarnessError("candidate tracked tree is dirty")
    commit = (
        runner.run(["git", "rev-parse", "HEAD"], "resolve candidate commit").stdout.decode().strip()
    )
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise HarnessError("candidate commit identity is invalid")
    context: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
        prefix="curie-provider-snapshot-"
    )
    root = pathlib.Path(context.name)
    os.chmod(root, 0o700)
    archive = root / "candidate.tar"
    runner.run(
        ["git", "archive", "--format=tar", f"--output={archive}", commit],
        "export candidate commit",
    )
    snapshot = root / "source"
    snapshot.mkdir(mode=0o700)
    with tarfile.open(archive, "r") as tar:
        tar.extractall(snapshot, filter="data")
    archive.unlink()
    return commit, snapshot, context


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--curie-bin", type=pathlib.Path, required=True, help=argparse.SUPPRESS)
    parser.add_argument("--seed", type=pathlib.Path)
    parser.add_argument("--eso", choices=("preinstalled", "none"))
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--real-aws", action="store_true")
    args = parser.parse_args()
    if args.ci and (args.real_aws or args.eso is not None):
        parser.error("--ci cannot be used with --real-aws or --eso")
    if args.real_aws and args.eso == "none":
        parser.error("--real-aws cannot be used with --eso none")
    return args


def main() -> int:
    args = arguments()
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    try:
        curie_bin = args.curie_bin.resolve(strict=True)
    except OSError:
        safe_print("current curie executable is unavailable", error=True)
        return 1
    if not curie_bin.is_file() or not os.access(curie_bin, os.X_OK):
        safe_print("current curie executable is not executable", error=True)
        return 1
    seed_path = args.seed or repo_root / "cli/tests/fixtures/provider-bundle/seed.json"
    try:
        seed = load_seed(seed_path)
    except ValueError as exc:
        safe_print(f"seed error: {exc}", error=True)
        return 2
    tools = {"git", "docker", "kind", "kubectl", "uv"}
    if args.real_aws or args.eso != "none" or args.ci:
        tools.update({"aws", "helm"})
    try:
        require_tools(tools)
        setup_context = tempfile.TemporaryDirectory(prefix="curie-provider-setup-")
        setup = pathlib.Path(setup_context.name)
        os.chmod(setup, 0o700)
        setup_commands: list[str] = []
        setup_runner = ToolRunner(setup, setup_commands)
        commit, snapshot, snapshot_context = candidate_snapshot(repo_root, setup_runner)
    except HarnessError as exc:
        safe_print(str(exc), error=True)
        return 1
    guard = install_signal_handlers()
    try:
        modes = ["preinstalled", "none"] if args.ci else [args.eso or "preinstalled"]
        for mode in modes:
            HarnessCase(
                repo_root,
                snapshot,
                commit,
                seed,
                mode,
                args.real_aws,
                curie_bin,
            ).run()
    except HarnessInterrupted:
        safe_print("provider harness interrupted after cleanup", error=True)
        return INTERRUPTED_EXIT
    except HarnessError as exc:
        safe_print(str(exc), error=True)
        return 1
    except Exception:
        safe_print("provider harness failed", error=True)
        return 1
    finally:
        guard.restore()
        snapshot_context.cleanup()
        setup_context.cleanup()
    safe_print("provider harness: all requested modes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
