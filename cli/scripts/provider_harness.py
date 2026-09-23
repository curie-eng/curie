#!/usr/bin/env python3
"""Owned AWS Secrets Manager acceptance harness.

The harness keeps provider values out of process arguments and user visible
output. Every external command writes stdout and stderr to private files. The
SQLite ledger is the sole authority for cleanup targets.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
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
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Sequence
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
ROTATION_MAX_BLIND_SECONDS = 1.0
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")
ROTATION_NAMESPACE = "curie-aws-secrets-e2e-rotation"
ROTATION_LOGICAL_NAME = "acme-harness-fixture"
ROTATION_PUSH_SECRET = f"{ROTATION_LOGICAL_NAME}-rotated-backup"
ROTATION_OWNER_EXTERNAL_SECRET = f"{ROTATION_LOGICAL_NAME}-owner"
ROTATION_WORKLOAD = "acme-harness-rotation-workload"
ROTATION_COUNT = 15
ROTATION_SPACING_SECONDS = 3.0
ROTATION_SAMPLE_INTERVAL = 0.25
ROTATION_BACKUP_BOUND_SECONDS = 15.0
ROTATION_REVERT_BOUND_SECONDS = 3.0
ROTATION_SEED_OUTCOMES = frozenset(
    {"no_backup", "not_in_backup", "created", "added", "already_present"}
)


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


def require_owned_secret_path(name: str) -> str:
    """Accept exactly `<owned prefix>/<leaf>` for a Secrets Manager entry name."""
    prefix, separator, leaf = name.partition("/")
    if (
        separator != "/"
        or "/" in leaf
        or ".." in name
        or len(name) > 512
        or SAFE_NAME.fullmatch(leaf) is None
    ):
        raise ValueError(f"provider entry name is outside the harness prefix: {name!r}")
    require_owned_name(prefix)
    return name


ROTATION_SLOT_TOLERANCE_SECONDS = 1.0


def first_rotation_sample_violation(
    samples: Sequence[tuple[float, float, str | None]],
    initial: str,
    rotations: Sequence[tuple[float, float, str]],
) -> dict[str, Any] | None:
    """Return the first sample that saw a value other than the live rotation.

    ``samples`` are (read_began, read_ended, digest) from a continuous reader;
    ``rotations`` are (write_began, write_returned, digest) in order, with
    ``initial`` live before the first write. Value r_k can be live from the
    start of write k until write k+1 returns, so a read whose span overlaps an
    in-flight write may see either side; any other digest is a revert.
    """
    starts = [float("-inf")] + [began for began, _, _ in rotations]
    ends = [returned for _, returned, _ in rotations] + [float("inf")]
    digests = [initial] + [digest for _, _, digest in rotations]
    for index, (began, ended, digest) in enumerate(samples):
        allowed = {
            digests[k]
            for k in range(len(digests))
            if starts[k] <= ended and began <= ends[k]
        }
        if digest not in allowed:
            return {
                "sample_index": index,
                "at": began,
                "observed": None if digest is None else digest[:12],
                "allowed": sorted(value[:12] for value in allowed),
            }
    return None


def max_sampling_blind_seconds(
    samples: Sequence[tuple[float, float, str | None]], phase_start: float, phase_end: float
) -> float:
    """Longest stretch of the phase no bounded read observed.

    A read only pins the value somewhere inside its own span, so a long read
    counts as blind for its full duration, as do the gaps before the first
    read, between read starts, and after the last read ends.
    """
    if not samples:
        return phase_end - phase_start
    begins = [began for began, _, _ in samples]
    blind = [begins[0] - phase_start, phase_end - samples[-1][1]]
    blind.extend(ended - began for began, ended, _ in samples)
    blind.extend(later - earlier for earlier, later in zip(begins, begins[1:], strict=False))
    return max(blind)


def parse_rotation_report(stdout: bytes, expected_keys: Sequence[str]) -> dict[str, str]:
    """Parse the value free rotation_apply report into {key: outcome}."""
    lines = [line for line in stdout.decode("utf-8", "strict").splitlines() if line.strip()]
    if len(lines) != 1:
        raise HarnessError("rotation apply report must be exactly one JSON line")
    parsed = parse_json(lines[0].encode(), "rotation apply report")
    if not isinstance(parsed, dict) or not isinstance(parsed.get("entry"), str):
        raise HarnessError("rotation apply report has no entry")
    seeds = parsed.get("seeds")
    if not isinstance(seeds, list) or not seeds:
        raise HarnessError("rotation apply report has no seeds")
    outcomes: dict[str, str] = {}
    for seed in seeds:
        if not isinstance(seed, dict):
            raise HarnessError("rotation apply seed has an invalid shape")
        key = seed.get("key")
        outcome = seed.get("outcome")
        if not isinstance(key, str) or key in outcomes:
            raise HarnessError("rotation apply seed key is invalid")
        if outcome not in ROTATION_SEED_OUTCOMES:
            raise HarnessError("rotation apply seed outcome is unknown")
        outcomes[key] = outcome
    if set(outcomes) != set(expected_keys):
        raise HarnessError("rotation apply seeds do not match the rotated keys")
    return outcomes


def decoded_document_key(document: dict[str, Any] | None, key: str) -> bytes | None:
    if document is None:
        return None
    data = document.get("data")
    encoded = data.get(key) if isinstance(data, dict) else None
    if not isinstance(encoded, str):
        return None
    try:
        return base64.b64decode(encoded, validate=True)
    except ValueError:
        return None


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
        self._lock = threading.Lock()

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
        sensitive: bool = False,
    ) -> ToolResult:
        """Run a tool, recording the command for evidence.

        ``sensitive`` keeps stdout in memory only: nothing the tool prints is
        written under the private directory, so a retained diagnostics
        directory cannot hold Secret values read through this path.
        """
        with self._lock:
            self.counter += 1
            number = self.counter
            self.evidence_commands.append(self._command_for_evidence(argv))
        stdout_path = self.private_dir / f"tool-{number:04d}.stdout"
        stderr_path = self.private_dir / f"tool-{number:04d}.stderr"
        if not sensitive:
            write_private_file(stdout_path, b"")
        write_private_file(stderr_path, b"")
        captured: bytes | None = None
        with contextlib.ExitStack() as stack:
            stderr_stream = stack.enter_context(stderr_path.open("wb"))
            stdout_target: Any = (
                subprocess.PIPE if sensitive else stack.enter_context(stdout_path.open("wb"))
            )
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.PIPE if input_data is not None else subprocess.DEVNULL,
                stdout=stdout_target,
                stderr=stderr_stream,
                env=env,
                start_new_session=True,
            )
            try:
                captured, _ = process.communicate(input=input_data, timeout=timeout)
            except subprocess.TimeoutExpired:
                self._stop_group(process)
                raise HarnessError(format_tool_error(action, 124)) from None
            except BaseException:
                self._stop_group(process)
                raise
        stdout = (captured or b"") if sensitive else stdout_path.read_bytes()
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


class RotationSampler:
    """Background reader of the rotation Secret for the 15 rotation phase."""

    def __init__(self, harness: Any, static_digest: str) -> None:
        self.harness = harness
        self.static_digest = static_digest
        self.samples: list[tuple[float, float, str | None]] = []
        self.static_ok = True
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="rotation-sampler", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=120)
        if self._thread.is_alive():
            raise HarnessError("rotation sampler did not stop")
        if self.error is not None:
            raise HarnessError("rotation sampler failed") from self.error

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                began = time.monotonic()
                document = self.harness.secret_document(TARGET_SECRET, ROTATION_NAMESPACE)
                ended = time.monotonic()
                rotated = decoded_document_key(document, ROTATED_KEY)
                static = decoded_document_key(document, STATIC_KEY)
                digest = None if rotated is None else full_digest(rotated)
                self.samples.append((began, ended, digest))
                if static is None or full_digest(static) != self.static_digest:
                    self.static_ok = False
                self._stop.wait(max(0.0, ROTATION_SAMPLE_INTERVAL - (ended - began)))
        except BaseException as exc:  # surfaced by stop()
            self.error = exc


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
    for key in tuple(environment):
        if key == "AWS_ENDPOINT_URL" or key.startswith("AWS_ENDPOINT_URL_"):
            environment.pop(key)
    environment["AWS_EC2_METADATA_DISABLED"] = "true"
    environment["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] = "true"
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
        sensitive: bool = False,
    ) -> ToolResult:
        return self.runner.run(
            self.kargs(*args),
            action,
            input_data=input_data,
            timeout=timeout,
            allow_failure=allow_failure,
            sensitive=sensitive,
        )

    def aws(
        self,
        service: str,
        operation: str,
        *args: str,
        action: str,
        allow_failure: bool = False,
        timeout: int = 180,
        sensitive: bool = False,
    ) -> ToolResult:
        if not self.real_aws and self.aws_endpoint is None:
            raise HarnessError("emulator AWS endpoint is unavailable")
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
            sensitive=sensitive,
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
                try:
                    self.enter_cleanup_signal_mode()
                except HarnessInterrupted as exc:
                    if pending is None:
                        orchestrator_status = "interrupted"
                        pending = exc
                try:
                    self.cleanup()
                    cleanup_status = "complete"
                except BaseException as exc:
                    cleanup_status = "failed"
                    if pending is None:
                        pending = exc
                if self.cleanup_signals and not isinstance(pending, HarnessInterrupted):
                    if isinstance(pending, HarnessError):
                        safe_print(str(pending), error=True)
                    orchestrator_status = "interrupted"
                    pending = HarnessInterrupted(self.cleanup_signals[0])
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
        if os.environ.get("CURIE_SECRETS_CLI_ONLY") == "1":
            self.create_cluster()
            self.create_namespace_and_rbac()
            if self.real_aws:
                raise HarnessError("the secrets command proof uses the emulator")
            self.start_moto()
            self.install_eso()
            self.kubectl(
                "-n",
                ESO_NAMESPACE,
                "rollout",
                "status",
                "deployment/external-secrets",
                "--timeout=180s",
                action="wait for External Secrets to reload its endpoint",
                timeout=200,
            )
            self.prove_secrets_commands()
            return
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
            "connector observed rotated key", observed != full_digest(rotated_value)
        )
        self.verify_static_key()
        self.wait_for_backup(full_digest(rotated_value))
        if not self.real_aws:
            self.run_rotation_suite()

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

    def prove_secrets_commands(self) -> None:
        """Standalone set, check, and rm against the emulator.

        Values stay in private files and process environment. Evidence records
        sha256 prefixes, object names, and pod uids only.
        """
        if self.aws_endpoint is None:
            raise HarnessError("emulator endpoint is unavailable")
        release = "acme"
        prefix = f"{OWNED_PREFIX}{self.suffix}"
        logical = "github-webhook-secret"
        key = "githubWebhookSecret"
        secret_id = f"{prefix}/{release}/{logical}"
        home = self.work / "cli-home"
        home.mkdir(mode=0o700)
        install = (
            "version: 1\n"
            "install:\n"
            "  namespace: curie-aws-secrets-e2e\n"
            "  release: acme\n"
            f"  context: {self.context}\n"
            "secrets:\n"
            "  provider: aws\n"
            "  region: us-east-1\n"
            f"  prefix: {prefix}\n"
            "  role_arn: arn:aws:iam::000000000000:role/curie-sync\n"
        )
        write_private_file(home / "curie.yaml", install)
        trip = self.work / "trip-bin"
        trip.mkdir(mode=0o700)
        marker = self.work / "kubectl-called"
        write_private_file(
            trip / "kubectl",
            "#!/bin/sh\nprintf x > " + shlex.quote(str(marker)) + "\nexit 86\n",
        )
        os.chmod(trip / "kubectl", 0o755)
        value = secrets.token_hex(16)
        write_private_file(self.work / "webhook.value", value)

        def curie_env(
            *,
            kube: bool,
            path_prefix: str | None = None,
            hold: str = value,
        ) -> dict[str, str]:
            env = {
                "HOME": str(home),
                "CURIE_CONFIG_DIR": str(home),
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "AWS_DEFAULT_REGION": REGION,
                "AWS_REGION": REGION,
                "AWS_ENDPOINT_URL": self.aws_endpoint or "",
                "AWS_EC2_METADATA_DISABLED": "true",
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "CURIE_HOLD": hold,
            }
            if path_prefix is not None:
                env["PATH"] = f"{path_prefix}:{env['PATH']}"
            if kube:
                env["KUBECONFIG"] = str(self.admin_kubeconfig)
            return env

        def run_curie(
            args: list[str],
            action: str,
            *,
            kube: bool,
            path_prefix: str | None = None,
            hold: str = value,
        ) -> tuple[int, str]:
            result = self.runner.run(
                [str(self.curie_bin), *args],
                action,
                env=curie_env(kube=kube, path_prefix=path_prefix, hold=hold),
                allow_failure=True,
                timeout=240,
            )
            # ToolRunner has no cwd. The installation is discovered from --file.
            text = result.stdout.decode("utf-8", "replace") + result.stderr_path.read_text(
                encoding="utf-8", errors="replace"
            )
            if hold in text:
                raise HarnessError(f"{action} printed a secret value")
            return result.status, text

        status, text = run_curie(
            [
                "secrets",
                "set",
                f"{logical}/{key}",
                "--from-env",
                "CURIE_HOLD",
                "--file",
                str(home / "curie.yaml"),
            ],
            "set on an unprovisioned install",
            kube=False,
            path_prefix=str(trip),
        )
        self.record_assertion(
            "unprovisioned set succeeds and names Secrets Manager only",
            status == 0 and "Secrets Manager only" in text and "not provisioned" in text,
        )
        self.record_assertion("unprovisioned set makes no kubectl call", not marker.exists())
        observed = self.provider_key_digest(secret_id, key)
        self.record_assertion(
            "unprovisioned set wrote the provider key",
            observed == digest_prefix(value),
            digest_prefix(value),
        )

        denied, denied_text = run_curie(
            [
                "secrets",
                "set",
                "postgres-password/postgresPassword",
                "--from-env",
                "CURIE_HOLD",
                "--file",
                str(home / "curie.yaml"),
            ],
            "set an immutable inventory entry",
            kube=False,
            path_prefix=str(trip),
        )
        immutable_id = f"{prefix}/{release}/postgres-password"
        absent = self.aws(
            "secretsmanager",
            "describe-secret",
            "--secret-id",
            immutable_id,
            action="confirm immutable entry was not written",
            allow_failure=True,
        )
        self.record_assertion(
            "immutable set is refused and writes nothing",
            denied != 0
            and "immutable" in denied_text
            and tool_error_has_code(absent, "ResourceNotFoundException"),
        )

        missing_id = f"{prefix}/{release}/slack-app-token"
        missing_body = write_private_file(
            self.work / "missing.json",
            json.dumps({"unused": "synthetic"}),
        )
        self.aws(
            "secretsmanager",
            "create-secret",
            "--name",
            missing_id,
            "--secret-string",
            f"file://{missing_body}",
            action="create an object missing its inventory key",
        )
        missing_status, missing_text = run_curie(
            ["secrets", "check", "slack-app-token", "--file", str(home / "curie.yaml"), "--json"],
            "check a missing inventory key",
            kube=False,
        )
        self.record_assertion(
            "check reports a missing key",
            missing_status != 0 and "missing" in missing_text and "slackAppToken" in missing_text,
        )

        warn_at = (dt.datetime.now(dt.UTC) + dt.timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
        warn_status, warn_text = run_curie(
            [
                "secrets",
                "set",
                f"{logical}/{key}",
                "--from-env",
                "CURIE_HOLD",
                "--expires",
                warn_at,
                "--file",
                str(home / "curie.yaml"),
            ],
            "set an expiry inside 30 days",
            kube=False,
        )
        check_warn, check_warn_text = run_curie(
            ["secrets", "check", logical, "--file", str(home / "curie.yaml"), "--json"],
            "check warns inside 30 days",
            kube=False,
        )
        self.record_assertion(
            "check warns 30 days out and still succeeds",
            warn_status == 0 and check_warn == 0 and "warning" in check_warn_text,
        )
        expired_status, _expired_text = run_curie(
            [
                "secrets",
                "set",
                f"{logical}/{key}",
                "--from-env",
                "CURIE_HOLD",
                "--expires",
                "2020-01-01T00:00:00Z",
                "--file",
                str(home / "curie.yaml"),
            ],
            "set an expired timestamp",
            kube=False,
        )
        check_expired, check_expired_text = run_curie(
            ["secrets", "check", logical, "--file", str(home / "curie.yaml"), "--json"],
            "check fails when expired",
            kube=False,
        )
        self.record_assertion(
            "check fails when the provider timestamp is expired",
            expired_status == 0 and check_expired != 0 and "expired" in check_expired_text,
        )

        self.install_command_store(release)
        self.install_pause_consumer(release)
        before = self.pod_uid(release)
        refreshed = secrets.token_hex(16)
        write_private_file(self.work / "webhook-next.value", refreshed)
        provisioned, provisioned_text = run_curie(
            [
                "secrets",
                "set",
                f"{logical}/{key}",
                "--from-env",
                "CURIE_HOLD",
                "--expires",
                "2027-06-01T00:00:00Z",
                "--file",
                str(home / "curie.yaml"),
            ],
            "set on a provisioned install",
            kube=True,
            hold=refreshed,
        )
        after = self.pod_uid(release)
        version = self.provider_version(secret_id)
        stamped = self.deployment_stamp(release)
        synced = self.provider_key_digest(secret_id, key)
        self.record_assertion(
            "provisioned set syncs and rolls only the inventory consumer",
            provisioned == 0
            and "rolled api" in provisioned_text
            and before != after
            and stamped == version
            and synced == digest_prefix(refreshed),
            f"uid changed={before != after} stamp_matches={stamped == version}",
        )

        edited = secrets.token_hex(16)
        edited_body = write_private_file(
            self.work / "webhook-edited.json",
            json.dumps({key: edited}),
        )
        self.aws(
            "secretsmanager",
            "put-secret-value",
            "--secret-id",
            secret_id,
            "--secret-string",
            f"file://{edited_body}",
            action="edit the provider object outside the CLI",
            sensitive=True,
        )
        self.kubectl(
            "-n",
            NAMESPACE,
            "annotate",
            "externalsecret",
            logical,
            "force-sync=harness-stale",
            "--overwrite",
            action="ask External Secrets to read the edited object",
        )
        wait_until(
            "External Secrets to publish the edited key",
            lambda: self.secret_key_digest(release, key) == digest_prefix(edited),
            timeout=180,
        )
        stale_status, stale_text = run_curie(
            ["secrets", "check", logical, "--file", str(home / "curie.yaml"), "--json"],
            "check a stale consumer after sync",
            kube=True,
        )
        self.record_assertion(
            "check reports a stale consumer after the provider edit synced",
            stale_status != 0 and "stale" in stale_text and "api" in stale_text,
        )

        removed, removed_text = run_curie(
            ["secrets", "rm", logical, "--file", str(home / "curie.yaml")],
            "remove the provider entry",
            kube=True,
        )
        gone = self.aws(
            "secretsmanager",
            "describe-secret",
            "--secret-id",
            secret_id,
            "--output",
            "json",
            action="confirm the provider entry is gone",
            allow_failure=True,
        )
        if gone.status == 0:
            described = parse_json(gone.stdout, "deleted provider object")
            provider_gone = bool(described.get("DeletedDate"))
        else:
            provider_gone = tool_error_has_code(gone, "ResourceNotFoundException")
        external = self.kubectl(
            "-n",
            NAMESPACE,
            "get",
            "externalsecret",
            logical,
            action="confirm the ExternalSecret is gone",
            allow_failure=True,
        )
        self.record_assertion(
            "rm removes the provider entry and its ExternalSecret",
            removed == 0
            and "ExternalSecret" in removed_text
            and provider_gone
            and external.status != 0,
        )

        bare = self.work / "local-home"
        bare.mkdir(mode=0o700)
        write_private_file(
            bare / "curie.yaml",
            "version: 1\ninstall:\n  namespace: curie-aws-secrets-e2e\n  release: acme\n",
        )
        local_value = secrets.token_hex(8)
        local_env = curie_env(kube=False, path_prefix=str(trip))
        local_env["HOME"] = str(bare)
        local_env["CURIE_CONFIG_DIR"] = str(bare)
        local_env["CURIE_LOCAL"] = local_value
        local = self.runner.run(
            [
                str(self.curie_bin),
                "secrets",
                "set",
                "MODEL_KEY",
                "--from-env",
                "CURIE_LOCAL",
                "--file",
                str(bare / "curie.yaml"),
            ],
            "set without a provider",
            env=local_env,
            allow_failure=True,
        )
        local_text = local.stdout.decode("utf-8", "replace") + local.stderr_path.read_text(
            encoding="utf-8", errors="replace"
        )
        stored = (bare / "credentials.json").read_text(encoding="utf-8")
        self.record_assertion(
            "provider-absent set stays in private storage",
            local.status == 0
            and "Curie private storage" in local_text
            and "MODEL_KEY" in stored
            and local_value not in local_text
            and not marker.exists(),
        )

    def provider_version(self, secret_id: str) -> str:
        result = self.aws(
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            secret_id,
            "--output",
            "json",
            action="read provider version",
            sensitive=True,
        )
        return str(parse_json(result.stdout, "provider version").get("VersionId", ""))

    def provider_key_digest(self, secret_id: str, key: str) -> str:
        result = self.aws(
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            secret_id,
            "--output",
            "json",
            action="read provider key digest",
            sensitive=True,
        )
        body = json.loads(parse_json(result.stdout, "provider object")["SecretString"])
        return digest_prefix(str(body[key]))

    def install_command_store(self, release: str) -> None:
        provider = self.apply_emulator_credentials(NAMESPACE)
        store = {
            "apiVersion": "external-secrets.io/v1",
            "kind": "SecretStore",
            "metadata": {"name": f"{release}-aws", "namespace": NAMESPACE},
            "spec": {"provider": provider},
        }
        self.apply(yaml_document(store), "create the install SecretStore")
        wait_until(
            "SecretStore readiness",
            lambda: self.resource_ready("secretstore", f"{release}-aws", NAMESPACE),
            timeout=180,
        )

    def install_pause_consumer(self, release: str) -> None:
        name = f"{release}-curie-api"
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name}},
                    "spec": {
                        "containers": [
                            {
                                "name": "pause",
                                "image": "registry.k8s.io/pause:3.10",
                            }
                        ]
                    },
                },
            },
        }
        self.apply(yaml_document(deployment), "create the inventory consumer")
        self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            f"deployment/{name}",
            "--timeout=180s",
            action="wait for the inventory consumer",
            timeout=200,
        )

    def pod_uid(self, release: str) -> str:
        name = f"{release}-curie-api"

        def running() -> str:
            result = self.kubectl(
                "-n",
                NAMESPACE,
                "get",
                "pod",
                "-l",
                f"app={name}",
                "-o",
                "json",
                action="read consumer pod uid",
                allow_failure=True,
            )
            if result.status != 0:
                return ""
            document = parse_json(result.stdout, "consumer pods")
            uids = [
                str(item.get("metadata", {}).get("uid", ""))
                for item in document.get("items", [])
                if item.get("status", {}).get("phase") == "Running"
                and not item.get("metadata", {}).get("deletionTimestamp")
            ]
            return uids[0] if len(uids) == 1 else ""

        wait_until("one running consumer pod", lambda: running() != "", timeout=120)
        return running()

    def deployment_stamp(self, release: str) -> str:
        name = f"{release}-curie-api"
        result = self.kubectl(
            "-n",
            NAMESPACE,
            "get",
            "deployment",
            name,
            "-o",
            "json",
            action="read consumer provider stamp",
        )
        document = parse_json(result.stdout, "consumer deployment")
        annotations = document["spec"]["template"]["metadata"].get("annotations", {})
        return str(annotations.get("curie.dev/provider-version", ""))

    def secret_key_digest(self, release: str, key: str) -> str:
        name = f"{release}-curie-github-webhook"
        result = self.kubectl(
            "-n",
            NAMESPACE,
            "get",
            "secret",
            name,
            "-o",
            "json",
            action="read synced key digest",
            allow_failure=True,
            sensitive=True,
        )
        if result.status != 0:
            return ""
        document = parse_json(result.stdout, "synced secret")
        encoded = document.get("data", {}).get(key)
        if not isinstance(encoded, str):
            return ""
        return digest_prefix(base64.b64decode(encoded))

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
        oidc_row = self.ledger.record_intent("iam_oidc_provider", issuer_host)
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
        self.ledger.mark_created(oidc_row, issuer_host)
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
                                "secretsmanager:UntagResource",
                                "secretsmanager:PutSecretValue",
                                "secretsmanager:DeleteResourcePolicy",
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

    def apply_emulator_credentials(self, namespace: str) -> dict[str, Any]:
        """Apply the synthetic moto credential Secret and return the store provider."""
        dummy = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "acme-harness-aws"},
            "stringData": {"access-key": "test", "secret-key": "test"},
        }
        self.apply(yaml_document(dummy), "apply isolated moto credentials", namespace=namespace)
        return {
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

    def apply_sync_objects(self) -> None:
        # Provider shape follows the official AWS and PushSecret references:
        # https://external-secrets.io/latest/provider/aws-secrets-manager/
        # https://raw.githubusercontent.com/external-secrets/external-secrets/v2.11.0/docs/snippets/aws-sm-push-secret-with-metadata.yaml
        if self.real_aws:
            provider = {
                "aws": {
                    "service": "SecretsManager",
                    "region": REGION,
                    "auth": {"jwt": {"serviceAccountRef": {"name": ESO_SERVICE_ACCOUNT}}},
                }
            }
        else:
            provider = self.apply_emulator_credentials(NAMESPACE)
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
        push_metadata_spec: dict[str, Any] = {"secretPushFormat": "string"}
        if self.real_aws:
            push_metadata_spec["tags"] = {"purpose": PURPOSE}
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
                        "metadata": {
                            "apiVersion": "kubernetes.external-secrets.io/v1alpha1",
                            "kind": "PushSecretMetadata",
                            "spec": push_metadata_spec,
                        },
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

    def secret_document(
        self, name: str, namespace: str = NAMESPACE
    ) -> dict[str, Any] | None:
        result = self.kubectl(
            "-n",
            namespace,
            "get",
            "secret",
            name,
            "-o",
            "json",
            action="read owned Secret",
            allow_failure=True,
            sensitive=True,
        )
        if result.status != 0:
            return None
        parsed = parse_json(result.stdout, "owned Secret")
        return parsed if isinstance(parsed, dict) else None

    def decoded_secret_key(
        self, name: str, key: str, namespace: str = NAMESPACE
    ) -> bytes | None:
        return decoded_document_key(self.secret_document(name, namespace), key)

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

    def start_rotation_pod(self, namespace: str = NAMESPACE) -> None:
        self.kubectl(
            "-n",
            namespace,
            "delete",
            "pod",
            ROTATION_POD,
            "--ignore-not-found",
            action="clear rotation pod",
        )
        self.apply(self.rotation_pod_manifest(), "start rotation pod", namespace=namespace)
        self.kubectl(
            "-n",
            namespace,
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
                sensitive=True,
            )
            if result.status != 0:
                return None
            payload = parse_json(result.stdout, "rotated backup")
            raw = payload.get("SecretString")
            if not isinstance(raw, str):
                return None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None
            value = parsed.get(ROTATED_KEY) if isinstance(parsed, dict) else None
            return full_digest(value) if isinstance(value, str) else None

        wait_until("PushSecret backup", lambda: backup_digest() == expected_digest, timeout=180)

        def push_secret_ready() -> bool:
            result = self.kubectl(
                "-n",
                NAMESPACE,
                "get",
                "pushsecret",
                "acme-harness-rotated",
                "-o",
                "json",
                action="read PushSecret readiness",
                allow_failure=True,
            )
            if result.status != 0:
                return False
            document = parse_json(result.stdout, "PushSecret readiness")
            conditions = document.get("status", {}).get("conditions", [])
            return isinstance(conditions, list) and any(
                isinstance(condition, dict)
                and condition.get("type") == "Ready"
                and condition.get("status") == "True"
                for condition in conditions
            )

        wait_until("PushSecret Ready=True", push_secret_ready, timeout=180)
        self.record_assertion("PushSecret reported Ready=True", True)
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
            tags = parse_json(description.stdout, "rotated backup identity").get("Tags", [])
            tagged = isinstance(tags, list) and any(
                isinstance(tag, dict)
                and tag.get("Key") == "purpose"
                and tag.get("Value") == PURPOSE
                for tag in tags
            )
            self.record_assertion("PushSecret backup has ownership tag", tagged)
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
        self.ledger.mark_created(target.id, self.backup_name)
        self.record_assertion("PushSecret backed up rotated key", True, expected_digest[:12])

    # Rotation owner suite (ADR 0163 decision 7). Emulator mode only; it drives
    # the Rust library through the rotation_apply example binary.

    def run_rotation_suite(self) -> None:
        assert self.ledger is not None
        prefix = require_owned_name(f"{OWNED_PREFIX}{self.suffix}")
        primary = require_owned_secret_path(f"{prefix}/{ROTATION_LOGICAL_NAME}")
        backup = require_owned_secret_path(f"{prefix}/{ROTATION_LOGICAL_NAME}-rotated")
        report: dict[str, Any] = {
            "namespace": ROTATION_NAMESPACE,
            "primary_entry": primary,
            "backup_entry": backup,
            "rotations": [],
        }
        try:
            self._rotation_suite(prefix, primary, backup, report)
            report["status"] = "passed"
        except BaseException:
            report["status"] = "failed"
            raise
        finally:
            write_private_file(
                self.evidence_root / "rotation-suite.json",
                json.dumps(report, indent=2, sort_keys=True) + "\n",
            )

    def _rotation_suite(
        self, prefix: str, primary: str, backup: str, report: dict[str, Any]
    ) -> None:
        assert self.ledger is not None
        binary = self.build_rotation_apply()
        static_value = self.seed[STATIC_KEY]
        static_digest = full_digest(static_value)
        report["static_digest"] = static_digest[:12]

        # 1. Namespace, store, primary entry, first apply with no backup, bootstrap.
        row = self.ledger.record_intent("kubernetes_namespace", ROTATION_NAMESPACE)
        self.setup_rotation_namespace()
        self.ledger.mark_created(row, ROTATION_NAMESPACE)
        primary_row = self.ledger.record_intent("secretsmanager", primary)
        self.write_rotation_primary(primary, {STATIC_KEY: static_value}, create=True)
        self.ledger.mark_created(primary_row, primary)
        backup_row = self.ledger.record_intent("secretsmanager", backup)
        backup_file = self.rotation_backup_file(backup, "backup-initial.json")
        self.record_assertion("rotation backup absent before first apply", backup_file is None)
        outcomes = self.run_rotation_apply(binary, prefix, backup_file)
        report["first_apply"] = outcomes
        self.record_assertion(
            "first apply reports no_backup",
            all(outcome == "no_backup" for outcome in outcomes.values()),
        )
        self.wait_rotation_key(STATIC_KEY, static_digest, "rotation static key sync")
        self.start_rotation_pod(ROTATION_NAMESPACE)
        r0_path, r0_digest = self.rotation_value("rotation-r0.value")
        self.exec_rotator(r0_path, r0_digest, bootstrap=True)
        self.wait_rotation_backup(backup, r0_digest, timeout=60)
        self.ledger.mark_created(backup_row, backup)
        report["r0"] = r0_digest[:12]

        # 2. Fifteen rotations on a fixed 3 s schedule while a background reader
        # samples the Secret continuously. The primary entry is deleted for
        # rotations 6 to 10 and the library re-applied at rotation 12; that
        # maintenance runs inside each slot so it never pauses the reader.
        sampler = RotationSampler(self, static_digest)
        rotations: list[tuple[float, float, str]] = []
        max_drift = 0.0
        slots_ok = True
        t0 = time.monotonic() + ROTATION_SPACING_SECONDS
        sampler.start()
        try:
            for index in range(1, ROTATION_COUNT + 1):
                record: dict[str, Any] = {"index": index}
                if index == 6:
                    self.delete_rotation_primary(primary)
                    record["primary_entry"] = "deleted"
                if index == 11:
                    self.write_rotation_primary(primary, {STATIC_KEY: static_value}, create=True)
                    record["primary_entry"] = "recreated"
                if index == 12:
                    reapply_file = self.rotation_backup_file(backup, "backup-reapply.json")
                    reapply = self.run_rotation_apply(binary, prefix, reapply_file)
                    record["reapply"] = reapply
                    self.record_assertion(
                        "re-apply at rotation 12 reports already_present",
                        all(outcome == "already_present" for outcome in reapply.values()),
                    )
                self.kubectl(
                    "-n",
                    ROTATION_NAMESPACE,
                    "annotate",
                    "externalsecret",
                    ROTATION_LOGICAL_NAME,
                    f"force-sync={self.suffix}-{index}-{secrets.token_hex(4)}",
                    "--overwrite",
                    action="force ExternalSecret reconcile",
                )
                value_path, expected = self.rotation_value(f"rotation-r{index}.value")
                slot = t0 + ROTATION_SPACING_SECONDS * (index - 1)
                wait = slot - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                began = time.monotonic()
                self.exec_rotator(value_path, expected)
                returned = time.monotonic()
                rotations.append((began, returned, expected))
                drift = began - slot
                max_drift = max(max_drift, abs(drift))
                if abs(drift) > ROTATION_SLOT_TOLERANCE_SECONDS:
                    slots_ok = False
                current = expected
                record.update(
                    {
                        "digest": current[:12],
                        "start_offset_seconds": round(began - t0, 3),
                        "slot_drift_seconds": round(drift, 3),
                        "write_seconds": round(returned - began, 3),
                    }
                )
                report["rotations"].append(record)
            last_return = rotations[-1][1]
            hold = last_return + ROTATION_SPACING_SECONDS - time.monotonic()
            if hold > 0:
                time.sleep(hold)
        finally:
            sampler.stop()
        samples = sampler.samples
        begins = [began for began, _, _ in samples]
        gaps = [later - earlier for earlier, later in zip(begins, begins[1:], strict=False)]
        violation = first_rotation_sample_violation(samples, r0_digest, rotations)
        starts = [began for began, _, _ in rotations]
        spacing = [later - earlier for earlier, later in zip(starts, starts[1:], strict=False)]
        report["sampling"] = {
            "samples": len(samples),
            "max_sample_gap_seconds": round(max(gaps, default=0.0), 3),
            "first_violation": violation,
            "max_slot_drift_seconds": round(max_drift, 3),
            "min_start_spacing_seconds": round(min(spacing, default=0.0), 3),
            "max_start_spacing_seconds": round(max(spacing, default=0.0), 3),
        }
        blind = max_sampling_blind_seconds(samples, t0, time.monotonic())
        report["sampling"]["max_blind_seconds"] = round(blind, 3)
        self.record_assertion(
            f"sampling left no blind stretch over {ROTATION_MAX_BLIND_SECONDS} s",
            blind <= ROTATION_MAX_BLIND_SECONDS,
            f"max blind {blind:.3f}s",
        )
        self.record_assertion(
            "no rotation was reverted across the continuous sample",
            bool(samples) and violation is None,
            "" if violation is None else f"sample {violation['sample_index']}",
        )
        self.record_assertion(
            "rotations started on the 3 s schedule within 1 s",
            slots_ok,
            f"max drift {max_drift:.3f}s",
        )
        self.record_assertion(
            "static key kept for the whole rotation phase", sampler.static_ok
        )
        r15_digest = current

        # 3. Backup convergence after the final rotation.
        deadline = last_return + 60
        converged: float | None = None
        while time.monotonic() < deadline:
            if self.rotation_backup_digest(backup) == r15_digest:
                converged = time.monotonic() - last_return
                break
            time.sleep(0.25)
        report["backup_convergence_seconds"] = None if converged is None else round(converged, 3)
        self.record_assertion(
            "backup equals final rotation within 10 s + 5 s",
            converged is not None and converged <= ROTATION_BACKUP_BOUND_SECONDS,
            "" if converged is None else f"{converged:.3f}s",
        )

        # 4. Seed restore into a static only Secret, then never overwrite.
        restore_file = self.rotation_backup_file(backup, "backup-restore.json")
        self.record_assertion("restore backup holds final rotation", restore_file is not None)
        self.kubectl(
            "-n",
            ROTATION_NAMESPACE,
            "patch",
            "secret",
            TARGET_SECRET,
            "--type=json",
            "-p",
            json.dumps([{"op": "remove", "path": f"/data/{ROTATED_KEY}"}]),
            action="remove rotated key from Secret",
        )
        self.record_assertion(
            "rotated key removed before restore",
            self.rotation_key_digest(ROTATED_KEY) is None,
        )
        restored = self.run_rotation_apply(binary, prefix, restore_file)
        report["restore_apply"] = restored
        self.record_assertion(
            "restore apply reports added",
            all(outcome == "added" for outcome in restored.values()),
        )
        self.record_assertion(
            "restore seeded final rotation", self.rotation_key_digest(ROTATED_KEY) == r15_digest
        )
        self.record_assertion(
            "restore kept static key", self.rotation_key_digest(STATIC_KEY) == static_digest
        )
        decoy_value = f"synthetic-decoy-{secrets.token_hex(24)}"
        decoy_file = write_private_file(
            self.work / "backup-decoy.json", json.dumps({ROTATED_KEY: decoy_value})
        )
        report["decoy"] = digest_prefix(decoy_value)
        decoy = self.run_rotation_apply(binary, prefix, decoy_file)
        report["decoy_apply"] = decoy
        self.record_assertion(
            "decoy apply reports already_present",
            all(outcome == "already_present" for outcome in decoy.values()),
        )
        self.record_assertion(
            "decoy never overwrote live value",
            self.rotation_key_digest(ROTATED_KEY) == r15_digest,
        )

        # 5. Namespace delete and rebuild: seed before any workload exists.
        self.kubectl(
            "delete",
            "namespace",
            ROTATION_NAMESPACE,
            "--wait=false",
            action="delete rotation namespace",
        )
        wait_until(
            "rotation namespace deletion",
            lambda: self.kubectl_reports_absent("namespace", ROTATION_NAMESPACE, None),
            timeout=300,
        )
        self.setup_rotation_namespace()
        rebuild_file = self.rotation_backup_file(backup, "backup-rebuild.json")
        self.record_assertion("rebuild backup holds final rotation", rebuild_file is not None)
        rebuilt = self.run_rotation_apply(binary, prefix, rebuild_file)
        report["rebuild_apply"] = rebuilt
        pods = self.kubectl(
            "-n", ROTATION_NAMESPACE, "get", "pods", "-o", "name", action="inventory rebuild pods"
        ).stdout.strip()
        self.record_assertion("no workload exists at rebuild seed", not pods)
        self.record_assertion(
            "rebuild apply reports created",
            all(outcome == "created" for outcome in rebuilt.values()),
        )
        self.record_assertion(
            "rebuild seeded final rotation before any workload",
            self.rotation_key_digest(ROTATED_KEY) == r15_digest,
        )
        self.wait_rotation_key(STATIC_KEY, static_digest, "rebuild static key sync")
        self.deploy_rotation_workload()
        observed = self.kubectl(
            "-n",
            ROTATION_NAMESPACE,
            "exec",
            f"deployment/{ROTATION_WORKLOAD}",
            "--",
            "python",
            "-c",
            "import hashlib,os;"
            f"print(hashlib.sha256(os.environ['{ROTATED_KEY}'].encode()).hexdigest())",
            action="read workload rotated key digest",
            sensitive=True,
        ).stdout.decode("ascii", "strict").strip()
        report["workload_digest"] = observed[:12]
        self.record_assertion("rebuilt workload reads final rotation", observed == r15_digest)

        # 6. Negative control: an Owner ExternalSecret over every key reverts a rotation.
        for kind, name in (
            ("deployment", ROTATION_WORKLOAD),
            ("externalsecret", ROTATION_LOGICAL_NAME),
            ("pushsecret", ROTATION_PUSH_SECRET),
            ("secret", TARGET_SECRET),
        ):
            self.kubectl(
                "-n",
                ROTATION_NAMESPACE,
                "delete",
                kind,
                name,
                "--ignore-not-found",
                "--wait=true",
                action=f"delete {kind} for negative control",
            )
        final_value = json.loads(require_private_file(rebuild_file).read_text(encoding="utf-8"))[
            ROTATED_KEY
        ]
        self.write_rotation_primary(
            primary, {STATIC_KEY: static_value, ROTATED_KEY: final_value}, create=False
        )
        del final_value
        owner = {
            "apiVersion": "external-secrets.io/v1",
            "kind": "ExternalSecret",
            "metadata": {"name": ROTATION_OWNER_EXTERNAL_SECRET},
            "spec": {
                "refreshInterval": "1s",
                "secretStoreRef": {"name": "acme-harness", "kind": "SecretStore"},
                "target": {"name": TARGET_SECRET, "creationPolicy": "Owner"},
                "data": [
                    {"secretKey": key, "remoteRef": {"key": primary, "property": key}}
                    for key in (STATIC_KEY, ROTATED_KEY)
                ],
            },
        }
        self.apply(
            yaml_document(owner), "apply Owner ExternalSecret control", namespace=ROTATION_NAMESPACE
        )
        self.wait_rotation_key(STATIC_KEY, static_digest, "Owner control static key")
        self.wait_rotation_key(ROTATED_KEY, r15_digest, "Owner control rotated key")
        self.start_rotation_pod(ROTATION_NAMESPACE)
        control_path, control_digest = self.rotation_value("rotation-control.value")
        self.exec_rotator(control_path, control_digest)
        returned = time.monotonic()
        reverted: float | None = None
        while time.monotonic() - returned <= ROTATION_REVERT_BOUND_SECONDS:
            if self.rotation_key_digest(ROTATED_KEY) == r15_digest:
                reverted = time.monotonic() - returned
                break
            time.sleep(0.1)
        report["negative_control_revert_seconds"] = (
            None if reverted is None else round(reverted, 3)
        )
        self.record_assertion(
            "Owner ExternalSecret reverted the rotation within 3 s",
            reverted is not None,
            "" if reverted is None else f"{reverted:.3f}s",
        )
        self.kubectl(
            "-n",
            ROTATION_NAMESPACE,
            "delete",
            "externalsecret",
            ROTATION_OWNER_EXTERNAL_SECRET,
            "--ignore-not-found",
            action="delete Owner ExternalSecret control",
        )

    def build_rotation_apply(self) -> pathlib.Path:
        target_dir = self.repo_root / "cli/target/provider-harness"
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(target_dir)
        self.runner.run(
            [
                "cargo",
                "build",
                "--locked",
                "--manifest-path",
                str(self.snapshot / "cli/Cargo.toml"),
                "--example",
                "rotation_apply",
            ],
            "build rotation apply driver",
            env=environment,
            timeout=1800,
        )
        binary = target_dir / "debug/examples/rotation_apply"
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise HarnessError("rotation apply driver was not built")
        return binary

    def setup_rotation_namespace(self) -> None:
        self.apply(
            yaml_document(
                {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ROTATION_NAMESPACE}}
            ),
            "create rotation namespace",
            namespace=None,
        )
        rbac_path = (
            self.snapshot / "cli/tests/fixtures/provider-bundle/manifests/rotation-rbac.yaml"
        )
        self.apply(
            rbac_path.read_bytes(), "apply rotation namespace RBAC", namespace=ROTATION_NAMESPACE
        )
        provider = self.apply_emulator_credentials(ROTATION_NAMESPACE)
        store = {
            "apiVersion": "external-secrets.io/v1",
            "kind": "SecretStore",
            "metadata": {"name": "acme-harness"},
            "spec": {"provider": provider},
        }
        self.apply(yaml_document(store), "apply rotation SecretStore", namespace=ROTATION_NAMESPACE)
        wait_until(
            "rotation SecretStore Ready=True",
            lambda: self.resource_ready("secretstore", "acme-harness", ROTATION_NAMESPACE),
            timeout=120,
        )

    def resource_ready(self, kind: str, name: str, namespace: str) -> bool:
        result = self.kubectl(
            "-n",
            namespace,
            "get",
            kind,
            name,
            "-o",
            "json",
            action=f"read {kind} readiness",
            allow_failure=True,
        )
        if result.status != 0:
            return False
        document = parse_json(result.stdout, f"{kind} readiness")
        conditions = document.get("status", {}).get("conditions", [])
        return isinstance(conditions, list) and any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        )

    def kubectl_reports_absent(self, kind: str, name: str, namespace: str | None) -> bool:
        args: list[str] = []
        if namespace is not None:
            args.extend(["-n", namespace])
        args.extend(["get", kind, name])
        result = self.kubectl(*args, action=f"verify {kind} absent", allow_failure=True)
        return tool_error_has_code(result, "NotFound")

    def write_rotation_primary(self, name: str, payload: dict[str, str], *, create: bool) -> None:
        document = write_private_file(self.work / "rotation-primary.json", json.dumps(payload))
        self.aws(
            "secretsmanager",
            "create-secret" if create else "put-secret-value",
            "--name" if create else "--secret-id",
            require_owned_secret_path(name),
            "--secret-string",
            f"file://{document}",
            action="write rotation primary entry",
        )

    def delete_rotation_primary(self, name: str) -> None:
        self.aws(
            "secretsmanager",
            "delete-secret",
            "--secret-id",
            require_owned_secret_path(name),
            "--force-delete-without-recovery",
            action="force delete rotation primary entry",
        )
        wait_until(
            "rotation primary entry deletion",
            lambda: self.aws_reports_absent(
                "secretsmanager",
                "describe-secret",
                ["--secret-id", name],
                "verify rotation primary entry absent",
                ["ResourceNotFoundException"],
            ),
            timeout=60,
            interval=0.5,
        )

    def read_provider_json(self, name: str) -> dict[str, Any] | None:
        result = self.aws(
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            require_owned_secret_path(name),
            "--output",
            "json",
            action="read rotation backup",
            allow_failure=True,
            sensitive=True,
        )
        if result.status != 0:
            if tool_error_has_code(result, "ResourceNotFoundException"):
                return None
            raise HarnessError(format_tool_error("read rotation backup", result.status))
        raw = parse_json(result.stdout, "rotation backup").get("SecretString")
        if not isinstance(raw, str):
            raise HarnessError("rotation backup has no string value")
        parsed = parse_json(raw.encode(), "rotation backup value")
        if not isinstance(parsed, dict):
            raise HarnessError("rotation backup value has an invalid shape")
        return parsed

    def rotation_backup_digest(self, name: str) -> str | None:
        parsed = self.read_provider_json(name)
        value = parsed.get(ROTATED_KEY) if parsed is not None else None
        return full_digest(value) if isinstance(value, str) else None

    def rotation_backup_file(self, name: str, filename: str) -> pathlib.Path | None:
        parsed = self.read_provider_json(name)
        if parsed is None:
            return None
        return write_private_file(self.work / filename, json.dumps(parsed))

    def wait_rotation_backup(self, name: str, expected: str, timeout: int) -> None:
        wait_until(
            "rotation PushSecret backup",
            lambda: self.rotation_backup_digest(name) == expected,
            timeout=timeout,
            interval=0.5,
        )
        self.record_assertion("rotation backup reached bootstrap value", True, expected[:12])

    def run_rotation_apply(
        self, binary: pathlib.Path, prefix: str, backup_file: pathlib.Path | None
    ) -> dict[str, str]:
        command = [
            str(binary),
            "--kubeconfig",
            str(self.admin_kubeconfig),
            "--context",
            self.context,
            "--namespace",
            ROTATION_NAMESPACE,
            "--store",
            "acme-harness",
            "--prefix",
            prefix,
            "--logical-name",
            ROTATION_LOGICAL_NAME,
            "--target",
            TARGET_SECRET,
            "--static-key",
            STATIC_KEY,
            "--rotated-key",
            ROTATED_KEY,
            "--refresh",
            "10s",
        ]
        if backup_file is not None:
            command.extend(["--backup-file", str(require_private_file(backup_file))])
        result = self.runner.run(command, "run rotation apply driver", timeout=180)
        return parse_rotation_report(result.stdout, [ROTATED_KEY])

    def rotation_value(self, filename: str) -> tuple[pathlib.Path, str]:
        value = f"synthetic-rotation-{secrets.token_hex(24)}"
        return write_private_file(self.work / filename, value), full_digest(value)

    def exec_rotator(
        self, value_path: pathlib.Path, expected_digest: str, *, bootstrap: bool = False
    ) -> None:
        arguments = [
            "-n",
            ROTATION_NAMESPACE,
            "exec",
            "-i",
            ROTATION_POD,
            "--",
            "python",
            "/app/rotate.py",
        ]
        if bootstrap:
            arguments.append("--bootstrap")
        result = self.kubectl(
            *arguments,
            action="run long lived rotation",
            sensitive=True,
            input_data=require_private_file(value_path).read_bytes(),
            timeout=60,
        )
        observed = result.stdout.decode("ascii", "strict").strip()
        if observed != expected_digest:
            self.record_assertion("long lived rotation digest matches local value", False)

    def rotation_key_digest(self, key: str) -> str | None:
        value = self.decoded_secret_key(TARGET_SECRET, key, ROTATION_NAMESPACE)
        return None if value is None else full_digest(value)

    def wait_rotation_key(self, key: str, expected: str, action: str) -> None:
        wait_until(action, lambda: self.rotation_key_digest(key) == expected, timeout=180)
        self.record_assertion(action, True, expected[:12])

    def deploy_rotation_workload(self) -> None:
        image = self.image_records["connector"]["tag"]
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": ROTATION_WORKLOAD},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": ROTATION_WORKLOAD}},
                "template": {
                    "metadata": {"labels": {"app": ROTATION_WORKLOAD}},
                    "spec": {
                        "containers": [
                            {
                                "name": "workload",
                                "image": image,
                                "imagePullPolicy": "Never",
                                "command": ["python", "-c", "import time; time.sleep(3600)"],
                                "env": [
                                    {
                                        "name": key,
                                        "valueFrom": {
                                            "secretKeyRef": {"name": TARGET_SECRET, "key": key}
                                        },
                                    }
                                    for key in (STATIC_KEY, ROTATED_KEY)
                                ],
                            }
                        ]
                    },
                },
            },
        }
        self.apply(
            yaml_document(deployment), "deploy rotation workload", namespace=ROTATION_NAMESPACE
        )
        self.kubectl(
            "-n",
            ROTATION_NAMESPACE,
            "rollout",
            "status",
            f"deployment/{ROTATION_WORKLOAD}",
            "--timeout=180s",
            action="wait for rotation workload",
        )

    def delete_cleanup_target(
        self, target: CleanupTarget, *, real_cluster_already_attempted: bool
    ) -> None:
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
                    lambda: self.aws_reports_absent(
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
        elif target.kind == "iam_oidc_provider":
            oidc_arn = f"arn:aws:iam::{self.account_id}:oidc-provider/{target.identity}"
            self.aws(
                "iam",
                "delete-open-id-connect-provider",
                "--open-id-connect-provider-arn",
                oidc_arn,
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
        elif target.kind == "kind_cluster" and not real_cluster_already_attempted:
            self.runner.run(
                [
                    "kind",
                    "delete",
                    "cluster",
                    "--name",
                    target.identity,
                    "--kubeconfig",
                    str(self.admin_kubeconfig),
                ],
                "delete owned kind cluster",
                allow_failure=True,
                timeout=180,
            )

    def cleanup(self) -> None:
        assert self.ledger is not None
        with self.cleanup_log.open("ab") as log:
            log.write(b"cleanup start\n")
            log.flush()
            os.fsync(log.fileno())
        targets = self.ledger.cleanup_targets()
        failures: list[str] = []
        cluster_target = next(
            (
                target
                for target in targets
                if target.kind == "kind_cluster" and target.identity == self.cluster
            ),
            None,
        )

        def collect_failure(scope: str, exc: Exception) -> None:
            detail = str(exc) if isinstance(exc, HarnessError) else "unexpected failure"
            failures.append(f"{scope}: {detail}")

        try:
            if self.real_aws and cluster_target is not None:
                cluster_name = cluster_target.identity
                try:
                    self.runner.run(
                        [
                            "kind",
                            "delete",
                            "cluster",
                            "--name",
                            cluster_name,
                            "--kubeconfig",
                            str(self.admin_kubeconfig),
                        ],
                        "delete owned kind cluster before AWS cleanup",
                        timeout=180,
                    )
                    wait_until(
                        "owned kind cluster deletion before AWS cleanup",
                        lambda: cluster_name not in self.current_clusters(),
                        timeout=180,
                    )
                except Exception as exc:
                    collect_failure("owned kind cluster cleanup", exc)
            for target in targets:
                real_cluster_already_attempted = (
                    self.real_aws and cluster_target is not None and target.id == cluster_target.id
                )
                try:
                    self.delete_cleanup_target(
                        target,
                        real_cluster_already_attempted=real_cluster_already_attempted,
                    )
                except Exception as exc:
                    collect_failure(f"{target.kind} cleanup", exc)
        finally:
            for process in reversed(self.background):
                try:
                    ToolRunner._stop_group(process)
                except Exception as exc:
                    collect_failure("background process cleanup", exc)
            self.background.clear()
        try:
            self.verify_cleanup(targets)
        except Exception as exc:
            collect_failure("cleanup verification", exc)
        if failures:
            raise HarnessError("cleanup failed: " + "; ".join(failures))
        with self.cleanup_log.open("ab") as log:
            log.write(b"cleanup complete\n")
            log.flush()
            os.fsync(log.fileno())

    def verify_cleanup(self, targets: list[CleanupTarget]) -> None:
        failures: list[str] = []

        def verify(label: str, operation: Callable[[], None]) -> None:
            try:
                operation()
            except HarnessError as exc:
                failures.append(f"{label}: {exc}")
            except Exception:
                failures.append(f"{label}: unexpected failure")

        def verify_kind_set() -> None:
            wait_until(
                "kind set mismatch",
                lambda: self.current_clusters() == self.prior_clusters,
                timeout=180,
            )
            self.record_assertion("post run kind set equals exact prior set", True)

        verify("kind set mismatch", verify_kind_set)
        for target in targets:
            if target.kind == "secretsmanager" and self.real_aws:
                verify(
                    "Secrets Manager absence",
                    lambda target=target: wait_until(
                        f"Secrets Manager deletion {target.identity}",
                        lambda: self.aws_reports_absent(
                            "secretsmanager",
                            "describe-secret",
                            ["--secret-id", target.identity],
                            "verify provider entry absent",
                            ["ResourceNotFoundException"],
                        ),
                        timeout=180,
                    ),
                )
            elif target.kind == "iam_role":
                verify(
                    "IAM role absence",
                    lambda target=target: wait_until(
                        f"IAM role deletion {target.identity}",
                        lambda: self.aws_reports_absent(
                            "iam",
                            "get-role",
                            ["--role-name", target.identity],
                            "verify IAM role absent",
                            ["NoSuchEntity"],
                        ),
                        timeout=120,
                    ),
                )
            elif target.kind == "iam_oidc_provider":
                oidc_arn = f"arn:aws:iam::{self.account_id}:oidc-provider/{target.identity}"
                verify(
                    "IAM OIDC provider absence",
                    lambda oidc_arn=oidc_arn: wait_until(
                        "IAM OIDC provider deletion",
                        lambda: self.aws_reports_absent(
                            "iam",
                            "get-open-id-connect-provider",
                            ["--open-id-connect-provider-arn", oidc_arn],
                            "verify IAM OIDC provider absent",
                            ["NoSuchEntity"],
                        ),
                        timeout=120,
                    ),
                )
            elif target.kind == "s3_bucket":
                verify(
                    "S3 bucket absence",
                    lambda target=target: wait_until(
                        f"S3 bucket deletion {target.identity}",
                        lambda: self.aws_reports_absent(
                            "s3api",
                            "head-bucket",
                            ["--bucket", target.identity],
                            "verify OIDC bucket absent",
                            ["404", "NoSuchBucket"],
                        ),
                        timeout=120,
                    ),
                )
        if self.real_aws:
            def verify_tag_inventory() -> None:
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

            verify("AWS tag inventory", verify_tag_inventory)
        if failures:
            raise HarnessError("cleanup verification failed: " + "; ".join(failures))

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
        body = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        write_private_file(self.evidence_root / "evidence.json", body)
        if os.environ.get("CURIE_SECRETS_CLI_ONLY") == "1":
            cli_root = (
                self.repo_root / ".projects/aws-secrets/evidence/aws-sec-secrets-cli" / self.suffix
            )
            cli_root.mkdir(mode=0o700, parents=True, exist_ok=False)
            os.chmod(cli_root, 0o700)
            write_private_file(cli_root / "evidence.json", body)


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
    if args.eso != "none" and not args.real_aws:
        # The emulator run drives the rotation suite, which builds a Rust example.
        tools.add("cargo")
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
    setup_guard = install_signal_handlers()
    try:
        modes = ["preinstalled", "none"] if args.ci else [args.eso or "preinstalled"]
        for mode in modes:
            case_guard = install_signal_handlers()
            try:
                HarnessCase(
                    repo_root,
                    snapshot,
                    commit,
                    seed,
                    mode,
                    args.real_aws,
                    curie_bin,
                ).run()
            finally:
                case_guard.restore()
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
        try:
            try:
                snapshot_context.cleanup()
            finally:
                setup_context.cleanup()
        finally:
            setup_guard.restore()
    safe_print("provider harness: all requested modes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
