#!/usr/bin/env python3
"""Live Slack kickoff acceptance driver for personal factory runs (#5380, ADR 0268 row 13).

Six subcommands share one private SQLite ledger under ``--state-dir``:

1. ``images`` (unpaid) starts a loopback ``registry:2.8.3`` through the caller's
   Docker endpoint, builds the candidate's api, worker, dispatcher and runner
   images and the dark-factory runner layer (from a private bundle copy carrying
   the reservation gate) with the OCI revision label, verifies every one, then
   removes the images and the registry.
2. ``slack-probe`` (unpaid) checks the Curie bot token, the app-level token, the
   sender bot token and the testing channel, and round-trips one sender post that
   it deletes. Codes only are recorded.
3. ``preflight`` (unpaid, offline) refuses ambient credentials, unsafe input
   files, an unapproved ceiling, an insufficient grant lifetime and a candidate
   that is not a local commit.
4. ``rehearse`` (unpaid, zero model sessions) runs every phase except the paid
   kickoff: it enrolls a well-formed token that was never issued, proves Socket
   Mode delivery and declared-driver admission with a ``[test action] ping``,
   runs the disconnected control, then the full finalizer. ``rehearse --kickoff``
   also runs the kickoff and read-back on that never-issued token (no grant
   probe), so the paid path's diagnostics are captured with nothing to spend.
5. ``run`` (also reachable as ``--live``) performs exactly one paid kickoff and
   the disconnected control, then finalizes every owned resource.
6. ``judge --criterion X`` reads the ledger only and prints a verdict.

No held secret (the grant, the three Slack tokens, the App key and token, the
API key, the worker token, the OIDC client secret or the console session) is
ever printed, logged, persisted or placed in a child's argv. Secrets reach
Kubernetes only through ``kubectl create -f -`` stdin. See README.md beside
this file, section "Personal Slack kickoff acceptance (#5380)".
"""

from __future__ import annotations

import argparse
import base64
import collections
import contextlib
import dataclasses
import functools
import hashlib
import http.client
import importlib.util
import json
import os
import queue
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import personal_slack_kickoff_support as sks  # noqa: E402

REPO_ROOT = HERE.parents[1]
FIXTURE_ISSUE = HERE / "fixtures" / "personal-slack-kickoff-issue.md"
OIDC_HARNESS = REPO_ROOT / "tools" / "oidc-e2e" / "oidc_e2e.py"
SUBCOMMANDS = ("images", "slack-probe", "preflight", "rehearse", "run", "judge")
DOCKERFILES = {
    "curie-api": "apps/api/Dockerfile",
    "curie-worker": "apps/worker/Dockerfile",
    "curie-runner": "runner/Dockerfile",
    "curie-dispatcher": "apps/dispatcher/Dockerfile",
}
BUNDLE_SOURCE = "examples/dark-factory"
REGISTRY_IMAGE = "registry:2.8.3"
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
CLAIM_AGENT_LABEL = "curietech.ai/agent"
POD_CLAIM_LABEL = "sandbox.users.io/curie-admission-claim"
CONTROLLER_NAMESPACE = "agent-sandbox-system"
CONTROLLER_DEPLOYMENT = "agent-sandbox-controller"
CLUSTER_SCOPED_KINDS = (
    "clusterroles,clusterrolebindings,priorityclasses,validatingadmissionpolicies,"
    "validatingadmissionpolicybindings,persistentvolumes"
)
# registry:2 answers an index-tagged manifest only when Accept names the index type.
MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    )
)
GITHUB_API_HOST = "api.github.com"
FORGE_PERMISSIONS = {
    "contents": "write",
    "pull_requests": "write",
    "issues": "write",
    "checks": "read",
    "statuses": "read",
}
MODEL_HOST = "api.anthropic.com"
API_PORT = 8000
POLL_SECONDS = 2.0
# The deadline enforcer's pass interval, and how long its stop waits for it.
DEADLINE_SECONDS = 1.0
DEADLINE_JOIN_SECONDS = 10.0
# The claim-pod follower's pass interval, its one listing's timeout, and how long its
# stop waits for it.
FOLLOW_SECONDS = 1.0
FOLLOW_GET_SECONDS = 20.0
FOLLOW_JOIN_SECONDS = 30.0
# A stream into a runner container that exits silently is respawned this often, this long.
STREAM_RETRY_SECONDS = 0.5
STREAM_RETRY_LIMIT = 120.0
# Diagnostics: each runner log tail, and the worker log read for the escalation reason.
# The read precedes the fence's scale, so its timeout is short: it delays the stop.
RUNNER_LOG_TAIL = 80
WORKER_LOG_TAIL = 400
WORKER_LOG_LINES = 60
WORKER_LOG_SECONDS = 15.0
# Scheduling and the image pull before a runner's first native record.
STARTUP_SECONDS = 600.0
RECEIPT_SECONDS = 90.0
ADMISSION_SECONDS = 180.0
REPLY_SECONDS = 240.0
TERMINAL_POST_SECONDS = 600.0
CONTROL_QUIET_SECONDS = 30.0
TEARDOWN_SECONDS = 600.0
IDENTITY_SECONDS = 300.0
# Node and driver clocks may differ slightly; replayed history is minutes older.
REPLAY_MARGIN_SECONDS = 30.0
SCAN_CHANNELS = ("stdout", "stderr", "children", "http", "harvest", "ledger", "evidence")
USER_AGENT = "curie-personal-slack-kickoff-acceptance"
GRANT_PROBE_PROMPT = b"Reply with the single word ok.\n"
GRANT_PROBE_SECONDS = 120.0
DEFAULT_PROBE_CLI = "claude"
TOKEN_PREFIXES = {"curie_bot": "xoxb-", "app": "xapp-", "sender": "xoxb-"}
SLACK_FLAGS = {
    "curie_bot": "--curie-bot-token-file",
    "app": "--app-token-file",
    "sender": "--sender-token-file",
}
_ISSUE_TITLE = re.compile(r"#\s+(.+)")


class StepFailed(Exception):
    """A live step failed. The message is fixed text naming the step, never output."""


class Clock:
    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    def wait(self, seconds: float) -> None:
        threading.Event().wait(max(0.0, seconds))


# --------------------------------------------------------------------------
# Output, child processes and HTTP: every channel is scanned and scrubbed
# --------------------------------------------------------------------------


class Output:
    """JSON on stdout and progress on stderr, both scanned raw and written scrubbed."""

    def __init__(self, scrubber: sks.Scrubber, scan: sks.TokenScan) -> None:
        self.scrubber = scrubber
        self.scan = scan

    def __repr__(self) -> str:
        return "<output>"

    def emit(self, payload: Mapping[str, Any], *, channel: str = "stdout") -> None:
        raw = json.dumps(payload, sort_keys=True)
        self.scan.scan(channel, raw.encode())
        print(self.scrubber.scrub(raw), file=sys.stdout, flush=True)

    def progress(self, message: str) -> None:
        self.scan.scan("stderr", message.encode())
        print(
            f"[personal-slack-kickoff] {self.scrubber.scrub(message)}", file=sys.stderr, flush=True
        )


@dataclasses.dataclass(frozen=True)
class Completed:
    returncode: int
    stdout: bytes
    stderr: bytes


class Shell:
    """Child processes with the filtered environment; held secrets never reach argv."""

    def __init__(
        self,
        *,
        env: Mapping[str, str],
        scrubber: sks.Scrubber,
        scan: sks.TokenScan,
        cwd: Path,
        log: Path | None = None,
    ) -> None:
        self.env = dict(env)
        self.scrubber = scrubber
        self.scan = scan
        self.cwd = cwd
        self.log = log

    def __repr__(self) -> str:
        return "<shell>"

    def guard(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        extra_env: Mapping[str, str] | None = None,
        secret_env: Sequence[str] = (),
        secret_stdin: bool = False,
        clean_env: bool = False,
    ) -> None:
        env = dict(extra_env or {}) if clean_env else {**self.env, **(extra_env or {})}
        for secret in self.scrubber.held():
            raw = secret.reveal()
            if not raw:
                continue
            if any(raw in arg for arg in argv):
                raise StepFailed("a held secret would reach a child argument")
            if any(raw in value for key, value in env.items() if key not in secret_env):
                raise StepFailed("a held secret would reach a child environment")
            if stdin is not None and not secret_stdin and raw.encode() in stdin:
                raise StepFailed("a held secret would reach a child input")

    def record(self, argv: Sequence[str], result: Completed, *, sensitive: bool = False) -> None:
        """Append one scrubbed entry (argv head, exit, stderr tail) to the private command log."""

        if self.log is None:
            return
        entry: dict[str, Any] = {
            "argv": [self.scrubber.scrub(arg) for arg in argv[:6]],
            "exit": result.returncode,
            "stderr_tail": ""
            if sensitive
            else self.scrubber.scrub(result.stderr[-800:].decode(errors="replace")),
        }
        descriptor = os.open(
            self.log, os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "a") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")

    def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        timeout: float = 120.0,
        check: bool = True,
        extra_env: Mapping[str, str] | None = None,
        secret_env: Sequence[str] = (),
        secret_stdin: bool = False,
        sensitive_output: bool = False,
        clean_env: bool = False,
        cwd: Path | None = None,
    ) -> Completed:
        """``clean_env`` gives the child only ``extra_env``, nothing inherited."""

        self.guard(
            argv,
            stdin=stdin,
            extra_env=extra_env,
            secret_env=secret_env,
            secret_stdin=secret_stdin,
            clean_env=clean_env,
        )
        try:
            completed = subprocess.run(
                list(argv),
                input=stdin,
                stdin=subprocess.DEVNULL if stdin is None else None,
                capture_output=True,
                timeout=timeout,
                cwd=cwd or self.cwd,
                env=dict(extra_env or {}) if clean_env else {**self.env, **(extra_env or {})},
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise StepFailed(f"{Path(argv[0]).name} could not complete") from None
        result = Completed(completed.returncode, completed.stdout, completed.stderr)
        if not sensitive_output:
            self.scan.scan("children", result.stdout)
            self.scan.scan("children", result.stderr)
        self.record(argv, result, sensitive=sensitive_output)
        if check and result.returncode != 0:
            verb = argv[1] if len(argv) > 1 else ""
            raise StepFailed(f"{Path(argv[0]).name} {verb} exited {result.returncode}")
        return result

    def spawn(self, argv: Sequence[str]) -> subprocess.Popen[bytes]:
        self.guard(argv)
        return subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.cwd,
            env=dict(self.env),
        )


def stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


class JsonHttp:
    """JSON over http.client with no proxy; responses are scanned unless sensitive."""

    def __init__(
        self, host: str, port: int, *, tls: bool, scan: sks.TokenScan, timeout: float = 30.0
    ) -> None:
        self.host = host
        self.port = port
        self.tls = tls
        self.scan = scan
        self.timeout = timeout

    def __repr__(self) -> str:
        return "<http>"

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        sensitive: bool = False,
    ) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode()
        connection: http.client.HTTPConnection = (
            http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout)
            if self.tls
            else http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        )
        sent = {"User-Agent": USER_AGENT, **(headers or {})}
        if data is not None:
            sent["Content-Type"] = "application/json"
        try:
            connection.request(method, path, body=data, headers=sent)
            response = connection.getresponse()
            raw = response.read(32 << 20)
            status = response.status
        except (OSError, http.client.HTTPException):
            raise StepFailed(f"{method} request could not complete") from None
        finally:
            connection.close()
        if not sensitive:
            self.scan.scan("http", raw)
        if not raw:
            return status, None
        try:
            return status, json.loads(raw)
        except ValueError:
            return status, None


def slack_https_transport(
    path: str, headers: Mapping[str, str], body: bytes
) -> tuple[int, Mapping[str, str], bytes]:
    """POST one Slack Web API method (https://docs.slack.dev/apis/web-api/)."""

    connection = http.client.HTTPSConnection(sks.SLACK_HOST, 443, timeout=30)
    try:
        connection.request(
            "POST", path, body=body, headers={"User-Agent": USER_AGENT, **dict(headers)}
        )
        response = connection.getresponse()
        raw = response.read(8 << 20)
        return response.status, {k.lower(): v for k, v in response.getheaders()}, raw
    except (OSError, http.client.HTTPException):
        raise StepFailed("a Slack request could not complete") from None
    finally:
        connection.close()


# --------------------------------------------------------------------------
# Finalizer and signals
# --------------------------------------------------------------------------


class Finalizer:
    """LIFO cleanup registered before each resource is created. Every step runs."""

    def __init__(self, ledger: sks.Ledger | None) -> None:
        self.ledger = ledger
        self.steps: list[tuple[str, Callable[[], None]]] = []

    def __repr__(self) -> str:
        return "<finalizer>"

    def register(self, name: str, step: Callable[[], None]) -> None:
        self.steps.append((name, step))

    def run(self) -> list[tuple[str, bool]]:
        results: list[tuple[str, bool]] = []
        while self.steps:
            name, step = self.steps.pop()
            detail = ""
            try:
                step()
                ok = True
            except BaseException as exc:  # noqa: BLE001 - every registered step runs
                ok = False
                detail = str(exc) if isinstance(exc, StepFailed) else type(exc).__name__
            results.append((name, ok))
            if self.ledger is not None:
                self.ledger.record_cleanup(f"step:{name}", name, "ok" if ok else "failed", detail)
        return results


@contextlib.contextmanager
def signals_to_exit() -> Iterator[None]:
    """SIGINT and SIGTERM become SystemExit so the finalizer runs."""

    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def handler(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


@contextlib.contextmanager
def signals_deferred() -> Iterator[list[int]]:
    """While cleanup runs, a further signal is recorded instead of interrupting it."""

    received: list[int] = []
    if threading.current_thread() is not threading.main_thread():
        yield received
        return

    def handler(signum: int, _frame: object) -> None:
        received.append(signum)

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield received
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


# --------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="personal_slack_kickoff.py",
        description="Live Slack kickoff acceptance driver for personal factory runs (#5380).",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in SUBCOMMANDS:
        sub = commands.add_parser(name)
        sub.add_argument("--candidate", default="")
        sub.add_argument("--state-dir", default="")
        sub.add_argument("--run-id", default="")
        if name in {"images", "rehearse", "run"}:
            for flag in ("--context", "--docker-context", "--registry-port", "--kubeconfig"):
                sub.add_argument(flag, default="")
        if name in {"rehearse", "run"}:
            for flag in ("--namespace", "--release", "--priority-classes"):
                sub.add_argument(flag, default="")
        if name in {"preflight", "rehearse", "run"}:
            sub.add_argument("--app-dir", default="")
        if name in {"slack-probe", "preflight", "rehearse", "run"}:
            for flag in (*SLACK_FLAGS.values(), "--channel-file", "--isolation-file"):
                sub.add_argument(flag, default="")
        if name in {"preflight", "rehearse", "run"}:
            # A rehearsal takes no grant or approval at all; None tells "supplied" from "absent".
            absent = None if name == "rehearse" else ""
            sub.add_argument("--grant-file", default=absent)
            sub.add_argument("--grant-expires-at", default=absent)
            sub.add_argument("--approve-live", default=None)
        if name in {"preflight", "run"}:
            sub.add_argument("--grant-probe-cli", default=DEFAULT_PROBE_CLI)
        if name == "rehearse":
            # The kickoff and read-back on the rehearsal's own never-issued token.
            sub.add_argument("--kickoff", action="store_true")
        if name == "run":
            # The item's ledger across attempts: its cap and what earlier attempts were
            # charged, both from the caller's approval.
            sub.add_argument("--item-cap", type=int, required=True)
            sub.add_argument("--item-charged-before", type=int, required=True)
        if name == "judge":
            sub.add_argument("--criterion", required=True, choices=[*sks.CRITERIA, "all"])
    return parser


@dataclasses.dataclass(frozen=True)
class SlackFiles:
    curie_bot: Path
    app: Path
    sender: Path
    channel: str
    isolation: sks.Isolation


@dataclasses.dataclass(frozen=True)
class Inputs:
    candidate: str
    run_id: str
    state_dir: Path
    context: str = ""
    docker_context: str = ""
    kubeconfig: str = ""
    registry_port: int = 0
    namespace: str = ""
    release: str = ""
    priority_classes: tuple[str, str] | None = None
    app_dir: Path | None = None
    app: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    slack: SlackFiles | None = None
    grant_file: Path | None = None
    expires_at: float = 0.0
    grant_probe_cli: str = DEFAULT_PROBE_CLI

    @property
    def registry(self) -> str:
        return f"localhost:{self.registry_port}"

    @property
    def grant_namespace(self) -> str:
        return f"{self.namespace}-grants"

    @property
    def work_dir(self) -> Path:
        return self.state_dir / f"{self.run_id}.work"

    @property
    def command_log(self) -> Path:
        return self.state_dir / f"{self.run_id}.commands.jsonl"

    @property
    def fullname(self) -> str:
        return sks.chart_fullname(self.release)


def base_inputs(args: argparse.Namespace) -> Inputs:
    candidate = sks.require_candidate(args.candidate)
    run_id = sks.require_run_id(args.run_id)
    if not args.state_dir:
        raise sks.Refusal("state_dir_unsafe", "--state-dir")
    state_dir = sks.require_private_dir(Path(args.state_dir))
    return Inputs(candidate=candidate, run_id=run_id, state_dir=state_dir)


def with_cluster(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    if not args.context:
        raise sks.Refusal("arguments_invalid", "--context")
    if not args.docker_context:
        raise sks.Refusal("arguments_invalid", "--docker-context")
    port = sks.require_registry_port(args.registry_port)
    _address, port = sks.require_registry(f"localhost:{port}")
    kubeconfig = ""
    if args.kubeconfig:
        if not Path(args.kubeconfig).is_file():
            raise sks.Refusal("arguments_invalid", "--kubeconfig")
        kubeconfig = str(Path(args.kubeconfig))
    return dataclasses.replace(
        inputs,
        context=args.context,
        docker_context=args.docker_context,
        kubeconfig=kubeconfig,
        registry_port=port,
    )


def with_install(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    # Room for the "-grants" suffix of the dedicated custody namespace.
    namespace = sks.require_dns_name(args.namespace, "--namespace", limit=56)
    release = sks.require_dns_name(sks.require_release(args.release), "--release", limit=30)
    classes: tuple[str, str] | None = None
    if args.priority_classes:
        names = [
            sks.require_dns_name(item.strip(), "--priority-classes", limit=63)
            for item in args.priority_classes.split(",")
        ]
        if len(names) != 2:
            raise sks.Refusal("arguments_invalid", "--priority-classes")
        classes = (names[0], names[1])
    return dataclasses.replace(
        inputs, namespace=namespace, release=release, priority_classes=classes
    )


def with_app(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    if not args.app_dir:
        raise sks.Refusal("app_kit_invalid", "--app-dir")
    app_dir = Path(args.app_dir)
    return dataclasses.replace(inputs, app_dir=app_dir, app=sks.require_app_kit(app_dir))


def with_slack(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    paths: dict[str, Path] = {}
    for key, flag in SLACK_FLAGS.items():
        raw = getattr(args, flag.removeprefix("--").replace("-", "_"))
        path = Path(raw) if raw else None
        sks.check_token_file(path, flag=flag, prefix=TOKEN_PREFIXES[key])
        if path is None:
            raise sks.Refusal("slack_kit_invalid", flag)
        paths[key] = path
    if paths["curie_bot"].resolve() == paths["sender"].resolve():
        raise sks.Refusal("slack_kit_invalid", "the sender must be another app's bot token")
    channel = sks.load_channel(Path(args.channel_file) if args.channel_file else None)
    isolation = sks.load_isolation(Path(args.isolation_file) if args.isolation_file else None)
    return dataclasses.replace(
        inputs,
        slack=SlackFiles(paths["curie_bot"], paths["app"], paths["sender"], channel, isolation),
    )


def with_grant(inputs: Inputs, args: argparse.Namespace, now: float) -> Inputs:
    grant_file = Path(args.grant_file) if args.grant_file else None
    sks.check_grant_file(grant_file)
    expires_at = sks.parse_expiry(args.grant_expires_at)
    sks.require_lifetime(expires_at, now)
    return dataclasses.replace(inputs, grant_file=grant_file, expires_at=expires_at)


def with_probe(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    cli = str(args.grant_probe_cli or "")
    if not cli or any(ch.isspace() for ch in cli):
        raise sks.Refusal("arguments_invalid", "--grant-probe-cli")
    return dataclasses.replace(inputs, grant_probe_cli=cli)


def private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def extract_archive(archive: Path, destination: Path) -> None:
    with tarfile.open(archive) as bundle:
        bundle.extractall(destination, filter="data")


def write_private(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def short_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:8]


# --------------------------------------------------------------------------
# Adapters: kubectl, curie, the api, the forge, Slack, the registry
# --------------------------------------------------------------------------


class Kube:
    def __init__(self, shell: Shell, context: str, namespace: str) -> None:
        self.shell = shell
        self.context = context
        self.namespace = namespace

    def argv(self, *args: str, namespaced: bool = True) -> list[str]:
        argv = ["kubectl", "--context", self.context]
        if namespaced:
            argv += ["--namespace", self.namespace]
        return [*argv, *args]

    def ctl(self, *args: str, namespaced: bool = True, **kwargs: Any) -> Completed:
        return self.shell.run(self.argv(*args, namespaced=namespaced), **kwargs)

    def start(self, *args: str) -> subprocess.Popen[bytes]:
        """Start a namespaced kubectl command without waiting on it."""

        return self.shell.spawn(self.argv(*args))

    def json(self, *args: str, namespaced: bool = True, **kwargs: Any) -> Any:
        result = self.ctl(*args, "-o", "json", namespaced=namespaced, **kwargs)
        try:
            return json.loads(result.stdout)
        except ValueError:
            raise StepFailed("kubectl returned unreadable JSON") from None

    def create(self, manifest: Mapping[str, Any], *, secret: bool = False) -> None:
        self.ctl(
            "create",
            "-f",
            "-",
            stdin=json.dumps(manifest).encode(),
            secret_stdin=secret,
            sensitive_output=secret,
        )


class CurieCli:
    def __init__(self, shell: Shell, binary: str, inputs: Inputs) -> None:
        self.shell = shell
        self.binary = binary
        self.inputs = inputs

    def argv(self, verb: str, *args: str) -> list[str]:
        return [
            self.binary,
            "cluster",
            "--context",
            self.inputs.context,
            verb,
            *args,
            "--namespace",
            self.inputs.namespace,
            "--release",
            self.inputs.release,
            "--json",
        ]

    def run(self, verb: str, *args: str, **kwargs: Any) -> Completed:
        return self.shell.run(self.argv(verb, *args), **kwargs)

    def json(self, verb: str, *args: str, **kwargs: Any) -> Any:
        result = self.run(verb, *args, **kwargs)
        try:
            return json.loads(result.stdout)
        except ValueError:
            raise StepFailed(f"curie cluster {verb} returned unreadable JSON") from None


class CurieApi:
    """The api through the run's own port-forward, with the release's generated platform key."""

    def __init__(self, http: JsonHttp, api_key: sks.HeldSecret) -> None:
        self.http = http
        self.api_key = api_key

    def __repr__(self) -> str:
        return "<curie api>"

    def get(self, path: str) -> Any:
        status, body = self.http.request("GET", path, headers={"X-API-Key": self.api_key.reveal()})
        if status != 200:
            raise StepFailed(f"api GET returned {status}")
        return body


class Forge:
    """The fixture repository through a GitHub App installation token (never a user login)."""

    def __init__(
        self,
        shell: Shell,
        http: JsonHttp,
        app_dir: Path,
        app: Mapping[str, Any],
        scrubber: sks.Scrubber,
        clock: Clock,
    ) -> None:
        self.shell = shell
        self.http = http
        self.app_dir = app_dir
        self.repo = str(app["repo"])
        self.app_id = str(app["id"])
        self.installation_id = str(app["installation_id"])
        self.scrubber = scrubber
        self.clock = clock
        self.token: sks.HeldSecret | None = None
        self.expires_at = 0.0

    def __repr__(self) -> str:
        return "<forge>"

    def jwt(self) -> sks.HeldSecret:
        def segment(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

        issued = int(self.clock.now())
        header = segment(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
        claims = {"iat": issued - 60, "exp": issued + 540, "iss": self.app_id}
        payload = segment(json.dumps(claims, separators=(",", ":")).encode())
        signed = self.shell.run(
            ["openssl", "dgst", "-sha256", "-sign", str(self.app_dir / "app.pem")],
            stdin=f"{header}.{payload}".encode(),
            sensitive_output=True,
        )
        return self.scrubber.hold(sks.HeldSecret(f"{header}.{payload}.{segment(signed.stdout)}"))

    def mint(self) -> None:
        jwt = self.jwt()
        name = self.repo.split("/", 1)[1]
        status, body = self.http.request(
            "POST",
            f"/app/installations/{self.installation_id}/access_tokens",
            headers=self._headers(jwt),
            body={"repositories": [name], "permissions": FORGE_PERMISSIONS},
            sensitive=True,
        )
        jwt.clear()
        token = body.get("token") if isinstance(body, dict) else None
        granted = body.get("permissions") if isinstance(body, dict) else None
        if status != 201 or not isinstance(token, str) or not isinstance(granted, dict):
            raise StepFailed(f"installation token request returned {status}")
        if any(granted.get(key) != level for key, level in FORGE_PERMISSIONS.items()):
            raise StepFailed("installation token permissions differ from the requested set")
        if self.token is not None:
            self.token.clear()
        self.token = self.scrubber.hold(sks.HeldSecret(token))
        self.expires_at = self.clock.now() + 3000

    @staticmethod
    def _headers(credential: sks.HeldSecret, scheme: str = "Bearer") -> dict[str, str]:
        return {
            "Authorization": f"{scheme} {credential.reveal()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if self.token is None or not self.token.reveal() or self.clock.now() > self.expires_at:
            self.mint()
        token = self.token
        if token is None:
            raise StepFailed("installation token is not available")
        return self.http.request(method, path, headers=self._headers(token, "token"), body=body)

    def call(self, method: str, path: str, body: Any = None, *, ok: Sequence[int] = (200,)) -> Any:
        status, payload = self.request(method, path, body)
        if status not in ok:
            raise StepFailed(f"forge {method} returned {status}")
        return payload

    def repo_path(self, suffix: str) -> str:
        return f"/repos/{self.repo}{suffix}"

    def revoke(self) -> None:
        if self.token is not None and self.token.reveal():
            self.http.request(
                "DELETE", "/installation/token", headers=self._headers(self.token, "token")
            )
            self.token.clear()


def forge_http(scan: sks.TokenScan) -> JsonHttp:
    return JsonHttp(GITHUB_API_HOST, 443, tls=True, scan=scan)


@dataclasses.dataclass
class SlackKit:
    """The three Slack clients and the identities their ``auth.test`` reported."""

    curie: sks.SlackWeb
    app: sks.SlackWeb
    sender: sks.SlackWeb
    channel: str
    curie_auth: sks.SlackAuth
    sender_auth: sks.SlackAuth

    def __repr__(self) -> str:
        return "<slack kit>"

    def clear(self) -> None:
        for client in (self.curie, self.app, self.sender):
            client.token.clear()


def open_slack(files: SlackFiles, runtime: Runtime, ledger: sks.Ledger) -> SlackKit:
    """Load each token into a held secret, authenticate it, and bind the isolation file.

    The Curie bot must be the confirmed app's bot user in the confirmed
    workspace, the sender another app's bot in the same workspace, the
    app-level token must open a Socket Mode connection URL (never dialed), and
    both bots must be members of the testing channel.
    """

    def client(path: Path, flag: str, prefix: str) -> sks.SlackWeb:
        token = runtime.scrubber.hold(sks.load_token(path, flag=flag, prefix=prefix))
        transport = runtime.slack_transport or slack_https_transport
        return sks.SlackWeb(transport, token, runtime.scan, wait=runtime.clock.wait)

    curie = client(files.curie_bot, SLACK_FLAGS["curie_bot"], TOKEN_PREFIXES["curie_bot"])
    app = client(files.app, SLACK_FLAGS["app"], TOKEN_PREFIXES["app"])
    sender = client(files.sender, SLACK_FLAGS["sender"], TOKEN_PREFIXES["sender"])
    try:
        curie_auth = curie.auth()
        ledger.record("slack", "curie_auth_ok", True)
        sender_auth = sender.auth()
        ledger.record("slack", "sender_auth_ok", True)
        if (
            curie_auth.team_id != files.isolation.team_id
            or curie_auth.user_id != files.isolation.curie_bot_user_id
        ):
            ledger.record("slack", "isolation_confirmed", False)
            raise sks.Refusal("isolation_unconfirmed", "the Curie bot token names another app")
        ledger.record("slack", "isolation_confirmed", True)
        same_team = sender_auth.team_id == curie_auth.team_id
        distinct = (
            sender_auth.user_id != curie_auth.user_id and sender_auth.bot_id != curie_auth.bot_id
        )
        ledger.record("slack", "same_team", same_team)
        ledger.record("slack", "sender_distinct", distinct)
        if not same_team or not distinct:
            raise sks.Refusal(
                "slack_kit_invalid", "the sender must be another bot in the workspace"
            )
        app_ok = app.connections_open()
        ledger.record("slack", "app_token_ok", app_ok)
        curie_member = curie.is_member(files.channel)
        sender_member = sender.is_member(files.channel)
        ledger.record("slack", "curie_in_channel", curie_member)
        ledger.record("slack", "sender_in_channel", sender_member)
        if not (app_ok and curie_member and sender_member):
            raise sks.Refusal("slack_unavailable", "app token or channel membership")
        ledger.record(
            "slack",
            "sender_driver",
            {
                "channel_id": files.channel,
                "bot_id": sender_auth.bot_id,
                "bot_user_id": sender_auth.user_id,
            },
        )
    except sks.SlackError as error:
        ledger.record("slack", "slack_error", {"method": error.method, "code": error.code})
        raise sks.Refusal("slack_unavailable", f"{error.method} {error.code}") from None
    return SlackKit(curie, app, sender, files.channel, curie_auth, sender_auth)


class LoopbackRegistry:
    """The run's own ``registry:2.8.3`` on 127.0.0.1 of the caller's Docker endpoint."""

    def __init__(self, shell: Shell, port: int, run_id: str, ledger: sks.Ledger) -> None:
        self.shell = shell
        self.port = port
        self.name = f"psk-registry-{short_digest(run_id)}"
        self.ledger = ledger
        self.get = sks.loopback_get("127.0.0.1", port, timeout=5)

    def answering(self) -> bool:
        try:
            status, _headers, _body = self.get("/v2/", {})
        except (OSError, http.client.HTTPException):
            return False
        return status == 200

    def listed(self) -> bool:
        result = self.shell.run(
            ["docker", "ps", "-a", "--filter", f"name=^/{self.name}$", "--format", "{{.ID}}"],
            timeout=60,
        )
        return bool(result.stdout.strip())

    def start(self, clock: Clock) -> None:
        if self.listed():
            raise StepFailed("a registry container with this run's name already exists")
        self.ledger.record("images", "registry_container", self.name)
        self.shell.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.name,
                "--label",
                f"{sks.MANAGED_BY_LABEL}={sks.MANAGED_BY_VALUE}",
                "-p",
                f"127.0.0.1:{self.port}:5000",
                "-e",
                "REGISTRY_STORAGE_DELETE_ENABLED=true",
                REGISTRY_IMAGE,
            ],
            timeout=300,
        )
        deadline = clock.monotonic() + 60
        while not self.answering():
            if clock.monotonic() >= deadline:
                raise StepFailed("the loopback registry is not reachable on 127.0.0.1")
            clock.wait(1.0)

    def remove(self) -> None:
        if self.listed():
            self.shell.run(["docker", "rm", "-f", "-v", self.name], timeout=120)
        if self.listed():
            raise StepFailed("the registry container still exists")


def registry_delete(port: int, repository: str, digest: str) -> int:
    """``DELETE /v2/<name>/manifests/<digest>`` (Registry HTTP API v2; deletes enabled)."""

    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        connection.request("DELETE", f"/v2/{repository}/manifests/{digest}")
        response = connection.getresponse()
        response.read()
        return response.status
    finally:
        connection.close()


def curie_binary(environ: Mapping[str, str]) -> str:
    return environ.get("CURIE_BIN") or str(REPO_ROOT / "cli" / "target" / "debug" / "curie")


def lock_runner_image(text: str) -> str | None:
    """``runner.image`` from a ``connectors.lock.yaml`` (cli/src/connector_build.rs shape)."""

    in_runner = False
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t")):
            in_runner = line.rstrip() == "runner:"
            continue
        if in_runner:
            match = re.fullmatch(r"\s+image:\s*['\"]?([^'\"\s]+)['\"]?\s*", line)
            if match:
                return match[1]
    return None


def docker_listed_tags(stdout: bytes) -> list[str]:
    return [
        line
        for line in stdout.decode(errors="replace").split()
        if ":" in line and not line.endswith(":<none>")
    ]


# --------------------------------------------------------------------------
# images: candidate images and the private bundle's runner layer
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Pushed:
    name: str
    repository: str
    reference: str
    digest: str


class ImageBuilder:
    def __init__(
        self,
        *,
        inputs: Inputs,
        shell: Shell,
        ledger: sks.Ledger,
        finalizer: Finalizer,
        curie_bin: str,
        gate_limits: Mapping[str, int],
    ) -> None:
        self.inputs = inputs
        self.shell = shell
        self.ledger = ledger
        self.finalizer = finalizer
        self.curie_bin = curie_bin
        self.gate_limits = dict(gate_limits)
        self.get = sks.loopback_get("127.0.0.1", inputs.registry_port)
        self.pushed: list[Pushed] = []
        self.local_tags: list[str] = []

    def platform(self) -> str:
        nodes = Kube(self.shell, self.inputs.context, "").json("get", "nodes", namespaced=False)
        arches = {
            str(((item.get("status") or {}).get("nodeInfo") or {}).get("architecture") or "")
            for item in nodes.get("items") or []
            if isinstance(item, dict)
        }
        if len(arches) != 1 or "" in arches:
            raise StepFailed("cluster nodes do not report exactly one architecture")
        return f"linux/{arches.pop()}"

    def remember(self) -> None:
        self.ledger.record("images", "local_tags", self.local_tags)
        self.ledger.record("images", "pushed", [dataclasses.asdict(p) for p in self.pushed])

    def build(self) -> dict[str, Any]:
        inputs = self.inputs
        work = private_dir(inputs.work_dir)
        source = work / "source"
        archive = work / "source.tar"
        self.finalizer.register("remove-source-tree", lambda: shutil.rmtree(source, True))
        self.finalizer.register("remove-source-tar", lambda: archive.unlink(missing_ok=True))
        self.shell.run(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "archive",
                "--format=tar",
                "-o",
                str(archive),
                inputs.candidate,
            ],
            timeout=600,
        )
        source.mkdir(mode=0o700, exist_ok=True)
        extract_archive(archive, source)
        platform = self.platform()
        tag = sks.image_tag(inputs.candidate, inputs.run_id)
        for name, dockerfile in DOCKERFILES.items():
            ref = f"{inputs.registry}/{name}:{tag}"
            self.local_tags.append(ref)
            self.remember()
            self.shell.run(
                [
                    "docker",
                    "build",
                    "--platform",
                    platform,
                    *[
                        arg
                        for label in sks.candidate_labels(inputs.candidate, tag)
                        for arg in ("--label", label)
                    ],
                    "-f",
                    str(source / dockerfile),
                    "-t",
                    ref,
                    str(source),
                ],
                timeout=3600,
            )
            self.shell.run(["docker", "push", ref], timeout=1800)
            image = sks.registry_image(self.get, name, tag)
            if image is not None:
                self.pushed.append(Pushed(name, name, tag, image.digest))
                self.remember()
            if image is None or image.revision != inputs.candidate:
                raise sks.Refusal("candidate_image_mismatch", name)
        bundle = work / "bundle"
        shutil.rmtree(bundle, ignore_errors=True)
        shutil.copytree(source / BUNDLE_SOURCE, bundle, symlinks=True)
        hooks_path = bundle / "hooks" / "hooks.json"
        hooks_path.write_text(
            json.dumps(
                sks.private_bundle_hooks(json.loads(hooks_path.read_text()), self.gate_limits),
                indent=2,
            )
            + "\n"
        )
        (bundle / "hooks" / sks.RESERVATION_GATE_FILE).write_text(sks.RESERVATION_GATE_PROGRAM)
        self.ledger.record("images", "gate_limits", self.gate_limits)
        manifest = json.loads((bundle / ".claude-plugin" / "plugin.json").read_text())
        repository = f"{inputs.registry}/{manifest.get('name')}-runner"
        self.ledger.record("images", "layer_repository", repository)
        layer_file = bundle / "runner.Dockerfile"
        layer_file.write_text(sks.run_layer_dockerfile(layer_file.read_text(), tag))
        layer = self.build_layer(bundle, platform, tag)
        if layer.revision != inputs.candidate:
            layer_file.write_text(
                layer_file.read_text() + f"\nLABEL {sks.REVISION_LABEL}={inputs.candidate}\n"
            )
            layer = self.build_layer(bundle, platform, tag)
        if layer.revision != inputs.candidate:
            raise sks.Refusal("candidate_image_mismatch", sks.LAYER_IMAGE)
        shutil.rmtree(source, ignore_errors=True)
        archive.unlink(missing_ok=True)
        return self.verify()

    def build_layer(self, bundle: Path, platform: str, tag: str) -> Any:
        self.shell.run(
            [
                self.curie_bin,
                "build",
                "--plugin-dir",
                str(bundle),
                "--registry",
                self.inputs.registry,
                "--runner-image",
                f"{self.inputs.registry}/curie-runner:{tag}",
                "--platform",
                platform,
            ],
            timeout=3600,
        )
        lock = bundle / "connectors.lock.yaml"
        layer_ref = lock_runner_image(lock.read_text()) if lock.is_file() else None
        if not layer_ref:
            raise sks.Refusal("candidate_image_mismatch", sks.LAYER_IMAGE)
        self.ledger.record("images", "layer_ref", layer_ref)
        repository, reference = sks.split_image_ref(layer_ref)
        listed = self.shell.run(
            [
                "docker",
                "image",
                "ls",
                "--format",
                "{{.Repository}}:{{.Tag}}",
                f"{self.inputs.registry}/{repository}",
            ],
            timeout=60,
        )
        for listed_tag in docker_listed_tags(listed.stdout):
            if listed_tag not in self.local_tags:
                self.local_tags.append(listed_tag)
        self.remember()
        image = sks.registry_image(self.get, repository, reference)
        if image is None:
            raise sks.Refusal("candidate_image_mismatch", sks.LAYER_IMAGE)
        self.pushed.append(Pushed(sks.LAYER_IMAGE, repository, reference, image.digest))
        self.remember()
        return image

    def verify(self) -> dict[str, Any]:
        layer_ref = self.ledger.observations("images").get("layer_ref")
        refs = sks.candidate_refs(
            self.inputs.candidate,
            self.inputs.run_id,
            layer_ref if isinstance(layer_ref, str) else None,
        )
        verified = sks.verify_candidate_images(self.get, self.inputs.candidate, refs)
        self.ledger.record(
            "images", "verified_digests", {name: image.digest for name, image in verified.items()}
        )
        return verified


def remove_registry_images(port: int, pushed: Sequence[Pushed]) -> None:
    failures = 0
    for item in pushed:
        status = registry_delete(port, item.repository, item.digest)
        failures += status not in {202, 404}
    if failures:
        raise StepFailed("the registry refused a manifest delete")


def registry_images_absent(port: int, pushed: Sequence[Pushed]) -> bool:
    get = sks.loopback_get("127.0.0.1", port, timeout=10)
    for item in pushed:
        try:
            status, _headers, _body = get(
                f"/v2/{item.repository}/manifests/{item.digest}", {"Accept": MANIFEST_ACCEPT}
            )
        except (OSError, http.client.HTTPException):
            # The run's registry is already gone, and every tag with it.
            return True
        if status != 404:
            return False
    return True


def remove_local_images(shell: Shell, tags: Sequence[str]) -> None:
    for tag in tags:
        listed = shell.run(["docker", "image", "ls", "-q", tag], timeout=60)
        if listed.stdout.strip():
            shell.run(["docker", "image", "rm", tag], timeout=120)


def local_images_absent(shell: Shell, tags: Sequence[str]) -> bool:
    return not any(
        shell.run(["docker", "image", "ls", "-q", tag], timeout=60).stdout.strip() for tag in tags
    )


def grant_probe(shell: Shell, grant: sks.HeldSecret, cli: str, clock: Clock) -> dict[str, Any]:
    """One haiku turn capped at 32 output tokens, with an empty HOME and only the grant.

    The child inherits nothing: no caller HOME, config dir or credential, and
    its working directory is the same empty directory, so no host login,
    project setting or helper can answer for the grant.
    """

    resolved = shutil.which(cli, path=shell.env.get("PATH"))
    if resolved is None:
        raise StepFailed("the grant probe CLI was not found")
    with tempfile.TemporaryDirectory(prefix="psk-grant-probe-") as home:
        if any(Path(home).iterdir()):
            raise StepFailed("the grant probe home was not empty")
        env = sks.grant_probe_env(
            home=home, cli_dir=str(Path(resolved).parent), grant=grant.reveal()
        )
        env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = str(sks.GRANT_PROBE_OUTPUT_CAP)
        started = clock.now()
        result = shell.run(
            sks.grant_probe_argv(resolved),
            stdin=GRANT_PROBE_PROMPT,
            timeout=GRANT_PROBE_SECONDS,
            check=False,
            extra_env=env,
            secret_env=(sks.GRANT_KEY,),
            clean_env=True,
            cwd=Path(home),
        )
        elapsed = clock.now() - started
    outcome = sks.grant_probe_result(result.returncode, result.stdout, result.stderr)
    outcome["turns"] = sks.probe_turns(result.stdout)
    outcome["elapsed"] = round(elapsed, 3)
    return outcome


def resolve_ipv4(host: str) -> list[str]:
    try:
        answers = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        raise StepFailed("the model provider address could not be resolved") from None
    return sorted({str(answer[4][0]) for answer in answers})


def oidc_harness() -> Any:
    """``tools/oidc-e2e/oidc_e2e.py`` by path, for its HTTP and cookie helpers and Dex pin."""

    spec = importlib.util.spec_from_file_location("curie_oidc_e2e_harness", OIDC_HARNESS)
    if spec is None or spec.loader is None:
        raise StepFailed("the OIDC harness is unavailable")
    module = importlib.util.module_from_spec(spec)
    # Its dataclasses resolve their own module by name while it executes.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# Subcommands
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Runtime:
    environ: Mapping[str, str]
    out: Output
    scrubber: sks.Scrubber
    scan: sks.TokenScan
    clock: Clock
    slack_transport: sks.SlackTransport | None = None


def make_shell(inputs: Inputs, runtime: Runtime) -> Shell:
    env = sks.child_environment(
        runtime.environ,
        kubeconfig=inputs.kubeconfig or None,
        docker_context=inputs.docker_context or None,
    )
    return Shell(
        env=env, scrubber=runtime.scrubber, scan=runtime.scan, cwd=REPO_ROOT, log=inputs.command_log
    )


def cmd_images(args: argparse.Namespace, runtime: Runtime) -> int:
    """Build, verify and remove every image and the registry; nothing outlives the command."""

    inputs = with_cluster(base_inputs(args), args)
    ledger = sks.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    shell = make_shell(inputs, runtime)
    registry = LoopbackRegistry(shell, inputs.registry_port, inputs.run_id, ledger)
    try:
        ledger.ensure_run(inputs.candidate, phase="images")
        if registry.answering():
            raise sks.Refusal("registry_port_busy", "--registry-port")
    except sks.Refusal:
        ledger.close()
        raise
    finalizer = Finalizer(ledger)
    builder = ImageBuilder(
        inputs=inputs,
        shell=shell,
        ledger=ledger,
        finalizer=finalizer,
        curie_bin=curie_binary(runtime.environ),
        gate_limits=sks.reviewer_limits(sks.ATTEMPT_ROLES),
    )
    finalizer.register("remove-registry-container", registry.remove)
    finalizer.register(
        "remove-local-images", lambda: remove_local_images(shell, builder.local_tags)
    )
    finalizer.register(
        "remove-registry-images",
        lambda: remove_registry_images(inputs.registry_port, builder.pushed),
    )
    finalizer.register("remove-work-dir", lambda: shutil.rmtree(inputs.work_dir, True))
    code = sks.EXIT_FAIL
    verified: dict[str, Any] = {}
    outcome: dict[str, Any] = {}
    with signals_to_exit():
        try:
            registry.start(runtime.clock)
            verified = builder.build()
            code = sks.EXIT_PASS
        except sks.Refusal as refusal:
            outcome = sks.refusal_payload(refusal)
            code = sks.EXIT_REFUSED
        except StepFailed as failure:
            outcome = {"error": "step_failed", "detail": str(failure)}
        except SystemExit:
            outcome = {"error": "interrupted"}
        finally:
            with signals_deferred():
                steps = finalizer.run()
    absent = {
        "registry_container": runtime_safe(lambda: not registry.listed()),
        "local_images": runtime_safe(lambda: local_images_absent(shell, builder.local_tags)),
        "work_dir": not inputs.work_dir.exists(),
    }
    for kind, ok in absent.items():
        ledger.record_cleanup(f"inventory:{kind}", kind, "absent" if ok else "present")
    passed = code == sks.EXIT_PASS and all(ok for _, ok in steps) and all(absent.values())
    ledger.close()
    runtime.out.emit(
        {
            "command": "images",
            "verdict": "pass" if passed else ("refused" if code == sks.EXIT_REFUSED else "fail"),
            "counts": {"images_verified": len(verified)},
            "cleanup_ok": all(ok for _, ok in steps) and all(absent.values()),
            **outcome,
        }
    )
    return sks.EXIT_PASS if passed else (code if code != sks.EXIT_PASS else sks.EXIT_FAIL)


def runtime_safe(call: Callable[[], bool]) -> bool:
    try:
        return call()
    except Exception:  # noqa: BLE001 - an unanswered absence query is not absence
        return False


def cmd_slack_probe(args: argparse.Namespace, runtime: Runtime) -> int:
    """Codes only: token identities, the app token, membership, and one deleted round trip."""

    inputs = with_slack(base_inputs(args), args)
    slack_files = inputs.slack
    if slack_files is None:
        raise sks.Refusal("slack_kit_invalid", "--channel-file")
    ledger = sks.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="slack-probe")
        kit = open_slack(slack_files, runtime, ledger)
        posted = ""
        try:
            posted = kit.sender.post(kit.channel, f"Slack kickoff probe {secrets.token_hex(4)}")
            seen = any(m.get("ts") == posted for m in kit.sender.replies(kit.channel, posted))
            kit.sender.delete(kit.channel, posted)
            gone = not kit.sender.message_present(kit.channel, posted)
        except sks.SlackError as error:
            ledger.record("slack", "slack_error", {"method": error.method, "code": error.code})
            if posted:
                with contextlib.suppress(sks.SlackError):
                    kit.sender.delete(kit.channel, posted)
            raise sks.Refusal("slack_unavailable", f"{error.method} {error.code}") from None
        finally:
            kit.clear()
        ledger.record("slack", "probe_round_trip", seen)
        ledger.record("slack", "probe_post_deleted", gone)
        checks = {
            "curie_bot_authenticated": True,
            "sender_authenticated": True,
            "isolation_confirmed": True,
            "app_token_accepted": True,
            "both_bots_in_channel": True,
            "round_trip": seen,
            "probe_post_deleted": gone,
        }
    finally:
        ledger.close()
    passed = all(checks.values())
    runtime.out.emit(
        {"command": "slack-probe", "verdict": "pass" if passed else "fail", "checks": checks}
    )
    return sks.EXIT_PASS if passed else sks.EXIT_FAIL


def cmd_preflight(args: argparse.Namespace, runtime: Runtime) -> int:
    """Offline: every input file, the ceiling, the grant lifetime and the candidate commit."""

    inputs = base_inputs(args)
    ceiling = sks.parse_ceiling(args.approve_live)
    roles = sks.attempt_roles(ceiling)
    sks.reviewer_model(ceiling)
    inputs = with_grant(inputs, args, runtime.clock.now())
    inputs = with_probe(with_slack(with_app(inputs, args), args), args)
    shell = Shell(
        env=sks.child_environment(runtime.environ, kubeconfig=None, docker_context=None),
        scrubber=runtime.scrubber,
        scan=runtime.scan,
        cwd=REPO_ROOT,
    )
    present = shell.run(
        ["git", "-C", str(REPO_ROOT), "cat-file", "-e", f"{inputs.candidate}^{{commit}}"],
        check=False,
    )
    if present.returncode != 0:
        raise sks.Refusal("candidate_invalid", "--candidate is not a local commit")
    ledger = sks.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="preflight")
        ledger.record("preflight", "ceiling", ceiling.as_json())
    finally:
        ledger.close()
    runtime.out.emit(
        {
            "command": "preflight",
            "verdict": "pass",
            "checks": {
                "service_credentials_absent": True,
                "fake_mode_absent": True,
                "grant_file_private": True,
                "grant_lifetime_sufficient": True,
                "live_execution_approved": True,
                "slack_files_private": True,
                "isolation_confirmed_file": True,
                "app_kit_present": True,
                "candidate_is_local_commit": True,
            },
            "counts": {"reserved_sessions": len(roles)},
            "ceiling": ceiling.as_json(),
        }
    )
    return sks.EXIT_PASS


def cmd_judge(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = base_inputs(args)
    ledger = sks.Ledger.open_existing(inputs.state_dir, inputs.run_id)
    try:
        if ledger.candidate() != inputs.candidate:
            raise sks.Refusal("candidate_mismatch", "--candidate")
        result = sks.judge(ledger, args.criterion)
    finally:
        ledger.close()
    runtime.out.emit(result)
    return sks.EXIT_PASS if result["verdict"] == "pass" else sks.EXIT_FAIL


def live_inputs(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    return with_slack(with_app(with_install(with_cluster(inputs, args), args), args), args)


def cmd_run(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = base_inputs(args)
    ceiling = sks.parse_ceiling(args.approve_live)
    sks.attempt_roles(ceiling)
    sks.reviewer_model(ceiling)
    inputs = with_grant(inputs, args, runtime.clock.now())
    inputs = with_probe(live_inputs(inputs, args), args)
    ledger = sks.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="run")
        ledger.mark_live_kind("run")
        ledger.record_ceiling(args.approve_live, ceiling)
        ledger.record_item_ledger(args.item_cap, args.item_charged_before, ceiling)
    except sks.Refusal:
        ledger.close()
        raise
    return LiveRun(inputs=inputs, runtime=runtime, ledger=ledger, ceiling=ceiling).execute()


def cmd_rehearse(args: argparse.Namespace, runtime: Runtime) -> int:
    """Every unpaid phase against the real cluster and Slack, then the full finalizer.

    ``--kickoff`` adds the kickoff and read-back, enrolled with the rehearsal's own
    never-issued token so nothing can be spent: the ceiling is ``--approve-live``
    and the live window ends at ``--grant-expires-at``, both required with it and
    refused without it. ``--grant-file`` is refused either way.
    """

    supplied = [
        flag
        for flag, value, allowed in (
            ("--grant-file", args.grant_file, False),
            ("--grant-expires-at", args.grant_expires_at, args.kickoff),
            ("--approve-live", args.approve_live, args.kickoff),
        )
        if value is not None and not allowed
    ]
    if supplied:
        raise sks.Refusal("arguments_invalid", "rehearse takes no " + ", ".join(supplied))
    inputs = base_inputs(args)
    ceiling: sks.Ceiling | None = None
    if args.kickoff:
        ceiling = sks.parse_ceiling(args.approve_live)
        sks.attempt_roles(ceiling)
        sks.reviewer_model(ceiling)
        expires_at = sks.parse_expiry(args.grant_expires_at)
        sks.require_lifetime(expires_at, runtime.clock.now())
        inputs = dataclasses.replace(inputs, expires_at=expires_at)
    inputs = live_inputs(inputs, args)
    ledger = sks.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="rehearse")
        ledger.mark_live_kind("rehearse")
        if ceiling is not None:
            ledger.record_ceiling(args.approve_live, ceiling)
            ledger.record("rehearsal", "kickoff_diagnostic", True)
    except sks.Refusal:
        ledger.close()
        raise
    return LiveRun(
        inputs=inputs,
        runtime=runtime,
        ledger=ledger,
        ceiling=ceiling,
        kickoff_diagnostic=ceiling is not None,
    ).execute()


# --------------------------------------------------------------------------
# rehearse and run: the live phases
# --------------------------------------------------------------------------


@dataclasses.dataclass
class RunnerPod:
    """One runner pod the run observed, with its own harvest and stderr classes.

    ``attach_pod`` creates it (on any thread) and ``audit_pod`` fills ``checks`` on
    the main thread once the pod runs; ``lock`` guards every field a reader thread
    writes. ``log_tail`` is the scrubbed tail of its runner container log.
    """

    name: str
    uid: str
    claim: str
    record: dict[str, Any]
    harvest: sks.HarvestState
    watch: sks.ProcessWatch
    lock: threading.Lock
    eof: threading.Event
    trace: sks.CaptureTrace = dataclasses.field(default_factory=sks.CaptureTrace)
    stderr: list[bytes] = dataclasses.field(default_factory=list)
    processes: list[subprocess.Popen[bytes]] = dataclasses.field(default_factory=list)
    checks: dict[str, Any] = dataclasses.field(default_factory=dict)
    audited: bool = False
    log_tail: collections.deque[str] = dataclasses.field(
        default_factory=lambda: collections.deque(maxlen=RUNNER_LOG_TAIL)
    )
    threads: list[threading.Thread] = dataclasses.field(default_factory=list)


class LiveRun:
    """Explicit phases; a LIFO finalizer registered before each resource is created."""

    def __init__(
        self,
        *,
        inputs: Inputs,
        runtime: Runtime,
        ledger: sks.Ledger,
        ceiling: sks.Ceiling | None,
        shell: Shell | None = None,
        resolver: Callable[[str], list[str]] | None = None,
        kickoff_diagnostic: bool = False,
    ) -> None:
        self.inputs = inputs
        self.runtime = runtime
        self.ledger = ledger
        self.ceiling = ceiling
        # ``rehearse --kickoff``: the run path's kickoff on the rehearsal's invalid token.
        self.kickoff_diagnostic = kickoff_diagnostic
        self.rehearsal = ceiling is None or kickoff_diagnostic
        self.command = "rehearse" if self.rehearsal else "run"
        self.clock = runtime.clock
        self.out = runtime.out
        self.scrubber = runtime.scrubber
        self.scan = runtime.scan
        self.shell = shell or make_shell(inputs, runtime)
        self.kube = Kube(self.shell, inputs.context, inputs.namespace)
        self.curie = CurieCli(self.shell, curie_binary(runtime.environ), inputs)
        self.resolver = resolver or resolve_ipv4
        self.finalizer = Finalizer(ledger)
        self.registry = LoopbackRegistry(self.shell, inputs.registry_port, inputs.run_id, ledger)
        self.builder: ImageBuilder | None = None
        self.mutated = False
        self.phase = "refusals"
        digest = hashlib.sha256(f"{inputs.run_id}:{secrets.token_hex(8)}".encode()).hexdigest()
        self.agent = f"acme-psk-{digest[:8]}"
        self.prefix = f"psk-{digest[8:16]}/"
        self.unused_label = f"psk-unused-{digest[16:24]}"
        self.nonce = secrets.token_hex(8)
        self.app_secret = f"{inputs.release}-github-app"
        self.slack_secret = f"{inputs.release}-slack-tokens"
        self.dex_name = f"{inputs.release}-dex"
        self.oidc_secret = self.scrubber.hold(sks.HeldSecret(secrets.token_urlsafe(32)))
        self.origins = sks.oidc_origins(
            namespace=inputs.namespace,
            dex_service=self.dex_name,
            api_service=f"{inputs.fullname}-api",
            api_port=API_PORT,
        )
        self.processes: list[subprocess.Popen[bytes]] = []
        self.namespace_uids: dict[str, str] = {}
        self.volumes: dict[str, str] = {}
        self.digests: dict[str, str] = {}
        self.chart = Path()
        self.kit: SlackKit | None = None
        self.forge: Forge | None = None
        self.api: CurieApi | None = None
        self.api_port = 0
        self.dex_port = 0
        self.session_file = inputs.work_dir / "console-session"
        self.token_file: Path | None = None
        self.agent_id = ""
        self.tenant_id = ""
        self.owner: dict[str, Any] = {}
        self.issues: dict[str, dict[str, Any]] = {}
        # Every root this run posted, by the poster: the sender's posts only.
        self.roots: list[str] = []
        self.kickoff_ts = ""
        self.work_item_id = ""
        self.reservations: list[sks.Reservation] = []
        self.pods: dict[str, RunnerPod] = {}
        self.attempt_live = False
        self.guard_starts = False
        self.attempt_started = 0.0
        # The per-response guard's events: watchdog checks, and every response they saw.
        self.guard_checks = 0
        self.guard_responses: set[str] = set()
        # The deadline enforcer's thread. Only ``deadline_breach`` is shared with the main
        # thread, under ``deadline_lock``; the fired set and spawned list are its own.
        self.deadline_lock = threading.Lock()
        self.deadline_breach = ""
        self.deadline_reported = False
        self.deadline_stop = threading.Event()
        self.deadline_thread: threading.Thread | None = None
        self.deadline_fired: set[str] = set()
        self.deadline_processes: list[subprocess.Popen[bytes]] = []
        # The claim-pod follower's thread. ``follow_lock`` guards ``pods``, ``processes``
        # appends from stream threads, ``present_pods`` and ``terminations``.
        self.follow_lock = threading.Lock()
        self.follow_stop = threading.Event()
        self.follow_thread: threading.Thread | None = None
        self.present_pods: set[str] = set()
        self.terminations: dict[tuple[str, str, str], dict[str, Any]] = {}
        # Branch to this run's published head; an empty head means ownership is unproved.
        self.publication_branches: dict[str, str] = {}
        self.publications_recovered = False
        self.publications_uncertain = False
        self.policy_set = False
        self.outcome: dict[str, Any] = {}
        self.signals: list[int] = []

    def __repr__(self) -> str:
        return "<live run>"

    # -- driver -------------------------------------------------------------

    def phases(self) -> list[tuple[str, Callable[[], None]]]:
        phases: list[tuple[str, Callable[[], None]]] = [
            ("refusals", self.phase_refusals),
            ("registry", self.phase_registry),
            ("images", self.phase_images),
            ("namespaces", self.phase_namespaces),
            ("dex", self.phase_dex),
            ("install", self.phase_install),
            ("setup", self.phase_setup),
            ("ping", self.phase_ping),
            ("enroll", self.phase_enroll),
            ("fixtures", self.phase_fixtures),
        ]
        if not self.rehearsal or self.kickoff_diagnostic:
            phases += [("kickoff", self.phase_kickoff), ("read-back", self.phase_read_back)]
        phases.append(("control", self.phase_control))
        if self.rehearsal:
            # The paid-start fence is exercised unpaid against the real install,
            # so a wrong selector or verb fails here, not live.
            phases.append(("fence", self.phase_fence_rehearsal))
        return phases

    def execute(self) -> int:
        code = sks.EXIT_FAIL
        cleanup_ok = False
        with signals_to_exit():
            try:
                for name, phase in self.phases():
                    self.phase = name
                    self.ledger.set_phase(name)
                    self.out.progress(f"phase {name}")
                    phase()
                code = sks.EXIT_PASS
            except sks.Refusal as refusal:
                self.outcome = sks.refusal_payload(refusal)
                code = sks.EXIT_REFUSED
            except StepFailed as failure:
                self.outcome = {"error": "step_failed", "detail": str(failure)}
            except SystemExit:
                self.outcome = {"error": "interrupted"}
            except Exception as exc:  # noqa: BLE001 - finalize after any failure
                self.outcome = {"error": "unexpected", "detail": type(exc).__name__}
            finally:
                with signals_deferred() as received:
                    try:
                        cleanup_ok = self.finalize()
                    except Exception:  # noqa: BLE001 - a broken finalizer is a failed cleanup
                        cleanup_ok = False
                self.signals = received
        if code == sks.EXIT_PASS and not cleanup_ok:
            code = sks.EXIT_FAIL
        status = {sks.EXIT_PASS: "pass", sks.EXIT_REFUSED: "refused"}.get(code, "fail")
        self.ledger.set_phase(self.phase, status)
        summary = {
            "command": self.command,
            "verdict": status,
            "phase": self.phase,
            "cleanup_ok": cleanup_ok,
            **self.outcome,
        }
        self.out.emit(summary)
        self.scan.scan("evidence", json.dumps(summary, sort_keys=True).encode())
        self.scan.scan("ledger", self.ledger.dump().encode())
        if self.mutated:
            self.ledger.record("cleanup", "token_scan", self.scan.as_json())
            self.ledger.record("cleanup", "scan_clean", sks.scan_clean(self.scan))
        self.ledger.close()
        return code

    def finalize(self) -> bool:
        if not self.mutated:
            self.finalizer.run()
            return True
        run = self.ledger.run_row()
        if run["live_started_at"] is not None:
            # Paid execution stops before the live window closes or anything is torn down.
            self.stop_deadline_enforcer()
            self.safe(self.record_deadline_breach, False)
            self.safe(self.fence_paid_starts, None)
            self.ledger.end_live(now=self.clock.now())
        steps = self.finalizer.run()
        inventory = self.query_inventory()
        for kind, status in sorted(inventory.items()):
            self.ledger.record_cleanup(f"inventory:{kind}", kind, status)
        # The inventory reads the forge and Slack, so their credentials end last.
        if self.forge is not None:
            try:
                self.forge.revoke()
                self.ledger.record_cleanup("step:revoke-installation-token", "forge", "ok")
            except Exception as exc:  # noqa: BLE001 - recorded as a failed step
                self.ledger.record_cleanup(
                    "step:revoke-installation-token", "forge", "failed", type(exc).__name__
                )
                steps.append(("revoke-installation-token", False))
        if self.kit is not None:
            self.kit.clear()
        return all(ok for _, ok in steps) and all(s == "absent" for s in inventory.values())

    @staticmethod
    def safe(call: Callable[[], Any], default: Any) -> Any:
        try:
            return call()
        except Exception:  # noqa: BLE001 - an unanswered query is reported as uncertain
            return default

    def wait_until(
        self,
        condition: Callable[[], bool],
        *,
        seconds: float,
        what: str,
        every: float = POLL_SECONDS,
    ) -> None:
        """Poll ``condition``; while a paid attempt runs, the watchdog checks every pass."""

        until = self.clock.monotonic() + seconds
        while True:
            self.tick()
            if condition():
                return
            if self.clock.monotonic() >= until:
                raise StepFailed(f"{what} did not happen in time")
            self.clock.wait(every)

    def require_kit(self) -> SlackKit:
        if self.kit is None:
            raise StepFailed("Slack clients are not ready")
        return self.kit

    def require_forge(self) -> Forge:
        if self.forge is None:
            raise StepFailed("forge client is not ready")
        return self.forge

    def require_api(self) -> CurieApi:
        if self.api is None:
            raise StepFailed("api client is not ready")
        return self.api

    def require_ceiling(self) -> sks.Ceiling:
        if self.ceiling is None:
            raise StepFailed("a rehearsal has no ceiling")
        return self.ceiling

    # -- phase: refusals (no mutation) ----------------------------------------

    def phase_refusals(self) -> None:
        if self.ceiling is not None:
            sks.require_lifetime(self.inputs.expires_at, self.clock.now())
        for name in (self.inputs.namespace, self.inputs.grant_namespace):
            existing = self.kube.ctl(
                "get", "namespace", name, "--ignore-not-found", "-o", "name", namespaced=False
            )
            if existing.stdout.strip():
                raise sks.Refusal("namespace_exists", name)
        if self.registry.answering():
            raise sks.Refusal("registry_port_busy", "--registry-port")
        slack_files = self.inputs.slack
        if slack_files is None:
            raise sks.Refusal("slack_kit_invalid", "--channel-file")
        self.kit = open_slack(slack_files, self.runtime, self.ledger)

    # -- phase: registry and images ---------------------------------------------

    def phase_registry(self) -> None:
        self.mutated = True
        # Registered first, so it runs last, after every image delete.
        self.finalizer.register("remove-registry-container", self.registry.remove)
        self.registry.start(self.clock)

    def phase_images(self) -> None:
        builder = ImageBuilder(
            inputs=self.inputs,
            shell=self.shell,
            ledger=self.ledger,
            finalizer=self.finalizer,
            curie_bin=self.curie.binary,
            gate_limits=sks.reviewer_limits(sks.ATTEMPT_ROLES),
        )
        self.builder = builder
        self.finalizer.register(
            "remove-local-images", lambda: remove_local_images(self.shell, builder.local_tags)
        )
        self.finalizer.register(
            "remove-registry-images",
            lambda: self.remove_registry_images(builder),
        )
        self.finalizer.register("remove-work-dir", self.remove_work_dir)
        verified = builder.build()
        self.digests = {name: image.digest for name, image in verified.items()}

    def remove_registry_images(self, builder: ImageBuilder) -> None:
        remove_registry_images(self.inputs.registry_port, builder.pushed)
        self.ledger.record(
            "cleanup",
            "registry_tags_absent",
            registry_images_absent(self.inputs.registry_port, builder.pushed),
        )

    def remove_work_dir(self) -> None:
        if self.inputs.work_dir.exists():
            shutil.rmtree(self.inputs.work_dir)

    # -- phase: namespaces ----------------------------------------------------------

    def phase_namespaces(self) -> None:
        self.finalizer.register("delete-owned-volumes", self.delete_owned_volumes)
        for name in (self.inputs.grant_namespace, self.inputs.namespace):
            self.finalizer.register(
                f"delete-namespace-{name}", functools.partial(self.delete_namespace, name)
            )
            self.kube.create(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": name,
                        "labels": {sks.MANAGED_BY_LABEL: sks.MANAGED_BY_VALUE},
                        "annotations": {sks.RUN_ANNOTATION: self.inputs.run_id},
                    },
                }
            )
            uid = self.kube.ctl(
                "get", "namespace", name, "-o", "jsonpath={.metadata.uid}", namespaced=False
            )
            self.namespace_uids[name] = uid.stdout.decode().strip()
        self.ledger.record("install", "namespaces_created", len(self.namespace_uids))

    def delete_namespace(self, name: str) -> None:
        """Delete only the namespace this run created: same UID, label and run annotation."""

        current = self.kube.ctl(
            "get", "namespace", name, "--ignore-not-found", "-o", "json", namespaced=False
        )
        if not current.stdout.strip():
            return
        meta = json.loads(current.stdout).get("metadata") or {}
        if (
            meta.get("uid") != self.namespace_uids.get(name)
            or (meta.get("labels") or {}).get(sks.MANAGED_BY_LABEL) != sks.MANAGED_BY_VALUE
            or (meta.get("annotations") or {}).get(sks.RUN_ANNOTATION) != self.inputs.run_id
        ):
            raise StepFailed("namespace ownership changed; not deleting")
        if name == self.inputs.namespace:
            self.safe(self.record_volumes, None)
        self.kube.ctl(
            "delete",
            "namespace",
            name,
            "--wait=true",
            "--timeout=240s",
            namespaced=False,
            timeout=260,
        )

    def record_volumes(self) -> None:
        """Remember the PV bound to each of the run's PVCs, before anything is torn down."""

        listed = self.kube.json("get", "persistentvolumeclaims")
        for item in listed.get("items") or [] if isinstance(listed, dict) else []:
            name = (item.get("spec") or {}).get("volumeName")
            uid = (item.get("metadata") or {}).get("uid")
            if isinstance(name, str) and name and isinstance(uid, str) and uid:
                self.volumes[name] = uid
        self.ledger.record("install", "owned_volumes", len(self.volumes))

    def volume(self, name: str) -> dict[str, Any] | None:
        result = self.kube.ctl(
            "get", "persistentvolume", name, "--ignore-not-found", "-o", "json", namespaced=False
        )
        if not result.stdout.strip():
            return None
        value = json.loads(result.stdout)
        return value if isinstance(value, dict) else {}

    def delete_owned_volumes(self) -> None:
        for name, uid in sorted(self.volumes.items()):
            pv = self.volume(name)
            if pv is None:
                continue
            claim = (pv.get("spec") or {}).get("claimRef") or {}
            if claim.get("uid") != uid or claim.get("namespace") != self.inputs.namespace:
                raise StepFailed("a recorded volume is now bound elsewhere; not deleting")
            self.kube.ctl(
                "delete",
                "persistentvolume",
                name,
                "--wait=true",
                "--timeout=120s",
                namespaced=False,
                timeout=140,
            )

    # -- phase: Dex ---------------------------------------------------------------------

    def phase_dex(self) -> None:
        oidc = oidc_harness()
        labels = {sks.MANAGED_BY_LABEL: sks.MANAGED_BY_VALUE}
        config = sks.dex_config(self.origins, client_secret=self.oidc_secret.reveal())
        for manifest in sks.dex_manifests(
            name=self.dex_name,
            labels=labels,
            config=config,
            client_secret=self.oidc_secret.reveal(),
            image=str(oidc.DEX_IMAGE),
        ):
            self.kube.create(manifest, secret=manifest["kind"] == "Secret")
        self.kube.ctl(
            "wait",
            "--for=condition=Ready",
            f"pod/{self.dex_name}",
            "--timeout=240s",
            timeout=260,
        )
        self.ledger.record("install", "dex_ready", True)

    # -- phase: install -------------------------------------------------------------------

    def phase_install(self) -> None:
        inputs = self.inputs
        kit = self.require_kit()
        work = private_dir(inputs.work_dir)
        chart_root = work / "chart"
        archive = work / "chart.tar"
        self.shell.run(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "archive",
                "--format=tar",
                "-o",
                str(archive),
                inputs.candidate,
                "charts/curie",
            ],
            timeout=300,
        )
        chart_root.mkdir(mode=0o700, exist_ok=True)
        extract_archive(archive, chart_root)
        self.chart = chart_root / "charts" / "curie"
        app_dir = inputs.app_dir
        if app_dir is None:
            raise sks.Refusal("app_kit_invalid", "--app-dir")
        pem = (app_dir / "app.pem").read_bytes()
        key = self.scrubber.hold(sks.HeldSecret(pem.decode("ascii", errors="replace")))
        self.finalizer.register("delete-app-secret", lambda: self.delete_secret(self.app_secret))
        self.kube.create(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": self.app_secret},
                "type": "Opaque",
                "data": {"privateKey": base64.b64encode(pem).decode()},
            },
            secret=True,
        )
        key.clear()
        self.finalizer.register(
            "delete-slack-secret", lambda: self.delete_secret(self.slack_secret)
        )
        self.kube.create(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": self.slack_secret},
                "type": "Opaque",
                "data": {
                    "slackAppToken": base64.b64encode(kit.app.token.reveal().encode()).decode(),
                    "slackBotToken": base64.b64encode(kit.curie.token.reveal().encode()).decode(),
                },
            },
            secret=True,
        )
        controller = self.kube.ctl(
            "get",
            "deployment",
            CONTROLLER_DEPLOYMENT,
            "--namespace",
            CONTROLLER_NAMESPACE,
            "--ignore-not-found",
            "-o",
            "name",
            namespaced=False,
        ).stdout.strip()
        ceiling = self.ceiling
        plan = sks.InstallPlan(
            release=inputs.release,
            registry=inputs.registry,
            digests=self.digests,
            agent=self.agent,
            repo=str(inputs.app["repo"]),
            app_id=str(inputs.app["id"]),
            app_secret=self.app_secret,
            slack_secret=self.slack_secret,
            channel=kit.channel,
            sender=kit.sender_auth,
            grant_namespace=inputs.grant_namespace,
            origins=self.origins,
            dex_secret=self.dex_name,
            unused_label=self.unused_label,
            priority_classes=inputs.priority_classes,
            controller_deploy=not controller,
            model_addresses=self.resolver(MODEL_HOST),
            implementer_turns=(ceiling.turns_for(sks.IMPLEMENTER) if ceiling is not None else 1),
            response_cap=sks.RESPONSE_OUTPUT_CAP,
        )
        values = sks.install_values(plan)
        self.ledger.record("slack", "declared_driver", values["testInstallation"]["drivers"][0])
        values_file = work / "values.json"
        self.finalizer.register("delete-values-file", lambda: values_file.unlink(missing_ok=True))
        write_private(values_file, json.dumps(values, indent=2).encode())
        # Slack messages are deleted only after the dispatcher and worker are gone,
        # so nothing posts into a thread after its messages were removed.
        self.finalizer.register("delete-slack-messages", self.delete_slack_messages)
        self.finalizer.register("helm-uninstall", self.helm_uninstall)
        self.shell.run(
            [
                "helm",
                "--kube-context",
                inputs.context,
                "--namespace",
                inputs.namespace,
                "install",
                inputs.release,
                str(self.chart),
                "-f",
                str(values_file),
                "--timeout",
                "900s",
            ],
            timeout=960,
        )
        try:
            for workload in ("api", "worker", "dispatcher", "channel-gateway"):
                self.kube.ctl(
                    "rollout",
                    "status",
                    f"deployment/{inputs.fullname}-{workload}",
                    "--timeout=600s",
                    timeout=620,
                )
            for store in ("postgres", "valkey"):
                self.kube.ctl(
                    "rollout",
                    "status",
                    f"statefulset/{inputs.fullname}-{store}",
                    "--timeout=600s",
                    timeout=620,
                )
        except Exception:
            self.record_rollout_diagnostics()
            raise
        required = ("curie-api", "curie-worker", "curie-dispatcher")
        failures = sks.running_image_failures(
            self.kube.json("get", "pods"),
            {name: self.digests[name] for name in required},
            registry=inputs.registry,
            required=required,
        )
        self.ledger.record("grant", "platform_images_are_candidate", not failures)
        self.safe(self.record_volumes, None)
        if failures:
            raise StepFailed("candidate images are not the running images")
        deployed = json.loads(
            self.shell.run(
                [
                    "helm",
                    "--kube-context",
                    inputs.context,
                    "--namespace",
                    inputs.namespace,
                    "get",
                    "values",
                    inputs.release,
                    "--all",
                    "-o",
                    "json",
                ],
            ).stdout
        )
        findings = sks.service_credential_findings(deployed)
        self.ledger.record("install", "service_credentials_absent", not findings)
        if findings:
            raise sks.Refusal("deployed_service_credential", ", ".join(findings))
        secret = self.kube.json(
            "get", "secret", f"{inputs.fullname}-secrets", sensitive_output=True
        )
        data = secret.get("data") if isinstance(secret, dict) else None
        generated = isinstance(data, dict) and sks.generated_secrets_ok(data)
        self.ledger.record("install", "generated_secrets", generated)
        if not generated or not isinstance(data, dict):
            raise StepFailed("the release secrets are not generated values")
        try:
            api_key = base64.b64decode(data["apiKey"]).decode()
        except (KeyError, ValueError):
            raise StepFailed("the release secrets lack the api key") from None
        self.api_port = self.port_forward(f"svc/{inputs.fullname}-api", API_PORT)
        self.api = CurieApi(
            JsonHttp("127.0.0.1", self.api_port, tls=False, scan=self.scan),
            self.scrubber.hold(sks.HeldSecret(api_key)),
        )

    def record_rollout_diagnostics(self) -> None:
        diagnostics = self.safe(
            lambda: sks.pod_diagnostics(
                self.kube.json("get", "pods"), self.kube.json("get", "events")
            ),
            {"pods": [], "warnings": [], "unanswered": True},
        )
        self.ledger.record("install", "rollout_diagnostics", diagnostics)
        self.out.progress("rollout stalled: " + json.dumps(diagnostics, sort_keys=True))

    def helm_uninstall(self) -> None:
        listed = self.shell.run(
            [
                "helm",
                "--kube-context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "status",
                self.inputs.release,
            ],
            check=False,
        )
        if listed.returncode == 0:
            self.shell.run(
                [
                    "helm",
                    "--kube-context",
                    self.inputs.context,
                    "--namespace",
                    self.inputs.namespace,
                    "uninstall",
                    self.inputs.release,
                    "--wait",
                    "--timeout",
                    "300s",
                ],
                timeout=320,
            )
        # The Socket Mode connection lives exactly as long as the dispatcher pod.
        gone = self.dispatcher_pods_absent()
        self.ledger.record("slack", "dispatcher_pods_gone", gone)
        if not gone:
            raise StepFailed("a dispatcher pod is still present after uninstall")

    def dispatcher_pods_absent(self) -> bool:
        pods = self.kube.json(
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/component=dispatcher,app.kubernetes.io/instance={self.inputs.release}",
        )
        return not (pods.get("items") if isinstance(pods, dict) else None)

    def delete_secret(self, name: str) -> None:
        self.kube.ctl("delete", "secret", name, "--ignore-not-found", "--wait=true")

    def port_forward(self, target: str, remote: int) -> int:
        process = self.shell.spawn(
            [
                "kubectl",
                "--context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "port-forward",
                "--address",
                "127.0.0.1",
                target,
                f":{remote}",
            ]
        )
        self.processes.append(process)
        self.finalizer.register("stop-port-forward", lambda: stop_process(process))
        lines: queue.Queue[bytes | None] = queue.Queue()

        def drain() -> None:
            stream = process.stdout
            if stream is None:
                lines.put(None)
                return
            for line in stream:
                self.scan.scan("children", line)
                lines.put(line)
            lines.put(None)

        threading.Thread(target=drain, daemon=True).start()
        deadline = self.clock.monotonic() + 60
        while self.clock.monotonic() < deadline:
            try:
                line = lines.get(timeout=max(0.05, deadline - self.clock.monotonic()))
            except queue.Empty:
                break
            if line is None:
                break
            match = re.search(rb"Forwarding from 127\.0\.0\.1:(\d+) ->", line)
            if match:
                return int(match[1])
        raise StepFailed("a port-forward did not become ready")

    # -- phase: setup ---------------------------------------------------------------------

    def sql(self, query: str) -> list[dict[str, Any]]:
        """Rows of one read through psql in the run's own Postgres pod, as JSON."""

        wrapped = f"SELECT coalesce(json_agg(t), '[]'::json) FROM ({query}) t;\n"
        result = self.kube.ctl(
            "exec",
            "-i",
            f"{self.inputs.fullname}-postgres-0",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-At",
            "-f",
            "-",
            stdin=wrapped.encode(),
        )
        try:
            rows = json.loads(result.stdout)
        except ValueError:
            raise StepFailed("a database read returned unreadable JSON") from None
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    def sql_write(self, statement: str) -> None:
        self.kube.ctl(
            "exec",
            "-i",
            f"{self.inputs.fullname}-postgres-0",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-q",
            "-f",
            "-",
            stdin=statement.encode(),
        )

    def configure_agent(self) -> None:
        """Publication policy, the explicit reviewer model, and the approved bounds.

        Personal model authentication refuses every session of an agent with no
        explicit Claude reviewer model, before any provider call, and the request
        ends runner_escalated; the bundle's reviewer agents name only an alias.
        """

        self.curie.run(
            "publication-policy",
            self.agent,
            "--policy",
            "auto",
            "--no-draft",
            "--branch-prefix",
            self.prefix,
        )
        overrides = ["--reviewer-model", sks.reviewer_model(self.ceiling)]
        if self.ceiling is not None:
            roles = sks.attempt_roles(self.ceiling)
            window = int(len(roles) * self.ceiling.session_seconds)
            overrides += ["--execution-deadline", str(window)]
        self.curie.run("overrides", self.agent, *overrides)
        if self.ceiling is not None:
            self.curie.run("budget", self.agent, "--output-tokens", str(self.ceiling.output_tokens))

    def phase_setup(self) -> None:
        inputs = self.inputs
        kit = self.require_kit()
        app_dir = inputs.app_dir
        if app_dir is None:
            raise sks.Refusal("app_kit_invalid", "--app-dir")
        self.forge = Forge(
            self.shell, forge_http(self.scan), app_dir, inputs.app, self.scrubber, self.clock
        )
        self.forge.mint()
        repo = str(inputs.app["repo"])
        self.curie.run(
            "deploy",
            "--plugin-dir",
            str(inputs.work_dir / "bundle"),
            "--agent",
            self.agent,
            "--env",
            "prod",
            "--chart",
            str(self.chart),
            "--slack-channel",
            kit.channel,
            timeout=900,
        )
        self.curie.run("surfaces", self.agent, "--add", f"github={repo}")
        self.finalizer.register("close-run-pulls", self.close_run_pulls)
        self.finalizer.register("delete-run-branches", self.delete_run_branches)
        # Registered last so it runs first, while the api and database are still up.
        self.finalizer.register("recover-publications", self.recover_publications)
        self.policy_set = True
        self.configure_agent()
        agents = self.sql(f"SELECT id, tenant_id FROM curie.agents WHERE name = '{self.agent}'")
        if len(agents) != 1:
            raise StepFailed("the deployed agent is missing")
        self.agent_id = sks.sql_uuid(agents[0].get("id"))
        self.tenant_id = sks.sql_uuid(agents[0].get("tenant_id"))
        self.dex_port = self.port_forward(f"svc/{self.dex_name}", sks.DEX_PORT)
        session = self.oidc_login()
        write_private(self.session_file, session.reveal().encode())
        self.finalizer.register("remove-session-file", lambda: self.session_file.unlink(True))
        oidc = oidc_harness()
        principal = oidc.request(
            "GET",
            f"http://127.0.0.1:{self.api_port}/console/principal",
            headers={"Cookie": f"{sks.SESSION_COOKIE}={session.reveal()}"},
        )
        if principal.status != 200:
            raise StepFailed("the OIDC session is not principal bound")
        body = principal.json()
        principal_id = sks.sql_uuid(body.get("id"))
        principal_tenant = sks.sql_uuid(body.get("tenant_id"))
        self.ledger.record("owner", "agent_tenant", self.tenant_id)
        self.ledger.record(
            "setup", "oidc_principal_in_agent_tenant", principal_tenant == self.tenant_id
        )
        if principal_tenant != self.tenant_id:
            raise StepFailed("the OIDC principal is not in the agent's tenant")
        self.owner = {
            "tenant_id": self.tenant_id,
            "mode": sks.PERSONAL_MODE,
            "principal_id": principal_id,
        }
        namespace_id = self.await_slack_namespace(kit.curie_auth.team_id)
        self.seed_slack_link(namespace_id, kit.sender_auth.user_id, principal_id)

    def oidc_login(self) -> sks.HeldSecret:
        """A principal-bound console session from a real Dex password login, split horizon.

        The API discovers and exchanges against the issuer's in-cluster Service
        name. The driver reaches Dex through its own port-forward while
        presenting that issuer's authority (``route_url``), and the callback
        through the API forward; no other origin is ever dialed.
        """

        oidc = oidc_harness()
        jar: dict[str, str] = {}

        def call(
            method: str, url: str, form: dict[str, str] | None = None, cookie: str = ""
        ) -> Any:
            routed = sks.route_url(
                url, self.origins, dex_local=self.dex_port, api_local=self.api_port
            )
            if routed is None:
                raise StepFailed("the login named an origin outside the run's forwards")
            target, extra = routed
            headers = dict(extra)
            if cookie:
                headers["Cookie"] = cookie
            elif jar and "Host" in extra:
                headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in jar.items())
            response = oidc.request(method, target, headers=headers, form=form)
            if "Host" in extra:
                for name, morsel in response.set_cookies().items():
                    jar[name] = morsel.value
            return response

        started = call("GET", f"http://127.0.0.1:{self.api_port}/console/oidc/login")
        state = started.set_cookies().get(sks.STATE_COOKIE)
        if started.status != 302 or state is None:
            raise StepFailed("the OIDC login did not redirect to the IdP")
        current = urllib.parse.urljoin(self.origins.issuer + "/", started.location)
        response = call("GET", current)
        callback = ""
        for _ in range(12):
            if response.status in (301, 302, 303, 307):
                target = urllib.parse.urljoin(current, response.location)
                if target.startswith(self.origins.redirect_uri):
                    callback = target
                    break
                current = target
                response = call("GET", current)
                continue
            chooser = re.search(r'href="(/dex/auth/local[^"]*)"', response.body)
            if response.status == 200 and 'name="password"' in response.body:
                response = call(
                    "POST", current, {"login": sks.OWNER_EMAIL, "password": oidc.USER_PASSWORD}
                )
            elif response.status == 200 and chooser:
                current = urllib.parse.urljoin(current, chooser.group(1).replace("&amp;", "&"))
                response = call("GET", current)
            else:
                break
        if not callback:
            raise StepFailed("Dex never redirected to the API callback")
        done = call("GET", callback, cookie=f"{sks.STATE_COOKIE}={state.value}")
        morsel = done.set_cookies().get(sks.SESSION_COOKIE)
        if done.status != 303 or morsel is None or not morsel.value:
            raise StepFailed("the OIDC callback did not mint a principal session")
        session = self.scrubber.hold(sks.HeldSecret(str(morsel.value)))
        self.ledger.record("setup", "oidc_session_minted", True)
        return session

    def await_slack_namespace(self, team_id: str) -> str:
        """The dispatcher's identity report: the workspace's slack_workspace namespace."""

        api = self.require_api()
        found: dict[str, str] = {}

        def reported() -> bool:
            listed = api.get(f"/identity/admin/{self.tenant_id}/namespaces?limit=200")
            for item in (listed or {}).get("items") or []:
                if (
                    isinstance(item, dict)
                    and item.get("provider") == "slack"
                    and item.get("kind") == "slack_workspace"
                    and item.get("key") == team_id
                    and item.get("status") == "active"
                ):
                    found["id"] = str(item.get("id"))
                    return True
            return False

        self.wait_until(
            reported, seconds=IDENTITY_SECONDS, what="the Slack identity report", every=5
        )
        self.ledger.record("setup", "slack_namespace_reported", True)
        return sks.sql_uuid(found["id"])

    def seed_slack_link(self, namespace_id: str, sender_user: str, principal_id: str) -> None:
        """Setup, not the proved path: no API creates a Slack link, so it is seeded by SQL.

        The sender bot user is mapped ``admin_mapped`` to the OIDC principal, the
        way #5375 seeds its principal; every value is validated before it is
        written into the statement.
        """

        link_id = str(uuid.uuid4())
        user = sks.require_slack_id(sender_user, "user")
        self.sql_write(
            "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, "
            "provider_native_id, principal_id, verification_source, created_by_principal_id) "
            f"VALUES ('{sks.sql_uuid(link_id)}', '{self.tenant_id}', '{namespace_id}', "
            f"'{user}', '{principal_id}', 'admin_mapped', '{principal_id}');\n"
        )
        links = self.require_api().get(f"/identity/admin/{self.tenant_id}/links?limit=200")
        mine = [
            item
            for item in (links or {}).get("items") or []
            if isinstance(item, dict) and item.get("id") == link_id
        ]
        link = mine[0] if mine else {}
        self.ledger.record("setup", "slack_link_seeded_admin_mapped", True)
        self.ledger.record("owner", "link_native_is_sender", link.get("provider_native_id") == user)
        self.ledger.record("owner", "link_principal", link.get("principal_id"))

    # -- phase: ping (Socket Mode delivery and declared-driver admission) ----------------

    def phase_ping(self) -> None:
        kit = self.require_kit()
        ts = kit.sender.post(kit.channel, sks.ping_text(kit.curie_auth.user_id))
        self.roots.append(ts)
        answered: dict[str, int] = {}

        def replied() -> bool:
            messages = kit.sender.replies(kit.channel, ts)
            answered["count"] = len(messages)
            return sks.ping_answered(
                messages,
                curie_user=kit.curie_auth.user_id,
                sender_user=kit.sender_auth.user_id,
                root_ts=ts,
            )

        self.wait_until(replied, seconds=REPLY_SECONDS, what="the ping answer", every=3)
        self.ledger.record("slack", "ping_answered", True)
        self.ledger.record("slack", "ping_messages", answered.get("count", 0))
        receipt = self.receipt_for(ts)
        self.ledger.record("slack", "ping_receipt_present", receipt is not None)

    def receipt_for(self, message_ts: str) -> sks.SlackMentionReceipt | None:
        """The dispatcher's stored receipt for one post, read inside the Valkey pod.

        The password stays in the pod: the shell there expands it into
        REDISCLI_AUTH, so it never reaches this process's argv or output.
        """

        script = (
            'export REDISCLI_AUTH="$VALKEY_PASSWORD"; '
            f"for k in $(valkey-cli --no-auth-warning --scan --pattern '{sks.RECEIPT_PATTERN}'); "
            'do printf "%s\\t" "$k"; valkey-cli --no-auth-warning GET "$k"; done'
        )
        result = self.kube.ctl(
            "exec", f"{self.inputs.fullname}-valkey-0", "--", "sh", "-c", script, timeout=60
        )
        values: dict[str, bytes] = {}
        for line in result.stdout.split(b"\n"):
            key, sep, value = line.partition(b"\t")
            if sep:
                values[key.decode(errors="replace")] = value
        kit = self.require_kit()
        return sks.find_receipt(values, channel=kit.channel, message_ts=message_ts)

    def dispatcher_logged(self, delivery_id: str) -> bool:
        logs = self.kube.ctl(
            "logs", f"deployment/{self.inputs.fullname}-dispatcher", "--tail=5000", timeout=60
        )
        return sks.enqueue_logged(logs.stdout + logs.stderr, delivery_id)

    def observe_delivery(self, name: str, ts: str) -> sks.SlackMentionReceipt | None:
        """Record the Socket Mode receipt and the dispatcher's enqueue line for one post."""

        kit = self.require_kit()
        found: dict[str, sks.SlackMentionReceipt] = {}

        def stored() -> bool:
            receipt = self.receipt_for(ts)
            if receipt is not None:
                found["receipt"] = receipt
            return receipt is not None

        try:
            self.wait_until(stored, seconds=RECEIPT_SECONDS, what=f"the {name} receipt", every=3)
        except StepFailed:
            self.ledger.record("slack", f"{name}_receipt_present", False)
            raise
        receipt = found["receipt"]
        self.ledger.record("slack", f"{name}_receipt_present", True)
        self.ledger.record(
            "slack",
            f"{name}_receipt_checks",
            sks.receipt_checks(receipt, channel=kit.channel, message_ts=ts, sender=kit.sender_auth),
        )
        logged = {"ok": False}

        def enqueued() -> bool:
            logged["ok"] = self.dispatcher_logged(receipt.delivery_id)
            return logged["ok"]

        with contextlib.suppress(StepFailed):
            self.wait_until(enqueued, seconds=RECEIPT_SECONDS, what="the enqueue line", every=3)
        self.ledger.record("slack", f"{name}_enqueue_logged", logged["ok"])
        return receipt

    # -- phase: enrollment through the owner surface --------------------------------------

    def subscription(self, verb: str, *args: str) -> tuple[int, dict[str, Any]]:
        argv = [
            self.curie.binary,
            "--json",
            "cluster",
            "--context",
            self.inputs.context,
            "subscription",
            verb,
            "--session-file",
            str(self.session_file),
            "--api-url",
            f"http://127.0.0.1:{self.api_port}",
            "--namespace",
            self.inputs.namespace,
            "--release",
            self.inputs.release,
            *args,
        ]
        result = self.shell.run(argv, check=False, timeout=180)
        try:
            body = json.loads(result.stdout)
        except ValueError:
            body = {}
        return result.returncode, body if isinstance(body, dict) else {}

    def phase_enroll(self) -> None:
        if self.rehearsal:
            # Well formed and never issued: the provider refuses it, and a
            # rehearsal reserves nothing, so no launch can start on it.
            invalid = self.scrubber.hold(
                sks.HeldSecret("sk-ant-oat01-" + secrets.token_urlsafe(72))
            )
            token_file = self.inputs.work_dir / "rehearsal-token"
            write_private(token_file, invalid.reveal().encode() + b"\n")
            self.finalizer.register("remove-rehearsal-token", lambda: token_file.unlink(True))
            self.token_file = token_file
        else:
            # The caller's private grant file is already the CLI's token-file shape.
            self.token_file = self.inputs.grant_file
        if self.token_file is None:
            raise StepFailed("no token file for enrollment")
        code, _body = self.subscription("enroll", "--token-file", str(self.token_file))
        self.ledger.record("setup", "enroll_exit", code)
        if code != 0:
            raise StepFailed(f"subscription enroll exited {code}")
        code, status = self.subscription("status")
        generation = status.get("generation")
        if code != 0 or status.get("state") != "active" or type(generation) is not int:
            raise StepFailed("the enrollment is not active after enroll")
        self.owner["generation"] = generation
        self.ledger.record("owner", "expected", self.owner)
        self.ledger.record("owner", "enrollment_state_at_kickoff", status.get("state"))
        self.ledger.record("owner", "enrollment_generation_at_kickoff", generation)

    # -- phase: forge fixtures ------------------------------------------------------------

    def phase_fixtures(self) -> None:
        forge = self.require_forge()
        text = FIXTURE_ISSUE.read_text()
        self.finalizer.register("close-fixture-issues", self.close_issues)
        for label in ("a", "b"):
            body = text.replace("{nonce}", f"{self.nonce}-{label}")
            title_match = _ISSUE_TITLE.match(body.splitlines()[0])
            title = f"{title_match.group(1) if title_match else 'Fixture'} ({label})"
            issue = forge.call(
                "POST", forge.repo_path("/issues"), {"title": title, "body": body}, ok=(201,)
            )
            number = issue.get("number") if isinstance(issue, dict) else None
            url = issue.get("html_url") if isinstance(issue, dict) else None
            if not isinstance(number, int) or not isinstance(url, str):
                raise StepFailed("a fixture issue was not created")
            self.issues[label] = {"number": number, "url": url}
            links = sks.find_issue_links(url)
            if len(links) != 1 or links[0].kind != "issue" or links[0].number != number:
                raise sks.Refusal("link_invalid", label)
        self.ledger.record(
            "refusal",
            "control_issue_distinct",
            self.issues["a"]["number"] != self.issues["b"]["number"],
        )

    def close_issues(self) -> None:
        forge = self.require_forge()
        for issue in self.issues.values():
            forge.call(
                "PATCH",
                forge.repo_path(f"/issues/{issue['number']}"),
                {"state": "closed", "state_reason": "not_planned"},
            )

    # -- phase: the paid kickoff -----------------------------------------------------------

    def phase_kickoff(self) -> None:
        ceiling = self.require_ceiling()
        kit = self.require_kit()
        roles = sks.attempt_roles(ceiling)
        built = self.ledger.observations("images").get("gate_limits")
        if built != sks.reviewer_limits(roles):
            raise sks.Refusal("gate_mismatch", "rebuild the runner layer")
        self.ledger.begin_live(
            now=self.clock.now(),
            grant_expires_at=self.inputs.expires_at,
            diagnostic=self.kickoff_diagnostic,
        )
        self.ledger.record("budget", "allowance_breach", [])
        if not self.kickoff_diagnostic:
            # A diagnostic enrolled the rehearsal's never-issued token: a probe would
            # only prove that token refused.
            self.run_grant_probe()
        self.reservations = self.ledger.reserve(roles, now=self.clock.now())
        self.attempt_live = True
        self.attempt_started = self.clock.now()
        self.start_deadline_enforcer(ceiling)
        self.start_follower()
        try:
            self.admit_kickoff(kit)
            try:
                self.watch_attempt()
            finally:
                self.attempt_live = False
                self.stop_deadline_enforcer()
                # No pod is attached and no stream respawned once the attempt settles.
                self.safe(self.close_follower, None)
                self.settle()
                self.ledger.close_gate()
        finally:
            # The attempt settled or failed: no deadline or follower pass runs after it.
            self.stop_deadline_enforcer()
            self.follow_stop.set()
        self.guard_starts = True
        # A deadline the enforcer reached while the main loop was blocked still fences.
        self.report_deadline_breach()

    def admit_kickoff(self, kit: SlackKit) -> None:
        """Post the kickoff, observe its delivery, and wait for its one admission."""

        before = self.counts()
        self.ledger.record(
            "admission",
            "before",
            {"work_items": before["work_items"], "requests": before["requests"]},
        )
        issue = self.issues["a"]
        ts = kit.sender.post(kit.channel, sks.kickoff_text(kit.curie_auth.user_id, issue["url"]))
        self.roots.append(ts)
        self.kickoff_ts = ts
        root = kit.sender.replies(kit.channel, ts)
        self.ledger.record(
            "slack",
            "kickoff_at_root",
            any(m.get("ts") == ts and m.get("thread_ts") in (None, ts) for m in root),
        )
        receipt = self.observe_delivery("kickoff", ts)
        if receipt is None:
            raise StepFailed("the kickoff receipt is missing")
        expected_request = str(
            sks.kickoff_request_id(receipt.channel_identity, receipt.delivery_id)
        )
        self.ledger.record("admission", "expected_request_id", expected_request)
        self.ledger.record("admission", "expected_mode", sks.PERSONAL_MODE)
        self.ledger.record("admission", "issue_url_digest", sks.ref_digest(issue["url"]))
        self.ledger.record("admission", "issue_number", issue["number"])
        self.ledger.record("admission", "expected_requester", f"slack:{kit.sender_auth.user_id}")
        expected_thread = {
            "channel_identity": sks.DEFAULT_IDENTITY,
            "channel": kit.channel,
            "root_ts": ts,
            "origin": sks.KICKOFF_ORIGIN,
        }
        self.ledger.record("admission", "expected_thread", expected_thread)
        self.ledger.record("progress", "expected_thread", expected_thread)
        self.wait_until(self.admitted, seconds=ADMISSION_SECONDS, what="the kickoff admission")
        self.record_admission()

    def run_grant_probe(self) -> None:
        """Right before the first reservation, recorded under grant_probe before it runs."""

        grant_file = self.inputs.grant_file
        self.ledger.record(
            "grant_probe",
            "started",
            {"at": self.clock.now(), "model": sks.GRANT_PROBE_MODEL},
        )
        self.ledger.record("grant_probe", "model", sks.GRANT_PROBE_MODEL)
        self.ledger.record("grant_probe", "output_cap", sks.GRANT_PROBE_OUTPUT_CAP)
        self.ledger.record("grant_probe", "turn_cap", sks.GRANT_PROBE_TURNS)
        grant = self.scrubber.hold(sks.load_grant(grant_file))
        try:
            outcome = grant_probe(self.shell, grant, self.inputs.grant_probe_cli, self.clock)
        finally:
            grant.clear()
        self.ledger.record("grant_probe", "result", outcome)
        failures = sks.probe_failures(outcome)
        self.ledger.record("grant_probe", "failures", failures)
        if failures:
            raise sks.Refusal("grant_probe_refused", ", ".join(failures))

    def counts(self) -> dict[str, int]:
        rows = self.sql(
            "SELECT (SELECT count(*) FROM curie.work_items WHERE agent_id = "
            f"'{self.agent_id}') AS work_items, (SELECT count(*) FROM curie.execution_requests r "
            "JOIN curie.work_items w ON r.work_item_id = w.id WHERE w.agent_id = "
            f"'{self.agent_id}') AS requests"
        )
        row = rows[0] if rows else {}
        claims = self.kube.json("get", CLAIM_RESOURCE, "-l", f"{CLAIM_AGENT_LABEL}={self.agent}")
        pods = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        return sks.counts_snapshot(
            work_items=int(row.get("work_items") or 0),
            requests=int(row.get("requests") or 0),
            claims=len((claims or {}).get("items") or []),
            runner_pods=len((pods or {}).get("items") or []),
            sessions=len(self.ledger.sessions()),
        )

    def admitted(self) -> bool:
        rows = self.sql(f"SELECT id FROM curie.work_items WHERE agent_id = '{self.agent_id}'")
        if len(rows) == 1:
            self.work_item_id = sks.sql_uuid(rows[0].get("id"))
            return True
        return False

    def work_item_rows(self) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT id, tenant_id, model_auth_mode, owner_principal_id, enrollment_generation, "
            "tracker_issue_id, trigger_label FROM curie.work_items WHERE agent_id = "
            f"'{self.agent_id}'"
        )

    def request_rows(self) -> list[dict[str, Any]]:
        if not self.work_item_id:
            return []
        return self.sql(
            "SELECT id, work_item_id, tenant_id, model_auth_mode, owner_principal_id, "
            "enrollment_generation, sequence, status, terminal_at IS NOT NULL AS terminal, "
            "terminal_cause, objective, requester FROM curie.execution_requests WHERE "
            f"work_item_id = '{self.work_item_id}' ORDER BY sequence"
        )

    def record_admission(self) -> None:
        items = self.work_item_rows()
        requests = self.request_rows()
        self.ledger.record("admission", "work_items", items)
        self.ledger.record(
            "admission",
            "requests",
            [
                {
                    **{k: v for k, v in row.items() if k not in {"objective", "requester"}},
                    "objective_digest": sks.ref_digest(str(row.get("objective") or "")),
                    "requester": row.get("requester"),
                }
                for row in requests[:1]
            ],
        )
        thread = self.slack_thread()
        self.ledger.record("admission", "thread", thread)
        self.ledger.record("owner", "work_item_owner", sks.owner_tuple(items[0] if items else None))
        self.ledger.record("owner", "request_owners", [sks.owner_tuple(row) for row in requests])

    def slack_thread(self) -> dict[str, Any] | None:
        rows = self.sql(
            "SELECT channel_identity, channel, root_ts, origin FROM curie.factory_slack_threads "
            f"WHERE work_item_id = '{self.work_item_id}'"
        )
        return rows[0] if len(rows) == 1 else None

    # -- the watchdog -------------------------------------------------------------------

    def tick(self) -> None:
        """While paid work can start: follow runner pods, then check every bound.

        A deadline the enforcer thread reached is reported and fenced first.
        After the attempt settles and until the disconnect's teardown ends, any
        runner the run did not follow (a continuation the platform queued after
        the attempt) is an unreserved paid start, and fences everything.
        """

        self.report_deadline_breach()
        if self.guard_starts and not self.attempt_live:
            if self.unfollowed_runner_pods():
                self.ledger.record("budget", "allowance_breach", ["unreserved:after_attempt"])
                self.guard_starts = False
                self.fence_paid_starts()
                raise StepFailed("a runner started after the attempt settled; paid work stopped")
            return
        if not self.attempt_live:
            return
        self.discover_runner_pods()
        ceiling = self.require_ceiling()
        breaches: list[str] = []
        facts = self.attempt_facts()
        if facts is not None:
            breaches = sks.watchdog_breaches(
                self.reservations,
                facts.sessions,
                ceiling,
                now=self.clock.now(),
                running=self.native_running(),
            )
            self.guard_checks += 1
            for session in facts.sessions:
                self.guard_responses.update(session.message_ids)
        window = sum(item.seconds for item in self.reservations)
        if self.clock.now() - self.attempt_started > window + STARTUP_SECONDS:
            breaches.append("seconds:attempt")
        if breaches:
            # In the order reached: output tokens first, then turns, then seconds.
            self.ledger.record("budget", "allowance_breach", list(dict.fromkeys(breaches)))
            self.attempt_live = False
            self.stop_deadline_enforcer()
            self.fence_paid_starts()
            raise StepFailed("an approved bound was reached; paid work stopped")

    def native_running(self) -> bool:
        """Whether any followed runner pod reports a native CLI process still running."""

        running = False
        for pod in self.followed():
            with pod.lock:
                running = running or pod.watch.running
        return running

    def report_deadline_breach(self) -> None:
        """Record and fence, on the main thread, a deadline the enforcer thread reached.

        The enforcer only interrupts the runner and deletes its pod; the ledger
        record and the full fence happen here, once, as for any other bound.
        """

        if not self.record_deadline_breach():
            return
        self.attempt_live = False
        self.guard_starts = False
        self.stop_deadline_enforcer()
        self.fence_paid_starts()
        raise StepFailed("a native session reached its absolute time bound; paid work stopped")

    def record_deadline_breach(self) -> bool:
        """Append, once, a deadline the enforcer reached to the recorded breaches.

        Returns whether this call recorded it. ``finalize`` calls it too, so a
        deadline reached while the main loop failed some other way is still
        recorded before the fence.
        """

        with self.deadline_lock:
            breach = "" if self.deadline_reported else self.deadline_breach
            self.deadline_reported = self.deadline_reported or bool(breach)
        if not breach:
            return False
        recorded = self.ledger.observations("budget").get("allowance_breach")
        prior = [str(item) for item in recorded] if isinstance(recorded, list) else []
        self.ledger.record("budget", "allowance_breach", list(dict.fromkeys([*prior, breach])))
        return True

    # -- the deadline enforcer ----------------------------------------------------------

    def start_deadline_enforcer(self, ceiling: sks.Ceiling) -> None:
        """Bound each native process's wall clock without waiting on the main loop.

        ``tick`` runs only between blocking calls (kubectl, SQL through
        kubectl, Slack HTTP) whose timeouts reach minutes, so it can overshoot
        a deadline. This daemon thread checks the followed pods about once a
        second instead, on the driver's clock. Its stop is registered with the
        finalizer before it starts.
        """

        self.finalizer.register("stop-deadline-enforcer", self.close_deadline_enforcer)
        thread = threading.Thread(
            target=self.deadline_loop, args=(float(ceiling.session_seconds),), daemon=True
        )
        self.deadline_thread = thread
        thread.start()

    def deadline_loop(self, seconds: float) -> None:
        while not self.deadline_stop.is_set():
            self.safe(lambda: self.enforce_deadlines(seconds), None)
            if self.deadline_stop.is_set():
                return
            self.clock.wait(DEADLINE_SECONDS)

    def enforce_deadlines(self, seconds: float) -> None:
        """One enforcer pass: stop each runner whose native process reached ``seconds``.

        A process's start is its wall-clock start (``ProcessWatch.started``), so
        the bound is absolute. Each native process runs one implementer-role
        session, the main session or the publish continuation, with its
        reviewer subagents inside it, so the breach is that role's seconds. At
        the deadline the runner interrupt (the fence's own command) and a
        ``--wait=false`` delete of the pod start with nothing waiting on them,
        and the breach is left for ``report_deadline_breach``. The ledger is
        never touched here: its connection belongs to the main thread.
        """

        now = self.clock.now()
        for pod in self.followed():
            with pod.lock:
                deadline = pod.watch.deadline(seconds)
            if deadline is None or now < deadline or pod.uid in self.deadline_fired:
                continue
            self.deadline_fired.add(pod.uid)
            with self.deadline_lock:
                self.deadline_breach = self.deadline_breach or f"seconds:{sks.IMPLEMENTER}"
            self.start_unwaited(
                "exec", pod.name, "-c", "runner", "--", "python3", "-c", INTERRUPT_PROGRAM
            )
            self.start_unwaited("delete", "pod", pod.name, "--wait=false")

    def start_unwaited(self, *args: str) -> None:
        """Start one kubectl command for the enforcer; its output is scanned, never awaited."""

        try:
            process = self.kube.start(*args)
        except Exception:  # noqa: BLE001 - the main thread's fence still stops the runner
            return
        self.deadline_processes.append(process)
        self.processes.append(process)
        for stream in (process.stdout, process.stderr):
            threading.Thread(target=self.scan_children, args=(stream,), daemon=True).start()

    def scan_children(self, stream: Any) -> None:
        for line in stream if stream is not None else ():
            self.scan.scan("children", line)

    def stop_deadline_enforcer(self) -> None:
        """No enforcer pass starts after this; a pass in flight may still finish."""

        self.deadline_stop.set()

    def close_deadline_enforcer(self) -> None:
        """The finalizer step: stop the enforcer, wait for it, and reap what it started."""

        self.stop_deadline_enforcer()
        thread = self.deadline_thread
        if thread is not None:
            thread.join(timeout=DEADLINE_JOIN_SECONDS)
        for process in tuple(self.deadline_processes):
            stop_process(process)
        if thread is not None and thread.is_alive():
            raise StepFailed("the deadline enforcer did not stop")

    def discover_runner_pods(self) -> None:
        """The main thread's pass over the agent's claims: terminations, attach, then audit.

        It notes every claim pod's container terminations, attaches any pod the
        follower has not yet (in any phase), and audits each attached pod once it
        runs. The audit reads the ledger and the registry, so it runs only here.
        """

        claims = self.kube.json("get", CLAIM_RESOURCE, "-l", f"{CLAIM_AGENT_LABEL}={self.agent}")
        for claim in (claims or {}).get("items") or []:
            name = str((claim.get("metadata") or {}).get("name") or "")
            if not name:
                continue
            pods = self.kube.json("get", "pods", "-l", f"{POD_CLAIM_LABEL}={name}")
            for pod in (pods or {}).get("items") or []:
                uid = str((pod.get("metadata") or {}).get("uid") or "")
                if not uid:
                    continue
                self.note_terminations(pod)
                self.attach_pod(pod)
                with self.follow_lock:
                    observed = self.pods.get(uid)
                if (
                    observed is None
                    or observed.audited
                    or (pod.get("status") or {}).get("phase") != "Running"
                ):
                    continue
                self.audit_pod(observed, pod, claim)

    def followed(self) -> list[RunnerPod]:
        """A snapshot of the followed pods; the follower thread may add one at any time."""

        with self.follow_lock:
            return list(self.pods.values())

    def audit_pod(self, observed: RunnerPod, pod: dict[str, Any], claim: dict[str, Any]) -> None:
        """Main thread only: the image and grant checks of one attached pod that runs."""

        checks, running = sks.runner_image_check(
            pod, registry=self.inputs.registry, digest=self.digests.get(sks.LAYER_IMAGE, "")
        )
        image = None
        if running:
            layer = self.ledger.observations("images").get("layer_ref")
            repository = sks.split_image_ref(str(layer))[0] if isinstance(layer, str) else ""
            get = sks.loopback_get("127.0.0.1", self.inputs.registry_port)
            image = self.safe(lambda: sks.registry_image(get, repository, running), None)
        checks["revision_matches"] = image is not None and image.revision == self.inputs.candidate
        audit = {
            "image": checks,
            **sks.pod_grant_checks(
                pod, claim, self.ledger.observations("owner").get("expected") or {}
            ),
        }
        started = sks.runner_started_at(pod)
        with observed.lock:
            observed.record = pod
            observed.checks = audit
            observed.audited = True
            if observed.watch.container_started is None:
                observed.watch.container_started = started
        self.ledger.record("grant", "runner_pods_seen", len(self.followed()))
        if not all(checks.values()):
            raise StepFailed("a runner pod is not running the candidate runner layer")

    def note_terminations(self, pod: Mapping[str, Any]) -> None:
        """Keep each terminated container of a claim pod, the latest per container and state."""

        name = str((pod.get("metadata") or {}).get("name") or "")
        if not name:
            return
        ref = sks.ref_digest(name)
        found = sks.container_terminations(pod)
        with self.follow_lock:
            for item in found:
                self.terminations[(ref, str(item["container"]), str(item["from"]))] = {
                    "pod": ref,
                    **item,
                }

    def attach_pod(self, pod: Mapping[str, Any]) -> RunnerPod | None:
        """Follow one claim pod from the moment it exists, in any phase. Thread safe.

        Creates its RunnerPod and starts its harvester ``exec`` and its runner
        ``logs -f`` stream, each through ``follow_stream``. It reads neither the
        ledger nor the registry and registers nothing: ``audit_pod`` runs on the
        main thread, and ``stop_runner_streams`` (registered before the follower
        starts) stops every stream. Returns None when the pod is already followed.
        """

        meta = pod.get("metadata") or {}
        uid, name = str(meta.get("uid") or ""), str(meta.get("name") or "")
        if not uid or not name:
            return None
        observed = RunnerPod(
            name=name,
            uid=uid,
            claim=str((meta.get("labels") or {}).get(POD_CLAIM_LABEL) or ""),
            record=dict(pod),
            harvest=sks.HarvestState(self.scan),
            watch=sks.ProcessWatch(container_started=sks.runner_started_at(pod)),
            lock=threading.Lock(),
            eof=threading.Event(),
        )
        with self.follow_lock:
            if uid in self.pods:
                return None
            self.pods[uid] = observed
        harvester = self.kube.argv(
            "exec",
            "-i",
            name,
            "-c",
            "runner",
            "--",
            "python3",
            "-c",
            sks.HARVEST_PROGRAM,
            sks.harvest_config(
                nonce=self.nonce,
                interval=sks.CAPTURE_REPORT_SECONDS,
                max_seconds=sum(item.seconds for item in self.reservations) + STARTUP_SECONDS,
            ),
        )
        logs = self.kube.argv("logs", "-f", name, "-c", "runner")

        def harvest(line: bytes) -> None:
            with observed.lock:
                observed.harvest.feed(line)
                observed.watch.feed(line, now=self.clock.now())
                observed.trace.feed_harvest(line)

        def log(line: bytes) -> None:
            self.scan.scan("children", line)
            text = self.scrubber.scrub(line.decode(errors="replace").rstrip("\r\n"))
            with observed.lock:
                observed.trace.feed_log(line)
                observed.log_tail.append(text)
                if sks.stderr_error_classes([line]):
                    observed.stderr.append(line)

        def read_harvest() -> None:
            try:
                self.follow_stream(observed, harvester, harvest)
            finally:
                observed.eof.set()

        threads = [
            threading.Thread(target=read_harvest, daemon=True),
            threading.Thread(target=self.follow_stream, args=(observed, logs, log), daemon=True),
        ]
        with observed.lock:
            observed.threads.extend(threads)
        for thread in threads:
            thread.start()
        return observed

    def follow_stream(
        self, observed: RunnerPod, argv: list[str], on_line: Callable[[bytes], None]
    ) -> None:
        """Read one stream into a runner container, respawning it while it exits silently.

        A ``kubectl exec`` or ``logs -f`` started before the container runs exits
        with nothing on stdout. While the pod still exists (in the follower's
        latest listing) the command is respawned every STREAM_RETRY_SECONDS, for
        at most STREAM_RETRY_LIMIT, so an early stream still connects and a
        container that already terminated still returns its logs. A stream that
        produced output is not respawned, and none is once the follower stops.
        Never touches the ledger.
        """

        until = self.clock.monotonic() + STREAM_RETRY_LIMIT
        while True:
            try:
                process = self.shell.spawn(argv)
            except Exception:  # noqa: BLE001 - the main-thread watchdog still bounds the attempt
                return
            with self.follow_lock:
                self.processes.append(process)
            with observed.lock:
                observed.processes.append(process)
            if self.follow_stop.is_set():
                stop_process(process)
                return
            if process.stderr is not None:
                threading.Thread(
                    target=self.scan_children, args=(process.stderr,), daemon=True
                ).start()
            produced = False
            for line in process.stdout if process.stdout is not None else ():
                produced = True
                on_line(line)
            if (
                produced
                or self.follow_stop.is_set()
                or self.clock.monotonic() >= until
                or not self.pod_present(observed.uid)
            ):
                return
            self.clock.wait(STREAM_RETRY_SECONDS)

    def pod_present(self, uid: str) -> bool:
        with self.follow_lock:
            return uid in self.present_pods

    # -- the claim-pod follower ---------------------------------------------------------

    def start_follower(self) -> None:
        """Attach each claim pod the moment it exists, without waiting on the main loop.

        ``discover_runner_pods`` runs only between the main loop's blocking calls,
        so a runner that lives seconds is missed. This daemon thread lists the
        claim pods about once a second on the driver's clock and attaches each new
        one in any phase. The stop of every stream, and its own stop, are
        registered with the finalizer before it starts. Everything the attach
        path needs is in memory already; nothing it does touches the ledger.
        """

        self.finalizer.register("stop-runner-streams", self.stop_runner_streams)
        self.finalizer.register("stop-runner-follower", self.close_follower)
        thread = threading.Thread(target=self.follow_loop, daemon=True)
        self.follow_thread = thread
        thread.start()

    def follow_loop(self) -> None:
        while not self.follow_stop.is_set():
            self.safe(self.follow_pass, None)
            if self.follow_stop.is_set():
                return
            self.clock.wait(FOLLOW_SECONDS)

    def follow_pass(self) -> None:
        """One follower pass: one claim-pod listing, its terminations, each new pod attached."""

        listing = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL, timeout=FOLLOW_GET_SECONDS)
        items = [pod for pod in (listing or {}).get("items") or [] if isinstance(pod, dict)]
        with self.follow_lock:
            self.present_pods = {str((pod.get("metadata") or {}).get("uid") or "") for pod in items}
        for pod in items:
            if self.follow_stop.is_set():
                return
            self.note_terminations(pod)
            if self.attach_pod(pod) is not None:
                continue
            # An attached pod's runner start, once reported, bounds its first process.
            with self.follow_lock:
                observed = self.pods.get(str((pod.get("metadata") or {}).get("uid") or ""))
            started = sks.runner_started_at(pod)
            if observed is not None and started is not None:
                with observed.lock:
                    if observed.watch.container_started is None:
                        observed.watch.container_started = started

    def close_follower(self) -> None:
        """Stop the follower and wait for it; no stream is respawned after this."""

        self.follow_stop.set()
        thread = self.follow_thread
        if thread is None:
            return
        thread.join(timeout=FOLLOW_JOIN_SECONDS)
        if thread.is_alive():
            raise StepFailed("the runner pod follower did not stop")

    def stop_runner_streams(self) -> None:
        """The finalizer step: stop every harvester and log stream into a runner container."""

        self.follow_stop.set()
        for pod in self.followed():
            with pod.lock:
                processes = tuple(pod.processes)
            for process in processes:
                stop_process(process)

    def attempt_facts(self) -> sks.AttemptFacts | None:
        files: list[dict[str, bytes]] = []
        for pod in self.followed():
            with pod.lock:
                files.append(pod.harvest.transcript_files())
        if not files:
            return None
        return sks.attempt_facts(files, not_before=self.attempt_started - REPLAY_MARGIN_SECONDS)

    def unfollowed_runner_pods(self) -> list[str]:
        pods = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        followed = {pod.uid for pod in self.followed()}
        return [
            str((pod.get("metadata") or {}).get("uid"))
            for pod in (pods or {}).get("items") or []
            if (pod.get("status") or {}).get("phase") in {"Pending", "Running"}
            and str((pod.get("metadata") or {}).get("uid")) not in followed
        ]

    def quiescent(self) -> bool:
        """Every request of the WorkItem ended and every runner pod's harvester stream ended.

        The runner's CLI lives until the kernel deletes its pod, so the stream
        ends with the pod; settlement reads what it reported until then.
        """

        if not self.requests_terminal():
            return False
        return all(pod.eof.is_set() for pod in self.followed())

    def requests_terminal(self) -> bool:
        requests = self.request_rows()
        return bool(requests) and all(row.get("terminal") for row in requests)

    def pods_gone(self) -> dict[str, bool]:
        """For each followed runner pod's uid, whether it is deleted or being deleted."""

        listing = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        items = listing.get("items") if isinstance(listing, dict) else None
        if not isinstance(items, list):
            return {}
        present = {
            str((pod.get("metadata") or {}).get("uid") or ""): bool(
                (pod.get("metadata") or {}).get("deletionTimestamp")
            )
            for pod in items
            if isinstance(pod, dict)
        }
        return {pod.uid: present.get(pod.uid, True) for pod in self.followed()}

    def watch_attempt(self) -> None:
        window = sum(item.seconds for item in self.reservations) + STARTUP_SECONDS
        remaining = window - (self.clock.now() - self.attempt_started)
        self.wait_until(self.quiescent, seconds=max(remaining, POLL_SECONDS), what="the attempt")

    def settle(self) -> None:
        if not self.reservations:
            return
        # Why the attempt ended, before anything else can fail: diagnostics never fail it.
        self.safe(self.record_diagnostics, None)
        facts = self.safe(self.attempt_facts, None)
        pods = self.followed()
        terminal = bool(self.safe(self.requests_terminal, False))
        gone = self.safe(self.pods_gone, None) or {}
        findings: list[dict[str, Any]] = (
            [] if pods else [{"code": "runner_pods", "file": None, "field": "pods", "value": 0}]
        )
        traces: list[dict[str, Any]] = []
        skeleton: dict[str, Any] = {}
        for pod in pods:
            with pod.lock:
                files = pod.harvest.transcript_files()
                findings.extend(
                    sks.capture_findings(
                        pod.trace,
                        files,
                        delivered=pod.harvest.complete and pod.eof.is_set(),
                        pod_gone=gone.get(pod.uid) is True,
                        request_terminal=terminal,
                        runner_pods=len(pods),
                    )
                )
                traces.append(pod.trace.sanitized())
                skeleton.update(sks.transcript_skeleton(files))
        failures = {item["code"] for item in findings}
        self.ledger.record(
            "budget",
            "capture",
            {
                "affirmed": not failures,
                "failures": sorted(failures),
                "findings": findings,
                "aci_usage": "not_carried",
                "pods": traces,
            },
        )
        self.ledger.record("grant", "transcript_skeleton", skeleton)
        complete = not failures
        sessions = facts.sessions if facts is not None else None
        settled = sks.settle_attempt(
            self.ledger,
            self.reservations,
            sessions,
            complete=complete,
            malformed=int(facts.malformed_lines) if facts is not None else 0,
        )
        self.ledger.record("budget", "settlement", settled)
        responses = {key for s in (facts.sessions if facts else ()) for key in s.message_ids}
        self.ledger.record(
            "budget",
            "guard_checks",
            {
                "checks": self.guard_checks,
                "responses": len(responses),
                "responses_checked": len(responses & self.guard_responses),
            },
        )
        if facts is None:
            return
        self.ledger.record("grant", "sessions", [sks.session_record(s) for s in facts.sessions])
        self.ledger.record("grant", "role_models", sks.role_models(facts.sessions))
        self.ledger.record("grant", "synthetic_records", int(facts.synthetic_records))
        self.ledger.record(
            "grant", "transcript_error_classes", sks.transcript_error_classes(facts.sessions)
        )
        lines: list[bytes] = []
        for pod in pods:
            with pod.lock:
                lines += pod.stderr
        self.ledger.record("grant", "stderr_error_classes", sks.stderr_error_classes(lines))
        self.ledger.record(
            "grant",
            "runner_pods",
            [
                {
                    **pod.checks,
                    "container_credential": (pod.harvest.container_credential or {}).get(
                        "credential"
                    ),
                }
                for pod in pods
            ],
        )
        searches = 0
        for pod in pods:
            with pod.lock:
                searches += sks.tool_search_calls(pod.harvest.transcript_files())
        self.ledger.record("budget", "tool_search_calls", searches)
        requests = self.request_rows()
        causes = [row.get("terminal_cause") for row in requests if row.get("terminal")]
        self.ledger.record("grant", "terminal_cause", causes[-1] if causes else None)
        self.ledger.record("owner", "request_owners", [sks.owner_tuple(row) for row in requests])

    def fence_paid_starts(self) -> None:
        """Make another paid start impossible: stop the worker first, then the runners.

        Order (accepted change 7): the worker Deployment is scaled to zero, the
        private reservation gate closes, and the worker pods are deleted with a
        one second grace period and waited on until the kubelet reports them gone,
        so no worker can claim a queued continuation or retry its turn. The worker
        is stopped, not drained: its readiness probe is a heartbeat file, so a
        draining worker stays Ready until its turn handler returns, and that
        handler waits on the very runner this fence must stop, which would hold a
        paid session past its absolute bound. Only then is each runner interrupted
        and its pod deleted, and the deletions awaited. That happens even when a
        worker would not stop, because the absolute session bound outranks the
        order; the fence still fails in that case. Last, no worker pod may be
        Ready and no runner container may run.

        The deadline enforcer deliberately stops a runner at its deadline without
        waiting on the worker; the main-thread fence that follows stops the worker
        and catches any runner a retry started.

        Once an attempt reserved anything, the worker's log is read first, while
        the worker can still be read, for the reason the attempt ended.
        """

        fullname = self.inputs.fullname
        worker_selector = self.worker_selector
        if self.reservations:
            self.capture_worker_escalation()
        self.kube.ctl("scale", f"deployment/{fullname}-worker", "--replicas=0", timeout=120)
        self.ledger.close_gate()
        self.kube.ctl(
            "delete",
            "pod",
            "-l",
            worker_selector,
            "--grace-period=1",
            "--wait=false",
            timeout=120,
        )
        self.kube.ctl(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            worker_selector,
            "--timeout=60s",
            timeout=75,
            check=False,
        )
        workers = self.kube.json("get", "pods", "-l", worker_selector)
        ready_workers = [
            pod
            for pod in (workers or {}).get("items") or []
            if any(
                item.get("type") == "Ready" and item.get("status") == "True"
                for item in (pod.get("status") or {}).get("conditions") or []
            )
        ]
        workers_stopped = not ready_workers
        self.ledger.record("budget", "workers_stopped_first", workers_stopped)
        runners = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        for pod in (runners or {}).get("items") or []:
            name = str((pod.get("metadata") or {}).get("name") or "")
            if name:
                self.kube.ctl(
                    "exec",
                    name,
                    "-c",
                    "runner",
                    "--",
                    "python3",
                    "-c",
                    INTERRUPT_PROGRAM,
                    check=False,
                    timeout=30,
                )
        self.kube.ctl("delete", "pod", "-l", POD_CLAIM_LABEL, "--wait=false", timeout=120)
        self.kube.ctl(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            POD_CLAIM_LABEL,
            "--timeout=180s",
            timeout=200,
            check=False,
        )
        runners = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        running = [
            pod
            for pod in (runners or {}).get("items") or []
            if any(
                (status.get("state") or {}).get("running")
                for status in (pod.get("status") or {}).get("containerStatuses") or []
            )
        ]
        fenced = workers_stopped and not running
        self.ledger.record("budget", "paid_starts_fenced", fenced)
        if not fenced:
            active = "a worker" if not workers_stopped else "a runner"
            raise StepFailed(f"{active} is still active after fencing")

    @property
    def worker_selector(self) -> str:
        return (
            f"app.kubernetes.io/component=worker,app.kubernetes.io/instance={self.inputs.release}"
        )

    # -- diagnostics: why an attempt ended ------------------------------------------------

    def record_diagnostics(self) -> None:
        """At settlement, into the private ledger: each followed runner's scrubbed log
        tail, every container termination seen, the worker's escalation lines and the
        attempt's request terminal detail. Pods appear only as ``ref_digest`` refs."""

        tails: list[dict[str, Any]] = []
        for pod in self.followed():
            with pod.lock:
                tails.append({"pod": sks.ref_digest(pod.name), "lines": list(pod.log_tail)})
        self.ledger.record("grant", "runner_log_tails", tails)
        with self.follow_lock:
            terminations = [self.terminations[key] for key in sorted(self.terminations)]
        self.ledger.record("grant", "runner_terminations", terminations)
        self.capture_worker_escalation()
        self.ledger.record("grant", "request_terminal", self.safe(self.request_terminal, None))

    def capture_worker_escalation(self) -> None:
        """Keep the worker's recent log lines about the attempt, scrubbed; never fails.

        One ``kubectl logs`` over the worker selector, every container, unprefixed;
        at most WORKER_LOG_LINES lines that name the attempt's work item or request
        or match an escalation marker (``sks.escalation_lines``). The shell scans
        the raw output. The latest capture wins; a failed read records an empty list.
        """

        expected = self.safe(
            lambda: self.ledger.observations("admission").get("expected_request_id"), None
        )
        ids = [str(item) for item in (self.work_item_id, expected) if item]
        lines: list[str] = []
        try:
            result = self.kube.ctl(
                "logs",
                "-l",
                self.worker_selector,
                f"--tail={WORKER_LOG_TAIL}",
                "--prefix=false",
                "--all-containers=true",
                check=False,
                timeout=WORKER_LOG_SECONDS,
            )
            if result.returncode == 0:
                lines = sks.escalation_lines(
                    result.stdout, ids, self.scrubber.scrub, limit=WORKER_LOG_LINES
                )
        except Exception:  # noqa: BLE001 - a diagnostic read never fails the run
            lines = []
        self.safe(lambda: self.ledger.record("grant", "worker_escalation_lines", lines), None)

    def request_terminal(self) -> dict[str, Any] | None:
        """The attempt's execution request: status, terminal cause, termination observation."""

        if not self.work_item_id:
            return None
        rows = self.sql(
            "SELECT id, status, terminal_cause, termination_observation FROM "
            f"curie.execution_requests WHERE work_item_id = '{self.work_item_id}' "
            "ORDER BY sequence"
        )
        expected = self.ledger.observations("admission").get("expected_request_id")
        row = next(
            (item for item in rows if str(item.get("id") or "") == expected),
            rows[0] if rows else None,
        )
        if row is None:
            return None
        observation = row.get("termination_observation")
        return {
            "status": row.get("status"),
            "terminal_cause": row.get("terminal_cause"),
            "termination_observation": (
                None if observation is None else self.scrubber.scrub(str(observation))
            ),
        }

    # -- phase: read-back ---------------------------------------------------------------

    def post_rows(self) -> list[dict[str, Any]]:
        return self.sql(
            "SELECT kind, posted_ref, posted_at IS NOT NULL AS posted, failed_at IS NOT NULL AS "
            f"failed FROM curie.factory_slack_posts WHERE work_item_id = '{self.work_item_id}' "
            "ORDER BY ordinal"
        )

    def phase_read_back(self) -> None:
        kit = self.require_kit()

        def terminal_settled() -> bool:
            rows = self.post_rows()
            return any(row.get("kind") == sks.TERMINAL_POST for row in rows) and all(
                row.get("posted") or row.get("failed") for row in rows
            )

        with contextlib.suppress(StepFailed):
            self.wait_until(
                terminal_settled, seconds=TERMINAL_POST_SECONDS, what="the terminal post", every=5
            )
        rows = self.post_rows()
        messages = kit.sender.replies(kit.channel, self.kickoff_ts)
        publications = self.sql(
            "SELECT p.id FROM curie.publications p JOIN curie.execution_requests r ON "
            f"p.execution_request_id = r.id WHERE r.work_item_id = '{self.work_item_id}'"
        )
        pull_request = False
        for row in publications:
            record = self.publication(sks.sql_uuid(row.get("id")))
            self.track_publication(record)
            pull_request = pull_request or bool(record.get("pr_number") or record.get("pr_url"))
        self.ledger.record("progress", "thread", self.slack_thread())
        self.ledger.record(
            "progress",
            "posts",
            [
                {
                    "kind": row.get("kind"),
                    "ref_digest": sks.ref_digest(str(row.get("posted_ref") or "")),
                    "posted": bool(row.get("posted")),
                    "failed": bool(row.get("failed")),
                }
                for row in rows
            ],
        )
        self.ledger.record(
            "progress", "thread_message_digests", sks.thread_digests(messages, self.kickoff_ts)
        )
        self.ledger.record(
            "progress", "foreign_messages", sks.foreign_messages(messages, self.kickoff_ts)
        )
        self.ledger.record(
            "progress", "root_posts", sum(1 for row in rows if row.get("kind") == sks.ROOT_POST)
        )
        self.ledger.record(
            "progress",
            "kickoff_reply_outcome",
            sks.reply_outcome(messages, author=kit.curie_auth.user_id),
        )
        self.ledger.record("progress", "pull_request_exists", pull_request)

    def publication(self, publication_id: str) -> dict[str, Any]:
        value = self.require_api().get(f"/publications/{publication_id}")
        return value if isinstance(value, dict) else {}

    # -- phase: the disconnected control ----------------------------------------------------

    def phase_control(self) -> None:
        kit = self.require_kit()
        code, _body = self.subscription("disconnect")
        _code, status = self.subscription("status")
        self.ledger.record("refusal", "disconnect", {"exit": code, "state": status.get("state")})
        if code != 0:
            raise StepFailed(f"subscription disconnect exited {code}")

        def settled() -> bool:
            _rc, current = self.subscription("status")
            teardown = current.get("teardown") or {}
            return (
                current.get("state") == "inactive"
                and teardown.get("pending_runtimes") == 0
                and teardown.get("pending_custody_cleanup") == 0
            )

        self.wait_until(settled, seconds=TEARDOWN_SECONDS, what="the enrollment teardown", every=5)
        # Inactive with no runtime left: the owner's grant can start nothing more.
        self.guard_starts = False
        _rc, status = self.subscription("status")
        self.ledger.record("refusal", "state_before_control", status.get("state"))
        self.ledger.record("refusal", "teardown_settled", True)
        self.ledger.record("refusal", "expected_outcome", sks.ENROLLMENT_INACTIVE)
        before = self.counts()
        seen_pods = {pod.uid for pod in self.followed()}
        self.ledger.record("refusal", "before", before)
        ts = kit.sender.post(
            kit.channel, sks.kickoff_text(kit.curie_auth.user_id, self.issues["b"]["url"])
        )
        self.roots.append(ts)
        self.observe_delivery("control", ts)
        outcome: dict[str, str | None] = {"code": None}

        def replied() -> bool:
            outcome["code"] = sks.reply_outcome(
                kit.sender.replies(kit.channel, ts), author=kit.curie_auth.user_id
            )
            return outcome["code"] is not None

        with contextlib.suppress(StepFailed):
            self.wait_until(replied, seconds=REPLY_SECONDS, what="the control reply", every=3)
        self.ledger.record("refusal", "control_reply_outcome", outcome["code"])
        # Anything the refused kickoff could have started would be visible by now.
        self.clock.wait(CONTROL_QUIET_SECONDS)
        after = self.counts()
        claims = self.kube.json("get", CLAIM_RESOURCE, "-l", f"{CLAIM_AGENT_LABEL}={self.agent}")
        new_pods = 0
        for claim in (claims or {}).get("items") or []:
            name = str((claim.get("metadata") or {}).get("name") or "")
            pods = self.kube.json("get", "pods", "-l", f"{POD_CLAIM_LABEL}={name}") if name else {}
            new_pods += sum(
                1
                for pod in (pods or {}).get("items") or []
                if str((pod.get("metadata") or {}).get("uid")) not in seen_pods
            )
        self.ledger.record("refusal", "after", after)
        self.ledger.record("refusal", "control_native_sessions", new_pods)

    def phase_fence_rehearsal(self) -> None:
        self.fence_paid_starts()
        self.ledger.record("rehearsal", "fence_exercised", True)

    def track_publication(self, record: Mapping[str, Any]) -> None:
        """Remember a publication's branch; a head that is not a commit stays unproved."""

        branch = str(record.get("branch") or "")
        head = str(record.get("lineage_head_sha") or "")
        if not branch:
            return
        if re.fullmatch(r"[0-9a-f]{40}", head):
            self.publication_branches[branch] = head
        else:
            self.publication_branches.setdefault(branch, "")

    def recover_publications(self) -> None:
        """Merge every publication of this run's agent into the branch map, once.

        Before the attempt went live no publication can exist. After it, a failed
        enumeration leaves the forge state uncertain, so cleanup fails and the
        inventory cannot report absent.
        """

        if self.publications_recovered:
            return
        self.publications_recovered = True
        if self.ledger.run_row()["live_started_at"] is None:
            return
        try:
            if not self.agent_id:
                raise StepFailed("the agent id is unknown")
            rows = self.sql(
                "SELECT p.id FROM curie.publications p JOIN curie.execution_requests r ON "
                "p.execution_request_id = r.id JOIN curie.work_items w ON r.work_item_id = w.id "
                f"WHERE w.agent_id = '{self.agent_id}'"
            )
            for row in rows:
                self.track_publication(self.publication(sks.sql_uuid(row.get("id"))))
        except Exception as error:  # noqa: BLE001 - any failure leaves the forge uncertain
            self.publications_uncertain = True
            raise StepFailed("this run's publications could not be enumerated") from error

    def require_publications_known(self) -> None:
        self.recover_publications()
        if self.publications_uncertain:
            raise StepFailed("this run's publications are uncertain; forge objects left in place")

    # -- cleanup steps ------------------------------------------------------------------------

    def close_run_pulls(self) -> None:
        """Close an open pull request only while its head is this run's published commit."""

        self.require_publications_known()
        forge = self.require_forge()
        skipped = 0
        for branch, head in sorted(self.publication_branches.items()):
            if not head:
                skipped += 1
                continue
            pulls = forge.call(
                "GET",
                forge.repo_path(
                    f"/pulls?state=open&head={urllib.parse.quote(forge.repo.split('/')[0])}:"
                    f"{urllib.parse.quote(branch)}"
                ),
            )
            for pull in pulls or []:
                if not isinstance(pull, dict):
                    continue
                if str((pull.get("head") or {}).get("sha")) != head:
                    skipped += 1
                    continue
                forge.call(
                    "PATCH", forge.repo_path(f"/pulls/{pull.get('number')}"), {"state": "closed"}
                )
        if skipped:
            raise StepFailed(
                "an open run pull request is not provably at this run's commit; left open"
            )

    def delete_run_branches(self) -> None:
        """Delete a branch only while its head is the commit this run's publication made."""

        self.require_publications_known()
        forge = self.require_forge()
        skipped = 0
        for branch, head in sorted(self.publication_branches.items()):
            if not head:
                skipped += 1
                continue
            status, current = forge.request("GET", forge.repo_path(f"/git/ref/heads/{branch}"))
            if status == 404:
                continue
            if str(((current or {}).get("object") or {}).get("sha")) != head:
                skipped += 1
                continue
            forge.call("DELETE", forge.repo_path(f"/git/refs/heads/{branch}"), ok=(204,))
        if skipped:
            raise StepFailed("a run branch is not provably at this run's commit; left in place")

    def delete_slack_messages(self) -> None:
        """Every message in the run's threads, each deleted with the token that posted it.

        Curie's replies and progress posts go with the Curie bot token, the
        sender's roots last with the sender token. A message by anyone else is
        never touched and fails the step.
        """

        kit = self.require_kit()
        foreign = 0
        for root in self.roots:
            try:
                messages = kit.sender.replies(kit.channel, root)
            except sks.SlackError as error:
                if error.code != "thread_not_found":
                    raise
                messages = []
            for message in messages:
                ts = message.get("ts")
                if not isinstance(ts, str) or ts == root:
                    continue
                if message.get("user") == kit.curie_auth.user_id:
                    kit.curie.delete(kit.channel, ts)
                elif message.get("user") == kit.sender_auth.user_id:
                    kit.sender.delete(kit.channel, ts)
                else:
                    foreign += 1
            kit.sender.delete(kit.channel, root)
        if foreign:
            raise StepFailed("a thread holds a message the run did not post; left in place")
        if not self.slack_messages_absent():
            raise StepFailed("a run message is still present")

    def slack_messages_absent(self) -> bool:
        kit = self.require_kit()
        for root in self.roots:
            if kit.sender.message_present(kit.channel, root):
                return False
            try:
                messages = kit.sender.replies(kit.channel, root)
            except sks.SlackError as error:
                if error.code == "thread_not_found":
                    continue
                raise
            if any(message.get("ts") != root for message in messages):
                return False
        return True

    # -- finalizer inventory ------------------------------------------------------------------

    def query_inventory(self) -> dict[str, str]:
        checks: dict[str, Callable[[], bool]] = {
            "namespace": lambda: self.namespace_absent(self.inputs.namespace),
            "grant_namespace": lambda: self.namespace_absent(self.inputs.grant_namespace),
            "cluster_scoped": self.cluster_scoped_absent,
            "registry_container": lambda: not self.registry.listed(),
            "local_images": lambda: local_images_absent(
                self.shell, self.builder.local_tags if self.builder else []
            ),
            "processes": lambda: all(
                process.poll() is not None for process in tuple(self.processes)
            ),
            "work_dir": lambda: not self.inputs.work_dir.exists(),
            "forge_issues": self.issues_closed,
            "forge_prs": self.pulls_closed,
            "forge_branches": self.branches_absent,
            "slack_messages": lambda: self.kit is None or self.slack_messages_absent(),
            "dispatcher_pods": self.dispatcher_pods_absent,
            "volumes": lambda: all(self.volume(name) is None for name in self.volumes),
        }
        results: dict[str, str] = {}
        for kind, check in checks.items():
            try:
                results[kind] = "absent" if check() else "present"
            except Exception:  # noqa: BLE001 - an unanswered query is a failure
                results[kind] = "uncertain"
        return results

    def namespace_absent(self, name: str) -> bool:
        result = self.kube.ctl(
            "get", "namespace", name, "--ignore-not-found", "-o", "name", namespaced=False
        )
        return not result.stdout.strip()

    def cluster_scoped_absent(self) -> bool:
        listed = self.kube.json("get", CLUSTER_SCOPED_KINDS, namespaced=False)
        release = self.inputs.release
        for item in (listed or {}).get("items") or []:
            meta = item.get("metadata") or {}
            annotations = meta.get("annotations") or {}
            if annotations.get("meta.helm.sh/release-name") == release or str(
                meta.get("name", "")
            ).startswith(f"{release}-"):
                return False
        return True

    def issues_closed(self) -> bool:
        if not self.issues:
            return True
        forge = self.require_forge()
        for issue in self.issues.values():
            current = forge.call("GET", forge.repo_path(f"/issues/{issue['number']}"))
            if not isinstance(current, dict) or current.get("state") != "closed":
                return False
        return True

    def pulls_closed(self) -> bool:
        self.require_publications_known()
        if not self.publication_branches:
            return True
        forge = self.require_forge()
        owner = forge.repo.split("/")[0]
        for branch in self.publication_branches:
            pulls = forge.call(
                "GET",
                forge.repo_path(
                    f"/pulls?state=open&head={urllib.parse.quote(owner)}:"
                    f"{urllib.parse.quote(branch)}"
                ),
            )
            if pulls:
                return False
        return True

    def branches_absent(self) -> bool:
        self.require_publications_known()
        if not self.publication_branches:
            return True
        forge = self.require_forge()
        for branch in self.publication_branches:
            status, _ = forge.request("GET", forge.repo_path(f"/git/ref/heads/{branch}"))
            if status != 404:
                return False
        return True


# Sent into the runner container at a fence: SIGINT to each native CLI process,
# the same interrupt a terminal gives it, before the pod is deleted.
INTERRUPT_PROGRAM = r"""
import os, signal
for pid in os.listdir("/proc"):
    if not pid.isdigit():
        continue
    try:
        with open(os.path.join("/proc", pid, "cmdline"), "rb") as handle:
            argv = handle.read().split(b"\0")
    except OSError:
        continue
    head = [os.path.basename(arg.decode("utf-8", "replace")) for arg in argv[:2]]
    if "claude" in head or any(b"claude-code" in arg for arg in argv[:2]):
        try:
            os.kill(int(pid), signal.SIGINT)
        except OSError:
            pass
"""


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

COMMANDS: dict[str, Callable[[argparse.Namespace, Runtime], int]] = {
    "images": cmd_images,
    "slack-probe": cmd_slack_probe,
    "preflight": cmd_preflight,
    "rehearse": cmd_rehearse,
    "run": cmd_run,
    "judge": cmd_judge,
}


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Clock | None = None,
    slack_transport: sks.SlackTransport | None = None,
) -> int:
    environ = dict(os.environ if environ is None else environ)
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["--live"]:
        # The ticket's literal invocation: `personal_slack_kickoff.py --live ...` is `run`.
        arguments = ["run", *arguments[1:]]
    scrubber = sks.Scrubber()
    scan = sks.TokenScan(scrubber, SCAN_CHANNELS)
    runtime = Runtime(
        environ, Output(scrubber, scan), scrubber, scan, clock or Clock(), slack_transport
    )
    try:
        sks.refuse_ambient_credentials(environ)
        args = build_parser().parse_args(arguments)
        return COMMANDS[args.command](args, runtime)
    except sks.Refusal as refusal:
        runtime.out.emit(sks.refusal_payload(refusal))
        return sks.EXIT_REFUSED
    except StepFailed as failure:
        runtime.out.emit({"error": "step_failed", "detail": str(failure)})
        return sks.EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
