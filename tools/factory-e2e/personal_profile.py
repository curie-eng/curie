#!/usr/bin/env python3
"""Single-profile factory acceptance driver for personal subscription execution (#5375).

Six subcommands share one private SQLite ledger under ``--state-dir``:

1. ``images`` (unpaid) builds the candidate's api, worker and runner images
   from ``git archive <candidate>`` with the OCI revision label, pushes them to
   the loopback registry, and builds the dark-factory runner layer from a
   private bundle copy.
2. ``preflight`` (unpaid) refuses service credentials, fake mode, a missing or
   unsafe grant file, unknown expiry, insufficient lifetime, unapproved live
   execution and candidate image mismatch before any model session.
3. ``forge-probe`` proves the App kit can write only run-owned forge objects.
4. ``rehearse`` (unpaid, no grant input) runs every phase before the grant
   against the real cluster, then the full finalizer, under a marked run ID.
5. ``run`` performs exactly one live attempt: install, seed one owner, admit
   the fixture issue twice (rejected then approved publication), observe the
   native transcripts in the runner pod, and finalize every owned resource.
6. ``judge --criterion X`` reads the ledger only and prints a verdict.

No held secret (grant, App token, API key, worker token, operator principal
tokens) is ever printed, logged, persisted or placed in a child's argv. The
grant reaches Kubernetes only through ``kubectl create -f -`` stdin. See
README.md beside this file.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import hashlib
import http.client
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

import personal_profile_support as pps  # noqa: E402

REPO_ROOT = HERE.parents[1]
FIXTURE_ISSUE = HERE / "fixtures" / "personal-profile-issue.md"
SUBCOMMANDS = ("images", "preflight", "forge-probe", "rehearse", "run", "judge")
DOCKERFILES = {
    "curie-api": "apps/api/Dockerfile",
    "curie-worker": "apps/worker/Dockerfile",
    "curie-runner": "runner/Dockerfile",
}
BUNDLE_SOURCE = "examples/dark-factory"
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
CLAIM_AGENT_LABEL = "curietech.ai/agent"
POD_CLAIM_LABEL = "sandbox.users.io/curie-admission-claim"
CONTROLLER_NAMESPACE = "agent-sandbox-system"
CONTROLLER_DEPLOYMENT = "agent-sandbox-controller"
EXECUTION_DEADLINE = 1800
POLL_SECONDS = 5.0
REJECTION_SETTLE_SECONDS = 30.0
# The last phase a rehearsal runs: everything unpaid, nothing from the grant on.
REHEARSAL_CUT = "forge"
# Node and driver clocks may differ slightly; replayed history is minutes older.
REPLAY_MARGIN_SECONDS = 30.0
CI_WAIT_SECONDS = 1200.0
PUBLICATION_WAIT_SECONDS = 600.0
CLUSTER_SCOPED_KINDS = (
    "clusterroles,clusterrolebindings,priorityclasses,validatingadmissionpolicies,"
    "validatingadmissionpolicybindings,persistentvolumes"
)
USER_AGENT = "curie-personal-profile-acceptance"


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

    def __init__(self, scrubber: pps.Scrubber, scan: pps.TokenScan) -> None:
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
        print(f"[personal-profile] {self.scrubber.scrub(message)}", file=sys.stderr, flush=True)


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
        scrubber: pps.Scrubber,
        scan: pps.TokenScan,
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
    ) -> None:
        env = {**self.env, **(extra_env or {})}
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

    def record(
        self,
        argv: Sequence[str],
        result: Completed,
        *,
        sensitive: bool = False,
        full: bool = False,
        withhold_stdout: bool = False,
    ) -> None:
        """Append one scrubbed entry to the private command log.

        ``full`` keeps the whole stdout and stderr (the approvals CLI writes its
        ``--json`` error to stdout); ``withhold_stdout`` keeps only its size.
        """

        if self.log is None:
            return
        entry: dict[str, Any] = {
            "argv": [self.scrubber.scrub(arg) for arg in argv[:6]],
            "exit": result.returncode,
        }
        if sensitive:
            entry["stderr_tail"] = ""
        elif full:
            entry["stdout"] = (
                f"<withheld: {len(result.stdout)} bytes>"
                if withhold_stdout
                else self.scrubber.scrub(result.stdout.decode(errors="replace"))
            )
            entry["stderr"] = self.scrubber.scrub(result.stderr.decode(errors="replace"))
        else:
            entry["stderr_tail"] = self.scrubber.scrub(
                result.stderr[-800:].decode(errors="replace")
            )
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
        full_output: bool = False,
        record_output: bool = True,
    ) -> Completed:
        self.guard(
            argv, stdin=stdin, extra_env=extra_env, secret_env=secret_env, secret_stdin=secret_stdin
        )
        try:
            completed = subprocess.run(
                list(argv),
                input=stdin,
                stdin=subprocess.DEVNULL if stdin is None else None,
                capture_output=True,
                timeout=timeout,
                cwd=self.cwd,
                env={**self.env, **(extra_env or {})},
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise StepFailed(f"{Path(argv[0]).name} could not complete") from None
        result = Completed(completed.returncode, completed.stdout, completed.stderr)
        if not sensitive_output:
            self.scan.scan("children", result.stdout)
            self.scan.scan("children", result.stderr)
        if record_output:
            self.record(argv, result, sensitive=sensitive_output, full=full_output)
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
        self,
        host: str,
        port: int,
        *,
        tls: bool,
        scan: pps.TokenScan,
        timeout: float = 30.0,
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


# --------------------------------------------------------------------------
# Finalizer and signals
# --------------------------------------------------------------------------


class Finalizer:
    """LIFO cleanup registered before each resource is created. Every step runs."""

    def __init__(self, ledger: pps.Ledger | None) -> None:
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
        prog="personal_profile.py",
        description="Single-profile factory acceptance driver (#5375).",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in SUBCOMMANDS:
        sub = commands.add_parser(name)
        sub.add_argument("--candidate", default="")
        sub.add_argument("--state-dir", default="")
        sub.add_argument("--run-id", default="")
        for flag in (
            "--context",
            "--namespace",
            "--release",
            "--registry",
            "--docker-context",
            "--kubeconfig",
            "--app-dir",
        ):
            sub.add_argument(flag, default="")
        # A rehearsal takes no grant input at all; None tells "supplied" from "absent".
        grant_default = None if name == "rehearse" else ""
        sub.add_argument("--grant-file", default=grant_default)
        sub.add_argument("--grant-expires-at", default=grant_default)
        if name in {"preflight", "run", "rehearse"}:
            sub.add_argument("--approve-live", default=None)
        if name == "judge":
            sub.add_argument("--criterion", required=True, choices=[*pps.CRITERIA, "all"])
    return parser


@dataclasses.dataclass(frozen=True)
class Inputs:
    candidate: str
    run_id: str
    state_dir: Path
    context: str = ""
    namespace: str = ""
    release: str = ""
    registry: str = ""
    registry_port: int = 0
    docker_context: str = ""
    kubeconfig: str = ""
    app_dir: Path | None = None
    app: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    grant_file: Path | None = None
    expires_at: float = 0.0

    @property
    def work_dir(self) -> Path:
        return self.state_dir / f"{self.run_id}.work"

    @property
    def command_log(self) -> Path:
        return self.state_dir / f"{self.run_id}.commands.jsonl"

    @property
    def fullname(self) -> str:
        return pps.chart_fullname(self.release)


def base_inputs(args: argparse.Namespace) -> Inputs:
    candidate = pps.require_candidate(args.candidate)
    run_id = pps.require_run_id(args.run_id)
    if not args.state_dir:
        raise pps.Refusal("state_dir_unsafe", "--state-dir")
    state_dir = pps.require_private_dir(Path(args.state_dir))
    return Inputs(candidate=candidate, run_id=run_id, state_dir=state_dir)


def with_cluster(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    if not args.context:
        raise pps.Refusal("arguments_invalid", "--context")
    namespace = pps.require_dns_name(args.namespace, "--namespace", limit=63)
    release = pps.require_release(args.release)
    kubeconfig = Path(args.kubeconfig) if args.kubeconfig else None
    if kubeconfig is None or not kubeconfig.is_file():
        raise pps.Refusal("arguments_invalid", "--kubeconfig")
    if not args.docker_context:
        raise pps.Refusal("arguments_invalid", "--docker-context")
    _address, port = pps.require_registry(args.registry)
    return dataclasses.replace(
        inputs,
        context=args.context,
        namespace=namespace,
        release=release,
        kubeconfig=str(kubeconfig),
        docker_context=args.docker_context,
        registry=f"localhost:{port}",
        registry_port=port,
    )


def with_app(inputs: Inputs, args: argparse.Namespace) -> Inputs:
    if not args.app_dir:
        raise pps.Refusal("app_kit_invalid", "--app-dir")
    app_dir = Path(args.app_dir)
    return dataclasses.replace(inputs, app_dir=app_dir, app=pps.require_app_kit(app_dir))


def with_grant(inputs: Inputs, args: argparse.Namespace, now: float) -> Inputs:
    grant_file = Path(args.grant_file) if args.grant_file else None
    pps.check_grant_file(grant_file)
    expires_at = pps.parse_expiry(args.grant_expires_at)
    pps.require_lifetime(expires_at, now)
    return dataclasses.replace(inputs, grant_file=grant_file, expires_at=expires_at)


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


# --------------------------------------------------------------------------
# Adapters: kubectl, helm, curie, the api, the forge, the registry
# --------------------------------------------------------------------------


class Kube:
    def __init__(self, shell: Shell, context: str, namespace: str) -> None:
        self.shell = shell
        self.context = context
        self.namespace = namespace

    def ctl(self, *args: str, namespaced: bool = True, **kwargs: Any) -> Completed:
        argv = ["kubectl", "--context", self.context]
        if namespaced:
            argv += ["--namespace", self.namespace]
        return self.shell.run([*argv, *args], **kwargs)

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
        if verb == "approvals":
            # Every approvals call keeps its whole output in the private log: the
            # CLI writes a refusal's API detail to stdout under --json.
            kwargs.setdefault("full_output", True)
        return self.shell.run(self.argv(verb, *args), **kwargs)

    def mint_operator_principal(self, agent: str, subject: str) -> pps.HeldSecret:
        """Mint one operator principal; the log keeps its output with the token redacted."""

        args = (agent, "--mint-operator-principal", subject)
        result = self.run(
            "approvals", *args, check=False, sensitive_output=True, record_output=False
        )
        token = pps.minted_token(result.stdout) if result.returncode == 0 else None
        held = self.shell.scrubber.hold(pps.HeldSecret(token)) if token else None
        self.shell.record(
            self.argv("approvals", *args),
            result,
            full=True,
            withhold_stdout=result.returncode == 0 and held is None,
        )
        if result.returncode != 0:
            raise StepFailed(f"operator principal mint exited {result.returncode}")
        if held is None:
            raise StepFailed("operator principal mint returned no token")
        return held

    def json(self, verb: str, *args: str, **kwargs: Any) -> Any:
        result = self.run(verb, *args, **kwargs)
        try:
            return json.loads(result.stdout)
        except ValueError:
            raise StepFailed(f"curie cluster {verb} returned unreadable JSON") from None


class CurieApi:
    """The api through the run's own port-forward: X-API-Key or the internal worker token."""

    def __init__(self, http: JsonHttp, api_key: pps.HeldSecret, worker_token: pps.HeldSecret):
        self.http = http
        self.api_key = api_key
        self.worker_token = worker_token

    def __repr__(self) -> str:
        return "<curie api>"

    def get(self, path: str) -> Any:
        status, body = self.http.request("GET", path, headers={"X-API-Key": self.api_key.reveal()})
        if status != 200:
            raise StepFailed(f"api GET returned {status}")
        return body

    def post(self, path: str, body: Any, *, internal: bool = False) -> tuple[int, Any]:
        headers = (
            {"X-Curie-Worker-Token": self.worker_token.reveal()}
            if internal
            else {"X-API-Key": self.api_key.reveal()}
        )
        return self.http.request("POST", path, headers=headers, body=body)


class Forge:
    """The fixture repository through a GitHub App installation token (never a user login)."""

    def __init__(
        self,
        shell: Shell,
        http: JsonHttp,
        app_dir: Path,
        app: Mapping[str, Any],
        scrubber: pps.Scrubber,
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
        self.token: pps.HeldSecret | None = None
        self.expires_at = 0.0

    def __repr__(self) -> str:
        return "<forge>"

    def jwt(self) -> pps.HeldSecret:
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
        return self.scrubber.hold(pps.HeldSecret(f"{header}.{payload}.{segment(signed.stdout)}"))

    def mint(self) -> None:
        jwt = self.jwt()
        name = self.repo.split("/", 1)[1]
        status, body = self.http.request(
            "POST",
            f"/app/installations/{self.installation_id}/access_tokens",
            headers=self._headers(jwt),
            body={"repositories": [name], "permissions": pps.FORGE_PERMISSIONS},
            sensitive=True,
        )
        jwt.clear()
        token = body.get("token") if isinstance(body, dict) else None
        granted = body.get("permissions") if isinstance(body, dict) else None
        if status != 201 or not isinstance(token, str) or not isinstance(granted, dict):
            raise StepFailed(f"installation token request returned {status}")
        if any(granted.get(key) != level for key, level in pps.FORGE_PERMISSIONS.items()):
            raise StepFailed("installation token permissions differ from the requested set")
        if self.token is not None:
            self.token.clear()
        self.token = self.scrubber.hold(pps.HeldSecret(token))
        self.expires_at = self.clock.now() + 3000

    def app_identity(self) -> tuple[str, int]:
        jwt = self.jwt()
        status, body = self.http.request("GET", "/app", headers=self._headers(jwt))
        jwt.clear()
        slug = body.get("slug") if isinstance(body, dict) else None
        if status != 200 or not isinstance(slug, str):
            raise StepFailed("App identity could not be read")
        user = self.call("GET", f"/users/{urllib.parse.quote(slug + '[bot]')}")
        bot_id = user.get("id") if isinstance(user, dict) else None
        if not isinstance(bot_id, int):
            raise StepFailed("App bot user could not be read")
        return slug, bot_id

    @staticmethod
    def _headers(credential: pps.HeldSecret, scheme: str = "Bearer") -> dict[str, str]:
        return {
            "Authorization": f"{scheme} {credential.reveal()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if self.token is None or not self.token.reveal() or self.clock.now() > self.expires_at:
            self.mint()
        assert self.token is not None
        return self.http.request(
            method, path, headers=self._headers(self.token, "token"), body=body
        )

    def call(self, method: str, path: str, body: Any = None, *, ok: Sequence[int] = (200,)) -> Any:
        status, payload = self.request(method, path, body)
        if status not in ok:
            raise StepFailed(f"forge {method} returned {status}")
        return payload

    def repo_path(self, suffix: str) -> str:
        return f"/repos/{self.repo}{suffix}"

    def matching_branches(self, prefix: str) -> list[str]:
        refs = self.call("GET", self.repo_path(f"/git/matching-refs/heads/{prefix}"))
        return [str(ref.get("ref")) for ref in refs or [] if isinstance(ref, dict)]

    def prefix_pulls(self, prefix: str) -> list[dict[str, Any]]:
        pulls = self.call("GET", self.repo_path("/pulls?state=all&per_page=100"))
        return [
            pull
            for pull in pulls or []
            if isinstance(pull, dict)
            and str(((pull.get("head") or {}).get("ref")) or "").startswith(prefix)
        ]

    def pull_files(self, number: int) -> tuple[list[str], bool]:
        """Every path a pull request changes, and whether the listing was complete.

        ``GET /repos/{owner}/{repo}/pulls/{n}/files`` pages at most 100 entries and
        lists at most 3000; a full last page past that limit is incomplete.
        """

        paths: list[str] = []
        for page in range(1, 31):
            rows = self.call(
                "GET", self.repo_path(f"/pulls/{number}/files?per_page=100&page={page}")
            )
            rows = [row for row in rows or [] if isinstance(row, dict)]
            paths += [str(row.get("filename")) for row in rows if row.get("filename")]
            if len(rows) < 100:
                return sorted(set(paths)), True
        return sorted(set(paths)), False

    def revoke(self) -> None:
        if self.token is not None and self.token.reveal():
            self.http.request(
                "DELETE", "/installation/token", headers=self._headers(self.token, "token")
            )
            self.token.clear()


def forge_http(scan: pps.TokenScan) -> JsonHttp:
    return JsonHttp(pps.GITHUB_API_HOST, 443, tls=True, scan=scan)


def registry_delete(port: int, repository: str, digest: str) -> int:
    """``DELETE /v2/<name>/manifests/<digest>`` (Registry HTTP API v2; deletes must be enabled)."""

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


# --------------------------------------------------------------------------
# images (unpaid): candidate images and the private bundle's runner layer
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
        ledger: pps.Ledger,
        finalizer: Finalizer,
        out: Output,
        curie_bin: str,
    ) -> None:
        self.inputs = inputs
        self.shell = shell
        self.ledger = ledger
        self.finalizer = finalizer
        self.out = out
        self.curie_bin = curie_bin
        self.get = pps.loopback_get("127.0.0.1", inputs.registry_port)

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

    def build(self) -> dict[str, Any]:
        inputs = self.inputs
        work = private_dir(inputs.work_dir)
        source = work / "source"
        archive = work / "source.tar"
        self.finalizer.register("remove-source-archive", lambda: shutil.rmtree(source, True))
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
        tag = pps.image_tag(inputs.candidate, inputs.run_id)
        pushed: list[Pushed] = []
        local_tags: list[str] = []
        self.ledger.record("images", "local_tags", local_tags)
        for name, dockerfile in DOCKERFILES.items():
            ref = f"{inputs.registry}/{name}:{tag}"
            local_tags.append(ref)
            self.ledger.record("images", "local_tags", local_tags)
            self.shell.run(
                [
                    "docker",
                    "build",
                    "--platform",
                    platform,
                    "--label",
                    f"{pps.REVISION_LABEL}={inputs.candidate}",
                    "-f",
                    str(source / dockerfile),
                    "-t",
                    ref,
                    str(source),
                ],
                timeout=3600,
            )
            self.shell.run(["docker", "push", ref], timeout=1800)
            image = pps.registry_image(self.get, name, tag)
            if image is not None:
                pushed.append(Pushed(name, name, tag, image.digest))
                self.ledger.record("images", "pushed", [dataclasses.asdict(p) for p in pushed])
            if image is None or image.revision != inputs.candidate:
                raise pps.Refusal("candidate_image_mismatch", name)
        bundle = work / "bundle"
        shutil.rmtree(bundle, ignore_errors=True)
        shutil.copytree(source / BUNDLE_SOURCE, bundle, symlinks=True)
        manifest_path = bundle / ".claude-plugin" / "plugin.json"
        manifest = json.loads(manifest_path.read_text())
        manifest_path.write_text(json.dumps(pps.private_bundle_manifest(manifest), indent=2) + "\n")
        hooks_path = bundle / "hooks" / "hooks.json"
        hooks_path.write_text(
            json.dumps(pps.private_bundle_hooks(json.loads(hooks_path.read_text())), indent=2)
            + "\n"
        )
        (bundle / "hooks" / pps.RESERVATION_GATE_FILE).write_text(pps.RESERVATION_GATE_PROGRAM)
        # The layer repository must hold nothing before this run builds into it, so
        # every tag found there later is this run's. It is recorded before the build.
        repository = pps.layer_repository(inputs.registry, str(manifest.get("name") or ""))
        if repository_local_tags(self.shell, repository) or repository_registry_tags(
            inputs.registry_port, repository
        ):
            raise pps.Refusal("layer_repository_not_owned", repository)
        self.ledger.record("images", "layer_repository", repository)
        layer = self.build_layer(bundle, platform, tag, pushed, local_tags)
        if layer.revision != inputs.candidate:
            layer_file = bundle / "runner.Dockerfile"
            layer_file.write_text(
                layer_file.read_text() + f"\nLABEL {pps.REVISION_LABEL}={inputs.candidate}\n"
            )
            layer = self.build_layer(bundle, platform, tag, pushed, local_tags)
        if layer.revision != inputs.candidate:
            raise pps.Refusal("candidate_image_mismatch", pps.LAYER_IMAGE)
        self.ledger.record("images", "platform", platform)
        shutil.rmtree(source, ignore_errors=True)
        archive.unlink(missing_ok=True)
        return {"images_pushed": len(pushed)}

    def build_layer(
        self, bundle: Path, platform: str, tag: str, pushed: list[Pushed], local_tags: list[str]
    ) -> pps.RegistryImage:
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
        layer_ref = pps.lock_runner_image(lock.read_text()) if lock.is_file() else None
        if not layer_ref:
            raise pps.Refusal("candidate_image_mismatch", pps.LAYER_IMAGE)
        self.ledger.record("images", "layer_ref", layer_ref)
        repository, reference = pps.split_image_ref(layer_ref)
        # curie build leaves its own local tag, named from the bundle's source
        # digest. Every tag in this repository of the driver-owned registry is
        # this run's, so each is recorded for removal.
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
        for listed_tag in pps.docker_listed_tags(listed.stdout):
            if listed_tag not in local_tags:
                local_tags.append(listed_tag)
        self.ledger.record("images", "local_tags", local_tags)
        image = pps.registry_image(self.get, repository, reference)
        if image is None:
            raise pps.Refusal("candidate_image_mismatch", pps.LAYER_IMAGE)
        pushed.append(Pushed(pps.LAYER_IMAGE, repository, reference, image.digest))
        self.ledger.record("images", "pushed", [dataclasses.asdict(p) for p in pushed])
        return image


def recorded_pushed(ledger: pps.Ledger) -> list[Pushed]:
    value = ledger.observations("images").get("pushed")
    items: list[Pushed] = []
    for item in value if isinstance(value, list) else []:
        if isinstance(item, dict):
            with contextlib.suppress(TypeError):
                items.append(Pushed(**item))
    return items


def registry_targets(
    port: int, pushed: Sequence[Pushed], tags: Sequence[str]
) -> list[tuple[str, str]]:
    """Every (repository, digest) the run put in the registry.

    The recorded digests, plus whatever each tag the driver built names now, so a
    push the driver could not read back is still found and deleted.
    """

    targets = {(item.repository, item.digest) for item in pushed}
    get = pps.loopback_get("127.0.0.1", port)
    for tag in tags:
        repository, reference = pps.split_image_ref(tag)
        status, _headers, body = get(
            f"/v2/{repository}/manifests/{reference}", {"Accept": pps.MANIFEST_ACCEPT}
        )
        if status == 200:
            targets.add((repository, "sha256:" + hashlib.sha256(body).hexdigest()))
    return sorted(targets)


def repository_local_tags(shell: Shell, repository: str) -> list[str]:
    listed = shell.run(
        ["docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}", repository], timeout=60
    )
    return pps.docker_listed_tags(listed.stdout)


def repository_registry_tags(port: int, repository: str) -> list[str]:
    """``GET /v2/<name>/tags/list`` (Registry HTTP API v2) as full references; 404 is none."""

    name = pps.split_image_ref(f"{repository}:probe")[0]
    status, _headers, body = pps.loopback_get("127.0.0.1", port)(f"/v2/{name}/tags/list", {})
    if status == 404:
        return []
    if status != 200:
        raise StepFailed("registry tag list was not readable")
    tags = json.loads(body).get("tags") or []
    return [f"{repository}:{tag}" for tag in tags if isinstance(tag, str)]


def owned_tags(ledger: pps.Ledger, port: int, shell: Shell | None) -> list[str]:
    """Every tag this run built: recorded tags plus all of its owned layer repository."""

    images = ledger.observations("images")
    tags = [str(tag) for tag in images.get("local_tags") or []]
    repository = images.get("layer_repository")
    if isinstance(repository, str) and repository:
        tags += repository_registry_tags(port, repository)
        if shell is not None:
            tags += repository_local_tags(shell, repository)
    return sorted(set(tags))


def remove_registry_images(port: int, pushed: Sequence[Pushed], tags: Sequence[str]) -> None:
    failures = 0
    for repository, digest in registry_targets(port, pushed, tags):
        status = registry_delete(port, repository, digest)
        failures += status not in {202, 404}
    if failures:
        raise StepFailed("registry refused a manifest delete; enable registry deletes")


def remove_local_images(shell: Shell, tags: Sequence[str]) -> None:
    for tag in tags:
        listed = shell.run(["docker", "image", "ls", "-q", tag], timeout=60)
        if listed.stdout.strip():
            shell.run(["docker", "image", "rm", tag], timeout=120)


# --------------------------------------------------------------------------
# Subcommands
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Runtime:
    environ: Mapping[str, str]
    out: Output
    scrubber: pps.Scrubber
    scan: pps.TokenScan
    clock: Clock


def make_shell(inputs: Inputs, runtime: Runtime) -> Shell:
    env = pps.child_environment(
        runtime.environ,
        kubeconfig=inputs.kubeconfig or None,
        docker_context=inputs.docker_context or None,
    )
    return Shell(
        env=env,
        scrubber=runtime.scrubber,
        scan=runtime.scan,
        cwd=REPO_ROOT,
        log=inputs.command_log,
    )


def cmd_images(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = with_cluster(base_inputs(args), args)
    ledger = pps.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    ledger.ensure_run(inputs.candidate, phase="images")
    shell = make_shell(inputs, runtime)
    finalizer = Finalizer(None)
    builder = ImageBuilder(
        inputs=inputs,
        shell=shell,
        ledger=ledger,
        finalizer=finalizer,
        out=runtime.out,
        curie_bin=curie_binary(runtime.environ),
    )
    try:
        counts = builder.build()
    except BaseException:
        finalizer.register(
            "remove-registry-images",
            lambda: remove_registry_images(
                inputs.registry_port,
                recorded_pushed(ledger),
                owned_tags(ledger, inputs.registry_port, None),
            ),
        )
        finalizer.register(
            "remove-local-images",
            lambda: remove_local_images(shell, owned_tags(ledger, inputs.registry_port, shell)),
        )
        finalizer.run()
        raise
    finalizer.run()
    runtime.out.emit({"command": "images", "verdict": "pass", "counts": counts})
    return pps.EXIT_PASS


def verify_images(inputs: Inputs, ledger: pps.Ledger) -> dict[str, pps.RegistryImage]:
    layer_ref = ledger.observations("images").get("layer_ref")
    refs = pps.candidate_refs(
        inputs.candidate, inputs.run_id, layer_ref if isinstance(layer_ref, str) else None
    )
    get = pps.loopback_get("127.0.0.1", inputs.registry_port)
    return pps.verify_candidate_images(get, inputs.candidate, refs)


def cmd_preflight(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = base_inputs(args)
    pps.require_live_approval(args.approve_live)
    inputs = with_grant(inputs, args, runtime.clock.now())
    inputs = with_app(with_cluster(inputs, args), args)
    ledger = pps.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="preflight")
        verified = verify_images(inputs, ledger)
        ledger.record(
            "images", "preflight_digests", {name: image.digest for name, image in verified.items()}
        )
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
                "candidate_images_match": True,
            },
            "counts": {"images_verified": len(verified)},
        }
    )
    return pps.EXIT_PASS


def cmd_forge_probe(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = with_app(base_inputs(args), args)
    ledger = pps.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    ledger.ensure_run(inputs.candidate, phase="forge-probe")
    shell = make_shell(inputs, runtime)
    assert inputs.app_dir is not None
    forge = Forge(
        shell, forge_http(runtime.scan), inputs.app_dir, inputs.app, runtime.scrubber, runtime.clock
    )
    finalizer = Finalizer(ledger)
    branch = f"ppa-probe-{hashlib.sha256(inputs.run_id.encode()).hexdigest()[:8]}"
    state: dict[str, Any] = {"pull": None, "ref": False}
    results = {
        "forge_push": False,
        "forge_pr_open": False,
        "forge_pr_closed": False,
        "forge_branch_deleted": False,
    }

    def close_pull() -> None:
        if state["pull"] is not None:
            forge.call("PATCH", forge.repo_path(f"/pulls/{state['pull']}"), {"state": "closed"})
            pull = forge.call("GET", forge.repo_path(f"/pulls/{state['pull']}"))
            results["forge_pr_closed"] = isinstance(pull, dict) and pull.get("state") == "closed"
            if not results["forge_pr_closed"]:
                raise StepFailed("probe pull request is not closed")

    def delete_ref() -> None:
        if state["ref"]:
            status, _ = forge.request("DELETE", forge.repo_path(f"/git/refs/heads/{branch}"))
            gone, _ = forge.request("GET", forge.repo_path(f"/git/ref/heads/{branch}"))
            results["forge_branch_deleted"] = status in {204, 422} and gone == 404
            if not results["forge_branch_deleted"]:
                raise StepFailed("probe branch still exists")

    code = pps.EXIT_FAIL
    with signals_to_exit():
        try:
            finalizer.register("revoke-installation-token", forge.revoke)
            forge.mint()
            repo = forge.call("GET", forge.repo_path(""))
            default = str(repo.get("default_branch"))
            head = forge.call("GET", forge.repo_path(f"/git/ref/heads/{default}"))
            head_sha = str((head.get("object") or {}).get("sha"))
            commit = forge.call("GET", forge.repo_path(f"/git/commits/{head_sha}"))
            tree = str((commit.get("tree") or {}).get("sha"))
            finalizer.register("delete-probe-branch", delete_ref)
            finalizer.register("close-probe-pull", close_pull)
            created = forge.call(
                "POST",
                forge.repo_path("/git/commits"),
                {"message": "personal profile forge probe", "tree": tree, "parents": [head_sha]},
                ok=(201,),
            )
            state["ref"] = True
            forge.call(
                "POST",
                forge.repo_path("/git/refs"),
                {"ref": f"refs/heads/{branch}", "sha": created.get("sha")},
                ok=(201,),
            )
            results["forge_push"] = True
            pull = forge.call(
                "POST",
                forge.repo_path("/pulls"),
                {
                    "title": "Personal profile forge probe",
                    "head": branch,
                    "base": default,
                    "body": "Owned probe; closed and deleted by the driver.",
                },
                ok=(201,),
            )
            state["pull"] = pull.get("number")
            results["forge_pr_open"] = isinstance(state["pull"], int)
            code = pps.EXIT_PASS
        except (Exception, SystemExit):  # noqa: BLE001 - deletion steps still run
            code = pps.EXIT_FAIL
        finally:
            with signals_deferred():
                steps = finalizer.run()
            for key, value in results.items():
                ledger.record("forge", key, value)
    passed = code == pps.EXIT_PASS and all(results.values()) and all(ok for _, ok in steps)
    runtime.out.emit(
        {"command": "forge-probe", "verdict": "pass" if passed else "fail", "checks": results}
    )
    return pps.EXIT_PASS if passed else pps.EXIT_FAIL


def cmd_judge(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = base_inputs(args)
    ledger = pps.Ledger.open_existing(inputs.state_dir, inputs.run_id)
    try:
        if ledger.candidate() != inputs.candidate:
            raise pps.Refusal("candidate_mismatch", "--candidate")
        if ledger.is_rehearsal() and args.criterion != "cleanup":
            raise pps.Refusal("rehearsal_ledger", "--criterion")
        result = pps.judge(ledger, args.criterion)
    finally:
        ledger.close()
    runtime.out.emit(result)
    return pps.EXIT_PASS if result["verdict"] == "pass" else pps.EXIT_FAIL


def cmd_run(args: argparse.Namespace, runtime: Runtime) -> int:
    inputs = base_inputs(args)
    pps.require_live_approval(args.approve_live)
    inputs = with_grant(inputs, args, runtime.clock.now())
    inputs = with_app(with_cluster(inputs, args), args)
    ledger = pps.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        if ledger.is_rehearsal():
            raise pps.Refusal("rehearsal_run_id", "--run-id")
        ledger.ensure_run(inputs.candidate, phase="run")
        ledger.refuse_paid_retry()
    except pps.Refusal:
        ledger.close()
        raise
    live = LiveRun(inputs=inputs, runtime=runtime, ledger=ledger)
    return live.execute()


def cmd_rehearse(args: argparse.Namespace, runtime: Runtime) -> int:
    """Every unpaid live phase against the real cluster, then the full finalizer. No grant."""

    supplied = [
        flag
        for flag, value in (
            ("--grant-file", args.grant_file),
            ("--grant-expires-at", args.grant_expires_at),
            ("--approve-live", args.approve_live),
        )
        if value is not None
    ]
    if supplied:
        raise pps.Refusal("arguments_invalid", "rehearse takes no " + ", ".join(supplied))
    inputs = with_app(with_cluster(base_inputs(args), args), args)
    ledger = pps.Ledger.open(inputs.state_dir, inputs.run_id, scrubber=runtime.scrubber)
    try:
        ledger.ensure_run(inputs.candidate, phase="rehearse")
        live = LiveRun(inputs=inputs, runtime=runtime, ledger=ledger, stop_after=REHEARSAL_CUT)
    except pps.Refusal:
        ledger.close()
        raise
    return live.execute()


# --------------------------------------------------------------------------
# run: one live attempt
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Attempt:
    number: int
    reservations: list[pps.Reservation] = dataclasses.field(default_factory=list)
    request_id: str = ""
    sequence: int | None = None
    admitted_at: float = 0.0
    claim: str = ""
    pod: str = ""
    pod_uid: str = ""
    grant_ref: tuple[str, str] | None = None
    harvest: pps.HarvestState | None = None
    process: subprocess.Popen[bytes] | None = None
    eof: threading.Event = dataclasses.field(default_factory=threading.Event)
    approval_id: str = ""
    publication_id: str = ""
    parked_at: float = 0.0
    gone_at: float = 0.0
    facts: pps.TranscriptFacts | None = None


class LiveRun:
    """Explicit phases; a LIFO finalizer registered before each resource is created."""

    def __init__(
        self,
        *,
        inputs: Inputs,
        runtime: Runtime,
        ledger: pps.Ledger,
        shell: Shell | None = None,
        resolver: Callable[[str], list[str]] | None = None,
        stop_after: str | None = None,
    ) -> None:
        self.inputs = inputs
        self.runtime = runtime
        self.ledger = ledger
        self.clock = runtime.clock
        self.out = runtime.out
        self.scrubber = runtime.scrubber
        self.scan = runtime.scan
        self.shell = shell or make_shell(inputs, runtime)
        self.kube = Kube(self.shell, inputs.context, inputs.namespace)
        self.curie = CurieCli(self.shell, curie_binary(runtime.environ), inputs)
        self.resolver = resolver or resolve_ipv4
        self.finalizer = Finalizer(ledger)
        self.mutated = False
        self.phase = "refusals"
        digest = hashlib.sha256(f"{inputs.run_id}:{secrets.token_hex(8)}".encode()).hexdigest()
        self.agent = f"acme-pp-{digest[:8]}"
        self.prefix = f"ppa-{digest[8:16]}/"
        self.trigger_label = f"ppa-unused-{digest[16:24]}"
        self.nonce = secrets.token_hex(8)
        self.owner_subject = f"ppa-owner-{secrets.token_hex(8)}"
        self.allowed_subject = pps.slack_shaped_subject(secrets.token_bytes(8))
        self.unlisted_subject = pps.slack_shaped_subject(secrets.token_bytes(8))
        self.app_secret = f"{inputs.release}-github-app"
        self.grant_secret = f"{inputs.release}-personal-grant"
        self.processes: list[subprocess.Popen[bytes]] = []
        self.pushed: list[Pushed] = []
        self.local_tags: list[str] = []
        self.forge: Forge | None = None
        self.api: CurieApi | None = None
        self.allowed_token: pps.HeldSecret | None = None
        self.unlisted_token: pps.HeldSecret | None = None
        self.agent_id = ""
        self.work_item_id = ""
        self.issue_number: int | None = None
        self.owner: dict[str, Any] = {}
        self.grant_identity: tuple[str, str] | None = None
        self.final_grant_identity: tuple[str, str] | None = None
        self.policy_set = False
        self.digests: dict[str, str] = {}
        self.chart = Path()
        self.repo_id = ""
        self.app_slug = ""
        self.bot_id = 0
        self.attempts: dict[int, Attempt] = {}
        self.foreign_before: dict[str, str] = {}
        self.outcome: dict[str, Any] = {}
        self.signals: list[int] = []
        names = [name for name, _phase in self.phases()]
        if stop_after is not None and stop_after not in names[: names.index("grant")]:
            raise ValueError("a phase cut must stop before the grant is loaded")
        self.stop_after = stop_after
        self.rehearsal = stop_after is not None
        self.command = "rehearse" if self.rehearsal else "run"
        if self.rehearsal:
            ledger.mark_rehearsal()

    def __repr__(self) -> str:
        return "<live run>"

    # -- driver -----------------------------------------------------------

    def phases(self) -> list[tuple[str, Callable[[], None]]]:
        return [
            ("refusals", self.phase_refusals),
            ("namespace", self.phase_namespace),
            ("images", self.phase_images),
            ("install", self.phase_install),
            ("setup", self.phase_setup),
            ("forge", self.phase_forge),
            ("grant", self.phase_grant),
            ("attempt-1", lambda: self.phase_attempt(1)),
            ("old-pod-gone", self.phase_old_pod_gone),
            ("unlisted-control", self.phase_unlisted_control),
            ("reject", self.phase_reject),
            ("attempt-2", lambda: self.phase_attempt(2)),
            ("approve", self.phase_approve),
            ("owner-after", self.phase_owner_after),
        ]

    def execute(self) -> int:
        code = pps.EXIT_FAIL
        with signals_to_exit():
            try:
                for name, phase in self.phases():
                    self.phase = name
                    self.ledger.set_phase(name)
                    self.out.progress(f"phase {name}")
                    phase()
                    if name == self.stop_after:
                        # The paid-start fence is exercised unpaid against the real
                        # install, so a wrong selector or verb fails here, not live.
                        self.fence_paid_starts()
                        self.ledger.record("rehearsal", "fence_exercised", True)
                        break
                code = pps.EXIT_PASS
            except pps.Refusal as refusal:
                self.outcome = refusal.payload()
                code = pps.EXIT_REFUSED
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
        if code == pps.EXIT_PASS and not cleanup_ok:
            code = pps.EXIT_FAIL
        status = {pps.EXIT_PASS: "pass", pps.EXIT_REFUSED: "refused"}.get(code, "fail")
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
        if self.mutated:
            self.ledger.record("cleanup", "token_scan", self.scan.as_json())
        self.ledger.close()
        return code

    def finalize(self) -> bool:
        if not self.mutated:
            self.finalizer.run()
            return True
        started = self.clock.monotonic()
        run = self.ledger.run_row()
        if run["live_started_at"] is not None:
            # Paid execution stops before the live window is closed.
            if self.grant_identity is not None:
                self.safe(self.fence_paid_starts, None)
            self.ledger.end_live(now=self.clock.now())
        steps = self.finalizer.run()
        inventory = self.query_inventory()
        for kind, status in sorted(inventory.items()):
            self.ledger.record_cleanup(f"inventory:{kind}", kind, status)
        after = self.safe(self.foreign_snapshot, {})
        self.ledger.record(
            "cleanup", "foreign_preserved", pps.foreign_preserved(self.foreign_before, after)
        )
        elapsed = self.clock.monotonic() - started
        self.ledger.record("budget", "cleanup_elapsed", round(elapsed, 3))
        if self.forge is not None:
            try:
                self.forge.revoke()
                self.ledger.record_cleanup("step:revoke-installation-token", "forge", "ok")
            except Exception as exc:  # noqa: BLE001 - recorded as a failed step
                self.ledger.record_cleanup(
                    "step:revoke-installation-token", "forge", "failed", type(exc).__name__
                )
                steps.append(("revoke-installation-token", False))
        dump = json.dumps(
            [
                self.ledger.observations(name)
                for name in (*pps.CRITERIA, "images", "forge", "rehearsal", "install")
            ]
            + [self.ledger.cleanup_rows()],
            sort_keys=True,
        )
        self.scan.scan("ledger", dump.encode())
        return all(ok for _, ok in steps) and all(s == "absent" for s in inventory.values())

    @staticmethod
    def safe(call: Callable[[], Any], default: Any) -> Any:
        try:
            return call()
        except Exception:  # noqa: BLE001 - an unanswered query is reported as uncertain
            return default

    def deadline(self) -> float:
        started = self.ledger.run_row()["live_started_at"]
        return float(started) + pps.LIVE_SECONDS if started is not None else self.clock.now()

    def wait_until(
        self, condition: Callable[[], bool], *, until: float, what: str, every: float = POLL_SECONDS
    ) -> None:
        while not condition():
            if self.clock.now() >= until:
                raise StepFailed(f"{what} did not happen in time")
            self.clock.wait(every)

    # -- phase 1: refusals (no mutation) ----------------------------------

    def phase_refusals(self) -> None:
        if not self.rehearsal:
            pps.require_lifetime(self.inputs.expires_at, self.clock.now())
        existing = self.kube.ctl(
            "get",
            "namespace",
            self.inputs.namespace,
            "--ignore-not-found",
            "-o",
            "name",
            namespaced=False,
        )
        if existing.stdout.strip():
            raise pps.Refusal("namespace_exists", "--namespace")
        if recorded_pushed(self.ledger):
            # Images built earlier for this run ID are verified before any mutation.
            verify_images(self.inputs, self.ledger)
        self.foreign_before = self.foreign_snapshot()

    def foreign_snapshot(self) -> dict[str, str]:
        found: dict[str, str] = {}
        system = self.kube.json("get", "namespace", "kube-system", namespaced=False)
        found["namespace/kube-system"] = str((system.get("metadata") or {}).get("uid"))
        crds = self.kube.json("get", "customresourcedefinitions", namespaced=False)
        for item in crds.get("items") or []:
            meta = item.get("metadata") or {}
            if str(meta.get("name", "")).endswith(".agents.x-k8s.io"):
                found[f"crd/{meta.get('name')}"] = str(meta.get("uid"))
        controller = (
            self.kube.ctl(
                "get",
                "deployment",
                CONTROLLER_DEPLOYMENT,
                "--namespace",
                CONTROLLER_NAMESPACE,
                "--ignore-not-found",
                "-o",
                "jsonpath={.metadata.uid}",
                namespaced=False,
            )
            .stdout.decode()
            .strip()
        )
        if controller:
            found["controller"] = controller
        for name in self.reused_priority_classes() or ():
            uid = (
                self.kube.ctl(
                    "get",
                    "priorityclass",
                    name,
                    "--ignore-not-found",
                    "-o",
                    "jsonpath={.metadata.uid}",
                    namespaced=False,
                )
                .stdout.decode()
                .strip()
            )
            if uid:
                found[f"priorityclass/{name}"] = uid
        return found

    def reused_priority_classes(self) -> tuple[str, str] | None:
        value = self.runtime.environ.get("CURIE_FACTORY_PRIORITY_CLASSES", "")
        names = [item.strip() for item in value.split(",") if item.strip()]
        return (names[0], names[1]) if len(names) == 2 else None

    # -- phase 2: namespace -------------------------------------------------

    def phase_namespace(self) -> None:
        self.mutated = True
        self.finalizer.register("delete-namespace", self.delete_namespace)
        self.kube.create(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": self.inputs.namespace,
                    "labels": {pps.MANAGED_BY_LABEL: pps.MANAGED_BY_VALUE},
                    "annotations": {pps.RUN_ANNOTATION: self.inputs.run_id},
                },
            }
        )

    def delete_namespace(self) -> None:
        key = pps.RUN_ANNOTATION.replace(".", "\\.")
        current = self.kube.ctl(
            "get",
            "namespace",
            self.inputs.namespace,
            "--ignore-not-found",
            "-o",
            "jsonpath={.metadata.annotations." + key + "}",
            namespaced=False,
        )
        owner = current.stdout.decode().strip()
        if not owner:
            return
        if owner != self.inputs.run_id:
            raise StepFailed("namespace ownership changed; not deleting")
        self.kube.ctl(
            "delete",
            "namespace",
            self.inputs.namespace,
            "--wait=true",
            "--timeout=240s",
            namespaced=False,
            timeout=260,
        )

    # -- phase 3: images ------------------------------------------------------

    def phase_images(self) -> None:
        self.finalizer.register(
            "remove-registry-images",
            lambda: remove_registry_images(
                self.inputs.registry_port,
                self.pushed,
                owned_tags(self.ledger, self.inputs.registry_port, None),
            ),
        )
        self.finalizer.register(
            "remove-local-images",
            lambda: remove_local_images(
                self.shell, owned_tags(self.ledger, self.inputs.registry_port, self.shell)
            ),
        )
        self.finalizer.register("remove-work-dir", self.remove_work_dir)
        images = self.ledger.observations("images")
        self.pushed = recorded_pushed(self.ledger)
        self.local_tags = [str(tag) for tag in images.get("local_tags") or []]
        previous = images.get("preflight_digests")
        reused = (
            bool(self.pushed)
            and isinstance(previous, dict)
            and previous == {item.name: item.digest for item in self.pushed}
        )
        if not reused:
            builder = ImageBuilder(
                inputs=self.inputs,
                shell=self.shell,
                ledger=self.ledger,
                finalizer=self.finalizer,
                out=self.out,
                curie_bin=self.curie.binary,
            )
            try:
                builder.build()
            finally:
                self.pushed = recorded_pushed(self.ledger)
                self.local_tags = [
                    str(tag) for tag in self.ledger.observations("images").get("local_tags") or []
                ]
        verified = verify_images(self.inputs, self.ledger)
        self.digests = {name: image.digest for name, image in verified.items()}
        self.ledger.record("images", "images_reused", reused)

    def remove_work_dir(self) -> None:
        if self.inputs.work_dir.exists():
            shutil.rmtree(self.inputs.work_dir)

    # -- phase 4: install -------------------------------------------------------

    def phase_install(self) -> None:
        inputs = self.inputs
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
        assert inputs.app_dir is not None
        pem = (inputs.app_dir / "app.pem").read_bytes()
        key = self.scrubber.hold(pps.HeldSecret(pem.decode("ascii", errors="replace")))
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
        values = pps.install_values(
            release=inputs.release,
            registry=inputs.registry,
            digests=self.digests,
            agent=self.agent,
            repo=str(inputs.app["repo"]),
            app_id=str(inputs.app["id"]),
            app_secret=self.app_secret,
            trigger_label=self.trigger_label,
            priority_classes=self.reused_priority_classes(),
            controller_deploy=not controller,
            model_addresses=self.resolver("api.anthropic.com"),
        )
        values_file = work / "values.json"
        self.finalizer.register("delete-values-file", lambda: values_file.unlink(missing_ok=True))
        write_private(values_file, json.dumps(values, indent=2).encode())
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
            # No Slack identity is configured, so the chart renders no dispatcher.
            for workload in ("api", "worker"):
                self.kube.ctl(
                    "rollout",
                    "status",
                    f"deployment/{inputs.fullname}-{workload}",
                    "--timeout=600s",
                    timeout=620,
                )
            self.kube.ctl(
                "rollout",
                "status",
                f"statefulset/{inputs.fullname}-postgres",
                "--timeout=600s",
                timeout=620,
            )
        except Exception:
            self.record_rollout_diagnostics()
            raise
        pods = self.kube.json("get", "pods")
        failures = pps.running_image_failures(
            pods,
            {name: self.digests[name] for name in ("curie-api", "curie-worker")},
            registry=inputs.registry,
            required=("curie-api", "curie-worker"),
        )
        self.ledger.record("roles", "candidate_images_running", not failures)
        if failures:
            raise StepFailed("candidate images are not the running images")
        documents = [
            json.loads(
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
            ),
            self.kube.json("get", "sandboxtemplates.extensions.agents.x-k8s.io"),
            self.kube.json("get", "deployments"),
        ]
        findings = pps.deployed_credential_findings(documents, grant_secret=self.grant_secret)
        self.ledger.record("roles", "service_credentials_absent", not findings)
        if findings:
            raise pps.Refusal("deployed_service_credential", ", ".join(findings))

    def record_rollout_diagnostics(self) -> None:
        """Pod states and Warning events for a stalled rollout, before teardown erases them."""

        diagnostics = self.safe(
            lambda: pps.pod_diagnostics(
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

    def delete_secret(self, name: str) -> None:
        self.kube.ctl("delete", "secret", name, "--ignore-not-found", "--wait=true")

    # -- phase 5: setup -------------------------------------------------------

    def phase_setup(self) -> None:
        inputs = self.inputs
        assert inputs.app_dir is not None
        self.forge = Forge(
            self.shell, forge_http(self.scan), inputs.app_dir, inputs.app, self.scrubber, self.clock
        )
        self.forge.mint()
        port = self.port_forward(f"svc/{inputs.fullname}-api", 8000)
        secret = self.kube.json(
            "get", "secret", f"{inputs.fullname}-secrets", sensitive_output=True
        )
        data = secret.get("data") or {}
        try:
            api_key = base64.b64decode(data["apiKey"]).decode()
            worker_token = base64.b64decode(data["internalWorkerToken"]).decode()
        except (KeyError, ValueError):
            raise StepFailed("release secrets lack the api key or worker token") from None
        self.api = CurieApi(
            JsonHttp("127.0.0.1", port, tls=False, scan=self.scan),
            self.scrubber.hold(pps.HeldSecret(api_key)),
            self.scrubber.hold(pps.HeldSecret(worker_token)),
        )
        principal = str(uuid.uuid4())
        enrollment = str(uuid.uuid4())
        self.owner = {
            "tenant_id": pps.DEFAULT_TENANT,
            "mode": pps.PERSONAL_MODE,
            "principal_id": principal,
            "generation": pps.OWNER_GENERATION,
        }
        sql = (
            "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
            f"VALUES ('{principal}', '{pps.DEFAULT_TENANT}', '{self.owner_subject}', 'human', "
            "'active', 'personal profile owner');\n"
            "INSERT INTO curie.personal_claude_enrollments "
            "(id, tenant_id, principal_id, generation, active, version) "
            f"VALUES ('{enrollment}', '{pps.DEFAULT_TENANT}', '{principal}', "
            f"{pps.OWNER_GENERATION}, true, 1);\n"
        )
        self.kube.ctl(
            "exec",
            "-i",
            f"{inputs.fullname}-postgres-0",
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
            stdin=sql.encode(),
        )
        self.ledger.record("roles", "owner_seeded", self.owner)
        self.ledger.record(
            "approval",
            "subject_digests",
            [
                pps.ref_digest(subject)
                for subject in (self.owner_subject, self.allowed_subject, self.unlisted_subject)
            ],
        )
        repo = str(inputs.app["repo"])
        status, body = self.api.post(
            "/agents",
            {
                "name": self.agent,
                "repo_full_name": repo,
                "channel": {"kind": "github", "address": repo},
            },
        )
        if status != 201 or not isinstance(body, dict) or not body.get("id"):
            raise StepFailed(f"agent create returned {status}")
        self.agent_id = str(body["id"])
        self.curie.run(
            "approvals",
            self.agent,
            "--route-resolution",
            f"{pps.PUBLICATION_ROUTE}={pps.RESOLUTION_CHANNEL}",
            "--route-approvers",
            f"{pps.PUBLICATION_ROUTE}=users:{self.allowed_subject}",
        )
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
            timeout=900,
        )
        self.curie.run(
            "overrides",
            self.agent,
            "--reviewer-model",
            pps.REVIEWER_MODEL,
            "--model",
            pps.IMPLEMENTER_MODEL,
            "--execution-deadline",
            str(EXECUTION_DEADLINE),
        )
        self.finalizer.register("close-run-pulls", self.close_run_pulls)
        self.finalizer.register("delete-run-branches", self.delete_run_branches)
        self.policy_set = True
        self.curie.run(
            "publication-policy",
            self.agent,
            "--policy",
            "approve",
            "--no-draft",
            "--branch-prefix",
            self.prefix,
        )
        self.allowed_token = self.mint_operator(self.allowed_subject)
        self.unlisted_token = self.mint_operator(self.unlisted_subject)
        self.check_read_paths()

    def check_read_paths(self) -> None:
        """Exercise every read the live phases parse, so a wrong flag or shape fails unpaid."""

        checks = pps.read_path_checks(
            pending=self.approvals_listing(),
            routes=self.curie.json("approvals", self.agent, "--list-routes"),
            work_items=self.curie.json("work-items", "--agent", self.agent),
            api_pending=self.require_api().get(
                f"/approvals?status_filter=pending&agent_id={self.agent_id}"
            ),
            allowed_subject=self.allowed_subject,
        )
        for key, value in checks.items():
            self.ledger.record("rehearsal", key, value)
        failed = sorted(key for key, value in checks.items() if not value)
        if failed:
            raise StepFailed("read path checks failed: " + ", ".join(failed))

    def mint_operator(self, subject: str) -> pps.HeldSecret:
        return self.curie.mint_operator_principal(self.agent, subject)

    def record_resolution(
        self, step: str, result: Completed, audit: Any, subject: str
    ) -> dict[str, Any]:
        """The private ledger's record of one ``--resolve``: CLI outcome and API denial."""

        outcome = {
            **pps.cli_outcome(result.returncode, result.stdout, result.stderr),
            **pps.audit_denial(audit, subject),
        }
        self.ledger.record("approval", f"cli_{step}", outcome)
        return outcome

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
            assert process.stdout is not None
            for line in process.stdout:
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
        raise StepFailed("api port-forward did not become ready")

    # -- phase 6: forge -------------------------------------------------------

    def phase_forge(self) -> None:
        forge = self.require_forge()
        repo = forge.call("GET", forge.repo_path(""))
        self.repo_id = str(repo.get("id"))
        self.app_slug, self.bot_id = forge.app_identity()
        body = FIXTURE_ISSUE.read_text().replace("{nonce}", self.nonce)
        title = body.splitlines()[0].lstrip("# ").strip()
        self.finalizer.register("close-fixture-issue", self.close_issue)
        issue = forge.call(
            "POST", forge.repo_path("/issues"), {"title": title, "body": body}, ok=(201,)
        )
        number = issue.get("number") if isinstance(issue, dict) else None
        if not isinstance(number, int):
            raise StepFailed("fixture issue was not created")
        self.issue_number = number

    def require_forge(self) -> Forge:
        if self.forge is None:
            raise StepFailed("forge client is not ready")
        return self.forge

    def require_api(self) -> CurieApi:
        if self.api is None:
            raise StepFailed("api client is not ready")
        return self.api

    def close_issue(self) -> None:
        if self.issue_number is None:
            return
        forge = self.require_forge()
        forge.call(
            "PATCH",
            forge.repo_path(f"/issues/{self.issue_number}"),
            {"state": "closed", "state_reason": "completed"},
        )

    def close_run_pulls(self) -> None:
        forge = self.require_forge()
        for pull in forge.prefix_pulls(self.prefix):
            if pull.get("state") == "open":
                forge.call(
                    "PATCH", forge.repo_path(f"/pulls/{pull.get('number')}"), {"state": "closed"}
                )

    def delete_run_branches(self) -> None:
        forge = self.require_forge()
        for ref in forge.matching_branches(self.prefix):
            if not ref.startswith(f"refs/heads/{self.prefix}"):
                continue
            forge.call("DELETE", forge.repo_path(f"/git/{ref}"), ok=(204,))

    # -- phase 7: grant -------------------------------------------------------

    def phase_grant(self) -> None:
        pps.require_lifetime(self.inputs.expires_at, self.clock.now())
        grant = self.scrubber.hold(pps.load_grant(self.inputs.grant_file))
        self.finalizer.register(
            "delete-grant-secret", lambda: self.delete_secret(self.grant_secret)
        )
        # Runs before the grant Secret delete: no continuation may start during teardown.
        self.finalizer.register("fence-paid-starts", self.fence_paid_starts)
        try:
            self.kube.create(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": self.grant_secret},
                    "type": "Opaque",
                    "data": {"token": base64.b64encode(grant.reveal().encode()).decode()},
                },
                secret=True,
            )
        finally:
            grant.clear()
        self.grant_identity = self.secret_identity(self.grant_secret)

    def secret_identity(self, name: str) -> tuple[str, str]:
        result = self.kube.ctl(
            "get",
            "secret",
            name,
            "-o",
            'jsonpath={.metadata.uid}{" "}{.metadata.resourceVersion}',
        )
        uid, _, version = result.stdout.decode().strip().partition(" ")
        if not uid or not version:
            raise StepFailed("grant secret identity could not be read")
        return uid, version

    # -- phases 8 and 12: attempts (paid) ---------------------------------------

    def phase_attempt(self, number: int) -> None:
        attempt = Attempt(number)
        self.attempts[number] = attempt
        if number == 1:
            self.ledger.begin_live(now=self.clock.now(), grant_expires_at=self.inputs.expires_at)
            self.ledger.record("budget", "allowance_breach", [])
        attempt.reservations = self.ledger.reserve(
            pps.ATTEMPT_ROLES, attempt=number, now=self.clock.now()
        )
        self.set_platform_budget(sum(item.output_tokens for item in attempt.reservations))
        attempt.admitted_at = self.clock.now()
        status, body = self.admit(attempt)
        if status != 200 or not isinstance(body, dict):
            # A 4xx refusal admitted nothing; any other answer leaves usage unknown.
            refused = 400 <= status < 500
            for item in attempt.reservations:
                self.ledger.settle(
                    item.seq,
                    pps.SessionUsage(0, 0, 0.0, "") if refused else None,
                    source="not_started" if refused else "unknown",
                )
            raise StepFailed(f"admission returned {status}")
        work_item = body.get("work_item") or {}
        request = body.get("request") or {}
        attempt.sequence = request.get("sequence") if isinstance(request, dict) else None
        if number == 1:
            self.work_item_id = str(work_item.get("id"))
        else:
            self.ledger.record(
                "approval",
                "work_item_unchanged_after_resume",
                str(work_item.get("id")) == self.work_item_id,
            )
            self.ledger.record(
                "approval",
                "owner_unchanged_after_resume",
                pps.owner_matches(pps.owner_of(work_item), self.owner)
                and pps.owner_matches(pps.owner_of(body.get("request")), self.owner),
            )
        try:
            self.observe_attempt(attempt)
        finally:
            self.settle(attempt)

    def fence_paid_starts(self) -> None:
        """Make another paid start impossible: no grant, no running runner, no worker.

        The platform's CI repair (apps/api/src/curie_api/factory_ci.py) can queue a
        continuation after publication. Every protective delete is issued before
        any wait, because a stopping worker drains its active handlers first.
        """

        if self.grant_identity is not None and self.final_grant_identity is None:
            self.final_grant_identity = self.safe(
                lambda: self.secret_identity(self.grant_secret), None
            )
        self.kube.ctl(
            "scale", f"deployment/{self.inputs.fullname}-worker", "--replicas=0", timeout=120
        )
        self.delete_secret(self.grant_secret)
        self.kube.ctl("delete", "pod", "-l", POD_CLAIM_LABEL, "--wait=false", timeout=120)
        for selector in (
            POD_CLAIM_LABEL,
            f"app.kubernetes.io/component=worker,app.kubernetes.io/instance={self.inputs.release}",
        ):
            self.kube.ctl(
                "wait",
                "--for=delete",
                "pod",
                "-l",
                selector,
                "--timeout=120s",
                timeout=140,
                check=False,
            )
        runners = self.kube.json("get", "pods", "-l", POD_CLAIM_LABEL)
        fenced = not [
            pod
            for pod in runners.get("items") or []
            if any(
                (status.get("state") or {}).get("running")
                for status in (pod.get("status") or {}).get("containerStatuses") or []
            )
        ]
        self.ledger.record("budget", "paid_starts_fenced", fenced)
        if not fenced:
            raise StepFailed("a runner container is still running after fencing")

    def check_allowance(self, attempt: Attempt) -> None:
        """Stop paid work once any running session reaches its reserved bound."""

        if attempt.harvest is None:
            return
        breaches = pps.allowance_breaches(
            attempt.reservations, self.attempt_facts(attempt), now=self.clock.now()
        )
        if breaches:
            self.ledger.record("budget", "allowance_breach", sorted(set(breaches)))
            self.fence_paid_starts()
            raise StepFailed("a session reached its reserved allowance; paid work stopped")

    def attempt_facts(self, attempt: Attempt) -> pps.TranscriptFacts | None:
        harvest = attempt.harvest
        if harvest is None:
            return None
        earlier = frozenset(
            item
            for other in self.attempts.values()
            if other.number < attempt.number and other.facts is not None
            for item in other.facts.message_ids()
        )
        return pps.parse_native_transcripts(
            harvest.transcript_files(),
            exclude_ids=earlier,
            not_before=attempt.admitted_at - REPLAY_MARGIN_SECONDS,
        )

    def set_platform_budget(self, output_tokens: int) -> None:
        self.curie.run("budget", self.agent, "--output-tokens", str(output_tokens))

    def admit(self, attempt: Attempt) -> tuple[int, Any]:
        repo = str(self.inputs.app["repo"])
        attempt.request_id = str(uuid.uuid4())
        return self.require_api().post(
            "/v1/internal/work-items/admissions",
            {
                "agent_id": self.agent_id,
                "kind": "github",
                "address": repo,
                "reply_conversation_id": f"issue-{self.issue_number}",
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": self.repo_id,
                    "issue_id": str(self.issue_number),
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": self.repo_id,
                    "path": repo,
                },
                "code_host_installation_id": int(self.inputs.app["installation_id"]),
                "objective": f"https://github.com/{repo}/issues/{self.issue_number}",
                "requester": f"github:{self.bot_id}:{self.app_slug}[bot]",
                "request_id": attempt.request_id,
                "model_auth_mode": pps.PERSONAL_MODE,
                "owner": {
                    "tenant_id": self.owner["tenant_id"],
                    "principal_id": self.owner["principal_id"],
                    "enrollment_generation": self.owner["generation"],
                },
            },
            internal=True,
        )

    def observe_attempt(self, attempt: Attempt) -> None:
        until = self.deadline()
        previous = self.attempts.get(attempt.number - 1)
        old_uids = {item.pod_uid for item in self.attempts.values() if item.pod_uid}

        def running() -> bool:
            """A Running runner pod of one of this agent's claims, never an earlier pod."""

            claims = self.kube.json(
                "get", CLAIM_RESOURCE, "-l", f"{CLAIM_AGENT_LABEL}={self.agent}"
            )
            for claim in claims.get("items") or []:
                name = str((claim.get("metadata") or {}).get("name") or "")
                if not name or not ((claim.get("status") or {}).get("sandbox") or {}).get("name"):
                    continue
                pods = self.kube.json("get", "pods", "-l", f"{POD_CLAIM_LABEL}={name}")
                for pod in pods.get("items") or []:
                    meta = pod.get("metadata") or {}
                    uid = str(meta.get("uid") or "")
                    phase = (pod.get("status") or {}).get("phase")
                    if uid and uid not in old_uids and phase == "Running":
                        attempt.claim = name
                        attempt.pod = str(meta.get("name"))
                        attempt.pod_uid = uid
                        attempt.grant_ref = pps.pod_grant_ref(pod)
                        return True
            return False

        self.wait_until(running, until=until, what="runner pod", every=2.0)
        if previous is not None:
            self.ledger.record(
                "pod-restart", "new_pod_uid_differs", attempt.pod_uid != previous.pod_uid
            )
        self.start_harvest(attempt)
        known_approvals = {item.approval_id for item in self.attempts.values() if item.approval_id}

        def parked() -> bool:
            self.check_allowance(attempt)
            pending = self.pending_approvals()
            item = self.work_item()
            mine = [
                row
                for row in item.get("requests") or []
                if isinstance(row, dict) and row.get("sequence") == attempt.sequence
            ]
            if any(row.get("terminal_at") for row in mine):
                raise StepFailed("attempt ended before parking for approval")
            fresh = [
                row
                for row in pending
                if row.get("id") not in known_approvals
                and (
                    row.get("route") == pps.PUBLICATION_ROUTE
                    or row.get("granted_tool") == pps.PUBLICATION_GATE
                )
            ]
            if fresh and item.get("state") == "awaiting_approval":
                attempt.approval_id = str(fresh[0]["id"])
                return True
            return False

        self.wait_until(parked, until=until, what="publication approval park")
        attempt.parked_at = pps.iso_epoch(self.approval(attempt.approval_id).get("created_at"))
        publications = self.require_api().get("/publications?limit=200")
        attempt.publication_id = next(
            (
                str(row.get("id"))
                for row in publications or []
                if isinstance(row, dict) and str(row.get("approval_id")) == attempt.approval_id
            ),
            "",
        )
        if attempt.number == 1:
            self.ledger.record("approval", "approval_parked", bool(attempt.publication_id))
        attempt.eof.wait(timeout=max(0.0, min(180.0, until - self.clock.now())))
        self.ensure_pod_gone(attempt)
        if attempt.number == 2:
            harvest = attempt.harvest
            assert harvest is not None
            self.ledger.record(
                "pod-restart", "reviewed_workspace", pps.workspace_record(harvest.workspace)
            )
            self.ledger.record(
                "publication", "diff_review_approved", harvest.review_verdicts()["diff_approved"]
            )
            self.ledger.record("pod-restart", "reviewed_pod_absent_at_park", attempt.gone_at > 0)
            first = self.attempts[1]
            start = harvest.process_start()
            self.ledger.record(
                "pod-restart",
                "new_process_after_deletion",
                start is not None and first.gone_at > 0 and start > first.gone_at,
            )

    def start_harvest(self, attempt: Attempt) -> None:
        attempt.harvest = pps.HarvestState(self.scan)
        config = pps.harvest_config(nonce=self.nonce)
        process = self.shell.spawn(
            [
                "kubectl",
                "--context",
                self.inputs.context,
                "--namespace",
                self.inputs.namespace,
                "exec",
                "-i",
                attempt.pod,
                "-c",
                "runner",
                "--",
                "python3",
                "-c",
                pps.HARVEST_PROGRAM,
                config,
            ]
        )
        attempt.process = process
        self.processes.append(process)
        self.finalizer.register(f"stop-harvest-{attempt.number}", lambda: stop_process(process))
        harvest = attempt.harvest

        def read() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                harvest.feed(line)
            attempt.eof.set()

        def drain() -> None:
            assert process.stderr is not None
            for line in process.stderr:
                self.scan.scan("children", line)

        threading.Thread(target=read, daemon=True).start()
        threading.Thread(target=drain, daemon=True).start()

    def ensure_pod_gone(self, attempt: Attempt) -> None:
        if self.pod_uid_present(attempt.pod, attempt.pod_uid):
            self.kube.ctl("delete", "pod", attempt.pod, "--wait=true", timeout=300)
        attempt.eof.wait(timeout=60)
        if not self.pod_uid_present(attempt.pod, attempt.pod_uid):
            attempt.gone_at = self.clock.now()

    def pod_uid_present(self, name: str, uid: str) -> bool:
        result = self.kube.ctl(
            "get", "pod", name, "--ignore-not-found", "-o", "jsonpath={.metadata.uid}"
        )
        return result.stdout.decode().strip() == uid

    def settle(self, attempt: Attempt) -> None:
        harvest = attempt.harvest
        if not attempt.reservations:
            return
        complete = (
            harvest is not None
            and attempt.eof.is_set()
            and attempt.parked_at > 0
            and harvest.terminal_after(attempt.parked_at)
        )
        facts = self.attempt_facts(attempt)
        attempt.facts = facts
        pps.settle_attempt(self.ledger, attempt.reservations, facts, complete=complete)
        if facts is None or harvest is None:
            return
        key = "sessions_initial" if attempt.number == 1 else "sessions_resumed"
        self.ledger.record("roles", key, [pps.session_summary(s) for s in facts.sessions])
        done = [a for a in self.attempts.values() if a.facts is not None and a.harvest is not None]
        self.ledger.record(
            "roles", "synthetic_records", sum(a.facts.synthetic_records for a in done if a.facts)
        )
        self.ledger.record(
            "roles", "aci_tool_calls", sum(a.facts.aci_tool_calls for a in done if a.facts)
        )
        self.ledger.record(
            "roles",
            "active_profile_processes_max",
            max(a.harvest.process_max for a in done if a.harvest),
        )
        self.ledger.record(
            "roles",
            "bundle_hook_observed",
            any(a.harvest.review_verdicts()["hook_observed"] for a in done if a.harvest),
        )

    def approvals_listing(self) -> Any:
        return self.curie.json("approvals", self.agent, "--list")

    def pending_approvals(self) -> list[dict[str, Any]]:
        listed = self.approvals_listing()
        pending = listed.get("pending") if isinstance(listed, dict) else None
        return [row for row in pending or [] if isinstance(row, dict)]

    def work_item(self) -> dict[str, Any]:
        item = self.require_api().get(f"/work-items/{self.work_item_id}")
        if not isinstance(item, dict):
            raise StepFailed("work item read returned no object")
        return item

    def approval(self, approval_id: str) -> dict[str, Any]:
        value = self.require_api().get(f"/approvals/{approval_id}")
        return value if isinstance(value, dict) else {}

    def audit(self, approval_id: str) -> Any:
        return self.require_api().get(f"/approvals/{approval_id}/audit")

    def publication(self, publication_id: str) -> dict[str, Any]:
        if not publication_id:
            return {}
        value = self.require_api().get(f"/publications/{publication_id}")
        return value if isinstance(value, dict) else {}

    # -- phase 9: the attempt-1 pod is gone while approval waits ----------------

    def phase_old_pod_gone(self) -> None:
        first = self.attempts[1]
        self.ledger.record("pod-restart", "old_pod_absent", first.gone_at > 0)
        self.ledger.record("pod-restart", "old_process_stream_ended", first.eof.is_set())
        approval = self.approval(first.approval_id)
        self.ledger.record(
            "pod-restart", "approval_pending_after_deletion", approval.get("status") == "pending"
        )
        publication = self.publication(first.publication_id)
        forge = self.require_forge()
        quiet = (
            publication.get("status") == "pending"
            and not forge.matching_branches(self.prefix)
            and not forge.prefix_pulls(self.prefix)
        )
        self.ledger.record("pod-restart", "no_publication_during_restart", quiet)

    # -- phase 10: an unlisted operator is refused --------------------------------

    def phase_unlisted_control(self) -> None:
        first = self.attempts[1]
        assert self.unlisted_token is not None
        before = self.work_item()
        result = self.curie.run(
            "approvals",
            self.agent,
            "--resolve",
            first.approval_id,
            check=False,
            extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": self.unlisted_token.reveal()},
            secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
        )
        status = pps.resolve_status(result.returncode, result.stdout + result.stderr)
        after = self.work_item()
        approval = self.approval(first.approval_id)
        publication = self.publication(first.publication_id)
        forge = self.require_forge()
        audit = self.audit(first.approval_id)
        self.record_resolution("unlisted", result, audit, self.unlisted_subject)
        record = {
            "unlisted_status": status,
            "pending_after_unlisted": approval.get("status") == "pending",
            # Only the explicit list's own refusal proves the control: a set that
            # admits nobody refuses the listed operator too (the tracker-surface case).
            "unlisted_audit_denied": pps.audit_row(
                audit,
                action="denied",
                subject=self.unlisted_subject,
                authorized=False,
                authorizer=pps.EXPLICIT_USERS_AUTHORIZER,
            ),
            "work_item_unchanged_after_unlisted": before.get("state") == after.get("state")
            and before.get("updated_at") == after.get("updated_at"),
            "execution_requests_unchanged_after_unlisted": len(before.get("requests") or [])
            == len(after.get("requests") or []),
            "publication_jobs_after_unlisted": 0 if publication.get("status") == "pending" else 1,
            "remote_branches_after_unlisted": len(forge.matching_branches(self.prefix)),
            "owner_unchanged_after_unlisted": pps.owner_matches(pps.owner_of(after), self.owner),
        }
        for key, value in record.items():
            self.ledger.record("approval", key, value)
        if status != 403:
            raise StepFailed("unlisted operator was not refused with 403")

    # -- phase 11: the allowed operator rejects ------------------------------------

    def phase_reject(self) -> None:
        first = self.attempts[1]
        assert self.allowed_token is not None
        result = self.curie.run(
            "approvals",
            self.agent,
            "--resolve",
            first.approval_id,
            "--reject",
            "--note",
            "personal profile rejection control",
            check=False,
            extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": self.allowed_token.reveal()},
            secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
        )
        approval = self.approval(first.approval_id)
        audit = self.audit(first.approval_id)
        outcome = self.record_resolution("reject", result, audit, self.allowed_subject)
        self.ledger.record(
            "approval",
            "reject_resolved",
            result.returncode == 0 and approval.get("status") == "rejected",
        )
        self.ledger.record(
            "approval",
            "reject_audit_operator",
            pps.audit_row(
                audit,
                action="resolved",
                subject=self.allowed_subject,
                authorized=True,
                decision="rejected",
            ),
        )
        if result.returncode != 0:
            raise StepFailed(
                f"allowed operator could not reject (HTTP {outcome['http_status']}; "
                "the reason is in the private ledger)"
            )

        def denied() -> bool:
            requests = self.work_item().get("requests") or []
            return any(row.get("terminal_cause") == "publication_denied" for row in requests)

        self.wait_until(denied, until=self.clock.now() + 180, what="publication_denied")
        self.clock.wait(REJECTION_SETTLE_SECONDS)
        forge = self.require_forge()
        self.ledger.record(
            "publication", "rejected_remote_branches", len(forge.matching_branches(self.prefix))
        )
        self.ledger.record("publication", "rejected_prs", len(forge.prefix_pulls(self.prefix)))

    # -- phase 13: the allowed operator approves the fresh attempt ---------------------

    def phase_approve(self) -> None:
        first, second = self.attempts[1], self.attempts[2]
        assert self.allowed_token is not None
        self.ledger.record(
            "approval",
            "approval_digests",
            [pps.ref_digest(first.approval_id), pps.ref_digest(second.approval_id)],
        )
        result = self.curie.run(
            "approvals",
            self.agent,
            "--resolve",
            second.approval_id,
            check=False,
            extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": self.allowed_token.reveal()},
            secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
        )
        audit = self.audit(second.approval_id)
        outcome = self.record_resolution("approve", result, audit, self.allowed_subject)
        self.ledger.record("approval", "approve_resolved", result.returncode == 0)
        self.ledger.record(
            "approval",
            "approve_audit_operator",
            pps.audit_row(
                audit,
                action="resolved",
                subject=self.allowed_subject,
                authorized=True,
                decision="approved",
            ),
        )
        if result.returncode != 0:
            raise StepFailed(
                f"allowed operator could not approve (HTTP {outcome['http_status']}; "
                "the reason is in the private ledger)"
            )
        publication: dict[str, Any] = {}

        def published() -> bool:
            nonlocal publication
            publication = self.publication(second.publication_id)
            if publication.get("status") in {"failed", "denied", "expired"}:
                raise StepFailed("publication did not succeed")
            return publication.get("status") == "succeeded" and bool(publication.get("pr_url"))

        self.wait_until(
            published, until=self.clock.now() + PUBLICATION_WAIT_SECONDS, what="publication"
        )
        self.ledger.record("publication", "publication_status", publication.get("status"))
        self.fence_paid_starts()
        self.ledger.end_live(now=self.clock.now())
        forge = self.require_forge()
        number = publication.get("pr_number")
        pull = forge.call("GET", forge.repo_path(f"/pulls/{number}"))
        head = pull.get("head") or {}
        head_sha = str(head.get("sha") or "")
        commit = forge.call("GET", forge.repo_path(f"/git/commits/{head_sha}"))
        self.ledger.record("publication", "pr_draft", pull.get("draft"))
        self.ledger.record(
            "publication", "pr_head_prefix", str(head.get("ref") or "").startswith(self.prefix)
        )
        self.ledger.record("publication", "head_sha", head_sha)
        self.ledger.record("publication", "tree_sha", str((commit.get("tree") or {}).get("sha")))
        self.ledger.record("publication", "prs_for_prefix", len(forge.prefix_pulls(self.prefix)))
        reviewed = self.ledger.observations("pod-restart").get("reviewed_workspace") or {}
        hashes: dict[str, str | None] = {}
        nonce_paths: list[str] = []
        for path in sorted((reviewed.get("hashes") or {}).keys()):
            status, body = forge.request(
                "GET", forge.repo_path(f"/contents/{urllib.parse.quote(path)}?ref={head_sha}")
            )
            if status == 404:
                hashes[path] = None
                continue
            if status != 200 or not isinstance(body, dict):
                raise StepFailed("published file could not be read")
            content = base64.b64decode(str(body.get("content") or ""))
            self.scan.scan("forge", content)
            hashes[path] = hashlib.sha256(content).hexdigest()
            if self.nonce.encode() in content:
                nonce_paths.append(path)
        changed, listed_all = forge.pull_files(int(str(number)))
        self.ledger.record(
            "pod-restart",
            "published_workspace",
            {
                "hashes": hashes,
                "nonce_paths": nonce_paths,
                "changed_paths": changed,
                "changed_paths_complete": listed_all,
            },
        )
        checks: dict[str, Any] = {}

        def completed() -> bool:
            nonlocal checks
            runs = forge.call(
                "GET", forge.repo_path(f"/commits/{head_sha}/check-runs?filter=latest&per_page=100")
            )
            statuses = forge.call("GET", forge.repo_path(f"/commits/{head_sha}/status"))
            checks = pps.judge_head_checks(runs, statuses)
            return int(checks["total"]) > 0 and int(checks["pending"]) == 0

        with contextlib.suppress(StepFailed):
            self.wait_until(
                completed,
                until=self.clock.now() + CI_WAIT_SECONDS,
                what="fixture checks",
                every=15.0,
            )
        self.ledger.record("publication", "head_checks", checks)
        self.ledger.record(
            "publication",
            "rejection_stays_rejected",
            self.approval(first.approval_id).get("status") == "rejected",
        )

        def item_published() -> bool:
            return self.work_item().get("state") == "published"

        with contextlib.suppress(StepFailed):
            self.wait_until(item_published, until=self.clock.now() + 120, what="published state")
        self.ledger.record("publication", "work_item_state", self.work_item().get("state"))

    # -- phase 14: owner facts after the run ------------------------------------------

    def phase_owner_after(self) -> None:
        item = self.work_item()
        cli = self.curie.json("work-items", self.work_item_id)
        cli_item = cli.get("item") if isinstance(cli, dict) else None
        self.ledger.record("roles", "owner_api", pps.owner_of(item))
        self.ledger.record("roles", "owner_cli", pps.owner_of(cli_item))
        self.ledger.record("roles", "request_owners", pps.request_owners(item))
        unchanged = pps.owner_matches(pps.owner_of(item), self.owner)
        self.ledger.record("publication", "owner_unchanged", unchanged)
        self.ledger.record("pod-restart", "owner_generation_unchanged", unchanged)
        first, second = self.attempts[1], self.attempts[2]
        # Read by the fence just before it deleted the Secret.
        same_secret = (
            self.grant_identity is not None and self.final_grant_identity == self.grant_identity
        )
        same_ref = first.grant_ref == second.grant_ref == (self.grant_secret, "token")
        clean = all(
            attempt.facts is not None and attempt.facts.synthetic_records == 0
            for attempt in (first, second)
        )
        self.ledger.record("approval", "same_grant", same_secret and same_ref and clean)

    # -- finalizer inventory ----------------------------------------------------------

    def query_inventory(self) -> dict[str, str]:
        checks: dict[str, Callable[[], bool]] = {
            "namespace": self.namespace_absent,
            "cluster_scoped": self.cluster_scoped_absent,
            "registry_tags": self.registry_absent,
            "local_images": self.local_images_absent,
            "processes": lambda: all(process.poll() is not None for process in self.processes),
            "values_file": lambda: not self.inputs.work_dir.exists(),
            "forge_issue": self.issue_closed,
            "forge_prs": lambda: (
                not self.policy_set
                or not any(
                    pull.get("state") == "open"
                    for pull in self.require_forge().prefix_pulls(self.prefix)
                )
            ),
            "forge_branches": lambda: (
                not self.policy_set or not self.require_forge().matching_branches(self.prefix)
            ),
        }
        results: dict[str, str] = {}
        for kind, check in checks.items():
            try:
                results[kind] = "absent" if check() else "present"
            except Exception:  # noqa: BLE001 - an unanswered query is a failure
                results[kind] = "uncertain"
        return results

    def namespace_absent(self) -> bool:
        result = self.kube.ctl(
            "get",
            "namespace",
            self.inputs.namespace,
            "--ignore-not-found",
            "-o",
            "name",
            namespaced=False,
        )
        return not result.stdout.strip()

    def cluster_scoped_absent(self) -> bool:
        listed = self.kube.json("get", CLUSTER_SCOPED_KINDS, namespaced=False)
        release = self.inputs.release
        for item in listed.get("items") or []:
            meta = item.get("metadata") or {}
            annotations = meta.get("annotations") or {}
            if annotations.get("meta.helm.sh/release-name") == release or str(
                meta.get("name", "")
            ).startswith(release):
                return False
        return True

    def registry_absent(self) -> bool:
        get = pps.loopback_get("127.0.0.1", self.inputs.registry_port)
        references = [(item.repository, item.digest) for item in self.pushed]
        owned = owned_tags(self.ledger, self.inputs.registry_port, None)
        references += [pps.split_image_ref(tag) for tag in owned]
        for repository, reference in references:
            status, _headers, _body = get(
                f"/v2/{repository}/manifests/{reference}", {"Accept": pps.MANIFEST_ACCEPT}
            )
            if status != 404:
                return False
        return True

    def local_images_absent(self) -> bool:
        for tag in owned_tags(self.ledger, self.inputs.registry_port, self.shell):
            if self.shell.run(["docker", "image", "ls", "-q", tag], timeout=60).stdout.strip():
                return False
        return True

    def issue_closed(self) -> bool:
        if self.issue_number is None:
            return True
        forge = self.require_forge()
        issue = forge.call("GET", forge.repo_path(f"/issues/{self.issue_number}"))
        return isinstance(issue, dict) and issue.get("state") == "closed"


def resolve_ipv4(host: str) -> list[str]:
    try:
        answers = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        raise StepFailed("model provider address could not be resolved") from None
    return sorted({str(answer[4][0]) for answer in answers})


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

COMMANDS: dict[str, Callable[[argparse.Namespace, Runtime], int]] = {
    "images": cmd_images,
    "preflight": cmd_preflight,
    "forge-probe": cmd_forge_probe,
    "rehearse": cmd_rehearse,
    "run": cmd_run,
    "judge": cmd_judge,
}


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Clock | None = None,
) -> int:
    environ = dict(os.environ if environ is None else environ)
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, pps.SCAN_CHANNELS)
    runtime = Runtime(environ, Output(scrubber, scan), scrubber, scan, clock or Clock())
    try:
        pps.refuse_ambient_credentials(environ)
        args = build_parser().parse_args(argv)
        return COMMANDS[args.command](args, runtime)
    except pps.Refusal as refusal:
        runtime.out.emit(refusal.payload())
        return pps.EXIT_REFUSED
    except StepFailed as failure:
        runtime.out.emit({"error": "step_failed", "detail": str(failure)})
        return pps.EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
