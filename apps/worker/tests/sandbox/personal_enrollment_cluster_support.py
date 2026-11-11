"""Owned candidate stack and the AC 5 scenario for owner-scoped enrollment (#5377).

Builds on #5374's exact-image harness (``runner/tests/personal_token_cluster_support.py``)
instead of repeating it: ``ClusterProof`` supplies the clean-candidate check, the
runner image build and loopback registry push, the fresh-namespace refusal, the
installation template that carries a service credential in every shape, pod image
assertions, claim observation, port-forwards, file and byte audits; ``HeldSecret``,
``scrubbed_env`` and the durable ``BudgetLedger`` come from the same module.

This driver adds, per run and owned by it: a source-process API and worker on the
candidate checkout, throwaway Postgres, Valkey, S3 and Dex containers on the
caller's scratch Docker engine bound to loopback (the caller mirrors that engine's
loopback ports onto this host's loopback), a second fresh namespace for custody
Secrets, and principal sessions minted by a real OIDC password login. The driver
is the explicit launch caller (#5374's boundary): it admits through the trusted
internal route, acquires and starts through the production worker client, reads
the grant through the production custody reader, renders the boot env through the
production binding resolver and claims through the production substrate.
Teardown is never the driver's: it flows through the API's work-item reconciler,
its terminate wakes and the candidate worker kernel, and custody cleanup through
the API's enrollment reconciler.

Determinism (plan R5): the candidate worker holds no model credential, and it is
stopped whenever a request of this run is waiting, so an execute wake can never
start a request before the driver has acquired and started it. A wake consumed
later is refused by acquire. Nothing is timed, and nothing paid is retried.

Every target comes from the caller's environment; nothing here names a host,
cluster, context, user or path. No private value reaches argv, a log, the
evidence or a public artifact.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import http.client
import importlib.util
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Coroutine, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__tracebackhide__ = True

ROOT = Path(__file__).resolve().parents[4]
# importlib import mode puts neither test directory on sys.path.
for _path in (ROOT / "runner" / "tests", Path(__file__).resolve().parent):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from personal_token_cluster_support import (  # noqa: E402
    GROUP,
    MODEL,
    VERSION,
    BudgetLedger,
    ClusterProof,
    HeldSecret,
    ProofCheckError,
    live_observations,
    live_transcript_identity,
    require,
    scrubbed_env,
    secret_env,
    value_env,
)

PHASES = ("synthetic", "live")
KIND = "slack"
ADDRESS = "C0EXAMPLE1"
CONVERSATION = "1700000000.000100"
SERVICE_CONVERSATION = "1700000000.000200"
REQUESTER = "U0EXAMPLE1"
REPOSITORY = "acme-corp/acme-bot"
AGENT_NAME = "acme-enrollment"
OWNER_EMAIL = "owner@example.com"
OTHER_EMAIL = "other@example.com"
# The public bcrypt of "password" the OIDC harness's Dex template already uses
# (tools/oidc-e2e/dex.yaml.template); Dex is private to this run and loopback only.
DEX_PASSWORD = "password"
DEX_PASSWORD_HASH = "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
OIDC_CLIENT_ID = "curie-console"
SESSION_COOKIE = "__Host-curie_console_session"
STATE_COOKIE = "__Host-curie_oidc_state"
POSTGRES_IMAGE = "postgres:16.15-alpine"
VALKEY_IMAGE = "valkey/valkey:8.1.10-alpine"
S3_IMAGE = "rustfs/rustfs:1.0.0-beta.12"
SOURCE_POOL = "custody-source-pool"
PLUGIN_DIR = "/bundle/custody"
DRIVER_OWNER = "personal-enrollment-proof-launcher"
WORKER_CONSUMER = "personal-enrollment-proof-worker"
ADMISSION_LABEL = "sandbox.users.io/curie-admission-claim"
CUSTODY_SELECTOR = "curietech.ai/personal-custody=true"
RUNTIME_TTL_SECONDS = 24
TOOL_SLEEP_SECONDS = 100
# Budget approved for this proof: four sessions, four assistant turns each,
# 4,096 output tokens in total, 180 seconds per session, no paid retries.
MAX_SESSIONS = 4
MAX_ASSISTANT_TURNS = 4
MAX_TOTAL_OUTPUT_TOKENS = 4096
MAX_SESSION_OUTPUT_TOKENS = 1024
MAX_SESSION_SECONDS = 180
PER_RESPONSE_OUTPUT_TOKENS = 256
# Each ACI turn is capped at two model turns, so the two-turn session stays at
# four assistant turns.
MAX_TURNS_PER_RUN = 2
TOKEN_SHAPE = re.compile(r"[\x21-\x7e]{1,4096}")
SYNTHETIC_SERVICE_CREDENTIAL = "installation-control-unusable"
SYNTHETIC_SERVICE_BASE_URL = "https://model.example.com"

_TRANSCRIPT_WRITE = """\
import asyncio, os, sys
from curie_runner.history import StateApiTranscriptStore, TurnRecord
env = os.environ
store = StateApiTranscriptStore(env["CURIE_HISTORY_REF"], env.get("CURIE_HISTORY_TOKEN"))
record = TurnRecord(user="Remember the code word " + sys.argv[1], assistant="STORED",
                    record_id=sys.argv[2])
asyncio.run(store.append(record))  # raises on refusal; returns replay kept
print("appended")
"""
_TRANSCRIPT_READ = """\
import asyncio, json, os, sys
from curie_runner.history import StateApiTranscriptStore
env = os.environ
store = StateApiTranscriptStore(env["CURIE_HISTORY_REF"], env.get("CURIE_HISTORY_TOKEN"))
records = asyncio.run(store.load())
text = [str(getattr(r, "user", "")) + " " + str(getattr(r, "assistant", "")) for r in records]
print(json.dumps({"records": len(records), "found": any(sys.argv[1] in t for t in text)}))
"""


@functools.cache
def oidc_harness() -> Any:
    """``tools/oidc-e2e/oidc_e2e.py`` by path: its HTTP and cookie helpers."""

    path = ROOT / "tools" / "oidc-e2e" / "oidc_e2e.py"
    spec = importlib.util.spec_from_file_location("curie_oidc_e2e_harness", path)
    require(spec is not None and spec.loader is not None, "OIDC harness is unavailable")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Its dataclasses resolve their own module by name while it executes.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class EnrollmentLedger(BudgetLedger):
    """#5374's durable ledger at this proof's approved ceiling.

    A refused session (the generation 2 provider refusal) spends a session and
    no output, so unpaid sessions are admitted; every session is still capped
    at four assistant turns and 180 seconds.
    """

    max_sessions = MAX_SESSIONS
    max_assistant_turns = MAX_ASSISTANT_TURNS
    max_session_seconds = MAX_SESSION_SECONDS
    unpaid_sessions = True

    def __init__(self, run_id: str) -> None:
        super().__init__(run_id)
        self.limit = MAX_TOTAL_OUTPUT_TOKENS
        self.session_limit = MAX_SESSION_OUTPUT_TOKENS


def wait_until[T](
    predicate: Callable[[], T | None], *, timeout: float, interval: float, failure: str
) -> T:
    """Poll ``predicate`` until it returns a value, within one deadline.

    Live driver only: the gated proof waits on cluster, API and worker state it
    does not own. The pause is an event wait so a stopped proof never lingers.
    """

    __tracebackhide__ = True
    deadline = time.monotonic() + timeout
    pause = threading.Event()
    while True:
        value = predicate()
        if value is not None:
            return value
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProofCheckError(failure) from None
        pause.wait(min(interval, remaining))


def read_private_input(path_text: str, what: str) -> HeldSecret:
    """A caller-private, nonempty, 0600 regular file, without one trailing newline.

    The value is held in memory only; it is never printed, logged or copied.
    """

    __tracebackhide__ = True
    path = Path(path_text)
    require(path.is_absolute(), f"{what} path must be absolute")
    try:
        info = path.lstat()
    except OSError:
        raise ProofCheckError(f"{what} is unavailable") from None
    require(stat.S_ISREG(info.st_mode), f"{what} must be a regular file")
    require(stat.S_IMODE(info.st_mode) == 0o600, f"{what} must be mode 0600")
    require(info.st_uid == os.getuid(), f"{what} must belong to the caller")
    try:
        raw = path.read_bytes()
    except OSError:
        raise ProofCheckError(f"{what} is unreadable") from None
    if raw.endswith(b"\r\n"):
        raw = raw[:-2]
    elif raw.endswith(b"\n"):
        raw = raw[:-1]
    try:
        value = raw.decode("ascii")
    except UnicodeDecodeError:
        raise ProofCheckError(f"{what} is not printable ASCII") from None
    require(TOKEN_SHAPE.fullmatch(value) is not None, f"{what} is empty or malformed")
    return HeldSecret(value)


def validate_inputs(phase: str) -> dict[str, str]:
    """Refuse a run whose required caller inputs are absent or unsafe."""

    __tracebackhide__ = True
    require(phase in PHASES, "proof phase must be synthetic or live")
    required = [
        "DOCKER_CONTEXT",
        "KUBECONFIG",
        "THROWAWAY_CONTEXT",
        "CURIE_PERSONAL_TEST_NAMESPACE",
        "CURIE_BIN",
        "CURIE_PERSONAL_RUNNER_API_HOST",
        "CURIE_PERSONAL_EVIDENCE_DIR",
    ]
    if phase == "live":
        required += [
            "SUBSCRIPTION_TEST_GRANT_FILE",
            "CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE",
            "CURIE_PERSONAL_PROOF_RUN_ID",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_BASE_URL",
        ]
    missing = [name for name in required if not os.environ.get(name)]
    require(not missing, "missing required proof inputs: " + ", ".join(missing))
    forbidden = ["CLAUDE_CODE_OAUTH_TOKEN", "CURIE_CREDENTIALS"]
    if phase != "live":
        forbidden.append("ANTHROPIC_API_KEY")
    for name in forbidden:
        require(name not in os.environ, "a model credential reached the driver environment")
    namespace = os.environ["CURIE_PERSONAL_TEST_NAMESPACE"]
    require(
        re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,40}[a-z0-9])?", namespace) is not None,
        "a fresh DNS-label namespace of at most 42 characters is required",
    )
    evidence = Path(os.environ["CURIE_PERSONAL_EVIDENCE_DIR"])
    require(evidence.is_absolute(), "evidence directory must be absolute")
    resolved = evidence.resolve()
    require(
        resolved != ROOT and ROOT not in resolved.parents,
        "evidence directory must be outside the checkout",
    )
    host = os.environ["CURIE_PERSONAL_RUNNER_API_HOST"]
    try:
        socket.inet_aton(host)
    except OSError:
        raise ProofCheckError("runner API host must be an IPv4 address of this host") from None
    require(not host.startswith("127."), "runner API host must be reachable from the cluster")
    return {"namespace": namespace, "grant_namespace": f"{namespace}-grants"}


def free_local_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def local_port_free(port: int) -> bool:
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def tcp_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def leaks(data: bytes, values: Sequence[str]) -> bool:
    """True when any of ``values``, or its base64 forms, occurs in ``data``."""

    for value in values:
        if not value:
            continue
        raw = value.encode()
        variants = {raw, base64.b64encode(raw), base64.urlsafe_b64encode(raw)}
        variants.update(item.rstrip(b"=") for item in tuple(variants))
        if any(item and item in data for item in variants):
            return True
    return False


def anonymize(data: bytes, targets: Mapping[str, str]) -> bytes:
    """Replace each caller target (namespace, context, host, path) by its placeholder."""

    for value, placeholder in sorted(targets.items(), key=lambda item: -len(item[0])):
        if value:
            data = data.replace(value.encode(), placeholder.encode())
    return data


class TcpRelay:
    """Expose a loopback-only service on one more address, for the cluster's pods.

    The API binds loopback, which the CLI requires for plain HTTP; pods reach it
    through this relay on the caller-supplied runner API host.
    """

    def __init__(self, listen_host: str, target_port: int) -> None:
        self._target = target_port
        self._server = socket.create_server((listen_host, 0))
        self._server.settimeout(0.5)
        self.port = int(self._server.getsockname()[1])
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._accept = threading.Thread(target=self._serve, daemon=True)
        self._accept.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", self._target), timeout=10)
            except OSError:
                client.close()
                continue
            upstream.settimeout(None)
            client.settimeout(None)
            for source, sink in ((client, upstream), (upstream, client)):
                thread = threading.Thread(target=self._pipe, args=(source, sink), daemon=True)
                self._threads.append(thread)
                thread.start()

    @staticmethod
    def _pipe(source: socket.socket, sink: socket.socket) -> None:
        try:
            while chunk := source.recv(65536):
                sink.sendall(chunk)
        except OSError:
            pass
        finally:
            for side in (source, sink):
                with contextlib.suppress(OSError):
                    side.shutdown(socket.SHUT_RDWR)
                side.close()

    def close(self) -> None:
        self._stop.set()
        self._server.close()
        self._accept.join(timeout=5)
        for thread in self._threads:
            thread.join(timeout=5)

    @property
    def alive(self) -> bool:
        return self._accept.is_alive() or any(thread.is_alive() for thread in self._threads)


def oidc_login(api: str, dex_origin: str, email: str) -> HeldSecret:
    """A principal-bound console session from a real Dex password login.

    The same browser half as ``tools/oidc-e2e``: the API starts the login, Dex's
    password form is followed over plain HTTP and the callback is replayed with
    its state cookie. The session value is held, never printed.
    """

    __tracebackhide__ = True
    oidc = oidc_harness()
    started = oidc.request("GET", f"{api}/console/oidc/login")
    require(started.status == 302, "OIDC login start did not redirect to the IdP")
    state_cookie = started.set_cookies()[STATE_COOKIE].value
    redirect_uri = f"{api}/console/oidc/callback"
    jar: dict[str, str] = {}

    def dex(method: str, url: str, form: dict[str, str] | None = None) -> Any:
        headers = {"Cookie": "; ".join(f"{k}={v}" for k, v in jar.items())} if jar else {}
        response = oidc.request(method, url, headers=headers, form=form)
        for name, morsel in response.set_cookies().items():
            jar[name] = morsel.value
        return response

    current = started.location
    response = dex("GET", current)
    chooser = re.search(r'href="(/dex/auth/local[^"]*)"', response.body)
    if chooser:
        current = urllib.parse.urljoin(dex_origin, chooser.group(1).replace("&amp;", "&"))
        response = dex("GET", current)
    callback = ""
    for _ in range(10):
        if response.status in (301, 302, 303, 307):
            target = urllib.parse.urljoin(current, response.location)
            if target.startswith(redirect_uri):
                callback = target
                break
            current = target
            response = dex("GET", current)
        elif response.status == 200 and 'name="password"' in response.body:
            response = dex("POST", current, {"login": email, "password": DEX_PASSWORD})
        else:
            break
    require(bool(callback), "Dex never redirected to the API callback")
    done = oidc.request("GET", callback, headers={"Cookie": f"{STATE_COOKIE}={state_cookie}"})
    morsel = done.set_cookies().get(SESSION_COOKIE)
    require(
        done.status == 303 and morsel is not None and bool(morsel.value),
        "the OIDC callback did not mint a principal session",
    )
    assert morsel is not None
    return HeldSecret(str(morsel.value))


@dataclass
class Launched:
    """One request the driver acquired, claimed and started."""

    request_id: uuid.UUID
    generation: int
    run: Any
    claim_name: str
    sandbox_name: str
    runner_token: HeldSecret
    grant: HeldSecret
    pod: str = ""
    stops: list[str] = field(default_factory=list)


class EnrollmentProof(ClusterProof):
    """AC 5 on the exact candidate, with every resource owned and removed."""

    def __repr__(self) -> str:
        return "<private enrollment proof>"

    def __init__(self, phase: str) -> None:
        names = validate_inputs(phase)
        self.grant_file = ""
        if phase == "live":
            self.grant_file = os.environ["SUBSCRIPTION_TEST_GRANT_FILE"]
            grant = read_private_input(self.grant_file, "grant input")
            service = read_private_input(
                os.environ["CURIE_PERSONAL_SERVICE_CREDENTIAL_FILE"], "service credential input"
            )
            require(os.environ["ANTHROPIC_API_KEY"] == service.reveal(),
                    "installation credential does not match its private input")
            service_url = HeldSecret(os.environ.pop("ANTHROPIC_BASE_URL"))
            parsed = urllib.parse.urlsplit(service_url.reveal())
            require(parsed.scheme in {"http", "https"} and bool(parsed.hostname),
                    "installation endpoint is malformed")
            os.environ.pop("ANTHROPIC_API_KEY")
            # Well formed, and refused by the provider: it was never issued.
            invalid = HeldSecret("sk-ant-oat01-" + secrets.token_urlsafe(72))
        else:
            grant = HeldSecret("synthetic-enrollment-grant-" + secrets.token_hex(16))
            service = HeldSecret(SYNTHETIC_SERVICE_CREDENTIAL)
            service_url = HeldSecret(SYNTHETIC_SERVICE_BASE_URL)
            invalid = HeldSecret("synthetic-enrollment-invalid-" + secrets.token_hex(16))
        super().__init__(grant, phase)
        self.grant_namespace = names["grant_namespace"]
        self.service = service
        self.service_url = service_url
        self.invalid = invalid
        self.runner_host = os.environ["CURIE_PERSONAL_RUNNER_API_HOST"]
        self.evidence_dir = Path(os.environ["CURIE_PERSONAL_EVIDENCE_DIR"])
        self.platform_key = HeldSecret(secrets.token_urlsafe(32))
        self.worker_token = HeldSecret(secrets.token_urlsafe(32))
        self.pg_password = HeldSecret(secrets.token_urlsafe(24))
        self.valkey_password = HeldSecret(secrets.token_urlsafe(24))
        self.s3_secret = HeldSecret(secrets.token_urlsafe(24))
        self.oidc_secret = HeldSecret(secrets.token_urlsafe(24))
        self.s3_access = "proof" + secrets.token_hex(6)
        self.sessions: dict[str, HeldSecret] = {}
        self.session_tokens: list[HeldSecret] = []
        self.private_dir = ""
        self.retained_dir: Path | None = None
        self.containers: list[str] = []
        self.container_volumes: list[str] = []
        self.ports: dict[str, int] = {}
        self.api_process: subprocess.Popen[bytes] | None = None
        self.worker_process: subprocess.Popen[bytes] | None = None
        self.relay: TcpRelay | None = None
        self.grant_namespace_uid = ""
        self.grant_namespace_requested = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self.loop_thread: threading.Thread | None = None
        self.launcher: Launcher | None = None
        self.files: dict[str, Path] = {}
        self.principals: dict[str, dict[str, str]] = {}
        self.agent_id = ""
        self.tenant_id = ""
        self.work_item_id = ""
        self.thread_key = ""
        self.service_claim = ""
        self.service_thread = ""
        self.headers_seen: dict[str, Any] = {}
        self.marker = "MARKER-" + secrets.token_hex(6).upper()
        self.ac: dict[str, dict[str, Any]] = {
            name: {"criterion": name, "checks": []} for name in ("AC1", "AC2", "AC3", "AC4", "AC5")
        }
        self.evidence["budget"] = {
            "max_sessions": MAX_SESSIONS,
            "max_assistant_turns": MAX_ASSISTANT_TURNS,
            "max_total_output_tokens": MAX_TOTAL_OUTPUT_TOKENS,
            "max_session_output_tokens": MAX_SESSION_OUTPUT_TOKENS,
            "max_session_seconds": MAX_SESSION_SECONDS,
            "model": MODEL,
            "paid_retries": 0,
        }
        self.evidence["sessions"] = []
        self.evidence["steps"] = []

    # --- private values ------------------------------------------------------

    def held(self) -> list[HeldSecret]:
        return [
            self.grant,
            self.invalid,
            self.service,
            self.service_url,
            self.platform_key,
            self.worker_token,
            self.pg_password,
            self.valkey_password,
            self.s3_secret,
            self.oidc_secret,
            self.runner_auth,
            self.progress_auth,
            *self.sessions.values(),
            *self.session_tokens,
        ]

    def held_values(self) -> list[str]:
        return [
            secret.reveal()
            for secret in self.held()
            if secret.reveal() and secret.reveal() != SYNTHETIC_SERVICE_CREDENTIAL
        ]

    def targets(self) -> dict[str, str]:
        """Caller targets and private paths, with the placeholder each becomes."""

        found = {
            self.namespace: "<namespace>",
            self.grant_namespace: "<grant-namespace>",
            self.context: "<context>",
            self.docker_context: "<docker-context>",
            self.runner_host: "<runner-api-host>",
            self.cli: "curie",
            self.private_dir: "<private>",
            str(self.evidence_dir): "<evidence>",
            str(Path.home()): "<home>",
        }
        if self.grant_file:
            found[self.grant_file] = "<grant-file>"
        if "api" in self.ports:
            found[self.api_url] = "<candidate-api>"
        return {key: value for key, value in found.items() if key}

    def check(self, criterion: str, name: str, expected: Any, observed: Any) -> None:
        """Record one expected/observed pair and fail the proof on a mismatch."""

        __tracebackhide__ = True
        self.ac[criterion]["checks"].append(
            {"check": name, "expected": expected, "observed": observed}
        )
        require(observed == expected, f"{criterion} {name}: observed differs from expected")

    def step(self, name: str) -> None:
        self.evidence["stage"] = name
        self.evidence["steps"].append({"step": name, "at_monotonic": round(time.monotonic(), 1)})

    def command(
        self,
        argv: Sequence[str],
        *,
        data: bytes | None = None,
        timeout: float = 120,
        check: bool = True,
        retain_output: bool = True,
        private_env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """#5374's audited subprocess, auditing every held value of this proof.

        ``private_env`` names private values a child needs (a container
        password, the platform key for ``cluster deploy``); they travel only in
        that child's environment and are redacted from the command log.
        """

        __tracebackhide__ = True
        held = self.held_values()
        for raw in held:
            require(all(raw not in arg for arg in argv), "credential in subprocess argument")
            require(
                all(raw not in value for value in self.env.values()),
                "credential in child environment",
            )
            require(data is None or raw.encode() not in data, "credential in subprocess input")
        try:
            result = subprocess.run(
                list(argv),
                input=data,
                stdin=subprocess.DEVNULL if data is None else None,
                capture_output=True,
                timeout=timeout,
                cwd=self.root,
                env={**self.env, **(private_env or {})},
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("private proof command could not complete") from None
        if self.command_log is not None:
            from curie_telemetry.redact import held_secret_literals

            outputs = [result.stdout, result.stderr]
            if not retain_output:
                outputs = [b"[private proof output omitted]", b""]
            for secret in held_secret_literals(held):
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

    def private_file(self, name: str, value: HeldSecret) -> Path:
        """Write ``value`` to a 0600 file in the private run directory."""

        path = Path(self.private_dir) / name
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(value.reveal() + "\n")
        self.files[name] = path
        return path

    # --- the event loop the production async clients run on -------------------

    def run_async[R](
        self, factory: Callable[[], Coroutine[Any, Any, R]], timeout: float = 300
    ) -> R:
        __tracebackhide__ = True
        assert self.loop is not None
        future = asyncio.run_coroutine_threadsafe(factory(), self.loop)
        return future.result(timeout=timeout)

    @property
    def launch(self) -> Launcher:
        assert self.launcher is not None
        return self.launcher

    # --- setup ---------------------------------------------------------------

    def ssh_argv(self) -> list[str]:
        endpoint = json.loads(
            self.docker(
                "context", "inspect", self.docker_context,
                "--format", "{{json .Endpoints.docker.Host}}",
            ).stdout
        )
        parsed = urllib.parse.urlsplit(endpoint)
        require(
            parsed.scheme == "ssh" and bool(parsed.hostname),
            "scratch Docker requires an explicit SSH endpoint",
        )
        remote = (f"{parsed.username}@" if parsed.username else "") + str(parsed.hostname)
        argv = ["ssh", "-o", "BatchMode=yes"]
        if parsed.port:
            argv += ["-p", str(parsed.port)]
        return [*argv, remote]

    def scratch_port(self) -> int:
        """A port free on the scratch engine's loopback and on this host's loopback.

        The caller mirrors the engine's loopback-published ports onto this
        host's loopback, so both sides must be free.
        """

        script = (
            "import socket\n"
            "for port in range(25100,26000):\n"
            " s=socket.socket()\n"
            " try: s.bind(('127.0.0.1',port))\n"
            " except OSError: s.close(); continue\n"
            " s.close(); print(port)\n"
        )
        listed = self.command([*self.ssh_argv(), shlex.join(["python3", "-c", script])]).stdout
        taken = set(self.ports.values())
        for line in listed.decode().split():
            port = int(line)
            if port not in taken and local_port_free(port):
                return port
        raise ProofCheckError("no free scratch port was available")

    def setup(self) -> None:
        __tracebackhide__ = True
        from curie_worker.sandbox.k8s import KubernetesSandboxClient
        from kubernetes import client, config

        state = Path(
            os.environ.get(
                "CURIE_PERSONAL_PROOF_STATE_DIR",
                str(Path.home() / ".local/state/curie-personal-token-verification"),
            )
        )
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(state.stat().st_mode & 0o077 == 0, "proof state directory must be private")
        self.retained_dir = state / f"enrollment-{self.ownership}"
        self.retained_dir.mkdir(mode=0o700)
        log_path = self.retained_dir / "commands.sqlite3"
        fd = os.open(log_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        self.command_log = sqlite3.connect(log_path)
        self.command_log.execute(
            "CREATE TABLE commands (ordinal INTEGER PRIMARY KEY, argv TEXT NOT NULL, "
            "exit_code INTEGER NOT NULL, stdout BLOB NOT NULL, stderr BLOB NOT NULL)"
        )
        self.private_dir = tempfile.mkdtemp(prefix="curie-enrollment-proof-")
        for executable in ("docker", "kubectl", "ssh", "git", self.cli):
            require(shutil.which(executable) is not None, "a required executable is unavailable")
        require(os.path.isabs(self.cli), "CURIE_BIN must name the source-built CLI")
        require(
            self.command(["git", "status", "--porcelain", "--untracked-files=all"]).stdout == b"",
            "candidate source must be clean before building",
        )
        self.candidate = self.command(["git", "rev-parse", "HEAD"]).stdout.decode().strip()
        require(bool(re.fullmatch(r"[0-9a-f]{40}", self.candidate)), "invalid candidate commit")
        self.evidence["candidate_sha"] = self.candidate
        self.evidence["candidate_tree"] = (
            self.command(["git", "rev-parse", "HEAD^{tree}"]).stdout.decode().strip()
        )
        if self.phase == "live":
            self.ledger = EnrollmentLedger(os.environ["CURIE_PERSONAL_PROOF_RUN_ID"])
            self.ledger.remaining()
        raw_config = self.kubectl(
            "config", "view", "--minify", "--flatten", "--raw", retain_output=False
        ).stdout
        fd, self.private_config = tempfile.mkstemp(
            prefix="kube-", suffix=".yaml", dir=self.private_dir
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw_config)
        del raw_config
        self.env = {**scrubbed_env(), "KUBECONFIG": self.private_config}
        configuration = client.Configuration()
        config.load_kube_config(  # type: ignore[no-untyped-call]
            config_file=self.private_config, client_configuration=configuration
        )
        configuration.retries = 0
        self.api_client = client.ApiClient(configuration)
        self.core = client.CoreV1Api(self.api_client)
        self.custom = client.CustomObjectsApi(self.api_client)
        for name in (self.namespace, self.grant_namespace):
            try:
                self.core.read_namespace(name, _request_timeout=20)
            except client.ApiException as exc:
                require(exc.status == 404, "could not verify a fresh namespace")
            else:
                raise ProofCheckError("a requested namespace already exists; refusing adoption")
        require(
            self.kubectl("get", "--raw=/readyz").stdout.strip() == b"ok", "cluster is not ready"
        )
        self.build_image()
        for name in (self.namespace, self.grant_namespace):
            if name == self.namespace:
                self.namespace_requested = True
            else:
                self.grant_namespace_requested = True
            created = self.core.create_namespace(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": name,
                        "labels": {
                            "curie-test": "personal-enrollment",
                            "curie-proof-owner": self.ownership,
                        },
                    },
                },
                _request_timeout=30,
            )
            if name == self.namespace:
                self.namespace_uid = created.metadata.uid
            else:
                self.grant_namespace_uid = created.metadata.uid
        self.worker = KubernetesSandboxClient(self.namespace, kubeconfig=self.private_config)
        self.create_inputs()
        self.start_backing_services()
        self.migrate()
        self.relay = TcpRelay(self.runner_host, self.port_api())
        self.start_api()
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.loop_thread.start()

    def create_inputs(self) -> None:
        """#5374's installation inputs, with the bundle under the chart's init names.

        The source template carries the installation's model credential as a
        reference, a declared env-key name and an opaque ``envFrom``; every
        personal claim must strip all three. Its bundle staging container is
        named like the chart's, so the claim's bundle ref has a container to land on.
        """

        original = self.pod_spec

        def pod_spec() -> dict[str, Any]:
            spec = original()
            for container in [*spec["containers"], *spec["initContainers"]]:
                container["env"] += [
                    secret_env(name, "installation-model", name)
                    for name in ("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL")
                ]
            staging = spec["initContainers"][0]
            staging["name"] = "bundle-fetch"
            extract = {**staging, "name": "bundle-extract", "command": ["python3", "-c", "pass"]}
            spec["initContainers"] = [staging, extract]
            return spec

        self.pod_spec = pod_spec  # type: ignore[method-assign]
        try:
            super().create_inputs()
        finally:
            self.pod_spec = original  # type: ignore[method-assign]
        self.core.patch_namespaced_secret(
            "installation-model", self.namespace,
            {"stringData": {"ANTHROPIC_API_KEY": self.service.reveal(),
                            "ANTHROPIC_BASE_URL": self.service_url.reveal()}},
            _request_timeout=30,
        )

    def port_api(self) -> int:
        if "api" not in self.ports:
            self.ports["api"] = free_local_port()
        return self.ports["api"]

    def run_container(
        self,
        role: str,
        image: str,
        container_port: int,
        *,
        env: Mapping[str, str] | None = None,
        args: Sequence[str] = (),
        extra: Sequence[str] = (),
        create_only: bool = False,
        host_port: int | None = None,
    ) -> int:
        """One owned, labelled container publishing ``container_port`` on loopback."""

        __tracebackhide__ = True
        port = host_port if host_port is not None else self.scratch_port()
        self.ports[role] = port
        name = f"{self.namespace}-{role}"
        require(
            self.docker("container", "inspect", name, check=False).returncode != 0,
            "an owned container name is already in use",
        )
        self.docker("image", "inspect", image)
        self.containers.append(name)
        verb = ["create"] if create_only else ["run", "--detach"]
        self.docker(
            *verb,
            "--name", name,
            "--label", f"curie-proof-owner={self.ownership}",
            "--publish", f"127.0.0.1:{port}:{container_port}",
            *[f"--env={key}" for key in (env or {})],
            *extra,
            image,
            *args,
            private_env=env,
        )
        return port

    def start_backing_services(self) -> None:
        __tracebackhide__ = True
        oidc = oidc_harness()
        self.run_container(
            "postgres", POSTGRES_IMAGE, 5432,
            env={"POSTGRES_PASSWORD": self.pg_password.reveal()},
            extra=["--tmpfs", "/var/lib/postgresql/data"],
            args=["-c", "fsync=off"],
        )
        self.run_container(
            "valkey", VALKEY_IMAGE, 6379,
            env={"VALKEY_PASSWORD": self.valkey_password.reveal()},
            args=["sh", "-c", 'exec valkey-server --requirepass "$VALKEY_PASSWORD"'],
        )
        self.run_container(
            "s3", S3_IMAGE, 9000,
            env={
                "RUSTFS_ACCESS_KEY": self.s3_access,
                "RUSTFS_SECRET_KEY": self.s3_secret.reveal(),
                "RUSTFS_VOLUMES": "/data",
                "RUSTFS_ADDRESS": "0.0.0.0:9000",
            },
            extra=[
                "--tmpfs", "/data:uid=10001,gid=10001",
                "--tmpfs", "/logs:uid=10001,gid=10001",
            ],
            args=["rustfs"],
        )
        dex_port = self.scratch_port()
        self.ports["dex"] = dex_port
        dex_config = {
            "issuer": f"http://127.0.0.1:{dex_port}/dex",
            "storage": {"type": "memory"},
            "web": {"http": f"0.0.0.0:{dex_port}"},
            "oauth2": {"skipApprovalScreen": True},
            "staticClients": [
                {
                    "id": OIDC_CLIENT_ID,
                    "name": "Curie console (enrollment proof)",
                    "secret": self.oidc_secret.reveal(),
                    "redirectURIs": [f"{self.api_url}/console/oidc/callback"],
                }
            ],
            "enablePasswordDB": True,
            "staticPasswords": [
                {
                    "email": email,
                    "username": email.split("@", 1)[0],
                    "userID": str(uuid.uuid5(uuid.NAMESPACE_URL, email)),
                    "hash": DEX_PASSWORD_HASH,
                }
                for email in (OWNER_EMAIL, OTHER_EMAIL)
            ],
        }
        # ``docker cp`` keeps the file's mode and Dex runs unprivileged, so the
        # copy is world-readable inside the owned container; on this host it
        # sits in the 0700 private directory and is removed right after the copy.
        # The mode is set explicitly: a caller's restrictive umask (077) would
        # otherwise make it 0600, and Dex would exit unable to read its config.
        config_path = Path(self.private_dir) / "dex.json"
        fd = os.open(config_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o644)
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(dex_config, stream)
        self.run_container(
            "dex", oidc.DEX_IMAGE, dex_port,
            args=["dex", "serve", "/tmp/dex.json"],
            create_only=True,
            host_port=dex_port,
        )
        dex = f"{self.namespace}-dex"
        self.docker("cp", str(config_path), f"{dex}:/tmp/dex.json")
        config_path.unlink()
        self.docker("start", dex)
        for container in self.containers:
            mounts = json.loads(
                self.docker("container", "inspect", container, "--format", "{{json .Mounts}}")
                .stdout
            )
            self.container_volumes += [m["Name"] for m in mounts if m["Type"] == "volume"]
        for role in ("postgres", "valkey", "s3", "dex"):
            port = self.ports[role]
            wait_until(
                functools.partial(lambda port: True if tcp_open("127.0.0.1", port) else None, port),
                timeout=90, interval=0.5, failure=f"owned {role} never became reachable",
            )

        def discovery() -> bool | None:
            try:
                answer = oidc.request(
                    "GET", f"{self.dex_origin}/dex/.well-known/openid-configuration"
                )
            except OSError:
                return None
            return True if answer.status == 200 else None

        wait_until(discovery, timeout=90, interval=0.5,
                   failure="owned Dex never served its discovery document")
        from aci_protocol.s3 import build_s3_client

        s3 = build_s3_client(
            endpoint_url=f"http://127.0.0.1:{self.ports['s3']}",
            access_key=self.s3_access,
            secret_key=self.s3_secret.reveal(),
            region="us-east-1",
        )

        def buckets() -> bool | None:
            try:
                existing = {item["Name"] for item in s3.list_buckets().get("Buckets", [])}
                for bucket in ("curie-bundles", "curie-workspaces"):
                    if bucket not in existing:
                        s3.create_bucket(Bucket=bucket)
            except Exception:  # noqa: BLE001 - the store is still starting
                return None
            return True

        wait_until(buckets, timeout=90, interval=1, failure="owned S3 never accepted buckets")
        s3.close()

        def postgres_ready() -> bool | None:
            try:
                return True if self.sql_scalar("SELECT 1") == 1 else None
            except Exception:  # noqa: BLE001 - the database is still starting
                return None

        wait_until(postgres_ready, timeout=90, interval=1, failure="owned Postgres never answered")
        self.evidence["backing_services"] = {
            "postgres": POSTGRES_IMAGE, "valkey": VALKEY_IMAGE, "s3": S3_IMAGE,
            "oidc": "dex (tools/oidc-e2e pin)", "bound": "loopback",
        }

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.port_api()}"

    @property
    def dex_origin(self) -> str:
        return f"http://127.0.0.1:{self.ports['dex']}"

    @property
    def runner_api_url(self) -> str:
        assert self.relay is not None
        return f"http://{self.runner_host}:{self.relay.port}"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://postgres:{self.pg_password.reveal()}"
            f"@127.0.0.1:{self.ports['postgres']}/postgres"
        )

    def service_env(self) -> dict[str, str]:
        """What the API, the worker and the migration share. No model credential."""

        return {
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "OTEL_SDK_DISABLED": "true",
            "DATABASE_URL": self.database_url,
            "DB_SCHEMA": "curie",
            "VALKEY_HOST": "127.0.0.1",
            "VALKEY_PORT": str(self.ports["valkey"]),
            "VALKEY_PASSWORD": self.valkey_password.reveal(),
            "S3_ENDPOINT_URL": f"http://127.0.0.1:{self.ports['s3']}",
            "S3_ACCESS_KEY": self.s3_access,
            "S3_SECRET_KEY": self.s3_secret.reveal(),
            "BUNDLE_BUCKET": "curie-bundles",
            "CURIE_WORKSPACE_BUCKET": "curie-workspaces",
            "CURIE_INTERNAL_WORKER_TOKEN": self.worker_token.reveal(),
            "CURIE_PERSONAL_GRANT_NAMESPACE": self.grant_namespace,
            "KUBECONFIG": self.private_config,
        }

    def spawn(
        self, name: str, argv: Sequence[str], env: Mapping[str, str]
    ) -> subprocess.Popen[bytes]:
        """A long-lived source process; its output goes to a private retained log."""

        assert self.retained_dir is not None
        log = self.retained_dir / f"{name}.log"
        fd = os.open(log, os.O_CREAT | os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "ab") as stream:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=stream,
                stderr=subprocess.STDOUT,
                cwd=self.private_dir,
                env=dict(env),
            )
        self.processes.append(process)
        return process

    def record_private_output(self, name: str, output: bytes) -> None:
        assert self.retained_dir is not None
        path = self.retained_dir / f"{name}.log"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "ab") as stream:
            stream.write(output)

    def retain_failure(self, exc: BaseException) -> None:
        """Keep a failure's traceback for the operator, in the private run directory only."""

        import traceback

        if self.retained_dir is None or not self.retained_dir.exists():
            return
        text = "".join(traceback.format_exception(exc)).encode()
        if leaks(text, self.held_values()):
            text = b"traceback withheld: it carried a private value\n"
        self.record_private_output("failure", text)

    def migrate(self) -> None:
        __tracebackhide__ = True
        try:
            result = subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=ROOT / "apps" / "api",
                env=self.service_env(),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=900,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("owned database migration could not complete") from None
        self.record_private_output("migrate", result.stdout + result.stderr)
        require(result.returncode == 0, "owned database migration failed; output withheld")
        expected = json.loads((ROOT / "apps/api/src/curie_api/schema_compat.json").read_text())
        head = self.sql_scalar("SELECT version_num FROM curie.alembic_version")
        self.check("AC5", "database migrated to the candidate head", expected["schema_head"], head)

    def api_env(self) -> dict[str, str]:
        return {
            **self.service_env(),
            "API_KEY": self.platform_key.reveal(),
            "KUBE_CONFIG_PATH": self.private_config,
            "CURIE_PERSONAL_ROLLOUT_HOLD": "false",
            "CURIE_PERSONAL_CUSTODY_RECONCILE_INTERVAL_SECONDS": "2",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "true",
            "CURIE_WORK_ITEM_RECONCILER_INTERVAL_SECONDS": "2",
            "CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS": str(RUNTIME_TTL_SECONDS),
            "CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS": "6",
            "RESUME_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
            "GITHUB_REPO_ALLOWLIST": json.dumps(["acme-corp/*"]),
            "GITHUB_WEBHOOK_SECRET": secrets.token_urlsafe(16),
            "CURIE_OIDC_ISSUER": f"{self.dex_origin}/dex",
            "CURIE_OIDC_AUDIENCE": OIDC_CLIENT_ID,
            "CURIE_OIDC_JWKS_URL": f"{self.dex_origin}/dex/keys",
            "CURIE_OIDC_CLIENT_SECRET": self.oidc_secret.reveal(),
            "CURIE_OIDC_REDIRECT_URI": f"{self.api_url}/console/oidc/callback",
            "CURIE_OIDC_SCOPES": "openid email profile",
            "CURIE_OIDC_REQUIRED_CLAIMS": "",
            "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED": "false",
        }

    def start_api(self) -> None:
        __tracebackhide__ = True
        oidc = oidc_harness()
        process = self.spawn(
            "api",
            [sys.executable, "-m", "curie_api", "--host", "127.0.0.1",
             "--port", str(self.port_api())],
            self.api_env(),
        )
        self.api_process = process

        def healthy() -> bool | None:
            require(process.poll() is None,
                    "candidate API exited during startup; log retained privately")
            try:
                answer = oidc.request("GET", f"{self.api_url}/health")
            except OSError:
                return None
            return True if answer.status == 200 else None

        wait_until(healthy, timeout=180, interval=1, failure="candidate API never became healthy")
        relay = self.relay
        assert relay is not None
        wait_until(
            lambda: True if tcp_open(self.runner_host, relay.port) else None,
            timeout=10, interval=0.5, failure="runner-facing relay is not listening",
        )
        self.evidence["api"] = {"source_process": True, "bound": "loopback", "runner_relay": True}

    def worker_env(self) -> dict[str, str]:
        return {
            **self.service_env(),
            "CURIE_SANDBOX_SUBSTRATE": "kubernetes",
            "CURIE_NAMESPACE": self.namespace,
            "CURIE_WARM_POOL": SOURCE_POOL,
            "CURIE_PLUGIN_DIR": PLUGIN_DIR,
            "CURIE_API_URL": self.api_url,
            "CURIE_RUNNER_API_URL": self.runner_api_url,
            "CURIE_API_KEY": self.platform_key.reveal(),
            "CURIE_CONSUMER_NAME": WORKER_CONSUMER,
            "CURIE_MODEL": MODEL,
            "CURIE_THINKING": "disabled",
            "CURIE_CLAIM_TIMEOUT_SECONDS": "150",
        }

    def start_worker(self) -> None:
        """Start the candidate worker. It holds no service model credential."""

        __tracebackhide__ = True
        require(self.worker_process is None, "the candidate worker is already running")
        env = self.worker_env()
        for name in ("CURIE_CREDENTIALS", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"):
            require(name not in env, "the candidate worker must hold no model credential")
        process = self.spawn("worker", [sys.executable, "-m", "curie_worker"], env)
        self.worker_process = process

        def consuming() -> bool | None:
            require(process.poll() is None,
                    "candidate worker exited during startup; log retained privately")
            return True if self.consumer_registered() else None

        wait_until(consuming, timeout=180, interval=1,
                   failure="candidate worker never joined the runs stream")

    def consumer_registered(self) -> bool:
        import redis
        from aci_protocol.service_config import RUNS_STREAM_DEFAULT

        client = redis.Redis(
            host="127.0.0.1", port=self.ports["valkey"], password=self.valkey_password.reveal()
        )
        try:
            for group in client.xinfo_groups(RUNS_STREAM_DEFAULT):
                consumers = client.xinfo_consumers(RUNS_STREAM_DEFAULT, group["name"])
                if any(item["name"] == WORKER_CONSUMER.encode() for item in consumers):
                    return True
        except redis.ResponseError:
            return False
        finally:
            client.close()
        return False

    def stop_worker(self) -> None:
        process = self.worker_process
        if process is None:
            return
        self.stop_process(process)
        self.worker_process = None

    # --- SQL observations -----------------------------------------------------

    def sql_rows(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        async def query() -> list[dict[str, Any]]:
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import create_async_engine

            engine = create_async_engine(self.database_url)
            try:
                async with engine.connect() as conn:
                    result = await conn.execute(text(statement), dict(params or {}))
                    return [dict(row._mapping) for row in result]
            finally:
                await engine.dispose()

        if self.loop is not None:
            return self.run_async(query)
        return asyncio.run(query())

    def sql_scalar(self, statement: str, params: Mapping[str, Any] | None = None) -> Any:
        rows = self.sql_rows(statement, params)
        return next(iter(rows[0].values())) if rows else None

    def request_row(self, request_id: uuid.UUID) -> dict[str, Any]:
        rows = self.sql_rows(
            "SELECT status, terminal_cause, termination_observation, teardown_unconfirmed_at, "
            "terminal_at, runtime_claim_name, enrollment_generation "
            "FROM curie.execution_requests WHERE id = :id",
            {"id": request_id},
        )
        require(len(rows) == 1, "a request of this run is missing")
        return rows[0]

    def transcript_has_marker(self) -> bool:
        value = self.sql_scalar(
            "SELECT value::text FROM curie.thread_transcripts "
            "WHERE agent_id = :agent AND thread_key = :thread AND binding_scope IS NULL",
            {"agent": uuid.UUID(self.agent_id), "thread": self.thread_key},
        )
        return isinstance(value, str) and self.marker in value

    def enrollment_headers(self) -> list[dict[str, Any]]:
        return self.sql_rows(
            "SELECT generation, version, active FROM curie.personal_claude_enrollments "
            "WHERE tenant_id = :tenant AND principal_id = :principal",
            {
                "tenant": uuid.UUID(self.tenant_id),
                "principal": uuid.UUID(self.principals["owner"]["id"]),
            },
        )

    # --- the CLI -------------------------------------------------------------

    def subscription(
        self,
        verb: str,
        who: str,
        *,
        token: Path | None = None,
        expected_version: int | None = None,
        expect_success: bool = True,
    ) -> tuple[dict[str, Any], int]:
        """``curie cluster subscription <verb>`` against the candidate API; one JSON object."""

        __tracebackhide__ = True
        argv = [
            self.cli, "--json", "cluster", "subscription", verb,
            "--session-file", str(self.files[f"session-{who}"]), "--api-url", self.api_url,
        ]
        if token is not None:
            argv += ["--token-file", str(token)]
        if expected_version is not None:
            argv += ["--expected-version", str(expected_version)]
        result = self.command(argv, check=False, timeout=60)
        output = result.stdout + result.stderr
        require(not leaks(output, self.held_values()), "a CLI output carried a private value")
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        require(len(lines) == 1, "a CLI call did not emit exactly one JSON object")
        body = json.loads(lines[0])
        require(isinstance(body, dict), "a CLI call emitted a non-object")
        observed = {
            key: body[key]
            for key in ("state", "generation", "version", "action", "changed", "previous_version",
                        "teardown")
            if key in body
        }
        if "error" in body:
            observed["error_code"] = next(
                (code for code in ("enrollment_absent", "stale_version", "enrollment_inactive",
                                   "enrollment_active", "enrollment_exists")
                 if code in str(body["error"])),
                "other",
            )
        self.ac["AC4"]["checks"].append(
            {
                "check": f"cluster subscription {verb} as {who}",
                "command": [anonymize(arg.encode(), self.targets()).decode() for arg in argv],
                "expected_exit": "0" if expect_success else "nonzero",
                "observed_exit": result.returncode,
                "json_objects": len(lines),
                "secret_in_output": False,
                "observed": observed,
            }
        )
        require(
            (result.returncode == 0) == expect_success,
            f"cluster subscription {verb} exit status differs from expected",
        )
        return body, result.returncode

    def write(
        self, verb: str, *, token: Path | None, previous: dict[str, Any] | None
    ) -> dict[str, Any]:
        """One owner write; generation +1, version moves, exactly one header."""

        __tracebackhide__ = True
        expected = None if previous is None else int(previous["version"])
        body, _ = self.subscription(verb, "owner", token=token, expected_version=expected)
        generation = 1 if previous is None else int(previous["generation"]) + 1
        state = "inactive" if verb == "disconnect" else "active"
        self.check("AC4", f"{verb} JSON generation", generation, body.get("generation"))
        self.check("AC4", f"{verb} JSON state", state, body.get("state"))
        self.check("AC4", f"{verb} JSON previous version", expected, body.get("previous_version"))
        require(
            previous is None or int(body["version"]) > int(previous["version"]),
            "enrollment version did not advance",
        )
        headers = self.enrollment_headers()
        self.check("AC1", f"one enrollment header after {verb}", 1, len(headers))
        self.check(
            "AC1", f"header generation after {verb}", generation, headers[0]["generation"]
        )
        return body

    # --- the launch boundary -----------------------------------------------------

    def admit(self, generation: int, *, principal: str = "owner") -> tuple[int, dict[str, Any]]:
        request_id = uuid.uuid4()
        status_code, body = self.run_async(
            lambda: self.launch.admit(
                request_id, generation, uuid.UUID(self.principals[principal]["id"])
            )
        )
        return status_code, {**body, "_request_id": str(request_id)}

    def start_session(self, generation: int) -> Launched:
        """Admit, acquire, read custody, render, claim and start one request.

        The worker is stopped first, so the execute wake this admission
        publishes can only be consumed after the driver started the request.
        """

        __tracebackhide__ = True
        from curie_worker.sandbox.claim_tokens import claim_object_names

        self.stop_worker()
        status_code, body = self.admit(generation)
        code = (body.get("detail") or {}).get("code") if status_code != 200 else None
        require(status_code == 200, f"personal admission was refused: {status_code} {code}")
        work_item = body["work_item"]
        self.check("AC5", f"generation {generation} admission is personal",
                   ["personal", generation],
                   [work_item["model_auth_mode"], work_item["enrollment_generation"]])
        if not self.work_item_id:
            self.work_item_id = str(work_item["id"])
            self.thread_key = str(work_item["conversation_id"])
        self.check("AC5", f"generation {generation} admission reuses the WorkItem",
                   self.work_item_id, str(work_item["id"]))
        request_id = uuid.UUID(body["_request_id"])
        launched = self.run_async(
            lambda: self.launch.launch(request_id, generation), timeout=400
        )
        pod = self.wait_pod(f"{ADMISSION_LABEL}={launched.claim_name}", timeout=240)
        launched.pod = str(pod["metadata"]["name"])
        self.observe_claim(launched.claim_name, pod, personal=True, grant=launched.grant.reveal())
        tokens = self.core.read_namespaced_secret(
            claim_object_names(launched.claim_name).secret, self.namespace, _request_timeout=30
        )
        stored = base64.b64decode((tokens.data or {}).get("CLAUDE_CODE_OAUTH_TOKEN", ""))
        self.check("AC2", f"generation {generation} grant only in its per-claim Secret", True,
                   stored == launched.grant.reveal().encode())
        self.assert_service_isolated(launched.pod, tokens.data or {})
        self.check("AC5", f"generation {generation} request running under the driver",
                   "running", self.request_row(request_id)["status"])
        self.start_worker()
        return launched

    def assert_service_isolated(self, pod: str, token_data: Mapping[str, str]) -> None:
        names = json.loads(self.exec(
            pod, "python3", "-c",
            "import json, os; print(json.dumps(sorted(set(os.environ) & "
            "{'ANTHROPIC_API_KEY', 'ANTHROPIC_BASE_URL'})))",
        ))
        self.check("AC5", "personal runner excludes installation key and endpoint", [], names)
        self.check(
            "AC5", "personal claim Secret excludes installation key and endpoint", False,
            leaks(json.dumps(dict(token_data)).encode(),
                  [self.service.reveal(), self.service_url.reveal()]),
        )

    def wait_teardown(self, launched: Launched, *, cause: str) -> dict[str, Any]:
        """The fenced runtime is torn down by the worker and its record confirmed."""

        __tracebackhide__ = True
        from curie_worker.sandbox.claim_tokens import claim_object_names

        def confirmed() -> dict[str, Any] | None:
            row = self.request_row(launched.request_id)
            return row if (
                row["terminal_at"] is not None
                and (row["termination_observation"] or "").strip()
                and row["teardown_unconfirmed_at"] is None
            ) else None

        row = wait_until(confirmed, timeout=300, interval=2,
                         failure="the fenced runtime's teardown was never confirmed")
        self.check("AC3", f"generation {launched.generation} terminal cause kept", cause,
                   row["terminal_cause"])
        self.check("AC3", f"generation {launched.generation} observation names its claim",
                   True, launched.claim_name in str(row["termination_observation"]))
        self.check("AC5", f"generation {launched.generation} claim absent", True,
                   self.absent("sandboxclaims", launched.claim_name))
        secret = claim_object_names(launched.claim_name).secret
        wait_until(
            lambda: True if self.secret_absent(self.namespace, secret) else None,
            timeout=120, interval=2, failure="a per-claim tokens Secret outlived its claim",
        )
        self.check("AC5", f"generation {launched.generation} <claim>-tokens is 404", True,
                   self.secret_absent(self.namespace, secret))
        return row

    def absent(self, plural: str, name: str) -> bool:
        from kubernetes import client

        try:
            self.custom.get_namespaced_custom_object(
                GROUP, VERSION, self.namespace, plural, name, _request_timeout=30
            )
        except client.ApiException as exc:
            require(exc.status == 404, "a cluster read failed")
            return True
        return False

    def secret_absent(self, namespace: str, name: str) -> bool:
        from kubernetes import client

        try:
            self.core.read_namespaced_secret(name, namespace, _request_timeout=30)
        except client.ApiException as exc:
            require(exc.status == 404, "a cluster read failed")
            return True
        return False

    def revalidate(self, generation: int) -> dict[str, Any]:
        __tracebackhide__ = True
        status_code, body = self.run_async(self.launch.revalidate)
        self.check("AC3", f"revalidation binds generation {generation}", [200, generation],
                   [status_code, body.get("enrollment_generation")])
        return body

    def capture_pod(self, pod: str) -> None:
        """Keep the pod's container logs for the final output audit."""

        listing = self.core.read_namespaced_pod(pod, self.namespace, _request_timeout=30)
        for container in [*listing.spec.containers, *(listing.spec.init_containers or [])]:
            self.captures.append(
                self.core.read_namespaced_pod_log(
                    pod, self.namespace, container=container.name, _request_timeout=30
                ).encode()
            )

    # --- live turns ------------------------------------------------------------

    @contextlib.contextmanager
    def session_deadline(self, pod: str, token: HeldSecret, port: int) -> Iterator[float]:
        deadline = time.monotonic() + MAX_SESSION_SECONDS
        expired = threading.Event()
        expiry_lock = threading.Lock()
        problems: list[str] = []

        def expire() -> None:
            with expiry_lock:
                if expired.is_set():
                    return
                expired.set()
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                try:
                    connection.request(
                        "POST", "/v1/interrupt", body=b"{}",
                        headers={"Authorization": f"Bearer {token.reveal()}",
                                 "Content-Type": "application/json"},
                    )
                    connection.getresponse().close()
                except (OSError, http.client.HTTPException):
                    pass
                finally:
                    connection.close()
                    try:
                        self.core.delete_namespaced_pod(
                            pod, self.namespace, grace_period_seconds=0,
                            _request_timeout=10,
                        )
                    except Exception:  # noqa: BLE001 - deadline failure remains fatal
                        problems.append("runner pod deletion failed")

        timer = threading.Timer(MAX_SESSION_SECONDS, expire)
        timer.start()
        try:
            yield deadline
        finally:
            timer.cancel()
            if time.monotonic() >= deadline:
                expire()
            timer.join()
            require(not expired.is_set(), "session exceeded its absolute deadline"
                    + (": " + "; ".join(problems) if problems else ""))

    def stream(
        self,
        port: int,
        token: HeldSecret,
        prompt: str,
        *,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        deadline: float,
    ) -> tuple[list[dict[str, Any]], bytes, float, bool]:
        """One ACI turn; returns its events and whether the stream ended unfinished."""

        __tracebackhide__ = True
        started = time.monotonic()
        remaining = deadline - started
        require(remaining > 0, "session exceeded its absolute deadline")
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=remaining)
        events: list[dict[str, Any]] = []
        raw = bytearray()
        ended = False
        try:
            connection.request(
                "POST", "/v1/event",
                body=json.dumps({"kind": "event", "type": "message", "text": prompt,
                                 "user": REQUESTER, "ts": "1"}).encode(),
                headers={"Authorization": f"Bearer {token.reveal()}",
                         "Content-Type": "application/json"},
            )
            response = connection.getresponse()
            require(response.status == 200, "ACI turn did not return a streaming response")
            while True:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "session exceeded its absolute deadline")
                if connection.sock is not None:
                    connection.sock.settimeout(remaining)
                line = response.readline()
                if not line:
                    ended = True
                    break
                raw.extend(line)
                event = json.loads(line)
                require(isinstance(event, dict), "ACI stream included an invalid event")
                events.append(event)
                if on_event is not None:
                    on_event(event)
                if event.get("type") == "final":
                    break
        except TimeoutError:
            raise ProofCheckError("session exceeded its absolute deadline") from None
        except (OSError, http.client.HTTPException):
            ended = True
        except ValueError:
            raise RuntimeError("ACI turn returned an unreadable event") from None
        finally:
            connection.close()
        elapsed = time.monotonic() - started
        self.captures.append(bytes(raw))
        return events, bytes(raw), elapsed, ended

    def account(
        self, sequence: int, launched: Launched, events: Sequence[Mapping[str, Any]],
        observed: Mapping[str, Any], elapsed: float, *, allowance: int, name: str,
    ) -> dict[str, Any]:
        """Settle one session in the durable ledger from its own observations."""

        __tracebackhide__ = True
        assert self.ledger is not None
        finals = [event for event in events if event.get("type") == "final"]
        final_tokens = sum(
            event["output_tokens"] for event in finals
            if isinstance(event.get("output_tokens"), int)
        )
        reported = sum(
            item.get("output_tokens", 0)
            for body in observed["usage"] for item in body.get("models", [])
        )
        by_id = {
            record.get("message", {}).get("id"): record
            for record in observed["assistant_messages"]
        }
        native = sum(
            int((record.get("message", {}).get("usage") or {}).get("output_tokens", 0))
            for record in by_id.values()
        )
        tokens = max(final_tokens, reported, native)
        usage = self.ledger.finish(sequence, tokens, len(set(by_id) - {None}), elapsed)
        record = {
            "session": sequence,
            "step": name,
            "generation": launched.generation,
            "allowance": allowance,
            **usage,
            "observations": live_observations(list(events), observed, elapsed, launched.grant),
        }
        self.evidence["sessions"].append(record)
        return record

    def live_session(
        self, launched: Launched, name: str, prompts: Sequence[str], *, expect: str
    ) -> list[dict[str, Any]]:
        """One paid session on its own fresh pod, settled in the ledger."""

        __tracebackhide__ = True
        assert self.ledger is not None
        remaining = self.ledger.remaining()
        events: list[dict[str, Any]] = []
        with self.forward(launched.pod) as port:
            sequence, allowance = self.ledger.start(self.candidate)
            require(allowance == remaining, "live allowance changed before session admission")
            started = time.monotonic()
            with self.session_deadline(launched.pod, launched.runner_token, port) as deadline:
                for prompt in prompts:
                    turn, _raw, _elapsed, ended = self.stream(
                        port, launched.runner_token, prompt, deadline=deadline,
                    )
                    events += turn
                    require(not ended, "a live turn ended without a final; no paid retry")
        elapsed = time.monotonic() - started
        observed = self.observe_files(launched.pod)
        self.account(sequence, launched, events, observed, elapsed, allowance=allowance, name=name)
        finals = [event for event in events if event.get("type") == "final"]
        self.check("AC5", f"session {sequence} final status", expect,
                   finals[-1].get("status") if finals else "missing")
        if expect == "done":
            live_transcript_identity(observed)
        return events

    # --- the scenario ----------------------------------------------------------

    def run(self) -> None:
        __tracebackhide__ = True
        for name, action in (
            ("prepare", self.prepare),
            ("a", self.step_a),
            ("b", self.step_b),
            ("c", self.step_c),
            ("d", self.step_d),
            ("e", self.step_e),
            ("f", self.step_f),
            ("retire", self.retire),
            ("output_audit", self.audit),
        ):
            self.step(name)
            action()
        self.step("completed")

    def prepare(self) -> None:
        """Deploy the agent, mint both principals, and start the service control."""

        __tracebackhide__ = True
        oidc = oidc_harness()
        bundle = Path(self.private_dir) / "bundle"
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / ".claude-plugin" / "plugin.json").write_text(
            json.dumps({"name": AGENT_NAME, "version": "0.1.0"})
        )
        deployed = self.command(
            [self.cli, "--json", "cluster", "deploy", "--plugin-dir", str(bundle),
             "--api-url", self.api_url, "--agent", AGENT_NAME, "--slack-channel", ADDRESS],
            private_env={"CURIE_API_KEY": self.platform_key.reveal()},
            timeout=180,
            check=False,
        )
        # The API deploy (agent, version, deployment, channel binding) completes
        # first; the connector step after it reads an installed Helm release,
        # which this proof deliberately does not install. Only that refusal is
        # tolerated, and the API state is verified below either way.
        require(
            deployed.returncode == 0
            or (
                b"could not read the app name for release" in deployed.stdout
                and b"as " + AGENT_NAME.encode() + b": ok" in deployed.stderr
            ),
            "cluster deploy failed before its API deploy completed",
        )
        self.evidence["agent_deploy"] = {
            "api_deploy": "ok",
            "helm_connector_step": "ok" if deployed.returncode == 0 else "no release (expected)",
        }
        agents = self.sql_rows(
            "SELECT id, tenant_id FROM curie.agents WHERE name = :name", {"name": AGENT_NAME}
        )
        require(len(agents) == 1, "the deployed agent is missing")
        self.agent_id = str(agents[0]["id"])
        self.tenant_id = str(agents[0]["tenant_id"])
        bound = self.sql_scalar(
            "SELECT count(*) FROM curie.agent_channels WHERE agent_id = :agent "
            "AND kind = :kind AND address = :address",
            {"agent": uuid.UUID(self.agent_id), "kind": KIND, "address": ADDRESS},
        )
        require(bound == 1, "the deployed agent is not bound to its channel")
        for who, email in (("owner", OWNER_EMAIL), ("other", OTHER_EMAIL)):
            session = oidc_login(self.api_url, self.dex_origin, email)
            self.sessions[who] = session
            self.private_file(f"session-{who}", session)
            principal = oidc.request(
                "GET", f"{self.api_url}/console/principal",
                headers={"Cookie": f"{SESSION_COOKIE}={session.reveal()}"},
            )
            require(principal.status == 200, "a minted session is not principal bound")
            self.principals[who] = {
                "id": str(principal.json()["id"]),
                "tenant_id": str(principal.json()["tenant_id"]),
            }
        self.check("AC1", "both principals belong to the agent's tenant", True,
                   all(item["tenant_id"] == self.tenant_id for item in self.principals.values()))
        self.check("AC1", "the two principals differ", True,
                   self.principals["owner"]["id"] != self.principals["other"]["id"])
        if self.phase == "synthetic":
            self.private_file("token-valid", self.grant)
        self.private_file("token-invalid", self.invalid)
        for who in ("owner", "other"):
            body, _ = self.subscription("status", who)
            self.check("AC4", f"status before enroll ({who})", "absent", body.get("state"))
        self.launcher = self.run_async(self._make_launcher)
        self.service_thread, self.service_claim = self.run_async(
            self.launch.service_claim, timeout=400
        )
        pod = self.wait_pod(f"{ADMISSION_LABEL}={self.service_claim}", timeout=240)
        self.observe_claim(self.service_claim, pod, personal=False, grant=self.grant.reveal())

    async def _make_launcher(self) -> Launcher:
        return Launcher(self)

    @property
    def valid_token(self) -> Path:
        return Path(self.grant_file) if self.grant_file else self.files["token-valid"]

    def step_a(self) -> None:
        """Enroll generation 1, deny the other principal, run session 1, write marker M."""

        __tracebackhide__ = True
        oidc = oidc_harness()
        self.headers_seen["enrolled"] = self.write("enroll", token=self.valid_token, previous=None)
        version = int(self.headers_seen["enrolled"]["version"])
        refused, _ = self.subscription(
            "disconnect", "other", expected_version=version, expect_success=False
        )
        self.check("AC1", "another principal's disconnect is refused as absent",
                   True, "enrollment_absent" in str(refused.get("error")))
        other_status, _ = self.subscription("status", "other")
        self.check("AC1", "another principal sees no enrollment", "absent", other_status["state"])
        platform = oidc.request(
            "DELETE", f"{self.api_url}/personal-claude/enrollment?expected_version={version}",
            headers={"X-API-Key": self.platform_key.reveal(), "Origin": self.api_url},
        )
        self.check("AC1", "the platform key cannot act on an enrollment", 401, platform.status)
        status_code, body = self.admit(1, principal="other")
        self.check("AC1", "admission as an unenrolled principal is refused",
                   [409, "enrollment_inactive"],
                   [status_code, (body.get("detail") or {}).get("code")])
        header = self.enrollment_headers()[0]
        self.check("AC1", "owner header unchanged by the refused writes", [1, version],
                   [header["generation"], header["version"]])
        self.s1 = self.start_session(1)
        if self.phase == "live":
            self.live_session(
                self.s1, "a",
                [f"Remember the code word {self.marker}. Reply with exactly: STORED"],
                expect="done",
            )
        else:
            output = self.exec(
                self.s1.pod, "python3", "-c", _TRANSCRIPT_WRITE, self.marker, uuid.uuid4().hex
            )
            self.check("AC5", "session 1 wrote marker M through the runner's history store",
                       b"appended", output.strip())
        wait_until(lambda: True if self.transcript_has_marker() else None, timeout=60,
                   interval=1, failure="marker M never reached the durable transcript")
        self.check("AC5", "marker M is in the thread transcript", True,
                   self.transcript_has_marker())
        self.capture_pod(self.s1.pod)

    def step_b(self) -> None:
        """Replace with a refused token, tear down, revalidate; session 2 is refused."""

        __tracebackhide__ = True
        from curie_worker.kernel import constants, failures

        self.headers_seen["replaced"] = self.write(
            "replace", token=self.files["token-invalid"], previous=self.headers_seen["enrolled"]
        )

        def fenced() -> dict[str, Any] | None:
            row = self.request_row(self.s1.request_id)
            return row if row["status"] != "running" else None

        row = wait_until(fenced, timeout=30, interval=1,
                         failure="the replacement did not fence generation 1")
        self.check("AC3", "replacement fences generation 1 immediately", "enrollment_fenced",
                   row["terminal_cause"])
        self.wait_teardown(self.s1, cause="enrollment_fenced")
        self.check("AC3", "the driver's heartbeat observed the fence", True,
                   "cancellation_requested" in self.s1.stops)
        self.revalidate(2)
        self.s2 = self.start_session(2)
        if self.phase == "live":
            events = self.live_session(
                self.s2, "b", ["Reply with exactly: REFUSED-CONTROL"],
                expect="classified-failure",
            )
            raw = next(
                (event.get("classification") for event in events
                 if event.get("type") == "error" and event.get("classification")),
                None,
            )
            self.check("AC5", "generation 2 provider refusal is authentication-required",
                       "authentication-required", raw)
        else:
            # Synthetic regression control only: the live phase observes the
            # real provider refusal; nothing synthetic stands in for it there.
            raw = "authentication-required"
            self.evidence["synthetic_refusal_control"] = True
        cause = constants._ESCALATION_CAUSES.get(
            failures.map_error_classification(raw), "runner_escalated"
        )
        self.check("AC5", "refusal maps to the authentication_required cause",
                   "authentication_required", cause)
        self.run_async(lambda: self.launch.finish(self.s2, cause))
        row = self.request_row(self.s2.request_id)
        self.check("AC5", "generation 2 request failed authentication_required",
                   ["failed", "authentication_required"], [row["status"], row["terminal_cause"]])
        self.check("AC5", "marker M survives the authentication-required finish", True,
                   self.transcript_has_marker())
        self.capture_pod(self.s2.pod)

    def step_c(self) -> None:
        """Replace with the valid grant, confirm teardown, resume in a fresh process."""

        __tracebackhide__ = True
        self.headers_seen["restored"] = self.write(
            "replace", token=self.valid_token, previous=self.headers_seen["replaced"]
        )
        self.wait_teardown(self.s2, cause="authentication_required")
        self.check("AC3", "confirmation preserved the failed outcome", "failed",
                   self.request_row(self.s2.request_id)["status"])
        self.check("AC5", "the thread transcript survived both fences", True,
                   self.transcript_has_marker())
        self.revalidate(3)
        self.s3 = self.start_session(3)
        self.check("AC5", "generation 3 runs in a fresh claim and pod", True,
                   self.s3.claim_name not in {self.s1.claim_name, self.s2.claim_name}
                   and self.s3.pod not in {self.s1.pod, self.s2.pod})
        if self.phase == "synthetic":
            output = json.loads(
                self.exec(self.s3.pod, "python3", "-c", _TRANSCRIPT_READ, self.marker)
            )
            self.check("AC5", "the fresh generation 3 process rehydrates marker M", True,
                       output.get("found"))

    def step_d(self) -> None:
        """Disconnect during work, restart the worker, observe durable denial."""

        __tracebackhide__ = True
        if self.phase == "live":
            self.headers_seen["disconnected"] = self.live_disconnect_during_work()
        else:
            self.capture_pod(self.s3.pod)
            self.headers_seen["disconnected"] = self.write(
                "disconnect", token=None, previous=self.headers_seen["restored"]
            )
        self.wait_teardown(self.s3, cause="enrollment_fenced")
        self.stop_worker()
        self.start_worker()
        for generation in (3, 4):
            refusal = self.run_async(functools.partial(self.launch.read_grant, generation))
            self.check("AC5", f"custody reader refuses generation {generation} after disconnect",
                       "enrollment_inactive", getattr(refusal, "code", "granted"))
        status_code, body = self.run_async(self.launch.revalidate)
        self.check("AC5", "revalidation refuses the disconnected enrollment",
                   [409, "enrollment_inactive"], [status_code, body.get("code")])
        status_code, body = self.admit(4)
        self.check("AC5", "admission refuses the disconnected enrollment",
                   [409, "enrollment_inactive"],
                   [status_code, (body.get("detail") or {}).get("code")])
        self.check("AC5", "no route is reusable for the thread", True,
                   self.run_async(self.launch.lookup) is None)
        self.check("AC5", "every earlier claim is absent", True,
                   all(self.absent("sandboxclaims", item.claim_name)
                       for item in (self.s1, self.s2, self.s3)))
        status, _ = self.subscription("status", "owner")
        self.check("AC4", "status after disconnect", ["inactive", 4],
                   [status.get("state"), status.get("generation")])

    def live_disconnect_during_work(self) -> dict[str, Any]:
        """Session 3: recall M, then a long tool turn the owner disconnects during."""

        __tracebackhide__ = True
        assert self.ledger is not None
        remaining = self.ledger.remaining()
        with self.forward(self.s3.pod) as port:
            sequence, allowance = self.ledger.start(self.candidate)
            require(allowance == remaining, "live allowance changed before session admission")
            started = time.monotonic()
            with self.session_deadline(self.s3.pod, self.s3.runner_token, port) as deadline:
                first, _raw, _elapsed, ended = self.stream(
                    port, self.s3.runner_token,
                    "What code word did I ask you to remember? Reply with only the code word.",
                    deadline=deadline,
                )
                require(not ended, "session 3's first turn ended without a final; no paid retry")
                final = next(event for event in first if event.get("type") == "final")
                self.check("AC5", "session 3 answers marker M from the rehydrated transcript",
                           [True, "done"],
                           [self.marker in str(final.get("text")), final.get("status")])
                tool = threading.Event()
                box: dict[str, Any] = {}

                def second_turn() -> None:
                    try:
                        box["result"] = self.stream(
                            port, self.s3.runner_token,
                            f"Use the Bash tool to run exactly: sleep {TOOL_SLEEP_SECONDS}. "
                            "Then reply DONE.",
                            on_event=lambda event: (
                                tool.set() if event.get("type") == "tool_note" else None
                            ),
                            deadline=deadline,
                        )
                    except Exception as exc:  # noqa: BLE001 - reported by the main thread
                        box["error"] = type(exc).__name__

                thread = threading.Thread(target=second_turn, daemon=True)
                self.readers.append(thread)
                thread.start()
                try:
                    ran_tool = wait_until(
                        lambda: True if tool.is_set() else (
                            False if not thread.is_alive() else None
                        ),
                        timeout=max(0, deadline - time.monotonic()), interval=0.5,
                        failure="session 3's second turn never ran its tool",
                    )
                    require(ran_tool, "session 3's second turn ended before running its tool")
                    observed = self.observe_files(self.s3.pod)
                    self.capture_pod(self.s3.pod)
                    self.check(
                        "AC5", "session 3's second turn is active when disconnect begins", True,
                        thread.is_alive() and "result" not in box and "error" not in box,
                    )
                    disconnected = self.write(
                        "disconnect", token=None, previous=self.headers_seen["restored"]
                    )
                finally:
                    thread.join(timeout=max(0, deadline - time.monotonic()))
                require(not thread.is_alive(), "session 3's interrupted turn did not stop")
                require("error" not in box, "session 3's second turn raised an error")
                require("result" in box, "session 3's second turn returned no result")
                second, _raw, _elapsed, ended = box["result"]
                finals = [event for event in second if event.get("type") == "final"]
                self.check(
                    "AC5", "session 3's disconnected turn interrupts or ends at teardown", True,
                    (len(finals) == 1 and finals[0].get("status") == "idle-awaiting-input")
                    or (not finals and ended),
                )
        self.account(sequence, self.s3, [*first, *second], observed,
                     time.monotonic() - started, allowance=allowance, name="c-d")
        return disconnected

    def step_e(self) -> None:
        """Reconnect generation 5; a fresh authorized process works."""

        __tracebackhide__ = True
        self.headers_seen["reconnected"] = self.write(
            "reconnect", token=self.valid_token, previous=self.headers_seen["disconnected"]
        )
        self.revalidate(5)
        self.s4 = self.start_session(5)
        self.check("AC5", "generation 5 runs in a fresh claim", True,
                   self.s4.claim_name not in {self.s1.claim_name, self.s2.claim_name,
                                              self.s3.claim_name})
        if self.phase == "live":
            self.live_session(self.s4, "e", ["Reply with exactly: RECONNECTED"], expect="done")
        self.capture_pod(self.s4.pod)

    def step_f(self) -> None:
        """Personal mode with no grant and a usable service credential refuses."""

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
                item for item in container.get("env", [])
                if item["name"] not in {"CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY",
                                        "ACME_MODEL_TOKEN", "CURIE_MODEL_AUTH_MODE",
                                        "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"}
            ]
        runner = spec["containers"][0]
        runner["env"] += [
            value_env("CURIE_MODEL_AUTH_MODE", "personal"),
            value_env("HTTPS_PROXY", "http://127.0.0.1:4319"),
            secret_env("CURIE_RUNNER_TOKEN", "refusal-auth", "CURIE_RUNNER_TOKEN"),
            secret_env("CURIE_PROGRESS_TOKEN", "refusal-auth", "CURIE_PROGRESS_TOKEN"),
        ]
        pod, _ = self.create_pod("missing-grant-service-present", spec)
        credential_names = json.loads(self.exec(
            pod, "python3", "-c",
            "import json, os; print(json.dumps(sorted(set(os.environ) & "
            "{'CURIE_CREDENTIALS', 'CURIE_MODEL_ENV_KEY', 'ACME_MODEL_TOKEN', "
            "'CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', "
            "'ANTHROPIC_BASE_URL'})))",
        ))
        self.check("AC5", "the missing-grant runner has no model credential environment", [],
                   credential_names)
        from curie_worker.sandbox.claim_tokens import claim_object_names

        personal_secret = self.core.read_namespaced_secret(
            claim_object_names(self.s4.claim_name).secret, self.namespace, _request_timeout=30,
        )
        self.assert_service_isolated(pod, personal_secret.data or {})
        with (
            self.forward(pod) as port,
            self.session_deadline(pod, self.runner_auth, port) as deadline,
        ):
            events, _raw, _elapsed, ended = self.stream(
                port, self.runner_auth, "Refuse before model work because no grant exists.",
                deadline=deadline,
            )
        require(not ended, "the missing-grant turn ended without a final")
        errors = [event for event in events if event.get("type") == "error"]
        finals = [event for event in events if event.get("type") == "final"]
        observations = self.observe_files(pod)
        self.check("AC5", "missing grant with a service credential present refuses", {
            "authentication_required": True, "final": "classified-failure",
            "provider_attempts": 0, "assistant_messages": 0, "usage_reports": 0, "tool_notes": 0,
        }, {
            "authentication_required": any(
                "authentication required" in str(item.get("message", "")).lower()
                for item in errors
            ),
            "final": finals[0].get("status") if len(finals) == 1 else "missing",
            "provider_attempts": observations["provider_attempts"],
            "assistant_messages": len(observations["assistant_messages"]),
            "usage_reports": len(observations["usage"]),
            "tool_notes": sum(event.get("type") == "tool_note" for event in events),
        })
        self.evidence["missing_grant_service_present"] = {
            "service_credential": "real" if self.phase == "live" else "placeholder",
            "sessions_consumed": 0,
        }
        self.capture_pod(pod)

    def retire(self) -> None:
        """Disconnect at the end; the product removes every runtime and custody copy."""

        __tracebackhide__ = True
        self.check("AC2", "the service claim stayed live through every fence", True,
                   not self.absent("sandboxclaims", self.service_claim))
        final = self.write("disconnect", token=None, previous=self.headers_seen["reconnected"])
        self.wait_teardown(self.s4, cause="enrollment_fenced")

        def custody_gone() -> bool | None:
            listed = self.core.list_namespaced_secret(
                self.grant_namespace, label_selector=CUSTODY_SELECTOR, _request_timeout=30
            )
            return True if not listed.items else None

        wait_until(custody_gone, timeout=180, interval=2,
                   failure="a custody Secret outlived its enrollment")
        self.check("AC2", "no custody Secret remains after disconnect", True, custody_gone())

        def settled() -> dict[str, Any] | None:
            body = self.subscription("status", "owner")[0]
            teardown = body.get("teardown") or {}
            pending = teardown.get("pending_runtimes") or teardown.get("pending_custody_cleanup")
            return None if pending else body

        status = wait_until(settled, timeout=120, interval=3,
                            failure="enrollment teardown stayed pending")
        self.check("AC4", "final status reports nothing pending",
                   ["inactive", int(final["generation"])], [status["state"], status["generation"]])
        self.check("AC2", "service control released through its own route", True,
                   self.run_async(self.launch.release_service))
        wait_until(lambda: True if self.absent("sandboxclaims", self.service_claim) else None,
                   timeout=120, interval=2, failure="the service control claim remained")

    def audit(self) -> None:
        """No grant copy anywhere but trusted storage, and none left at the end."""

        __tracebackhide__ = True
        for plural in ("sandboxclaims", "sandboxtemplates", "sandboxwarmpools"):
            self.captures.append(json.dumps(self.custom.list_namespaced_custom_object(
                GROUP, VERSION, self.namespace, plural, _request_timeout=30
            )).encode())
        for namespace in (self.namespace, self.grant_namespace):
            self.captures.append(json.dumps(self.api_client.sanitize_for_serialization(
                self.core.list_namespaced_event(namespace, _request_timeout=30)
            )).encode())
        grants = [self.grant.reveal(), self.invalid.reveal()]
        self.check("AC2", "grant bytes absent from manifests, pod logs, ACI streams, traces",
                   True, all(not leaks(capture, grants) for capture in self.captures))
        copies = 0
        for namespace in (self.namespace, self.grant_namespace):
            for secret in self.core.list_namespaced_secret(namespace, _request_timeout=30).items:
                for encoded in (secret.data or {}).values():
                    if base64.b64decode(encoded).decode(errors="replace") in grants:
                        copies += 1
        self.check("AC2", "grant copies left in any Secret after retirement", 0, copies)
        self.evidence["output_audit"] = {"passed": True, "captures": len(self.captures)}

    # --- cleanup ---------------------------------------------------------------------

    def cleanup(self) -> None:
        __tracebackhide__ = True
        problems: list[str] = []
        values = self.held_values()
        if self.launcher is not None:
            try:
                self.run_async(self.launch.close, timeout=60)
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("launcher cleanup failed")
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.loop.stop)
            if self.loop_thread is not None:
                self.loop_thread.join(timeout=10)
                if self.loop_thread.is_alive():
                    problems.append("driver event loop did not stop")
        for process in (self.worker_process, self.api_process):
            if process is not None:
                try:
                    self.stop_process(process)
                except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                    problems.append("source process cleanup failed")
        if self.relay is not None:
            self.relay.close()
            if self.relay.alive:
                problems.append("runner relay remains")
        for name in reversed(self.containers):
            try:
                owned = self.docker(
                    "container", "inspect", name,
                    "--format", '{{index .Config.Labels "curie-proof-owner"}}', check=False,
                )
                if owned.returncode == 0:
                    require(owned.stdout.decode().strip() == self.ownership,
                            "owned container label changed")
                    self.docker("rm", "--force", "--volumes", name)
                require(self.docker("container", "inspect", name, check=False).returncode != 0,
                        "owned container remains")
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("owned container cleanup failed")
        for volume in self.container_volumes:
            try:
                require(self.docker("volume", "inspect", volume, check=False).returncode != 0,
                        "owned container volume remains")
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("owned container volume remains")
        if self.grant_namespace_requested and self.core is not None:
            try:
                self.delete_owned_namespace(self.grant_namespace, self.grant_namespace_uid)
            except Exception:  # noqa: BLE001 - finish every independent owned cleanup
                problems.append("grant namespace cleanup failed")
        problems += self.audit_retained()
        private = self.private_dir
        try:
            super().cleanup()
        except Exception:  # noqa: BLE001 - recorded with every other cleanup problem
            problems.append("namespace, registry or image cleanup failed")
        if private:
            shutil.rmtree(private, ignore_errors=True)
            if Path(private).exists():
                problems.append("private run directory remains")
        self.evidence["cleanup"] = {
            **self.evidence.get("cleanup", {}),
            "passed": not problems,
            "grant_namespace_absent": "grant namespace cleanup failed" not in problems,
            "owned_containers_absent": not any("container" in item for item in problems),
            "private_files_absent": not Path(private).exists() if private else True,
            "source_processes_running": sum(p.poll() is None for p in self.processes),
            "problems": problems,
        }
        try:
            self.write_evidence(values=values)
        finally:
            for secret in self.held():
                secret.clear()
            values.clear()
        require(not problems, "; ".join(problems))

    def delete_owned_namespace(self, name: str, uid: str) -> None:
        """Delete one owned namespace by UID and wait for its absence."""

        from kubernetes import client

        if not uid:
            try:
                current = self.core.read_namespace(name, _request_timeout=30)
            except client.ApiException as exc:
                if exc.status == 404:
                    return
                raise
            require(
                (current.metadata.labels or {}).get("curie-proof-owner") == self.ownership,
                "namespace creation did not establish ownership",
            )
            uid = current.metadata.uid
        self.core.delete_namespace(
            name,
            body={"preconditions": {"uid": uid}, "propagationPolicy": "Foreground"},
            _request_timeout=30,
        )

        def absent() -> bool | None:
            try:
                current = self.core.read_namespace(name, _request_timeout=30)
            except client.ApiException as exc:
                if exc.status == 404:
                    return True
                raise
            require(current.metadata.uid == uid, "owned namespace identity changed")
            return None

        wait_until(absent, timeout=240, interval=2, failure="owned namespace still exists")

    def audit_retained(self) -> list[str]:
        """The retained private logs must not carry a held value; remove any that do."""

        problems: list[str] = []
        if self.retained_dir is None or not self.retained_dir.exists():
            return problems
        values = self.held_values()
        for path in self.retained_dir.iterdir():
            if path.suffix == ".log" and leaks(path.read_bytes(), values):
                path.unlink()
                problems.append("a retained log carried a private value and was removed")
        return problems

    def write_evidence(self, *, values: Sequence[str] | None = None) -> None:
        """Anonymized JSON per acceptance criterion, in the caller's directory."""

        values = self.held_values() if values is None else values
        require(bool(values) and all(values), "evidence audit has no private values")
        self.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        common = {
            "candidate_sha": self.evidence.get("candidate_sha"),
            "candidate_tree": self.evidence.get("candidate_tree"),
            "manifest_digest": self.evidence.get("manifest_digest"),
            "phase": self.phase,
            "budget": self.evidence.get("budget"),
            "cleanup": self.evidence.get("cleanup"),
            "failure": self.evidence.get("failure"),
            "stage": self.evidence.get("stage"),
        }
        documents = {name.lower(): {**common, **body} for name, body in self.ac.items()}
        documents["ac5"]["sessions"] = self.evidence.get("sessions")
        documents["ac5"]["steps"] = self.evidence.get("steps")
        documents["run"] = self.evidence
        withheld = False
        for name, document in documents.items():
            encoded = anonymize(
                json.dumps(document, indent=2, sort_keys=True, default=str).encode(),
                self.targets(),
            )
            if leaks(encoded, values):
                withheld = True
                encoded = json.dumps({"withheld": "evidence carried a private value"}).encode()
            path = self.evidence_dir / f"personal-enrollment-{name}.json"
            fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
        require(not withheld, "evidence carried a private value and was withheld")


class Launcher:
    """The explicit launch caller, on the driver's event loop (#5374's boundary).

    Every collaborator is a production class wired from the same values the
    candidate worker reads: the work-item dispatch client and run record, the
    binding resolver, the custody reader over a private Secret reader, and the
    sandbox substrate over a private kubeconfig.
    """

    def __init__(self, proof: EnrollmentProof) -> None:
        import httpx
        import redis
        from curie_worker.binding import BindingResolver
        from curie_worker.config import WorkerConfig
        from curie_worker.personal_custody import PersonalCustodyReader, build_secret_reader
        from curie_worker.run import _substrate_config
        from curie_worker.sandbox import AffinityStore, KubernetesSandboxClient, SandboxSubstrate
        from curie_worker.workitem_dispatch import WorkItemDispatchClient
        from redis.asyncio import Redis as AsyncRedis
        from sqlalchemy.ext.asyncio import create_async_engine

        self.proof = proof
        self.config = WorkerConfig(
            database_url=proof.database_url,
            db_schema="curie",
            valkey_host="127.0.0.1",
            valkey_port=proof.ports["valkey"],
            valkey_password=proof.valkey_password.reveal(),
            api_base_url=proof.api_url,
            runner_api_base_url=proof.runner_api_url,
            api_key=proof.platform_key.reveal(),
            internal_worker_token=proof.worker_token.reveal(),
            personal_grant_namespace=proof.grant_namespace,
            bundle_plugin_dir=PLUGIN_DIR,
            model=MODEL,
            thinking="disabled",
            connector_namespace=proof.namespace,
        )
        self.engine = create_async_engine(self.config.database_url)
        self.http = httpx.AsyncClient(timeout=30)
        self.headers = {"X-Curie-Worker-Token": proof.worker_token.reveal()}
        self.client = WorkItemDispatchClient(
            api_base_url=proof.api_url, worker_token=proof.worker_token.reveal(), client=self.http
        )
        self.resolver = BindingResolver(self.engine, self.config)
        self.reader = PersonalCustodyReader(
            self.engine,
            schema=self.config.db_schema,
            namespace=self.config.personal_grant_namespace,
            secrets=build_secret_reader(kubeconfig=proof.private_config),
        )
        self.sync_redis = redis.Redis(**self.config.valkey_client_kwargs())
        self.async_redis = AsyncRedis(**self.config.valkey_client_kwargs())
        self.substrate = SandboxSubstrate(
            KubernetesSandboxClient(proof.namespace, kubeconfig=proof.private_config),
            AffinityStore(self.sync_redis, pressure_client=self.async_redis),
            _substrate_config(proof.worker_env()),
        )
        self.runs: list[Any] = []

    def owner(self) -> tuple[uuid.UUID, uuid.UUID]:
        return uuid.UUID(self.proof.tenant_id), uuid.UUID(self.proof.principals["owner"]["id"])

    def wake(self) -> Any:
        from curie_worker.binding import FactoryWake

        return FactoryWake(kind=KIND, address=ADDRESS, adapter=None, conversation_id=CONVERSATION)

    async def admit(
        self, request_id: uuid.UUID, generation: int, principal: uuid.UUID
    ) -> tuple[int, dict[str, Any]]:
        """The trusted internal admission (#5376), as the worker would call it."""

        facts = {
            "agent_id": self.proof.agent_id,
            "kind": KIND,
            "address": ADDRESS,
            "reply_conversation_id": CONVERSATION,
            "tracker": {"kind": "github", "host": "github.com", "scope_id": "1000001",
                        "issue_id": "7"},
            "repository": {"code_host_kind": "github", "host": "github.com",
                           "project_id": "1000001", "path": REPOSITORY},
            # A GitHub code host carries the installation that admitted it
            # (``work_items_github_identity_ck``); nothing here redeems it.
            "code_host_installation_id": 202,
            "objective": "Owner-scoped enrollment proof request.",
            "requester": REQUESTER,
            "request_id": str(request_id),
            "model_auth_mode": "personal",
            "owner": {"tenant_id": self.proof.tenant_id, "principal_id": str(principal),
                      "enrollment_generation": generation},
        }
        response = await self.http.post(
            f"{self.proof.api_url}/v1/internal/work-items/admissions",
            headers=self.headers, json=facts,
        )
        return response.status_code, response.json()

    async def launch(self, request_id: uuid.UUID, generation: int) -> Launched:
        """Acquire, read custody, render the boot env, claim and start (#5374's boundary)."""

        from channel_protocol.work_item_events import execute_event_id
        from curie_worker.binding import RUNNER_TOKEN_ENV
        from curie_worker.personal_custody import PersonalGrant
        from curie_worker.workitem_dispatch import WorkItemRun

        view = await self.http.get(
            f"{self.proof.api_url}/v1/internal/work-items/requests/{request_id}",
            headers=self.headers,
        )
        require(view.status_code == 200, "the admitted request is unreadable")
        dispatch_generation = int(view.json()["dispatch_generation"])
        acquired = await self.client.acquire(
            request_id, owner=DRIVER_OWNER, generation=dispatch_generation, wake=self.wake(),
            context=None,
        )
        stops: list[str] = []

        async def stopped(thread_key: str, run: Any) -> None:
            stops.append("cancellation_requested")

        async def stale(thread_key: str, run: Any) -> None:
            stops.append("stale_owner")

        run = WorkItemRun(
            client=self.client, request_id=request_id, owner=DRIVER_OWNER, grant=acquired,
            wake=self.wake(), event_id=execute_event_id(request_id, dispatch_generation),
            thread_key=acquired.conversation_id, on_stop=stopped, on_stale=stale,
        )
        self.runs.append(run)
        resolved = await self.resolver.resolve_factory(acquired.context)
        require(resolved is not None, "the binding resolver found no deployment")
        assert resolved is not None
        tenant, principal = self.owner()
        grant = await self.reader.read_grant(tenant, principal, generation)
        require(isinstance(grant, PersonalGrant), "the custody reader refused a current grant")
        assert isinstance(grant, PersonalGrant)
        env = {
            **self.resolver.boot_env(
                resolved, acquired.conversation_id, kind=KIND, address=ADDRESS
            ),
            **grant.claim_env(),
            # The approved ceiling, enforced by the runner on every turn.
            "CURIE_BUDGET": json.dumps(
                {"max_output_tokens_per_run": MAX_SESSION_OUTPUT_TOKENS, "max_usd_per_day": 1}
            ),
            "CURIE_MAX_TURNS": str(MAX_TURNS_PER_RUN),
            "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(PER_RESPONSE_OUTPUT_TOKENS),
            "CURIE_MODEL": MODEL,
            "CURIE_REVIEWER_MODEL": MODEL,
        }
        runner_token = HeldSecret(env[RUNNER_TOKEN_ENV])
        held_grant = HeldSecret(grant.claim_env()["CLAUDE_CODE_OAUTH_TOKEN"])
        self.proof.session_tokens.append(runner_token)
        handle = await asyncio.to_thread(
            self.substrate.claim, acquired.conversation_id, env=env,
            agent_name=resolved.agent_name,
        )
        del env
        await run.start(claim_name=handle.claim_name, sandbox_name=handle.sandbox_name)
        return Launched(
            request_id=request_id, generation=generation, run=run,
            claim_name=handle.claim_name, sandbox_name=handle.sandbox_name,
            runner_token=runner_token, grant=held_grant, stops=stops,
        )

    async def service_claim(self) -> tuple[str, str]:
        """A service-mode claim of the same agent on its own thread."""

        from aci_protocol.turn import route_identity
        from curie_worker.binding import RUNNER_TOKEN_ENV, FactoryContext
        from sqlalchemy import text

        async with self.engine.connect() as conn:
            binding = (await conn.execute(
                text("SELECT id, adapter FROM curie.agent_channels WHERE agent_id = :agent "
                     "AND kind = :kind AND address = :address"),
                {"agent": uuid.UUID(self.proof.agent_id), "kind": KIND, "address": ADDRESS},
            )).one()
        # The context the API would issue for this binding (``_factory_context``).
        resolved = await self.resolver.resolve_factory(FactoryContext(
            agent_id=uuid.UUID(self.proof.agent_id), tenant_id=uuid.UUID(self.proof.tenant_id),
            binding_id=binding.id, kind=KIND, address=ADDRESS,
            adapter=route_identity(KIND, binding.adapter),
        ))
        require(resolved is not None, "the binding resolver found no deployment")
        assert resolved is not None
        thread = f"{KIND}:{ADDRESS}:{SERVICE_CONVERSATION}"
        env = self.resolver.boot_env(resolved, thread, kind=KIND, address=ADDRESS)
        self.proof.session_tokens.append(HeldSecret(env[RUNNER_TOKEN_ENV]))
        handle = await asyncio.to_thread(
            self.substrate.claim, thread, env=env, agent_name=resolved.agent_name
        )
        return thread, handle.claim_name

    async def release_service(self) -> bool:
        return bool(await asyncio.to_thread(
            self.substrate.release_if_claim, self.proof.service_thread, self.proof.service_claim
        ))

    async def read_grant(self, generation: int) -> Any:
        tenant, principal = self.owner()
        return await self.reader.read_grant(tenant, principal, generation)

    async def lookup(self) -> Any:
        return await asyncio.to_thread(self.substrate.lookup, self.proof.thread_key)

    async def finish(self, launched: Launched, cause: str) -> None:
        await launched.run.finish(outcome="failed", cause=cause, detail=None, ci_fix_round=None)

    async def revalidate(self) -> tuple[int, dict[str, Any]]:
        """Read the token-free personal context, then compare-and-set its versions."""

        tenant, principal = self.owner()
        base = f"{self.proof.api_url}/v1/internal/work-items/{self.proof.work_item_id}"
        context = await self.http.get(
            f"{base}/personal-context", headers=self.headers,
            params={"tenant_id": str(tenant), "agent_id": self.proof.agent_id,
                    "owner_principal_id": str(principal)},
        )
        require(context.status_code == 200, "the personal context is unreadable")
        facts = context.json()
        enrollment = facts.get("enrollment") or {}
        response = await self.http.post(
            f"{base}/personal-context/revalidate", headers=self.headers,
            json={
                "tenant_id": str(tenant), "agent_id": self.proof.agent_id,
                "owner_principal_id": str(principal), "request_id": facts["request"]["id"],
                "expected_work_item_version": facts["work_item_version"],
                "expected_request_version": facts["request"]["version"],
                "expected_enrollment_version": int(enrollment.get("version") or 1),
            },
        )
        body = response.json()
        detail = body.get("detail")
        return response.status_code, detail if isinstance(detail, dict) else body

    async def close(self) -> None:
        for run in self.runs:
            with contextlib.suppress(Exception):
                await run.close()
        await self.http.aclose()
        await self.engine.dispose()
        await self.async_redis.aclose()
        self.sync_redis.close()


__all__ = [
    "EnrollmentLedger",
    "EnrollmentProof",
    "HeldSecret",
    "ProofCheckError",
    "TcpRelay",
    "anonymize",
    "leaks",
    "read_private_input",
    "validate_inputs",
    "wait_until",
]
