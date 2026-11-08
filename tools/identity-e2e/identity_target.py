"""Process, kube and platform API access for the identity E2E harness.

Everything here reaches the install only through ``kubectl``, ``helm``, a
loopback port-forward or the ``curie`` CLI. The in-pod scripts run inside the
release's API container so signing and adoption use the deployed product code.
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
# The email binding adapter slug: the worker's reply secret key and the
# receiver registration's binding owner.
ADAPTER = "identity-e2e"
API_PORT = 8000
TOKEN_TTL_SECONDS = 86_400


class HarnessError(Exception):
    """A refusal or failed step; the message never carries secret material."""


def log(message: str) -> None:
    sys.stderr.write(f"identity-e2e: {message}\n")
    sys.stderr.flush()


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def summarize_event(event: Any, limit: int = 200) -> Any:
    """A reply event with long strings truncated, for evidence."""

    if isinstance(event, dict):
        return {key: summarize_event(value, limit) for key, value in event.items()}
    if isinstance(event, list):
        return [summarize_event(value, limit) for value in event[:20]]
    if isinstance(event, str) and len(event) > limit:
        return event[:limit] + "..."
    return event


def write_private(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` with mode 0600, replacing it atomically."""

    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, path)
    os.chmod(path, 0o600)


# --- process helpers -------------------------------------------------------------


def run(
    argv: Sequence[str],
    *,
    input_text: str | None = None,
    check: bool = True,
    timeout: float = 600,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command, capturing output. A failure names the command, not its input."""

    try:
        result = subprocess.run(  # noqa: S603 - argv lists only
            list(argv),
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=None if env is None else {**os.environ, **env},
            cwd=cwd,
        )
    except FileNotFoundError:
        raise HarnessError(f"{argv[0]} is not installed or not on PATH") from None
    except subprocess.TimeoutExpired:
        raise HarnessError(f"{' '.join(argv[:3])} timed out after {timeout:.0f}s") from None
    if check and result.returncode != 0:
        tail = (result.stderr or result.stdout).strip().splitlines()[-8:]
        raise HarnessError(f"{' '.join(argv[:4])} exited {result.returncode}: " + " | ".join(tail))
    return result


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http(
    method: str,
    url: str,
    *,
    body: Any = None,
    raw: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 60,
) -> tuple[int, Any]:
    """One HTTP request; returns (status, parsed JSON or text)."""

    data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
    request = urllib.request.Request(url, data=data, method=method)  # noqa: S310
    if data is not None:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            status, payload = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, payload = exc.code, exc.read()
    text = payload.decode("utf-8", "replace")
    try:
        return status, json.loads(text) if text else None
    except ValueError:
        return status, text


# --- in-pod product calls ----------------------------------------------------------

# Each script reads its parameters as JSON on stdin and prints one JSON line.
# They run in the API container, against the deployed product code.
POD_PRELUDE = """
import asyncio, json, sys, uuid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from curie_api.config import get_settings
P = json.load(sys.stdin)

async def _with_session(body):
    engine = create_async_engine(get_settings().database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            return await body(session)
    finally:
        await engine.dispose()

def emit(value):
    print(json.dumps(value, default=str))
"""

SQL_SCRIPT = (
    POD_PRELUDE
    + """
async def body(session):
    rows = []
    async with session.begin():
        for statement in P["statements"]:
            result = await session.execute(text(statement["sql"]), statement.get("params", {}))
            if result.returns_rows:
                rows.append([dict(row._mapping) for row in result])
            else:
                rows.append(result.rowcount)
    return rows

emit(asyncio.run(_with_session(body)))
"""
)

SIGN_SCRIPT = """
import json, sys
from curie_internal import identity_context as codec
from curie_internal.sandbox_token import b64url_decode
P = json.load(sys.stdin)
proof = codec.EmailReceiverProofV1.model_validate_json(json.dumps(P["proof"]))
ring = codec.build_identity_keyring(
    "receiver", ((P["proof"]["receiver_id"], P["proof"]["receiver_generation"],
                  b64url_decode(P["key"])),),
)
print(json.dumps({"proof": codec.sign_email_receiver_proof(proof, ring)}))
"""

ADOPT_SCRIPT = (
    POD_PRELUDE
    + """
from curie_api import principal_authority
from curie_api.models import TenantIdentityProvider
from curie_api.schemas.http_authority import HttpSelectionV1

async def body(session):
    tenant = uuid.UUID(P["tenant_id"])
    selection = HttpSelectionV1.model_validate(P["selection"])
    async with session.begin():
        _, row = await principal_authority.lock_scope(session, tenant)
        if row is None:
            result = await principal_authority.adopt_http_authority(
                session, tenant_id=tenant, expected_version=0,
                expected_lifecycle_generation=0, selection=selection,
            )
            return {"action": "adopted", "created_row": True,
                    "version": result.configuration.version}
        current = row.http_selection
        if current == selection.model_dump(mode="json"):
            return {"action": "unchanged", "created_row": False, "version": row.version}
        previous = {"http_selection": current, "issuer": row.issuer}
        writer = (principal_authority.adopt_http_authority if current is None
                  else principal_authority.replace_http_configuration)
        result = await writer(
            session, tenant_id=tenant, expected_version=row.version,
            expected_lifecycle_generation=row.lifecycle_generation, selection=selection,
        )
        return {"action": "replaced" if current else "adopted", "created_row": False,
                "previous": previous, "version": result.configuration.version}

emit(asyncio.run(_with_session(body)))
"""
)

ROLE_FILTER_SCRIPT = (
    POD_PRELUDE
    + """
from curie_api import agent_role_policy
from curie_api.schemas.agent_access import SetRoleFilterV1

async def body(session):
    tenant, agent = uuid.UUID(P["tenant_id"]), uuid.UUID(P["agent_id"])
    async with session.begin():
        current = await agent_role_policy.read_role_policy(
            session, tenant_id=tenant, agent_id=agent)
    version = 0 if current is None else current.version
    role = None if current is None else current.roles.invocation
    previous = None if role is None or role.filter is None else role.filter.model_dump(mode="json")
    if previous == P["filter"] and role is not None:
        return {"action": "unchanged", "previous": previous, "version": version}
    change = SetRoleFilterV1.model_validate({
        "schema_version": 1, "tenant_id": str(tenant), "agent_id": str(agent),
        "expected_version": version, "role": "invocation", "operation": "set_filter",
        "filter": P["filter"],
    })
    async with session.begin():
        result = await agent_role_policy.apply_role_change(session, change=change)
    return {"action": "set", "previous": previous, "version": result.version}

emit(asyncio.run(_with_session(body)))
"""
)


# --- the target install ------------------------------------------------------------


class Target:
    """One release on one kube context, reached only through kubectl and helm."""

    def __init__(
        self, *, context: str, namespace: str, release: str, curie_bin: str, chart: Path
    ) -> None:
        if not context or not namespace or not release:
            raise HarnessError("pass an explicit --context, --namespace and --release")
        self.context = context
        self.namespace = namespace
        self.release = release
        self.curie_bin = curie_bin
        self.chart = chart
        # Set by the harness to a directory under --state-dir.
        self.private_config = ""
        self._fullname: str | None = None

    # kubectl / helm ---------------------------------------------------------------

    def kubectl(
        self, *args: str, input_text: str | None = None, check: bool = True, timeout: float = 600
    ) -> subprocess.CompletedProcess[str]:
        return run(
            ["kubectl", "--context", self.context, "-n", self.namespace, *args],
            input_text=input_text,
            check=check,
            timeout=timeout,
        )

    def kubectl_json(self, *args: str) -> Any:
        return json.loads(self.kubectl(*args, "-o", "json").stdout)

    def helm(self, *args: str, timeout: float = 1200) -> subprocess.CompletedProcess[str]:
        return run(
            ["helm", "--kube-context", self.context, "-n", self.namespace, *args],
            timeout=timeout,
        )

    def helm_values(self) -> dict[str, Any]:
        out = self.helm("get", "values", self.release, "-o", "json").stdout.strip()
        value = json.loads(out) if out and out != "null" else {}
        return value or {}

    # discovery ----------------------------------------------------------------

    def component(self, component: str) -> str:
        names = self.kubectl(
            "get",
            "deployment",
            "-l",
            f"app.kubernetes.io/instance={self.release},app.kubernetes.io/component={component}",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ).stdout.split()
        if len(names) != 1:
            raise HarnessError(
                f"expected one {component} Deployment for release {self.release}, found {names}"
            )
        return names[0]

    @property
    def fullname(self) -> str:
        if self._fullname is None:
            api = self.component("api")
            if not api.endswith("-api"):
                raise HarnessError(f"unexpected api Deployment name {api}")
            self._fullname = api.removesuffix("-api")
        return self._fullname

    def api_image(self) -> str:
        deployment = self.kubectl_json("get", "deployment", self.component("api"))
        containers = deployment["spec"]["template"]["spec"]["containers"]
        return str(next(c for c in containers if c["name"] == "api")["image"])

    def platform_key(self) -> str:
        encoded = self.kubectl(
            "get", "secret", f"{self.fullname}-secrets", "-o", "jsonpath={.data.apiKey}"
        ).stdout.strip()
        if not encoded:
            raise HarnessError(f"{self.fullname}-secrets has no apiKey")
        return base64.b64decode(encoded).decode("utf-8")

    def rollout(self, *deployments: str, restart: bool = False, timeout: str = "600s") -> None:
        for name in deployments:
            if restart:
                self.kubectl("rollout", "restart", f"deployment/{name}")
        for name in deployments:
            self.kubectl("rollout", "status", f"deployment/{name}", f"--timeout={timeout}")

    # in-pod product calls -----------------------------------------------------------

    def pod_python(self, script: str, params: Mapping[str, Any]) -> Any:
        result = run(
            [
                "kubectl",
                "--context",
                self.context,
                "-n",
                self.namespace,
                "exec",
                "-i",
                f"deployment/{self.component('api')}",
                "-c",
                "api",
                "--",
                "python",
                "-c",
                script,
            ],
            input_text=json.dumps(params),
            timeout=120,
        )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if not lines:
            raise HarnessError("the in-pod product call printed nothing")
        return json.loads(lines[-1])

    def sql(self, *statements: tuple[str, Mapping[str, Any]]) -> list[Any]:
        """Run statements in one transaction inside the API pod."""

        return list(
            self.pod_python(
                SQL_SCRIPT,
                {
                    "statements": [
                        {"sql": sql, "params": dict(params)} for sql, params in statements
                    ]
                },
            )
        )

    # port-forwards --------------------------------------------------------------

    @contextlib.contextmanager
    def forward(self, service: str, remote_port: int, health: str | None) -> Iterator[str]:
        port = free_port()
        process = subprocess.Popen(  # noqa: S603
            [
                "kubectl",
                "--context",
                self.context,
                "-n",
                self.namespace,
                "port-forward",
                f"service/{service}",
                f"{port}:{remote_port}",
                "--address",
                "127.0.0.1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        base = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise HarnessError(f"port-forward to {service} exited")
                try:
                    if health is None:
                        with socket.create_connection(("127.0.0.1", port), timeout=2):
                            break
                    else:
                        status, _ = http("GET", base + health, timeout=5)
                        if status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise HarnessError(f"port-forward to {service} never became healthy")
                time.sleep(0.5)
            yield base
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

    # curie CLI --------------------------------------------------------------------

    def curie(self, *args: str, timeout: float = 600) -> subprocess.CompletedProcess[str]:
        """Run the curie CLI against this release.

        The platform key travels in the child's environment, never on argv.
        The child gets an empty private config directory: over a loopback
        port-forward the CLI otherwise substitutes a key stored for a local
        compose stack when the supplied key is the dev default, and a
        ``--dev`` install's key is that default.
        """

        config = Path(self.private_config)
        config.mkdir(mode=0o700, parents=True, exist_ok=True)
        return run(
            [
                self.curie_bin,
                *args,
                "--namespace",
                self.namespace,
                "--release",
                self.release,
                "--context",
                self.context,
            ],
            check=False,
            timeout=timeout,
            env={"CURIE_API_KEY": self.platform_key(), "CURIE_CONFIG_DIR": str(config)},
            # A source-built CLI resolves charts/curie from its working directory.
            cwd=REPO,
        )


class Api:
    """The platform API through a port-forward, authenticated with the platform key."""

    def __init__(self, base: str, key: str) -> None:
        self.base = base
        self._key = key

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return http(method, self.base + path, body=body, headers={"X-API-Key": self._key})

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        status, payload = self.call(method, path, body)
        if not 200 <= status < 300:
            raise HarnessError(f"{method} {path} answered {status}: {summarize_event(payload)}")
        return payload

    def agents(self) -> dict[str, dict[str, Any]]:
        return {agent["name"]: agent for agent in self.ok("GET", "/agents")}

    def agent(self, agent_id: str) -> dict[str, Any]:
        return dict(self.ok("GET", f"/agents/{agent_id}"))

    def mint_channel_token(self, address: str) -> dict[str, Any]:
        token = self.ok(
            "POST",
            "/channels/token",
            {"kind": "email", "address": address, "adapter": ADAPTER, "ttl_s": TOKEN_TTL_SECONDS},
        )["token"]
        return {"token": token, "expires_at": int(time.time()) + TOKEN_TTL_SECONDS}
