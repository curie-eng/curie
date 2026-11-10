"""#4630: a replaced static forge token takes effect without an API restart.

A source-built CLI and API image are installed in an owned, disposable kind
cluster with the real chart, Postgres, Valkey, Helm storage and Kubernetes
Secret projection. The only fixture is the external forge: the stateful GitLab
fake `apps/api/tests/forge_fakes/gitlab.py`, served over TLS from a test CA
inside the same cluster by the candidate API image. It is reachable as
``https://fake-gitlab.localhost:<P>`` both from this machine (``*.localhost``
resolves to loopback, where the kind host port mapped to node port 30081
listens) and from the API pod (CoreDNS answers the name with the fake's
Service address, and the Service listens on the same port P).

The fake registers two tokens for its bot user, the initial and the
replacement; the rejected token is never registered, so the fake answers it
401. The regression replaces the referenced Secret with the rejected token,
waits for the kubelet to project it into the running API pod, and expects
intake to stop and cluster status to name the rejection. Restoring a valid
(the replacement) token must resume intake in the same pod.

Runtime prerequisites: Docker, kind, kubectl, Helm, Cargo, git and openssl.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
PREFIX = "test-4630"
AGENT = "acme-rotation"
# The fake's own project (forge_fakes.gitlab.PROJECT_PATH / PROJECT_ID).
PROJECT_PATH = "acme/platform/widgets"
PROJECT_ID = 4242
NAMESPACE = "test-4630-rotation"
FORGE_NAMESPACE = "test-4630-forge"
RELEASE = "test-4630"
FULLNAME = "test-4630"
FORGE_HOST = "fake-gitlab.localhost"
FORGE_CONTAINER_PORT = 8443
API_NODE_PORT = 30080
FORGE_NODE_PORT = 30081
# AC 1: the windows after each projection change, and the test's poll ceiling.
WINDOW_S = 60.0
POLL_S = 5.0
# The kubelet refreshes a projected Secret on its sync period plus its cache
# TTL; this is the outer bound the harness waits for that, not a product limit.
PROJECTION_TIMEOUT_S = 240.0
# Positive is the only bound `TrackerConfig.poll_interval_s` sets; the API's
# reconciler loop (5 seconds) is then the effective cadence.
POLL_INTERVAL_SECONDS = 1
# A status call inside a polling window fails explicitly well before the window
# ends, so a hung port-forward cannot stall a phase silently.
STATUS_TIMEOUT_S = 30
# A configured expiry well past the 14 day warning window, so status reports it
# as `known` while authentication is judged separately.
TOKEN_EXPIRES_AT = (datetime.now(UTC).date() + timedelta(days=200)).isoformat()

# Served by the candidate API image (python, starlette, uvicorn, httpx and
# curie_api are installed there) with the fake module beside it.
FORGE_SERVER = '''
"""The GitLab fake behind TLS, plus the control routes the #4630 test drives."""

import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import uvicorn
from curie_api.forges.types import Actor
from forge_fakes.gitlab import BOT, GitLabAPI, _Token
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

API = os.environ["FAKE_GITLAB_API"]
ORIGIN = API.removesuffix("/api/v4")
STARTER = Actor(id="901", login="acme-starter")
DEVELOPER = 30


class Forge(GitLabAPI):
    """The fake plus project labels, which the quickstart reads and creates
    (https://docs.gitlab.com/api/labels/)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.project_labels = {}

    # The API's poll cursor follows the server's Date header, so the fake's
    # clock follows real time: each change is at least one step after the last.
    def tick(self, at=None):
        if at is not None:
            return super().tick(at)
        self.now = max(self.now + self.step, datetime.now(UTC))
        return self.now

    def handle(self, request):
        self.now = max(self.now, datetime.now(UTC))
        return super().handle(request)

    def _project_route(self, request, user, rest, params, query, method):
        if rest == ["labels"]:
            if method == "POST":
                body = self.body(request) or {}
                name = body.get("name", "")
                if name in self.project_labels:
                    return self._error(request, 409, "Label already exists")
                self.project_labels[name] = {
                    "id": self._label_id(name),
                    "name": name,
                    "color": body.get("color", "#5319e7"),
                    "description": body.get("description", ""),
                }
                return self._json(request, self.project_labels[name], status=201)
            return self._page(request, list(self.project_labels.values()))
        return super()._project_route(request, user, rest, params, query, method)


fake = Forge(base_url=API, step_s=1.0)
# Error bodies quote the presented token and URL, so any path that surfaces a
# forge response text carries a sentinel the test sweeps for.
fake.echo_token_in_errors = True
# Only the tokens this test registers authenticate. The initial token reports
# a configured future expiry, which the quickstart records as expiresAt.
fake.tokens.clear()
ROLES = {}
EXPIRES_AT = os.environ["FAKE_TOKEN_EXPIRES_AT"]
for role in ("initial", "replacement"):
    value = Path("/tokens", role).read_text()
    fake.tokens[value] = _Token(
        int(BOT.id), ("api", "write_repository"), expires_at=EXPIRES_AT, id=len(fake.tokens) + 1
    )
    ROLES[value] = role
fake.add_user(STARTER)
fake.set_member(STARTER, DEVELOPER)


async def health(request: Request) -> Response:
    return JSONResponse({"ok": True})


async def create_issue(request: Request) -> Response:
    body = await request.json()
    iid = fake.add_issue(str(body["title"]), author=STARTER)
    return JSONResponse({"iid": iid}, status_code=201)


async def label_issue(request: Request) -> Response:
    body = await request.json()
    iid = request.path_params["iid"]
    fake.label(iid, str(body["label"]), STARTER)
    return JSONResponse({"iid": iid})


async def received(request: Request) -> Response:
    """How many forge requests presented each registered token, by role only."""

    counts = {"initial": 0, "replacement": 0, "unregistered": 0, "none": 0}
    for seen in fake.requests:
        presented = seen.headers.get("private-token")
        if presented is None:
            header = seen.headers.get("authorization", "")
            presented = header.removeprefix("Bearer ") if header.startswith("Bearer ") else ""
        counts[ROLES.get(presented, "unregistered") if presented else "none"] += 1
    return JSONResponse(counts)


async def recent(request: Request) -> Response:
    """The last forge requests by method, path and status-free query; no headers."""

    return JSONResponse(
        [f"{seen.method} {seen.url.raw_path.decode()}" for seen in fake.requests[-80:]]
    )


async def forge(request: Request) -> Response:
    # The raw path keeps a percent-encoded project path as GitLab routes it.
    raw = request.scope.get("raw_path") or request.url.path.encode()
    query = request.scope.get("query_string", b"").decode()
    url = ORIGIN + raw.decode() + (f"?{query}" if query else "")
    built = httpx.Request(
        request.method,
        url,
        headers=list(request.headers.items()),
        content=await request.body(),
    )
    answer = fake.handle(built)
    return Response(answer.content, answer.status_code, headers=dict(answer.headers))


app = Starlette(
    routes=[
        Route("/_control/health", health),
        Route("/_control/issues", create_issue, methods=["POST"]),
        Route("/_control/issues/{iid:int}/label", label_issue, methods=["POST"]),
        Route("/_control/received", received),
        Route("/_control/recent", recent),
        Route("/{path:path}", forge, methods=["GET", "POST", "PUT", "DELETE"]),
    ]
)

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8443,
        ssl_certfile="/tls/tls.crt",
        ssl_keyfile="/tls/tls.key",
        log_level="warning",
        date_header=False,
        server_header=False,
    )
'''

OPENSSL_CONFIG = f"""
[req]
distinguished_name = dn
prompt = no
[dn]
CN = {PREFIX} fixture CA
[ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
[server]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:{FORGE_HOST}
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
"""


class Pacer:
    """Poll start times until ``deadline``: each starts at most POLL_S after the
    previous one started, sleeping only the remainder of the interval. A poll
    that itself runs longer is followed at once; ``max_gap`` records the
    largest observed gap between two poll starts."""

    def __init__(self, deadline: float) -> None:
        self.deadline = deadline
        self.max_gap = 0.0

    def __iter__(self) -> Iterator[float]:
        previous: float | None = None
        while (started := time.monotonic()) < self.deadline:
            if previous is not None:
                self.max_gap = max(self.max_gap, started - previous)
            previous = started
            yield started
            time.sleep(max(0.0, min(started + POLL_S, self.deadline) - time.monotonic()))

    @property
    def note(self) -> str:
        return f"(largest poll-start gap {self.max_gap:.1f} s)"


class Sentinels(tuple[str, ...]):
    """Generated credential values; pytest's traceback shows only their count."""

    def __repr__(self) -> str:
        return f"<{len(self)} generated sentinels>"


class InfrastructureError(RuntimeError):
    """An unavailable prerequisite must be an error during fixture setup."""


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in list(env):
        if name.startswith(
            ("CURIE_", "ANTHROPIC_", "OPENROUTER_", "CLAUDE_", "GITHUB_", "GH_", "SLACK_")
        ) or name in {"HELM_DRIVER", "KUBECONFIG"}:
            env.pop(name)
    env["HELM_DRIVER"] = "secret"
    env["NO_COLOR"] = "1"
    return env


class SecretText(str):
    """Credential-bearing text (stdin documents): tracebacks show a placeholder."""

    def __repr__(self) -> str:
        return "<credential-bearing text>"


class SentinelRedactor:
    """A pytest plugin that replaces every registered sentinel value in each
    test report (its traceback and captured sections) with a placeholder, so no
    failure, error or captured output can print a generated credential."""

    PLACEHOLDER = "<generated-sentinel>"

    def __init__(self) -> None:
        self.values: set[str] = set()
        self.retired = False

    def add(self, value: str) -> None:
        """Register a value and the base64 form a Kubernetes Secret carries."""

        self.values.add(value)
        self.values.add(base64.b64encode(value.encode()).decode())

    def redact(self, text: str) -> str:
        for value in sorted(self.values, key=len, reverse=True):
            text = text.replace(value, self.PLACEHOLDER)
        return text

    def scrub(self, report: pytest.TestReport) -> None:
        if not self.values:
            return
        if report.longrepr is not None:
            text = str(report.longrepr)
            if any(value in text for value in self.values):
                report.longrepr = self.redact(text)
        report.sections = [(name, self.redact(content)) for name, content in report.sections]

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(
        self, item: pytest.Item, call: pytest.CallInfo[None]
    ) -> Iterator[None]:
        outcome = yield
        self.scrub(outcome.get_result())  # type: ignore[attr-defined]
        if self.retired and call.when == "teardown":
            # The module's last teardown report is scrubbed before unregistering.
            item.config.pluginmanager.unregister(self)


@pytest.fixture(scope="module")
def sentinel_redactor(request: pytest.FixtureRequest) -> Iterator[SentinelRedactor]:
    redactor = SentinelRedactor()
    request.config.pluginmanager.register(redactor, f"{PREFIX}-sentinel-redactor")
    yield redactor
    # Unregistered by the plugin itself once this teardown's report is scrubbed.
    redactor.retired = True


def _write_secret(path: Path, text: str) -> None:
    """Write a credential file owner-only; a failure names only the file."""

    try:
        path.write_text(text)
        path.chmod(0o600)
    except OSError as error:
        raise InfrastructureError(
            f"could not write fixture credential file {path.name}: {type(error).__name__}"
        ) from None


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
    quiet: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            input=SecretText(stdin) if stdin is not None else None,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        # Neither the stdin document nor captured output goes into the error.
        raise InfrastructureError(
            f"{argv[0]} {argv[1]} did not run: {type(error).__name__}"
        ) from None
    if check and result.returncode:
        # Values, Secret manifests and Secret reads can carry fixture
        # credentials even on error.
        secretive = quiet or ("get" in argv and any(x in argv for x in ("values", "secret")))
        detail = "" if secretive else _redact(result.stdout + result.stderr, secrets)[-6000:]
        raise InfrastructureError(f"{argv[0]} {argv[1]} failed ({result.returncode}): {detail}")
    return result


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
    for tool in ("docker", "kind", "kubectl", "helm", "cargo", "git", "openssl"):
        if shutil.which(tool) is None:
            raise InfrastructureError(f"#4630 runtime prerequisite missing: {tool}")
    _command(["docker", "info", "--format", "{{.ServerVersion}}"], env=env, cwd=ROOT)
    # Docker's configured engine is inherited. A verifier checkout can be in
    # /tmp, which is not shared with a remote Docker host.
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
                    ["docker", "image", "inspect", "--format", "{{json .}}", image],
                    env=env,
                    cwd=ROOT,
                    check=False,
                )
                if found.returncode == 0:
                    inspected = json.loads(found.stdout)
                    # Another tag may share a cached build; only an ID this
                    # tag alone named must disappear with it.
                    shared = [tag for tag in inspected.get("RepoTags") or [] if tag != image]
                    _command(["docker", "image", "rm", image], env=env, cwd=ROOT)
                    gone = _command(
                        ["docker", "image", "inspect", image], env=env, cwd=ROOT, check=False
                    )
                    if gone.returncode == 0:
                        raise InfrastructureError("owned candidate API image tag survived removal")
                    if not shared:
                        remaining = _command(
                            ["docker", "image", "ls", "--quiet", "--no-trunc"], env=env, cwd=ROOT
                        ).stdout.split()
                        if inspected["Id"] in remaining:
                            raise InfrastructureError("owned candidate API image survived removal")
        finally:
            shutil.rmtree(directory)
            if directory.exists():
                raise InfrastructureError("owned candidate directory survived removal")

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
def candidate_curie(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Path | None:
    """Override this directory's autouse shell double for this runtime module.

    Only a test that installs the candidate builds it; the redaction self-check
    needs no toolchain.
    """
    if "rotation" not in request.fixturenames:
        return None
    candidate: Candidate = request.getfixturevalue("source_candidate")
    monkeypatch.setenv("CURIE_BIN", str(candidate.binary))
    return candidate.binary


@dataclass(frozen=True)
class CredentialRef:
    """One factory credential as the release records and the API mounts it."""

    section: str
    name: str
    secret: str
    key: str
    path: str


@dataclass(frozen=True)
class ApiPod:
    name: str
    uid: str
    restarts: int


class Rotation:
    def __init__(self, candidate: Candidate, directory: Path) -> None:
        self.candidate = candidate
        self.directory = directory
        # One name per test invocation, with no reuse of another run's resources.
        self.cluster = f"{PREFIX}-{uuid.uuid4().hex[:8]}"
        self.context = f"kind-{self.cluster}"
        self.env = dict(candidate.env)
        self.env.update(
            {
                "KUBECONFIG": str(directory / "kubeconfig"),
                "CURIE_BIN": str(candidate.binary),
                "CURIE_API_KEY": "fixture-api-" + uuid.uuid4().hex,
                "CURIE_CONFIG_DIR": str(directory / "curie-config"),
                "XDG_CONFIG_HOME": str(directory / "config"),
            }
        )
        self.tokens = {
            "initial": "fixture-gitlab-initial-" + uuid.uuid4().hex,
            "replacement": "fixture-gitlab-replacement-" + uuid.uuid4().hex,
            "rejected": "fixture-gitlab-rejected-" + uuid.uuid4().hex,
        }
        self.webhook_secret = "fixture-webhook-" + uuid.uuid4().hex
        self.model_credentials = "sk-ant-fixture-" + uuid.uuid4().hex
        self.secrets = Sentinels(
            (
                self.env["CURIE_API_KEY"],
                *self.tokens.values(),
                self.webhook_secret,
                self.model_credentials,
            )
        )
        # Every captured command output, for the sentinel sweep (AC 3).
        self.captured: list[str] = []
        self.cluster_started = False
        self.sentinel_name = f"{PREFIX}-sentinel-" + uuid.uuid4().hex
        self.sentinel_label = f"curie.test.sentinel={self.sentinel_name}"
        self.sentinel_id: str | None = None
        self.forge_port = 0
        self.http: httpx.Client | None = None
        self.control: httpx.Client | None = None
        # The report redactor; every encoded credential document is registered.
        self.redactor: SentinelRedactor | None = None

    # Commands ---------------------------------------------------------------

    def run(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        result = _command(
            list(argv),
            env=self.env,
            cwd=self.candidate.source,
            secrets=self.secrets,
            **kwargs,
        )
        self.captured.append(result.stdout + result.stderr)
        return result

    def kubectl(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run("kubectl", "--context", self.context, *argv, **kwargs)

    def helm(self, *argv: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.run(
            "helm", "--kube-context", self.context, "--namespace", NAMESPACE, *argv, **kwargs
        )

    def curie(self, *argv: str, timeout: int = 300) -> subprocess.CompletedProcess[str]:
        try:
            return self.run(str(self.candidate.binary), *argv, check=False, timeout=timeout)
        except InfrastructureError:
            raise InfrastructureError(
                f"curie {' '.join(argv[:3])} did not finish within {timeout} seconds"
            ) from None

    def apply(self, manifest: dict[str, Any]) -> None:
        self.kubectl("apply", "-f", "-", stdin=json.dumps(manifest))

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

    def write_token(self, role: str) -> None:
        """The role's token file; only the role name is ever an argument."""

        _write_secret(self.file(f"token-{role}"), self.tokens[role])

    def file(self, name: str) -> Path:
        return self.directory / name

    @property
    def forge_url(self) -> str:
        return f"https://{FORGE_HOST}:{self.forge_port}"

    # Setup ------------------------------------------------------------------

    def setup(self) -> None:
        resolved = {info[4][0] for info in socket.getaddrinfo(FORGE_HOST, None)}
        if not resolved & {"127.0.0.1", "::1"}:
            raise InfrastructureError(f"{FORGE_HOST} does not resolve to loopback on this host")
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
        for role in self.tokens:
            self.write_token(role)
        self.certificates()
        self.create_cluster()
        self.start_forge()
        self.install_release()

    def certificates(self) -> None:
        config = self.file("openssl.cnf")
        config.write_text(OPENSSL_CONFIG)
        self.run(
            "openssl",
            "req",
            "-x509",
            "-config",
            str(config),
            "-extensions",
            "ca",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-keyout",
            str(self.file("ca.key")),
            "-out",
            str(self.file("ca.pem")),
        )
        self.run(
            "openssl",
            "req",
            "-new",
            "-config",
            str(config),
            "-subj",
            f"/CN={FORGE_HOST}",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(self.file("server.key")),
            "-out",
            str(self.file("server.csr")),
        )
        self.run(
            "openssl",
            "x509",
            "-req",
            "-in",
            str(self.file("server.csr")),
            "-CA",
            str(self.file("ca.pem")),
            "-CAkey",
            str(self.file("ca.key")),
            "-CAcreateserial",
            "-days",
            "2",
            "-extfile",
            str(config),
            "-extensions",
            "server",
            "-out",
            str(self.file("server.pem")),
        )
        for key in ("ca.key", "server.key"):
            self.file(key).chmod(0o600)

    def create_cluster(self) -> None:
        mappings = [
            {"containerPort": port, "hostPort": 0, "listenAddress": "127.0.0.1"}
            for port in (API_NODE_PORT, FORGE_NODE_PORT)
        ]
        config_path = self.file("kind.json")
        config_path.write_text(
            json.dumps(
                {
                    "kind": "Cluster",
                    "apiVersion": "kind.x-k8s.io/v1alpha4",
                    "nodes": [{"role": "control-plane", "extraPortMappings": mappings}],
                }
            )
        )
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
            raise InfrastructureError("could not allocate an unused #4630 kind cluster name")
        self.run("kind", "load", "docker-image", self.candidate.image, "--name", self.cluster)
        self.env["CURIE_API_URL"] = f"http://127.0.0.1:{self.host_port(API_NODE_PORT)}"
        self.forge_port = self.host_port(FORGE_NODE_PORT)

    def host_port(self, node_port: int) -> int:
        mapping = self.run(
            "docker", "port", f"{self.cluster}-control-plane", f"{node_port}/tcp"
        ).stdout.split()
        if not mapping:
            raise InfrastructureError(f"kind published no host port for node port {node_port}")
        return int(mapping[0].rsplit(":", 1)[1])

    def start_forge(self) -> None:
        """The fake GitLab in its own namespace, its name served by CoreDNS."""

        self.kubectl("create", "namespace", FORGE_NAMESPACE)
        init = self.file("forge-init.py")
        init.write_text("")
        server = self.file("forge-server.py")
        server.write_text(FORGE_SERVER)
        fake_module = self.candidate.source / "apps/api/tests/forge_fakes/gitlab.py"
        self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "create",
            "configmap",
            "fake-gitlab-code",
            f"--from-file=serve.py={server}",
            f"--from-file=init.py={init}",
            f"--from-file=gitlab.py={fake_module}",
        )
        self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "create",
            "secret",
            "tls",
            "fake-gitlab-tls",
            f"--cert={self.file('server.pem')}",
            f"--key={self.file('server.key')}",
            quiet=True,
        )
        self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "create",
            "secret",
            "generic",
            "fake-gitlab-tokens",
            f"--from-file=initial={self.file('token-initial')}",
            f"--from-file=replacement={self.file('token-replacement')}",
            quiet=True,
        )
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "fake-gitlab", "namespace": FORGE_NAMESPACE},
                "spec": {
                    "type": "NodePort",
                    "selector": {"app": "fake-gitlab"},
                    "ports": [
                        {
                            "name": "https",
                            "protocol": "TCP",
                            "port": self.forge_port,
                            "targetPort": FORGE_CONTAINER_PORT,
                            "nodePort": FORGE_NODE_PORT,
                        }
                    ],
                },
            }
        )
        mounts = [
            {"name": "code", "mountPath": "/forge", "readOnly": True},
            {"name": "tls", "mountPath": "/tls", "readOnly": True},
            {"name": "tokens", "mountPath": "/tokens", "readOnly": True},
        ]
        code_items = [
            {"key": "serve.py", "path": "serve.py"},
            {"key": "init.py", "path": "forge_fakes/__init__.py"},
            {"key": "gitlab.py", "path": "forge_fakes/gitlab.py"},
        ]
        container = {
            "name": "forge",
            "image": self.candidate.image,
            "imagePullPolicy": "Never",
            "command": ["python", "/forge/serve.py"],
            "env": [
                {"name": "PYTHONPATH", "value": "/forge"},
                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                {"name": "FAKE_GITLAB_API", "value": f"{self.forge_url}/api/v4"},
                {"name": "FAKE_TOKEN_EXPIRES_AT", "value": TOKEN_EXPIRES_AT},
            ],
            "ports": [{"containerPort": FORGE_CONTAINER_PORT}],
            "readinessProbe": {
                "tcpSocket": {"port": FORGE_CONTAINER_PORT},
                "periodSeconds": 2,
            },
            "volumeMounts": mounts,
        }
        self.apply(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "fake-gitlab", "namespace": FORGE_NAMESPACE},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "fake-gitlab"}},
                    "template": {
                        "metadata": {"labels": {"app": "fake-gitlab"}},
                        "spec": {
                            "automountServiceAccountToken": False,
                            "containers": [container],
                            "volumes": [
                                {
                                    "name": "code",
                                    "configMap": {"name": "fake-gitlab-code", "items": code_items},
                                },
                                {"name": "tls", "secret": {"secretName": "fake-gitlab-tls"}},
                                {"name": "tokens", "secret": {"secretName": "fake-gitlab-tokens"}},
                            ],
                        },
                    },
                },
            }
        )
        self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "rollout",
            "status",
            "deployment/fake-gitlab",
            "--timeout=180s",
            timeout=210,
        )
        # In-cluster name: CoreDNS answers it with the Service address.
        address = self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "get",
            "service",
            "fake-gitlab",
            "-o",
            "jsonpath={.spec.clusterIP}",
        ).stdout.strip()
        coredns = json.loads(
            self.kubectl("-n", "kube-system", "get", "configmap", "coredns", "-o", "json").stdout
        )
        corefile = coredns["data"]["Corefile"]
        opening = ".:53 {\n"
        if opening not in corefile or "hosts" in corefile:
            raise InfrastructureError("the kind CoreDNS Corefile has an unexpected shape")
        hosts = f"    hosts {{\n        {address} {FORGE_HOST}\n        fallthrough\n    }}\n"
        patch = {"data": {"Corefile": corefile.replace(opening, opening + hosts, 1)}}
        self.kubectl(
            "-n",
            "kube-system",
            "patch",
            "configmap",
            "coredns",
            "--type",
            "merge",
            "-p",
            json.dumps(patch),
        )
        self.kubectl("-n", "kube-system", "rollout", "restart", "deployment/coredns")
        self.kubectl(
            "-n",
            "kube-system",
            "rollout",
            "status",
            "deployment/coredns",
            "--timeout=120s",
            timeout=150,
        )
        trust = ssl.create_default_context(cafile=str(self.file("ca.pem")))
        self.control = httpx.Client(
            base_url=self.forge_url, verify=trust, timeout=30, trust_env=False
        )
        deadline = time.monotonic() + 120
        last = "no attempt"
        while True:
            try:
                answer = self.control.get("/_control/health")
                if answer.status_code == 200:
                    break
                last = f"HTTP {answer.status_code}"
            except httpx.TransportError as error:
                last = type(error).__name__
            if time.monotonic() >= deadline:
                raise InfrastructureError(
                    "the fake GitLab did not answer over TLS through kind host port "
                    f"{self.forge_port} (last attempt: {last})"
                )
            time.sleep(1)

    def install_release(self) -> None:
        self.kubectl("create", "namespace", NAMESPACE)
        repository, tag = self.candidate.image.rsplit(":", 1)
        values = {
            "fullnameOverride": FULLNAME,
            "api": {
                "image": {
                    "repository": repository,
                    "tag": tag,
                    "digest": "",
                    "pullPolicy": "Never",
                },
                "apiKey": self.env["CURIE_API_KEY"],
                "githubWebhookSecret": self.webhook_secret,
                "commitPollIntervalSeconds": 0,
                "service": {"type": "NodePort", "port": 8000, "nodePort": API_NODE_PORT},
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
                "controller": {"deploy": False},
                "runner": {
                    "fakeModel": False,
                    "model": "fixture-no-execution",
                    "credentials": self.model_credentials,
                    "prewarm": {"enabled": False},
                },
            },
            "priorityClasses": {
                "platform": {"create": True, "name": f"{PREFIX}-platform"},
                "sandbox": {"create": True, "name": f"{PREFIX}-sandbox"},
            },
            "security": {"allowDevDefaults": True, "gvisor": {"mode": "off"}},
        }
        # Post-install migration and bucket hooks must run before API readiness.
        self.helm(
            "install",
            RELEASE,
            str(self.candidate.source / "charts/curie"),
            "--skip-crds",
            "-f",
            "-",
            "--timeout",
            "600s",
            stdin=json.dumps(values),
            timeout=660,
        )
        self.rollout_api()
        self.http = httpx.Client(
            base_url=self.env["CURIE_API_URL"],
            headers={"X-API-Key": self.env["CURIE_API_KEY"]},
            timeout=30,
            trust_env=False,
        )
        deadline = time.monotonic() + 90
        while True:
            try:
                if self.http.get("/ready").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() >= deadline:
                raise InfrastructureError("candidate API did not become ready through kind")
            time.sleep(0.5)
        response = self.http.get("/agents")
        if response.status_code != 200 or response.json() != []:
            raise InfrastructureError("candidate API did not start with empty isolated storage")
        pong = self.kubectl(
            "-n",
            NAMESPACE,
            "exec",
            f"statefulset/{FULLNAME}-valkey",
            "--",
            "valkey-cli",
            "-a",
            "valkeypass",
            "PING",
        ).stdout.strip()
        if pong != "PONG":
            raise InfrastructureError("the real Valkey backing service did not answer PING")
        # The API pod resolves the forge name to the in-cluster fake.
        address = self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "get",
            "service",
            "fake-gitlab",
            "-o",
            "jsonpath={.spec.clusterIP}",
        ).stdout.strip()
        seen = self.kubectl(
            "-n",
            NAMESPACE,
            "exec",
            f"deployment/{FULLNAME}-api",
            "--",
            "python",
            "-c",
            f"import socket; print(socket.gethostbyname({FORGE_HOST!r}))",
        ).stdout.strip()
        if seen != address:
            raise InfrastructureError("the API pod does not resolve the fake GitLab in-cluster")
        bundle = self.file("bundle")
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / ".claude-plugin/plugin.json").write_text(
            json.dumps(
                {
                    "name": "acme-rotation",
                    "version": "0.1.0",
                    "description": "Static token rotation fixture",
                }
            )
        )
        (bundle / "AGENTS.md").write_text("Persist this bundle without starting a worker turn.\n")

    def rollout_api(self) -> None:
        settled = self.kubectl(
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            f"deployment/{FULLNAME}-api",
            "--timeout=420s",
            timeout=450,
            check=False,
        )
        if settled.returncode:
            raise InfrastructureError("the API rollout did not settle: " + self.pod_states())

    def pod_states(self) -> str:
        """Each release pod's phase and container states: names and reasons only."""

        pods = json.loads(
            self.kubectl("-n", NAMESPACE, "get", "pods", "-o", "json", check=False).stdout
            or '{"items": []}'
        )["items"]
        rows = []
        for pod in pods:
            states = []
            for status in pod["status"].get("containerStatuses", []) + pod["status"].get(
                "initContainerStatuses", []
            ):
                for kind, detail in (status.get("state") or {}).items():
                    states.append(
                        f"{status['name']}={kind}:{detail.get('reason', '')}"
                        f"/restarts={status.get('restartCount', 0)}"
                    )
            rows.append(f"{pod['metadata']['name']} {pod['status'].get('phase')} {states}")
            if any("CrashLoopBackOff" in state or "Error" in state for state in states):
                crashed = self.kubectl(
                    "-n",
                    NAMESPACE,
                    "logs",
                    pod["metadata"]["name"],
                    "--all-containers=true",
                    "--previous",
                    "--tail=40",
                    check=False,
                )
                rows.append("last log lines: " + (crashed.stdout + crashed.stderr)[-4000:])
        return _redact("; ".join(rows), self.secrets)

    # Cleanup ----------------------------------------------------------------

    def close(self) -> None:
        def verify_cluster() -> None:
            if self.owned_containers():
                raise InfrastructureError("owned kind containers survived deletion")
            if self.cluster in self.run("kind", "get", "clusters").stdout.split():
                raise InfrastructureError("owned kind cluster survived deletion")

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

        def verify_sentinel() -> None:
            if self.sentinel_id is not None:
                running = self.run(
                    "docker", "inspect", "--format", "{{.State.Running}}", self.sentinel_id
                ).stdout.strip()
                if running != "true":
                    raise InfrastructureError("unrelated sentinel did not survive kind cleanup")

        def delete_sentinel() -> None:
            owned = sentinel_containers()
            if owned:
                self.run("docker", "rm", "--force", *owned)

        def verify_sentinel_removed() -> None:
            if sentinel_containers():
                raise InfrastructureError("owned sentinel survived removal")

        operations: list[Callable[[], Any]] = []
        for client in (self.http, self.control):
            if client is not None:
                operations.append(client.close)
        if self.cluster_started:
            # Deleting the owned cluster removes both namespaces, the
            # PriorityClasses and the CoreDNS patch with it.
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

    # Product reads ----------------------------------------------------------

    def quickstart(self) -> subprocess.CompletedProcess[str]:
        return self.curie(
            "--json",
            "factory",
            "quickstart",
            "--context",
            self.context,
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
            "--repo",
            PROJECT_PATH,
            "--agent",
            AGENT,
            "--plugin-dir",
            str(self.file("bundle")),
            "--chart",
            str(self.candidate.source / "charts/curie"),
            "--code-host",
            "gitlab",
            "--tracker",
            "gitlab",
            "--token-file",
            str(self.file("token-initial")),
            "--forge-url",
            self.forge_url,
            "--ca-file",
            str(self.file("ca.pem")),
            timeout=900,
        )

    def release_values(self) -> dict[str, Any]:
        values = json.loads(self.helm("get", "values", RELEASE, "-o", "json").stdout)
        if not isinstance(values, dict):
            raise InfrastructureError("Helm did not return a release values object")
        return values

    def shorten_poll_interval(self) -> None:
        """The tracker poll at the shortest interval its config accepts."""

        trackers = self.release_values()["factory"]["trackers"]
        for tracker in trackers:
            if tracker["kind"] == "gitlab":
                tracker["pollIntervalSeconds"] = POLL_INTERVAL_SECONDS
        self.helm(
            "upgrade",
            RELEASE,
            str(self.candidate.source / "charts/curie"),
            "--skip-crds",
            "--reuse-values",
            "-f",
            "-",
            "--timeout",
            "600s",
            stdin=json.dumps({"factory": {"trackers": trackers}}),
            timeout=660,
        )
        self.rollout_api()

    def credential_refs(self) -> list[CredentialRef]:
        """Each GitLab credential: its Secret key from the release, its mount
        path from the API's CURIE_FORGES document."""

        values = self.release_values()["factory"]
        deployment = json.loads(
            self.kubectl(
                "-n", NAMESPACE, "get", "deployment", f"{FULLNAME}-api", "-o", "json"
            ).stdout
        )
        container = next(
            c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "api"
        )
        document = json.loads(
            next(e["value"] for e in container["env"] if e["name"] == "CURIE_FORGES")
        )
        refs = []
        for section, key, mounted in (
            ("code_host", "codeHosts", "code_hosts"),
            ("tracker", "trackers", "trackers"),
        ):
            for entry in values[key]:
                if entry["kind"] != "gitlab":
                    continue
                credential = entry["credential"]
                path = next(
                    item["credential"]["token_file"]
                    for item in document[mounted]
                    if item["name"] == entry["name"]
                )
                refs.append(
                    CredentialRef(
                        section,
                        entry["name"],
                        credential["existingSecret"],
                        credential["existingSecretKey"],
                        path,
                    )
                )
        if {ref.section for ref in refs} != {"code_host", "tracker"}:
            raise InfrastructureError("the quickstart recorded no GitLab code host and tracker")
        return refs

    def api_pod(self) -> ApiPod:
        pods = json.loads(
            self.kubectl(
                "-n",
                NAMESPACE,
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=api",
                "-o",
                "json",
            ).stdout
        )["items"]
        live = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]
        assert len(live) == 1, f"expected one API pod, found {len(live)}"
        pod = live[0]
        statuses = pod["status"].get("containerStatuses", [])
        return ApiPod(
            pod["metadata"]["name"],
            pod["metadata"]["uid"],
            sum(status["restartCount"] for status in statuses),
        )

    def api_generation(self) -> tuple[int, str]:
        deployment = json.loads(
            self.kubectl(
                "-n", NAMESPACE, "get", "deployment", f"{FULLNAME}-api", "-o", "json"
            ).stdout
        )
        annotations = deployment["spec"]["template"]["metadata"].get("annotations") or {}
        return (
            int(deployment["metadata"]["generation"]),
            str(annotations.get("kubectl.kubernetes.io/restartedAt", "")),
        )

    def sql(self, statement: str) -> str:
        return self.kubectl(
            "-n",
            NAMESPACE,
            "exec",
            "-i",
            f"statefulset/{FULLNAME}-postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-qAt",
            stdin=statement,
        ).stdout.strip()

    def admitted(self, iid: int) -> bool:
        count = self.sql(
            "SELECT count(*) FROM curie.work_items WHERE tracker_kind = 'gitlab' "
            f"AND tracker_scope_id = '{PROJECT_ID}' AND tracker_issue_id = '{int(iid)}';"
        )
        return int(count) > 0

    def wait_admitted(self, iid: int, timeout: float) -> None:
        pacer = Pacer(time.monotonic() + timeout)
        for _started in pacer:
            if self.admitted(iid):
                return
        raise AssertionError(
            f"issue {iid} was not admitted within {timeout:.0f} seconds {pacer.note}; "
            f"forge requests by token role {self.received()}; "
            f"API diagnostics:\n{self.diagnostics()}"
        )

    def diagnostics(self) -> str:
        """The API's own INFO-or-higher factory and forge records, redacted."""

        logs = self.kubectl(
            "-n", NAMESPACE, "logs", f"deployment/{FULLNAME}-api", "-c", "api", check=False
        ).stdout
        lines = []
        for line in logs.splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            logger = str(record.get("logger", ""))
            if any(part in logger for part in ("factory", "forges", "workitem", "reconciler")):
                exception = str(record.get("exception", ""))[-1500:]
                lines.append(
                    f"{record.get('severity')} {logger}: {record.get('message')} {exception}"
                )
        if not lines:
            lines = logs.splitlines()[-40:]
        recent = self.forge("GET", "/_control/recent")
        lines.append("forge requests, newest last: " + " | ".join(recent[-40:]))
        return _redact("\n".join(lines[-70:]), self.secrets)

    def projected_digests(self, pod: ApiPod, refs: list[CredentialRef]) -> set[str]:
        """sha256 of each mounted credential file; its bytes are never read here."""

        listed = self.kubectl(
            "-n",
            NAMESPACE,
            "exec",
            pod.name,
            "-c",
            "api",
            "--",
            "sha256sum",
            *sorted({ref.path for ref in refs}),
        ).stdout
        return {line.split()[0] for line in listed.splitlines() if line.strip()}

    def replace_credential(self, refs: list[CredentialRef], role: str) -> None:
        """Replace each referenced Secret key with ``role``'s token, from a file."""

        source = self.file(f"token-{role}")
        keys: dict[str, set[str]] = {}
        for ref in refs:
            keys.setdefault(ref.secret, set()).add(ref.key)
        for secret, names in keys.items():
            self.kubectl(
                "apply",
                "-f",
                "-",
                stdin=self.secret_manifest(secret, sorted(names), source),
                quiet=True,
            )

    def secret_manifest(self, secret: str, keys: list[str], source: Path) -> SecretText:
        """The Secret document holding ``source`` under ``keys``, rendered by
        kubectl. It carries the encoded credential, so it exists only as
        `SecretText`, is registered with the report redactor, and a failure
        raises a sanitized error."""

        argv = [
            "kubectl",
            "--context",
            self.context,
            "-n",
            NAMESPACE,
            "create",
            "secret",
            "generic",
            secret,
            *(f"--from-file={key}={source}" for key in keys),
            "--dry-run=client",
            "-o",
            "yaml",
        ]
        try:
            rendered = subprocess.run(
                argv,
                cwd=self.candidate.source,
                env=self.env,
                text=True,
                capture_output=True,
                timeout=60,
                check=True,
            )
            document = SecretText(rendered.stdout)
        except (OSError, subprocess.SubprocessError):
            raise InfrastructureError(f"kubectl could not render Secret {secret}") from None
        if self.redactor is not None:
            self.redactor.values.add(str(document))
        return document

    def wait_projection(self, pod: ApiPod, refs: list[CredentialRef], role: str) -> float:
        """The monotonic time every mounted credential file holds ``role``'s token."""

        expected = hashlib.sha256(self.tokens[role].encode()).hexdigest()
        deadline = time.monotonic() + PROJECTION_TIMEOUT_S
        while True:
            if self.projected_digests(pod, refs) == {expected}:
                return time.monotonic()
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"the kubelet did not project the {role} credential within "
                    f"{PROJECTION_TIMEOUT_S:.0f} seconds"
                )
            time.sleep(1)

    def status(
        self, *, json_output: bool, timeout: int = STATUS_TIMEOUT_S
    ) -> subprocess.CompletedProcess[str]:
        """`curie cluster status`. The verb has no --context flag, so the call
        is pinned to the owned context: the private kubeconfig must hold that
        one context, current, before every call."""

        contexts = self.kubectl("config", "get-contexts", "-o", "name").stdout.split()
        current = self.kubectl("config", "current-context").stdout.strip()
        if contexts != [self.context] or current != self.context:
            raise InfrastructureError("the private kubeconfig is not pinned to the owned context")
        argv = ["--json"] if json_output else []
        return self.curie(
            *argv,
            "cluster",
            "status",
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
            timeout=timeout,
        )

    def status_document(self) -> dict[str, Any]:
        result = self.status(json_output=True)
        try:
            document = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise AssertionError(
                "cluster status --json printed no JSON object: "
                + _redact(result.stderr, self.secrets)[-2000:]
            ) from None
        assert isinstance(document, dict)
        return document

    def authentication(self, document: dict[str, Any]) -> dict[tuple[str, str], Any]:
        return {
            (line.get("section"), line.get("name")): line.get("authentication")
            for line in document.get("credentials", [])
        }

    def credential_lines(self, document: dict[str, Any]) -> dict[tuple[str, str], Any]:
        return {
            (line.get("section"), line.get("name")): line
            for line in document.get("credentials", [])
        }

    def digest(self, role: str) -> str:
        return hashlib.sha256(self.tokens[role].encode()).hexdigest()

    def forge_reachable(self, reachable: bool) -> None:
        """Point the fake's Service at no pod, or back at it. The pod and its
        state stay; every connection to the forge is refused meanwhile."""

        selector = {"app": "fake-gitlab" if reachable else "fake-gitlab-unreachable"}
        self.kubectl(
            "-n",
            FORGE_NAMESPACE,
            "patch",
            "service",
            "fake-gitlab",
            "--type",
            "merge",
            "-p",
            json.dumps({"spec": {"selector": selector}}),
        )
        if not reachable:
            return
        deadline = time.monotonic() + 90
        while True:
            try:
                if self.control is not None and (
                    self.control.get("/_control/health").status_code == 200
                ):
                    return
            except httpx.TransportError:
                pass
            if time.monotonic() >= deadline:
                raise InfrastructureError("the fake GitLab did not come back after the outage")
            time.sleep(1)

    def api_logs(self, pod: ApiPod) -> str:
        return self.kubectl("-n", NAMESPACE, "logs", pod.name, "-c", "api").stdout

    # The fake's control routes ------------------------------------------------

    def forge(self, method: str, path: str, body: Any = None) -> Any:
        if self.control is None:
            raise InfrastructureError("the fake GitLab control client is not initialized")
        response = self.control.request(method, path, json=body)
        response.raise_for_status()
        return response.json()

    def labelled_issue(self, title: str, label: str) -> int:
        iid = int(self.forge("POST", "/_control/issues", {"title": title})["iid"])
        self.forge("POST", f"/_control/issues/{iid}/label", {"label": label})
        return iid

    def received(self) -> dict[str, int]:
        counts = self.forge("GET", "/_control/received")
        assert isinstance(counts, dict)
        return {str(key): int(value) for key, value in counts.items()}


@pytest.fixture
def rotation(
    request: pytest.FixtureRequest,
    sentinel_redactor: SentinelRedactor,
    source_candidate: Candidate,
) -> Rotation:
    directory = Path(tempfile.mkdtemp(prefix="rotation-", dir=source_candidate.directory))
    instance = Rotation(source_candidate, directory)
    instance.redactor = sentinel_redactor
    for value in instance.secrets:
        sentinel_redactor.add(value)
    request.addfinalizer(instance.close)
    instance.setup()
    return instance


def _authentication_warnings(logs: str) -> list[dict[str, Any]]:
    """The API's structured WARNING-or-higher intake records naming authentication."""

    found = []
    for line in logs.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        message = str(record.get("message", ""))
        if (
            record.get("severity") in {"WARNING", "ERROR", "CRITICAL"}
            and "tracker intake" in message
            and "authentication" in message
        ):
            found.append(record)
    return found


def test_secret_replacement_reports_rejection_and_recovers(rotation: Rotation) -> None:
    started = rotation.quickstart()
    assert started.returncode == 0, _redact(started.stdout + started.stderr, rotation.secrets)
    label = json.loads(started.stdout)["effective_label"]
    rotation.shorten_poll_interval()
    refs = rotation.credential_refs()
    entries = {(ref.section, ref.name) for ref in refs}

    # Intake works with the initial token.
    first = rotation.labelled_issue("Initial intake", label)
    rotation.wait_admitted(first, timeout=180)
    before = rotation.received()
    assert before["initial"] > 0
    assert before["replacement"] == 0
    pod = rotation.api_pod()
    generation = rotation.api_generation()
    if rotation.projected_digests(pod, refs) != {rotation.digest("initial")}:
        pytest.fail("the API pod does not mount the initial credential")

    # Replace the Secret with a nonempty token the forge rejects. Within 60
    # seconds of the projection change, status must report the rejection while
    # a freshly labelled issue is not admitted at any poll.
    rotation.replace_credential(refs, "rejected")
    projected = rotation.wait_projection(pod, refs, "rejected")
    second = rotation.labelled_issue("Labelled after the rejected token", label)
    # Status and admission are both polled for the whole window; no reading
    # after the projection change may be accepted.
    deadline = projected + WINDOW_S
    rejected: dict[str, Any] | None = None
    pacer = Pacer(deadline)
    for started in pacer:
        assert not rotation.admitted(second), (
            f"issue {second} was admitted {started - projected:.0f} seconds after the rejected "
            f"credential was projected {pacer.note}: the API still authenticates with its "
            "startup token"
        )
        document = rotation.status_document()
        states = rotation.authentication(document)
        assert "accepted" not in states.values(), (
            f"status reported a projected rejected credential as accepted {pacer.note}: {states}"
        )
        if (
            rejected is None
            and all(states.get(entry) == "rejected" for entry in entries)
            and time.monotonic() <= deadline
        ):
            rejected = document
    assert not rotation.admitted(second)
    assert rejected is not None, (
        f"cluster status did not report authentication rejected for {sorted(entries)} within "
        f"{WINDOW_S:.0f} seconds of the projection change {pacer.note}"
    )
    assert rejected["healthy"] is False
    lines = rotation.credential_lines(rejected)
    for entry in entries:
        # Rejection is reported apart from the configured future expiry.
        assert lines[entry]["expiry"] == "known", lines[entry]
        assert str(lines[entry]["expires_at"]).startswith(TOKEN_EXPIRES_AT), lines[entry]
        assert lines[entry]["warning"] is None, lines[entry]
    unhealthy = rejected["pods"]["unhealthy"]
    for section, name in entries:
        assert any(
            f"credential {section} {name}" in item and "authentication rejected" in item
            for item in unhealthy
        ), f"pods.unhealthy does not name {section} {name}: {unhealthy}"
    human = rotation.status(json_output=False)
    human_lines = (human.stdout + human.stderr).splitlines()
    assert human.returncode != 0, "cluster status exited zero with a rejected credential"
    for section, name in entries:
        assert any(
            f"credential {section} {name}" in line and "authentication rejected" in line
            for line in human_lines
        ), f"human cluster status does not name {section} {name} as authentication rejected"
    assert _authentication_warnings(rotation.api_logs(pod)), (
        "the API logged no WARNING intake deferral naming authentication"
    )

    # The forge unreachable: an unanswerable read is unknown, never accepted.
    rotation.forge_reachable(False)
    outage = time.monotonic()
    deadline = outage + WINDOW_S
    unknown_at: float | None = None
    pacer = Pacer(deadline)
    for _started in pacer:
        states = rotation.authentication(rotation.status_document())
        observed = time.monotonic()
        assert "accepted" not in states.values(), (
            f"an unreachable forge read as {states} {pacer.note}"
        )
        if (
            unknown_at is None
            and all(states.get(entry) == "unknown" for entry in entries)
            and observed <= deadline
        ):
            unknown_at = observed
        assert not rotation.admitted(second), (
            f"an issue was admitted while the forge was down {pacer.note}"
        )
    assert unknown_at is not None, (
        f"status did not report unknown for an unreachable forge within {WINDOW_S:.0f} seconds "
        f"{pacer.note}"
    )
    assert not rotation.admitted(second)
    rotation.forge_reachable(True)

    # Restore a valid credential: the replacement, so the new file is what is read.
    rotation.replace_credential(refs, "replacement")
    restored = rotation.wait_projection(pod, refs, "replacement")
    deadline = restored + WINDOW_S
    recovered: float | None = None
    document: dict[str, Any] = {}
    states: dict[tuple[str, str], Any] = {}
    admitted = False
    pacer = Pacer(deadline)
    for _started in pacer:
        document = rotation.status_document()
        states = rotation.authentication(document)
        admitted = rotation.admitted(second)
        if admitted and all(states.get(entry) == "accepted" for entry in entries):
            recovered = time.monotonic()
            break
    assert recovered is not None and recovered <= deadline, (
        f"within {WINDOW_S:.0f} seconds of the valid credential's projection {pacer.note}, "
        f"issue {second} admitted={admitted} and authentication {states}"
    )
    unhealthy = document["pods"]["unhealthy"]
    assert not [item for item in unhealthy if "authentication" in item], unhealthy
    if not unhealthy:
        assert document["healthy"] is True
    human = rotation.status(json_output=False)
    human_lines = (human.stdout + human.stderr).splitlines()
    assert not [line for line in human_lines if "authentication rejected" in line]
    for section, name in entries:
        assert any(
            f"credential {section} {name}" in line and "authentication accepted" in line
            for line in human_lines
        ), f"human cluster status does not name {section} {name} as authentication accepted"
    after = rotation.received()
    assert after["replacement"] > 0, "the forge never received the replacement token"
    assert after["unregistered"] > before["unregistered"], "the rejected token never reached it"

    # No restart and no manual reload across every phase.
    assert rotation.api_pod() == pod
    assert rotation.api_generation() == generation
    # No credential in the API log or anything captured, status included. The
    # fake quoted each presented token in its error bodies, so a path that
    # surfaced response text would carry one. pytest.fail keeps the value out of
    # the report.
    logs = rotation.api_logs(pod)
    sources = [("API pod log", logs), *(("command output", c) for c in rotation.captured)]
    for role, token in rotation.tokens.items():
        for where, text in sources:
            if token in text:
                pytest.fail(f"the {role} token appeared in {where}", pytrace=False)


def test_reports_never_carry_a_registered_sentinel(
    request: pytest.FixtureRequest, sentinel_redactor: SentinelRedactor
) -> None:
    """The redactor is registered for this module and scrubs a real failure
    report: traceback, frame arguments and captured sections."""

    sentinel = "fixture-gitlab-selfcheck-" + uuid.uuid4().hex
    encoded = base64.b64encode(sentinel.encode()).decode()
    plugin = SentinelRedactor()
    plugin.add(sentinel)

    def leaks(token: str) -> None:
        manifest = f"data:\n  token: {base64.b64encode(token.encode()).decode()}\n"
        raise RuntimeError(f"forge answered 401 for {token}; applied {manifest}")

    try:
        leaks(sentinel)
    except RuntimeError:
        info = pytest.ExceptionInfo.from_current()
    report = pytest.TestReport(
        nodeid="selfcheck",
        location=("selfcheck.py", 0, "selfcheck"),
        keywords={},
        outcome="failed",
        longrepr=info.getrepr(style="long", funcargs=True),
        when="call",
        sections=[("Captured stdout call", f"token {sentinel}\nsecret data {encoded}\n")],
    )
    assert sentinel in str(report.longrepr)
    assert encoded in str(report.longrepr)

    class Outcome:
        def get_result(self) -> pytest.TestReport:
            return report

    class Call:
        when = "call"

    hook = plugin.pytest_runtest_makereport(None, Call())  # type: ignore[arg-type]
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(Outcome())
    rendered = str(report.longrepr) + "".join(content for _, content in report.sections)
    if sentinel in rendered or encoded in rendered:
        pytest.fail("the redactor left a sentinel or its base64 form in the report", pytrace=False)
    assert SentinelRedactor.PLACEHOLDER in str(report.longrepr)
    assert SentinelRedactor.PLACEHOLDER in report.sections[0][1]
    assert repr(SecretText("x")) == "<credential-bearing text>"
    assert request.config.pluginmanager.is_registered(sentinel_redactor)
