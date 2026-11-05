"""Shared fixtures for the personal Claude authentication tests (#5373, ADR 0268).

These tests put the real bundled Claude Code CLI behind the runner's production
wiring and observe what it sends. The CLI reaches the model only the way an
installation would let it: ``HTTPS_PROXY`` names the in-process intercepting
proxy here and ``NODE_EXTRA_CA_CERTS`` names a CA generated in ``tmp_path``.
Both are set in the runner's process env, never in ``ClaudeAgentOptions.env``.

The proxy answers ``CONNECT`` for ``api.anthropic.com`` and for a set of
alternate ``*.example.com`` hosts with a leaf certificate signed by that CA,
then reads the HTTP/1.1 requests inside the tunnel. Each request is recorded
with its host, method, path, auth scheme, the NAME of the placeholder
credential it carried (never the value), the body ``model`` and the system
text. ``POST .../v1/messages`` gets a scripted Messages streaming answer;
every other path gets a 404 JSON body.

Observed on CLI 2.1.301 (SDK 0.2.168) with the CLI's own OAuth token env: the
CLI sends ``CONNECT api.anthropic.com:443``, then ``GET
/api/claude_code/settings``, ``GET /api/claude_code/policy_limits`` and ``POST
/v1/messages?beta=true`` with ``Authorization: Bearer <grant>`` and no
``x-api-key``.

Every credential here is a placeholder. Assertion messages name the
placeholder that matched, never a header value.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime
import hashlib
import json
import os
import shutil
import ssl
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import anyio
import claude_agent_sdk
import pytest
from aci_protocol import ErrorEvent, Event, Final, parse_ndjson
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from curie_runner import RunTracer, SideEffectClassifier, load_plugins
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.plugin import bundle_skill_names
from curie_runner.sdk_auth import resolve_sdk_env
from curie_runner.session import SessionRunner
from curie_runner.side_effects import CLAUDE_READONLY_TOOLS
from curie_runner.tool_access import TurnToolAccess, front_pre_tool_use_hooks

BUNDLED_CLI = Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
CLI_AVAILABLE = BUNDLED_CLI.is_file() or shutil.which("claude") is not None
requires_cli = pytest.mark.skipif(
    not CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns"
)

TURN_TIMEOUT_S = 90
# A refusal happens before the CLI starts; a connect that gets this far has
# already failed the case, so it is bounded well under the turn timeout.
CONNECT_TIMEOUT_S = 30
IMPLEMENTER_MODEL = "claude-sonnet-5-5"
REVIEWER_MODEL = "claude-opus-5-5"
# A real Claude model ID that is neither the implementer nor the reviewer, so a
# request on it can only come from CLAUDE_CODE_SUBAGENT_MODEL.
FORCED_SUBAGENT_MODEL = "claude-haiku-4-5"
IMPLEMENTER_MARKER = "IMPLEMENTER-PLACEHOLDER: you implement one issue."

# --- placeholder credentials ----------------------------------------------------
#
# Shapes follow the real ones (an OAuth token is ``sk-ant-oat01-...``, an API key
# ``sk-ant-api03-...``) so prefix routing behaves as it would in production.

OWNER_GRANT = "sk-ant-oat01-PLACEHOLDER-owner"
SERVICE_OAUTH = "sk-ant-oat01-PLACEHOLDER-service"
SERVICE_API_KEY = "sk-ant-api03-PLACEHOLDER-service"
SERVICE_AUTH_TOKEN = "PLACEHOLDER-service-auth-token"
FOUNDRY_KEY = "PLACEHOLDER-foundry-key"
HELPER_KEY = "sk-ant-api03-PLACEHOLDER-helper"
ENV_KEY_TARGET = "sk-ant-api03-PLACEHOLDER-env-key-target"
GATEWAY_TOKEN = "PLACEHOLDER-gateway-token"
BEDROCK_BEARER = "PLACEHOLDER-bedrock-bearer"
AWS_ACCESS_KEY_ID = "PLACEHOLDER-aws-access-key-id"
AWS_SECRET = "PLACEHOLDER-aws-secret"
SAVED_LOGIN = "sk-ant-oat01-PLACEHOLDER-saved-login"
SAVED_API_KEY = "sk-ant-api03-PLACEHOLDER-saved-key"
CUSTOM_HEADER = "PLACEHOLDER-custom-header"
# 2100-01-01, in seconds and in ms: every planted session is unexpired.
FAR_FUTURE_S = 4_102_444_800
FAR_FUTURE_MS = FAR_FUTURE_S * 1000


def _placeholder_jwt(claims: Mapping[str, Any]) -> str:
    """An unsigned JWT-shaped placeholder (``alg: none``) carrying ``claims``."""

    def part(data: Mapping[str, Any]) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{part({'alg': 'none', 'typ': 'JWT'})}.{part(claims)}.c2ln"


# The signed-in gateway session's token. The CLI reads the record's own
# ``expiresAt``; the ``exp`` claim is future too so no decoder sees it expired.
GATEWAY_JWT = _placeholder_jwt({"sub": "example", "exp": FAR_FUTURE_S})

PLACEHOLDER_NAMES: dict[str, str] = {
    OWNER_GRANT: "owner grant",
    SERVICE_OAUTH: "service oauth",
    SERVICE_API_KEY: "service api key",
    SERVICE_AUTH_TOKEN: "service auth token",
    FOUNDRY_KEY: "foundry key",
    HELPER_KEY: "apiKeyHelper key",
    ENV_KEY_TARGET: "CURIE_MODEL_ENV_KEY target",
    GATEWAY_TOKEN: "gateway token",
    BEDROCK_BEARER: "bedrock bearer",
    AWS_ACCESS_KEY_ID: "aws access key id",
    AWS_SECRET: "aws secret",
    SAVED_LOGIN: "saved claude.ai login",
    SAVED_API_KEY: "saved api key",
    CUSTOM_HEADER: "custom header",
    GATEWAY_JWT: "gateway jwt",
}

# --- hosts ----------------------------------------------------------------------

FIRST_PARTY_HOST = "api.anthropic.com"
ALT_BASE_HOST = "alt-base.example.com"
ALT_MODEL_BASE_HOST = "alt-model-base.example.com"
ALT_BEDROCK_HOST = "alt-bedrock.example.com"
ALT_VERTEX_HOST = "alt-vertex.example.com"
ALT_FOUNDRY_HOST = "alt-foundry.example.com"
ALT_GATEWAY_HOST = "alt-gateway.example.com"
ALT_PROFILE_HOST = "alt-profile.example.com"
ALTERNATE_HOSTS = frozenset(
    {
        ALT_BASE_HOST,
        ALT_MODEL_BASE_HOST,
        ALT_BEDROCK_HOST,
        ALT_VERTEX_HOST,
        ALT_FOUNDRY_HOST,
        ALT_GATEWAY_HOST,
        ALT_PROFILE_HOST,
    }
)
_INTERCEPTED_HOSTS = frozenset({FIRST_PARTY_HOST, *ALTERNATE_HOSTS})

# Inherited env this file owns: anything a developer shell or CI job could leak
# into a real-CLI run. Cleared before each case so only what the case sets is live.
_OWNED_ENV_PREFIXES = (
    "ANTHROPIC_",
    "CLAUDE_",
    "CURIE_",
    "AWS_",
    "AZURE_",
    "GOOGLE_",
    "GCLOUD_",
    "CLOUD_ML_",
    "VERTEX_",
)
_OWNED_ENV_NAMES = (
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
    "NODE_EXTRA_CA_CERTS",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "XDG_CONFIG_HOME",
    "DISABLE_TELEMETRY",
    "CLAUDECODE",
)


# --- test CA --------------------------------------------------------------------


@dataclass(frozen=True)
class TestAuthority:
    """A throwaway CA and one leaf for every intercepted host, under ``tmp_path``.

    ``leaf_sha256`` is the SHA-256 of the leaf's DER bytes as lowercase hex
    without colons: the form the CLI pins a gateway's TLS certificate in.
    """

    ca_path: Path
    server_context: ssl.SSLContext
    leaf_sha256: str


def make_test_authority(root: Path) -> TestAuthority:
    root.mkdir(parents=True, exist_ok=True)
    now = datetime.datetime.now(datetime.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "curie-test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, FIRST_PARTY_HOST)]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(host) for host in sorted(_INTERCEPTED_HOSTS)]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = root / "ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    leaf_path = root / "leaf.pem"
    leaf_path.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    key_path = root / "leaf.key"
    key_path.write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(leaf_path, key_path)
    return TestAuthority(
        ca_path=ca_path,
        server_context=context,
        leaf_sha256=hashlib.sha256(leaf.public_bytes(serialization.Encoding.DER)).hexdigest(),
    )


# --- what the proxy records -----------------------------------------------------


def credential_name(value: str | None) -> str | None:
    """The placeholder name ``value`` equals, ``"unrecognized"``, or None."""

    if value is None:
        return None
    return PLACEHOLDER_NAMES.get(value, "unrecognized")


def _system_text(body: Mapping[str, Any]) -> str:
    system = body.get("system")
    if isinstance(system, str):
        return system
    if isinstance(system, list):
        return "\n".join(str(block.get("text", "")) for block in system if isinstance(block, dict))
    return ""


@dataclass(frozen=True)
class RecordedRequest:
    """One HTTP request the CLI sent through the proxy. Holds no credential value."""

    host: str
    method: str
    path: str
    bearer: str | None
    api_key: str | None
    other_authorization: bool
    credentials_seen: frozenset[str]
    model: str | None
    system: str
    body: dict[str, Any] = field(repr=False)

    @property
    def is_messages(self) -> bool:
        route = urlsplit(self.path).path
        return self.method == "POST" and route.endswith("/v1/messages")

    def tool_results(self) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for message in self.body.get("messages") or ():
            content = message.get("content") if isinstance(message, dict) else None
            if isinstance(content, list):
                found.extend(
                    block
                    for block in content
                    if isinstance(block, dict) and block.get("type") == "tool_result"
                )
        return found

    def tool_result_text(self) -> str:
        parts: list[str] = []
        for block in self.tool_results():
            content = block.get("content")
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                parts.extend(
                    str(item.get("text", "")) for item in content if isinstance(item, dict)
                )
        return "\n".join(parts)


@dataclass(frozen=True)
class TextReply:
    text: str


@dataclass(frozen=True)
class ToolCall:
    name: str
    input: dict[str, Any]
    call_id: str


Reply = TextReply | ToolCall
Responder = Callable[[RecordedRequest], Reply]


def answer_done(_request: RecordedRequest) -> Reply:
    return TextReply("done")


def _sse(event: str, data: Mapping[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def messages_stream(request: RecordedRequest, reply: Reply, message_id: str) -> bytes:
    """A Messages streaming answer, the shape ``test_harness_skills_real_cli`` uses."""

    message: dict[str, Any] = {
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": request.model or "stand-in",
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    block: dict[str, Any]
    delta: dict[str, Any]
    if isinstance(reply, ToolCall):
        stop_reason = "tool_use"
        block = {"type": "tool_use", "id": reply.call_id, "name": reply.name, "input": {}}
        delta = {"type": "input_json_delta", "partial_json": json.dumps(reply.input)}
    else:
        stop_reason = "end_turn"
        block = {"type": "text", "text": ""}
        delta = {"type": "text_delta", "text": reply.text}
    events: list[tuple[str, dict[str, Any]]] = [
        ("message_start", {"type": "message_start", "message": message}),
        (
            "content_block_start",
            {"type": "content_block_start", "index": 0, "content_block": block},
        ),
        ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    return b"".join(_sse(name, data) for name, data in events)


# --- the intercepting proxy -----------------------------------------------------


_NOT_FOUND = b'{"type":"error","error":{"type":"not_found_error","message":"stand-in"}}'


class _Closed(Exception):
    """The client closed the connection between requests."""


class InterceptingProxy:
    """An HTTP proxy that terminates TLS for the hosts this file names.

    Runs its own event loop on a background thread so a test can drive the CLI
    from ``anyio.run`` and still read what the proxy saw afterwards.
    """

    def __init__(self, authority: TestAuthority, responder: Responder = answer_done) -> None:
        self._authority = authority
        self.responder = responder
        self.connects: list[str] = []
        self.plain_hosts: list[str] = []
        self.requests: list[RecordedRequest] = []
        self.errors: list[str] = []
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping: asyncio.Event | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._port = 0
        self._messages_answered = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    @property
    def authority(self) -> TestAuthority:
        return self._authority

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="intercepting-proxy", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("the intercepting proxy did not start")

    def stop(self) -> None:
        loop, stopping, thread = self._loop, self._stopping, self._thread
        if loop is not None and stopping is not None and thread is not None:
            loop.call_soon_threadsafe(stopping.set)
            thread.join(timeout=10)

    def _run(self) -> None:
        # asyncio.run cancels the open tunnels when serving ends.
        asyncio.run(self._serve_forever())

    async def _serve_forever(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stopping = asyncio.Event()
        server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self._port = int(server.sockets[0].getsockname()[1])
        self._ready.set()
        try:
            await self._stopping.wait()
        finally:
            server.close()
            server.abort_clients()

    # The views tests assert on. Copies, so a late request cannot change them mid-assert.

    def connect_hosts(self) -> list[str]:
        with self._lock:
            return [target.rpartition(":")[0] for target in self.connects]

    def model_requests(self) -> list[RecordedRequest]:
        with self._lock:
            return [request for request in self.requests if request.is_messages]

    def all_requests(self) -> list[RecordedRequest]:
        with self._lock:
            return list(self.requests)

    def alternate_hits(self) -> list[str]:
        """Every CONNECT, plain request or tunnelled request to an alternate host."""

        with self._lock:
            hosts = [target.rpartition(":")[0] for target in self.connects]
            hosts.extend(self.plain_hosts)
            hosts.extend(request.host for request in self.requests)
        return [host for host in hosts if host in ALTERNATE_HOSTS]

    def total_traffic(self) -> int:
        with self._lock:
            return len(self.connects) + len(self.plain_hosts) + len(self.requests)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            method, target, headers = await self._read_head(reader)
            if method == "CONNECT":
                host = target.rpartition(":")[0]
                with self._lock:
                    self.connects.append(target)
                if host not in _INTERCEPTED_HOSTS:
                    writer.write(b"HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\n\r\n")
                    await writer.drain()
                    return
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                await writer.start_tls(self._authority.server_context)
                await self._serve(reader, writer, host, None)
            else:
                parsed = urlsplit(target)
                host = parsed.hostname or ""
                with self._lock:
                    self.plain_hosts.append(host)
                path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
                await self._serve(reader, writer, host, (method, path or "/", headers))
        except (_Closed, ConnectionError, asyncio.IncompleteReadError, ssl.SSLError):
            pass
        except Exception as exc:  # noqa: BLE001 - recorded so a fixture bug is visible
            with self._lock:
                self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            writer.close()

    @staticmethod
    async def _read_head(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str]]:
        line = await reader.readline()
        if not line:
            raise _Closed
        method, target, _version = line.decode("latin-1").split(" ", 2)
        headers: dict[str, str] = {}
        while True:
            raw = await reader.readline()
            if raw in (b"\r\n", b"\n", b""):
                break
            name, _, value = raw.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        return method, target, headers

    @staticmethod
    async def _read_body(reader: asyncio.StreamReader, headers: Mapping[str, str]) -> bytes:
        if "content-length" in headers:
            return await reader.readexactly(int(headers["content-length"]))
        if headers.get("transfer-encoding", "").lower() == "chunked":
            chunks: list[bytes] = []
            while True:
                size = int((await reader.readline()).split(b";")[0].strip(), 16)
                if size == 0:
                    await reader.readline()
                    return b"".join(chunks)
                chunks.append(await reader.readexactly(size))
                await reader.readline()
        return b""

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        host: str,
        first: tuple[str, str, dict[str, str]] | None,
    ) -> None:
        while True:
            if first is not None:
                method, path, headers = first
                first = None
            else:
                method, path, headers = await self._read_head(reader)
            raw_body = await self._read_body(reader, headers)
            request = self._record(host, method, path, headers, raw_body)
            if request.is_messages:
                with self._lock:
                    self._messages_answered += 1
                    message_id = f"msg_stand_in_{self._messages_answered}"
                payload = messages_stream(request, self.responder(request), message_id)
                head = (
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n"
                    b"content-length: %d\r\n\r\n" % len(payload)
                )
            else:
                payload = _NOT_FOUND
                head = (
                    b"HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\n"
                    b"content-length: %d\r\n\r\n" % len(payload)
                )
            writer.write(head + payload)
            await writer.drain()

    def _record(
        self,
        host: str,
        method: str,
        path: str,
        headers: Mapping[str, str],
        raw_body: bytes,
    ) -> RecordedRequest:
        authorization = headers.get("authorization")
        bearer: str | None = None
        other_authorization = False
        if authorization is not None:
            scheme, _, token = authorization.partition(" ")
            if scheme.lower() == "bearer":
                bearer = credential_name(token.strip())
            else:
                other_authorization = True
        seen = frozenset(
            name
            for value, name in PLACEHOLDER_NAMES.items()
            if any(value in header for header in headers.values())
        )
        body: dict[str, Any] = {}
        if raw_body:
            try:
                decoded = json.loads(raw_body)
            except ValueError:
                decoded = None
            if isinstance(decoded, dict):
                body = decoded
        model = body.get("model")
        request = RecordedRequest(
            host=host,
            method=method,
            path=path,
            bearer=bearer,
            api_key=credential_name(headers.get("x-api-key")),
            other_authorization=other_authorization,
            credentials_seen=seen,
            model=model if isinstance(model, str) else None,
            system=_system_text(body),
            body=body,
        )
        with self._lock:
            self.requests.append(request)
        return request


# --- process env, as an installation would set it --------------------------------


@dataclass(frozen=True)
class Installation:
    """The paths one real-CLI case owns. All under ``tmp_path``."""

    root: Path
    home: Path
    workspace: Path
    proxy: InterceptingProxy


@contextlib.contextmanager
def installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, responder: Responder = answer_done
) -> Iterator[Installation]:
    """Start the proxy and set the runner's process env as an installation would.

    Every inherited auth, provider, proxy and CA variable is cleared first, so a
    case sees only what it sets. ``resolve_sdk_env(os.environ)`` mutates the
    process env in service mode, so the whole env is restored on exit.
    """

    snapshot = dict(os.environ)
    for key in list(os.environ):
        if key.startswith(_OWNED_ENV_PREFIXES) or key in _OWNED_ENV_NAMES:
            monkeypatch.delenv(key)
    root = tmp_path / "installation"
    home = root / "home"
    workspace = root / "workspace"
    home.mkdir(parents=True)
    workspace.mkdir()
    proxy = InterceptingProxy(make_test_authority(root / "tls"), responder)
    proxy.start()
    try:
        monkeypatch.setenv("HOME", str(home))
        # Installation transport (ADR 0268 section 4): the only way the CLI
        # reaches the stand-in. Docs: https://code.claude.com/docs/en/env-vars
        # (HTTPS_PROXY, HTTP_PROXY, NODE_EXTRA_CA_CERTS).
        monkeypatch.setenv("HTTPS_PROXY", proxy.url)
        monkeypatch.setenv("HTTP_PROXY", proxy.url)
        monkeypatch.setenv("NODE_EXTRA_CA_CERTS", str(proxy_ca_path(root)))
        # Docs (env-vars): disables autoupdate, bug reports, error reporting and
        # telemetry, so the proxy sees only the model path.
        monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        yield Installation(root=root, home=home, workspace=workspace, proxy=proxy)
    finally:
        proxy.stop()
        os.environ.clear()
        os.environ.update(snapshot)


def proxy_ca_path(root: Path) -> Path:
    return root / "tls" / "ca.pem"


# --- files a case plants ----------------------------------------------------------


def write_json(path: Path, data: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def merge_json(path: Path, data: Mapping[str, Any]) -> Path:
    """Add top-level keys to a JSON object file, creating it when absent."""

    existing = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    return write_json(path, {**existing, **data})


def restorable_gateway_session(installation: Installation) -> dict[str, Any]:
    """A signed-in gateway session the pinned CLI restores at startup.

    The ``.credentials.json`` shape the CLI 2.1.301 startup restore reads:
    ``enterpriseGateway`` with ``url``, ``jwt`` and ``expiresAt`` (ms), and
    ``gatewayTrust`` keyed by the gateway URL's hostname holding the gateway's
    TLS certificate SHA-256 (lowercase hex, no colons). Without the trust entry
    the CLI prints "Cloud gateway <host> is not trusted on this machine" and
    skips the session; an expired session without ``idpRefreshToken`` is skipped
    too. The restore then probes the gateway's TLS through ``HTTPS_PROXY``,
    which here is the intercepting proxy, so the pin is this run's leaf.
    """

    return {
        "enterpriseGateway": {
            "url": f"https://{ALT_GATEWAY_HOST}",
            "jwt": GATEWAY_JWT,
            "expiresAt": FAR_FUTURE_MS,
        },
        "gatewayTrust": {ALT_GATEWAY_HOST: installation.proxy.authority.leaf_sha256},
    }


UNSET = "<unset>"


def env_probe_command(names: Sequence[str]) -> str:
    """A Bash command printing ``NAME=<value>`` for each name, ``<unset>`` when absent."""

    return "; ".join(f"printf '{name}=%s\\n' \"${{{name}-{UNSET}}}\"" for name in names)


def env_probe_values(text: str, names: Sequence[str]) -> dict[str, str]:
    """The ``NAME=<value>`` lines ``env_probe_command`` printed, by name."""

    values: dict[str, str] = {}
    for line in text.splitlines():
        name, sep, value = line.strip().partition("=")
        if sep and name in names:
            values[name] = value
    missing = [name for name in names if name not in values]
    assert not missing, f"the probe printed no line for {missing}"
    return values


def write_api_key_helper(root: Path) -> tuple[Path, Path]:
    """An ``apiKeyHelper`` command that leaves a marker file when it runs.

    Returns ``(helper, marker)``. The marker is absent until the CLI runs it.
    """

    helper = root / "api-key-helper.sh"
    marker = root / "api-key-helper.ran"
    helper.parent.mkdir(parents=True, exist_ok=True)
    helper.write_text(
        f"#!/bin/sh\nprintf ran > '{marker}'\nprintf '%s' '{HELPER_KEY}'\n", encoding="utf-8"
    )
    helper.chmod(0o700)
    return helper, marker


def write_bundle(root: Path, *, name: str = "probe", agents: Mapping[str, str] = {}) -> str:
    """A bundle with ``.claude-plugin/plugin.json`` and ``agents/<file>`` texts."""

    write_json(root / ".claude-plugin" / "plugin.json", {"name": name, "version": "0.1.0"})
    for filename, text in agents.items():
        target = root / "agents" / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return str(root)


# --- the production wiring ----------------------------------------------------------


@dataclass(frozen=True)
class Wiring:
    """What the runner builds before a session: the options and the turn gate."""

    options: Any
    access: TurnToolAccess


def wire_session(
    installation: Installation,
    *,
    reviewer_model: str | None = None,
    explicit_env: Mapping[str, str] | None = None,
    plugin_dir: str | None = None,
    max_turns: int = 6,
) -> Wiring:
    """``resolve_sdk_env(os.environ)`` then ``build_options``, as ``__main__`` does.

    ``explicit_env`` is what a caller hands ``build_options`` as the SDK env on
    top of the boot override (``ClaudeAgentOptions.env``).
    """

    override = resolve_sdk_env(os.environ)
    access = TurnToolAccess(CLAUDE_READONLY_TOOLS)
    options = build_options(
        plugins=load_plugins(plugin_dir),
        model=IMPLEMENTER_MODEL,
        reviewer_model=reviewer_model,
        system_prompt=IMPLEMENTER_MARKER,
        max_turns=max_turns,
        max_budget_usd=None,
        resume=None,
        cwd=str(installation.workspace),
        hooks=front_pre_tool_use_hooks(None, access),
        skills=bundle_skill_names(plugin_dir),
        env={**(override or {}), **(explicit_env or {})},
    )
    return Wiring(options=options, access=access)


def connect_session(wiring: Wiring) -> None:
    """``ClaudeAgentSession(options).connect()`` then close; a refusal propagates."""

    async def scenario() -> None:
        session = ClaudeAgentSession(wiring.options)
        try:
            with anyio.fail_after(CONNECT_TIMEOUT_S):
                await session.connect()
        finally:
            await session.close()

    anyio.run(scenario)


@dataclass(frozen=True)
class TurnOutcome:
    final: Final
    errors: list[ErrorEvent]


def run_turn(wiring: Wiring, prompt: str = "Implement the issue.") -> TurnOutcome:
    """One turn through ``SessionRunner`` with a ``ClaudeAgentSession`` factory."""

    async def scenario() -> TurnOutcome:
        runner = SessionRunner(
            held_secrets=frozenset(),
            session_factory=lambda: ClaudeAgentSession(wiring.options),
            ceiling=0,
            max_usd_per_day=1.0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="curie-run:personal-auth",
            session_id="session-PLACEHOLDER",
            model=IMPLEMENTER_MODEL,
            tool_access=wiring.access,
        )
        lines: list[str] = []
        try:
            with anyio.fail_after(TURN_TIMEOUT_S):
                async for line in runner.run_turn(
                    Event(type="message", text=prompt, user="U0EXAMPLE1", ts="1")
                ):
                    lines.append(line)
        finally:
            await runner.close()
        events = parse_ndjson("".join(lines))
        finals = [event for event in events if isinstance(event, Final)]
        assert len(finals) == 1, f"expected one final, got {len(finals)}"
        errors = [event for event in events if isinstance(event, ErrorEvent)]
        return TurnOutcome(final=finals[0], errors=errors)

    return anyio.run(scenario)


# --- shared assertions ------------------------------------------------------------


def assert_owner_grant_only(proxy: InterceptingProxy) -> None:
    """Every model request went to first-party Messages with the owner grant alone.

    The request facts asserted (host, ``/v1/messages`` with query parameters,
    ``Authorization: Bearer`` and no ``x-api-key``) are the observed fixture on
    CLI 2.1.301 for a ``CLAUDE_CODE_OAUTH_TOKEN`` session, and match the
    precedence docs at https://code.claude.com/docs/en/authentication
    (``CLAUDE_CODE_OAUTH_TOKEN``, rank 5, the only source left once ranks 1 to 4
    and the saved sources are removed).
    """

    requests = proxy.model_requests()
    assert requests, "the CLI sent no model request"
    for request in requests:
        assert request.host == FIRST_PARTY_HOST, f"a model request went to {request.host}"
        assert urlsplit(request.path).path == "/v1/messages", (
            f"a model request used {urlsplit(request.path).path}"
        )
        assert request.bearer == "owner grant", (
            f"the model request Bearer matched {request.bearer!r}, not the owner grant"
        )
        assert request.api_key is None, f"the model request carried x-api-key ({request.api_key})"
        assert not request.other_authorization, "the model request used another auth scheme"
    for request in proxy.all_requests():
        assert request.credentials_seen <= {"owner grant"}, (
            f"{request.method} {urlsplit(request.path).path} on {request.host} carried "
            f"{sorted(request.credentials_seen)}"
        )
    exchanges = [
        request.host
        for request in proxy.all_requests()
        if urlsplit(request.path).path.endswith("/oauth/token")
    ]
    # A profile or federation credential shows up as a token exchange first
    # (https://platform.claude.com/docs/en/manage-claude/wif-reference, "Token
    # exchange request"); none may happen.
    assert exchanges == [], f"the CLI attempted a token exchange on {exchanges}"
    assert proxy.alternate_hits() == [], f"alternate hosts were reached: {proxy.alternate_hits()}"
    assert set(proxy.connect_hosts()) == {FIRST_PARTY_HOST}, (
        f"the CLI tunnelled to {sorted(set(proxy.connect_hosts()))}"
    )
    assert proxy.errors == [], proxy.errors


def assert_cli_never_started(proxy: InterceptingProxy) -> None:
    """Zero CONNECTs and zero requests: nothing reached the network."""

    assert proxy.total_traffic() == 0, (
        f"the proxy saw traffic: CONNECT {proxy.connect_hosts()}, "
        f"{len(proxy.all_requests())} requests"
    )
