"""A static token is normalized at the config boundary and never leaks (#4623).

An operator's ``token_file`` usually ends in a newline. The loader and
`StaticTokenCredentialConfig` strip surrounding ASCII whitespace, so the token
that reaches an HTTP header and git's ``http.extraHeader`` is exactly the
token. A token that is empty after stripping, or still carries CR, LF, another
ASCII control character or DEL, is refused before any request, naming the entry
and the file (or the entry and "static token"), never the token.

Every forge kind with a static token is driven end to end: load through
``load_forges_json``, build through the production registry, one real adapter
call against an in-process HTTP server on 127.0.0.1, and real ``git ls-remote``
with the credential's header against the same server. The server records the
auth headers it received.

The transports turn an ``httpx`` failure into ``Unavailable`` with no cause and
no context, since the ``httpx`` error text can carry the header value, and log
a safe diagnostic naming the API root instead.
"""

from __future__ import annotations

import base64
import http.server
import json
import logging
import os
import secrets
import socket
import subprocess
import threading
import traceback
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.forges.bitbucket_cloud.transport import CloudTransport
from curie_api.forges.bitbucket_dc.transport import DcTransport
from curie_api.forges.config import (
    BindingConfig,
    CodeHostConfig,
    ForgesConfig,
    RepositoryBindingConfig,
    StaticTokenCredentialConfig,
    TrackerConfig,
)
from curie_api.forges.credential_providers import TokenSnapshot
from curie_api.forges.errors import ForgeError, Unavailable
from curie_api.forges.gitlab.transport import GitLabTransport
from curie_api.forges.jira_cloud.transport import JiraTransport
from curie_api.forges.loader import load_forges_json
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import CredentialScope, TrackerIssueRef
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import SecretStr, ValidationError

JIRA_USER = "factory-bot@example.test"
GITLAB_PROJECT = "42"
JIRA_SCOPE = "site-0001"
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")

# Every forge entry a static token can authenticate: (section, kind, name).
CODE_HOSTS = ("gitlab", "bitbucket_cloud", "bitbucket_dc")
TRACKERS = ("gitlab_tracker", "jira_cloud")
ENTRIES = CODE_HOSTS + TRACKERS
NAMES = {
    "gitlab": "gl",
    "bitbucket_cloud": "bbc",
    "bitbucket_dc": "bbdc",
    "gitlab_tracker": "gl-issues",
    "jira_cloud": "site",
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def sentinel() -> str:
    return f"snt{secrets.token_hex(8)}"


@pytest.fixture(autouse=True)
def _no_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PROXY_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")


# The recording server ---------------------------------------------------------


@dataclass(frozen=True)
class Seen:
    method: str
    path: str
    authorization: str | None
    private_token: str | None


@dataclass
class Recorder:
    port: int
    requests: list[Seen] = field(default_factory=list)

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _handler(recorder: Recorder) -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def _answer(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            recorder.requests.append(
                Seen(
                    self.command,
                    self.path,
                    self.headers.get("Authorization"),
                    self.headers.get("PRIVATE-TOKEN"),
                )
            )
            if ".git/" in self.path:
                body, status = b"not a git server", 404
            else:
                # Enough for each adapter to get past its version gate.
                payload = {"version": "17.5.0", "values": [], "isLastPage": True}
                body, status = json.dumps(payload).encode(), 200
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_PUT = _answer

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return Handler


@pytest.fixture
def server() -> Iterator[Recorder]:
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)
    httpd.daemon_threads = True
    recorder = Recorder(port=httpd.server_address[1])
    httpd.RequestHandlerClass = _handler(recorder)
    thread = threading.Thread(target=httpd.serve_forever, args=(0.02,), daemon=True)
    try:
        thread.start()
        yield recorder
    finally:
        if thread.is_alive():
            httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
    assert not thread.is_alive()


@pytest.fixture
def closed_port() -> Iterator[int]:
    """A port bound and never listened on, so a connect is refused."""

    held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        held.bind(("127.0.0.1", 0))
        yield held.getsockname()[1]
    finally:
        held.close()


# Config documents --------------------------------------------------------------


def _entry(kind: str, origin: str, credential: dict[str, Any]) -> dict[str, Any]:
    name = NAMES[kind]
    if kind == "gitlab":
        return {
            "name": name,
            "kind": "gitlab",
            "host": "gitlab.test",
            "api_url": f"{origin}/api/v4",
            "credential": credential,
        }
    if kind == "bitbucket_cloud":
        return {
            "name": name,
            "kind": "bitbucket_cloud",
            "host": "bitbucket.org",
            "api_url": origin,
            "credential": credential,
        }
    if kind == "bitbucket_dc":
        return {
            "name": name,
            "kind": "bitbucket_dc",
            "host": "bitbucket-dc.test",
            "api_url": origin,
            "credential": credential,
        }
    if kind == "gitlab_tracker":
        return {
            "name": name,
            "kind": "gitlab",
            "host": "gitlab.test",
            "api_url": f"{origin}/api/v4",
            "credential": credential,
            "label": "curie",
            "mention": "factory-bot",
        }
    assert kind == "jira_cloud"
    return {
        "name": name,
        "kind": "jira_cloud",
        "host": "site.atlassian.test",
        "api_url": origin,
        "credential": {**credential, "username": JIRA_USER},
        "label": "curie",
        "mention": "712020:00000000-0000-4000-8000-000000000009",
    }


def _section(kind: str) -> str:
    return "code_hosts" if kind in CODE_HOSTS else "trackers"


def _label(kind: str) -> str:
    what = "code host" if kind in CODE_HOSTS else "tracker"
    return f"{what} {NAMES[kind]!r}"


def _file_document(kind: str, origin: str, token_file: Path) -> str:
    credential = {"type": "static_token", "token_file": str(token_file)}
    return json.dumps({_section(kind): [_entry(kind, origin, credential)]})


def _programmatic(kind: str, origin: str, token: str) -> ForgesConfig:
    # The entry model validates the credential, so a refusal can name the entry.
    username = JIRA_USER if kind == "jira_cloud" else None
    entry = _entry(kind, origin, {"type": "static_token", "token": token, "username": username})
    if kind in CODE_HOSTS:
        return ForgesConfig(code_hosts=(CodeHostConfig(**entry),))
    return ForgesConfig(trackers=(TrackerConfig(**entry),))


def _token_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "token"
    path.write_bytes(content.encode())
    return path


# Driving one real request ------------------------------------------------------


def _binding(kind: str) -> BindingConfig:
    scope = GITLAB_PROJECT if kind == "gitlab_tracker" else JIRA_SCOPE
    return BindingConfig(
        name="intake",
        tracker=NAMES[kind],
        tracker_scope_id=scope,
        repos=(
            RepositoryBindingConfig(
                alias="app", code_host="gl", project_id=GITLAB_PROJECT, path="grp/app"
            ),
        ),
        default_repo="app",
    )


async def _drive(registry: ForgeRegistry, kind: str) -> None:
    """One adapter call through the transport. The server's answers are not a
    real forge's, so a port error after the request is expected."""

    name = NAMES[kind]
    try:
        if kind in CODE_HOSTS:
            connection: Any = registry.code_host(name)
            async with connection.client() as client:
                await connection.adapter(client).resolve_repository(GITLAB_PROJECT)
            return
        tracker_connection: Any = registry.tracker(name)
        config = registry.config.tracker(name)
        scope = GITLAB_PROJECT if kind == "gitlab_tracker" else JIRA_SCOPE
        issue = TrackerIssueRef(config.kind, config.host, scope, "7")
        async with httpx.AsyncClient(timeout=10.0) as client:
            tracker = tracker_connection.for_binding(client, _binding(kind))
            await tracker.ticket_labels(issue)
    except ForgeError:
        pass


def _basic(username: str, token: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{token}".encode()).decode()


def _expected_api(kind: str, token: str) -> tuple[str, str]:
    """(header, value) the API request must carry."""

    if kind in ("gitlab", "gitlab_tracker"):
        return "PRIVATE-TOKEN", token
    if kind == "jira_cloud":
        return "Authorization", _basic(JIRA_USER, token)
    return "Authorization", f"Bearer {token}"


def _expected_git(kind: str, token: str) -> str:
    if kind == "gitlab":
        return _basic("oauth2", token)
    if kind == "bitbucket_cloud":
        return _basic("x-token-auth", token)
    return f"Bearer {token}"


def _sent(seen: Seen, header: str) -> str | None:
    return seen.private_token if header == "PRIVATE-TOKEN" else seen.authorization


def _assert_api_saw(server: Recorder, kind: str, token: str) -> None:
    header, value = _expected_api(kind, token)
    api = [seen for seen in server.requests if ".git/" not in seen.path]
    sent = [_sent(seen, header) for seen in api if _sent(seen, header) is not None]
    assert sent, f"no authenticated API request reached the server: {api}"
    assert set(sent) == {value}


def _git_ls_remote(server: Recorder, header: str) -> None:
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/nonexistent"),
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "NO_PROXY": "127.0.0.1",
    }
    # Git fails: the server is not a git server. Only the request matters.
    subprocess.run(
        ["git", "-c", f"http.extraHeader={header}", "ls-remote", f"{server.origin}/x/y.git"],
        env=env,
        capture_output=True,
        timeout=30,
        check=False,
    )


async def _assert_git_saw(server: Recorder, registry: ForgeRegistry, kind: str, token: str) -> None:
    connection: Any = registry.code_host(NAMES[kind])
    credential = await connection.credentials.get(CredentialScope.CLONE)
    _git_ls_remote(server, credential.git_header())
    refs = [seen for seen in server.requests if seen.path.startswith("/x/y.git/info/refs")]
    assert refs, "git sent no request"
    assert {seen.authorization for seen in refs} == {_expected_git(kind, token)}


# A. The regression pin -------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
@pytest.mark.parametrize(
    "content", ["{t}\n", "{t}\r\n", "  \t{t} \n", " {t}\t"], ids=["lf", "crlf", "padded", "spaces"]
)
async def test_static_token_file_newline_reaches_real_http_and_git_normalized(
    server: Recorder, tmp_path: Path, sentinel: str, kind: str, content: str
) -> None:
    token = f"tok-{sentinel}"
    path = _token_file(tmp_path, content.format(t=token))

    config = load_forges_json(_file_document(kind, server.origin, path))
    registry = build_registry(config)
    await _drive(registry, kind)

    _assert_api_saw(server, kind, token)
    if kind in CODE_HOSTS:
        await _assert_git_saw(server, registry, kind, token)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
async def test_a_programmatic_static_token_reaches_http_and_git_normalized(
    server: Recorder, sentinel: str, kind: str
) -> None:
    token = f"tok-{sentinel}"

    registry = build_registry(_programmatic(kind, server.origin, f"  {token}\r\n"))
    await _drive(registry, kind)

    _assert_api_saw(server, kind, token)
    if kind in CODE_HOSTS:
        await _assert_git_saw(server, registry, kind, token)


# B. Refusals before any request ---------------------------------------------

REFUSED = {
    "blank": "   \n",
    "lf": "tok\n{s}",
    "cr": "tok\r{s}",
    "nul": "tok\x00{s}",
    "esc": "tok\x1b{s}",
    "del": "tok\x7f{s}",
}


def _chain(error: BaseException) -> list[BaseException]:
    # Every exception still reachable from ``error``, suppressed context included.
    seen: list[BaseException] = []
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or any(current is known for known in seen):
            continue
        seen.append(current)
        pending.extend((current.__cause__, current.__context__))
    return seen


def _telemetry(error: BaseException) -> str:
    # What an OpenTelemetry span records for the exception: type, message, stack.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with provider.get_tracer(__name__).start_as_current_span("forge") as span:
        span.record_exception(error)
    provider.shutdown()
    return json.dumps(
        [
            dict(event.attributes or {})
            for span in exporter.get_finished_spans()
            for event in span.events
        ]
    )


USERNAMES = (JIRA_USER, "oauth2", "x-token-auth")


def _needles(sentinel: str) -> list[str]:
    """The raw sentinel and every encoded header form a token could take."""

    needles: list[str] = []
    # The tokens these tests use: bare, prefixed, and with a trailing newline.
    for token in (sentinel, f"tok-{sentinel}", f"tok-{sentinel}\n"):
        needles.append(token)
        needles.append(f"Bearer {token}")
        needles.append(base64.b64encode(token.encode()).decode())
        for username in USERNAMES:
            needles.append(base64.b64encode(f"{username}:{token}".encode()).decode())
    return needles


def _leaks(error: BaseException, sentinel: str) -> list[str]:
    texts = {
        "str": str(error),
        "repr": repr(error),
        "traceback": "".join(traceback.format_exception(error)),
        "telemetry": _telemetry(error),
    }
    for depth, linked in enumerate(_chain(error)):
        texts[f"chain[{depth}]"] = f"{linked!s} {linked!r}"
    if isinstance(error, ValidationError):
        # Default arguments: the refusal must not carry the value even then.
        texts["errors"] = repr(error.errors())
        texts["json"] = error.json()
        texts["errors-no-input"] = repr(error.errors(include_input=False))
        texts["json-no-input"] = error.json(include_input=False)
    needles = _needles(sentinel)
    return [where for where, text in texts.items() if any(needle in text for needle in needles)]


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
@pytest.mark.parametrize("problem", sorted(REFUSED))
async def test_a_bad_token_file_is_refused_naming_entry_and_path_before_io(
    server: Recorder, tmp_path: Path, sentinel: str, kind: str, problem: str
) -> None:
    path = _token_file(tmp_path, REFUSED[problem].format(s=sentinel))

    with pytest.raises(ValueError) as refused:
        registry = build_registry(load_forges_json(_file_document(kind, server.origin, path)))
        await _drive(registry, kind)

    message = str(refused.value)
    assert _label(kind) in message, message
    assert str(path) in message, message
    assert _leaks(refused.value, sentinel) == []
    assert server.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
@pytest.mark.parametrize("problem", sorted(REFUSED))
async def test_a_bad_programmatic_token_is_refused_naming_the_entry_before_io(
    server: Recorder, sentinel: str, kind: str, problem: str
) -> None:
    token = REFUSED[problem].format(s=sentinel)

    with pytest.raises(ValueError) as refused:
        registry = build_registry(_programmatic(kind, server.origin, token))
        await _drive(registry, kind)

    message = str(refused.value)
    assert repr(NAMES[kind]) in message, message
    assert "static token" in message, message
    assert _leaks(refused.value, sentinel) == []
    assert server.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
@pytest.mark.parametrize("problem", sorted(REFUSED))
async def test_a_bad_inline_token_in_a_forges_document_is_refused_naming_the_entry(
    server: Recorder, sentinel: str, kind: str, problem: str
) -> None:
    credential = {"type": "static_token", "token": REFUSED[problem].format(s=sentinel)}
    document = {_section(kind): [_entry(kind, server.origin, credential)]}

    with pytest.raises(ValueError) as refused:
        registry = build_registry(ForgesConfig.model_validate(document))
        await _drive(registry, kind)

    message = str(refused.value)
    assert repr(NAMES[kind]) in message, message
    assert "static token" in message, message
    assert _leaks(refused.value, sentinel) == []
    assert server.requests == []


@pytest.mark.parametrize("problem", sorted(REFUSED))
def test_a_bad_standalone_static_token_is_refused_without_echo(sentinel: str, problem: str) -> None:
    with pytest.raises(ValidationError) as refused:
        StaticTokenCredentialConfig.model_validate({"token": REFUSED[problem].format(s=sentinel)})

    assert "static token" in str(refused.value)
    assert _leaks(refused.value, sentinel) == []


def test_a_valid_token_with_conflicting_expiry_is_refused_without_echo(sentinel: str) -> None:
    with pytest.raises(ValidationError) as refused:
        StaticTokenCredentialConfig.model_validate(
            {
                "token": sentinel,
                "expires_at": datetime(2030, 1, 1, tzinfo=UTC),
                "never_expires": True,
            }
        )

    assert "not both" in str(refused.value)
    assert _leaks(refused.value, sentinel) == []


@pytest.mark.parametrize("kind", ENTRIES)
def test_a_programmatic_entry_with_an_invalid_sibling_refuses_without_echo(
    sentinel: str, kind: str
) -> None:
    token = f"tok-{sentinel}"
    username = JIRA_USER if kind == "jira_cloud" else None
    credential = {"type": "static_token", "token": token, "username": username}
    entry = _entry(kind, "http://127.0.0.1:1", credential)
    entry["host"] = ""
    model = CodeHostConfig if kind in CODE_HOSTS else TrackerConfig

    with pytest.raises(ValidationError) as refused:
        model(**entry)

    assert _leaks(refused.value, sentinel) == []
    with pytest.raises(ValidationError) as refused:
        ForgesConfig.model_validate({_section(kind): [entry]})
    assert _leaks(refused.value, sentinel) == []


def test_a_github_app_key_is_not_echoed_when_a_sibling_field_is_invalid(sentinel: str) -> None:
    credential = {"type": "github_app", "app_id": "", "private_key": sentinel}
    entry = {"name": "gh", "kind": "github", "host": "github.test", "credential": credential}

    with pytest.raises(ValidationError) as refused:
        CodeHostConfig(**entry)

    assert sentinel not in repr(refused.value.errors())
    assert sentinel not in refused.value.json()
    assert _leaks(refused.value, sentinel) == []


@pytest.mark.parametrize("raw", [" \ttok-{s}\r\n", "\x0b\x0ctok-{s}\x0c\x0b"])
def test_a_standalone_static_token_strips_surrounding_ascii_whitespace(
    sentinel: str, raw: str
) -> None:
    credential = StaticTokenCredentialConfig.model_validate({"token": raw.format(s=sentinel)})
    assert credential.current_token() == f"tok-{sentinel}"


# C. Liveness -----------------------------------------------------------------

OPAQUE = "glpat-AbC_123.~+/="


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ENTRIES)
async def test_an_opaque_token_with_punctuation_reaches_the_server_unchanged(
    server: Recorder, tmp_path: Path, kind: str
) -> None:
    path = _token_file(tmp_path, OPAQUE)

    registry = build_registry(load_forges_json(_file_document(kind, server.origin, path)))
    await _drive(registry, kind)

    _assert_api_saw(server, kind, OPAQUE)
    if kind in CODE_HOSTS:
        await _assert_git_saw(server, registry, kind, OPAQUE)


# D. Transport failures carry no httpx error ----------------------------------


def _snapshot(token: str) -> TokenSnapshot:
    """``token`` exactly as given, unnormalized, so the transport's own header
    guard is what refuses an unsafe value."""

    return TokenSnapshot(StaticTokenCredentialConfig.model_construct(token=SecretStr(token)))


async def _gitlab(client: httpx.AsyncClient, origin: str, token: str) -> None:
    async def source() -> str:
        return token

    await GitLabTransport(
        client, api_url=f"{origin}/api/v4", token=source, entry=NAMES["gitlab"]
    ).get("/user")


async def _jira(client: httpx.AsyncClient, origin: str, token: str) -> None:
    transport = JiraTransport(
        client,
        api_url=origin,
        username=JIRA_USER,
        token=_snapshot(token),
        entry=NAMES["jira_cloud"],
    )
    await transport.get("/myself", what="myself")


async def _cloud(client: httpx.AsyncClient, origin: str, token: str) -> None:
    await CloudTransport(
        client, api_url=origin, token=_snapshot(token), entry=NAMES["bitbucket_cloud"]
    ).request("GET", f"{origin}/user", what="user")


async def _dc(client: httpx.AsyncClient, origin: str, token: str) -> None:
    await DcTransport(
        client, api_url=origin, token=_snapshot(token), entry=NAMES["bitbucket_dc"]
    ).json("GET", "/application-properties", what="version")


# (call, reason, path appended to the origin that the log must name)
TRANSPORTS = {
    "gitlab": (_gitlab, "transport", "/api/v4"),
    "jira_cloud": (_jira, "myself", ""),
    "bitbucket_cloud": (_cloud, "transport", ""),
    "bitbucket_dc": (_dc, "transport", ""),
}


async def _fails_cleanly(
    caplog: pytest.LogCaptureFixture, kind: str, origin: str, token: str, sentinel: str
) -> None:
    call, reason, suffix = TRANSPORTS[kind]
    caplog.set_level(logging.DEBUG)

    with pytest.raises(Unavailable) as failed:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await call(client, origin, token)

    error = failed.value
    assert error.args == (reason,)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _leaks(error, sentinel) == []
    assert not [needle for needle in _needles(sentinel) if needle in caplog.text]
    root = f"{origin}{suffix}"
    assert any(
        record.levelno >= logging.WARNING and root in record.getMessage()
        for record in caplog.records
    ), caplog.text
    # The warning names the configured entry, not only the API root.
    assert repr(NAMES[kind]) in caplog.text, caplog.text
    assert any(
        record.levelno >= logging.WARNING and repr(NAMES[kind]) in record.getMessage()
        for record in caplog.records
    ), caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize("kind", sorted(TRANSPORTS))
async def test_a_refused_connection_is_unavailable_with_no_httpx_context(
    caplog: pytest.LogCaptureFixture, closed_port: int, sentinel: str, kind: str
) -> None:
    origin = f"http://127.0.0.1:{closed_port}"
    await _fails_cleanly(caplog, kind, origin, f"tok-{sentinel}", sentinel)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["gitlab", "bitbucket_cloud", "bitbucket_dc"])
async def test_an_illegal_header_value_is_unavailable_with_no_httpx_context(
    caplog: pytest.LogCaptureFixture, server: Recorder, sentinel: str, kind: str
) -> None:
    """httpx's own LocalProtocolError quotes the header value; a raw token
    handed straight to the transport must still not reach the error."""

    await _fails_cleanly(caplog, kind, server.origin, f"tok-{sentinel}\n", sentinel)
    assert server.requests == []
