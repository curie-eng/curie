"""The shared caller proxy, driven from a test (ADR 0239, cp4).

``SharedProxy`` is one complete shared application: registered through the
real API with ``run_tools`` and ``gated_tools`` in its policy, its real
resolver launched by ``Fleet.launch`` (optionally on a ``HandleRegistry`` with
custom bounds), the fixture MCP server (``_fixture_mcp_server``) as its
upstream, and ``build_shared_app`` (the entry point's own wiring) served on the
resolver harness's own event loop with ``web.AppRunner(app,
handler_cancellation=True)`` on ``127.0.0.1``. The proxy's configuration comes
from ``SharedProxyConfig.from_env`` over the frozen render's names, with the
grant store file holding a real Valkey URL. The caller verify key is never in
that environment (cp5): it is published into the database by the API's own
``caller_keys.publish`` (``publish_caller_keys``) before the proxy is built,
and the proxy reads it through the application role.

Members are real: an agent created through the API, its alias rows written by
the membership API, and a platform-custody static binding imported through the
API. Caller tokens are ``cct2`` minted with ``curie_worker.caller_token.mint``
(agent_id the member's, installation the application's) under a signing key
generated per application; shared grants are ``csg`` minted with
``connector_grant.mint_shared`` under the same key. Nothing is minted into a
file.

Requests go over real TCP with ``http.client`` (so headers can repeat), raw
sockets for a client that hangs up mid-call, and a held ``GET`` for SSE. Every
observation is taken where it happens: ``fixture.requests`` for what reached
the server, ``provider.requests`` for what the provider saw, the resolver's
``admissions`` counter, and the registry only where the plan names it.

Readings this module settles so every suite uses one value:

1. A refusal's status and ``curie_caller`` come from the frozen table in
   ``tests/vectors/connector-shared-caller.json``; the body's message is the
   dedicated proxy's for that ``curie_caller`` (``server.refusal_body``).
2. The token's ``agent`` display name is ``agent-<first 8 of agent_id>``, the
   name ``ResolverHarness.principal`` uses; it never authorizes.
3. ``initialize`` is followed by ``notifications/initialized`` on the new
   session, as an MCP client does.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import dataclasses
import datetime as dt
import functools
import gzip
import http.client
import itertools
import json
import os
import re
import socket
import time
import uuid
import zlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from _fake_provider import FakeProvider, authorization_digest
from _fixture_mcp_server import MCP_PATH, PROTOCOL_VERSION, TOOLS, FixtureMCPServer, Hold
from _migration_support import sql_dicts
from _resolver_harness import STATIC_FIELD, Fleet, Member, ResolverHarness, SharedApp, envelope
from _shared_connector_support import PROXY_IMAGE, SLOT, in_background, source_metadata
from aiohttp import web
from curie_api.config import get_settings
from curie_connector_proxy import server
from curie_connector_proxy.resolver.handles import (
    MAX_INVOCATIONS,
    MAX_SESSIONS,
    HandleRegistry,
    InvocationContext,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from nacl.signing import SigningKey
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

REPO = Path(__file__).resolve().parents[4]
SHARED_VECTOR_FILE = REPO / "tests" / "vectors" / "connector-shared-caller.json"
CALLER_TOKEN_VECTOR_FILE = REPO / "tests" / "vectors" / "connector-caller-token.json"

# The wire names, spelled out so the suites do not need the production module
# to build a request; test_shared_proxy_admission pins them to shared.py's.
CALLER_HEADER = "X-Curie-Caller"
ALIAS_HEADER = "X-Curie-Connector-Alias"
SESSION_HEADER = "Mcp-Session-Id"
INVOCATION_HEADER = "X-Curie-Invocation"
GRANT_HEADER = "X-Curie-Connector-Grant"
GRANT_KEY_PREFIX = "connector-shared-grant:"

INSTALLATION_ENV = "CURIE_SHARED_INSTALLATION"
APPLICATION_ENV = "CURIE_SHARED_APPLICATION"
PROXY_PORT_ENV = "CURIE_SHARED_PROXY_PORT"
UPSTREAM_URL_ENV = "CURIE_SHARED_UPSTREAM_URL"
GRANT_STORE_FILE_ENV = "CURIE_SHARED_GRANT_STORE_FILE"
PUBLIC_KEYS_ENV = "CURIE_CALLER_PROXY_PUBLIC_KEYS"
# cp5: the dedicated verify-key variable is dedicated-only too; a shared proxy
# reads its keys from the database.
DEDICATED_ONLY_ENV = (
    PUBLIC_KEYS_ENV,
    "CURIE_CALLER_PROXY_ADMITS",
    "CURIE_CALLER_PROXY_GATED_TOOLS",
    "CURIE_CALLER_PROXY_CONNECTOR",
    "CURIE_CALLER_PROXY_GRANT_STORE",
    "CURIE_CALLER_PROXY_PORT",
    "CURIE_CALLER_PROXY_UPSTREAM_PORT",
    "CURIE_CALLER_PROXY_UPSTREAM_HOST",
)
LISTEN_PORT = "8480"

HANDLE_SHAPE = re.compile(r"^crh1\.[A-Za-z0-9_-]{43}$")
# Bounds every wait a request makes, so a hang fails instead of stalling.
PROXY_WAIT_S = 90.0
# How long a cleanup the proxy runs after answering may take to show.
CLEANUP_WAIT_S = 10.0
DEFAULT_EXPIRES_IN = 900


# --- the frozen vector --------------------------------------------------------


@functools.cache
def shared_vector() -> dict[str, Any]:
    document: dict[str, Any] = json.loads(SHARED_VECTOR_FILE.read_text(encoding="utf-8"))
    return document


def refusal_table() -> dict[str, tuple[int, str]]:
    """code -> (status, curie_caller), from the frozen vector."""

    return {
        str(row["code"]): (int(row["status"]), str(row["curie_caller"]))
        for row in shared_vector()["refusals"]
    }


def expected_refusal_body(code: str) -> dict[str, Any]:
    """The refusal body, built from the vector and the dedicated proxy's message."""

    _, curie_caller = refusal_table()[code]
    dedicated = server.refusal_body(curie_caller)
    error = dedicated["error"]
    assert isinstance(error, dict)
    return {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": shared_vector()["jsonrpc_error_code"],
            "message": error["message"],
            "data": {"curie_caller": curie_caller, "curie_shared": code},
        },
    }


# --- small pieces ---------------------------------------------------------------


def valkey_url() -> str:
    """The test Valkey as a grant store URL."""

    auth = f":{quote(VALKEY_PW, safe='')}@" if VALKEY_PW else ""
    return f"redis://{auth}{VALKEY_HOST}:{VALKEY_PORT}/0"


def signing_pair() -> tuple[str, str]:
    """A fresh caller signing seed and its verify key, both standard base64."""

    signing = SigningKey.generate()
    return (
        base64.b64encode(bytes(signing)).decode("ascii"),
        base64.b64encode(bytes(signing.verify_key)).decode("ascii"),
    )


def publish_caller_keys(installation: str, current: str = "", previous: str = "") -> str:
    """One tick of the API's live publisher for ``installation`` (cp5, D1).

    Runs the production ``caller_keys.publish`` against the test database
    with the API's settings narrowed to this installation and verify keys,
    exactly as an API process configured with ``CURIE_CONNECTOR_CALLER_PUBLIC_KEY``
    (and the previous key) publishes on each tick. A blank ``current`` is a
    process with no verify key configured: it heartbeats and publishes no key.

    A second API app is deliberately not started for this: its own 60 s
    publisher would keep ticking for the rest of the test, and the
    ``monkeypatch`` it needs would change the env the fixture's app reads.
    """

    from curie_api.shared_connectors import caller_keys

    settings = get_settings().model_copy(
        update={
            "installation_id": installation,
            "installation_id_observed": True,
            "connector_caller_public_key": current,
            "connector_caller_previous_public_key": previous,
            "connector_proxy_image": PROXY_IMAGE,
        }
    )

    async def run() -> str:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                outcome = await caller_keys.publish(session, settings)
                await session.commit()
                return str(outcome)
        finally:
            await engine.dispose()

    outcome = asyncio.run(run())
    assert outcome == "published", outcome
    return outcome


def closed_port() -> int:
    """A loopback port nothing listens on."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


def process_env(env: Mapping[str, str]) -> dict[str, str]:
    """``env`` over this process's own, minus every inherited proxy variable."""

    inherited = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("CURIE_SHARED_", "CURIE_CALLER_PROXY_", "CURIE_CONNECTOR_CALLER_"))
    }
    return {**inherited, **env}


def refuses_connection(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return False
    except OSError:
        return True


def agent_name(agent_id: str) -> str:
    return f"agent-{agent_id[:8]}"


def wait_until(check: Callable[[], bool], what: str, timeout: float = PROXY_WAIT_S) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.05)


class MutableClock:
    """The proxy's clock: real time plus an offset the test advances."""

    def __init__(self) -> None:
        self.offset = 0.0

    def __call__(self) -> float:
        return time.time() + self.offset

    def advance(self, seconds: float) -> None:
        self.offset += seconds


class _Auto:
    """The default request id: the next one from the proxy's counter."""


AUTO = _Auto()


@dataclass(frozen=True)
class Observation:
    """What a refusal must leave unchanged."""

    upstream: int
    admissions: int
    provider: int


@dataclass(frozen=True)
class ProxyReply:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def values(self, name: str) -> list[str]:
        wanted = name.lower()
        return [value for key, value in self.headers if key.lower() == wanted]

    def header(self, name: str) -> str | None:
        found = self.values(name)
        return found[0] if found else None

    @property
    def json(self) -> Any:
        return json.loads(self.body)

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")

    @property
    def session_id(self) -> str | None:
        return self.header(SESSION_HEADER)

    @property
    def message(self) -> dict[str, Any]:
        """The one JSON-RPC response object of an admitted, answered request."""

        assert self.status == 200, (self.status, self.text)
        document = self.json
        assert isinstance(document, dict), document
        assert "error" not in document, document
        return document

    @property
    def result(self) -> dict[str, Any]:
        """The tool's structured result; a resolver refusal shows as ``refused``."""

        content: dict[str, Any] = self.message["result"]["structuredContent"]
        return content


class EventStream:
    """One held exchange through the proxy: a ``GET /mcp``, or a POST answered as SSE."""

    def __init__(
        self,
        port: int,
        headers: Sequence[tuple[str, str]],
        *,
        method: str = "GET",
        body: bytes | None = None,
    ) -> None:
        self._connection = http.client.HTTPConnection("127.0.0.1", port, timeout=PROXY_WAIT_S)
        self._connection.putrequest(method, MCP_PATH, skip_accept_encoding=True)
        for name, value in headers:
            self._connection.putheader(name, value)
        if body is not None:
            self._connection.putheader("Content-Length", str(len(body)))
        self._connection.endheaders(body)
        self.response = self._connection.getresponse()
        self.status = self.response.status
        self.headers = tuple(self.response.getheaders())

    def read_line(self) -> bytes:
        return self.response.readline()

    def header(self, name: str) -> str | None:
        wanted = name.lower()
        return next((value for key, value in self.headers if key.lower() == wanted), None)

    def read_rest(self) -> bytes:
        """Everything left until the proxy ends the stream."""

        return self.response.read()

    def reply(self) -> ProxyReply:
        """The whole answer of a refused stream."""

        return ProxyReply(self.status, self.headers, self.response.read())

    def close(self) -> None:
        self._connection.close()


# --- registration -----------------------------------------------------------------


def register_application(
    fleet: Fleet,
    provider: FakeProvider,
    *,
    tools: Sequence[str] = TOOLS,
    run_tools: Sequence[str] = (),
    gated_tools: Sequence[str] = (),
) -> SharedApp:
    """A platform-custody application whose policy carries run and gated tools."""

    store = fleet.store
    application_id = str(uuid.uuid4())
    body = store.registration_body(application_id, slots=(SLOT,), tools=tuple(tools))
    body["application"]["provider_origins"] = [provider.origin]
    if run_tools:
        body["policy"]["run_tools"] = list(run_tools)
    if gated_tools:
        body["policy"]["gated_tools"] = list(gated_tools)
    registered = store.operator("POST", "/applications", body)
    assert registered.status_code == 201, registered.text
    policy = registered.json()["policy"]
    assert policy["tools"] == list(tools), policy
    assert policy["run_tools"] == list(run_tools), policy
    assert policy["gated_tools"] == list(gated_tools), policy
    recipient = store.register_recipient(application_id)
    database, dsn = store.provision_bundled(application_id, recipient)
    return SharedApp(
        store=store,
        application_id=application_id,
        installation=store.installation,
        recipient=recipient,
        slots=(SLOT,),
        custody="platform",
        origins=(provider.origin,),
        database=database,
        dsn=dsn,
    )


def membership_row(application_id: str, agent_id: str, alias: str) -> dict[str, Any] | None:
    """The alias row as the owner reads it, or None when it does not exist."""

    rows = sql_dicts(
        "SELECT alias, tools, created_at, updated_at FROM curie.shared_connector_memberships "
        "WHERE application_id = :application AND agent_id = :agent AND alias = :alias",
        {"application": uuid.UUID(application_id), "agent": uuid.UUID(agent_id), "alias": alias},
    )
    assert len(rows) <= 1, rows
    return rows[0] if rows else None


# --- one shared application behind its proxy ----------------------------------------


class SharedProxy:
    """One shared application: API rows, resolver, fixture server and the proxy."""

    def __init__(
        self,
        fleet: Fleet,
        provider: FakeProvider,
        *,
        tools: Sequence[str] = TOOLS,
        run_tools: Sequence[str] = (),
        gated_tools: Sequence[str] = (),
        max_invocations: int | None = None,
        max_sessions: int | None = None,
        session_idle_seconds: float | None = None,
        grant_store_url: str | None = None,
    ) -> None:
        self.fleet = fleet
        self.provider = provider
        self.store = fleet.store
        self.app = register_application(
            fleet, provider, tools=tools, run_tools=run_tools, gated_tools=gated_tools
        )
        handles = (
            None
            if max_invocations is None and max_sessions is None
            else HandleRegistry(
                installation=self.app.installation,
                application=self.app.application_id,
                max_invocations=MAX_INVOCATIONS if max_invocations is None else max_invocations,
                max_sessions=MAX_SESSIONS if max_sessions is None else max_sessions,
            )
        )
        self.resolver: ResolverHarness = fleet.launch(self.app, handles=handles)
        self.registry: HandleRegistry = self.resolver.handles
        self.clock = MutableClock()
        self.seed, self.public_key = signing_pair()
        self.grant_store_url = valkey_url() if grant_store_url is None else grant_store_url
        self._ids = itertools.count(1000)
        self._runner: web.AppRunner | None = None
        self._closed = False
        self.fixture = FixtureMCPServer(self.resolver.socket_path, provider, slot=SLOT)
        try:
            store_file = self.resolver.directory / "grant-store"
            store_file.write_text(self.grant_store_url + "\n")
            store_file.chmod(0o600)
            self.env: dict[str, str] = {
                **self.resolver.env,
                PROXY_PORT_ENV: LISTEN_PORT,
                UPSTREAM_URL_ENV: self.fixture.url,
                GRANT_STORE_FILE_ENV: str(store_file),
            }
            from curie_connector_proxy import shared

            config = shared.SharedProxyConfig.from_env(self.env)
            # The verify key reaches the proxy only through the database, as
            # one API process configured with it publishes it.
            publish_caller_keys(self.app.installation, self.public_key)
            if session_idle_seconds is not None:
                config = dataclasses.replace(config, session_idle_seconds=session_idle_seconds)
            self.config = config
            self.port: int = self.resolver._wait(self._serve(), PROXY_WAIT_S)
        except BaseException:
            self.fixture.close()
            raise

    async def _serve(self) -> int:
        from curie_connector_proxy import shared

        app = await shared.build_shared_app(self.config, self.resolver.runtime, clock=self.clock)
        runner = web.AppRunner(app, handler_cancellation=True, shutdown_timeout=5.0)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self._runner = runner
        addresses = runner.addresses
        assert addresses, "the shared proxy bound no address"
        return int(addresses[0][1])

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.fixture.release_all()
        try:
            if self._runner is not None:
                self.resolver._wait(self._runner.cleanup(), PROXY_WAIT_S)
        finally:
            self.fixture.close()

    # --- members ----------------------------------------------------------------

    def member(
        self, *, alias: str = "primary", aliases: Sequence[Mapping[str, Any]] | None = None
    ) -> Member:
        """A member with its own static credential and account; optionally more aliases."""

        member = self.app.add_member(alias=alias)
        if aliases is not None:
            self.set_aliases(member, aliases)
        return member

    def outsider(self) -> str:
        """A real agent with no membership and no binding: its agent_id."""

        return self.store.create_agent()

    def member_without_binding(self, alias: str = "primary") -> str:
        """A real agent with an alias row and no binding: its agent_id."""

        agent_id = self.store.create_agent()
        joined = self.store.put_members(
            self.app.application_id, agent_id, [{"alias": alias, "tools": None}]
        )
        assert joined.status_code == 200, joined.text
        return agent_id

    def set_aliases(self, member: Member | str, aliases: Sequence[Mapping[str, Any]]) -> None:
        agent_id = member if isinstance(member, str) else member.agent_id
        response = self.store.put_members(
            self.app.application_id, agent_id, [dict(alias) for alias in aliases]
        )
        assert response.status_code == 200, response.text

    def remove(self, member: Member) -> None:
        response = self.store.operator(
            "DELETE", f"/applications/{self.app.application_id}/members/{member.agent_id}"
        )
        assert response.status_code == 200, response.text

    def disable_binding(self, member: Member) -> None:
        response = self.store.disable_binding(member.binding_id)
        assert response.status_code == 200, response.text

    def disable_application(self) -> None:
        response = self.store.disable_application(self.app.application_id)
        assert response.status_code == 200, response.text

    def replace_account(self, member: Member) -> tuple[str, str]:
        """Account replacement through the API: disable, then import a new binding.

        Updates ``member`` to the new binding; answers (old binding id, old text).
        """

        old_binding, old_text = member.binding_id, member.text
        self.disable_binding(member)
        text = f"tok-replaced-{uuid.uuid4().hex}"
        material = envelope({SLOT: {STATIC_FIELD: text}})
        source = source_metadata(uid=f"uid-{uuid.uuid4().hex[:12]}")
        account = f"acct-{uuid.uuid4().hex[:12]}"
        binding = self.store.import_binding(
            self.app.application_id,
            member.agent_id,
            self.app.recipient,
            material,
            provider_account=account,
            source=source,
        )
        member.binding = binding
        member.account = account
        member.fields = {STATIC_FIELD: text}
        member.plaintext = material
        member.source = source
        member.revision = 1
        return old_binding, old_text

    def generation(self) -> int:
        return self.app.generation()

    def stamp(self, member: Member, alias: str | None = None) -> str:
        """The membership stamp of the member's alias row as stored now.

        Timestamps are read in UTC, as asyncpg (the proxy's driver) answers them.
        """

        from curie_connector_proxy.resolver.admission import membership_stamp

        row = membership_row(self.app.application_id, member.agent_id, alias or member.alias)
        assert row is not None, "the alias row does not exist"
        tools = row["tools"]
        if isinstance(tools, str):
            tools = json.loads(tools)
        return membership_stamp(
            row["alias"],
            tools,
            row["created_at"].astimezone(dt.UTC),
            row["updated_at"].astimezone(dt.UTC),
        )

    def digest(self, member: Member) -> str:
        """The Authorization digest the provider records for the member's credential."""

        return authorization_digest(f"Bearer {member.text}")

    # --- tokens and grants ---------------------------------------------------------

    def token(
        self,
        member: Member | None = None,
        *,
        agent_id: str | None = None,
        agent: str | None = None,
        installation: str | None = None,
        exp: int | None = None,
        expires_in: int = DEFAULT_EXPIRES_IN,
        run: str | None = None,
        work_item: str | None = None,
    ) -> str:
        """A ``cct2`` caller token signed by this application's proxy key."""

        from curie_worker.caller_token import mint

        chosen = agent_id if agent_id is not None else member.agent_id if member else None
        assert chosen is not None, "a token names an agent"
        return mint(
            self.seed,
            installation=self.app.installation if installation is None else installation,
            agent_id=chosen,
            agent=agent_name(chosen) if agent is None else agent,
            exp=int(time.time()) + expires_in if exp is None else exp,
            run=run,
            work_item=work_item,
        )

    def grant(
        self,
        member: Member,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        alias: str | None = None,
        application: str | None = None,
        binding: str | None = None,
        policy_generation: int | None = None,
        exp: int | None = None,
        expires_in: int = 300,
        jti: str | None = None,
    ) -> tuple[str, str]:
        """A ``csg`` shared grant and its jti; every claim defaults to the matching value."""

        from curie_worker.connector_grant import (
            arguments_sha256,
            canonical_arguments,
            mint_shared,
        )

        chosen_jti = jti or f"jti-{uuid.uuid4().hex}"
        token = mint_shared(
            self.seed,
            application=self.app.application_id if application is None else application,
            binding=member.binding_id if binding is None else binding,
            alias=member.alias if alias is None else alias,
            tool=tool,
            args_sha256=arguments_sha256(canonical_arguments(dict(arguments))),
            policy_generation=self.generation() if policy_generation is None else policy_generation,
            exp=int(time.time()) + expires_in if exp is None else exp,
            jti=chosen_jti,
        )
        return token, chosen_jti

    def dedicated_grant(self, member: Member, tool: str, arguments: Mapping[str, Any]) -> str:
        """A dedicated ``ccg`` grant for the same call, under the same key."""

        from curie_worker.connector_grant import canonical_arguments, mint

        return mint(
            self.seed,
            agent=agent_name(member.agent_id),
            connector=member.alias,
            tool=tool,
            args=canonical_arguments(dict(arguments)),
            exp=int(time.time()) + 300,
            jti=f"jti-{uuid.uuid4().hex}",
        )

    # --- requests -------------------------------------------------------------------

    def headers(
        self,
        member: Member | None,
        *,
        tokens: Sequence[str] | None = None,
        aliases: Sequence[str] | None = None,
        alias: str | None = None,
        session: str | None = None,
        grants: Sequence[str] = (),
        extra: Sequence[tuple[str, str]] = (),
        token_options: Mapping[str, Any] | None = None,
    ) -> list[tuple[str, str]]:
        """Request headers: the caller token, the alias, a session, grants, extras."""

        pairs = [
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
            ("MCP-Protocol-Version", PROTOCOL_VERSION),
        ]
        if tokens is None:
            tokens = [] if member is None else [self.token(member, **dict(token_options or {}))]
        pairs.extend((CALLER_HEADER, token) for token in tokens)
        if aliases is None:
            chosen = alias if alias is not None else member.alias if member is not None else None
            aliases = [] if chosen is None else [chosen]
        pairs.extend((ALIAS_HEADER, value) for value in aliases)
        if session is not None:
            pairs.append((SESSION_HEADER, session))
        pairs.extend((GRANT_HEADER, grant) for grant in grants)
        pairs.extend(extra)
        return pairs

    def send(
        self,
        method: str,
        *,
        body: bytes | None = None,
        headers: Sequence[tuple[str, str]] = (),
        path: str = MCP_PATH,
    ) -> ProxyReply:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=PROXY_WAIT_S)
        try:
            connection.putrequest(method, path, skip_accept_encoding=True)
            for name, value in headers:
                connection.putheader(name, value)
            if body is not None:
                connection.putheader("Content-Length", str(len(body)))
            connection.endheaders(body)
            response = connection.getresponse()
            payload = response.read()
            return ProxyReply(response.status, tuple(response.getheaders()), payload)
        finally:
            connection.close()

    def next_id(self) -> int:
        return next(self._ids)

    def message(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        request_id: int | str | _Auto = AUTO,
        notification: bool = False,
    ) -> dict[str, Any]:
        document: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if not notification:
            document["id"] = self.next_id() if isinstance(request_id, _Auto) else request_id
        if params is not None:
            document["params"] = dict(params)
        return document

    def post(self, member: Member | None, payload: object, **options: Any) -> ProxyReply:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return self.send("POST", body=body, headers=self.headers(member, **options))

    def initialize(self, member: Member, **options: Any) -> str:
        """Open a session as an MCP client does; answer its ``Mcp-Session-Id``."""

        params = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "curie-test", "version": "1"},
        }
        reply = self.post(member, self.message("initialize", params), **options)
        assert reply.status == 200, (reply.status, reply.text)
        session = reply.session_id
        assert session, reply.headers
        initialized = self.post(
            member,
            self.message("notifications/initialized", notification=True),
            session=session,
            **options,
        )
        assert initialized.status == 202, (initialized.status, initialized.text)
        return session

    def call_message(
        self,
        tool: str,
        arguments: Mapping[str, Any] | None = None,
        *,
        request_id: int | str | _Auto = AUTO,
        notification: bool = False,
    ) -> dict[str, Any]:
        return self.message(
            "tools/call",
            {"name": tool, "arguments": dict(arguments or {})},
            request_id=request_id,
            notification=notification,
        )

    def call(
        self,
        member: Member | None,
        tool: str,
        arguments: Mapping[str, Any] | None = None,
        *,
        request_id: int | str | _Auto = AUTO,
        notification: bool = False,
        **options: Any,
    ) -> ProxyReply:
        message = self.call_message(
            tool, arguments, request_id=request_id, notification=notification
        )
        return self.post(member, message, **options)

    def call_in_background(
        self,
        member: Member,
        tool: str,
        arguments: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> concurrent.futures.Future[ProxyReply]:
        """The same request as ``call``, built now and sent on a daemon thread."""

        message = self.call_message(tool, arguments, **_pop(options, "request_id", "notification"))
        headers = self.headers(member, **options)
        body = json.dumps(message, separators=(",", ":")).encode("utf-8")
        return in_background(lambda: self.send("POST", body=body, headers=headers))

    def start_hold(
        self,
        member: Member,
        key: str,
        *,
        arguments: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> tuple[Hold, concurrent.futures.Future[ProxyReply]]:
        """Start a ``hold`` call and return once the server has parked it.

        A call the proxy refused never parks; that fails here with its reply.
        """

        hold = self.fixture.hold(key)
        future = self.call_in_background(
            member, "hold", {"key": key, **dict(arguments or {})}, **options
        )
        wait_until(lambda: hold.entered.is_set() or future.done(), f"hold {key!r} to park")
        if not hold.entered.is_set():
            reply = future.result(PROXY_WAIT_S)
            raise AssertionError(f"hold {key!r} never parked: {reply.status} {reply.text}")
        return hold, future

    @staticmethod
    def finish_hold(hold: Hold, future: concurrent.futures.Future[ProxyReply]) -> ProxyReply:
        """Release a parked ``hold`` and answer the proxy's reply to it."""

        hold.release()
        return future.result(PROXY_WAIT_S)

    def cancel(
        self, member: Member, request_id: int | str, reason: str = "user cancelled", **options: Any
    ) -> ProxyReply:
        message = self.message(
            "notifications/cancelled",
            {"requestId": request_id, "reason": reason},
            notification=True,
        )
        return self.post(member, message, **options)

    def delete(self, member: Member, session: str, **options: Any) -> ProxyReply:
        return self.send("DELETE", headers=self.headers(member, session=session, **options))

    def open_stream(self, member: Member, session: str | None, **options: Any) -> EventStream:
        return EventStream(self.port, self.headers(member, session=session, **options))

    def open_request_stream(self, member: Member, payload: object, **options: Any) -> EventStream:
        """A POST whose answer is streamed back as it arrives (a streaming initialize)."""

        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return EventStream(self.port, self.headers(member, **options), method="POST", body=body)

    def post_encoded(
        self, member: Member | None, payload: object, encoding: str, **options: Any
    ) -> ProxyReply:
        """POST ``payload`` with ``Content-Encoding: <encoding>``, body encoded to match.

        Each comma-separated coding is applied in order: ``gzip`` and
        ``deflate`` really compress, ``identity`` leaves the bytes, and any
        other token (``br``, ``x-made-up``) labels the JSON bytes unchanged.
        """

        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        for coding in (part.strip().lower() for part in encoding.split(",")):
            if coding == "gzip":
                body = gzip.compress(body)
            elif coding == "deflate":
                body = zlib.compress(body)
        extra = [*options.pop("extra", ()), ("Content-Encoding", encoding)]
        return self.send("POST", body=body, headers=self.headers(member, extra=extra, **options))

    def raw_post(self, body: bytes, headers: Sequence[tuple[str, str]]) -> socket.socket:
        """Send one POST on a raw socket and answer it unread; closing it hangs up."""

        sock = socket.create_connection(("127.0.0.1", self.port), timeout=PROXY_WAIT_S)
        lines = [
            f"POST {MCP_PATH} HTTP/1.1",
            f"Host: 127.0.0.1:{self.port}",
            *(f"{name}: {value}" for name, value in headers),
            f"Content-Length: {len(body)}",
            "",
            "",
        ]
        sock.sendall("\r\n".join(lines).encode("latin-1") + body)
        return sock

    # --- observations -----------------------------------------------------------------

    def observe(self) -> Observation:
        return Observation(
            upstream=self.fixture.count,
            admissions=self.resolver.admissions,
            provider=len(self.provider.requests),
        )

    def assert_refused(self, reply: ProxyReply, code: str, before: Observation) -> None:
        """The exact shared refusal, and nothing reached the server, resolver or provider."""

        from curie_connector_proxy import shared

        status, _ = refusal_table()[code]
        assert reply.status == status, (code, reply.status, reply.text)
        assert (reply.header("Content-Type") or "").startswith("application/json"), reply.headers
        assert reply.header("WWW-Authenticate") is None, reply.headers
        assert reply.json == expected_refusal_body(code), (code, reply.text)
        assert reply.json == shared.refusal_body(code), (code, reply.text)
        assert self.observe() == before, (code, before, self.observe())

    def context(self, handle: str) -> InvocationContext:
        """The registry's context for a live handle (only where the plan reads it)."""

        return self.registry.admit(handle)

    def wait_for_invocations(self, count: int, timeout: float = CLEANUP_WAIT_S) -> None:
        """The proxy's ``finally`` may still be running when the client has its reply."""

        wait_until(
            lambda: self.registry.active_invocations == count,
            f"{count} active invocation(s)",
            timeout,
        )

    def wait_for_sessions(self, count: int, timeout: float = CLEANUP_WAIT_S) -> None:
        wait_until(lambda: self.registry.sessions == count, f"{count} open session(s)", timeout)


def _pop(options: dict[str, Any], *names: str) -> dict[str, Any]:
    return {name: options.pop(name) for name in names if name in options}


class Worlds:
    """Builds ``SharedProxy`` worlds for one test and closes them before the fleet."""

    def __init__(self, fleet: Fleet, provider: FakeProvider) -> None:
        self.fleet = fleet
        self.provider = provider
        self._built: list[SharedProxy] = []

    def __call__(self, **options: Any) -> SharedProxy:
        world = SharedProxy(self.fleet, self.provider, **options)
        self._built.append(world)
        return world

    def close(self) -> None:
        for world in reversed(self._built):
            world.close()
