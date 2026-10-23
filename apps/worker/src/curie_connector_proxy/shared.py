"""The caller proxy's shared application mode (ADR 0239).

One shared connector server serves several agents, so who a call is for is
decided here on every request and reaches the server only as an invocation
handle the resolver in this same process admits ("Bundle, identity and request
contract" 3 and 6). ``server.shared_mode`` selects this mode by the presence of
``CURIE_SHARED_APPLICATION``; the dedicated admits list, ``caller.decide`` and
the ``ccg`` grant are never consulted here, and a Pod rendered with any
dedicated-only name refuses to start.

Every request, any method or path, is checked in this order, and a refusal
answers before anything reaches the server:

1. Exactly one ``X-Curie-Caller`` that ``caller.decide_shared`` admits for the
   configured installation.
2. Exactly one ``X-Curie-Connector-Alias`` of alias shape. It only selects among
   the verified agent's own memberships.
3. A presented ``Mcp-Session-Id`` must be one this proxy recorded, for the same
   installation, application, agent and alias. Ownership is proved here,
   before any refusal below may close the session.
4. The body, at most 1 MiB and not content-encoded, since it is relayed undecoded.
5. One read-only REPEATABLE READ transaction through the application's own
   role (the resolver's pool) reads the application, the agent's alias row and
   its binding. Nothing is cached; any database failure is ``store_unavailable``.
   ``not_member`` on an owned session closes it, and so does a changed
   membership stamp (``policy_generation_mismatch``).
6. At most one ``tools/call``, whose tool the application policy and the alias
   both allow, with the token's own run pair when the tool is a run tool, and a
   matching ``csg`` grant when it is gated.
7. A session-less request reserves a session slot, a ``tools/call`` reserves its
   cancellation key and an invocation slot, a gated call spends its grant in
   Valkey (so no capacity refusal can follow a spend), and only then is the
   handle registered, immediately before the upstream request. Closing the
   invocation in ``finally`` removes the handle on completion, on an upstream
   error and on cancellation alike.

A session survives a policy generation change only while the principal's own
alias row is unchanged: the session row keeps that row's membership stamp, and
handles on the session carry the session's principal with this call's stamp,
which the resolver checks the same way. Only a forwarded ``tools/call`` POST
carries ``X-Curie-Invocation``; every inbound ``x-curie-*`` header, the
credential headers and the hop-by-hop headers are stripped first.

The log line is the dedicated one's shape. It names the agent, the outcome
code, the method, the quoted path and, for a refused grant, a sub-reason from a
fixed set. It never carries a token, handle, grant, credential or argument.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse

from aiohttp import ClientError, ClientSession, web
from aiohttp.typedefs import Handler
from multidict import CIMultiDict, CIMultiDictProxy
from nacl.signing import VerifyKey
from yarl import URL

from . import caller, server
from .canonical import arguments_sha256
from .resolver.admission import (
    ACQUIRE_TIMEOUT_S,
    MEMBERSHIP_SQL,
    STORE_ERRORS,
    jsonb,
    membership_stamp,
)
from .resolver.handles import (
    REGISTRY_FOREIGN,
    HandleRegistry,
    Invocation,
    InvocationContext,
    Principal,
    RegistryRefusal,
    uuid_ok,
)
from .resolver.http import ResolverRuntime
from .resolver.wire import CodedError

ALIAS_HEADER = "X-Curie-Connector-Alias"
INVOCATION_HEADER = "X-Curie-Invocation"
SESSION_HEADER = "Mcp-Session-Id"
GRANT_KEY_PREFIX = "connector-shared-grant:"

# The frozen shared render's names
# (plugin_format.shared_connector_render.render_application).
SHARED_APPLICATION_ENV = "CURIE_SHARED_APPLICATION"
INSTALLATION_ENV = "CURIE_SHARED_INSTALLATION"
PROXY_PORT_ENV = "CURIE_SHARED_PROXY_PORT"
UPSTREAM_URL_ENV = "CURIE_SHARED_UPSTREAM_URL"
GRANT_STORE_FILE_ENV = "CURIE_SHARED_GRANT_STORE_FILE"
# Present only in a mis-rendered Pod: the admits list and the dedicated gated
# tools can never authorize a shared call, because shared mode never starts
# beside them.
DEDICATED_ONLY_ENV: tuple[str, ...] = (
    server.ADMITS_ENV,
    server.GATED_TOOLS_ENV,
    server.CONNECTOR_ENV,
    server.GRANT_STORE_ENV,
    server.LISTEN_PORT_ENV,
    server.UPSTREAM_PORT_ENV,
    server.UPSTREAM_HOST_ENV,
)
DEFAULT_SESSION_IDLE_SECONDS = 3600.0

MISSING = caller.MISSING
INVALID = caller.INVALID
EXPIRED = caller.EXPIRED
NOT_ADMITTED = caller.NOT_ADMITTED
ALIAS_REQUIRED = "alias_required"
APPLICATION_UNKNOWN = "application_unknown"
INSTALLATION_MISMATCH = "installation_mismatch"
APPLICATION_DISABLED = "application_disabled"
NOT_MEMBER = "not_member"
BINDING_MISSING = "binding_missing"
BINDING_DISABLED = "binding_disabled"
TOOL_NOT_ALLOWED = "tool_not_allowed"
MULTIPLE_TOOL_CALLS = "multiple_tool_calls"
RUN_REQUIRED = "run_required"
SESSION_UNKNOWN = "session_unknown"
SESSION_PRINCIPAL_MISMATCH = "session_principal_mismatch"
POLICY_GENERATION_MISMATCH = "policy_generation_mismatch"
SESSIONS_FULL = "sessions_full"
INVOCATIONS_FULL = "invocations_full"
REQUEST_ID_IN_USE = "request_id_in_use"
BODY_TOO_LARGE = "body_too_large"
BODY_ENCODING_UNSUPPORTED = "body_encoding_unsupported"
GRANT_REQUIRED = caller.GRANT_REQUIRED
STORE_UNAVAILABLE = "store_unavailable"

# code -> (HTTP status, curie_caller), frozen in tests/vectors/connector-shared-caller.json.
# curie_caller is always one of caller.REFUSALS, so a runner that reads only it
# still classifies every shared refusal.
REFUSALS: Mapping[str, tuple[int, str]] = MappingProxyType(
    {
        MISSING: (403, MISSING),
        INVALID: (403, INVALID),
        EXPIRED: (403, EXPIRED),
        NOT_ADMITTED: (403, NOT_ADMITTED),
        ALIAS_REQUIRED: (403, NOT_ADMITTED),
        APPLICATION_UNKNOWN: (403, NOT_ADMITTED),
        INSTALLATION_MISMATCH: (403, NOT_ADMITTED),
        APPLICATION_DISABLED: (403, NOT_ADMITTED),
        NOT_MEMBER: (403, NOT_ADMITTED),
        BINDING_MISSING: (403, NOT_ADMITTED),
        BINDING_DISABLED: (403, NOT_ADMITTED),
        TOOL_NOT_ALLOWED: (403, NOT_ADMITTED),
        MULTIPLE_TOOL_CALLS: (403, NOT_ADMITTED),
        RUN_REQUIRED: (403, NOT_ADMITTED),
        SESSION_UNKNOWN: (403, NOT_ADMITTED),
        SESSION_PRINCIPAL_MISMATCH: (403, NOT_ADMITTED),
        POLICY_GENERATION_MISMATCH: (403, NOT_ADMITTED),
        SESSIONS_FULL: (403, NOT_ADMITTED),
        INVOCATIONS_FULL: (403, NOT_ADMITTED),
        REQUEST_ID_IN_USE: (403, NOT_ADMITTED),
        BODY_TOO_LARGE: (403, NOT_ADMITTED),
        BODY_ENCODING_UNSUPPORTED: (403, NOT_ADMITTED),
        GRANT_REQUIRED: (403, GRANT_REQUIRED),
        STORE_UNAVAILABLE: (503, NOT_ADMITTED),
    }
)

# Why a gated call was refused, for the proxy's log line only; the client gets
# the one ``grant_required`` body. Each names a check, never a claim value.
GRANT_APPLICATION_MISMATCH = "application_mismatch"
GRANT_BINDING_MISMATCH = "binding_mismatch"
GRANT_ALIAS_MISMATCH = "alias_mismatch"
GRANT_GENERATION_MISMATCH = "generation_mismatch"
# The one warning a failed shared spend logs; it never carries a value.
SPEND_FAILURE = "shared connector grant spend failed"

_UPSTREAM_URL = re.compile(r"^http://127\.0\.0\.1:([1-9][0-9]{0,4})$")
_STORE_SCHEMES = ("redis://", "rediss://")
# Far above a Valkey URL; bounds what a mis-mounted file can cost.
_MAX_STORE_FILE_BYTES = 65536
# Stripped from every forward on top of the dedicated hop-by-hop set, together
# with every ``x-curie-*`` header: the server learns who a call is for only
# through the handle.
_CREDENTIAL_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie"})
_CURIE_PREFIX = "x-curie-"

logger = server.logger


class SharedRefusal(CodedError):
    """A shared-mode refusal. ``str(exc)`` is its code; it never carries a value."""


class _GrantRefused(SharedRefusal):
    """``grant_required``, with the check that refused it for the log line only."""

    def __init__(self, reason: str) -> None:
        super().__init__(GRANT_REQUIRED)
        self.reason = reason


def refusal_body(code: str) -> dict[str, object]:
    """The dedicated JSON-RPC refusal for ``code``'s ``curie_caller``, plus ``curie_shared``."""

    body = server.refusal_body(REFUSALS[code][1])
    error = cast("dict[str, Any]", body["error"])
    error["data"]["curie_shared"] = code
    return body


def refusal_response(code: str) -> web.Response:
    """The refusal answer for ``code``: its status and ``refusal_body``."""

    return web.json_response(refusal_body(code), status=REFUSALS[code][0])


def run_context(
    decision: caller.Decision, *, requires_run: bool
) -> tuple[str | None, str | None]:
    """The token's own ``(run, work_item)``, read from ``decision`` alone.

    Raises ``run_required`` when ``requires_run`` and the token carries no
    pair. Headers, arguments and the session are never a source of either.
    """

    if decision.run is not None and decision.work_item is not None:
        return decision.run, decision.work_item
    if requires_run:
        raise SharedRefusal(RUN_REQUIRED)
    return None, None


# --- configuration -------------------------------------------------------------


def _port(env: Mapping[str, str], name: str) -> int:
    raw = env.get(name, "")
    # ASCII first: ``int()`` also parses non-ASCII decimal digits.
    if not raw.isascii() or not raw.isdigit() or not 0 < int(raw) < 65536:
        raise ValueError(f"{name} must be a TCP port")
    return int(raw)


def _upstream_port(env: Mapping[str, str]) -> int:
    matched = _UPSTREAM_URL.fullmatch(env.get(UPSTREAM_URL_ENV, ""))
    if matched is None or not 0 < int(matched.group(1)) < 65536:
        raise ValueError(
            f"{UPSTREAM_URL_ENV} must be the plain http loopback upstream on 127.0.0.1 "
            "with an explicit port and nothing else"
        )
    return int(matched.group(1))


def _grant_store_url(env: Mapping[str, str]) -> str:
    path = env.get(GRANT_STORE_FILE_ENV, "")
    if not path:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} is required")
    try:
        with open(path, "rb") as handle:
            raw = handle.read(_MAX_STORE_FILE_BYTES + 1)
    except OSError:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} names a file that cannot be read") from None
    if len(raw) > _MAX_STORE_FILE_BYTES:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} names a file that is too large")
    try:
        url = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} names a file that is not UTF-8 text") from None
    if not url:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} names an empty file")
    try:
        host = urlparse(url).hostname
    except ValueError:
        host = None
    if not url.startswith(_STORE_SCHEMES) or any(char.isspace() for char in url) or not host:
        raise ValueError(f"{GRANT_STORE_FILE_ENV} does not hold a redis:// or rediss:// URL")
    return url


@dataclass(frozen=True)
class SharedProxyConfig:
    installation: str
    application: str
    listen_port: int
    upstream_port: int
    public_keys: tuple[VerifyKey, ...]
    # Carries the store's password.
    grant_store_url: str = field(repr=False)
    session_idle_seconds: float = DEFAULT_SESSION_IDLE_SECONDS

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> SharedProxyConfig:
        """Read the shared render's env. Raises ``ValueError`` naming the variable.

        No message carries the refused value.
        """

        for name in DEDICATED_ONLY_ENV:
            if name in env:
                raise ValueError(f"{name} belongs to dedicated mode and must not be present")
        installation = env.get(INSTALLATION_ENV, "")
        if not caller.nonblank_installation_ok(installation):
            raise ValueError(
                f"{INSTALLATION_ENV} must be a nonblank installation ID of at most 128 UTF-8 "
                "bytes with no control or white space character"
            )
        application = env.get(SHARED_APPLICATION_ENV, "")
        if not uuid_ok(application):
            raise ValueError(f"{SHARED_APPLICATION_ENV} must be a canonical lowercase UUID")
        return cls(
            installation=installation,
            application=application,
            listen_port=_port(env, PROXY_PORT_ENV),
            upstream_port=_upstream_port(env),
            public_keys=server._public_keys(env),
            grant_store_url=_grant_store_url(env),
        )


# --- the per-call policy read ----------------------------------------------------

_APPLICATION_SQL = """
SELECT a.installation_id, a.enabled, a.policy_generation, a.policy
FROM curie.shared_connector_applications a
WHERE a.id = $1
"""

_BINDING_SQL = """
SELECT b.id::text AS id, b.enabled
FROM curie.shared_connector_bindings b
WHERE b.application_id = $1 AND b.agent_id = $2
ORDER BY b.enabled DESC, b.created_at DESC
LIMIT 1
"""


@dataclass(frozen=True)
class _Policy:
    """What one call was admitted on, as one snapshot read it."""

    generation: int
    tools: frozenset[str]
    run_tools: frozenset[str]
    gated_tools: frozenset[str]
    # None admits every policy tool.
    alias_tools: frozenset[str] | None
    stamp: str
    binding: str

    def allows(self, tool: str) -> bool:
        return tool in self.tools and (self.alias_tools is None or tool in self.alias_tools)


def _names(value: object) -> frozenset[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("not a JSON list of strings")
    return frozenset(value)


def _evaluate(
    config: SharedProxyConfig, alias: str, application: Any, member: Any, binding: Any
) -> _Policy:
    """The admitted snapshot, or the first refusal in the frozen order."""

    if application is None:
        raise SharedRefusal(APPLICATION_UNKNOWN)
    if application["installation_id"] != config.installation:
        raise SharedRefusal(INSTALLATION_MISMATCH)
    if not application["enabled"]:
        raise SharedRefusal(APPLICATION_DISABLED)
    if member is None:
        raise SharedRefusal(NOT_MEMBER)
    if binding is None:
        raise SharedRefusal(BINDING_MISSING)
    if not binding["enabled"]:
        raise SharedRefusal(BINDING_DISABLED)
    try:
        policy = jsonb(application["policy"])
        if not isinstance(policy, dict):
            raise ValueError("the policy is not a JSON object")
        alias_tools = jsonb(member["tools"])
        generation = application["policy_generation"]
        if type(generation) is not int or generation < 1:
            raise ValueError("the policy generation is not a positive integer")
        return _Policy(
            generation=generation,
            tools=_names(policy.get("tools")),
            run_tools=_names(policy.get("run_tools", [])),
            gated_tools=_names(policy.get("gated_tools", [])),
            alias_tools=None if alias_tools is None else _names(alias_tools),
            stamp=membership_stamp(alias, alias_tools, member["created_at"], member["updated_at"]),
            binding=str(binding["id"]),
        )
    except (ValueError, TypeError, KeyError):
        # A row the API never writes is a store fault: closed, never admitted.
        raise SharedRefusal(STORE_UNAVAILABLE) from None


async def _read_policy(
    pool: Any, config: SharedProxyConfig, agent_id: str, alias: str
) -> _Policy:
    """One read-only REPEATABLE READ snapshot through the application's role.

    ``pool`` is the resolver's asyncpg pool (the package ships no types). Every
    database failure, including a pool that cannot hand out a connection in
    time, is ``store_unavailable``.
    """

    application_id = uuid.UUID(config.application)
    agent = uuid.UUID(agent_id)
    try:
        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as connection:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                application = await connection.fetchrow(_APPLICATION_SQL, application_id)
                member = await connection.fetchrow(MEMBERSHIP_SQL, application_id, agent, alias)
                binding = await connection.fetchrow(_BINDING_SQL, application_id, agent)
    except STORE_ERRORS:
        raise SharedRefusal(STORE_UNAVAILABLE) from None
    return _evaluate(config, alias, application, member, binding)


# --- request pieces ----------------------------------------------------------------


@dataclass(frozen=True)
class _Caller:
    """The verified claims of an admitted caller token."""

    decision: caller.Decision
    installation: str
    agent_id: str
    agent: str
    exp: int


def _verified(decision: caller.Decision) -> _Caller:
    if (
        decision.installation is None
        or decision.agent_id is None
        or decision.agent is None
        or decision.exp is None
    ):
        raise SharedRefusal(INVALID)
    return _Caller(
        decision=decision,
        installation=decision.installation,
        agent_id=decision.agent_id,
        agent=decision.agent,
        exp=decision.exp,
    )


def _alias(headers: CIMultiDictProxy[str]) -> str:
    presented = headers.getall(ALIAS_HEADER, [])
    if len(presented) != 1 or not caller.alias_ok(presented[0]):
        raise SharedRefusal(ALIAS_REQUIRED)
    return presented[0]


def _identity_encoded(headers: CIMultiDictProxy[str]) -> None:
    """Refuse a ``Content-Encoding`` other than ``identity``.

    The proxy relays a body as sent and never decodes it, so an encoded body
    is one it cannot read: a ``tools/call`` inside it would reach a decoding
    server with no policy, run or grant check. Any coding but a lone
    ``identity`` refuses, a list such as ``gzip, gzip`` included.
    """

    for value in headers.getall("Content-Encoding", []):
        if value.strip().lower() != "identity":
            raise SharedRefusal(BODY_ENCODING_UNSUPPORTED)


async def _read_body(request: web.Request) -> bytes | None:
    if not request.body_exists:
        return None
    try:
        body = await request.read()
    except web.HTTPRequestEntityTooLarge:
        # ``make_shared_app`` sets ``client_max_size`` to the limit, so ``read`` enforces it.
        raise SharedRefusal(BODY_TOO_LARGE) from None
    return body


def _messages(body: bytes | None) -> tuple[dict[str, Any], ...]:
    """The JSON-RPC objects of one object or a batch array.

    A body that is not JSON yields none, so it is forwarded without a handle
    and can authorize no provider call.
    """

    if not body:
        return ()
    try:
        parsed = json.loads(body)
    except (ValueError, RecursionError):
        return ()
    items = parsed if isinstance(parsed, list) else [parsed]
    return tuple(item for item in items if isinstance(item, dict))


def _request_id(value: object) -> str | None:
    """A JSON-RPC id as canonical JSON text, or None when it cannot key a cancellation."""

    if isinstance(value, bool) or not isinstance(value, str | int | float):
        return None
    try:
        return json.dumps(value, separators=(",", ":"), allow_nan=False)
    except ValueError:
        return None


def _without_identity(headers: CIMultiDict[str]) -> CIMultiDict[str]:
    return CIMultiDict(
        (key, value)
        for key, value in headers.items()
        if key.lower() not in _CREDENTIAL_HEADERS and not key.lower().startswith(_CURIE_PREFIX)
    )


def _response_headers(headers: CIMultiDictProxy[str]) -> CIMultiDict[str]:
    forwarded = server._forwardable(headers)
    return CIMultiDict(
        (key, value)
        for key, value in forwarded.items()
        if not key.lower().startswith(_CURIE_PREFIX)
    )


def _registry_code(code: str) -> str:
    """A registry refusal as a shared code."""

    if code in REFUSALS:
        return code
    if code == REGISTRY_FOREIGN:
        return NOT_ADMITTED
    # ``invalid_context``: every context built here is well formed, so what is
    # left is the token's exp passing on the registry's clock.
    return EXPIRED


# --- state ---------------------------------------------------------------------------


@dataclass
class _Session:
    """One recorded MCP session: who opened it, through which alias, on which row."""

    principal: Principal
    alias: str
    stamp: str
    last_used: float
    # Forwarded exchanges on it still running, an open SSE stream included.
    in_flight: int = 0


@dataclass
class _Pending:
    """The cancellation key a live ``tools/call`` holds."""

    invocation: Invocation | None = None
    cancelled: bool = False


_Key = tuple[str, str, str | None, str]


@dataclass
class _Exchange:
    """What one admitted request holds until its upstream exchange ends."""

    caller: _Caller
    alias: str
    session_id: str | None
    row: _Session | None
    body: bytes | None = None
    principal: Principal | None = None
    stamp: str = ""
    reserved_slot: bool = False
    reserved_invocation: bool = False
    key: _Key | None = None
    pending: _Pending | None = None
    invocation: Invocation | None = None
    ends_session: bool = False


class _SharedProxy:
    """The per-application state every request on one app shares.

    Everything here runs on the app's one event loop, so a check followed by
    an insert with no ``await`` between them is atomic.
    """

    def __init__(
        self,
        config: SharedProxyConfig,
        runtime: ResolverRuntime,
        clock: Callable[[], float],
    ) -> None:
        self.config = config
        self.registry: HandleRegistry = runtime.handles
        self.pool: Any = runtime.pool
        self.clock = clock
        self.sessions: dict[str, _Session] = {}
        self.reserved_sessions = 0
        # Invocation slots held by calls between their capacity check and their
        # registration, which a grant spend awaits in.
        self.reserved_invocations = 0
        self.pending: dict[_Key, _Pending] = {}
        self.upstream: ClientSession | None = None
        self.grants = server._ValkeyGrantStore(
            config.grant_store_url,
            prefix=GRANT_KEY_PREFIX,
            failure=SPEND_FAILURE,
        )

    # --- sessions -----------------------------------------------------------------

    def close_session(self, session_id: str) -> None:
        """Forget ``session_id`` and revoke every invocation bound to it."""

        self.sessions.pop(session_id, None)
        self.registry.close_session(session_id)

    def _prune(self, now: float) -> None:
        idle = self.config.session_idle_seconds
        stale = [
            session_id
            for session_id, row in self.sessions.items()
            if row.in_flight == 0 and now - row.last_used > idle
        ]
        for session_id in stale:
            self.close_session(session_id)

    def _reserve_session(self) -> None:
        """Hold one session slot for a response that may open a session."""

        self._prune(self.clock())
        if self.registry.sessions + self.reserved_sessions >= self.registry.max_sessions:
            raise SharedRefusal(SESSIONS_FULL)
        self.reserved_sessions += 1

    def _reserve_invocation(self, exchange: _Exchange) -> None:
        """Hold one invocation slot, so no later refusal is for capacity.

        Checked with the registry's live count and taken with no ``await``
        between, so a grant spent after this is never burned by
        ``invocations_full``.
        """

        registry = self.registry
        if registry.active_invocations + self.reserved_invocations >= registry.max_invocations:
            raise SharedRefusal(INVOCATIONS_FULL)
        self.reserved_invocations += 1
        exchange.reserved_invocation = True

    def _release_invocation(self, exchange: _Exchange) -> None:
        if exchange.reserved_invocation:
            exchange.reserved_invocation = False
            self.reserved_invocations -= 1

    def _release_slot(self, exchange: _Exchange) -> None:
        if exchange.reserved_slot:
            exchange.reserved_slot = False
            self.reserved_sessions -= 1

    def _owned_session(
        self, request: web.Request, verified: _Caller, alias: str
    ) -> tuple[str | None, _Session | None]:
        """The presented session and its row, proved to be this caller's."""

        presented = request.headers.getall(SESSION_HEADER, [])
        if not presented:
            return None, None
        if len(presented) != 1:
            raise SharedRefusal(SESSION_UNKNOWN)
        session_id = presented[0]
        row = self.sessions.get(session_id)
        if row is None:
            raise SharedRefusal(SESSION_UNKNOWN)
        principal = row.principal
        if (
            principal.installation != verified.installation
            or principal.application != self.config.application
            or principal.agent_id != verified.agent_id
            or row.alias != alias
        ):
            raise SharedRefusal(SESSION_PRINCIPAL_MISMATCH)
        return session_id, row

    def _open_session(self, session_id: str, exchange: _Exchange) -> None:
        """Record the session a response opened for ``exchange``'s principal."""

        principal = exchange.principal
        assert principal is not None
        existing = self.sessions.get(session_id)
        if existing is not None and (
            existing.principal != principal or existing.alias != exchange.alias
        ):
            raise SharedRefusal(SESSION_PRINCIPAL_MISMATCH)
        try:
            self.registry.open_session(session_id, principal)
        except RegistryRefusal as refusal:
            raise SharedRefusal(_registry_code(refusal.code)) from None
        row = existing
        if row is None:
            row = self.sessions[session_id] = _Session(
                principal=principal,
                alias=exchange.alias,
                stamp=exchange.stamp,
                last_used=self.clock(),
            )
        # The reservation becomes the session in the same step, so it counts
        # once; and the opening exchange, which may still be streaming, is in
        # flight on it, so idle pruning never takes it while open.
        self._release_slot(exchange)
        row.in_flight += 1
        exchange.row = row

    # --- cancellation and grants ----------------------------------------------------

    def _cancel(self, verified: _Caller, alias: str, session_id: str | None, message: Any) -> None:
        """Revoke the live invocation an authenticated ``notifications/cancelled`` names."""

        params = message.get("params")
        if not isinstance(params, dict) or "requestId" not in params:
            return
        request_id = _request_id(params["requestId"])
        if request_id is None:
            return
        pending = self.pending.get((verified.agent_id, alias, session_id, request_id))
        if pending is None:
            return
        pending.cancelled = True
        if pending.invocation is not None:
            pending.invocation.close()

    def _grant(
        self,
        request: web.Request,
        call: server._ToolCall,
        alias: str,
        policy: _Policy,
        now: int,
    ) -> caller.SharedGrant:
        """The one matching ``csg`` grant for ``call``, verified but not yet spent."""

        presented = request.headers.getall(server.GRANT_HEADER, [])
        if not presented:
            raise _GrantRefused(server.GRANT_HEADER_MISSING)
        if len(presented) != 1:
            raise _GrantRefused(server.GRANT_HEADER_REPEATED)
        grant = caller.verify_shared_grant(self.config.public_keys, presented[0])
        if grant is None:
            raise _GrantRefused(server.GRANT_INVALID)
        if grant.exp <= now:
            raise _GrantRefused(server.GRANT_EXPIRED)
        if grant.application != self.config.application:
            raise _GrantRefused(GRANT_APPLICATION_MISMATCH)
        if grant.binding != policy.binding:
            raise _GrantRefused(GRANT_BINDING_MISMATCH)
        if grant.alias != alias:
            raise _GrantRefused(GRANT_ALIAS_MISMATCH)
        if grant.tool != call.name:
            raise _GrantRefused(server.GRANT_TOOL_MISMATCH)
        canonical = server._canonical_arguments(call.arguments)
        if canonical is None or arguments_sha256(canonical) != grant.args_sha256:
            raise _GrantRefused(server.GRANT_ARGUMENTS_MISMATCH)
        if grant.policy_generation != policy.generation:
            raise _GrantRefused(GRANT_GENERATION_MISMATCH)
        return grant

    async def _spend(self, grant: caller.SharedGrant, now: int) -> None:
        """Spend ``grant`` once in Valkey; any failure refuses. Runs last of every check."""

        # The store answers False, after its own warning, for whatever failed
        # inside it; the catch covers what fails around it, such as handing
        # the call to a worker thread.
        try:
            spent = await asyncio.to_thread(self.grants.spend, grant.jti, grant.exp - now)
        except Exception:  # noqa: BLE001 - a failed spend refuses, whatever raised it
            logger.warning(SPEND_FAILURE)
            spent = False
        if not spent:
            raise _GrantRefused(server.GRANT_NOT_SPENT)

    # --- one request -------------------------------------------------------------------

    def _decide(self, request: web.Request) -> caller.Decision:
        tokens = request.headers.getall(caller.HEADER, [])
        if len(tokens) > 1:
            return caller.Decision(agent=None, refusal=INVALID)
        return caller.decide_shared(
            self.config.public_keys,
            tokens[0] if tokens else None,
            installation=self.config.installation,
            now=int(self.clock()),
        )

    async def _admit(self, request: web.Request, verified: _Caller) -> _Exchange:
        """Every check, then every reservation; the exchange holds what it reserved."""

        alias = _alias(request.headers)
        session_id, row = self._owned_session(request, verified, alias)
        exchange = _Exchange(caller=verified, alias=alias, session_id=session_id, row=row)
        if row is not None:
            # Counted from here, so idle pruning never closes it under this request.
            row.in_flight += 1
            row.last_used = self.clock()
        try:
            await self._check(request, exchange)
        except BaseException:
            self.finish(exchange)
            raise
        return exchange

    async def _check(self, request: web.Request, exchange: _Exchange) -> None:
        verified, alias, session_id, row = (
            exchange.caller,
            exchange.alias,
            exchange.session_id,
            exchange.row,
        )
        _identity_encoded(request.headers)
        exchange.body = await _read_body(request)
        messages = _messages(exchange.body) if request.method == "POST" else ()
        try:
            policy = await _read_policy(self.pool, self.config, verified.agent_id, alias)
        except SharedRefusal as refusal:
            # Ownership is already proved, so the owner's own removal ends it.
            if refusal.code == NOT_MEMBER and session_id is not None:
                self.close_session(session_id)
            raise
        if session_id is not None and self.sessions.get(session_id) is not row:
            # Closed while this request waited on the database.
            raise SharedRefusal(SESSION_UNKNOWN)
        if session_id is not None and row is not None and row.stamp != policy.stamp:
            self.close_session(session_id)
            raise SharedRefusal(POLICY_GENERATION_MISMATCH)

        calls = [message for message in messages if message.get("method") == "tools/call"]
        if len(calls) > 1:
            raise SharedRefusal(MULTIPLE_TOOL_CALLS)
        call = server._tool_call(calls[0]) if calls else None
        if calls and (call is None or not policy.allows(call.name)):
            raise SharedRefusal(TOOL_NOT_ALLOWED)
        run, work_item = run_context(
            verified.decision, requires_run=call is not None and call.name in policy.run_tools
        )
        now = int(self.clock())
        grant = (
            self._grant(request, call, alias, policy, now)
            if call is not None and call.name in policy.gated_tools
            else None
        )

        # A session on this request keeps its opener's principal, so the
        # registry's session check holds; the stamp is this call's, equal to it.
        principal = (
            row.principal
            if row is not None
            else Principal(
                installation=verified.installation,
                application=self.config.application,
                agent_id=verified.agent_id,
                agent=verified.agent,
                policy_generation=policy.generation,
            )
        )
        exchange.principal = principal
        exchange.stamp = policy.stamp
        if session_id is None:
            self._reserve_session()
            exchange.reserved_slot = True
        if calls:
            request_id = _request_id(calls[0].get("id"))
            if request_id is not None:
                key: _Key = (verified.agent_id, alias, session_id, request_id)
                if key in self.pending:
                    raise SharedRefusal(REQUEST_ID_IN_USE)
                exchange.key, exchange.pending = key, _Pending()
                self.pending[key] = exchange.pending
        if call is not None:
            self._reserve_invocation(exchange)
        if grant is not None:
            await self._spend(grant, now)
        for message in messages:
            if message.get("method") == "notifications/cancelled":
                self._cancel(verified, alias, session_id, message)
        if call is not None:
            context = InvocationContext(
                principal=principal,
                alias=alias,
                expires_at=verified.exp,
                binding=policy.binding,
                membership=policy.stamp,
                run=run,
                work_item=work_item,
                session_id=session_id,
            )
            # The held slot becomes the registered invocation in one step.
            self._release_invocation(exchange)
            try:
                exchange.invocation = self.registry.register(context)
            except RegistryRefusal as refusal:
                raise SharedRefusal(_registry_code(refusal.code)) from None
            if exchange.pending is not None:
                exchange.pending.invocation = exchange.invocation
                if exchange.pending.cancelled:
                    # Cancelled while its grant was being spent.
                    exchange.invocation.close()
        exchange.ends_session = request.method == "DELETE" and session_id is not None

    def finish(self, exchange: _Exchange) -> None:
        """Release everything ``exchange`` holds. Runs in ``finally``, cancellation included."""

        if exchange.invocation is not None:
            exchange.invocation.close()
        if exchange.key is not None and self.pending.get(exchange.key) is exchange.pending:
            del self.pending[exchange.key]
        self._release_invocation(exchange)
        self._release_slot(exchange)
        if exchange.row is not None:
            exchange.row.in_flight -= 1
            exchange.row.last_used = self.clock()
            exchange.row = None
        if exchange.ends_session and exchange.session_id is not None:
            self.close_session(exchange.session_id)

    def _log(
        self, request: web.Request, agent: str, outcome: str, grant_reason: str | None = None
    ) -> None:
        # The path is decoded, so it is quoted: an encoded newline stays ``\n``
        # inside this line. ``grant=`` is from a fixed set and never a value.
        logger.info(
            "caller=%s mode=shared outcome=%s method=%s path=%r%s",
            agent,
            outcome,
            request.method,
            request.path,
            f" grant={grant_reason}" if grant_reason is not None else "",
        )

    async def handle(self, request: web.Request) -> web.StreamResponse:
        agent = "-"
        try:
            decision = self._decide(request)
            agent = decision.agent or "-"
            if decision.refusal is not None:
                raise SharedRefusal(decision.refusal)
            exchange = await self._admit(request, _verified(decision))
        except SharedRefusal as refusal:
            reason = refusal.reason if isinstance(refusal, _GrantRefused) else None
            self._log(request, agent, refusal.code, reason)
            return refusal_response(refusal.code)
        try:
            self._log(request, agent, "admitted")
            return await self._forward(request, exchange)
        finally:
            self.finish(exchange)

    async def _forward(self, request: web.Request, exchange: _Exchange) -> web.StreamResponse:
        upstream = self.upstream
        assert upstream is not None, "the upstream client starts with the app"
        target = URL.build(
            scheme="http",
            host="127.0.0.1",
            port=self.config.upstream_port,
            path=request.rel_url.raw_path,
            query_string=request.rel_url.raw_query_string,
            encoded=True,
        )
        forwarded = _without_identity(server._forwardable(request.headers))
        if exchange.invocation is not None:
            forwarded.add(INVOCATION_HEADER, exchange.invocation.handle)
        try:
            answer = await upstream.request(
                request.method,
                target,
                headers=forwarded,
                data=exchange.body,
                allow_redirects=False,
            )
        except ClientError:
            return web.Response(status=502, text="the connector server is unreachable\n")
        try:
            assigned = answer.headers.getall(SESSION_HEADER, [])
            if exchange.reserved_slot and assigned:
                try:
                    if len(assigned) != 1:
                        raise SharedRefusal(SESSION_UNKNOWN)
                    self._open_session(assigned[0], exchange)
                except SharedRefusal as refusal:
                    self._log(request, exchange.caller.agent, refusal.code)
                    return refusal_response(refusal.code)
            if exchange.ends_session and exchange.session_id is not None:
                # The server has answered the DELETE: end the session before the
                # caller hears so, and again in ``finish`` on any other path.
                self.close_session(exchange.session_id)
            response = web.StreamResponse(
                status=answer.status,
                reason=answer.reason,
                headers=_response_headers(answer.headers),
            )
            await response.prepare(request)
            async for chunk in answer.content.iter_any():
                await response.write(chunk)
            await response.write_eof()
            return response
        finally:
            # Also on cancellation: a caller that hangs up closes the server's
            # stream with it.
            answer.release()


_PROXY = web.AppKey("shared_proxy", _SharedProxy)


@web.middleware
async def _handle(request: web.Request, handler: Handler) -> web.StreamResponse:
    del handler  # A middleware, as in dedicated mode: every request is decided here.
    return await request.app[_PROXY].handle(request)


def make_shared_app(
    config: SharedProxyConfig,
    runtime: ResolverRuntime | None,
    *,
    clock: Callable[[], float] = time.time,
) -> web.Application:
    """The shared-mode proxy app in front of ``runtime``, the resolver of this application.

    Refuses (``ValueError``) without a resolver, or with one for another
    installation or application. Serve it with ``handler_cancellation=True``,
    so a caller that hangs up cancels its exchange and revokes its handle.
    """

    if runtime is None:
        raise ValueError("shared application mode needs its resolver")
    registry = runtime.handles
    if (
        registry.installation != config.installation
        or registry.application != config.application
    ):
        raise ValueError("the resolver serves another installation or application")
    idle: object = config.session_idle_seconds
    if (
        isinstance(idle, bool)
        or not isinstance(idle, int | float)
        or not math.isfinite(idle)
        or idle <= 0
    ):
        raise ValueError("session_idle_seconds must be a positive number of seconds")
    proxy = _SharedProxy(config, runtime, clock)

    async def upstream(_app: web.Application) -> AsyncIterator[None]:
        session = server._upstream_client()
        proxy.upstream = session
        yield
        proxy.upstream = None
        await session.close()

    app = web.Application(
        middlewares=[_handle],
        handler_args=server._HANDLER_ARGS,
        client_max_size=server._MAX_BODY_BYTES,
    )
    app[_PROXY] = proxy
    app.cleanup_ctx.append(upstream)
    return app
