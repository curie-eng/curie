"""The shared connector resolver (ADR 0239).

An explicit shared connector application serves several agents from one
server process, so the server cannot hold any one agent's credential at
startup. Instead the proxy in front of it verifies each caller, forwards a
fresh invocation handle, and this resolver, running in the proxy container,
answers ``POST /v1/resolve`` on a pod-local Unix socket: given that handle, a
declared slot and an audit-only purpose, it admits the call against the
authoritative Postgres rows under the application's own restricted role and
releases that binding's credential, or identity-only admission for
connector-held custody. Every provider call needs a new resolve; nothing is
cached between them.

Modules, lowest first:

``wire``        the request shape, refusal codes and their HTTP statuses
``handles``     the invocation handle registry and its bounds
``keys``        the application's X25519 key ring
``config``      the frozen render's environment, read and checked
``privileges``  the role's actual privileges, verified before activation
``admission``   the per-resolve admission transaction
``sources``     the credential source seam and the static and identity sources
``refresh``     refresh-capable sources and the fenced refresh protocol
``service``     the ordered resolve: refuse, admit, release, commit, audit
``http``        the strict HTTP/1.1 server on the socket, and start/stop

The reference SDK connectors vendor lives in ``sdk/`` as a standalone file; it
imports nothing from this package.
"""

from __future__ import annotations

from .admission import Admitted, RefreshIntent
from .config import ResolverConfig
from .handles import (
    HANDLE_PREFIX,
    MAX_INVOCATIONS,
    MAX_SESSIONS,
    HandleRegistry,
    Invocation,
    InvocationContext,
    Principal,
    RegistryRefusal,
)
from .http import ResolverRuntime, ResolverStartRefused, serve_unix, start_resolver
from .keys import KeyRing, KeyUnavailable
from .privileges import PrivilegeRefusal, verify_role
from .refresh import RefreshCapableSource, RefreshCoordinator
from .service import Resolver
from .sources import (
    KIND_CONNECTOR,
    KIND_REFRESH,
    KIND_STATIC,
    ConnectorIdentitySource,
    CredentialSource,
    Field,
    RefreshNeeded,
    Release,
    SourceRefusal,
    SourceRegistry,
    StaticSource,
    classify,
    default_registry,
    parse_envelope,
)
from .wire import (
    MAX_REQUEST_BYTES,
    REFUSAL_STATUS,
    RESOLVE_PATH,
    SOCKET_ENV,
    ResolveRefusal,
    parse_request,
)

__all__ = [
    "HANDLE_PREFIX",
    "KIND_CONNECTOR",
    "KIND_REFRESH",
    "KIND_STATIC",
    "MAX_INVOCATIONS",
    "MAX_REQUEST_BYTES",
    "MAX_SESSIONS",
    "REFUSAL_STATUS",
    "RESOLVE_PATH",
    "SOCKET_ENV",
    "Admitted",
    "ConnectorIdentitySource",
    "CredentialSource",
    "Field",
    "HandleRegistry",
    "Invocation",
    "InvocationContext",
    "KeyRing",
    "KeyUnavailable",
    "Principal",
    "PrivilegeRefusal",
    "RefreshCapableSource",
    "RefreshCoordinator",
    "RefreshIntent",
    "RefreshNeeded",
    "RegistryRefusal",
    "Release",
    "ResolveRefusal",
    "Resolver",
    "ResolverConfig",
    "ResolverRuntime",
    "ResolverStartRefused",
    "SourceRefusal",
    "SourceRegistry",
    "StaticSource",
    "classify",
    "default_registry",
    "parse_envelope",
    "parse_request",
    "serve_unix",
    "start_resolver",
    "verify_role",
]
